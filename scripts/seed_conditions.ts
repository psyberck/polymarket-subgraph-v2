import {
  appendFileSync,
  createReadStream,
  createWriteStream,
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from 'node:fs';
import { once } from 'node:events';
import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import { createInterface } from 'node:readline';
import {
  COLLATERAL_SCALE,
  CONDITIONAL_TOKENS,
  NEG_RISK_ADAPTER,
  NEG_RISK_WRAPPED_COLLATERAL,
  USDC,
} from '@utils/constants';
import { getPositionId } from '@utils/getPositionId';
import { parse as parseYaml } from 'yaml';
import { createPublicClient, http, parseAbi } from 'viem';
import { polygon } from 'viem/chains';

const GAMMA = 'https://gamma-api.polymarket.com/events';
const OUT_DIR = 'logs/markets';
const META_FILE = `${OUT_DIR}/meta.json`;
const RAW = `${OUT_DIR}/markets.jsonl`;

/**
 * The RPC client
 */
const RPC_URL = process.env.POLYGON_RPC_URL;
const client = createPublicClient({
  chain: polygon,
  transport: http(RPC_URL, { retryCount: 5, retryDelay: 500, timeout: 30_000 }),
  batch: { multicall: { batchSize: 16_384, wait: 16 } },
});

/**
 * Which stage to run: fetch, rebuild, verify, or all.
 *
 * Read from argv rather than hardcoded, because rebuild has to be runnable
 * WITHOUT re-fetching -- it is what re-runs when start_block moves and the gate
 * changes. Typed `string` on purpose: a literal narrows every comparison in the
 * dispatch to provably-false and tsc rejects the whole block.
 */
const PHASE: string = (() => {
  const i = process.argv.indexOf('--phase');
  return i === -1 ? 'all' : (process.argv[i + 1] ?? 'all');
})();

const RPS = 50;
const PAGE_SIZE = 100;
const WORKERS = 10;
const MAXIMUM_RETRY = 10;
const EMPTY_WAVES_TO_TERMINATE = 10;

// ──────────────────────────────────── Progress ───────────────────────────────────────────────────

interface Progress {
  eventId: number | null;
  pages: number;
  events: number;
  markets: number;
  updated_at: string;
  completed: boolean;
}

const initialProgress = (): Progress => ({
  eventId: null,
  pages: 0,
  events: 0,
  markets: 0,
  updated_at: new Date().toISOString(),
  completed: false,
});

const getProgress = (): Progress =>
  existsSync(META_FILE)
    ? (JSON.parse(readFileSync(META_FILE, 'utf8')) as Progress)
    : initialProgress();
const saveProgress = (p: Progress) => {
  p.updated_at = new Date().toISOString();
  writeFileSync(META_FILE, JSON.stringify(p, null, 2));
};

async function getBlockId(): Promise<{ block: bigint; ms: number }> {
  const cfg = parseYaml(readFileSync('config.yaml', 'utf8')) as {
    chains?: { id: number; start_block: number }[];
  };
  const chains = cfg.chains ?? [];
  const chain = chains.find((c) => c.id === 137) ?? (chains.length === 1 ? chains[0] : undefined);
  if (!chain)
    throw new Error(`config.yaml: cannot find the polygon chain (${chains.length} defined)`);
  const blockId = BigInt(chain.start_block);
  const block = await client.getBlock({ blockNumber: blockId });
  const ms = Number(block.timestamp) * 1000;
  console.log(`  start_block ${blockId.toString()} -> ${new Date(ms).toISOString()}`);
  return { block: blockId, ms };
}

/**
 * One JSON record on exactly one line.
 *
 * MODULE SCOPE because both fetch paths append lines. JSON.stringify leaves
 * U+2028 and U+2029 RAW while readline SPLITS on them, so one market's question
 * text broke a record in two and JSON.parse got 85 truncated characters. Escaped
 * here, at the only place lines are written.
 */
const jsonLine = (v: unknown): string =>
  JSON.stringify(v)
    .replace(/\u2028/g, '\\u2028')
    .replace(/\u2029/g, '\\u2029');

const getMaxEventId = (evs: GammaEvent[]): number =>
  evs.reduce((hi, e) => {
    const n = Number(e.id);
    return Number.isInteger(n) && n > hi ? n : hi;
  }, 0);

interface GammaEvent {
  id: string;
  markets: unknown[];
}

interface MarketQueryParams {
  startBlockId?: number;
  endBlockId?: number;
  pageSize: number;
}

async function fetchMarkets(): Promise<void> {
  let slot = 0;

  const getEventsByIds = async (params: MarketQueryParams): Promise<GammaEvent[]> => {
    let reason = '';
    const url = new URL(GAMMA);
    url.searchParams.set('limit', String(params.pageSize));
    url.searchParams.set('order', 'id');
    url.searchParams.set('ascending', 'true');
    if (params.startBlockId !== undefined && params.endBlockId !== undefined) {
      for (let id = params.startBlockId; id <= params.endBlockId; id++)
        url.searchParams.append('id', String(id));
    }

    for (let attempt = 1; attempt <= MAXIMUM_RETRY; attempt++) {
      const now = Date.now();
      const at = Math.max(now, slot);
      slot = at + 1000 / RPS;
      if (at > now) await new Promise((r) => setTimeout(r, at - now));

      try {
        const res = await fetch(url);
        if (res.ok) return (await res.json()) as GammaEvent[];
        reason = `HTTP ${res.status}: ${await res.text()}`;
      } catch (e) {
        reason = e instanceof Error ? e.message : String(e);
      }

      if (attempt < MAXIMUM_RETRY) {
        await new Promise((r) => setTimeout(r, Math.min(250 * 2 ** (attempt - 1), 5_000)));
      }
    }
    throw new Error(
      `ids ${params?.startBlockId}-${params?.endBlockId}: ${MAXIMUM_RETRY} attempts reached: ${reason}`,
    );
  };

  mkdirSync(OUT_DIR, { recursive: true });
  const p = getProgress();
  p.completed = false;
  let waves = 0;

  let nextId = 0;
  let none = 0;

  for (;;) {
    let evs: GammaEvent[];

    let currMaxEventId: number;

    if (!p.eventId) {
      evs = await getEventsByIds({ pageSize: PAGE_SIZE });
      p.pages += 1;
      currMaxEventId = getMaxEventId(evs);
      if (!currMaxEventId)
        throw new Error('first page returned no events — cannot find a start id');
      console.log(`  first page ends at id ${currMaxEventId.toLocaleString()}`);
    } else {
      const items: MarketQueryParams[] = [];
      for (let i = p.eventId; items.length < WORKERS; i += PAGE_SIZE) {
        items.push({ startBlockId: i, endBlockId: i + PAGE_SIZE - 1, pageSize: PAGE_SIZE });
      }
      evs = (await Promise.all(items.map((item) => getEventsByIds(item)))).flat();
      p.pages += items.length;
      currMaxEventId = p.eventId + items.length * PAGE_SIZE - 1;
    }

    const markets = evs.flatMap((e) => e.markets ?? []);
    if (markets.length) {
      appendFileSync(RAW, markets.map((m) => jsonLine(m)).join('\n') + '\n');
    }

    nextId = Math.max(nextId, getMaxEventId(evs));
    p.eventId = currMaxEventId + 1;
    p.events += evs.length;
    p.markets += markets.length;
    saveProgress(p);

    if (evs.length === 0) {
      if (++none >= EMPTY_WAVES_TO_TERMINATE) {
        p.eventId = nextId + 1;
        saveProgress(p);
        break;
      }
    } else {
      none = 0;
    }

    if (++waves % 20 === 0) {
      console.log(
        `  id ${currMaxEventId.toLocaleString()}  events=${p.events.toLocaleString()} ` +
          `markets=${p.markets.toLocaleString()}`,
      );
    }
  }

  p.completed = true;
  saveProgress(p);
  console.log(
    `\n  DONE  events=${p.events.toLocaleString()} markets=${p.markets.toLocaleString()}`,
  );
}

const jsonArray = (v: unknown): string[] => {
  if (typeof v !== 'string') return [];
  try {
    const a: unknown = JSON.parse(v);
    return Array.isArray(a) ? (a as string[]) : [];
  } catch {
    return [];
  }
};

/**
 * Settled prices, or null when this market's `outcomePrices` is not a settlement.
 *
 * Rejections are all deferred to a chain read, so the reason no longer needs to
 * ride along. It did once: the reason breakdown is what established that 29.5%
 * of deferrals are 2021 AMM quotes and 69.5% are markets Gamma never priced,
 * and that of 3,506,830 markets NOT ONE was rejected for non-decimal text or
 * for a settlement of the wrong arity.
 *
 * `outcomePrices` is NOT always settled -- it also carries live quotes. A closed
 * 2021 market returns
 * ["0.0000004113679809846114013590098187297978", "0.9999995886..."], which is an
 * AMM price, not a payout. Taking that as a settled price would book a
 * redemption at a market quote.
 *
 * The discriminator is exactness: a settlement is a clean ratio of the payout
 * numerators, so scaled to micro-units every entry is an INTEGER and the vector
 * sums to exactly 1e6. ["0","1"] and ["0.5","0.5"] pass; the 40-decimal quote
 * above does not. This is the same invariant `payout_sum` checks after the fact,
 * applied before the row is ever written.
 */
function settledPrices(raw: string[]): bigint[] | null {
  if (raw.length === 0) return null;
  const out: bigint[] = [];
  let sum = 0n;
  for (const s of raw) {
    // Parse decimal text exactly, without float rounding.
    const m = /^(\d+)(?:\.(\d+))?$/.exec(s.trim());
    if (!m) return null;
    const frac = (m[2] ?? '').padEnd(6, '0');
    if (frac.length > 6 && /[1-9]/.test(frac.slice(6))) return null; // finer than 1e-6
    const v = BigInt(m[1]) * COLLATERAL_SCALE + BigInt(frac.slice(0, 6) || '0');
    out.push(v);
    sum += v;
  }
  return sum === COLLATERAL_SCALE ? out : null;
}

/**
 * questionIndex from questionID's last byte -- FOR NEG-RISK CONDITIONS ONLY.
 *
 * `getNegRiskQuestionId` builds a question id as the market id with its last
 * byte replaced by the index, so the byte IS the index. That construction is
 * neg-risk specific. For a standalone market `questionID` is an ordinary UMA
 * hash and its last byte is arbitrary: reading it anyway produced values like
 * 132, 188 and 73 on 16,958 of 18,914 non-neg-risk conditions -- meaningless
 * data that also DISAGREED with the live handler, which writes questionIndex
 * only from QuestionPrepared and therefore only for neg-risk questions.
 */
function questionIndexOf(negRisk: boolean, questionID: string | undefined): number | null {
  if (!negRisk) return null;
  if (!questionID || !/^0x[0-9a-fA-F]{64}$/.test(questionID)) return null;
  return parseInt(questionID.slice(-2), 16);
}

// ── streaming pipeline: derive and insert in one pass ──────────────
//
// This replaced a phased design that wrote condition.tsv (340 MB) and
// outcome_token.tsv (1.27 GB) and then loaded them -- two full passes over
// derived data plus 1.6 GB of intermediate files whose only reader was the
// loader. Deriving and inserting in one pass keeps memory flat and drops the
// 3.5M-entry dedup Map, because the "prefer priced" rule moved into the upsert.
//
// Equivalence with the phased version was proven before it was deleted: the same
// 12 GB corpus through both paths produced identical row counts and identical
// additive md5 checksums across all 10.6M rows in Condition, OutcomeToken and
// NegRiskEvent.

/**
 * Cap on rows written to the diagnostic TSVs. Holding them unbounded is the
 * mistake the join() emit made: today's 16,619 deferrals fit easily, but a
 * corpus where the gate misbehaves could defer millions and the diagnostic
 * would exhaust the run it exists to explain.
 */
const MAX_DETAIL = 50_000;
/** Conditions buffered before a flush; their tokens ride along. */
const COND_FLUSH = 10_000;
/** Deferred conditions per ConditionResolution getLogs. */
const DEFER_FLUSH = 250;
/** Neg-risk market ids per getQuestionCount multicall. */
const NEGRISK_FLUSH = 400;

interface Sink {
  startBlock: bigint;
  cond: string[];
  tok: string[];
  defer: { id: string; slots: number }[];
  /** Market ids already counted -- one read per event, not per condition. */
  negSeen: Set<string>;
  negBuf: string[];
  mismatchOut: ReturnType<typeof createWriteStream> | null;
  mismatchDetail: number;
  c: {
    lines: number;
    seen: number;
    unparseable: number;
    noConditionId: number;
    noOutcomes: number;
    gateBadTs: number;
    gateAfter: number;
    pricedGated: number;
    deferred: number;
    idMismatch: number;
    conditions: number;
    tokens: number;
    chainPriced: number;
    negCounted: number;
  };
}

const createSink = (startBlock: bigint): Sink => ({
  startBlock,
  cond: [],
  tok: [],
  defer: [],
  negSeen: new Set(),
  negBuf: [],
  mismatchOut: createWriteStream(`${OUT_DIR}/id_mismatch.tsv`),
  mismatchDetail: 0,
  c: {
    lines: 0,
    seen: 0,
    unparseable: 0,
    noConditionId: 0,
    noOutcomes: 0,
    gateBadTs: 0,
    gateAfter: 0,
    pricedGated: 0,
    deferred: 0,
    idMismatch: 0,
    conditions: 0,
    tokens: 0,
    chainPriced: 0,
    negCounted: 0,
  },
});

/**
 * Run a COPY script whose payload is a buffer rather than a file.
 *
 * The file-backed sibling exists because the TSVs are on disk; here the rows
 * were never written anywhere, so they go straight down psql's stdin.
 *
 * @param header - SQL up to and including the COPY statement.
 * @param data - the COPY payload, newline-terminated rows.
 * @param footer - the `\.` terminator and the SQL after it.
 */
async function psqlCopyData(header: string, data: string, footer: string): Promise<void> {
  const { argv, env } = pgCommand('-v', 'ON_ERROR_STOP=1', '-f', '-');
  const child = spawn(argv[0], argv.slice(1), { env, stdio: ['pipe', 'ignore', 'pipe'] });
  let err = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (d: string) => {
    err += d;
  });
  child.stdin.on('error', () => {});
  const closed = once(child, 'close') as Promise<[number | null]>;
  child.stdin.write(header);
  child.stdin.write(data);
  child.stdin.end(footer);
  const [code] = await closed;
  if (code !== 0) throw new Error(`psql exited ${code}: ${err.trim().slice(0, 600)}`);
}

/**
 * Conditions and their tokens, into Postgres.
 *
 * `payoutPrices` upserts through COALESCE rather than plain assignment: that is
 * where `build`'s "prefer the row that has prices" rule lives now that no Map
 * holds every condition. Measured zero duplicate conditionIds across 3,506,830
 * markets, so it never fires today -- it is here so it cannot silently regress.
 *
 * @param sink - buffers to drain.
 * @param force - drain regardless of the threshold.
 */
async function flushRows(sink: Sink, force = false): Promise<void> {
  if (!sink.cond.length || (!force && sink.cond.length < COND_FLUSH)) return;
  const cols = quoted(CONDITION_COLS);
  await psqlCopyData(
    'BEGIN;\nCREATE TEMP TABLE _c (LIKE "Condition" INCLUDING DEFAULTS) ON COMMIT DROP;\n' +
      `COPY _c (${cols}) FROM stdin WITH (FORMAT text, NULL '\\N');\n`,
    sink.cond.join('\n') + '\n',
    '\\.\n' +
      `INSERT INTO "Condition" (${cols}) SELECT ${cols} FROM _c\n` +
      'ON CONFLICT ("id") DO UPDATE SET\n' +
      '  "outcomeSlotCount" = EXCLUDED."outcomeSlotCount",\n' +
      '  "negRisk" = EXCLUDED."negRisk",\n' +
      '  "negRiskMarketId" = EXCLUDED."negRiskMarketId",\n' +
      '  "questionIndex" = EXCLUDED."questionIndex",\n' +
      '  "payoutPrices" = COALESCE(EXCLUDED."payoutPrices", "Condition"."payoutPrices");\n' +
      'COMMIT;\n',
  );
  sink.c.conditions += sink.cond.length;
  sink.cond.length = 0;

  if (sink.tok.length) {
    const tcols = quoted(TOKEN_COLS);
    await psqlCopyData(
      'BEGIN;\nCREATE TEMP TABLE _t (LIKE "OutcomeToken" INCLUDING DEFAULTS) ON COMMIT DROP;\n' +
        `COPY _t (${tcols}) FROM stdin WITH (FORMAT text, NULL '\\N');\n`,
      sink.tok.join('\n') + '\n',
      '\\.\n' +
        `INSERT INTO "OutcomeToken" (${tcols}) SELECT ${tcols} FROM _t\n` +
        'ON CONFLICT ("id") DO UPDATE SET "conditionId" = EXCLUDED."conditionId", ' +
        '"outcomeIndex" = EXCLUDED."outcomeIndex", "collateral" = EXCLUDED."collateral";\n' +
        'COMMIT;\n',
    );
    sink.c.tokens += sink.tok.length;
    sink.tok.length = 0;
  }
}

/**
 * Settled prices from the chain, for the conditions Gamma could not price.
 *
 * Flushes pending rows FIRST: the UPDATE targets Condition rows by id, so a
 * deferral whose condition has not been inserted yet would match nothing and
 * the price would be lost with no error.
 *
 * `toBlock` is pinned to startBlock - 1, so a resolution inside the indexed
 * window is never returned rather than filtered out afterwards -- the temporal
 * guarantee is structural, not a check that can be forgotten.
 *
 * @param sink - buffers to drain.
 * @param force - drain regardless of the threshold.
 */
async function flushDeferrals(sink: Sink, force = false): Promise<void> {
  if (!sink.defer.length || (!force && sink.defer.length < DEFER_FLUSH)) return;
  await flushRows(sink, true);

  const batch = sink.defer.splice(0, sink.defer.length);
  const ids = batch.map((d) => d.id as `0x${string}`);
  const logs = await client.request({
    method: 'eth_getLogs',
    params: [
      {
        address: CONDITIONAL_TOKENS,
        topics: [CONDITION_RESOLUTION as `0x${string}`, ids],
        fromBlock: '0x0',
        toBlock: `0x${(sink.startBlock - 1n).toString(16)}`,
      },
    ],
  });

  const rows: string[] = [];
  for (const l of logs) {
    const d = l.data.slice(2);
    const word = (n: number) => BigInt(`0x${d.slice(n * 64, (n + 1) * 64)}`);
    const off = Number(word(1)) / 32;
    const len = Number(word(off));
    const nums = Array.from({ length: len }, (_, k) => word(off + 1 + k));
    const den = nums.reduce((a, b) => a + b, 0n);
    if (den === 0n) continue; // cancelled: nothing to price
    const prices = nums.map((n) => (n * COLLATERAL_SCALE) / den);
    rows.push(`${l.topics[1]?.toLowerCase()}\t{${prices.join(',')}}`);
  }
  if (!rows.length) return;
  sink.c.chainPriced += rows.length;

  // Guarded on IS NULL so a chain answer can never overwrite an API answer.
  await psqlCopyData(
    'BEGIN;\nCREATE TEMP TABLE _p (id text, "payoutPrices" text[]) ON COMMIT DROP;\n' +
      'COPY _p (id, "payoutPrices") FROM stdin WITH (FORMAT text, NULL \'\\N\');\n',
    rows.join('\n') + '\n',
    '\\.\nUPDATE "Condition" c SET "payoutPrices" = p."payoutPrices"\n' +
      'FROM _p p WHERE c.id = p.id AND c."payoutPrices" IS NULL;\nCOMMIT;\n',
  );
}

/**
 * `NegRiskEvent.questionCount`, read from the NegRiskAdapter.
 *
 * Not derived from max(questionIndex)+1 because Gamma's market list can be
 * short, and an undercount makes `indexSet >> questionCount` reject legitimate
 * conversions and misprices every YES leg through negRiskYesPrice's denominator.
 *
 * @param sink - buffers to drain.
 * @param force - drain regardless of the threshold.
 */
async function flushNegRisk(sink: Sink, force = false): Promise<void> {
  if (!sink.negBuf.length || (!force && sink.negBuf.length < NEGRISK_FLUSH)) return;
  const batch = sink.negBuf.splice(0, sink.negBuf.length);
  // Issued together so the shared client's multicall can fold them; awaiting one
  // at a time would batch nothing.
  const counts = await Promise.all(
    batch.map((id) =>
      client
        .readContract({
          address: NEG_RISK_ADAPTER,
          abi: NEG_RISK_ABI,
          functionName: 'getQuestionCount',
          args: [id as `0x${string}`],
        })
        .then((n) => Number(n))
        .catch(() => null),
    ),
  );
  const rows: string[] = [];
  for (let k = 0; k < batch.length; k++) {
    const n = counts[k];
    if (!n) continue; // unreadable or zero: write nothing rather than a guess
    rows.push(`${batch[k]}\t${n}`);
  }
  if (!rows.length) return;
  sink.c.negCounted += rows.length;
  const ncols = quoted(NEGRISK_COLS);
  await psqlCopyData(
    'BEGIN;\nCREATE TEMP TABLE _n (LIKE "NegRiskEvent" INCLUDING DEFAULTS) ON COMMIT DROP;\n' +
      `COPY _n (${ncols}) FROM stdin WITH (FORMAT text, NULL '\\N');\n`,
    rows.join('\n') + '\n',
    '\\.\n' +
      `INSERT INTO "NegRiskEvent" (${ncols}) SELECT ${ncols} FROM _n\n` +
      'ON CONFLICT ("id") DO UPDATE SET "questionCount" = EXCLUDED."questionCount";\n' +
      'COMMIT;\n',
  );
}

/**
 * One market: gate it, derive its token ids, buffer its rows.
 *
 * Extracted verbatim from `build`'s loop body so the two paths cannot drift --
 * the equivalence test compares their output against the same corpus.
 *
 * @param m - one parsed Gamma market object.
 * @param gateTs - start_block's timestamp in ms; payouts are seeded only from
 *   markets Gamma last touched before it.
 * @param sink - buffers and counters to write into.
 */
function deriveMarket(m: Record<string, unknown>, gateTs: number, sink: Sink): void {
  const c = sink.c;
  const conditionId = (m.conditionId as string | undefined)?.toLowerCase();
  if (!conditionId || !/^0x[0-9a-f]{64}$/.test(conditionId)) {
    c.noConditionId++;
    return;
  }
  c.seen++;

  const outcomes = jsonArray(m.outcomes);
  const clob = jsonArray(m.clobTokenIds);
  const outcomeSlotCount = outcomes.length || clob.length;
  if (!outcomeSlotCount) {
    c.noOutcomes++;
    return;
  }

  const negRisk = m.negRisk === true;
  const questionIndex = questionIndexOf(negRisk, m.questionID as string | undefined);

  for (let i = 0; i < Math.min(clob.length, outcomeSlotCount); i++) {
    const want = clob[i];
    if (!want) continue;
    const got = getPositionId(conditionId, i, negRisk).toString();
    if (got !== want) {
      c.idMismatch++;
      if (sink.mismatchDetail < MAX_DETAIL) {
        sink.mismatchDetail++;
        sink.mismatchOut?.write(
          [conditionId, i, outcomeSlotCount, negRisk ? 't' : 'f', want, got].join('\t') + '\n',
        );
      }
    }
  }

  let payoutPrices: bigint[] | null = null;
  const updatedAt = Date.parse((m.updatedAt as string) ?? '');
  if (Number.isNaN(updatedAt)) c.gateBadTs++;
  else if (updatedAt >= gateTs) c.gateAfter++;

  if (!Number.isNaN(updatedAt) && updatedAt < gateTs) {
    const prices = settledPrices(jsonArray(m.outcomePrices));
    // Arity still checked: a vector of the wrong length would break the
    // payout-len invariant. Zero occurrences in 3,506,830 markets.
    if (prices && prices.length === outcomeSlotCount) {
      payoutPrices = prices;
      c.pricedGated++;
    } else {
      c.deferred++;
      sink.defer.push({ id: conditionId, slots: outcomeSlotCount });
    }
  }

  const N = '\\N';
  const negRiskMarketId = ((m.negRiskMarketID as string) ?? null)?.toLowerCase() ?? null;
  sink.cond.push(
    [
      conditionId,
      outcomeSlotCount,
      negRisk ? 't' : 'f',
      negRiskMarketId ?? N,
      questionIndex ?? N,
      payoutPrices ? `{${payoutPrices.join(',')}}` : N,
    ].join('\t'),
  );
  const collateral = negRisk ? NEG_RISK_WRAPPED_COLLATERAL : USDC;
  for (let i = 0; i < outcomeSlotCount; i++) {
    sink.tok.push(
      [getPositionId(conditionId, i, negRisk).toString(), conditionId, i, collateral].join('\t'),
    );
  }
  if (negRisk && negRiskMarketId && !sink.negSeen.has(negRiskMarketId)) {
    sink.negSeen.add(negRiskMarketId);
    sink.negBuf.push(negRiskMarketId);
  }
}

// ── phase: chain ───────────────────────────────────────────────────

const CONDITION_RESOLUTION = '0xb44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894';

// ── phase: negrisk ─────────────────────────────────────────────────

const NEG_RISK_ABI = parseAbi([
  'function getQuestionCount(bytes32 marketId) view returns (uint256)',
]);

// ── phase: load ────────────────────────────────────────────────────

const execFileAsync = promisify(execFile);

/** Column order must match the TSV writers above. A mismatch is SILENT:
 *  Postgres reads questionIndex into outcomeSlotCount happily, both being
 *  integers. */
const CONDITION_COLS = [
  'id',
  'outcomeSlotCount',
  'negRisk',
  'negRiskMarketId',
  'questionIndex',
  'payoutPrices',
];
const TOKEN_COLS = ['id', 'conditionId', 'outcomeIndex', 'collateral'];
const NEGRISK_COLS = ['id', 'questionCount'];

/**
 * argv and env for invoking psql.
 *
 * Mirrors scripts/_pgconn.py, which the Python verification scripts share. The
 * env vars are deliberately the ones envio itself reads, so the seed and the
 * indexer cannot end up pointed at different databases -- which would produce
 * results that look clean because they describe another dataset.
 *
 * @param flags - psql flags appended to the invocation; callers differ.
 * @returns argv to spawn, and the env to spawn it with.
 */
function pgCommand(...flags: string[]): { argv: string[]; env: NodeJS.ProcessEnv } {
  const host = process.env.ENVIO_PG_HOST;
  const user = process.env.ENVIO_PG_USER ?? 'postgres';
  const database = process.env.ENVIO_PG_DATABASE ?? 'envio-dev';

  // No host means the local dev container. A real host is the RDS path.
  if (!host) {
    return {
      argv: [
        'docker',
        'exec',
        '-i',
        process.env.ENVIO_PG_CONTAINER ?? 'envio-postgres',
        'psql',
        '-U',
        user,
        '-d',
        database,
        ...flags,
      ],
      env: process.env,
    };
  }

  const env: NodeJS.ProcessEnv = { ...process.env };
  if (process.env.ENVIO_PG_PASSWORD) env.PGPASSWORD = process.env.ENVIO_PG_PASSWORD;
  // `require`, not libpq's `prefer`: prefer falls back to plaintext against an
  // instance without rds.force_ssl and sends the password in the clear.
  env.PGSSLMODE ??= process.env.ENVIO_PG_SSLMODE ?? 'require';
  // Fail fast rather than hang on the TCP default when a security group has not
  // been opened -- the likeliest first-run failure against RDS.
  env.PGCONNECT_TIMEOUT ??= '15';
  return {
    argv: [
      'psql',
      '-h',
      host,
      '-p',
      process.env.ENVIO_PG_PORT ?? '5432',
      '-U',
      user,
      '-d',
      database,
      '-v',
      'ON_ERROR_STOP=1',
      ...flags,
    ],
    env,
  };
}

/**
 * One scalar out of Postgres.
 *
 * @param sql - a query returning a single value.
 * @returns Its value as trimmed text, '' when the query returned no rows.
 */
async function psql(sql: string): Promise<string> {
  const { argv, env } = pgCommand('-tAc', sql);
  try {
    const { stdout } = await execFileAsync(argv[0], argv.slice(1), {
      env,
      maxBuffer: 1 << 24,
    });
    return stdout.trim();
  } catch (e) {
    const err = e as { stderr?: string; message?: string };
    throw new Error(`psql: ${(err.stderr || err.message || '').trim().slice(0, 400)}`, {
      cause: e,
    });
  }
}

const quoted = (cols: string[]): string => cols.map((c) => `"${c}"`).join(', ');

/**
 * The structural invariants that would make a loaded seed useless.
 *
 * Read-only. The rows are written by the streaming pipeline as it goes, so this
 * runs after it and is the only check that sees BOTH tables -- the two
 * cross-table invariants below cannot be evaluated during the stream.
 *
 * The seed itself must be in place AFTER `envio local db-migrate setup` and
 * BEFORE `envio start`: envio's `initialize()` is `DROP SCHEMA ... CASCADE` and
 * runs whenever `isInitialized()` is false, so seeding first loses everything.
 * Seeded rows with zero `envio_history_*` entries do survive a restart, but they
 * do not survive an initialize.
 *
 * @returns Number of structural problems found; 0 is clean.
 */
async function verifySeed(): Promise<number> {
  // ── verification ─────────────────────────────────────────────────
  console.log('\n  verification');
  const scalar = async (sql: string): Promise<number> => Number(await psql(sql)) || 0;
  const conds = await scalar('SELECT count(*) FROM "Condition"');
  const toks = await scalar('SELECT count(*) FROM "OutcomeToken"');
  const priced = await scalar('SELECT count(*) FROM "Condition" WHERE "payoutPrices" IS NOT NULL');
  const negs = await scalar('SELECT count(*) FROM "NegRiskEvent"');
  console.log(`    Condition rows                ${conds.toLocaleString().padStart(12)}`);
  console.log(`    OutcomeToken rows             ${toks.toLocaleString().padStart(12)}`);
  console.log(`    ...with payoutPrices          ${priced.toLocaleString().padStart(12)}`);
  console.log(`    NegRiskEvent rows             ${negs.toLocaleString().padStart(12)}`);

  const checks: { label: string; sql: string }[] = [
    {
      // Every OutcomeToken must name a Condition that exists, or the reverse
      // lookup resolves to a condition the fold cannot price.
      label: 'OutcomeToken with no Condition',
      sql:
        'SELECT count(*) FROM "OutcomeToken" t ' +
        'LEFT JOIN "Condition" c ON c.id = t."conditionId" WHERE c.id IS NULL',
    },
    {
      // One row per (condition, outcome). A duplicate makes the reverse lookup
      // ambiguous.
      label: 'duplicate (cond, outcome)',
      sql:
        'SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) n FROM "OutcomeToken" ' +
        'GROUP BY "conditionId", "outcomeIndex" HAVING count(*) > 1) d',
    },
    {
      // A condition's token count must match its own outcomeSlotCount.
      //
      // Aggregate-then-join, NOT a correlated subquery per condition. envio
      // indexes OutcomeToken on its primary key only, so
      // `count(*) WHERE conditionId = c.id` per row scans every token for every
      // condition: ~2 minutes on a 32k sample, and quadratic, so unusable at
      // 3.5M conditions against 7M tokens. This is one scan plus a hash join.
      label: 'token count <> outcomeSlots',
      sql:
        'SELECT count(*) FROM "Condition" c ' +
        'LEFT JOIN (SELECT "conditionId", count(*) n FROM "OutcomeToken" ' +
        '           GROUP BY "conditionId") t ON t."conditionId" = c.id ' +
        'WHERE coalesce(t.n, 0) <> c."outcomeSlotCount"',
    },
    {
      // questionIndex is meaningless on a standalone condition, and writing one
      // disagrees with the live handler, which sets it only from
      // QuestionPrepared. This caught a real bug: the seed read the last byte of
      // an ordinary UMA question hash and stored it, giving 16,958 of 18,914
      // non-neg-risk conditions a bogus index (132, 188, 73...). Nothing else
      // constrains that column and every summary counter looked healthy.
      label: 'questionIndex w/o negRisk',
      sql: 'SELECT count(*) FROM "Condition" WHERE "negRisk" = false AND "questionIndex" IS NOT NULL',
    },
    {
      // The mirror: a neg-risk condition with no index cannot be placed within
      // its event, so negRiskYesPrice has no leg to price.
      label: 'negRisk w/o questionIndex',
      sql: 'SELECT count(*) FROM "Condition" WHERE "negRisk" = true AND "questionIndex" IS NULL',
    },
  ];

  if (negs) {
    checks.push(
      {
        label: 'negRisk cond with no event',
        sql:
          'SELECT count(*) FROM "Condition" c LEFT JOIN "NegRiskEvent" e ' +
          'ON e.id = c."negRiskMarketId" ' +
          'WHERE c."negRiskMarketId" IS NOT NULL AND e.id IS NULL',
      },
      {
        // questionIndex >= questionCount means the condition claims to be
        // question N of an event with fewer than N questions -- exactly what a
        // short count looks like, and a short count makes
        // `indexSet >> questionCount` reject real conversions and misprice every
        // YES leg. Only checkable once both tables are loaded, which is why it
        // lives here rather than in either phase.
        label: 'questionIndex >= count',
        sql:
          'SELECT count(*) FROM "Condition" c JOIN "NegRiskEvent" e ' +
          'ON e.id = c."negRiskMarketId" ' +
          'WHERE c."questionIndex" IS NOT NULL AND c."questionIndex" >= e."questionCount"',
      },
    );
  }

  let problems = 0;
  for (const { label, sql } of checks) {
    const n = await scalar(sql);
    problems += n;
    console.log(`    ${label.padEnd(30)}${n.toLocaleString().padStart(12)}${n ? '  <- BAD' : ''}`);
  }

  if (problems) {
    console.log(
      `\n  ${problems.toLocaleString()} structural problem(s). Do not start the indexer.`,
    );
  } else {
    console.log('\n  clean. Safe to `envio start` — it will resume and keep these rows.');
  }
  return problems;
}

/**
 * Derive and insert straight from the archived corpus, with no TSVs.
 *
 * The offline half of the streaming pipeline: same derivation as the network
 * path, fed from markets.jsonl instead of Gamma. This is what re-runs when
 * start_block moves -- the gate changes, every payout decision changes, and
 * nothing needs re-fetching.
 *
 * STREAMED, not read whole: the corpus is 12 GB and Node caps a string near
 * 512 MB.
 *
 * @param gateTs - start_block's timestamp in ms.
 * @param startBlock - start_block; chain reads are pinned below it.
 * @returns Structural problems found by the closing verification; 0 is clean.
 */
async function rebuildFromFile(gateTs: number, startBlock: bigint): Promise<number> {
  if (!existsSync(RAW)) throw new Error(`${RAW} missing — run --phase fetch first`);

  // Same guard as the loader: seed rows would overwrite event-derived ones.
  const indexed = Number(await psql('SELECT count(*) FROM "UserPosition"')) || 0;
  if (indexed) {
    throw new Error(
      `UserPosition already has ${indexed.toLocaleString()} rows — the indexer has run. ` +
        'Seed before `envio start`, not after.',
    );
  }

  const sink = createSink(startBlock);
  const t0 = Date.now();
  const rl = createInterface({ input: createReadStream(RAW, 'utf8'), crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line.trim()) continue;
    sink.c.lines++;
    let m: Record<string, unknown>;
    try {
      m = JSON.parse(line) as Record<string, unknown>;
    } catch {
      sink.c.unparseable++;
      continue;
    }
    deriveMarket(m, gateTs, sink);
    await flushRows(sink);
    await flushDeferrals(sink);
    await flushNegRisk(sink);
    if (sink.c.lines % 500_000 === 0) {
      console.log(
        `    ${sink.c.lines.toLocaleString()} markets  ` +
          `${sink.c.conditions.toLocaleString()} conditions in  ` +
          `${Math.round((Date.now() - t0) / 1000)}s`,
      );
    }
  }
  // Remainders, regardless of thresholds. Deferrals last but one: the flush
  // drains pending rows first, so their conditions exist before the UPDATE.
  await flushRows(sink, true);
  await flushDeferrals(sink, true);
  await flushNegRisk(sink, true);
  sink.mismatchOut?.end();
  if (sink.mismatchOut) await once(sink.mismatchOut, 'finish');

  const c = sink.c;
  console.log(`\n  markets parsed          ${c.seen.toLocaleString()}`);
  console.log(`  Condition rows          ${c.conditions.toLocaleString()}`);
  console.log(`  OutcomeToken rows       ${c.tokens.toLocaleString()}`);
  console.log(`  payoutPrices seeded     ${c.pricedGated.toLocaleString()}`);
  console.log(`  deferred to chain       ${c.deferred.toLocaleString()}`);
  console.log(`  ...priced from chain    ${c.chainPriced.toLocaleString()}`);
  console.log(`  NegRiskEvent counted    ${c.negCounted.toLocaleString()}`);
  console.log(`  derived-id mismatches   ${c.idMismatch.toLocaleString()}  (expect 95)`);
  console.log(`\n  skipped before the gate`);
  console.log(`    no/invalid conditionId  ${c.noConditionId.toLocaleString()}`);
  console.log(`    no outcomes or clob ids ${c.noOutcomes.toLocaleString()}`);
  console.log(`\n  gate (updatedAt < ${new Date(gateTs).toISOString()})`);
  console.log(`    before  -> considered   ${(c.pricedGated + c.deferred).toLocaleString()}`);
  console.log(`    at/after -> no price    ${c.gateAfter.toLocaleString()}`);
  console.log(`    updatedAt unparseable   ${c.gateBadTs.toLocaleString()}`);
  const accounted =
    c.unparseable +
    c.noConditionId +
    c.noOutcomes +
    c.gateAfter +
    c.gateBadTs +
    c.pricedGated +
    c.deferred;
  console.log(
    `    accounted for           ${accounted.toLocaleString()}` +
      (accounted === c.lines
        ? `  = ${c.lines.toLocaleString()} lines`
        : `  != ${c.lines.toLocaleString()} LINES -- BUCKETS DO NOT BALANCE`),
  );
  console.log(`\n  elapsed ${Math.round((Date.now() - t0) / 1000)}s`);
  return verifySeed();
}

// ── main ───────────────────────────────────────────────────────────
//
// fetch appends to markets.jsonl; rebuild derives from it and inserts. There is
// no build/chain/negrisk/load any more -- rebuild does all four in one pass, and
// its equivalence with the phased version was verified by checksumming all
// 10.6M rows against the output of the old path on the same corpus.
if (PHASE === 'fetch' || PHASE === 'all') await fetchMarkets();
if (PHASE === 'rebuild' || PHASE === 'all') {
  const w = await getBlockId();
  if (await rebuildFromFile(w.ms, w.block)) process.exitCode = 1;
}
if (PHASE === 'verify') {
  if (await verifySeed()) process.exitCode = 1;
}

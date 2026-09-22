/**
 * Derive the seed from the corpus and insert it.
 *
 * Reads markets.jsonl, reads the chain for what Gamma could not answer, writes
 * Condition / OutcomeToken / NegRiskEvent, then runs stage 3 as a closing gate.
 *
 * MUST RUN AFTER `envio local db-migrate setup` AND BEFORE `envio start`.
 */
import { createReadStream, readFileSync } from 'node:fs';
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
import { RAW } from './utils/literals';
import { count, exec, pool } from './utils/postgres';
import { verify } from './verify_conditions';

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
 * Batch size
 */
const INSERT_BATCH = 10_000;
const NEGRISK_FLUSH_SIZE = 400;
const PAYOUT_FLUSH_SIZE = 250;

const CONDITION_RESOLUTION = '0xb44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894';

const NEG_RISK_ABI = parseAbi([
  'function getQuestionCount(bytes32 marketId) view returns (uint256)',
]);

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
 * Sink Status
 */
interface SinkStats {
  lines: number;
  parsed: number;
  tokens: number;
  unparseable: number;
  noConditionId: number;
  noOutcomes: number;
  gateBadTs: number;
  gateAfter: number;
  pricedGated: number;
  deferred: number;
  idMismatch: number;
  chainPriced: number;
  negCounted: number;
  conditions: number;
}

/** A batched INSERT sink. `flush` sends what is buffered without closing it. */
interface Inserter {
  write: (vals: Val[]) => Promise<void>;
  flush: () => Promise<void>;
  close: () => Promise<void>;
}

/**
 * A value bound as an INSERT parameter.
 *
 * `string[]` is for text[] columns -- node-postgres serialises a JS array into a
 * Postgres array literal, which a joined string would not be.
 */
type Val = string | number | boolean | string[] | null;

interface Market {
  conditionId: string;
  negRisk: boolean;
  collateral: string;
  questionIndex: number | null;
  negRiskMarketId: string | null;
  payoutPrices: bigint[] | null;
  outcomeSlotCount: number;
  invalidPayoutPrices: boolean;
}

/**
 * The indexed window's start, from config.yaml.
 *
 * Read from config rather than passed in, so the seed's gate and the indexer's
 * first block cannot disagree.
 *
 * @returns The start block and its timestamp in ms, which is the payout gate.
 */
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
 * A Gamma field that holds JSON *inside* a string.
 *
 * `outcomes`, `clobTokenIds` and `outcomePrices` all arrive as encoded text
 * rather than arrays, and malformed ones are common enough that throwing would
 * end a 12 GB pass over one bad market.
 *
 * @param v - the raw field.
 * @returns Its elements, or [] if absent, unparseable, or not an array.
 */
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
 * Settled prices, or null when `outcomePrices` is not a settlement.
 *
 * It also carries LIVE QUOTES: a closed 2021 market returns
 * ["0.0000004113679809846114013590098187297978", "0.9999995886..."] -- an AMM
 * price, not a payout -- and taking it would book a redemption at a market quote.
 *
 * The discriminator is exactness. A settlement is a clean ratio of the payout
 * numerators, so in micro-units every entry is an INTEGER and the vector sums to
 * exactly 1e6; ["0","1"] and ["0.5","0.5"] pass, the quote above does not. Same
 * invariant `payout_sum` checks after the fact, applied before the row exists.
 *
 * @param raw - decoded `outcomePrices`.
 * @returns Micro-unit prices, or null. No reason accompanies a rejection because
 *   every one of them is deferred to a ConditionResolution read.
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
 * Zeroed counters for one rebuild.
 *
 * @returns Fresh stats. Pure -- it opens no database resource, so it is safe to
 *   construct outside a run.
 */
const createSinkStats = (): SinkStats => ({
  lines: 0,
  parsed: 0,
  tokens: 0,
  unparseable: 0,
  noConditionId: 0,
  noOutcomes: 0,
  gateBadTs: 0,
  gateAfter: 0,
  pricedGated: 0,
  deferred: 0,
  idMismatch: 0,
  chainPriced: 0,
  negCounted: 0,
  conditions: 0,
});

/**
 * Buffers rows and INSERTs them in batches, with bound parameters.
 *
 * EVERY `write` MUST PASS EXACTLY `cols.length` VALUES: placeholders are numbered
 * `$(k * cols.length + c + 1)`, so a short row shifts every later tuple onto the
 * wrong parameters.
 *
 * No ON CONFLICT, so a duplicate id aborts the load. The TRUNCATE in
 * rebuildFromFile covers rows already in the table, but NOT duplicate lines in
 * the corpus: fetch appends before it saves progress, so a crash mid-wave leaves
 * lines the resume point does not know about and the next run re-appends them.
 * Measured 0 duplicates across 3,581,853 markets, so this has never fired -- but
 * it is an unguarded assumption, not a guarantee.
 *
 * `limit` exists only to stay under the 65,535 parameters one statement allows,
 * hence dividing by the column count.
 *
 * @param table - destination table, which must be empty.
 * @param cols - column order; `write` supplies one value per entry, in order.
 * @returns A handle to `write` rows into, `flush`, and `close` when done.
 */
function insertHandler(table: string, cols: string[]): Inserter {
  const names = cols.map((c) => `"${c}"`).join(', ');
  const head = `INSERT INTO "${table}" (${names}) VALUES `;
  const limit = Math.min(INSERT_BATCH, Math.floor(65_000 / cols.length));
  let rows: Val[][] = [];

  const write = async (vals: Val[]): Promise<void> => {
    rows.push(vals);
    if (rows.length >= limit) await flush();
  };

  const flush = async (): Promise<void> => {
    if (!rows.length) return;
    const batch = rows;
    rows = [];
    const tuples = batch.map(
      (r, k) => `(${r.map((_, c) => `$${k * cols.length + c + 1}`).join(',')})`,
    );
    await pool.query(head + tuples.join(','), batch.flat());
  };

  const close = async (): Promise<void> => {
    await flush();
  };

  return {
    write,
    flush,
    close,
  };
}

const condOut = insertHandler('Condition', CONDITION_COLS);
const tokOut = insertHandler('OutcomeToken', TOKEN_COLS);
const negOut = insertHandler('NegRiskEvent', NEGRISK_COLS);

/**
 * Settled prices from the chain, for conditions Gamma could not price.
 *
 * FLUSHES condOut first: the UPDATE joins on Condition.id, and rows still
 * buffered in insertHandler would match nothing and lose their prices silently.
 *
 * Guarded on `IS NULL` so a chain answer never overwrites an API one -- they
 * should agree, and quietly preferring one would hide it if they did not.
 *
 * `toBlock` is pinned to startBlock - 1, so a resolution inside the indexed
 * window is never returned rather than filtered out after the fact.
 *
 * @param startBlock - chain reads are pinned below it.
 * @param conditionIds - conditions to look up; not mutated, the caller drains.
 * @returns How many conditions were priced.
 */
async function updatePayoutPrices(
  startBlock: bigint,
  conditionIds: readonly string[],
): Promise<number> {
  if (!conditionIds.length) return 0;
  const logs = await client.request({
    method: 'eth_getLogs',
    params: [
      {
        address: CONDITIONAL_TOKENS,
        topics: [CONDITION_RESOLUTION as `0x${string}`, conditionIds as `0x${string}`[]],
        fromBlock: '0x0',
        toBlock: `0x${(startBlock - 1n).toString(16)}`,
      },
    ],
  });

  const payoutPrices: Record<string, bigint[]> = {};
  for (const l of logs) {
    const d = l.data.slice(2);
    const word = (n: number) => BigInt(`0x${d.slice(n * 64, (n + 1) * 64)}`);
    const off = Number(word(1)) / 32;
    const len = Number(word(off));
    const nums = Array.from({ length: len }, (_, k) => word(off + 1 + k));
    const den = nums.reduce((a, b) => a + b, 0n);
    if (den === 0n) continue;
    const conditionId = l.topics[1]?.toLowerCase();
    if (!conditionId) {
      console.warn(`empty condition id has been detected from ${l.topics.join(',')}`);
      continue;
    }
    payoutPrices[conditionId] = nums.map((n) => (n * COLLATERAL_SCALE) / den);
  }
  const entries = Object.entries(payoutPrices);
  if (!entries.length) return 0;

  await condOut.flush();
  await pool.query(
    'UPDATE "Condition" c SET "payoutPrices" = v.prices::text[] ' +
      'FROM (SELECT * FROM unnest($1::text[], $2::text[]) AS t(id, prices)) v ' +
      'WHERE c.id = v.id AND c."payoutPrices" IS NULL',
    [entries.map(([id]) => id), entries.map(([, prices]) => `{${prices.join(',')}}`)],
  );
  return entries.length;
}

/**
 * `NegRiskEvent.questionCount`, read from the NegRiskAdapter.
 *
 * Not derived from max(questionIndex)+1: Gamma's market list can be short, and
 * an undercount makes `indexSet >> questionCount` reject legitimate conversions
 * and misprices every YES leg through negRiskYesPrice's denominator.
 *
 * Reads are issued together so viem's multicall can fold them; awaiting one at a
 * time would batch nothing.
 *
 * @param marketIds - markets to read; not mutated, the caller drains.
 * @returns Rows written. Ids whose count reads 0 or fails are skipped, so this
 *   is below the number attempted rather than equal to it.
 */
async function updateNegRisk(marketIds: readonly string[]): Promise<number> {
  if (!marketIds.length) return 0;
  const markets = marketIds;
  const counts = await Promise.all(
    markets.map((id) =>
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
  let written = 0;
  for (let k = 0; k < markets.length; k++) {
    const id = markets[k];
    const n = counts[k];
    if (id === undefined || !n) continue;
    await negOut.write([id, n]);
    written++;
  }
  return written;
}

/**
 * One Gamma market, reduced to the condition it describes.
 *
 * Writes nothing and owns no collection: it returns a description and bumps
 * counters, and the caller decides which rows to insert and what to queue. Token
 * ids are NOT returned -- only `outcomeSlotCount`, from which the caller derives
 * them with getPositionId.
 *
 * `payoutPrices` is filled only when `updatedAt` predates the gate AND the prices
 * are a real settlement of the right arity; otherwise `invalidPayoutPrices` marks
 * the condition for a chain read.
 *
 * @param m - one parsed Gamma market object.
 * @param gateTs - start_block's timestamp in ms; payouts are seeded only from
 *   markets Gamma last touched before it.
 * @param stats - counters to record into.
 * @returns The condition, or null when the market has no valid conditionId or no
 *   outcomes at all.
 */
function parseMarket(m: Record<string, unknown>, gateTs: number, stats: SinkStats): Market | null {
  const conditionId = (m.conditionId as string | undefined)?.toLowerCase();
  if (!conditionId || !/^0x[0-9a-f]{64}$/.test(conditionId)) {
    stats.noConditionId++;
    return null;
  }
  stats.parsed++;

  const outcomes = jsonArray(m.outcomes);
  const clob = jsonArray(m.clobTokenIds);
  const outcomeSlotCount = outcomes.length || clob.length;
  if (!outcomeSlotCount) {
    stats.noOutcomes++;
    return null;
  }

  const negRisk = m.negRisk === true;
  const questionID = m.questionID as string | undefined;
  const questionIndex =
    negRisk && questionID && /^0x[0-9a-fA-F]{64}$/.test(questionID)
      ? parseInt(questionID.slice(-2), 16)
      : null;

  for (let i = 0; i < Math.min(clob.length, outcomeSlotCount); i++) {
    const want = clob[i];
    if (!want) continue;
    const got = getPositionId(conditionId, i, negRisk).toString();
    if (got !== want) {
      stats.idMismatch++;
      console.log(
        `  id mismatch ${conditionId} idx=${i} slots=${outcomeSlotCount} ` +
          `negRisk=${negRisk ? 'true' : 'false'} gamma=${want} derived=${got}`,
      );
    }
  }

  let payoutPrices: bigint[] | null = null;
  let invalidPayoutPrices: boolean = false;
  const updatedAt = Date.parse((m.updatedAt as string) ?? '');
  if (Number.isNaN(updatedAt)) stats.gateBadTs++;
  else if (updatedAt >= gateTs) stats.gateAfter++;

  if (!Number.isNaN(updatedAt) && updatedAt < gateTs) {
    const prices = settledPrices(jsonArray(m.outcomePrices));
    if (prices && prices.length === outcomeSlotCount) {
      payoutPrices = prices;
      stats.pricedGated++;
    } else {
      invalidPayoutPrices = true;
      stats.deferred++;
    }
  }

  const negRiskMarketId = ((m.negRiskMarketID as string) ?? null)?.toLowerCase() ?? null;

  const collateral = negRisk ? NEG_RISK_WRAPPED_COLLATERAL : USDC;
  return {
    conditionId,
    negRisk,
    collateral,
    questionIndex,
    negRiskMarketId,
    payoutPrices,
    outcomeSlotCount,
    invalidPayoutPrices,
  };
}

/**
 * Derive the whole seed from the archived corpus and insert it.
 *
 * This is what re-runs when start_block moves: the gate changes, so every payout
 * decision changes, and nothing needs re-fetching.
 *
 * STREAMED line by line -- the corpus is 12 GB and Node caps a string near
 * 512 MB.
 *
 * TRUNCATEs first, which is required rather than tidy: the inserts carry no ON
 * CONFLICT, so anything already present aborts the load. Refuses outright when
 * UserPosition has rows -- seeding over an indexed schema would overwrite
 * event-derived data with values derived from metadata.
 *
 * Owns the two batch arrays and drains them at the call site; the inserters are
 * module-level.
 *
 * @param gateTs - start_block's timestamp in ms.
 * @param startBlock - start_block; chain reads are pinned below it.
 * @returns Structural problems found by the closing verification; 0 is clean.
 */
async function rebuildFromFile(gateTs: number, startBlock: bigint): Promise<number> {
  const indexed = await count('SELECT count(*) FROM "UserPosition"');
  if (indexed) {
    throw new Error(
      `UserPosition already has ${indexed.toLocaleString()} rows — the indexer has run. ` +
        'Seed before `envio start`, not after.',
    );
  }

  await exec('TRUNCATE "Condition", "OutcomeToken", "NegRiskEvent"');

  const t0 = Date.now();
  const stats = createSinkStats();
  const conditionIds: string[] = [];

  const negSeen = new Set<string>();
  const negMarketIds: string[] = [];

  const rl = createInterface({ input: createReadStream(RAW, 'utf8'), crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line.trim()) continue;
    stats.lines++;
    let m: Record<string, unknown>;
    try {
      m = JSON.parse(line) as Record<string, unknown>;
    } catch {
      stats.unparseable++;
      continue;
    }

    const market = parseMarket(m, gateTs, stats);
    if (market) {
      const conditionId = market.conditionId;
      const negRiskMarketId = market.negRiskMarketId;
      await condOut.write([
        conditionId,
        market.outcomeSlotCount,
        market.negRisk,
        negRiskMarketId,
        market.questionIndex,
        market.payoutPrices?.map(String) ?? null,
      ]);
      for (let i = 0; i < market.outcomeSlotCount; i++) {
        await tokOut.write([
          getPositionId(conditionId, i, market.negRisk).toString(),
          conditionId,
          i,
          market.collateral,
        ]);
      }
      stats.conditions++;
      stats.tokens += market.outcomeSlotCount;
      if (market.invalidPayoutPrices) conditionIds.push(conditionId);
      if (negRiskMarketId && !negSeen.has(negRiskMarketId)) {
        negSeen.add(negRiskMarketId);
        negMarketIds.push(negRiskMarketId);
      }
    }
    if (conditionIds.length >= PAYOUT_FLUSH_SIZE) {
      stats.chainPriced += await updatePayoutPrices(
        startBlock,
        conditionIds.splice(0, conditionIds.length),
      );
    }
    if (negMarketIds.length >= NEGRISK_FLUSH_SIZE) {
      stats.negCounted += await updateNegRisk(negMarketIds.splice(0, negMarketIds.length));
    }

    if (stats.lines % 500_000 === 0) {
      console.log(
        `    ${stats.lines.toLocaleString()} markets  ` +
          `${stats.conditions.toLocaleString()} conditions in  ` +
          `${Math.round((Date.now() - t0) / 1000)}s`,
      );
    }
  }

  stats.chainPriced += await updatePayoutPrices(
    startBlock,
    conditionIds.splice(0, conditionIds.length),
  );
  stats.negCounted += await updateNegRisk(negMarketIds.splice(0, negMarketIds.length));

  await condOut.close();
  await tokOut.close();
  await negOut.close();
  console.log(`  gate: updatedAt < ${new Date(gateTs).toISOString()}`);
  console.log(`  elapsed ${Math.round((Date.now() - t0) / 1000)}s`);

  report(stats);
  return await verify();
}

/**
 * Print the run's counters.
 *
 * `processed` is the audit: every line lands in exactly one bucket, so the
 * six must sum to the line count. A mismatch means a branch is double-counting
 * or dropping markets, which makes every other number here suspect.
 *
 * @param stats - counters gathered during the walk.
 */
function report(stats: SinkStats): void {
  const processed =
    stats.unparseable +
    stats.noConditionId +
    stats.noOutcomes +
    stats.gateAfter +
    stats.gateBadTs +
    stats.pricedGated +
    stats.deferred;
  const report: [string, string][] = [
    ['markets parsed', stats.parsed.toLocaleString()],
    ['Condition rows', stats.conditions.toLocaleString()],
    ['OutcomeToken rows', stats.tokens.toLocaleString()],
    ['payoutPrices seeded', stats.pricedGated.toLocaleString()],
    ['validation needed', stats.deferred.toLocaleString()],
    ['...priced from chain', stats.chainPriced.toLocaleString()],
    ['NegRiskEvent counted', stats.negCounted.toLocaleString()],
    ['derived-id mismatches', `${stats.idMismatch.toLocaleString()}  (expect 95)`],
    ['no/invalid conditionId', stats.noConditionId.toLocaleString()],
    ['no outcomes or clob ids', stats.noOutcomes.toLocaleString()],
    [`at/after gate`, stats.gateAfter.toLocaleString()],
    ['updatedAt unparseable', stats.gateBadTs.toLocaleString()],
    [
      'processed',
      processed === stats.lines
        ? `${processed.toLocaleString()} = ${stats.lines.toLocaleString()} lines`
        : `${processed.toLocaleString()} != ${stats.lines.toLocaleString()} LINES -- DOES NOT BALANCE`,
    ],
  ];
  for (const [k, v] of report) console.log(k ? `  ${k.padEnd(25)}${v}` : '');
}

const w = await getBlockId();
if (await rebuildFromFile(w.ms, w.block)) process.exitCode = 1;

import { COLLATERAL_SCALE, NEG_RISK_WRAPPED_COLLATERAL, USDC } from '@utils/constants';
import { computePositionId } from '@utils/ctf-utils';
import { pool } from './postgres';

/**
 * Batch size
 */
export const INSERT_BATCH = 10_000;

/** Column order must match the TSV writers above. A mismatch is SILENT:
 *  Postgres reads questionIndex into outcomeSlotCount happily, both being
 *  integers. */
export const CONDITION_COLS = [
  'id',
  'outcomeSlotCount',
  'negRisk',
  'negRiskMarketId',
  'questionIndex',
  'payoutPrices',
];

export const TOKEN_COLS = ['id', 'conditionId', 'outcomeIndex', 'collateral'];

export const NEGRISK_COLS = ['id', 'questionCount'];

/**
 * A value bound as an INSERT parameter.
 *
 * `string[]` is for text[] columns -- node-postgres serialises a JS array into a
 * Postgres array literal, which a joined string would not be.
 */
export type Val = string | number | boolean | string[] | null;

/** A batched INSERT sink. `flush` sends what is buffered without closing it. */
export interface Inserter {
  write: (vals: Val[]) => Promise<void>;
  flush: () => Promise<void>;
  close: () => Promise<void>;
}

/**
 * Sink Status
 */
export interface SinkStats {
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

/**
 * Zeroed counters for one rebuild.
 *
 * @returns Fresh stats. Pure -- it opens no database resource, so it is safe to
 *   construct outside a run.
 */
export const createSinkStats = (): SinkStats => ({
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

export interface Market {
  conditionId: string;
  positionIds: string[];
  negRisk: boolean;
  collateral: string;
  questionIndex: number | null;
  negRiskMarketId: string | null;
  payoutPrices: bigint[] | null;
  outcomeSlotCount: number;
  invalidPayoutPrices: boolean;
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
export const jsonArray = (v: unknown): string[] => {
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
export function settledPrices(raw: string[]): bigint[] | null {
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
 * Buffers rows and INSERTs them in batches, with bound parameters.
 *
 * EVERY `write` MUST PASS EXACTLY `cols.length` VALUES: placeholders are numbered
 * `$(k * cols.length + c + 1)`, so a short row shifts every later tuple onto the
 * wrong parameters.
 *
 * No ON CONFLICT by default, so a duplicate id aborts the load. The TRUNCATE in
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
 *   With `onConflict`, the first column must be the conflict key.
 * @param onConflict - e.g. `ON CONFLICT ("id") DO NOTHING`; omit to keep
 *   duplicates fatal.
 * @returns A handle to `write` rows into, `flush`, and `close` when done.
 */
export function insertHandler(table: string, cols: string[], onConflict?: string): Inserter {
  const names = cols.map((c) => `"${c}"`).join(', ');
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
    await pool.query(
      `INSERT INTO "${table}" (${names}) VALUES ${tuples.join(',')} ${onConflict ? ` ${onConflict}` : ''}`,
      batch.flat(),
    );
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
export function parseMarket(
  m: Record<string, unknown>,
  gateTs: number,
  stats: SinkStats,
): Market | null {
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

  const collateral = negRisk ? NEG_RISK_WRAPPED_COLLATERAL : USDC;

  const positionIds = Array.from({ length: outcomeSlotCount }, (_, i) =>
    computePositionId(collateral, conditionId, i).toString(),
  );

  for (let i = 0; i < Math.min(clob.length, outcomeSlotCount); i++) {
    const want = clob[i];
    const got = positionIds[i];
    if (!want || got === want) continue;
    stats.idMismatch++;
    console.log(
      `  id mismatch ${conditionId} idx=${i} slots=${outcomeSlotCount} ` +
        `negRisk=${negRisk ? 'true' : 'false'} gamma=${want} derived=${got}`,
    );
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

  return {
    conditionId,
    positionIds,
    negRisk,
    collateral,
    questionIndex,
    negRiskMarketId,
    payoutPrices,
    outcomeSlotCount,
    invalidPayoutPrices,
  };
}

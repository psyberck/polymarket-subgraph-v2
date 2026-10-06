import { COLLATERAL_SCALE, NEG_RISK_WRAPPED_COLLATERAL, USDC } from '#src/utils/constants.ts';
import { computePositionId } from '#src/utils/ctf-utils.ts';
import { pool } from '#tools/postgres.ts';

/** Max rows per INSERT. */
export const INSERT_BATCH = 10_000;

/** Must match the order of values passed to `write`. A mismatch is SILENT when
 *  types agree (e.g. questionIndex into outcomeSlotCount). */
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

/** An INSERT parameter. `string[]` binds to text[] columns as a Postgres array. */
export type Val = string | number | boolean | string[] | null;

/** A batched INSERT sink. `flush` sends what is buffered without closing it. */
export interface Inserter {
  write: (vals: Val[]) => Promise<void>;
  flush: () => Promise<void>;
  close: () => Promise<void>;
}

/** Counters for one rebuild. */
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
  unResolved: number;
  deferred: number;
  idMismatch: number;
  chainPriced: number;
  conditions: number;
}

/** Zeroed SinkStats. Pure; safe to call outside a run. */
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
  unResolved: 0,
  deferred: 0,
  idMismatch: 0,
  chainPriced: 0,
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
 * Decodes a Gamma field holding a JSON array as a string (`outcomes`,
 * `clobTokenIds`, `outcomePrices`). Never throws: malformed values are common.
 *
 * @returns The elements, or [] if absent, unparseable, or not an array.
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
 * Micro-unit payout prices, or null if `outcomePrices` is not a settlement.
 *
 * `outcomePrices` can hold live AMM quotes (e.g. ["0.00000041...", "0.99999958..."]).
 * A settlement is exact: every entry is an integer in micro-units and they sum to
 * exactly 1e6, so ["0","1"] and ["0.5","0.5"] pass and quotes do not.
 * Rejections are deferred to a ConditionResolution read.
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
 * Buffers rows and INSERTs them in batches with bound parameters.
 *
 * @param table - destination table, which must be empty.
 * @param cols - column order for `write`; with `onConflict`, the first is the key.
 * @param onConflict - e.g. `ON CONFLICT ("id") DO NOTHING`; omit to keep
 *   duplicates fatal.
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

/** True when the Gamma market is closed AND UMA-resolved. */
function isResolvedMarket(m: Record<string, unknown>): boolean {
  return m.closed === true && m.umaResolutionStatus === 'resolved';
}

/**
 * Reduces one Gamma market to its condition. Writes nothing; only bumps `stats`.
 *
 * `payoutPrices` is set only if `updatedAt` < `gateTs`, the market is resolved,
 * and the prices are an exact settlement of the right arity. Otherwise
 * `invalidPayoutPrices` defers it to a chain read pinned below start_block.
 *
 * @param m - one parsed Gamma market object.
 * @param gateTs - start_block's timestamp in ms.
 * @param stats - counters to record into.
 * @returns The condition, or null if conditionId is invalid or there are no outcomes.
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
  let invalidPayoutPrices: boolean = true;
  const updatedAt = Date.parse((m.updatedAt as string) ?? '');
  if (Number.isNaN(updatedAt)) {
    stats.gateBadTs++;
  } else if (updatedAt >= gateTs) {
    stats.gateAfter++;
  }

  if (!Number.isNaN(updatedAt) && updatedAt < gateTs) {
    const prices = isResolvedMarket(m) ? settledPrices(jsonArray(m.outcomePrices)) : null;
    if (!isResolvedMarket(m)) {
      stats.unResolved++;
    } else if (prices && prices.length === outcomeSlotCount) {
      stats.pricedGated++;
      payoutPrices = prices;
      invalidPayoutPrices = false;
    } else {
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

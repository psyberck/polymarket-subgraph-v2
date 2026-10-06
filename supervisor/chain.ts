import { createPublicClient, http } from 'viem';
import { polygon } from 'viem/chains';
import { pgSchema, pool, quote } from '#tools/postgres.ts';
import type { Schedule } from '#supervisor/config.ts';
import type { ChainState } from '#supervisor/envio.ts';
import { log } from '#supervisor/util.ts';

export interface Rpc extends BlockClock {
  /** The `finalized` block number, or null if the endpoint does not report one. */
  finalized(): Promise<number | null>;
  /** Hashes of the given blocks. */
  blockHashes(blocks: number[]): Promise<Map<number, string>>;
  /** Whether the endpoint answers at all. */
  ping(): Promise<boolean>;
  /** Wait for PRC client connection to the server. */
  wait(): Promise<void>;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * @param url - defaults to POLYGON_RPC_URL.
 */
export function createRpc(url = process.env.POLYGON_RPC_URL): Rpc {
  if (!url) throw new Error('POLYGON_RPC_URL is not set');
  const client = createPublicClient({
    chain: polygon,
    transport: http(url, { batch: { batchSize: 100 }, retryCount: 3, timeout: 30_000 }),
  });
  const timestamps = new Map<number, number>();

  const ping = async () => {
    try {
      await client.getBlockNumber({ cacheTime: 0 });
      return true;
    } catch {
      return false;
    }
  };

  const wait = async () => {
    for (let delay = 2_000; !(await ping()); delay = Math.min(delay * 2, 60_000)) {
      console.log(
        `rpc: proxy at ${process.env.POLYGON_RPC_URL} is not responding; retrying in ${delay / 1000}s`,
      );
      await sleep(delay);
    }
  };

  return {
    async head() {
      const block = await client.getBlock({ blockTag: 'latest' });
      return { number: Number(block.number), timestamp: Number(block.timestamp) };
    },

    async timestamp(block) {
      const cached = timestamps.get(block);
      if (cached !== undefined) return cached;
      const { timestamp } = await client.getBlock({ blockNumber: BigInt(block) });
      timestamps.set(block, Number(timestamp));
      return Number(timestamp);
    },

    async finalized() {
      try {
        return Number((await client.getBlock({ blockTag: 'finalized' })).number);
      } catch {
        return null;
      }
    },

    async blockHashes(blocks) {
      const results = await Promise.all(
        blocks.map((n) => client.getBlock({ blockNumber: BigInt(n) })),
      );
      return new Map(results.map((b) => [Number(b.number), b.hash]));
    },
    ping,
    wait,
  };
}

// ── schedule ──────────────────────────────────────────────────────────
/*
 * The next snapshot block (snapshot-design.md §3.2). Every schedule reduces to
 * a sequence of block numbers; the loop never knows which schedule produced B.
 *
 * Pure apart from the BlockClock it is given, so tests run on a simulated
 * chain.
 *
 * - stride: B is the next multiple of the stride. Aligned to multiples, not to
 *   when the supervisor started, so restarts and reindexes hit the same blocks.
 * - monthly / weekly: B is the last block before a UTC calendar boundary T
 *   (00:00 on the 1st, or on Monday for ISO weeks): the first block at or after
 *   T, minus 1, the convention config.yaml's start_block is pinned with. If T
 *   has not been reached yet, B does not exist; the result is then an estimate,
 *   and the loop runs Envio to a point safely before it, waits for T, and asks
 *   again.
 */

/** What the schedule needs to know about the chain. */
export interface BlockClock {
  /** The latest block. */
  head(): Promise<{ number: number; timestamp: number }>;
  /** A block's timestamp, in seconds. */
  timestamp(block: number): Promise<number>;
}

export type Boundary =
  /** B is known: run Envio to exactly this block. */
  | { kind: 'block'; b: number; id: string }
  /** T is in the future: `estimate` assumes the measured block time. */
  | { kind: 'future'; at: number; estimate: number; id: string };

/** Polygon's measured block time (§3.2); only used to estimate future boundaries. */
export const BLOCK_SECONDS = 1.5;

const DAY = 86_400;

/** Next 00:00 UTC on the 1st strictly after `ts` (seconds). */
export function nextMonthStart(ts: number): number {
  const d = new Date(ts * 1000);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1) / 1000;
}

/** Next 00:00 UTC Monday strictly after `ts` (seconds). */
export function nextMonday(ts: number): number {
  const d = new Date(ts * 1000);
  const midnight = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) / 1000;
  const daysToMonday = (8 - d.getUTCDay()) % 7 || 7; // getUTCDay: 0 = Sunday
  return midnight + daysToMonday * DAY;
}

/** ISO 8601 week of the day containing `ts`: [year, week]. */
function isoWeek(ts: number): [number, number] {
  const d = new Date(ts * 1000);
  const day = d.getUTCDay() || 7; // Monday 1 … Sunday 7
  // The Thursday of this week decides the ISO year.
  const thursday = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 4 - day);
  const year = new Date(thursday).getUTCFullYear();
  const week = Math.floor((thursday - Date.UTC(year, 0, 1)) / (7 * DAY * 1000)) + 1;
  return [year, week];
}

/**
 * The id of the period that ends at boundary T: `2026-09` for T = 2026-10-01,
 * `2026-W39` for the Monday that ends ISO week 39.
 */
export function periodId(schedule: 'monthly' | 'weekly', t: number): string {
  const last = t - 1; // the last second of the period
  if (schedule === 'monthly') {
    const d = new Date(last * 1000);
    return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
  }
  const [year, week] = isoWeek(last);
  return `${year}-W${String(week).padStart(2, '0')}`;
}

/**
 * The first block whose timestamp is at or after `t`, by binary search between
 * a block known to be before `t` and one known to be at or after it.
 */
export async function firstBlockAtOrAfter(
  clock: BlockClock,
  t: number,
  before: number,
  atOrAfter: number,
): Promise<number> {
  let lo = before; // timestamp(lo) < t
  let hi = atOrAfter; // timestamp(hi) >= t
  while (hi - lo > 1) {
    const mid = lo + Math.floor((hi - lo) / 2);
    if ((await clock.timestamp(mid)) < t) lo = mid;
    else hi = mid;
  }
  return hi;
}

/**
 * The first snapshot boundary strictly after `after` (normally progress_block).
 *
 * @returns null when no schedule is set.
 */
export async function nextBoundary(
  schedule: Schedule | null,
  stride: number | null,
  after: number,
  clock: BlockClock,
): Promise<Boundary | null> {
  if (schedule === null) return null;
  if (schedule === 'stride') {
    if (!stride) throw new Error('stride schedule without a stride');
    const b = (Math.floor(after / stride) + 1) * stride;
    return { kind: 'block', b, id: `b${b}` };
  }

  // B(T) = firstBlockAtOrAfter(T) - 1 > after  ⇔  timestamp(after + 1) < T, so
  // T is the first calendar boundary after the timestamp of block after + 1.
  const head = await clock.head();
  const from = Math.min(after + 1, head.number);
  const fromTs = await clock.timestamp(from);
  const t = schedule === 'monthly' ? nextMonthStart(fromTs) : nextMonday(fromTs);
  const id = periodId(schedule, t);
  if (head.timestamp < t)
    return {
      kind: 'future',
      at: t,
      estimate: head.number + Math.ceil((t - head.timestamp) / BLOCK_SECONDS) - 1,
      id,
    };
  const first = await firstBlockAtOrAfter(clock, t, from, head.number);
  return { kind: 'block', b: first - 1, id };
}

// ── finality ──────────────────────────────────────────────────────────
/*
 * Finality and the checkpoint hash check (snapshot-design.md §3.4).
 *
 * Envio only detects reorgs within max_reorg_depth of the head, and deletes
 * the evidence once it resumes deeper (§8.4). So before resuming past a
 * snapshot block, every stored checkpoint is compared with the chain: a block
 * replaced between Envio indexing it and the chain finalising it is caught
 * here or nowhere.
 */

export interface FinalityResult {
  /** `finalized` block when checked, or null if decided by depth. */
  finalized: number | null;
  by: 'finalized' | 'depth';
}

/** A replaced block: halt, no snapshot, no resume (§3.4). */
export class HashMismatchError extends Error {}

/**
 * Wait until B is final: `finalized ≥ B`, or, if the endpoint stops reporting
 * `finalized`, `head ≥ B + max_reorg_depth`.
 *
 */
export async function waitFinal(
  rpc: Rpc,
  b: number,
  maxReorgDepth: number,
): Promise<FinalityResult> {
  for (let attempt = 0; ; attempt++) {
    const finalized = await rpc.finalized();
    if (finalized !== null && finalized >= b) return { finalized, by: 'finalized' };
    if (finalized === null) {
      const head = await rpc.head();
      if (head.number >= b + maxReorgDepth) return { finalized: null, by: 'depth' };
    }
    if (attempt % 12 === 0)
      log(`finality: waiting for ${b} (finalized ${finalized ?? 'not reported'})`);
    await new Promise((r) => setTimeout(r, 5_000));
  }
}

/**
 * Compare every stored checkpoint with the chain's hash for that block.
 *
 * A backfilled B has no checkpoints (Envio only records them near the head);
 * then B must have been deeper than max_reorg_depth when it was indexed.
 *
 * @returns How many checkpoints matched.
 * @throws HashMismatchError on any difference.
 */
export async function checkHashes(rpc: Rpc, b: number, chain: ChainState): Promise<number> {
  const { rows } = await pool.query<{ block_number: number; block_hash: string }>(
    `SELECT block_number, block_hash FROM ${quote(pgSchema)}.${quote('envio_checkpoints')}
     WHERE chain_id = 137 ORDER BY block_number`,
  );
  if (rows.length === 0) {
    if (chain.sourceBlock - b <= chain.maxReorgDepth)
      throw new HashMismatchError(
        `no checkpoints at ${b}, yet the head (${chain.sourceBlock}) is within max_reorg_depth`,
      );
    return 0;
  }
  const chainHashes = await rpc.blockHashes(rows.map((r) => r.block_number));
  const mismatches = rows.filter(
    (r) => chainHashes.get(r.block_number)?.toLowerCase() !== r.block_hash.toLowerCase(),
  );
  if (mismatches.length)
    throw new HashMismatchError(
      `${mismatches.length} of ${rows.length} checkpoints differ from the chain, e.g. block ${mismatches[0].block_number}: stored ${mismatches[0].block_hash}, chain ${chainHashes.get(mismatches[0].block_number)}`,
    );
  return rows.length;
}

/**
 * Retention (snapshot-design.md §3.8): archive, then delete, what can no longer
 * change. Runs only while Envio is stopped at a snapshot block whose snapshot
 * is verified.
 *
 * Positions move, they are not dropped: a row deleted at B is in B's snapshot
 * but in no later one, so the closed rows go to their own verified file first,
 * and the delete takes its ids from that file. Trades need no closed file: the
 * trade files are their archive, so a trade is deleted only once a verified,
 * gap-free trade file holds it.
 *
 * Every step is idempotent. A closed file for B means the selection is done;
 * a rerun only deletes whatever of its ids still exists.
 */
import { existsSync, readdirSync, readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import type { DuckDBConnection } from '@duckdb/node-api';
import { pgSchema, pool } from '#tools/postgres.ts';
import {
  type ExportUnit,
  exportUnits,
  type Manifest,
  verifyAndPublish,
} from '#supervisor/archive.ts';
import { type BlockClock, firstBlockAtOrAfter } from '#supervisor/chain.ts';
import { loadSchema } from '#supervisor/typemap.ts';
import { literal, log, since, table } from '#supervisor/util.ts';

/** A closed position must have been quiet this long before it is pruned (§3.8). */
export const RESOLVED_GRACE_DAYS = 30;
/** Trades are kept this long in Postgres; older ones live in the trade files. */
export const TRADE_KEEP_DAYS = 90;
/** Rows per DELETE statement. */
export const DELETE_BATCH = 10_000;
/** Blocks per trade DELETE statement (~65 trades a block, so ~650k rows). */
const TRADE_DELETE_BLOCKS = 10_000;

const DAY = 86_400;

export interface PruneOptions {
  outputDir: string;
  /** The snapshot block Envio is stopped at. */
  b: number;
  /** Its timestamp, seconds. */
  bTimestamp: number;
  /** envio_chains.start_block: where the trade-file chain starts. */
  startBlock: number;
  /** Log what would be deleted; write and delete nothing. */
  debug: boolean;
  /** Overrides for tests; the constants above otherwise. */
  graceSeconds?: number;
  keepSeconds?: number;
}

export interface PruneResult {
  positionsArchived: number;
  positionsDeleted: number;
  tradesDeleted: number;
  /** Ids of the previous closed file that exist again in UserPosition. */
  recreated: number;
}

export const closedDir = (outputDir: string, b: number): string =>
  join(outputDir, 'closed', `b${b}`);

/**
 * Positions that can no longer change: fully sold or redeemed, on a resolved
 * market, quiet for the grace period. `amount = 0` alone is not enough: a
 * closed position on an open market can be re-entered, and its running totals
 * would restart at zero.
 */
export function closedPositionsWhere(cutoffTimestamp: number): string {
  return `amount = 0 AND "lastUpdatedAt" < ${cutoffTimestamp} AND "conditionId" IN (SELECT id FROM ${table('Condition')} WHERE "payoutPrices" IS NOT NULL)`;
}

/** A closed file's ids, DELETE_BATCH at a time, by Parquet row number. */
async function* idBatches(conn: DuckDBConnection, file: string, rows: number) {
  for (let from = 0; from < rows; from += DELETE_BATCH)
    yield (
      await conn.runAndReadAll(
        `SELECT id FROM read_parquet(${literal(file)}, file_row_number = true)
         WHERE file_row_number >= ${from} AND file_row_number < ${from + DELETE_BATCH}`,
      )
    )
      .getRowObjectsJson()
      .map((r) => r.id as string);
}

/** Delete a closed file's ids from UserPosition; returns rows deleted. */
async function deleteByFile(conn: DuckDBConnection, file: string, rows: number): Promise<number> {
  let deleted = 0;
  for await (const ids of idBatches(conn, file, rows)) {
    const res = await pool.query(`DELETE FROM ${table('UserPosition')} WHERE id = ANY($1)`, [ids]);
    deleted += res.rowCount ?? 0;
  }
  return deleted;
}

/** Archive and delete closed positions (§3.8, steps 1–3). */
async function prunePositions(
  conn: DuckDBConnection,
  opts: PruneOptions,
): Promise<{ archived: number; deleted: number }> {
  const cutoff = opts.bTimestamp - (opts.graceSeconds ?? RESOLVED_GRACE_DAYS * DAY);
  const where = closedPositionsWhere(cutoff);
  const dir = closedDir(opts.outputDir, opts.b);
  const file = join(dir, 'UserPosition.parquet');
  const manifestPath = join(dir, 'manifest.json');

  if (opts.debug) {
    const { rows } = await pool.query<{ n: string }>(
      `SELECT count(*) AS n FROM ${table('UserPosition')} WHERE ${where}`,
    );
    const n = Number(rows[0].n);
    log(`prune (debug): would archive and delete ${n} closed positions`);
    return { archived: 0, deleted: 0 };
  }

  let archived: number;
  let resuming = false;
  if (existsSync(manifestPath)) {
    // Selected and verified in an earlier attempt; only the delete may be unfinished.
    archived = (JSON.parse(readFileSync(manifestPath, 'utf8')) as Manifest).files[0].rows;
    resuming = true;
    log(
      `prune: closed file for ${opts.b} already verified (${archived} rows); finishing its delete`,
    );
  } else {
    const entity = loadSchema().find((e) => e.name === 'UserPosition')!;
    // Every qualifying row, no cap (decided 2026-10-06): the copy and its
    // check values run the same filter in the same frozen view, so they select
    // the same rows.
    const unit: ExportUnit = { entity, where, path: file };
    const result = await exportUnits(conn, [unit], {
      expectBlock: (p) => (p === opts.b ? null : `expected ${opts.b}`),
    });
    archived = Number(result.files[0].pgCheck.rows);
    if (archived === 0) {
      rmSync(result.files[0].partialPath, { force: true });
      log('prune: no closed positions qualify');
      return { archived: 0, deleted: 0 };
    }
    await verifyAndPublish(conn, result, manifestPath, {
      kind: 'closed',
      id: `b${opts.b}`,
      schedule: 'prune',
      bPrev: null,
      blockHash: null,
      blockTimestamp: opts.bTimestamp,
    });
  }

  const started = Date.now();
  const deleted = await deleteByFile(conn, file, archived);
  log(`prune: deleted ${deleted} of ${archived} archived positions (${since(started)})`);
  // Envio is stopped, so nothing changed between export and delete: every
  // archived row must have gone, unless an earlier attempt deleted some.
  if (!resuming && deleted !== archived)
    throw new Error(`prune: deleted ${deleted} positions but archived ${archived}`);
  return { archived, deleted };
}

/** Whether OrderFilled has an index led by blockNumber (§3.7). */
async function hasBlockNumberIndex(): Promise<boolean> {
  const { rows } = await pool.query<{ n: string }>(
    `SELECT count(*) AS n FROM pg_indexes
     WHERE schemaname = $1 AND tablename = 'OrderFilled' AND indexdef LIKE '%("blockNumber"%'`,
    [pgSchema],
  );
  return Number(rows[0].n) > 0;
}

/**
 * The last block covered by an unbroken chain of verified trade files starting
 * at `from` (exclusive): trades up to it are archived.
 */
export function archivedThrough(outputDir: string, from: number): number {
  const dir = join(outputDir, 'trades');
  if (!existsSync(dir)) return from;
  const ranges = readdirSync(dir)
    .filter((f) => f.endsWith('.manifest.json'))
    .map((f) => JSON.parse(readFileSync(join(dir, f), 'utf8')) as Manifest)
    .map((m) => [m.bPrev ?? -1, m.b] as const)
    .sort((x, y) => x[0] - y[0]);
  let through = from;
  for (const [prev, b] of ranges) {
    if (prev > through) break; // a gap: nothing beyond it is archived
    if (prev === through) through = b;
  }
  return through;
}

/** Delete trades older than the keep window that a verified trade file holds (§3.8). */
async function pruneTrades(clock: BlockClock, opts: PruneOptions): Promise<number> {
  if (!(await hasBlockNumberIndex())) {
    log('prune: no index on OrderFilled.blockNumber; trade retention refused (§3.7)');
    return 0;
  }
  const cutoffTs = opts.bTimestamp - (opts.keepSeconds ?? TRADE_KEEP_DAYS * DAY);
  if ((await clock.timestamp(opts.startBlock)) >= cutoffTs) {
    log('prune: no trades older than the keep window');
    return 0;
  }
  // The last block before the cutoff time.
  const cutoffBlock = (await firstBlockAtOrAfter(clock, cutoffTs, opts.startBlock, opts.b)) - 1;
  const archived = archivedThrough(opts.outputDir, opts.startBlock - 1);
  const upTo = Math.min(cutoffBlock, archived);
  if (archived < cutoffBlock)
    log(`prune: trade files are gap-free only through ${archived}; trades after it stay`);
  const { rows } = await pool.query<{ min: number | null; n: string }>(
    `SELECT min("blockNumber") AS min, count(*) AS n FROM ${table('OrderFilled')} WHERE "blockNumber" <= $1`,
    [upTo],
  );
  const n = Number(rows[0].n);
  if (opts.debug) {
    log(`prune (debug): would delete ${n} trades at or below block ${upTo}`);
    return 0;
  }
  if (n === 0) return 0;
  const started = Date.now();
  let deleted = 0;
  for (let lo = rows[0].min! - 1; lo < upTo; lo += TRADE_DELETE_BLOCKS) {
    const hi = Math.min(lo + TRADE_DELETE_BLOCKS, upTo);
    const res = await pool.query(
      `DELETE FROM ${table('OrderFilled')} WHERE "blockNumber" > $1 AND "blockNumber" <= $2`,
      [lo, hi],
    );
    deleted += res.rowCount ?? 0;
  }
  log(`prune: deleted ${deleted} trades at or below block ${upTo} (${since(started)})`);
  return deleted;
}

/**
 * How many ids of the previous closed file (before B) exist again in
 * UserPosition (§3.8). One file per cycle, not every file ever written, so
 * the cost stays flat while still measuring the recreation rate.
 */
async function countRecreated(
  conn: DuckDBConnection,
  outputDir: string,
  b: number,
): Promise<number> {
  const root = join(outputDir, 'closed');
  if (!existsSync(root)) return 0;
  const previous = readdirSync(root)
    .map((name) => Number(name.slice(1)))
    .filter((block) => block < b && existsSync(join(closedDir(outputDir, block), 'manifest.json')))
    .sort((x, y) => y - x)[0];
  if (previous === undefined) return 0;
  const dir = closedDir(outputDir, previous);
  const rows = (JSON.parse(readFileSync(join(dir, 'manifest.json'), 'utf8')) as Manifest).files[0]
    .rows;
  let recreated = 0;
  for await (const ids of idBatches(conn, join(dir, 'UserPosition.parquet'), rows)) {
    const { rows: found } = await pool.query<{ n: string }>(
      `SELECT count(*) AS n FROM ${table('UserPosition')} WHERE id = ANY($1)`,
      [ids],
    );
    recreated += Number(found[0].n);
  }
  return recreated;
}

/** One prune run at snapshot block B. */
export async function pruneCycle(
  conn: DuckDBConnection,
  clock: BlockClock,
  opts: PruneOptions,
): Promise<PruneResult> {
  const started = Date.now();
  log(`prune: at ${opts.b}${opts.debug ? ' (debug: nothing is written or deleted)' : ''}`);
  const positions = await prunePositions(conn, opts);
  const tradesDeleted = await pruneTrades(clock, opts);
  const recreated = opts.debug ? 0 : await countRecreated(conn, opts.outputDir, opts.b);
  if (!opts.debug)
    log(`prune: ${recreated} positions archived by the previous prune exist again in UserPosition`);
  log(`prune: done (${since(started)})`);
  return {
    positionsArchived: positions.archived,
    positionsDeleted: positions.deleted,
    tradesDeleted,
    recreated,
  };
}

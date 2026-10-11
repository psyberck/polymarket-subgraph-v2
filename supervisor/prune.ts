import { join } from 'node:path';
import type { DuckDBConnection } from '@duckdb/node-api';
import { count, exec, literal, table } from '#tools/postgres.ts';
import { createDuckDB } from '#tools/duckdb.ts';
import {
  FINALIZED_FILE,
  type Manifest,
  outputFolderPath,
  since,
  type SnapshotFile,
  snapshotFiles,
  snapshotManifests,
  TRADES_FILE,
} from '#supervisor/snapshot.ts';
import { loadSchema } from '#supervisor/typemap.ts';
import type { PolygonChainState } from '#supervisor/chain.ts';
import type { SupervisorConfig } from '#supervisor/config.ts';

/** Ids per DELETE or count query. */
const ID_BATCH = 10_000;

/**
 * Read one file's row count from a manifest.
 *
 * @param manifest - The snapshot's manifest.
 * @param name - The file name, e.g. TRADES_FILE.
 * @returns The rows the file holds.
 * @throws Error if the manifest has no such file.
 */
export function fileRows(manifest: Manifest, name: string): number {
  const file = manifest.files[name];
  if (!file)
    throw new Error(`invalid manifest for block ${manifest.endBlockId}: no ${name} has been found`);
  return file.rows;
}

/** A snapshot file's ids, in batches of about ID_BATCH, from one streamed scan. */
async function* getIdBatches(conn: DuckDBConnection, file: string) {
  const result = await conn.stream(`SELECT id FROM read_parquet(${literal(file)})`);
  let batch: string[] = [];
  for await (const rows of result.yieldRowsJson()) {
    for (const [id] of rows) batch.push(id as string);
    if (batch.length >= ID_BATCH) {
      yield batch;
      batch = [];
    }
  }
  if (batch.length) yield batch;
}

/**
 * Delete the rows a snapshot file lists, committing each batch; a retry
 * deletes what is left. Then check no row still matches the file's WHERE
 * clause, i.e. nothing was missed by the archive.
 *
 * @param conn - DuckDB, to read the file's ids.
 * @param folder - The snapshot folder.
 * @param target - The file, as snapshotFiles describes it.
 * @param signal - Aborting it stops between batches; a retry finishes the job.
 * @returns How many rows were deleted; 0 once an earlier attempt deleted them.
 * @throws Error if the file has no WHERE clause, or rows matching it are
 *   left (those are kept); the abort reason once `signal` is aborted.
 */
async function pruneFile(
  conn: DuckDBConnection,
  folder: string,
  target: SnapshotFile,
  signal: AbortSignal,
): Promise<number> {
  let rows = 0;
  const { name, entity, where } = target;
  if (!where) throw new Error(`prune: ${name} has no WHERE clause to check the delete against`);
  for await (const ids of getIdBatches(conn, join(folder, name))) {
    signal.throwIfAborted();
    rows += await exec(`DELETE FROM ${table(entity.name)} WHERE id = ANY($1)`, [ids]);
  }
  const left = await count(`SELECT count(*) AS n FROM ${table(entity.name)} WHERE ${where}`);
  if (left > 0)
    throw new Error(`prune: ${left} ${entity.name} rows match ${name} but are not in it`);
  return rows;
}

/**
 * Prune at the block Envio has paused at: delete the trades and finalized
 * positions its snapshot archived. With `--debug`, only log the counts.
 *
 * @param config - Output directory, `--debug` and DuckDB limits.
 * @param chain - Envio's chain state; `progressBlock` is the paused block.
 * @param signal - Aborting it stops between delete batches.
 * @throws Error if the snapshot or one of its files is missing (nothing is
 *   deleted), or rows the files should hold are left in Postgres; the abort
 *   reason once `signal` is aborted.
 */
export async function pruneAt(
  config: SupervisorConfig,
  chain: PolygonChainState,
  signal: AbortSignal,
): Promise<void> {
  const started = Date.now();
  const endBlockId = chain.progressBlock;
  const manifests = snapshotManifests(config.outputDir);
  const manifest = manifests.find((m) => m.endBlockId === endBlockId);
  if (!manifest)
    throw new Error(
      `prune: no archive file has been found at block ${endBlockId}, deletion will be aborted`,
    );

  console.log(
    `prune: at block ${endBlockId}, ${fileRows(manifest, FINALIZED_FILE)} finalized positions and ` +
      `${fileRows(manifest, TRADES_FILE)} trades archived` +
      (config.debug ? ' (debug: nothing is deleted)' : ''),
  );
  if (config.debug) return;

  const files = snapshotFiles(loadSchema(), manifest.startBlockId, endBlockId);
  const target = (name: string) => files.find((f) => f.name === name)!;
  const folder = outputFolderPath(config.outputDir, endBlockId);
  using db = await createDuckDB(config);

  // To be removed after the verified run
  const previous = manifests.filter((m) => m.endBlockId < endBlockId).at(-1);
  if (previous) {
    const recreated = await countRecreated(db.conn, config.outputDir, previous);
    if (recreated > 0)
      console.log(
        `prune: WARNING ${recreated} positions finalized in snapshot ${previous.endBlockId} were written again since`,
      );
  }

  const positions = await pruneFile(db.conn, folder, target(FINALIZED_FILE), signal);
  const trades = await pruneFile(db.conn, folder, target(TRADES_FILE), signal);
  console.log(
    `prune: deleted ${positions} positions and ${trades} trades successfully (${since(started)})`,
  );
}

/**
 * Count the previous snapshot's finalized positions that Envio has written
 * again since.
 *
 * @returns Ids from its finalized file whose `lastBlock` is past it.
 */
async function countRecreated(
  conn: DuckDBConnection,
  outputDir: string,
  previous: Manifest,
): Promise<number> {
  const file = join(outputFolderPath(outputDir, previous.endBlockId), FINALIZED_FILE);
  let recreated = 0;
  for await (const ids of getIdBatches(conn, file))
    recreated += await count(
      `SELECT count(*) AS n FROM ${table('UserPosition')} WHERE id = ANY($1) AND "lastBlock" > $2`,
      [ids, previous.endBlockId],
    );
  return recreated;
}

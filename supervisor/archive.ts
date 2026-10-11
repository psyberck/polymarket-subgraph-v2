import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, renameSync, rmSync, statSync } from 'node:fs';
import { setTimeout as sleepFor } from 'node:timers/promises';
import { join } from 'node:path';
import type { DuckDBConnection } from '@duckdb/node-api';
import { literal, pool, quote, table } from '#tools/postgres.ts';
import { createDuckDB, pgQuery, queryRows } from '#tools/duckdb.ts';
import {
  type CheckRow,
  checkValues,
  diffCheckRows,
  dkCastList,
  loadSchema,
  pgSelectList,
} from '#supervisor/typemap.ts';
import { CHAIN_ID, type SupervisorConfig } from '#supervisor/config.ts';
import { type PolygonChainState, type Rpc } from '#supervisor/chain.ts';
import { getEnvioVersion } from '#supervisor/envio.ts';
import {
  assertFileSchema,
  fileCheckValues,
  type FileReport,
  type Manifest,
  MANIFEST_FILE,
  type ManifestHeader,
  outputFolderPath,
  sha256,
  since,
  type SnapshotFile,
  snapshotFiles,
  snapshotManifests,
  writeManifest,
} from '#supervisor/snapshot.ts';

/** Rows per Parquet row group. */
const ROW_GROUP_ROWS = 1_000_000;

export class HashMismatchError extends Error {}

const partialPath = (folder: string, file: SnapshotFile): string =>
  join(folder, `${file.name}.partial`);

/** Wait until the paused block is final: finalized, or max_reorg_depth below the head. */
async function waitFinal(rpc: Rpc, chain: PolygonChainState, signal: AbortSignal): Promise<void> {
  const blockId = chain.progressBlock;
  for (let attempt = 0; ; attempt++) {
    const finalized = await rpc.finalized();
    if (finalized !== null && finalized >= blockId) {
      console.log(`current finalized block is ${finalized} >= ${blockId}`);
      return;
    }
    if (finalized === null) {
      const head = await rpc.head();
      if (head.number >= blockId + chain.maxReorgDepth) {
        console.log(
          `the diff between the head block and ${blockId} is larger than ${chain.maxReorgDepth}`,
        );
        return;
      }
    }
    if (attempt % 12 === 0)
      console.log(`finality: waiting for ${blockId} (finalized ${finalized ?? 'unknown'})`);
    await sleepFor(5_000, undefined, { signal });
  }
}

/**
 * Compare every block hash Envio recorded up to the paused block with the
 * final chain. Envio records hashes only near the head; a backfilled block has
 * none, and is safe only if it was deeper than max_reorg_depth when indexed.
 *
 * @param rpc - The Polygon RPC client.
 * @param chain - Envio's chain state; `progressBlock` is the paused block.
 * @returns How many hashes matched.
 * @throws HashMismatchError on any difference, or on no hashes near the head.
 */
export async function checkHashes(rpc: Rpc, chain: PolygonChainState): Promise<number> {
  const blockId = chain.progressBlock;
  const { rows } = await pool.query<{ block_number: number; block_hash: string }>(
    `SELECT block_number, block_hash FROM ${table('envio_checkpoints')}
     WHERE chain_id = $1 AND block_hash IS NOT NULL AND block_number <= $2
     ORDER BY block_number`,
    [CHAIN_ID, blockId],
  );
  if (rows.length === 0) {
    if (chain.sourceBlock - blockId <= chain.maxReorgDepth)
      throw new HashMismatchError(
        `no hashes recorded up to ${blockId}, yet Envio stopped within max_reorg_depth of the head (${chain.sourceBlock})`,
      );
    return 0;
  }
  const onChain = await rpc.getBlockHashes(rows.map((r) => r.block_number));
  const bad = rows.find(
    (r) => onChain.get(r.block_number)?.toLowerCase() !== r.block_hash.toLowerCase(),
  );
  if (bad)
    throw new HashMismatchError(
      `block ${bad.block_number}: stored ${bad.block_hash}, chain ${onChain.get(bad.block_number)}`,
    );
  return rows.length;
}

/**
 * Archive the block Envio has paused at: wait for finality, check hashes, then
 * write its snapshot. A block that already has a manifest is skipped.
 *
 * @param config - Output directory, schedule and DuckDB limits.
 * @param chain - Envio's chain state; `progressBlock` is the paused block.
 * @param rpc - The Polygon RPC client.
 * @param signal - Aborting it stops the wait for finality.
 * @throws HashMismatchError if the indexed chain differs from the final one.
 */
export async function archiveAt(
  config: SupervisorConfig,
  chain: PolygonChainState,
  rpc: Rpc,
  signal: AbortSignal,
): Promise<void> {
  const startAt = Date.now();
  const endBlockId = chain.progressBlock;
  const snapshots = snapshotManifests(config.outputDir);
  if (snapshots.some((m) => m.endBlockId === endBlockId)) {
    console.log(`archive: block ${endBlockId} already archived`);
    return;
  }
  await waitFinal(rpc, chain, signal);
  const matched = await checkHashes(rpc, chain);
  console.log(`block ${endBlockId} has been finalized, ${matched} recorded hashes match the chain`);
  const blockHash = (await rpc.getBlockHashes([endBlockId])).get(endBlockId);
  if (!blockHash) throw new Error(`no hash for block ${endBlockId}`);

  const startBlockId =
    snapshots.filter((m) => m.endBlockId < endBlockId).at(-1)?.endBlockId ?? chain.startBlock - 1;

  using db = await createDuckDB(config);
  await writeSnapshot(db.conn, config.outputDir, {
    schedule: config.schedule,
    startBlockId,
    endBlockId,
    blockHash,
    sourceBlock: chain.sourceBlock,
    maxReorgDepth: chain.maxReorgDepth,
  });
  console.log(`archive: block ${endBlockId} has been archived successfully (${since(startAt)})`);
}

/**
 * Export, verify and publish one snapshot: a leftover folder without a
 * manifest is removed, every file is exported in one frozen view, and the
 * manifest is written last.
 *
 * @param conn - DuckDB with Postgres attached.
 * @param outputDir - The output directory (`--output-dir`).
 * @param header - The manifest fields from the caller, block range included.
 * @returns The manifest written.
 * @throws Error if the snapshot already has a manifest, or any check fails.
 */
export async function writeSnapshot(
  conn: DuckDBConnection,
  outputDir: string,
  header: ManifestHeader,
): Promise<Manifest> {
  const { startBlockId, endBlockId } = header;
  const targets = snapshotFiles(loadSchema(), startBlockId, endBlockId);
  const folder = outputFolderPath(outputDir, endBlockId);
  const manifestPath = join(folder, MANIFEST_FILE);
  if (existsSync(manifestPath))
    throw new Error(`block ${endBlockId} is already archived: ${manifestPath}`);
  if (existsSync(folder)) {
    console.log(`export: removing unverified files at ${folder}`);
    rmSync(folder, { recursive: true, force: true });
  }
  mkdirSync(folder, { recursive: true });

  const files = await exportFiles(conn, folder, targets, endBlockId);
  const schema = readFileSync(new URL('../schema.graphql', import.meta.url));
  const manifest: Manifest = {
    ...header,
    envioVersion: await getEnvioVersion(),
    schemaSha256: createHash('sha256').update(schema).digest('hex'),
    createdAt: new Date().toISOString(),
    files,
  };
  writeManifest(manifestPath, manifest);
  console.log(`export: ${MANIFEST_FILE} has been written to ${manifestPath} successfully`);
  return manifest;
}

/**
 * Export every target in one transaction, so all files come from the same
 * frozen view, pinned at `endBlockId`. Only once every file is verified are
 * they renamed from `.partial`; on any failure, the partial files are deleted.
 *
 * @returns Each file's report, by file name.
 */
async function exportFiles(
  conn: DuckDBConnection,
  folder: string,
  targets: SnapshotFile[],
  endBlockId: number,
): Promise<Record<string, FileReport>> {
  const reports: Record<string, FileReport> = {};
  await conn.run('BEGIN TRANSACTION');
  try {
    const [view] = await queryRows(
      conn,
      `SELECT * FROM ${pgQuery(`SELECT progress_block FROM ${table('envio_chains')}`)}`,
    );
    if (!view) throw new Error('envio_chains is empty');
    if (Number(view.progress_block) !== endBlockId)
      throw new Error(
        `frozen view at progress_block ${String(view.progress_block)}, expected ${endBlockId}`,
      );
    for (const target of targets) reports[target.name] = await exportFile(conn, folder, target);
    await conn.run('COMMIT');
  } catch (e) {
    await conn.run('ROLLBACK').catch(() => undefined);
    for (const target of targets) rmSync(partialPath(folder, target), { force: true });
    throw e;
  }
  for (const target of targets) renameSync(partialPath(folder, target), join(folder, target.name));
  return reports;
}

/**
 * Copy one target to `<name>.partial` and verify it: the schema assertion,
 * then the check values DuckDB computes over the file against those Postgres
 * computes over the same rows. Runs inside exportFiles' transaction.
 *
 * @throws Error on any difference.
 */
async function exportFile(
  conn: DuckDBConnection,
  folder: string,
  target: SnapshotFile,
): Promise<FileReport> {
  const start = Date.now();
  const { entity } = target;
  const path = partialPath(folder, target);
  const where = target.where ? ` WHERE ${target.where}` : '';
  await conn.run(
    `COPY (SELECT ${dkCastList(entity)} FROM ${pgQuery(
      `SELECT ${pgSelectList(entity)} FROM ${table(entity.name)}${where}`,
    )}) TO ${literal(path)} (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE ${ROW_GROUP_ROWS})`,
  );
  console.log(`export: ${entity.name} has been exported to ${target.name} (${since(start)})`);

  const checks = checkValues(entity);
  const [expected] = (await queryRows(
    conn,
    `SELECT * FROM ${pgQuery(
      `SELECT ${checks.map((c) => `${c.pgValue} AS ${quote(c.name)}`).join(', ')} FROM ${table(entity.name)}${where}`,
    )}`,
  )) as CheckRow[];
  await assertFileSchema(conn, path, entity);
  const diffs = diffCheckRows(
    'postgres',
    expected,
    'parquet',
    await fileCheckValues(conn, path, entity),
  );
  if (diffs.length) throw new Error(`${target.name}: check values differ\n  ${diffs.join('\n  ')}`);

  const report: FileReport = {
    entity: entity.name,
    rows: Number(expected.rows),
    bytes: statSync(path).size,
    sha256: await sha256(path),
    checkValues: expected,
  };
  console.log(
    `export: ${target.name} has been verified with ${report.rows} rows, ${report.bytes} bytes (${since(start)})`,
  );
  return report;
}

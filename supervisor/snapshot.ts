import { createHash } from 'node:crypto';
import {
  createReadStream,
  existsSync,
  readdirSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from 'node:fs';
import { basename, join } from 'node:path';
import type { DuckDBConnection } from '@duckdb/node-api';
import { literal, table } from '#tools/postgres.ts';
import { queryRows } from '#tools/duckdb.ts';
import type { Schedule } from '#supervisor/config.ts';
import { type CheckRow, checkValues, codec, type Entity } from '#supervisor/typemap.ts';

export const MANIFEST_FILE = 'manifest.json';

/** Trades in (startBlockId, endBlockId]. */
export const TRADES_FILE = 'OrderFilled.parquet';

/** Finalized positions; prune deletes their ids. */
export const FINALIZED_FILE = 'UserPosition.finalized.parquet';

/** 0.1 tokens: below it a position counts as closed, as in the sell handler. */
const CLOSED_POSITION_DUST = 100_000n;

/**
 * Positions that can no longer change: closed, on a resolved market. A closed
 * position on an open market can be re-entered, so it does not qualify.
 */
export const FINALIZED_POSITIONS = `amount < ${CLOSED_POSITION_DUST} AND EXISTS (SELECT 1 FROM ${table('Condition')} c WHERE c.id = ${table('UserPosition')}."conditionId" AND c."payoutPrices" IS NOT NULL)`;

/**
 * Not in the restore point: OrderFilled is archived per interval; SplitMarker
 * rows are dead at a block boundary (prune clears them, with `--prune`).
 */
const NOT_IN_RESTORE_POINT = new Set(['OrderFilled', 'SplitMarker']);

/** One file to export: an entity's rows, optionally filtered. */
export interface SnapshotFile {
  /** File name; written as `<name>.partial` first. */
  name: string;
  entity: Entity;
  /** Postgres WHERE clause for both the copy and its check values. */
  where?: string;
}

/** A verified file, as the manifest records it. */
export interface FileReport {
  /** Entity name. */
  entity: string;
  rows: number;
  bytes: number;
  sha256: string;
  checkValues: CheckRow;
}

/** A snapshot's record; written last, so it exists only for a complete snapshot. */
export interface Manifest {
  schedule: Schedule | null;
  /** Previous snapshot's block (exclusive). */
  startBlockId: number;
  /** Paused block (inclusive); names the folder. */
  endBlockId: number;
  /** endBlockId's hash on the final chain. */
  blockHash: string;
  /** Chain head Envio last saw. */
  sourceBlock: number;
  maxReorgDepth: number;
  envioVersion: string | null;
  /** sha256 of schema.graphql. */
  schemaSha256: string;
  /** ISO timestamp. */
  createdAt: string;
  /** By file name. */
  files: Record<string, FileReport>;
}

/** Manifest fields the caller supplies; the rest come from the export. */
export type ManifestHeader = Pick<
  Manifest,
  'schedule' | 'startBlockId' | 'endBlockId' | 'blockHash' | 'sourceBlock' | 'maxReorgDepth'
>;

/** A snapshot's folder, `<outputDir>/<blockId>`. */
export const outputFolderPath = (outputDir: string, blockId: number): string =>
  join(outputDir, String(blockId));

/** Entities copied in full as the restore point: all but OrderFilled and SplitMarker. */
export const snapshotEntities = (entities: Entity[]): Entity[] =>
  entities.filter((e) => !NOT_IN_RESTORE_POINT.has(e.name));

/** Seconds since `start` (a Date.now() value), for step timings. */
export const since = (start: number): string => `${((Date.now() - start) / 1000).toFixed(1)}s`;

/**
 * List a snapshot's files: trades, finalized positions, then the restore
 * point. The two position files are disjoint.
 *
 * @param entities - Every entity in schema.graphql.
 * @param startBlockId - Previous snapshot's block (exclusive).
 * @param endBlockId - This snapshot's block (inclusive).
 * @returns The files to export, in order.
 * @throws Error on an empty interval or an entity missing from the schema.
 */
export function snapshotFiles(
  entities: Entity[],
  startBlockId: number,
  endBlockId: number,
): SnapshotFile[] {
  if (startBlockId >= endBlockId)
    throw new Error(`empty trade interval (${startBlockId}, ${endBlockId}]`);
  const entity = (name: string) => {
    const found = entities.find((e) => e.name === name);
    if (!found) throw new Error(`${name} is not a valid entity defined in schema.graphql`);
    return found;
  };
  return [
    {
      name: TRADES_FILE,
      entity: entity('OrderFilled'),
      where: `"blockNumber" > ${startBlockId} AND "blockNumber" <= ${endBlockId}`,
    },
    { name: FINALIZED_FILE, entity: entity('UserPosition'), where: FINALIZED_POSITIONS },
    ...snapshotEntities(entities).map((e) => ({
      name: `${e.name}.parquet`,
      entity: e,
      where: e.name === 'UserPosition' ? `(${FINALIZED_POSITIONS}) IS NOT TRUE` : undefined,
    })),
  ];
}

/**
 * Write a manifest atomically, via `<path>.partial` and a rename.
 *
 * @param path - The manifest's path in the snapshot folder.
 * @param manifest - The manifest to write.
 */
export function writeManifest(path: string, manifest: Manifest): void {
  writeFileSync(`${path}.partial`, `${JSON.stringify(manifest, null, 2)}\n`);
  renameSync(`${path}.partial`, path);
}

/**
 * Read a snapshot folder's manifest.
 *
 * @param dir - The snapshot folder.
 * @returns The manifest, or null if the snapshot was never completed.
 */
export function readManifest(dir: string): Manifest | null {
  const path = join(dir, MANIFEST_FILE);
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, 'utf8')) as Manifest;
}

/**
 * Read every complete snapshot's manifest.
 *
 * @param outputDir - `--output-dir`.
 * @returns Manifests by endBlockId; folders without one are skipped.
 */
export function snapshotManifests(outputDir: string): Manifest[] {
  if (!existsSync(outputDir)) return [];
  return readdirSync(outputDir)
    .filter((name) => /^\d+$/.test(name))
    .map((name) => readManifest(join(outputDir, name)))
    .filter((m) => m !== null)
    .sort((a, b) => a.endBlockId - b.endBlockId);
}

/**
 * Hash a file, streamed.
 *
 * @param path - The file.
 * @returns Its sha256, in hex.
 */
export async function sha256(path: string): Promise<string> {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(path)) hash.update(chunk as Buffer);
  return hash.digest('hex');
}

/**
 * Assert a Parquet file's column names, order and types match the type map.
 *
 * @param conn - DuckDB connection.
 * @param path - The Parquet file.
 * @param entity - The entity it holds.
 * @throws Error listing the file's columns and the expected ones.
 */
export async function assertFileSchema(
  conn: DuckDBConnection,
  path: string,
  entity: Entity,
): Promise<void> {
  const described = await queryRows(conn, `DESCRIBE SELECT * FROM read_parquet(${literal(path)})`);
  const got = described.map((r) => `${r.column_name as string} ${r.column_type as string}`);
  const want = entity.columns.map((c) => `${c.name} ${codec(c).parquetType}`);
  if (got.join(', ') !== want.join(', '))
    throw new Error(
      `${basename(path)}: schema assertion failed\n  file:     ${got.join(', ')}\n  expected: ${want.join(', ')}`,
    );
}

/**
 * Compute a Parquet file's check values in DuckDB.
 *
 * @param conn - DuckDB connection.
 * @param path - The Parquet file.
 * @param entity - The entity it holds.
 * @returns The check values, comparable with Postgres' over the same rows.
 */
export async function fileCheckValues(
  conn: DuckDBConnection,
  path: string,
  entity: Entity,
): Promise<CheckRow> {
  const checks = checkValues(entity);
  const [row] = await queryRows(
    conn,
    `SELECT ${checks.map((c) => `${c.dkValue} AS "${c.name}"`).join(', ')} FROM read_parquet(${literal(path)})`,
  );
  return row as CheckRow;
}

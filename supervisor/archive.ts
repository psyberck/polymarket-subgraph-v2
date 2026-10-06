/**
 * The Parquet archive end to end (snapshot-design.md §3.5–3.6): export from
 * Postgres through DuckDB, verify, publish manifests, and read back which
 * verified files exist.
 */
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  createReadStream,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { basename, join } from 'node:path';
import { type DuckDBConnection, DuckDBInstance } from '@duckdb/node-api';
import { pgSchema, pgSettings } from '#tools/postgres.ts';
import type { DuckDBConfig } from '#supervisor/restore.ts';
import {
  checkValues,
  duckCastList,
  type Entity,
  pgSelectList,
  udtMatches,
} from '#supervisor/typemap.ts';
import { literal, log, quote, since, table } from '#supervisor/util.ts';

// ── export ────────────────────────────────────────────────────────────
/*
 * Postgres → Parquet through DuckDB. No row passes through JavaScript.
 *
 * Everything for one export runs inside one DuckDB transaction. Inside it,
 * every postgres_query uses a single Postgres connection at REPEATABLE READ
 * (measured, §8.6), so the files and the check values describe one frozen
 * view, even if the indexer writes meanwhile. The first statement reads
 * envio_chains, which both freezes the view and proves which block it is.
 *
 * Files are written under a `.partial` name; the verify step renames them once they
 * have been checked, so a final name always means a verified file.
 */

/** One file to write: an entity, optionally filtered (trades by block range). */
export interface ExportUnit {
  entity: Entity;
  /** Postgres WHERE clause, applied to both the copy and its check values. */
  where?: string;
  /** Final path; the file is written to `<path>.partial` first. */
  path: string;
}

export interface ExportedFile {
  unit: ExportUnit;
  partialPath: string;
  /** Check values computed by Postgres over exactly the rows copied. */
  pgCheck: Record<string, string | null>;
  seconds: number;
}

export interface FrozenView {
  progressBlock: number;
  sourceBlock: number;
  maxReorgDepth: number;
  envioVersion: string | null;
}

export interface ExportResult {
  view: FrozenView;
  files: ExportedFile[];
}

export interface ExportOptions {
  /** Abort unless the frozen view's progress_block passes this check. */
  expectBlock: (progressBlock: number) => string | null;
  /** Called once the view is frozen: the after_freeze resume point (§3.10). */
  onFrozen?: (view: FrozenView) => Promise<void>;
}

/** `postgres_query` over the attached database; the SQL is dollar-quoted. */
function pgQuery(sql: string): string {
  if (sql.includes('$$')) throw new Error('Postgres SQL must not contain $$');
  return `postgres_query('pg', $$${sql}$$)`;
}

async function rows(conn: DuckDBConnection, sql: string): Promise<Record<string, unknown>[]> {
  return (await conn.runAndReadAll(sql)).getRowObjectsJson();
}

/** Rows per Parquet row group. */
const ROW_GROUP_ROWS = 1_000_000;

/**
 * A DuckDB connection with the configured resource limits and Postgres
 * attached, read-only unless `readWrite` (only the restore writes).
 *
 * Limits are set when the instance starts: DuckDB's defaults (80% of RAM,
 * every core) froze this box once, and it also runs Postgres and Envio.
 * Beyond the memory limit DuckDB spills to its default `.tmp`, relative to
 * the working directory (the indexer directory under run_indexer.sh).
 * Credentials go through an in-memory DuckDB secret rather than a connection
 * string, so the password never appears in an ATTACH statement or its errors.
 * `sslmode`, which a postgres secret cannot hold, is passed in the ATTACH
 * string and applies together with the secret (measured).
 *
 * The postgres extension must already be installed (deploy step,
 * scripts/install_duckdb_extensions.ts); autoinstall is off so an export never
 * downloads anything, and ATTACH loads the installed extension itself.
 */
export async function openDuckDB(
  limits: DuckDBConfig,
  { readWrite = false }: { readWrite?: boolean } = {},
): Promise<DuckDBConnection> {
  const instance = await DuckDBInstance.create(':memory:', {
    memory_limit: limits.memoryLimit,
    threads: String(limits.threads),
    autoinstall_known_extensions: 'false',
  });
  const conn = await instance.connect();
  const s = pgSettings;
  const options = s.sslMode ? `sslmode=${s.sslMode}` : '';
  try {
    // Both statements need the extension; a missing one fails at the first.
    await conn.run(
      `CREATE SECRET pg (TYPE postgres, HOST ${literal(s.host)}, PORT ${s.port}, ` +
        `DATABASE ${literal(s.database)}, USER ${literal(s.user)}, PASSWORD ${literal(s.password)})`,
    );
    await conn.run(
      `ATTACH ${literal(options)} AS pg (TYPE postgres, SECRET pg${readWrite ? '' : ', READ_ONLY'})`,
    );
  } catch (e) {
    if (/Extension/.test(String(e)))
      throw new Error(
        'DuckDB postgres extension not installed; run scripts/install_duckdb_extensions.ts',
        { cause: e },
      );
    throw e;
  }
  return conn;
}

/** Fail unless the table has exactly the columns and types the schema says. */
async function assertPgColumns(conn: DuckDBConnection, entity: Entity): Promise<void> {
  const actual = await rows(
    conn,
    `SELECT * FROM ${pgQuery(
      `SELECT column_name, udt_name, is_nullable FROM information_schema.columns
       WHERE table_schema = ${literal(pgSchema)} AND table_name = ${literal(entity.name)}
       ORDER BY ordinal_position`,
    )}`,
  );
  const got = actual.map(
    (r) => `${String(r.column_name)}:${String(r.udt_name)}:${String(r.is_nullable)}`,
  );
  const want = entity.columns.map((c) => `${c.name}:${c.pgUdt}:${c.nullable ? 'YES' : 'NO'}`);
  const matches =
    actual.length === entity.columns.length &&
    entity.columns.every((c, i) => {
      const r = actual[i];
      return (
        r.column_name === c.name &&
        udtMatches(c, String(r.udt_name)) &&
        r.is_nullable === (c.nullable ? 'YES' : 'NO')
      );
    });
  if (!matches)
    throw new Error(
      `${pgSchema}.${entity.name} does not match schema.graphql:\n  table:  ${got.join(', ')}\n  schema: ${want.join(', ')}`,
    );
}

/**
 * Copy every unit to Parquet and compute its check values, all in one frozen
 * view. Rolls back and deletes partial files on any failure.
 */
export async function exportUnits(
  conn: DuckDBConnection,
  units: ExportUnit[],
  options: ExportOptions,
): Promise<ExportResult> {
  const partials = units.map((u) => `${u.path}.partial`);
  for (const u of units) {
    if (existsSync(u.path)) throw new Error(`${u.path} already exists; refusing to overwrite`);
    mkdirSync(join(u.path, '..'), { recursive: true });
  }
  for (const p of partials) rmSync(p, { force: true });

  await conn.run('BEGIN TRANSACTION');
  try {
    // First statement: freezes the view and says which block it holds.
    const [chain] = await rows(
      conn,
      `SELECT * FROM ${pgQuery(
        `SELECT c.progress_block, c.source_block, c.max_reorg_depth,
                (SELECT config::jsonb->>'version' FROM ${table('envio_info')} LIMIT 1) AS envio_version
         FROM ${table('envio_chains')} c`,
      )}`,
    );
    if (!chain) throw new Error('envio_chains is empty');
    const view: FrozenView = {
      progressBlock: Number(chain.progress_block),
      sourceBlock: Number(chain.source_block),
      maxReorgDepth: Number(chain.max_reorg_depth),
      envioVersion: (chain.envio_version as string | null) ?? null,
    };
    const problem = options.expectBlock(view.progressBlock);
    if (problem) throw new Error(`frozen view at progress_block ${view.progressBlock}: ${problem}`);
    log(`export: view frozen at progress_block ${view.progressBlock}`);
    await options.onFrozen?.(view);

    const files: ExportedFile[] = [];
    for (const [i, unit] of units.entries()) {
      const start = Date.now();
      const { entity } = unit;
      await assertPgColumns(conn, entity);
      const where = unit.where ? ` WHERE ${unit.where}` : '';
      await conn.run(
        `COPY (SELECT ${duckCastList(entity)} FROM ${pgQuery(
          `SELECT ${pgSelectList(entity)} FROM ${table(entity.name)}${where}`,
        )}) TO ${literal(partials[i])} (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE ${ROW_GROUP_ROWS})`,
      );
      const copied = since(start);
      const checks = checkValues(entity);
      const [row] = await rows(
        conn,
        `SELECT * FROM ${pgQuery(
          `SELECT ${checks.map((c) => `${c.pg} AS ${quote(c.key)}`).join(', ')}FROM ${table(entity.name)}${where}`,
        )}`,
      );
      const pgCheck = row as Record<string, string | null>;
      files.push({
        unit,
        partialPath: partials[i],
        pgCheck,
        seconds: (Date.now() - start) / 1000,
      });
      log(
        `export: ${entity.name}${where} → ${pgCheck.rows ?? 0} rows (copy ${copied}, check values ${since(start)} total)`,
      );
    }
    await conn.run('COMMIT');
    return { view, files };
  } catch (e) {
    await conn.run('ROLLBACK').catch(() => undefined);
    for (const p of partials) rmSync(p, { force: true });
    throw e;
  }
}

/** A snapshot's directory under the output directory (`--output-dir`). */
export const snapshotDir = (outputDir: string, snapshotId: string): string =>
  join(outputDir, 'snapshots', snapshotId);

/** The trade file for blocks (fromExclusive, toInclusive] under the output directory. */
export const tradesPath = (outputDir: string, fromExclusive: number, toInclusive: number): string =>
  join(outputDir, 'trades', `b${fromExclusive + 1}-b${toInclusive}.parquet`);

/**
 * Entities a snapshot copies in full: every entity except OrderFilled, which
 * is archived per interval in trade files (§3.7), and SplitMarker, which is
 * cleared at each pause (§3.8). Derived, not configured: a snapshot is only a
 * restore point if nothing is left out.
 */
const NOT_IN_SNAPSHOT = new Set(['OrderFilled', 'SplitMarker']);

export const snapshotEntities = (entities: Entity[]): Entity[] =>
  entities.filter((e) => !NOT_IN_SNAPSHOT.has(e.name));

/** Units for a snapshot: one file per snapshot entity. */
export function snapshotUnits(
  outputDir: string,
  entities: Entity[],
  snapshotId: string,
): ExportUnit[] {
  const dir = snapshotDir(outputDir, snapshotId);
  return snapshotEntities(entities).map((entity) => ({
    entity,
    path: join(dir, `${entity.name}.parquet`),
  }));
}

/** The unit for one trade interval (§3.7). */
export function tradesUnit(
  outputDir: string,
  entities: Entity[],
  fromExclusive: number,
  toInclusive: number,
): ExportUnit {
  const entity = entities.find((e) => e.name === 'OrderFilled');
  if (!entity) throw new Error('OrderFilled is not in schema.graphql');
  if (!(toInclusive > fromExclusive))
    throw new Error(`empty trade interval (${fromExclusive}, ${toInclusive}]`);
  return {
    entity,
    where: `"blockNumber" > ${fromExclusive} AND "blockNumber" <= ${toInclusive}`,
    path: tradesPath(outputDir, fromExclusive, toInclusive),
  };
}

// ── verify ────────────────────────────────────────────────────────────
/*
 * Verify exported files and write their manifest (snapshot-design.md §3.6).
 *
 * For each file: the schema assertion (every column has exactly its type-map
 * type, which turns DuckDB's silent DOUBLE fallbacks into failures), then the
 * check values recomputed by DuckDB from the file and compared, as text, with
 * the values Postgres computed over the same rows in the export's frozen view.
 * The fingerprint among them compares every value of every row.
 *
 * Only after every file passes are they renamed from `.partial` and the
 * manifest written, last. A file or snapshot without a manifest does not
 * exist.
 *
 * Reads the local staged files. Reading back from S3 replaces this once
 * uploads exist; the checks stay the same.
 */

export interface FileReport {
  name: string;
  entity: string;
  rows: number;
  bytes: number;
  sha256: string;
  checkValues: Record<string, string | null>;
}

export interface Manifest {
  kind: 'snapshot' | 'trades' | 'closed';
  id: string;
  schedule: string;
  b: number;
  bPrev: number | null;
  /** Not yet filled: needs the RPC module (Phase 3). */
  blockHash: string | null;
  blockTimestamp: number | null;
  sourceBlock: number;
  maxReorgDepth: number;
  envioVersion: string | null;
  gitCommit: string;
  gitDirty: boolean;
  schemaSha256: string;
  /** `verified-local` until files are verified from S3. */
  status: 'verified-local' | 'verified';
  createdAt: string;
  files: FileReport[];
}

/** The manifest fields the caller supplies; the rest come from the export. */
export type ManifestHeader = Pick<
  Manifest,
  'kind' | 'id' | 'schedule' | 'bPrev' | 'blockHash' | 'blockTimestamp'
>;

/** sha256 of a file, streamed. */
export async function sha256(path: string): Promise<string> {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(path)) hash.update(chunk as Buffer);
  return hash.digest('hex');
}

/**
 * The schema assertion: names, order and types exactly as the type map says.
 *
 * @throws listing every column that differs.
 */
export async function assertFileSchema(
  conn: DuckDBConnection,
  path: string,
  entity: Entity,
): Promise<void> {
  const described = (
    await conn.runAndReadAll(`DESCRIBE SELECT * FROM read_parquet(${literal(path)})`)
  ).getRowObjectsJson();
  const got = described.map((r) => `${r.column_name as string} ${r.column_type as string}`);
  const want = entity.columns.map((c) => `${c.name} ${c.parquetType}`);
  if (got.join(', ') !== want.join(', '))
    throw new Error(
      `${basename(path)}: schema assertion failed\n  file:     ${got.join(', ')}\n  expected: ${want.join(', ')}`,
    );
}

/** Check values recomputed by DuckDB over a file. */
export async function fileCheckValues(
  conn: DuckDBConnection,
  path: string,
  entity: Entity,
): Promise<Record<string, string | null>> {
  const checks = checkValues(entity);
  const [row] = (
    await conn.runAndReadAll(
      `SELECT ${checks.map((c) => `${c.duck} AS "${c.key}"`).join(', ')} FROM read_parquet(${literal(path)})`,
    )
  ).getRowObjectsJson();
  return row as Record<string, string | null>;
}

/** Verify one exported file; throws on any difference. */
async function verifyFile(conn: DuckDBConnection, file: ExportedFile): Promise<FileReport> {
  const start = Date.now();
  const { entity } = file.unit;
  await assertFileSchema(conn, file.partialPath, entity);
  const duck = await fileCheckValues(conn, file.partialPath, entity);
  const diffs = Object.keys(file.pgCheck)
    .filter((k) => (file.pgCheck[k] ?? null) !== (duck[k] ?? null))
    .map((k) => `${k}: postgres ${file.pgCheck[k]} file ${duck[k]}`);
  if (diffs.length)
    throw new Error(`${basename(file.unit.path)}: check values differ\n  ${diffs.join('\n  ')}`);
  const report: FileReport = {
    name: basename(file.unit.path),
    entity: entity.name,
    rows: Number(file.pgCheck.rows),
    bytes: statSync(file.partialPath).size,
    sha256: await sha256(file.partialPath),
    checkValues: file.pgCheck,
  };
  log(`verify: ${report.name} ok, ${report.rows} rows, ${report.bytes} bytes (${since(start)})`);
  return report;
}

function git(args: string[]): string {
  return execFileSync('git', args, { cwd: new URL('..', import.meta.url).pathname })
    .toString()
    .trim();
}

/** The fields every manifest takes from the working tree. */
export function provenance(): Pick<Manifest, 'gitCommit' | 'gitDirty' | 'schemaSha256'> {
  const schema = readFileSync(new URL('../schema.graphql', import.meta.url));
  return {
    gitCommit: git(['rev-parse', 'HEAD']),
    gitDirty: git(['status', '--porcelain', '--', '.']) !== '',
    schemaSha256: createHash('sha256').update(schema).digest('hex'),
  };
}

/**
 * Verify every file of an export, then publish: rename the files to their
 * final names and write the manifest last. Nothing is renamed unless every
 * file passed.
 *
 * @param manifestPath - where the manifest goes (inside a snapshot directory,
 *   or `<trade file>.manifest.json`).
 * @param header - manifest fields that depend on the caller.
 */
export async function verifyAndPublish(
  conn: DuckDBConnection,
  result: ExportResult,
  manifestPath: string,
  header: ManifestHeader,
): Promise<Manifest> {
  const files: FileReport[] = [];
  for (const file of result.files) files.push(await verifyFile(conn, file));
  for (const file of result.files) renameSync(file.partialPath, file.unit.path);
  const manifest: Manifest = {
    ...header,
    ...provenance(),
    b: result.view.progressBlock,
    sourceBlock: result.view.sourceBlock,
    maxReorgDepth: result.view.maxReorgDepth,
    envioVersion: result.view.envioVersion,
    status: 'verified-local',
    createdAt: new Date().toISOString(),
    files,
  };
  writeFileSync(`${manifestPath}.partial`, `${JSON.stringify(manifest, null, 2)}\n`);
  renameSync(`${manifestPath}.partial`, manifestPath);
  log(`verify: manifest written, ${manifestPath}`);
  return manifest;
}

// ── files ─────────────────────────────────────────────────────────────
/*
 * What the output directory says (snapshot-design.md §3.1): the manifests are
 * the only record of which snapshots and trade files exist, and a file without
 * one does not exist.
 */

const readManifest = (path: string): Manifest => JSON.parse(readFileSync(path, 'utf8')) as Manifest;

/** Every verified snapshot's manifest, by block. */
export function snapshotManifests(outputDir: string): Manifest[] {
  const root = join(outputDir, 'snapshots');
  if (!existsSync(root)) return [];
  return readdirSync(root)
    .map((id) => join(root, id, 'manifest.json'))
    .filter((p) => existsSync(p))
    .map(readManifest)
    .sort((a, b) => a.b - b.b);
}

/** Whether a verified snapshot exists at block `b`. */
export const hasSnapshotAt = (outputDir: string, b: number): boolean =>
  snapshotManifests(outputDir).some((m) => m.b === b);

/**
 * The start (exclusive) of the trade range a snapshot at `b` closes: the
 * previous verified snapshot's block, or `fallback` before the first one.
 */
export function previousSnapshotB(outputDir: string, b: number, fallback: number): number {
  const earlier = snapshotManifests(outputDir).filter((m) => m.b < b);
  return earlier.length ? earlier[earlier.length - 1].b : fallback;
}

/** Whether the trade file for (fromExclusive, toInclusive] is verified. */
export const hasTradesFile = (outputDir: string, from: number, to: number): boolean =>
  existsSync(`${tradesPath(outputDir, from, to)}.manifest.json`);

/**
 * Delete what a crash left behind (§3.1): `.partial` files, snapshot
 * directories without a manifest, and trade files without one. None of them
 * was ever verified, and an export refuses to overwrite a final name.
 */
export function removeUnverified(outputDir: string): void {
  const snapshots = join(outputDir, 'snapshots');
  if (existsSync(snapshots))
    for (const id of readdirSync(snapshots)) {
      const dir = snapshotDir(outputDir, id);
      if (!existsSync(join(dir, 'manifest.json'))) {
        log(`files: removing unverified snapshot ${id}`);
        rmSync(dir, { recursive: true, force: true });
      }
    }
  const trades = join(outputDir, 'trades');
  if (existsSync(trades))
    for (const name of readdirSync(trades)) {
      const path = join(trades, name);
      const unverified =
        name.endsWith('.partial') ||
        (name.endsWith('.parquet') && !existsSync(`${path}.manifest.json`));
      if (unverified) {
        log(`files: removing unverified ${name}`);
        rmSync(path, { force: true });
      }
    }
}

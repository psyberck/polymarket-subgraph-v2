/**
 * Load a verified snapshot into a freshly migrated Envio schema
 * (snapshot-design.md §3.14). The reverse of the export in archive.ts.
 *
 * Refuses unless the target is provably fresh: every entity table empty and
 * envio_chains a chain that has never run, starting at B + 1. That is what
 * `envio local db-migrate up` leaves with start_block = B + 1, and it makes a
 * restore over the live index impossible by accident.
 *
 * Values never travel as DuckDB numbers. DuckDB sees Postgres's unconstrained
 * `numeric` as DOUBLE, so inserting into the entity tables through the
 * attached database would round token ids and amounts (the §3.5 pitfall in
 * reverse). Each file is copied instead into an unlogged staging table whose
 * numeric columns are text, and Postgres converts text → numeric exactly with
 * INSERT … SELECT. All entity inserts, their check values and the staging
 * cleanup run in one Postgres transaction: the restore lands whole or not at
 * all.
 */
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import type { DuckDBConnection } from '@duckdb/node-api';
import { pgSchema, pool } from '#tools/postgres.ts';
import { assertFileSchema, type Manifest, sha256, snapshotEntities } from '#supervisor/archive.ts';
import {
  checkValues,
  type Column,
  type Entity,
  loadSchema,
  udtMatches,
} from '#supervisor/typemap.ts';
import { literal, log, quote, since } from '#supervisor/util.ts';

export interface DuckDBConfig {
  /** DuckDB memory limit, e.g. 2GB. */
  memoryLimit: string;
  /** DuckDB worker threads. */
  threads: number;
}

export interface RestoreResult {
  /** The snapshot's block; Envio continues at B + 1. */
  b: number;
  tables: { entity: string; rows: number; seconds: number }[];
}

const table = (name: string): string => `${quote(pgSchema)}.${quote(name)}`;
const stagingName = (entity: Entity): string => `_restore_${entity.name}`;

/** Staging column type: numeric travels as text, everything else as itself. */
const STAGING_TYPE: Record<Column['pgUdt'], string> = {
  text: 'text',
  numeric: 'text',
  int4: 'integer',
  bool: 'boolean',
  _numeric: 'text[]',
};

/** DuckDB select from the file: decimals back to their exact text. */
function duckToText(entity: Entity): string {
  return entity.columns
    .map((c) => {
      const col = quote(c.name);
      if (c.kind === 'decimal') return `${col}::VARCHAR AS ${col}`;
      if (c.kind === 'decimalList') return `list_transform(${col}, v -> v::VARCHAR) AS ${col}`;
      return col;
    })
    .join(', ');
}

/**
 * Postgres select from staging into the entity table: text → numeric, and the
 * list to whichever array type the target actually has (`udts`, §3.5 legacy).
 */
function pgFromText(entity: Entity, udts: Map<string, string>): string {
  return entity.columns
    .map((c) => {
      const col = quote(c.name);
      if (c.pgUdt === 'numeric') return `${col}::numeric`;
      if (c.kind === 'decimalList')
        return udts.get(c.name) === '_numeric' ? `${col}::numeric[]` : col;
      return col;
    })
    .join(', ');
}

/** The target table's column types, checked against the schema. */
async function targetUdts(entity: Entity): Promise<Map<string, string>> {
  const { rows } = await pool.query<{ column_name: string; udt_name: string }>(
    `SELECT column_name, udt_name FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`,
    [pgSchema, entity.name],
  );
  const ok =
    rows.length === entity.columns.length &&
    entity.columns.every(
      (c, i) => rows[i].column_name === c.name && udtMatches(c, rows[i].udt_name),
    );
  if (!ok)
    throw new Error(
      `${pgSchema}.${entity.name} does not match schema.graphql: ${rows.map((r) => `${r.column_name}:${r.udt_name}`).join(', ')}`,
    );
  return new Map(rows.map((r) => [r.column_name, r.udt_name]));
}

/** Read and check the snapshot's manifest and files before touching Postgres. */
async function checkSnapshot(
  conn: DuckDBConnection,
  dir: string,
  entities: Entity[],
): Promise<Manifest> {
  const manifest = JSON.parse(readFileSync(join(dir, 'manifest.json'), 'utf8')) as Manifest;
  if (manifest.kind !== 'snapshot') throw new Error(`${dir}: not a snapshot (${manifest.kind})`);
  if (manifest.status !== 'verified' && manifest.status !== 'verified-local')
    throw new Error(`${dir}: manifest status ${String(manifest.status)}, not verified`);
  const want = entities.map((e) => `${e.name}.parquet`).sort();
  const got = manifest.files.map((f) => f.name).sort();
  if (want.join() !== got.join())
    throw new Error(`${dir}: files ${got.join(', ')} do not match the schema's ${want.join(', ')}`);
  for (const entity of entities) {
    const file = manifest.files.find((f) => f.entity === entity.name)!;
    const path = join(dir, file.name);
    // Columns against today's schema.graphql: a snapshot from other columns
    // needs a conversion, never a guess.
    await assertFileSchema(conn, path, entity);
    if ((await sha256(path)) !== file.sha256)
      throw new Error(`${file.name}: sha256 differs from the manifest`);
  }
  return manifest;
}

/** Refuse anything but a fresh, never-run schema starting at B + 1. */
async function checkTarget(b: number, entities: Entity[]): Promise<void> {
  const { rows } = await pool.query<{ start_block: number; progress_block: number }>(
    `SELECT start_block, progress_block FROM ${table('envio_chains')}`,
  );
  if (rows.length !== 1) throw new Error(`envio_chains has ${rows.length} rows, expected 1`);
  const [{ start_block: start, progress_block: progress }] = rows as [
    { start_block: number; progress_block: number },
  ];
  if (start !== b + 1)
    throw new Error(`envio_chains.start_block is ${start}; a restore at ${b} needs ${b + 1}`);
  if (progress >= start)
    throw new Error(`envio_chains.progress_block is ${progress}: this schema has already indexed`);
  for (const name of [...entities.map((e) => e.name), 'OrderFilled', 'SplitMarker']) {
    const { rows: any } = await pool.query(`SELECT 1 FROM ${table(name)} LIMIT 1`);
    if (any.length) throw new Error(`${pgSchema}.${name} is not empty`);
  }
}

/**
 * Restore the snapshot in `dir` into the schema ENVIO_PG_* points at.
 *
 * @param conn - DuckDB opened with `readWrite: true`.
 * @param dir - the snapshot's directory (manifest.json and one file per entity).
 */
export async function restoreSnapshot(conn: DuckDBConnection, dir: string): Promise<RestoreResult> {
  const entities = snapshotEntities(loadSchema());
  const manifest = await checkSnapshot(conn, dir, entities);
  await checkTarget(manifest.b, entities);
  const udts = new Map<string, Map<string, string>>();
  for (const entity of entities) udts.set(entity.name, await targetUdts(entity));
  log(`restore: ${manifest.id} at ${manifest.b} into ${pgSchema}, checks passed`);

  // Stage every file as text, outside the transaction (DuckDB writes on its
  // own connection).
  const times = new Map<string, number>();
  for (const entity of entities) {
    const start = Date.now();
    const staging = table(stagingName(entity));
    await pool.query(
      `DROP TABLE IF EXISTS ${staging};
       CREATE UNLOGGED TABLE ${staging} (${entity.columns
         .map((c) => `${quote(c.name)} ${STAGING_TYPE[c.pgUdt]}`)
         .join(', ')})`,
    );
    await conn.run('CALL pg_clear_cache()'); // DuckDB must see the new table
    const file = join(dir, `${entity.name}.parquet`);
    await conn.run(
      `INSERT INTO pg.${quote(pgSchema)}.${quote(stagingName(entity))}
       SELECT ${duckToText(entity)} FROM read_parquet(${literal(file)})`,
    );
    times.set(entity.name, Date.now() - start);
    log(`restore: staged ${entity.name} (${since(start)})`);
  }

  // One transaction: inserts, check values, cleanup.
  const client = await pool.connect();
  const tables: RestoreResult['tables'] = [];
  try {
    await client.query('BEGIN');
    for (const entity of entities) {
      const start = Date.now();
      const cols = entity.columns.map((c) => quote(c.name)).join(', ');
      await client.query(
        `INSERT INTO ${table(entity.name)} (${cols})
         SELECT ${pgFromText(entity, udts.get(entity.name)!)} FROM ${table(stagingName(entity))}
         ORDER BY id -- index pages fill in order; random order was ~15k rows/s`,
      );
      const checks = checkValues(entity);
      const { rows } = await client.query<Record<string, string | null>>(
        `SELECT ${checks.map((c) => `${c.pg} AS ${quote(c.key)}`).join(', ')}FROM ${table(entity.name)}`,
      );
      const expected = manifest.files.find((f) => f.entity === entity.name)!.checkValues;
      const diffs = checks
        .map((c) => c.key)
        .filter((k) => (rows[0][k] ?? null) !== (expected[k] ?? null))
        .map((k) => `${k}: restored ${rows[0][k]} manifest ${expected[k]}`);
      if (diffs.length)
        throw new Error(
          `${entity.name}: check values differ after restore\n  ${diffs.join('\n  ')}`,
        );
      await client.query(`DROP TABLE ${table(stagingName(entity))}`);
      const rowCount = Number(rows[0].rows);
      tables.push({
        entity: entity.name,
        rows: rowCount,
        seconds: (times.get(entity.name)! + Date.now() - start) / 1000,
      });
      log(`restore: ${entity.name} loaded and verified, ${rowCount} rows (${since(start)})`);
    }
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK').catch(() => undefined);
    // The rolled-back inserts leave millions of dead rows, and the next
    // attempt's emptiness check would crawl through them (measured: >10 min
    // on 3.7M). The tables were proven empty and nothing else writes them, so
    // TRUNCATE is safe and restores the fresh state instantly.
    await pool
      .query(`TRUNCATE ${entities.map((entity) => table(entity.name)).join(', ')}`)
      .catch(() => undefined);
    for (const entity of entities)
      await pool.query(`DROP TABLE IF EXISTS ${table(stagingName(entity))}`).catch(() => undefined);
    throw e;
  } finally {
    client.release();
  }
  return { b: manifest.b, tables };
}

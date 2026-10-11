import { join } from 'node:path';
import type { DuckDBConnection } from '@duckdb/node-api';
import { count, exec, literal, pgSchema, quote, table, transaction } from '#tools/postgres.ts';
import { createDuckDB } from '#tools/duckdb.ts';
import { getPolygonChainState } from '#supervisor/chain.ts';
import type { SupervisorConfig } from '#supervisor/config.ts';
import {
  assertFileSchema,
  type Manifest,
  MANIFEST_FILE,
  outputFolderPath,
  readManifest,
  sha256,
  since,
  snapshotEntities,
} from '#supervisor/snapshot.ts';
import {
  type CheckRow,
  checkValues,
  codec,
  diffCheckRows,
  type Entity,
  fromFileList,
  loadSchema,
  toPgList,
} from '#supervisor/typemap.ts';

/** A restore-point file of the snapshot, checked against its manifest. */
interface RestoreTable {
  entity: Entity;
  /** Path to `<Entity>.parquet`. */
  file: string;
  /** The staging table's name, unquoted and in pgSchema. */
  staging: string;
  /** Check values the manifest recorded for the file. */
  expected: CheckRow;
}

/**
 * Check the snapshot's restore-point files before touching Postgres: each is
 * listed in the manifest, and its columns and sha256 match.
 */
async function readSnapshot(
  conn: DuckDBConnection,
  manifest: Manifest,
  dir: string,
): Promise<RestoreTable[]> {
  const entities = snapshotEntities(loadSchema());
  const missing = entities.map((e) => `${e.name}.parquet`).filter((n) => !manifest.files[n]);
  if (missing.length) throw new Error(`${dir}: no ${missing.join(', ')} in its manifest`);

  const tables: RestoreTable[] = [];
  for (const entity of entities) {
    const name = `${entity.name}.parquet`;
    const file = join(dir, name);
    await assertFileSchema(conn, file, entity);
    if ((await sha256(file)) !== manifest.files[name].sha256)
      throw new Error(`${name}: sha256 differs from the manifest`);
    tables.push({
      entity,
      file,
      staging: `_restore_${entity.name}`,
      expected: manifest.files[name].checkValues,
    });
  }
  return tables;
}

/** Refuse anything but a fresh, never-run schema with all its tables empty. */
async function assertFreshSchema(): Promise<void> {
  const chain = await getPolygonChainState();
  if (!chain) throw new Error(`${pgSchema} has no envio_chains; create the schema first`);
  if (chain.progressBlock >= chain.startBlock)
    throw new Error(
      `envio_chains.progress_block is ${chain.progressBlock}: this schema has already indexed`,
    );
  for (const entity of loadSchema()) {
    const cnt = await count(`SELECT count(*) FROM (SELECT 1 FROM ${table(entity.name)} LIMIT 1) t`);
    if (cnt > 0) throw new Error(`${pgSchema}.${entity.name} is not empty`);
  }
}

/**
 * Copy each file into its own UNLOGGED staging table, numbers as text. DuckDB
 * writes on its own connection, so this runs outside the load transaction.
 */
async function stageTables(conn: DuckDBConnection, tables: RestoreTable[]): Promise<void> {
  for (const t of tables) {
    const start = Date.now();
    const columns = t.entity.columns.map((c) => `${quote(c.name)} ${codec(c).stagingType}`);
    await exec(
      `DROP TABLE IF EXISTS ${table(t.staging)};
       CREATE UNLOGGED TABLE ${table(t.staging)} (${columns.join(', ')})`,
    );
    await conn.run('CALL pg_clear_cache()');
    await conn.run(
      `INSERT INTO pg.${quote(pgSchema)}.${quote(t.staging)}
       SELECT ${fromFileList(t.entity)}
       FROM read_parquet(${literal(t.file)})`,
    );
    console.log(
      `restore: create staging table ${t.staging} for ${t.entity.name} successfully (${since(start)})`,
    );
  }
}

/**
 * Load every staging table into its target and check the values, all in one
 * transaction: a failure rolls every table back.
 */
async function loadTables(tables: RestoreTable[]): Promise<void> {
  return transaction(async (client) => {
    for (const t of tables.values()) {
      const start = Date.now();
      const cols = t.entity.columns.map((c) => quote(c.name)).join(', ');
      await client.query(
        `INSERT INTO ${table(t.entity.name)} (${cols})
         SELECT ${toPgList(t.entity)}
         FROM ${table(t.staging)}
         ORDER BY id`,
      );
      const checks = checkValues(t.entity);
      const { rows } = await client.query<CheckRow>(
        `SELECT ${checks.map((c) => `${c.pgValue} AS ${quote(c.name)}`).join(', ')}
         FROM ${table(t.entity.name)}`,
      );
      const diffs = diffCheckRows('postgres', rows[0], 'parquet', t.expected);
      if (diffs.length)
        throw new Error(
          `${t.entity.name}: check values differ after restore\n  ${diffs.join('\n  ')}`,
        );
      const rowCount = Number(rows[0].rows);
      console.log(
        `restore: ${t.entity.name} loaded and verified, ${rowCount} rows (${since(start)})`,
      );
    }
  });
}

/**
 * Restore the snapshot at `endBlockId` into the schema ENVIO_PG_* points at.
 * The caller must hold the supervisor lock: nothing else may write the schema.
 *
 * @param config - Output directory and DuckDB limits.
 * @param endBlockId - The snapshot's block; only its restore-point files are loaded.
 * @throws Error if the schema or the snapshot fails a check, or the restored
 *   values differ; nothing is kept then.
 */
export async function restoreAt(config: SupervisorConfig, endBlockId: number): Promise<void> {
  const dir = outputFolderPath(config.outputDir, endBlockId);
  const manifest = readManifest(dir);
  if (!manifest) throw new Error(`invalid ${dir}: ${MANIFEST_FILE} has not been found`);

  await assertFreshSchema();
  using db = await createDuckDB(config, false);
  const { conn } = db;
  const tables = await readSnapshot(conn, manifest, dir);
  console.log(`restore: block ${endBlockId} into ${pgSchema}, checks passed`);

  try {
    await stageTables(conn, tables);
    await loadTables(tables);
  } catch (e) {
    await exec(`TRUNCATE ${tables.map((t) => table(t.entity.name)).join(', ')}`).catch(
      () => undefined,
    );
    throw e;
  } finally {
    for (const t of tables)
      await exec(`DROP TABLE IF EXISTS ${table(t.staging)}`).catch(() => undefined);
  }
}

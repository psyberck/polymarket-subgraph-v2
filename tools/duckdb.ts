import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { sep } from 'node:path';
import { type DuckDBConnection, DuckDBInstance } from '@duckdb/node-api';
import { literal, pgSettings } from '#tools/postgres.ts';

export interface DuckDBConfig {
  /** DuckDB memory limit, e.g. 2GB. */
  memoryLimit: string;
  /** DuckDB worker threads. */
  threads: number;
}

/** A DuckDB connection with the indexer's Postgres attached as `pg`; disposing closes both. */
export interface DuckDB extends Disposable {
  conn: DuckDBConnection;
}

/**
 * Open an in-memory DuckDB with the indexer's Postgres attached as `pg`.
 *
 * @param config - DuckDB memory limit and threads.
 * @param readOnly - Attach Postgres read-only (default); false to write, as restore does.
 * @returns The connection; dispose it (`using`) to close it, its instance and
 *   its spill folder.
 * @throws Error if the postgres extension cannot be installed or loaded, or
 *   Postgres refuses.
 */
export async function createDuckDB(
  config: DuckDBConfig,
  readOnly: boolean = true,
): Promise<DuckDB> {
  const folderPath = mkdtempSync(tmpdir() + sep);
  let instance: DuckDBInstance | undefined;
  let conn: DuckDBConnection | undefined;

  const close = () => {
    conn?.closeSync();
    instance?.closeSync();
    rmSync(folderPath, { recursive: true, force: true });
  };
  try {
    instance = await DuckDBInstance.create(':memory:', {
      memory_limit: config.memoryLimit,
      threads: String(config.threads),
      temp_directory: folderPath,
      autoinstall_known_extensions: 'false',
    });
    conn = await instance.connect();
    await conn.run('INSTALL postgres; LOAD postgres;').catch((e: unknown) => {
      throw new Error('fail to install or load the DuckDB postgres extension', {
        cause: e,
      });
    });
    const s = pgSettings;
    await conn.run(
      `CREATE TEMPORARY SECRET pg (TYPE postgres, HOST ${literal(s.host)}, PORT ${s.port}, ` +
        `DATABASE ${literal(s.database)}, USER ${literal(s.user)}, PASSWORD ${literal(s.password)})`,
    );
    const options = s.sslMode ? `sslmode=${s.sslMode}` : '';
    await conn.run(
      `ATTACH ${literal(options)} AS pg (TYPE postgres, SECRET pg${readOnly ? ', READ_ONLY' : ''})`,
    );
    return { conn, [Symbol.dispose]: close };
  } catch (e) {
    close();
    throw e;
  }
}

/**
 * Wrap Postgres SQL as a DuckDB table function over the attached database.
 *
 * @param sql - Postgres SQL, run as is; it is dollar-quoted.
 * @returns A `postgres_query('pg', ...)` expression to select from.
 */
export function pgQuery(sql: string): string {
  return `postgres_query('pg', $$${sql}$$)`;
}

/**
 * Run a DuckDB query and read every row.
 *
 * @param conn - The DuckDB connection.
 * @param sql - DuckDB SQL.
 * @returns Each row as an object; big numbers come back as text.
 */
export async function queryRows(
  conn: DuckDBConnection,
  sql: string,
): Promise<Record<string, unknown>[]> {
  return (await conn.runAndReadAll(sql)).getRowObjectsJson();
}

import { type ClientConfig, Pool, type PoolClient } from 'pg';

/** Connection settings, read the same way Envio reads them (`ENVIO_PG_*`). */
export const pgSettings = {
  host: process.env.ENVIO_PG_HOST ?? 'localhost',
  port: Number(process.env.ENVIO_PG_PORT ?? 5433),
  user: process.env.ENVIO_PG_USER ?? 'postgres',
  database: process.env.ENVIO_PG_DATABASE ?? 'envio-dev',
  password: process.env.ENVIO_PG_PASSWORD ?? 'testing',
  sslMode: process.env.ENVIO_PG_HOST ? (process.env.ENVIO_PG_SSL_MODE ?? 'require') : undefined,
};

/** The schema Envio writes its tables to. */
export const pgSchema = process.env.ENVIO_PG_SCHEMA ?? 'public';

/** Quote an SQL identifier (both dialects accept double quotes). */
export const quote = (name: string): string => `"${name.replace(/"/g, '""')}"`;

/** Quote an SQL string literal (both dialects). */
export const literal = (value: string): string => `'${value.replace(/'/g, "''")}'`;

/** A table in the schema Envio writes to (ENVIO_PG_SCHEMA). */
export const table = (name: string): string => `${quote(pgSchema)}.${quote(name)}`;

/** pg connection options from pgSettings, for the pool and any dedicated client. */
export const pgConfig: ClientConfig = {
  host: pgSettings.host,
  port: pgSettings.port,
  user: pgSettings.user,
  database: pgSettings.database,
  password: pgSettings.password,
  ssl: pgSettings.sslMode
    ? { rejectUnauthorized: pgSettings.sslMode === 'verify-full' }
    : undefined,
  connectionTimeoutMillis: 15_000,
};

export const pool = new Pool({ ...pgConfig, max: 4 });

/**
 * Run `fn` in one transaction on its own pooled connection: COMMIT if it
 * returns, ROLLBACK if it throws, and the error is rethrown.
 *
 * @param fn - every statement of the transaction, run through `client`.
 */
export async function transaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  let err: Error | undefined;
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (e) {
    await client.query('ROLLBACK').catch((r: unknown) => {
      err = r instanceof Error ? r : new Error(String(r));
    });
    throw e;
  } finally {
    client.release(err);
  }
}

/**
 * Run a statement, discarding any rows it returns.
 *
 * @param sql - the statement.
 * @param params - bound parameters; never interpolated into `sql`.
 * @returns How many rows it affected (0 when it reports none).
 */
export async function exec(sql: string, params: readonly unknown[] = []): Promise<number> {
  return (await pool.query(sql, params as unknown[])).rowCount ?? 0;
}

/**
 * One number out of Postgres.
 *
 * @param sql - a query whose first column of its first row is a count.
 * @param params - bound parameters; never interpolated into `sql`.
 * @returns The value, or 0 when the query returned no rows.
 */
export async function count(sql: string, params: readonly unknown[] = []): Promise<number> {
  const res = await pool.query<Record<string, string>>(sql, params as unknown[]);
  const row = res.rows[0];
  return row ? Number(Object.values(row)[0] ?? 0) || 0 : 0;
}

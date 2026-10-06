import { Pool } from 'pg';

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

export const pool = new Pool({
  host: pgSettings.host,
  port: pgSettings.port,
  user: pgSettings.user,
  database: pgSettings.database,
  password: pgSettings.password,
  ssl: pgSettings.sslMode
    ? { rejectUnauthorized: pgSettings.sslMode === 'verify-full' }
    : undefined,
  connectionTimeoutMillis: 15_000,
  max: 4,
});

/**
 * Run a statement, discarding any result.
 *
 * @param sql - the statement.
 * @param params - bound parameters; never interpolated into `sql`.
 */
export async function exec(sql: string, params: readonly unknown[] = []): Promise<void> {
  await pool.query(sql, params as unknown[]);
}

/**
 * One number out of Postgres.
 *
 * @param sql - a query whose first column of its first row is a count.
 * @returns The value, or 0 when the query returned no rows.
 */
export async function count(sql: string): Promise<number> {
  const res = await pool.query<Record<string, string>>(sql);
  const row = res.rows[0];
  return row ? Number(Object.values(row)[0] ?? 0) || 0 : 0;
}

import { Pool } from 'pg';

export const pool = new Pool({
  host: process.env.ENVIO_PG_HOST ?? 'localhost',
  port: Number(process.env.ENVIO_PG_PORT ?? 5433),
  user: process.env.ENVIO_PG_USER ?? 'postgres',
  database: process.env.ENVIO_PG_DATABASE ?? 'envio-dev',
  password: process.env.ENVIO_PG_PASSWORD ?? 'testing',
  ssl: process.env.ENVIO_PG_HOST
    ? { rejectUnauthorized: (process.env.ENVIO_PG_SSL_MODE ?? 'require') === 'verify-full' }
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

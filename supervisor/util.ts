/**
 * Small helpers every supervisor module uses: log lines and SQL quoting.
 */

/**
 * Timestamped log lines on stdout, which run_indexer.sh sends to logs/run.log
 * alongside Envio's own output. Prefixed so the two can be told apart.
 */
export function log(message: string): void {
  process.stdout.write(`${new Date().toISOString()} [supervisor] ${message}\n`);
}

/** Seconds since `start` (a Date.now() value), for step timings. */
export const since = (start: number): string => `${((Date.now() - start) / 1000).toFixed(1)}s`;

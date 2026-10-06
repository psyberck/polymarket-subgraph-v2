import { parseArgs } from 'node:util';

const SCHEDULES = ['monthly', 'weekly', 'stride'] as const;
export type Schedule = (typeof SCHEDULES)[number];

export interface SupervisorConfig {
  /** null: no scheduled snapshots. */
  schedule: Schedule | null;
  /** Blocks between snapshots; set only for the stride schedule. */
  stride: number | null;
  /** Archive, then delete, closed rows and old trades. */
  prune: boolean;
  /** With prune: log what would be deleted, write and delete nothing. */
  debug: boolean;
  /** Where snapshot and trade files are written; absolute. */
  outputDir: string;
  /** DuckDB memory limit, e.g. 2GB. */
  memoryLimit: string;
  /** DuckDB worker threads. */
  threads: number;
}

const isSchedule = (value: string): value is Schedule =>
  (SCHEDULES as readonly string[]).includes(value);

/**
 * A positive integer from a flag or variable.
 */
function parsePositiveInt(name: string, value: string): number {
  const n = Number(value);
  if (!/^[1-9]\d*$/.test(value) || !Number.isSafeInteger(n))
    throw new Error(`${name}: expected a positive integer, got "${value}"`);
  return n;
}

/**
 * Parse the supervisor's flags and environment.
 *
 * @param args - command-line arguments, without node and the script path.
 * @param env - environment; defaults to process.env.
 * @returns The settings; throws with the offending flag or variable named.
 */
export function parseConfig(
  args: string[],
  env: NodeJS.ProcessEnv = process.env,
): SupervisorConfig {
  const { values } = parseArgs({
    args,
    options: {
      schedule: { type: 'string' },
      stride: { type: 'string' },
      prune: { type: 'boolean', default: false },
      debug: { type: 'boolean', default: false },
      'output-dir': { type: 'string', default: '' },
    },
  });

  if (values.schedule !== undefined && !isSchedule(values.schedule))
    throw new Error(`--schedule: expected ${SCHEDULES.join(' | ')}, got "${values.schedule}"`);
  const schedule = values.schedule ?? null;
  if (schedule === 'stride' && values.stride === undefined)
    throw new Error('--schedule stride needs --stride <blocks>');
  if (schedule !== 'stride' && values.stride !== undefined)
    throw new Error('--stride is only valid with --schedule stride');
  if (values.debug && !values.prune) throw new Error('--debug is only valid with --prune');

  const memoryLimit = env.DUCKDB_MEMORY_LIMIT || '2GB';
  if (!/^\d+(\.\d+)?\s*(KB|MB|GB|TB|KiB|MiB|GiB|TiB)$/i.test(memoryLimit))
    throw new Error(`DUCKDB_MEMORY_LIMIT: expected a size such as 2GB, got "${memoryLimit}"`);

  const outputDir = values['output-dir'];
  if (!outputDir) throw new Error('--output-dir: expected a path');

  return {
    schedule,
    stride: values.stride === undefined ? null : parsePositiveInt('--stride', values.stride),
    prune: values.prune,
    debug: values.debug,
    outputDir,
    memoryLimit,
    threads: env.DUCKDB_THREADS ? parsePositiveInt('DUCKDB_THREADS', env.DUCKDB_THREADS) : 2,
  };
}

import { parseArgs } from 'node:util';
import { POLYGON_BLOCK_SECONDS } from '#supervisor/chain.ts';
import type { DuckDBConfig } from '#tools/duckdb.ts';

const SCHEDULES = ['monthly', 'weekly', 'stride'] as const;
export type Schedule = (typeof SCHEDULES)[number];

/** Seconds in a day. */
const DAY = 86_400;
/** Blocks in a schedule's period, at Polygon's average block time. */
const PERIOD_STRIDES = {
  weekly: Math.round((7 * DAY) / POLYGON_BLOCK_SECONDS),
  monthly: Math.round((30 * DAY) / POLYGON_BLOCK_SECONDS),
} as const;

export interface SupervisorConfig extends DuckDBConfig {
  /** null: no scheduled snapshots. */
  schedule: Schedule | null;
  /** Blocks between snapshots; set whenever a schedule is, null otherwise. */
  stride: number | null;
  /** Archive, then delete, closed rows and old trades. */
  prune: boolean;
  /** With prune: log what would be deleted, write and delete nothing. */
  debug: boolean;
  /** Where snapshot and trade files are written; absolute. */
  outputDir: string;
}

export class InvalidValueError extends Error {}

export const USAGE = `usage: run_indexer.sh --output-dir <path> [--schedule monthly|weekly|stride [--stride <blocks>]]
                      [--prune [--debug]]
       env: DUCKDB_MEMORY_LIMIT (default 2GB), DUCKDB_THREADS (default 2)`;

const isSchedule = (value: string): value is Schedule =>
  (SCHEDULES as readonly string[]).includes(value);

/**
 * A positive integer from a flag or variable.
 */
function parsePositiveInt(name: string, value: string): number {
  const n = Number(value);
  if (!/^[1-9]\d*$/.test(value) || !Number.isSafeInteger(n))
    throw new InvalidValueError(`${name}: expected a positive integer, got "${value}"`);
  return n;
}

/**
 * Parse the supervisor's flags and environment.
 *
 * @param args - command-line arguments, without node and the script path.
 * @param env - environment; defaults to process.env.
 * @returns The settings.
 * @throws InvalidValueError naming the offending flag or variable.
 */
export function parseConfig(
  args: string[],
  env: NodeJS.ProcessEnv = process.env,
): SupervisorConfig {
  let values;
  try {
    ({ values } = parseArgs({
      args,
      options: {
        schedule: { type: 'string' },
        stride: { type: 'string' },
        prune: { type: 'boolean', default: false },
        debug: { type: 'boolean', default: false },
        'output-dir': { type: 'string', default: '' },
      },
    }));
  } catch (e) {
    throw new InvalidValueError((e as Error).message, { cause: e });
  }

  if (values.schedule !== undefined && !isSchedule(values.schedule))
    throw new InvalidValueError(
      `--schedule: expected ${SCHEDULES.join(' | ')}, got "${values.schedule}"`,
    );
  const schedule = values.schedule ?? null;
  if (schedule === 'stride' && values.stride === undefined)
    throw new InvalidValueError('--schedule stride needs --stride <blocks>');
  if (schedule !== 'stride' && values.stride !== undefined)
    throw new InvalidValueError('--stride is only valid with --schedule stride');
  if (values.debug && !values.prune)
    throw new InvalidValueError('--debug is only valid with --prune');

  const outputDir = values['output-dir'];
  if (!outputDir) throw new InvalidValueError('--output-dir: expected a output folder path');

  const memoryLimit = env.DUCKDB_MEMORY_LIMIT || '2GB';
  if (!/^\d+(\.\d+)?\s*(KB|MB|GB|TB|KiB|MiB|GiB|TiB)$/i.test(memoryLimit))
    throw new InvalidValueError(
      `DUCKDB_MEMORY_LIMIT: expected a memory size such as 2GB, got "${memoryLimit}"`,
    );
  const threads = env.DUCKDB_THREADS ? parsePositiveInt('DUCKDB_THREADS', env.DUCKDB_THREADS) : 2;

  return {
    threads,
    schedule,
    outputDir,
    memoryLimit,
    prune: values.prune,
    debug: values.debug,
    stride:
      schedule === null
        ? null
        : schedule === 'stride'
          ? parsePositiveInt('--stride', values.stride!)
          : PERIOD_STRIDES[schedule],
  };
}

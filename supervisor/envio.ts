import { spawn } from 'node:child_process';
import { existsSync, mkdirSync, readdirSync, readFileSync, readlinkSync, rmSync, writeFileSync, } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { pgSchema, pool, quote } from '#tools/postgres.ts';
import { log } from '#supervisor/util.ts';

/** Envio versions the end_block patch has been validated on (§8.2–8.4). */
export const ENVIO_VERSIONS: readonly string[] = ['3.6.1'];

const INDEXER_DIR = new URL('..', import.meta.url).pathname;
const ENVIO_PID_DIR = join(homedir(), '.envio');
const pidFile = (pid: number): string => join(ENVIO_PID_DIR, String(pid));
const STOP_TIMEOUT_MS = 120_000;

const table = (name: string): string => `${quote(pgSchema)}.${quote(name)}`;

export interface ChainState {
  startBlock: number;
  endBlock: number | null;
  progressBlock: number;
  sourceBlock: number;
  maxReorgDepth: number;
}

/** envio_chains, or null for a schema Envio has not initialized yet. */
export async function getChainState(): Promise<ChainState | null> {
  const exists = await pool.query(`SELECT to_regclass($1) AS t`, [`${pgSchema}.envio_chains`]);
  if (!(exists.rows[0] as { t: string | null }).t) return null;
  const { rows } = await pool.query<{
    start_block: number;
    end_block: number | null;
    progress_block: number;
    source_block: number;
    max_reorg_depth: number;
  }>(
    `SELECT start_block, end_block, progress_block, source_block, max_reorg_depth FROM ${table('envio_chains')}`,
  );
  if (rows.length !== 1) throw new Error(`envio_chains has ${rows.length} rows, expected 1`);
  const r = rows[0];
  return {
    startBlock: r.start_block,
    endBlock: r.end_block,
    progressBlock: r.progress_block,
    sourceBlock: r.source_block,
    maxReorgDepth: r.max_reorg_depth,
  };
}

/**
 * Set end_block for the next start (§3.3), in envio_chains and in the config
 * Envio compares on resume, in one transaction. Only while Envio is stopped,
 * only on an allow-listed Envio version, and null removes the limit.
 */
export async function patchEndBlock(endBlock: number | null): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query<{ version: string | null }>(
      `SELECT config::jsonb->>'version' AS version FROM ${table('envio_info')} WHERE id = 1`,
    );
    const version = rows[0]?.version ?? null;
    if (!version || !ENVIO_VERSIONS.includes(version))
      throw new Error(
        `Envio ${version} is not on the allow-list (${ENVIO_VERSIONS.join(', ')}); re-validate the end_block patch (§8.2–8.4) first`,
      );
    await client.query(`UPDATE ${table('envio_chains')} SET end_block = $1 WHERE id = 137`, [
      endBlock,
    ]);
    await client.query(
      endBlock === null
        ? `UPDATE ${table('envio_info')} SET config = (config::jsonb #- '{evm,chains,polygon,endBlock}')::text WHERE id = 1`
        : `UPDATE ${table('envio_info')} SET config = jsonb_set(config::jsonb, '{evm,chains,polygon,endBlock}', to_jsonb($1::int))::text WHERE id = 1`,
      endBlock === null ? [] : [endBlock],
    );
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw e;
  } finally {
    client.release();
  }
}

const alive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

async function waitForExit(pid: number, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (alive(pid)) {
    if (Date.now() > deadline) return false;
    await new Promise((r) => setTimeout(r, 500));
  }
  return true;
}

/** SIGTERM the process group, then SIGKILL if it outlives the timeout. */
async function stopGroup(pid: number): Promise<void> {
  try {
    process.kill(-pid, 'SIGTERM');
  } catch {
    return; // already gone
  }
  if (!(await waitForExit(pid, STOP_TIMEOUT_MS))) {
    log(`envio: pid ${pid} ignored SIGTERM for ${STOP_TIMEOUT_MS / 1000}s; SIGKILL`);
    try {
      process.kill(-pid, 'SIGKILL');
    } catch {
      /* gone */
    }
    await waitForExit(pid, 10_000);
  }
}

/** Whether `pid` is a live Envio started from this indexer's directory. */
function ownEnvio(pid: number): boolean {
  try {
    const cmdline = readFileSync(`/proc/${pid}/cmdline`, 'utf8').replace(/\0/g, ' ');
    const cwd = readlinkSync(`/proc/${pid}/cwd`);
    return cmdline.includes('envio/bin.mjs') && resolve(cwd) === resolve(INDEXER_DIR);
  } catch {
    return false; // not running
  }
}

/**
 * Stop an Envio left behind by a supervisor that was killed (§3.1): every
 * file in ~/.envio whose process is alive, really is Envio, and runs from
 * this indexer. Files of dead pids are removed; a live process that is not
 * ours (another checkout, or a reused pid) is left alone, file included.
 */
export async function stopLeftoverEnvio(): Promise<void> {
  if (!existsSync(ENVIO_PID_DIR)) return;
  for (const name of readdirSync(ENVIO_PID_DIR)) {
    const pid = Number(name);
    if (!Number.isSafeInteger(pid) || pid <= 0) continue;
    if (ownEnvio(pid)) {
      log(`envio: stopping leftover Envio, pid ${pid}`);
      await stopGroup(pid);
    } else if (alive(pid)) {
      continue;
    }
    rmSync(pidFile(pid), { force: true });
  }
}

export interface EnvioRun {
  pid: number;
  /** Resolves with the exit code (null when killed by a signal). */
  exited: Promise<number | null>;
  stop(): Promise<void>;
}

/**
 * Create Envio's schema in a database that has none (start_block from the
 * config), so the loop has a position to plan from. Only for a missing schema:
 * on an existing one `db-migrate up` runs Envio's resume check, which fails
 * whenever the stored end_block differs from the unset one, i.e. after any
 * snapshot pause (measured 2026-10-06; the data is never touched).
 */
export async function createSchema(): Promise<void> {
  const s = await getChainState();
  if (s) return;
  const tsx = process.env.TSX_CLI;
  if (!tsx || !existsSync(tsx)) throw new Error('TSX_CLI is not set or missing');
  const child = spawn(
    process.execPath,
    [tsx, 'node_modules/envio/bin.mjs', 'local', 'db-migrate', 'up'],
    {
      cwd: INDEXER_DIR,
      stdio: ['ignore', 'inherit', 'inherit'],
      env: { ...process.env, ENVIO_TUI: 'false' },
    },
  );
  const code = await new Promise<number | null>((done) => child.once('exit', done));
  if (code !== 0) throw new Error(`envio local db-migrate up exited with ${code}`);
}

/**
 * Start Envio toward `endBlock` (null: no limit). Its output goes to the
 * supervisor's own stdout, i.e. logs/run.log.
 *
 * @param endBlock - extra environment, e.g. ENVIO_START_BLOCK for a fresh schema.
 */
export function startEnvio(endBlock: number | null): EnvioRun {
  const tsx = process.env.TSX_CLI!;
  const child = spawn(process.execPath, [tsx, 'node_modules/envio/bin.mjs', 'start'], {
    cwd: INDEXER_DIR,
    detached: true,
    stdio: ['ignore', 'inherit', 'inherit'],
    env: {
      ...process.env,
      ENVIO_TUI: 'false',
      ENVIO_END_BLOCK: endBlock === null ? '' : String(endBlock),
    },
  });
  const pid = child.pid;
  if (pid === undefined) throw new Error('failed to start Envio');
  mkdirSync(ENVIO_PID_DIR, { recursive: true });
  writeFileSync(pidFile(pid), '');
  log(`envio: start, pid ${pid}, end_block ${endBlock ?? 'none'}`);
  const exited = new Promise<number | null>((done) =>
    child.once('exit', (code) => {
      rmSync(pidFile(pid), { force: true });
      done(code);
    }),
  );
  return { pid, exited, stop: () => stopGroup(pid) };
}

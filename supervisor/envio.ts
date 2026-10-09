import { type ChildProcess, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { pgConfig, pgSchema, table, transaction } from '#tools/postgres.ts';
import { Client } from 'pg';
import { getPolygonChainState } from '#supervisor/chain.ts';

const CHAIN_ID = 137;
const INDEXER_DIR = fileURLToPath(new URL('..', import.meta.url));
const INDEXER_PID_DIR = join(homedir(), '.envio');
const LOCK_QUERY_TIMEOUT_MS = 10_000;
const PG_UNDEFINED_OBJECT = '42704';

/** Indexer, its process or its schema is not in the state the supervisor expects. */
export class InternalError extends Error {}
/** The supervisor lock is held by another session, or no longer held by this one. */
export class LockHeldError extends InternalError {}

/** The pid file that records indexer process `pid`. */
const pidFile = (pid: number): string => join(INDEXER_PID_DIR, String(pid));

/** /proc/{pid}/stat, whose start time tells a process from a later pid reuse; Linux only. */
function readProcStat(pid: number): string | null {
  try {
    const stats = readFileSync(`/proc/${pid}/stat`, 'utf8');
    const fields = stats.slice(stats.lastIndexOf(')') + 2).split(' ');
    return fields[19];
  } catch {
    return null;
  }
}

/** Poll until process group `pid` is gone; false if it outlives `timeoutMs`. */
async function isExited(pid: number, timeoutMs: number): Promise<boolean> {
  const deadline = performance.now() + timeoutMs;
  for (;;) {
    try {
      process.kill(-pid, 0);
    } catch {
      return true;
    }
    if (performance.now() > deadline) return false;
    await sleep(500);
  }
}

class AdvisoryLock implements AsyncDisposable {
  readonly name: string;
  private readonly client: Client;
  private released = false;

  private constructor(name: string, client: Client) {
    this.name = name;
    this.client = client;
  }

  /**
   * Take a session-level advisory lock on a dedicated connection.
   *
   * @param name - Hashed into the lock key; also the connection's application_name.
   * @returns The held lock.
   * @throws LockHeldError if another session holds the lock.
   */
  static async acquire(name: string): Promise<AdvisoryLock> {
    const key = createHash('sha256').update(name).digest().readBigInt64BE(0).toString();
    const client = new Client({
      ...pgConfig,
      keepAlive: true,
      application_name: name,
      query_timeout: LOCK_QUERY_TIMEOUT_MS,
    });

    client.on('error', (e) => {
      console.log(`lock ${name}: ${e.message}`);
    });

    let ok: boolean;
    try {
      await client.connect();
      await client.query('SET idle_session_timeout = 0').catch((e: { code?: string }) => {
        if (e.code !== PG_UNDEFINED_OBJECT) throw e; // backward compatible
      });
      const { rows } = await client.query<{ ok: boolean }>(
        `SELECT pg_try_advisory_lock($1::bigint) AS ok`,
        [key],
      );
      ok = rows[0].ok;
    } catch (e) {
      await client.end().catch(() => undefined);
      throw e;
    }
    if (!ok) {
      await client.end().catch(() => undefined);
      throw new LockHeldError(`lock ${name} is held: key=${key}`);
    }

    console.log(`lock ${name} has been successfully acquired`);
    return new AdvisoryLock(name, client);
  }

  /**
   * Check that the lock is still held. The lock ends only with its session, and
   * the client never reconnects, so a round trip on its connection is proof.
   *
   * @throws LockHeldError if the lock was released or its connection is gone.
   */
  async assertHeld(): Promise<void> {
    if (this.released) throw new LockHeldError(`lock ${this.name} has been released`);
    try {
      await this.client.query('SELECT 1');
    } catch (e) {
      throw new LockHeldError(`lock ${this.name} is no longer held`, { cause: e });
    }
  }

  /** Release the lock by ending its connection; calling it again does nothing. */
  async release(): Promise<void> {
    if (this.released) return;
    this.released = true;
    await this.client.end().catch(() => undefined);
  }

  /** Same as {@link AdvisoryLock.release}. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.release();
  }
}

/**
 * Indexer process manager; holding one means holding the supervisor lock.
 * Every method checks the lock first and throws LockHeldError if it is gone.
 */
export class Indexer implements AsyncDisposable {
  private readonly al: AdvisoryLock;
  private pid: number | null = null;

  private constructor(al: AdvisoryLock) {
    this.al = al;
  }

  /**
   * Take the supervisor lock, then stop any indexer a killed supervisor left behind.
   *
   * @returns The session, holding the lock.
   * @throws LockHeldError if another supervisor holds the lock.
   */
  static async create(): Promise<Indexer> {
    const session = new Indexer(await AdvisoryLock.acquire(`Indexer:${pgSchema}`));
    try {
      await session.terminateOrphans();
    } catch (e) {
      await session.close();
      throw e;
    }
    return session;
  }

  /**
   * Create indexer's schema if the database has none. Never on an existing one:
   * there `db-migrate up` fails indexer's resume check after any snapshot pause.
   *
   * @throws InternalError if `db-migrate up` fails.
   */
  async createSchema(): Promise<void> {
    await this.al.assertHeld();
    const s = await getPolygonChainState();
    if (s) return;
    const child = spawn(process.execPath, this.envioArgs('local', 'db-migrate', 'up'), {
      cwd: INDEXER_DIR,
      stdio: ['ignore', 'inherit', 'inherit'],
      env: { ...process.env, ENVIO_TUI: 'false' },
    });
    const c = await this.wait(child, 'db-migrate');
    if (c !== 0) throw new InternalError(`envio local db-migrate up exited with ${c}`);
  }

  /**
   * Set the end block for the next start, in envio_chains and in the
   * config Envio checks on resume, in one transaction.
   *
   * @param endBlock - The block to stop at, or null for no limit.
   * @throws InternalError if indexer is running, or a row to update is missing.
   */
  async patchEndBlock(endBlock: number | null): Promise<void> {
    await this.al.assertHeld();
    if (this.pid !== null)
      throw new InternalError(`patchEndBlock: indexer is running at pid ${this.pid}`);
    await transaction(async (client) => {
      const chains = await client.query(
        `UPDATE ${table('envio_chains')} SET end_block = $1 WHERE id = $2`,
        [endBlock, CHAIN_ID],
      );
      const info = await client.query(
        endBlock === null
          ? `UPDATE ${table('envio_info')} SET config = (config::jsonb #- '{evm,chains,polygon,endBlock}')::text WHERE id = 1`
          : `UPDATE ${table('envio_info')} SET config = jsonb_set(config::jsonb, '{evm,chains,polygon,endBlock}', to_jsonb($1::int))::text WHERE id = 1`,
        endBlock === null ? [] : [endBlock],
      );
      if (chains.rowCount !== 1)
        throw new InternalError(`envio_chains has no row for chain ${CHAIN_ID}`);
      if (info.rowCount !== 1) throw new InternalError('envio_info has no row with id 1');
    });
  }

  /**
   * Run indexer until it exits. However this ends, indexer's whole process group is
   * stopped and its pid file removed before it settles.
   *
   * @param targetEndBlock - The block to stop at, or null for no limit.
   * @param signal - Aborting it stops indexer.
   * @returns Indexer's exit code, or null if a signal killed it.
   * @throws AbortError if `signal` is aborted.
   * @throws InternalError if indexer is already running, or fails to start.
   */
  async start(targetEndBlock: number | null, signal: AbortSignal): Promise<number | null> {
    await this.al.assertHeld();
    if (this.pid !== null)
      throw new InternalError(`start: indexer is already running at pid ${this.pid}`);
    signal.throwIfAborted();

    const child = spawn(process.execPath, this.envioArgs('start'), {
      cwd: INDEXER_DIR,
      detached: true,
      stdio: ['ignore', 'inherit', 'inherit'],
      env: {
        ...process.env,
        ENVIO_TUI: 'false',
        ENVIO_END_BLOCK: targetEndBlock === null ? '' : String(targetEndBlock),
      },
    });
    child.on('error', (e) => console.log(`indexer error: ${e.message}`));
    const pid = child.pid;
    if (pid === undefined) throw new InternalError('failed to start the indexer');
    this.pid = pid;

    try {
      mkdirSync(INDEXER_PID_DIR, { recursive: true });
      const contents = readProcStat(pid);
      if (!contents) throw new InternalError(`indexer ${pid}: /proc/${pid}/stat not found`);
      writeFileSync(pidFile(pid), contents);
      console.log(`indexer: started, pid ${pid}, end block ${targetEndBlock ?? 'none'}`);
      return await this.wait(child, `pid ${pid}`, signal);
    } finally {
      await this.terminate(pid);
      this.pid = null;
    }
  }

  /** Stop any indexer still running, then release the lock; calling it again does nothing. */
  async close(): Promise<void> {
    if (this.pid !== null) await this.terminate(this.pid);
    await this.al.release();
  }

  /** Wait for `child` to exit and return its code; aborting `signal` rejects. */
  private async wait(
    child: ChildProcess,
    name: string,
    signal?: AbortSignal,
  ): Promise<number | null> {
    const [code, killedBy] = (await once(child, 'exit', { signal })) as [
      number | null,
      NodeJS.Signals | null,
    ];
    if (killedBy) console.log(`envio: ${name} killed by ${killedBy}`);
    return code;
  }

  /** The node arguments that run the Envio CLI with `args` under tsx. */
  private envioArgs(...args: string[]): string[] {
    const tsx = process.env.TSX_CLI;
    if (!tsx || !existsSync(tsx)) throw new Error('TSX_CLI is not set or missing');
    return [tsx, 'node_modules/envio/bin.mjs', ...args];
  }

  /** Stop process group `pid` (SIGTERM, then SIGKILL) and remove its pid file. */
  private async terminate(pid: number, timeoutMs = 120_000): Promise<void> {
    for (const sig of ['SIGTERM', 'SIGKILL'] as const) {
      try {
        process.kill(-pid, sig);
      } catch {
        break;
      }
      if (await isExited(pid, timeoutMs)) break;
      if (sig === 'SIGKILL')
        throw new InternalError(`indexer: process<${pid}> is still alive after SIGKILL`);
    }
    rmSync(pidFile(pid), { force: true });
  }

  /** Stop each indexer left behind, as named by its pid file. */
  private async terminateOrphans(): Promise<void> {
    await this.al.assertHeld();
    if (!existsSync(INDEXER_PID_DIR)) return;
    for (const filename of readdirSync(INDEXER_PID_DIR)) {
      const path = join(INDEXER_PID_DIR, filename);
      if (/^[1-9]\d*$/.test(filename)) {
        const pid = Number(filename);
        const stat = readProcStat(pid);
        if (stat === readFileSync(path, 'utf8').trim()) {
          console.log(`indexer: stopping orphan, pid ${pid}`);
          await this.terminate(pid);
        }
      }
      rmSync(path, { force: true });
    }
  }

  /** Same as {@link Indexer.close}. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }
}

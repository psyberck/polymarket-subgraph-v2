import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { pgSchema, pool } from '#tools/postgres.ts';
import {
  exportUnits,
  hasSnapshotAt,
  hasTradesFile,
  openDuckDB,
  previousSnapshotB,
  removeUnverified,
  snapshotDir,
  snapshotUnits,
  tradesPath,
  tradesUnit,
  verifyAndPublish,
} from '#supervisor/archive.ts';
import {
  BLOCK_SECONDS,
  checkHashes,
  createRpc,
  HashMismatchError,
  nextBoundary,
  type Rpc,
  waitFinal,
} from '#supervisor/chain.ts';
import {
  type ChainState,
  createSchema,
  type EnvioRun,
  getChainState,
  patchEndBlock,
  startEnvio,
  stopLeftoverEnvio,
} from '#supervisor/envio.ts';
import { pruneCycle } from '#supervisor/prune.ts';
import { loadSchema } from '#supervisor/typemap.ts';
import { InvalidValueError, parseConfig, type SupervisorConfig, USAGE, } from '#supervisor/config.ts';
import { log, quote, since } from '#supervisor/util.ts';

/** How far short of an estimated future boundary Envio stops: 10 minutes (§3.2). */
const FUTURE_MARGIN_BLOCKS = Math.ceil(600 / BLOCK_SECONDS);

/**
 * Set once shutdown begins. Only two places need it: shutdown itself, against
 * running twice, and the main loop, which must not restart Envio when the
 * Envio that just exited was stopped by shutdown. Everything else is cut short
 * by shutdown's process.exit.
 */
const stopping = false;
let envio: EnvioRun | null = null;

/** The schedule as manifests name it: monthly, weekly or stride:<N>. */
const scheduleLabel = (config: SupervisorConfig): string =>
  config.schedule === 'stride' ? `stride:${config.stride}` : String(config.schedule);

/**
 * Pause point reached: finality, hash check, SplitMarker clear, then the trade
 * file and the snapshot. The snapshot manifest is written last, so its
 * existence means the whole cycle is done.
 */
async function snapshotCycle(
  config: SupervisorConfig,
  rpc: Rpc,
  chain: ChainState,
  b: number,
  id: string,
): Promise<void> {
  const started = Date.now();
  log(`cycle: snapshot ${id} at ${b}`);
  const final = await waitFinal(rpc, b, chain.maxReorgDepth);
  const matched = await checkHashes(rpc, b, chain);
  log(`cycle: ${b} final by ${final.by}; ${matched} checkpoints match the chain`);

  // Dead intra-transaction scratch rows: every transaction up to B is done (§3.8).
  const cleared = await pool.query(`DELETE FROM ${quote(pgSchema)}.${quote('SplitMarker')}`);
  log(`cycle: cleared ${cleared.rowCount} SplitMarker rows`);

  const entities = loadSchema();
  const bPrev = previousSnapshotB(config.outputDir, b, chain.startBlock - 1);
  const blockHash = (await rpc.blockHashes([b])).get(b) ?? null;
  const blockTimestamp = await rpc.timestamp(b);
  const header = {
    schedule: scheduleLabel(config),
    bPrev,
    blockHash,
    blockTimestamp,
  };
  const pinned = (p: number) => (p === b ? null : `expected ${b}`);
  const conn = await openDuckDB(config);
  try {
    if (bPrev < b && !hasTradesFile(config.outputDir, bPrev, b)) {
      const trades = await exportUnits(conn, [tradesUnit(config.outputDir, entities, bPrev, b)], {
        expectBlock: pinned,
      });
      await verifyAndPublish(
        conn,
        trades,
        `${tradesPath(config.outputDir, bPrev, b)}.manifest.json`,
        {
          ...header,
          kind: 'trades',
          id: `b${bPrev + 1}-b${b}`,
        },
      );
    }
    const snapshot = await exportUnits(conn, snapshotUnits(config.outputDir, entities, id), {
      expectBlock: pinned,
    });
    await verifyAndPublish(
      conn,
      snapshot,
      join(snapshotDir(config.outputDir, id), 'manifest.json'),
      {
        ...header,
        kind: 'snapshot',
        id,
      },
    );
  } finally {
    conn.closeSync();
  }
  log(`cycle: snapshot ${id} done (${since(started)})`);
}

/** Run Envio to `end`; returns its exit code. */
async function runEnvio(end: number | null, chain: ChainState): Promise<number | null> {
  if (chain.endBlock !== end) await patchEndBlock(end);
  envio = startEnvio(end);
  const code = await envio.exited;
  envio = null;
  return code;
}

/** One pass: snapshot if paused at a boundary, then run Envio to the next. */
async function pass(config: SupervisorConfig, rpc: Rpc): Promise<number | null | 'waited'> {
  const chain = (await getChainState()) as ChainState;
  const progress = Math.max(chain.progressBlock, chain.startBlock - 1);

  // Paused exactly at a boundary: take its snapshot if missing, then prune.
  // Both are idempotent, so a restart while paused simply repeats them.
  if (chain.endBlock !== null && chain.progressBlock === chain.endBlock) {
    const at = await nextBoundary(config.schedule, config.stride, progress - 1, rpc);
    if (at?.kind === 'block' && at.b === progress) {
      if (!hasSnapshotAt(config.outputDir, at.b)) {
        await snapshotCycle(config, rpc, chain, at.b, at.id);
      }
      if (config.prune && hasSnapshotAt(config.outputDir, at.b)) {
        const conn = await openDuckDB(config);
        try {
          await pruneCycle(conn, rpc, {
            outputDir: config.outputDir,
            b: at.b,
            bTimestamp: await rpc.timestamp(at.b),
            startBlock: chain.startBlock,
            debug: config.debug,
          });
        } finally {
          conn.closeSync();
        }
      }
    }
  }

  const next = await nextBoundary(config.schedule, config.stride, progress, rpc);
  let end: number | null;
  if (next === null) end = null;
  else if (next.kind === 'block') end = next.b;
  else {
    end = next.estimate - FUTURE_MARGIN_BLOCKS;
    if (end <= progress) {
      // Close to a boundary that has not happened: wait for the chain to pass it.
      log(`schedule: ${next.id} ends at ${new Date(next.at * 1000).toISOString()}; waiting`);
      while ((await rpc.head()).timestamp < next.at) await sleep(30_000);
      return 'waited';
    }
  }
  log(`schedule: next ${next ? next.id : 'none'}; Envio runs to ${end ?? 'the head, unbounded'}`);
  return runEnvio(end, chain);
}

async function exit(code: number) {
  await envio?.stop();
  process.exit(code);
}

async function main(args: string[]): Promise<void> {
  const config = parseConfig(args);

  process.on('SIGTERM', () => void exit(0));
  process.on('SIGINT', () => void exit(0));

  await createSchema();
  await stopLeftoverEnvio();
  mkdirSync(config.outputDir, { recursive: true });
  removeUnverified(config.outputDir);
  const rpc = createRpc();
  await rpc.wait();

  for (;;) {
    const result = await pass(config, rpc);
    // Envio exited because shutdown stopped it: do not start another.
    if (stopping) break;
    if (result === 0 || result === 'waited') continue;
    // The supervisor keeps snapshots; it does not nurse Envio. A failed Envio
    // stops the supervisor too, and whoever runs it (systemd) decides on a
    // restart, which resumes from envio_chains and the manifests.
    log(`envio: exited with ${result}; stopping, Envio is not restarted`);
    await exit(1);
  }
}

// Only when run (node supervisor/main.ts), not when imported.
if (import.meta.main) {
  try {
    await main(process.argv.slice(2));
  } catch (e) {
    if (e instanceof InvalidValueError) {
      console.log(`invalid input: ${e.message}\n${USAGE}`);
      process.exit(64);
    }
    if (e instanceof HashMismatchError)
      log(`HALT: ${e.message}. No snapshot, Envio left stopped; recover by restoring (§3.14).`);
    else log(`fatal: ${e instanceof Error ? (e.stack ?? e.message) : String(e)}`);
    // Assigned inside functions, which TypeScript's narrowing cannot see.
    await (envio as EnvioRun | null)?.stop();
    process.exit(e instanceof HashMismatchError ? 2 : 1);
  }
}

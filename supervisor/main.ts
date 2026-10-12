import { mkdirSync } from 'node:fs';
import { pool } from '#tools/postgres.ts';
import { archiveAt, HashMismatchError } from '#supervisor/archive.ts';
import {
  atBoundary,
  createRpc,
  getPolygonChainState,
  nextEndBlockId,
  type Rpc,
} from '#supervisor/chain.ts';
import { Indexer, LockHeldError } from '#supervisor/envio.ts';
import { pruneAt } from '#supervisor/prune.ts';
import {
  InvalidValueError,
  parseConfig,
  type SupervisorConfig,
  USAGE,
} from '#supervisor/config.ts';

class CleanShutdown extends Error {}

/**
 * One pass: archive (and prune) if Envio is paused at a snapshot boundary,
 * then run it to its next end block.
 *
 * @returns Envio's exit code, or null if a signal killed it.
 * @throws AbortError, or the abort reason, once `signal` is aborted.
 */
async function iterate(
  config: SupervisorConfig,
  rpc: Rpc,
  indexer: Indexer,
  signal: AbortSignal,
): Promise<number | null> {
  const chain = await getPolygonChainState();
  if (!chain) throw new Error('envio_chains is missing; createSchema should have created it');

  if (atBoundary(config, chain)) {
    await archiveAt(config, chain, rpc, signal);
    if (config.prune) await pruneAt(config, chain, signal);
  }

  const end = nextEndBlockId(config, chain);
  if (end !== chain.endBlock) await indexer.patchEndBlock(end);
  return indexer.start(end, signal);
}

/**
 * Run the supervisor until Envio fails or a signal stops it.
 *
 * @param args - Command-line arguments, without node and the script path.
 * @returns The exit code: 1 once Envio exits non-zero.
 * @throws CleanShutdown on SIGTERM, SIGINT or SIGHUP; any other error is fatal.
 */
async function main(args: string[]): Promise<number> {
  const controller = new AbortController();
  const onSignal = (name: string) => {
    if (controller.signal.aborted) return;
    controller.abort(new CleanShutdown(name));
  };

  for (const signal of ['SIGTERM', 'SIGINT', 'SIGHUP']) {
    process.on(signal, () => onSignal(signal));
  }

  const config = parseConfig(args);
  mkdirSync(config.outputDir, { recursive: true });

  await using indexer = await Indexer.create();
  await indexer.createSchema();

  try {
    const rpc = await createRpc(process.env.POLYGON_RPC_URL, controller.signal);
    for (;;) {
      controller.signal.throwIfAborted();
      const result = await iterate(config, rpc, indexer, controller.signal);
      if (result !== 0) {
        console.log(`envio: exited with ${result}; stopping, Envio is not restarted`);
        return 1;
      }
    }
  } catch (e) {
    const isAbort = controller.signal.aborted && e instanceof Error && e.name === 'AbortError';
    throw isAbort ? controller.signal.reason : e;
  }
}

/** Log why the supervisor stopped; returns the exit code. */
function exitCodeFor(e: unknown): number {
  if (e instanceof CleanShutdown) return 0;
  if (e instanceof InvalidValueError) {
    console.log(`invalid input: ${e.message}\n${USAGE}`);
    return 64;
  }
  if (e instanceof HashMismatchError) {
    console.log(
      `HALT: ${e.message}. No snapshot, Envio left stopped; recover by restoring (§3.14).`,
    );
    return 2;
  }
  if (e instanceof LockHeldError) {
    console.log(`fatal: ${e.message}`);
    return 1;
  }
  console.log(`fatal: ${e instanceof Error ? (e.stack ?? e.message) : String(e)}`);
  return 1;
}

let code: number;
try {
  code = await main(process.argv.slice(2));
} catch (e) {
  code = exitCodeFor(e);
}
console.log(`supervisor: stopped, exit ${code}`);
await pool.end().catch(() => undefined);
process.exit(code);

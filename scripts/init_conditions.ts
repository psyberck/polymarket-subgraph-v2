/**
 * Seeds Condition / OutcomeToken / NegRiskEvent from markets.jsonl, falling back
 * to the chain where Gamma can't answer, then verifies. Parsing runs in worker
 * threads (utils/worker.ts); this file splits the work and gathers counters.
 */
import { open, stat } from 'node:fs/promises';
import { availableParallelism } from 'node:os';
import { Worker } from 'node:worker_threads';
import { createPublicClient, http } from 'viem';
import { polygon } from 'viem/chains';
import { RAW } from '#scripts/utils/literals.ts';
import { createSinkStats, type SinkStats } from '#scripts/utils/parse.ts';
import { count, exec } from '#tools/postgres.ts';
import type { Request, Response } from '#scripts/utils/worker.ts';
import { verify } from '#scripts/verify_conditions.ts';
import { getPolygonChainState } from '#supervisor/chain.ts';

/** A byte range [start, end) of whole lines. */
interface Batch {
  start: number;
  end: number;
}

const RPC_URL = process.env.POLYGON_RPC_URL;
const client = createPublicClient({
  chain: polygon,
  transport: http(RPC_URL, { retryCount: 5, retryDelay: 500, timeout: 30_000 }),
});

/** Capped at 8: each worker opens its own pg pool (max 4) and RPC client. */
const WORKER_COUNT = Math.max(
  1,
  Number(process.env.WORKER_COUNT) || Math.min(availableParallelism(), 8),
);
const WORKER_URL = new URL('./utils/worker.ts', import.meta.url);

/**
 * start_block and its timestamp in ms, which is the payout gate.
 *
 * @throws Error if the schema has not been created yet.
 */
async function getBlockId(): Promise<{ block: bigint; ms: number }> {
  const chain = await getPolygonChainState();
  if (!chain)
    throw new Error('envio_chains is missing: create the schema first (envio local db-migrate up)');
  const blockId = BigInt(chain.startBlock);
  const block = await client.getBlock({ blockNumber: blockId });
  const ms = Number(block.timestamp) * 1000;
  console.log(`  start_block ${blockId.toString()} -> ${new Date(ms).toISOString()}`);
  return { block: blockId, ms };
}

/**
 * Splits a file into at most `n` contiguous byte ranges of whole lines. Each
 * boundary moves forward from `size * i / n` to just past the next '\n'.
 * Returns [] for an empty file.
 */
async function getBatches(filePath: string, n: number): Promise<Batch[]> {
  const { size } = await stat(filePath);
  const fh = await open(filePath, 'r');
  const buf = Buffer.alloc(64 * 1024);
  const res: Batch[] = [];
  let start = 0;
  try {
    for (let i = 1; i < n; i++) {
      let pos = Math.max(Math.floor((size * i) / n), start);
      for (;;) {
        const { bytesRead } = await fh.read(buf, 0, buf.length, pos);
        if (bytesRead === 0) {
          pos = size;
          break;
        }
        const nl = buf.subarray(0, bytesRead).indexOf('\n');
        if (nl !== -1) {
          pos += nl + 1;
          break;
        }
        pos += bytesRead;
      }
      if (pos > start) {
        res.push({ start, end: pos });
        start = pos;
      }
    }
  } finally {
    await fh.close();
  }
  if (size > start) res.push({ start, end: size });
  return res;
}

/**
 * Rebuilds the seed from the corpus; re-run whenever start_block moves.
 *
 * Refuses if UserPosition has rows (the indexer has run). TRUNCATEs first, so
 * the only conflicts are repeats within the corpus. If a worker fails, the rest are
 * terminated and the tables stay partial until the next run.
 *
 * @param gateTs - start_block's timestamp in ms, the payout gate.
 * @param startBlock - chain reads are pinned below it.
 * @returns Problems found by verification; 0 is clean.
 */
async function rebuildFromFile(gateTs: number, startBlock: bigint): Promise<number> {
  const indexed = await count('SELECT count(*) FROM "UserPosition"');
  if (indexed) {
    throw new Error(
      `UserPosition already has ${indexed.toLocaleString()} rows — the indexer has run. ` +
        'Seed before `envio start`, not after.',
    );
  }

  await exec('TRUNCATE "Condition", "OutcomeToken", "NegRiskEvent"');

  const t0 = Date.now();
  const batches = await getBatches(RAW, WORKER_COUNT);
  const requests: Request[] = batches.map(({ start, end }, worker) => ({
    worker,
    start,
    end,
    gateTs,
    filePath: RAW,
    startBlock: startBlock.toString(),
  }));
  console.log(`  ${requests.length} workers`);

  const workers = new Set<Worker>();
  let responses: Response[];
  try {
    responses = await Promise.all(
      requests.map(
        (req) =>
          new Promise<Response>((resolve, reject) => {
            const w = new Worker(WORKER_URL);
            workers.add(w);
            let reply: Response | undefined;
            w.once('message', (r: Response) => {
              reply = r;
            });
            w.once('error', reject);
            w.once('exit', (code) => {
              workers.delete(w);
              if (!reply) {
                reject(new Error(`worker ${req.worker} exited with code ${code}`));
                return;
              }
              console.log(
                `    worker ${reply.worker} done: ${reply.stats.lines.toLocaleString()} markets in ` +
                  `${Math.round((Date.now() - t0) / 1000)}s`,
              );
              resolve(reply);
            });
            w.postMessage(req);
          }),
      ),
    );
  } catch (e) {
    await Promise.all([...workers].map((w) => w.terminate()));
    throw e;
  }

  const stats = createSinkStats();
  for (const { stats: values } of responses) {
    for (const k of Object.keys(stats) as (keyof SinkStats)[]) stats[k] += values[k];
  }

  console.log(`  gate: updatedAt < ${new Date(gateTs).toISOString()}`);
  console.log(`  elapsed ${Math.round((Date.now() - t0) / 1000)}s`);

  report(stats);
  return await verify();
}

/**
 * Prints the counters. `processed` is the audit: every line lands in exactly one
 * bucket, so the buckets must sum to the line count.
 */
function report(stats: SinkStats): void {
  const processed =
    stats.unparseable +
    stats.noConditionId +
    stats.noOutcomes +
    stats.gateAfter +
    stats.gateBadTs +
    stats.pricedGated +
    stats.unResolved +
    stats.deferred;
  const report: [string, string][] = [
    ['markets parsed', stats.parsed.toLocaleString()],
    ['Condition rows', stats.conditions.toLocaleString()],
    ['OutcomeToken rows', stats.tokens.toLocaleString()],
    ['payoutPrices seeded', stats.pricedGated.toLocaleString()],
    ['not resolved in Gamma', stats.unResolved.toLocaleString()],
    ['validation needed', stats.deferred.toLocaleString()],
    ['...priced from chain', stats.chainPriced.toLocaleString()],
    ['derived-id mismatches', `${stats.idMismatch.toLocaleString()}  (expect 95)`],
    ['no/invalid conditionId', stats.noConditionId.toLocaleString()],
    ['no outcomes or clob ids', stats.noOutcomes.toLocaleString()],
    [`at/after gate`, stats.gateAfter.toLocaleString()],
    ['updatedAt unparseable', stats.gateBadTs.toLocaleString()],
    [
      'processed',
      processed === stats.lines
        ? `${processed.toLocaleString()} = ${stats.lines.toLocaleString()} lines`
        : `${processed.toLocaleString()} != ${stats.lines.toLocaleString()} LINES -- DOES NOT BALANCE`,
    ],
  ];
  for (const [k, v] of report) console.log(k ? `  ${k.padEnd(25)}${v}` : '');
}

const w = await getBlockId();
if (await rebuildFromFile(w.ms, w.block)) process.exitCode = 1;

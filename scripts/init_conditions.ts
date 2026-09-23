/**
 * Derive the seed from the corpus and insert it.
 *
 * Reads markets.jsonl, reads the chain for what Gamma could not answer, writes
 * Condition / OutcomeToken / NegRiskEvent, then runs stage 3 as a closing gate.
 * The corpus is split into line-aligned byte ranges, one per worker thread (see
 * utils/worker.ts); this file only prepares the run and gathers the counters.
 *
 * MUST RUN AFTER `envio local db-migrate setup` AND BEFORE `envio start`.
 */
import { readFileSync } from 'node:fs';
import { open, stat } from 'node:fs/promises';
import { availableParallelism } from 'node:os';
import { Worker } from 'node:worker_threads';
import { parse as parseYaml } from 'yaml';
import { createPublicClient, http } from 'viem';
import { polygon } from 'viem/chains';
import { RAW } from './utils/literals.ts';
import { createSinkStats, type SinkStats } from './utils/parse.ts';
import { count, exec } from './utils/postgres.ts';
import type { Request, Response } from './utils/worker.ts';
import { verify } from './verify_conditions.ts';

/** A byte range [start, end) of whole lines. */
interface Batch {
  start: number;
  end: number;
}

/**
 * The RPC client
 */
const RPC_URL = process.env.POLYGON_RPC_URL;
const client = createPublicClient({
  chain: polygon,
  transport: http(RPC_URL, { retryCount: 5, retryDelay: 500, timeout: 30_000 }),
});

/**
 * Worker count. Each worker opens its own pg pool (max 4) and RPC client, so
 * this is capped well below Postgres's default 100 connections.
 */
const WORKER_COUNT = Math.max(
  1,
  Number(process.env.WORKER_COUNT) || Math.min(availableParallelism(), 8),
);
const WORKER_URL = new URL('./utils/worker.ts', import.meta.url);

/**
 * The indexed window's start, from config.yaml.
 *
 * Read from config rather than passed in, so the seed's gate and the indexer's
 * first block cannot disagree.
 *
 * @returns The start block and its timestamp in ms, which is the payout gate.
 */
async function getBlockId(): Promise<{ block: bigint; ms: number }> {
  const cfg = parseYaml(readFileSync('config.yaml', 'utf8')) as {
    chains?: { id: number; start_block: number }[];
  };
  const chains = cfg.chains ?? [];
  const chain = chains.find((c) => c.id === 137) ?? (chains.length === 1 ? chains[0] : undefined);
  if (!chain)
    throw new Error(`config.yaml: cannot find the polygon chain (${chains.length} defined)`);
  const blockId = BigInt(chain.start_block);
  const block = await client.getBlock({ blockNumber: blockId });
  const ms = Number(block.timestamp) * 1000;
  console.log(`  start_block ${blockId.toString()} -> ${new Date(ms).toISOString()}`);
  return { block: blockId, ms };
}

/**
 * Splits a file into at most `n` byte ranges of whole lines.
 *
 * Each boundary starts at `size * i / n` and moves forward to just past the next
 * '\n', so it never falls inside a line or a multi-byte character.
 *
 * @param filePath - File to split.
 * @param n - Number of ranges wanted.
 * @returns Contiguous, non-empty ranges covering the whole file, in order. Fewer
 *   than `n` when lines are long relative to the file; [] for an empty file.
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
        // '\n' marks the end of each line in markets.jsonl
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
 * Derive the whole seed from the archived corpus and insert it.
 *
 * This is what re-runs when start_block moves: the gate changes, so every payout
 * decision changes, and nothing needs re-fetching.
 *
 * TRUNCATEs first, which is required rather than tidy: Condition and
 * OutcomeToken inserts carry no ON CONFLICT, so anything already present aborts
 * the load. Refuses outright when UserPosition has rows -- seeding over an
 * indexed schema would overwrite event-derived data with values derived from
 * metadata.
 *
 * If any worker fails, the rest are terminated and the error is rethrown; the
 * tables are left partial until the next run TRUNCATEs them.
 *
 * @param gateTs - start_block's timestamp in ms.
 * @param startBlock - start_block; chain reads are pinned below it.
 * @returns Structural problems found by the closing verification; 0 is clean.
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
 * Print the run's counters.
 *
 * `processed` is the audit: every line lands in exactly one bucket, so the
 * six must sum to the line count. A mismatch means a branch is double-counting
 * or dropping markets, which makes every other number here suspect.
 *
 * @param stats - counters gathered during the walk.
 */
function report(stats: SinkStats): void {
  const processed =
    stats.unparseable +
    stats.noConditionId +
    stats.noOutcomes +
    stats.gateAfter +
    stats.gateBadTs +
    stats.pricedGated +
    stats.deferred;
  const report: [string, string][] = [
    ['markets parsed', stats.parsed.toLocaleString()],
    ['Condition rows', stats.conditions.toLocaleString()],
    ['OutcomeToken rows', stats.tokens.toLocaleString()],
    ['payoutPrices seeded', stats.pricedGated.toLocaleString()],
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

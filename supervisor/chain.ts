import { setTimeout as sleepFor } from 'node:timers/promises';
import { createPublicClient, http } from 'viem';
import { polygon } from 'viem/chains';
import { pgSchema, pool, table } from '#tools/postgres.ts';
import type { SupervisorConfig } from '#supervisor/config.ts';

export const POLYGON_BLOCK_SECONDS = 2;

export interface Rpc {
  /** The latest block. */
  head(): Promise<{ number: number; timestamp: number }>;
  /** The `finalized` block number, or null if the endpoint does not report one. */
  finalized(): Promise<number | null>;
  /** Whether the endpoint answers at all. */
  ping(): Promise<boolean>;
  /** Hashes of the given blocks. */
  getBlockHashes(blocks: number[]): Promise<Map<number, string>>;
}

export interface PolygonChainState {
  /** First block Envio indexes. */
  startBlock: number;
  /** Block Envio stops at, inclusive; null to follow the head. */
  endBlock: number | null;
  /** Last block fully indexed and committed. */
  progressBlock: number;
  /** Chain head as last seen by Envio's source. */
  sourceBlock: number;
  /** How many blocks behind the head Envio watches for reorgs. */
  maxReorgDepth: number;
}

/**
 * Connect to a Polygon RPC endpoint, waiting (with backoff) until it answers.
 *
 * @param url - The endpoint, normally POLYGON_RPC_URL.
 * @param signal - Aborting it stops the wait.
 * @returns The client.
 * @throws Error if `url` is not set.
 * @throws AbortError if `signal` is aborted while waiting.
 */
export async function createRpc(url?: string, signal?: AbortSignal): Promise<Rpc> {
  if (!url) throw new Error('POLYGON_RPC_URL is not set');
  const client = createPublicClient({
    chain: polygon,
    transport: http(url, { batch: { batchSize: 100 }, retryCount: 3, timeout: 30_000 }),
  });

  const ping = async () => {
    try {
      await client.getBlockNumber({ cacheTime: 0 });
      return true;
    } catch {
      return false;
    }
  };

  for (let delay = 2_000; !(await ping()); delay = Math.min(delay * 2, 60_000)) {
    console.log(
      `rpc: proxy at ${process.env.POLYGON_RPC_URL} is not responding; retrying in ${delay / 1000}s`,
    );
    await sleepFor(delay, undefined, { signal });
  }

  return {
    async head() {
      const block = await client.getBlock({ blockTag: 'latest' });
      return { number: Number(block.number), timestamp: Number(block.timestamp) };
    },

    async finalized() {
      try {
        return Number((await client.getBlock({ blockTag: 'finalized' })).number);
      } catch {
        return null;
      }
    },

    async getBlockHashes(blocks) {
      const results = await Promise.all(
        blocks.map((n) => client.getBlock({ blockNumber: BigInt(n) })),
      );
      return new Map(results.map((b) => [Number(b.number), b.hash]));
    },
    ping,
  };
}

/**
 * Read Envio's progress for Polygon from envio_chains.
 *
 * @returns The chain's row, or null if the schema has not been created yet.
 * @throws Error if envio_chains does not hold exactly one row.
 */
export async function getPolygonChainState(): Promise<PolygonChainState | null> {
  const exists = await pool.query<{ t: string | null }>(`SELECT to_regclass($1) AS t`, [
    `${pgSchema}.envio_chains`,
  ]);
  if (!exists.rows[0].t) return null;
  const { rows } = await pool.query<{
    start_block: number;
    end_block: number | null;
    progress_block: number;
    source_block: number;
    max_reorg_depth: number;
  }>(
    `SELECT id, start_block, end_block, progress_block, source_block, max_reorg_depth FROM ${table('envio_chains')}`,
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

/** Whether Envio is paused at a snapshot boundary: a schedule is set and it reached its end block. */
export const atBoundary = (config: SupervisorConfig, chain: PolygonChainState): boolean =>
  config.schedule !== null && chain.endBlock !== null && chain.progressBlock === chain.endBlock;

/**
 * Decide the block Envio should stop at next.
 *
 * @param config - The schedule and its stride.
 * @param chain - Envio's chain state.
 * @returns The pending boundary if Envio has not reached it, else one stride
 *   past its progress; null without a schedule.
 */
export function nextEndBlockId(config: SupervisorConfig, chain: PolygonChainState): number | null {
  const progress = Math.max(chain.progressBlock, chain.startBlock - 1);
  if (config.schedule !== null && chain.endBlock !== null && progress < chain.endBlock)
    return chain.endBlock;
  return config.stride === null ? null : progress + config.stride;
}

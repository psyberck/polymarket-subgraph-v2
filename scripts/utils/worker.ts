import { createReadStream } from 'node:fs';
import { createInterface } from 'node:readline';
import { parentPort } from 'node:worker_threads';
import { createPublicClient, http, parseAbi } from 'viem';
import { polygon } from 'viem/chains';
import {
  COLLATERAL_SCALE,
  CONDITIONAL_TOKENS,
  NEG_RISK_ADAPTER,
} from '../../src/utils/constants.ts';
import {
  CONDITION_COLS,
  createSinkStats,
  insertHandler,
  NEGRISK_COLS,
  parseMarket,
  type SinkStats,
  TOKEN_COLS,
} from './parse.ts';
import { pool } from '../../tools/postgres.ts';

/** A slice of the corpus for one worker. */
export interface Request {
  /** Worker id, echoed back in the Response. */
  worker: number;
  /** Byte offset of a line start. */
  start: number;
  /** Exclusive byte offset of a line start or EOF; must be > start. */
  end: number;
  /** start_block timestamp in ms, used as the payout gate. */
  gateTs: number;
  /** Path to markets.jsonl. */
  filePath: string;
  /** start_block as a decimal string, since bigint cannot be posted. */
  startBlock: string;
}

/** A worker's result. */
export interface Response {
  worker: number;
  stats: SinkStats;
}

const RPC_URL = process.env.POLYGON_RPC_URL;
const PAYOUT_FLUSH_SIZE = 250;
const NEGRISK_FLUSH_SIZE = 400;
const CONDITION_RESOLUTION = '0xb44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894';
const NEG_RISK_ABI = parseAbi([
  'function getQuestionCount(bytes32 marketId) view returns (uint256)',
]);

/**
 * Seeds Condition, OutcomeToken and NegRiskEvent from one slice of the corpus.
 * Chain reads are batched and pinned to startBlock - 1.
 *
 * @param r - The slice to process.
 * @returns The worker id and its counters.
 */
async function run(r: Request): Promise<Response> {
  const client = createPublicClient({
    chain: polygon,
    transport: http(RPC_URL, { retryCount: 5, retryDelay: 500, timeout: 30_000 }),
    batch: { multicall: { batchSize: 16_384, wait: 16 } },
  });
  const startBlock = BigInt(r.startBlock);
  const stats = createSinkStats();
  const condOut = insertHandler('Condition', CONDITION_COLS);
  const tokOut = insertHandler('OutcomeToken', TOKEN_COLS);
  const negOut = insertHandler('NegRiskEvent', NEGRISK_COLS, 'ON CONFLICT ("id") DO NOTHING');

  const negSeen = new Set<string>();
  let negMarketIds: string[] = [];
  let conditionIds: string[] = [];

  /**
   * Drains `conditionIds` and fills their payoutPrices from ConditionResolution
   * logs. Flushes condOut first so the UPDATE can find the rows, and never
   * overwrites prices Gamma already set.
   *
   * @returns Resolves once the prices are written.
   */
  const updatePayoutPrices = async (): Promise<void> => {
    if (!conditionIds.length) return;
    const ids = conditionIds;
    conditionIds = [];
    await condOut.flush();

    const logs = await client.request({
      method: 'eth_getLogs',
      params: [
        {
          address: CONDITIONAL_TOKENS,
          topics: [CONDITION_RESOLUTION as `0x${string}`, ids as `0x${string}`[]],
          fromBlock: '0x0',
          toBlock: `0x${(startBlock - 1n).toString(16)}`,
        },
      ],
    });

    const prices: Record<string, bigint[]> = {};
    for (const l of logs as { data: string; topics: string[] }[]) {
      const d = l.data.slice(2);
      const word = (n: number): bigint => BigInt(`0x${d.slice(n * 64, (n + 1) * 64)}`);
      const off = Number(word(1)) / 32;
      const len = Number(word(off));
      const nums = Array.from({ length: len }, (_, k) => word(off + 1 + k));
      const den = nums.reduce((a, b) => a + b, 0n);
      if (den === 0n) continue;
      const cid = l.topics[1]?.toLowerCase();
      if (!cid) {
        console.warn(`empty condition id has been detected from ${l.topics.join(',')}`);
        continue;
      }
      prices[cid] = nums.map((n) => (n * COLLATERAL_SCALE) / den);
    }
    const entries = Object.entries(prices);
    if (!entries.length) return;

    await pool.query(
      'UPDATE "Condition" c SET "payoutPrices" = v.prices::text[] ' +
        'FROM (SELECT * FROM unnest($1::text[], $2::text[]) AS t(id, prices)) v ' +
        'WHERE c.id = v.id AND c."payoutPrices" IS NULL',
      [entries.map(([id]) => id), entries.map(([, p]) => `{${p.join(',')}}`)],
    );
    stats.chainPriced += entries.length;
  };

  /**
   * Drains `negMarketIds` and writes their questionCount from the NegRiskAdapter.
   * Reads need an archive node; failed reads are skipped.
   *
   * @returns Resolves once the rows are buffered in negOut.
   */
  const updateNegRisk = async (): Promise<void> => {
    if (!negMarketIds.length) return;
    const ids = negMarketIds;
    negMarketIds = [];
    const counts = await Promise.all(
      ids.map((id) =>
        client
          .readContract({
            address: NEG_RISK_ADAPTER,
            abi: NEG_RISK_ABI,
            functionName: 'getQuestionCount',
            args: [id as `0x${string}`],
            blockNumber: startBlock - 1n,
          })
          .then((n) => Number(n))
          .catch(() => null),
      ),
    );
    for (let k = 0; k < ids.length; k++) {
      const id = ids[k];
      const n = counts[k];
      if (id === undefined || n === null || n === undefined) continue;
      await negOut.write([id, n]);
    }
  };

  const rl = createInterface({
    input: createReadStream(r.filePath, { encoding: 'utf8', start: r.start, end: r.end - 1 }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if (!line.trim()) continue;
    stats.lines++;
    let m: Record<string, unknown>;
    try {
      m = JSON.parse(line) as Record<string, unknown>;
    } catch {
      stats.unparseable++;
      continue;
    }

    const market = parseMarket(m, r.gateTs, stats);
    if (!market) continue;

    await condOut.write([
      market.conditionId,
      market.outcomeSlotCount,
      market.negRisk,
      market.negRiskMarketId,
      market.questionIndex,
      market.payoutPrices?.map(String) ?? null,
    ]);
    let outcomeIndex = 0;
    for (const positionId of market.positionIds) {
      await tokOut.write([positionId, market.conditionId, outcomeIndex++, market.collateral]);
    }
    stats.conditions++;
    stats.tokens += market.positionIds.length;

    if (market.invalidPayoutPrices) conditionIds.push(market.conditionId);
    if (market.negRiskMarketId && !negSeen.has(market.negRiskMarketId)) {
      negSeen.add(market.negRiskMarketId);
      negMarketIds.push(market.negRiskMarketId);
    }
    if (conditionIds.length >= PAYOUT_FLUSH_SIZE) await updatePayoutPrices();
    if (negMarketIds.length >= NEGRISK_FLUSH_SIZE) await updateNegRisk();
  }

  await updatePayoutPrices();
  await updateNegRisk();
  await condOut.close();
  await tokOut.close();
  await negOut.close();
  await pool.end();

  return { worker: r.worker, stats };
}

parentPort?.once('message', (req: Request) => {
  run(req).then(
    (reply) => parentPort?.postMessage(reply),
    (e: unknown) => {
      throw e instanceof Error ? e : new Error(String(e));
    },
  );
});

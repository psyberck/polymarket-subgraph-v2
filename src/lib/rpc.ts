/**
 * The one RPC client, and the one ConditionalTokens address.
 *
 * Both lib/payouts.ts and lib/collateral.ts read view functions off the same
 * contract on the same chain, and each used to carry its own copy of the
 * address, the URL resolution and the transport config. Two clients cannot
 * batch each other's calls, and two copies of an address constant are one
 * edit away from disagreeing.
 *
 * BATCHING IS THE REASON THIS IS SHARED. `batch.multicall` coalesces every
 * `readContract` issued inside the same short window into a single
 * eth_call against Multicall3 — but only calls made through ONE client, and
 * only calls that are actually in flight together. A `for` loop that awaits
 * each read issues them one at a time and batches nothing, so callers here
 * fire their reads with Promise.all and let the client fold them.
 *
 * With that, an effect probing a binary condition costs one round trip
 * instead of three, and several conditions probed concurrently — envio runs
 * them up to the effect's rate limit — fold into the same one.
 */

import { createPublicClient, getAddress, http } from 'viem';
import { polygon } from 'viem/chains';

/** Gnosis ConditionalTokens on Polygon.
 *
 * Checksummed via getAddress rather than written out: viem rejects an address
 * whose EIP-55 casing is wrong, and it rejects it at call time, deep inside an
 * effect, where the only symptom is every probe failing.
 */
export const CONDITIONAL_TOKENS = getAddress('0x4d97dcd97ec945f40cf65f87097ace5ea0476045');

export const RPC_URL = process.env.POLYGON_RPC_URL ?? 'https://polygon.gateway.tenderly.co';

export const rpc = createPublicClient({
  chain: polygon,
  transport: http(RPC_URL, { retryCount: 5, retryDelay: 500, timeout: 30_000 }),
  batch: {
    multicall: {
      batchSize: 16_384,
      wait: 16,
    },
  },
});

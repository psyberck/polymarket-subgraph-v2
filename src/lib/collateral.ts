/**
 * Outcome token ids for a condition under ANY collateral.
 *
 * Why this exists
 * ---------------
 * A CTF position id is `keccak256(collateral, collectionId)`, so the same
 * market has a DIFFERENT set of token ids per collateral token. The markets
 * table only carries one set — the USDC.e-denominated ids the CLOB quotes — so
 * every other collateral was handled by an allowlist that skipped it.
 *
 * That allowlist is a claim about the world that decays. Polymarket already
 * runs pUSD ("Polymarket USD", a 1:1 USDC wrapper) alongside USDC.e, and their
 * docs describe pUSD as the collateral for all trading. Today pUSD-collateralised
 * positions are negligible — $16k locked in ConditionalTokens against $183M for
 * USDC.e at block 92.4M — but if that ever flips, an allowlist keyed to USDC.e
 * stops being a filter for noise and starts silently discarding real positions.
 * That is the same failure mode as the adapter redeploy: no error, no gap, just
 * activity that stops being credited.
 *
 * The fix is to treat collateral as a DIMENSION rather than a filter.
 *
 * How
 * ---
 * `collectionId` depends only on (parentCollectionId, conditionId, indexSet) —
 * NOT on the collateral. So one on-chain `getCollectionId` per outcome yields a
 * value from which every collateral's position id is a local keccak:
 *
 *     collectionId = getCollectionId(0x0, conditionId, indexSet)   <- on chain, once
 *     getPositionId   = keccak256(abi.encodePacked(collateral, collectionId))  <- local
 *
 * Deriving `collectionId` off-chain would mean reimplementing alt_bn128
 * hash-to-curve, which conditionTokens.ts deliberately avoids; one batched
 * round trip per condition is far cheaper than that risk.
 *
 * Scope, honestly
 * ---------------
 * This resolves the FORWARD direction: given a conditionId and a collateral —
 * which every lifecycle event carries — produce the right token ids. That covers
 * splits, merges and redemptions under any collateral.
 *
 * It does NOT give the reverse direction for a non-canonical id seen in
 * isolation (an ERC-1155 transfer names only a tokenId). Reversing that would
 * need every condition's ids precomputed per collateral — 2.5M conditions times
 * one RPC call each — which is not viable. Ids derived here are registered for
 * reverse lookup, so a position becomes reversible once a lifecycle event has
 * named its condition; a pUSD position acquired purely by trade stays
 * unresolvable, exactly as it is today.
 *
 * KNOWN GAP. That guarantee holds against the `Token` entity but not against
 * the memo in front of it. handlers/AdapterTransfers.ts records a negative for
 * an id no tier could resolve, and nothing invalidates it when a later
 * lifecycle event writes the row here — so within a run such an id can stay
 * unresolvable after it has become resolvable. Dormant while every
 * non-canonical collateral is negligible and no `Token` row is ever written.
 */

import { createEffect, type EvmOnEventContext, S } from 'envio';
import { encodePacked, getAddress, keccak256, parseAbi } from 'viem';
import { client, CONDITIONAL_TOKENS } from '@lib/rpc';
import { resolveConditionTokens } from '@lib/conditionTokens';

// Collaterals whose ids the markets table already holds, so they need no
// derivation and no RPC. Everything else goes through getCollectionId.
const TABLE_COLLATERALS = new Set([
  '0x2791bca1f2de4661ed88a30c99a7a9449aa84174', // USDC.e
  '0x3a3bd7bb9528e159577f7c2e685cc81a765002e2', // neg-risk wrapped collateral
]);

export const isTableCollateral = (a: string) => TABLE_COLLATERALS.has(a.toLowerCase());

/**
 * Collaterals whose CTF positions are measured as economically negligible, and
 * whose events are therefore skipped WITHOUT deriving ids.
 *
 * This is a throughput decision, not a correctness one. Deriving costs one
 * batched `getCollectionId` round trip per condition, which is fine at low
 * volume — but
 * pUSD accounts for ~47% of all PayoutRedemption events, essentially all of them
 * no-ops (9,863 of 9,865 sampled had payout == 0, $3.00 total against
 * $196,311.50 for canonical collateral). Those are `redeemPositions` calls
 * against ids the caller never held. Deriving ids for tens of thousands of
 * conditions to discover that each burns nothing would cost hours of RPC for no
 * change to any position.
 *
 * Justified by measurement, not assumption: at block 92.4M, ConditionalTokens
 * held $16,247 of pUSD against $182,841,743 of USDC.e — 0.009%.
 *
 * `scripts/check_addresses.py --collateral` re-measures that ratio. When pUSD
 * stops being negligible, delete it from this set and it is derived like any
 * other collateral; the mechanism is already in place.
 */
const NEGLIGIBLE_COLLATERALS = new Set([
  '0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb', // pUSD (proxy)
  '0x6bbcef9f7ef3b6c592c99e0f206a0de94ad0925f', // pUSD (impl)
]);

export const isNegligibleCollateral = (a: string) => NEGLIGIBLE_COLLATERALS.has(a.toLowerCase());

const ABI = parseAbi([
  'function getCollectionId(bytes32 parentCollectionId, bytes32 conditionId, uint256 indexSet) view returns (bytes32)',
]);

/**
 * collectionId per outcome index. Empty when the condition is unknown, null
 * when the probe failed.
 *
 * Those last two are not the same answer. `[]` is a fact about the CONDITION —
 * it is not in the markets table — and the caller skips it silently, the way it
 * skips any market it has no metadata for. `null` is a fact about the RPC,
 * which says nothing about the condition, so the caller logs instead: an event
 * that should have been booked was dropped, and `[]` alone cannot tell that
 * apart from a deliberate skip.
 *
 * NEITHER is memoized, which is what makes the split safe to act on. An earlier
 * version cached the two together, and one timeout then pinned an empty answer
 * for the rest of the run — every later split, merge and redemption of that
 * condition under a non-canonical collateral skipped, with no second log line.
 *
 * Envio's disk cache is off, and the nullable output is why: it was observed
 * inserting a cache row with a null id under RPC failure, violating the cache
 * table's primary key and killing the write batch. (A different reason from
 * payouts.ts, which turns it off because "unresolved" is a fact with an expiry
 * date.)
 */
const collectionIdsEffect = createEffect(
  {
    name: 'collectionIds',
    input: S.string,
    output: S.nullable(S.array(S.string)),
    cache: false,
    rateLimit: { calls: 5, per: 'second' },
  },
  async ({ input, context }): Promise<string[] | null> => {
    const outcomeCount = resolveConditionTokens(input).length;
    if (!outcomeCount) return [];

    try {
      return await Promise.all(
        Array.from({ length: outcomeCount }, (_, i) =>
          client.readContract({
            address: CONDITIONAL_TOKENS,
            abi: ABI,
            functionName: 'getCollectionId',
            args: [
              '0x0000000000000000000000000000000000000000000000000000000000000000',
              input as `0x${string}`,
              1n << BigInt(i),
            ],
          }),
        ),
      );
    } catch (err) {
      context.log.warn(`getCollectionId failed for ${input}: ${String(err).split('\n')[0]}`);
      return null;
    }
  },
);

/** getPositionId for one collateral, given a collectionId. Pure local hashing. */
function getPositionId(collateral: string, collectionId: string): bigint {
  return BigInt(
    keccak256(
      encodePacked(['address', 'bytes32'], [getAddress(collateral), collectionId as `0x${string}`]),
    ),
  );
}

/**
 * Outcome token ids for `conditionId` denominated in `collateral`.
 *
 * Canonical collaterals short-circuit to the markets table with no RPC, so the
 * hot path is unchanged for the ~100% of activity that is USDC.e-denominated.
 * Anything else derives its ids and registers them for reverse lookup.
 *
 * Returns [] when the condition is unknown or the derivation fails — callers
 * already treat an empty list as "skip", which keeps a failed probe from
 * booking against the wrong collateral's positions. A failure additionally logs
 * here, because [] alone cannot tell a deliberate skip from a lost event.
 */
export async function tokensForCollateral(
  context: EvmOnEventContext,
  conditionId: string,
  collateral: string,
): Promise<bigint[]> {
  if (isTableCollateral(collateral)) return resolveConditionTokens(conditionId);
  if (isNegligibleCollateral(collateral)) return [];

  const collections = await context.effect(collectionIdsEffect, conditionId);
  if (collections === null) {
    context.log.warn(
      `collectionId derivation failed, event skipped: condition=${conditionId} ` +
        `collateral=${collateral.toLowerCase()}`,
    );
    return [];
  }
  if (!collections.length) return [];

  const ids = collections.map((c) => getPositionId(collateral, c));
  ids.forEach((tokenId, outcomeIndex) =>
    context.Token.set({
      id: tokenId.toString(),
      conditionId,
      outcomeIndex,
      collateral: collateral.toLowerCase(),
    }),
  );
  return ids;
}

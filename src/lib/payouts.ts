/**
 * Settled prices for conditions that resolved before the indexed window.
 *
 * `OutcomePayout` is normally written by the `ConditionResolution` handler, which
 * only sees resolutions inside the indexed range. Every redemption of a market
 * that resolved earlier therefore found no settled price and fell back:
 *
 *   PayoutRedemption   -> allocate the aggregate payout by balance held
 *   adapter transfer   -> price the surrender at 1/2, as if it were a merge
 *
 * The second is the damaging one. A losing leg surrendered to a redemption
 * books +0.50/token of realized PnL where the truth is -avgPrice — a sign
 * error, not a rounding one. On a bounded backfill most redeemed markets
 * resolved before the start block, so this was the common path.
 *
 * Both fallbacks disappear if the payouts are simply read from the chain.
 * `payoutNumerators` / `payoutDenominator` are public getters on the Gnosis
 * ConditionalTokens contract, so the settled price of any condition is one
 * `eth_call` away regardless of when it resolved — and it is authoritative in
 * a way Gamma's `resolution_idx` is not, because it also expresses fractional
 * resolutions (a 50/50 settlement is 1/2 each, which a winning-index cannot
 * represent).
 *
 * Wrapped in an Envio effect so the reads are deduplicated across events and
 * rate limited, and issued through the shared batching client in lib/rpc.ts,
 * so a condition's 1 + outcomeCount reads cost ONE round trip — folded in with
 * any other condition being probed at the same moment.
 *
 * Not that the probe runs often. Of 2.95M markets, 2.69M carry a single
 * winning outcome index and never reach it at all (see TRUST_METADATA), and a
 * condition that does probe writes OutcomePayout rows that answer every later
 * event from the database. The effect's own disk cache is off and envio drops a
 * `cache: false` entry at every commit, so nothing else memoizes this — by
 * design, since "unresolved" is a fact with an expiry date.
 */

import { createEffect, type EvmOnEventContext, S } from 'envio';
import { parseAbi } from 'viem';
import { client, CONDITIONAL_TOKENS } from '@lib/rpc';
import { conditionResolutionIndex, resolveConditionTokens, UNRESOLVED } from '@lib/conditionTokens';
import { USDC_SCALE } from '@lib/position';

const ABI = parseAbi([
  'function payoutDenominator(bytes32 conditionId) view returns (uint256)',
  'function payoutNumerators(bytes32 conditionId, uint256 index) view returns (uint256)',
]);

/**
 * Settled price per outcome index, 1e6. Empty when the condition has not
 * resolved on chain (or is not in the markets table, so its outcome count is
 * unknown); null when the probe failed.
 *
 * The caller treats both as "no settled price", so the two are kept apart for
 * one reason: `null` logs and `[]` does not. A chain that says a condition is
 * still open is ordinary; a probe that could not run is a redemption dropped or
 * an adapter transfer merge-priced, and that deserves a line naming it.
 *
 * Keep them apart if a memo is ever added in front of this. `[]` is a fact
 * about the CONDITION and safe to remember; `null` is a fact about the RPC and
 * is not. An earlier version stored both, and one timeout then pinned
 * "unresolved" for the rest of the run — every later redemption of that
 * condition dropped, every adapter transfer of it merge-priced, which is the
 * +0.50 sign error this module exists to remove.
 *
 * (Same encoding as collectionIdsEffect in lib/collateral.ts, deliberately.)
 *
 * Read at head rather than at a historical block: a byResolution is final, so
 * the answer is the same at any block after it, and asking at head keeps the
 * cache key a plain conditionId.
 */
export const conditionPayout = createEffect(
  {
    name: 'conditionPayout',
    input: S.string,
    output: S.nullable(S.array(S.bigint)),
    cache: false,
    rateLimit: { calls: 5, per: 'second' },
  },
  async ({ input, context }): Promise<bigint[] | null> => {
    // Unreachable from ensureOutcomePayouts, which returns before calling this
    // when the condition has no tokens. Kept because the effect is exported.
    const outcomeCount = resolveConditionTokens(input).length;
    if (!outcomeCount) return [];

    try {
      const [denominator, ...numerators] = await Promise.all([
        client.readContract({
          address: CONDITIONAL_TOKENS,
          abi: ABI,
          functionName: 'payoutDenominator',
          args: [input as `0x${string}`],
        }),
        ...Array.from({ length: outcomeCount }, (_, i) =>
          client.readContract({
            address: CONDITIONAL_TOKENS,
            abi: ABI,
            functionName: 'payoutNumerators',
            args: [input as `0x${string}`, BigInt(i)],
          }),
        ),
      ]);

      if (denominator === 0n) return [];
      return numerators.map((numerator) => (numerator * USDC_SCALE) / denominator);
    } catch (err) {
      context.log.warn(
        `payout probe failed for ${input}, falling back to merge pricing: ` +
          String(err).split('\n')[0],
      );
      return null;
    }
  },
);

// Derive settled prices from the local winning-outcome index instead of probing
// the chain, when the metadata expresses a single winner.
//
// This exists because the probe cannot keep up on the hot path. Handlers `await`
// it, so event processing blocks on it — and a 48-block window produced 507
// distinct conditions. Measured live: the probe backlog hit 725 queued calls
// while log fetching sat behind it, and the indexer committed nothing at all.
// Priority queuing in the proxy does not help, because the block is inside the
// handler, not in the queue order.
//
// The trade is trusting Gamma for WHICH outcome won. Measured over 300 random
// resolved binary conditions: 300/300 agreement with on-chain
// payoutNumerators, no disagreements and none unresolved on chain. The chain
// stays authoritative for everything the metadata cannot express — a -1 index
// (50/50 settlement or cancellation) still probes, which is the case a
// winner-index genuinely cannot represent.
//
// Set PAYOUT_TRUST_METADATA=0 to disable and probe every condition, which is
// how to reproduce the authoritative path or re-measure the agreement rate.
const TRUST_METADATA = process.env.PAYOUT_TRUST_METADATA !== '0';

/**
 * Ensure `OutcomePayout` rows exist for every outcome of `conditionId`,
 * probing the chain if the byResolution predates the indexed window.
 *
 * Returns the settled prices by outcome index, or null when the condition is
 * unresolved or unknown — in which case callers fall back to merge pricing,
 * which is now the only thing that fallback means.
 *
 * `knownResolved` is the caller asserting that the EVENT proves the condition
 * settled, whatever the local metadata says. A PayoutRedemption does: it cannot
 * be emitted without a non-zero payoutDenominator on chain. An adapter transfer
 * does not — it is a merge or a redemption and the event does not say which —
 * so it passes false and a local "unresolved" is taken at face value, which is
 * what keeps merges free of RPC.
 */
export async function ensureOutcomePayouts(
  context: EvmOnEventContext,
  conditionId: string,
  knownResolved: boolean,
): Promise<bigint[] | null> {
  const tokens = resolveConditionTokens(conditionId);
  if (!tokens.length) return null;

  const rows = await Promise.all(
    tokens.map((_, i) => context.OutcomePayout.get(outcomePayoutId(conditionId, i))),
  );
  const payouts = rows.filter(Boolean).length;
  if (payouts) {
    if (payouts < rows.length) {
      context.log.warn(
        `OutcomePayout set incomplete: condition=${conditionId} ` +
          `have=${payouts} want=${rows.length}`,
      );
    }
    const prices: bigint[] = [];
    rows.forEach((row, i) => {
      if (row) prices[i] = row.price;
    });
    return prices;
  }

  const resolutionIdx = conditionResolutionIndex(conditionId);
  if (resolutionIdx === UNRESOLVED && !knownResolved) return null;

  const resolvedWinner =
    TRUST_METADATA &&
    resolutionIdx !== UNRESOLVED &&
    resolutionIdx >= 0 &&
    resolutionIdx < tokens.length
      ? resolutionIdx
      : UNRESOLVED;

  const prices =
    resolvedWinner === UNRESOLVED
      ? await context.effect(conditionPayout, conditionId)
      : Array.from({ length: tokens.length }, (_, i) => (i === resolvedWinner ? USDC_SCALE : 0n));
  if (!prices?.length) return null;

  writeOutcomePayouts(context, conditionId, prices);
  return prices;
}

/**
 * Record the settled prices a ConditionResolution event reports.
 *
 * The event carries the numerators directly, so this is the one writer that
 * needs no lookup of any kind — the chain has already said everything. It lives
 * here rather than in the handler so that every write to `OutcomePayout` goes
 * through this module, which is what lets the row shape stay private to it.
 *
 * Writes nothing when the condition is unresolvable or unknown, and logs in
 * both cases: no row means merge pricing later, with no trail back here.
 * `context.log` already carries chain, block and log index.
 */
export function recordConditionResolution(
  context: EvmOnEventContext,
  conditionId: string,
  numerators: readonly bigint[],
): void {
  const denominator = numerators.reduce((a, b) => a + b, 0n);
  if (denominator === 0n) {
    context.log.warn(
      `ConditionResolution pays nothing (cancelled): condition=${conditionId} ` +
        `outcomes=${numerators.length}`,
    );
    return;
  }

  const tokens = resolveConditionTokens(conditionId);
  if (!tokens.length) {
    context.log.warn(
      `ConditionResolution of a condition absent from the markets table: ` +
        `condition=${conditionId} outcomes=${numerators.length}`,
    );
    return;
  }

  const count = Math.min(tokens.length, numerators.length);
  if (tokens.length !== numerators.length) {
    context.log.warn(
      `ConditionResolution outcome mismatch: condition=${conditionId} ` +
        `table=${tokens.length} chain=${numerators.length}`,
    );
  }

  const prices = Array.from(
    { length: count },
    (_, i) => (numerators[i] * USDC_SCALE) / denominator,
  );
  writeOutcomePayouts(context, conditionId, prices);
}

/**
 * Row key for one outcome's settled price.
 *
 * A payout is a property of (condition, outcome), not of a tokenId — the same
 * price serves every collateral's id for that outcome — so this is the key the
 * whole module reads and writes by.
 */
function outcomePayoutId(conditionId: string, outcomeIndex: number): string {
  return `${conditionId}-${outcomeIndex}`;
}

/**
 * Persist one settled price per outcome.
 *
 * The row records the price and nothing about where it came from. All three
 * writers — a ConditionResolution event, a chain probe, and the metadata fast
 * path — produce the same settled price for the same condition, and no reader
 * has ever needed to tell them apart.
 */
function writeOutcomePayouts(
  context: EvmOnEventContext,
  conditionId: string,
  prices: readonly bigint[],
): void {
  for (let i = 0; i < prices.length; i++) {
    context.OutcomePayout.set({
      id: outcomePayoutId(conditionId, i),
      conditionId,
      outcomeIndex: i,
      price: prices[i],
    });
  }
}

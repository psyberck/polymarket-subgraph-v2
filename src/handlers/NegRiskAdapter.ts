/**
 * Neg-risk market position lifecycle.
 *
 * Neg-risk (multi-candidate) markets route splits, merges, redemptions and
 * conversions through their own adapter, which survived the v2 migration and
 * is still the live contract — verified on-chain at head. Its events carry
 * DIFFERENT signatures from ConditionalTokens: no `collateralToken`, no
 * `partition`, and redemption reports per-outcome `amounts` directly.
 *
 * Each neg-risk conditionId is a binary YES/NO question within a larger
 * market, so a split is always a 2-leg full set.
 */

import { type EvmEvent, type EvmOnEventContext, indexer } from 'envio';
import {
  applyBuy,
  applySell,
  negRiskYesPrice,
  type PositionState,
  type Side,
  SIDE_BUY,
  SIDE_SELL,
  splitLegPrice,
} from '@lib/position';
import { getPositionId, mutatePosition } from '@lib/store';
import { ensureOutcomePayouts } from '@lib/payouts';
import {
  indexSetContains,
  negRiskQuestionCount,
  negRiskQuestionTokens,
  resolveConditionTokens,
} from '@lib/conditionTokens';

// Outcome ordering within a neg-risk question, matching both the reference
// and the markets table (every row is ["Yes", "No"]).
const YES_INDEX = 0;
const NO_INDEX = 1;

/**
 * A split or a merge, whichever emitted. These carry DIFFERENT params from
 * their ConditionalTokens namesakes — no collateralToken, no partition — so
 * they are pinned to this contract.
 */
type Event =
  EvmEvent<'NegRiskAdapter', 'PositionSplit'> | EvmEvent<'NegRiskAdapter', 'PositionsMerge'>;

/**
 * Book every leg of a split or a merge at 1/N of the collateral.
 *
 * The two events are the same movement in opposite directions, so they differ
 * only in `apply`. Each neg-risk conditionId is a binary question, so N is 2 —
 * taken from the resolved legs rather than assumed.
 */
async function applyLegs(
  context: EvmOnEventContext,
  event: Event,
  apply: (s: PositionState, price: bigint, amount: bigint) => PositionState,
): Promise<void> {
  const conditionId = event.params.conditionId;
  const tokenIds = resolveConditionTokens(conditionId);
  if (!tokenIds.length) return;

  const price = splitLegPrice(tokenIds.length);
  const amount = event.params.amount;

  for (const tokenId of tokenIds) {
    await mutatePosition(context, event, event.params.stakeholder, { tokenId, conditionId }, (s) =>
      apply(s, price, amount),
    );
  }
}

/**
 * The token ids a conversion moves on one side of the index set.
 *
 * SIDE_SELL gives the NO leg of every question NAMED by the index set, the ones
 * being burned. SIDE_BUY gives the YES leg of every OTHER question. The side and
 * the outcome index move together — that pairing is the conversion rule, so it
 * lives here rather than at two call sites that could drift apart.
 *
 * A question whose tokens are unknown is skipped, not guessed.
 */
function parseLegs(marketId: string, totalCount: number, indexSet: bigint, side: Side): bigint[] {
  const inSet = side === SIDE_SELL;
  const outcomeIndex = inSet ? NO_INDEX : YES_INDEX;

  const ids: bigint[] = [];
  for (let i = 0; i < totalCount; i++) {
    if (indexSetContains(indexSet, i) !== inSet) continue;
    const tokenId = negRiskQuestionTokens(marketId, i)[outcomeIndex];
    if (tokenId !== undefined) ids.push(tokenId);
  }
  return ids;
}

// ── Split: collateral -> YES + NO ──────────────────────────────────
indexer.onEvent(
  { contract: 'NegRiskAdapter', event: 'PositionSplit' },
  async ({ event, context }) => {
    await applyLegs(context, event, applyBuy);
  },
);

// ── Merge: YES + NO -> collateral ──────────────────────────────────
indexer.onEvent(
  { contract: 'NegRiskAdapter', event: 'PositionsMerge' },
  async ({ event, context }) => {
    await applyLegs(context, event, applySell);
  },
);

// ── Redemption: resolved position -> payout ────────────────────────
//
// Unlike ConditionalTokens, this reports per-outcome `amounts` alongside the
// aggregate `payout` — which is never needed, because neg-risk conditions
// still resolve through the CTF and so each leg can be settled at its true
// price, whether the byResolution was indexed or read from chain state.
indexer.onEvent(
  { contract: 'NegRiskAdapter', event: 'PayoutRedemption' },
  async ({ event, context }) => {
    const conditionId = event.params.conditionId;
    const tokenIds = resolveConditionTokens(conditionId);
    if (!tokenIds.length) return;

    const amounts = event.params.amounts;
    const redeemer = event.params.redeemer;

    const prices = await ensureOutcomePayouts(context, conditionId, true);
    if (!prices) {
      context.log.warn(
        `Neg-risk redemption skipped, no settled prices readable: ` +
          `condition=${conditionId} redeemer=${redeemer} block=${event.block.number}`,
      );
      return;
    }

    if (tokenIds.length !== amounts.length) {
      context.log.warn(
        `Neg-risk redemption outcome mismatch: condition=${conditionId} ` +
          `table=${tokenIds.length} chain=${amounts.length}`,
      );
    }
    const length = Math.min(tokenIds.length, amounts.length);
    for (let i = 0; i < length; i++) {
      const amount = amounts[i];
      if (amount === 0n) continue;
      const price = prices[i];
      if (price === undefined) continue;
      await mutatePosition(context, event, redeemer, { tokenId: tokenIds[i], conditionId }, (s) =>
        applySell(s, price, amount),
      );
    }
  },
);

// ── Conversion: NO tokens -> YES tokens + collateral ───────────────
//
// convertPositions() burns `amount` NO tokens across the questions named by
// `indexSet` and mints YES tokens for every OTHER question in the market.
// This was previously logged and skipped as unguessable; it is now folded the
// way the reference folds it (src/handlers/NegRiskAdapter.ts):
//
//   - each converted NO leg is disposed of at its own average cost, so the
//     conversion realizes nothing by itself and only moves the balance;
//   - each minted YES leg is acquired at the price that makes the conversion
//     self-financing (see negRiskYesPrice).
//
// The one input the reference gets from events we do not index is the market's
// question count; it is probed from the markets table instead (see
// negRiskQuestionCount). When it cannot be established, or when the index set
// references a question beyond it, nothing is booked and the event is logged —
// a wrong questionCount would misprice every YES leg, and conversion is a
// sophisticated-trader move, so the error would land precisely on the wallets
// being scored.
//
// WHAT THAT VETO COSTS, which is more than mispricing. This handler is the ONLY
// writer for a conversion: the ERC-1155 legs it moves are NegRiskAdapter <->
// wallet, and 0xd91e80cf (the NegRiskAdapter) is deliberately not in
// AdapterTransfers' V2_ADAPTERS — that list holds the COLLATERAL adapters and
// the AutoRedeemer — so `fromIsAdapter === toIsAdapter` discards both sides and
// books nothing. Traced on a real wallet's conversions: 0 of 10 and 0 of 15 CTF
// transfers in the transaction are bookable there.
//
// So returning early does not fall back to a coarser price, it drops the
// balance change: the burned NO stays on our books and the minted YES never
// appears. Unlike a wrong price, which stays local to the position, a wrong
// `amount` propagates into every later sell and redemption.
//
// MEASURED over the last full run: 27,158 vetoes, every one questionCount=0.
// 26,604 of them name a collateral adapter as stakeholder, which mutatePosition
// drops anyway — those cost nothing. The remaining 554, across 18 real wallets,
// are genuinely lost. Nearly all of that was the guard firing for the wrong
// reason: `declared` was inflated by neg-risk siblings Gamma lists but never
// deployed on chain, and the markets table was stale. With those fixed, 742 of
// the 745 vetoed markets now return a usable count. The 3 that do not are an
// accepted loss on the same reasoning as above.
indexer.onEvent(
  { contract: 'NegRiskAdapter', event: 'PositionsConverted' },
  async ({ event, context }) => {
    const marketId = event.params.marketId;
    const indexSet = event.params.indexSet;
    const stakeholder = event.params.stakeholder;
    const amount = event.params.amount;

    const count = negRiskQuestionCount(marketId);
    if (count === 0 || indexSet >> BigInt(count) !== 0n) {
      context.log.warn(
        `PositionsConverted not folded into cost basis: stakeholder=${stakeholder} ` +
          `market=${marketId} indexSet=${indexSet} count=${count} ` +
          `amount=${amount} block=${event.block.number}`,
      );
      return;
    }

    let noPriceSum = 0n;
    let noCnt = 0;

    for (const tokenId of parseLegs(marketId, count, indexSet, SIDE_SELL)) {
      const userPosition = await context.UserPosition.get(getPositionId(stakeholder, tokenId));
      noPriceSum += userPosition?.avgPrice ?? 0n;
      noCnt++;

      await mutatePosition(context, event, stakeholder, { tokenId }, (s) =>
        applySell(s, s.avgPrice, amount),
      );
    }

    if (noCnt === 0 || noCnt >= count) {
      context.log.warn(`${noCnt} no tokens /${count} total tokens`);
      return;
    }

    const yesPrice = negRiskYesPrice(noPriceSum / BigInt(noCnt), noCnt, count - noCnt);

    for (const tokenId of parseLegs(marketId, count, indexSet, SIDE_BUY)) {
      await mutatePosition(context, event, stakeholder, { tokenId }, (s) =>
        applyBuy(s, yesPrice, amount),
      );
    }
  },
);

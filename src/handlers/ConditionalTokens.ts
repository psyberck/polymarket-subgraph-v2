/**
 * Position lifecycle outside the order book: splits, merges, redemptions,
 * plus the byResolution payouts redemptions are priced against.
 *
 * These move token balances without a trade. Omitting them was never an
 * option — a wallet that mints a full set and sells one leg would otherwise
 * look like it acquired the other leg for free, which is exactly the pattern
 * sophisticated wallets use.
 */

import { type EvmEvent, type EvmOnEventContext, indexer } from 'envio';
import { applyBuy, applySell, type PositionState, splitLegPrice } from '@lib/position';
import { mutatePosition } from '@lib/store';
import { indexSetsToLegs, type OutcomeToken } from '@lib/conditionTokens';
import { tokensForCollateral } from '@lib/collateral';
import { ensureOutcomePayouts, recordConditionResolution } from '@lib/payouts';

/**
 * The outcome tokens an index-set list names, resolved for the event's own
 * collateral.
 *
 * Token ids depend on the collateral, so they are resolved FOR the collateral
 * the event carries rather than assuming the markets table's USDC.e set.
 * Canonical collateral short-circuits with no RPC; see lib/collateral.ts.
 *
 * Empty when the collateral is one we skip, the condition is unknown, or the
 * index sets name no outcome we hold an id for. All three mean the same thing
 * to a caller — there is nothing here to book — so all three return [].
 */
async function resolveLegs(
  context: EvmOnEventContext,
  conditionId: string,
  collateralToken: string,
  indexSets: readonly bigint[],
): Promise<OutcomeToken[]> {
  const all = await tokensForCollateral(context, conditionId, collateralToken);
  return all.length ? indexSetsToLegs(conditionId, all, indexSets) : [];
}

/**
 * A split or a merge, whichever emitted. `EvmEvent` without generics is the
 * union of every event on every contract, which is why it answers nothing about
 * params; naming the two here narrows it to the pair this helper handles.
 */
type Event =
  EvmEvent<'ConditionalTokens', 'PositionSplit'> | EvmEvent<'ConditionalTokens', 'PositionsMerge'>;

/**
 * Book every leg of a split or a merge at 1/N of the collateral.
 *
 * The two events are the same movement in opposite directions — same params,
 * same pricing, same legs — so they differ only in `apply`, which is applyBuy
 * for a split and applySell for a merge.
 */
async function applyLegs(
  context: EvmOnEventContext,
  event: Event,
  apply: (s: PositionState, price: bigint, amount: bigint) => PositionState,
): Promise<void> {
  const legs = await resolveLegs(
    context,
    event.params.conditionId,
    event.params.collateralToken,
    event.params.partition,
  );
  if (!legs.length) return;

  // Each leg is acquired at 1/N of the collateral — see splitLegPrice.
  const price = splitLegPrice(legs.length);
  const amount = event.params.amount;

  for (const leg of legs) {
    await mutatePosition(context, event, event.params.stakeholder, leg, (s) =>
      apply(s, price, amount),
    );
  }
}

// ── Resolution: the settled price of every outcome token ───────────
//
// Without this, a redemption can only be priced from its own aggregate
// `payout` field, which cannot distinguish a winning leg from a losing one
// when a wallet holds both.
indexer.onEvent(
  { contract: 'ConditionalTokens', event: 'ConditionResolution' },
  async ({ event, context }) => {
    recordConditionResolution(context, event.params.conditionId, event.params.payoutNumerators);
  },
);

// ── Split: collateral -> full set of outcome tokens ────────────────
indexer.onEvent(
  { contract: 'ConditionalTokens', event: 'PositionSplit' },
  async ({ event, context }) => {
    await applyLegs(context, event, applyBuy);
  },
);

// ── Merge: full set of outcome tokens -> collateral ────────────────
indexer.onEvent(
  { contract: 'ConditionalTokens', event: 'PositionsMerge' },
  async ({ event, context }) => {
    await applyLegs(context, event, applySell);
  },
);

// ── Redemption: resolved position -> payout ────────────────────────
indexer.onEvent(
  { contract: 'ConditionalTokens', event: 'PayoutRedemption' },
  async ({ event, context }) => {
    // Same collateral gate as split and merge, and for the same reason: the
    // tokenIds resolved from the markets table are the position ids of the
    // CANONICAL collateral. A redemption denominated in anything else — pUSD,
    // most of it — concerns different position ids entirely, so booking it
    // against these disposes of a position the wallet still holds.
    //
    // Missing this gate was not a silent under-count like the split case, it
    // FABRICATED realized PnL. Caught by scripts/check_balances.py: wallet
    // 0xe9ef6493 bought 58.875925 of token 76301063…64997 and still held every
    // one of them on chain, while the indexer had zeroed the position at block
    // 86,134,814 and booked +29.176023 of profit on a sale that never happened.
    // The redemption behind it carried pUSD collateral
    // (0xc011a7e1…, "Polymarket USD"), whose real position ids are not in the
    // markets table at all.
    // resolveLegs answers for THIS event's collateral, so a pUSD redemption
    // resolves pUSD ids, which the wallet holds none of, and applySell no-ops
    // exactly as it should.
    const conditionId = event.params.conditionId;
    const legs = await resolveLegs(
      context,
      conditionId,
      event.params.collateralToken,
      event.params.indexSets,
    );
    if (!legs.length) return;

    const redeemer = event.params.redeemer;

    // Close each leg at its resolved price: a wallet holding both sides
    // settles the winner at 1.00 and the loser at 0.00, which the event's
    // single aggregate `payout` cannot express.
    //
    // ensureOutcomePayouts reads the numerators straight off ConditionalTokens
    // when the byResolution predates the indexed window, so there is no longer a
    // "resolved before our start block" case to approximate around. Redeeming
    // requires a non-zero payoutDenominator on-chain, so null here means the
    // condition is genuinely unreadable — an RPC failure — and booking nothing
    // is the only honest answer.
    const prices = await ensureOutcomePayouts(context, conditionId, true);
    if (!prices) {
      context.log.warn(
        `Redemption skipped, no settled prices readable: condition=${conditionId} ` +
          `redeemer=${redeemer} block=${event.block.number}`,
      );
      return;
    }

    for (const leg of legs) {
      // Indexed by OUTCOME, not by leg — `legs` may cover a subset of
      // outcomes, and pairing them off positionally would swap winner for
      // loser on any partial redemption.
      const price = prices[leg.outcomeIndex];
      if (price === undefined) continue;
      await mutatePosition(context, event, redeemer, leg, (s) => applySell(s, price, s.amount));
    }
  },
);

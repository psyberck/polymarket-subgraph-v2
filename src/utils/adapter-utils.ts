import { getConditionPositionIds } from '@utils/getPositionId';
import { splitNegRiskPrice } from '@utils/computeNegRiskYesPrice';
import type { EvmEvent, EvmOnEventContext } from 'envio';
import { updateUserPositionWithBuy } from '@utils/updateUserPositionWithBuy';
import { updateUserPositionWithSell } from '@utils/updateUserPositionWithSell';

/**
 * Book a full-set mint: X collateral yields X of every outcome, so each leg is
 * acquired at `1/N` -- a price set by leg count, never by which outcome.
 *
 * No `Condition` row means no derivable ids, so it books nothing rather than
 * booking to ids that name no market.
 *
 * @param context - handler context; entity reads and writes go through it.
 * @param event - source event, for block/log ordering.
 * @param initiator - buying wallet. CALLER must exclude operators;
 *   `updateUserPositionWithBuy` does not filter.
 * @param conditionId - 0x-prefixed 32-byte condition id.
 * @param amount - collateral split, and the size of every leg.
 */
const applyPositionSplit = async (
  context: EvmOnEventContext,
  event: EvmEvent,
  initiator: string,
  conditionId: string,
  amount: bigint,
): Promise<void> => {
  const positionIds = await getConditionPositionIds(context, conditionId);
  if (!positionIds.length) return;
  const price = splitNegRiskPrice(positionIds.length);
  for (const positionId of positionIds) {
    await updateUserPositionWithBuy(
      context,
      event,
      initiator,
      positionId,
      price,
      amount,
      conditionId,
    );
  }
};

/**
 * Book a full-set burn: a split inverted, at the same `1/N` price.
 *
 * @param context - handler context; entity reads and writes go through it.
 * @param event - source event, for block/log ordering.
 * @param initiator - selling wallet. CALLER must exclude operators.
 * @param conditionId - 0x-prefixed 32-byte condition id.
 * @param amount - collateral returned, and the size of every leg.
 */
const applyPositionMerge = async (
  context: EvmOnEventContext,
  event: EvmEvent,
  initiator: string,
  conditionId: string,
  amount: bigint,
): Promise<void> => {
  const positionIds = await getConditionPositionIds(context, conditionId);
  if (!positionIds.length) return;
  const price = splitNegRiskPrice(positionIds.length);
  for (const positionId of positionIds) {
    await updateUserPositionWithSell(
      context,
      event,
      initiator,
      positionId,
      price,
      amount,
      conditionId,
    );
  }
};

/**
 * Book a settled redemption, pricing each outcome at what it paid.
 *
 * Array position IS the outcome index -- safe only because ids, prices and
 * amounts all span the full outcome vector in index order. The
 * ConditionalTokens path must not copy this: its `indexSets` is often a subset
 * or reordered, where pricing by position books a winner at $0.00.
 *
 * Skips unsettled conditions rather than guessing: the event's aggregate payout
 * sums the legs and cannot separate a winner from a loser.
 *
 * @param context - handler context; entity reads and writes go through it.
 * @param event - source event, for block/log ordering.
 * @param initiator - redeeming wallet. CALLER must exclude operators.
 * @param conditionId - 0x-prefixed 32-byte condition id.
 * @param amounts - tokens per outcome, in index order; short arrays tolerated.
 */
const applyPositionRedeem = async (
  context: EvmOnEventContext,
  event: EvmEvent,
  initiator: string,
  conditionId: string,
  amounts: readonly bigint[],
): Promise<void> => {
  const tokenIds = await getConditionPositionIds(context, conditionId);
  if (!tokenIds.length) return;
  const prices = (await context.Condition.get(conditionId))?.payoutPrices;
  if (!prices?.length) {
    context.log.warn(
      `Adapter redemption skipped, no settled prices readable: condition=${conditionId} ` +
        `wallet=${initiator} block=${event.block.number}`,
    );
    return;
  }
  const outcomes = Math.min(tokenIds.length, amounts.length, prices.length);
  for (let i = 0; i < outcomes; i++) {
    await updateUserPositionWithSell(
      context,
      event,
      initiator,
      tokenIds[i],
      prices[i],
      amounts[i],
      conditionId,
    );
  }
};

export { applyPositionSplit, applyPositionMerge, applyPositionRedeem };

import { indexer } from 'envio';
import { updateUserPositionWithBuy } from '@utils/updateUserPositionWithBuy';
import { getConditionPositionIds } from '@utils/getPositionId';
import { computeNegRiskYesPrice, splitNegRiskPrice } from '@utils/computeNegRiskYesPrice';
import { updateUserPositionWithSell } from '@utils/updateUserPositionWithSell';
import { indexSetContains } from '@utils/indexSetContains';
import { getNegRiskPositionId } from '@utils/getNegRiskPositionId';
import { loadUserPosition } from '@utils/loadUserPosition';
import { upsertCondition } from '@utils/upsertCondition';
import { getNegRiskConditionId } from '@utils/getNegRiskConditionId';
import {
  AUTO_REDEEMER,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V1,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V2,
  NEG_RISK_EXCHANGE,
  NEG_RISK_EXCHANGE_V2,
} from '@utils/constants';

/** Outcome ordering within a neg-risk question: every one is [Yes, No]. */
const YES_INDEX = 0;
const NO_INDEX = 1;
/**
 * Addresses that can be `stakeholder` on a neg-risk split, merge or conversion
 * without owning the position.
 *
 * The exchanges mint the set to themselves when filling an order, so a split
 * naming one is the exchange's own settlement leg, not a user acquiring
 * anything. The collateral adapters are the v2 routing path: measured over
 * blocks 86,226,978-86,227,478, barely 1% of PositionSplit events name a real
 * wallet, and the user-facing leg is the ERC-1155 transfer between wallet and
 * adapter — booking the stakeholder as well double-counts every v2 split.
 *
 * The subgraph filtered only `NEG_RISK_EXCHANGE` here, because the v2 exchange
 * and both adapter generations postdate it.
 */
const NEG_RISK_LIFECYCLE_NON_OWNERS: string[] = [
  NEG_RISK_EXCHANGE,
  NEG_RISK_EXCHANGE_V2,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V1,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V2,
] as const;

/**
 * Addresses that can be `redeemer` on a neg-risk redemption without owning the
 * position.
 *
 * `NEG_RISK_LIFECYCLE_NON_OWNERS` plus the AutoRedeemer, which redeems on a
 * user's behalf: the payout reaches the wallet, so crediting the redeemer
 * invents a position that never existed.
 */
const NEG_RISK_REDEEM_NON_OWNERS: string[] = [
  ...NEG_RISK_LIFECYCLE_NON_OWNERS,
  AUTO_REDEEMER,
] as const;

/**
 * Split: collateral in, one outcome token per leg out.
 *
 * A full set costs the collateral and redeems for it, so each of N legs is
 * acquired at 1/N.
 */
indexer.onEvent(
  { contract: 'NegRiskAdapter', event: 'PositionSplit' },
  async ({ event, context }) => {
    const conditionId = event.params.conditionId;
    const positionIds = await getConditionPositionIds(context, conditionId);
    if (!positionIds.length) return;

    if (NEG_RISK_LIFECYCLE_NON_OWNERS.includes(event.params.stakeholder.toLowerCase())) return;

    const price = splitNegRiskPrice(positionIds.length);
    for (const positionId of positionIds) {
      await updateUserPositionWithBuy(
        context,
        event,
        event.params.stakeholder,
        positionId,
        price,
        event.params.amount,
        conditionId,
      );
    }
  },
);

/**
 * Merge: the full set back into collateral. The split read backwards, so the
 * legs are disposed of at the same 1/N they were acquired at.
 */
indexer.onEvent(
  { contract: 'NegRiskAdapter', event: 'PositionsMerge' },
  async ({ event, context }) => {
    if (NEG_RISK_LIFECYCLE_NON_OWNERS.includes(event.params.stakeholder.toLowerCase())) return;

    const conditionId = event.params.conditionId;
    const positionIds = await getConditionPositionIds(context, conditionId);
    if (!positionIds.length) return;

    const price = splitNegRiskPrice(positionIds.length);
    for (const positionId of positionIds) {
      await updateUserPositionWithSell(
        context,
        event,
        event.params.stakeholder,
        positionId,
        price,
        event.params.amount,
        conditionId,
      );
    }
  },
);

/**
 * Convert: surrender NO on the questions named by `indexSet`, receive YES on
 * the rest.
 *
 * The NO legs are disposed of at their own average cost — a wash — and the YES
 * legs are acquired at the price that makes the conversion self-financing.
 * Both sides are counted against the event's question count, so a market whose
 * count is not established is skipped rather than guessed at: an undercount
 * would reject legitimate conversions and misprice every YES leg.
 */
indexer.onEvent(
  { contract: 'NegRiskAdapter', event: 'PositionsConverted' },
  async ({ event, context }) => {
    // Lower-cased once here: NegRiskEvent rows are keyed lower-case by both
    // writers (MarketPrepared, QuestionPrepared), so the lookup below has to
    // match, and getNegRiskPositionId hashes the same bytes either way.
    const marketId = event.params.marketId.toLowerCase();
    const indexSet = event.params.indexSet;
    const stakeholder = event.params.stakeholder;
    const amount = event.params.amount;

    if (NEG_RISK_LIFECYCLE_NON_OWNERS.includes(stakeholder.toLowerCase())) return;

    const negRiskEvent = await context.NegRiskEvent.get(marketId);
    if (negRiskEvent == null) {
      return;
    }
    const questionCount = negRiskEvent.questionCount;
    // TODO: to be removed once verified dead code in indexing
    if (questionCount === 0 || indexSet >> BigInt(questionCount) !== 0n) {
      context.log.warn(
        `NegRisk market=${marketId} has the invalid number ${questionCount} of questions or indexSet: ${indexSet}`,
      );
      return;
    }

    let noCnt = 0;
    let noPriceSum = 0n;

    for (let i = 0; i < questionCount; i++) {
      if (indexSetContains(indexSet, i)) {
        noCnt++;
        const positionId = getNegRiskPositionId(marketId, i, NO_INDEX);
        const userPosition = await loadUserPosition(context, stakeholder, positionId);

        await updateUserPositionWithSell(
          context,
          event,
          stakeholder,
          positionId,
          userPosition.avgPrice,
          amount,
        );

        noPriceSum += userPosition.avgPrice;
      }
    }
    if (noCnt === 0) {
      context.log.info(
        `PositionsConverted resolved no NO legs: stakeholder=${stakeholder} ` +
          `market=${marketId} indexSet=${indexSet} questions=${questionCount} ` +
          `block=${event.block.number}`,
      );
      return;
    }
    if (noCnt >= questionCount) {
      context.log.info(
        `PositionsConverted covers all ${questionCount} questions, no YES legs to price: ` +
          `stakeholder=${stakeholder} market=${marketId} indexSet=${indexSet} ` +
          `block=${event.block.number}`,
      );
      return;
    }

    const noPrice = noPriceSum / BigInt(noCnt);
    let yesPrice = computeNegRiskYesPrice(noPrice, noCnt, questionCount - noCnt);

    // TODO: to be removed once verified dead code in indexing
    if (yesPrice < 0n) {
      context.log.warn(
        `NegRisk market=${marketId} has the invalid yes price ${yesPrice}$. The total number of questions is ${questionCount} with ${noCnt} no price ${noPrice}`,
      );
      yesPrice = 0n;
    }

    for (let i = 0; i < questionCount; i++) {
      if (!indexSetContains(indexSet, i)) {
        const positionId = getNegRiskPositionId(marketId, i, YES_INDEX);
        await updateUserPositionWithBuy(
          context,
          event,
          event.params.stakeholder,
          positionId,
          yesPrice,
          event.params.amount,
        );
      }
    }
  },
);
/**
 * Redeem: a resolved market's legs settle at their payout price.
 *
 * Priced per outcome rather than from the event's aggregate payout, which
 * cannot tell a winning leg from a losing one when a wallet holds both.
 */
indexer.onEvent(
  { contract: 'NegRiskAdapter', event: 'PayoutRedemption' },
  async ({ event, context }) => {
    const conditionId = event.params.conditionId;
    const tokenIds = await getConditionPositionIds(context, conditionId);
    if (!tokenIds.length) return;

    const amounts = event.params.amounts;
    const redeemer = event.params.redeemer;

    if (NEG_RISK_REDEEM_NON_OWNERS.includes(redeemer.toLowerCase())) return;

    const prices = (await context.Condition.get(conditionId))?.payoutPrices;
    if (!prices?.length) return;
    if (tokenIds.length !== amounts.length) {
      context.log.warn(
        `Neg-risk redemption outcome mismatch: condition=${conditionId} ` +
          `table=${tokenIds.length} chain=${amounts.length}`,
      );
    }

    const outcomeCount = Math.min(tokenIds.length, amounts.length);
    for (let outcomeIndex = 0; outcomeIndex < outcomeCount; outcomeIndex++) {
      await updateUserPositionWithSell(
        context,
        event,
        redeemer,
        tokenIds[outcomeIndex],
        prices[outcomeIndex],
        amounts[outcomeIndex],
        conditionId,
      );
    }
  },
);

/**
 * A neg-risk event exists.
 *
 * Creating the row is the claim that its question count is complete: questions
 * can only be added to a market that already exists, so none are behind us.
 * It starts empty and QuestionPrepared fills it in.
 */
indexer.onEvent(
  { contract: 'NegRiskAdapter', event: 'MarketPrepared' },
  async ({ event, context }) => {
    context.NegRiskEvent.set({ id: event.params.marketId.toLowerCase(), questionCount: 0 });
  },
);

/**
 * A question was added to a neg-risk event.
 *
 * The count is RAISED to `index + 1` rather than incremented, which is
 * idempotent under replay. It is skipped entirely when no row exists: that
 * market was prepared before the window, so `index + 1` would be a lower bound
 * posing as a total.
 *
 * The question's condition is recorded unconditionally, because nothing else
 * can — the conditionId is a hash of the marketId and index, so the pairing
 * has no reverse and is lost if not written here.
 */
indexer.onEvent(
  { contract: 'NegRiskAdapter', event: 'QuestionPrepared' },
  async ({ event, context }) => {
    const questionIndex = Number(event.params.index);
    const negRiskMarketId = event.params.marketId.toLowerCase();
    const negRiskEvent = await context.NegRiskEvent.get(negRiskMarketId);

    if (negRiskEvent) {
      context.NegRiskEvent.set({
        id: negRiskMarketId,
        questionCount: Math.max(questionIndex + 1, negRiskEvent.questionCount),
      });
    }

    await upsertCondition(context, getNegRiskConditionId(negRiskMarketId, questionIndex), {
      outcomeSlotCount: 2,
      negRisk: true,
      negRiskMarketId,
      questionIndex,
    });
  },
);

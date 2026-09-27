import { indexer } from 'envio';
import { upsertCondition } from '@utils/upsertCondition';
import { applyNegRiskConversion } from '@utils/negrisk-utils';
import { getNegRiskConditionId } from '@utils/getNegRiskConditionId';
import {
  AUTO_REDEEMER,
  AUTO_REDEEMER_V1,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V1,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V2,
  NEG_RISK_EXCHANGE,
  NEG_RISK_EXCHANGE_V2,
} from '@utils/constants';
import { applyPositionMerge, applyPositionRedeem, applyPositionSplit } from '@utils/adapter-utils';

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
const NEG_RISK_LIFECYCLE_NON_OWNERS: ReadonlySet<string> = new Set<string>([
  NEG_RISK_EXCHANGE,
  NEG_RISK_EXCHANGE_V2,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V1,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V2,
]);

/**
 * Addresses that can be `redeemer` on a neg-risk redemption without owning the
 * position.
 *
 * `NEG_RISK_LIFECYCLE_NON_OWNERS` plus both auto-redeemers, which redeem on a
 * user's behalf: the payout reaches the wallet, so crediting the redeemer
 * invents a position that never existed.
 */
const NEG_RISK_REDEEM_NON_OWNERS: ReadonlySet<string> = new Set<string>([
  ...NEG_RISK_LIFECYCLE_NON_OWNERS,
  AUTO_REDEEMER,
  AUTO_REDEEMER_V1,
]);

/**
 * Split: collateral in, one outcome token per leg out.
 *
 * A full set costs the collateral and redeems for it, so each of N legs is
 * acquired at 1/N.
 */
indexer.onEvent(
  { contract: 'NegRiskAdapter', event: 'PositionSplit' },
  async ({ event, context }) => {
    const wallet = event.params.stakeholder.toLowerCase();
    if (NEG_RISK_LIFECYCLE_NON_OWNERS.has(wallet)) return;

    await applyPositionSplit(
      context,
      event,
      wallet,
      event.params.conditionId.toLowerCase(),
      event.params.amount,
    );
  },
);

/**
 * Merge: the full set back into collateral. The split read backwards, so the
 * legs are disposed of at the same 1/N they were acquired at.
 */
indexer.onEvent(
  { contract: 'NegRiskAdapter', event: 'PositionsMerge' },
  async ({ event, context }) => {
    const wallet = event.params.stakeholder.toLowerCase();
    if (NEG_RISK_LIFECYCLE_NON_OWNERS.has(wallet)) return;

    await applyPositionMerge(
      context,
      event,
      wallet,
      event.params.conditionId.toLowerCase(),
      event.params.amount,
    );
  },
);

/**
 * Convert: surrender NO on the questions named by `indexSet`, receive YES on
 * the rest. Priced by applyNegRiskConversion, the routine the gen-2 collateral
 * adapter's conversions use too, following the Data API: k >= 2 at fair value
 * (NO out at (n-1)/n, YES in at 1/n), k = 1 carrying the NO cost into the YES
 * legs. See negrisk-conversion-design.md.
 *
 * A stakeholder that is an adapter or exchange is skipped: the adapter's own
 * PositionsConverted names the real wallet and is booked from there.
 */
indexer.onEvent(
  { contract: 'NegRiskAdapter', event: 'PositionsConverted' },
  async ({ event, context }) => {
    const wallet = event.params.stakeholder.toLowerCase();
    if (NEG_RISK_LIFECYCLE_NON_OWNERS.has(wallet)) return;

    await applyNegRiskConversion(
      context,
      event,
      wallet,
      event.params.marketId.toLowerCase(),
      event.params.indexSet,
      event.params.amount,
    );
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
    const redeemer = event.params.redeemer.toLowerCase();
    if (NEG_RISK_REDEEM_NON_OWNERS.has(redeemer)) return;

    await applyPositionRedeem(
      context,
      event,
      redeemer,
      event.params.conditionId.toLowerCase(),
      event.params.amounts,
    );
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

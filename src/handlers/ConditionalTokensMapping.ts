/** CTF splits, merges and redemptions, plus condition identity and payouts. */

import {
  AUTO_REDEEMER,
  AUTO_REDEEMER_V1,
  COLLATERAL_SCALE,
  CTF_COLLATERAL_ADAPTER_V1,
  CTF_COLLATERAL_ADAPTER_V2,
  EXCHANGE,
  NEG_RISK_ADAPTER,
  NEG_RISK_EXCHANGE,
  NEG_RISK_WRAPPED_COLLATERAL,
  PUSD_IMPLEMENTATION,
  PUSD_PROXY,
  REDEMPTION_OPERATORS,
  USDC,
} from '@utils/constants';
import { type EvmOnEventContext, indexer } from 'envio';
import { getConditionPositionIds, getPositionId } from '@utils/getPositionId';
import { upsertCondition } from '@utils/upsertCondition';
import { splitNegRiskPrice } from '@utils/computeNegRiskYesPrice';
import { updateUserPositionWithBuy } from '@utils/updateUserPositionWithBuy';
import { updateUserPositionWithSell } from '@utils/updateUserPositionWithSell';
import { loadUserPosition } from '@utils/loadUserPosition';

/** Collaterals the markets table covers; no OutcomeToken row needed. */
const USDC_COLLATERAL: Set<string> = new Set([USDC, NEG_RISK_WRAPPED_COLLATERAL]);

/**
 * Split/merge operators, booked elsewhere. Load-bearing: the position
 * updaters do not filter.
 */
const SET_OPERATORS: ReadonlySet<string> = new Set<string>([
  CTF_COLLATERAL_ADAPTER_V1,
  CTF_COLLATERAL_ADAPTER_V2,
  NEG_RISK_ADAPTER,
  EXCHANGE,
  NEG_RISK_EXCHANGE,
]);

/** Redeem operators; auto-redeemers are booked in AutoRedeemerMapping. */
const REDEEM_OPERATORS: ReadonlySet<string> = new Set<string>([
  ...SET_OPERATORS,
  AUTO_REDEEMER,
  AUTO_REDEEMER_V1,
  ...REDEMPTION_OPERATORS,
]);

/** pUSD proxy and implementation; skipped, uncounted. */
const PUSD_COLLATERALS: Set<string> = new Set([PUSD_PROXY, PUSD_IMPLEMENTATION]);

/** Outcome token and its index. */
interface OutcomeToken {
  id: bigint;
  outcomeIndex: number;
}

/**
 * Outcome tokens named by `indexSets` in this collateral; empty for pUSD or an
 * unknown condition. Price by `outcomeIndex`, not array position: `indexSets`
 * can be a subset.
 */
async function getCollateralTokenIds(
  context: EvmOnEventContext,
  conditionId: string,
  collateral: string,
  indexSets: readonly bigint[],
) {
  collateral = collateral.toLowerCase();
  if (PUSD_COLLATERALS.has(collateral)) {
    return [];
  }

  const ids = await getConditionPositionIds(context, conditionId, collateral);
  if (!ids.length) return [];

  if (!USDC_COLLATERAL.has(collateral)) {
    ids.forEach((tokenId, outcomeIndex) =>
      context.OutcomeToken.set({
        id: tokenId.toString(),
        conditionId,
        outcomeIndex,
        collateral: collateral.toLowerCase(),
      }),
    );
  }

  // Bit i set = outcome i.
  const indexSet2Outcomes = (indexSet: bigint): number[] => {
    const outcomes: number[] = [];
    for (let i = 0, rest = indexSet; rest > 0n; i++, rest >>= 1n) {
      if (rest & 1n) outcomes.push(i);
    }
    return outcomes;
  };

  const outcomeTokens: OutcomeToken[] = [];
  for (const indexSet of indexSets) {
    for (const outcomeIndex of indexSet2Outcomes(indexSet)) {
      const tokenId = ids[outcomeIndex];
      if (tokenId === undefined) {
        context.log.warn(
          `outcomeIndex ${outcomeIndex} at indexSet ${indexSet.toString(2)} has 0 token out of ${ids.length}`,
        );
        continue;
      }
      outcomeTokens.push({ id: tokenId, outcomeIndex });
    }
  }
  return outcomeTokens;
}

// ── Split: each leg bought at 1/N ─────────────────────────────────
indexer.onEvent(
  { contract: 'ConditionalTokens', event: 'PositionSplit' },
  async ({ event, context }) => {
    const conditionId = event.params.conditionId;
    if (SET_OPERATORS.has(event.params.stakeholder.toLowerCase())) return;

    const tokens = await getCollateralTokenIds(
      context,
      conditionId,
      event.params.collateralToken,
      event.params.partition,
    );
    if (!tokens.length) return;

    const price = splitNegRiskPrice(tokens.length);

    for (const token of tokens) {
      await updateUserPositionWithBuy(
        context,
        event,
        event.params.stakeholder,
        token.id,
        price,
        event.params.amount,
        conditionId,
      );
    }
  },
);

// ── Merge: each leg sold at 1/N ───────────────────────────────────
indexer.onEvent(
  { contract: 'ConditionalTokens', event: 'PositionsMerge' },
  async ({ event, context }) => {
    const conditionId = event.params.conditionId;
    if (SET_OPERATORS.has(event.params.stakeholder.toLowerCase())) return;

    const tokens = await getCollateralTokenIds(
      context,
      conditionId,
      event.params.collateralToken,
      event.params.partition,
    );
    if (!tokens.length) return;

    const price = splitNegRiskPrice(tokens.length);

    for (const token of tokens) {
      await updateUserPositionWithSell(
        context,
        event,
        event.params.stakeholder,
        token.id,
        price,
        event.params.amount,
        conditionId,
      );
    }
  },
);

// ── Redemption: close at the settled price ────────────────────────
indexer.onEvent(
  { contract: 'ConditionalTokens', event: 'PayoutRedemption' },
  async ({ event, context }) => {
    const conditionId = event.params.conditionId;
    if (REDEEM_OPERATORS.has(event.params.redeemer.toLowerCase())) return;

    const tokens = await getCollateralTokenIds(
      context,
      conditionId,
      event.params.collateralToken,
      event.params.indexSets,
    );
    if (!tokens.length) return;
    const redeemer = event.params.redeemer;
    const prices = (await context.Condition.get(conditionId))?.payoutPrices;
    if (!prices?.length) {
      context.log.warn(
        `Redemption skipped, no settled prices readable: condition=${conditionId} ` +
          `redeemer=${redeemer} block=${event.block.number}`,
      );
      return;
    }

    for (const token of tokens) {
      const price = prices[token.outcomeIndex];
      if (price === undefined) {
        context.log.warn(
          `Redemption token has no settled price: condition=${conditionId} ` +
            `outcome=${token.outcomeIndex} of ${prices.length} redeemer=${redeemer}`,
        );
        continue;
      }
      const userPosition = await loadUserPosition(context, redeemer, token.id);
      await updateUserPositionWithSell(
        context,
        event,
        redeemer,
        token.id,
        price,
        userPosition.amount,
      );
    }
  },
);

// ── Preparation: outcome count and neg-risk flag (oracle) ─────────
indexer.onEvent(
  { contract: 'ConditionalTokens', event: 'ConditionPreparation' },
  async ({ event, context }) => {
    const conditionId = event.params.conditionId;
    const outcomeSlotCount = Number(event.params.outcomeSlotCount);
    const negRisk = event.params.oracle.toLowerCase() === NEG_RISK_ADAPTER;

    await upsertCondition(context, conditionId, { outcomeSlotCount, negRisk });
    const collateral = negRisk ? NEG_RISK_WRAPPED_COLLATERAL : USDC;
    for (let outcomeIndex = 0; outcomeIndex < outcomeSlotCount; outcomeIndex++) {
      context.OutcomeToken.set({
        id: getPositionId(conditionId, outcomeIndex, negRisk).toString(),
        conditionId,
        outcomeIndex,
        collateral,
      });
    }
  },
);

// ── Resolution: per-outcome settled prices, 1e6 ───────────────────
indexer.onEvent(
  { contract: 'ConditionalTokens', event: 'ConditionResolution' },
  async ({ event, context }) => {
    const conditionId = event.params.conditionId;
    const payoutNumerators = event.params.payoutNumerators;

    const payoutDenominator = payoutNumerators.reduce((a, b) => a + b, 0n);
    if (payoutDenominator === 0n) {
      context.log.warn(
        `ConditionResolution pays nothing: condition=${conditionId} outcomes=${payoutNumerators.length}`,
      );
      return;
    }

    const outcomeSlotCount = payoutNumerators.length;
    if (Number(event.params.outcomeSlotCount) !== outcomeSlotCount) {
      context.log.warn(
        `ConditionResolution contradicts itself: condition=${conditionId} ` +
          `outcomeSlotCount=${event.params.outcomeSlotCount} numerators=${outcomeSlotCount}`,
      );
    }

    const prices = payoutNumerators.map((n) => (n * COLLATERAL_SCALE) / payoutDenominator);

    const stored = await context.Condition.get(conditionId);
    if (stored && stored.outcomeSlotCount !== outcomeSlotCount) {
      context.log.warn(
        `ConditionResolution corrects a stored outcome count: condition=${conditionId} ` +
          `stored=${stored.outcomeSlotCount} chain=${outcomeSlotCount}`,
      );
    }

    await upsertCondition(context, conditionId, { outcomeSlotCount, payoutPrices: prices });
  },
);

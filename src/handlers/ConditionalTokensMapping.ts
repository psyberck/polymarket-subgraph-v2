/**
 * Position lifecycle on the Gnosis ConditionalTokens contract: splits, merges,
 * redemptions, and the condition identity and settled prices they are priced
 * against.
 *
 * Rationale and measurements for every rule here live in
 * ../conditional-tokens-mapping.md. Comments below state WHAT a rule is; that
 * file holds why, with the numbers behind it.
 */

import {
  AUTO_REDEEMER,
  COLLATERAL_SCALE,
  CTF_COLLATERAL_ADAPTER_V1,
  CTF_COLLATERAL_ADAPTER_V2,
  EXCHANGE,
  NEG_RISK_ADAPTER,
  NEG_RISK_EXCHANGE,
  NEG_RISK_WRAPPED_COLLATERAL,
  PUSD_IMPLEMENTATION,
  PUSD_PROXY,
  USDC,
} from '@utils/constants';
import { type EvmOnEventContext, indexer } from 'envio';
import { getConditionPositionIds, getPositionId } from '@utils/getPositionId';
import { upsertCondition } from '@utils/upsertCondition';
import { splitNegRiskPrice } from '@utils/computeNegRiskYesPrice';
import { updateUserPositionWithBuy } from '@utils/updateUserPositionWithBuy';
import { updateUserPositionWithSell } from '@utils/updateUserPositionWithSell';
import { loadUserPosition } from '@utils/loadUserPosition';

/** Collaterals the markets table already holds ids for; they need no
 *  OutcomeToken row for reverse lookup. */
const USDC_COLLATERAL: Set<string> = new Set([USDC, NEG_RISK_WRAPPED_COLLATERAL]);

/**
 * Contracts that split or merge on a user's behalf, so their name in
 * `stakeholder` is an operator and not an owner. Each is booked elsewhere.
 *
 * Narrower than PROTOCOL_ADDRESSES in lib/store.ts, and load-bearing:
 * updateUserPositionWithBuy/Sell do not filter. The omissions rest on
 * measurement, not construction — see the doc before adding a call site.
 */
const SET_OPERATORS: ReadonlySet<string> = new Set<string>([
  CTF_COLLATERAL_ADAPTER_V1,
  CTF_COLLATERAL_ADAPTER_V2,
  NEG_RISK_ADAPTER,
  EXCHANGE,
  NEG_RISK_EXCHANGE,
]);

/** Contracts that redeem on a user's behalf: the set operators plus
 *  AutoRedeemer, which redeems for 50.3% of events and is booked by
 *  AdapterTransfers instead. */
const REDEEM_OPERATORS: ReadonlySet<string> = new Set<string>([...SET_OPERATORS, AUTO_REDEEMER]);

/**
 * pUSD, skipped without deriving ids: a cash layer, not a CTF denomination.
 * Both entries are the same token and both appear as a collateralToken.
 *
 * NOTE this drops the NegligibleCollateralSkip counter that lib/collateral.ts
 * keeps on the handlers/ path — port it before this file goes live.
 */
const PUSD_COLLATERALS: Set<string> = new Set([PUSD_PROXY, PUSD_IMPLEMENTATION]);

/** An outcome token named by an index set, tagged with the outcome it is. */
interface OutcomeToken {
  id: bigint;
  outcomeIndex: number;
}

/**
 * Outcome tokens named by `indexSets`, denominated in this event's collateral.
 * Ids outside the markets table get an OutcomeToken row so they stay reversible.
 *
 * CALLERS MUST PRICE BY `outcomeIndex`, never by position in this array:
 * `indexSets` is a subset on 14.3% of bookable redemptions, and pairing a payout
 * array positionally settles one outcome at another's price.
 *
 * Empty when the collateral is pUSD or the condition is unknown.
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

  // Bitmap over outcomes: bit i set means outcome i is in the set.
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

// ── Split: collateral -> a full set of outcome tokens, each at 1/N ─
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

    // 1/N of the collateral per leg, independent of which outcome a leg is.
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

// ── Merge: the inverse of a split, same legs and same 1/N pricing ──
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

    // 1/N of the collateral per leg, independent of which outcome a leg is.
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

// ── Redemption: a resolved position closes at its settled price ────
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

    // By outcomeIndex, NOT by position in `tokens` — see the doc. An outcome
    // with no settled price is left open rather than closed at a made-up one.
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

// ── Condition identity: outcome count, and whether it is neg-risk ──
//
// Recorded so token ids can be DERIVED rather than looked up. `oracle` is the
// only on-chain statement that a condition is neg-risk.
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

// ── Resolution: the settled price of every outcome, 1e6 ────────────
//
// Stored per condition so redemptions can price each leg individually; the
// event's aggregate `payout` sums the legs and cannot separate them.
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

    // The event is the authority on the outcome count, and it is free — no
    // lookup, no RPC. Counted off the numerators so `prices` and the stored
    // count come from one value; the cross-check catches the chain
    // contradicting itself.
    const outcomeSlotCount = payoutNumerators.length;
    if (Number(event.params.outcomeSlotCount) !== outcomeSlotCount) {
      context.log.warn(
        `ConditionResolution contradicts itself: condition=${conditionId} ` +
          `outcomeSlotCount=${event.params.outcomeSlotCount} numerators=${outcomeSlotCount}`,
      );
    }

    const prices = payoutNumerators.map((n) => (n * COLLATERAL_SCALE) / payoutDenominator);

    // A stored count that disagrees is WRONG: correct the row rather than
    // truncating the prices to fit it.
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

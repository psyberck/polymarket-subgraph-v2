import type { EvmEvent, EvmOnEventContext } from 'envio';
import { COLLATERAL_SCALE, FIFTY_CENTS } from '@utils/constants';
import { computeNegRiskYesPrice } from '@utils/computeNegRiskYesPrice';
import { getNegRiskConditionId } from '@utils/getNegRiskConditionId';
import { getNegRiskPositionId } from '@utils/getNegRiskPositionId';
import { indexSetContains } from '@utils/indexSetContains';
import { loadUserPosition } from '@utils/loadUserPosition';
import { updateUserPositionWithBuy } from '@utils/updateUserPositionWithBuy';
import { updateUserPositionWithSell } from '@utils/updateUserPositionWithSell';

/** Outcome ordering within a neg-risk question: every one is [Yes, No]. */
const YES_INDEX = 0;
const NO_INDEX = 1;

const min = (a: bigint, b: bigint): bigint => (a < b ? a : b);
const splitMarkerId = (txHash: string, wallet: string, conditionId: string): string =>
  `${txHash}-${wallet}-${conditionId}`.toLowerCase();

/**
 * Add a gen-2 adapter split's amount to the wallet's same-tx marker on a
 * condition, for a later k == 1 conversion in the tx to price as a basket.
 *
 * Accumulates rather than overwrites: several splits of one question in one tx
 * all count. The pair of `consumeSplitMarker`.
 *
 * @param context - Handler context.
 * @param wallet - The split's initiator, lower-case.
 * @param conditionId - The split question's condition.
 * @param txHash - Transaction hash.
 * @param amount - Tokens the split minted per leg, 1e6.
 */
const recordSplitMarker = async (
  context: EvmOnEventContext,
  wallet: string,
  conditionId: string,
  txHash: string,
  amount: bigint,
): Promise<void> => {
  const id = splitMarkerId(txHash, wallet, conditionId);
  const marker = await context.SplitMarker.get(id);
  context.SplitMarker.set({ id, amount: (marker?.amount ?? 0n) + amount });
};

/**
 * Take up to `amount` from the wallet's same-tx split marker on a condition.
 *
 * Every conversion leg consumes the marker, whatever its k, so split units a
 * k >= 2 conversion already used cannot turn a later k == 1 conversion in the
 * same tx into a basket.
 *
 * @param context - Handler context.
 * @param wallet - The conversion's initiator, lower-case.
 * @param conditionId - The NO leg's condition.
 * @param txHash - Transaction hash.
 * @param amount - Tokens the leg converts, 1e6.
 * @returns The part of `amount` the marker covered; 0 when there is none.
 */
const consumeSplitMarker = async (
  context: EvmOnEventContext,
  wallet: string,
  conditionId: string,
  txHash: string,
  amount: bigint,
): Promise<bigint> => {
  const marker = await context.SplitMarker.get(splitMarkerId(txHash, wallet, conditionId));
  if (!marker) return 0n;
  const used = min(marker.amount, amount);
  const left = marker.amount - used;
  if (left > 0n) context.SplitMarker.set({ id: marker.id, amount: left });
  else context.SplitMarker.deleteUnsafe(marker.id);
  return used;
};

/**
 * Book a neg-risk conversion the way the Data API values it.
 *
 * Converting k NO legs yields YES on the other n - k questions plus
 * (k - 1) x amount of collateral. The API's rule, measured over every
 * conversion in the 2026-09-18 window (negrisk-conversion-design.md):
 *
 *   k >= 2           NO legs out at (n - 1)/n (PnL realised), YES legs in at 1/n
 *   k == 1           NO leg out at its own average cost (PnL 0),
 *                    YES legs in at that cost / (n - 1)
 *   k == 1 after a   split of the same question earlier in the same tx: a
 *   same-tx split    basket, every YES the wallet ends with at 1/n and the
 *                    split's NO buy undone
 *
 * A k == 1 conversion only partly covered by the split is booked as a basket
 * for the covered units first, then by the plain k == 1 rule for the rest, so
 * the rest prices from the NO average as it stood before the split.
 *
 * `n` is the event's question count as of this event. Sells stay capped at the
 * balance held (updateUserPositionWithSell), so a NO leg bought before the
 * indexed window disposes of nothing and, for k == 1, prices YES from a zero
 * cost: the same under-reporting the rest of the fold accepts.
 *
 * @param context - Handler context.
 * @param event - The conversion event; its block/log position stamps the rows.
 * @param wallet - Owner of the positions: the direct stakeholder, or the gen-2
 *   adapter's `initiator`.
 * @param marketId - Neg-risk market id.
 * @param indexSet - Bitmask of the questions whose NO is converted.
 * @param amount - Tokens per leg, 1e6.
 * @param totalAmount - The total number of amount expected.
 * @param txHash - The event's transaction hash, when known; enables basket
 *   detection. The direct path has no split marker to find and omits it.
 */
const applyNegRiskConversion = async (
  context: EvmOnEventContext,
  event: EvmEvent,
  wallet: string,
  marketId: string,
  indexSet: bigint,
  amount: bigint,
  totalAmount?: bigint,
  txHash?: string,
): Promise<void> => {
  const market = marketId.toLowerCase();
  const user = wallet.toLowerCase();

  const negRiskEvent = await context.NegRiskEvent.get(market);
  const n = negRiskEvent?.questionCount ?? 0;
  if (n === 0 || indexSet >> BigInt(n) !== 0n) {
    context.log.warn(
      `Conversion skipped, question count unusable: market=${market} questions=${n} ` +
        `indexSet=${indexSet} wallet=${user} block=${event.block.number}`,
    );
    return;
  }

  const noQuestions: number[] = [];
  for (let i = 0; i < n; i++) if (indexSetContains(indexSet, i)) noQuestions.push(i);
  const k = noQuestions.length;
  if (k === 0) return;

  if (totalAmount !== undefined && totalAmount !== (BigInt(k) - 1n) * amount) {
    context.log.warn(
      `Adapter conversion amountOut unexpected: wallet=${wallet} market=${marketId} ` +
        `k=${k} amount=${amount} amountOut=${totalAmount} block=${event.block.number}`,
    );
  }
  const stamps = {
    lastBlock: event.block.number,
    lastLogIndex: event.logIndex,
    lastUpdatedAt: event.block.timestamp,
  };

  const share = COLLATERAL_SCALE / BigInt(n);
  const noId = (q: number) => getNegRiskPositionId(market, q, NO_INDEX);
  const yesId = (q: number) => getNegRiskPositionId(market, q, YES_INDEX);
  const condId = (q: number) => getNegRiskConditionId(market, q);

  /** Buy `units` of YES on every question the conversion did not name. */
  const buyYes = async (price: bigint, units: bigint) => {
    for (let q = 0; q < n; q++) {
      if (noQuestions.includes(q)) continue;
      await updateUserPositionWithBuy(context, event, user, yesId(q), price, units, condId(q));
    }
  };

  /** Sell `units` of every converted NO leg. */
  const sellNo = async (price: bigint, units: bigint) => {
    for (const q of noQuestions) {
      await updateUserPositionWithSell(context, event, user, noId(q), price, units, condId(q));
    }
  };

  if (k >= 2) {
    if (txHash) {
      for (const q of noQuestions) {
        await consumeSplitMarker(context, user, condId(q), txHash, amount);
      }
    }
    await sellNo((COLLATERAL_SCALE * BigInt(n - 1)) / BigInt(n), amount);
    await buyYes(share, amount);
    return;
  }

  const q = noQuestions[0];
  const basket = txHash ? await consumeSplitMarker(context, user, condId(q), txHash, amount) : 0n;
  if (basket > 0n) {
    const no = await loadUserPosition(context, user, noId(q));
    const removed = min(no.amount, basket);
    const amountLeft = no.amount - removed;
    const totalBought = no.totalBought - removed;
    if (removed > 0n) {
      if (amountLeft === 0n && totalBought === 0n && no.realizedPnl === 0n && no.feesPaid === 0n) {
        context.UserPosition.deleteUnsafe(no.id);
      } else {
        const cost = no.avgPrice * no.amount - FIFTY_CENTS * removed;
        context.UserPosition.set({
          ...no,
          amount: amountLeft,
          avgPrice: amountLeft > 0n ? (cost > 0n ? cost / amountLeft : 0n) : no.avgPrice,
          totalBought,
          ...stamps,
        });
      }
    }

    const yes = await loadUserPosition(context, user, yesId(q));
    const repriced = min(yes.amount, basket);
    if (repriced < basket) {
      context.log.debug(
        `Basket YES short of the split: wallet=${user} condition=${condId(q)} ` +
          `held=${yes.amount} basket=${basket} tx=${txHash}`,
      );
    }
    if (repriced > 0n) {
      const cost = yes.avgPrice * yes.amount - (FIFTY_CENTS - share) * repriced;
      context.UserPosition.set({
        ...yes,
        avgPrice: cost > 0n ? cost / yes.amount : 0n,
        ...stamps,
      });
    }

    await buyYes(share, basket);
  }

  const rest = amount - basket;
  if (rest === 0n) return;

  const noCost = (await loadUserPosition(context, user, noId(q))).avgPrice;
  await sellNo(noCost, rest);
  if (n > 1) await buyYes(computeNegRiskYesPrice(noCost, 1, n - 1), rest);
};

export { applyNegRiskConversion, recordSplitMarker };

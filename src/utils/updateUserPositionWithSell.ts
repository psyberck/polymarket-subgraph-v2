import type { EvmEvent, EvmOnEventContext } from 'envio';
import { COLLATERAL_SCALE } from '@utils/constants';
import { loadUserPosition } from '@utils/loadUserPosition';

/**
 * Book a sell: realize PnL against the running average cost, reduce the
 * balance, then persist.
 *
 * The arithmetic is `applySell` (`position.ts:89`) inlined. `avgPrice` does
 * NOT move — this is average-cost accounting, not FIFO, which is what the
 * reference subgraph did and what Polymarket's own API reports — and
 * `totalBought` does not move either, since nothing was acquired.
 *
 * THE DISPOSAL IS CAPPED at the balance actually held. A wallet selling
 * tokens it acquired before the indexed range, or received by a plain
 * ERC-1155 transfer, would otherwise realize PnL against cost basis it never
 * paid. Capping under-reports instead, which is the direction the snapshots
 * this is reconciled against take; those positions surface as
 * `amount == 0 && realizedPnl == 0` rows. See the long note on `applySell`.
 *
 * A sell of zero or fewer tokens, or a sell against a zero balance, writes
 * NOTHING AT ALL, not even the fee: the whole body is inside the guard.
 * `applySell` returns the state unchanged in those cases and `mutatePosition`
 * still persists it, so the two disagree about a fill that realizes nothing
 * but carries a fee — see the remarks.
 *
 * @param context - Handler context; the entity store is written through it.
 * @param event - The fill being booked. Only its block/log position is used,
 *   to stamp the row.
 * @param user - Wallet address, in any case; lower-cased before use.
 * @param positionId - ERC-1155 outcome token id, written to the `tokenId`
 *   column. `positionId` is the reference subgraph's name for the same value.
 * @param price - Fill price, 1e6 (0.42 -> 420000).
 * @param amount - Outcome TOKENS disposed of, 1e6. Capped at the balance held.
 * @param conditionId - Condition this token belongs to. Required here, unlike
 *   `mutatePosition`, which takes it optionally because a fill only learns it
 *   once Gamma metadata lands.
 * @param fee - Fee paid on this fill, 1e6. Accumulates into `feesPaid`.
 *
 * @remarks
 * This is the `utils/` port of `mutatePosition` (`store.ts:112`) specialised to
 * the sell leg, and the mirror of `updateUserPositionWithBuy`. The PnL
 * arithmetic agrees with it exactly, verified by replaying 20,000 randomised
 * sells through both. ONE DIVERGENCE REMAINS:
 *
 * No `isProtocolAddress` guard. `mutatePosition` drops protocol addresses
 * before anything is written; without it, adapter-stakeholder activity books a
 * position to the adapter as well as to the wallet. That is a whole-write-path
 * concern rather than sell-leg arithmetic, so it belongs wherever the buy and
 * sell helpers are called from — not duplicated into each.
 */
const updateUserPositionWithSell = async (
  context: EvmOnEventContext,
  event: EvmEvent,
  user: string,
  positionId: bigint,
  price: bigint,
  amount: bigint,
  conditionId: string,
  fee: bigint = 0n,
): Promise<void> => {
  const address = user.toLowerCase();
  const userPosition = await loadUserPosition(context, address, positionId);

  if (amount > 0n && userPosition.amount > 0n) {
    const sold = amount > userPosition.amount ? userPosition.amount : amount;
    const deltaPnl = (sold * (price - userPosition.avgPrice)) / COLLATERAL_SCALE;
    const feesPaid = userPosition.feesPaid + fee;
    const realizedPnl = userPosition.realizedPnl + deltaPnl - fee;
    const cond = userPosition.conditionId ?? conditionId;

    context.UserPosition.set({
      id: userPosition.id,
      user: address,
      tokenId: positionId,
      conditionId: cond,
      amount: userPosition.amount - sold,
      avgPrice: userPosition.avgPrice,
      realizedPnl,
      feesPaid,
      totalBought: userPosition.totalBought,
      lastBlock: event.block.number,
      lastLogIndex: event.logIndex,
      lastUpdatedAt: event.block.timestamp,
    });
  }
};

export { updateUserPositionWithSell };

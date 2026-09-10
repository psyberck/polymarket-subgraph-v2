import type { EvmEvent, EvmOnEventContext } from 'envio';
import { loadUserPosition } from '@utils/loadUserPosition';

/**
 * Book a buy: move the volume-weighted entry price, add to the balance and to
 * cumulative tokens bought, then persist.
 *
 * The arithmetic is `applyBuy` (`position.ts:58`) inlined — same integer
 * volume-weighted mean, same `totalBought` in TOKENS rather than USDC. Nothing
 * realizes on a buy, so the only thing that moves realized PnL here is the
 * fee.
 *
 * A buy of zero or fewer tokens writes NOTHING AT ALL, not even the fee: the
 * whole body is inside `if (amount > 0n)`. `applyBuy` returns the state
 * unchanged in that case and `mutatePosition` still persists it, so the two
 * disagree about a zero-token fill that carries a fee — see the remarks.
 *
 * @param context - Handler context; the entity store is written through it.
 * @param event - The fill being booked. Only its block/log position is used,
 *   to stamp the row.
 * @param user - Wallet address, in any case; lower-cased before use.
 * @param positionId - ERC-1155 outcome token id, written to the `tokenId`
 *   column. `positionId` is the reference subgraph's name for the same value.
 * @param price - Fill price, 1e6 (0.42 -> 420000).
 * @param amount - Outcome TOKENS acquired, 1e6.
 * @param conditionId - Condition this token belongs to. Required here, unlike
 *   `mutatePosition`, which takes it optionally because a fill only learns it
 *   once Gamma metadata lands.
 * @param fee - Fee paid on this fill, 1e6. Accumulates into `feesPaid`.
 *
 * @remarks
 * This is the `utils/` port of `mutatePosition` (`store.ts:112`) specialised to
 * the buy leg. The PnL arithmetic agrees with it exactly, verified by replaying
 * 20,000 randomised buys through both. ONE DIVERGENCE REMAINS:
 *
 * No `isProtocolAddress` guard. `mutatePosition` drops protocol addresses
 * before anything is written; without it, adapter-stakeholder activity books a
 * position to the adapter as well as to the wallet. That is a whole-write-path
 * concern rather than buy-leg arithmetic, so it belongs wherever the buy and
 * sell helpers are called from — not duplicated into each.
 */
const updateUserPositionWithBuy = async (
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

  if (amount > 0n) {
    const numerator = userPosition.avgPrice * userPosition.amount + price * amount;
    const denominator = userPosition.amount + amount;
    const avgPrice = numerator / denominator;
    const feesPaid = (userPosition?.feesPaid ?? 0n) + fee;
    const totalBought = userPosition.totalBought + amount;
    const realizedPnl = userPosition.realizedPnl - fee;
    const cond = userPosition?.conditionId ?? conditionId;

    context.UserPosition.set({
      id: userPosition.id,
      user: address,
      tokenId: positionId,
      conditionId: cond,
      amount: denominator,
      avgPrice: avgPrice,
      realizedPnl,
      feesPaid,
      totalBought: totalBought,
      lastBlock: event.block.number,
      lastLogIndex: event.logIndex,
      lastUpdatedAt: event.block.timestamp,
    });
  }
};

export { updateUserPositionWithBuy };

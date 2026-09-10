import type { EvmEvent, EvmOnEventContext } from 'envio';
import { loadUserPosition } from '@utils/loadUserPosition';

/**
 * Book a buy: move the volume-weighted entry price, add to the balance and to
 * cumulative tokens bought, then persist.
 *
 * `avgPrice` is an integer volume-weighted mean, and `totalBought` accumulates
 * TOKENS rather than USDC — verified against the retired subgraph's own output,
 * where `total_bought` equals `amount` exactly on positions never sold.
 * Nothing realizes on a buy, so the only thing that moves realized PnL here is
 * the fee.
 *
 * A buy of zero or fewer tokens writes NOTHING AT ALL, not even the fee: the
 * whole body is inside `if (amount > 0n)`.
 *
 * ⚠️ PROTOCOL ADDRESSES ARE NOT FILTERED. Exchanges and the collateral
 * adapters appear as a party to position movement without ever holding a
 * position of their own — an adapter named as split stakeholder is the
 * load-bearing case — and booking them invents wallets that hold millions of
 * tokens. Nothing in this path drops them, so the caller must, before it gets
 * here. Same for the sell leg; it is a whole-write-path concern rather than
 * buy-leg arithmetic, which is why it is not duplicated into both.
 *
 * @param context - Handler context; the entity store is written through it.
 * @param event - The fill being booked. Only its block/log position is used,
 *   to stamp the row.
 * @param user - Wallet address, in any case; lower-cased before use.
 * @param positionId - ERC-1155 outcome token id, written to the `tokenId`
 *   column. `positionId` is the reference subgraph's name for the same value.
 * @param price - Fill price, 1e6 (0.42 -> 420000).
 * @param amount - Outcome TOKENS acquired, 1e6.
 * @param conditionId - Condition this token belongs to, when the caller knows
 *   it. Optional: split and merge events carry it, but a fill does not learn it
 *   until Gamma metadata lands, so `ExchangeMapping.ts` omits it and the column
 *   stays null until a later event supplies one.
 * @param fee - Fee paid on this fill, 1e6. Accumulates into `feesPaid`.
 */
const updateUserPositionWithBuy = async (
  context: EvmOnEventContext,
  event: EvmEvent,
  user: string,
  positionId: bigint,
  price: bigint,
  amount: bigint,
  conditionId?: string,
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

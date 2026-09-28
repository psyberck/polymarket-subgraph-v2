import type { EvmEvent, EvmOnEventContext } from 'envio';
import { COLLATERAL_SCALE } from '@utils/constants';
import { loadUserPosition } from '@utils/loadUserPosition';

/**
 * Book a buy: move the volume-weighted entry price, add to the balance and to
 * cumulative tokens bought, then persist.
 *
 * `avgPrice` is an integer volume-weighted mean and `totalBought` accumulates
 * TOKENS, not USDC -- verified against the retired subgraph, where
 * `total_bought` equals `amount` exactly on positions never sold.
 *
 * THE FEE IS PART OF THE COST BASIS, not a realized loss. A buy of zero or
 * fewer tokens writes nothing at all, not even the fee.
 *
 * @param context - Handler context; the entity store is written through it.
 * @param event - The fill; only its block/log position is used, to stamp the row.
 * @param user - Wallet address, any case; lower-cased here.
 * @param positionId - ERC-1155 outcome token id, stored as `tokenId`.
 * @param price - Fill price, 1e6 (0.42 -> 420000).
 * @param amount - Outcome TOKENS acquired, 1e6.
 * @param conditionId - Optional: a fill does not learn it until Gamma metadata
 *   lands, so the column stays null until a later event supplies one.
 * @param fee - Fee on this fill, 1e6 USDC. Folded into `avgPrice` and also kept
 *   in `feesPaid`, so adding `feesPaid` to `avgPrice * amount` double-counts.
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
  if (amount <= 0n) return;

  const address = user.toLowerCase();
  const userPosition = await loadUserPosition(context, address, positionId);
  const held = userPosition.amount + amount;
  const avgPrice =
    (userPosition.avgPrice * userPosition.amount + price * amount + fee * COLLATERAL_SCALE) / held;

  context.UserPosition.set({
    id: userPosition.id,
    user: address,
    tokenId: positionId,
    conditionId: userPosition.conditionId ?? conditionId,
    amount: held,
    avgPrice,
    realizedPnl: userPosition.realizedPnl,
    feesPaid: userPosition.feesPaid + fee,
    totalBought: userPosition.totalBought + amount,
    lastBlock: event.block.number,
    lastLogIndex: event.logIndex,
    lastUpdatedAt: event.block.timestamp,
  });
};

export { updateUserPositionWithBuy };

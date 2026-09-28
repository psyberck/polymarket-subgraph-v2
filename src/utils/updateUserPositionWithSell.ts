import type { EvmEvent, EvmOnEventContext } from 'envio';
import { COLLATERAL_SCALE } from '@utils/constants';
import { loadUserPosition } from '@utils/loadUserPosition';

/**
 * Book a sell: realize PnL against the running average cost, reduce the balance.
 *
 * - `avgPrice` and `totalBought` stay put -- average-cost, not FIFO, as the
 *   reference subgraph and Polymarket's API both do.
 * - Capped at the balance held: tokens acquired before the indexed range, or by
 *   a plain ERC-1155 transfer, would otherwise realize against basis never paid.
 *   Capping under-reports; those surface as `amount == 0 && realizedPnl == 0`.
 * - The fee reduces proceeds, not basis -- the buy leg capitalises it instead.
 * - Zero tokens, or a zero balance, writes nothing at all, not even the fee.
 *
 * @param context - Handler context; writes go through it.
 * @param event - The fill; only its block/log position is used.
 * @param user - Wallet address, any case; lower-cased here.
 * @param positionId - ERC-1155 outcome token id, stored as `tokenId`.
 * @param price - Fill price, 1e6 (0.42 -> 420000).
 * @param amount - TOKENS disposed of, 1e6; capped at the balance.
 * @param conditionId - Optional; a fill learns it only when Gamma metadata lands.
 * @param fee - Fee, 1e6 USDC; charged against PnL and kept in `feesPaid`.
 */
const updateUserPositionWithSell = async (
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
  if (userPosition.amount <= 0n) return;

  const sold = amount > userPosition.amount ? userPosition.amount : amount;
  const deltaPnl = (sold * (price - userPosition.avgPrice)) / COLLATERAL_SCALE - fee;

  context.UserPosition.set({
    id: userPosition.id,
    user: address,
    tokenId: positionId,
    conditionId: userPosition.conditionId ?? conditionId,
    amount: userPosition.amount - sold,
    avgPrice: userPosition.avgPrice,
    realizedPnl: userPosition.realizedPnl + deltaPnl,
    feesPaid: userPosition.feesPaid + fee,
    totalBought: userPosition.totalBought,
    lastBlock: event.block.number,
    lastLogIndex: event.logIndex,
    lastUpdatedAt: event.block.timestamp,
  });
};

export { updateUserPositionWithSell };

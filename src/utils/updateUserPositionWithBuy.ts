import type { EvmEvent, EvmOnEventContext } from 'envio';
import { COLLATERAL_SCALE } from '@utils/constants';
import { loadUserPosition } from '@utils/loadUserPosition';

/**
 * Book a buy: blend `avgPrice` (fee-exclusive) and `avgFeePrice`, add to
 * `amount` and `totalBought` (tokens, not USDC), and charge the fee to
 * `realizedPnl` up front.
 *
 * @param context - Handler context.
 * @param event - Source event; stamps block/log position.
 * @param user - Wallet, any case.
 * @param positionId - Outcome token id.
 * @param price - Price, 1e6.
 * @param amount - Tokens bought, 1e6.
 * @param conditionId - Used only if the row has none yet.
 * @param fee - Fee, 1e6 USDC.
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
  const avgPrice = (userPosition.avgPrice * userPosition.amount + price * amount) / held;
  const avgFeePrice =
    (userPosition.avgFeePrice * userPosition.amount + fee * COLLATERAL_SCALE) / held;

  context.UserPosition.set({
    id: userPosition.id,
    user: address,
    tokenId: positionId,
    conditionId: userPosition.conditionId ?? conditionId,
    amount: held,
    avgPrice,
    avgFeePrice,
    realizedPnl: userPosition.realizedPnl - fee,
    feesPaid: userPosition.feesPaid + fee,
    totalBought: userPosition.totalBought + amount,
    lastBlock: event.block.number,
    lastLogIndex: event.logIndex,
    lastUpdatedAt: event.block.timestamp,
  });
};

export { updateUserPositionWithBuy };

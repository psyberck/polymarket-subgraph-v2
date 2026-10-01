import type { EvmEvent, EvmOnEventContext } from 'envio';
import { COLLATERAL_SCALE } from '@utils/constants';
import { loadUserPosition } from '@utils/loadUserPosition';

/**
 * Book a sell at average cost: realize PnL against the fee-exclusive
 * `avgPrice` minus the fee, and reduce `amount`. Averages and `totalBought`
 * stay put. Capped at the balance; a zero balance writes nothing.
 *
 * @param context - Handler context.
 * @param event - Source event; stamps block/log position.
 * @param user - Wallet, any case.
 * @param positionId - Outcome token id.
 * @param price - Price, 1e6.
 * @param amount - Tokens sold, 1e6.
 * @param conditionId - Used only if the row has none yet.
 * @param fee - Fee, 1e6 USDC.
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
  const left = userPosition.amount - sold;
  const closed = left === 0n;

  context.UserPosition.set({
    id: userPosition.id,
    user: address,
    tokenId: positionId,
    conditionId: userPosition.conditionId ?? conditionId,
    amount: left,
    avgPrice: closed ? userPosition.avgPrice + userPosition.avgFeePrice : userPosition.avgPrice,
    avgFeePrice: closed ? 0n : userPosition.avgFeePrice,
    realizedPnl: userPosition.realizedPnl + deltaPnl,
    feesPaid: userPosition.feesPaid + fee,
    totalBought: userPosition.totalBought,
    lastBlock: event.block.number,
    lastLogIndex: event.logIndex,
    lastUpdatedAt: event.block.timestamp,
  });
};

export { updateUserPositionWithSell };

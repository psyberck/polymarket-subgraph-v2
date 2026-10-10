import type { EvmEvent, EvmOnEventContext } from 'envio';
import { CLOSED_POSITION_DUST, COLLATERAL_SCALE } from '@utils/constants';
import { loadUserPosition } from '@utils/loadUserPosition';

/**
 * Book a sell at average cost: realize PnL against the fee-inclusive cost
 * `avgPrice + avgFeePrice`, so the sold part's buy fees are charged, less the
 * sell fee; reduce `amount`. Averages and `totalBought` stay put. Closing the
 * position folds `avgFeePrice` into `avgPrice`. Capped at the balance; a zero
 * balance writes nothing.
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
  const cost = userPosition.avgPrice + userPosition.avgFeePrice;
  const deltaPnl = (sold * (price - cost)) / COLLATERAL_SCALE - fee;
  const left = userPosition.amount - sold;
  const closed = left < CLOSED_POSITION_DUST;

  context.UserPosition.set({
    id: userPosition.id,
    user: address,
    tokenId: positionId,
    conditionId: userPosition.conditionId ?? conditionId,
    amount: left,
    avgPrice: closed ? cost : userPosition.avgPrice,
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

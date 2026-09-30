import type { EvmEvent, EvmOnEventContext } from 'envio';
import { loadUserPosition } from '@utils/loadUserPosition';

/**
 * Book tokens received by transfer: a buy with no fee that leaves `totalBought`
 * alone, which is how the Data API shows a receiver.
 *
 * @param context - Handler context; writes go through it.
 * @param event - The transfer; only its block/log position is used.
 * @param user - Receiving wallet, any case.
 * @param positionId - ERC-1155 outcome token id.
 * @param price - Acquisition price, 1e6: the price the sender disposed at.
 * @param amount - Tokens received, 1e6.
 * @param conditionId - Optional; the token's condition when known.
 */
const updateUserPositionWithTransfer = async (
  context: EvmOnEventContext,
  event: EvmEvent,
  user: string,
  positionId: bigint,
  price: bigint,
  amount: bigint,
  conditionId?: string,
): Promise<void> => {
  if (amount <= 0n) return;

  const userPosition = await loadUserPosition(context, user, positionId);
  const held = userPosition.amount + amount;

  context.UserPosition.set({
    ...userPosition,
    conditionId: userPosition.conditionId ?? conditionId,
    amount: held,
    avgPrice: (userPosition.avgPrice * userPosition.amount + price * amount) / held,
    lastBlock: event.block.number,
    lastLogIndex: event.logIndex,
    lastUpdatedAt: event.block.timestamp,
  });
};

export { updateUserPositionWithTransfer };

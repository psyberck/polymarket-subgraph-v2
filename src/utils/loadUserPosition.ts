import type { EvmOnEventContext, UserPosition } from 'envio';
import { getUserPositionEntityId } from '@utils/getUserPositionEntityId';

/**
 * A wallet's position in a token, or a zero-valued one (not persisted) if it
 * has none. `last*` fields default to 0, not a real block. `realizedPnl` is
 * already net of fees.
 *
 * @param context - Handler context.
 * @param user - Wallet, any case.
 * @param tokenId - Outcome token id.
 * @returns The stored or default position; use `UserPosition.get()` to tell
 *   them apart.
 */
const loadUserPosition = async (
  context: EvmOnEventContext,
  user: string,
  tokenId: bigint,
): Promise<UserPosition> => {
  const userPositionEntityId = getUserPositionEntityId(user, tokenId);
  const userPosition = await context.UserPosition.get(userPositionEntityId);

  return (
    userPosition ?? {
      id: userPositionEntityId,
      user: user.toLowerCase(),
      tokenId,
      conditionId: undefined,
      amount: 0n,
      avgPrice: 0n,
      avgFeePrice: 0n,
      realizedPnl: 0n,
      feesPaid: 0n,
      totalBought: 0n,
      lastBlock: 0,
      lastLogIndex: 0,
      lastUpdatedAt: 0,
    }
  );
};

export { loadUserPosition };

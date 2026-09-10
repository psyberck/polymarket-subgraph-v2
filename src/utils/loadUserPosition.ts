import type { EvmOnEventContext, UserPosition } from 'envio';
import { getUserPositionEntityId } from '@utils/getUserPositionEntityId';

/**
 * A wallet's position in one outcome token, or a zero-valued one if it has
 * never held that token.
 *
 * Defaulting rather than returning `UserPosition | undefined` is what the
 * reference subgraph's `loadOrCreateUserPosition` does, and it is what every
 * fold path here wants: a first buy and the thousandth buy differ only in the
 * numbers they start from, so callers should not each re-derive what "no
 * position yet" means.
 *
 * NOTHING IS PERSISTED. The defaulted entity exists only in memory; a caller
 * that means to keep it must `context.UserPosition.set()` it. A caller that
 * reads and discards leaves the store untouched, which is why this is safe to
 * call on a path that may decide not to book anything.
 *
 * `lastBlock`, `lastLogIndex` and `lastUpdatedAt` default to `0` — placeholders
 * for "never written", overwritten from the event on the first `set()`. They
 * are not meaningful block numbers and must not be compared against one.
 *
 * `realizedPnl` is stored NET of every fee the position has paid. To fold a buy
 * or sell, work in gross — `realizedPnl + feesPaid` — and net the result again
 * on the way out. Folding the net value directly re-subtracts the cumulative
 * fee each time and compounds.
 *
 * @param context - The handler's envio context; the entity store is read
 *   through it, so this must be awaited inside a handler.
 * @param user - Wallet address, in any case.
 * @param tokenId - ERC-1155 outcome token id (a CTF position id).
 * @returns The stored entity, or a zero-valued one with the same id. Never
 *   `undefined`, so the two cases are indistinguishable to the caller by
 *   design — if you need to know whether the position existed, call
 *   `context.UserPosition.get()` directly.
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

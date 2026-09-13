import { NEG_RISK_WRAPPED_COLLATERAL, USDC } from '@utils/constants';
import { computePositionId } from '@utils/ctf-utils';
import type { EvmOnEventContext } from 'envio';

/**
 * The token id for one outcome of a condition, in its canonical collateral.
 *
 * Neg-risk outcomes are denominated in the wrapped collateral, everything else
 * in USDC.e — the choice the subgraph's getPositionId made, with the same name
 * and argument order.
 *
 * NOT the UserPosition entity id, which is built from a wallet and a token by
 * `getUserPositionEntityId`. The subgraph keeps both names and separates them
 * the same way.
 *
 * @param conditionId - 0x-prefixed 32-byte condition id.
 * @param outcomeIndex - Zero-based outcome.
 * @param negRisk - Whether the condition belongs to a neg-risk event.
 * @returns The position id, as the decimal-valued bigint the CTF uses.
 */
const getPositionId = (conditionId: string, outcomeIndex: number, negRisk: boolean): bigint =>
  computePositionId(negRisk ? NEG_RISK_WRAPPED_COLLATERAL : USDC, conditionId, outcomeIndex);

/**
 * Every outcome's position id for a condition, ordered by outcome index.
 *
 * Pass `collateral` when the event names one: the CTF gives each collateral its
 * own id space, so resolving a pUSD event against USDC.e ids books against
 * positions the wallet does not hold. Omit it for the canonical ids, which
 * `getPositionId` picks from `negRisk`.
 *
 * [] means the condition has no row. There is no chain fallback — `negRisk` is
 * not recoverable from a conditionId (see `loadCondition`) — so it signals an
 * incomplete store, not a quiet chain, and callers skip rather than guess.
 *
 * @param context - Handler context; the condition row is read through it.
 * @param conditionId - 0x-prefixed 32-byte condition id.
 * @param collateral - Collateral to denominate in; omit for the canonical one.
 * @returns Position ids indexed by outcome, or [] when the condition is unknown.
 */
const getConditionPositionIds = async (
  context: EvmOnEventContext,
  conditionId: string,
  collateral?: string,
): Promise<bigint[]> => {
  const condition = await context.Condition.get(conditionId);
  if (!condition) return [];

  return Array.from({ length: condition.outcomeSlotCount }, (_, outcomeIndex) =>
    collateral === undefined
      ? getPositionId(conditionId, outcomeIndex, condition.negRisk)
      : computePositionId(collateral, conditionId, outcomeIndex),
  );
};

export { getPositionId, getConditionPositionIds };

import { NEG_RISK_WRAPPED_COLLATERAL, USDC } from '@utils/constants';
import { computePositionId } from '@utils/ctf-utils';

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

export { getPositionId };

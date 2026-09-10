import { NEG_RISK_WRAPPED_COLLATERAL } from '@utils/constants';
import { computePositionId } from '@utils/ctf-utils';
import { getNegRiskConditionId } from '@utils/getNegRiskConditionId';

/**
 * The token id for one outcome of one question of a neg-risk market.
 *
 * Pure — no table, no RPC. A neg-risk question is binary by construction:
 * getConditionId derives its conditionId with an outcome slot count of 2 baked
 * in, so the question cannot have any other shape and there is nothing to look
 * up. The collateral is always the wrapped one.
 *
 * @param marketId - 0x-prefixed 32-byte neg-risk market id.
 * @param questionIndex - Zero-based question index, < 256.
 * @param outcomeIndex - 0 is YES, 1 is NO.
 * @returns The position id, as the decimal-valued bigint the CTF uses.
 */
const getNegRiskPositionId = (
  marketId: string,
  questionIndex: number,
  outcomeIndex: number,
): bigint =>
  computePositionId(
    NEG_RISK_WRAPPED_COLLATERAL,
    getNegRiskConditionId(marketId, questionIndex),
    outcomeIndex,
  );

export { getNegRiskPositionId };

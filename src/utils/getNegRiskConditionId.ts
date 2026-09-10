import { NEG_RISK_ADAPTER } from '@utils/constants';
import { getConditionId } from '@utils/getConditionId';
import { getNegRiskQuestionId } from '@utils/getNegRiskQuestionId';

/**
 * The conditionId of question `questionIndex` within a neg-risk market.
 *
 * Pure: the adapter is the oracle for every question it prepares, so the id
 * follows from the market id and the index alone. This is the only direction
 * that works — see getNegRiskQuestionId on why it cannot be inverted.
 *
 * @param marketId - 0x-prefixed 32-byte neg-risk market id.
 * @param questionIndex - Zero-based question index, < 256.
 * @returns 0x-prefixed 32-byte condition id.
 */
const getNegRiskConditionId = (marketId: string, questionIndex: number): string =>
  getConditionId(NEG_RISK_ADAPTER, getNegRiskQuestionId(marketId, questionIndex));

export { getNegRiskConditionId };

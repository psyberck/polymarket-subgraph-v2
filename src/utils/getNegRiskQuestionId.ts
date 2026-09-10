/**
 * The questionId of question `questionIndex` within a neg-risk market.
 *
 * The market id with its last byte replaced by the question index — so a
 * neg-risk event's questions occupy a contiguous run of ids, and the marketId
 * is recoverable from any of them by masking that byte back off. (Nothing here
 * does that: the conditionId those ids hash into is not invertible, so going
 * the other way needs a lookup table rather than arithmetic.)
 *
 * @param marketId - 0x-prefixed 32-byte neg-risk market id.
 * @param questionIndex - Zero-based question index. Must be < 256 — it
 *   occupies a single byte, which is what the subgraph's `u8` parameter type
 *   enforced and what TypeScript's `number` will not.
 * @returns 0x-prefixed 32-byte question id.
 */
const getNegRiskQuestionId = (marketId: string, questionIndex: number): `0x${string}` =>
  `0x${marketId.slice(2, 64)}${questionIndex.toString(16).padStart(2, '0')}`;

export { getNegRiskQuestionId };

import type { EvmOnEventContext } from 'envio';

/**
 * Questions in the neg-risk event a condition belongs to.
 *
 * A PURE READ -- row presence is the gate. MarketPrepared and QuestionPrepared
 * maintain the count in-window; seed_conditions.ts --phase negrisk supplies it
 * for events prepared before start_block. Do not add a probe back:
 * questionCount GROWS, so a chain read answers as of the tip and misprices
 * every earlier block.
 *
 * @param context - Handler context; both entity rows are read through it, so
 *   this must be awaited inside a handler.
 * @param conditionId - 0x-prefixed 32-byte condition id.
 * @returns The event's question count, or `null` -- which means UNKNOWN, NEVER
 *   "not neg-risk". Callers must keep those apart: a standalone condition and a
 *   neg-risk event of unknown size both answer null.
 */
const negRiskEventSize = async (
  context: EvmOnEventContext,
  conditionId: string,
): Promise<number | null> => {
  const condition = await context.Condition.get(conditionId);
  const marketId = condition?.negRiskMarketId;
  if (!marketId) return null;

  const event = await context.NegRiskEvent.get(marketId.toLowerCase());
  return event?.questionCount || null;
};

export { negRiskEventSize };

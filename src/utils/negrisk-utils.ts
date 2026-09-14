import type { EvmOnEventContext } from 'envio';
import { negRiskEventCountEffect } from '@effects/negRiskQuestionEffect';

const negRiskEventSize = async (
  context: EvmOnEventContext,
  conditionId: string,
): Promise<number | null> => {
  const condition = await context.Condition.get(conditionId);
  const marketId = condition?.negRiskMarketId;
  if (marketId) {
    const key = marketId.toLowerCase();
    const event = await context.NegRiskEvent.get(key);
    if (event) return event.questionCount || null;

    const cnt = await context.effect(negRiskEventCountEffect, key);
    if (!cnt) return null;

    context.NegRiskEvent.set({ id: key, questionCount: cnt });
    return cnt;
  }
  return null;
};

export { negRiskEventSize };

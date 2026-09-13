import type { Condition, EvmOnEventContext } from 'envio';
import { outcomePayoutPriceEffect } from '@effects/outcomePayoutEffect';

/** Fields a writer may set on a condition; anything omitted is left alone. */
type ConditionRec = {
  /** How many outcomes the condition has. 0 means unknown — callers skip. */
  outcomeSlotCount?: number;

  /** Whether the condition belongs to a neg-risk event. */
  negRisk?: boolean;

  /** The neg-risk event it belongs to, known only from QuestionPrepared. */
  negRiskMarketId?: string;

  /** Which question of that event, zero-based. */
  questionIndex?: number;

  /** Settled price per outcome, 1e6. Absent until the condition resolves. */
  payoutPrices?: readonly bigint[];
};

/**
 * Insert a condition row, or merge a patch into the existing one.
 *
 * Four things write a `Condition` — ConditionPreparation, QuestionPrepared, the
 * getOutcomeSlotCount probe, and a settled payout — in any order, each knowing
 * only its own fields, so a bare `.set` from any of them erases what the others
 * established. Second writes are the normal case here, not an error: a neg-risk
 * question is written by two of them.
 *
 * The merge is nullish, so a writer can overwrite with `false` or `0` but
 * cannot clear a field back to absent — `undefined` means "leave alone".
 * `outcomeSlotCount` falls back to 0, which callers already read as "unknown"
 * and skip.
 *
 * @param context - Handler context; the row is read and written through it.
 * @param conditionId - 0x-prefixed 32-byte condition id, used as the row id.
 * @param rec - Fields to establish; omitted ones keep their current value.
 * @returns The merged row as written, so callers need not read it back.
 */
const upsertCondition = async (
  context: EvmOnEventContext,
  conditionId: string,
  rec: ConditionRec,
): Promise<Condition> => {
  const cond = await context.Condition.get(conditionId);

  const condition: Condition = {
    id: conditionId,
    outcomeSlotCount: rec.outcomeSlotCount ?? cond?.outcomeSlotCount ?? 0,
    negRisk: rec.negRisk ?? cond?.negRisk ?? false,
    negRiskMarketId: rec.negRiskMarketId ?? cond?.negRiskMarketId,
    questionIndex: rec.questionIndex ?? cond?.questionIndex,
    payoutPrices: rec.payoutPrices ?? cond?.payoutPrices,
  };

  context.Condition.set(condition);
  return condition;
};

/**
 * Settled prices for a condition, by outcome index — read the row, else probe
 * once and persist.
 *
 * `null` means unknown or not yet settled, which callers read as "use merge
 * pricing". One entity read answers it: the prices sit on the `Condition` row
 * beside the outcome count, so a surrender batch costs one get per token rather
 * than one per outcome.
 *
 * @param context - Handler context; the row is read and written through it.
 * @param conditionId - 0x-prefixed 32-byte condition id.
 * @returns Settled price per outcome, 1e6, or null when there is none.
 */
const upsertPayoutPrices = async (
  context: EvmOnEventContext,
  conditionId: string,
): Promise<readonly bigint[] | null> => {
  const condition = await context.Condition.get(conditionId);
  if (!condition) return null;

  if (condition.payoutPrices?.length) return condition.payoutPrices;

  const prices = await context.effect(outcomePayoutPriceEffect, conditionId);
  if (!prices?.length) return null;

  await upsertCondition(context, conditionId, { payoutPrices: prices });
  return prices;
};

export { upsertCondition, upsertPayoutPrices };
export type { ConditionRec };

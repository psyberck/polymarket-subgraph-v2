import { createEffect, S } from 'envio';
import { outcomeSlotCountEffect } from '@effects/outcomeSlotCountEffect';
import { client } from '@lib/rpc';
import { parseAbi } from 'viem';
import { COLLATERAL_SCALE, CONDITIONAL_TOKENS } from '@utils/constants';

const ABI = parseAbi([
  'function payoutDenominator(bytes32 conditionId) view returns (uint256)',
  'function payoutNumerators(bytes32 conditionId, uint256 index) view returns (uint256)',
]);

/**
 * Settled price per outcome index, 1e6, read straight off the CTF.
 *
 * Answers for any condition regardless of when it resolved, which is what frees
 * redemptions and adapter surrenders from falling back to merge pricing — a
 * losing leg priced at 1/2 books +0.50/token where the truth is -avgPrice, a
 * sign error rather than a rounding one.
 *
 * THREE RETURNS, AND THEY MUST STAY APART:
 *   [bigint]  settled prices
 *   []        the chain says unresolved — a fact about the CONDITION
 *   null      the probe failed — a fact about the RPC
 * Callers treat the last two alike, but only `null` logs. If a memo is ever put
 * in front of this, `[]` is safe to remember and `null` is not: an earlier
 * version stored both and a single timeout pinned "unresolved" for the rest of
 * the run, reinstating the +0.50 error above.
 *
 * PROMISE.ALL IS LOAD-BEARING, not style. lib/rpc.ts's client folds calls that
 * are in flight together into one Multicall3 round trip, so 1 + outcomeCount
 * reads cost one. A `for` loop awaiting each read batches nothing.
 *
 * Read at head, not at a historical block: a resolution is final, so the answer
 * is the same at any later block and the cache key stays a plain conditionId.
 */
const outcomePayoutPriceEffect = createEffect(
  {
    name: 'outcomePayoutPrice',
    input: S.string,
    output: S.nullable(S.array(S.bigint)),
    cache: false,
    rateLimit: { calls: 5, per: 'second' },
  },
  async ({ input, context }): Promise<bigint[] | null> => {
    const outcomeCount = await context.effect(outcomeSlotCountEffect, input);
    if (!outcomeCount) return [];

    try {
      const [denominator, ...numerators] = await Promise.all([
        client.readContract({
          address: CONDITIONAL_TOKENS,
          abi: ABI,
          functionName: 'payoutDenominator',
          args: [input as `0x${string}`],
        }),
        ...Array.from({ length: outcomeCount }, (_, i) =>
          client.readContract({
            address: CONDITIONAL_TOKENS,
            abi: ABI,
            functionName: 'payoutNumerators',
            args: [input as `0x${string}`, BigInt(i)],
          }),
        ),
      ]);

      if (denominator === 0n) return [];
      return numerators.map((numerator) => (numerator * COLLATERAL_SCALE) / denominator);
    } catch (err) {
      context.log.warn(
        `payoutNumerators/payoutDenominator failed for ${input}: ${String(err).split('\n')[0]}`,
      );
      return null;
    }
  },
);

export { outcomePayoutPriceEffect };

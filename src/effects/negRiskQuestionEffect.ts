import { createEffect, S } from 'envio';
import { client } from '@lib/rpc';
import { getAddress, parseAbi } from 'viem';
import { NEG_RISK_ADAPTER } from '@utils/constants';

const ABI = parseAbi(['function getQuestionCount(bytes32 marketId) view returns (uint256)']);

/**
 * `getQuestionCount` for a neg-risk event prepared before the indexed window.
 *
 * The count is how many legs a neg-risk split produces, which is what prices
 * each leg at 1/N — see negRiskEventSize in lib/conditions.ts.
 *
 * `null` MEANS UNKNOWN, NEVER "NOT NEG-RISK". Callers must keep those apart:
 * collapsing them makes a failed probe look like a standalone market and books
 * an 11-question leg at 1/2 instead of 1/11, a 5.5x cost-basis error.
 *
 * `cache: false` for the same reason as outcomeSlotCountEffect: a cached null
 * would pin that failure for the whole run.
 */
const negRiskEventCountEffect = createEffect(
  {
    name: 'negRiskQuestionCount',
    input: S.string,
    output: S.nullable(S.int32),
    cache: false,
    rateLimit: { calls: 5, per: 'second' },
  },
  async ({ input, context }): Promise<number | null> => {
    try {
      const count = await client.readContract({
        address: getAddress(NEG_RISK_ADAPTER),
        abi: ABI,
        functionName: 'getQuestionCount',
        args: [input as `0x${string}`],
      });
      return Number(count);
    } catch (err) {
      context.log.warn(`getQuestionCount failed for ${input}: ${String(err).split('\n')[0]}`);
      return null;
    }
  },
);

export { negRiskEventCountEffect };

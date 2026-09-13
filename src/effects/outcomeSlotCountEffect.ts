import { createEffect, S } from 'envio';
import { client } from '@lib/rpc';
import { CONDITIONAL_TOKENS } from '@utils/constants';
import { getAddress, parseAbi } from 'viem';

const ABI = parseAbi(['function getOutcomeSlotCount(bytes32 conditionId) view returns (uint256)']);

/**
 * `getOutcomeSlotCount` for a condition prepared before the indexed window.
 *
 * `null` is an RPC failure, `0` a condition the CTF has never heard of. Callers
 * read both as "no ids", but only the first is worth logging.
 *
 * `cache: false` IS DELIBERATE. A nullable output caches the null too, so one
 * timeout would pin "unknown" for the rest of the run — and envio inserts a
 * null-id cache row that kills the write batch. Successful answers are persisted
 * as `Condition` rows instead: survives restarts, cannot pin a failure.
 *
 * The 5/s limit is the hot path's budget — handlers `await` this, so the queue
 * backs up behind log fetching if it is raised carelessly.
 */
const outcomeSlotCountEffect = createEffect(
  {
    name: 'outcomeSlotCount',
    input: S.string,
    output: S.nullable(S.int32),
    cache: false,
    rateLimit: { calls: 5, per: 'second' },
  },
  async ({ input, context }): Promise<number | null> => {
    try {
      const count = await client.readContract({
        address: getAddress(CONDITIONAL_TOKENS),
        abi: ABI,
        functionName: 'getOutcomeSlotCount',
        args: [input as `0x${string}`],
      });
      return Number(count);
    } catch (err) {
      context.log.warn(`getOutcomeSlotCount failed for ${input}: ${String(err).split('\n')[0]}`);
      return null;
    }
  },
);

export { outcomeSlotCountEffect };

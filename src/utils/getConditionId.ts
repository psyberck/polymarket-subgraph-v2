import { encodePacked, getAddress, keccak256 } from 'viem';

/**
 * The conditionId for a question, as the CTF derives it.
 *
 * `keccak256(oracle ‖ questionId ‖ outcomeSlotCount)`, SPECIALISED TO TWO
 * OUTCOME SLOTS — the same specialisation the subgraph's getConditionId makes,
 * and for the same reason: every condition addressed this way is a neg-risk
 * question, which is binary by construction. A condition with any other slot
 * count hashes differently and this will not find it.
 *
 * @param oracle - The oracle that prepared the condition.
 * @param questionId - 0x-prefixed 32-byte question id.
 * @returns 0x-prefixed 32-byte condition id.
 */
const getConditionId = (oracle: string, questionId: string): `0x${string}` =>
  keccak256(
    encodePacked(
      ['address', 'bytes32', 'uint256'],
      [getAddress(oracle), questionId as `0x${string}`, 2n],
    ),
  );

export { getConditionId };

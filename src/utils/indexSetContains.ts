/**
 * Whether question `index` is a member of an index-set bitmask.
 *
 * A neg-risk conversion names the questions it covers as a bitmask, one bit
 * per question, bit `i` set meaning question `i` is in the set. Shifting the
 * bit of interest down to position 0 and masking it is equivalent to the
 * `indexSet & (1 << index)` formulation the contract uses, but keeps the
 * result a boolean rather than a wide bigint.
 *
 * @param indexSet - The conversion's bitmask, as emitted by `PositionsConverted`.
 * @param index - Zero-based question index to test. Must be a non-negative
 *   integer — the caller guarantees it, iterating `0 <= i < questionCount`.
 *   A negative `index` would not throw: BigInt `>>`
 *   with a negative count shifts the other way, so the answer would silently
 *   be about the wrong bit.
 * @returns `true` when bit `index` is set. An `index` past the mask's highest
 *   set bit is simply not a member, so it returns `false` rather than failing.
 *
 * @example
 * ```ts
 * const indexSet = 0b00010000n; // question 4 only
 *
 * indexSetContains(indexSet, 4); // (indexSet >> 4n) & 1n === 1n  -> true
 * indexSetContains(indexSet, 2); // (indexSet >> 2n) & 1n === 0n  -> false
 * ```
 */
const indexSetContains = (indexSet: bigint, index: number): boolean => {
  return ((indexSet >> BigInt(index)) & 1n) === 1n;
};

export { indexSetContains };

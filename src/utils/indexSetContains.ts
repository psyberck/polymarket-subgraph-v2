// ex: indexSet = 00010000
// if questionIndex = 4, then 1 << 4 = 00010000
// so indexSet AND (1 << questionIndex) = 00010000 > 0
// if questionIndex = 2, then 1 << 2 = 00000100
// so indexSet AND (1 << questionIndex) = 00000000 == 0

/** Whether question `index` is a member of an index-set bitmask. */
const indexSetContains = (indexSet: bigint, index: number): boolean => {
  return ((indexSet >> BigInt(index)) & 1n) === 1n;
};

export { indexSetContains };

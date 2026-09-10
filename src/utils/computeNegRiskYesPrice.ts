import { COLLATERAL_SCALE } from '@utils/constants';

/**
 * YES price implied by a neg-risk conversion, 1e6.
 *
 * Converting NO across `noCount` questions mints YES for the rest. The NO legs
 * are disposed of at their own average cost (a wash); the YES legs are acquired
 * at the price that makes the conversion self-financing:
 *
 *     yesPrice = (noPrice * noCount - SCALE * (noCount - 1)) / yesCount
 *
 * NOT floored at zero. A cheap enough NO average drives the numerator negative,
 * and a negative acquisition price corrupts `avgPrice` in the fold —
 * `computeNegRiskYesPrice(100n, 3, 2)` is `-999_850n`. Clamp at the call site if
 * the fold cannot take one.
 *
 * @param noPrice - Average cost of the NO legs, 1e6.
 * @param noCount - Questions the conversion covers.
 * @param yesCount - Questions remaining (`questionCount - noCount`), already
 *   subtracted by the caller. Must be > 0: a conversion covering every question
 *   mints no YES legs, so there is nothing to price, and the caller has to
 *   reject it before getting here.
 * @returns Price per minted YES leg, 1e6. NOT floored.
 */
const computeNegRiskYesPrice = (noPrice: bigint, noCount: number, yesCount: number): bigint =>
  (noPrice * BigInt(noCount) - COLLATERAL_SCALE * BigInt(noCount - 1)) / BigInt(yesCount);

/**
 * Per-leg price of splitting collateral into a full outcome set, 1e6.
 *
 * X collateral mints X of every leg and the set redeems for X, so each of
 * `count` legs costs 1/count. Merges read the identity backwards and use it too.
 *
 * The remainder is dropped rather than distributed, so a set can cost less than
 * the 1e6 it redeems for: 1 micro-unit short at count 3 or 7, 4 at 6.
 * Distributing it would move the binary path off the reference fold.
 *
 * Nothing here is neg-risk specific despite the name — a neg-risk question is
 * binary, so `count` is 2 and the result is `FIFTY_CENTS`, which is what the
 * reference hardcodes. The generic form is kept for the 39 non-binary markets
 * measured 2026-08-31 (3 to 7 outcomes), which 0.50 would misprice by 2x or more.
 *
 * @param count - Legs in the set. Must be > 0: BigInt division by zero throws
 *   rather than returning a sentinel, so guard whenever the count is derived
 *   from decoded event data.
 * @returns Price per leg, 1e6.
 */
const splitNegRiskPrice = (count: number): bigint => COLLATERAL_SCALE / BigInt(count);

export { computeNegRiskYesPrice, splitNegRiskPrice };

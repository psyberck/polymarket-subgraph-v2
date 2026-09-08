import { COLLATERAL_SCALE } from '@utils/constants';

/**
 * YES price implied by a neg-risk conversion, in 1e6 fixed point.
 *
 * Converting NO tokens across `noCount` of a market's questions mints YES
 * tokens for the remaining questions. The NO legs are disposed of at their own
 * average cost (a wash), and the YES legs are acquired at the price that makes
 * the conversion self-financing:
 *
 * ```text
 * yesPrice = (noPrice * noCount - SCALE * (noCount - 1)) / yesCount
 * ```
 *
 * Integer division truncates toward zero, matching the reference fold.
 *
 * @param noPrice - Average cost of the NO legs being converted, 1e6.
 * @param noCount - How many of the market's questions the conversion covers.
 * @param yesCount - How many questions remain, i.e. `questionCount - noCount`.
 *   Already subtracted at the call site; the reference instead takes
 *   `questionCount` and subtracts internally. Must be greater than zero — the
 *   caller guarantees it, by rejecting a conversion that covers every question
 *   in the market before pricing it (`NegRiskAdapter.ts:236-243`). No YES legs
 *   would be minted in that case, so there is nothing to price.
 * @returns Acquisition price for each minted YES leg, 1e6. Not floored — see
 *   the remarks.
 *
 * @remarks
 * Not interchangeable with `negRiskYesPrice` in `lib/position.ts`. The
 * arithmetic is identical, but the result here is not clamped to zero. A low
 * enough average NO price drives the numerator negative, and a negative
 * acquisition price corrupts `avgPrice` in the fold. `negRiskYesPrice` floors
 * at free; this does not, so `scripts/fold_parity.ts` would fail against the
 * reference if this were substituted for it.
 *
 * @example
 * ```ts
 * // 3 of 5 questions converted at an average NO price of $0.90.
 * computeNegRiskYesPrice(900_000n, 3, 2); // 350_000n — $0.35 per YES leg
 *
 * // Cheap NO legs push the price below zero rather than clamping.
 * computeNegRiskYesPrice(100n, 3, 2); // -999_850n
 * ```
 */
const computeNegRiskYesPrice = (noPrice: bigint, noCount: number, yesCount: number): bigint =>
  (noPrice * BigInt(noCount) - COLLATERAL_SCALE * BigInt(noCount - 1)) / BigInt(yesCount);

export { computeNegRiskYesPrice };

/**
 * Position cost-basis fold.
 *
 * This is the piece the retired pnl-subgraph used to do for us: collapse a
 * stream of fills, splits, merges and redemptions into per-(user, token)
 * avg_price / realized_pnl / total_bought.
 *
 * The arithmetic here is a port of enviodev/polymarket-indexer
 * (src/utils/pnl.ts), which is itself a port of Polymarket's pnl-subgraph.
 * Parity with that fold is the point — where a different formulation would be
 * more precise, parity wins, because these numbers are compared against
 * snapshots produced by the original subgraph.
 *
 * All values are integer micro-units (1e6), matching USDC_SCALE in config.py.
 * Integer math throughout — a float fold over hundreds of millions of events
 * accumulates visible drift, and these numbers feed wallet scoring.
 */

export const USDC_SCALE = 1_000_000n;

export interface PositionState {
  amount: bigint;
  avgPrice: bigint;
  realizedPnl: bigint;
  totalBought: bigint;
}

export const EMPTY: PositionState = {
  amount: 0n,
  avgPrice: 0n,
  realizedPnl: 0n,
  totalBought: 0n,
};

/**
 * Per-token price implied by the two legs of a fill, 1e6.
 *
 * Prices are derived once and then carried through the fold, rather than
 * letting each call divide its own way. The truncation happens here and only
 * here, which is what makes the result match the reference fold bit for bit.
 */
export function getFillPrice(tokens: bigint, usdc: bigint): bigint {
  return tokens > 0n ? (usdc * USDC_SCALE) / tokens : 0n;
}

/**
 * Acquire `tokens` at `price`. Moves the volume-weighted average entry price
 * and adds to cumulative tokens bought. Used for buys and for the acquisition
 * leg of a split.
 *
 * `totalBought` accumulates TOKENS, not USDC. Verified against the real
 * subgraph output in res/positions/2026-05.parquet: on positions that were
 * never sold, total_bought equals amount exactly, and equals amount*avg_price
 * essentially never. The comment on database/schema.sql:269 ("cumulative USDC
 * spent buying") is wrong; roi = Σ pnl / Σ total_bought has always been
 * denominated in shares.
 */
export function applyBuy(s: PositionState, price: bigint, amount: bigint): PositionState {
  if (amount <= 0n) {
    console.warn(`applyBuy(${price}, ${amount}) amount is a negative number`);
    return s;
  }

  const newAmount = s.amount + amount;
  return {
    avgPrice: newAmount > 0n ? (s.avgPrice * s.amount + price * amount) / newAmount : 0n,
    amount: newAmount,
    realizedPnl: s.realizedPnl,
    totalBought: s.totalBought + amount,
  };
}

/**
 * Dispose of `tokens` at `price`. Realizes PnL against the running average
 * cost; avgPrice itself is unchanged (average-cost accounting, not FIFO —
 * this is what the subgraph did and what Polymarket's own API reports).
 * Used for sells, for the disposal leg of a merge, and for redemptions.
 *
 * The disposal is CAPPED at the balance actually held, and a disposal against
 * a zero balance is dropped entirely. That is a deliberate change from the
 * earlier behaviour here, which let balances go negative to signal "we saw a
 * sell without its buy" (opening trade predates the indexed range, or the
 * tokens arrived by plain ERC-1155 transfer). The reference fold caps instead,
 * and capping is what the snapshots we reconcile against contain: an
 * uncapped sell books PnL on cost basis the wallet never paid, which is worse
 * than under-reporting. Positions from before the start block are visible as
 * `amount == 0 && realizedPnl == 0` rows instead.
 */
export function applySell(s: PositionState, price: bigint, amount: bigint): PositionState {
  if (amount <= 0n || s.amount <= 0n) {
    console.warn(`applySell(${price}, ${amount}), current position amount is ${s.amount}`);
    return s;
  }

  const sold = amount > s.amount ? s.amount : amount;
  return {
    amount: s.amount - sold,
    avgPrice: s.avgPrice,
    realizedPnl: s.realizedPnl + (sold * (price - s.avgPrice)) / USDC_SCALE,
    totalBought: s.totalBought,
  };
}

/**
 * Per-token price implied by splitting/merging a full outcome set.
 *
 * Splitting X collateral yields X of every outcome token, and the complete
 * set redeems for X — so the set costs X and each of N legs is priced at
 * 1/N. For a binary market that is 0.50, which is the FIFTY_CENTS constant
 * the reference hardcodes.
 *
 * The generic form is kept because non-binary conditions are not hypothetical.
 * Measured 2026-08-31 over the 2,755,558 markets carrying tokens: 39 are not
 * binary — 14 with 3 outcomes, 6 with 4, 9 with 5, 8 with 6 and 2 with 7.
 * An earlier version of this comment asserted that every row was binary and
 * used that to justify the generic form as pure caution; it was true of a
 * smaller table and stopped being true without anything failing. Hardcoding
 * 0.50 would misprice every leg of those 39 by 2x or more.
 *
 * N does not divide 1e6 evenly for N of 3, 6 or 7, and the remainder is
 * dropped rather than distributed across legs: a set costs up to N-1
 * micro-units less than the 1,000,000 it redeems for (4 at N=6, the worst case
 * present). Left as is deliberately — spreading the remainder would change the
 * binary path, which is where essentially all volume is, and that path has to
 * stay bit-for-bit with the reference fold.
 */
export function splitLegPrice(numOutcomes: number): bigint {
  return USDC_SCALE / BigInt(numOutcomes);
}

/**
 * YES price implied by a neg-risk conversion, 1e6.
 *
 * Converting NO tokens across `noCount` of a market's `questionCount`
 * questions mints YES tokens for the remaining questions. The NO legs are
 * disposed of at their own average cost (a wash), and the YES legs are
 * acquired at the price that makes the conversion self-financing:
 *
 *   yesPrice = (noPrice * noCount - SCALE * (noCount - 1)) / (questionCount - noCount)
 *
 * Ported verbatim from the reference, including the clamp: a low average NO
 * price can drive the numerator negative, and a negative acquisition price
 * would corrupt avgPrice, so it floors at free.
 */
export function negRiskYesPrice(noPrice: bigint, noCount: number, yesCount: number): bigint {
  if (yesCount <= 0) return 0n;

  const price = (noPrice * BigInt(noCount) - USDC_SCALE * BigInt(noCount - 1)) / BigInt(yesCount);
  return price < 0n ? 0n : price;
}

// ── Order side ─────────────────────────────────────────────────────
//
// v2 OrderFilled carries `side` directly (v1 required inferring direction
// from which asset id was zero). `side` describes the MAKER's order.
//
// The values are the contract's own encoding, so decodeFill can compare a raw
// event param against them. `Side` narrows that to the two the protocol
// defines, for callers naming a direction rather than reading one off an event.
export const SIDE_BUY = 0;
export const SIDE_SELL = 1;
export type Side = typeof SIDE_BUY | typeof SIDE_SELL;

/**
 * Resolve one OrderFilled into what the MAKER did.
 *
 * side == BUY:  maker pays makerAmountFilled USDC for takerAmountFilled tokens.
 * side == SELL: maker gives makerAmountFilled tokens for takerAmountFilled USDC.
 *
 * Only the maker is booked. `taker` on a Polymarket fill is the operator
 * settling the match, not a counterparty taking the other side of it — a
 * genuine counterparty gets its own OrderFilled, so booking `taker` as well
 * both invents positions for protocol addresses and double-counts every
 * trade that has one. The reference is maker-only for the same reason, and so
 * was the subgraph.
 */
export function decodeFill(
  side: number,
  makerAmountFilled: bigint,
  takerAmountFilled: bigint,
): { tokens: bigint; usdc: bigint; isBuy: boolean } {
  return side === SIDE_BUY
    ? { tokens: takerAmountFilled, usdc: makerAmountFilled, isBuy: true }
    : { tokens: makerAmountFilled, usdc: takerAmountFilled, isBuy: false };
}

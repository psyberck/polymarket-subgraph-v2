import { TradeType } from '@utils/constants';
import type { EvmEvent } from 'envio';

/** One side of a filled order, normalised into base/quote terms. */
type Order = {
  /** The maker's address, exactly as emitted — not lower-cased. */
  account: string;

  /** Direction of the maker's order. */
  side: TradeType;

  /** Outcome tokens moved. */
  baseAmount: bigint;

  /** Collateral moved, 1e6. */
  quoteAmount: bigint;

  /** Outcome token id the fill is against. */
  positionId: bigint;
};

type OrderFilled = EvmEvent<'CTFExchangeV2', 'OrderFilled'>;

/**
 * Resolve one v2 `OrderFilled` into what the MAKER did.
 *
 * The event reports two amounts without saying which is collateral; `side`
 * decides. Both legs are named from the maker's perspective:
 *
 * ```text
 * side   baseAmount          quoteAmount         meaning
 * BUY    takerAmountFilled   makerAmountFilled   maker pays USDC for tokens
 * SELL   makerAmountFilled   takerAmountFilled   maker gives tokens for USDC
 * ```
 *
 * v2 carries a single `tokenId`, so `positionId` is the same either way — the
 * maker/taker asset-id pair is a v1 shape and has no equivalent here.
 *
 * Only the maker is booked. `taker` is the operator settling the match, not a
 * counterparty taking the other side — a genuine counterparty emits its own
 * `OrderFilled`, so booking `taker` as well both invents positions for protocol
 * addresses and double-counts every trade that has one. The reference subgraph
 * is maker-only for the same reason.
 *
 * `side` is a `uint8` on the wire and reaches us as a `bigint`, so it is
 * compared against `0n`. Comparing against `0` is always false and would book
 * every fill as a SELL, inverting the legs on every buy.
 *
 * `account` is passed through unmodified. Consumers that key on it need their
 * own `.toLowerCase()`: the `OrderFilled` row lower-cases where it is written,
 * and the position helpers lower-case before building the entity id.
 *
 * @param event - A decoded `CTFExchangeV2.OrderFilled` log.
 * @returns The maker's side of the fill. Never the taker's.
 */
const parseOrderFilled = (event: OrderFilled): Order => {
  const { maker, side, tokenId, makerAmountFilled, takerAmountFilled } = event.params;

  const isBuy = side === 0n;

  return {
    account: maker,
    side: isBuy ? TradeType.BUY : TradeType.SELL,
    baseAmount: isBuy ? takerAmountFilled : makerAmountFilled,
    quoteAmount: isBuy ? makerAmountFilled : takerAmountFilled,
    positionId: tokenId,
  };
};

export { parseOrderFilled };
export type { Order, OrderFilled };

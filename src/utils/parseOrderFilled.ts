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
 * @param event - A decoded `CTFExchangeV2.OrderFilled` log.
 * @returns The maker's side of the fill. Never the taker's — see the remarks.
 *
 * @remarks
 * Only the maker is booked. `taker` is the operator settling the match, not a
 * counterparty taking the other side — a genuine counterparty emits its own
 * `OrderFilled`, so booking `taker` as well both invents positions for
 * protocol addresses and double-counts every trade that has one. The reference
 * indexer and the retired subgraph are maker-only for the same reason.
 *
 * `side` is a `uint8` on the wire and reaches us as a `bigint`, so it is
 * compared against `0n`. Comparing against `0` is always false and would book
 * every fill as a SELL, inverting the legs on every buy.
 *
 * Same decode as `decodeFill` in `lib/position.ts`, which is what the handler
 * calls today: `baseAmount` is its `tokens`, `quoteAmount` its `usdc`, and
 * `side` its `isBuy`. This adds `account` and `positionId`, which that one
 * leaves to the caller to pull off the event.
 *
 * `account` is passed through unmodified. Consumers that key on it need their
 * own `.toLowerCase()` — `Fill.maker` and `mutatePosition` (`store.ts:107`)
 * both do this themselves.
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

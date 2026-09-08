import { TradeType } from '@utils/constants';
import type { EvmEvent } from 'envio';

type Order = {
  account: string;
  side: TradeType;
  baseAmount: bigint;
  quoteAmount: bigint;
  positionId: bigint;
};

type OrderFilled = EvmEvent<'CTFExchangeV2', 'OrderFilled'>;

/**
 * Resolve one v2 OrderFilled into what the MAKER did.
 *
 * side == BUY:  maker pays makerAmountFilled USDC for takerAmountFilled tokens.
 * side == SELL: maker gives makerAmountFilled tokens for takerAmountFilled USDC.
 *
 * Only the maker is booked. `taker` is the operator settling the match, not a
 * counterparty taking the other side — a genuine counterparty emits its own
 * OrderFilled. See the note on decodeFill in lib/position.ts.
 *
 * v2 carries a single `tokenId`; the maker/taker asset-id pair is a v1 shape.
 * Which leg is collateral is told by `side` alone, so only the amounts swap.
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
export type { Order };

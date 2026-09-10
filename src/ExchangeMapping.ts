/**
 * Trade fills -> position cost basis. Subgraph-shaped port of
 * `handlers/CTFExchangeV2.ts`.
 *
 * Only the maker is booked. `taker` on a Polymarket fill is the operator
 * settling the match rather than the other side of it — a real counterparty
 * emits its own OrderFilled — so booking both sides double-counts matched
 * trades and invents positions for protocol addresses. `parseOrderFilled`
 * enforces that, the same way `decodeFill` does for the handler.
 */

import { indexer } from 'envio';
import { COLLATERAL_SCALE, TradeType } from '@utils/constants';
import { parseOrderFilled } from '@utils/parseOrderFilled';
import { updateUserPositionWithBuy } from '@utils/updateUserPositionWithBuy';
import { updateUserPositionWithSell } from '@utils/updateUserPositionWithSell';

// ── OrderFilled: one fill -> one raw row, one position mutation ────
//
// `parseOrderFilled` resolves the event into what the MAKER did: `baseAmount`
// is always outcome tokens and `quoteAmount` always collateral, whichever way
// round the legs were emitted, so nothing below has to re-check `side`.
//
// handlers/CTFExchangeV2.ts registers this same contract and event. Only one
// of the two may ever be loaded — both, and every fill is booked twice.
indexer.onEvent({ contract: 'CTFExchangeV2', event: 'OrderFilled' }, async ({ event, context }) => {
  const order = parseOrderFilled(event);
  const isBuy = order.side === TradeType.BUY;

  const price =
    order.baseAmount > 0n ? (order.quoteAmount * COLLATERAL_SCALE) / order.baseAmount : 0n;

  context.OrderFilled.set({
    id: `${event.block.number}-${event.logIndex}`,
    orderHash: event.params.orderHash,
    maker: event.params.maker.toLowerCase(),
    taker: event.params.taker.toLowerCase(),
    exchange: event.srcAddress.toLowerCase(),
    tokenId: event.params.tokenId,
    side: Number(event.params.side),
    makerAmountFilled: event.params.makerAmountFilled,
    takerAmountFilled: event.params.takerAmountFilled,
    fee: event.params.fee,
    tokens: order.baseAmount,
    usdc: order.quoteAmount,
    price,
    isBuy,
    blockNumber: event.block.number,
    logIndex: event.logIndex,
    timestamp: event.block.timestamp,
  });

  const mutate = isBuy ? updateUserPositionWithBuy : updateUserPositionWithSell;
  await mutate(
    context,
    event,
    order.account,
    order.positionId,
    price,
    order.baseAmount,
    undefined,
    event.params.fee,
  );
});

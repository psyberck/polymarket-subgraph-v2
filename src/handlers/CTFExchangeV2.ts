/**
 * Trade fills -> position cost basis.
 *
 * Only the maker is booked. `taker` on a Polymarket fill is the operator
 * settling the match rather than the other side of it — a real counterparty
 * emits its own OrderFilled — so booking both sides double-counts matched
 * trades and invents positions for protocol addresses. The reference indexer
 * and the retired subgraph are both maker-only; see decodeFill.
 */

import { indexer } from 'envio';
import { applyBuy, applySell, decodeFill, fillPrice } from '@lib/position';
import { mutatePosition } from '@lib/store';

indexer.onEvent({ contract: 'CTFExchangeV2', event: 'OrderFilled' }, async ({ event, context }) => {
  const { tokens, usdc, isBuy } = decodeFill(
    Number(event.params.side),
    event.params.makerAmountFilled,
    event.params.takerAmountFilled,
  );

  const price = tokens > 0n ? fillPrice(tokens, usdc) : 0n;

  context.Fill.set({
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
    tokens,
    usdc,
    price,
    isBuy,
    blockNumber: event.block.number,
    logIndex: event.logIndex,
    timestamp: event.block.timestamp,
  });

  if (tokens === 0n) return;

  // The fee rides on the fill and is accumulated by mutatePosition, which
  // expenses it against realized PnL only once the position realizes. It is
  // deliberately NOT part of avgPrice or totalBought — the Data API's
  // avgPrice matched ours exactly while fee-free.
  await mutatePosition(
    context,
    event,
    event.params.maker,
    event.params.tokenId,
    undefined,
    (s) => (isBuy ? applyBuy(s, price, tokens) : applySell(s, price, tokens)),
    event.params.fee,
  );
});

/**
 * Trade fills -> position cost basis. Subgraph-shaped port of
 * `handlers/CTFExchangeV2.ts`.
 *
 * Only the maker is booked. `taker` on a Polymarket fill is the operator
 * settling the match rather than the other side of it — a real counterparty
 * emits its own OrderFilled — so booking both sides double-counts matched
 * trades and invents positions for protocol addresses. `parseOrderFilled`
 * enforces that.
 */

import { indexer } from 'envio';
import {
  AUTO_REDEEMER,
  AUTO_REDEEMER_V1,
  COLLATERAL_SCALE,
  COMBOS_EXCHANGE,
  COMBOS_POSITION_MANAGER,
  EXCHANGE,
  EXCHANGE_V2,
  EXCHANGE_V2_THIRD,
  NEG_RISK_EXCHANGE,
  NEG_RISK_EXCHANGE_V2,
  TradeType,
} from '@utils/constants';
import { parseOrderFilled } from '@utils/parseOrderFilled';
import { updateUserPositionWithBuy } from '@utils/updateUserPositionWithBuy';
import { updateUserPositionWithSell } from '@utils/updateUserPositionWithSell';

/**
 * Addresses that can be `maker` on a fill without owning the position.
 *
 * Mostly historical: this guard existed because the exchanges appeared as the
 * TAKER on fills — 4,291 of 33,788 positions, 12.7%, on a 3,200-block sample —
 * and fills are booked maker-only now, so that pathway is largely gone. It
 * stays because the AutoRedeemer and the Combos contracts can still act as
 * maker on a user's behalf, and because a backfill reaching before the v2
 * cutover sees the v1 exchanges as maker directly.
 */
const ORDER_FILLED_NON_USER_ADDRESSES: string[] = [
  AUTO_REDEEMER,
  AUTO_REDEEMER_V1,
  COMBOS_POSITION_MANAGER,
  COMBOS_EXCHANGE,
  EXCHANGE,
  NEG_RISK_EXCHANGE,
  EXCHANGE_V2,
  NEG_RISK_EXCHANGE_V2,
  EXCHANGE_V2_THIRD,
] as const;

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
    blockNumber: event.block.number,
    logIndex: event.logIndex,
    timestamp: event.block.timestamp,
  });

  if (ORDER_FILLED_NON_USER_ADDRESSES.includes(order.account.toLowerCase())) return;

  const mutate =
    order.side === TradeType.BUY ? updateUserPositionWithBuy : updateUserPositionWithSell;
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

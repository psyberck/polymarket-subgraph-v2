/**
 * Position read-modify-write, plus the version-history append that makes
 * point-in-time snapshots possible.
 *
 * Every mutation goes through mutatePosition() so that UserPosition (current
 * state) and UserPositionVersion (immutable history) can never diverge.
 */

import type { EvmEvent, EvmOnEventContext } from 'envio';
import type { PositionState } from '@lib/position';
import { EMPTY } from '@lib/position';

export const getPositionId = (user: string, tokenId: bigint) =>
  `${user.toLowerCase()}-${tokenId.toString()}`;

/**
 * Contracts that appear as a party to position movement but never hold a
 * position of their own. Cost basis is never booked to them.
 *
 * The load-bearing case is the V2 collateral adapters as split/merge/redeem
 * STAKEHOLDER, not exchanges as a counterparty on fills. Measured over blocks
 * 86,226,978-86,227,478: 16,391 of 20,750 PositionSplit events name
 * CtfCollateralAdapter and 4,246 name NegRiskAdapter — barely 1% name a real
 * wallet. Those tokens do belong to users, and AdapterTransfers.ts credits
 * them from the ERC-1155 leg between wallet and adapter. So this guard is
 * what keeps that attribution from double-counting: without it every V2
 * split lands twice, once on the adapter and once on the wallet.
 *
 * The zero address is here for the same reason — splits mint from it and
 * merges burn to it, and booking it makes 0x0 the largest wallet on Polygon.
 *
 * AdapterTransfers.ts also uses this set to decide whether a transfer is a
 * user flow at all: an adapter-to-protocol hop is exchange settlement that
 * OrderFilled already booked, which discards ~94% of what the topic filter
 * matches.
 *
 * Historical note: this guard originally existed because exchanges showed up
 * as the TAKER on fills (4,291 of 33,788 positions, 12.7%, on a 3,200-block
 * sample). Fills are booked maker-only now, so that pathway is largely gone
 * — do not conclude from its absence that the set is unnecessary. The
 * lifecycle-stakeholder case above is why it stays, and AutoRedeemer and the
 * Combos contracts can still act as maker on a user's behalf.
 *
 * Two consequences. Token flow is deliberately NOT conserved here: tokens
 * entering an adapter leave the ledger, so totals cannot be reconciled
 * against on-chain supply — this is a per-user cost-basis record, not an
 * accounting of the CTF. And because the guard returns before any write, no
 * version row is emitted either, so the event leaves no trace in history at
 * all. Anything that seeds UserPosition from an outside source must apply the
 * same filter, or it will plant rows the indexer can never update: v1-final
 * carries 1,942 such rows, booked to the two gen-1 adapters by a subgraph
 * written before those contracts existed.
 */
const PROTOCOL_ADDRESSES = new Set([
  '0x0000000000000000000000000000000000000000',
  // ── v1 exchanges ────────────────────────────────────────────────
  // Dead as emitters since 86,126,998 and never indexed by this project, so
  // they matter only for a backfill that reaches back before the cutover.
  // There they are the single largest fake wallet available: the v1 CTF
  // Exchange is the split STAKEHOLDER on 98,982 of 100,180 wallet-tagged
  // batch-mint legs sampled across 85.1M-86.1M (16,245 splits per 200
  // blocks), because filling a v1 order mints the set to the exchange first.
  // Without these two entries every one of those lands as a position owned by
  // the exchange — the same bug as the 1,942 adapter rows in v1-final, three
  // orders of magnitude larger. Cheap to keep, and 2,107 of them fall inside
  // the very first 20 blocks this indexer now reads.
  '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e', // CTF Exchange (v1)
  '0xc5d563a36ae78145c45a50134d48a1215220f80a', // Neg Risk CTF Exchange (v1)
  '0xe111180000d2663c0091e4f400237545b87b996b', // CTF Exchange V2
  '0xe2222d279d744050d28e00520010520000310f59', // Neg Risk CTF Exchange V2
  '0xe2222d002000ba0053cef3375333610f64600036', // CTF Exchange (3rd)
  '0x4d97dcd97ec945f40cf65f87097ace5ea0476045', // ConditionalTokens
  '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296', // NegRiskAdapter
  '0xada100874d00e3331d00f2007a9c336a65009718', // CtfCollateralAdapter (gen 1)
  '0xada100db00ca00073811820692005400218fce1f', // CtfCollateralAdapter (gen 2)
  '0xada200001000ef00d07553cee7006808f895c6f1', // NegRiskCtfCollateralAdapter (gen 1)
  '0xada2005600dec949baf300f4c6120000bdb6eaab', // NegRiskCtfCollateralAdapter (gen 2)
  '0xa1200000d0002264c9a1698e001292d00e1b00af', // AutoRedeemer — redeems on
  // users' behalf, so it appears as a counterparty but never holds a position.
  '0x006f54f7f9a22e0000cc2ab60031000000ae9fef', // Combos PositionManager
  '0xe3333700ca9d93003f00f0f71f8515005f6c00aa', // Combos Exchange
]);

export const isProtocolAddress = (a: string) => PROTOCOL_ADDRESSES.has(a.toLowerCase());

/**
 * Load the position, apply `fn`, then persist current state and append a
 * version row stamped with this event's block and log index.
 *
 * (blockNumber, logIndex) is the ordering key — the equivalent of `vid` in
 * the old CryptoHouse table. It is monotonic within a chain, so "latest
 * version at or before block B" is well defined even when a position is
 * touched several times in one block.
 */
export async function mutatePosition(
  context: EvmOnEventContext,
  event: EvmEvent,
  user: string,
  token: { tokenId: bigint; conditionId?: string },
  fn: (s: PositionState) => PositionState,
  fee: bigint = 0n,
): Promise<void> {
  if (isProtocolAddress(user)) return;

  const { tokenId, conditionId } = token;

  const walletAddress = user.toLowerCase();
  const id = getPositionId(walletAddress, tokenId);
  const userPosition = await context.UserPosition.get(id);

  const currUserPosition: PositionState = userPosition
    ? {
        amount: userPosition.amount,
        avgPrice: userPosition.avgPrice,
        realizedPnl: userPosition.realizedPnlGross,
        totalBought: userPosition.totalBought,
      }
    : EMPTY;

  const after = fn(currUserPosition);

  const feesPaid = (userPosition?.feesPaid ?? 0n) + fee;

  // Expense fees immediately, whether or not anything has been realized.
  //
  // MEASURED AGAINST data-api.polymarket.com, 2026-08-31, over 2,344,380
  // comparable positions:
  //
  //   unrealized (size still == totalBought)
  //       the API reports exactly -feesPaid, NOT 0. 375 of 376 fee-bearing
  //       positions matched to six decimal places; the 24 reporting 0 had no
  //       fees at all.
  //   realized
  //       unchanged either way -- gross minus every fee, entry and exit.
  //
  // This REVERSES the previous rule, which gated the deduction on
  // `totalBought > amount` and reported 0 until a disposal. That was chosen
  // from a 9-position sample which, on this evidence, must have been zero-fee
  // positions: those read as 0 under both models, so the sample could not tell
  // them apart. Gated, unrealized positions agreed with the API on 32% of the
  // full set; ungated, 93%. It was the largest single source of divergence in
  // the census -- 1,130,318 of 1,218,357 rows.
  //
  // The objection to this rule -- that realized PnL should not move before
  // anything is realized -- is a fair one, but Polymarket's books treat the fee
  // as spent at the fill, and matching them is what makes our numbers checkable
  // against theirs at all.
  const realizedPnl = after.realizedPnl - feesPaid;

  // Keep the first conditionId we learn; split/merge events know it directly,
  // fills only learn it once Gamma metadata lands.
  //
  // `undefined`, never `null`: codegen maps a nullable schema field to
  // `T | undefined` and emits `| null` nowhere, so null was outside the entity
  // contract and only type-checked because `context` was `any`.
  const cond = userPosition?.conditionId ?? conditionId;

  context.UserPosition.set({
    id,
    user: walletAddress,
    tokenId,
    conditionId: cond,
    amount: after.amount,
    avgPrice: after.avgPrice,
    realizedPnl,
    realizedPnlGross: after.realizedPnl,
    feesPaid,
    totalBought: after.totalBought,
    lastBlock: event.block.number,
    lastLogIndex: event.logIndex,
    lastUpdatedAt: event.block.timestamp,
  });

  context.UserPositionVersion.set({
    id: `${id}-${event.block.number}-${event.logIndex}`,
    positionId: id,
    user: walletAddress,
    tokenId,
    conditionId: cond,
    amount: after.amount,
    avgPrice: after.avgPrice,
    realizedPnl,
    realizedPnlGross: after.realizedPnl,
    feesPaid,
    totalBought: after.totalBought,
    blockNumber: event.block.number,
    logIndex: event.logIndex,
    timestamp: event.block.timestamp,
  });
}

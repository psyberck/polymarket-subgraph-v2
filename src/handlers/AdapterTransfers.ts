/**
 * V2 collateral-adapter flows -> position cost basis.
 *
 * In the V2 stack the adapters, not the wallet, are the stakeholder on
 * CTF-level lifecycle events: over blocks 86,226,978-86,227,478, 16,391 of
 * 20,750 PositionSplit events name the CtfCollateralAdapter and 4,246 name the
 * NegRiskAdapter — barely 1% name a real wallet. Those events are skipped for
 * cost basis (mutatePosition drops protocol addresses), so without this
 * handler essentially all V2 split, merge and redeem activity would be
 * missing.
 *
 * The user-attributed leg is the ERC-1155 transfer between wallet and adapter:
 *
 *   adapter -> wallet   tokens minted by a split          buy at 1/2
 *   wallet -> adapter   tokens surrendered to merge       sell at 1/2
 *                       ...or to redeem a resolved market sell at settled price
 *
 * Hops where the counterparty is itself a protocol address (adapter <->
 * exchange settlement, mints and burns) are discarded: those are trades, and
 * they are already booked per-maker from OrderFilled. Ported from the
 * reference indexer's src/handlers/v2/CtfCollateralAdapter.ts.
 *
 */

import { type EvmEvent, type EvmOnEventContext, indexer } from 'envio';
import { getAddress } from 'viem';
import { applyBuy, applySell, splitLegPrice } from '@lib/position';
import { isProtocolAddress, mutatePosition } from '@lib/store';
import {
  negRiskMarketsCount,
  type OutcomeToken,
  resolveConditionTokens,
  resolveTokenCondition,
} from '@lib/conditionTokens';
import { ensureOutcomePayouts } from '@lib/payouts';

// Checksummed: envio matches `where` filter values against decoded params,
// which are EIP-55 checksummed, so lowercase values would never match.
// BOTH generations of each adapter. Polymarket redeployed the collateral
// adapters between 2026-04 and 2026-08, and an indexer pinned to one generation
// silently stops attributing V2 lifecycle activity from the swap onward —
// no error, no gap in the logs, positions just quietly stop being credited.
//
// Measured per 100 blocks of CT TransferSingle with the adapter as `from`:
//
//                          2026-04 (window)   2026-08 (head)
//   0xada10087… (v1)             1,113               3
//   0xAdA100Db… (v2)                 0             467
//
// Keep old generations here forever — they are what make a historical backfill
// correct, and they cost nothing once dormant. Addresses from
// https://docs.polymarket.com/resources/contracts;
const V2_ADAPTERS = [
  '0xada100874d00e3331d00f2007a9c336a65009718', // CtfCollateralAdapter (gen 1)
  '0xAdA100Db00Ca00073811820692005400218FcE1f', // CtfCollateralAdapter (gen 2)
  '0xada200001000ef00d07553cee7006808f895c6f1', // NegRiskCtfCollateralAdapter (gen 1)
  '0xadA2005600Dec949baf300f4C6120000bDB6eAab', // NegRiskCtfCollateralAdapter (gen 2)
  // AutoRedeemer. Not a collateral adapter, but custodially identical for our
  // purposes and by far the biggest of these flows at head: it is the redeemer
  // on 1,214 of 2,011 PayoutRedemption events (60%), against 468 for the
  // collateral adapter. It did not exist during the 2026-04 validation window.
  //
  // Traced end to end (tx 0xbf167d5d…):
  //
  //   TransferBatch   wallet -> AutoRedeemer     user hands over its positions
  //   TransferSingle  AutoRedeemer -> 0x0        burned
  //   ERC20 USDC.e    AutoRedeemer -> …          payout, wrapped to pUSD
  //   ERC20 pUSD      -> wallet                  proceeds returned
  //
  // So the user-facing leg is the wallet -> AutoRedeemer transfer, exactly as
  // it is for the adapters, and bookAdapterFlow already prices it correctly: a
  // surrender of a resolved condition settles at its payout price. The burn leg
  // needs no special case — its counterparty is 0x0, which isProtocolAddress
  // rejects.
  //
  // Without this, 60% of redemption disposals at head are invisible: positions
  // stay open forever and their realized PnL is never booked.
  //
  // NOTE the inbound leg is TransferBatch, not TransferSingle. Measuring only
  // TransferSingle shows `to = 0` and makes it look like AutoRedeemer never
  // receives anything, which is what initially hid this flow.
  '0xa1200000d0002264C9a1698e001292D00E1b00af', // AutoRedeemer
].map((a) => getAddress(a));

const ADAPTER_SET = new Set(V2_ADAPTERS.map((a) => a.toLowerCase()));
const isAdapter = (a: string) => ADAPTER_SET.has(a.toLowerCase());

// First V2 OrderFilled. The adapters themselves are slightly older —
// CtfCollateralAdapter at 84,902,341 and NegRiskCtfCollateralAdapter at
// 84,902,347, both 2026-03-31 — but they sat idle until trading opened:
// scanning the 148,029 blocks from deployment to here returns zero
// TransferBatch and zero TransferSingle with either adapter as a party. So
// this is the tightest bound that still captures every adapter flow, and
// being historical it cannot stop being true.
//
// (The reference uses 84,902,320, which is PolyUSD's deployment — 21 blocks
// before the adapters exist. Same event set, 148k more blocks scanned.)
const V2_START_BLOCK = 85_050_371;

const HALF = splitLegPrice(2);

/**
 * The condition an outcome token belongs to, across both categories of id.
 *
 * A transfer names a tokenId and nothing else, and that id may be either the
 * USDC.e-denominated kind the markets table holds or one lib/collateral.ts
 * derived under some other collateral. Only this path needs both, which is why
 * it lives here rather than beside either category: conditionTokens.ts is also
 * called from scripts/, outside envio, where there is no context to read
 * entities with.
 *
 * Two tiers: the markets table (synchronous sqlite behind its own memo, a
 * unique-index hit) answers for ~100% of traffic, and only an id it does not
 * hold reaches the `Token` entity.
 *
 * NO MEMO IN FRONT OF EITHER. Envio's entity store already is one, and a better
 * one: it records misses as well as hits, so a repeat lookup of an unknown id
 * costs nothing; it groups concurrent gets into a single query; and it keeps
 * db-loaded entries across batches, dropping them only under memory pressure
 * and last of all. What a local Map cannot copy is the invalidation — the
 * entity store sees the `Token.set` in lib/collateral.ts and a rollback, where
 * a Map went stale on both. An earlier memo here pinned a negative for an id
 * that a later lifecycle event made resolvable, and left it unresolvable for
 * the rest of the run.
 */
async function getOutcomeToken(
  context: EvmOnEventContext,
  tokenId: bigint,
): Promise<OutcomeToken | null> {
  const fromTable = resolveTokenCondition(tokenId);
  if (fromTable !== null) return fromTable;

  try {
    const row = await context.Token.get(tokenId.toString());
    return row ? { conditionId: row.conditionId, tokenId, outcomeIndex: row.outcomeIndex } : null;
  } catch (e) {
    console.error(`getOutcomeToken(${tokenId}) failed:`, e);
    return null;
  }
}

/**
 * Price of one leg of a full set: the collateral divided N ways.
 *
 * Shared by both directions on purpose, not merely to save two lines. A full
 * set costs 1.00 to mint and returns 1.00 to burn, so split pricing and
 * unresolved-merge pricing MUST be the same function of the same N -- if they
 * ever diverged, a split followed by a merge would fabricate PnL out of the
 * gap. Two copies of the expression held that identity by coincidence; one
 * copy makes it structural.
 *
 * HALF is the fallback for a condition we cannot resolve at all, which is the
 * only case legCount answers 0 for -- a standalone binary market still answers
 * 2 through resolveConditionTokens.
 */
function setLegPrice(conditionId: string | undefined): bigint {
  const legs = conditionId
    ? negRiskMarketsCount(conditionId) || resolveConditionTokens(conditionId).length
    : 0;
  return legs > 0 ? splitLegPrice(legs) : HALF;
}

async function transferAsLifecycle(
  context: EvmOnEventContext,
  event: EvmEvent,
  from: string,
  to: string,
  ids: readonly bigint[],
  values: readonly bigint[],
): Promise<void> {
  const fromIsAdapter = isAdapter(from);
  const toIsAdapter = isAdapter(to);

  if (fromIsAdapter === toIsAdapter) return;
  const wallet = fromIsAdapter ? to : from;
  if (isProtocolAddress(wallet)) return;

  for (let i = 0; i < ids.length; i++) {
    const value = values[i];
    if (value === 0n) continue;

    const tokenId = ids[i];
    // A transfer names a tokenId and nothing else, so the condition has to be
    // recovered before anything can be said about how the market settled.
    // Also lets the position record its conditionId, which this path used to
    // leave null.
    const token = await getOutcomeToken(context, tokenId);
    const conditionId = token?.conditionId;

    if (fromIsAdapter) {
      // Split: the adapter forwards freshly minted outcome tokens to the
      // wallet. A full set costs the collateral, so each of N legs costs 1/N.
      // N is the number of legs the SPLIT produced -- for a neg-risk event its
      // question count, not the two outcomes of the one question this token
      // belongs to. resolveConditionTokens() answers the latter, so alone it
      // books an 11-question leg at 1/2 instead of 1/11.
      await mutatePosition(context, event, wallet, { tokenId, conditionId }, (s) =>
        applyBuy(s, setLegPrice(conditionId), value),
      );
      continue;
    }

    // Surrender: a merge before byResolution, a redemption after it. Whether the
    // condition has resolved on-chain is what tells them apart — and
    // ensureOutcomePayouts can answer that for any condition, not just ones
    // whose ConditionResolution fell inside the indexed window.
    //
    // Priced per token rather than all-or-nothing across the batch. That gate
    // existed to stop settled legs mixing with balance-weighted ones inside a
    // single redemption; there is no balance-weighting left to mix with, and a
    // TransferBatch can legitimately span conditions that settled differently.
    //
    // A resolved condition is priced at its settled value even in the rare
    // case where the wallet is really merging rather than redeeming: settled
    // prices across a full set sum to 1.00, exactly as merge pricing does, so
    // the total is identical and only the per-leg split differs.
    //
    // Narrowed on `token` rather than guarding `prices && token`: conditionId
    // is derived from token, so the two can never be half-true, and the old
    // form read as though a payout could exist without the token it indexes.
    const settled = token
      ? (await ensureOutcomePayouts(context, token.conditionId, false))?.[token.outcomeIndex]
      : undefined;

    await mutatePosition(context, event, wallet, { tokenId, conditionId }, (s) =>
      applySell(s, settled ?? setLegPrice(conditionId), value),
    );
  }
}

indexer.onEvent(
  {
    contract: 'ConditionalTokens',
    event: 'TransferSingle',
    where: {
      params: [{ from: V2_ADAPTERS }, { to: V2_ADAPTERS }],
      block: { number: { _gte: V2_START_BLOCK } },
    },
  },
  async ({ event, context }) => {
    await transferAsLifecycle(
      context,
      event,
      event.params.from,
      event.params.to,
      [event.params.id],
      [event.params.value],
    );
  },
);

indexer.onEvent(
  {
    contract: 'ConditionalTokens',
    event: 'TransferBatch',
    where: {
      params: [{ from: V2_ADAPTERS }, { to: V2_ADAPTERS }],
      block: { number: { _gte: V2_START_BLOCK } },
    },
  },
  async ({ event, context }) => {
    await transferAsLifecycle(
      context,
      event,
      event.params.from,
      event.params.to,
      event.params.ids,
      event.params.values,
    );
  },
);

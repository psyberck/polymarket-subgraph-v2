/**
 * Named addresses and scale constants, ported from the retired subgraph's
 * `constants.ts`.
 *
 * The original was AssemblyScript against the Graph runtime (`Address`,
 * `BigInt.fromI32`, `BigDecimal`) with `{{ contracts.X.address }}` templates
 * that were never rendered. Every value here is instead taken from a literal
 * that already exists in `src/lib` or `src/handlers`, cited per entry, so this
 * module renames rather than introduces. Nothing was invented: the subgraph
 * names with no counterpart in this codebase are listed at the bottom.
 *
 * Addresses are LOWER-CASE, which is the comparison form used throughout —
 * `isProtocolAddress`, `isTableCollateral` and `isNegligibleCollateral` all
 * `.toLowerCase()` before testing set membership (`store.ts:84`,
 * `collateral.ts:62`, `collateral.ts:90`). Wrap in viem's `getAddress()` at
 * call sites that need a checksummed address, as `rpc.ts:31` does.
 */

import { USDC_SCALE } from '@lib/position';

// ── Core protocol ──────────────────────────────────────────────────

/** Gnosis ConditionalTokens — the live CTF. Source: `rpc.ts:31`. */
const CONDITIONAL_TOKENS = '0x4d97dcd97ec945f40cf65f87097ace5ea0476045' as const;

/** NegRiskAdapter. Source: `conditionTokens.ts:40`, `store.ts:73`. */
const NEG_RISK_ADAPTER = '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296' as const;

/** Redeems on users' behalf, so it is a counterparty that never holds a
 *  position. Source: `store.ts:78`, `AdapterTransfers.ts:82`. */
const AUTO_REDEEMER = '0xa1200000d0002264c9a1698e001292d00e1b00af' as const;

/** Burn/mint counterparty on CTF transfers. Source: `store.ts:55`. */
const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000' as const;

// ── Collateral ─────────────────────────────────────────────────────

/**
 * USDC.e — the bridged token Polymarket actually denominates in, NOT native
 * USDC. Source: `collateral.ts:58`.
 */
const USDC = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' as const;

/** Neg-risk wrapped collateral. Source: `collateral.ts:59`. */
const NEG_RISK_WRAPPED_COLLATERAL = '0x3a3bd7bb9528e159577f7c2e685cc81a765002e2' as const;

/**
 * pUSD, measured as economically negligible and skipped without deriving ids.
 * Both entries are the same token — proxy and implementation. Source:
 * `collateral.ts:86-87`.
 */
const PUSD_PROXY = '0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb' as const;
const PUSD_IMPLEMENTATION = '0x6bbcef9f7ef3b6c592c99e0f206a0de94ad0925f' as const;

// ── Exchanges ──────────────────────────────────────────────────────
//
// The subgraph's bare EXCHANGE / NEG_RISK_EXCHANGE were the v1 contracts, so
// those names keep their v1 meaning here and the v2 contracts are suffixed.
// v1 is dead as an emitter since block 86,126,998 and is not indexed — it
// matters only to a backfill reaching back before the cutover, where it is
// the largest source of fake wallets (`store.ts:58-66`). v2 is what
// `config.yaml` subscribes to.

/** CTF Exchange (v1). Source: `store.ts:67`. */
const EXCHANGE = '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e' as const;

/** Neg Risk CTF Exchange (v1). Source: `store.ts:68`. */
const NEG_RISK_EXCHANGE = '0xc5d563a36ae78145c45a50134d48a1215220f80a' as const;

/** CTF Exchange V2. Source: `store.ts:69`, `config.yaml`. */
const EXCHANGE_V2 = '0xe111180000d2663c0091e4f400237545b87b996b' as const;

/** Neg Risk CTF Exchange V2. Source: `store.ts:70`, `config.yaml`. */
const NEG_RISK_EXCHANGE_V2 = '0xe2222d279d744050d28e00520010520000310f59' as const;

/** Third v2 exchange deployment. Source: `store.ts:71`, `config.yaml`. */
const EXCHANGE_V2_THIRD = '0xe2222d002000ba0053cef3375333610f64600036' as const;

/** Combos. Source: `store.ts:80-81`. */
const COMBOS_POSITION_MANAGER = '0x006f54f7f9a22e0000cc2ab60031000000ae9fef' as const;
const COMBOS_EXCHANGE = '0xe3333700ca9d93003f00f0f71f8515005f6c00aa' as const;

// ── Collateral adapters ────────────────────────────────────────────
//
// Two generations of each, gen 2 having replaced gen 1 between 2026-04 and
// 2026-08. Both are needed: a backfill spanning that swap silently stops
// attributing lifecycle events if either is missing. Source:
// `AdapterTransfers.ts:54-57`, `store.ts:74-77`.

const CTF_COLLATERAL_ADAPTER_V1 = '0xada100874d00e3331d00f2007a9c336a65009718' as const;
const CTF_COLLATERAL_ADAPTER_V2 = '0xada100db00ca00073811820692005400218fce1f' as const;
const NEG_RISK_CTF_COLLATERAL_ADAPTER_V1 = '0xada200001000ef00d07553cee7006808f895c6f1' as const;
const NEG_RISK_CTF_COLLATERAL_ADAPTER_V2 = '0xada2005600dec949baf300f4c6120000bdb6eaab' as const;

// ── Scales ─────────────────────────────────────────────────────────

/**
 * One unit of collateral, 1e6.
 *
 * Re-exported from `lib/position.ts` rather than restated, so the fold and
 * anything reading this constant cannot drift apart. The subgraph built the
 * same value as `BigInt.fromI32(10).pow(6)`.
 */
const COLLATERAL_SCALE = USDC_SCALE;

/**
 * Half of one unit of collateral, 1e6.
 *
 * The price of one leg of a binary outcome set, which is the case
 * `splitLegPrice(2)` computes generically (`position.ts:127`) and what
 * `AdapterTransfers.ts:100` holds as `HALF`.
 */
const FIFTY_CENTS = COLLATERAL_SCALE / 2n;

// ── Order side ─────────────────────────────────────────────────────

/**
 * Direction of a maker's order, in the contract's own `uint8` encoding.
 *
 * A `const` object rather than a TypeScript `enum`: Node 24 runs `.ts` in
 * strip-only mode, which rejects `enum` outright with
 * `ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX`. This form erases to a plain object and
 * still supports both `TradeType.BUY` as a value and `TradeType` as a type,
 * so call sites are unchanged. `position.ts:160-161` avoids `enum` the same
 * way, spelling it `SIDE_BUY` / `SIDE_SELL` with a union type.
 *
 * Note that `OrderFilled.side` arrives as a `bigint`, so compare against `0n`
 * rather than against these directly — see `parseOrderFilled`.
 */
const TradeType = {
  BUY: 0,
  SELL: 1,
} as const;

type TradeType = (typeof TradeType)[keyof typeof TradeType];

export {
  AUTO_REDEEMER,
  COLLATERAL_SCALE,
  COMBOS_EXCHANGE,
  COMBOS_POSITION_MANAGER,
  CONDITIONAL_TOKENS,
  CTF_COLLATERAL_ADAPTER_V1,
  CTF_COLLATERAL_ADAPTER_V2,
  EXCHANGE,
  EXCHANGE_V2,
  EXCHANGE_V2_THIRD,
  FIFTY_CENTS,
  NEG_RISK_ADAPTER,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V1,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V2,
  NEG_RISK_EXCHANGE,
  NEG_RISK_EXCHANGE_V2,
  NEG_RISK_WRAPPED_COLLATERAL,
  PUSD_IMPLEMENTATION,
  PUSD_PROXY,
  TradeType,
  USDC,
  ZERO_ADDRESS,
};

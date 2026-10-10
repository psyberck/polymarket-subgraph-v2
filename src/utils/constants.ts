/** Addresses and scales, ported from the subgraph's `constants.ts`. */

// ── Core protocol ──────────────────────────────────────────────────

/** Gnosis ConditionalTokens (CTF). */
const CONDITIONAL_TOKENS = '0x4d97dcd97ec945f40cf65f87097ace5ea0476045' as const;

/** NegRiskAdapter. */
const NEG_RISK_ADAPTER = '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296' as const;

/** Where the NegRiskAdapter parks converted NO. */
const NEG_RISK_ESCROW_SINK = '0xa5ef39c3d3e10d0b270233af41cac69796b12966' as const;

/** Redeems for users; never holds a position. */
const AUTO_REDEEMER = '0xa1200000d0002264c9a1698e001292d00e1b00af' as const;

/** Gen-1 auto-redeemer; still live, handled like AUTO_REDEEMER. */
const AUTO_REDEEMER_V1 = '0xf3cfb6a6ebfeb51876289eb235719eb1c65252b0' as const;

/** Redeem for wallets by approval, with no per-wallet event. */
const REDEMPTION_OPERATORS: readonly string[] = ['0x111f73e91f85b6fe4de1ddec3de2fe32122e355b'];

/** Burn/mint counterparty on CTF transfers. */
const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000' as const;

// ── Collateral ─────────────────────────────────────────────────────

/** USDC.e (bridged), not native USDC. */
const USDC = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' as const;

/** Neg-risk wrapped collateral. */
const NEG_RISK_WRAPPED_COLLATERAL = '0x3a3bd7bb9528e159577f7c2e685cc81a765002e2' as const;

/** pUSD proxy and implementation; negligible, skipped. */
const PUSD_PROXY = '0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb' as const;
const PUSD_IMPLEMENTATION = '0x6bbcef9f7ef3b6c592c99e0f206a0de94ad0925f' as const;

// ── Exchanges ──────────────────────────────────────────────────────
//
// Unsuffixed names are v1 (as in the subgraph), dead since block 86,126,998
// and not indexed; pre-cutover it is the split stakeholder on most mints.

/** CTF Exchange (v1). */
const EXCHANGE = '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e' as const;

/** Neg Risk CTF Exchange (v1). */
const NEG_RISK_EXCHANGE = '0xc5d563a36ae78145c45a50134d48a1215220f80a' as const;

/** CTF Exchange V2. */
const EXCHANGE_V2 = '0xe111180000d2663c0091e4f400237545b87b996b' as const;

/** Neg Risk CTF Exchange V2. */
const NEG_RISK_EXCHANGE_V2 = '0xe2222d279d744050d28e00520010520000310f59' as const;

/** Third v2 exchange. */
const EXCHANGE_V2_THIRD = '0xe2222d002000ba0053cef3375333610f64600036' as const;

/** Combos. */
const COMBOS_POSITION_MANAGER = '0x006f54f7f9a22e0000cc2ab60031000000ae9fef' as const;
const COMBOS_EXCHANGE = '0xe3333700ca9d93003f00f0f71f8515005f6c00aa' as const;

// ── Collateral adapters ────────────────────────────────────────────
//
// Gen 2 replaced gen 1 over 2026-04..08; a backfill across it needs both.

const CTF_COLLATERAL_ADAPTER_V1 = '0xada100874d00e3331d00f2007a9c336a65009718' as const;
const CTF_COLLATERAL_ADAPTER_V2 = '0xada100db00ca00073811820692005400218fce1f' as const;
const NEG_RISK_CTF_COLLATERAL_ADAPTER_V1 = '0xada200001000ef00d07553cee7006808f895c6f1' as const;
const NEG_RISK_CTF_COLLATERAL_ADAPTER_V2 = '0xada2005600dec949baf300f4c6120000bdb6eaab' as const;

// ── Scales ─────────────────────────────────────────────────────────

/** One unit of collateral; every amount and price is in 1e6. */
const COLLATERAL_SCALE = 1_000_000n;

/** Below this balance (0.1 tokens) a sold-down position counts as closed: rounding leaves dust. */
const CLOSED_POSITION_DUST = COLLATERAL_SCALE / 10n;

/** 0.50, the price of one leg of a binary split. */
const FIFTY_CENTS = COLLATERAL_SCALE / 2n;

const V2_START_BLOCK = 85_050_370;

// ── Order side ─────────────────────────────────────────────────────

/** Maker order side, as the contract's `uint8`. */
const TradeType = {
  BUY: 0,
  SELL: 1,
} as const;

type TradeType = (typeof TradeType)[keyof typeof TradeType];

export {
  AUTO_REDEEMER,
  AUTO_REDEEMER_V1,
  CLOSED_POSITION_DUST,
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
  NEG_RISK_ESCROW_SINK,
  NEG_RISK_EXCHANGE,
  NEG_RISK_EXCHANGE_V2,
  NEG_RISK_WRAPPED_COLLATERAL,
  PUSD_IMPLEMENTATION,
  PUSD_PROXY,
  REDEMPTION_OPERATORS,
  TradeType,
  USDC,
  ZERO_ADDRESS,
  V2_START_BLOCK,
};

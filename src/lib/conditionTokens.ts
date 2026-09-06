/**
 * conditionId -> ordered outcome tokenIds, from the local markets DB.
 *
 * Split/merge/redemption events identify outcomes by index set, not tokenId.
 * Deriving tokenIds on-chain would mean reimplementing the Gnosis CTF
 * collection-id hash (keccak + alt_bn128 point addition).
 */

import { DatabaseSync } from 'node:sqlite';
import { encodePacked, keccak256 } from 'viem';
import { SizedCache } from '@lib/types';

/** An outcome token, fully identified: which condition, which outcome, which id. */
export interface OutcomeToken {
  tokenId: bigint;
  conditionId: string;
  outcomeIndex: number;
}

/**
 * Entry cap shared by every bounded memo here and in lib/collateral.ts.
 *
 * Validated, not merely coerced. `size >= NaN` is false for every size, so a
 * typo'd CONDITION_CACHE_CAP would not fall back to the default — it would
 * silently unbound every cache and turn a bounded memo into an OOM over a long
 * backfill.
 */
function parseCacheSize(): number {
  const raw = process.env.CONDITION_CACHE_CAP;
  if (raw === undefined) return 200_000;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`CONDITION_CACHE_CAP must be a positive integer, got ${JSON.stringify(raw)}`);
  }
  return parsed;
}

/** Exported as a value, not a function: one parse of one env var per process. */
export const MAX_CACHE_SIZE = parseCacheSize();
const NEG_RISK_ADAPTER = '0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296' as const;

const marketCache = new SizedCache<bigint[]>(MAX_CACHE_SIZE);
const tableTokenCache = new SizedCache<OutcomeToken>(MAX_CACHE_SIZE);
const resolutionCache = new SizedCache<number | null>(MAX_CACHE_SIZE);
const negRiskCache = new SizedCache<number>(MAX_CACHE_SIZE);
const negRiskMarketCache = new SizedCache<number>(MAX_CACHE_SIZE);
const negRiskCountCache = new SizedCache<number>(MAX_CACHE_SIZE);

const DB_PATH = process.env.POLYMARKET_DB_PATH!;

/** Open the markets DB */
const databaseHandler = new DatabaseSync(DB_PATH, { readOnly: true });

/** One row of a query, or undefined when it matched nothing. */
const getRow = <R>(sql: string, param: string) =>
  databaseHandler.prepare(sql).get(param) as R | undefined;

/** Every row of a query, in the order the query gave them. */
const getRows = <R>(sql: string, param: string) => databaseHandler.prepare(sql).all(param) as R[];

/**
 * One read against the markets DB.
 *
 * `undefined` means "no answer to keep" — either `execute` threw, which is logged
 * here, or it looked and found nothing. Callers turn that into their own
 * fallback: [] for a token list, 0 for a count, null for a condition.
 *
 * `null` is deliberately NOT that signal. conditionResolutionIndex stores one
 * as a real answer, so the two have to stay distinguishable.
 */
function readDb<V>(label: string, key: string, execute: () => V | undefined): V | undefined {
  try {
    return execute();
  } catch (e) {
    console.error(`${label}(${key}) failed:`, e);
    throw e;
  }
}

/**
 * One read behind a memo: answer from the cache, else execute, else memoize.
 *
 * Nothing is memoized unless the query produced it. A read that threw says
 * nothing about the row — pinning its fallback would turn one transient sqlite
 * error into a permanent wrong answer — and a read that missed may be only half
 * an answer, as it is for resolveTokenCondition.
 *
 * Which is safe precisely because a cache cannot hold `undefined`: Map.get
 * already spends that value on "absent", so a miss and a memoized miss are
 * indistinguishable to every caller, and there is nothing to get wrong.
 */
function memoDb<V>(
  cache: SizedCache<V>,
  label: string,
  key: string,
  execute: () => V | undefined,
): V | undefined {
  const hit = cache.get(key);
  if (hit !== undefined) return hit;

  const value = readDb(label, key, execute);
  if (value !== undefined) cache.set(key, value);
  return value;
}

/**
 * How many legs a split of this condition's collateral actually produces.
 *
 * Returns the number of questions in the condition's neg-risk event, or 0 when
 * the market is standalone.
 *
 * WHY THIS EXISTS. A split costs the collateral and yields one leg per outcome,
 * so each leg costs 1/N -- but for a neg-risk event the split spans every
 * QUESTION in the event, not the two outcomes of one question.
 * resolveConditionTokens() answers the latter, so pricing off it books an
 * 11-question neg-risk leg at 1/2 instead of 1/11: a 5.5x overstatement of cost
 * basis that flows straight into ROI and into wallet scoring.
 *
 * MEASURED against data-api.polymarket.com over 55,575 positions the indexer
 * had priced at exactly 0.50: on 79.5% the API's avgPrice is exactly
 * 1 / (this count), and every one of them belongs to a neg-risk market. The
 * API applies the same 1/N rule -- it just uses the right N.
 *
 * The remaining 20.5% are mostly not clean reciprocals at all, which is what a
 * wallet that both bought and split looks like: a blended basis no single
 * denominator reproduces.
 */
export function negRiskMarketsCount(conditionId: string): number {
  if (!conditionId) return 0;
  return memoDb(negRiskMarketCache, 'negRiskMarketsCount', conditionId, () => {
    const row = getRow<{ id?: string }>(
      'SELECT neg_risk_market_id as id FROM markets ' +
        'WHERE condition_id = ? AND neg_risk_market_id IS NOT NULL ' +
        "AND neg_risk_market_id <> '' LIMIT 1",
      conditionId,
    );
    return row?.id ? negRiskCountOfMarket(row.id) : 0;
  }) as number;
}

/**
 * Questions in a neg-risk event. The single counting primitive --
 * negRiskMarketsCount reaches it through the condition's parent,
 * negRiskQuestionCount calls it with the marketId directly.
 *
 * `condition_id <> ''` IS THE WHOLE CORRECTNESS ARGUMENT, not tidiness. 1,111
 * neg-risk siblings across 148 parents carry an empty condition_id: Gamma lists
 * them under the event but they were never deployed on chain, so the adapter
 * never registered them as questions and a split never pays for them. Counting
 * them answers 24 for an event with 17 on-chain questions and prices the leg at
 * 1/24 -- and the extra rows are exactly the ones with no CLOB tokens either
 * (1,111 of 1,111 overlap, 0 rows have a condition_id but no tokens).
 *
 * VERIFIED against NegRiskAdapter.getQuestionCount() over 550 families: exact
 * on 548, off by one on the remaining 2 (104 vs 105, 10 vs 11). The unfiltered
 * count is right on 0 of the 150 families where the two differ.
 *
 * A contiguous walk of the derived conditionIds was measured as an alternative
 * and is exactly equivalent -- same 148/150, same 2 misses -- while costing one
 * query per question instead of one per event, and missing by 42 rather than 1
 * on those 2. It is not worth its price.
 *
 * KNOWN LIMIT. A stale markets table missing a sibling silently shortens this,
 * with no local signal: the walk cannot tell that case apart from the
 * over-grouping above, so nothing here can. getQuestionCount() is the only
 * authority that settles it.
 */
function negRiskCountOfMarket(marketId: string): number {
  if (!marketId) return 0;
  return memoDb(negRiskCountCache, 'negRiskCountOfMarket', marketId, () => {
    const row = getRow<{ n?: number }>(
      'SELECT count(*) AS n FROM markets ' + "WHERE neg_risk_market_id = ? AND condition_id <> ''",
      marketId.toLowerCase(),
    );
    return Number(row?.n ?? 0);
  }) as number;
}

/**
 * Outcome token ids for a condition, ordered by outcome index.
 * Returns [] when the condition is not in the markets table — callers skip.
 */
export function resolveConditionTokens(conditionId: string): bigint[] {
  return memoDb(marketCache, 'resolveConditionTokens', conditionId, () =>
    getRows<{ clob_token_id: string }>(
      'SELECT t.clob_token_id FROM market_tokens t JOIN markets m ON m.market_id = t.market_id WHERE m.condition_id = ? ORDER BY t.outcome_index',
      conditionId,
    ).map((row) => BigInt(row.clob_token_id)),
  ) as bigint[];
}

/**
 * The condition an outcome token belongs to, and its index within it.
 *
 * Returns null when the token is not in the markets table — the same class of
 * gap as resolveConditionTokens returning [], and callers treat it the same
 * way: skip rather than guess.
 *
 * Table-denominated ids only. Ids derived under a non-canonical collateral are
 * a separate category with its own memo, and never appear here; lib/collateral.ts
 * owns those, and handlers/AdapterTransfers.ts layers this lookup and that one
 * into the resolver that answers for both.
 *
 * A miss is NOT memoized — it answers undefined rather than null, which is what
 * tells memoDb to keep nothing. "Not in the table" is only half an answer, and
 * the negative belongs to whoever has asked the other half too.
 */
export function resolveTokenCondition(tokenId: bigint): OutcomeToken | null {
  const key = tokenId.toString();
  return (
    memoDb(tableTokenCache, 'resolveTokenCondition', key, () => {
      const row = getRow<{ condition_id?: string; outcome_index?: number }>(
        'SELECT m.condition_id, t.outcome_index FROM market_tokens t JOIN markets m ON m.market_id = t.market_id WHERE t.clob_token_id = ?',
        key,
      );
      if (row?.condition_id === undefined || row?.outcome_index === undefined) {
        console.warn(
          `conditional id ${row?.condition_id}, outcome index ${row?.outcome_index} for ${tokenId}`,
        );
        return undefined;
      }
      return { conditionId: row.condition_id, tokenId, outcomeIndex: Number(row.outcome_index) };
    }) ?? null
  );
}

// UNRESOLVED is a distinct sentinel from -1 — conflating "not settled yet" with
// "settled with no single winner" would price an open market as a redemption.
export const UNRESOLVED = null;

/**
 * Whether the local metadata thinks this condition has resolved.
 *
 * A cheap negative filter in front of the on-chain payout probe, and the reason
 * merges cost no RPC at all. The probe deliberately does not cache an
 * "unresolved" answer — a condition can resolve later, and pinning that answer
 * would misprice every subsequent redemption — so without this gate every
 * merge would re-probe the chain forever, on the hot path.
 *
 * Only ever used to skip work, never to price anything: when this says
 * resolved, lib/payouts.ts still reads the numerators from chain state, which
 * is authoritative and can express fractional settlements that resolution_idx
 * cannot. A stale NULL here costs the same merge pricing we would have had
 * anyway; it cannot produce a wrong settled price.
 */
export function conditionResolutionIndex(conditionId: string): number | null {
  return (
    memoDb(resolutionCache, 'conditionResolutionIndex', conditionId, () => {
      const row = getRow<{ resolution_idx?: number }>(
        'SELECT resolution_idx FROM markets WHERE condition_id = ? AND resolution_idx IS NOT NULL LIMIT 1',
        conditionId,
      );
      return row === undefined ? UNRESOLVED : Number(row.resolution_idx ?? -1);
    }) ?? UNRESOLVED
  );
}

/**
 * Index-set bitmask -> outcome indices.
 *
 * CTF encodes an outcome subset as a bitmask: bit i set means outcome i is in
 * the set. A split's `partition` is the list of such masks covering the full
 * set; a redemption's `indexSets` are the ones being redeemed.
 */
function indexSetToOutcomes(indexSet: bigint): number[] {
  const out: number[] = [];
  for (let i = 0, rest = indexSet; rest > 0n; i++, rest >>= 1n) {
    if (rest & 1n) out.push(i);
  }
  return out;
}

/**
 * The legs a list of index-set bitmasks names, out of a condition's ordered
 * token ids.
 *
 * `all` is indexed by outcome, so an index set naming an outcome the caller has
 * no token id for contributes nothing rather than an undefined leg — which is
 * what keeps a partial `all` from producing legs that cannot be booked.
 */
export function indexSetsToLegs(
  conditionId: string,
  all: bigint[],
  indexSets: readonly bigint[],
): OutcomeToken[] {
  const legs: OutcomeToken[] = [];
  for (const set of indexSets) {
    for (const outcomeIndex of indexSetToOutcomes(set)) {
      const tokenId = all[outcomeIndex];
      if (tokenId !== undefined) legs.push({ conditionId, tokenId, outcomeIndex });
    }
  }
  return legs;
}

// ── Neg-risk market -> its questions ───────────────────────────────
//
// Neg-risk markets are addressed by a marketId; the conditions underneath are
// derived, not emitted. A conversion event names only the marketId and an
// index-set bitmask, so folding it requires (a) each question's conditionId
// and (b) how many questions the market has.
//
// (a) is pure keccak, ported from the reference (src/utils/negRisk.ts):
// questionId is the marketId with its last byte replaced by the question
// index, and conditionId is the standard CTF derivation with the adapter as
// oracle and 2 outcome slots.
//
// (b) is NOT derivable — the reference reads it from MarketPrepared /
// QuestionPrepared events it indexes from genesis. Indexing those here would
// only cover markets prepared after our start block, which for a bounded
// backfill is most of them missing. Instead the question list is probed
// against the markets table, which covers all history. Validated against the
// three conversions in run.log: questions resolve contiguously from index 0
// and then stop cleanly (11, 3 and 11 questions respectively).

/** conditionId of question `index` within a neg-risk market. */
export function negRiskConditionId(marketId: string, index: number): string {
  const questionId = `${marketId.slice(0, 64)}${index.toString(16).padStart(2, '0')}`;
  return keccak256(
    encodePacked(
      ['address', 'bytes32', 'uint256'],
      [NEG_RISK_ADAPTER, questionId as `0x${string}`, 2n],
    ),
  );
}
/** Max questions to probe. Neg-risk index sets are uint256 bitmasks. */
const MAX_QUESTIONS = 256;
/**
 * Number of questions in a neg-risk market, or 0 when it cannot be established.
 *
 * Two independent answers, which must agree:
 *
 *   declared  siblings sharing this neg_risk_market_id (one query)
 *   probed    walk indices from 0, deriving each conditionId by keccak, until
 *             one has no tokens in market_tokens
 *
 * Disagreement means a question exists that we cannot resolve, so the answer is
 * 0 -- "unknown" -- and the caller skips the event.
 *
 * WHY BOTH. The probe alone was the previous implementation and it silently
 * undercounted: resolveConditionTokens returns [] both for "no such question"
 * and for "question exists but its market has no CLOB tokens". A gap at index
 * 17 of 31 ended the walk and returned 17 on six such families.
 *
 * That reading was wrong about which side was at fault, and the chain settled
 * it: getQuestionCount() says those events really do have 17 questions. The
 * probe was right and `declared` was inflated by siblings Gamma lists but never
 * deployed. Filtering them out of negRiskCountOfMarket makes the two agree, so
 * the 148 families this rule used to reject now return a usable count.
 *
 * That is worse than useless downstream. `questionCount === 0` is the only
 * guard that catches a bad count and an undercount slips past it; then
 * `indexSet >> questionCount` rejects legitimate conversions as out of range,
 * and any that survive divide by a wrong `yesCount` in negRiskYesPrice,
 * mispricing every YES leg. The old contract -- "a gap is treated as unknown,
 * never as shorter" -- held only for a gap at index 0. This makes it hold
 * everywhere.
 *
 * Not to be confused with negRiskMarketsCount, which is keyed by conditionId
 * and asks only how many questions the event has. That is all a split leg needs
 * to be priced; here an unresolvable question invalidates the whole fold, so
 * this one additionally demands that every question's tokens are known.
 */
export function negRiskQuestionCount(marketId: string): number {
  const key = marketId.toLowerCase();
  return (
    memoDb(negRiskCache, 'negRiskQuestionCount', key, () => {
      const declared = negRiskCountOfMarket(key);

      let probed = 0;
      while (probed < MAX_QUESTIONS) {
        if (!resolveConditionTokens(negRiskConditionId(marketId, probed)).length) break;
        probed++;
      }

      return declared > 0 && declared === probed ? declared : 0;
    }) ?? 0
  );
}

/** Whether question `index` is a member of an index-set bitmask. */
export function indexSetContains(indexSet: bigint, index: number): boolean {
  return ((indexSet >> BigInt(index)) & 1n) === 1n;
}

/** Outcome token ids for question `index` of a neg-risk market, [] if unknown. */
export function negRiskQuestionTokens(marketId: string, index: number): bigint[] {
  return resolveConditionTokens(negRiskConditionId(marketId, index));
}

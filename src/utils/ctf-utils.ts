import { encodePacked, getAddress, keccak256 } from 'viem';

/**
 * Gnosis CTF collection- and position-id derivation, computed locally.
 *
 * A port of `CTHelpers.sol` from gnosis/conditional-tokens-contracts, by way of
 * the retired subgraph's `common/utils/ctf-utils.ts`. Parent collection ids are
 * ignored: Polymarket never uses them, and every id this project resolves has
 * the zero parent.
 *
 * Deriving an id has two halves, and only the first is expensive:
 *
 * ```text
 * collectionId = f(conditionId, outcomeIndex)        alt_bn128 point search
 * positionId   = keccak256(collateral ‖ collectionId)
 * ```
 *
 * The collection id does NOT depend on the collateral, so one point search
 * serves every collateral that condition is denominated in. That is what makes
 * this worth computing rather than fetching: `getCollectionId` on
 * ConditionalTokens returns the same value forever, so an RPC round trip buys
 * nothing a pure function cannot.
 */

/** The alt_bn128 field prime. Curve is y² = x³ + 3 over F_P. */
const P = 21888242871839275222246405745257275088696311157297823662689037894645226208583n;

/** Curve constant `b` in y² = x³ + b. */
const B = 3n;

/**
 * `base ** exponent mod P`, by binary expansion of the exponent.
 *
 * Square-and-multiply rather than anything cleverer: the exponent here is a
 * fixed 254-bit constant, so this is ~254 modular squarings per call and there
 * is no input-dependent path worth optimising.
 */
function powModP(base: bigint, exponent: bigint): bigint {
  let result = 1n;
  let b = base % P;
  let e = exponent;

  while (e > 0n) {
    if (e & 1n) result = (result * b) % P;
    b = (b * b) % P;
    e >>= 1n;
  }

  return result;
}

/**
 * Euler's criterion: `a ** ((P-1)/2) mod P`.
 *
 * Returns `1n` when `a` is a non-zero quadratic residue mod P — i.e. a square,
 * i.e. an x for which the curve has a y. Returns `P - 1n` when it is not, and
 * `0n` only for `a === 0n`.
 */
function legendreSymbol(a: bigint): bigint {
  return powModP(a, (P - 1n) >> 1n);
}

/** Left-pad a bigint to a bare 32-byte hex string, no `0x`. */
function toWord(value: bigint): string {
  return value.toString(16).padStart(64, '0');
}

/**
 * The collection id for an arbitrary index set over a condition's outcomes.
 *
 * This is the CTF primitive — `getCollectionId(0x0, conditionId, indexSet)`.
 * An index set is a bitmask of outcomes, so one outcome is the singleton
 * `1 << outcomeIndex`, which `computeCollectionId` wraps. Sets with more than
 * one bit are legitimate collections, and the raw form is exposed so
 * scripts/token_parity.ts can identify ids built from a WRONG index set.
 *
 * It needs that because Gamma publishes 95 such ids: for the 39 non-binary
 * markets it derives `clob_token_id` with the index set as a 1-based counter
 * rather than a bitmask, so its "outcome 2" id is really the collection
 * {outcome0, outcome1}. Being able to name the set behind an id is what turns
 * "this id does not match" into "Gamma used indexSet 3 where the outcome's own
 * set is 4". The protocol only ever splits one-hot, so those collections have
 * no supply and the ids are unreachable -- see token_parity.ts.
 *
 * `keccak256(conditionId ‖ indexSet)` is treated as a candidate x-coordinate
 * and incremented until it lands on the curve, which is what the CTF does to
 * map an arbitrary hash onto a group element. The high bit of the hash is
 * carried separately as a sign, toggled into bit 254 of the result.
 *
 * @param conditionId - 0x-prefixed 32-byte condition id.
 * @param indexSet - Bitmask of outcomes, written as a full 32-byte word.
 * @returns 0x-prefixed 32-byte collection id.
 *
 * @remarks
 * The index set is written as a full 32-byte word. The subgraph's version
 * assigns it to a single byte (`hashPayload[63] = 1 << outcomeIndex`), which
 * silently wraps to zero at `outcomeIndex >= 8` and cannot express a
 * multi-outcome set at all.
 *
 * The search loop terminates quickly: about half the field is a quadratic
 * residue, so the expected number of iterations is ~2.
 */
function collectionIdForIndexSet(conditionId: string, indexSet: bigint): `0x${string}` {
  const payload = `0x${conditionId.slice(2).toLowerCase().padStart(64, '0')}${toWord(indexSet)}`;

  const hash = BigInt(keccak256(payload as `0x${string}`));

  // The top bit is not part of the x-coordinate; it selects which of the two
  // roots (y, -y) the collection id stands for.
  const odd = hash >> 255n !== 0n;

  let x = hash;
  let yy: bigint;

  do {
    x = (x + 1n) % P;
    yy = (((x * x) % P) * x + B) % P;
  } while (legendreSymbol(yy) !== 1n);

  if (odd) {
    const oddToggle = 1n << 254n;
    x = (x & oddToggle) === 0n ? x + oddToggle : x - oddToggle;
  }

  return `0x${toWord(x)}`;
}

/**
 * The ERC-1155 token id for an index set, denominated in a given collateral.
 *
 * @param collateral - Collateral token address. Case-insensitive; checksummed
 *   internally, though the packed encoding is byte-identical either way.
 * @param conditionId - 0x-prefixed 32-byte condition id.
 * @param indexSet - Bitmask of outcomes.
 * @returns The position id, as the decimal-valued bigint the CTF uses.
 */
function positionIdForIndexSet(collateral: string, conditionId: string, indexSet: bigint): bigint {
  const collectionId = collectionIdForIndexSet(conditionId, indexSet);

  return BigInt(
    keccak256(encodePacked(['address', 'bytes32'], [getAddress(collateral), collectionId])),
  );
}

/** The collection id for one outcome: the singleton index set `1 << outcomeIndex`. */
function computeCollectionId(conditionId: string, outcomeIndex: number): `0x${string}` {
  return collectionIdForIndexSet(conditionId, 1n << BigInt(outcomeIndex));
}

/** The token id for one outcome, denominated in `collateral`. */
function computePositionId(collateral: string, conditionId: string, outcomeIndex: number): bigint {
  return positionIdForIndexSet(collateral, conditionId, 1n << BigInt(outcomeIndex));
}

export { collectionIdForIndexSet, computeCollectionId, computePositionId, positionIdForIndexSet };

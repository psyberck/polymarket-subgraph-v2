/**
 * The entity id for one (wallet, outcome token) position.
 *
 * `{lower-case address}-{decimal tokenId}` — the id format the retired
 * pnl-subgraph used. That is not cosmetic. `seed_v1final.py:184` loads the
 * subgraph's own `id` column straight out of the v1-final parquet, so a seeded
 * v1 row and an indexed v2 row for the same position collide on one key and
 * continue a single position; any other rendering would open a second one
 * beside it and split the cost basis in two.
 *
 * Lower-casing belongs here rather than at the call sites. Addresses arrive in
 * whatever case the log carried, and a checksummed address and its lower-case
 * form would otherwise be two positions for one wallet. Callers that already
 * normalise (`updateUserPositionWithBuy.ts:24`, `store.ts:124`) pass a
 * lower-cased string in, so the second call is a no-op — keeping it makes the
 * canonical form a property of the id instead of something every caller has to
 * remember.
 *
 * `tokenId` is rendered decimal and unpadded: `toString()` with no radix. Hex
 * or a zero-padded form is a different id for the same position, and matches
 * neither the seed nor the rows already written.
 *
 * @param user - Wallet address, in any case. Lower-cased here.
 * @param tokenId - ERC-1155 outcome token id (a CTF position id), as emitted.
 * @returns The `UserPosition.id`.
 *
 * @remarks
 * Identical in behaviour to `getPositionId` in `lib/store.ts:13` — same
 * expression, and the two must stay that way, because they key the same table.
 * That one is what the live handlers call (`store.ts:125`,
 * `NegRiskAdapter.ts:219`); this one is called from
 * `updateUserPositionWithBuy.ts:25`.
 *
 * Nothing splits the id back apart today, so the format's only standing
 * contract is that it does not change. The `-` would be an unambiguous
 * separator if that ever did become necessary, since neither a lower-case hex
 * address nor a decimal integer contains one.
 *
 * @example
 * ```ts
 * getUserPositionEntityId(
 *   '0xA8010b09465357748BDbFEC4725d27cd901E37cF',
 *   38802566554675533469334539678096057917098128525210300490555444287538389166025n,
 * );
 * // -> '0xa8010b09465357748bdbfec4725d27cd901e37cf-388025665546755334693345396780960
 * //     57917098128525210300490555444287538389166025'   (one line; a real row)
 * ```
 */
const getUserPositionEntityId = (user: string, tokenId: bigint): string =>
  `${user.toLowerCase()}-${tokenId.toString()}`;

export { getUserPositionEntityId };

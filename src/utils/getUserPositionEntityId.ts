const getUserPositionEntityId = (user: string, tokenId: bigint): string =>
  `${user.toLowerCase()}-${tokenId.toString()}`;

export { getUserPositionEntityId };

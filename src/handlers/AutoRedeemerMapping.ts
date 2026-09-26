/**
 * Auto-redemptions, booked from the redeemers' own events.
 *
 * Both auto-redeemers (AUTO_REDEEMER and AUTO_REDEEMER_V1) pull a wallet's
 * resolved tokens, redeem them as the ConditionalTokens redeemer, and pay the
 * wallet, emitting BinaryRedemption / NegRiskRedemption(from, conditionId,
 * payout). Measured 2026-09-25 over 20,000 blocks, every CTF pull to a
 * redeemer (117,682) has one of these for the same wallet in the same tx, so
 * the redeemers are not in LegacyAdapterTransfersMapping's transfer inference.
 *
 * AutoRedeemer's third event, Redemption(from, positionId, payout), is not
 * indexed: it redeems combo (parlay) positions held on Polymarket's separate
 * PositionManager ERC-1155 (0x006f54f7…) through its CombinatorialModule
 * (0x30000034…), and no CTF token moves. Checked 2026-09-25: none of its ids
 * is an OutcomeToken, and it never shared a tx with a CTF pull. Combos are out
 * of scope.
 */

import type { EvmEvent, EvmOnEventContext } from 'envio';
import { indexer } from 'envio';
import { COLLATERAL_SCALE } from '@utils/constants';
import { getConditionPositionIds } from '@utils/getPositionId';
import { loadUserPosition } from '@utils/loadUserPosition';
import { updateUserPositionWithSell } from '@utils/updateUserPositionWithSell';

/**
 * Books an auto-redemption by selling the wallet's whole balance of every
 * outcome at its settled price. The event has no per-outcome amounts, and
 * whole-balance held on 600/600 sampled legs (2026-09-25).
 *
 * @param context - Handler context; the entity store is written through it.
 * @param event - The redemption event being booked.
 * @param from - Wallet whose tokens were redeemed.
 * @param conditionId - Resolved condition, lowercase.
 * @param payout - Event payout (1e6); a mismatch with indexed balances is
 *   logged at debug, as expected for positions opened before start_block.
 */
const redeem = async (
  context: EvmOnEventContext,
  event: EvmEvent,
  from: string,
  conditionId: string,
  payout: bigint,
): Promise<void> => {
  const wallet = from.toLowerCase();
  const tokenIds = await getConditionPositionIds(context, conditionId);
  if (!tokenIds.length) return;
  const prices = (await context.Condition.get(conditionId))?.payoutPrices;
  if (!prices?.length) {
    context.log.warn(
      `Auto redemption skipped, no settled prices available: condition=${conditionId} ` +
        `wallet=${wallet} block=${event.block.number}`,
    );
    return;
  }

  if (tokenIds.length !== prices.length) {
    context.log.warn(`Auto redemption token length differs from payout price length:
     token length is ${tokenIds.length} and payout prices is ${prices.length}
    `);
  }

  let implied = 0n;
  const outcomes = Math.min(tokenIds.length, prices.length);
  for (let i = 0; i < outcomes; i++) {
    const position = await loadUserPosition(context, wallet, tokenIds[i]);
    if (position.amount <= 0n) continue;
    implied += (position.amount * prices[i]) / COLLATERAL_SCALE;
    await updateUserPositionWithSell(
      context,
      event,
      wallet,
      tokenIds[i],
      prices[i],
      position.amount,
      conditionId,
    );
  }

  const slack = payout / 1000n > 2n ? payout / 1000n : 2n;
  if (implied - payout > slack || payout - implied > slack) {
    context.log.debug(
      `Auto redemption payout differs from indexed balances: condition=${conditionId} ` +
        `wallet=${wallet} payout=${payout} implied=${implied} block=${event.block.number}`,
    );
  }
  return;
};

indexer.onEvent(
  { contract: 'AutoRedeemer', event: 'BinaryRedemption' },
  async ({ event, context }) => {
    await redeem(
      context,
      event,
      event.params.from,
      event.params.conditionId.toLowerCase(),
      event.params.payout,
    );
  },
);

indexer.onEvent(
  { contract: 'AutoRedeemer', event: 'NegRiskRedemption' },
  async ({ event, context }) => {
    await redeem(
      context,
      event,
      event.params.from,
      event.params.conditionId.toLowerCase(),
      event.params.payout,
    );
  },
);

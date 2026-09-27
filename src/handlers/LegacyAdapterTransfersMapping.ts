import { type EvmEvent, type EvmOnEventContext, indexer } from 'envio';
import { getAddress } from 'viem';
import {
  AUTO_REDEEMER,
  AUTO_REDEEMER_V1,
  CTF_COLLATERAL_ADAPTER_V1,
  CTF_COLLATERAL_ADAPTER_V2,
  EXCHANGE,
  EXCHANGE_V2,
  FIFTY_CENTS,
  NEG_RISK_ADAPTER,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V1,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V2,
  NEG_RISK_ESCROW_SINK,
  NEG_RISK_EXCHANGE,
  NEG_RISK_EXCHANGE_V2,
  V2_START_BLOCK,
  ZERO_ADDRESS,
} from '@utils/constants';
import { splitNegRiskPrice } from '@utils/computeNegRiskYesPrice';
import { updateUserPositionWithBuy } from '@utils/updateUserPositionWithBuy';
import { updateUserPositionWithSell } from '@utils/updateUserPositionWithSell';

const LEGACY_ADAPTERS = [CTF_COLLATERAL_ADAPTER_V1, NEG_RISK_CTF_COLLATERAL_ADAPTER_V1].map((a) =>
  getAddress(a),
);

const ADAPTER_ADDRESS_SET: ReadonlySet<string> = new Set(
  LEGACY_ADAPTERS.map((a) => a.toLowerCase()),
);

const NON_WALLET_COUNTERPARTIES: ReadonlySet<string> = new Set<string>([
  ZERO_ADDRESS,
  CTF_COLLATERAL_ADAPTER_V2,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V2,
  AUTO_REDEEMER,
  AUTO_REDEEMER_V1,
  EXCHANGE_V2,
  NEG_RISK_EXCHANGE_V2,
  NEG_RISK_ADAPTER,
  NEG_RISK_ESCROW_SINK,
  EXCHANGE,
  NEG_RISK_EXCHANGE,
]);

/**
 * Acquisition price per leg: `1/N` of the collateral, N being the outcome count.
 *
 * Asymmetric on purpose. An unregistered token carries no conditionId, so N is
 * unknowable and this assumes binary; a token that IS registered but whose
 * condition is missing returns null, and the caller skips rather than guess.
 *
 * @param context - handler context.
 * @param conditionId - the token's condition, or undefined when no
 *   `OutcomeToken` row exists for it.
 * @returns Price in 1e6 units, or null when the condition is unusable.
 */
async function getPrice(
  context: EvmOnEventContext,
  conditionId: string | undefined,
): Promise<bigint | null> {
  if (!conditionId) return FIFTY_CENTS;

  const condition = await context.Condition.get(conditionId);
  if (!condition || condition.outcomeSlotCount <= 0) return null;
  return splitNegRiskPrice(condition.outcomeSlotCount);
}

/**
 * Book a wallet<->adapter ERC-1155 leg for the gen-1 collateral adapters.
 *
 * Gen-1 emits no lifecycle events, so this transfer is the ONLY place the user
 * appears. Gen-2 is booked from its own PositionSplit/Merged/Redeemed instead.
 *
 * Direction picks the side: adapter -> wallet is an acquisition at `1/N`,
 * wallet -> adapter a disposal at the settled price once resolved, else `1/N`.
 * Settled legs read `payoutPrices[outcomeIndex]` -- by index, never by array
 * position -- and coalesce with `??`, so a losing leg's legitimate 0 is not
 * mistaken for "no price" and re-priced at 1/N.
 *
 * Returns early when both sides or neither are adapters, and when the
 * counterparty is a protocol address rather than an owner: booking those
 * invents a position for a contract that holds nothing.
 *
 * @param context - handler context.
 * @param event - source transfer, for block/log ordering.
 * @param from - sender.
 * @param to - recipient.
 * @param ids - ERC-1155 token ids; TransferSingle passes one element.
 * @param values - amounts, aligned to `ids`.
 */
async function mutate(
  context: EvmOnEventContext,
  event: EvmEvent,
  from: string,
  to: string,
  ids: readonly bigint[],
  values: readonly bigint[],
): Promise<void> {
  const fromIsAdapter = ADAPTER_ADDRESS_SET.has(from.toLowerCase());
  const toIsAdapter = ADAPTER_ADDRESS_SET.has(to.toLowerCase());

  if (fromIsAdapter === toIsAdapter) return;

  const wallet = fromIsAdapter ? to : from;
  if (NON_WALLET_COUNTERPARTIES.has(wallet.toLowerCase())) return;

  for (let i = 0; i < ids.length; i++) {
    const value = values[i];
    if (value === 0n) continue;

    const tokenId = ids[i];

    const token = await context.OutcomeToken.get(tokenId.toString());
    const conditionId = token?.conditionId;

    if (fromIsAdapter) {
      const price = await getPrice(context, conditionId);
      if (price === null) {
        context.log.warn(
          `Adapter split skipped, condition unknown: ` +
            `condition=${conditionId} token=${tokenId} wallet=${wallet} ` +
            `block=${event.block.number}`,
        );
        continue;
      }
      await updateUserPositionWithBuy(context, event, wallet, tokenId, price, value, conditionId);
    } else {
      const settled = token
        ? (await context.Condition.get(token.conditionId))?.payoutPrices?.[token.outcomeIndex]
        : undefined;

      const settledPrice = settled ?? (await getPrice(context, conditionId));
      if (settledPrice === null) {
        context.log.warn(
          `Adapter surrender skipped, condition unknown: ` +
            `condition=${conditionId} token=${tokenId} wallet=${wallet} ` +
            `block=${event.block.number}`,
        );
        continue;
      }
      await updateUserPositionWithSell(
        context,
        event,
        wallet,
        tokenId,
        settledPrice,
        value,
        conditionId,
      );
    }
  }
}

indexer.onEvent(
  {
    contract: 'ConditionalTokens',
    event: 'TransferSingle',
    where: {
      params: [{ from: LEGACY_ADAPTERS }, { to: LEGACY_ADAPTERS }],
      block: { number: { _gte: V2_START_BLOCK + 1 } },
    },
  },
  async ({ event, context }) => {
    await mutate(
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
      params: [{ from: LEGACY_ADAPTERS }, { to: LEGACY_ADAPTERS }],
      block: { number: { _gte: V2_START_BLOCK + 1 } },
    },
  },
  async ({ event, context }) => {
    await mutate(
      context,
      event,
      event.params.from,
      event.params.to,
      event.params.ids,
      event.params.values,
    );
  },
);

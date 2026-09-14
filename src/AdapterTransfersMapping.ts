import { type EvmEvent, type EvmOnEventContext, indexer } from 'envio';
import { getAddress } from 'viem';
import {
  AUTO_REDEEMER,
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
import { negRiskEventSize } from '@utils/negrisk-utils';
import { splitNegRiskPrice } from '@utils/computeNegRiskYesPrice';
import { updateUserPositionWithBuy } from '@utils/updateUserPositionWithBuy';
import { upsertPayoutPrices } from '@utils/upsertCondition';
import { updateUserPositionWithSell } from '@utils/updateUserPositionWithSell';

const V2_ADAPTERS = [
  CTF_COLLATERAL_ADAPTER_V1,
  CTF_COLLATERAL_ADAPTER_V2,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V1,
  NEG_RISK_CTF_COLLATERAL_ADAPTER_V2,
  AUTO_REDEEMER,
].map((a) => getAddress(a));

const ADAPTER_ADDRESS_SET = new Set(V2_ADAPTERS.map((a) => a.toLowerCase()));

const NON_WALLET_COUNTERPARTIES: ReadonlySet<string> = new Set<string>([
  ZERO_ADDRESS,
  EXCHANGE_V2,
  NEG_RISK_EXCHANGE_V2,
  NEG_RISK_ADAPTER,
  NEG_RISK_ESCROW_SINK,
  EXCHANGE,
  NEG_RISK_EXCHANGE,
]);

async function getPrice(
  context: EvmOnEventContext,
  conditionId: string | undefined,
): Promise<bigint | null> {
  if (!conditionId) return FIFTY_CENTS;

  let count = await negRiskEventSize(context, conditionId);
  if (count === 0) {
    count = (await context.Condition.get(conditionId))?.outcomeSlotCount ?? null;
  }
  if (count === null) return null;
  return count > 0 ? splitNegRiskPrice(count) : FIFTY_CENTS;
}

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
          `Adapter split skipped, neg-risk question count unreadable: ` +
            `condition=${conditionId} token=${tokenId} wallet=${wallet} ` +
            `block=${event.block.number}`,
        );
        continue;
      }
      await updateUserPositionWithBuy(context, event, wallet, tokenId, price, value, conditionId);
    } else {
      const settled = token
        ? (await upsertPayoutPrices(context, token.conditionId))?.[token.outcomeIndex]
        : undefined;

      const settledPrice = settled ?? (await getPrice(context, conditionId));
      if (settledPrice === null) {
        context.log.warn(
          `Adapter surrender skipped, neg-risk question count unreadable: ` +
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
      params: [{ from: V2_ADAPTERS }, { to: V2_ADAPTERS }],
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
      params: [{ from: V2_ADAPTERS }, { to: V2_ADAPTERS }],
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

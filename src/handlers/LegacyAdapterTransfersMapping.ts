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
  REDEMPTION_OPERATORS,
  V2_START_BLOCK,
  ZERO_ADDRESS,
} from '@utils/constants';
import { splitNegRiskPrice } from '@utils/computeNegRiskYesPrice';
import { loadUserPosition } from '@utils/loadUserPosition';
import { updateUserPositionWithBuy } from '@utils/updateUserPositionWithBuy';
import { updateUserPositionWithSell } from '@utils/updateUserPositionWithSell';

const LEGACY_ADAPTERS = [CTF_COLLATERAL_ADAPTER_V1, NEG_RISK_CTF_COLLATERAL_ADAPTER_V1].map((a) =>
  getAddress(a),
);

const ADAPTER_ADDRESS_SET: ReadonlySet<string> = new Set(
  LEGACY_ADAPTERS.map((a) => a.toLowerCase()),
);

const REDEMPTION_OPERATOR_LIST = REDEMPTION_OPERATORS.map((a) => getAddress(a));
const REDEMPTION_OPERATOR_SET: ReadonlySet<string> = new Set(
  REDEMPTION_OPERATORS.map((a) => a.toLowerCase()),
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
 * Price per leg, `1/N`. No conditionId assumes binary; a known condition
 * without an outcome count returns null.
 *
 * @param context - handler context.
 * @param conditionId - the token's condition; undefined if unregistered.
 * @returns Price in 1e6, or null to skip.
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
 * Book a gen-1 adapter transfer or a redemption operator's pull.
 *
 * - adapter -> wallet: a buy at `1/N`; gen 1 emits no lifecycle events.
 * - wallet -> adapter: a sale at the settled price, else `1/N`.
 * - `REDEMPTION_OPERATORS` pull from a wallet: a sale at the settled price.
 *
 * @param context - handler context.
 * @param event - source transfer, for block/log ordering.
 * @param operator - the transfer's operator.
 * @param from - sender.
 * @param to - recipient.
 * @param ids - ERC-1155 token ids; TransferSingle passes one element.
 * @param values - amounts, aligned to `ids`.
 */
async function mutate(
  context: EvmOnEventContext,
  event: EvmEvent,
  operator: string,
  from: string,
  to: string,
  ids: readonly bigint[],
  values: readonly bigint[],
): Promise<void> {
  const fromIsAdapter = ADAPTER_ADDRESS_SET.has(from.toLowerCase());
  const toIsAdapter = ADAPTER_ADDRESS_SET.has(to.toLowerCase());
  const op = operator.toLowerCase();
  const isPull = REDEMPTION_OPERATOR_SET.has(op) && !fromIsAdapter && !toIsAdapter;
  if (!isPull && fromIsAdapter === toIsAdapter) return;

  const wallet = fromIsAdapter ? to : from;
  const owner = wallet.toLowerCase();
  if (NON_WALLET_COUNTERPARTIES.has(owner) || (isPull && owner === op)) return;

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
      continue;
    }

    const settledPrice = token
      ? (await context.Condition.get(token.conditionId))?.payoutPrices?.[token.outcomeIndex]
      : undefined;

    let price: bigint;
    if (settledPrice !== undefined) {
      price = settledPrice;
    } else if (isPull) {
      price = (await loadUserPosition(context, owner, tokenId)).avgPrice;
      context.log.warn(
        `Redemption pull before market settlement, disposed at cost: ` +
          `token=${tokenId} wallet=${wallet} block=${event.block.number}`,
      );
    } else {
      const avgPrice = await getPrice(context, conditionId);
      if (avgPrice === null) {
        context.log.warn(
          `Adapter surrender skipped, condition unknown: ` +
            `condition=${conditionId} token=${tokenId} wallet=${wallet} ` +
            `block=${event.block.number}`,
        );
        continue;
      }
      price = avgPrice;
    }

    await updateUserPositionWithSell(context, event, wallet, tokenId, price, value, conditionId);
  }
}

indexer.onEvent(
  {
    contract: 'ConditionalTokens',
    event: 'TransferSingle',
    where: {
      params: [
        { from: LEGACY_ADAPTERS },
        { to: LEGACY_ADAPTERS },
        { operator: REDEMPTION_OPERATOR_LIST },
      ],
      block: { number: { _gte: V2_START_BLOCK + 1 } },
    },
  },
  async ({ event, context }) => {
    await mutate(
      context,
      event,
      event.params.operator,
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
      params: [
        { from: LEGACY_ADAPTERS },
        { to: LEGACY_ADAPTERS },
        { operator: REDEMPTION_OPERATOR_LIST },
      ],
      block: { number: { _gte: V2_START_BLOCK + 1 } },
    },
  },
  async ({ event, context }) => {
    await mutate(
      context,
      event,
      event.params.operator,
      event.params.from,
      event.params.to,
      event.params.ids,
      event.params.values,
    );
  },
);

import { type EvmEvent, type EvmOnEventContext, indexer } from 'envio';
import { getAddress } from 'viem';
import {
  AUTO_REDEEMER,
  AUTO_REDEEMER_V1,
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
  REDEMPTION_OPERATORS,
  V2_START_BLOCK,
  ZERO_ADDRESS,
} from '@utils/constants';
import { splitNegRiskPrice } from '@utils/computeNegRiskYesPrice';
import { loadUserPosition } from '@utils/loadUserPosition';
import { updateUserPositionWithTransfer } from '@utils/updateUserPositionWithTransfer';
import { updateUserPositionWithBuy } from '@utils/updateUserPositionWithBuy';
import { updateUserPositionWithSell } from '@utils/updateUserPositionWithSell';

/**
 * Book wallet-to-wallet transfers. Off unless `INDEX_INCLUDE_P2P_TRANSFERS=true`.
 */
const INCLUDE_P2P_TRANSFERS = process.env.INDEX_INCLUDE_P2P_TRANSFERS === 'true';

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

/** Addresses whose transfers another handler books, or that hold no position */
const P2P_EXCLUDED: ReadonlySet<string> = new Set<string>([
  ...NON_WALLET_COUNTERPARTIES,
  ...ADAPTER_ADDRESS_SET,
  ...REDEMPTION_OPERATOR_SET,
  EXCHANGE_V2_THIRD,
  COMBOS_EXCHANGE,
  COMBOS_POSITION_MANAGER,
  CONDITIONAL_TOKENS,
]);

const BLOCK_FILTER = { number: { _gte: V2_START_BLOCK + 1 } };

/** Delivery filter */
const TRANSFER_FILTERS = INCLUDE_P2P_TRANSFERS
  ? { block: BLOCK_FILTER }
  : {
      params: [
        { from: LEGACY_ADAPTERS },
        { to: LEGACY_ADAPTERS },
        { operator: REDEMPTION_OPERATOR_LIST },
      ],
      block: BLOCK_FILTER,
    };

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
 * Book a gen-1 adapter transfer, a redemption operator's pull, or (with
 * `INCLUDE_P2P_TRANSFERS`) a wallet-to-wallet transfer.
 *
 * - adapter -> wallet: a buy at `1/N`; gen 1 emits no lifecycle events.
 * - wallet -> adapter: a sale at the settled price, else `1/N`.
 * - `REDEMPTION_OPERATORS` pull from a wallet: a sale at the settled price,
 *   else the wallet's average.
 * - wallet -> wallet: the sender sells as a pull does; the receiver acquires
 *   the full value at that price, `totalBought` unchanged.
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
  const sender = from.toLowerCase();
  const recipient = to.toLowerCase();
  const op = operator.toLowerCase();

  const fromIsAdapter = ADAPTER_ADDRESS_SET.has(sender);
  const toIsAdapter = ADAPTER_ADDRESS_SET.has(recipient);
  const isPull = REDEMPTION_OPERATOR_SET.has(op) && !fromIsAdapter && !toIsAdapter;
  const isP2p =
    INCLUDE_P2P_TRANSFERS &&
    !isPull &&
    sender !== recipient &&
    ![op, sender, recipient].some((a) => P2P_EXCLUDED.has(a));
  if (!isPull && !isP2p && fromIsAdapter === toIsAdapter) return;

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
    } else if (isPull || isP2p) {
      const position = await loadUserPosition(context, owner, tokenId);
      price = position.avgPrice + position.avgFeePrice;
      if (isPull) {
        context.log.warn(
          `Redemption pull before market settlement, disposed at cost: ` +
            `token=${tokenId} wallet=${wallet} block=${event.block.number}`,
        );
      }
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
    if (isP2p)
      await updateUserPositionWithTransfer(context, event, to, tokenId, price, value, conditionId);
  }
}

indexer.onEvent(
  {
    contract: 'ConditionalTokens',
    event: 'TransferSingle',
    where: TRANSFER_FILTERS,
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
    where: TRANSFER_FILTERS,
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

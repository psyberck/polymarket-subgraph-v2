import { indexer } from 'envio';
import { AUTO_REDEEMER, AUTO_REDEEMER_V1 } from '@utils/constants';
import { applyPositionMerge, applyPositionRedeem, applyPositionSplit } from '@utils/adapter-utils';
import { getConditionPositionIds } from '@utils/getPositionId';
import { applyNegRiskConversion, recordSplitMarker } from '@utils/negrisk-utils';

const NON_OWNERS: ReadonlySet<string> = new Set<string>([AUTO_REDEEMER, AUTO_REDEEMER_V1]);

indexer.onEvent(
  { contract: 'NegRiskCtfCollateralAdapter', event: 'PositionSplit' },
  async ({ event, context }) => {
    const wallet = event.params.initiator.toLowerCase();
    const conditionId = event.params.conditionId.toLowerCase();
    if (NON_OWNERS.has(wallet)) return;

    const positionIds = await getConditionPositionIds(context, conditionId);
    if (!positionIds.length) return;

    await applyPositionSplit(context, event, wallet, conditionId, event.params.amount);
    await recordSplitMarker(
      context,
      wallet,
      conditionId,
      event.transaction.hash,
      event.params.amount,
    );
  },
);

indexer.onEvent(
  { contract: 'NegRiskCtfCollateralAdapter', event: 'PositionsMerged' },
  async ({ event, context }) => {
    const wallet = event.params.initiator.toLowerCase();
    if (NON_OWNERS.has(wallet)) return;

    await applyPositionMerge(
      context,
      event,
      wallet,
      event.params.conditionId.toLowerCase(),
      event.params.amount,
    );
  },
);

indexer.onEvent(
  { contract: 'NegRiskCtfCollateralAdapter', event: 'PositionsConverted' },
  async ({ event, context }) => {
    const wallet = event.params.initiator.toLowerCase();
    if (NON_OWNERS.has(wallet)) return;

    await applyNegRiskConversion(
      context,
      event,
      wallet,
      event.params.marketId.toLowerCase(),
      event.params.indexSet,
      event.params.amount,
      event.params.amountOut,
      event.transaction.hash,
    );
  },
);

indexer.onEvent(
  { contract: 'NegRiskCtfCollateralAdapter', event: 'PositionsRedeemed' },
  async ({ event, context }) => {
    const wallet = event.params.initiator.toLowerCase();
    if (NON_OWNERS.has(wallet)) return;

    await applyPositionRedeem(
      context,
      event,
      wallet,
      event.params.conditionId.toLowerCase(),
      event.params.amounts,
    );
  },
);

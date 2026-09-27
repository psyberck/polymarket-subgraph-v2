import { indexer } from 'envio';
import { applyPositionMerge, applyPositionRedeem, applyPositionSplit } from '@utils/adapter-utils';
import { AUTO_REDEEMER, AUTO_REDEEMER_V1 } from '@utils/constants';

const ADAPTER_NON_OWNERS: ReadonlySet<string> = new Set<string>([AUTO_REDEEMER, AUTO_REDEEMER_V1]);

indexer.onEvent(
  { contract: 'CtfCollateralAdapter', event: 'PositionSplit' },
  async ({ event, context }) => {
    const wallet = event.params.initiator.toLowerCase();
    if (ADAPTER_NON_OWNERS.has(wallet)) return;

    await applyPositionSplit(
      context,
      event,
      wallet,
      event.params.conditionId.toLowerCase(),
      event.params.amount,
    );
  },
);

indexer.onEvent(
  { contract: 'CtfCollateralAdapter', event: 'PositionsMerged' },
  async ({ event, context }) => {
    const wallet = event.params.initiator.toLowerCase();
    if (ADAPTER_NON_OWNERS.has(wallet)) return;

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
  { contract: 'CtfCollateralAdapter', event: 'PositionsRedeemed' },
  async ({ event, context }) => {
    const wallet = event.params.initiator.toLowerCase();
    if (ADAPTER_NON_OWNERS.has(wallet)) return;

    await applyPositionRedeem(
      context,
      event,
      wallet,
      event.params.conditionId.toLowerCase(),
      event.params.amounts,
    );
  },
);

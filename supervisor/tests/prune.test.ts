/* eslint-disable @typescript-eslint/no-floating-promises -- node:test awaits describe/it itself */
/** Snapshot layout and prune's manifest reads (snapshot-design.md §3.7–3.8). */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { fileRows } from '#supervisor/prune.ts';
import {
  FINALIZED_FILE,
  FINALIZED_POSITIONS,
  type Manifest,
  snapshotFiles,
  TRADES_FILE,
} from '#supervisor/snapshot.ts';
import { loadSchema } from '#supervisor/typemap.ts';

describe('fileRows', () => {
  const manifest = {
    endBlockId: 200,
    files: { [TRADES_FILE]: { rows: 42 } },
  } as unknown as Manifest;

  it("reads a file's row count from the manifest", () => {
    assert.equal(fileRows(manifest, TRADES_FILE), 42);
  });

  it('throws for a file the manifest does not list', () => {
    assert.throws(
      () => fileRows(manifest, FINALIZED_FILE),
      /invalid manifest for block 200: no UserPosition/,
    );
  });
});

describe('FINALIZED_POSITIONS', () => {
  it('requires a balance below the dust and a resolved market, with no grace period', () => {
    assert.match(FINALIZED_POSITIONS, /amount < 100000 /);
    assert.match(FINALIZED_POSITIONS, /"payoutPrices" IS NOT NULL/);
    assert.doesNotMatch(FINALIZED_POSITIONS, /lastUpdatedAt/);
  });
});

describe('snapshotFiles', () => {
  const files = snapshotFiles(loadSchema(), 100, 200);
  const file = (name: string) => files.find((f) => f.name === name)!;

  it('lists trades, finalized positions and the restore point', () => {
    assert.deepEqual(
      files.map((f) => f.name),
      [
        TRADES_FILE,
        FINALIZED_FILE,
        'UserPosition.parquet',
        'NegRiskEvent.parquet',
        'Condition.parquet',
        'OutcomeToken.parquet',
      ],
    );
    assert.equal(file(TRADES_FILE).where, '"blockNumber" > 100 AND "blockNumber" <= 200');
  });

  it('keeps the two position files disjoint', () => {
    assert.equal(file(FINALIZED_FILE).where, FINALIZED_POSITIONS);
    assert.equal(file('UserPosition.parquet').where, `(${FINALIZED_POSITIONS}) IS NOT TRUE`);
    assert.equal(file('Condition.parquet').where, undefined);
  });

  it('refuses an empty trade interval', () => {
    assert.throws(() => snapshotFiles(loadSchema(), 200, 200), /empty trade interval/);
  });
});

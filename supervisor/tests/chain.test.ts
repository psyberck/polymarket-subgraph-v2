/* eslint-disable @typescript-eslint/no-floating-promises -- node:test awaits describe/it itself */
/** Snapshot boundaries (snapshot-design.md §3.2). */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { atBoundary, nextEndBlockId, type PolygonChainState } from '#supervisor/chain.ts';
import { parseConfig } from '#supervisor/config.ts';

/** A snapshot every 100 blocks. */
const every100 = parseConfig(
  ['--schedule', 'stride', '--stride', '100', '--output-dir', '/tmp/out'],
  {},
);
/** No schedule: follow the head. */
const none = parseConfig(['--output-dir', '/tmp/out'], {});

/** Envio's state, indexing from block 1000. */
const chain = (progressBlock: number, endBlock: number | null): PolygonChainState => ({
  startBlock: 1000,
  endBlock,
  progressBlock,
  sourceBlock: 5000,
  maxReorgDepth: 200,
});

describe('atBoundary', () => {
  it('is true once Envio reaches a scheduled end block', () => {
    assert.equal(atBoundary(every100, chain(1099, 1099)), true);
  });

  it('is false before the end block, without one, or without a schedule', () => {
    assert.equal(atBoundary(every100, chain(1050, 1099)), false);
    assert.equal(atBoundary(every100, chain(1050, null)), false);
    assert.equal(atBoundary(none, chain(1099, 1099)), false);
  });
});

describe('nextEndBlockId', () => {
  it('starts one stride after the start block on a fresh schema', () => {
    assert.equal(nextEndBlockId(every100, chain(-1, null)), 1099);
  });

  it('keeps a pending end block when restarted mid-interval', () => {
    assert.equal(nextEndBlockId(every100, chain(1050, 1099)), 1099);
  });

  it('moves one stride on at a boundary, so snapshots chain end to end', () => {
    assert.equal(nextEndBlockId(every100, chain(1099, 1099)), 1199);
  });

  it('follows the head without a schedule, even with an end block left over', () => {
    assert.equal(nextEndBlockId(none, chain(1050, null)), null);
    assert.equal(nextEndBlockId(none, chain(1050, 1099)), null);
  });
});

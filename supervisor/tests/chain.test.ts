/* eslint-disable @typescript-eslint/no-floating-promises -- node:test awaits describe/it itself */
/** Snapshot boundaries (snapshot-design.md §3.2) on a simulated chain. */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  type BlockClock,
  nextBoundary,
  nextMonday,
  nextMonthStart,
  periodId,
} from '#supervisor/chain.ts';

const utc = (iso: string): number => Date.parse(iso) / 1000;

/**
 * A chain whose block n has timestamp genesis + n * 1.5 s, rounded down as
 * real timestamps are, with the head at `headBlock`. Counts lookups.
 */
function chain(genesisIso: string, headBlock: number) {
  const genesis = utc(genesisIso);
  const lookups = { n: 0 };
  const ts = (n: number) => genesis + Math.floor(n * 1.5);
  const clock: BlockClock = {
    head: async () => ({ number: headBlock, timestamp: ts(headBlock) }),
    timestamp: async (n) => {
      if (n > headBlock) throw new Error(`block ${n} is beyond the head ${headBlock}`);
      lookups.n++;
      return ts(n);
    },
  };
  return { clock, ts, lookups };
}

describe('calendar boundaries', () => {
  it('finds the next month start, strictly after', () => {
    assert.equal(nextMonthStart(utc('2026-09-15T12:00:00Z')), utc('2026-10-01T00:00:00Z'));
    assert.equal(nextMonthStart(utc('2026-10-01T00:00:00Z')), utc('2026-11-01T00:00:00Z'));
    assert.equal(nextMonthStart(utc('2026-12-31T23:59:59Z')), utc('2027-01-01T00:00:00Z'));
  });

  it('finds the next Monday 00:00 UTC, strictly after', () => {
    assert.equal(nextMonday(utc('2026-09-27T23:59:59Z')), utc('2026-09-28T00:00:00Z')); // Sun
    assert.equal(nextMonday(utc('2026-09-28T00:00:00Z')), utc('2026-10-05T00:00:00Z')); // Mon 00:00
    assert.equal(nextMonday(utc('2026-09-30T10:00:00Z')), utc('2026-10-05T00:00:00Z')); // Wed
  });

  it('names the period that ends at a boundary', () => {
    assert.equal(periodId('monthly', utc('2026-10-01T00:00:00Z')), '2026-09');
    assert.equal(periodId('monthly', utc('2027-01-01T00:00:00Z')), '2026-12');
    assert.equal(periodId('weekly', utc('2026-09-28T00:00:00Z')), '2026-W39');
    assert.equal(periodId('weekly', utc('2026-10-05T00:00:00Z')), '2026-W40');
    // ISO years: the week ending 2027-01-03 is 2026's 53rd.
    assert.equal(periodId('weekly', utc('2027-01-04T00:00:00Z')), '2026-W53');
    assert.equal(periodId('weekly', utc('2021-01-04T00:00:00Z')), '2020-W53');
  });
});

describe('nextBoundary', () => {
  it('returns null without a schedule', async () => {
    assert.equal(
      await nextBoundary(null, null, 100, chain('2026-09-01T00:00:00Z', 1000).clock),
      null,
    );
  });

  it('takes the next multiple of the stride, strictly after', async () => {
    const { clock } = chain('2026-09-01T00:00:00Z', 1000);
    assert.deepEqual(await nextBoundary('stride', 100, 94_799_999, clock), {
      kind: 'block',
      b: 94_800_000,
      id: 'b94800000',
    });
    assert.deepEqual(await nextBoundary('stride', 100, 94_800_000, clock), {
      kind: 'block',
      b: 94_800_100,
      id: 'b94800100',
    });
  });

  it('puts a monthly B on the last block before 00:00 UTC on the 1st', async () => {
    // Genesis 2026-09-20; 2026-10-01 is ~11 days (~633,600 blocks) later.
    const { clock, ts, lookups } = chain('2026-09-20T00:00:00Z', 2_000_000);
    const result = await nextBoundary('monthly', null, 1_000, clock);
    assert.ok(result?.kind === 'block');
    const t = utc('2026-10-01T00:00:00Z');
    assert.ok(ts(result.b) < t && ts(result.b + 1) >= t, `B ${result.b}`);
    assert.equal(result.id, '2026-09');
    assert.ok(lookups.n < 30, `binary search used ${lookups.n} lookups`);
  });

  it('moves to the next period once progress has reached B', async () => {
    const { clock, ts } = chain('2026-09-20T00:00:00Z', 4_000_000);
    const first = await nextBoundary('monthly', null, 1_000, clock);
    assert.ok(first?.kind === 'block');
    const again = await nextBoundary('monthly', null, first.b, clock);
    assert.ok(again?.kind === 'block');
    const t = utc('2026-11-01T00:00:00Z');
    assert.ok(ts(again.b) < t && ts(again.b + 1) >= t);
    assert.equal(again.id, '2026-10');
    // One block short of B is still before it.
    assert.deepEqual(await nextBoundary('monthly', null, first.b - 1, clock), first);
  });

  it('puts a weekly B on the last block before Monday 00:00 UTC', async () => {
    const { clock, ts } = chain('2026-09-24T00:00:00Z', 1_000_000); // a Thursday
    const result = await nextBoundary('weekly', null, 10, clock);
    assert.ok(result?.kind === 'block');
    const t = utc('2026-09-28T00:00:00Z');
    assert.ok(ts(result.b) < t && ts(result.b + 1) >= t);
    assert.equal(result.id, '2026-W39');
  });

  it('estimates a boundary the chain has not reached yet', async () => {
    // Head on 2026-09-30, a day before the month ends.
    const { clock, ts } = chain('2026-09-29T00:00:00Z', 57_600);
    const result = await nextBoundary('monthly', null, 57_000, clock);
    assert.ok(result?.kind === 'future');
    assert.equal(result.at, utc('2026-10-01T00:00:00Z'));
    assert.equal(result.id, '2026-09');
    // The estimate is the block 1.5 s blocks would put just before T.
    const expected = 57_600 + Math.ceil((result.at - ts(57_600)) / 1.5) - 1;
    assert.equal(result.estimate, expected);
  });

  it('handles progress already at the head', async () => {
    const { clock } = chain('2026-09-29T00:00:00Z', 57_600);
    const result = await nextBoundary('weekly', null, 57_600, clock);
    assert.ok(result?.kind === 'future');
    assert.equal(result.at, utc('2026-10-05T00:00:00Z'));
  });
});

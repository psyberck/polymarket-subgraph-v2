/* eslint-disable @typescript-eslint/no-floating-promises -- node:test awaits describe/it itself */
/** Trade retention's archive check (snapshot-design.md §3.7–3.8). */
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { archivedThrough, closedPositionsWhere } from '#supervisor/prune.ts';

const dirs: string[] = [];
after(() => dirs.forEach((d) => rmSync(d, { recursive: true, force: true })));

/** An output directory holding trade manifests for the given (bPrev, b] ranges. */
function outputWith(ranges: [number, number][]): string {
  const dir = mkdtempSync(join(tmpdir(), 'prune-test-'));
  dirs.push(dir);
  mkdirSync(join(dir, 'trades'));
  for (const [bPrev, b] of ranges)
    writeFileSync(
      join(dir, 'trades', `b${bPrev + 1}-b${b}.parquet.manifest.json`),
      JSON.stringify({ kind: 'trades', bPrev, b }),
    );
  return dir;
}

describe('archivedThrough', () => {
  it('follows an unbroken chain to its end', () => {
    assert.equal(
      archivedThrough(
        outputWith([
          [99, 200],
          [200, 300],
          [300, 450],
        ]),
        99,
      ),
      450,
    );
  });

  it('stops at a gap: nothing beyond it counts as archived', () => {
    assert.equal(
      archivedThrough(
        outputWith([
          [99, 200],
          [250, 300],
          [300, 450],
        ]),
        99,
      ),
      200,
    );
  });

  it('archives nothing when the chain does not start where indexing did', () => {
    assert.equal(archivedThrough(outputWith([[150, 200]]), 99), 99);
  });

  it('archives nothing without trade files', () => {
    const dir = mkdtempSync(join(tmpdir(), 'prune-test-'));
    dirs.push(dir);
    assert.equal(archivedThrough(dir, 99), 99);
  });
});

describe('closedPositionsWhere', () => {
  it('requires a zero balance, a resolved market and the grace period', () => {
    const where = closedPositionsWhere(1_790_000_000);
    assert.match(where, /amount = 0/);
    assert.match(where, /"lastUpdatedAt" < 1790000000/);
    assert.match(where, /"payoutPrices" IS NOT NULL/);
  });
});

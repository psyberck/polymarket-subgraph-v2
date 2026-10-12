/* eslint-disable @typescript-eslint/no-floating-promises -- node:test awaits describe/it itself */
/** Manifests on disk (snapshot-design.md §3.6): the only record of what was archived. */
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  type Manifest,
  MANIFEST_FILE,
  outputFolderPath,
  readManifest,
  snapshotManifests,
  writeManifest,
} from '#supervisor/snapshot.ts';

const dirs: string[] = [];
after(() => dirs.forEach((d) => rmSync(d, { recursive: true, force: true })));
const outputDir = () => {
  const dir = mkdtempSync(join(tmpdir(), 'snapshot-test-'));
  dirs.push(dir);
  return dir;
};

const manifest = (startBlockId: number, endBlockId: number): Manifest => ({
  schedule: 'stride',
  startBlockId,
  endBlockId,
  blockHash: '0xabc',
  sourceBlock: endBlockId + 1000,
  maxReorgDepth: 200,
  envioVersion: '3.6.1',
  schemaSha256: 'f00',
  createdAt: '2026-10-11T00:00:00.000Z',
  files: {},
});

/** A snapshot folder holding `m` as its manifest. */
const archived = (out: string, m: Manifest) => {
  const folder = outputFolderPath(out, m.endBlockId);
  mkdirSync(folder, { recursive: true });
  writeManifest(join(folder, MANIFEST_FILE), m);
};

describe('writeManifest and readManifest', () => {
  it('round-trips a manifest and leaves no .partial behind', () => {
    const folder = outputFolderPath(outputDir(), 1099);
    mkdirSync(folder);
    writeManifest(join(folder, MANIFEST_FILE), manifest(999, 1099));
    assert.deepEqual(readManifest(folder), manifest(999, 1099));
    assert.deepEqual(readdirSync(folder), [MANIFEST_FILE]);
  });

  it('reads null for a folder whose snapshot never completed', () => {
    const folder = outputFolderPath(outputDir(), 1099);
    mkdirSync(folder);
    writeFileSync(join(folder, 'UserPosition.parquet.partial'), '');
    assert.equal(readManifest(folder), null);
  });
});

describe('snapshotManifests', () => {
  it('lists complete snapshots in block order, numerically', () => {
    const out = outputDir();
    archived(out, manifest(999, 1099));
    archived(out, manifest(9, 99)); // lexically after 1099, numerically first
    archived(out, manifest(99, 999));
    assert.deepEqual(
      snapshotManifests(out).map((m) => m.endBlockId),
      [99, 999, 1099],
    );
  });

  it('skips folders without a manifest and names that are not block numbers', () => {
    const out = outputDir();
    archived(out, manifest(999, 1099));
    mkdirSync(outputFolderPath(out, 1199)); // export interrupted: no manifest
    mkdirSync(join(out, 'notes'));
    archived(out, { ...manifest(0, 1), endBlockId: 5 });
    rmSync(join(out, '5'), { recursive: true }); // removed again
    assert.deepEqual(
      snapshotManifests(out).map((m) => m.endBlockId),
      [1099],
    );
  });

  it('is empty for an output directory that does not exist yet', () => {
    const missing = join(outputDir(), 'not-created');
    assert.equal(existsSync(missing), false);
    assert.deepEqual(snapshotManifests(missing), []);
  });
});

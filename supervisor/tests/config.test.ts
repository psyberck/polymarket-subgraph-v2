/* eslint-disable @typescript-eslint/no-floating-promises -- node:test awaits describe/it itself */
/** Supervisor flags and environment (snapshot-design.md §3.9). */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { InvalidValueError, parseConfig } from '#supervisor/config.ts';
import { snapshotEntities } from '#supervisor/snapshot.ts';
import { loadSchema } from '#supervisor/typemap.ts';

/** --output-dir is required; supply one unless the test is about it. */
const parse = (args: string[], env: NodeJS.ProcessEnv = {}) =>
  parseConfig(
    args.some((a) => a.startsWith('--output-dir')) ? args : [...args, '--output-dir', '/tmp/out'],
    env,
  );

describe('parseConfig', () => {
  it('defaults to no schedule, no pruning and 2GB / 2 threads', () => {
    const config = parse([]);
    assert.equal(config.schedule, null);
    assert.equal(config.stride, null);
    assert.equal(config.prune, false);
    assert.equal(config.debug, false);
    assert.equal(config.memoryLimit, '2GB');
    assert.equal(config.threads, 2);
  });

  it('reads every flag', () => {
    const config = parse(['--schedule', 'weekly', '--prune', '--debug', '--output-dir', '/tmp/s']);
    assert.equal(config.schedule, 'weekly');
    assert.equal(config.prune, true);
    assert.equal(config.debug, true);
    assert.equal(config.outputDir, '/tmp/s');
    assert.equal(parse(['--schedule=monthly']).schedule, 'monthly');
  });

  it('turns weekly and monthly into a stride at 2 s blocks', () => {
    assert.equal(parse(['--schedule', 'weekly']).stride, 302_400);
    assert.equal(parse(['--schedule', 'monthly']).stride, 1_296_000);
  });

  it('takes any positive stride with --schedule stride, with no minimum', () => {
    const config = parse(['--schedule', 'stride', '--stride', '40']);
    assert.equal(config.schedule, 'stride');
    assert.equal(config.stride, 40);
  });

  it('rejects a stride schedule without --stride, and --stride without it', () => {
    assert.throws(() => parse(['--schedule', 'stride']), /needs --stride/);
    assert.throws(() => parse(['--stride', '100']), /only valid with --schedule stride/);
    assert.throws(() => parse(['--schedule', 'weekly', '--stride', '100']), /only valid/);
  });

  it('rejects a stride that is not a positive integer', () => {
    for (const bad of ['0', '-5', '1.5', 'abc', '1e6'])
      assert.throws(
        () => parse(['--schedule', 'stride', `--stride=${bad}`]),
        /expected a positive integer/,
        bad,
      );
  });

  it('rejects unknown values and unknown flags', () => {
    assert.throws(
      () => parse(['--schedule', 'daily']),
      /--schedule: expected monthly \| weekly \| stride/,
    );
    assert.throws(() => parse(['--prune', 'yes']), /positional/i); // a switch takes no value
    assert.throws(() => parse(['--prune=yes']), /does not take an argument/);
    assert.throws(() => parse(['--shedule', 'weekly']), /Unknown option '--shedule'/);
    assert.throws(() => parse(['weekly']), /positional/i);
    assert.throws(() => parse(['--dry-run']), /Unknown option '--dry-run'/);
    assert.throws(() => parse(['--folderPath', '/tmp/s']), /Unknown option '--folderPath'/);
  });

  it('prunes for real only with --prune alone; --debug needs --prune', () => {
    assert.deepEqual([parse(['--prune']).prune, parse(['--prune']).debug], [true, false]);
    assert.throws(() => parse(['--debug']), /--debug is only valid with --prune/);
  });

  it('requires --output-dir and takes it as given', () => {
    assert.throws(() => parseConfig([], {}), /--output-dir: expected a output folder path/);
    assert.throws(() => parse(['--output-dir', '']), /--output-dir: expected a output folder path/);
    assert.equal(parse(['--output-dir', 'out/x']).outputDir, 'out/x');
  });

  it('reads the DuckDB limits from the environment', () => {
    const config = parse([], { DUCKDB_MEMORY_LIMIT: '4GB', DUCKDB_THREADS: '4' });
    assert.equal(config.memoryLimit, '4GB');
    assert.equal(config.threads, 4);
    assert.throws(() => parse([], { DUCKDB_MEMORY_LIMIT: 'lots' }), /DUCKDB_MEMORY_LIMIT/);
    assert.throws(() => parse([], { DUCKDB_THREADS: '0' }), /DUCKDB_THREADS/);
  });
});

describe('InvalidValueError', () => {
  it('is what every invalid flag or variable throws, parseArgs errors included', () => {
    const bad: [string[], NodeJS.ProcessEnv][] = [
      [['--schedule', 'daily'], {}], // bad value
      [['--schedule', 'stride'], {}], // missing --stride
      [['--stride', '100'], {}], // --stride without stride schedule
      [['--schedule', 'stride', '--stride=0'], {}], // not a positive integer
      [['--debug'], {}], // --debug without --prune
      [['--output-dir', ''], {}], // empty path
      [['--shedule', 'weekly'], {}], // unknown flag (parseArgs)
      [['--prune=yes'], {}], // value for a switch (parseArgs)
      [['weekly'], {}], // positional (parseArgs)
      [[], { DUCKDB_THREADS: '0' }], // bad environment variable
    ];
    for (const [args, env] of bad)
      assert.throws(
        () => parse(args, env),
        InvalidValueError,
        args.join(' ') || JSON.stringify(env),
      );
    assert.throws(() => parseConfig([], {}), InvalidValueError); // no --output-dir
    assert.throws(() => parse([], { DUCKDB_MEMORY_LIMIT: 'lots' }), InvalidValueError);
  });

  it('keeps parseArgs messages and their cause', () => {
    try {
      parse(['--shedule', 'weekly']);
      assert.fail('expected an error');
    } catch (e) {
      assert.ok(e instanceof InvalidValueError);
      assert.match(e.message, /Unknown option '--shedule'/);
      assert.ok(e.cause instanceof Error);
    }
  });
});

describe('snapshotEntities', () => {
  it('is every entity but OrderFilled (trade files) and SplitMarker (cleared)', () => {
    assert.deepEqual(
      snapshotEntities(loadSchema()).map((e) => e.name),
      ['UserPosition', 'NegRiskEvent', 'Condition', 'OutcomeToken'],
    );
  });
});

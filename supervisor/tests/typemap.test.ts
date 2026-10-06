/* eslint-disable @typescript-eslint/no-floating-promises -- node:test awaits describe/it itself */
/**
 * Type map and export casts (snapshot-design.md §3.5, §6 step 3). DuckDB runs
 * in memory; no Postgres needed. The Postgres side of the fingerprint is
 * covered by scripts/test_snapshot_export.ts against a real database.
 */
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DuckDBInstance, type DuckDBConnection } from '@duckdb/node-api';
import {
  checkValues,
  duckCastList,
  loadSchema,
  parseSchema,
  pgSelectList,
  type Entity,
  udtMatches,
} from '#supervisor/typemap.ts';
import { assertFileSchema, fileCheckValues } from '#supervisor/archive.ts';

const entities = loadSchema();
const byName = (name: string): Entity => {
  const e = entities.find((x) => x.name === name);
  assert.ok(e, `${name} in schema.graphql`);
  return e;
};
const col = (entity: string, name: string) => {
  const c = byName(entity).columns.find((x) => x.name === name);
  assert.ok(c, `${entity}.${name}`);
  return c;
};

describe('parseSchema', () => {
  it('maps every field type of schema.graphql', () => {
    assert.deepEqual(
      entities.map((e) => e.name),
      ['UserPosition', 'OrderFilled', 'NegRiskEvent', 'Condition', 'SplitMarker', 'OutcomeToken'],
    );
    assert.equal(col('UserPosition', 'id').parquetType, 'VARCHAR');
    assert.equal(col('UserPosition', 'tokenId').parquetType, 'VARCHAR'); // 78 digits
    assert.equal(col('UserPosition', 'tokenId').pgUdt, 'numeric');
    assert.equal(col('UserPosition', 'amount').parquetType, 'DECIMAL(38,0)');
    assert.equal(col('UserPosition', 'lastBlock').parquetType, 'INTEGER');
    assert.equal(col('UserPosition', 'conditionId').nullable, true);
    assert.equal(col('UserPosition', 'amount').nullable, false);
    assert.equal(col('Condition', 'negRisk').parquetType, 'BOOLEAN');
    assert.equal(col('Condition', 'payoutPrices').parquetType, 'DECIMAL(38,0)[]');
    assert.equal(col('Condition', 'payoutPrices').nullable, true);
    assert.equal(col('Condition', 'payoutPrices').pgUdt, '_numeric'); // Envio 3.6.1
  });

  it('ignores comments, including ones that continue on the next line', () => {
    const [e] = parseSchema(`type T @storage(postgres: true) {
      id: ID! # a comment
      # a whole-line comment: x: Int!
      n: Int
    }`);
    assert.deepEqual(
      e.columns.map((c) => [c.name, c.nullable]),
      [
        ['id', false],
        ['n', true],
      ],
    );
  });

  it('skips field directives such as @index', () => {
    const [e] = parseSchema(`type T @storage(postgres: true) {
      id: ID!
      blockNumber: Int! @index # comment
      other: String @index @config(x: "y")
    }`);
    assert.deepEqual(
      e.columns.map((c) => [c.name, c.parquetType, c.nullable]),
      [
        ['id', 'VARCHAR', false],
        ['blockNumber', 'INTEGER', false],
        ['other', 'VARCHAR', true],
      ],
    );
  });

  it('refuses a type it has no mapping for', () => {
    assert.throws(
      () => parseSchema('type T {\n x: Float!\n}'),
      /T\.x: type Float has no snapshot mapping/,
    );
  });

  it('accepts text[] for the list column, as the live database stores it', () => {
    const list = col('Condition', 'payoutPrices');
    assert.ok(udtMatches(list, '_numeric'));
    assert.ok(udtMatches(list, '_text'));
    assert.ok(!udtMatches(list, 'numeric'));
    assert.ok(!udtMatches(col('UserPosition', 'amount'), 'text')); // no legacy for scalars
    assert.match(pgSelectList(byName('Condition')), /"payoutPrices"::text\[\] AS "payoutPrices"/);
  });

  it('reads numeric as text from Postgres, never as numeric', () => {
    const list = pgSelectList(byName('UserPosition'));
    assert.match(list, /"amount"::text AS "amount"/);
    assert.match(list, /"tokenId"::text AS "tokenId"/);
    assert.doesNotMatch(list, /"lastBlock"::text/);
  });
});

describe('export casts in DuckDB', () => {
  let conn: DuckDBConnection;
  let dir: string;

  before(async () => {
    conn = await (await DuckDBInstance.create(':memory:')).connect();
    dir = mkdtempSync(join(tmpdir(), 'typemap-test-'));
  });
  after(() => rmSync(dir, { recursive: true, force: true }));

  const one = async (sql: string) => (await conn.runAndReadAll(sql)).getRowObjectsJson()[0];

  it('raises, not truncates, past DECIMAL(38,0) and HUGEINT', async () => {
    const e39 = `'1${'0'.repeat(38)}'`; // 10^38: fits HUGEINT, not 38 digits
    assert.equal((await one(`SELECT ${e39}::HUGEINT::VARCHAR v`)).v, `1${'0'.repeat(38)}`);
    await assert.rejects(
      conn.run(`SELECT ${e39}::HUGEINT::DECIMAL(38,0)`),
      /out of range|overflow|Could not cast/i,
    );
    await assert.rejects(
      conn.run(`SELECT '${'9'.repeat(40)}'::HUGEINT`),
      /out of range|overflow|Could not convert/i,
    );
    await assert.rejects(
      conn.run(`SELECT '-${'9'.repeat(40)}'::HUGEINT`),
      /out of range|overflow|Could not convert/i,
    );
  });

  it('round-trips UserPosition values exactly, nulls and negatives included', async () => {
    const entity = byName('UserPosition');
    const tokenId = '14085483699939691540' + '7'.repeat(58); // 78 digits
    const big = '9'.repeat(38);
    // Columns as postgres_query delivers them: numeric as text, int4 as INTEGER.
    await conn.run(`CREATE TABLE src AS SELECT * FROM (VALUES
      ('0xabc-1', '0xabc', '${tokenId}', NULL, '0', '420000', '0', '-92434342585', '1', '${big}', 1, 2, 3),
      ('0xdef-2', '0xdef', '1', 'cond', '-${big}', '0', '5', '0', '0', '0', 2147483647, 0, -1)
    ) t(id, "user", "tokenId", "conditionId", amount, "avgPrice", "avgFeePrice", "realizedPnl",
        "feesPaid", "totalBought", "lastBlock", "lastLogIndex", "lastUpdatedAt")`);
    const file = join(dir, 'up.parquet');
    await conn.run(`COPY (SELECT ${duckCastList(entity)} FROM src) TO '${file}' (FORMAT parquet)`);
    await assertFileSchema(conn, file, entity);
    const rows = (
      await conn.runAndReadAll(
        `SELECT id, "tokenId", "conditionId", amount::VARCHAR a, "realizedPnl"::VARCHAR r, "totalBought"::VARCHAR t, "lastBlock" FROM read_parquet('${file}') ORDER BY id`,
      )
    ).getRowObjectsJson();
    assert.deepEqual(rows, [
      {
        id: '0xabc-1',
        tokenId,
        conditionId: null,
        a: '0',
        r: '-92434342585',
        t: big,
        lastBlock: 1,
      },
      {
        id: '0xdef-2',
        tokenId: '1',
        conditionId: 'cond',
        a: `-${big}`,
        r: '0',
        t: '0',
        lastBlock: 2147483647,
      },
    ]);
  });

  it('casts payoutPrices to a DECIMAL list, null and empty kept apart', async () => {
    const entity = byName('Condition');
    await conn.run(`CREATE TABLE cond AS SELECT * FROM (VALUES
      ('c1', 2, false, NULL, NULL, ['1000000', '0']),
      ('c2', 3, true, 'm', 1, NULL::VARCHAR[]),
      ('c3', 2, false, NULL, NULL, []::VARCHAR[])
    ) t(id, "outcomeSlotCount", "negRisk", "negRiskMarketId", "questionIndex", "payoutPrices")`);
    const file = join(dir, 'cond.parquet');
    await conn.run(`COPY (SELECT ${duckCastList(entity)} FROM cond) TO '${file}' (FORMAT parquet)`);
    await assertFileSchema(conn, file, entity);
    const rows = (
      await conn.runAndReadAll(
        `SELECT id, "payoutPrices"::VARCHAR p FROM read_parquet('${file}') ORDER BY id`,
      )
    ).getRowObjectsJson();
    assert.deepEqual(rows, [
      { id: 'c1', p: '[1000000, 0]' },
      { id: 'c2', p: null },
      { id: 'c3', p: '[]' },
    ]);
    const checks = await fileCheckValues(conn, file, entity);
    assert.equal(checks.rows, '3');
    assert.deepEqual(Object.keys(checks), ['rows', 'fingerprint']);
  });

  it('fails the schema assertion on a HUGEINT column, which Parquet stores as double', async () => {
    const entity = byName('NegRiskEvent');
    const file = join(dir, 'bad.parquet');
    await conn.run(
      `COPY (SELECT 'm' AS id, 1::HUGEINT AS "questionCount") TO '${file}' (FORMAT parquet)`,
    );
    await assert.rejects(
      assertFileSchema(conn, file, entity),
      /schema assertion failed[\s\S]*questionCount DOUBLE/,
    );
  });

  it('fingerprints the documented canonical text', async () => {
    const entity = byName('Condition');
    const file = join(dir, 'fp.parquet');
    await conn.run(
      `COPY (SELECT 'c1' AS id, 2 AS "outcomeSlotCount", true AS "negRisk", NULL::VARCHAR AS "negRiskMarketId",
              NULL::INTEGER AS "questionIndex", [1000000::DECIMAL(38,0), 0] AS "payoutPrices")
       TO '${file}' (FORMAT parquet)`,
    );
    const text = 'c1|2|t|\\N|\\N|1000000,0';
    const expected = BigInt('0x' + createHash('md5').update(text).digest('hex').slice(0, 15));
    assert.equal((await fileCheckValues(conn, file, entity)).fingerprint, expected.toString());
  });

  it('defines the same check-value keys on both sides', () => {
    for (const e of entities)
      for (const c of checkValues(e)) assert.ok(c.pg && c.duck, `${e.name} ${c.key}`);
  });
});

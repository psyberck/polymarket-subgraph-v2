/**
 * Verify the structural invariants a loaded seed must satisfy.
 *
 * Read-only, so it is safe to run at any time. Exported because init runs it as
 * its own closing gate; the block at the bottom runs it when this file is the
 * entry point instead.
 */
import { pathToFileURL } from 'node:url';
import { count } from './utils/postgres.ts';

/**
 * The structural invariants that would make a loaded seed useless.
 *
 * Read-only. The rows are written by the streaming pipeline as it goes, so this
 * runs after it and is the only check that sees BOTH tables -- the two
 * cross-table invariants below cannot be evaluated during the stream.
 *
 * The seed itself must be in place AFTER `envio local db-migrate setup` and
 * BEFORE `envio start`: envio's `initialize()` is `DROP SCHEMA ... CASCADE` and
 * runs whenever `isInitialized()` is false, so seeding first loses everything.
 * Seeded rows with zero `envio_history_*` entries do survive a restart, but they
 * do not survive an initialize.
 *
 * @returns Number of structural problems found; 0 is clean.
 */
export async function verify(): Promise<number> {
  const conds = await count('SELECT count(*) FROM "Condition"');
  const toks = await count('SELECT count(*) FROM "OutcomeToken"');
  const priced = await count('SELECT count(*) FROM "Condition" WHERE "payoutPrices" IS NOT NULL');
  const negs = await count('SELECT count(*) FROM "NegRiskEvent"');
  console.log(`    Condition rows                ${conds.toLocaleString().padStart(12)}`);
  console.log(`    OutcomeToken rows             ${toks.toLocaleString().padStart(12)}`);
  console.log(`    ...with payoutPrices          ${priced.toLocaleString().padStart(12)}`);
  console.log(`    NegRiskEvent rows             ${negs.toLocaleString().padStart(12)}`);

  const checks: { label: string; sql: string }[] = [
    {
      label: 'outcomeToken with no condition',
      sql:
        'SELECT count(*) FROM "OutcomeToken" t ' +
        'LEFT JOIN "Condition" c ON c.id = t."conditionId" WHERE c.id IS NULL',
    },
    {
      label: 'duplicate (cond, outcome)',
      sql:
        'SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) n FROM "OutcomeToken" ' +
        'GROUP BY "conditionId", "outcomeIndex" HAVING count(*) > 1) d',
    },
    {
      label: 'token count <> outcomeSlots',
      sql:
        'SELECT count(*) FROM "Condition" c ' +
        'LEFT JOIN (SELECT "conditionId", count(*) n FROM "OutcomeToken" ' +
        '           GROUP BY "conditionId") t ON t."conditionId" = c.id ' +
        'WHERE coalesce(t.n, 0) <> c."outcomeSlotCount"',
    },
    {
      label: 'payoutPrices non-numeric',
      sql:
        'SELECT count(*) FROM "Condition" WHERE "payoutPrices" IS NOT NULL ' +
        `AND EXISTS (SELECT 1 FROM unnest("payoutPrices") p WHERE p !~ '^[0-9]+$')`,
    },
    {
      label: 'payoutPrices bad length/sum',
      sql:
        'SELECT count(*) FROM "Condition" c, LATERAL (' +
        'SELECT sum(p::numeric) s, count(*) n FROM unnest(c."payoutPrices") p) v ' +
        'WHERE c."payoutPrices" IS NOT NULL AND (' +
        'v.n <> c."outcomeSlotCount" OR v.s > 1000000 ' +
        'OR v.s < 1000000 - (c."outcomeSlotCount" - 1))',
    },
    {
      label: 'outcomeIndex not 0..slots-1',
      sql:
        'SELECT count(*) FROM "Condition" c JOIN (' +
        'SELECT "conditionId", min("outcomeIndex") lo, max("outcomeIndex") hi ' +
        'FROM "OutcomeToken" GROUP BY "conditionId") t ON t."conditionId" = c.id ' +
        'WHERE t.lo <> 0 OR t.hi <> c."outcomeSlotCount" - 1',
    },
    {
      label: 'questionIndex w/o negRisk',
      sql: 'SELECT count(*) FROM "Condition" WHERE "negRisk" = false AND "questionIndex" IS NOT NULL',
    },
    {
      label: 'negRisk w/o questionIndex',
      sql: 'SELECT count(*) FROM "Condition" WHERE "negRisk" = true AND "questionIndex" IS NULL',
    },
  ];

  if (negs) {
    checks.push(
      {
        label: 'negRisk cond with no event',
        sql:
          'SELECT count(*) FROM "Condition" c LEFT JOIN "NegRiskEvent" e ' +
          'ON e.id = c."negRiskMarketId" ' +
          'WHERE c."negRiskMarketId" IS NOT NULL AND e.id IS NULL',
      },
      {
        label: 'questionIndex >= count',
        sql:
          'SELECT count(*) FROM "Condition" c JOIN "NegRiskEvent" e ' +
          'ON e.id = c."negRiskMarketId" ' +
          'WHERE c."questionIndex" IS NOT NULL AND e."questionCount" > 0 ' +
          'AND c."questionIndex" >= e."questionCount"',
      },
    );
  }

  let problems = 0;

  for (const { label, sql } of checks) {
    const n = await count(sql);
    problems += n;
    console.log(
      `    ${label.padEnd(30)}${n.toLocaleString().padStart(12)}${n ? '  <- FAILED' : ''}`,
    );
  }

  if (problems) {
    console.log(
      `\n  ${problems.toLocaleString()} structural problem(s). Do not start the indexer.`,
    );
  } else {
    console.log('\n  clean. Safe to `envio start` — it will resume and keep these rows.');
  }
  return problems;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  if (await verify()) process.exitCode = 1;
}

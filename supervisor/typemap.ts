/**
 * The snapshot type map (snapshot-design.md §3.5), generated from
 * schema.graphql so a schema change cannot silently drift from the export.
 *
 * For every column it produces four things, each used on one side of the
 * Postgres → Parquet copy:
 *   - how Postgres hands the value over (`numeric` always as text: DuckDB reads
 *     undeclared-precision numeric as DOUBLE and loses token ids);
 *   - how DuckDB casts it for Parquet (text → HUGEINT → DECIMAL(38,0), never
 *     text → DECIMAL directly, which is 70x slower; never HUGEINT in the file,
 *     which Parquet stores as double);
 *   - the type the file must report back (the schema assertion);
 *   - check values and a canonical text form, computed identically by
 *     Postgres over the table and by DuckDB over the file (archive.ts).
 */
import { readFileSync } from 'node:fs';
import { literal, quote } from '#supervisor/util.ts';

/** The Parquet column type a file must report, as DuckDB's DESCRIBE prints it. */
export type ParquetType = 'VARCHAR' | 'INTEGER' | 'BOOLEAN' | 'DECIMAL(38,0)' | 'DECIMAL(38,0)[]';

export type ColumnKind = 'text' | 'int' | 'bool' | 'decimal' | 'decimalList';

export interface Column {
  name: string;
  kind: ColumnKind;
  nullable: boolean;
  parquetType: ParquetType;
  /** Postgres column type, as information_schema's `udt_name`. */
  pgUdt: 'text' | 'int4' | 'bool' | 'numeric' | '_numeric';
}

export interface Entity {
  name: string;
  columns: Column[];
}

/**
 * BigInt fields that hold an outcome token id: 78 digits, beyond
 * DECIMAL(38,0), so they stay text.
 */
const TOKEN_ID_FIELDS = new Set(['tokenId']);

function column(name: string, type: string, nullable: boolean, entity: string): Column {
  const base = { name, nullable };
  switch (type) {
    case 'ID':
    case 'String':
      return { ...base, kind: 'text', parquetType: 'VARCHAR', pgUdt: 'text' };
    case 'Int':
      return { ...base, kind: 'int', parquetType: 'INTEGER', pgUdt: 'int4' };
    case 'Boolean':
      return { ...base, kind: 'bool', parquetType: 'BOOLEAN', pgUdt: 'bool' };
    case 'BigInt':
      return TOKEN_ID_FIELDS.has(name)
        ? { ...base, kind: 'text', parquetType: 'VARCHAR', pgUdt: 'numeric' }
        : { ...base, kind: 'decimal', parquetType: 'DECIMAL(38,0)', pgUdt: 'numeric' };
    case '[BigInt!]':
      // numeric[] as Envio 3.6.1 creates it; text[] is accepted too (LEGACY_UDTS).
      return { ...base, kind: 'decimalList', parquetType: 'DECIMAL(38,0)[]', pgUdt: '_numeric' };
    default:
      throw new Error(`${entity}.${name}: type ${type} has no snapshot mapping`);
  }
}

/**
 * Parse the entity types out of schema.graphql. Only the subset this schema
 * uses: `type X @storage(...) { field: Type[!] # comment }`.
 *
 * @param source - schema.graphql's text.
 * @returns Entities in file order, columns in field order.
 */
export function parseSchema(source: string): Entity[] {
  const entities: Entity[] = [];
  let current: Entity | null = null;
  // Line by line, comments stripped first: comments contain braces
  // (`# "{user}-{tokenId}"`), so a regex over the raw body would cut it short.
  for (const raw of source.split('\n')) {
    const line = raw.replace(/#.*/, '').trim();
    if (!line) continue;
    if (!current) {
      const m = /^type\s+(\w+)\b[^{]*\{$/.exec(line);
      if (!m) throw new Error(`cannot parse schema line "${raw.trim()}"`);
      current = { name: m[1], columns: [] };
    } else if (line === '}') {
      entities.push(current);
      current = null;
    } else {
      // Field directives (`@index`) do not change the column, so they are skipped.
      const m = /^(\w+)\s*:\s*(\[\w+!?\]|\w+)(!?)(?:\s+@\w+(?:\([^)]*\))?)*$/.exec(line);
      if (!m) throw new Error(`${current.name}: cannot parse field line "${raw.trim()}"`);
      const [, field, type, bang] = m;
      current.columns.push(column(field, type, bang !== '!', current.name));
    }
  }
  if (current) throw new Error(`${current.name}: missing closing brace`);
  return entities;
}

/** Entities from the indexer's schema.graphql. */
export function loadSchema(path = new URL('../schema.graphql', import.meta.url)): Entity[] {
  return parseSchema(readFileSync(path, 'utf8'));
}

/**
 * Column types also accepted for a kind: the live database (created by an
 * earlier Envio or schema) stores `[BigInt!]` as text[]; a schema migrated by
 * Envio 3.6.1 stores numeric[]. Both hold the same integers.
 */
export const LEGACY_UDTS: Partial<Record<ColumnKind, readonly string[]>> = {
  decimalList: ['_text'],
};

/** Whether a Postgres column type is acceptable for a column. */
export const udtMatches = (c: Column, udt: string): boolean =>
  udt === c.pgUdt || (LEGACY_UDTS[c.kind]?.includes(udt) ?? false);

/** Postgres select list: numbers as text (lists as text[]), everything else as stored. */
export function pgSelectList(entity: Entity): string {
  return entity.columns
    .map((c) => {
      const col = quote(c.name);
      if (c.pgUdt === 'numeric') return `${col}::text AS ${col}`;
      if (c.kind === 'decimalList') return `${col}::text[] AS ${col}`;
      return col;
    })
    .join(', ');
}

/** DuckDB select list over the Postgres rows: the casts that make the file. */
export function duckCastList(entity: Entity): string {
  return entity.columns
    .map((c) => {
      const col = quote(c.name);
      if (c.kind === 'decimal') return `${col}::HUGEINT::DECIMAL(38,0) AS ${col}`;
      if (c.kind === 'decimalList')
        return `list_transform(${col}, v -> v::HUGEINT::DECIMAL(38,0)) AS ${col}`;
      return col;
    })
    .join(', ');
}

/** The null marker in canonical text; cannot collide with a stored value here. */
const NULL_TEXT = '\\N';

/**
 * One row as text, identical in both dialects for identical values. Booleans
 * as t/f, numbers in plain integer form, lists comma-joined, nulls as \N.
 */
function canonicalText(entity: Entity, dialect: 'pg' | 'duck'): string {
  const text = dialect === 'pg' ? 'text' : 'VARCHAR';
  const parts = entity.columns.map((c) => {
    const col = quote(c.name);
    let expr: string;
    switch (c.kind) {
      case 'text':
        expr = dialect === 'pg' && c.pgUdt === 'numeric' ? `${col}::text` : col;
        break;
      case 'int':
      case 'decimal':
        expr = `${col}::${text}`;
        break;
      case 'bool':
        expr = `CASE WHEN ${col} THEN 't' ELSE 'f' END`;
        break;
      case 'decimalList':
        expr =
          dialect === 'pg'
            ? `array_to_string(${col}, ',')`
            : `array_to_string(list_transform(${col}, v -> v::VARCHAR), ',')`;
        break;
    }
    return c.nullable ? `coalesce(${expr}, ${literal(NULL_TEXT)})` : expr;
  });
  return `concat_ws('|', ${parts.join(', ')})`;
}

export interface CheckValue {
  /** Key in the manifest: `rows` or `fingerprint`. */
  key: string;
  pg: string;
  duck: string;
}

/**
 * Check values for one entity. Each is an aggregate returned as text, so
 * Postgres and DuckDB results compare as strings and stay exact.
 *
 * - `rows`: count(*)
 * - `fingerprint`: sum of a 60-bit md5 prefix of every row's canonical text.
 *   Any changed value in any row changes it (with probability 1 − 2⁻⁶⁰), so it
 *   replaces a row-by-row compare, needs no ordering (the database collation
 *   is en_US, not byte order) and runs in one pass.
 */
export function checkValues(entity: Entity): CheckValue[] {
  return [
    { key: 'rows', pg: 'count(*)::text', duck: 'count(*)::VARCHAR' },
    {
      key: 'fingerprint',
      pg: `sum(('x' || substr(md5(${canonicalText(entity, 'pg')}), 1, 15))::bit(60)::bigint)::text`,
      duck: `sum(('0x' || substr(md5(${canonicalText(entity, 'duck')}), 1, 15))::BIGINT)::VARCHAR`,
    },
  ];
}

import { readFileSync } from 'node:fs';
import { type FieldDefinitionNode, Kind, parse, print } from 'graphql';
import { literal, quote } from '#tools/postgres.ts';

/** The Parquet column type a file must report, as DuckDB's DESCRIBE prints it. */
export type ParquetType = 'VARCHAR' | 'INTEGER' | 'BOOLEAN' | 'DECIMAL(38,0)' | 'DECIMAL(38,0)[]';
export type ColumnKind = 'text' | 'bigText' | 'int' | 'bool' | 'decimal' | 'decimalList';

export interface Column {
  name: string;
  kind: ColumnKind;
  nullable: boolean;
}

export interface Entity {
  name: string;
  columns: Column[];
}

export interface ColumnCodec {
  /**
   * Postgres types (information_schema's `udt_name`) accepted for the column:
   * the first is what Envio 3.6.1 creates, the rest what earlier schemas left.
   */
  pgUdts: readonly string[];
  /** The Parquet type the file must have, as DuckDB's DESCRIBE prints it. */
  parquetType: ParquetType;
  /** Postgres read for the export: numbers as text, never as numeric. */
  pgSelect: (col: string) => string;
  /** DuckDB cast from those Postgres rows to the file's type. */
  dkCast: (col: string) => string;
  /** Canonical text in Postgres, for the fingerprint. */
  pgText: (col: string) => string;
  /** Canonical text in DuckDB; equal to `pgText` for equal values. */
  dkText: (col: string) => string;
  /** Restore: the staging table's column type; numbers travel as text. */
  stagingType: string;
  /** Restore: DuckDB read from the file into staging. */
  fromFile: (col: string) => string;
  /** Restore: Postgres cast from staging into a target column of type `udt`. */
  toPg: (col: string, udt: string) => string;
}

const asIs = (col: string): string => col;
const pgToText = (col: string): string => `${col}::text`;
const dkToText = (col: string): string => `${col}::VARCHAR`;
const boolText = (col: string): string => `CASE WHEN ${col} THEN 't' WHEN NOT ${col} THEN 'f' END`;

export const CODECS = {
  text: {
    parquetType: 'VARCHAR',
    pgUdts: ['text'],
    pgSelect: asIs,
    dkCast: asIs,
    pgText: asIs,
    dkText: asIs,
    stagingType: 'text',
    fromFile: asIs,
    toPg: asIs,
  },
  bigText: {
    parquetType: 'VARCHAR',
    pgUdts: ['numeric'],
    pgSelect: pgToText,
    dkCast: asIs,
    pgText: pgToText,
    dkText: asIs,
    stagingType: 'text',
    fromFile: asIs,
    toPg: (col) => `${col}::numeric`,
  },
  int: {
    parquetType: 'INTEGER',
    pgUdts: ['int4'],
    pgSelect: asIs,
    dkCast: asIs,
    pgText: pgToText,
    dkText: dkToText,
    stagingType: 'integer',
    fromFile: asIs,
    toPg: asIs,
  },
  bool: {
    parquetType: 'BOOLEAN',
    pgUdts: ['bool'],
    pgSelect: asIs,
    dkCast: asIs,
    pgText: boolText,
    dkText: boolText,
    stagingType: 'boolean',
    fromFile: asIs,
    toPg: asIs,
  },
  decimal: {
    parquetType: 'DECIMAL(38,0)',
    pgUdts: ['numeric'],
    pgSelect: pgToText,
    dkCast: (col) => `${col}::HUGEINT::DECIMAL(38,0)`,
    pgText: pgToText,
    dkText: dkToText,
    stagingType: 'text',
    fromFile: dkToText,
    toPg: (col) => `${col}::numeric`,
  },
  decimalList: {
    parquetType: 'DECIMAL(38,0)[]',
    pgUdts: ['_numeric', '_text'],
    pgSelect: (col) => `${col}::text[]`,
    dkCast: (col) => `list_transform(${col}, v -> v::HUGEINT::DECIMAL(38,0))`,
    pgText: (col) => `array_to_string(${col}, ',')`,
    dkText: (col) => `array_to_string(list_transform(${col}, v -> v::VARCHAR), ',')`,
    stagingType: 'text[]',
    fromFile: (col) => `list_transform(${col}, v -> v::VARCHAR)`,
    toPg: (col, udt) => (udt === '_numeric' ? `${col}::numeric[]` : col),
  },
} as const satisfies Record<ColumnKind, ColumnCodec>;

/** The codec of a column's kind. */
export const codec = (c: Column): ColumnCodec => CODECS[c.kind];

/** schema.graphql field types (nullability aside) and the kind each becomes. */
const FIELD_KINDS = new Map<string, ColumnKind>([
  ['ID', 'text'],
  ['String', 'text'],
  ['Int', 'int'],
  ['Boolean', 'bool'],
  ['BigInt', 'decimal'],
  ['[BigInt!]', 'decimalList'],
]);
/** BigInt fields holding a token id: 78 digits, beyond DECIMAL(38,0), so kept as text. */
const TOKEN_ID_FIELDS = new Set(['tokenId']);

function column(entity: string, field: FieldDefinitionNode): Column {
  const name = field.name.value;
  const nullable = field.type.kind !== Kind.NON_NULL_TYPE;
  const type = print(field.type.kind === Kind.NON_NULL_TYPE ? field.type.type : field.type);
  const kind = FIELD_KINDS.get(type);
  if (!kind) throw new Error(`${entity}.${name}: type ${type} has no snapshot mapping`);
  return {
    name,
    nullable,
    kind: kind === 'decimal' && TOKEN_ID_FIELDS.has(name) ? 'bigText' : kind,
  };
}

/**
 * Parse the entity types out of schema.graphql. `@derivedFrom` fields are
 * virtual (no column) and skipped; any other definition is refused.
 *
 * @param source - schema.graphql's text.
 * @returns Entities in file order, columns in field order.
 * @throws GraphQLError on a syntax error, Error on a type with no mapping.
 */
export function parseSchema(source: string): Entity[] {
  return parse(source).definitions.map((def) => {
    if (def.kind !== Kind.OBJECT_TYPE_DEFINITION)
      throw new Error(`schema.graphql: ${def.kind} is not supported`);
    const name = def.name.value;
    const fields = (def.fields ?? []).filter(
      (f) => !f.directives?.some((d) => d.name.value === 'derivedFrom'),
    );
    return { name, columns: fields.map((f) => column(name, f)) };
  });
}

/** Entities from the indexer's schema.graphql. */
export function loadSchema(path = new URL('../schema.graphql', import.meta.url)): Entity[] {
  return parseSchema(readFileSync(path, 'utf8'));
}

/** Whether a Postgres column type is acceptable for a column. */
export const udtMatches = (c: Column, udt: string): boolean => codec(c).pgUdts.includes(udt);

/** `entity`'s columns through `expr`, each kept under its own name. */
const selectList = (
  entity: Entity,
  expr: (codec: ColumnCodec) => (col: string) => string,
): string =>
  entity.columns
    .map((c) => {
      const col = quote(c.name);
      const e = expr(codec(c))(col);
      return e === col ? col : `${e} AS ${col}`;
    })
    .join(', ');

/** Postgres select list: numbers as text (lists as text[]), everything else as stored. */
export const pgSelectList = (entity: Entity): string => selectList(entity, (c) => c.pgSelect);

/** DuckDB select list over the Postgres rows: the casts that make the file. */
export const dkCastList = (entity: Entity): string => selectList(entity, (c) => c.dkCast);

/** Restore: DuckDB select list from the file into staging, numbers as exact text. */
export const fromFileList = (entity: Entity): string => selectList(entity, (c) => c.fromFile);

/**
 * Restore: Postgres select list from staging into the entity table.
 *
 * @param entity - Entity table definitions.
 * @param udts - The target's actual column types, by column name.
 */
export const toPgList = (entity: Entity, udts: Map<string, string>): string =>
  entity.columns
    .map((c) => codec(c).toPg(quote(c.name), udts.get(c.name) ?? codec(c).pgUdts[0]))
    .join(', ');

/** The null marker in canonical text; no stored value here is the text `\N`. */
const NULL_TEXT = '\\N';

export interface CheckValue {
  name: string;
  pgValue: string;
  dkValue: string;
}

/**
 * One row as text, identical in both dialects for identical values. Booleans
 * as t/f, numbers in plain integer form, lists comma-joined, nulls as \N.
 * Values are joined with `|`, which no stored value here contains.
 */
function canonicalText(entity: Entity, dialect: 'pg' | 'dk'): string {
  const parts = entity.columns.map((c) => {
    const col = quote(c.name);
    const expr = dialect === 'pg' ? codec(c).pgText(col) : codec(c).dkText(col);
    return c.nullable ? `coalesce(${expr}, ${literal(NULL_TEXT)})` : expr;
  });
  return `concat_ws('|', ${parts.join(', ')})`;
}

/**
 * The aggregates that prove a table and a file hold the same rows.
 *
 * @param entity - The entity whose table or file is checked.
 * @returns SQL select expressions per check, keyed as in the manifest.
 */
export function checkValues(entity: Entity): CheckValue[] {
  return [
    { name: 'rows', pgValue: 'count(*)::text', dkValue: 'count(*)::VARCHAR' },
    {
      name: 'fingerprint',
      pgValue: `sum(('x' || substr(md5(${canonicalText(entity, 'pg')}), 1, 15))::bit(60)::bigint)::text`,
      dkValue: `sum(('0x' || substr(md5(${canonicalText(entity, 'dk')}), 1, 15))::BIGINT)::VARCHAR`,
    },
  ];
}

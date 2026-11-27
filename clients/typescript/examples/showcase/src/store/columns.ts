// The readers of one column of a row: the column types of the Acme tables as `pg` gives them. A value of another type
// is a defect of the schema or of the statement: the reader throws `TypeError`, which no store function catches.
//
// `pg` gives `text` as a string, `integer` as a number, `boolean` as a boolean, `timestamptz` as a `Date` and
// `bigint` (a count, `nextval`) as a string.

import type { Row } from "./connection.ts";

function defect(column: string, expected: string, value: unknown): never {
  throw new TypeError(`store: the column ${column} is not ${expected}: ${String(value)}`);
}

/** A `text` column. */
export function textColumn(row: Row, column: string): string {
  const value = row[column];

  return typeof value === "string" ? value : defect(column, "text", value);
}

/** A `text` column that may be NULL. */
export function nullableTextColumn(row: Row, column: string): string | null {
  const value = row[column];

  return value === null ? null : textColumn(row, column);
}

/** An `integer` column. */
export function intColumn(row: Row, column: string): number {
  const value = row[column];

  return typeof value === "number" && Number.isSafeInteger(value) ? value : defect(column, "an integer", value);
}

/** A `bigint` column whose value is a safe integer (a count, a sequence value). */
export function bigintColumn(row: Row, column: string): number {
  const value = row[column];
  const parsed = typeof value === "string" ? Number(value) : Number.NaN;

  return Number.isSafeInteger(parsed) ? parsed : defect(column, "a bigint in the safe range", value);
}

/** A `boolean` column. */
export function booleanColumn(row: Row, column: string): boolean {
  const value = row[column];

  return typeof value === "boolean" ? value : defect(column, "a boolean", value);
}

/** A `timestamptz` column, to the millisecond (the precision of a `Date`). */
export function instantColumn(row: Row, column: string): Temporal.Instant {
  const value = row[column];

  return value instanceof Date ? Temporal.Instant.fromEpochMilliseconds(value.getTime()) : defect(column, "a timestamptz", value);
}

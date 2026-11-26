// The `CallerTransaction` adapter for `postgres.js`, the entry `horsies/rerun/postgres`. It imports
// nothing of `postgres`: it calls the transaction `sql` that the caller passes.
//
// `postgres.js` types an untyped parameter by the server's answer and then serializes the value with
// its serializer of that type: a text for a `bool`, a `bytea` or a `timestamptz` parameter would be
// changed. So each parameter goes typed as `text`; the cast in the SQL text converts it on the
// server. Each row is read raw: the bytes of the text that the server sent, with no parser and no
// column transform.

import type { Result } from "../wire/result.ts";
import { err, ok } from "../wire/result.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import { driverFailed, oneRowOf } from "./caller.ts";
import type { CallerQueryFailure, CallerRow, CallerStatement, CallerTransaction, SqlText } from "./caller.ts";

/** The types that the public declarations of this entry name: the same types as the `horsies` entry gives. */
export type {
  CallerColumn,
  CallerQueryFailure,
  CallerRow,
  CallerStatement,
  CallerTransaction,
  CallerType,
  SqlText,
} from "./caller.ts";

export type { Err, Ok, Result } from "../wire/result.ts";

/** The PostgreSQL type id of `text`. */
const TEXT_TYPE_OID = 25;

/** The rows of a raw query: each cell as the bytes of its text, or `null`. */
export type PostgresRawRows = readonly (readonly (Uint8Array | null)[])[];

/** A query of `postgres.js` that the adapter runs raw. */
export interface PostgresRawQuery {
  raw(): PromiseLike<PostgresRawRows>;
}

/**
 * The part of a `postgres.js` transaction `sql` (the argument of the callback of `sql.begin`) that the
 * adapter calls. `Parameter` is the parameter type that `typed` gives and `unsafe` takes.
 */
export interface PostgresTransactionSql<Parameter> {
  unsafe(query: string, parameters: (Parameter | null)[]): PostgresRawQuery;
  typed(value: string, oid: number): Parameter;
  /**
   * A transaction `sql` has no `begin`; the root `sql` and a reserved `sql` have one, so this type
   * refuses them: each of their queries can run outside the caller's transaction.
   */
  readonly begin?: never;
}

/** The decoder of a cell's bytes: strict UTF-8. */
const CELL_TEXT = new TextDecoder("utf-8", { fatal: true });

/**
 * The caller's transaction on `sql`. Each statement runs on `sql` and gives its one row. Nothing here
 * commits or rolls back: `sql.begin` commits when its callback returns. After an error, throw from the
 * callback so that `sql.begin` rolls back.
 */
export function postgresTransaction<Parameter>(sql: PostgresTransactionSql<Parameter>): CallerTransaction {
  return {
    queryOne: async (statement: CallerStatement): Promise<Result<CallerRow, CallerQueryFailure>> =>
      postgresQueryOne(sql, statement),
  };
}

/** Run `statement` on `sql` and give its one row in text form. */
async function postgresQueryOne<Parameter>(
  sql: PostgresTransactionSql<Parameter>,
  statement: CallerStatement,
): Promise<Result<CallerRow, CallerQueryFailure>> {
  let rows: PostgresRawRows;

  try {
    const parameters = statement.parameters.map((text) => (text === null ? null : sql.typed(text, TEXT_TYPE_OID)));

    rows = await sql.unsafe(statement.sql, parameters).raw();
  } catch (cause) {
    return err(driverFailed(cause));
  }

  const texts = rowTexts(statement, rows);

  return texts.ok ? oneRowOf(statement, texts.value) : texts;
}

/** The text of each cell of `rows`, or the refusal of a cell that is not UTF-8. Pure. */
function rowTexts(
  statement: CallerStatement,
  rows: PostgresRawRows,
): Result<readonly (readonly SqlText[])[], CallerQueryFailure> {
  try {
    return ok(rows.map((row) => row.map((cell) => (cell === null ? null : CELL_TEXT.decode(cell)))));
  } catch (cause) {
    return err({
      kind: "rowRefused",
      detail: `statement ${statement.name} gave a cell that is not UTF-8 text: ${thrownFacts(cause).message}`,
    });
  }
}

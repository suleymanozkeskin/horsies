// A core statement in a transaction that the caller owns, and the one method that a transaction
// needs to run it. The `pg` and `postgres.js` adapters implement `CallerTransaction`; a caller can
// write an adapter for another driver.
//
// The core builds each statement. Its SQL text is positional (`$1`, `$2`, ...) with a cast on each
// parameter, and each parameter and each result column has a built-in PostgreSQL type, so a driver
// infers nothing and needs no type extension. Every value crosses in the PostgreSQL text form.

import type { Result } from "../wire/result.ts";
import { err, ok } from "../wire/result.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";

/** The text form of one SQL value: its PostgreSQL text input or output, or `null` for SQL NULL. */
export type SqlText = string | null;

/** A built-in PostgreSQL type of a caller statement, by its SQL name. */
export type CallerType = "bool" | "int2" | "int4" | "int8" | "text" | "bytea" | "uuid" | "timestamptz";

/** One result column of a caller statement. */
export interface CallerColumn {
  readonly name: string;
  readonly type: CallerType;
}

/** One core statement for a transaction that the caller owns. */
export interface CallerStatement {
  /** The statement's name, for errors and logs. */
  readonly name: string;
  /** Positional SQL text (`$1`, `$2`, ...) with a cast on each parameter. */
  readonly sql: string;
  /**
   * One value per parameter, `$1` first, as PostgreSQL text input: `int8` as decimal text,
   * `timestamptz` as RFC 3339 text with microseconds, `bytea` as `\x` and hexadecimal digits, `uuid` as
   * lower-case hyphenated text, `bool` as `true` or `false`. Send each one as text: the cast in the
   * SQL text gives its type on the server.
   */
  readonly parameters: readonly SqlText[];
  /** The result columns, in the order of the row. */
  readonly columns: readonly CallerColumn[];
}

/**
 * The one row of a caller statement: one cell per result column, in column order, each the text that
 * the server sends in text format (`bytea` in its hex or its escape output, `timestamptz` under
 * `DateStyle` ISO), or `null` for SQL NULL.
 */
export interface CallerRow {
  readonly cells: readonly SqlText[];
}

/** The state of the caller's connection when it has no open transaction for a statement. */
export type CallerConnectionState =
  | {
      /** No transaction is open: no `BEGIN` ran, or the transaction ended. */
      readonly kind: "idle";
    }
  | {
      /** A statement of the transaction failed: the server runs only `ROLLBACK` in it. */
      readonly kind: "failedTransaction";
    }
  | {
      /** The client has not connected. */
      readonly kind: "notConnected";
    }
  | {
      /** The driver gives a transaction status that is not one of the protocol's: `status`. */
      readonly kind: "unrecognized";
      readonly status: string;
    };

/** Why a transaction could not give the row of a statement. */
export type CallerQueryFailure =
  | {
      /** The driver refused or failed the statement. The caller's transaction is aborted. */
      readonly kind: "driverFailed";
      readonly driverError: Error;
    }
  | {
      /**
       * The connection has no open transaction, so the statement did not run: without a transaction
       * it would commit by itself.
       */
      readonly kind: "noOpenTransaction";
      readonly state: CallerConnectionState;
    }
  | {
      /**
       * The driver gave the result column `column` in the format `format`, not in text format, for
       * example a `pg` client with `binary: true`. The statement ran.
       */
      readonly kind: "resultNotText";
      readonly column: string;
      readonly format: string;
    }
  | {
      /** The result is not one row in text form. */
      readonly kind: "rowRefused";
      readonly detail: string;
    };

/**
 * A database transaction that the caller owns. It runs one statement and gives its one row. It never
 * commits and never rolls back: the caller does, after the call.
 */
export interface CallerTransaction {
  queryOne(statement: CallerStatement): Promise<Result<CallerRow, CallerQueryFailure>>;
}

/** The one row of `rows`, the result of `statement`, or the refusal of another count of rows. Pure. */
export function oneRowOf(
  statement: CallerStatement,
  rows: readonly (readonly SqlText[])[],
): Result<CallerRow, CallerQueryFailure> {
  const [row, ...others] = rows;

  return row !== undefined && others.length === 0
    ? ok({ cells: row })
    : err({ kind: "rowRefused", detail: `statement ${statement.name} gave ${rows.length} rows; it gives one row` });
}

/** The failure of a driver that threw or rejected with `cause`. A value that is not an `Error` is wrapped in one. */
export function driverFailed(cause: unknown): CallerQueryFailure {
  return {
    kind: "driverFailed",
    driverError: cause instanceof Error ? cause : new Error(thrownFacts(cause).message),
  };
}

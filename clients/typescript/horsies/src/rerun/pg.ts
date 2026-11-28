// The `CallerTransaction` adapter for `pg` (node-postgres), the entry `horsies/rerun/pg`. It imports
// nothing of `pg`: it calls the `query` method of the client that the caller passes.

import type { Result } from "../wire/result.ts";
import { err } from "../wire/result.ts";
import { driverFailed, oneRowOf } from "./caller.ts";
import type {
  CallerConnectionState,
  CallerQueryFailure,
  CallerRow,
  CallerStatement,
  CallerTransaction,
  SqlText,
} from "./caller.ts";

/** The types that the public declarations of this entry name: the same types as the `horsies` entry gives. */
export type {
  CallerColumn,
  CallerConnectionState,
  CallerQueryFailure,
  CallerRow,
  CallerStatement,
  CallerTransaction,
  CallerType,
  SqlText,
} from "./caller.ts";

export type { Err, Ok, Result } from "../wire/result.ts";

/** A `pg` type parser that keeps the text that the server sent. */
export type PgTextParser = (text: string) => string;

/** The type parsers of one `pg` query. */
export interface PgTextParsers {
  getTypeParser(oid: number): PgTextParser;
}

/**
 * The query that the adapter passes to `pg`: each parameter as text or `null` (the cast in the SQL
 * text types it on the server), each row as an array, and each cell as the text that the server sent.
 */
export interface PgTextQuery {
  readonly text: string;
  readonly values?: SqlText[];
  readonly rowMode: "array";
  readonly types?: PgTextParsers;
}

/** One result column of a `pg` result: its name, and its format, `"text"` or `"binary"`. */
export interface PgField {
  readonly name: string;
  readonly format: string;
}

/** The result of a `PgTextQuery`: its result columns, and each row as an array of cell texts. */
export interface PgTextResult {
  readonly fields: readonly PgField[];
  readonly rows: readonly (readonly SqlText[])[];
}

/**
 * The part of a `pg` client that the adapter calls: a `pg.Client` or a `pg.PoolClient` of `pg` 8.23.1
 * or later, on which the caller ran `BEGIN`. A `pg.Pool` has no `getTransactionStatus`, so this type
 * refuses it: each query of a pool can run on another connection, outside the caller's transaction.
 */
export interface PgClient {
  query(config: PgTextQuery): Promise<PgTextResult>;
  /**
   * The transaction status of the connection from the server's last answer: `"T"` in a transaction,
   * `"I"` with no transaction, `"E"` in a failed transaction, `null` before the connect.
   */
  getTransactionStatus(): string | null;
}

/** The transaction status of a connection in an open transaction. */
const IN_TRANSACTION_STATUS = "T";

/** The transaction status of a connection with no transaction. */
const IDLE_STATUS = "I";

/** The transaction status of a connection in a failed transaction. */
const FAILED_TRANSACTION_STATUS = "E";

/** The format of a result column in text format. */
const TEXT_FORMAT = "text";

/** The transaction of a `pg` connection by its status: open, or the state of a connection with none. */
type PgTransactionStatus =
  | { readonly kind: "open" }
  | { readonly kind: "notOpen"; readonly state: CallerConnectionState };

/** The parsers that keep each cell as the text that the server sent. */
const TEXT_PARSERS: PgTextParsers = {
  getTypeParser: (): PgTextParser => (text: string): string => text,
};

/**
 * The caller's transaction on `client`. Each statement runs on `client` and gives its one row; a
 * statement runs only when the client's status is an open transaction. Nothing here commits or rolls
 * back: after the rerun, the caller commits, or rolls back after an error. After a statement of the
 * caller's transaction fails, `pg` reports the failed state only after its next ready message, so a
 * rerun started in that window gives `DRIVER_FAILED` instead of `NO_OPEN_TRANSACTION`; both leave
 * nothing written.
 */
export function pgTransaction(client: PgClient): CallerTransaction {
  return {
    queryOne: async (statement: CallerStatement): Promise<Result<CallerRow, CallerQueryFailure>> =>
      pgQueryOne(client, statement),
  };
}

/**
 * Run `statement` on `client` and give its one row in text form. A client with no open transaction
 * runs nothing. The status is the one that the client has before the statement: a statement that the
 * caller sent on the same client before it, and that has no answer yet, is not in it.
 */
async function pgQueryOne(client: PgClient, statement: CallerStatement): Promise<Result<CallerRow, CallerQueryFailure>> {
  const transaction = pgTransactionStatus(client.getTransactionStatus());

  if (transaction.kind === "notOpen") {
    return err({ kind: "noOpenTransaction", state: transaction.state });
  }

  let result: PgTextResult;

  try {
    result = await client.query({
      text: statement.sql,
      values: [...statement.parameters],
      rowMode: "array",
      types: TEXT_PARSERS,
    });
  } catch (cause) {
    return err(driverFailed(cause));
  }

  const notText = result.fields.find((field) => field.format !== TEXT_FORMAT);

  return notText === undefined
    ? oneRowOf(statement, result.rows)
    : err({ kind: "resultNotText", column: notText.name, format: notText.format });
}

/** The transaction of a connection whose `pg` transaction status is `status`. Pure. */
function pgTransactionStatus(status: string | null): PgTransactionStatus {
  switch (status) {
    case IN_TRANSACTION_STATUS:
      return { kind: "open" };
    case IDLE_STATUS:
      return { kind: "notOpen", state: { kind: "idle" } };
    case FAILED_TRANSACTION_STATUS:
      return { kind: "notOpen", state: { kind: "failedTransaction" } };
    case null:
      return { kind: "notOpen", state: { kind: "notConnected" } };
    default:
      return { kind: "notOpen", state: { kind: "unrecognized", status } };
  }
}

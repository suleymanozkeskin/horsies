// The `CallerTransaction` adapter for `pg` (node-postgres), the entry `horsies/rerun/pg`. It imports
// nothing of `pg`: it calls the `query` method of the client that the caller passes.

import type { Result } from "../wire/result.ts";
import { err } from "../wire/result.ts";
import { driverFailed, oneRowOf } from "./caller.ts";
import type { CallerQueryFailure, CallerRow, CallerStatement, CallerTransaction, SqlText } from "./caller.ts";

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

/** The result of a `PgTextQuery`: each row as an array of cell texts. */
export interface PgTextResult {
  readonly rows: readonly (readonly SqlText[])[];
}

/**
 * The part of a `pg` client that the adapter calls: a `pg.Client` or a `pg.PoolClient` on which the
 * caller ran `BEGIN`. A `pg.Pool` has no `escapeLiteral`, so this type refuses it: each query of a pool
 * can run on another connection, outside the caller's transaction.
 */
export interface PgClient {
  query(config: PgTextQuery): Promise<PgTextResult>;
  escapeLiteral(text: string): string;
}

/** The parsers that keep each cell as the text that the server sent. */
const TEXT_PARSERS: PgTextParsers = {
  getTypeParser: (): PgTextParser => (text: string): string => text,
};

/**
 * The caller's transaction on `client`. Each statement runs on `client` and gives its one row. Nothing
 * here commits or rolls back: after the rerun, the caller commits, or rolls back after an error.
 */
export function pgTransaction(client: PgClient): CallerTransaction {
  return {
    queryOne: async (statement: CallerStatement): Promise<Result<CallerRow, CallerQueryFailure>> =>
      pgQueryOne(client, statement),
  };
}

/** Run `statement` on `client` and give its one row in text form. */
async function pgQueryOne(client: PgClient, statement: CallerStatement): Promise<Result<CallerRow, CallerQueryFailure>> {
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

  return oneRowOf(statement, result.rows);
}

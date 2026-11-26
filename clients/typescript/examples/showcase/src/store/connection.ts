// The Acme store: a `pg` pool on the showcase database, and the transaction that every store function runs in. The
// SQL of the store is the app's own; it reads and writes the `acme_*` tables only.
//
// The failure rule: `pg` throws for a failed connection and for a statement that the server refuses. The transaction
// (`inTransaction`) is the boundary: it rolls back and gives `StoreFailure` with the operation and the cause. A
// `TypeError`, a `RangeError` or a `ReferenceError` is a defect of the showcase (for example a column of an
// unexpected type): it passes on.

import pg from "pg";

import type { Result } from "horsies";

import { err, ok } from "../result.ts";

/** The most connections of one store pool. Each task child of a worker holds one pool. */
export const STORE_POOL_MAX_CONNECTIONS = 4;

/** How long a connect of the store waits for the server, in milliseconds. */
export const STORE_CONNECT_TIMEOUT_MS = 10_000;

/** A store operation that failed: the operation, and the cause that the driver or the server gave. */
export type StoreFailure = { readonly operation: string; readonly cause: string };

/** A row that a key names, or no row. */
export type Lookup<T> = { readonly kind: "found"; readonly value: T } | { readonly kind: "notFound" };

/** The connection of a store function: the pool's client of one transaction. */
export type StoreClient = pg.PoolClient;

/** One row of a result, as `pg` gives it: by column name. */
export type Row = Readonly<Record<string, unknown>>;

/** The Acme store: one pool. */
export type Store = { readonly pool: pg.Pool };

/** A store on `url`. Connects nothing: the first operation connects. */
export function openStore(url: string): Store {
  return {
    pool: new pg.Pool({
      connectionString: url,
      max: STORE_POOL_MAX_CONNECTIONS,
      connectionTimeoutMillis: STORE_CONNECT_TIMEOUT_MS,
    }),
  };
}

/** Close every connection of `store`. A later operation fails. */
export async function closeStore(store: Store): Promise<void> {
  await store.pool.end();
}

/** A found row. Pure. */
export function found<T>(value: T): Lookup<T> {
  return { kind: "found", value };
}

/** No row. */
export const NOT_FOUND: Lookup<never> = Object.freeze({ kind: "notFound" });

/**
 * Run `work` in one transaction of `store`: commit when it returns, roll back when it throws. A driver or server
 * failure is `StoreFailure` of `operation`; nothing of `work` is committed then.
 */
export async function inTransaction<T>(
  store: Store,
  operation: string,
  work: (client: StoreClient) => Promise<T>,
): Promise<Result<T, StoreFailure>> {
  return runTransaction(store, operation, work, () => "commit");
}

/**
 * Run `work` in one transaction of `store`, and commit only when its result is a success: a failure of `work` rolls
 * back, so nothing of it is written. A driver or server failure is `StoreFailure` of `operation`.
 */
export async function inTransactionUntilFailure<T, E>(
  store: Store,
  operation: string,
  work: (client: StoreClient) => Promise<Result<T, E>>,
): Promise<Result<Result<T, E>, StoreFailure>> {
  return runTransaction(store, operation, work, (result) => (result.ok ? "commit" : "rollBack"));
}

/** The end of a transaction whose work returned. */
type TransactionEnd = "commit" | "rollBack";

async function runTransaction<T>(
  store: Store,
  operation: string,
  work: (client: StoreClient) => Promise<T>,
  endOf: (value: T) => TransactionEnd,
): Promise<Result<T, StoreFailure>> {
  let client: StoreClient;

  try {
    client = await store.pool.connect();
  } catch (cause) {
    return err(failureOf(operation, cause));
  }

  try {
    await client.query("BEGIN");
    const value = await work(client);

    await client.query(endStatement(endOf(value)));

    return ok(value);
  } catch (cause) {
    await rollBack(client);

    return err(failureOf(operation, cause));
  } finally {
    client.release();
  }
}

function endStatement(end: TransactionEnd): string {
  switch (end) {
    case "commit":
      return "COMMIT";
    case "rollBack":
      return "ROLLBACK";
    default: {
      const unexpected: never = end;

      throw new TypeError(`endStatement: an end outside its type: ${String(unexpected)}`);
    }
  }
}

/** The rows of one statement. Throws what `pg` throws; `inTransaction` converts it. */
export async function rowsOf(client: StoreClient, text: string, values: readonly unknown[] = []): Promise<readonly Row[]> {
  const result = await client.query(text, [...values]);

  return result.rows;
}

/** The count of the rows that one statement changed. Throws what `pg` throws; `inTransaction` converts it. */
export async function changedRows(client: StoreClient, text: string, values: readonly unknown[] = []): Promise<number> {
  const result = await client.query(text, [...values]);

  return result.rowCount ?? 0;
}

/** The first row of one statement, or none. Throws what `pg` throws; `inTransaction` converts it. */
export async function firstRow(client: StoreClient, text: string, values: readonly unknown[] = []): Promise<Lookup<Row>> {
  const [row] = await rowsOf(client, text, values);

  return row === undefined ? NOT_FOUND : found(row);
}

async function rollBack(client: StoreClient): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch {
    // The connection is gone: the server rolls the transaction back itself.
  }
}

/** The failure of `operation` for a thrown driver error. A defect of the showcase passes on. */
function failureOf(operation: string, cause: unknown): StoreFailure {
  if (cause instanceof TypeError || cause instanceof RangeError || cause instanceof ReferenceError) {
    throw cause;
  }

  return { operation, cause: cause instanceof Error ? cause.message : String(cause) };
}

/** The text of a store failure. Pure. */
export function storeFailureText(failure: StoreFailure): string {
  return `${failure.operation} failed: ${failure.cause}`;
}

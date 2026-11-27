// Where the apps of a test connect, and the environment that gives it to the worker entry. The test process sets
// these variables on each worker it starts; every child of the worker inherits them.

import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";

/** The database URL of the worker entry. */
export const DATABASE_URL_ENV = "HORSIES_TS_E2E_DATABASE_URL";

/** The session database URL of the worker entry; set only through a PgBouncer transaction pool. */
export const SESSION_DATABASE_URL_ENV = "HORSIES_TS_E2E_SESSION_DATABASE_URL";

/** Where the apps of a test connect: the database directly, or a PgBouncer transaction pool with a session URL. */
export type Connection =
  | { readonly kind: "direct"; readonly databaseUrl: string }
  | { readonly kind: "transactionPool"; readonly poolUrl: string; readonly sessionUrl: string };

/** A read of one environment variable. */
export type EnvironmentRead = (name: string) => string | undefined;

/** The connection that the environment `read` gives, or the variable that is missing. Pure. */
export function connectionFromEnv(read: EnvironmentRead): Result<Connection, string> {
  const databaseUrl = read(DATABASE_URL_ENV) ?? "";
  const sessionUrl = read(SESSION_DATABASE_URL_ENV) ?? "";

  if (databaseUrl === "") {
    return err(`${DATABASE_URL_ENV} is not set`);
  }

  return ok(sessionUrl === "" ? { kind: "direct", databaseUrl } : { kind: "transactionPool", poolUrl: databaseUrl, sessionUrl });
}

/** The variables that give `connection` to the worker entry. Pure. */
export function connectionEnv(connection: Connection): Readonly<Record<string, string>> {
  switch (connection.kind) {
    case "direct":
      return { [DATABASE_URL_ENV]: connection.databaseUrl };
    case "transactionPool":
      return { [DATABASE_URL_ENV]: connection.poolUrl, [SESSION_DATABASE_URL_ENV]: connection.sessionUrl };
  }
}

/** The URL of every pooled connection of `connection`. Pure. */
export function pooledUrl(connection: Connection): string {
  switch (connection.kind) {
    case "direct":
      return connection.databaseUrl;
    case "transactionPool":
      return connection.poolUrl;
  }
}

// The databases of the end-to-end tests on the test server.
//
// - The server URL comes from `HORSIES_TEST_DATABASE_URL` only. There is no default: a run with the variable unset
//   or empty stops before any test, so a general `DATABASE_URL` of the shell never selects the test server.
// - Each test creates a new empty database `<prefix>ts_<32 hex>` and drops it at its end. The prefix is
//   `HORSIES_TEST_DATABASE_PREFIX` (default `horsies_test_`, at most 24 bytes). The first 8 hex digits are the run id
//   that the runner gives (`HORSIES_TS_E2E_RUN_ID`), so the runner can drop the databases that a failed test left.
//   These functions never create or drop a database of another name.
// - The PgBouncer lane sets `HORSIES_TEST_DATABASE_URL_TRANSACTION`: the apps then connect through the transaction
//   pool, with the direct URL of the database as their session database.

import { randomBytes } from "node:crypto";

import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";
import type { Connection } from "../src/connection.ts";
import { openSession, parseTarget, type PostgresError, type PostgresSession } from "./postgres/client.ts";

/** The variable that names the test server. */
export const SERVER_URL_VARIABLE = "HORSIES_TEST_DATABASE_URL";

/** The variable that sets the prefix of every database name. */
export const DATABASE_PREFIX_VARIABLE = "HORSIES_TEST_DATABASE_PREFIX";

/** The prefix when `DATABASE_PREFIX_VARIABLE` is unset or empty. */
export const DEFAULT_DATABASE_PREFIX = "horsies_test_";

/** The URL of the PgBouncer transaction pool in front of the test server; set only by the PgBouncer lane. */
export const TRANSACTION_POOL_URL_VARIABLE = "HORSIES_TEST_DATABASE_URL_TRANSACTION";

/** The variable of the run id: 8 lower-case hex digits that the runner gives each layer process. */
export const RUN_ID_VARIABLE = "HORSIES_TS_E2E_RUN_ID";

/** The most bytes of a prefix: `<prefix>ts_<32 hex>` then stays inside PostgreSQL's 63-byte identifier limit. */
const PREFIX_MAX_BYTES = 24;

/** The part of a name that marks a database of the TypeScript tests. */
const TYPESCRIPT_INFIX = "ts_";

/** The hex digits of a run id, and of the random part of a name. */
const RUN_ID_DIGITS = 8;
const RANDOM_DIGITS = 24;

/** The database that the create and the drop connect to. */
const ADMIN_DATABASE = "postgres";

/** How the apps of a test reach the test server. */
export type PoolTopology = { readonly kind: "direct" } | { readonly kind: "transactionPool"; readonly poolUrl: string };

/** The test server settings of a run. */
export type ServerSettings = {
  readonly serverUrl: string;
  readonly prefix: string;
  readonly runId: string;
  readonly topology: PoolTopology;
};

/** A database of one test. */
export type TestDatabase = {
  readonly name: string;
  /** The direct URL of the database. */
  readonly url: string;
  /** Where the apps of the test connect. */
  readonly connection: Connection;
};

/** Why the settings of a run are refused. */
export type SettingsRefused = { readonly variable: string; readonly rule: string };

/** A read of one environment variable. */
export type VariableRead = (name: string) => string | undefined;

/** The test server settings of the environment `read`. Pure. */
export function serverSettings(read: VariableRead): Result<ServerSettings, SettingsRefused> {
  const serverUrl = read(SERVER_URL_VARIABLE) ?? "";

  if (serverUrl === "") {
    return err({ variable: SERVER_URL_VARIABLE, rule: "set it to the URL of a throwaway PostgreSQL server; there is no default" });
  }

  const prefix = nonEmpty(read(DATABASE_PREFIX_VARIABLE)) ?? DEFAULT_DATABASE_PREFIX;

  if (!isValidPrefix(prefix)) {
    return err({
      variable: DATABASE_PREFIX_VARIABLE,
      rule: `a lower-case identifier of at most ${String(PREFIX_MAX_BYTES)} bytes that ends in "_"`,
    });
  }

  const runId = nonEmpty(read(RUN_ID_VARIABLE)) ?? randomHex(RUN_ID_DIGITS);

  if (!isHex(runId, RUN_ID_DIGITS)) {
    return err({ variable: RUN_ID_VARIABLE, rule: `${String(RUN_ID_DIGITS)} lower-case hex digits` });
  }

  const poolUrl = nonEmpty(read(TRANSACTION_POOL_URL_VARIABLE));
  const topology: PoolTopology = poolUrl === undefined ? { kind: "direct" } : { kind: "transactionPool", poolUrl };

  return ok({ serverUrl, prefix, runId, topology });
}

/** The text of a refused setting. Pure. */
export function settingsRefusedText(refused: SettingsRefused): string {
  return `${refused.variable}: ${refused.rule}`;
}

/** The name of a new database of the run: `<prefix>ts_<run id><24 random hex>`. Pure. */
export function databaseName(settings: ServerSettings, random: string): string {
  return `${runDatabasePrefix(settings)}${random}`;
}

/** The start of every database name of the run. Pure. */
export function runDatabasePrefix(settings: ServerSettings): string {
  return `${settings.prefix}${TYPESCRIPT_INFIX}${settings.runId}`;
}

/** `url` with the database path `database`, its query kept. A URL that does not parse is refused. Pure. */
export function urlWithDatabase(url: string, database: string): Result<string, PostgresError> {
  if (!URL.canParse(url)) {
    return err({ kind: "urlRefused", detail: "the server URL does not parse" });
  }

  const parsed = new URL(url);

  parsed.pathname = `/${database}`;

  return ok(parsed.toString());
}

/** Create a new empty database of the run. Effects: connects to the admin database and runs `CREATE DATABASE`. */
export async function createDatabase(settings: ServerSettings): Promise<Result<TestDatabase, PostgresError>> {
  const name = databaseName(settings, randomHex(RANDOM_DIGITS));
  const created = await onAdminDatabase(settings, async (session) => session.query(`CREATE DATABASE "${name}"`));

  if (!created.ok) {
    return created;
  }

  const url = urlWithDatabase(settings.serverUrl, name);

  if (!url.ok) {
    return url;
  }

  const connection = connectionOf(settings.topology, name, url.value);

  return connection.ok ? ok({ name, url: url.value, connection: connection.value }) : connection;
}

/** Drop the database `name` with its open sessions. A name outside the run is refused. */
export async function dropDatabase(settings: ServerSettings, name: string): Promise<Result<undefined, PostgresError>> {
  if (!isRunDatabase(settings, name)) {
    return err({ kind: "urlRefused", detail: `${name} is not a database of this run` });
  }

  const dropped = await onAdminDatabase(settings, async (session) => session.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`));

  return dropped.ok ? ok(undefined) : dropped;
}

/** The databases of the run that still exist, by name. */
export async function runDatabases(settings: ServerSettings): Promise<Result<readonly string[], PostgresError>> {
  const listed = await onAdminDatabase(settings, async (session) =>
    session.query(`SELECT datname FROM pg_database WHERE starts_with(datname, '${runDatabasePrefix(settings)}') ORDER BY datname`),
  );

  if (!listed.ok) {
    return listed;
  }

  return ok(listed.value.rows.flatMap(([name]) => (name === null || name === undefined ? [] : [name])));
}

/** A session on the database `name` of the run. The caller closes it. */
export async function openDatabaseSession(settings: ServerSettings, name: string): Promise<Result<PostgresSession, PostgresError>> {
  const url = urlWithDatabase(settings.serverUrl, name);

  if (!url.ok) {
    return url;
  }

  const target = parseTarget(url.value);

  return target.ok ? openSession(target.value) : target;
}

/** Whether `name` is a database name of the run: its prefix and 24 hex digits. Pure. */
export function isRunDatabase(settings: ServerSettings, name: string): boolean {
  const prefix = runDatabasePrefix(settings);

  return name.startsWith(prefix) && isHex(name.slice(prefix.length), RANDOM_DIGITS);
}

async function onAdminDatabase<T>(
  settings: ServerSettings,
  use: (session: PostgresSession) => Promise<Result<T, PostgresError>>,
): Promise<Result<T, PostgresError>> {
  const session = await openDatabaseSession(settings, ADMIN_DATABASE);

  if (!session.ok) {
    return session;
  }

  try {
    return await use(session.value);
  } finally {
    await session.value.close();
  }
}

function connectionOf(topology: PoolTopology, name: string, directUrl: string): Result<Connection, PostgresError> {
  switch (topology.kind) {
    case "direct":
      return ok({ kind: "direct", databaseUrl: directUrl });
    case "transactionPool": {
      const poolUrl = urlWithDatabase(topology.poolUrl, name);

      return poolUrl.ok ? ok({ kind: "transactionPool", poolUrl: poolUrl.value, sessionUrl: directUrl }) : poolUrl;
    }
  }
}

function isValidPrefix(value: string): boolean {
  return Buffer.byteLength(value, "utf8") <= PREFIX_MAX_BYTES && /^[a-z][a-z0-9_]*_$/.test(value);
}

function isHex(value: string, digits: number): boolean {
  return value.length === digits && /^[0-9a-f]+$/.test(value);
}

function nonEmpty(value: string | undefined): string | undefined {
  return value === undefined || value === "" ? undefined : value;
}

/** `digits` random lower-case hex digits. Effects: reads random bytes. */
export function randomHex(digits: number): string {
  return randomBytes(Math.ceil(digits / 2))
    .toString("hex")
    .slice(0, digits);
}

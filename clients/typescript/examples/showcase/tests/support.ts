// What the showcase tests share: the opt-in database and unique ids. It imports no task module: the app of a test is
// in `showcase.ts`.
//
// The database tests are opt-in: with no `ACME_DATABASE_URL`, `testDatabase` answers `skipped` and each such test
// passes with no check, as the Rust showcase tests do. Every test of one run shares the database; each test makes its
// own rows with unique ids.
//
// The tests run on Node (`node --test`) and on Bun (`bun test`), each with the dev start module
// (`../dev/source-package.ts`); see the README.

import { randomUUID } from "node:crypto";

import { ACME_URL_VARIABLE, type DatabaseSettings, processDatabaseSettings, settingsRefusedText } from "../src/settings.ts";
import { closeStore, openStore, type Store, storeFailureText } from "../src/store/connection.ts";
import { ensureDatabase, ensureSchema } from "../src/store/schema.ts";

/** The database of a test run, or no database. */
export type TestDatabase =
  | { readonly kind: "skipped" }
  | { readonly kind: "ready"; readonly settings: DatabaseSettings; readonly store: Store };

/** A URL of a database that is never connected: the tests that need no database build the app with it. */
export const OFFLINE_URL = "postgresql://postgres@127.0.0.1:1/acme_offline";

/** The settings of `OFFLINE_URL`. */
export const OFFLINE_SETTINGS: DatabaseSettings = {
  url: OFFLINE_URL,
  databaseName: "acme_offline",
  maintenanceUrl: "postgresql://postgres@127.0.0.1:1/postgres",
  source: "ACME_DATABASE_URL",
};

/**
 * The database of `ACME_DATABASE_URL` with the Acme tables, or `skipped` when the variable is not set. A set variable
 * that does not give a usable database throws: the run was asked to use it. The caller closes the store.
 */
export async function testDatabase(): Promise<TestDatabase> {
  if ((process.env[ACME_URL_VARIABLE] ?? "") === "") {
    return { kind: "skipped" };
  }

  const settings = processDatabaseSettings();

  if (!settings.ok) {
    throw new Error(`the test database: ${settingsRefusedText(settings.error)}`);
  }

  const created = await ensureDatabase(settings.value);

  if (!created.ok) {
    throw new Error(`the test database: ${storeFailureText(created.error)}`);
  }

  const store = openStore(settings.value.url);
  const schema = await ensureSchema(store);

  if (!schema.ok) {
    await closeStore(store);
    throw new Error(`the test database: ${storeFailureText(schema.error)}`);
  }

  return { kind: "ready", settings: settings.value, store };
}

/** A unique id that starts with `prefix`: the rows of one test do not meet the rows of another. */
export function uniqueId(prefix: string): string {
  return `${prefix}-${randomUUID().replaceAll("-", "").slice(0, 12)}`;
}

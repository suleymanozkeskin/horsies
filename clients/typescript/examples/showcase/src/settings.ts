// The database of the showcase: one PostgreSQL database holds the horsies tables and the Acme tables. The database is
// of the TypeScript client family: the first process that uses it installs the horsies schema as that family, so a
// database of the Python or the Rust showcase does not serve this app.
//
// Resolution, in order:
// 1. `ACME_DATABASE_URL`: the URL as it is.
// 2. `DATABASE_URL`: the same server, with the database name `ACME_DATABASE_NAME`.
// There is no default URL: with neither variable, the showcase does not start.

import type { Result } from "horsies";

import { err, ok } from "./result.ts";

export const ACME_URL_VARIABLE = "ACME_DATABASE_URL";
export const SHARED_URL_VARIABLE = "DATABASE_URL";

/** The database name of rule 2: not the name of the Python and Rust showcases, whose databases are of other families. */
export const ACME_DATABASE_NAME = "acme_demo_ts";

/** The database of the server that `seed` connects to when it creates the Acme database. */
export const MAINTENANCE_DATABASE_NAME = "postgres";

/** The URL schemes of a PostgreSQL connection URL. */
const ACCEPTED_SCHEMES: readonly string[] = ["postgresql:", "postgres:"];

/** A database name that `CREATE DATABASE` takes without quoting rules: letters, digits and `_`, at most 63 bytes. */
const DATABASE_NAME_PATTERN = /^[A-Za-z0-9_]{1,63}$/u;

/** The variable that gave the URL. */
export type UrlSource = typeof ACME_URL_VARIABLE | typeof SHARED_URL_VARIABLE;

/** The database of the showcase. */
export type DatabaseSettings = {
  /** The URL of the Acme database: the horsies broker and the Acme store both connect to it. */
  readonly url: string;
  readonly databaseName: string;
  /** The URL of `MAINTENANCE_DATABASE_NAME` on the same server, with the same user. */
  readonly maintenanceUrl: string;
  readonly source: UrlSource;
};

/** Why no database settings resolve. */
export type SettingsRefused =
  | { readonly kind: "notSet" }
  | { readonly kind: "notUrl"; readonly source: UrlSource; readonly detail: string }
  | { readonly kind: "schemeRefused"; readonly source: UrlSource; readonly scheme: string }
  | { readonly kind: "noDatabaseName"; readonly source: UrlSource }
  | { readonly kind: "databaseNameRefused"; readonly source: UrlSource; readonly databaseName: string };

/** How the resolution reads one environment variable. */
export type EnvironmentRead = (name: string) => string | undefined;

/** The settings of the environment `read`, by the two rules. Pure. */
export function resolveDatabaseSettings(read: EnvironmentRead): Result<DatabaseSettings, SettingsRefused> {
  const acme = read(ACME_URL_VARIABLE) ?? "";

  if (acme !== "") {
    return settingsOf(acme, ACME_URL_VARIABLE, { kind: "keepName" });
  }

  const shared = read(SHARED_URL_VARIABLE) ?? "";

  if (shared !== "") {
    return settingsOf(shared, SHARED_URL_VARIABLE, { kind: "replaceName", databaseName: ACME_DATABASE_NAME });
  }

  return err({ kind: "notSet" });
}

/** The settings of the process environment. Effects: reads `process.env`. */
export function processDatabaseSettings(): Result<DatabaseSettings, SettingsRefused> {
  return resolveDatabaseSettings((name) => process.env[name]);
}

/** Whether rule 2 replaces the database name of the URL. */
type NameRule = { readonly kind: "keepName" } | { readonly kind: "replaceName"; readonly databaseName: string };

function settingsOf(text: string, source: UrlSource, rule: NameRule): Result<DatabaseSettings, SettingsRefused> {
  let parsed: URL;

  try {
    parsed = new URL(text);
  } catch (cause) {
    return err({ kind: "notUrl", source, detail: cause instanceof Error ? cause.message : String(cause) });
  }

  if (!ACCEPTED_SCHEMES.includes(parsed.protocol)) {
    return err({ kind: "schemeRefused", source, scheme: parsed.protocol });
  }

  if (rule.kind === "replaceName") {
    parsed.pathname = `/${rule.databaseName}`;
  }

  const databaseName = decodeURIComponent(parsed.pathname.replace(/^\//u, ""));

  if (databaseName === "") {
    return err({ kind: "noDatabaseName", source });
  }

  if (!DATABASE_NAME_PATTERN.test(databaseName)) {
    return err({ kind: "databaseNameRefused", source, databaseName });
  }

  const maintenance = new URL(parsed.href);

  maintenance.pathname = `/${MAINTENANCE_DATABASE_NAME}`;

  return ok({ url: parsed.href, databaseName, maintenanceUrl: maintenance.href, source });
}

/** The text of a refusal, with the fix. Pure. */
export function settingsRefusedText(refused: SettingsRefused): string {
  switch (refused.kind) {
    case "notSet":
      return `set ${ACME_URL_VARIABLE} to the URL of the showcase database (or ${SHARED_URL_VARIABLE}, whose server then holds ${ACME_DATABASE_NAME})`;
    case "notUrl":
      return `${refused.source} is not a URL: ${refused.detail}`;
    case "schemeRefused":
      return `${refused.source} must start with postgresql:// or postgres://, not ${refused.scheme}//`;
    case "noDatabaseName":
      return `${refused.source} names no database; append /${ACME_DATABASE_NAME}`;
    case "databaseNameRefused":
      return `${refused.source} names the database ${JSON.stringify(refused.databaseName)}; use letters, digits and _ only, at most 63`;
    default: {
      const unexpected: never = refused;

      throw new TypeError(`settingsRefusedText: a refusal outside its type: ${String(unexpected)}`);
    }
  }
}

// A producer manifest of the native tests, and the database they use.

/** The environment variable that names the database of the tests: a throwaway PostgreSQL. */
export const DATABASE_URL_VARIABLE = "HORSIES_TEST_DATABASE_URL";

/** The database of the tests. Throws when the variable is unset: the tests need it. */
export function testDatabaseUrl(): string {
  const url = process.env[DATABASE_URL_VARIABLE];

  if (url === undefined) {
    throw new Error(`set ${DATABASE_URL_VARIABLE} to a throwaway PostgreSQL database`);
  }

  return url;
}

/** The schema modes of a producer manifest. */
export type ManifestSchemaMode = "MIGRATE_AND_VALIDATE" | "OBSERVE_ONLY" | "ASSUME_INITIALIZED";

/** The JSON text of a producer manifest for `databaseUrl` that installs the schema. `echo` turns on the statement log. */
export function manifestJson(databaseUrl: string, echo: boolean): string {
  return manifestJsonWithSchema(databaseUrl, echo, "MIGRATE_AND_VALIDATE");
}

/** The pool of a producer manifest: `size` connections, and up to `maxOverflow` more under load. */
export interface ManifestPool {
  readonly size: number;
  readonly maxOverflow: number;
}

/** The pool of the manifests of the native tests: 5 connections, no overflow. */
export const TEST_POOL: ManifestPool = { size: 5, maxOverflow: 0 };

/** The JSON text of a producer manifest for `databaseUrl` with the schema mode `schema`. */
export function manifestJsonWithSchema(databaseUrl: string, echo: boolean, schema: ManifestSchemaMode): string {
  return manifestJsonWithPool(databaseUrl, echo, schema, TEST_POOL);
}

/** The JSON text of a producer manifest for `databaseUrl` with the schema mode `schema` and the pool `pool`. */
export function manifestJsonWithPool(
  databaseUrl: string,
  echo: boolean,
  schema: ManifestSchemaMode,
  pool: ManifestPool,
): string {
  return JSON.stringify({
    manifest_version: 1,
    database: {
      database_url: databaseUrl,
      session_database: { kind: "SAME_AS_DATABASE_URL" },
      pgbouncer_transaction_mode: false,
      pool_pre_ping: true,
      pool_size: pool.size,
      max_overflow: pool.maxOverflow,
      pool_timeout: 30,
      pool_recycle: { kind: "AFTER_SECONDS", seconds: 1800 },
      echo,
      tcp_keepalives: { kind: "ON", idle_seconds: 30, interval_seconds: 10, probes: 3 },
      tcp_user_timeout: { kind: "AFTER", milliseconds: 30_000 },
    },
    schema,
    send: { retain_rerun_input_default: false, idempotency_reservation_window: 86_400_000_000 },
  });
}

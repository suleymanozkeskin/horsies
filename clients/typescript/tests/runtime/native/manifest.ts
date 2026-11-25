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

/** The JSON text of a producer manifest for `databaseUrl`. `echo` turns on the statement log. */
export function manifestJson(databaseUrl: string, echo: boolean): string {
  return JSON.stringify({
    manifest_version: 1,
    database: {
      database_url: databaseUrl,
      session_database: { kind: "SAME_AS_DATABASE_URL" },
      pgbouncer_transaction_mode: false,
      pool_pre_ping: true,
      pool_size: 5,
      max_overflow: 0,
      pool_timeout: 30,
      pool_recycle: { kind: "AFTER_SECONDS", seconds: 1800 },
      echo,
      tcp_keepalives: { kind: "ON", idle_seconds: 30, interval_seconds: 10, probes: 3 },
      tcp_user_timeout: { kind: "AFTER", milliseconds: 30_000 },
    },
    schema: "MIGRATE_AND_VALIDATE",
    send: { retain_rerun_input_default: false, idempotency_reservation_window: 86_400_000_000 },
  });
}

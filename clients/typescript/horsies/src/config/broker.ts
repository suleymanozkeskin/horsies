// The broker settings of the config: the database, the general pool and the socket settings of every connection
// (the core's `PostgresConfig`, `core/src/core/config/broker.rs`). A field that the config leaves out is left out of
// the core's config text, so the core's default applies.
//
// The code of a refusal: a database URL has the code of the broker URL (`HRS-203`), a pool setting the code of a
// broker pool value (`HRS-213`), and a socket setting the code of a keepalive or user timeout value (`HRS-215`).

import { HorsiesErrorCode } from "../_catalog/error_codes.ts";
import {
  KEEPALIVE_PROBES_MAX,
  KEEPALIVE_PROBES_MIN,
  KEEPALIVE_SECONDS_MAX,
  KEEPALIVE_SECONDS_MIN,
  TCP_USER_TIMEOUT_MS_MAX,
  TCP_USER_TIMEOUT_MS_MIN,
} from "../_catalog/manifest.ts";
import { unreachable } from "../wire/unreachable.ts";
import {
  accepted,
  type Checked,
  checkedEntries,
  checkedInteger,
  checkedObject,
  type ConfigJson,
  entry,
  type IntegerBound,
  type JsonEntry,
  memberPath,
  optionalEntry,
  u32Field,
} from "./checked.ts";
import { type EnvironmentRead, resolvedSecret, type Secret } from "./secret.ts";

/** Where schema installs, partition maintenance and LISTEN/NOTIFY connect: the database URL, or a URL of its own. */
export type SessionDatabase =
  | { readonly kind: "sameAsDatabaseUrl" }
  | { readonly kind: "separate"; readonly url: Secret };

/**
 * How the runtime pool connects to `databaseUrl`:
 * - `direct`: a connection that keeps session state. `sessionDatabase` left out: the core's default.
 * - `pgbouncerTransaction`: `databaseUrl` is a PgBouncer transaction-pool endpoint, with prepared statement tracking
 *   on (`max_prepared_statements > 0`). Transaction pooling keeps no session state, so schema installs, partition
 *   maintenance and LISTEN/NOTIFY connect to `sessionDatabaseUrl`, a URL that keeps session state.
 */
export type BrokerConnection =
  | { readonly kind: "direct"; readonly sessionDatabase?: SessionDatabase }
  | { readonly kind: "pgbouncerTransaction"; readonly sessionDatabaseUrl: Secret };

/** When the pool replaces a connection by its age: never, or after a number of seconds (0: at every checkout). */
export type PoolRecycle = { readonly kind: "never" } | { readonly kind: "after"; readonly seconds: number };

/** TCP keepalives of every core connection. */
export type TcpKeepalives =
  | { readonly kind: "off" }
  | {
      readonly kind: "on";
      readonly idleSeconds: number;
      readonly intervalSeconds: number;
      readonly probes: number;
    };

/** The TCP user timeout of every core connection: the system's, or a number of milliseconds. */
export type TcpUserTimeout = { readonly kind: "systemDefault" } | { readonly kind: "after"; readonly milliseconds: number };

/** Whether a send keeps the input that a rerun needs, when the send does not choose. */
export type RerunInputDefault = "keep" | "drop";

/** The broker settings. Only `databaseUrl` is required; every other field has the core's default. */
export interface BrokerConfigInput {
  /** The runtime connection URL. It names a PgBouncer transaction-pool endpoint in the `pgbouncerTransaction` mode. */
  readonly databaseUrl: Secret;
  /** How the runtime pool connects to `databaseUrl`: directly, or through a PgBouncer transaction pool. */
  readonly connection?: BrokerConnection;
  readonly poolPrePing?: boolean;
  /** Whether the pool of the worker process pings a connection before use. */
  readonly workerPoolPrePing?: boolean;
  /** The persistent connections of the general broker pool. */
  readonly poolSize?: number;
  /** The connections past `poolSize` that the general broker pool opens under load. */
  readonly maxOverflow?: number;
  readonly retainRerunInputDefault?: RerunInputDefault;
  /** The time to wait for a pool connection, in seconds. */
  readonly poolTimeoutSeconds?: number;
  readonly poolRecycle?: PoolRecycle;
  /** Log each SQL statement. */
  readonly echo?: boolean;
  readonly tcpKeepalives?: TcpKeepalives;
  readonly tcpUserTimeout?: TcpUserTimeout;
}

/** A pool setting of the broker: a `u32` field. */
const POOL_U32 = u32Field(HorsiesErrorCode.CONFIG_INVALID_DATABASE_POOL);

/** A keepalive idle time or interval, in seconds (`KeepaliveSeconds` of the core). */
const KEEPALIVE_SECONDS: IntegerBound = Object.freeze({
  min: KEEPALIVE_SECONDS_MIN,
  max: KEEPALIVE_SECONDS_MAX,
  code: HorsiesErrorCode.CONFIG_INVALID_KEEPALIVE,
});

/** A count of keepalive probes (`KeepaliveProbes` of the core). */
const KEEPALIVE_PROBES: IntegerBound = Object.freeze({
  min: KEEPALIVE_PROBES_MIN,
  max: KEEPALIVE_PROBES_MAX,
  code: HorsiesErrorCode.CONFIG_INVALID_KEEPALIVE,
});

/** A TCP user timeout, in milliseconds (`TcpUserTimeoutMs` of the core). */
const TCP_USER_TIMEOUT_MS: IntegerBound = Object.freeze({
  min: TCP_USER_TIMEOUT_MS_MIN,
  max: TCP_USER_TIMEOUT_MS_MAX,
  code: HorsiesErrorCode.CONFIG_INVALID_KEEPALIVE,
});

/** The core's `broker` object of `input`, with each secret read through `environment`. */
export function brokerJson(input: BrokerConfigInput, environment: EnvironmentRead, path: string): Checked<ConfigJson> {
  return checkedObject([
    entry("database_url", resolvedSecret(input.databaseUrl, environment, memberPath(path, "databaseUrl"))),
    connectionEntries(input.connection, environment, memberPath(path, "connection")),
    optionalEntry("pool_pre_ping", input.poolPrePing, accepted),
    optionalEntry("worker_pool_pre_ping", input.workerPoolPrePing, accepted),
    optionalEntry("pool_size", input.poolSize, (value) =>
      checkedInteger(value, POOL_U32, memberPath(path, "poolSize")),
    ),
    optionalEntry("max_overflow", input.maxOverflow, (value) =>
      checkedInteger(value, POOL_U32, memberPath(path, "maxOverflow")),
    ),
    optionalEntry("retain_rerun_input_default", input.retainRerunInputDefault, (value) =>
      accepted(retainsRerunInput(value)),
    ),
    optionalEntry("pool_timeout", input.poolTimeoutSeconds, (value) =>
      checkedInteger(value, POOL_U32, memberPath(path, "poolTimeoutSeconds")),
    ),
    optionalEntry("pool_recycle", input.poolRecycle, (value) =>
      poolRecycleJson(value, memberPath(path, "poolRecycle")),
    ),
    optionalEntry("echo", input.echo, accepted),
    optionalEntry("tcp_keepalives", input.tcpKeepalives, (value) =>
      keepalivesJson(value, memberPath(path, "tcpKeepalives")),
    ),
    optionalEntry("tcp_user_timeout", input.tcpUserTimeout, (value) =>
      userTimeoutJson(value, memberPath(path, "tcpUserTimeout")),
    ),
  ]);
}

/**
 * The core's members `session_database_url` and `pgbouncer_transaction_mode` of a connection mode, with the session
 * secret read through `environment`; none when the config leaves the mode out.
 */
function connectionEntries(
  connection: BrokerConnection | undefined,
  environment: EnvironmentRead,
  path: string,
): Checked<readonly JsonEntry[]> {
  if (connection === undefined) {
    return accepted([]);
  }

  switch (connection.kind) {
    case "direct":
      return checkedEntries([
        optionalEntry("session_database_url", sessionUrl(connection.sessionDatabase), (url) =>
          resolvedSecret(url, environment, memberPath(path, "sessionDatabase.url")),
        ),
        accepted([["pgbouncer_transaction_mode", false]]),
      ]);
    case "pgbouncerTransaction":
      return checkedEntries([
        entry(
          "session_database_url",
          resolvedSecret(connection.sessionDatabaseUrl, environment, memberPath(path, "sessionDatabaseUrl")),
        ),
        accepted([["pgbouncer_transaction_mode", true]]),
      ]);
    default:
      return unreachable(connection, "connectionEntries");
  }
}

/** The secret of a separate session database; none when the session uses the database URL. Pure. */
function sessionUrl(session: SessionDatabase | undefined): Secret | undefined {
  if (session === undefined) {
    return undefined;
  }

  switch (session.kind) {
    case "sameAsDatabaseUrl":
      return undefined;
    case "separate":
      return session.url;
    default:
      return unreachable(session, "sessionUrl");
  }
}

/** The core's flag of a rerun-input default. Pure. */
function retainsRerunInput(choice: RerunInputDefault): boolean {
  switch (choice) {
    case "keep":
      return true;
    case "drop":
      return false;
    default:
      return unreachable(choice, "retainsRerunInput");
  }
}

function poolRecycleJson(recycle: PoolRecycle, path: string): Checked<ConfigJson> {
  switch (recycle.kind) {
    case "never":
      return accepted({ kind: "NEVER" });
    case "after":
      return checkedObject([
        accepted([["kind", "AFTER_SECONDS"]]),
        entry("seconds", checkedInteger(recycle.seconds, POOL_U32, memberPath(path, "seconds"))),
      ]);
    default:
      return unreachable(recycle, "poolRecycleJson");
  }
}

function keepalivesJson(keepalives: TcpKeepalives, path: string): Checked<ConfigJson> {
  switch (keepalives.kind) {
    case "off":
      return accepted({ kind: "OFF" });
    case "on":
      return checkedObject([
        accepted([["kind", "ON"]]),
        entry(
          "idle_seconds",
          checkedInteger(keepalives.idleSeconds, KEEPALIVE_SECONDS, memberPath(path, "idleSeconds")),
        ),
        entry(
          "interval_seconds",
          checkedInteger(keepalives.intervalSeconds, KEEPALIVE_SECONDS, memberPath(path, "intervalSeconds")),
        ),
        entry("probes", checkedInteger(keepalives.probes, KEEPALIVE_PROBES, memberPath(path, "probes"))),
      ]);
    default:
      return unreachable(keepalives, "keepalivesJson");
  }
}

function userTimeoutJson(timeout: TcpUserTimeout, path: string): Checked<ConfigJson> {
  switch (timeout.kind) {
    case "systemDefault":
      return accepted({ kind: "SYSTEM_DEFAULT" });
    case "after":
      return checkedObject([
        accepted([["kind", "AFTER"]]),
        entry(
          "milliseconds",
          checkedInteger(timeout.milliseconds, TCP_USER_TIMEOUT_MS, memberPath(path, "milliseconds")),
        ),
      ]);
    default:
      return unreachable(timeout, "userTimeoutJson");
  }
}

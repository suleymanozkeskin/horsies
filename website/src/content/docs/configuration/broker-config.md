---
title: Broker Config
section: shared
summary: The PostgreSQL connection settings of the app - the broker fields, the URL format, PgBouncer transaction pooling, JIT, pool sizing and schema initialization.
related: [broker-connections, ../rust/configuration/app-config, remote-postgres, ../troubleshooting/pgbouncer]
tags: [configuration, PostgresConfig, database]
---

The broker config is the `broker` field of the app config: the PostgreSQL URLs, the connection pool and the TCP settings.

The API is on the client pages: [Broker Connections](/horsies/configuration/broker-connections/) (Python: `PostgresConfig`, field types and refusals, psycopg and libpq, event loops and the connection ceiling) and [PostgresConfig](/horsies/rust/configuration/app-config/#postgresconfig) (Rust).

## Fields

| Field | Default | Range | Description |
|-------|---------|-------|-------------|
| `database_url` | required | – | PostgreSQL connection URL; see [Connection URL Format](#connection-url-format) |
| `session_database_url` | none | – | Direct/session-capable URL for schema setup and LISTEN/NOTIFY |
| `pgbouncer_transaction_mode` | off | – | Disable prepared statements for transaction-pooled PgBouncer |
| `pool_size` | 5 | 1 to 10,000 | Connection pool size (raise for high-throughput producers) |
| `max_overflow` | 10 | 0 to 10,000 | Additional connections beyond pool_size |
| `pool_timeout` | 30 | 0 to 4,294,967,295; `0`: a checkout does not wait | Seconds to wait for connection |
| `pool_pre_ping` | on | – | Ping a pooled connection at checkout when it waited in the pool for more than 1 s (`PRE_PING_AFTER_IDLE_S`), in every pool but the one of the worker process; see [Remote PostgreSQL](/horsies/configuration/remote-postgres/) |
| `worker_pool_pre_ping` | off | – | Ping a pooled connection of the worker process at checkout when it waited in the pool for more than 1 s. Off: the worker sends a one-statement write in one round trip, and a dead pooled connection fails its first statement with a connection error that the worker retries; see [Remote PostgreSQL](/horsies/configuration/remote-postgres/#health-checks-at-high-rtt) |
| `tcp_keepalives` | on | – | Enable libpq TCP keepalives on broker and child-process connections; see [Remote PostgreSQL](/horsies/configuration/remote-postgres/) |
| `tcp_keepalives_idle` | 30 | 1 to 32,767 | Idle seconds before the first keepalive probe (libpq `keepalives_idle`) |
| `tcp_keepalives_interval` | 10 | 1 to 32,767 | Seconds between keepalive probes (libpq `keepalives_interval`) |
| `tcp_keepalives_count` | 3 | 1 to 127 | Unacknowledged probes before the connection is dropped (libpq `keepalives_count`) |
| `tcp_user_timeout_ms` | 30000 | `0` or 1 to 2,147,483,647 | Milliseconds that sent data may stay unacknowledged before the connection is closed (libpq `tcp_user_timeout`); `0` uses the operating system default; see [Remote PostgreSQL](/horsies/configuration/remote-postgres/) |
| `retain_rerun_input_default` | off | – | App-level default for rerun-input retention at enqueue; a per-task value overrides it |
| `echo` | off | – | Log each SQL statement at INFO on the logger `horsies.sql` (Rust: the `tracing` target `horsies.sql`): the SQL text as the message, the statement name as the field `statement`. Bound values are not shown. Python: a broker built with `echo=True` sets `horsies.sql` to INFO and, when that logger has no handler, adds one stdout handler per process |
| `pool_recycle` | 1800 | `-1` (never) or 0 to 4,294,967,295 | Recycle connections after N seconds |

A value outside its range is refused when the config is built. The three keepalive values are checked only when `tcp_keepalives` is on.

The pools of a worker are not broker fields. Each child kind has its own `pool` and `task_send_pool` in the worker profile, and the worker process has the `pool` of the profile. See [Connection Pools and the Default Executor](/horsies/workers/worker-profiles/#connection-pools-and-the-default-executor) (Python).

## Connection URL Format

```
postgresql://user:password@host:port/database
```

Components:

- `postgresql` - Scheme; `postgres` is accepted too. The URL names no driver. A URL that names a driver, such as `postgresql+psycopg://`, is refused, and the error names the fix
- `user:password` - Credentials
- `host:port` - Server location (default port: 5432)
- `database` - Database name

## PgBouncer / Transaction Pooling

Use a split configuration when the main SQL URL points to PgBouncer transaction
pooling. `database_url` is used for ordinary SQL work. `session_database_url` is
used for schema setup, partition maintenance, and PostgreSQL `LISTEN`/`NOTIFY`.
These operations need persistent session semantics.

Some managed providers, including PlanetScale Postgres, expose separate pooled
and direct connection strings. Treat provider-specific ports and hostnames as an
example, not a Horsies requirement. Check your provider dashboard or current
provider docs and copy the pooled transaction URL into `database_url` and the
direct/session-capable URL into `session_database_url`.

Rules:

- Workers require `session_database_url` when `pgbouncer_transaction_mode` is on.
- The two URLs must be different in transaction-pool mode.
- Worker startup checks session behavior before partition maintenance starts.
- The web UI event stream listens on the session URL for real-time updates.
- PgBouncer-only worker deployment is unsupported.
- Horsies does not infer PgBouncer mode from port numbers.

## JIT

Horsies runs `SET jit = off` once on each new broker connection that keeps
session settings. Some partition-maintenance statements have cost estimates
above the default `jit_above_cost`, so a JIT-enabled server would compile
them on every call.

- Direct connections and the `session_database_url` pool always run without JIT.
- With `pgbouncer_transaction_mode` on, the `database_url` pool keeps the
  server's JIT setting, because server sessions are shared between clients.
  To disable JIT there too, set it for the role:
  `ALTER ROLE <horsies_role> SET jit = off;`
- Your `database_url` options are not changed.

## Connection Pooling

`pool_size` and `max_overflow` bound the connections of each broker pool: `pool_size` persistent connections, and up to `max_overflow` more under load. `pool_timeout` is how long a checkout waits when all connections are busy, and `pool_recycle` closes and recreates a connection after that many seconds.

### Sizing Guidelines

`pool_size` and `max_overflow` size the general broker used by producers,
web processes, schedulers, and app-level APIs. They do not size the pools of a
worker: the worker process opens `WorkerProfile.pool`, and task code in a worker
child opens the `task_send_pool` of its child kind.

| Deployment | pool_size | max_overflow |
|------------|-----------|--------------|
| Development | 2-5 | 5 |
| Small production | 5-10 | 10-20 |
| High traffic | 10-20 | 20-40 |

For direct PostgreSQL connections, budget a worker roughly as:

```
pool max_size (the pool of the worker process)
  + pool max_size again, when session_database_url differs from database_url (the session pool)
  + 1 listener connection
  + 1 runner heartbeat connection
  + isolated children * isolated.pool max_size
  + shared async children * shared_async.pool max_size
  + shared thread children * shared_thread.pool max_size
  + task code that sends: children * task_send_pool of their kind, per event loop
```

Each `max_size` is the resolved value: a `PoolSize`, or the size that
`PoolSizeFromSeats()` derives from the seats; see
[Connections](/horsies/workers/worker-profiles/#connections). With the defaults
and a profile of 8 isolated children, the framework ceiling is about
`5 + 1 + 1 + 8 * 1 = 15` direct connections, and `8 * 2 = 16` more when every
child sends from one event loop. One listener connection carries every
`LISTEN` of the worker process.
Idle child processes keep no database connection by default because the
`min_size` of each pool is 0; connections are opened lazily when task
lifecycle work starts. An isolated child uses one connection at a time, for the
RUNNING update and the finalizing mark: the worker process writes the runner
heartbeats over its own connection. So the default
`IsolatedPool(max_size=PoolSize(connections=1))` is enough for the own work of
an isolated child. The seats of a shared child share the pool of their child;
see [Connections](/horsies/workers/shared-thread-tasks/#connections) for its
size.

When using PgBouncer transaction pooling, those client-side pool slots are
multiplexed onto fewer server backends. Connections are still not safe to share
between OS processes; Horsies creates child pools inside each child process, and
a child inherits no database socket from the worker.

## Health Checks

Connection health comes from `pool_pre_ping`, `worker_pool_pre_ping`, the TCP keepalives and `tcp_user_timeout_ms` in the [field table](#fields). How to tune them for a remote database is on [Remote PostgreSQL](/horsies/configuration/remote-postgres/).

## Schema Initialization

The broker installs the schema at its first initialization. A fresh install takes the schema advisory lock on a direct connection to the session URL. The steps, the tables and the refusals are on [Schema Creation](/horsies/internals/database-schema/#schema-creation).

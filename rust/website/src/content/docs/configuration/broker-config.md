---
title: Broker Config
summary: PostgreSQL connection settings via PostgresConfig.
related: [app-config, ../../internals/database-schema]
tags: [configuration, PostgresConfig, database]
---

## Basic Usage

```rust
use horsies::PostgresConfig;

let broker = PostgresConfig::from_url(
    "postgresql://user:password@localhost:5432/mydb"
);
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database_url` | `String` | required | PostgreSQL runtime connection URL |
| `session_database_url` | `Option<String>` | `None` | Direct/session-capable URL for schema initialization and LISTEN/NOTIFY |
| `pgbouncer_transaction_mode` | `bool` | `false` | Disable SQLx named prepared-statement caching for PgBouncer transaction pools |
| `pool_size` | `u32` | 30 | Connection pool size |
| `max_overflow` | `u32` | 30 | Additional connections beyond pool_size |
| `pool_timeout` | `u32` | 30 | Seconds to wait for connection |
| `pool_pre_ping` | `bool` | `true` | Pre-ping connections before use |
| `echo` | `bool` | `false` | Echo SQL statements to logs |
| `pool_recycle` | `u32` | 1800 | Recycle connections after N seconds |

## Connection URL Format

```
postgresql://user:password@host:port/database
```

Components:

- `postgresql` or `postgres` - Scheme (both accepted)
- `user:password` - Credentials
- `host:port` - Server location (default port: 5432)
- `database` - Database name

**Note:** The Python version uses `postgresql+psycopg://` (SQLAlchemy driver prefix). In Rust, use plain `postgresql://` or `postgres://`.

## Examples

### Local Development

```rust
PostgresConfig::from_url(
    "postgresql://postgres:postgres@localhost:5432/horsies_dev"
)
```

### Production with Connection Pool

```rust
PostgresConfig {
    database_url: "postgresql://app:secret@db.example.com:5432/production".into(),
    session_database_url: None,
    pgbouncer_transaction_mode: false,
    pool_size: 10,
    max_overflow: 20,
    pool_timeout: 30,
    pool_recycle: 3600,
    pool_pre_ping: true,
    echo: false,
}
```

### From Environment Variable

```rust
PostgresConfig::from_url(
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be set")
)
```

## Connection Pooling

The broker manages an async connection pool:

- `pool_size`: Base number of persistent connections
- `max_overflow`: Additional connections created under load (temporary)
- `pool_timeout`: How long to wait if all connections are busy
- `pool_recycle`: Close and recreate connections after this many seconds

### Sizing Guidelines

| Deployment | pool_size | max_overflow |
|------------|-----------|--------------|
| Development | 2-5 | 5 |
| Small production | 5-10 | 10-20 |
| High traffic | 10-20 | 20-40 |

Consider: `pool_size + max_overflow` should not exceed PostgreSQL's `max_connections` (divided by number of worker instances).

## PgBouncer Transaction Pooling

PgBouncer transaction pooling can be used for normal task and workflow SQL, but it cannot preserve session state for `LISTEN/NOTIFY`. Horsies therefore needs two URLs:

- `database_url`: runtime SQL URL, which may point at the transaction-pool endpoint.
- `session_database_url`: direct or session-pooled URL used for schema initialization and `LISTEN/NOTIFY`.

```rust
let broker = PostgresConfig::from_pgbouncer_urls(
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
    std::env::var("SESSION_DATABASE_URL").expect("SESSION_DATABASE_URL must be set"),
);
```

With `pgbouncer_transaction_mode=true`, Horsies disables SQLx's named prepared-statement cache for the runtime pool. Schema initialization, workers, result listeners, and workflow listeners use `session_database_url`.

Different providers expose direct and pooled Postgres endpoints differently. Some publish separate ports, some publish separate hostnames, and those details can change; check your provider's current connection documentation instead of copying an example port.

`app.check_live()` also runs a bounded `LISTEN` + `NOTIFY` delivery probe in PgBouncer transaction mode. If the session URL is accidentally transaction-pooled, the check fails with a message indicating that `session_database_url` cannot preserve `LISTEN/NOTIFY` state.

## Multiple Components

The broker creates two connection types:

1. **Query pool**: For queries, inserts, updates
2. **LISTEN/NOTIFY**: For real-time notifications

By default both use `database_url`. When `session_database_url` is configured, schema initialization and LISTEN/NOTIFY use the session URL while normal SQL uses `database_url`.

## Schema Initialization

When you use high-level `Horsies` entry points such as task sends, workflow starts, `app.run_worker()`, `app.run_scheduler()`, `app.get_broker()`, or `app.check_live()`, the library ensures the schema automatically on first use.

Schema initialization uses a fast path when the embedded migrations are already current. Fresh or upgraded databases still run the migration path under a PostgreSQL advisory lock, double-check migration state after acquiring the lock, and retry PostgreSQL deadlocks.

Use an explicit broker call when you want a preflight step outside those paths, or when you're working with `PostgresBroker` directly:

- `horsies_tasks` - Task storage
- `horsies_task_attempts` - Per-attempt execution history
- `horsies_heartbeats` - Liveness tracking
- `horsies_worker_states` - Worker monitoring
- `horsies_schedule_state` - Scheduler state

```rust
let broker = PostgresBroker::connect_with(config).await?;
broker.ensure_schema_initialized().await?;
```

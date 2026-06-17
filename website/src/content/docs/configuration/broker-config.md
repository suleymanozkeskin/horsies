---
title: Broker Config
summary: PostgreSQL connection settings via PostgresConfig.
related: [app-config, ../../internals/postgres-broker]
tags: [configuration, PostgresConfig, database]
---

## Basic Usage

```python
from horsies import PostgresConfig

broker = PostgresConfig(
    database_url="postgresql+psycopg://user:password@localhost:5432/mydb",
)
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database_url` | `str` | required | SQLAlchemy connection URL |
| `session_database_url` | `str \| None` | `None` | Direct/session-capable URL for schema setup and LISTEN/NOTIFY |
| `pgbouncer_transaction_mode` | `bool` | `False` | Disable prepared statements for transaction-pooled PgBouncer |
| `pool_size` | `int` | 5 | Connection pool size (raise for high-throughput producers) |
| `max_overflow` | `int` | 10 | Additional connections beyond pool_size |
| `worker_pool_size` | `int \| None` | 3 | Worker coordinator pool size; `None` inherits `pool_size` |
| `worker_max_overflow` | `int \| None` | 2 | Worker coordinator overflow; `None` inherits `max_overflow` |
| `worker_child_pool_min_size` | `int` | 0 | Minimum connections kept by each child worker process |
| `worker_child_pool_max_size` | `int` | 2 | Maximum connections allowed per child worker process |
| `worker_child_pool_check` | `bool` | `True` | Health-check child pool connections on checkout; see [Remote PostgreSQL](remote-postgres) |
| `pool_timeout` | `int` | 30 | Seconds to wait for connection |
| `pool_pre_ping` | `bool` | `True` | Pre-ping connections before use; see [Remote PostgreSQL](remote-postgres) |
| `tcp_keepalives` | `bool` | `True` | Enable libpq TCP keepalives on broker and child-process connections; see [Remote PostgreSQL](remote-postgres) |
| `tcp_keepalives_idle` | `int` | 30 | Idle seconds before the first keepalive probe (libpq `keepalives_idle`) |
| `tcp_keepalives_interval` | `int` | 10 | Seconds between keepalive probes (libpq `keepalives_interval`) |
| `tcp_keepalives_count` | `int` | 3 | Unacknowledged probes before the connection is dropped (libpq `keepalives_count`) |
| `echo` | `bool` | `False` | Echo SQL statements to logs |
| `pool_recycle` | `int` | 1800 | Recycle connections after N seconds |

## Connection URL Format

```
postgresql+psycopg://user:password@host:port/database
```

Components:

- `postgresql+psycopg` - Driver (psycopg3)
- `user:password` - Credentials
- `host:port` - Server location (default port: 5432)
- `database` - Database name

## Examples

### Local Development

```python
PostgresConfig(
    database_url="postgresql+psycopg://postgres:postgres@localhost:5432/horsies_dev",
)
```

### Production with Connection Pool

```python
PostgresConfig(
    database_url="postgresql+psycopg://app:secret@db.example.com:5432/production",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
)
```

### From Environment Variable

```python
import os

PostgresConfig(
    database_url=os.environ["DATABASE_URL"],
)
```

### PgBouncer / Transaction Pooling

Use a split configuration when the main SQL URL points to PgBouncer transaction
pooling. `database_url` is used for ordinary SQL work. `session_database_url` is
used for schema setup and PostgreSQL `LISTEN`/`NOTIFY`, which need persistent
session semantics.

```python
from horsies import AppConfig, PostgresConfig

config = AppConfig(
    broker=PostgresConfig(
        database_url=os.environ["DATABASE_URL_POOLED"],
        session_database_url=os.environ["DATABASE_URL_DIRECT"],
        pgbouncer_transaction_mode=True,
    ),
)
```

Some managed providers, including PlanetScale Postgres, expose separate pooled
and direct connection strings. Treat provider-specific ports and hostnames as an
example, not a Horsies requirement. Check your provider dashboard or current
provider docs and copy the pooled transaction URL into `database_url` and the
direct/session-capable URL into `session_database_url`.

Rules:

- Workers require `session_database_url` when `pgbouncer_transaction_mode=True`.
- Syce requires the session URL for real-time updates.
- PgBouncer-only worker deployment is unsupported.
- Horsies does not infer PgBouncer mode from port numbers.

## Connection Pooling

The broker uses SQLAlchemy's async connection pool:

- `pool_size`: Base number of persistent connections
- `max_overflow`: Additional connections created under load (temporary)
- `pool_timeout`: How long to wait if all connections are busy
- `pool_recycle`: Close and recreate connections after this many seconds

### Sizing Guidelines

`pool_size` and `max_overflow` size the general broker used by producers,
web processes, schedulers, and app-level APIs. Workers use the smaller
`worker_pool_size` and `worker_max_overflow` profile by default so an idle or
moderately loaded worker does not reserve a web-sized pool.

| Deployment | pool_size | max_overflow |
|------------|-----------|--------------|
| Development | 2-5 | 5 |
| Small production | 5-10 | 10-20 |
| High traffic | 10-20 | 20-40 |

For direct PostgreSQL connections, budget a worker roughly as:

```
worker_pool_size + worker_max_overflow
  + 2 session/LISTEN connections
  + processes * worker_child_pool_max_size
  + task-code database connections
```

With the defaults and `--processes=8`, the framework ceiling is about
`3 + 2 + 2 + 8 * 2 = 23` direct connections, before application task code.
Idle child processes keep no database connection by default because
`worker_child_pool_min_size=0`; connections are opened lazily when task
lifecycle work starts. Set `worker_child_pool_max_size=1` only after testing heartbeat and task
lifecycle latency for your workload; `2` leaves one connection for task
lifecycle work and one for heartbeat overlap.

When using PgBouncer transaction pooling, those client-side pool slots are
multiplexed onto fewer server backends. Connections are still not safe to share
between OS processes; Horsies creates child pools after worker processes start
and uses a non-inheriting start method for replacement executors after startup.

## Multiple Components

The broker creates two connection types:

1. **Async engine** (SQLAlchemy): For queries, inserts, updates
2. **LISTEN/NOTIFY** (psycopg): For real-time notifications

By default both use `database_url`. With split PgBouncer configuration, ordinary
SQL uses `database_url`, while schema setup and `LISTEN`/`NOTIFY` use
`session_database_url`.

## Health Monitoring

The broker includes automatic health checks:

- Proactive connection monitoring
- Auto-reconnect on connection loss
- Exponential backoff for reconnection attempts

## Schema Initialization

On first use, the broker creates required tables:

- `horsies_tasks` - Task storage
- `horsies_heartbeats` - Liveness tracking
- `horsies_worker_states` - Worker monitoring
- `horsies_schedule_state` - Scheduler state

Uses PostgreSQL advisory locks to prevent race conditions during schema creation.

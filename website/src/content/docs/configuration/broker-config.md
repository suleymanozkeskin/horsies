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
| `tcp_user_timeout_ms` | `int` | 30000 | Milliseconds that sent data may stay unacknowledged before the connection is closed (libpq `tcp_user_timeout`); `0` uses the operating system default; see [Remote PostgreSQL](remote-postgres) |
| `echo` | `bool` | `False` | Echo SQL statements to logs. Bound values are not shown |
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
used for schema setup, partition maintenance, and PostgreSQL `LISTEN`/`NOTIFY`.
These operations need persistent session semantics.

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
- The two URLs must be different in transaction-pool mode.
- Worker startup checks session behavior before partition maintenance starts.
- Syce requires the session URL for real-time updates.
- PgBouncer-only worker deployment is unsupported.
- Horsies does not infer PgBouncer mode from port numbers.

### JIT

Horsies runs `SET jit = off` once on each new broker connection that keeps
session settings. Some partition-maintenance statements have cost estimates
above the default `jit_above_cost`, so a JIT-enabled server would compile
them on every call.

- Direct connections and the `session_database_url` pool always run without JIT.
- With `pgbouncer_transaction_mode=True`, the `database_url` pool keeps the
  server's JIT setting, because server sessions are shared between clients.
  To disable JIT there too, set it for the role:
  `ALTER ROLE <horsies_role> SET jit = off;`
- Your `database_url` options are not changed.

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
between OS processes; Horsies creates child pools inside each child process, and
a child inherits no database socket from the worker.

## Event Loops

A broker keeps one resource set per open event loop that calls it. A set is a
runtime engine with its own pool, a session engine when `session_database_url`
differs from `database_url`, and a LISTEN/NOTIFY listener. Sync calls
(`send()`, `TaskHandle.get()`, `spec.start()`, the sync `WorkflowHandle`
methods, `app.ping_workers()`) run on the broker's own sync runner loop, which
has its own set.

- A set is built on the first call from its loop. Its pools open connections
  on demand, so a set that makes no database call opens none.
- When a loop closes, the next broker call from any loop releases that loop's
  set: its idle connections and its listener connections close. A connection
  that a task of the closed loop still held is closed by garbage collection.
- A broker serves at most 5 open loops other than its sync runner loop. A call
  from one more open loop returns `EVENT_LOOP_LIMIT` in the error type of that
  call, and nothing runs. The error clears when one of those loops closes.
  The limit covers pytest-asyncio, which keeps one loop open per fixture scope
  (session, package, module, class, function).
- `close_async()` and `close()` close every set: the calling loop's set
  directly, the set of a closed loop by release, and the set of another open
  loop on that loop. When that loop does not run the close within 5 s, the
  result is `CLOSE_FAILED` with the loop's thread in the message, and the
  close stays scheduled on that loop. A later call builds a new set.

### Async Code and Jupyter

A sync call blocks its thread until the sync runner loop completes the call.
On a thread that runs an event loop, this wait stops every coroutine on that
loop. Coroutines and Jupyter notebook cells run on such a thread.

On such a thread, a sync call returns `ASYNC_CONTEXT` in the error type of
that call. Nothing runs, and the sync runner does not start.

| Sync call | Returns |
|---|---|
| `send()`, `schedule()`, `retry_send()`, `retry_schedule()` | `TaskSendAsyncContext` (`TaskSendErrorCode.ASYNC_CONTEXT`); the error keeps the payload for `retry_send_async()` / `retry_schedule_async()` |
| `app.get_result()`, `app.get_raw_result()`, `app.get_task_info()`, `app.ping_database()`, `app.ping_workers()`, `app.list_worker_states()`, `app.get_worker_state()`, `app.get_worker_state_history()`, `TaskHandle.info()`, `broker.enqueue()`, `broker.get_task_info()`, `broker.get_raw_result_record()`, `broker.close()` | `BrokerAsyncContext` (`BrokerErrorCode.ASYNC_CONTEXT`) |
| `TaskHandle.get()`, `WorkflowHandle.get()`, `WorkflowHandle.result_for()` | A result with `RetrievalCode.ASYNC_CONTEXT`, not cached |
| `WorkflowHandle.status()`, `.results()`, `.tasks()`, `.cancel()`, `.pause()`, `.resume()`, `pause_workflow_sync()`, `resume_workflow_sync()` | `HandleAsyncContext` (`HandleErrorCode.ASYNC_CONTEXT`) |
| `spec.start()`, `spec.retry_start()` | `WorkflowStartAsyncContext` (`WorkflowStartErrorCode.ASYNC_CONTEXT`); the error keeps the workflow id for `retry_start_async()` |
| `app.check(live=True)` | `HRS-217` (`CHECK_LIVE_ASYNC_CONTEXT`); the live check does not connect |

On such a thread, use the `*_async` form:

```python
# In a coroutine or a notebook cell
handle = (await my_task.send_async(value=1)).unwrap()
result = await handle.get_async(timeout_ms=5000)
```

- A `TaskHandle` that already holds its result returns it from `get()`,
  because that call does not wait.
- `await spec.retry_start_async(err)` completes a start that returned
  `ASYNC_CONTEXT`, with the error's workflow id.
- To run a sync call from async code, run it on a thread without an event
  loop, for example `await asyncio.to_thread(app.check, live=True)`.

### Connection Ceiling

Each set can open:

```text
pool_size + max_overflow
+ pool_size + max_overflow    # only when session_database_url differs
+ 2                           # listener connections
```

A process can open this once per set:

| Process | Sets |
|---|---|
| Sync calls only, or async calls from one loop only | 1 |
| ASGI app with async endpoints and sync calls in a thread pool | 2 |
| pytest run with a session-scoped broker and function-scoped async tests | 2 (session loop, current test loop), plus 1 when tests make sync calls |

### Things to Avoid

**Don't leave event loops open after their work is done.** Each open loop
keeps its set and its idle connections, and the sixth open loop gets
`EVENT_LOOP_LIMIT`.

```python
# Wrong: each call leaves one more open loop
loop = asyncio.new_event_loop()
loop.run_until_complete(broker.ping_database_async())

# Correct: asyncio.run() closes its loop
asyncio.run(broker.ping_database_async())
```

**Don't call sync `close()` inside a coroutine.** It returns `ASYNC_CONTEXT`
and closes nothing. Use `await broker.close_async()`.

## Multiple Components

The broker creates three connection types:

1. **Runtime async engine** (SQLAlchemy): For task and workflow SQL.
2. **Maintenance async engine** (SQLAlchemy): For schema and partition DDL.
3. **LISTEN/NOTIFY** (psycopg): For real-time notifications.

By default all three use `database_url`. With split PgBouncer configuration, ordinary
SQL uses `database_url`. Schema setup, partition maintenance, and
`LISTEN`/`NOTIFY` use `session_database_url`.

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

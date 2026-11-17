---
title: Broker Config
section: shared
summary: PostgreSQL connection settings via PostgresConfig.
related: [app-config]
tags: [configuration, PostgresConfig, database]
---

## Basic Usage

```python
from pydantic import SecretStr
from horsies import PostgresConfig

broker = PostgresConfig(
    database_url=SecretStr("postgresql://user:password@localhost:5432/mydb"),
)
```

## Fields

| Field | Type | Default | Range | Description |
|-------|------|---------|-------|-------------|
| `database_url` | `SecretStr` | required | – | SQLAlchemy connection URL |
| `session_database_url` | `SecretStr \| None` | `None` | – | Direct/session-capable URL for schema setup and LISTEN/NOTIFY |
| `pgbouncer_transaction_mode` | `bool` | `False` | – | Disable prepared statements for transaction-pooled PgBouncer |
| `pool_size` | `int` | 5 | 1 to 10,000 | Connection pool size (raise for high-throughput producers) |
| `max_overflow` | `int` | 10 | 0 to 10,000 | Additional connections beyond pool_size |
| `pool_timeout` | `int` | 30 | 0 to 4,294,967,295; `0`: a checkout does not wait | Seconds to wait for connection |
| `pool_pre_ping` | `bool` | `True` | – | Ping a pooled connection at checkout when it waited in the pool for more than 1 s (`PRE_PING_AFTER_IDLE_S`), in every engine but the one of the worker process; see [Remote PostgreSQL](/horsies/configuration/remote-postgres/) |
| `worker_pool_pre_ping` | `bool` | `False` | – | Ping a pooled connection of the worker process at checkout when it waited in the pool for more than 1 s. Off: the worker sends a one-statement write in one round trip, and a dead pooled connection fails its first statement with a connection error that the worker retries; see [Remote PostgreSQL](/horsies/configuration/remote-postgres/#health-checks-at-high-rtt) |
| `tcp_keepalives` | `bool` | `True` | – | Enable libpq TCP keepalives on broker and child-process connections; see [Remote PostgreSQL](/horsies/configuration/remote-postgres/) |
| `tcp_keepalives_idle` | `int` | 30 | 1 to 32,767 | Idle seconds before the first keepalive probe (libpq `keepalives_idle`) |
| `tcp_keepalives_interval` | `int` | 10 | 1 to 32,767 | Seconds between keepalive probes (libpq `keepalives_interval`) |
| `tcp_keepalives_count` | `int` | 3 | 1 to 127 | Unacknowledged probes before the connection is dropped (libpq `keepalives_count`) |
| `tcp_user_timeout_ms` | `int` | 30000 | `0` or 1 to 2,147,483,647 | Milliseconds that sent data may stay unacknowledged before the connection is closed (libpq `tcp_user_timeout`); `0` uses the operating system default; see [Remote PostgreSQL](/horsies/configuration/remote-postgres/) |
| `retain_rerun_input_default` | `bool` | `False` | – | App-level default for rerun-input retention at enqueue; a per-task value overrides it |
| `echo` | `bool` | `False` | – | Echo SQL statements to logs. Bound values are not shown |
| `pool_recycle` | `int` | 1800 | `-1` (never) or 0 to 4,294,967,295 | Recycle connections after N seconds |

A pool value outside its range is refused with `ConfigurationError` `HRS-213` (`CONFIG_INVALID_BROKER_POOL`), and a TCP value outside its range with `HRS-215` (`CONFIG_INVALID_KEEPALIVE`). `pool_size=0` and `max_overflow=-1` mean no limit in SQLAlchemy; they are refused, because the core always bounds its connections. The three keepalive values are checked only when `tcp_keepalives` is `True`.

The pools of a worker are not `PostgresConfig` fields. Each child kind has its own `pool` and `task_send_pool` in the worker profile, and the worker process has `WorkerProfile.pool`. See [Connection Pools and the Default Executor](/horsies/workers/worker-profiles/#connection-pools-and-the-default-executor). `worker_child_pool_min_size`, `worker_child_pool_max_size`, `worker_child_pool_check`, `worker_pool_size` and `worker_max_overflow` were removed in 0.7.0; a config that sets one fails with `HRS-213`, and the error names the profile field that replaces it.

## Connection URL Format

```
postgresql://user:password@host:port/database
```

Components:

- `postgresql` - Scheme; `postgres` is accepted too. The URL names no driver: horsies opens the database with psycopg 3. A URL that names a driver, such as `postgresql+psycopg://`, is refused with `ConfigurationError` `HRS-203`, and the error names the fix
- `user:password` - Credentials
- `host:port` - Server location (default port: 5432)
- `database` - Database name

## psycopg and libpq

horsies needs psycopg 3.3.7 or later. The install sets where the libpq of psycopg comes from:

| Install | psycopg loads |
|---------|---------------|
| `horsies` | the libpq of the system |
| `horsies[pg-binary]` | the libpq that `psycopg-binary` bundles |
| `horsies[pg-c]` | the libpq of the system, through `psycopg-c` |

When a cancelled asyncio task, `KeyboardInterrupt` or `SystemExit` interrupts a running query, psycopg:

1. Sends a cancel request to the server, for at most 5 seconds.
2. Waits for the query to end, for at most 5 seconds.
3. Closes the connection, when the query has not ended.

The 5-second limit of step 1 needs libpq 17 or later. With an older libpq, the cancel request has no limit, and in async code psycopg sends it from a thread of the default executor. The 5-second limit of step 2 applies with every libpq. Read the version that psycopg loaded with `psycopg.pq.version()`: libpq 17.0 reads `170000`.

In a [shared async child](/horsies/workers/shared-async-tasks/#the-timeout), that thread is work of the task: the attempt ends only when the cancel request returns. A second cancel does not stop a thread.

## Examples

### Local Development

```python
PostgresConfig(
    database_url=SecretStr("postgresql://postgres:postgres@localhost:5432/horsies_dev"),
)
```

### Production with Connection Pool

```python
PostgresConfig(
    database_url=SecretStr("postgresql://app:secret@db.example.com:5432/production"),
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
    database_url=SecretStr(os.environ["DATABASE_URL"]),
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
        database_url=SecretStr(os.environ["DATABASE_URL_POOLED"]),
        session_database_url=SecretStr(os.environ["DATABASE_URL_DIRECT"]),
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
  + 2 session/LISTEN connections
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
`5 + 2 + 1 + 8 * 1 = 16` direct connections, and `8 * 2 = 16` more when every
child sends from one event loop.
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

## Event Loops

A workflow start (`spec.start()`, `start_async()`, `retry_start()`,
`retry_start_async()`) and a workflow resume (`WorkflowHandle.resume()`,
`resume_async()`, `resume_workflow()`, `resume_workflow_sync()`) use a
resource set of the calling event loop. The other calls of the app, the
broker, and the task and workflow handles run on the core of the process: a
broker has one set of core pools for every thread and event loop that calls
it, so these calls have no event loop limit. See
[Calls That Run on the Core](/horsies/tasks/error-types/#calls-that-run-on-the-core)
for the failure exits of the broker calls and the sends.

A broker keeps one resource set per open event loop that starts or resumes a
workflow. A set is a runtime engine with its own pool, a session engine when
`session_database_url` differs from `database_url`, and a LISTEN/NOTIFY
listener. The sync forms of the workflow calls and the sync reads of the app
(for example `app.ping_workers()`) run on the broker's own sync runner loop.
`send()`, `broker.enqueue()`, `broker.get_task_info()` and
`broker.get_raw_result_record()` call the core on the calling thread. See
[Connection Ceiling](#connection-ceiling).

- A set is built on the first call from its loop. Its pools open connections
  on demand, so a set that makes no database call opens none.
- When a loop closes, the next broker call from any loop releases that loop's
  set: its idle connections and its listener connections close. A connection
  that a task of the closed loop still held is closed by garbage collection.
- A broker serves at most 5 open loops other than its sync runner loop and, in
  a worker child, the task loop that runs `async def` tasks. A workflow start
  or resume from one more open loop returns
  `EVENT_LOOP_LIMIT` in the error type of that call, and nothing runs. The
  error clears when one of those loops closes. The limit covers
  pytest-asyncio, which keeps one loop open per fixture scope (session,
  package, module, class, function).
- `close_async()` and `close()` close every set: the calling loop's set
  directly, the set of a closed loop by release, and the set of another open
  loop on that loop. When that loop does not run the close within 5 s, the
  result is `CLOSE_FAILED` with the loop's thread in the message, and the
  close stays scheduled on that loop. A later call builds a new set.

### Async Code and Jupyter

A sync call blocks its thread until the call completes, on the core or on the
sync runner loop. On a thread that runs an event loop, this wait stops every
coroutine on that loop. Coroutines and Jupyter notebook cells run on such a
thread.

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

The core pools of a broker, for every thread and event loop, can open:

```text
pool_size + max_overflow      # main pool on database_url
+ pool_size + max_overflow    # session pool, only when session_database_url differs
```

The core's listener is one connection per process on the session URL
(`session_database_url`, else `database_url`). It opens at the first result
wait or worker ping.

Each resource set can open:

```text
pool_size + max_overflow      # runtime engine
+ pool_size + max_overflow    # session engine, only when session_database_url differs
+ 2                           # listener of the set
```

```text
connections of one process = the core pools + the core listener
                           + sets × (the ceiling of one set)
```

| Process | Sets |
|---|---|
| No workflow start or resume, for example a script that sends and reads results | 0 |
| Workflow starts or resumes from sync code only | 1 (the sync runner loop) |
| Workflow starts or resumes from one event loop only | 1 |
| ASGI app with async workflow starts and sync workflow starts in a thread pool | 2 |
| pytest run with a session-scoped broker and function-scoped async tests that start workflows | 2 (session loop, current test loop), plus 1 when tests start workflows with sync calls |

At the defaults (`pool_size=5`, `max_overflow=10`, no `session_database_url`),
a process that only sends and reads task results opens up to 15 connections,
and 1 more when it waits for a result. Each pool opens a connection only when
a call finds none free.

### Things to Avoid

**Don't leave event loops open after their work is done.** Each open loop
that made a workflow call keeps its set and its idle connections, and the
sixth open loop gets `EVENT_LOOP_LIMIT`.

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# Wrong: each call leaves one more open loop
loop = asyncio.new_event_loop()
loop.run_until_complete(spec.start_async())

# Correct: asyncio.run() closes its loop
asyncio.run(spec.start_async())
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

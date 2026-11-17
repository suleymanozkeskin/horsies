---
title: Broker Connections
section: python
summary: PostgresConfig in the Python client - examples, field types and refusals, psycopg and libpq - and how a Python broker uses event loops, the sync calls in async code, and its connection ceiling.
related: [../configuration/broker-config, app-config, ../guides/async-frameworks, ../configuration/remote-postgres]
tags: [configuration, PostgresConfig, psycopg, event loop, connections]
---

# Broker Connections

`PostgresConfig` is the `broker` field of `AppConfig`. The meaning, the default and the range of each field are on [Broker Config](/horsies/configuration/broker-config/).

## PostgresConfig

```python
from pydantic import SecretStr
from horsies import PostgresConfig

broker = PostgresConfig(
    database_url=SecretStr("postgresql://user:password@localhost:5432/mydb"),
)
```

### Field Types

| Field | Type |
|-------|------|
| `database_url` | `SecretStr` |
| `session_database_url` | `SecretStr \| None` |
| `pgbouncer_transaction_mode` | `bool` |
| `pool_size` | `int` |
| `max_overflow` | `int` |
| `pool_timeout` | `int` |
| `pool_pre_ping` | `bool` |
| `worker_pool_pre_ping` | `bool` |
| `tcp_keepalives` | `bool` |
| `tcp_keepalives_idle` | `int` |
| `tcp_keepalives_interval` | `int` |
| `tcp_keepalives_count` | `int` |
| `tcp_user_timeout_ms` | `int` |
| `retain_rerun_input_default` | `bool` |
| `echo` | `bool` |
| `pool_recycle` | `int` |

### Refusals

- A pool value outside its range is refused with `ConfigurationError` `HRS-213` (`CONFIG_INVALID_BROKER_POOL`), and a TCP value outside its range with `HRS-215` (`CONFIG_INVALID_KEEPALIVE`). `pool_size=0` and `max_overflow=-1` mean no limit in SQLAlchemy; they are refused, because the core always bounds its connections. The three keepalive values are checked only when `tcp_keepalives` is `True`.
- A URL that names a driver, such as `postgresql+psycopg://`, is refused with `ConfigurationError` `HRS-203`, and the error names the fix. horsies opens the database with psycopg 3.
- `worker_child_pool_min_size`, `worker_child_pool_max_size`, `worker_child_pool_check`, `worker_pool_size` and `worker_max_overflow` were removed in 0.7.0; a config that sets one fails with `HRS-213`, and the error names the profile field that replaces it. See [Connection Pools and the Default Executor](/horsies/workers/worker-profiles/#connection-pools-and-the-default-executor).

### Examples

#### Local Development

```python
PostgresConfig(
    database_url=SecretStr("postgresql://postgres:postgres@localhost:5432/horsies_dev"),
)
```

#### Production with Connection Pool

```python
PostgresConfig(
    database_url=SecretStr("postgresql://app:secret@db.example.com:5432/production"),
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
)
```

#### From Environment Variable

```python
import os

PostgresConfig(
    database_url=SecretStr(os.environ["DATABASE_URL"]),
)
```

#### PgBouncer / Transaction Pooling

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

The rules of the split configuration are on [PgBouncer / Transaction Pooling](/horsies/configuration/broker-config/#pgbouncer--transaction-pooling).

#### Check Connections at Checkout

`worker_pool_pre_ping` checks the pool of the worker process. `IsolatedPool(check=True)` and `SharedPool(check=True)` check the pool of each child of that kind. When to turn the checks on, and what each check costs, is on [Health Checks at High RTT](/horsies/configuration/remote-postgres/#health-checks-at-high-rtt).

```python
from horsies import (
    AppConfig,
    IsolatedChildren,
    IsolatedPool,
    PostgresConfig,
    SharedAsyncChildren,
    SharedPool,
    WorkerProfile,
)

config = AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr(os.environ['DATABASE_URL']),
        worker_pool_pre_ping=True,
    ),
    worker_profiles={
        'idle_behind_nat': WorkerProfile(
            isolated=IsolatedChildren(count=8, pool=IsolatedPool(check=True)),
            shared_async=SharedAsyncChildren(
                count=2,
                seats=50,
                pool=SharedPool(check=True),
            ),
        ),
    },
)
```

#### Shorter Keepalive Idle

For a pooler that reaps idle connections in less than 30 seconds. See [TCP Keepalives for Idle Pooled Connections](/horsies/configuration/remote-postgres/#tcp-keepalives-for-idle-pooled-connections).

```python
config = AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr(os.environ['DATABASE_URL']),
        tcp_keepalives_idle=15,
        tcp_keepalives_interval=5,
    ),
)
```

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

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
| `session_database` | `SessionDatabaseSameAsDatabaseUrl \| SessionDatabaseSeparate(url: SecretStr)` |
| `pgbouncer_transaction_mode` | `bool` |
| `pool_size` | `int` |
| `max_overflow` | `int` |
| `pool_timeout_seconds` | `int` |
| `pool_pre_ping` | `bool` |
| `worker_pool_pre_ping` | `bool` |
| `tcp_keepalives` | `TcpKeepalivesOn(idle_seconds: int, interval_seconds: int, probes: int) \| TcpKeepalivesOff` |
| `tcp_user_timeout` | `TcpUserTimeoutAfter(milliseconds: int) \| TcpUserTimeoutSystemDefault` |
| `retain_rerun_input_default` | `bool` |
| `echo` | `bool` |
| `pool_recycle` | `PoolRecycleAfterSeconds(seconds: int) \| PoolRecycleNever` |

### Refusals

- A pool value outside its range is refused with `ConfigurationError` `HRS-213` (`CONFIG_INVALID_BROKER_POOL`), and a TCP value outside its range with `HRS-215` (`CONFIG_INVALID_KEEPALIVE`). `pool_size=0` and `max_overflow=-1` mean no limit in SQLAlchemy; they are refused, because the core always bounds its connections. The message of a refused value names its field, for example `tcp_keepalives.probes`.
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
    pool_timeout_seconds=30,
    pool_recycle=PoolRecycleAfterSeconds(seconds=3600),
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
from horsies import AppConfig, PostgresConfig, SessionDatabaseSeparate

config = AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr(os.environ["DATABASE_URL_POOLED"]),
        session_database=SessionDatabaseSeparate(
            url=SecretStr(os.environ["DATABASE_URL_DIRECT"]),
        ),
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
        tcp_keepalives=TcpKeepalivesOn(idle_seconds=15, interval_seconds=5),
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

Every call of the app, the broker, and the task and workflow handles runs on
the core of the process. A broker has one set of core pools and the process
has one core listener, for every thread and event loop that calls it, so no
call has an event loop limit. A sync call runs the core's blocking form on
the calling thread; an async call awaits the core on the calling loop. See
[Calls That Run on the Core](/horsies/tasks/error-types/#calls-that-run-on-the-core)
for the failure exits of the broker calls and the sends, and
[Connection Ceiling](#connection-ceiling).

### Async Code and Jupyter

A sync call blocks its thread until the core completes the call. On a thread
that runs an event loop, this wait stops every coroutine on that loop.
Coroutines and Jupyter notebook cells run on such a thread.

On such a thread, a sync call returns `ASYNC_CONTEXT` in the error type of
that call. Nothing runs.

| Sync call | Returns |
|---|---|
| `send()`, `schedule()`, `retry_send()`, `retry_schedule()` | `TaskSendAsyncContext` (`TaskSendErrorCode.ASYNC_CONTEXT`); the error keeps the payload for `retry_send_async()` / `retry_schedule_async()` |
| `app.get_raw_result()`, `app.get_task_info()`, `app.ping_database()`, `app.ping_workers()`, `app.list_worker_states()`, `app.get_worker_state()`, `app.get_worker_state_history()`, `TaskHandle.info()`, `broker.enqueue()`, `broker.get_task_info()`, `broker.get_raw_result_record()`, `broker.close()` | `BrokerAsyncContext` (`BrokerErrorCode.ASYNC_CONTEXT`) |
| `TaskHandle.get()`, `WorkflowHandle.get()`, `WorkflowHandle.result_for()`, `app.get_result()` | `Err(AsyncContext(...))` (`RetrievalCode.ASYNC_CONTEXT`), not kept by the handle |
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
+ pool_size + max_overflow    # session pool, only when the session database URL differs
```

The core's listener is one connection per process on the session URL
(the URL of a separate `session_database`, else `database_url`). It opens at the first result
wait or worker ping.

```text
connections of one process = the core pools + the core listener
```

At the defaults (`pool_size=5`, `max_overflow=10`, `SessionDatabaseSameAsDatabaseUrl()`),
a process opens up to 15 connections, and 1 more when it waits for a result
or pings the workers. Each pool opens a connection only when a call finds
none free.

### Things to Avoid

**Don't call sync `close()` inside a coroutine.** It returns `ASYNC_CONTEXT`
and closes nothing. Use `await broker.close_async()`.

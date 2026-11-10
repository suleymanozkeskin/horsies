---
title: Shared Async Tasks
summary: SHARED_ASYNC tasks run as coroutines, many at once, in shared async children - what runs where, the timeout, the rules for task authors, recycling and sizing.
related: [worker-profiles, worker-architecture, child-process-hooks, ../tasks/defining-tasks]
tags: [workers, async, shared-async, execution, timeouts, concurrency]
---

# Shared Async Tasks

A task with `execution=Execution.SHARED_ASYNC` can run in a shared async child. A shared async child is one process that runs up to `seats` such tasks at once, as coroutines on one event loop. A seat that becomes free takes the next task at once.

## Why and When

Use a shared async task for work that mostly waits on the network: HTTP calls, queries, messages to other services. One child then holds many waits, where each isolated child holds one. Keep `Execution.ISOLATED` (the default) for CPU work, for code that blocks, and for tasks that must not share a process with other tasks.

The deal is different from an isolated task. The tasks of a shared child share one process, one event loop and its memory. A crash of that process, or a kill of it, ends every task in it.

## What Runs Where

<!-- members: Execution subset -->
| Row class | Worker with shared async children | Worker with none |
| --------- | --------------------------------- | ---------------- |
| `SHARED_ASYNC` | a shared async child, on a seat | an isolated child, one task in the process |
| `ISOLATED` | an isolated child | an isolated child |

A worker may run a task more isolated than its row says, never less. A worker with no shared async children runs `SHARED_ASYNC` tasks in isolated children, with the usual kill at `timeout_ms`. An isolated child of a worker that has shared async children never runs a `SHARED_ASYNC` task.

A shared async child runs only `async def` tasks. A `SHARED_ASYNC` row whose function is now a `def` function in the worker's code fails with `WORKER_RESOLUTION_ERROR`.

## How To

Declare the task. `timeout_ms` is required; `cancel_grace_ms` is optional.

```python
import asyncio

from pydantic import SecretStr

from horsies import (
    AppConfig,
    Execution,
    Horsies,
    IsolatedChildren,
    PostgresConfig,
    SharedAsyncChildren,
    TaskError,
    TaskResult,
    WorkerProfile,
)

app = Horsies(AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr('postgresql+psycopg://user:pass@localhost:5432/mydb'),
    ),
    worker_profiles={
        'io': WorkerProfile(
            isolated=IsolatedChildren(count=2),
            shared_async=SharedAsyncChildren(count=4, seats=25),
        ),
    },
))


@app.task(
    'fetch_quote',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=10_000,
    cancel_grace_ms=2_000,
)
async def fetch_quote(*, symbol: str) -> TaskResult[str, TaskError]:
    await asyncio.sleep(0.1)
    return TaskResult(ok=symbol)
```

Start a worker with a profile that has shared async children:

```bash
horsies worker myapp.instance:app --profile io
```

This worker runs `ISOLATED` tasks in 2 isolated children and `SHARED_ASYNC` tasks on 4 × 25 = 100 seats. See [Worker Profiles](worker-profiles#sharedasyncchildren) for the fields.

### Keep a client for the life of the child

Build a long-lived async client in an `on_shared_async_child_start` hook. The hook runs on the event loop of each shared async child before the child takes its first task, so the client belongs to the loop that every task of the child uses.

<!-- check: skip partial -- the client module is the reader's own -->
```python
import httpx

clients: dict[str, httpx.AsyncClient] = {}


@app.on_shared_async_child_start
async def open_http_client() -> None:
    clients['http'] = httpx.AsyncClient(timeout=5.0)


@app.task('get_status', execution=Execution.SHARED_ASYNC, timeout_ms=10_000)
async def get_status(*, url: str) -> TaskResult[int, TaskError]:
    response = await clients['http'].get(url)
    return TaskResult(ok=response.status_code)
```

See [Child-Process Hooks](child-process-hooks#shared-async-child-start-hooks).

## The Timeout

The shared async child holds the deadline of each of its tasks. It starts the deadline when it starts the task.

1. At `timeout_ms`, the child cancels the task: the task gets `asyncio.CancelledError` at its next `await`.
2. When the coroutine of the task has ended, the child reports the end. The worker then writes `TASK_TIMEOUT`, the retry of the retry policy, and the next workflow step. The attempt ends before its row moves, so a retry never runs next to the attempt that it replaces.
3. A task that returns a value after its deadline fired still fails with `TASK_TIMEOUT`.

The other tasks of the child continue. A timeout is terminal by default; add `"TASK_TIMEOUT"` to `auto_retry_for` for a retry.

The worker keeps one more timer per task, at `timeout_ms + cancel_grace_ms + 5 seconds`. A child reports each cancel at once, so a child that has not reported the cancel by then is frozen: a task blocks its event loop, or C code holds the GIL. The worker kills that child. The timed-out task fails with `TASK_TIMEOUT`, and every other task of that child takes the crash path (`WORKER_CRASHED`, or a retry when its policy names that code).

A task that catches its cancel and goes on keeps its seat until it ends, or until the worker stops and its drain timeout kills the child. Do not catch `asyncio.CancelledError` to go on working.

## Rules for Task Authors

**Do not block the event loop.** Every task of the child runs on one loop. A call that blocks it, such as `time.sleep()`, a sync HTTP or database client, or long CPU work, stops every task of the child for that time, and no deadline can act during it.

```python
import time

# Wrong: stops every task of the child for 2 seconds
@app.task('wait_wrong', execution=Execution.SHARED_ASYNC, timeout_ms=10_000)
async def wait_wrong() -> TaskResult[None, TaskError]:
    time.sleep(2)
    return TaskResult(ok=None)


# Correct
@app.task('wait_right', execution=Execution.SHARED_ASYNC, timeout_ms=10_000)
async def wait_right() -> TaskResult[None, TaskError]:
    await asyncio.sleep(2)
    return TaskResult(ok=None)
```

A short sync call can go to `asyncio.to_thread()`. A cancel does not stop a thread: at the timeout the coroutine ends, and the thread runs on.

**Send with `send_async()`.** In an async task the sync `send()` returns `Err(TaskSendAsyncContext)`, as in any running event loop.

**Await what you start.** A task that creates an asyncio task must await it, or cancel it and await it, before it returns. Work that is left on the loop keeps running beside the next tasks of the child, and a cancel at the timeout does not reach it.

**Set `timeout_ms`.** The decorator refuses a `SHARED_ASYNC` task without one (`HRS-109`). Size it as a hang backstop above the normal runtime.

**Expect to share the process.** Module state, caches and clients are shared by every task of the child. A crash of the child, an out-of-memory kill, or a kill by the worker ends every task in it. Such a task takes the crash path. A task that must not share a process stays `ISOLATED`.

## Recycling

A shared async child is replaced when it reaches a limit of `SharedRecycle`:

- `after_per_seat`: after `after_per_seat.tasks × seats` tasks. The default is 100 tasks per seat. The limit counts per seat, so a child with many seats is not replaced after a few seconds of work.
- `above`: when its RSS, read after each task, reaches the limit. Its tasks share the process, so this bounds them together.

At a limit the child closes: it takes no new task, its running tasks finish, and then it stops and a new child starts. A child that does not exit 10 seconds after its stop frame is killed. While a child closes, its free seats take no task.

## Sizing

The seats needed are about the rate of tasks times their mean duration. For example, 20 tasks per second that each wait 4 seconds need 80 seats: 4 children of 25 seats, or 5 with room to spare.

A new task goes to the open shared child with the fewest tasks in flight, so the tasks spread over the children. A crash then ends about the tasks in flight divided by the children, not all the seats of one child.

A seat that waits on the network holds at least one socket, and a process has a limit of open files (`ulimit -n`, often 1024). `seats` is at most 1000.

## Limits

- A cancel ends the coroutine of a task, not threads or subprocesses that it started.
- A task that ignores its cancel keeps its seat until it ends or the worker stops.
- A blocked loop delays every timeout of the child until the block ends, or until the worker kills the frozen child.
- Each shared async child opens one async connection pool of `worker_child_pool_max_size` connections, as an isolated child opens one sync pool. See [Connections](worker-profiles#connections).

## Things to Avoid

**Don't mark CPU work `SHARED_ASYNC`.** It blocks the loop of every task in the child. Keep it `ISOLATED`.

**Don't open a client per task in module state on first use.** A client that a task opens on first use belongs to the loop as well, but the child cannot tell it from work that the task left running. Open it in `on_shared_async_child_start`.

**Don't rely on a thread to stop at the timeout.** `asyncio.to_thread()` work runs to its end after the cancel. Give the sync call its own timeout.

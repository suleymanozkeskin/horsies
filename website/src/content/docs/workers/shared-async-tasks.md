---
title: Shared Async Tasks
summary: SHARED_ASYNC tasks run as coroutines, many at once, in shared async children - what runs where, the timeout (cancel, second cancel, close, kill), a return that leaves work alive, a blocked event loop, the rules for task authors, recycling and sizing.
related: [execution-modes, worker-profiles, worker-architecture, child-process-hooks, ../tasks/defining-tasks]
tags: [workers, async, shared-async, execution, timeouts, cancellation, concurrency]
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

A worker may run a task more isolated than its row says, never less. A worker with no shared async children claims `SHARED_ASYNC` rows into its `ISOLATED` budget and runs them in isolated children, with the usual kill at `timeout_ms`. A worker with shared async children claims them into its `SHARED_ASYNC` budget, whose capacity is the seats of those children; its isolated children never run one. See [Budgets per Execution Class](/horsies/workers/concurrency/#budgets-per-execution-class).

A shared async child runs only `async def` tasks. When a deploy changed the function of a queued `SHARED_ASYNC` row to `def`, the worker writes `ISOLATED` on the row and returns it to the queue before any child takes it.

Enqueue stores a `timeout_ms` and a `cancel_grace_ms` on every `SHARED_ASYNC` row. A row without a usable one (absent, out of bounds, not an int, or options that do not parse) fails with `WORKER_SERIALIZATION_ERROR` on every worker, after its RUNNING update and before the task resolves, as an isolated row with a refused stored timeout does. The task never runs, and the row does not return to the queue. Such a row comes only from a write outside enqueue.

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

This worker runs `ISOLATED` tasks in 2 isolated children and `SHARED_ASYNC` tasks on 4 × 25 = 100 seats. See [Worker Profiles](/horsies/workers/worker-profiles/#sharedasyncchildren) for the fields.

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

See [Child-Process Hooks](/horsies/workers/child-process-hooks/#shared-async-child-start-hooks).

## The Timeout

The shared async child holds the deadline of each of its tasks. It starts the deadline when it starts the task. Here T is the deadline and G is `cancel_grace_ms` (default 1,000 ms, from 100 to 10,000 ms).

1. **At T**, the child cancels the task: the task gets `asyncio.CancelledError` at its next `await`.
2. **The attempt ends** when its coroutine has ended and no work that the task started is alive. That work is each asyncio task that the task created, itself or through a library, and each call that it sent to the default executor (`asyncio.to_thread()`, `loop.run_in_executor(None, ...)`, the DNS lookups of `loop.getaddrinfo()`). The child waits for that work, then reports the end. Only then does the worker write `TASK_TIMEOUT`, the retry of the retry policy, and the next workflow step. So a retry never runs next to the attempt that it replaces.
3. **At T + G**, when anything of the attempt is still alive, the child sends a second cancel to the task and to the asyncio tasks that it started. Then it cancels the tasks that it finds alive again, two more times, 0.1 seconds apart. The child takes no new task from this point: cancelled code can leave broken state in the process, so the child is replaced when its other tasks end, also when this attempt ends.
4. **0.3 seconds after the second cancel**, an attempt that is still alive is stuck. It keeps its seat. The other tasks of the child run to their own end. The worker kills the child when every task left in it is stuck: then no task in it has time left. Each of those tasks fails with `TASK_TIMEOUT`, and its retry (when the policy names `TASK_TIMEOUT`) runs in an isolated child.

A task that returns a value after its deadline fired still fails with `TASK_TIMEOUT`. A task that returns before its deadline keeps what it returned, also when work that it started is still alive then: see [A Return That Leaves Work](#a-return-that-leaves-work). The other tasks of the child continue in every case. A timeout is terminal by default; add `"TASK_TIMEOUT"` to `auto_retry_for` for a retry.

The finalizing marker of a task is written when its attempt ends, never before: after its coroutine and the work that it started. Only a task that returned before its deadline, and whose work ended within the grace, gets it; the child reports the end after the marker, and waits for it at most until the deadline of the task. The marker stops the runner heartbeats of the row and gives the worker `finalizing_stale_threshold_ms` to write the outcome before stale recovery. A task that timed out, or whose work needed a cancel, gets no marker: the child reports its end at once, and the heartbeats of the worker cover the row until the worker reads that end. So the heartbeats of a row never stop while work of its attempt still runs, and the marker never uses the cancel grace or changes the outcome of a task.

Library work that a task causes counts as the work of that task. For example, when a cancel closes a connection of an asyncpg pool, the pool starts a refill task in the context of the task, and the attempt ends when that refill task ends. Work that an `on_shared_async_child_start` hook started is not the work of any task.

What a cancel does to common libraries, measured on one shared child (CPython 3.13, local network, see the notes below the table):

| Case | After the first cancel | After the second cancel |
| ---- | ---------------------- | ----------------------- |
| healthy server, every library below | ended within 16 ms | not needed |
| httpx or aiohttp, server that does not answer or stops mid-body | ended within 6 ms (one run: 64 ms, during a stall of the host) | not needed |
| psycopg 3.3.6 or later, server that stops answering during a query | ends after 10.0 s (cancel request 5 s, then a drain of 5 s) | ended within 5 ms |
| SQLAlchemy with asyncpg, server that stops answering during a query | ends after 2.0 s | ended within 7 ms |
| asyncpg, server that stops answering | no end | connection: within 1 ms; pool: one more round, about 0.1 s |
| SQLAlchemy 2.0.55 or later, server that stops answering while a session closes | no end (the close holds the cancel back) | one more round, about 0.1 s |
| `asyncio.to_thread()` with a sync client | the thread runs on | the thread runs on: it ends only by the client's own timeout |
| DNS lookup with a resolver that does not answer | the lookup thread runs on, 9.7 s with glibc defaults | the thread runs on |

Notes: a pause of the database container stood in for a server that stops answering; the kernel still accepted TCP connections. Networks that drop packets, TLS and remote servers add round trips to each cleanup step, and were not measured. The psycopg row used `psycopg-binary` 3.3.8, with its bundled libpq 18.6. With a libpq older than 17, psycopg sends its cancel request from a thread with no limit, and the attempt ends only when that thread returns. See [psycopg and libpq](/horsies/configuration/broker-config/#psycopg-and-libpq).

The worker keeps one timer per task, at `timeout_ms + cancel_grace_ms + 10 seconds`. The child reports the second cancel at T + G, so a child that has reported neither the second cancel nor the end by then is frozen: C code holds the GIL, and nothing in the child can run. The worker kills that child. Each task in it that is past its deadline fails with `TASK_TIMEOUT`; every other task takes the crash path (`WORKER_CRASHED`, or a retry when its policy names that code). No task is named as the cause, so no retry moves to an isolated child.

## A Return That Leaves Work

A task can return before its deadline while work that it started is still alive: an asyncio task that it created and did not await, or a call that it sent to the default executor. A return is a value, or an exception that the exception mapper of the task turns into a result. Here R is that return and G is `cancel_grace_ms`.

1. **From R**, the deadline of the task no longer applies: the task returned in time, and its outcome is what it returned. The child waits up to G for that work.
2. **At R + G**, when that work is still alive, the child cancels it, then cancels what it finds alive again, two more times, 0.1 seconds apart. The child takes no new task from this point: cancelled code can leave broken state in the process, so the child is replaced when its other tasks end.
3. **When that work has ended**, the child reports the end. Only then does the worker write what the task returned, with its retry and its next workflow step, as for any result. So a retry never runs next to work of the attempt that it replaces.
4. **0.3 seconds after the first cancel**, work that is still alive is stuck: a task that catches every cancel, or a thread in the default executor. The task keeps its seat, and the other tasks of the child run to their own end. The worker kills the child when every task left in it is stuck, and writes what the task returned after the exit. The task is not a cause of the kill: it gets no `TASK_TIMEOUT`, and a retry of the result that it returned stays in its execution class. Another kill by the worker after this point (a blocked loop, a frozen child, the drain timeout at a stop) also writes what the task returned.

| The work at R + G | Outcome of the task | The child |
| ----------------- | ------------------- | --------- |
| ended | what the task returned | takes the next task |
| ended after the cancel | what the task returned | replaced when its tasks end |
| alive after the cancel rounds | what the task returned, written after the kill | killed when every task left in it is stuck |

For each task that left work, the child logs one line: the task name, the task id, the number of the run, the coroutine names of that work (at most 10), the executor calls, and how the work ended (within the grace, cancelled, or stuck).

A client that a task opens on first use, in module state, starts its background tasks (a keepalive, a pool refill) in the context of that task. They count as the work of that task, and they are still alive when it returns, so the child cancels them and is replaced. The next child opens the client again in its first task and is replaced again: each task that opens such a client costs one child restart. The other tasks of the closing child use a client whose background work was cancelled. Open such a client in an `on_shared_async_child_start` hook instead: work that a start hook starts belongs to no task (see [Keep a client for the life of the child](#keep-a-client-for-the-life-of-the-child)).

## A Blocked Event Loop

Every task of the child runs on one event loop. A thread in the child, not on the loop, watches it. When the loop has not turned for 5 seconds (the block limit), that thread names the task that holds the loop, with the source line, and reports it to the worker. The worker kills the child at once: no task of a blocked loop can move, and no deadline can act during the block.

| Task | Outcome |
| ---- | ------- |
| the task that held the loop, before its deadline | `TASK_BLOCKED_LOOP`; its retry runs in an isolated child |
| the task that held the loop, past its deadline | `TASK_TIMEOUT`; its retry runs in an isolated child |
| another task, past its deadline | `TASK_TIMEOUT` |
| another task, before its deadline | `NEIGHBOUR_BLOCKED_LOOP` |

Each code retries only when `auto_retry_for` names it: a neighbour is not retried for free, because its side effects may already have happened. When the loop runs code of no task (a callback, or work of a start hook), no task is named: each task before its deadline takes the crash path. C code that holds the GIL stops the watching thread as well, so no report comes; the timer of the worker above ends that child, and no task is named.

The block limit is 7.4 times the longest common burst of CPU work measured on one call: 674 ms for a backtracking `re.findall` over 10 MB of text. A full garbage collection of about one million objects took 258 ms, and `json.loads` of 10 MB took 118 ms. During a block no timer of the loop runs, so each deadline in the child can be late by up to the block limit.

## Rules for Task Authors

**Let `CancelledError` propagate.** Catch it only to clean up, then raise it again. A task that catches its cancel to go on working needs the second cancel, and closes its child. A task that ignores every cancel is stuck: its child is killed when every task left in it is stuck. `anyio` shielded scopes (`CancelScope(shield=True)`) protect only against cancels of other `anyio` scopes, not against `Task.cancel()`: the cancel of the worker can cut such a section.

**Give each client call its own timeout, below the task timeout.** A database or HTTP call that waits on a server that stopped answering then ends by itself, and the task can return its own error. For a sync call in `asyncio.to_thread()`, a timeout is the only way the call ends: a cancel does not stop a thread. Set the client timeout so that the call returns before `timeout_ms + cancel_grace_ms`, or the task is stuck.

**Open long-lived clients in the start hook.** A client that a task opens on first use carries the mark of that task: its background work counts as the work of that task. The child cancels that work `cancel_grace_ms` after the task returns and is replaced, so each task that opens the client costs one child restart (see [A Return That Leaves Work](#a-return-that-leaves-work)). Open it in `on_shared_async_child_start` (see [Keep a client for the life of the child](#keep-a-client-for-the-life-of-the-child)).

**Do not block the event loop.** A call that blocks it, such as `time.sleep()`, a sync HTTP or database client, or long CPU work, stops every task of the child for that time. A block of 5 seconds kills the child.

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

**Await what you start.** The attempt ends only when the work that the task started has ended. A task that returns while an asyncio task that it started still runs holds its seat until that task ends. After `cancel_grace_ms` the child cancels that task and is replaced; the outcome stays what the task returned.

**Send with `send_async()`.** In an async task the sync `send()` returns `Err(TaskSendAsyncContext)`, as in any running event loop.

**Set `timeout_ms`.** The decorator refuses a `SHARED_ASYNC` task without one (`HRS-109`). Size it as a hang backstop above the normal runtime.

**Expect to share the process.** Module state, caches and clients are shared by every task of the child. A crash of the child, an out-of-memory kill, or a kill by the worker ends every task in it. A task that must not share a process stays `ISOLATED`.

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

- A cancel ends coroutines, not threads. A thread that a task started, in the default executor or by itself, runs on after the cancel. Threads and processes that a task starts itself are not seen as its work.
- A stuck task keeps its seat until every task left in its child is stuck, or until the worker stops and its drain timeout kills the child.
- The worker holds what a task returned only from the end of its left work, or from the report that this work is stuck. A kill of the child before then (a blocked loop, a frozen child, the drain timeout at a stop) gives that task the verdict of a task that has not returned: within the grace, `TASK_TIMEOUT` past its deadline and the crash path before it; in the cancel rounds, the crash path.
- A blocked loop delays every timeout of the child until the block ends, or until the worker kills the child.
- A refusal (a row whose stored timeout or grace the worker cannot use) runs no task code. A child that still holds one 120 seconds after its placement is frozen or waits on a database that does not answer; the worker kills it, and the refusal takes the crash path.
- Each shared async child opens one async connection pool of `worker_child_pool_max_size` connections, as an isolated child opens one sync pool. Its seats share that pool for the RUNNING update and the finalizing marker of each task, and wait up to 30 seconds for a free connection. Size it as for [shared thread children](/horsies/workers/shared-thread-tasks/#connections). See [Connections](/horsies/workers/worker-profiles/#connections).

## Things to Avoid

**Don't mark CPU work `SHARED_ASYNC`.** It blocks the loop of every task in the child. Keep it `ISOLATED`.

**Don't open a client per task in module state on first use.** Its background work carries the mark of the task that opened it: the child cancels it after that task returns and is replaced. Open it in `on_shared_async_child_start`.

**Don't rely on a thread to stop at the timeout.** `asyncio.to_thread()` work runs to its end after the cancel. Give the sync call its own timeout.

**Don't catch `CancelledError` to go on working.** The task then needs the second cancel, and its child is replaced.

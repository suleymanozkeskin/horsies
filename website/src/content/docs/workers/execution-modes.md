---
title: Choosing an Execution Mode
summary: Which execution mode fits a task - isolated, shared async or shared thread - what each mode requires, what a timeout and a crash do, the rules for task authors, and a worker profile for each, with measured conditions and results.
related: [worker-profiles, shared-async-tasks, shared-thread-tasks, worker-architecture, concurrency, ../tasks/defining-tasks]
tags: [workers, execution, isolated, shared-async, shared-thread, profiles, timeouts, free-threading]
---

# Choosing an Execution Mode

A task declares its execution mode with `execution=` on `@app.task()`. The mode sets how a worker runs the task: alone in a child process, as a coroutine next to other tasks in one process, or on a thread next to other tasks in one process. The worker profile sets how many children of each mode a worker starts. This page helps you choose a mode. The linked pages hold the details.

## The Three Modes

<!-- members: Execution -->
| `execution` | A child runs | Function | `timeout_ms` | Details |
| ----------- | ------------ | -------- | ------------ | ------- |
| `Execution.ISOLATED` | one task at a time, in a process of its own. This is the default | `def` or `async def` | optional | [Worker Architecture](/horsies/workers/worker-architecture/) |
| `Execution.SHARED_ASYNC` | up to `seats` tasks at once, as coroutines on one event loop | `async def` | required | [Shared Async Tasks](/horsies/workers/shared-async-tasks/) |
| `Execution.SHARED_THREAD` | up to `seats` tasks at once, each on a thread of its own | `def` | required | [Shared Thread Tasks](/horsies/workers/shared-thread-tasks/) |

The decorator refuses a declaration that breaks these rules, one code per rule (`HRS-107` to `HRS-111`). See [Choose the Execution Class](/horsies/tasks/defining-tasks/#choose-the-execution-class).

A worker gives a task more isolation than its row asks for, never less. A worker with no shared async children runs `SHARED_ASYNC` tasks in its isolated children, with the usual kill at `timeout_ms`. The same applies to `SHARED_THREAD` tasks on a worker with no shared thread children. So you can mark a task shared before any worker has shared children. See [Budgets per Execution Class](/horsies/workers/concurrency/#budgets-per-execution-class).

## Choose by the Work

| The work of the task | Mode |
| -------------------- | ---- |
| Waits on the network, a database or an API, with an async client | `SHARED_ASYNC` |
| Waits on the network, a database or an API, with a sync client only | `SHARED_THREAD` |
| CPU work in C code that releases the GIL, such as `zlib`, `hashlib` or many NumPy array operations | `SHARED_THREAD` |
| CPU work in pure Python, on a free-threaded build (CPython 3.13t or 3.14t) | `SHARED_THREAD` |
| CPU work in pure Python, on a standard build | `ISOLATED` |
| Needs a process of its own (see [When to Keep a Task Isolated](#when-to-keep-a-task-isolated)) | `ISOLATED` |

### Waits on the Network: Shared Async

A shared async seat is one coroutine on the event loop of the child. A seat that waits costs no process and no thread. It holds at least one socket while it waits, and the process has a limit of open files (`ulimit -n`, often 1024). `seats` is at most 1000 per child.

The list below shows what a cancel did to common clients. Conditions: one shared async child, CPython 3.13, a local network, 30 runs per case. A paused database container stood in for a server that stops answering.

- A healthy server: httpx, aiohttp, psycopg, asyncpg and SQLAlchemy ended within 16 ms of the first cancel.
- httpx and aiohttp, against a server that does not answer or stops mid-body: ended within 6 ms of the first cancel. One run took 64 ms, during a stall of the host.
- psycopg 3.3.6 or later, against a server that stops answering: ended 10.0 s after the first cancel, and within 5 ms of the second cancel.
- asyncpg, against a server that stops answering: did not end after the first cancel. The second cancel ended a connection within 1 ms. A pool needed one more cancel round, about 0.1 s.
- `asyncio.to_thread()` and a DNS lookup: no cancel stopped the thread. It ran until the client timeout or the resolver gave up (9.7 s with glibc defaults).

When the task is still alive at `timeout_ms + cancel_grace_ms`, the child sends the second cancel, then two more rounds. The full table is in [The Timeout](/horsies/workers/shared-async-tasks/#the-timeout).

A cancel of one task did not slow the other tasks of the loop. Conditions: one event loop with 25 tasks; 24 tasks sent HTTP requests to a server with a 100 ms delay through one shared `httpx.AsyncClient`; one task ran one of 13 cancel cases, among them servers that stop answering; 10 runs per case, about 26,500 requests per case. Result: the request latency of the 24 tasks stayed within 0.6 ms (p50) and 1.6 ms (p99) of runs with no cancelled task.

### Waits in a Sync Client: Shared Thread

A wait in a sync client releases the GIL, so the seat threads of a child wait at the same time. This case was not measured. A thread cannot be cancelled, so give each call a client timeout below `timeout_ms`. A call that hangs keeps its seat, and its child closes at the deadline of the task.

### CPU Work in C That Releases the GIL: Shared Thread

C code that releases the GIL runs on several threads of one process at the same time. Conditions: Linux with 4 CPUs; CPython 3.13 and 3.14, standard and free-threaded builds; `zlib.compress` of 1 MB and `hashlib.sha256` of 10 MB. Results:

- 4 threads did 2.6 to 3.8 times the work of 1 thread, on every build.
- 4 threads did 0.89 to 1.13 times the work of 4 processes.

The measurement covered `zlib` and `hashlib` only. Other C code scales in the same way only when it releases the GIL for the whole call.

A shared thread seat is an OS thread with a stack of its own: 8 MiB of address space by default on Linux with glibc, 512 KiB on macOS. `seats` is at most 256 per child.

### CPU Work in Pure Python

On a standard build, the GIL lets one thread run Python code at a time. Conditions as above, with a pure-Python loop and `json.loads` of 1 MB. Results:

| Build | 4 threads against 1 thread | 4 threads against 4 processes |
| ----- | -------------------------- | ----------------------------- |
| Standard CPython 3.13 and 3.14 | 0.56 to 1.02 times: no gain | 0.22 to 0.40 |
| Free-threaded CPython 3.13t and 3.14t | 1.9 to 3.6 times | 0.73 to 1.06 |

- On a standard build, keep pure-Python CPU work `ISOLATED`. Each isolated child is a process, so the children use the CPUs at the same time.
- On a free-threaded build, `SHARED_THREAD` scales like processes. The measurement used the standard library only.
- On a free-threaded build, each C extension that the child imports must support free-threading, those of horsies and its dependencies included. An extension without that support turns the GIL back on. In a worker child, `sys._is_gil_enabled()` returns `False` when the GIL is off.

### More Threads Than CPUs

For CPU work, more thread seats than CPUs added nothing. Conditions: 4 CPUs, all four builds, all four workloads above. Result: 8 and 16 threads stayed within the run-to-run spread of 4 threads. Processes showed the same limit.

For CPU work, give all the thread children of a machine together at most one seat per CPU. For tasks that wait in a sync client, the seats needed are about the rate of tasks times their mean duration. See [Sizing](/horsies/workers/shared-thread-tasks/#sizing).

### When to Keep a Task Isolated

Keep a task `ISOLATED` when one of these is true:

- It must stop exactly at `timeout_ms`. An isolated task is killed at its deadline, with the subprocesses in its session. A shared task is not: see [Timeouts and Crashes](#timeouts-and-crashes).
- It runs native code that can crash the process. A crash of a shared child ends every task in it.
- Its memory grows while it runs. The tasks of a shared child share one process and one memory limit.
- It starts subprocesses or threads that must end with it.
- It keeps module state that is not safe to share with other tasks.
- It runs pure-Python CPU work on a standard build.

## Timeouts and Crashes

| Event | `ISOLATED` | `SHARED_ASYNC` | `SHARED_THREAD` |
| ----- | ---------- | -------------- | --------------- |
| The task reaches `timeout_ms` | The worker kills the child and the subprocesses in its session | The child cancels the task: `asyncio.CancelledError` at its next `await` | The child reports the deadline and takes no new task. The task runs on, because a thread cannot be cancelled |
| After that | `TASK_TIMEOUT`, after the child has exited | `TASK_TIMEOUT`, after the coroutine and the work that it started have ended. Still alive `cancel_grace_ms` later: a second cancel, and the child takes no new task. Still alive 0.3 s after the second cancel: the task is stuck | `TASK_TIMEOUT`, after the worker kills the child. The kill comes when no task in the child has time left |
| The task blocks the process | Nothing else is affected | A loop that has not turned for 5 s: the worker kills the child at once. The task fails with `TASK_BLOCKED_LOOP`, each other task before its deadline with `NEIGHBOUR_BLOCKED_LOOP` | C code that holds the GIL stops every thread. When the child has not reported the deadline by `timeout_ms + 10 s`, the worker kills it |
| The child crashes | That task fails with `WORKER_CRASHED` | Every task in the child fails with `WORKER_CRASHED` | Every task in the child fails with `WORKER_CRASHED` |

A task retries only when its `auto_retry_for` names the code. In every mode, the worker writes the outcome only after the attempt has ended, so a retry never runs next to the attempt that it replaces. A shared task that the worker names as the cause of a kill or of a stuck stop retries in an isolated child.

## Rules for Task Authors

Each mode adds rules to [Defining Tasks](/horsies/tasks/defining-tasks/). The pages of the modes explain each rule.

`ISOLATED`:

- Set `timeout_ms` as a hang backstop above the normal runtime.
- In an `async def` task, await the asyncio tasks that it starts. See [Async Tasks](/horsies/tasks/defining-tasks/#async-tasks).

`SHARED_ASYNC` ([Rules for Task Authors](/horsies/workers/shared-async-tasks/#rules-for-task-authors)):

- Let `asyncio.CancelledError` propagate. Catch it only to clean up, then raise it again.
- Give each client call its own timeout, below `timeout_ms`.
- Open long-lived clients in an `@app.on_shared_async_child_start` hook.
- Do not block the event loop: no `time.sleep()`, no sync client, no long CPU work.
- Await what you start.
- Send with `send_async()`.

`SHARED_THREAD` ([Rules for Task Authors](/horsies/workers/shared-thread-tasks/#rules-for-task-authors)):

- Use a lock for module state that a task changes.
- Give each blocking call its own timeout, below `timeout_ms`.
- Do not start threads that outlive the task.

## Profile Examples

A worker profile sets the children of each mode. A profile can mix the modes.

```python
from pydantic import SecretStr

from horsies import (
    AppConfig,
    Horsies,
    IsolatedChildren,
    PostgresConfig,
    SharedAsyncChildren,
    SharedThreadChildren,
    WorkerProfile,
)

app = Horsies(AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr('postgresql+psycopg://user:pass@localhost:5432/mydb'),
    ),
    worker_profiles={
        # Every task in an isolated child
        'default': WorkerProfile(isolated=IsolatedChildren(count=8)),
        # Network waits on 4 x 25 = 100 seats; other tasks in 2 isolated children
        'io': WorkerProfile(
            isolated=IsolatedChildren(count=2),
            shared_async=SharedAsyncChildren(count=4, seats=25),
        ),
        # C code that releases the GIL, on a machine with 8 CPUs: 2 x 4 = 8 seats
        'compute': WorkerProfile(
            isolated=IsolatedChildren(count=2),
            shared_thread=SharedThreadChildren(count=2, seats=4),
        ),
    },
))
```

One task of each mode:

```python
import asyncio
import zlib

from horsies import Execution, TaskError, TaskResult


@app.task('fetch_rate', execution=Execution.SHARED_ASYNC, timeout_ms=10_000)
async def fetch_rate(*, currency: str) -> TaskResult[str, TaskError]:
    await asyncio.sleep(0.1)  # stands in for an HTTP call with its own timeout
    return TaskResult(ok=currency)


@app.task('compress_report', execution=Execution.SHARED_THREAD, timeout_ms=60_000)
def compress_report(*, text: str) -> TaskResult[int, TaskError]:
    return TaskResult(ok=len(zlib.compress(text.encode(), level=6)))


@app.task('render_invoice')
def render_invoice(*, invoice_id: int) -> TaskResult[str, TaskError]:
    return TaskResult(ok=f'invoice-{invoice_id}.pdf')
```

```bash
horsies worker myapp.instance:app --profile io
```

A worker with the `io` profile runs `fetch_rate` on a shared async seat. It runs `compress_report` and `render_invoice` in its isolated children, because the profile has no shared thread children. A worker with the `compute` profile runs `compress_report` on a seat thread, and `fetch_rate` in an isolated child.

A profile can also hold only shared children. Its worker claims no `ISOLATED` rows, so another worker must have isolated children for them. The fleet check reports a class that no live worker serves. See [Does a live worker serve every pending class?](/horsies/monitoring/worker-health/#does-a-live-worker-serve-every-pending-class)

Each child opens one connection pool, whatever its seats, so a shared child counts as one child in the connection budget. See [Connections](/horsies/workers/worker-profiles/#connections).

## Things to Avoid

**Don't mark CPU work `SHARED_ASYNC`.** It holds the event loop, and every other task of the child waits. A block of 5 seconds kills the child.

**Don't mark pure-Python CPU work `SHARED_THREAD` on a standard build.** All its seats together get about one CPU.

**Don't add thread seats above the CPU count for CPU work.** They add no throughput.

**Don't mark a task shared when it must stop exactly at its timeout.** A shared task stops at a cancel, or when its child is killed. Keep it `ISOLATED`.

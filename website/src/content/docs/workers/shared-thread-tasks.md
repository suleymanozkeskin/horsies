---
title: Shared Thread Tasks
summary: SHARED_THREAD tasks run on threads, many at once, in shared thread children - when threads help (measured), what runs where, the timeout and the kill, the rules for task authors, recycling and sizing.
related: [worker-profiles, worker-architecture, shared-async-tasks, ../tasks/defining-tasks]
tags: [workers, threads, shared-thread, execution, timeouts, concurrency, free-threading]
---

# Shared Thread Tasks

A task with `execution=Execution.SHARED_THREAD` can run in a shared thread child. A shared thread child is one process that runs up to `seats` such tasks at once, each on a thread of its own. A seat that becomes free takes the next task at once.

## Why and When

Threads share one process, so a thread task costs less memory and less start time than an isolated task. Whether the threads run at the same time depends on the work and on the Python build. Conditions of the table: Linux with 4 CPUs; CPython 3.13 and 3.14, standard and free-threaded builds; tasks per second of 4 threads in a `ThreadPoolExecutor`, against 1 thread and against a pool of 4 processes:

| Work | Standard CPython: 4 threads against 1 | Free-threaded CPython: 4 threads against 1 | 4 threads against 4 processes |
|------|---------------------------------------|--------------------------------------------|-------------------------------|
| C code that releases the GIL (`zlib.compress` of 1 MB, `hashlib.sha256` of 10 MB) | 2.8 to 3.8 times | 2.6 to 3.8 times | 0.89 to 1.13 |
| Pure Python (a loop and a dict) | 0.84 to 1.02 times: no gain | 3.0 to 3.6 times | standard 0.30 to 0.37, free-threaded 0.93 to 1.06 |
| `json.loads` of 1 MB | 0.56 to 1.00 times: no gain | 1.9 to 2.7 times | standard 0.22 to 0.40, free-threaded 0.73 to 0.94 |

- C code that releases the GIL scales with threads on every build, as processes do.
- Pure-Python code needs a free-threaded build to scale with threads. On a standard build, more seats give the throughput of about one CPU.
- More threads than CPUs added nothing in this set, for any work.
- A third-party C extension without free-threading support can turn the GIL back on in a free-threaded build.
- Threads that wait in sync I/O (a sync database or HTTP client) were not measured. Such a wait releases the GIL, so the threads wait at the same time.

Use a shared thread task for `def` code that waits in a sync client, or that calls C code that releases the GIL, such as hashing, compression or image work. Use [Shared Async Tasks](shared-async-tasks) for code that has an async client. Keep `Execution.ISOLATED` (the default) for CPU work in pure Python on a standard build, and for tasks that must not share a process with other tasks.

## What Runs Where

<!-- members: Execution subset -->
| Row class | Worker with shared thread children | Worker with none |
| --------- | ---------------------------------- | ---------------- |
| `SHARED_THREAD` | a shared thread child, on a seat thread | an isolated child, one task in the process |
| `ISOLATED` | an isolated child | an isolated child |

A worker may run a task more isolated than its row says, never less. A worker with no shared thread children claims `SHARED_THREAD` rows into its `ISOLATED` budget and runs them in isolated children, with the usual kill at `timeout_ms`. A worker with shared thread children claims them into its `SHARED_THREAD` budget, whose capacity is the seats of those children; its isolated children and its shared async children never run one. See [Budgets per Execution Class](concurrency#budgets-per-execution-class).

A shared thread child runs only `def` tasks. When a deploy changed the function of a queued `SHARED_THREAD` row to `async def`, the worker writes `ISOLATED` on the row and returns it to the queue before any child takes it.

Enqueue stores a `timeout_ms` on every `SHARED_THREAD` row. A row without a usable one (absent, out of bounds, not an int, or options that do not parse) fails with `WORKER_SERIALIZATION_ERROR` on every worker, after its RUNNING update and before the task resolves. The task never runs, and the row does not return to the queue. Such a row comes only from a write outside enqueue.

## How To

Declare the task. `timeout_ms` is required. A thread cannot be cancelled, so a thread task takes no `cancel_grace_ms`.

```python
import hashlib

from pydantic import SecretStr

from horsies import (
    AppConfig,
    Execution,
    Horsies,
    IsolatedChildren,
    PostgresConfig,
    SharedThreadChildren,
    TaskError,
    TaskResult,
    WorkerProfile,
)

app = Horsies(AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr('postgresql+psycopg://user:pass@localhost:5432/mydb'),
    ),
    worker_profiles={
        'files': WorkerProfile(
            isolated=IsolatedChildren(count=2),
            shared_thread=SharedThreadChildren(count=2, seats=4),
        ),
    },
))


@app.task('digest_file', execution=Execution.SHARED_THREAD, timeout_ms=60_000)
def digest_file(*, path: str) -> TaskResult[str, TaskError]:
    digest = hashlib.sha256()
    with open(path, 'rb') as source:
        for block in iter(lambda: source.read(1 << 20), b''):
            digest.update(block)
    return TaskResult(ok=digest.hexdigest())
```

Start a worker with a profile that has shared thread children:

```bash
horsies worker myapp.instance:app --profile files
```

This worker runs `ISOLATED` tasks in 2 isolated children and `SHARED_THREAD` tasks on 2 × 4 = 8 seats. See [Worker Profiles](worker-profiles#sharedthreadchildren) for the fields.

## The Timeout

The shared thread child holds the deadline of each of its tasks. It starts the deadline when it takes the task. A thread cannot be cancelled, so a deadline cannot stop the task:

1. At `timeout_ms`, the child reports the deadline, and the task runs on. The child closes: it takes no new task, and a new child starts when it has ended.
2. When no task in the child has time left, that is, when every task still in it is past its own deadline, the worker kills the child with its session, so the subprocesses of its tasks go with it. After the exit, the worker writes `TASK_TIMEOUT`, the retry of the retry policy, and the next workflow step for each of those tasks. The task whose deadline closed the child is the culprit: its retry runs in an isolated child, where its own timeout kills only it. A task that is alone in its child is killed at its deadline.
3. A task in the child that still has time left runs on, to its end or to its own deadline.
4. A task that ends by itself after its deadline fails with `TASK_TIMEOUT`, also when it returns a value.

The attempt ends before its row moves, so a retry never runs next to the attempt that it replaces. A timeout is terminal by default; add `"TASK_TIMEOUT"` to `auto_retry_for` for a retry.

The worker keeps one more timer per task, at `timeout_ms + 10 seconds`. A child reports each deadline at once, so a child that has not reported the deadline by then is frozen: C code holds the GIL, and no thread of the child runs. The worker kills that child. Each task in it that is past its deadline fails with `TASK_TIMEOUT`, and every other task takes the crash path (`WORKER_CRASHED`, or a retry when its policy names that code). The worker cannot tell which thread held the GIL, so it names no culprit.

## Rules for Task Authors

**The function runs next to itself.** Up to `seats` calls of the same function, and of the other thread tasks of the app, run at once in one process, each on its own thread.

**Do not use module state without a lock.** Module variables, caches and clients are shared by every thread of the child. Use a lock for state that a task changes, or keep the state inside the call. A client is safe to share only when its library says so.

```python
import threading

_seen: set[str] = set()
_seen_lock = threading.Lock()


@app.task('mark_seen', execution=Execution.SHARED_THREAD, timeout_ms=10_000)
def mark_seen(*, key: str) -> TaskResult[bool, TaskError]:
    with _seen_lock:
        first = key not in _seen
        _seen.add(key)
    return TaskResult(ok=first)
```

**Set `timeout_ms`, and give each blocking call its own timeout.** The decorator refuses a `SHARED_THREAD` task without one (`HRS-109`). A task that passes its deadline closes its child, and the kill ends every task that is still in it once none has time left. Set the timeout of a sync client below `timeout_ms`, so that a call that hangs returns an error to the task instead of closing the child.

**Expect to share the process.** A crash of the child, an out-of-memory kill, or a kill by the worker ends every task in it. Such a task takes the crash path, or `TASK_TIMEOUT` after the kill of a closed child. A task that must not share a process stays `ISOLATED`.

**Do not start threads that outlive the task.** A thread that a task starts and does not join keeps running beside the next tasks of the child.

## Recycling

A shared thread child is replaced when it reaches a limit of `SharedRecycle`, as a shared async child is:

- `after_per_seat`: after `after_per_seat.tasks × seats` tasks. The default is 100 tasks per seat.
- `above`: when its RSS, read after each task, reaches the limit. Its tasks share the process, so this bounds them together.

At a limit the child closes: it takes no new task, its running tasks finish, and then it stops and a new child starts. A child that does not exit 10 seconds after its stop frame is killed.

## Sizing

- For C code that releases the GIL, and for pure Python on a free-threaded build, use up to one seat per CPU in all the thread children of the machine. More seats added no throughput in the measurement above.
- For tasks that wait in sync I/O, the seats needed are about the rate of tasks times their mean duration.
- `seats` is at most 256. Each seat is an OS thread with a stack of its own: 8 MiB of address space by default on Linux with glibc, 512 KiB on macOS.

A new task goes to the open shared thread child with the fewest tasks in flight, so the tasks spread over the children. A crash or a kill then ends about the tasks in flight divided by the children.

## Connections

Each shared thread child opens one sync connection pool of `worker_child_pool_max_size` connections, as an isolated child does, whatever its seats. Its seat threads share that pool for the RUNNING update and the finalizing marker of each task, and a thread waits up to 30 seconds for a free connection. Each task holds a connection only for those two short writes, so a pool smaller than the seats is enough for most tasks. See [Connections](worker-profiles#connections).

## Limits

- A deadline does not stop a thread. The kill of the child does, when no task in it has time left.
- A task with a long timeout keeps a closed child, and the tasks past their deadline in it, alive until that timeout.
- C code that holds the GIL stops every thread of the child, also the threads that report deadlines. Up to 5 seconds past a deadline the report is late; past that the worker kills the child.

## Things to Avoid

**Don't mark pure-Python CPU work `SHARED_THREAD` on a standard build.** It gets the throughput of one CPU, whatever the seats. Keep it `ISOLATED`, or run a free-threaded build.

**Don't call a sync client without a timeout.** A call that never returns keeps the thread until the worker kills the child, with the other tasks in it.

**Don't mark `async def` code `SHARED_THREAD`.** The decorator refuses it (`HRS-108`). Use `SHARED_ASYNC`.

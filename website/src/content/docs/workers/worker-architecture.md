---
title: Worker Architecture
summary: Worker-owned child processes for claiming and executing tasks.
related: [concurrency, heartbeats-recovery, shared-async-tasks, shared-thread-tasks, ../../concepts/architecture]
tags: [workers, architecture, child-processes]
---

## Overview

<!-- todo:diagram-needed - Worker process architecture -->

```text
┌─────────────────────────────────────────────────────────┐
│  Worker Process (Main)                                  │
│  - Subscribes to NOTIFY channels                        │
│  - Claims tasks from PostgreSQL                         │
│  - Assigns tasks to child slots                         │
│  - Writes runner heartbeats (own thread, not the loop)  │
│  - Writes results back                                  │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │  Slots (one child, one socket each)             │    │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐            │    │
│  │  │ Child 1 │ │ Child 2 │ │ Child N │  ...       │    │
│  │  │ Task A  │ │ Task B  │ │ (idle)  │            │    │
│  │  └─────────┘ └─────────┘ └─────────┘            │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

## Process Model

A worker owns one child process per slot:

- **Main process**: Coordinates claiming, dispatching, result handling, and writes the runner heartbeats of the running tasks
- **Isolated children**: Execute task code in isolation, one task at a time. Set by `IsolatedChildren(count=N)` in the [worker profile](/horsies/workers/worker-profiles/)
- **Shared async children**: Execute up to `seats` `SHARED_ASYNC` tasks at once, as coroutines on one event loop. Set by `SharedAsyncChildren(count=N, seats=K)`. See [Shared Async Children](#shared-async-children)
- **Shared thread children**: Execute up to `seats` `SHARED_THREAD` tasks at once, each on a thread of its own. Set by `SharedThreadChildren(count=N, seats=K)`. See [Shared Thread Children](#shared-thread-children)

Each child:

- Starts from the [preload parent](#preload-parent) on Linux, and with `exec` on other platforms. It inherits no socket or lock from the worker.
- Has its own session, so a kill also reaches the subprocesses of its task.
- Has its own socket to the worker. Tasks and results move over it as JSON frames.

Benefits of isolated children:

- GIL doesn't block concurrent task execution
- A crash, a timeout, or an exit in one child affects only the task of that child
- Memory isolation between tasks

Shared children trade these for more tasks per process. See [Choosing an Execution Mode](/horsies/workers/execution-modes/).

## Failure Isolation

Each isolated child holds one task, so a failure stays with that task. A shared child holds the tasks of its seats, so a crash or a kill of it ends every task in it; see [Shared Async Children](#shared-async-children) and [Shared Thread Children](#shared-thread-children).

| Case | Result |
| ---- | ------ |
| A task exceeds `timeout_ms` | Its child and the subprocesses in its session are killed. After the child exits, the task fails with `TASK_TIMEOUT`. Other tasks continue |
| A child crashes (segfault, out-of-memory kill, `os._exit()`) | The worker sees the exit at once and recovers that task. Other tasks continue |
| The worker dies | Each child stops at once, also while C code holds the GIL. A subprocess that a task started keeps running, because the child cannot act after a SIGKILL |
| A supervisor sends SIGTERM to every process | Running tasks continue, and the worker drains them |
| A task forks a helper, then its child dies | The worker sees the death at once. The helper does not hide it |
| A timeout fires as a task ends | The kill is bound to that task, so it cannot reach the next task. The killed child takes no next task |
| An `async def` task returns while asyncio tasks that it started are still on the loop | The child waits up to 1 s for them, then cancels them in three rounds 0.1 s apart. The worker writes what the task returned after that work ended, and replaces the child. The deadline no longer applies. Work still alive after the rounds, or no report within 26.3 s of the return: the worker kills the child, and writes what the task returned after the exit. See [Async Tasks](/horsies/tasks/defining-tasks/#async-tasks) |
| A task holds the GIL in C code | Its runner heartbeats continue: the worker process writes them, so the reaper does not fail the task |
| The worker cannot write runner heartbeats for half of `running_stale_threshold_ms`, or its heartbeat thread dies | With `auto_fail_stale_running` on, the worker kills every child that holds a task, stops, and exits non-zero. With it off, the worker logs it and runs on. See [When the Worker Cannot Write Heartbeats](/horsies/workers/heartbeats-recovery/#when-the-worker-cannot-write-heartbeats) |

## Execution Flow

### 1. Startup

```bash
# In CLI
horsies worker myapp.instance:app --profile default
```

1. Load app from module
2. Import task modules (for registry population)
3. Start the preload parent (Linux)
4. Start N children (each loads app independently). A child whose app has an `async def` task creates its event loop before it reports ready
5. Wait until each child reports ready
6. Subscribe to NOTIFY channels

### 2. Main Loop

<!-- check: skip partial -- uses names that the page does not define -->
```python
while not stopped:
    await claim_and_dispatch_all()
    await wait_for_any_notify()
```

1. **Claim pass**: Query for available tasks, claim them
2. **Dispatch**: Assign claimed tasks to child slots, oldest first
3. **Wait**: Block on NOTIFY until new tasks arrive
4. Repeat

### 3. Claiming

One stream per queue and execution class, simplified:

```sql
WITH next AS (
  SELECT id FROM tasks
  WHERE queue_name = :queue
    AND execution = :class
    AND status = 'PENDING'
    AND enqueued_at <= now()
    AND (good_until IS NULL OR good_until > now())
  ORDER BY priority ASC, enqueued_at ASC, id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT :take
)
UPDATE tasks SET status='CLAIMED', claimed_at=now()
FROM next WHERE tasks.id = next.id
RETURNING tasks.id, tasks.execution
```

Key features:

- `FOR UPDATE SKIP LOCKED`: Non-blocking concurrent claiming
- One budget per execution class that the worker runs children for; see
  [Budgets per Execution Class](/horsies/workers/concurrency/#budgets-per-execution-class)
- Priority ordering within one class: Lower priority number = higher priority
- FIFO within same priority
- Expiry check: Skip tasks past `good_until`
- Class check: a claimed row of a shared class whose registered task has
  another class goes back to the queue as `ISOLATED`

### 4. Task Execution

In child process:

1. Set the row RUNNING, with the first runner heartbeat in the same transaction
2. Resolve task from registry by name
3. Deserialize arguments
4. Call task function. An `async def` task runs on the event loop that the child created at boot. The child keeps that loop until it stops, and the loop runs only while an async task runs
5. Serialize the result
6. Mark the row finalizing, and report the result. When the body of an `async def` task returned with asyncio tasks still on the loop, the child reports the return with the result, waits up to 1 second for that work, and cancels the work that is still alive. When that work has ended, it marks the row finalizing and reports how the work ended; work that did not stop gets no marker. The worker writes the result only after that report, and replaces the child

The child runs no heartbeat thread. While the child holds the task, a thread of the worker process writes its runner heartbeats. See [Heartbeats & Recovery](/horsies/workers/heartbeats-recovery/#runner-heartbeat).

### 5. Result Handling

Main process receives result from child:

1. Parse TaskResult (ok or err)
2. Check for retry eligibility
3. Update task status (COMPLETED or FAILED)
4. Send NOTIFY for result waiters

## Shared Async Children

A shared async child runs `SHARED_ASYNC` tasks of the claimed rows. It has four threads:

- The control thread reads the frames of the worker. It hands each frame to the event loop, so EOF still means the worker is gone, whatever the loop does.
- The main thread runs the event loop from the end of the start steps to the stop frame. A run frame starts its task at once on a free seat, as an asyncio task, while other tasks run.
- The writer thread writes the frames of the child. The loop queues a frame and returns, so a worker that reads slowly never stops the loop.
- The watchdog thread watches the event loop. When the loop has not turned for 5 seconds, it names the task that holds the loop, with the source line, and reports it to the worker through the writer thread.

The start steps, after the import of the app and the `on_child_process_start` hooks: the child opens an async connection pool on its loop, runs the [`on_shared_async_child_start` hooks](/horsies/workers/child-process-hooks/#shared-async-child-start-hooks), and sends its ready frame. Each task then runs on the loop: the RUNNING update with the first runner heartbeat, the task body, and the finalizing marker. Each write goes through the async pool, so no write of the child blocks the loop.

The worker places each `SHARED_ASYNC` task on the open shared child with the fewest tasks in flight and a free seat. Each result frame names its run, and a free seat takes the next queued task at once. The runner heartbeats of every task of the child come from the worker process, as for an isolated child.

| Case | Result |
| ---- | ------ |
| A task reaches `timeout_ms` | The child cancels it. When its coroutine and the work that it started have ended, the worker writes `TASK_TIMEOUT`, the retry and the next workflow step. Other tasks of the child continue |
| A task returns a value after its deadline fired | The outcome is `TASK_TIMEOUT` |
| A task is still alive `cancel_grace_ms` after its cancel | The child cancels it a second time, with the asyncio tasks that it started, in up to three rounds 0.1 s apart. The child takes no new task, and a new child replaces it when its tasks end |
| A task is still alive after the cancel rounds | The task is stuck and keeps its seat. The worker kills the child when every task left in it is stuck; each fails with `TASK_TIMEOUT`, and its retry runs in an isolated child |
| The event loop has not turned for 5 s | The worker kills the child at once. The task that held the loop fails with `TASK_BLOCKED_LOOP` and retries isolated; each other task before its deadline fails with `NEIGHBOUR_BLOCKED_LOOP`; a task past its deadline with `TASK_TIMEOUT` |
| The child reports neither the second cancel nor the end of a task by `timeout_ms + cancel_grace_ms + 10 s` | C code holds the GIL in the child. The worker kills it: each task past its deadline fails with `TASK_TIMEOUT`, and every other task takes the crash path. A task that the child reported stuck retries in an isolated child |
| The child reported the second cancel of a task, or the cancel of the work that a task left, then reports neither the task stuck nor its end by `timeout_ms + cancel_grace_ms + 20.3 s` | The child froze after its report. The worker kills it, with the same outcomes as in the row above |
| The child crashes | Every task of the child takes the crash path, after the worker saw the exit. A task that returned before its deadline, and whose left work the child reported stuck, ends with what it returned. A new child starts |
| The child reaches a recycle limit | It takes no new task, its tasks finish, then it stops and a new child starts |

At a stop the worker closes each shared child: it takes no new task, and it exits by stop frame when its tasks have ended. At the drain deadline the worker kills a shared child that still runs tasks: each of them past its deadline fails with `TASK_TIMEOUT`, and every other one takes the crash path. A task that the worker already named as the cause of a stuck stop (a task that a shared async child reported stuck, or the task whose deadline closed a shared thread child) retries in an isolated child at this kill too.

## Shared Thread Children

A shared thread child runs `SHARED_THREAD` tasks of the claimed rows. Its threads:

- The control thread reads the frames of the worker and hands each one to the main thread.
- The main thread starts the deadline of each run and hands the run to a free seat.
- `seats` seat threads, started at boot. Each one runs one task at a time, and takes the next run when its task has ended.
- The deadline thread reports each deadline at its time.
- The writer thread writes the frames of the child.

The child boots as an isolated child does: the import of the app, the `on_child_process_start` hooks, and one sync connection pool. Then it starts its threads and sends its ready frame. A refused thread fails the boot (`ThreadStartRefused`), and that cause can clear. Each task runs on its seat thread: the RUNNING update with the first runner heartbeat, the task body, and the finalizing marker. The seat threads share the sync pool of the child.

The worker places each `SHARED_THREAD` task on the open shared thread child with the fewest tasks in flight and a free seat. The runner heartbeats of every task of the child come from the worker process.

| Case | Result |
| ---- | ------ |
| A task reaches `timeout_ms` | The child reports the deadline, and the task runs on: a thread cannot be cancelled. The child closes: it takes no new task |
| No task in a closed child has time left | The worker kills the child with its session. After the exit, each task that was still in it fails with `TASK_TIMEOUT`, with the retry and the next workflow step; the task whose deadline closed the child retries in an isolated child. A new child starts |
| A task ends by itself after its deadline | The outcome is `TASK_TIMEOUT` |
| A task in the closed child still has time left | It runs to its end or to its own deadline |
| The child does not report a deadline by `timeout_ms + 10 s` | C code holds the GIL, and no thread of the child runs. The worker kills it: each task past its deadline fails with `TASK_TIMEOUT`, and every other task of the child takes the crash path. The task whose deadline closed the child, when one did, retries in an isolated child |
| The child crashes or is killed | Every task of the child takes the crash path, after the worker saw the exit. A new child starts |
| The child reaches a recycle limit | It takes no new task, its tasks finish, then it stops and a new child starts |

See [Shared Thread Tasks](/horsies/workers/shared-thread-tasks/).

## Child Process Initialization

Each child process:

1. Reads its settings from the worker over its socket. The database URL is not in `argv`, because `ps` shows `argv`.
2. Loads app via `_child_initializer()`
3. Imports task modules
4. Populates local task registry
5. Suppresses sends during import (prevents side effects)

<!-- check: skip partial -- uses names that the page does not define -->
```python
def _child_initializer(app_locator, imports):
    app = _locate_app(app_locator)
    set_current_app(app)
    app.suppress_sends(True)
    for module in imports:
        import_module(module)
    app.suppress_sends(False)
```

A child differs from a `multiprocessing` process:

- `multiprocessing.parent_process()` returns `None`.
- `multiprocessing.current_process().name` is `horsies-child-<slot>`.
- `__main__` is the child program, not your script. The worker refuses a `__main__:app` locator at startup.

## Preload Parent

On Linux, a worker starts one extra process: the preload parent. It imports code once, then forks each child. A child then starts in milliseconds and shares those memory pages with the other children.

`WorkerProfile.preload` selects what it imports:

| Mode | Imports | Use it when |
| ---- | ------- | ----------- |
| `library` (default) | Horsies and its dependencies | Always. Each child still imports your app |
| `app` | Horsies, its dependencies, and your app | Your app has no side effect at import |
| `none` | Nothing, and there is no preload parent | You must rule the preload parent out. Every child starts with `exec` |

Conditions: 8 children, Linux, tasks that run pydantic, SQLAlchemy, and pandas (pandas app only), Postgres in the loop. Memory is the summed PSS of the children and the preload parent.

| App | `preload` | Next child, p50 | Memory |
| --- | --------- | --------------- | ------ |
| Small | `none` | 618 ms | 461 MiB |
| Small | `library` | 27 ms | 169 MiB |
| Small | `app` | 20 ms | 151 MiB |
| With pandas | `none` | 1,174 ms | 715 MiB |
| With pandas | `library` | 486 ms | 472 MiB |
| With pandas | `app` | 26 ms | 236 MiB |

With `app`:

- Your app's module code runs once, in the preload parent, before any child exists. Put per-child setup in an [`on_child_process_start` hook](/horsies/workers/child-process-hooks/), which still runs in every child.
- The worker refuses to start if the import leaves a Python thread or a socket. A fork copies a lock in whatever state another thread left it, and all children would share one socket.

With 4 children, the whole worker at start, including its main process, is measured as summed PSS:

| App | `none` | `library` | `app` |
| --- | ------ | --------- | ----- |
| Small | 290 MiB | 159 MiB | 146 MiB |
| With pandas | 445 MiB | 342 MiB | 212 MiB |

Throughput is on the [performance](/horsies/internals/performance/#worker-start-paths) page.

The worker stays the real parent of each child, so exit codes, kills, and parent-death behavior do not change.

If the preload parent dies, running children continue, and new children start with `exec`. The worker replaces it after 5 seconds, up to 5 times. In `ps`, a forked child shows the command line of the preload parent.

Other platforms have no preload parent, and every child starts with `exec`. Linux also falls back to `exec` and logs a warning if the kernel has no `pidfd_open` (below 5.3) or blocks `prctl`.

### Choosing a Mode

Keep `library` unless memory is your limit. Most of the gain from `library` is Horsies and its dependencies, so an app with heavy imports gains little from it. With 4 children, an app that imports boto3, openai, google-genai, firebase-admin, and lxml uses 651 MiB on 0.4.6, 575 MiB with `library`, and 298 MiB with `app`.

Before you use `app` in production, run it on staging with real calls to every client library inside tasks. An import that passes the guard does not prove that a library works after a fork.

### Limits of Each Mode

Both preload modes:

| Limit | Effect |
| ----- | ------ |
| Library code loads before your app | Anything your app sets first arrives too late. See [Things to Avoid](#things-to-avoid) |
| A recycled child does not reload library code | After an in-place package upgrade, restart the worker |
| Copied pages grow as tasks run | Python reference counts write to shared pages, so memory rises after start |
| One more process | The preload parent uses about 20–30 MiB. The tables above include it |

`app` only:

| Limit | Effect |
| ----- | ------ |
| Your module code runs once | Anything built at import is shared by all children |
| A recycled child does not reload your code | New app code needs a worker restart. Recycling still returns memory |
| The guard does not see native threads | A BLAS pool or a gRPC client built at import passes the guard. It may hang or fail in a child |
| Native libraries may not be fork-safe | Build their clients in a hook, after the fork |

## Worker Configuration

`horsies worker` builds the internal `WorkerConfig` from the app config and the chosen [worker profile](/horsies/workers/worker-profiles/):

| Field | Source |
| ----- | ------ |
| `dsn` | `PostgresConfig.database_url` |
| `session_dsn` | `PostgresConfig.session_database_url`, or `database_url` |
| `pgbouncer_transaction_mode` | `PostgresConfig.pgbouncer_transaction_mode`. Disables child-process prepared statements for PgBouncer transaction pooling |
| `queues` | The queues of the app |
| `processes` | `IsolatedChildren.count`: the number of isolated children, 0 with `NoIsolatedChildren()` |
| `max_tasks_per_child` | `IsolatedRecycle.after` |
| `max_memory_per_child_mb` | `IsolatedRecycle.above` |
| `shared_async` | `WorkerProfile.shared_async` |
| `shared_thread` | `WorkerProfile.shared_thread` |
| `max_claim_batch` | `ClaimLimits.per_queue_per_pass` |
| `max_claim_per_worker` | `ClaimLimits.claimed_per_worker` |
| `drain_timeout_s` | `WorkerProfile.drain_timeout` |
| `child_preload` | `WorkerProfile.preload` |
| `parent_pool_size` | `PostgresConfig.worker_pool_size` |
| `parent_max_overflow` | `PostgresConfig.worker_max_overflow` |
| `isolated_pool` | `IsolatedChildren.pool`, resolved; the default `IsolatedPool()` with `NoIsolatedChildren()`. The pool of a shared child comes from `shared_async.pool` or `shared_thread.pool` |
| `app_locator` | The app locator of the command |
| `imports` | Task module paths |

Workers require PostgreSQL `LISTEN` for dispatch. When using transaction-pooled
PgBouncer, configure `PostgresConfig.session_database_url` with a direct or
session-capable URL. Workers fail startup instead of silently switching to
poll-only dispatch if `LISTEN` is unavailable.

Child database pools are initialized inside the child process. Psycopg
connections are not process-safe. A child starts with `exec`, so it inherits no
parent database socket. Child DB connections remain lazy by default: the `min_size` of each child
pool is 0. An isolated child uses one connection at a time: for the RUNNING
update, and for the finalizing mark. Its pool is `IsolatedChildren.pool`, 0 to
2 connections by default; see
[Connection Pools and the Default Executor](/horsies/workers/worker-profiles/#connection-pools-and-the-default-executor). The seats of a
shared child share the pool of the child for the same writes. See
[Connections](/horsies/workers/shared-thread-tasks/#connections). The worker process holds one more
connection, for the runner heartbeats.

## Graceful Shutdown

On SIGTERM/SIGINT the worker:

1. Stops new claims
2. Returns tasks that wait for a child or a seat to `PENDING`, because they have not started
3. Waits up to the `drain_timeout` of the profile for running tasks (default 300 seconds). A shared child takes no new task in this time
4. Kills the children that still run a task. Each of these tasks takes the [child crash](/horsies/workers/heartbeats-recovery/#child-process-crash) path; for a shared child, every task in it
5. Waits for pending result writes
6. Closes database connections

Set `drain_timeout` below the stop window of your supervisor, and leave a few seconds for result writes. If the supervisor kills the worker first, its running tasks wait for the reaper. At the deadline, the worker records each killed task at once. A task whose child does not exit within 30 seconds of the kill is left to stale-task recovery, or, with stale RUNNING recovery off, written when the worker sees the exit before it stops.

| Supervisor | Default stop window | Signal goes to |
| ---------- | ------------------- | -------------- |
| Fly.io | 5 s (`kill_timeout`), SIGINT | Main process |
| Docker | 10 s (`docker stop --time`) | Main process |
| Kubernetes | 30 s (`terminationGracePeriodSeconds`) | Main process |
| Amazon ECS | 30 s (`stopTimeout`) | Main process |
| Heroku | 30 s | Every process |
| systemd | 90 s (`TimeoutStopSec`) | Every process, or the main process with `KillMode=mixed` |


Children ignore SIGTERM and SIGINT. The worker decides when a child stops, so a task continues when a supervisor signals every process. A subprocess that a task starts does not ignore these signals. To keep such a subprocess running on systemd, set `KillMode=mixed`.

If the worker process dies, each child stops at once. A child that continues has no way to write its result, and a retry could overlap it. The reaper recovers these rows. See [Recovery Time After a Worker Stops](/horsies/workers/heartbeats-recovery/#recovery-time-after-a-worker-stops).

## Resilience

Workers recover from transient infrastructure failures without operator intervention. Configure via `WorkerResilienceConfig` in `AppConfig`.

### What It Handles

- **Database outages**: Worker retries on connection loss during startup, claiming, and result writes. No crash, no manual restart.
- **Silent NOTIFY channels**: If the PostgreSQL NOTIFY mechanism stops delivering (broken listener connection, dropped trigger), the worker falls back to periodic polling so tasks are still picked up.
- **Child loss**: If a child dies, the worker starts a new child in that slot. Other children continue. A task still in CLAIMED status is requeued back to PENDING. A task already in RUNNING status is recovered through the same retry policy used by the reaper: if `WORKER_CRASHED` is listed in `auto_retry_for` and retries remain, the task is scheduled for retry; otherwise it is marked FAILED with a `WORKER_CRASHED` result.
- **Child start failure**: The cause of the failure decides what the worker does. See [Child Start Failures](#child-start-failures).

### Child Start Failures

A child that cannot start reports the cause of the failure to the worker, before it exits. The worker sees the causes that the child cannot report: a spawn that the OS refuses, a child that dies with no report, and a child that is not ready in time.

| Cause | Class | What the worker does |
| ----- | ----- | -------------------- |
| The app or a task module does not import (`AppLoadFailed`) | Repeats | Counts an attempt |
| The child connection pool cannot be set up, for example a URL that does not parse (`ChildPoolSetupFailed`) | Repeats | Counts an attempt |
| The child dies with no report: a crash in native code, `os._exit()` in app code (`LostBeforeReady`) | Repeats | Counts an attempt |
| No ready report within 120 seconds (`NotReadyInTime`) | Repeats | Counts an attempt |
| The child cannot create its event loop for `async def` tasks for a cause other than the errno values below, for example an event loop policy that raises (`TaskLoopCreateFailed`) | Repeats | Counts an attempt |
| The OS refuses to start the child process with another errno, for example `ENOENT` or `EACCES` for the interpreter, or a forked child does not reach the worker in 5 seconds (`SpawnFailed`) | Repeats | Counts an attempt |
| An `on_child_process_start` [hook](/horsies/workers/child-process-hooks/) fails | Repeats | Stops the worker at once |
| A start step of the child or of the worker (the socket pair, the spawn, the event loop) fails with `OSError` `EMFILE`, `ENFILE`, `ENOMEM` or `EAGAIN` (`ResourceShortage`) | Can clear | Takes the slot down |
| The OS refuses a thread that the child starts at boot: the control thread, the watchdog of a start hook, a thread of the connection pool (`ThreadStartRefused`) | Can clear | Takes the slot down |

A child that dies with no report names no cause, so the worker counts it as a cause that repeats. The worker cannot tell a crash that clears from a crash in the code. Counting it stops a worker whose code crashes at every start, instead of keeping a slot down with no end.

- After three counted attempts in a row, the worker exits non-zero, so that its supervisor restarts it. This also applies when other children serve tasks.
- A slot that is down takes no task, and the claim budget leaves it out: a worker with 4 processes and 1 slot down claims as a worker with 3 processes. The worker starts a new child for that slot after 1 second, then 2, 4, up to 60 seconds between attempts. Each failed attempt logs one line with the slot, the cause, and the next wait. The slot returns to service when a child starts.
- A cause that can clear counts an attempt only when every other slot is down too. A worker that serves no task must fail where its supervisor can see it. A failed attempt that does not count, because another slot is in service, starts the count again. A worker with 1 process has no other slot, so each of its failed attempts counts.
- At the first start, the worker starts when each slot has a ready child or is down, and at least one child is ready. Its [worker state](/horsies/monitoring/worker-health/#which-slots-are-down) names the slots that are down. When no child starts, the worker exits non-zero.
- An outside SIGKILL of a booting replacement child, such as an out-of-memory kill, does not count an attempt while another child is serving: the worker waits up to 5 seconds and starts another. A kill during the first start counts, and so does a kill when no other child is serving.

### Configuration

```python
from horsies import WorkerResilienceConfig

config = AppConfig(
    broker=broker,
    resilience=WorkerResilienceConfig(
        db_retry_initial_ms=500,      # initial backoff on DB errors
        db_retry_max_ms=30_000,       # backoff cap
        db_retry_max_attempts=0,      # 0 = retry forever
        notify_poll_interval_ms=5_000, # poll fallback when NOTIFY is silent
    ),
)
```

| Field | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `db_retry_initial_ms` | `int` | `500` | Initial backoff delay (100–60,000ms) |
| `db_retry_max_ms` | `int` | `30000` | Maximum backoff delay (500–300,000ms) |
| `db_retry_max_attempts` | `int` | `0` | Max retry attempts; `0` = infinite (0–10,000) |
| `notify_poll_interval_ms` | `int` | `5000` | Poll interval when NOTIFY is silent (1,000–300,000ms) |

Backoff uses exponential growth (`initial_ms * 2^attempt`) with ±25% jitter, capped at `db_retry_max_ms`. Backoff resets after each successful loop iteration.

## Module Discovery

Workers need to know where tasks are defined:

```python
# In instance.py
# Dotted module paths (recommended)
app.discover_tasks(["myapp.tasks", "myapp.jobs.worker_tasks"])

# Or file paths
app.discover_tasks(["tasks.py", "more_tasks.py"])
```

This list is passed to child processes for import.

## Error Handling

| Error Type | Handling |
| ---------- | -------- |
| Task exception | Wrapped in TaskResult(err=...) |
| Serialization error | WORKER_SERIALIZATION_ERROR |
| Task not found | WORKER_RESOLUTION_ERROR |
| Child crash | Detected at once, `WORKER_CRASHED` for that task only |

## Multiple Workers

Run multiple worker processes for horizontal scaling:

```bash
# On machine 1
horsies worker myapp.instance:app --profile default

# On machine 2
horsies worker myapp.instance:app --profile default
```

- Each worker claims independently
- `SKIP LOCKED` prevents duplicate claims
- Advisory locks serialize certain operations
- `cluster_wide_cap` limits total concurrency

## Things to Avoid

**Don't build a client at import with `preload=ChildPreload.APP`.** The client exists before the fork, and a client with native threads, such as gRPC, can hang in a child.

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Wrong: this runs once, in the preload parent
client = firestore.Client()

# Correct: this runs in every child, after the fork
client = None

@app.on_child_process_start
def build_client() -> None:
    global client
    client = firestore.Client()
```

**Don't set a library's import-time setting in `.env` that your app loads.** `library` and `app` both import psycopg, SQLAlchemy, and pydantic before your app runs, so `load_dotenv()` in your app is too late for `PSYCOPG_IMPL`. Set such a value in the process environment.

```bash
# Wrong: in a .env file that the app loads
PSYCOPG_IMPL=binary

# Correct: in the environment of the worker process
PSYCOPG_IMPL=binary horsies worker myapp.instance:app --profile default
```

Monkeypatching has the same limit: a patch that must run before those imports, such as gevent, is too late. Use `preload=ChildPreload.NONE`.

**Don't expect a recycle to load new code with `preload=ChildPreload.APP`.** A recycled child is a new fork of the same preload parent. Restart the worker.

**Don't count children by process name.** A forked child shows the command line of the preload parent, and there is one more process than the profile has children. Read `processes` in [worker health](/horsies/monitoring/worker-health/).

**Don't add `children_memory_mb` to get the real footprint.** It sums RSS, so with preload it counts each shared page once per child. Read the container or cgroup total.


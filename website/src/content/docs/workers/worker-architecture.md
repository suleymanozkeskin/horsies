---
title: Worker Architecture
summary: Worker-owned child processes for claiming and executing tasks.
related: [concurrency, heartbeats-recovery, ../../concepts/architecture]
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

- **Main process**: Coordinates claiming, dispatching, result handling
- **Child processes**: Execute task code in isolation, one task at a time
- **N children**: Configurable via `--processes` flag

Each child:

- Starts from the [preload parent](#preload-parent) on Linux, and with `exec` on other platforms. It inherits no socket or lock from the worker.
- Has its own session, so a kill also reaches the subprocesses of its task.
- Has its own socket to the worker. Tasks and results move over it as JSON frames.

Benefits:

- GIL doesn't block concurrent task execution
- A crash, a timeout, or an exit in one child affects only the task of that child
- Memory isolation between tasks

## Failure Isolation

Each child holds one task, so a failure stays with that task.

| Case | Result |
| ---- | ------ |
| A task exceeds `timeout_ms` | Its child and the subprocesses in its session are killed, and the task fails with `TASK_TIMEOUT`. Other tasks continue |
| A child crashes (segfault, out-of-memory kill, `os._exit()`) | The worker sees the exit at once and recovers that task. Other tasks continue |
| The worker dies | Each child stops at once, also while C code holds the GIL |
| A supervisor sends SIGTERM to every process | Running tasks continue, and the worker drains them |
| A task forks a helper, then its child dies | The worker sees the death at once. The helper does not hide it |
| A timeout fires as a task ends | The kill is bound to that task, so it cannot reach the next task |

## Execution Flow

### 1. Startup

```bash
# In CLI
horsies worker myapp.instance:app --processes=8
```

1. Load app from module
2. Import task modules (for registry population)
3. Start the preload parent (Linux)
4. Start N children (each loads app independently)
5. Wait until each child reports ready
6. Subscribe to NOTIFY channels

### 2. Main Loop

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

```sql
WITH next AS (
  SELECT id FROM tasks
  WHERE queue_name = :queue
    AND status = 'PENDING'
    AND enqueued_at <= now()
    AND (good_until IS NULL OR good_until > now())
  ORDER BY priority ASC, enqueued_at ASC
  FOR UPDATE SKIP LOCKED
  LIMIT :limit
)
UPDATE tasks SET status='CLAIMED', claimed_at=now()
FROM next WHERE tasks.id = next.id
RETURNING tasks.id
```

Key features:

- `FOR UPDATE SKIP LOCKED`: Non-blocking concurrent claiming
- Priority ordering: Lower priority number = higher priority
- FIFO within same priority
- Expiry check: Skip tasks past `good_until`

### 4. Task Execution

In child process:

1. Resolve task from registry by name
2. Deserialize arguments
3. Start heartbeat thread
4. Call task function
5. Serialize result
6. Stop heartbeat thread

### 5. Result Handling

Main process receives result from child:

1. Parse TaskResult (ok or err)
2. Check for retry eligibility
3. Update task status (COMPLETED or FAILED)
4. Send NOTIFY for result waiters

## Child Process Initialization

Each child process:

1. Reads its settings from the worker over its socket. The database URL is not in `argv`, because `ps` shows `argv`.
2. Loads app via `_child_initializer()`
3. Imports task modules
4. Populates local task registry
5. Suppresses sends during import (prevents side effects)

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

`--child-preload` selects what it imports:

| Mode | Imports | Use it when |
| ---- | ------- | ----------- |
| `library` (default) | Horsies and its dependencies | Always. Each child still imports your app |
| `app` | Horsies, its dependencies, and your app | Your app has no side effect at import |
| `none` | Nothing, and there is no preload parent | You must rule the preload parent out. Every child starts with `exec` |

Conditions: 8 children, Linux, tasks that run pydantic, SQLAlchemy, and pandas (pandas app only), Postgres in the loop. Memory is the summed PSS of the children and the preload parent.

| App | `--child-preload` | Next child, p50 | Memory |
| --- | ----------------- | --------------- | ------ |
| Small | `none` | 618 ms | 461 MiB |
| Small | `library` | 27 ms | 169 MiB |
| Small | `app` | 20 ms | 151 MiB |
| With pandas | `none` | 1,174 ms | 715 MiB |
| With pandas | `library` | 486 ms | 472 MiB |
| With pandas | `app` | 26 ms | 236 MiB |

With `app`:

- Your app's module code runs once, in the preload parent, before any child exists. Put per-child setup in an [`on_child_process_start` hook](child-process-hooks), which still runs in every child.
- The worker refuses to start if the import leaves a Python thread or a socket. A fork copies a lock in whatever state another thread left it, and all children would share one socket.

With 4 children, the whole worker at start, including its main process, is measured as summed PSS:

| App | `none` | `library` | `app` |
| --- | ------ | --------- | ----- |
| Small | 290 MiB | 159 MiB | 146 MiB |
| With pandas | 445 MiB | 342 MiB | 212 MiB |

Throughput is on the [performance](../internals/performance#worker-start-paths) page.

The worker stays the real parent of each child, so exit codes, kills, and parent-death behavior do not change.

If the preload parent dies, running children continue, and new children start with `exec`. The worker replaces it after 5 seconds, up to 5 times. In `ps`, a forked child shows the command line of the preload parent.

Other platforms have no preload parent, and every child starts with `exec`.

## Worker Configuration

From `WorkerConfig`:

| Field | Description |
| ----- | ----------- |
| `dsn` | SQLAlchemy database URL |
| `session_dsn` | Session-capable URL used for listener-capable operations |
| `pgbouncer_transaction_mode` | Disables child-process prepared statements for PgBouncer transaction pooling |
| `queues` | Queue names to process |
| `processes` | Child process count |
| `drain_timeout_s` | Seconds to wait for running tasks at shutdown |
| `child_preload` | What the preload parent imports: `library`, `app`, or `none` |
| `parent_pool_size` | Worker coordinator SQLAlchemy pool size |
| `parent_max_overflow` | Worker coordinator overflow connections |
| `child_pool_min_size` | Minimum per-child psycopg pool size |
| `child_pool_max_size` | Maximum per-child psycopg pool size |
| `max_claim_batch` | Max claims per queue per pass; `0` means auto-fill available capacity |
| `max_claim_per_worker` | Total claim limit |
| `app_locator` | Path to app instance |
| `imports` | Task module paths |

Workers require PostgreSQL `LISTEN` for dispatch. When using transaction-pooled
PgBouncer, configure `PostgresConfig.session_database_url` with a direct or
session-capable URL. Workers fail startup instead of silently switching to
poll-only dispatch if `LISTEN` is unavailable.

Child database pools are initialized inside the child process. Psycopg
connections are not process-safe. A child starts with `exec`, so it inherits no
parent database socket. Child DB connections remain lazy by default when
`worker_child_pool_min_size=0`.

## Graceful Shutdown

On SIGTERM/SIGINT the worker:

1. Stops new claims
2. Returns tasks that wait for a child to `PENDING`, because they have not started
3. Waits up to `--drain-timeout` seconds for running tasks (default 300)
4. Kills the children that still run a task. Each of these tasks takes the [child crash](heartbeats-recovery#child-process-crash) path
5. Waits for pending result writes
6. Closes database connections

Set `--drain-timeout` below the stop window of your supervisor, and leave a few seconds for result writes. If the supervisor kills the worker first, its running tasks wait for the reaper. At the deadline, the worker records each killed task at once.

| Supervisor | Default stop window | Signal goes to |
| ---------- | ------------------- | -------------- |
| Fly.io | 5 s (`kill_timeout`), SIGINT | Main process |
| Docker | 10 s (`docker stop --time`) | Main process |
| Kubernetes | 30 s (`terminationGracePeriodSeconds`) | Main process |
| Amazon ECS | 30 s (`stopTimeout`) | Main process |
| Heroku | 30 s | Every process |
| systemd | 90 s (`TimeoutStopSec`) | Every process, or the main process with `KillMode=mixed` |


Children ignore SIGTERM and SIGINT. The worker decides when a child stops, so a task continues when a supervisor signals every process. A subprocess that a task starts does not ignore these signals. To keep such a subprocess running on systemd, set `KillMode=mixed`.

If the worker process dies, each child stops at once. A child that continues has no way to write its result, and a retry could overlap it. The reaper recovers these rows. See [Recovery Time After a Worker Stops](heartbeats-recovery#recovery-time-after-a-worker-stops).

## Resilience

Workers recover from transient infrastructure failures without operator intervention. Configure via `WorkerResilienceConfig` in `AppConfig`.

### What It Handles

- **Database outages**: Worker retries on connection loss during startup, claiming, and result writes. No crash, no manual restart.
- **Silent NOTIFY channels**: If the PostgreSQL NOTIFY mechanism stops delivering (broken listener connection, dropped trigger), the worker falls back to periodic polling so tasks are still picked up.
- **Child loss**: If a child dies, the worker starts a new child in that slot. Other children continue. A task still in CLAIMED status is requeued back to PENDING. A task already in RUNNING status is recovered through the same retry policy used by the reaper: if `WORKER_CRASHED` is listed in `auto_retry_for` and retries remain, the task is scheduled for retry; otherwise it is marked FAILED with a `WORKER_CRASHED` result.
- **Child start failure**: A child that fails to start gets three attempts. If they all fail, or the OS refuses to start a process, the worker exits non-zero so that a supervisor restarts it. A failed [child hook](child-process-hooks) stops the worker at once, because a retry runs the same hook.

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
horsies worker myapp.instance:app --processes=8

# On machine 2
horsies worker myapp.instance:app --processes=8
```

- Each worker claims independently
- `SKIP LOCKED` prevents duplicate claims
- Advisory locks serialize certain operations
- `cluster_wide_cap` limits total concurrency

---
title: Worker Architecture
summary: Process pool model for claiming and executing tasks.
related: [concurrency, heartbeats-recovery, ../../concepts/architecture]
tags: [workers, architecture, process-pool]
---

## Overview

<!-- todo:diagram-needed - Worker process architecture -->

```text
┌─────────────────────────────────────────────────────────┐
│  Worker Process (Main)                                  │
│  - Subscribes to NOTIFY channels                        │
│  - Claims tasks from PostgreSQL                         │
│  - Dispatches to process pool                           │
│  - Writes results back                                  │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │  ProcessPoolExecutor                            │    │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐            │    │
│  │  │ Child 1 │ │ Child 2 │ │ Child N │  ...       │    │
│  │  │ Task A  │ │ Task B  │ │ (idle)  │            │    │
│  │  └─────────┘ └─────────┘ └─────────┘            │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

## Process Model

Workers use Python's `ProcessPoolExecutor`:

- **Main process**: Coordinates claiming, dispatching, result handling
- **Child processes**: Execute task code in isolation
- **N workers**: Configurable via `--processes` flag

Benefits:

- GIL doesn't block concurrent task execution
- Crashed tasks don't kill the main worker
- Memory isolation between tasks

## Execution Flow

### 1. Startup

```bash
# In CLI
horsies worker myapp.instance:app --processes=8
```

1. Load app from module
2. Import task modules (for registry population)
3. Create ProcessPoolExecutor with N workers
4. Initialize child processes (each loads app independently)
5. Subscribe to NOTIFY channels

### 2. Main Loop

```python
while not stopped:
    await claim_and_dispatch_all()
    await wait_for_any_notify()
```

1. **Claim pass**: Query for available tasks, claim them
2. **Dispatch**: Submit claimed tasks to process pool
3. **Wait**: Block on NOTIFY until new tasks arrive
4. Repeat

### 3. Claiming

```sql
WITH next AS (
  SELECT id FROM tasks
  WHERE queue_name = :queue
    AND status = 'PENDING'
    AND sent_at <= now()
    AND (good_until IS NULL OR good_until > now())
  ORDER BY priority ASC, sent_at ASC
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

1. Loads app via `_child_initializer()`
2. Imports task modules
3. Populates local task registry
4. Suppresses sends during import (prevents side effects)

```python
def _child_initializer(app_locator, imports):
    app = _locate_app(app_locator)
    set_current_app(app)
    app.suppress_sends(True)
    for module in imports:
        import_module(module)
    app.suppress_sends(False)
```

## Worker Configuration

From `WorkerConfig`:

| Field | Description |
| ----- | ----------- |
| `dsn` | SQLAlchemy database URL |
| `queues` | Queue names to process |
| `processes` | Child process count |
| `max_claim_batch` | Max claims per queue per pass |
| `max_claim_per_worker` | Total claim limit |
| `app_locator` | Path to app instance |
| `imports` | Task module paths |

## Graceful Shutdown

On SIGTERM/SIGINT:

1. Stop accepting new claims
2. Wait for running tasks to complete
3. Close process pool
4. Close database connections

```python
async def stop(self):
    self._stop.set()
    await self.listener.close()
    self._executor.shutdown(wait=True)
```

## Resilience

Workers recover from transient infrastructure failures without operator intervention. Configure via `WorkerResilienceConfig` in `AppConfig`.

### What It Handles

- **Database outages**: Worker retries on connection loss during startup, claiming, and result writes. No crash, no manual restart.
- **Silent NOTIFY channels**: If the PostgreSQL NOTIFY mechanism stops delivering (broken listener connection, dropped trigger), the worker falls back to periodic polling so tasks are still picked up.
- **Broken process pool**: If child processes crash, the process pool is recreated. Tasks still in CLAIMED status are requeued back to PENDING. Tasks already in RUNNING status are eventually marked FAILED by the [recovery reaper](../heartbeats-recovery) and only retried if the task has `max_retries > 0`.

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
| Child crash | Detected via heartbeat, marked FAILED |

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

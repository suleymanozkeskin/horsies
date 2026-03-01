---
title: Task Lifecycle
summary: Task states and transitions from submission to completion.
related: [result-handling, ../../workers/heartbeats-recovery, ../../tasks/retry-policy]
tags: [concepts, tasks, states]
---

## Task States

| State | Description |
|-------|-------------|
| `PENDING` | Task is queued, waiting to be claimed |
| `CLAIMED` | Worker has claimed the task, preparing to execute |
| `RUNNING` | Task is actively executing in a worker process |
| `COMPLETED` | Task finished successfully |
| `FAILED` | Task failed (error returned or exception) |
| `CANCELLED` | Task was cancelled before execution |
| `REQUEUED` | Task was requeued after failure (for retry) |

**Terminal states:** `COMPLETED`, `FAILED`, `CANCELLED`.
`REQUEUED` is transitional and returns the task to `PENDING`.

## Status Enum

`TaskStatus` values:

| Enum | Value | Terminal |
|------|-------|----------|
| `PENDING` | `"PENDING"` | No |
| `CLAIMED` | `"CLAIMED"` | No |
| `RUNNING` | `"RUNNING"` | No |
| `COMPLETED` | `"COMPLETED"` | Yes |
| `FAILED` | `"FAILED"` | Yes |
| `CANCELLED` | `"CANCELLED"` | Yes |
| `REQUEUED` | `"REQUEUED"` | No |

Use `is_terminal` or `TASK_TERMINAL_STATES` to check terminal status programmatically:

```python
from horsies import TaskStatus, TASK_TERMINAL_STATES

status = TaskStatus.FAILED
status.is_terminal  # True

TaskStatus.RUNNING.is_terminal  # False

# Frozenset for use in queries or filters
TASK_TERMINAL_STATES  # frozenset({TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED})
```

## State Transitions

<!-- todo:diagram-needed - State machine diagram -->

```
                    ┌──────────────┐
                    │   PENDING    │
                    └──────┬───────┘
                           │ Worker claims
                           ▼
                    ┌──────────────┐
         timeout    │   CLAIMED    │
        (requeue)◄──┤              │
                    └──────┬───────┘
                           │ Execution starts
                           ▼
                    ┌──────────────┐
                    │   RUNNING    │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
       ┌──────────┐ ┌──────────┐ ┌──────────┐
       │COMPLETED │ │  FAILED  │ │ REQUEUED │
       └──────────┘ └──────────┘ └────┬─────┘
                                      │
                                      ▼
                               ┌──────────────┐
                               │   PENDING    │
                               └──────────────┘
```

## Transition Details

### PENDING → CLAIMED

- Worker executes claim query with `FOR UPDATE SKIP LOCKED`
- Sets `claimed=TRUE`, `claimed_at=NOW()`, `claimed_by_worker_id`
- Task is reserved for this worker

### CLAIMED → RUNNING

- Task dispatched to worker's process pool
- Child process sets `status=RUNNING`, `started_at=NOW()`
- Heartbeat thread begins sending runner heartbeats

### RUNNING → COMPLETED

- Task returns `TaskResult(ok=value)`
- Worker stores serialized result
- Sets `completed_at=NOW()`

`COMPLETED` means the task succeeded (returned `TaskResult.ok`). Execution that ends with `TaskResult.err` or an exception is `FAILED`, not `COMPLETED`.

### RUNNING → FAILED

- Task returns `TaskResult(err=TaskError(...))` **or**
- Task raises unhandled exception **or**
- Worker process crashes (detected via missing heartbeats)
- Sets `failed_at=NOW()`, stores error in `result`

### FAILED → PENDING (via REQUEUED)

- Only if retry policy configured and retries remaining
- Sets `status=PENDING`, increments `retry_count`
- Sets `next_retry_at` based on retry policy intervals

### CLAIMED → PENDING (stale recovery)

- Claimer heartbeat missing for `claimed_stale_threshold_ms`
- Reaper automatically requeues (safe - user code never ran)
- Sets `claimed=FALSE`, `claimed_at=NULL`

## Timestamps

Each task records timing information:

| Field | Set When |
|-------|----------|
| `sent_at` | Immutable call-site timestamp — when `.send()` or `.schedule()` was called |
| `enqueued_at` | When task becomes eligible for claiming (updated on retry) |
| `claimed_at` | Worker claims task |
| `started_at` | Execution begins in child process |
| `completed_at` | Successful completion |
| `failed_at` | Failure |
| `next_retry_at` | Scheduled retry time |

## Heartbeats

Two types of heartbeats track task health:

1. **Claimer heartbeat**: Worker sends for CLAIMED tasks (task not yet running)
2. **Runner heartbeat**: Child process sends for RUNNING tasks

Missing heartbeats trigger automatic recovery. See [Heartbeats & Recovery](../../workers/heartbeats-recovery).

## Task Expiry

Tasks can have a `good_until` deadline:

- If task isn't claimed before `good_until`, it's skipped
- Expired tasks remain in PENDING but are never claimed
- Useful for time-sensitive operations

```python
from datetime import datetime, timedelta, timezone

deadline = datetime.now(timezone.utc) + timedelta(minutes=5)

@app.task("urgent_task", good_until=deadline)
def urgent_task() -> TaskResult[str, TaskError]:
    ...
```

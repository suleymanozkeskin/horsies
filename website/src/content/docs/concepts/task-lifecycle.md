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
| `EXPIRED` | Task's `good_until` deadline passed before execution started |

**Terminal states:** `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`.

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
| `EXPIRED` | `"EXPIRED"` | Yes |

Use `is_terminal` or `TASK_TERMINAL_STATES` to check terminal status programmatically:

```python
from horsies import TaskStatus, TASK_TERMINAL_STATES

status = TaskStatus.FAILED
status.is_terminal  # True

TaskStatus.RUNNING.is_terminal  # False

# Frozenset for use in queries or filters
TASK_TERMINAL_STATES  # frozenset({TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.EXPIRED})
```

## State Transitions

<!-- todo:diagram-needed - State machine diagram -->

```
                    ┌──────────────┐
              ┌─────│   PENDING    │─────┐
              │     └──────┬───────┘     │
              │            │ Worker      │ good_until
              │            │ claims      │ passed
              │            ▼             ▼
              │     ┌──────────────┐ ┌──────────┐
    timeout   │     │   CLAIMED    │►│ EXPIRED  │
   (requeue)◄─┼─────┤              │ └──────────┘
              │     └──────┬───────┘  good_until
              │            │ Execution  passed
              │            │ starts
              │            ▼
              │     ┌──────────────┐
              │     │   RUNNING    │
              │     └──────┬───────┘
              │            │
              │ ┌──────────┼────────────┐
              │ │          │            │
              │ ▼          ▼            ▼
              │ ┌──────────┐ ┌──────────┐  (retry)
              │ │COMPLETED │ │  FAILED  │────┐
              │ └──────────┘ └──────────┘    │
              │                              │
              └──────────────────────────────┘
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
- Non-workflow tasks skip workflow-status preflight work; workflow tasks update
  the workflow task row in the same start transaction

### RUNNING → COMPLETED

- Task returns `TaskResult(ok=value)`
- Child marks the row with `finalizing_at` as it exits user code
- Worker stores serialized result
- Sets `completed_at=NOW()`

`COMPLETED` means the task succeeded (returned `TaskResult.ok`). Execution that ends with `TaskResult.err` or an exception is `FAILED`, not `COMPLETED`.

### RUNNING → FAILED

- Task returns `TaskResult(err=TaskError(...))` **or**
- Task raises unhandled exception **or**
- Worker process crashes (detected via missing heartbeats)
- Sets `failed_at=NOW()`, stores error in `result`
- Worker/process failures synthesize a `TaskResult(err=TaskError(...))` where
  possible so downstream workflow completion and capacity notifications still run

### RUNNING → PENDING (retry)

- Only if retry policy configured and retries remaining
- Sets `status=PENDING`, increments `retry_count`
- Sets `next_retry_at` based on retry policy intervals

### PENDING → EXPIRED

- Task has `good_until` set and deadline has passed without being claimed
- Reaper transitions to EXPIRED with `TASK_EXPIRED` outcome code
- No attempt row is written (task never started)
- The claim SQL also filters out expired tasks, preventing new claims after the deadline

### CLAIMED → EXPIRED

- Task was claimed by a worker but `good_until` passed before execution started
- Detected at two points: the preflight check and the ownership confirmation (`CLAIMED → RUNNING` transition)
- Worker marks task EXPIRED with `TASK_EXPIRED` outcome code and a result payload containing `task_id` and `worker_id`
- Claim metadata (`claimed_by_worker_id`, `claimed_at`) is preserved for forensics
- No attempt row is written (user code never ran)
- Takes precedence over workflow PAUSED/CANCELLED handling — an expired task is not put back to PENDING

### CLAIMED → PENDING (stale recovery)

- Claimer heartbeat missing for `claimed_stale_threshold_ms`
- Reaper automatically requeues (safe - user code never ran)
- Sets `claimed=FALSE`, `claimed_at=NULL`

## Timestamps

Each task records timing information:

| Field | Set When |
|-------|----------|
| `sent_at` | Immutable call-site timestamp -- when `.send()` or `.schedule()` was called. Captured inside the `TaskSendResult` flow before enqueue. |
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

The runner stale threshold is heartbeat-based, not duration-based. A long task
can run past `running_stale_threshold_ms` as long as its child process continues
sending runner heartbeats.

## Task Expiry

Tasks can have a `good_until` deadline set at send time via `.with_options()`:

- **PENDING expiry:** If the task isn't claimed before `good_until`, the claim SQL skips it. The reaper periodically transitions unclaimed expired tasks to `EXPIRED`.
- **CLAIMED expiry:** If the task was claimed but `good_until` passes before execution starts, the worker marks it `EXPIRED` directly instead of proceeding to `RUNNING`.
- Both paths produce a `TASK_EXPIRED` outcome code. No attempt row is written since user code never ran.
- `good_until` also caps retries — a retry scheduled at or past the deadline is rejected.

```python
from datetime import datetime, timedelta, timezone

@app.task("urgent_task")
def urgent_task() -> TaskResult[str, TaskError]:
    ...

# Compute deadline at send time, not module load time
deadline = datetime.now(timezone.utc) + timedelta(minutes=5)
urgent_task.with_options(good_until=deadline).send()
```

`good_until` must be timezone-aware. For workflow nodes, use `.node(good_until=...)` instead — see [Typed Node Builder](../../concepts/workflows/typed-node-builder).

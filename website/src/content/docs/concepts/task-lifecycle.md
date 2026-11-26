---
title: Task Lifecycle
section: shared
summary: Task states and transitions from submission to completion.
related: [result-handling, fencing-token, ../../workers/heartbeats-recovery, ../../tasks/retry-policy]
tags: [concepts, tasks, states]
---

## Task States

<!-- members: TaskStatus -->
| State | Description |
|-------|-------------|
| `PENDING` | Task is queued, waiting to be claimed |
| `CLAIMED` | Worker has claimed the task, preparing to execute |
| `RUNNING` | Task is actively executing in a worker process |
| `COMPLETED` | Task finished successfully |
| `FAILED` | Task failed (error returned or exception) |
| `CANCELLED` | An operator cancelled the task: before it ran, or while it ran when the cancel asked for it |
| `EXPIRED` | Task's `good_until` deadline passed before execution started |

**Terminal states:** `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`.

## Status Enum

`TaskStatus` values:

<!-- members: TaskStatus -->
| Enum | Value | Terminal |
|------|-------|----------|
| `PENDING` | `"PENDING"` | No |
| `CLAIMED` | `"CLAIMED"` | No |
| `RUNNING` | `"RUNNING"` | No |
| `COMPLETED` | `"COMPLETED"` | Yes |
| `FAILED` | `"FAILED"` | Yes |
| `CANCELLED` | `"CANCELLED"` | Yes |
| `EXPIRED` | `"EXPIRED"` | Yes |

The Python client checks a status with `is_terminal` and holds the terminal statuses in `TASK_TERMINAL_STATES`: see [Terminal Statuses](/horsies/tasks/retrieving-results/#terminal-statuses) (Python).

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

Not drawn: an operator cancel moves a `PENDING` or `CLAIMED` task to
`CANCELLED`, and a `RUNNING` task when the cancel asks for it.

## Transition Details

### PENDING → CLAIMED

- Worker executes claim query with `FOR UPDATE SKIP LOCKED`
- Sets `claimed=TRUE`, `claimed_at=NOW()`, `claimed_by_worker_id`
- Task is reserved for this worker

### CLAIMED → RUNNING

- Task dispatched to one of the worker's child processes
- Child process sets `status=RUNNING`, `started_at=NOW()`
- The start writes no runner heartbeat. The worker process writes them while
  the child holds the task; until the first one, the reaper counts from
  `started_at`
- Non-workflow tasks skip workflow-status preflight work; workflow tasks update
  the workflow task row in the same start transaction

### RUNNING → COMPLETED

- Task returns `Ok(value)`
- Child marks the row with `finalizing_at` as it exits user code
- Worker stores serialized result
- Sets `completed_at=NOW()`

`COMPLETED` means the task succeeded (returned `Ok`). Execution that ends with `Err` or an exception is `FAILED`, not `COMPLETED`.

### RUNNING → FAILED

- Task returns `Err(TaskError(...))` **or**
- Task raises unhandled exception **or**
- A child process died (the worker sees it at once), or the worker died (detected via missing heartbeats)
- Sets `failed_at=NOW()`, stores error in `result`
- Worker/process failures synthesize an error result (`TaskError`) where
  possible so downstream workflow completion and capacity notifications still run

### PENDING or CLAIMED → CANCELLED

- An operator cancel: `cancel_task`, or `POST /api/tasks/{id}/cancel`
- The task moves to history with the error code `TASK_CANCELLED`
- No attempt row is written: no task code ran. The child of a claimed task
  stops at its start write with `CLAIM_LOST`

### RUNNING → CANCELLED

- An operator cancel with the policy `INCLUDE_RUNNING`
- One statement writes the attempt row with outcome `CANCELLED` and moves the
  task to history
- The worker that holds the run gives the cancel to the task code. The code
  stops at its next check; what it returns after the cancel is dropped
- See [Action Semantics](/horsies/monitoring/action-semantics/#cancelling-a-running-task)

### RUNNING → PENDING (retry)

- Only if retry policy configured and retries remaining
- Sets `status=PENDING`, increments `retry_count`
- Sets `next_retry_at` based on retry policy intervals
- A RUNNING row goes back to PENDING only with `retry_count + 1`, so the next attempt that runs has a greater [fencing token](/horsies/concepts/fencing-token/)

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
2. **Runner heartbeat**: Worker process writes for RUNNING tasks that its children hold

Missing heartbeats trigger automatic recovery. See [Heartbeats & Recovery](../../workers/heartbeats-recovery).

The runner stale threshold is heartbeat-based, not duration-based. A long task
can run past `running_stale_threshold_ms` as long as its child process holds it
and the worker writes its runner heartbeats.

## Task Expiry

Tasks can have a `good_until` deadline, set at send time:

- **PENDING expiry:** If the task isn't claimed before `good_until`, the claim SQL skips it. The reaper periodically transitions unclaimed expired tasks to `EXPIRED`.
- **CLAIMED expiry:** If the task was claimed but `good_until` passes before execution starts, the worker marks it `EXPIRED` directly instead of proceeding to `RUNNING`.
- Both paths produce a `TASK_EXPIRED` outcome code. No attempt row is written since user code never ran.
- `good_until` also caps retries — a retry scheduled at or past the deadline is rejected.

The API is on the client pages: [Set a Task Deadline](/horsies/tasks/sending-tasks/#set-a-task-deadline) (Python) and [Send Options](/horsies/rust/tasks/sending-tasks/#send-options) (Rust).

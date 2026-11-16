---
title: Task Lifecycle
summary: Task states and transitions from submission to completion.
related: [result-handling, ../../workers/heartbeats-recovery, ../../tasks/retry-policy]
tags: [concepts, tasks, states]
---

## Task States

| State | Description |
|-------|-------------|
| `Pending` | Task is queued, waiting to be claimed |
| `Claimed` | Worker has claimed the task, preparing to execute |
| `Running` | Task is actively executing in the worker |
| `Completed` | Task finished successfully |
| `Failed` | Task failed (error returned or panic) |
| `Cancelled` | Task was cancelled via workflow cancellation |
| `Expired` | `good_until` deadline passed before execution started |

**Terminal states:** `Completed`, `Failed`, `Cancelled`, `Expired`.

Terminal states do not remain in `horsies_tasks`. Terminalization inserts the
immutable record into `horsies_task_history` and deletes the live row in one
transaction.

## Status Enum

```rust
pub enum TaskStatus {
    Pending,
    Claimed,
    Running,
    Completed,
    Failed,
    Cancelled,
    Expired,
}

pub const TASK_TERMINAL_STATES: &[TaskStatus] = &[
    TaskStatus::Completed,
    TaskStatus::Failed,
    TaskStatus::Cancelled,
    TaskStatus::Expired,
];
```

## State Transitions

```
                    ┌──────────────┐
              ┌─────│   Pending    │─────────┐
              │     └──────┬───────┘         │
              │            │ Worker          │ good_until
              │            │ claims          │ passed
              │            ▼                 ▼
              │     ┌──────────────┐      ┌──────────┐
    timeout   │     │   Claimed    │   ►  │ Expired  │
   (requeue)◄─┼─────┤              │      └──────────┘
              │     └──────┬───────┘       good_until passed
              │            │ Execution
              │            ▼ starts
              │     ┌──────────────┐
              │     │   Running    │
              │     └──────┬───────┘
              │            │
              │ ┌──────────┼────────────┐
              │ │          │            │
              │ ▼          ▼            ▼
              │ ┌──────────┐ ┌──────────┐  (retry)
              │ │Completed │ │  Failed  │────┐
              │ └──────────┘ └──────────┘    │
              │                              │
              └──────────────────────────────┘
```

## Transition Details

### Pending → Claimed

- Worker executes claim query with `FOR UPDATE SKIP LOCKED`
- Sets `claimed=TRUE`, `claimed_at=NOW()`, `claimed_by_worker_id`
- Task is reserved for this worker

### Claimed → Running

- Task dispatched via `tokio::spawn` (async) or `spawn_blocking` (blocking)
- Sets `status=Running`, `started_at=NOW()`
- Runner heartbeat loop begins

### Running → Completed

- Task returns `Ok(value)`
- Worker stores the serialized result and `completed_at` in task history
- The terminalization transaction removes the live row

`Completed` means the task succeeded (returned `Ok`). Execution that ends with `Err(TaskError)` or a panic is `Failed`, not `Completed`.

### Running → Failed

- Task returns `Err(TaskError { ... })` **or**
- Task panics (caught, wrapped as `UnhandledError`) **or**
- Worker crashes (detected via missing heartbeats)
- Worker stores `failed_at` and the structured error in task history

### Running → Pending (retry)

- Only if retry policy configured and retries remaining
- Sets `status=Pending`, increments `retry_count`
- Sets `next_retry_at` based on retry policy intervals

### Pending → Expired

- Task has `good_until` set and deadline has passed before it was claimed
- Reaper moves it to history with the `TaskExpired` outcome code

### Claimed → Expired

- Worker claimed the task, but `good_until` passed before user code started
- Worker moves it to history with `TaskExpired`
- No attempt row is written because the task body did not run

### Claimed → Pending (stale recovery)

- Claimer heartbeat missing for `claimed_stale_threshold_ms`
- Reaper automatically requeues (safe — user code never ran)
- Sets `claimed=FALSE`, `claimed_at=NULL`

## Timestamps

Each task records timing information:

| Field | Set When |
|-------|----------|
| `sent_at` | Immutable call-site timestamp — when `.send()` or `.schedule()` was called |
| `enqueued_at` | When task becomes eligible for claiming (updated on retry) |
| `claimed_at` | Worker claims task |
| `started_at` | Execution begins |
| `completed_at` | Successful completion |
| `failed_at` | Failure |
| `next_retry_at` | Scheduled retry time |

## Heartbeats

Two types of heartbeats track task health:

1. **Claimer heartbeat**: Worker sends for Claimed tasks (task not yet running)
2. **Runner heartbeat**: Worker sends for Running tasks

Missing heartbeats trigger automatic recovery. See [Heartbeats & Recovery](../../workers/heartbeats-recovery).

## Task Expiry

Tasks can have a `good_until` deadline:

- If the task does not start before `good_until`, it becomes unclaimable
- The reaper transitions unclaimed expired tasks to `Expired`
- Workers also guard the Claimed → Running transition and expire tasks whose deadline passed while they were claimed
- Useful for time-sensitive operations

Set `good_until` per send:

```rust
use chrono::{Utc, Duration};
use horsies::TaskSendOptions;

let deadline = Utc::now() + Duration::minutes(5);

let handle = urgent_task::with_options(
    TaskSendOptions::new().good_until(deadline),
)
.send(input)
.await?;
```

For workflow tasks, use `.good_until(deadline)` on the workflow node while building the spec.

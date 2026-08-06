---
title: Heartbeats & Recovery
summary: Detecting and recovering from worker crashes via heartbeats.
tags: [workers, heartbeats, recovery, crash-detection]
---

## Overview

When a worker dies mid-task, the task would be stuck forever without detection. Heartbeats solve this:

1. Workers send periodic heartbeats for their tasks
2. A reaper checks for missing heartbeats
3. Stale tasks are automatically recovered

The recovery path depends on the task state:

| State when worker disappears | Recovery action | Why |
| --- | --- | --- |
| `CLAIMED` | requeue to `PENDING` | user code never started |
| `RUNNING` | retry or fail | user code may have partially run |
| workflow bookkeeping out of sync | reconcile workflow state | parent workflow still needs completion handling |

## Heartbeat Types

### Claimer Heartbeat

Sent by the worker for CLAIMED tasks:

- Indicates worker is alive and will soon start the task
- Sent at `claimer_heartbeat_interval_ms` interval
- Covers the gap between claim and execution start

### Runner Heartbeat

Sent by the spawned tokio task for RUNNING tasks:

- Indicates task is actively executing
- Sent at `runner_heartbeat_interval_ms` interval
- From a background tokio task spawned alongside the task execution

## Heartbeat Flow

<!-- todo:diagram-needed - Heartbeat sequence diagram -->

```text
CLAIMED                          RUNNING
   |                               |
   |  Claimer heartbeat            |  Runner heartbeat
   |  (from worker loop)           |  (from spawned task)
   |                               |
   +---- HB ----+                  +---- HB ----+
   |            |                  |            |
   +---- HB ----+  30s interval    +---- HB ----+  30s interval
   |            |                  |            |
   +------------+------------------+------------+--->
```

## What the Reaper Checks

The reaper periodically checks for stale tasks:

```text
for each task with status = CLAIMED:
    last_hb = latest heartbeat(task, role = "claimer")
    if now - last_hb > claimed_stale_threshold:
        requeue(task)  // Safe - code never ran

for each task with status = RUNNING:
    last_hb = latest heartbeat(task, role = "runner")
    if now - last_hb > running_stale_threshold:
        retry_or_fail(task)  // Retry if policy allows, otherwise fail
```

## Recovery Actions

### Stale CLAIMED -> PENDING

When a CLAIMED task has no recent claimer heartbeat:

- **Safe to requeue**: User code never started
- Task reset to PENDING
- Another worker will claim it
- No data corruption risk

This is the cleanest recovery path because execution never began.

### Stale RUNNING Recovery

When a RUNNING task has no recent runner heartbeat:

- **Not safe to blindly requeue**: Code was executing, may have partial side effects
- If the task has a retry policy with `WORKER_CRASHED` in `auto_retry_for` and retries remaining: scheduled for retry (returns to PENDING with `next_retry_at`)
- Otherwise: marked as FAILED with `WORKER_CRASHED` error

The terminal branch runs through `horsies_fail_stale_task`, which captures
the heartbeat and finalizing state under its own row lock and re-judges
staleness from that capture — the reaper's scan is advisory. A heartbeat
that lands between the scan and the call refuses the failure and reports the
compared values (`last_heartbeat_at`, `finalizing_at`, thresholds, and the
evaluation instant) instead of failing a live task.

This is why idempotent task design still matters: crash recovery can retry work that may have partially completed before the worker died.

### Orphaned Workflow Task Cleanup

A workflow task is *orphaned* when it sits CLAIMED or PENDING but no
`workflow_tasks` row for it remains in a runnable state — it can never
legitimately reach RUNNING, and left alone it holds a claim forever. With
`auto_terminate_orphaned_workflow_tasks` (default `true`):

- the reaper sweeps orphans CANCELLED in bounded batches (500 per batch, up
  to 200 batches per pass); the sweep disables itself after 3 consecutive
  permanent failures and logs that manual intervention is needed
- a worker handed an orphan cancels it before start: the node RUNNING
  handoff finding no runnable linkage triggers `horsies_cancel_owned_orphan`,
  which re-verifies the linkage under its own lock and lets the task run if
  a runnable link exists after all

Set the flag to `false` to leave orphans CLAIMED for inspection instead.

### Workflow Task Recovery

When a worker crashes during a **workflow** task, the reaper marks `tasks.status = FAILED`, but the worker dies before calling the workflow task completion handler. This leaves `workflow_tasks.status` stuck in `RUNNING` while the underlying task is already terminal.

The recovery loop detects this mismatch automatically:

1. Finds `workflow_tasks` rows in non-terminal status where the linked `tasks` row is terminal (`COMPLETED`, `FAILED`, or `CANCELLED`)
2. Deserializes the `TaskResult` from the task's stored result
3. If no result is stored, synthesizes an error result:
   - `WORKER_CRASHED` for failed tasks
   - `TASK_CANCELLED` for cancelled tasks
   - `RESULT_NOT_AVAILABLE` for completed tasks with missing results
4. Triggers the normal completion path: updates `workflow_tasks` status, applies `on_error` policy, propagates to dependents, and checks workflow completion

This runs before workflow finalization, so dependents are resolved in the same recovery pass.

## What Is Public API vs Worker Internals

The worker reaper handles stale task recovery automatically during normal worker operation.

The public Rust API does not expose dedicated helpers for:

- requeueing stale `CLAIMED` tasks
- failing stale `RUNNING` tasks

For those paths, the supported approach is to let the worker reaper handle them or to use targeted operational SQL if you are doing manual intervention.

For workflow-level reconciliation, there is a separate public helper:

```rust
horsies::recover_stuck_workflows(&pool, &registry).await?;
```

## Configuration

```rust
use horsies::{AppConfig, RecoveryConfig};

let config = AppConfig {
    recovery: RecoveryConfig {
        // Claimer detection
        auto_requeue_stale_claimed: true,
        claimed_stale_threshold_ms: 120_000,     // 2 minutes
        claimer_heartbeat_interval_ms: 30_000,   // 30 seconds

        // Runner detection
        auto_fail_stale_running: true,
        running_stale_threshold_ms: 300_000,     // 5 minutes
        runner_heartbeat_interval_ms: 30_000,    // 30 seconds

        // Check frequency
        check_interval_ms: 30_000,               // 30 seconds

        ..Default::default()
    },
    ..AppConfig::for_database_url("postgresql://...")
};
```

## Timing Guidelines

### Rule: Threshold >= 2x Interval

Stale thresholds must be at least 2x the heartbeat interval:

```rust
// Valid
RecoveryConfig {
    runner_heartbeat_interval_ms: 30_000, // 30s
    running_stale_threshold_ms: 60_000,   // 60s (2x)
    ..Default::default()
}

// Invalid - will produce validation error
RecoveryConfig {
    runner_heartbeat_interval_ms: 30_000, // 30s
    running_stale_threshold_ms: 30_000,   // 30s (too tight!)
    ..Default::default()
}
```

### For CPU-Heavy Tasks

Long-running blocking tasks (via `#[blocking_task]`) may delay the heartbeat:

```rust
RecoveryConfig {
    runner_heartbeat_interval_ms: 60_000,  // Heartbeat every minute
    running_stale_threshold_ms: 300_000,   // 5 minutes before stale
    ..Default::default()
}
```

### For Quick Tasks

Fast tasks can use tighter detection:

```rust
RecoveryConfig {
    runner_heartbeat_interval_ms: 10_000, // 10 seconds
    running_stale_threshold_ms: 30_000,   // 30 seconds
    ..Default::default()
}
```

## Database Schema

Heartbeats are stored in the `horsies_heartbeats` table:

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | int | Auto-increment ID |
| `task_id` | str | Task being tracked |
| `sender_id` | str | Worker identifier |
| `role` | str | 'claimer' or 'runner' |
| `sent_at` | datetime | Heartbeat timestamp |
| `hostname` | str | Machine hostname |
| `pid` | int | Process ID |

## Disabling Recovery

Not recommended, but possible:

```rust
RecoveryConfig {
    auto_requeue_stale_claimed: false,
    auto_fail_stale_running: false,
    ..Default::default()
}
```

Tasks will remain stuck until manually resolved.

## Table Cleanup

Heartbeat cleanup is automatic by default. The worker reaper deletes expired heartbeats on every tick based on `RecoveryConfig.heartbeat_retention_hours` (default: `Some(24)`, set to `None` to disable).

For manual cleanup:

```sql
-- Delete heartbeats older than 24 hours
DELETE FROM horsies_heartbeats WHERE sent_at < NOW() - INTERVAL '24 hours';
```

## Troubleshooting

### False Positives (Tasks Marked Stale But Running)

Increase thresholds:

```rust
RecoveryConfig {
    running_stale_threshold_ms: 600_000, // 10 minutes
    ..Default::default()
}
```

Common causes:

- Blocking tasks holding the tokio runtime
- Network latency to database
- Database contention

### Tasks Not Recovering

Check:

- `auto_requeue_stale_claimed` / `auto_fail_stale_running` enabled?
- Reaper loop running? (Check worker logs)
- Database connectivity?

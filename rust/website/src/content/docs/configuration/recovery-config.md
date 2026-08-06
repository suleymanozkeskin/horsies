---
title: Recovery Config
summary: Automatic detection and recovery of stale tasks.
related: [../../workers/heartbeats-recovery, app-config]
tags: [configuration, recovery, heartbeats]
---

## Overview

Tasks can become stale when:

- Worker process crashes mid-execution
- Network partition prevents heartbeats
- Worker machine goes down

Horsies automatically detects and recovers these tasks.

## Basic Usage

```rust
use horsies::{AppConfig, RecoveryConfig};

let config = AppConfig {
    recovery: RecoveryConfig {
        auto_requeue_stale_claimed: true,
        claimed_stale_threshold_ms: 120_000,
        auto_fail_stale_running: true,
        running_stale_threshold_ms: 300_000,
        ..RecoveryConfig::default()
    },
    ..AppConfig::for_database_url("postgresql://...")
};
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_requeue_stale_claimed` | `bool` | `true` | Requeue tasks stuck in CLAIMED |
| `claimed_stale_threshold_ms` | `u64` | 120,000 | Ms before CLAIMED task is stale |
| `auto_fail_stale_running` | `bool` | `true` | Fail tasks stuck in RUNNING |
| `auto_terminate_orphaned_workflow_tasks` | `bool` | `true` | Cancel workflow tasks with no runnable `workflow_tasks` linkage (reaper sweep + pre-start check); `false` leaves them CLAIMED for inspection |
| `running_stale_threshold_ms` | `u64` | 300,000 | Ms before RUNNING task is stale |
| `check_interval_ms` | `u64` | 30,000 | How often to check for stale tasks |
| `runner_heartbeat_interval_ms` | `u64` | 30,000 | RUNNING task heartbeat frequency |
| `claimer_heartbeat_interval_ms` | `u64` | 30,000 | CLAIMED task heartbeat frequency |
| `heartbeat_retention_hours` | `Option<u32>` | `Some(24)` | Hours to keep heartbeat rows; `None` disables pruning |
| `worker_state_retention_hours` | `Option<u32>` | `Some(168)` (7 days) | Hours to keep worker_state snapshots; `None` disables pruning |
| `terminal_record_retention_hours` | `Option<u32>` | `Some(720)` (30 days) | Hours to keep terminal task/workflow rows; `None` disables pruning |
| `queue_terminal_record_retention_hours` | `HashMap<String, u32>` | `{}` | Per-queue overrides of the terminal window for plain (non-workflow) tasks (1h–5y). Queues not listed use the global window; overrides apply even when the global window is `None`. Override keys must name declared queues |
| `retention_sweep_interval_s` | `u64` | `300` | Seconds between retention sweep passes (30s–24h). Frequent small sweeps keep each pass short instead of accumulating an hourly spike |
| `retention_delete_batch_size` | `u32` | `500` | Rows per retention DELETE batch (50–10,000). Bounds per-statement duration, row locks, and WAL; each batch commits independently |

All time values for thresholds and intervals are in milliseconds. Retention values are in hours.

## Recovery Behaviors

### Stale CLAIMED Tasks

When a task is CLAIMED but the claimer heartbeat stops:

- **Safe to requeue**: User code never started executing
- Task is reset to PENDING for another worker to claim
- Original worker may have crashed before dispatching

### Stale RUNNING Tasks

When a **regular** task is RUNNING but the runner heartbeat stops:

- **Not safe to blindly requeue**: User code was executing, could have partial side effects
- If the task has a retry policy with `WORKER_CRASHED` in `auto_retry_for` and retries remaining: scheduled for retry (returns to PENDING with `next_retry_at`)
- Otherwise: marked as FAILED with `WORKER_CRASHED` error

For **workflow** tasks, the recovery loop also detects when `workflow_tasks` is stuck non-terminal while the underlying task is already terminal, and triggers the normal completion path. See [Heartbeats & Recovery](../../workers/heartbeats-recovery) for details.

## Heartbeat System

<!-- todo:diagram-needed - Heartbeat flow diagram -->

Two heartbeat types:

1. **Claimer heartbeat**: Sent by the worker for CLAIMED tasks (not yet running)
2. **Runner heartbeat**: Sent by the spawned task for RUNNING tasks

The reaper (running as a tokio task in each worker) checks for missing heartbeats.

## Threshold Guidelines

| Threshold | Constraint |
|-----------|------------|
| Stale threshold | Must be >= 2x heartbeat interval |
| Claimed stale | 1 second to 1 hour |
| Running stale | 1 second to 2 hours |
| Check interval | 1 second to 10 minutes |
| Heartbeat intervals | 1 second to 2 minutes |

### For CPU-Heavy Tasks

Long-running blocking tasks may delay the heartbeat:

```rust
RecoveryConfig {
    runner_heartbeat_interval_ms: 60_000,    // Heartbeat every minute
    running_stale_threshold_ms: 600_000,     // 10 minutes before considered stale
    ..Default::default()
}
```

### For Quick Tasks

Fast tasks can use tighter thresholds:

```rust
RecoveryConfig {
    runner_heartbeat_interval_ms: 10_000,    // Heartbeat every 10s
    running_stale_threshold_ms: 30_000,      // 30s before considered stale
    ..Default::default()
}
```

## Validation

The config validates that thresholds are safe:

```rust
// This will produce a validation error:
RecoveryConfig {
    runner_heartbeat_interval_ms: 30_000,
    running_stale_threshold_ms: 30_000, // Must be >= 60_000 (2x heartbeat)
    ..Default::default()
}
```

## Retention Cleanup

The reaper loop automatically prunes old rows every `retention_sweep_interval_s` seconds (default 5 minutes), deleting in batches of `retention_delete_batch_size` rows (default 500) under a shared 60-second pass budget. A doomed task's `horsies_task_attempts` history is purged set-wise in the same statement. Three categories are cleaned independently:

| Category | Config field | Default | What gets deleted |
|----------|-------------|---------|-------------------|
| Heartbeats | `heartbeat_retention_hours` | 24h | `horsies_heartbeats` rows older than threshold |
| Worker states | `worker_state_retention_hours` | 7 days | `horsies_worker_states` snapshots older than threshold |
| Terminal records | `terminal_record_retention_hours` | 30 days | `horsies_tasks`, `horsies_workflows`, and `horsies_workflow_tasks` rows in COMPLETED/FAILED/CANCELLED status older than threshold |

Set any field to `None` to disable pruning for that category.

```rust
RecoveryConfig {
    heartbeat_retention_hours: Some(48),         // Keep heartbeats for 2 days
    worker_state_retention_hours: Some(24 * 14), // Keep worker snapshots for 2 weeks
    terminal_record_retention_hours: Some(24 * 90), // Keep terminal records for 90 days
    ..Default::default()
}
```

Per-queue overrides shorten (or lengthen) the terminal window for plain tasks on specific queues — for example an hours-scale window for a high-volume metrics queue whose terminal rows have no audit value:

```rust
RecoveryConfig {
    terminal_record_retention_hours: Some(24 * 30),
    queue_terminal_record_retention_hours: HashMap::from([
        ("metrics".to_owned(), 6), // metrics-queue terminal rows kept 6 hours
    ]),
    ..Default::default()
}
```

Overrides govern plain (non-workflow) tasks only: workflow-backing task rows always age under the global window, so a workflow and its task rows are retained as a unit. Override keys must name declared queues (`custom_queues` in CUSTOM mode, `"default"` in DEFAULT mode) — a typo'd key fails config validation instead of silently doing nothing.

To disable all automatic cleanup:

```rust
RecoveryConfig {
    heartbeat_retention_hours: None,
    worker_state_retention_hours: None,
    terminal_record_retention_hours: None,
    ..Default::default()
}
```

## Disabling Recovery

To disable automatic recovery (not recommended):

```rust
RecoveryConfig {
    auto_requeue_stale_claimed: false,
    auto_fail_stale_running: false,
    ..Default::default()
}
```

Tasks will remain stuck until manually resolved.

## Manual Recovery

For stale `CLAIMED` and stale `RUNNING` tasks, Rust does not expose dedicated public recovery helpers on `Horsies` or `PostgresBroker`.

The supported paths are:

- let the worker reaper handle stale-task recovery automatically
- use targeted operational SQL for manual intervention

For workflow-level reconciliation, there is a separate public helper:

```rust
horsies::recover_stuck_workflows(&pool, &registry).await?;
```

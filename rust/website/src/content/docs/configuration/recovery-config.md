---
title: Recovery Config
summary: Automatic detection and recovery of stale tasks.
related: [retention-config, ../../workers/heartbeats-recovery, app-config]
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
| `finalizing_stale_threshold_ms` | `u64` | 300,000 | Ms a task may remain in finalization before recovery |
| `crashed_worker_recovery_grace_ms` | `u64` | 10,000 | Grace before the worker consumes workflow progression evidence. `0` removes the grace |
| `phase2_quarantine_after_attempts` | `u32` | 25 | Failed recovery passes before evidence moves to quarantine. Range: 3–1,000 |
| `check_interval_ms` | `u64` | 30,000 | How often to check for stale tasks |
| `runner_heartbeat_interval_ms` | `u64` | 30,000 | RUNNING task heartbeat frequency |
| `claimer_heartbeat_interval_ms` | `u64` | 30,000 | CLAIMED task heartbeat frequency |
| `worker_state_snapshot_interval_ms` | `u64` | 30,000 | How often a worker writes a monitoring snapshot. Range: 1,000–300,000 ms |

Threshold and interval values on this page use milliseconds.

## Moved and removed fields

These fields moved from the alpha.25 `RecoveryConfig` to
`AppConfig.retention`:

- `terminal_record_retention_hours`
- `worker_state_retention_hours`
- `retention_sweep_interval_s`
- `retention_delete_batch_size`

These `AppConfig.retention` fields are new in alpha.26:

- `retention_classes`
- `history_leaf_horizon_days`
- `heartbeat_leaf_horizon_hours`
- `partition_maintenance_interval_s`
- `paused_workflow_auto_cancel_after`

`RecoveryConfig` refuses all nine names above. Each error names
`AppConfig.retention.<field>` as the successor.

`queue_terminal_record_retention_hours` was removed. Use
`AppConfig.retention.queue_retention`.

`heartbeat_retention_hours` was removed. Use partitioned heartbeat storage and
`AppConfig.retention.heartbeat_leaf_horizon_hours`.

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

For workflow tasks, terminalization writes the owed DAG progression to a
transactional outbox. The worker consumes that evidence after
`crashed_worker_recovery_grace_ms`. See [Heartbeats &
Recovery](../../workers/heartbeats-recovery).

### Phase-2 quarantine

Some workflow progression evidence cannot be applied. The worker retries each
row on later passes. It moves a row to quarantine after
`phase2_quarantine_after_attempts` failed passes.

The quarantine keeps the source evidence. Discovery stops retrying that row.
Worker health reports pending, failed, over-bound, and quarantined counts.

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
| Finalizing stale threshold | Must be >= 2x runner heartbeat interval |
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

## Retention

Retention now has a separate config object. It controls task-history classes,
partition coverage, workflow cleanup, and worker-state cleanup.

Use [`AppConfig.retention`](../retention-config). Do not place retention fields in
`RecoveryConfig`.

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

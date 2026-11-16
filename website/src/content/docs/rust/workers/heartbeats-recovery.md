---
title: Heartbeats & Recovery
summary: Detect stale work, recover task state, and complete workflow progression.
related: [../../configuration/recovery-config, ../../configuration/retention-config, worker-architecture]
tags: [workers, heartbeats, recovery, crash-detection]
---

## Overview

Workers send heartbeats for owned tasks. The reaper checks stored heartbeats.
It repairs stale task and workflow state.

| Task state | Recovery action | Reason |
|---|---|---|
| `CLAIMED` | Requeue to `PENDING` | User code did not start |
| `RUNNING` | Retry or fail | User code may have run |
| Terminal workflow backing task | Consume phase-2 evidence | The DAG still needs progression |

## Heartbeat roles

A claimer heartbeat covers a `CLAIMED` task. The worker sends it while the task
waits for execution.

A runner heartbeat covers a `RUNNING` task. The spawned task sends it while
user code runs.

```text
CLAIMED                          RUNNING
   |                               |
   |  claimer heartbeat            |  runner heartbeat
   |                               |
   +---- HB ----+                  +---- HB ----+
   |            |                  |            |
   +---- HB ----+                  +---- HB ----+
   |            |                  |            |
   +------------+------------------+------------+-->
```

## Partitioned heartbeat storage

`horsies_heartbeats` uses hourly range partitions. Task IDs use PostgreSQL
`uuid`.

The worker creates future leaves at startup. It maintains them every
`partition_maintenance_interval_s`. The default interval is 900 seconds.

`heartbeat_leaf_horizon_hours` sets the heartbeat horizon. The default is six
hours. The allowed range is 2–48 hours.

The maintenance pass drops old heartbeat leaves. It does not delete heartbeat
rows. `heartbeat_retention_hours` no longer exists.

Partition detach uses a five-second statement timeout. A blocked leaf is
reported and retried later. Other retention classes continue.

The worker role needs `CREATE` on the heartbeat partition parent. An external
coverage job must create leaves when the worker role lacks that privilege.

## Stale `CLAIMED` tasks

A stale claimer heartbeat means that user code did not start. Recovery returns
the task to `PENDING`.

The drain operation uses the same distinction. A stale claim must be requeued
through normal recovery before an offline cutover can continue.

## Stale `RUNNING` tasks

A stale runner heartbeat does not prove that the task made no side effects.
Recovery never treats it as an untouched send.

The action is:

- Retry when `WORKER_CRASHED` matches the retry policy and attempts remain.
- Otherwise, move the task to history as `FAILED` with `WORKER_CRASHED`.

The terminal operation locks the task and reads its heartbeat facts again. A
new heartbeat refuses the failure. The initial scan is only a candidate scan.

`finalizing_at` protects the handoff after user code returns. Recovery waits
until `finalizing_stale_threshold_ms` has also elapsed.

## Workflow progression outbox

Workflow task finalization has two durable steps:

1. Move the terminal backing task to history.
2. Apply the result to the workflow DAG.

The first transaction writes `horsies_workflow_phase2_pending`. The row records
the exact workflow node and terminal evidence. A worker crash cannot lose the
owed progression.

The healthy finalizer may consume the evidence immediately. The reaper waits
`crashed_worker_recovery_grace_ms` before recovery consumption. The default is
10,000 ms. Set it to zero for no grace.

Each evidence row is processed in its own transaction. One bad row does not
block later rows.

## Quarantine

Some evidence cannot apply because stored source and workflow state conflict.
The worker keeps the evidence and increments its attempt count.

After `phase2_quarantine_after_attempts`, the worker moves the row to
`horsies_workflow_phase2_quarantine`. The default is 25 passes. The allowed
range is 3–1,000 passes.

Quarantine preserves the source facts. It removes the row from normal
discovery. Worker health reports these values:

- applied rows
- retained rows
- failed rows
- rows over the attempt bound
- quarantined rows
- quarantine refusals

Deleting a terminal workflow deletes its unconsumed pending evidence through a
foreign-key cascade.

## Orphaned workflow tasks

An orphaned backing task has no runnable workflow-node link. It cannot start
valid work.

`auto_terminate_orphaned_workflow_tasks` defaults to `true`. The reaper cancels
orphans in bounded batches. A worker also checks the link before task start.

Set the field to `false` to leave orphaned claims for inspection.

## Unified maintenance pass

One reaper pass owns these independent operations:

- stale task recovery
- workflow recovery
- phase-2 evidence consumption
- phase-2 quarantine
- history and heartbeat partition coverage
- history and heartbeat partition pruning
- paused workflow expiry
- workflow and worker-state row cleanup

A cluster-wide advisory gate keeps one maintenance owner active. A database
error while acquiring the gate skips that pass. It does not run ungated.

Each operation reports its own health. A failed history class does not stop
heartbeat coverage or later history classes.

## Configuration

```rust
use horsies::{AppConfig, RecoveryConfig, RetentionConfig};

let config = AppConfig {
    recovery: RecoveryConfig {
        auto_requeue_stale_claimed: true,
        claimed_stale_threshold_ms: 120_000,
        auto_fail_stale_running: true,
        running_stale_threshold_ms: 300_000,
        finalizing_stale_threshold_ms: 300_000,
        crashed_worker_recovery_grace_ms: 10_000,
        phase2_quarantine_after_attempts: 25,
        claimer_heartbeat_interval_ms: 30_000,
        runner_heartbeat_interval_ms: 30_000,
        ..RecoveryConfig::default()
    },
    retention: RetentionConfig {
        heartbeat_leaf_horizon_hours: 6,
        partition_maintenance_interval_s: 900,
        ..RetentionConfig::default()
    },
    ..AppConfig::for_database_url("postgresql://...")
};
```

A stale threshold must be at least twice its heartbeat interval.

## Troubleshooting

### Healthy tasks are marked stale

Raise the matching stale threshold. Check database latency. Check runtime
starvation in blocking work.

### Tasks do not recover

Check the worker reaper logs. Check database access. Check the automatic
recovery flags. Check the maintenance health fields.

### Heartbeat writes fail near a leaf boundary

Check partition coverage health. Confirm that the worker role can create
partitions. Keep at least two future heartbeat leaves available.

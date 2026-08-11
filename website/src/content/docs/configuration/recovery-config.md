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

```python
from horsies.core.models.recovery import RecoveryConfig

config = AppConfig(
    broker=PostgresConfig(...),
    recovery=RecoveryConfig(
        auto_requeue_stale_claimed=True,
        claimed_stale_threshold_ms=120_000,
        auto_fail_stale_running=True,
        running_stale_threshold_ms=300_000,
        finalizing_stale_threshold_ms=300_000,
    ),
)
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_requeue_stale_claimed` | `bool` | `True` | Requeue tasks stuck in CLAIMED |
| `claimed_stale_threshold_ms` | `int` | 120,000 | Ms before CLAIMED task is stale |
| `auto_fail_stale_running` | `bool` | `True` | Fail tasks stuck in RUNNING |
| `running_stale_threshold_ms` | `int` | 300,000 | Ms before RUNNING task is stale |
| `finalizing_stale_threshold_ms` | `int` | 300,000 | Ms a completed child may remain in finalization handoff before recovery |
| `crashed_worker_recovery_grace_ms` | `int` | 10,000 | Grace before recovering a workflow task whose underlying task is terminal but whose workflow progression was not applied; `0` disables |
| `check_interval_ms` | `int` | 30,000 | How often to check for stale tasks |
| `runner_heartbeat_interval_ms` | `int` | 30,000 | RUNNING task heartbeat frequency |
| `claimer_heartbeat_interval_ms` | `int` | 30,000 | CLAIMED task heartbeat frequency |
| `worker_state_snapshot_interval_ms` | `int` | 30,000 | How often each worker persists a monitoring snapshot row to `horsies_worker_states` (1s–5min) |
| `auto_terminate_orphaned_workflow_tasks` | `bool` | `True` | Terminate claimed tasks whose workflow node linkage no longer exists |
| `phase2_quarantine_after_attempts` | `int` | 25 | Recovery passes an unresolvable crashed-worker progression row may retain before its evidence moves to the quarantine table and discovery stops retrying it (3–1,000) |

All time values for thresholds and intervals are in milliseconds. Retention windows are in hours; the sweep interval is in seconds.

**Removed in 0.5.0 (setting either fails validation naming the successor):**
`heartbeat_retention_hours` — heartbeats live in hourly partitions and old
partitions drop whole; a row-delete window no longer exists.
`queue_terminal_record_retention_hours` — terminal task records age by the
retention class assigned at enqueue; per-queue row-delete windows no longer
exist.

Retention and partition-coverage fields moved to
[`AppConfig.retention`](retention-config); this table lists only what
`RecoveryConfig` still owns.

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

`running_stale_threshold_ms` is based on missing runner heartbeats, not total
task duration. Long tasks remain healthy as long as they continue heartbeating.
After user code returns, the child marks the task as finalizing before the
parent writes the terminal result. The reaper will not recover that task until
`finalizing_stale_threshold_ms` has also elapsed.

For **workflow** tasks, terminalization records the owed workflow progression in a transactional outbox as it moves the task to history, and the reaper consumes those records. Task finalization is two transactions — the task is terminalized first, then the workflow DAG is advanced — so a progression can be owed for a brief moment while its finalizer is still in flight. The reaper waits `crashed_worker_recovery_grace_ms` (default 10s) before consuming such a record, so it does not race a healthy finalizer; only a genuine crash in that gap is recovered (after the grace plus one reaper sweep). This grace is independent of the heartbeat-coupled thresholds. See [Heartbeats & Recovery](../../workers/heartbeats-recovery) for details.

### Unresolvable progression evidence quarantines

A progression record whose disposition keeps refusing to resolve is retried for
`phase2_quarantine_after_attempts` recovery passes (default 25, bounds 3–1,000),
then moved to a quarantine table with its evidence preserved; discovery stops
retrying it. Retaining dispositions are either transient races, which resolve
within a pass or two, or structural conflicts, which never do — the bound gives
a transient an order of magnitude more passes than it needs while capping a
structural row's error-log noise. Quarantined counts, rows over the attempt
bound, and the quarantine function's refusals are published on the worker
health surface beside the phase-2 pass summary.

## Heartbeat System

<!-- todo:diagram-needed - Heartbeat flow diagram -->

Two heartbeat types:

1. **Claimer heartbeat**: Sent by worker for CLAIMED tasks (not yet running)
2. **Runner heartbeat**: Sent by child process for RUNNING tasks

The reaper (running in each worker) checks for missing heartbeats.

## Threshold Guidelines

| Threshold | Constraint |
|-----------|------------|
| Stale threshold | Must be >= 2x heartbeat interval |
| Finalizing stale | Must be >= 2x runner heartbeat interval |
| Claimed stale | 1 second to 1 hour |
| Running stale | 1 second to 2 hours |
| Check interval | 1 second to 10 minutes |
| Heartbeat intervals | 1 second to 2 minutes |

### For CPU-Heavy Tasks

Long-running CPU tasks may block the heartbeat thread:

```python
RecoveryConfig(
    runner_heartbeat_interval_ms=60_000,    # Heartbeat every minute
    running_stale_threshold_ms=600_000,     # 10 minutes before considered stale
)
```

### For Quick Tasks

Fast tasks can use tighter thresholds:

```python
RecoveryConfig(
    runner_heartbeat_interval_ms=10_000,    # Heartbeat every 10s
    running_stale_threshold_ms=30_000,      # 30s before considered stale
)
```

## Validation

The config validates that thresholds are safe:

```python
# This will raise ValueError:
RecoveryConfig(
    runner_heartbeat_interval_ms=30_000,
    running_stale_threshold_ms=30_000,  # Must be >= 60_000 (2x heartbeat)
)
```

## Retention

Retention moved to its own configuration section:
[`AppConfig.retention`](retention-config). It decides how long records
live and how their storage is prepared and reclaimed, which is a
different question from the one this page answers — recovery is about
work that went wrong, retention about work that went right and is now
history.

Setting a retention field on `RecoveryConfig` fails at construction and
names its new home; there is no alias.

## Disabling Recovery

To disable automatic recovery (not recommended):

```python
RecoveryConfig(
    auto_requeue_stale_claimed=False,
    auto_fail_stale_running=False,
)
```

Tasks will remain stuck until manually resolved.

## Manual Recovery

Query stale tasks via broker:

```python
broker = app.get_broker()

# Find stale tasks
stale = await broker.get_stale_tasks(stale_threshold_minutes=5)

# Manually fail stale RUNNING tasks
failed_count = await broker.mark_stale_tasks_as_failed(stale_threshold_ms=300_000)

# Manually requeue stale CLAIMED tasks
requeued_count = await broker.requeue_stale_claimed(stale_threshold_ms=120_000)
```

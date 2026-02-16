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

```python
from horsies.core.models.recovery import RecoveryConfig

config = AppConfig(
    broker=PostgresConfig(...),
    recovery=RecoveryConfig(
        auto_requeue_stale_claimed=True,
        claimed_stale_threshold_ms=120_000,
        auto_fail_stale_running=True,
        running_stale_threshold_ms=300_000,
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
| `check_interval_ms` | `int` | 30,000 | How often to check for stale tasks |
| `runner_heartbeat_interval_ms` | `int` | 30,000 | RUNNING task heartbeat frequency |
| `claimer_heartbeat_interval_ms` | `int` | 30,000 | CLAIMED task heartbeat frequency |
| `heartbeat_retention_hours` | `int \| None` | 24 | Hours to keep heartbeat rows; `None` disables pruning |
| `worker_state_retention_hours` | `int \| None` | 168 (7 days) | Hours to keep worker_state snapshots; `None` disables pruning |
| `terminal_record_retention_hours` | `int \| None` | 720 (30 days) | Hours to keep terminal task/workflow rows; `None` disables pruning |

All time values for thresholds and intervals are in milliseconds. Retention values are in hours.

## Recovery Behaviors

### Stale CLAIMED Tasks

When a task is CLAIMED but the claimer heartbeat stops:

- **Safe to requeue**: User code never started executing
- Task is reset to PENDING for another worker to claim
- Original worker may have crashed before dispatching

### Stale RUNNING Tasks

When a **regular** task is RUNNING but the runner heartbeat stops:

- **Not safe to requeue**: User code was executing
- Task is marked as FAILED with `WORKER_CRASHED` error
- Could have partial side effects

For **workflow** tasks, the recovery loop also detects when `workflow_tasks` is stuck non-terminal while the underlying task is already terminal, and triggers the normal completion path. See [Heartbeats & Recovery](../../workers/heartbeats-recovery) for details.

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

## Retention Cleanup

The reaper loop automatically prunes old rows every hour. Three categories are cleaned independently:

| Category | Config field | Default | What gets deleted |
|----------|-------------|---------|-------------------|
| Heartbeats | `heartbeat_retention_hours` | 24h | `horsies_heartbeats` rows older than threshold |
| Worker states | `worker_state_retention_hours` | 7 days | `horsies_worker_states` snapshots older than threshold |
| Terminal records | `terminal_record_retention_hours` | 30 days | `horsies_tasks`, `horsies_workflows`, and `horsies_workflow_tasks` rows in COMPLETED/FAILED/CANCELLED status older than threshold |

Set any field to `None` to disable pruning for that category.

```python
RecoveryConfig(
    heartbeat_retention_hours=48,              # Keep heartbeats for 2 days
    worker_state_retention_hours=24 * 14,      # Keep worker snapshots for 2 weeks
    terminal_record_retention_hours=24 * 90,   # Keep terminal records for 90 days
)
```

To disable all automatic cleanup:

```python
RecoveryConfig(
    heartbeat_retention_hours=None,
    worker_state_retention_hours=None,
    terminal_record_retention_hours=None,
)
```

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

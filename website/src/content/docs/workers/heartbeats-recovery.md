---
title: Heartbeats & Recovery
summary: Detecting and recovering from worker crashes via heartbeats.
related: [../../configuration/recovery-config, worker-architecture]
tags: [workers, heartbeats, recovery, crash-detection]
---

## Overview

When a worker dies mid-task, the task would be stuck forever without detection. Heartbeats solve this:

1. Workers send periodic heartbeats for their tasks
2. A reaper checks for missing heartbeats
3. Stale tasks are automatically recovered

## Heartbeat Types

### Claimer Heartbeat

Sent by the main worker process for CLAIMED tasks:

- Indicates worker is alive and will soon start the task
- Sent at `claimer_heartbeat_interval_ms` interval
- Covers the gap between claim and execution start

### Runner Heartbeat

Sent by the child process for RUNNING tasks:

- Indicates task is actively executing
- Sent at `runner_heartbeat_interval_ms` interval
- From a separate thread within the task process

## Heartbeat Flow

<!-- todo:diagram-needed - Heartbeat sequence diagram -->

```text
CLAIMED                          RUNNING
   │                               │
   │  Claimer heartbeat            │  Runner heartbeat
   │  (from main process)          │  (from task process)
   │                               │
   ├──── HB ────┐                  ├──── HB ────┐
   │            │                  │            │
   ├──── HB ────┤  30s interval    ├──── HB ────┤  30s interval
   │            │                  │            │
   └────────────┴──────────────────┴────────────┴───>
```

## Stale Detection

The reaper periodically checks for stale tasks:

```python
# Simplified logic
for task in tasks.filter(status='CLAIMED'):
    last_hb = get_latest_heartbeat(task, role='claimer')
    if now - last_hb > claimed_stale_threshold:
        requeue(task)  # Safe - code never ran

for task in tasks.filter(status='RUNNING'):
    last_hb = get_latest_heartbeat(task, role='runner')
    if now - last_hb > running_stale_threshold:
        fail(task)  # Not safe to requeue
```

## Recovery Actions

### Stale CLAIMED → PENDING

When a CLAIMED task has no recent claimer heartbeat:

- **Safe to requeue**: User code never started
- Task reset to PENDING
- Another worker will claim it
- No data corruption risk

### Stale RUNNING → FAILED

When a RUNNING task has no recent runner heartbeat:

- **Not safe to requeue**: Code was executing
- Marked as FAILED with `WORKER_CRASHED` error
- May have partial side effects
- Requires manual review

### Workflow Task Recovery

When a worker crashes during a **workflow** task, the reaper marks `tasks.status = FAILED`, but the worker dies before calling `on_workflow_task_complete()`. This leaves `workflow_tasks.status` stuck in `RUNNING` while the underlying task is already terminal.

The recovery loop detects this mismatch automatically:

1. Finds `workflow_tasks` rows in non-terminal status where the linked `tasks` row is terminal (`COMPLETED`, `FAILED`, or `CANCELLED`)
2. Deserializes the `TaskResult` from the task's stored result
3. If no result is stored, synthesizes an error result:
   - `WORKER_CRASHED` for failed tasks
   - `TASK_CANCELLED` for cancelled tasks
   - `RESULT_NOT_AVAILABLE` for completed tasks with missing results
4. Triggers the normal completion path: updates `workflow_tasks` status, applies `on_error` policy, propagates to dependents, and checks workflow completion

This runs before workflow finalization, so dependents are resolved in the same recovery pass.

## Configuration

```python
from horsies.core.models.recovery import RecoveryConfig

config = AppConfig(
    broker=PostgresConfig(...),
    recovery=RecoveryConfig(
        # Claimer detection
        auto_requeue_stale_claimed=True,
        claimed_stale_threshold_ms=120_000,     # 2 minutes
        claimer_heartbeat_interval_ms=30_000,   # 30 seconds

        # Runner detection
        auto_fail_stale_running=True,
        running_stale_threshold_ms=300_000,     # 5 minutes
        runner_heartbeat_interval_ms=30_000,    # 30 seconds

        # Check frequency
        check_interval_ms=30_000,               # 30 seconds
    ),
)
```

## Timing Guidelines

### Rule: Threshold >= 2x Interval

Stale thresholds must be at least 2x the heartbeat interval:

```python
# Valid
RecoveryConfig(
    runner_heartbeat_interval_ms=30_000,  # 30s
    running_stale_threshold_ms=60_000,    # 60s (2x)
)

# Invalid - will raise ValueError
RecoveryConfig(
    runner_heartbeat_interval_ms=30_000,  # 30s
    running_stale_threshold_ms=30_000,    # 30s (too tight!)
)
```

### For CPU-Heavy Tasks

Long-running CPU tasks may block the heartbeat thread:

```python
RecoveryConfig(
    runner_heartbeat_interval_ms=60_000,   # Heartbeat every minute
    running_stale_threshold_ms=300_000,    # 5 minutes before stale
)
```

### For Quick Tasks

Fast tasks can use tighter detection:

```python
RecoveryConfig(
    runner_heartbeat_interval_ms=10_000,   # 10 seconds
    running_stale_threshold_ms=30_000,     # 30 seconds
)
```

## Database Schema

Heartbeats are stored in the `heartbeats` table:

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | int | Auto-increment ID |
| `task_id` | str | Task being tracked |
| `sender_id` | str | Worker/process identifier |
| `role` | str | 'claimer' or 'runner' |
| `sent_at` | datetime | Heartbeat timestamp |
| `hostname` | str | Machine hostname |
| `pid` | int | Process ID |

## Manual Recovery

Query stale tasks:

```python
broker = app.get_broker()

# Find stale RUNNING tasks
stale = await broker.get_stale_tasks(stale_threshold_minutes=5)
for task in stale:
    print(f"Stale: {task['id']} on {task['worker_hostname']}")
```

Force recovery:

```python
# Manually fail stale RUNNING
failed = await broker.mark_stale_tasks_as_failed(stale_threshold_ms=300_000)
print(f"Failed {failed} stale tasks")

# Manually requeue stale CLAIMED
requeued = await broker.requeue_stale_claimed(stale_threshold_ms=120_000)
print(f"Requeued {requeued} stale tasks")
```

## Disabling Recovery

Not recommended, but possible:

```python
RecoveryConfig(
    auto_requeue_stale_claimed=False,
    auto_fail_stale_running=False,
)
```

Tasks will remain stuck until manually resolved.

## Table Cleanup

The `heartbeats` table grows over time. Recommended cleanup:

```sql
-- Delete heartbeats older than 24 hours
DELETE FROM heartbeats WHERE sent_at < NOW() - INTERVAL '24 hours';
```

Run periodically (daily) via scheduler or external cron.

## Troubleshooting

### False Positives (Tasks Marked Stale But Running)

Increase thresholds:

```python
RecoveryConfig(
    running_stale_threshold_ms=600_000,  # 10 minutes
)
```

Common causes:

- CPU-bound tasks blocking heartbeat thread
- Network latency to database
- Database contention

### Tasks Not Recovering

Check:

- `auto_requeue_stale_claimed` / `auto_fail_stale_running` enabled?
- Reaper loop running? (Check worker logs)
- Database connectivity?

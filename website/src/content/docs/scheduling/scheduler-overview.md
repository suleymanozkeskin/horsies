---
title: Scheduler Overview
summary: How the scheduler process enqueues tasks on a schedule.
related: [schedule-patterns, schedule-config, ../../configuration/app-config]
tags: [scheduling, scheduler, process]
---

## Architecture

<!-- todo:diagram-needed - Scheduler architecture -->

```text
┌─────────────────────────────┐
│  Scheduler Process          │
│  - Checks schedules         │
│  - Calculates next run      │
│  - Enqueues via broker      │
└──────────────┬──────────────┘
               │ enqueue_async()
               ▼
┌─────────────────────────────┐
│  PostgreSQL                 │
│  - tasks table              │
│  - schedule_state table     │
└──────────────┬──────────────┘
               │ NOTIFY
               ▼
┌─────────────────────────────┐
│  Workers                    │
│  - Execute scheduled tasks  │
└─────────────────────────────┘
```

The scheduler:

1. Runs as a separate process/dyno from workers
2. Checks configured schedules at regular intervals
3. Enqueues tasks when schedules are due
4. Tracks state in database to prevent duplicates

## Configuration

Add a `ScheduleConfig` to your `AppConfig`:

```python
from horsies.core.models.schedule import ScheduleConfig, TaskSchedule, DailySchedule
from datetime import time

config = AppConfig(
    broker=PostgresConfig(...),
    schedule=ScheduleConfig(
        enabled=True,
        check_interval_seconds=1,
        schedules=[
            TaskSchedule(
                name="daily-cleanup",
                task_name="cleanup_old_data",
                pattern=DailySchedule(time=time(3, 0, 0)),
                timezone="UTC",
            ),
        ],
    ),
)
```

## Running the Scheduler

```bash
horsies scheduler myapp.instance:app --loglevel=INFO
```

Run separately from workers. One scheduler per cluster is sufficient.

## Key Concepts

### Schedule Name

Each schedule has a unique name used for state tracking:

```python
TaskSchedule(
    name="daily-report",  # Must be unique
    task_name="generate_report",
    ...
)
```

### Task Name

The `task_name` must match a registered `@app.task()`:

```python
@app.task("generate_report")
def generate_report() -> TaskResult[str, TaskError]:
    ...

TaskSchedule(
    name="...",
    task_name="generate_report",  # Must match decorator
    ...
)
```

### Check Interval

How often the scheduler checks for due schedules:

```python
ScheduleConfig(
    check_interval_seconds=1,  # Check every second
    ...
)
```

Range: 1-60 seconds. Lower values provide better precision but more database queries.

## State Tracking

The `schedule_state` table tracks:

| Field | Purpose |
| ----- | ------- |
| `schedule_name` | Schedule identifier |
| `last_run_at` | When schedule last executed |
| `next_run_at` | When schedule should run next |
| `last_task_id` | ID of most recent enqueued task |
| `run_count` | Total execution count |
| `config_hash` | Detects configuration changes |

This prevents duplicate executions when:

- Scheduler restarts
- Multiple schedulers run (advisory locks serialize)
- Network issues cause delays

### Known behavior: failed initialization is restart-scoped

State rows are created once, at scheduler startup. If a schedule's row
creation fails there (for example a transient database error during
`start()`), that schedule is logged as failed and stays **dormant until
the next scheduler restart** — the tick loop only visits schedules that
already have a state row, so it cannot retry the initialization. Other
schedules are unaffected. If you see
`N schedule(s) failed to initialize` in the logs, restart the scheduler
once the underlying issue is resolved.

## Catch-Up Logic

When `catch_up_missed=True`, missed runs are executed:

```python
TaskSchedule(
    name="hourly-sync",
    task_name="sync_data",
    pattern=IntervalSchedule(hours=1),
    catch_up_missed=True,  # Execute missed runs
)
```

If the scheduler was down for 3 hours, it will enqueue 3 tasks on restart
(bounded by `max_catch_up_runs`).

When `catch_up_missed=False` (default), missed runs are dropped: after
downtime the scheduler fires at most one run — the most recent due slot —
logs how many older slots were skipped, and resumes at the next future
slot. Slot alignment is preserved, so task ids stay deterministic.

## Timezone Support

Each schedule can have its own timezone:

```python
TaskSchedule(
    name="morning-report",
    task_name="send_report",
    pattern=DailySchedule(time=time(9, 0, 0)),
    timezone="America/New_York",  # 9 AM Eastern
)
```

Uses Python's `zoneinfo` module. Default is "UTC".

## Validation

Schedules are validated at startup:

- Task must be registered
- Queue must be valid (CUSTOM mode)
- Required arguments must be provided

```python
# This will fail at scheduler start:
TaskSchedule(
    name="bad",
    task_name="nonexistent_task",  # Not registered
    ...
)
```

## Graceful Shutdown

The scheduler handles SIGTERM/SIGINT for clean shutdown:

```bash
# Sends SIGTERM
kill <scheduler_pid>
```

Current schedule check completes, then scheduler exits.

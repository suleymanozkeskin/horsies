---
title: Scheduler Overview
section: shared
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
│  - Fires due slots          │
└──────────────┬──────────────┘
               │ horsies_fire_schedule()
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

<!-- check: skip elided -- ... stands for a config shown on other pages -->
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

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
TaskSchedule(
    name="daily-report",  # Must be unique
    task_name="generate_report",
    ...
)
```

### Task Name

The `task_name` must match a registered `@app.task()`:

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
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

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
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
- Multiple schedulers run (the fire locks the state row)
- Network issues cause delays

### One fire, one statement

Each due schedule fires in one statement, `horsies_fire_schedule`: it locks the state row, checks that
`next_run_at` is still the value of the tick's due read, writes the task row of each slot, and moves the row,
in one transaction. A committed slot therefore always has a moved row, and it never fires again, also after its
task finished. A second scheduler that fires the same slot waits for that statement, then finds the row moved and
writes nothing.

- A connection failure during the fire: the slots stay due unless the statement committed; the next tick's due
  read shows which one happened, and a repeat of a committed slot writes no second task.
- The database refuses the fire (for example a CHECK violation of the task row): nothing is written, and a second
  statement moves the row past the refused slot and records no run, so a refusal that repeats does not stop the
  schedule.
- The state row is gone (deleted after the due read): the scheduler creates it again, with its first run after
  the database time of the tick.

### Clock

The scheduler takes its time from the database, not from the host it runs on. Each tick reads the due rows
and the database time (`statement_timestamp()`) in one statement; the slots of that tick come from that time,
and `last_run_at` is the database time of the fire. The start and the self-healing check compute
`next_run_at` from the database time too. A host clock that is ahead of or behind the database does not make
a slot fire early or late.

### A slot that already has a task

Each slot has a deterministic task id: a UUIDv7 whose time is the slot time (in milliseconds) and whose other bits come from a hash of the schedule name and the slot time (in microseconds). The row stores a fingerprint of the command. When the slot's task id already holds a live task, the fire compares the stored command: the same command is the slot's own task; a different command is `PAYLOAD_MISMATCH`. The command is different when an earlier horsies version wrote the task, or when the schedule's kwargs or the task's options changed.

Either way the slot already has its task, so the scheduler counts the slot as fired:

1. On `PAYLOAD_MISMATCH` it logs one warning that names the schedule, the slot and the task id.
2. It records the run with that task id and moves to the next slot.
3. The existing task keeps the command that it was enqueued with.

### Self-healing state rows

State rows are created at scheduler startup. If a schedule's row
creation fails there (for example a transient database error during
`start()`), the schedule is logged as failed and the **tick loop
recreates the missing row on a later tick** — each tick checks for
enabled schedules without a state row and re-runs their initialization.
The same check recovers a row deleted externally. The recreated row's
`next_run_at` is computed from the database time of the tick, so no
missed-slot backlog is replayed.

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
(bounded by `max_catch_up_runs`). The due slots of one tick fire in one
statement: all of them or none, and the fire counts as one run.

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
- Kwargs must encode against the task's types and fit `payload.reject_bytes`

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
# This will fail at scheduler start:
TaskSchedule(
    name="bad",
    task_name="nonexistent_task",  # Not registered
    ...
)
```

The scheduler encodes the kwargs of each schedule once at startup and sends that text for every slot until the next start. A kwargs model field with a `default_factory`, or a validator that reads the clock, runs once per start, not once per fire. If such a validator refuses the value later, the task is enqueued and fails at the worker with `WORKER_SERIALIZATION_ERROR`.

## Task Options

A scheduled run stores the options of its task, as a plain `.send()` of the task does. The worker reads `timeout_ms`, `retry_policy`, `execution` and `cancel_grace_ms` from the row, so a scheduled run gets the same timeout, retries and execution class as a sent one. A schedule has no options of its own: set them on `@app.task()`.

`@app.task()` encodes the options once, when the task is defined. The scheduler takes that text at startup and sends it for every slot until the next start. If the options do not encode, `@app.task()` raises `ValueError` with the task name and the cause, and the task is not registered. `horsies scheduler` then exits 1 at startup: the startup check reports the module import with `MODULE_EXEC_ERROR`, and `Scheduler.start()` refuses the schedule with `TASK_NOT_REGISTERED`.

## Graceful Shutdown

The scheduler handles SIGTERM/SIGINT for clean shutdown:

```bash
# Sends SIGTERM
kill <scheduler_pid>
```

Current schedule check completes, then scheduler exits.

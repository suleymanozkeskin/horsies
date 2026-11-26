---
title: Scheduler Overview
section: shared
summary: How the scheduler process enqueues tasks on a schedule.
related: [schedule-config, schedule-patterns, ../concepts/task-lifecycle]
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

The app config holds the schedule config: whether the scheduler is enabled, the check interval, and the list of schedules. Each schedule names itself, the task it sends, its pattern, its keyword arguments, an optional queue, its timezone and its catch-up rule.

The API is on the client pages: [Schedule Config](/horsies/scheduling/schedule-config/) and [Schedule Patterns](/horsies/scheduling/schedule-patterns/) (Python), and [Schedule Config](/horsies/rust/scheduling/schedule-config/) (Rust).

## Running the Scheduler

Run the scheduler as its own process, separate from the workers. One scheduler per cluster is sufficient. The Python command is `horsies scheduler`: see the [CLI Reference](/horsies/cli/#horsies-scheduler).

## Key Concepts

### Schedule Name

Each schedule has a name. The name is unique across the schedules, and the state row of the schedule is keyed by it.

### Task Name

The task name of a schedule must be the name of a registered task.

### Check Interval

The check interval is how often the scheduler checks for due schedules. Range: 1-60 seconds. Lower values provide better precision but more database queries.

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

## Configuration Changes

The state row stores a hash of the schedule config (`config_hash`). When the config of a schedule changes:

1. At startup, the scheduler detects the change through `config_hash`.
2. It computes `next_run_at` again from the database time; `run_count`, `last_run_at` and `last_task_id` stay.
3. It logs a warning about the configuration change.

This prevents issues when the pattern changes (for example hourly to daily), the timezone changes, or a time within the pattern changes.

The hash covers the pattern and the timezone. A change of the queue, the kwargs or the catch-up settings does not change it.

### The hash after an upgrade from 0.8

The hash text keeps one fixed form for each pattern. It does not follow the names of the pattern fields:

- The pattern tag `kind` (was `type`), the schedule `queue` (was `queue_name`), `WeeklySchedule.weekdays` (was `days`) and `MonthlySchedule.day_of_month` (was `day`) keep the stored hash.
- The hash text holds the length of an interval in four parts, the largest unit first: days, hours, minutes and seconds, with no zero part. An interval of 90 seconds is 1 minute and 30 seconds.

An interval whose 0.8 config gave another split of the same length gets a new hash: for example `seconds=90` (now `PT1M30S`) or `seconds=60` (now `PT1M`). At the first start after the upgrade, the scheduler resets `next_run_at` of that schedule one time, to the next slot after the database time. `run_count`, `last_run_at` and `last_task_id` stay. The slots between the old `next_run_at` and the start do not fire, also with `catch_up_missed`.

## Catch-Up Logic

When catch-up is on (`catch_up_missed`), missed runs are executed. If the scheduler was down for 3 hours on an hourly schedule, it enqueues 3 tasks on restart (bounded by `max_catch_up_runs`). The due slots of one tick fire in one statement: all of them or none, and the fire counts as one run.

When catch-up is off (the default), missed runs are dropped: after
downtime the scheduler fires at most one run — the most recent due slot —
logs how many older slots were skipped, and resumes at the next future
slot. Slot alignment is preserved, so task ids stay deterministic.

## Timezone Support

Each schedule has its own timezone, an IANA name such as `America/New_York`. The default is `UTC`.

## Validation

Schedules are validated at startup:

- Task must be registered
- Queue must be valid (CUSTOM mode)
- Required arguments must be provided
- Kwargs must encode against the task's types and fit `payload.reject_bytes`

The scheduler encodes the kwargs of each schedule once at startup and sends that text for every slot until the next start. A value that the encoding computes, such as a default that reads the clock, is computed once per start, not once per fire. If the worker refuses the stored value later, the task is enqueued and fails at the worker with `WORKER_SERIALIZATION_ERROR`.

## Task Options

A scheduled run stores the options of its task, as a plain send of the task does. The worker reads `timeout_ms`, `retry_policy`, `execution` and `cancel_grace_ms` from the row, so a scheduled run gets the same timeout, retries and execution class as a sent one. A schedule has no options of its own: set them on the task definition.

The scheduler takes the encoded options of each task at startup and sends that text for every slot until the next start. A task whose options do not encode is not registered, so the scheduler refuses to start. The Python messages are in [Task Options](/horsies/scheduling/schedule-config/#task-options).

## Graceful Shutdown

The scheduler handles SIGTERM/SIGINT for clean shutdown:

```bash
# Sends SIGTERM
kill <scheduler_pid>
```

Current schedule check completes, then scheduler exits.

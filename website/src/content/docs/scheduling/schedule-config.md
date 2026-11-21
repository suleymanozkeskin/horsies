---
title: Schedule Config
section: python
summary: ScheduleConfig and TaskSchedule configuration models.
related: [scheduler-overview, schedule-patterns]
tags: [scheduling, configuration, TaskSchedule]
---

## ScheduleConfig

Top-level scheduler configuration added to `AppConfig`.

<!-- check: skip elided -- ... stands for a config shown on other pages -->
```python
from horsies.core.models.schedule import ScheduleConfig, TaskSchedule

config = AppConfig(
    broker=PostgresConfig(...),
    schedule=ScheduleConfig(
        enabled=True,
        check_interval_seconds=1,
        schedules=[...],
    ),
)
```

### Fields

| Field | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `enabled` | `bool` | `True` | Enable/disable scheduler |
| `check_interval_seconds` | `int` | 1 | Seconds between schedule checks (1-60) |
| `schedules` | `list[TaskSchedule]` | `[]` | Schedule definitions |

## TaskSchedule

Defines a single scheduled task.

```python
from horsies.core.models.schedule import TaskSchedule, DailySchedule
from datetime import time

TaskSchedule(
    name="daily-report",
    task_name="generate_report",
    pattern=DailySchedule(time=time(9, 0, 0)),
    kwargs={"format": "pdf"},
    queue_name=None,  # Use task's default queue
    enabled=True,
    timezone="UTC",
    catch_up_missed=False,
)
```

### Fields

| Field | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `name` | `str` | required | Unique schedule identifier |
| `task_name` | `str` | required | Registered task name |
| `pattern` | `SchedulePattern` | required | When to run |
| `kwargs` | `dict` | `{}` | Keyword arguments passed to the task. Schedules are kwargs-only; `TaskSchedule` rejects any other field (including `args`) at construction. |
| `queue_name` | `str` | `None` | Queue override (CUSTOM mode) |
| `enabled` | `bool` | `True` | Enable/disable this schedule |
| `timezone` | `str` | `"UTC"` | Timezone for schedule evaluation |
| `catch_up_missed` | `bool` | `False` | `True`: replay missed runs on restart. `False`: drop missed runs, fire only the most recent due slot |
| `max_catch_up_runs` | `int` | `100` | Maximum runs to enqueue per scheduler tick when `catch_up_missed=True` (range: 1–10000) |

### Name

Must be unique across all schedules. Used for state tracking:

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
TaskSchedule(name="hourly-sync", ...)
TaskSchedule(name="daily-cleanup", ...)
```

### Task Name

Must match a registered `@app.task()`:

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
@app.task("send_notification")
def send_notification(*, user_id: int) -> TaskResult[None, TaskError]:
    ...

TaskSchedule(
    name="notify-users",
    task_name="send_notification",  # Must match
    ...
)
```

### Arguments

Pass arguments to the scheduled task via `kwargs`. Schedules are keyword-only: `TaskSchedule` has no `args` field, and passing one raises a validation error at construction.

```python
@app.task("process_region")
def process_region(*, region: str, full_sync: bool) -> TaskResult[None, TaskError]:
    ...

TaskSchedule(
    name="sync-us-east",
    task_name="process_region",
    pattern=IntervalSchedule(hours=1),
    kwargs={"region": "us-east", "full_sync": True},
)
```

The kwargs are encoded once per scheduler start (see [Validation](/horsies/scheduling/scheduler-overview/#validation)). A kwargs model field with a `default_factory`, or a validator that reads the clock, runs once per start, not once per fire.

### Task Options

`TaskSchedule` has no task options. A scheduled run stores the options that the task declares on `@app.task()` (`timeout_ms`, `retry_policy`, `execution`, `cancel_grace_ms`), as a plain `.send()` of the task does. See [Task Options](/horsies/scheduling/scheduler-overview/#task-options).

`@app.task()` encodes the options once, when the task is defined. If the options do not encode, `@app.task()` raises `ValueError` with the task name and the cause, and the task is not registered. `horsies scheduler` then exits 1 at startup: the startup check reports the module import with `MODULE_EXEC_ERROR`, and `Scheduler.start()` refuses the schedule with `TASK_NOT_REGISTERED`.

### Queue Override

In CUSTOM mode, override the task's default queue:

<!-- check: skip elided -- ... stands for a config shown on other pages -->
```python
@app.task("background_job", queue_name="normal")
def background_job() -> TaskResult[None, TaskError]:
    ...

TaskSchedule(
    name="priority-job",
    task_name="background_job",
    pattern=...,
    queue_name="critical",  # Override to higher priority queue
)
```

### Timezone

Schedule evaluated in specified timezone:

```python
# Runs at 9 AM New York time (EST/EDT)
TaskSchedule(
    name="morning-task",
    task_name="...",
    pattern=DailySchedule(time=time(9, 0, 0)),
    timezone="America/New_York",
)
```

`timezone` is an IANA name that Python `zoneinfo` reads. Common values:

- `"UTC"` - Coordinated Universal Time
- `"America/New_York"` - US Eastern
- `"America/Los_Angeles"` - US Pacific
- `"Europe/London"` - UK
- `"Asia/Tokyo"` - Japan

### Catch-Up

When `catch_up_missed=True`, missed runs are executed:

```python
TaskSchedule(
    name="hourly-sync",
    task_name="sync_data",
    pattern=IntervalSchedule(hours=1),
    catch_up_missed=True,
)
```

Use for:

- Data synchronization (must process all intervals)
- Compliance reporting (all periods must be covered)

Avoid for:

- Notifications (users don't want 24 emails at once)
- Status updates (only latest matters)

What happens with each setting is on [Catch-Up Logic](/horsies/scheduling/scheduler-overview/#catch-up-logic).

## Disabling Schedules

Disable individual schedules:

<!-- check: skip elided -- ... stands for a config shown on other pages -->
```python
TaskSchedule(
    name="deprecated-job",
    task_name="old_task",
    pattern=...,
    enabled=False,  # Won't run
)
```

Disable entire scheduler:

<!-- check: skip elided -- ... stands for a config shown on other pages -->
```python
ScheduleConfig(
    enabled=False,  # No schedules run
    schedules=[...],
)
```

## Configuration Changes

A changed schedule takes a new `next_run_at` at the next scheduler start. See [Configuration Changes](/horsies/scheduling/scheduler-overview/#configuration-changes).

## Validation

The scheduler validates each schedule at startup. The rules are on [Validation](/horsies/scheduling/scheduler-overview/#validation).

<!-- check: skip elided -- ... stands for a config shown on other pages -->
```python
# Will fail at startup:
TaskSchedule(
    name="bad",
    task_name="task_requiring_arg",  # def task_requiring_arg(required_param: str)
    pattern=...,
    # Missing: kwargs={"required_param": "value"}
)
```

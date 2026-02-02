---
title: Scheduling Tasks
summary: Run tasks on recurring schedules.
related: [03-defining-workflows]
tags: [quickstart, scheduling]
---

For full scheduler documentation, see [Scheduler Overview](/scheduling/scheduler-overview/).

## Scheduled Tasks

```python
from horsies import TaskResult, TaskError
from .config import app

@app.task("sync_inventory", queue_name="low")
def sync_inventory() -> TaskResult[dict, TaskError]:
    return TaskResult(ok={
        "synced_items": 150,
        "status": "completed",
    })

@app.task("generate_shipping_report", queue_name="low")
def generate_shipping_report() -> TaskResult[dict, TaskError]:
    return TaskResult(ok={
        "report_id": "RPT-2024-001",
        "orders_shipped": 42,
    })

@app.task("cleanup_completed_orders", queue_name="low")
def cleanup_completed_orders() -> TaskResult[dict, TaskError]:
    return TaskResult(ok={
        "archived_count": 100,
    })
```

## Schedule Configuration

```python
from datetime import time as datetime_time
from horsies import AppConfig, PostgresConfig, QueueMode, CustomQueueConfig
from horsies import (
    ScheduleConfig,
    TaskSchedule,
    IntervalSchedule,
    DailySchedule,
    WeeklySchedule,
    Weekday,
)

schedule_config = ScheduleConfig(
    enabled=True,
    schedules=[
        TaskSchedule(
            name="inventory_sync",
            task_name="sync_inventory",
            pattern=IntervalSchedule(minutes=30),
        ),
        TaskSchedule(
            name="daily_shipping_report",
            task_name="generate_shipping_report",
            pattern=DailySchedule(time=datetime_time(2, 0, 0)),
        ),
        TaskSchedule(
            name="weekly_cleanup",
            task_name="cleanup_completed_orders",
            pattern=WeeklySchedule(
                days=[Weekday.SUNDAY],
                time=datetime_time(3, 0, 0),
            ),
        ),
    ],
    check_interval_seconds=30,
)

config = AppConfig(
    queue_mode=QueueMode.CUSTOM,
    custom_queues=[
        CustomQueueConfig(name="urgent", priority=1, max_concurrency=10),
        CustomQueueConfig(name="standard", priority=50, max_concurrency=20),
        CustomQueueConfig(name="low", priority=100, max_concurrency=5),
    ],
    broker=PostgresConfig(
        database_url="postgresql+psycopg://user:password@localhost:5432/shipping_db",
    ),
    schedule=schedule_config,
)
```

## Schedule Patterns

| Pattern | Example | Use Case |
|---------|---------|----------|
| `IntervalSchedule` | `minutes=30` | Frequent syncs |
| `HourlySchedule` | `minute=0` | Hourly reports |
| `DailySchedule` | `time=02:00` | Nightly jobs |
| `WeeklySchedule` | `SUNDAY 03:00` | Weekly cleanup |
| `MonthlySchedule` | `day=1` | Monthly reports |

## Running the Scheduler

```bash
horsies scheduler myapp.config:app --loglevel=INFO
```

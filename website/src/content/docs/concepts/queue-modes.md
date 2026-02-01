---
title: Queue Modes
summary: DEFAULT vs CUSTOM queue modes, priorities, and per-queue concurrency.
related: [../../configuration/app-config, ../../workers/concurrency]
tags: [concepts, queues, mode, configuration]
---

# Queue Modes

Horsies supports two queue modes: DEFAULT for simple single-queue setups, and CUSTOM for multiple named queues with priorities and concurrency limits.

## DEFAULT Mode

The simplest configuration - all tasks go to a single "default" queue.

```python
from horsies import Horsies, AppConfig, PostgresConfig, TaskResult, TaskError
from horsies.core.models.queues import QueueMode

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(database_url="postgresql+psycopg://..."),
)
app = Horsies(config)

@app.task("my_task")  # Goes to "default" queue
def my_task() -> TaskResult[str, TaskError]:
    return TaskResult(ok="done")
```

Characteristics:

- Single queue named "default"
- All tasks processed in order ( priority=100 --> means lowest )
- No per-queue concurrency limits
- Cannot specify `queue_name` in task decorator
- FIFO applies

## CUSTOM Mode

Multiple named queues with individual priorities and concurrency limits.
Queues limits are supported within the same deployed device, no need for deploying a separate device for each queue.

```python
from horsies import Horsies, AppConfig, PostgresConfig, TaskResult, TaskError
from horsies.core.models.queues import QueueMode, CustomQueueConfig

config = AppConfig(
    queue_mode=QueueMode.CUSTOM,
    custom_queues=[
        CustomQueueConfig(name="critical", priority=1, max_concurrency=10),
        CustomQueueConfig(name="normal", priority=50, max_concurrency=5),
        CustomQueueConfig(name="low", priority=100, max_concurrency=2),
    ],
    broker=PostgresConfig(database_url="postgresql+psycopg://..."),
)
app = Horsies(config)

@app.task("urgent_alert", queue_name="critical")
def urgent_alert() -> TaskResult[str, TaskError]:
    return TaskResult(ok="alerted")

@app.task("process_order", queue_name="normal")
def process_order() -> TaskResult[str, TaskError]:
    return TaskResult(ok="processed")

@app.task("ship_item", queue_name="shipping")
def ship_to_address() -> TaskResult[str, TaskError]:
    return TaskResult(ok="shipped")
```

## CustomQueueConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | Unique queue identifier |
| `priority` | `int` | 1 | Priority level (1=highest, 100=lowest) |
| `max_concurrency` | `int` | 5 | Max simultaneous RUNNING tasks in this queue |

## How Priority Works

Workers claim tasks in priority order:

1. All priority=1 queues checked first
2. Then priority=2, priority=3, etc.
3. Within same priority, FIFO by `sent_at`

This means "critical" tasks (priority=1) are always processed before "low" tasks (priority=100) when both are pending.

## Queue Concurrency

`max_concurrency` limits how many tasks from a queue can be RUNNING simultaneously **across the entire cluster**:

- If `critical` has `max_concurrency=10`, at most 10 critical tasks run at once
- This is checked during claiming, not just per-worker
- Useful for rate-limiting external API calls or database-heavy operations

## When to Use Each Mode

**DEFAULT mode** when:

- Simple application with uniform task priority
- No need for per-queue rate limiting
- Getting started / prototyping

**CUSTOM mode** when:

- Different task types need different priorities
- Some operations need rate limiting (e.g., external API calls)
- You want to prevent low-priority operations from blocking urgent tasks
- Resource isolation is important

## Validation

Queue configuration is validated at multiple points:

**At app creation**: Queue names must be unique, custom_queues required in CUSTOM mode

**At task definition**: `queue_name` must match a configured queue (CUSTOM mode) or be omitted (DEFAULT mode)

**At task send time**: Queue name re-validated against current configuration

```python
# This raises ConfigurationError (E103) at definition time:
@app.task("bad_task", queue_name="nonexistent")
def bad_task() -> TaskResult[str, TaskError]:
    ...
```

```text
error[E103]: invalid queue_name 'nonexistent'
  --> /app/tasks.py:12
   |
12 | @app.task("bad_task", queue_name="nonexistent")
   | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
   = note: valid queues: ['critical', 'normal', 'low']

   = help:
        use one of the configured queue names
```

## Cluster-Wide Cap

In addition to per-queue concurrency, you can limit total RUNNING tasks across all queues:

```python
config = AppConfig(
    queue_mode=QueueMode.CUSTOM,
    custom_queues=[...],
    cluster_wide_cap=50,  # Max 50 tasks running across entire cluster
    broker=PostgresConfig(...),
)
```

This is useful when:

- Database or external service has connection limits
- You want to cap overall resource usage
- Running multiple worker instances

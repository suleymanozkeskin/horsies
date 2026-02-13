---
title: Getting Started
summary: Minimal working example with a task producer and worker.
related: [../../tasks/defining-tasks, ../../tasks/sending-tasks, ../../concepts/task-lifecycle]
tags: [quickstart, installation, tutorial]
---

## Prerequisites

- PostgreSQL 12+
- Python 3.10+

## Installation

```bash
uv add horsies
```

## 1. Create the App Instance

Create `instance.py`:

```python
from horsies import Horsies, AppConfig, PostgresConfig, TaskResult, TaskError

config = AppConfig(
    broker=PostgresConfig(
        database_url="postgresql+psycopg://user:password@localhost:5432/mydb",
    ),
)

app = Horsies(config)
```

## 2. Define Tasks

Add task definitions to `instance.py`:

```python
@app.task("add_numbers")
def add_numbers(a: int, b: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=a + b)

@app.task("process_data")
def process_data(data: dict) -> TaskResult[str, TaskError]:
    if not data:
        return TaskResult(err=TaskError(
            error_code="EMPTY_DATA",
            message="Data cannot be empty",
        ))
    return TaskResult(ok=f"Processed {len(data)} items")

# Register task modules for worker discovery
app.discover_tasks(["instance"])
```

Requirements:

- Every task returns `TaskResult[T, TaskError]`
- `TaskResult(ok=value)` for success
- `TaskResult(err=TaskError(...))` for domain errors
- `app.discover_tasks([...])` registers modules for workers

## 3. Send Tasks

Create `producer.py`:

```python
from instance import app, add_numbers, process_data

# Send and wait for result
handle = add_numbers.send(5, 3)
result = handle.get()

if result.is_err():
    handle_error(result.err_value)
else:
    do_something(result.ok_value)


# Send with blocking timeout
handle = process_data.send({"key": "value"})
result = handle.get(timeout_ms=5000)
```

## 4. Run the Worker

```bash
horsies worker instance:app --processes=8 --loglevel=INFO
```

The worker connects to PostgreSQL, subscribes to notifications, claims tasks, executes them, and stores results.

## 5. Async Usage

For async frameworks (FastAPI, etc.):

```python
from instance import add_numbers

async def my_endpoint():
    handle = await add_numbers.send_async(10, 20)
    result = await handle.get_async(timeout_ms=10000)
    return {"result": result.ok if result.is_ok() else None}
```

These async APIs are producer-side I/O helpers; task execution still happens in worker processes.

## Project Structure

Simplest start — an overview of components:

```text
myproject/
├── instance.py      # App configuration and Horsies instance
├── tasks.py         # Task definitions (@app.task)
├── workflows.py     # Workflow definitions (WorkflowDefinition, TaskNode)
├── producer.py      # Code that sends tasks / starts workflows
├── worker.py        # Custom worker entry point (optional)
└── scheduler.py     # Scheduled task configuration (optional)
```

Adjust to your project needs.
Isolate entry points, configs, and workflow definitions.

## Next Steps

- [Task Lifecycle](../../concepts/task-lifecycle) — task states and transitions
- [Queue Modes](../../concepts/queue-modes) — multiple queues
- [Scheduler Overview](../../scheduling/scheduler-overview) — scheduled tasks
- [Recovery Config](../../configuration/recovery-config) — crash recovery

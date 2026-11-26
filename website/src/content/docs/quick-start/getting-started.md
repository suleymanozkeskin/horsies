---
title: Getting Started
section: python
summary: Minimal working example with a task producer and worker.
related: [../../tasks/defining-tasks, ../../tasks/sending-tasks, ../../concepts/task-lifecycle]
tags: [quickstart, installation, tutorial]
---

## Prerequisites

- PostgreSQL 14+
- Python 3.13+
- libpq 17+ for a time limit on the cancel request of an interrupted query. The `horsies[pg-binary]` extra bundles its own libpq. See [psycopg and libpq](/horsies/configuration/broker-connections/#psycopg-and-libpq)

## Installation

```bash
uv add horsies
```

Prebuilt wheels are for Linux x86_64 and aarch64 (`manylinux_2_28`) and macOS arm64. On other platforms, pip and uv build horsies from the source distribution. The build needs a Rust toolchain and a C compiler; see [The native extension](/horsies/migrations/migration-to-0-8-0/#the-native-extension).

## 1. Create the App Instance

Create `instance.py`:

```python
from pydantic import SecretStr
from horsies import (
    Horsies,
    AppConfig,
    IsolatedChildren,
    PostgresConfig,
    Ok,
    Err,
    Result,
    TaskError,
    JsonValue,
    WorkerProfile,
)

config = AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr("postgresql://user:password@localhost:5432/mydb"),
    ),
    worker_profiles={
        "default": WorkerProfile(isolated=IsolatedChildren(count=8)),
    },
)

app = Horsies(config)
```

`worker_profiles` holds the settings that a worker starts with. See [Worker Profiles](../../workers/worker-profiles).

## 2. Define Tasks

Add task definitions to `instance.py`:

```python
@app.task("add_numbers")
def add_numbers(*, a: int, b: int) -> Result[int, TaskError]:
    return Ok(a + b)

@app.task("process_data")
def process_data(*, data: dict[str, JsonValue]) -> Result[str, TaskError]:
    if not data:
        return Err(TaskError(
            code="EMPTY_DATA",
            message="Data cannot be empty",
        ))
    return Ok(f"Processed {len(data)} items")

# Register task modules for worker discovery
app.discover_tasks(["instance"])
```

Requirements:

- Every task returns `Result[T, TaskError]`
- `Ok(value)` for success
- `Err(TaskError(...))` for domain errors
- `app.discover_tasks([...])` registers modules for workers

## 3. Send Tasks

Create `producer.py`:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Ok, Err, Succeeded, Failed
from instance import app, add_numbers, process_data

# Send and wait for result
match add_numbers.send(a=5, b=3):
    case Ok(handle):
        match handle.get():
            case Ok(Succeeded(total)):
                do_something(total)
            case Ok(Failed(task_error)):
                handle_error(task_error)
            case Err(read_error):
                print(f"Read failed: {read_error.code} - {read_error.message}")
    case Err(send_err):
        print(f"Send failed: {send_err.code} - {send_err.message}")

# Send with blocking timeout
match process_data.send(data={"key": "value"}):
    case Ok(handle):
        result = handle.get(timeout_ms=5000)
    case Err(send_err):
        print(f"Send failed: {send_err.code}")
```

## 4. Run the Worker

```bash
horsies worker instance:app --profile default --loglevel=INFO
```

The worker starts the 8 children of the `default` profile, connects to PostgreSQL, subscribes to notifications, claims tasks, executes them, and stores results.

## 5. Async Usage

For async frameworks (FastAPI, etc.):

```python
from horsies import Ok, Err, Succeeded, Failed
from instance import add_numbers

async def my_endpoint():
    match await add_numbers.send_async(a=10, b=20):
        case Ok(handle):
            match await handle.get_async(timeout_ms=10000):
                case Ok(Succeeded(total)):
                    return {"result": total}
                case Ok(Failed(task_error)):
                    return {"error": task_error.message}
                case Err(read_error):
                    return {"error": read_error.message}
        case Err(send_err):
            return {"error": send_err.message}
```

`get()` and `get_async()` return `ReadResult[T, TaskError]`. `Ok(Succeeded(value))` holds the value of the task. `Ok(Failed(task_error))` holds the error of the task. `Err(read_error)` means the read gave no outcome, for example `WaitTimeout`.

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

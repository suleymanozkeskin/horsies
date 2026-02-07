---
title: Defining Tasks
summary: How to define tasks with the @app.task decorator.
related: [sending-tasks, retrieving-results, ../../concepts/result-handling]
tags: [tasks, decorator, definition]
---

# Defining Tasks

Define tasks by decorating functions with `@app.task()` and returning `TaskResult[T, TaskError]`. For argument serialization rules, see `sending-tasks`.

**Note:** Task functions must be synchronous (`def`). `async def` task functions are not supported yet. Use `.send_async()` / `.get_async()` on the producer side for non-blocking I/O.

## How To

### Basic Task

```python
from horsies import Horsies, AppConfig, PostgresConfig, TaskResult, TaskError

config = AppConfig(
    broker=PostgresConfig(database_url="postgresql+psycopg://..."),
)
app = Horsies(config)

@app.task("process_order")
def process_order(order_id: int) -> TaskResult[str, TaskError]:
    # Do work
    return TaskResult(ok=f"Order {order_id} processed")
```

### Return Success

```python
@app.task("compute")
def compute(x: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=x * 2)
```

For void tasks:

```python
@app.task("fire_and_forget")
def fire_and_forget() -> TaskResult[None, TaskError]:
    do_something()
    return TaskResult(ok=None)
```

### Return Errors

Domain errors (expected failures):

```python
@app.task("validate_input")
def validate_input(data: dict) -> TaskResult[dict, TaskError]:
    if not data.get("email"):
        return TaskResult(err=TaskError(
            error_code="MISSING_EMAIL",
            message="Email is required",
            data={"field": "email"},
        ))
    return TaskResult(ok=data)
```

Unhandled exceptions are wrapped automatically:

```python
@app.task("might_crash")
def might_crash() -> TaskResult[str, TaskError]:
    raise ValueError("Something went wrong")
    # Becomes TaskResult(err=TaskError(
    #     error_code=LibraryErrorCode.UNHANDLED_EXCEPTION,
    #     message="Unhandled exception in task might_crash: ...",
    # ))
```

### Configure Retries

With explicit try/except:

```python
from horsies import RetryPolicy

@app.task(
    "some_api_call",
    retry_policy=RetryPolicy.exponential(
        base_seconds=30,
        max_retries=5,
    ),
    auto_retry_for=["TIMEOUT", "CONNECTION_ERROR"],
)
def some_api_call() -> TaskResult[dict, TaskError]:
    try:
        result = call_external_api()
        return TaskResult(ok=result)
    except TimeoutError:
        return TaskResult(err=TaskError(error_code="TIMEOUT", message="Request timed out"))
    except ConnectionError:
        return TaskResult(err=TaskError(error_code="CONNECTION_ERROR", message="Connection failed"))
```

Or with the exception mapper (no try/except needed):

```python
@app.task(
    "some_api_call",
    retry_policy=RetryPolicy.exponential(
        base_seconds=30,
        max_retries=5,
    ),
    auto_retry_for=["TIMEOUT", "CONNECTION_ERROR"],
    exception_mapper={
        TimeoutError: "TIMEOUT",
        ConnectionError: "CONNECTION_ERROR",
    },
)
def some_api_call() -> TaskResult[dict, TaskError]:
    result = call_external_api()
    return TaskResult(ok=result)
```

For backoff strategies, jitter, and auto-retry triggers, see [Retry Policy](../retry-policy).

### Assign to Queue (CUSTOM Mode)

```python
@app.task("urgent_task", queue_name="critical")
def urgent_task() -> TaskResult[str, TaskError]:
    ...
```

### Register Tasks for Workers

```python
# Dotted module paths (recommended)
app.discover_tasks(["myapp.tasks", "myapp.jobs.worker_tasks"])

# Or file paths
app.discover_tasks(["tasks.py", "more_tasks.py"])
```

`discover_tasks` imports the **exact entries listed**. Dotted module paths use `importlib.import_module()`, while `.py` entries are imported by file path — it does not recursively scan submodules. To discover tasks in `myapp.tasks.scraping`, either list it explicitly or export the decorated functions from `myapp.tasks.__init__.py`.

## Things to Avoid

**Don't omit the return type annotation.**

```python
# Wrong - raises TypeError at definition time
@app.task("bad_task")
def bad_task():
    return {"status": "done"}

# Correct
@app.task("good_task")
def good_task() -> TaskResult[dict, TaskError]:
    return TaskResult(ok={"status": "done"})
```

**Don't reuse task names.**

```python
@app.task("duplicate_name")
def task_one() -> TaskResult[str, TaskError]:
    ...

@app.task("duplicate_name")  # Raises error
def task_two() -> TaskResult[str, TaskError]:
    ...
```

## API Reference

### `@app.task(task_name, **options)`

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `task_name` | `str` | Yes | Unique task identifier |
| `queue_name` | `str` | No | Target queue (CUSTOM mode only) |
| `retry_policy` | `RetryPolicy` | No | Retry timing and backoff |
| `auto_retry_for` | `list[str]` | No | Error codes that trigger retry |
| `good_until` | `datetime` | No | Task expiry deadline (set at definition time) |
| `exception_mapper` | `dict[type[BaseException], str]` | No | Maps exception classes to error codes |
| `default_unhandled_error_code` | `str` | No | Error code for unmapped exceptions (overrides global) |

**Returns:** Decorated function that can be called directly or via `.send()` / `.send_async()`.

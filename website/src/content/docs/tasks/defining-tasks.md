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
from horsies import Horsies, AppConfig, PostgresConfig, TaskResult, TaskError, JsonValue

config = AppConfig(
    broker=PostgresConfig(database_url="postgresql+psycopg://..."),
)
app = Horsies(config)

@app.task("process_order")
def process_order(*, order_id: int) -> TaskResult[str, TaskError]:
    # Do work
    return TaskResult(ok=f"Order {order_id} processed")
```

### Parameters are keyword-only

Every task parameter must be keyword-only: declare them after a bare `*,`. The leading `*` forces callers to pass arguments by name.

```python
@app.task("send_email")
def send_email(*, to: str, subject: str) -> TaskResult[None, TaskError]:
    ...
```

The queue transmits arguments by name only — the signature is the wire contract. A positionally-bindable parameter (`def f(order_id: int)`) is rejected at registration, so the error surfaces when the app loads, not at send time. With keyword-only parameters, a positional call (`process_order.send(1)`) also fails the type checker at the call site rather than at runtime.

### Return Success

```python
@app.task("compute")
def compute(*, x: int) -> TaskResult[int, TaskError]:
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
def validate_input(*, data: dict[str, JsonValue]) -> TaskResult[dict[str, JsonValue], TaskError]:
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
    #     error_code=OperationalErrorCode.UNHANDLED_EXCEPTION,
    #     message="Unhandled exception in task might_crash: ...",
    # ))
```

### Configure Retries

With explicit try/except:

```python
from horsies import RetryPolicy, TaskResult, TaskError, JsonValue

@app.task(
    "some_api_call",
    retry_policy=RetryPolicy.exponential(
        base_seconds=30,
        max_retries=5,
        auto_retry_for=["TIMEOUT", "CONNECTION_ERROR"],
    ),
)
def some_api_call() -> TaskResult[dict[str, JsonValue], TaskError]:
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
        auto_retry_for=["TIMEOUT", "CONNECTION_ERROR"],
    ),
    exception_mapper={
        TimeoutError: "TIMEOUT",
        ConnectionError: "CONNECTION_ERROR",
    },
)
def some_api_call() -> TaskResult[dict[str, JsonValue], TaskError]:
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

### Set an Execution Timeout

`timeout_ms` bounds task execution time, measured from the moment a worker dispatches the task to a child process. Minimum 1000 (1 second).

```python
@app.task("resize_video", timeout_ms=120_000)
def resize_video(*, video_id: str) -> TaskResult[str, TaskError]:
    ...
```

On expiry, the worker kills the child process and the task fails with `TASK_TIMEOUT`. If the deadline elapses before user code starts, the task is requeued instead. Timeouts are terminal by default; opt into retries by adding `"TASK_TIMEOUT"` to `auto_retry_for`:

```python
@app.task(
    "resize_video",
    timeout_ms=120_000,
    retry_policy=RetryPolicy.fixed([60, 300], auto_retry_for=["TASK_TIMEOUT"]),
)
def resize_video(*, video_id: str) -> TaskResult[str, TaskError]:
    ...
```

Killing the child restarts the worker's process pool: sibling tasks running on the same worker are interrupted and recovered through crash recovery (requeued, or retried per their own policies). Size `timeout_ms` as a hang backstop well above normal runtime, not as routine control flow.

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
def good_task() -> TaskResult[dict[str, JsonValue], TaskError]:
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
| `retry_policy` | `RetryPolicy` | No | Retry timing, backoff, and auto-retry triggers |
| `exception_mapper` | `dict[type[BaseException], str]` | No | Maps exception classes to error codes |
| `default_unhandled_error_code` | `str` | No | Error code for unmapped exceptions (overrides global) |
| `timeout_ms` | `int` | No | Execution time limit in milliseconds (>= 1000), measured from dispatch. On expiry the child process is killed and the task fails with `TASK_TIMEOUT` |

**Returns:** Decorated function that can be called directly or via `.send()` / `.send_async()`.

`good_until` is not accepted here — task expiry deadlines are a per-send concern. See [Sending Tasks — Set a Task Deadline](sending-tasks#set-a-task-deadline).

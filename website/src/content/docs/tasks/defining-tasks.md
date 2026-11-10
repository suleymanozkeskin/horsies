---
title: Defining Tasks
summary: How to define tasks with the @app.task decorator.
related: [sending-tasks, retrieving-results, ../../concepts/result-handling]
tags: [tasks, decorator, definition]
---

# Defining Tasks

Define tasks by decorating functions with `@app.task()` and returning `TaskResult[T, TaskError]`. A task function is a `def` or an `async def` function. For argument serialization rules, see `sending-tasks`.

## How To

### Basic Task

```python
from pydantic import SecretStr
from horsies import Horsies, AppConfig, PostgresConfig, TaskResult, TaskError, JsonValue

config = AppConfig(
    broker=PostgresConfig(database_url=SecretStr("postgresql+psycopg://...")),
)
app = Horsies(config)

@app.task("process_order")
def process_order(*, order_id: int) -> TaskResult[str, TaskError]:
    # Do work
    return TaskResult(ok=f"Order {order_id} processed")
```

### Async Tasks

Define a task with `async def` to await I/O in its body. `@app.task()` checks it as it checks a `def` task, and it returns a `TaskResult` in the same way.

```python
import asyncio

@app.task("fetch_price", timeout_ms=30_000)
async def fetch_price(*, symbol: str) -> TaskResult[float, TaskError]:
    await asyncio.sleep(0.1)  # stands in for an awaited client call
    return TaskResult(ok=101.5)
```

A worker child runs one task at a time, `def` or `async def`. Each child keeps one event loop until the child stops, and an async task runs on that loop. An async client that a module keeps stays open across the tasks of one child.

The loop runs only while an async task runs. A `def` task runs with no running event loop, so it can call `asyncio.run()`.

An async task must await the work that it starts:

- When an async task ends and asyncio tasks that it started, for example with `asyncio.create_task()`, are still on the loop, the worker replaces the child after that task. The next task runs in a new child, so no task shares the loop with that work. When the old child stops, it cancels that work, within 5 seconds.
- A client that keeps its own background tasks on the loop, for example a keepalive or a consumer task, causes a child replacement after each async task.
- A thread that `asyncio.to_thread()` starts can outlive its task, for example when a timeout ends the wait for it. The thread is not an asyncio task, so it does not cause a replacement. When the worker stops the child, the child does not wait for that thread past the 5-second close: it exits, and its `atexit` handlers do not run.
- The worker log names the coroutine of each asyncio task that was left, for up to 10 tasks, for example `Retiring child pid=4242: task <id> left 1 task(s) on its event loop: refresh_session`.

A task that cancels a helper task in `finally` must also await it. `cancel()` only asks the helper to stop. A helper that awaits while it stops, for example to close a session, is still on the loop when the task ends, and the worker replaces the child. Await the helper after the cancel:

```python
async def refresh_session() -> None:
    try:
        while True:
            await asyncio.sleep(30)  # stands in for a keepalive call
    finally:
        await asyncio.sleep(0)  # stands in for closing the session


@app.task("watch_price")
async def watch_price(*, symbol: str) -> TaskResult[float, TaskError]:
    helper = asyncio.create_task(refresh_session())
    try:
        await asyncio.sleep(0.1)  # stands in for the work of the task
        return TaskResult(ok=101.5)
    finally:
        helper.cancel()
        # gather returns the CancelledError of the helper instead of raising it.
        await asyncio.gather(helper, return_exceptions=True)
```

A child creates its event loop at boot, before it reports ready, when the app has at least one `async def` task. A child of an app with no `async def` task creates no loop. When the loop cannot be created, for example because the process has no file descriptor left or an event loop policy raises:

- The child fails its boot, and no task reaches it.
- The worker counts it as a failed child start. After three failed attempts, the worker exits non-zero, so its supervisor restarts it. See [Worker Architecture](/horsies/workers/worker-architecture/#what-it-handles).
- The log and the error name the cause, for example `the child could not create the event loop for async tasks: OSError: [Errno 24] Too many open files`.

In an async task, send with `.send_async()` or `.schedule_async()`. The sync `.send()` and `.schedule()` refuse a running event loop: they return `Err(TaskSendAsyncContext)` and enqueue nothing. See [Sending Tasks](sending-tasks).

```python
from horsies import Ok, Err

@app.task("refresh_price")
async def refresh_price(*, symbol: str) -> TaskResult[str, TaskError]:
    match await fetch_price.send_async(symbol=symbol):
        case Ok(handle):
            return TaskResult(ok=handle.task_id)
        case Err(send_err):
            return TaskResult(err=TaskError(error_code="SEND_FAILED", message=send_err.message))
```

`timeout_ms` works as for a `def` task: on expiry, the worker kills the child. An `async def` function that uses `yield` is refused with `HRS-101`.

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

<!-- check: skip partial -- uses names that the page does not define -->
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

<!-- check: skip partial -- uses names that the page does not define -->
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

<!-- check: skip partial -- uses names that the page does not define -->
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

`timeout_ms` bounds task execution time, measured from the moment a worker gives the task to a child process. Time that a task waits for a free child does not count. The value is from 1000 (1 second) to `MAX_TASK_TIMEOUT_MS` (4,294,967,295, about 49.7 days), which is also the limit of the Rust port. `@app.task` refuses a value outside that range with `TASK_INVALID_OPTIONS`. A queued row that holds a value outside it fails with `WORKER_SERIALIZATION_ERROR` when a worker claims it, and no user code runs.

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

The kill goes to the child and to the subprocesses in its session. Other tasks on the same worker continue. A subprocess that calls `setsid` itself leaves the session, and the kill does not reach it. The worker starts a new child in that slot, so size `timeout_ms` as a hang backstop well above normal runtime, not as routine control flow.

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

<!-- check: skip elided -- shows a wrong form next to the right one -->
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
| `task_name` | `str` | Yes | Unique task identifier. An empty name raises `TaskDefinitionError` (`HRS-102`) |
| `queue_name` | `str` | No | Target queue (CUSTOM mode only) |
| `retry_policy` | `RetryPolicy` | No | Retry timing, backoff, and auto-retry triggers |
| `exception_mapper` | `dict[type[BaseException], str]` | No | Maps exception classes to error codes |
| `default_unhandled_error_code` | `str` | No | Error code for unmapped exceptions (overrides global) |
| `timeout_ms` | `int` | No | Execution time limit in milliseconds, from 1000 to `MAX_TASK_TIMEOUT_MS` (4,294,967,295), measured from the moment a child takes the task. On expiry the child process is killed and the task fails with `TASK_TIMEOUT` |

**Returns:** For a `def` function, a `TaskFunction`: a direct call returns the `TaskResult`. For an `async def` function, an `AsyncTaskFunction`: a direct call returns a coroutine, and `await` gives the `TaskResult`. Both send with `.send()` / `.send_async()`.

`good_until` is not accepted here — task expiry deadlines are a per-send concern. See [Sending Tasks — Set a Task Deadline](sending-tasks#set-a-task-deadline).

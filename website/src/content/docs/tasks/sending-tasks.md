---
title: Sending Tasks
summary: How to enqueue tasks for background execution.
related: [defining-tasks, retrieving-results]
tags: [tasks, send, async, scheduling]
---

# Sending Tasks

Enqueue tasks with `.send()`, `.send_async()`, `.schedule()`, or `.schedule_async()`. All return a `TaskSendResult[TaskHandle[T]]` -- a `Result` type that is either `Ok(TaskHandle)` on success or `Err(TaskSendError)` on failure.

## How To

### Send a Task (Sync)

```python
from horsies import Ok, Err
from instance import my_task

match my_task.send(name="alice", count=3):
    case Ok(handle):
        print(f"Task submitted: {handle.task_id}")
    case Err(send_err):
        print(f"Send failed: {send_err.code} - {send_err.message}")
```

### Send a Task (Async)

```python
from horsies import Ok, Err

async def my_endpoint():
    match await my_task.send_async(name="alice", count=3):
        case Ok(handle):
            return {"task_id": handle.task_id}
        case Err(send_err):
            return {"error": send_err.message}
```
`send_async()` only enqueues the task. Use `handle.get_async()` if you want to wait for completion.

The sync variants are blocking database round trips, so calling them from a
running event loop would stall every coroutine on it. They fail closed
instead: `.send()`, `.schedule()`, `.retry_send()`, and `.retry_schedule()`
called inside a running loop return `Err(TaskSendError(ASYNC_CONTEXT))`
without touching the broker. Use the `*_async` variant there.

### Set a Task Deadline

Use `.with_options(good_until=...)` to set a per-send expiry deadline. If the task is not executed before the deadline, it transitions to `EXPIRED`.

```python
from datetime import datetime, timedelta, timezone
from horsies import Ok, Err

deadline = datetime.now(timezone.utc) + timedelta(minutes=5)

match my_task.with_options(good_until=deadline).send(name="alice", count=3):
    case Ok(handle):
        print(f"Task submitted with 5-minute deadline: {handle.task_id}")
    case Err(err):
        print(f"Send failed: {err.code}")
```

`good_until` must be a timezone-aware `datetime`. Naive datetimes return `Err(VALIDATION_FAILED)`.

`with_options()` works with all send methods:

```python
opts = my_task.with_options(good_until=deadline)

opts.send(name="alice", count=3)                        # sync
await opts.send_async(name="alice", count=3)            # async
opts.schedule(60, name="alice", count=3)                # delayed
await opts.schedule_async(60, name="alice", count=3)    # delayed, async
```

For workflow nodes, use `.node(good_until=...)` instead — see [Typed Node Builder](../concepts/workflows/typed-node-builder).

### Delay Execution

```python
from horsies import Ok, Err

match my_task.schedule(60, name="alice", count=3):
    case Ok(handle):
        print(f"Scheduled: {handle.task_id}")
    case Err(err):
        print(f"Schedule failed: {err.code}")
```

From async code, use `.schedule_async()`:

```python
match await my_task.schedule_async(60, name="alice", count=3):
    case Ok(handle):
        print(f"Scheduled: {handle.task_id}")
    case Err(err):
        print(f"Schedule failed: {err.code}")
```

### Wait for Result

```python
from horsies import Ok, Err

match my_task.send(name="alice", count=3):
    case Ok(handle):
        # Blocking wait
        result = handle.get()

        # With timeout (milliseconds)
        result = handle.get(timeout_ms=5000)

        # Async wait
        result = await handle.get_async(timeout_ms=5000)
    case Err(err):
        print(f"Send failed: {err.code}")
```
`get_async()` waits via broker notifications (LISTEN/NOTIFY) with a polling fallback.

### Fire and Forget

```python
# Send without waiting for result -- discard the TaskSendResult
my_task.send(name="alice", count=3)
```

### Pass Complex Arguments

Arguments must be keyword-only and JSON-serializable. Positional `.send(arg1, arg2)` is rejected with `Err(VALIDATION_FAILED)`. Pydantic models and dataclass instances are supported directly; the worker decodes them using the registered task's parameter type via `pydantic.TypeAdapter`.

```python
from horsies import Ok, Err

match process.send(data={"key": "value", "nested": {"a": 1}}, items=[1, 2, 3]):
    case Ok(handle):
        result = handle.get()
    case Err(err):
        print(f"Send failed: {err.code}")

# Pydantic models - pass the instance to preserve type metadata
order = Order(id=123, items=["a", "b"])
match process_order.send(order=order):
    case Ok(handle):
        result = handle.get()
    case Err(err):
        print(f"Send failed: {err.code}")
```

Pydantic models and dataclasses must be defined in importable modules (not `__main__` and not inside functions) so the worker can resolve the declared parameter type.

### Execute Directly (Skip Queue)

```python
# Runs immediately in current process (plain Python call; bypasses the queue)
result = my_task(name="alice", count=3)
```

Direct calls bypass the queue entirely. Library features do not apply:

- No retries (`retry_policy`)
- No persistence (task not recorded in database)
- No worker distribution
- No scheduling

Use only for **unit testing**. For production, always use `.send()` or `.send_async()`.

## Things to Avoid

**Don't call `.send()` at module level.**

```python
# Wrong - returns Err(TaskSendError(SEND_SUPPRESSED)) during worker import
# tasks.py
result = my_task.send(name="test")  # Err(SEND_SUPPRESSED)

# Correct - call from functions/endpoints
def process():
    match my_task.send(name="test"):
        case Ok(handle):
            ...
        case Err(err):
            ...
```

**Don't pass non-serializable objects.**

```python
# Wrong
my_task.send(connection=db_connection)

# Correct
my_task.send(connection_url=str(db_connection.url))
```

## Retrying Failed Sends

When `.send()` fails with `ENQUEUE_FAILED` (a transient broker error), use the retry methods to replay the exact same payload without re-supplying arguments. The `enqueue_sha` on the stored `TaskSendPayload` guarantees the retry carries the identical serialized payload.

```python
from horsies import Ok, Err

match my_task.send(name="alice", count=3):
    case Ok(handle):
        result = handle.get()
    case Err(err) if err.retryable:
        match my_task.retry_send(err):
            case Ok(handle):
                result = handle.get()
            case Err(retry_err):
                print(f"Retry failed: {retry_err.code}")
    case Err(err):
        print(f"Permanent failure: {err.code}")
```

Retry methods only accept `ENQUEUE_FAILED` errors. Passing `SEND_SUPPRESSED`, `VALIDATION_FAILED`, or `PAYLOAD_MISMATCH` returns `Err(TaskSendError(VALIDATION_FAILED))`.

### Automatic Retry via Config

Set `resend_on_transient_err=True` in `AppConfig` to have the library automatically retry transient enqueue failures before returning the error:

```python
config = AppConfig(
    resend_on_transient_err=True,
    # ...
)
```

## API Reference

### `.send(**kwargs) -> TaskSendResult[TaskHandle[T]]`

Enqueue task for immediate execution. Keyword-only — positional arguments are rejected with `Err(VALIDATION_FAILED)`.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `**kwargs` | task kwargs | Keyword arguments matching the task's signature |

**Returns:** `TaskSendResult[TaskHandle[T]]` -- `Ok(TaskHandle)` on success, `Err(TaskSendError)` on failure.

### `.send_async(**kwargs) -> TaskSendResult[TaskHandle[T]]`

Async variant of `.send()`. Use in async code (FastAPI, etc.).
This does not execute the task locally; it only enqueues. Keyword-only.

**Returns:** `TaskSendResult[TaskHandle[T]]`

### `.schedule(delay, **kwargs) -> TaskSendResult[TaskHandle[T]]`

Enqueue task for delayed execution. Task arguments are keyword-only.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `delay` | `int` | Seconds to wait before task becomes claimable |
| `**kwargs` | task kwargs | Keyword arguments matching the task's signature |

**Returns:** `TaskSendResult[TaskHandle[T]]`

### `.schedule_async(delay, **kwargs) -> TaskSendResult[TaskHandle[T]]`

Async variant of `.schedule()`. Use in async code (FastAPI, etc.).
Same `delay` validation as `.schedule()`.

**Returns:** `TaskSendResult[TaskHandle[T]]`

### `.with_options(*, good_until=None) -> TaskSendOptions[P, T]`

Return a per-send options builder. The returned object exposes `.send()`, `.send_async()`, `.schedule()`, and `.schedule_async()` with the overridden options applied.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `good_until` | `datetime \| None` | Task expiry deadline (must be timezone-aware) |

**Returns:** `TaskSendOptions[P, T]` — a builder with `.send()`, `.send_async()`, `.schedule()`, and `.schedule_async()`.

Passing `good_until=None` explicitly clears any internally inherited deadline.

### `.retry_send(error) -> TaskSendResult[TaskHandle[T]]`

Retry a failed send using the stored payload from the error. Only valid for `ENQUEUE_FAILED` errors.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `error` | `TaskSendError` | The error from a previous `.send()` call |

**Returns:** `TaskSendResult[TaskHandle[T]]`

### `.retry_send_async(error) -> TaskSendResult[TaskHandle[T]]`

Async variant of `.retry_send()`.

### `.retry_schedule(error) -> TaskSendResult[TaskHandle[T]]`

Retry a failed schedule using the stored payload. Only valid for `ENQUEUE_FAILED` errors that originated from `.schedule()`.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `error` | `TaskSendError` | The error from a previous `.schedule()` call |

**Returns:** `TaskSendResult[TaskHandle[T]]`

### `.retry_schedule_async(error) -> TaskSendResult[TaskHandle[T]]`

Async variant of `.retry_schedule()`.

### `TaskSendResult[T]`

Type alias: `Result[T, TaskSendError]`. The `Ok` side is `TaskHandle[T]` when returned from send methods.

| Property/Method | Type | Description |
| --------------- | ---- | ----------- |
| `.is_ok()` | `bool` | True if send succeeded |
| `.is_err()` | `bool` | True if send failed |
| `.ok_value` | `T` | The `TaskHandle`; raises `ValueError` if error |
| `.err_value` | `TaskSendError` | The error; raises `ValueError` if success |

Use `is_ok(result)` / `is_err(result)` from `horsies` as type-narrowing guards.

### `TaskSendError`

| Field | Type | Description |
| ----- | ---- | ----------- |
| `code` | `TaskSendErrorCode` | Failure category |
| `message` | `str` | Human-readable description |
| `retryable` | `bool` | Whether the caller can retry with the same payload |
| `task_id` | `str \| None` | Generated task ID (`None` for `SEND_SUPPRESSED`, `VALIDATION_FAILED`) |
| `payload` | `TaskSendPayload \| None` | Serialized envelope for replay (`None` when no serialization happened) |
| `exception` | `BaseException \| None` | The original cause, if any |

### `TaskSendErrorCode`

| Code | Description | Retryable |
| ---- | ----------- | --------- |
| `SEND_SUPPRESSED` | Send suppressed during worker import/discovery | No |
| `ASYNC_CONTEXT` | Sync send/schedule called inside a running event loop; use the `*_async` variant | No |
| `VALIDATION_FAILED` | Argument serialization or validation failed | No |
| `ENQUEUE_FAILED` | Broker/database failure during enqueue | Yes |
| `PAYLOAD_MISMATCH` | Retry payload SHA does not match (payload was altered) | No |

### `TaskHandle[T]`

| Property/Method | Type | Description |
| --------------- | ---- | ----------- |
| `.task_id` | `str` | Unique task identifier |
| `.get(timeout_ms=None)` | `TaskResult[T, TaskError]` | Wait for result (blocking) |
| `.get_async(timeout_ms=None)` | `TaskResult[T, TaskError]` | Wait for result (async) |
| `.info(include_result=False, include_failed_reason=False)` | `BrokerResult[TaskInfo \| None]` | Fetch task metadata from broker |
| `.info_async(include_result=False, include_failed_reason=False)` | `BrokerResult[TaskInfo \| None]` | Async variant of `.info()` |

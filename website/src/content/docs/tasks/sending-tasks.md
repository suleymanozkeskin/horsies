---
title: Sending Tasks
summary: How to enqueue tasks for background execution.
related: [defining-tasks, retrieving-results]
tags: [tasks, send, async, scheduling]
---

# Sending Tasks

Enqueue tasks with `.send()`, `.send_async()`, or `.schedule()`. All return a `TaskSendResult[TaskHandle[T]]` -- a `Result` type that is either `Ok(TaskHandle)` on success or `Err(TaskSendError)` on failure.

## How To

### Send a Task (Sync)

```python
from instance import my_task

handle = my_task.send(arg1, arg2, key="value").unwrap()
print(f"Task submitted: {handle.task_id}")
```

Or with explicit error handling:

```python
from horsies import is_ok, is_err

send_result = my_task.send(arg1, arg2, key="value")
if is_ok(send_result):
    handle = send_result.ok_value
    print(f"Task submitted: {handle.task_id}")
else:
    error = send_result.err_value
    print(f"Send failed: {error.code} - {error.message}")
```

### Send a Task (Async)

```python
async def my_endpoint():
    send_result = await my_task.send_async(arg1, arg2)
    handle = send_result.unwrap()
    return {"task_id": handle.task_id}
```
`send_async()` only enqueues the task. Use `handle.get_async()` if you want to wait for completion.

### Delay Execution

```python
# Execute after 60 seconds
handle = my_task.schedule(60, arg1, arg2).unwrap()

# Execute after 5 minutes
handle = my_task.schedule(300, arg1, arg2).unwrap()
```

### Wait for Result

```python
handle = my_task.send(...).unwrap()

# Blocking wait
result = handle.get()

# With timeout (milliseconds)
result = handle.get(timeout_ms=5000)

# Async wait
result = await handle.get_async(timeout_ms=5000)
```
`get_async()` waits via broker notifications (LISTEN/NOTIFY) with a polling fallback.

### Fire and Forget

```python
# Send without waiting for result -- discard the TaskSendResult
my_task.send(arg1, arg2)
```

### Pass Complex Arguments

Arguments must be JSON-serializable. Pydantic models and dataclass instances are supported directly and rehydrated on the worker side.

```python
handle = process.send(
    data={"key": "value", "nested": {"a": 1}},
    items=[1, 2, 3],
).unwrap()

# Pydantic models - pass the instance to preserve type metadata
order = Order(id=123, items=["a", "b"])
handle = process_order.send(order=order).unwrap()

# Use model_dump() only when a plain dict is required
handle = process_order.send(order=order.model_dump()).unwrap()
```

Pydantic models and dataclasses must be defined in importable modules (not `__main__` and not inside functions).

### Execute Directly (Skip Queue)

```python
# Runs immediately in current process
result = my_task("arg1", "arg2")
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
result = my_task.send("test")  # Err(SEND_SUPPRESSED)

# Correct - call from functions/endpoints
def process():
    handle = my_task.send("test").unwrap()
```

**Don't pass non-serializable objects.**

```python
# Wrong
handle = my_task.send(connection=db_connection).unwrap()

# Correct
handle = my_task.send(connection_url=str(db_connection.url)).unwrap()
```

## Retrying Failed Sends

When `.send()` fails with `ENQUEUE_FAILED` (a transient broker error), use the retry methods to replay the exact same payload without re-supplying arguments. The `enqueue_sha` on the stored `TaskSendPayload` guarantees the retry carries the identical serialized payload.

```python
from horsies import is_err, TaskSendErrorCode

send_result = my_task.send(arg1, arg2)
if is_err(send_result):
    error = send_result.err_value
    if error.retryable:
        retry_result = my_task.retry_send(error)
        handle = retry_result.unwrap()
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

### `.send(*args, **kwargs) -> TaskSendResult[TaskHandle[T]]`

Enqueue task for immediate execution.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `*args` | task args | Positional arguments for the task |
| `**kwargs` | task kwargs | Keyword arguments for the task |

**Returns:** `TaskSendResult[TaskHandle[T]]` -- `Ok(TaskHandle)` on success, `Err(TaskSendError)` on failure.

### `.send_async(*args, **kwargs) -> TaskSendResult[TaskHandle[T]]`

Async variant of `.send()`. Use in async code (FastAPI, etc.).
This does not execute the task locally; it only enqueues.

**Returns:** `TaskSendResult[TaskHandle[T]]`

### `.schedule(delay, *args, **kwargs) -> TaskSendResult[TaskHandle[T]]`

Enqueue task for delayed execution.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `delay` | `int` | Seconds to wait before task becomes claimable |
| `*args` | task args | Positional arguments for the task |
| `**kwargs` | task kwargs | Keyword arguments for the task |

**Returns:** `TaskSendResult[TaskHandle[T]]`

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

### `TaskSendResult[T]`

Type alias: `Result[T, TaskSendError]`. The `Ok` side is `TaskHandle[T]` when returned from send methods.

| Property/Method | Type | Description |
| --------------- | ---- | ----------- |
| `.is_ok()` | `bool` | True if send succeeded |
| `.is_err()` | `bool` | True if send failed |
| `.ok_value` | `T` | The `TaskHandle`; raises `ValueError` if error |
| `.err_value` | `TaskSendError` | The error; raises `ValueError` if success |
| `.unwrap()` | `T` | Same as `.ok_value` |
| `.unwrap_err()` | `TaskSendError` | Same as `.err_value` |

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

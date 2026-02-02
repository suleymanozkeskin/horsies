---
title: Sending Tasks
summary: How to enqueue tasks for background execution.
related: [defining-tasks, retrieving-results]
tags: [tasks, send, async, scheduling]
---

# Sending Tasks

Enqueue tasks with `.send()`, `.send_async()`, or `.schedule()`. All return a `TaskHandle` for retrieving results.

## How To

### Send a Task (Sync)

```python
from instance import my_task

handle = my_task.send(arg1, arg2, key="value")
print(f"Task submitted: {handle.task_id}")
```

### Send a Task (Async)

```python
async def my_endpoint():
    handle = await my_task.send_async(arg1, arg2)
    return {"task_id": handle.task_id}
```
`send_async()` only enqueues the task. Use `handle.get_async()` if you want to wait for completion.

### Delay Execution

```python
# Execute after 60 seconds
handle = my_task.schedule(60, arg1, arg2)

# Execute after 5 minutes
handle = my_task.schedule(300, arg1, arg2)
```

### Wait for Result

```python
handle = my_task.send(...)

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
# Send without waiting for result
my_task.send(arg1, arg2)
```

### Pass Complex Arguments

Arguments must be JSON-serializable. Pydantic models and dataclass instances are supported directly and rehydrated on the worker side.

```python
handle = process.send(
    data={"key": "value", "nested": {"a": 1}},
    items=[1, 2, 3],
)

# Pydantic models - pass the instance to preserve type metadata
order = Order(id=123, items=["a", "b"])
handle = process_order.send(order=order)

# Use model_dump() only when a plain dict is required
handle = process_order.send(order=order.model_dump())
```

Pydantic models and dataclasses must be defined in importable modules (not `__main__` and not inside functions).

### Execute Directly (Skip Queue)

```python
# Runs immediately in current process
result = my_task("arg1", "arg2")
```

Direct calls bypass the queue entirely. Library features do not apply:

- No retries (`retry_policy`, `auto_retry_for`)
- No persistence (task not recorded in database)
- No worker distribution
- No scheduling

Use only for **unit testing**. For production, always use `.send()` or `.send_async()`.

## Things to Avoid

**Don't call `.send()` at module level.**

```python
# Wrong - suppressed during worker import
# tasks.py
handle = my_task.send("test")  # SEND_SUPPRESSED error

# Correct - call from functions/endpoints
def process():
    handle = my_task.send("test")
```

**Don't pass non-serializable objects.**

```python
# Wrong
handle = my_task.send(connection=db_connection)

# Correct
handle = my_task.send(connection_url=str(db_connection.url))
```

## API Reference

### `.send(*args, **kwargs) -> TaskHandle[T]`

Enqueue task for immediate execution.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `*args` | task args | Positional arguments for the task |
| `**kwargs` | task kwargs | Keyword arguments for the task |

**Returns:** `TaskHandle[T]`

### `.send_async(*args, **kwargs) -> TaskHandle[T]`

Async variant of `.send()`. Use in async code (FastAPI, etc.).
This does not execute the task locally; it only enqueues.

### `.schedule(delay, *args, **kwargs) -> TaskHandle[T]`

Enqueue task for delayed execution.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `delay` | `int` | Seconds to wait before task becomes claimable |
| `*args` | task args | Positional arguments for the task |
| `**kwargs` | task kwargs | Keyword arguments for the task |

**Returns:** `TaskHandle[T]`

### `TaskHandle[T]`

| Property/Method | Type | Description |
| --------------- | ---- | ----------- |
| `.task_id` | `str` | Unique task identifier |
| `.get(timeout_ms=None)` | `TaskResult[T, TaskError]` | Wait for result (blocking) |
| `.get_async(timeout_ms=None)` | `TaskResult[T, TaskError]` | Wait for result (async) |
| `.info(include_result=False, include_failed_reason=False)` | `TaskInfo \| None` | Fetch task metadata from broker |
| `.info_async(include_result=False, include_failed_reason=False)` | `TaskInfo \| None` | Async variant of `.info()` |

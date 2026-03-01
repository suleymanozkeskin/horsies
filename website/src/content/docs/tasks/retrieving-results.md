---
title: Retrieving Results
summary: Getting task outcomes via TaskHandle.get().
related: [sending-tasks, ../../concepts/result-handling]
tags: [tasks, results, TaskHandle, errors]
---

# Retrieving Results

Retrieve task outcomes through `TaskHandle.get()` / `get_async()`. For error handling strategy, see `error-handling` and `../concepts/result-handling`.

## How To

### Basic Retrieval

```python
from horsies import LibraryErrorCode
from instance import my_task

handle = my_task.send(10, 20).unwrap()

# Block until complete (or timeout/error)
result = handle.get()

if result.is_err():
    error = result.err_value
    match error.error_code:
        case LibraryErrorCode.WAIT_TIMEOUT:
            print(f"Timeout waiting for {handle.task_id}")
        case _:
            print(f"Error: {error.error_code} - {error.message}")
else:
    print(f"Success: {result.ok_value}")
```

### Async Retrieval

```python
handle = (await my_task.send_async(10, 20)).unwrap()
result = await handle.get_async()
```
`get_async()` waits for broker notifications (LISTEN/NOTIFY) with a polling fallback if notifications are lost.

### Timeouts

Specify maximum wait time in milliseconds:

```python
from horsies import LibraryErrorCode

# Wait up to 5 seconds
result = handle.get(timeout_ms=5000)

if result.is_err() and result.err_value.error_code == LibraryErrorCode.WAIT_TIMEOUT:
    # Task didn't complete in time - may still be running
    print("Timed out, task may still complete later")
```

### Result Caching

Results are cached on the handle:

```python
handle = my_task.send(...).unwrap()

# First call fetches from database
result1 = handle.get()

# Subsequent calls return cached result
result2 = handle.get()  # No database query
```

### Checking Result State

```python
result = handle.get()

# Check state
if result.is_ok():
    value = result.ok_value   # Get success value
elif result.is_err():
    error = result.err_value  # Get error object
```

### Testing

For unit tests, call tasks directly:

```python
def test_my_task():
    result = my_task("test_input")  # Direct call, no queue

    assert result.is_ok()
    assert result.ok_value == "expected_output"
```

## Things to Avoid

**Don't silently ignore errors by only logging them.**

```python
# Wrong - logs error but takes no action
result = handle.get()
if result.is_err():
    print(result.err_value.message)
# Code continues as if nothing happened...

# Correct - handle or propagate errors
result = handle.get()
if result.is_err():
    # depending on your use case, either pass to a error handler ( prefer this when possible )
    your_error_handler(result.err_value)
    # or return/raise to propagate
    return result.err_value.error_code

print(f"Success: {result.ok_value}")
```

For detailed error handling patterns, see [error-handling](error-handling.md).

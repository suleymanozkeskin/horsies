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

send_result = my_task.send(10, 20)
if send_result.is_err():
    raise RuntimeError(f"Send failed: {send_result.err_value.code}")
handle = send_result.ok_value

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
send_result = await my_task.send_async(10, 20)
if send_result.is_err():
    raise RuntimeError(f"Send failed: {send_result.err_value.code}")
handle = send_result.ok_value
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
send_result = my_task.send(...)
if send_result.is_err():
    raise RuntimeError(f"Send failed: {send_result.err_value.code}")
handle = send_result.ok_value

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

### Task Metadata and Attempt History

Use `handle.info()` to fetch task metadata without waiting for completion:

```python
from horsies import is_ok

info_result = handle.info(include_result=True, include_failed_reason=True)
if is_ok(info_result) and info_result.ok_value is not None:
    task_info = info_result.ok_value
    print(f"Status: {task_info.status}, Retries: {task_info.retry_count}")
    print(f"Error code: {task_info.error_code}")  # final error_code (None if not failed)
```

To inspect per-attempt execution history, pass `include_attempts=True`:

```python
info_result = handle.info(include_attempts=True)
if is_ok(info_result) and info_result.ok_value is not None:
    task_info = info_result.ok_value
    for attempt in task_info.attempts or []:
        print(f"Attempt {attempt.attempt}: {attempt.outcome} "
              f"(retry={attempt.will_retry}, error={attempt.error_code})")
```

`TaskInfo.attempts` is `None` when `include_attempts=False` (default) and `[]` when requested but no history exists. Attempts are ordered by attempt number descending (most recent first).

Attempt history and `error_code` are only available from version `0.1.0a26` onward.

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

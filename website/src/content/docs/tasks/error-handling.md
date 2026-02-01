---
title: Error Handling
summary: Patterns for handling TaskResult errors with explicit control flow.
related: [errors, retrieving-results, retry-policy]
tags: [tasks, errors, TaskResult, patterns]
---

# Error Handling

Handle errors explicitly through dedicated handlers or explicit returns. Avoid raising exceptions - they break control flow and obscure error paths.

For exhaustive error code reference, see [errors](errors.md).

This document means to be a pattern guide rather than definitive way to handle your errors.
Adjust to your needs.

## Why and When

### Error Categories

| Category | Source | Examples | Auto-Retry? |
| -------- | ------ | -------- | ----------- |
| Retrieval errors | `handle.get()` | `WAIT_TIMEOUT`, `BROKER_ERROR` | No |
| Execution errors | Task raised exception | `TASK_EXCEPTION`, `WORKER_CRASHED` | If in `auto_retry_for` |
| Domain errors | User returned error | `"RATE_LIMITED"`, `"VALIDATION_FAILED"` | If in `auto_retry_for` |

### When to Handle Errors

Handle errors when:

- The error is a **retrieval error** (task may still complete)
- The error is **not in `auto_retry_for`** (won't be retried)
- The task has **exhausted retries** (final failure)

## How To

### Configure Automatic Retries

The library handles retries automatically when `auto_retry_for` matches the error:

```python
from horsies import RetryPolicy, TaskResult, TaskError

@app.task(
    "fetch_api_data",
    retry_policy=RetryPolicy.exponential(base_seconds=30, max_retries=3),
    auto_retry_for=["TASK_EXCEPTION", "RATE_LIMITED", "ConnectionError"],
)
def fetch_api_data(url: str) -> TaskResult[dict, TaskError]:
    # If this raises ConnectionError or returns RATE_LIMITED error,
    # the worker automatically retries up to 3 times
    ...
```

See [retry-policy](../retry-policy) for configuration details.

### Handle Errors with Explicit Returns

Prefer explicit returns over raising exceptions:

```python
from horsies import LibraryErrorCode
from instance import my_task

def process_task() -> str | None:
    handle = my_task.send(10, 20)
    result = handle.get(timeout_ms=5000)

    if result.is_err():
        error = result.err
        match error.error_code:
            case LibraryErrorCode.WAIT_TIMEOUT:
                # Task may still be running
                # check status again using task id if you need absolute decisions
            case LibraryErrorCode.TASK_NOT_FOUND:
                return None
            case _:
                # Pass to centralized handler
                handle_error(error)
                return None

    return result.ok
```

### Centralized Error Handler

Create a handler that **returns actions**, not just logs.

## Things to Avoid

**Don't raise exceptions for flow control.**

```python
# Wrong - exceptions break control flow
if result.is_err():
    raise RuntimeError(f"Task failed: {result.err.error_code}")

# Correct - explicit return and handling
if result.is_err():
    handle_error(result.err)
    return None
```

**Don't ignore errors after logging.**

```python
# Wrong - logs but continues as if nothing happened
if result.is_err():
    print(result.err.message)
# Code continues...

# Correct - handle and return explicitly
if result.is_err():
    handle_error(result.err)
    return result.err.error_code
```

**Don't manually retry errors that should be auto-retried.**

```python
# Wrong - duplicates library retry logic
if error.error_code == "RATE_LIMITED":
    manually_schedule_retry(task_id)

# Correct - configure auto_retry_for on the task
@app.task(
    "my_task",
    retry_policy=RetryPolicy.fixed([60, 120, 300]),
    auto_retry_for=["RATE_LIMITED"],
)
def my_task() -> TaskResult[str, TaskError]:
    ...
```

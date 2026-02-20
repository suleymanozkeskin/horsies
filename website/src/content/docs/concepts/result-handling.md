---
title: Result Handling
summary: TaskResult pattern, error types, and explicit error handling.
related: [task-lifecycle, ../../tasks/retrieving-results]
tags: [concepts, results, errors, TaskResult]
---

## TaskResult[T, E]

Every task must return `TaskResult[T, TaskError]` where:

- `T` is the success type
- `TaskError` is the error type (always `TaskError`)

A `TaskResult` contains exactly one of:

- `ok`: The success value (type `T`)
- `err`: The error value (type `TaskError`)

```python
from horsies import TaskResult, TaskError

@app.task("divide")
def divide(a: int, b: int) -> TaskResult[float, TaskError]:
    if b == 0:
        return TaskResult(err=TaskError(
            error_code="DIVISION_BY_ZERO",
            message="Cannot divide by zero",
        ))
    return TaskResult(ok=a / b)
```

## Checking Results

```python
handle = divide.send(10, 2)
result = handle.get()

if result.is_ok():
    print(f"Result: {result.ok}")
    # or use result.unwrap() which raises if err
else:
    error = result.err
    print(f"Error: {error.error_code} - {error.message}")
    # or use result.unwrap_err() which raises if ok
```

## TaskError Structure

`TaskError` is a Pydantic model with optional fields:

| Field | Type | Purpose |
|-------|------|---------|
| `error_code` | `str` or `LibraryErrorCode` | Identifies the error type |
| `message` | `str` | Human-readable description |
| `data` | `Any` | Additional context (dict, list, etc.) |
| `exception` | `BaseException` or `dict` | Original exception if applicable |

```python
return TaskResult(err=TaskError(
    error_code="VALIDATION_FAILED",
    message="Input validation failed",
    data={"field": "email", "reason": "invalid format"},
))
```

## Library Error Codes

When the library itself encounters an error (not your task code), it uses `LibraryErrorCode`:

### Execution Errors

| Code | When |
| ---- | ---- |
| `UNHANDLED_EXCEPTION` | Task raised an uncaught exception |
| `TASK_EXCEPTION` | Task function threw an exception |
| `WORKER_CRASHED` | Worker process died (detected via heartbeat) |

### Retrieval Errors

Returned by `handle.get()` for issues retrieving results:

| Code | When |
| ---- | ---- |
| `WAIT_TIMEOUT` | Timeout elapsed while waiting for result (task may still be running) |
| `TASK_NOT_FOUND` | Task ID doesn't exist in database |
| `TASK_CANCELLED` | Task was cancelled before completion |
| `RESULT_NOT_AVAILABLE` | Result cache is empty for an immediate/sync handle |
| `RESULT_DESERIALIZATION_ERROR` | Stored result JSON is corrupt or could not be deserialized |

### Broker Errors

Returned when result retrieval fails due to broker or database issues:

| Code | When |
| ---- | ---- |
| `BROKER_ERROR` | Broker failed while retrieving the result |

### Worker Errors

| Code | When |
| ---- | ---- |
| `WORKER_RESOLUTION_ERROR` | Worker couldn't find the task in registry |
| `WORKER_SERIALIZATION_ERROR` | Result couldn't be serialized |

### Validation Errors

| Code | When |
| ---- | ---- |
| `RETURN_TYPE_MISMATCH` | Returned value doesn't match declared type |
| `PYDANTIC_HYDRATION_ERROR` | Task succeeded but return value could not be rehydrated to declared type |

### Lifecycle Errors

| Code | When |
| ---- | ---- |
| `SEND_SUPPRESSED` | Task send was suppressed during import |

## Domain Errors vs Library Errors

**Domain errors**: Your task returns an error for business logic reasons.

```python
@app.task("transfer_funds")
def transfer_funds(amount: float) -> TaskResult[str, TaskError]:
    if amount <= 0:
        # This is a domain error - expected, handled
        return TaskResult(err=TaskError(
            error_code="INVALID_AMOUNT",
            message="Amount must be positive",
        ))
    return TaskResult(ok="Transfer complete")
```

**Library errors**: Something went wrong in the infrastructure.

```python
# If your task raises an exception:
@app.task("buggy_task")
def buggy_task() -> TaskResult[str, TaskError]:
    raise ValueError("Oops")  # Becomes UNHANDLED_EXCEPTION
```

Both cases result in `TaskResult(err=...)`, but the error codes differ.

## Runtime Type Validation

Horsies validates that your returned value matches the declared type:

```python
@app.task("typed_task")
def typed_task() -> TaskResult[int, TaskError]:
    return TaskResult(ok="not an int")  # RETURN_TYPE_MISMATCH error
```

This validation happens at runtime using Pydantic's `TypeAdapter`.

## None as Valid Success

`TaskResult[None, TaskError]` is valid for tasks that don't return a value:

```python
@app.task("fire_and_forget")
def fire_and_forget() -> TaskResult[None, TaskError]:
    do_something()
    return TaskResult(ok=None)
```

## Convenience Properties

```python
result = handle.get()

# Check state
result.is_ok()    # True if success
result.is_err()   # True if error

# Access values (raises if wrong state)
result.ok_value   # Same as unwrap()
result.err_value  # Same as unwrap_err()
```

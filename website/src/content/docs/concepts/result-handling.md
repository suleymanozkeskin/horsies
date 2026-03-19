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
from horsies import Ok, Err

match divide.send(10, 2):
    case Ok(handle):
        result = handle.get()

        if result.is_ok():
            print(f"Result: {result.ok_value}")
        else:
            error = result.err_value
            print(f"Error: {error.error_code} - {error.message}")
    case Err(send_err):
        print(f"Send failed: {send_err.code} - {send_err.message}")
```

## TaskError Structure

`TaskError` is a Pydantic model with optional fields:

| Field | Type | Purpose |
|-------|------|---------|
| `error_code` | `str` or `BuiltInTaskCode` | Identifies the error type |
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

## Built-In Error Codes

When the library itself encounters an error (not your task code), it uses one of four error code families. The umbrella type alias `BuiltInTaskCode` covers all of them.

### OperationalErrorCode

| Code | When |
| ---- | ---- |
| `UNHANDLED_EXCEPTION` | Task raised an uncaught exception |
| `TASK_EXCEPTION` | Task function threw an exception |
| `WORKER_CRASHED` | Worker process died (detected via heartbeat) |
| `BROKER_ERROR` | Broker failed while retrieving the result |
| `WORKER_RESOLUTION_ERROR` | Worker couldn't find the task in registry |
| `WORKER_SERIALIZATION_ERROR` | Result couldn't be serialized |
| `RESULT_DESERIALIZATION_ERROR` | Stored result JSON is corrupt or could not be deserialized |
| `WORKFLOW_ENQUEUE_FAILED` | Workflow node failed during enqueue/build |
| `SUBWORKFLOW_LOAD_FAILED` | Subworkflow definition could not be loaded |

### ContractCode

| Code | When |
| ---- | ---- |
| `RETURN_TYPE_MISMATCH` | Returned value doesn't match declared type |
| `PYDANTIC_HYDRATION_ERROR` | Task succeeded but return value could not be rehydrated to declared type |
| `WORKFLOW_CTX_MISSING_ID` | Workflow context is missing required ID |

### RetrievalCode

Returned by `handle.get()` for issues retrieving results:

| Code | When |
| ---- | ---- |
| `WAIT_TIMEOUT` | Timeout elapsed while waiting for result (task may still be running) |
| `TASK_NOT_FOUND` | Task ID doesn't exist in database |
| `WORKFLOW_NOT_FOUND` | Workflow ID doesn't exist in database |
| `RESULT_NOT_AVAILABLE` | Result cache is empty for an immediate/sync handle |
| `RESULT_NOT_READY` | Result not yet available; task is still running |

### OutcomeCode

| Code | When |
| ---- | ---- |
| `TASK_CANCELLED` | Task was cancelled before completion |
| `WORKFLOW_PAUSED` | Workflow was paused |
| `WORKFLOW_FAILED` | Workflow failed |
| `WORKFLOW_CANCELLED` | Workflow was cancelled |
| `UPSTREAM_SKIPPED` | Upstream task in workflow was skipped |
| `SUBWORKFLOW_FAILED` | Subworkflow failed |
| `WORKFLOW_SUCCESS_CASE_NOT_MET` | Workflow success condition was not satisfied |

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

**Built-in errors**: Something went wrong in the infrastructure.

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

# Access values (raises ValueError if wrong state)
result.ok_value   # Success value
result.err_value  # Error value

# Unwrap shortcuts (raises ValueError if wrong state)
result.unwrap()       # Returns ok value, raises if err
result.unwrap_err()   # Returns err value, raises if ok
```

`unwrap()` and `unwrap_err()` are equivalent to `ok_value` and `err_value` — use whichever reads better in context. Both raise `ValueError` when called on the wrong variant, so always check `is_ok()`/`is_err()` first or use pattern matching.

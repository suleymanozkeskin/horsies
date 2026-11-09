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
def divide(*, a: int, b: int) -> TaskResult[float, TaskError]:
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

match divide.send(a=10, b=2):
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

`TaskError` is a Pydantic model. `error_code` is required. The other fields are optional. A key that the model does not declare is refused.

| Field | Type | Purpose |
|-------|------|---------|
| `error_code` | `str` or `BuiltInTaskCode` (required) | Identifies the error type |
| `message` | `str` | Human-readable description |
| `data` | `Any` | Additional context (dict, list, etc.) |
| `exception` | `BaseException` or `FlattenedException` | Live exception in-process; flattened on the wire to `type`, `module`, `message`, `repr`, and `traceback` |

<!-- check: skip elided -- a return statement shown without its function -->
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

<!-- members: OperationalErrorCode -->
| Code | When |
| ---- | ---- |
| `UNHANDLED_EXCEPTION` | Task raised an uncaught exception |
| `TASK_EXCEPTION` | The worker caught an exception outside the task wrapper, or a validator raised on a default |
| `WORKER_CRASHED` | A child process died (the worker sees it at once), or the worker died (detected via missing heartbeats) |
| `BROKER_ERROR` | Broker failed while retrieving the result |
| `WORKER_RESOLUTION_ERROR` | Worker couldn't find the task in registry |
| `WORKER_SERIALIZATION_ERROR` | Result couldn't be serialized |
| `RESULT_DESERIALIZATION_ERROR` | A stored result or a stored workflow error is corrupt or could not be deserialized |
| `WORKFLOW_ENQUEUE_FAILED` | Workflow node failed during enqueue/build |
| `SUBWORKFLOW_LOAD_FAILED` | Subworkflow definition could not be loaded |
| `WORKFLOW_RECOVERY_LIMIT_REACHED` | Workflow recovery raised a persistent error in too many reaper passes |
| `SUBWORKFLOW_DEPTH_LIMIT_REACHED` | The engine did not start a child workflow, because its depth would pass 32 |

### ContractCode

<!-- members: ContractCode -->
| Code | When |
| ---- | ---- |
| `RETURN_TYPE_MISMATCH` | The returned value does not fit the declared type, or it is not a `TaskResult`, including `None` |
| `NO_TYPE_AVAILABLE` | A successful result could not be decoded because the caller does not have the task's declared success type locally registered |
| `WORKFLOW_CTX_MISSING_ID` | Workflow context is missing required ID |

### RetrievalCode

Returned by `handle.get()` for issues retrieving results:

<!-- members: RetrievalCode -->
| Code | When |
| ---- | ---- |
| `WAIT_TIMEOUT` | Timeout elapsed while waiting for result (task may still be running) |
| `TASK_NOT_FOUND` | Task ID doesn't exist in database |
| `WORKFLOW_NOT_FOUND` | Workflow ID doesn't exist in database, or retention removed the workflow after `get()` read its terminal status |
| `RESULT_NOT_AVAILABLE` | Result cache is empty for an immediate/sync handle |
| `RESULT_NOT_READY` | Result not yet available; task is still running |
| `EVENT_LOOP_LIMIT` | The broker serves its limit of open event loops and this is a new one; not cached, clears when one of those loops closes |
| `ASYNC_CONTEXT` | Sync `get()` / `result_for()` was called on a thread that runs an event loop; nothing ran; not cached. Call `await handle.get_async()` / `result_for_async()` |
| `INVALID_INPUT` | An argument of `get()` / `get_async()` was refused before any read, for example a `timeout_ms` that is negative, not an int, or above `MAX_TASK_TIMEOUT_MS`; not cached |
| `WAIT_INSIDE_TASK` | A result wait was called inside a worker child, where task bodies run; it waited for nothing; not cached. Compose the tasks with a workflow: see [Waiting for a Result Inside a Task](#waiting-for-a-result-inside-a-task) |

### OutcomeCode

<!-- members: OutcomeCode -->
| Code | When |
| ---- | ---- |
| `TASK_CANCELLED` | Task was cancelled before completion |
| `TASK_EXPIRED` | Task's `good_until` deadline passed before execution started |
| `TASK_TIMEOUT` | Task exceeded its `timeout_ms` execution limit; the child process was killed |
| `WORKFLOW_PAUSED` | Workflow was paused |
| `WORKFLOW_EXPIRED` | A paused workflow passed `paused_workflow_auto_cancel_after` and expired |
| `WORKFLOW_FAILED` | Workflow failed |
| `WORKFLOW_CANCELLED` | Workflow was cancelled |
| `UPSTREAM_SKIPPED` | Upstream task in workflow was skipped |
| `SUBWORKFLOW_FAILED` | Subworkflow failed |
| `WORKFLOW_SUCCESS_CASE_NOT_MET` | Workflow success condition was not satisfied |
| `WORKFLOW_OUTPUT_SKIPPED` | A `COMPLETED` workflow has no output: its output node, or a terminal node of an outputless workflow, is SKIPPED, and a success case does not require that node |

## Waiting for a Result Inside a Task

A task does not wait for the result of another task. In a worker child, these calls return `RetrievalCode.WAIT_INSIDE_TASK` at once and wait for nothing:

- `TaskHandle.get()` and `TaskHandle.get_async()` on a handle from `send()` or `send_async()`
- `app.get_result()` and `app.get_result_async()`
- `WorkflowHandle.get()` and `WorkflowHandle.get_async()`

The raw result reads wait too. In a worker child they return `Err(BrokerWaitInsideTask)` (`BrokerErrorCode.WAIT_INSIDE_TASK`) at once: `app.get_raw_result()`, `app.get_raw_result_async()`, `broker.get_raw_result_record()` and `broker.get_raw_result_record_async()`.

The refusal is the first check of each call. It comes before the check of `timeout_ms` and before the `ASYNC_CONTEXT` check. The rule covers the whole child process: the task body, the threads that it starts, and the `on_child_process_start` hooks.

These calls do not wait, so they work in a task:

- `send()` and `send_async()`
- starting a workflow
- `TaskHandle.info()`
- `workflow_ctx.result_for()`

A task that waits keeps its child while it waits. When enough tasks wait for other tasks, no child is free for the tasks that they wait for, and nothing moves until timeouts end the waits. A loop in a task that polls `info()` and sleeps holds its child the same way; it is outside this contract.

A workflow node that waits for its dependencies is only a row, and the worker starts it when they finish. Compose the steps as a workflow:

| The task… | Workflow form |
| --------- | ------------- |
| sends a, waits, then sends b with the result of a | `TaskNode`s with `waits_for` and `args_from` |
| sends several tasks, then combines their results | fan-out, then a fan-in node |
| gets the number of tasks from a result | a `SubWorkflowNode`: `build_with()` runs when the node is ready, with the upstream results ([Dynamic Fan-Out with `args_from`](/horsies/concepts/workflows/subworkflows/#dynamic-fan-out-with-args_from)) |
| chooses the next step from a result | `build_with()` chooses the nodes |
| has steps that can fail | `allow_failed_deps`, `join="any"` or `join="quorum"`, `success_policy` |

## Domain Errors vs Library Errors

**Domain errors**: Your task returns an error for business logic reasons.

```python
@app.task("transfer_funds")
def transfer_funds(*, amount: float) -> TaskResult[str, TaskError]:
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

<!-- check: skip elided -- returns a wrong type on purpose, to show the runtime check -->
```python
@app.task("typed_task")
def typed_task() -> TaskResult[int, TaskError]:
    return TaskResult(ok="not an int")  # RETURN_TYPE_MISMATCH error
```

This validation happens at runtime using Pydantic's `TypeAdapter`.

## None as Valid Success

`TaskResult[None, TaskError]` is valid for tasks that don't return a value:

<!-- check: skip partial -- uses names that the page does not define -->
```python
@app.task("fire_and_forget")
def fire_and_forget() -> TaskResult[None, TaskError]:
    do_something()
    return TaskResult(ok=None)
```

## Convenience Properties

<!-- check: skip elided -- shows a wrong form next to the right one -->
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

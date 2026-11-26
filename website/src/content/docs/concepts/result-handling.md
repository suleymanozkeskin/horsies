---
title: Result Handling
section: python
summary: The Result that a task body returns, the TaskOutcome and the ReadResult that a reader gets, the ReadError classes, and the built-in error codes.
related: [task-lifecycle, ../../tasks/retrieving-results]
tags: [concepts, results, errors, Result, TaskOutcome, ReadResult, ReadError]
---

## Result

A task body returns `Result[T, TaskError]`:

- `T` is the success type.
- `TaskError` is the error type. It is always `TaskError`.

The body returns one of two values:

- `Ok(value)`: the task succeeded with a value of type `T`.
- `Err(task_error)`: the task failed with a `TaskError`.

`Result`, `Ok` and `Err` come from `horsies`. The annotation `Ok[T] | Err[TaskError]` is the same type, and `@app.task` accepts it too.

```python
from horsies import Err, Ok, Result, TaskError

@app.task("divide")
def divide(*, a: int, b: int) -> Result[float, TaskError]:
    if b == 0:
        return Err(TaskError(
            error_code="DIVISION_BY_ZERO",
            message="Cannot divide by zero",
        ))
    return Ok(a / b)
```

A direct call returns the same `Result`: `divide(a=1, b=2)` returns `Ok(0.5)`.

## TaskOutcome

`TaskOutcome[T, TaskError]` tells how a task ended. It is one of two frozen classes:

| Class | Field | The task |
|-------|-------|----------|
| `Succeeded(value)` | `value: T` | Returned `Ok(value)` |
| `Failed(error)` | `error: TaskError` | Returned `Err(error)`, or the library recorded the error for the task, for example `TASK_TIMEOUT`, `TASK_CANCELLED` or `WORKER_CRASHED` |

These places give a `TaskOutcome`:

- A read of a task or a workflow, inside `Ok(...)`. See [ReadResult](#readresult).
- A node parameter that `args_from` fills.
- `WorkflowContext.result_for(node)`.

```python
from horsies import Failed, Succeeded, TaskOutcome

@app.task("describe_quotient")
def describe_quotient(*, quotient: TaskOutcome[float, TaskError]) -> Result[str, TaskError]:
    match quotient:
        case Succeeded(value):
            return Ok(f"quotient={value}")
        case Failed(error):
            return Err(TaskError(error_code="UPSTREAM_FAILED", message=error.message))
```

## ReadResult

A read of a task or a workflow returns `ReadResult[T, TaskError]`. It has three forms:

| Form | Meaning |
|------|---------|
| `Ok(Succeeded(value))` | The task returned `value` |
| `Ok(Failed(task_error))` | The task returned this error, or the library recorded it for the task |
| `Err(read_error)` | The read gave no outcome. `read_error` is one class of [`ReadError`](#readerror) |

These calls return a `ReadResult`:

- `TaskHandle.get()` and `TaskHandle.get_async()`
- `WorkflowHandle.get()` and `WorkflowHandle.get_async()`
- `WorkflowHandle.result_for()` and `WorkflowHandle.result_for_async()`
- `app.get_result()` and `app.get_result_async()`

Read the result with one `match`, with one arm for each form:

```python
match divide.send(a=10, b=2):
    case Ok(handle):
        match handle.get(timeout_ms=30_000):
            case Ok(Succeeded(value)):
                print(f"Result: {value}")
            case Ok(Failed(task_error)):
                print(f"Task failed: {task_error.error_code} - {task_error.message}")
            case Err(read_error):
                print(f"Read failed: {read_error.code} - {read_error.message}")
    case Err(send_err):
        print(f"Send failed: {send_err.code} - {send_err.message}")
```

`Err(read_error)` does not tell how the task ended. For some classes, the task can still run: see the table below.

### ReadError

`ReadError` is a closed union of frozen classes. Each class has the name of its code. A `match` on `ReadError` is exhaustive under a type checker. Each class has `code` (its catalog member) and `message` (the subject and the cause). `BrokerError` also has `retryable` (whether the cause may clear) and `exception` (the cause, or `NoCause()`).

| Class | Code | When | Kept by `TaskHandle` |
|-------|------|------|----------------------|
| `WaitTimeout` | `RetrievalCode.WAIT_TIMEOUT` | The wait limit passed before the task or the workflow ended. It can still run, and a later read can find it ended | No |
| `TaskNotFound` | `RetrievalCode.TASK_NOT_FOUND` | Neither the live table nor the task history holds the task. A handle built by id can race the commit of its send, so a later read can find it | No |
| `WorkflowNotFound` | `RetrievalCode.WORKFLOW_NOT_FOUND` | No workflow has the id of the handle, or retention removed the workflow after `get()` read its terminal status | No |
| `ResultNotAvailable` | `RetrievalCode.RESULT_NOT_AVAILABLE` | The task reached a terminal status and stored no result, or a handle with no broker holds no result | Yes |
| `ResultNotReady` | `RetrievalCode.RESULT_NOT_READY` | `WorkflowHandle.result_for()`: the node stores no result yet. A later read can find one | No |
| `WaitInsideTask` | `RetrievalCode.WAIT_INSIDE_TASK` | A result wait in a worker child, where task bodies run. Nothing was read. See [Waiting for a Result Inside a Task](#waiting-for-a-result-inside-a-task) | No |
| `AsyncContext` | `RetrievalCode.ASYNC_CONTEXT` | A sync read on a thread that runs an event loop. Nothing was read. Call the `*_async` form there | No |
| `InvalidInput` | `RetrievalCode.INVALID_INPUT` | `timeout_ms` is negative, not an int, or above `MAX_TASK_TIMEOUT_MS`. Nothing was read | No |
| `ResultDeserializationError` | `OperationalErrorCode.RESULT_DESERIALIZATION_ERROR` | The stored result does not read as the result of the declared types. The same read gives the same error until the stored value changes | Yes |
| `BrokerError` | `OperationalErrorCode.BROKER_ERROR` | The database, the connection or the core call failed, or a workflow handle read found stored workflow data that is not valid. The condition can clear, so a later read can succeed | No |
| `NoTypeAvailable` | `ContractCode.NO_TYPE_AVAILABLE` | The task succeeded, and this process has no declared type to read the value with: the handle was built by id, or the task or the workflow is not registered here | Yes |
| `WorkflowCtxMissingId` | `ContractCode.WORKFLOW_CTX_MISSING_ID` | `WorkflowHandle.result_for()` got a node with no `node_id`. Nothing was read | No |

A `TaskHandle` keeps an outcome and each class with "Yes": a later `get()` returns it with no read. A class with "No" is not kept, so a later `get()` reads again.

A read does its checks before it reads, in this order. Only the first check that fails gives its class:

1. `InvalidInput`: the wait limit (`timeout_ms`) is not `None` or an int from 0 to `MAX_TASK_TIMEOUT_MS`.
2. `WaitInsideTask`: the read runs in a process that runs task bodies.
3. A `TaskHandle` returns the read that it keeps.
4. `AsyncContext`: a sync read runs on a thread that runs an event loop.

The Rust and the TypeScript clients use the same order. So a read inside a task with a wait above the limit gives `InvalidInput` in each client.

Match a class to handle one failure in its own arm:

```python
from horsies import BrokerError, ReadResult, TaskNotFound, WaitTimeout

def describe_read(read: ReadResult[float, TaskError]) -> str:
    match read:
        case Ok(Succeeded(value)):
            return f"value {value}"
        case Ok(Failed(task_error)):
            return f"task failed: {task_error.error_code}"
        case Err(WaitTimeout() | TaskNotFound() | BrokerError()):
            return "no outcome yet: read again later"
        case Err(read_error):
            return f"no outcome: {read_error.code} - {read_error.message}"
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
return Err(TaskError(
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
| `TASK_BLOCKED_LOOP` | The task held the event loop of its shared async child past the block limit, before its `timeout_ms`; the worker killed the child |
| `NEIGHBOUR_BLOCKED_LOOP` | Another task held the event loop of this task's shared async child past the block limit, before this task's `timeout_ms`; the worker killed the child |
| `WORKFLOW_START_INCOMPLETE` | A RUNNING workflow has no node rows: its start did not finish, and recovery failed it |

### ContractCode

<!-- members: ContractCode -->
| Code | When |
| ---- | ---- |
| `RETURN_TYPE_MISMATCH` | The returned value does not fit the declared type, or it is not an `Ok` or an `Err`, including `None` |
| `NO_TYPE_AVAILABLE` | A successful result could not be decoded because the caller does not have the task's declared success type locally registered |
| `WORKFLOW_CTX_MISSING_ID` | Workflow context is missing required ID |

### RetrievalCode

A read gives these codes as `read_error.code`, not inside a `TaskError`. Each code has its own class: see [ReadError](#readerror).

<!-- members: RetrievalCode -->
| Code | When |
| ---- | ---- |
| `WAIT_TIMEOUT` | Timeout elapsed while waiting for result (task may still be running) |
| `TASK_NOT_FOUND` | Task ID doesn't exist in database |
| `WORKFLOW_NOT_FOUND` | Workflow ID doesn't exist in database, or retention removed the workflow after `get()` read its terminal status |
| `RESULT_NOT_AVAILABLE` | Result cache is empty for an immediate/sync handle |
| `RESULT_NOT_READY` | Result not yet available; task is still running |
| `ASYNC_CONTEXT` | Sync `get()` / `result_for()` was called on a thread that runs an event loop; nothing ran; not cached. Call `await handle.get_async()` / `result_for_async()` |
| `INVALID_INPUT` | An argument of `get()` / `get_async()` was refused before any read, for example a `timeout_ms` that is negative, not an int, or above `MAX_TASK_TIMEOUT_MS`; not cached |
| `WAIT_INSIDE_TASK` | A result wait was called inside a worker child, where task bodies run; it waited for nothing; not cached. Compose the tasks with a workflow: see [Waiting for a Result Inside a Task](#waiting-for-a-result-inside-a-task) |

### OutcomeCode

<!-- members: OutcomeCode -->
| Code | When |
| ---- | ---- |
| `TASK_CANCELLED` | Task was cancelled before completion |
| `TASK_EXPIRED` | Task's `good_until` deadline passed before execution started |
| `TASK_TIMEOUT` | Task exceeded its `timeout_ms` execution limit: the worker killed its child process, or, for a `SHARED_ASYNC` task on a shared async child, cancelled it and ended its work. `data` has `task_id` and `timeout_ms`; a `SHARED_ASYNC` task on a shared async child also has `cancel_end`: `RETURNED_AFTER_SIGNAL`, `DROPPED_AFTER_GRACE` or `KILLED_WITH_CHILD` ([The Timeout](/horsies/workers/shared-async-tasks/#the-timeout)) |
| `WORKFLOW_PAUSED` | Workflow was paused |
| `WORKFLOW_EXPIRED` | A paused workflow passed `paused_workflow_auto_cancel_after` and expired |
| `WORKFLOW_FAILED` | Workflow failed |
| `WORKFLOW_CANCELLED` | Workflow was cancelled |
| `UPSTREAM_SKIPPED` | Upstream task in workflow was skipped |
| `SUBWORKFLOW_FAILED` | Subworkflow failed |
| `WORKFLOW_SUCCESS_CASE_NOT_MET` | Workflow success condition was not satisfied |
| `WORKFLOW_OUTPUT_SKIPPED` | A `COMPLETED` workflow has no output: its output node, or a terminal node of an outputless workflow, is SKIPPED, and a success case does not require that node |

## Waiting for a Result Inside a Task

A task does not wait for the result of another task. In a worker child, these calls return `Err(WaitInsideTask(...))` (`RetrievalCode.WAIT_INSIDE_TASK`) at once and wait for nothing:

- `TaskHandle.get()` and `TaskHandle.get_async()` on a handle from `send()` or `send_async()`
- `app.get_result()` and `app.get_result_async()`
- `WorkflowHandle.get()` and `WorkflowHandle.get_async()`

The raw result reads wait too. In a worker child they return `Err(BrokerWaitInsideTask)` (`BrokerErrorCode.WAIT_INSIDE_TASK`) at once: `app.get_raw_result()`, `app.get_raw_result_async()`, `broker.get_raw_result_record()` and `broker.get_raw_result_record_async()`.

The refusal is the first check of each call. It comes before the check of `timeout_ms` (`InvalidInput`) and before the check of the event loop (`AsyncContext`). The rule covers the whole child process: the task body, the threads that it starts, and the `on_child_process_start` hooks.

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
def transfer_funds(*, amount: float) -> Result[str, TaskError]:
    if amount <= 0:
        # This is a domain error - expected, handled
        return Err(TaskError(
            error_code="INVALID_AMOUNT",
            message="Amount must be positive",
        ))
    return Ok("Transfer complete")
```

**Built-in errors**: Something went wrong in the infrastructure.

```python
# If your task raises an exception:
@app.task("buggy_task")
def buggy_task() -> Result[str, TaskError]:
    raise ValueError("Oops")  # Becomes UNHANDLED_EXCEPTION
```

In both cases, a reader gets `Ok(Failed(task_error))`. The error codes are different.

## Runtime Type Validation

Horsies validates that your returned value matches the declared type:

<!-- check: skip elided -- returns a wrong type on purpose, to show the runtime check -->
```python
@app.task("typed_task")
def typed_task() -> Result[int, TaskError]:
    return Ok("not an int")  # RETURN_TYPE_MISMATCH error
```

This validation happens at runtime using Pydantic's `TypeAdapter`.

## None as Valid Success

`Result[None, TaskError]` is valid for tasks that don't return a value:

<!-- check: skip partial -- uses names that the page does not define -->
```python
@app.task("fire_and_forget")
def fire_and_forget() -> Result[None, TaskError]:
    do_something()
    return Ok(None)
```

## Shortcut Methods and Test Asserts

`Ok` and `Err` keep `is_ok()`, `is_err()`, `ok_value`, `err_value`, `unwrap()` and `unwrap_err()`. On a `ReadResult`, `ok_value` and `unwrap()` give the `TaskOutcome`, not the value: `Succeeded(value)` or `Failed(task_error)`. `err_value` gives the `ReadError`. `unwrap()` and `unwrap_err()` raise an exception on the wrong form. Read the value with a `match`.

In a test, compare the whole result with `==`. `Ok`, `Succeeded` and `Failed` compare by value:

```python
import pytest

from horsies import OutcomeCode


def test_divide() -> None:
    assert divide(a=1, b=2) == Ok(0.5)


def assert_succeeded(read: ReadResult[int, TaskError]) -> None:
    assert read == Ok(Succeeded(42))


def assert_task_timeout(read: ReadResult[int, TaskError]) -> None:
    match read:
        case Ok(Failed(task_error)):
            assert task_error.error_code == OutcomeCode.TASK_TIMEOUT
        case _:
            pytest.fail(f"expected a failed outcome, got {read!r}")


def assert_wait_timeout(read: ReadResult[int, TaskError]) -> None:
    assert isinstance(read, Err) and isinstance(read.err_value, WaitTimeout)
```

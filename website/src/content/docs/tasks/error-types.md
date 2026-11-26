---
title: Error Types
section: python
summary: The Python types of task errors, read errors, broker errors, send errors, workflow start and handle errors, and startup errors.
related: [errors, error-handling, retrieving-results, sending-tasks]
tags: [errors, TaskError, ReadError, ReadResult, TaskOutcome, BrokerOperationError, TaskSendError, HorsiesError]
---

# Error Types

The Python types that carry the error codes of the [Errors Reference](/horsies/tasks/errors/). A task returns `Result[T, TaskError]`; a result read returns `ReadResult[T, TaskError]`; a broker call returns `BrokerResult[T]`; a send returns `TaskSendResult`; a workflow start returns `WorkflowStartResult`; a workflow handle call returns `HandleResult`. Startup errors raise `HorsiesError`.

## TaskError

| Field | Type | Description |
| ----- | ---- | ----------- |
| `code` | `BuiltInTaskCode \| str` | Required. Built-in or custom error code |
| `message` | `str \| None` | Human-readable description |
| `data` | `JsonInput` on write, `JsonValue` on read | Additional context (task_id, etc.) |
| `exception` | `BaseException \| FlattenedException \| None` | Live exception in-process; flattened to a `FlattenedException` TypedDict (`type`, `module`, `message`, `repr`, `traceback`) when serialized to the wire |

`code` must not be null: `auto_retry_for` and outcome routing read it. `TaskError` refuses a key that it does not declare, at construction and at decode. `SubWorkflowError` adds `sub_workflow_id` and `sub_workflow_summary`, and it refuses any other key. A stored error with an unknown key, or without the key `error_code` (the stored key of `code`), is stored data that does not parse: see [Decode Errors](/horsies/tasks/task-types/#decode-errors).

## Validation Failure Text

When a value fails pydantic validation, the library writes each failure as `<loc>: <msg> [<type>]`. Error messages, stored `TaskError` fields, flattened exceptions and log lines use this text. It names the field, the message and the error type. It does not contain the value.

- The text shows at most 10 failures, then `(+N more)`.
- For a task kwarg, `<loc>` starts with the kwarg name.
- When a message template uses a value from the input, the library writes `<input>` in its place. Example: `Input tag '<input>' found using 'kind' does not match any of the expected tags: 'cat', 'dog'`.
- Text that your own validator writes is kept as written. This applies to `value_error`, `assertion_error` and custom `PydanticCustomError` types. Do not put input values into validator messages that must not be stored.
- A key from the input can be part of `<loc>` in two cases: the validated type is a dict or a mapping, or contains one; or a model, `TypedDict` or dataclass with `extra='forbid'` refuses the key.

The cause object follows the same rule. Where the library catches a pydantic `ValidationError`, it keeps a `ValueValidationError` with the same text instead (`horsies.core.codec.ValueValidationError`, a subclass of `SerializationError` and of `ValueError`). For example, `TaskSendValidationFailed.exception` is a `ValueValidationError` when a kwarg fails its declared type. When task code raises a `ValidationError`, the stored `TaskError` keeps the same text in `message` and in the `repr` and `traceback` of the flattened exception.

## Read Errors (ReadResult)

`TaskHandle.get()`, `WorkflowHandle.get()`, `WorkflowHandle.result_for()`, `app.get_result()` and their `*_async` forms return `ReadResult[T, TaskError]` = `Ok[Succeeded[T]] | Ok[Failed[TaskError]] | Err[ReadError]`:

- `Ok(Succeeded(value))`: the task returned `value`.
- `Ok(Failed(task_error))`: the task returned `task_error`, or the library recorded it (`TASK_CANCELLED`, `TASK_TIMEOUT`, `WORKER_CRASHED`, ...).
- `Err(read_error)`: the read gave no outcome.

`ReadError` is a closed union of frozen classes, one class per catalog code. Every class has `code` (its catalog member) and `message` (the subject and the cause). `BrokerError` adds `retryable` (whether the cause may clear) and `exception` (the cause, or `NoCause()`). Match on the class; a `match` that omits a class fails pyright's exhaustiveness check.

| Class | `code` | When | Kept by a `TaskHandle` |
| ----- | ------ | ---- | ---------------------- |
| `WaitTimeout` | `RetrievalCode.WAIT_TIMEOUT` | The task or the workflow did not end before the wait limit. It may still run | No |
| `TaskNotFound` | `RetrievalCode.TASK_NOT_FOUND` | Neither the live table nor the task history holds the task. A handle built by id can race the commit of its send | No |
| `WorkflowNotFound` | `RetrievalCode.WORKFLOW_NOT_FOUND` | No workflow has the id of the handle | – |
| `ResultNotAvailable` | `RetrievalCode.RESULT_NOT_AVAILABLE` | The task reached a terminal status and stored no result, or the handle has no broker | Yes |
| `ResultNotReady` | `RetrievalCode.RESULT_NOT_READY` | `result_for()`: the node stores no result yet | – |
| `WaitInsideTask` | `RetrievalCode.WAIT_INSIDE_TASK` | A result wait in a process that runs task bodies. Nothing was read | No |
| `AsyncContext` | `RetrievalCode.ASYNC_CONTEXT` | A sync read on a thread that runs an event loop. Nothing was read. Call the `*_async` form | No |
| `InvalidInput` | `RetrievalCode.INVALID_INPUT` | `timeout_ms` is not `None` or an int from 0 to `MAX_TASK_TIMEOUT_MS`. Nothing was read | No |
| `ResultDeserializationError` | `OperationalErrorCode.RESULT_DESERIALIZATION_ERROR` | The stored result does not read as the result of the declared types | Yes |
| `UnknownCustomCode` | `OperationalErrorCode.RESULT_DESERIALIZATION_ERROR` | The stored error has a custom code that the declared `Enum` of the task does not name. `custom_code` holds the stored text | Yes |
| `BrokerError` | `OperationalErrorCode.BROKER_ERROR` | The database, the connection or the core call failed. A later read may succeed | No |
| `NoTypeAvailable` | `ContractCode.NO_TYPE_AVAILABLE` | The task succeeded, and this process has no declared type to read the value: the handle was built by id, or the task is not registered here | Yes |
| `WorkflowCtxMissingId` | `ContractCode.WORKFLOW_CTX_MISSING_ID` | `result_for()` got a node with no `node_id`. Nothing was read | – |

A failed task decodes without a declared type, so `NoTypeAvailable` comes only for a task that succeeded. A process that does not import the task code still reads failed tasks.

A read does its checks before it reads, in this order. Only the first check that fails gives its class:

1. `InvalidInput`: the wait limit (`timeout_ms`) is not `None` or an int from 0 to `MAX_TASK_TIMEOUT_MS`.
2. `WaitInsideTask`: the read runs in a process that runs task bodies.
3. A `TaskHandle` returns the read that it keeps.
4. `AsyncContext`: a sync read runs on a thread that runs an event loop.

The Rust and the TypeScript clients use the same order. So a read inside a task with a wait above the limit gives `InvalidInput` in each client.

```python
from horsies import (
    BrokerError,
    Err,
    Failed,
    Ok,
    ReadResult,
    Succeeded,
    TaskError,
    TaskNotFound,
    WaitTimeout,
)


def describe_read(read: ReadResult[int, TaskError]) -> str:
    match read:
        case Ok(Succeeded(value)):
            return f'value: {value}'
        case Ok(Failed(task_error)):
            return f'task failed: {task_error.code}'
        case Err(WaitTimeout() | TaskNotFound() | BrokerError()):
            return 'no outcome yet: read again later'
        case Err(read_error):
            return f'no outcome: {read_error.code}: {read_error.message}'
```

## Broker Errors (BrokerResult)

Broker methods return `BrokerResult[T]` = `Result[T, BrokerOperationError]`. The `Err` is an infrastructure-level failure, distinct from a `TaskError` in `Failed`.

Three broker methods return a subset of `BrokerOperationError`, their only failures:

| Method | Returns |
| ------ | ------- |
| `enqueue_async()` | `Result[uuid.UUID, BrokerEnqueueError]` |
| `enqueue()` | `Result[uuid.UUID, BrokerEnqueueError \| BrokerSyncCallError]` |
| `ensure_schema_initialized()` | `Result[None, BrokerOperationFailed]` |

`enqueue()` and `enqueue_async()` require `sent_at`, the call time of the send, which the row stores. A send that repeats a task id is the same send when the stored command fingerprint and `sent_at` equal its own, and answers with that task id; otherwise it is `PAYLOAD_MISMATCH`. A resend therefore passes the `sent_at` of the first send.

They also require `send_kind` (`from horsies import SendKind`): `SendKind.FIRST` for the first send of a task id, `SendKind.REPEAT` for a resend. A repeat reads the task history first, so a resend of a task that finished answers with its task id, or `PAYLOAD_MISMATCH`, and the task does not run again. A first send does not read history.

`BrokerEnqueueError` is `BrokerEnqueueFailed | BrokerPayloadMismatch | BrokerIdempotencyKeyConflict | BrokerInvalidTaskOptions | BrokerInvalidInput`. `BrokerSyncCallError` is `BrokerAsyncContext`: the failure that the sync form adds, where nothing ran. Both are exported from `horsies`.

### Calls That Run on the Core

These calls run on the core of the process: `enqueue()`, `enqueue_async()`, `ensure_schema_initialized()`, `get_task_info()`, `get_task_info_async()`, `get_raw_result_record()`, `get_raw_result_record_async()`, `ping_database_async()`, `ping_workers_async()`, and the task sends (`send()`, `send_async()` and the two resend methods). The core has one set of pools per broker, for every thread and event loop.

- The sync forms `enqueue()`, `get_task_info()` and `get_raw_result_record()` call the core on the calling thread. They return `ASYNC_CONTEXT` on a thread that runs an event loop.

When the core of the process cannot run the call, the call returns its failure code (for example `ENQUEUE_FAILED`, `TASK_INFO_QUERY_FAILED`, `DB_PING_FAILED`, `WORKER_PING_FAILED` or `SCHEMA_INIT_FAILED`) with the core error as `exception`. Import the core errors from `horsies`:

| Core error | When | `retryable` | After the failure |
| ---------- | ---- | ----------- | ----------------- |
| `TooManyPendingCalls` | 65,536 core calls are in flight on the calling event loop | `True` | Nothing ran |
| `CoreCallTimedOut` | A sync form did not end within 30 minutes. For `get_raw_result_record()` with a `timeout_ms`, the bound is `timeout_ms` plus 30 minutes; with no `timeout_ms`, the call has no bound | `True` | A read changed nothing. After `enqueue()` or a sync send, the row may or may not exist |
| `CoreStartFailed` | The core runtime could not start | `False` | Nothing ran |

Two more core errors are never a result: see [Process-State Refusals](#process-state-refusals).

#### Process-State Refusals

`ForkedAfterCoreStart` and `CoreStopped` are states of the process, not of one call. The core refuses every later call in the process, so a retry in the same process gets the same refusal. Import both from `horsies`.

- Every call that runs on the core raises them. No call returns them as a result, and no result type has a variant for them.
- This covers the task sends and the two resend methods, `enqueue()`, the task, result and health reads of the app and the broker, the broker's task lists and recovery passes, `ensure_schema_initialized()`, `check(live=True)`, the workflow start and its retry, every `WorkflowHandle` method that reads or changes the workflow, `pause_workflow()` and `resume_workflow()`, the reads and `cancel_task()` of `horsies.monitoring`, and the schema probe of `horsies web`. Each sync form and each `*_async` form raises them.
- Nothing ran. `close()` and `close_async()` raise them from the close of the core handle.
- In a worker child, the child ends itself at its next core call and the worker replaces it. The child exits with code 80; its last log line names the cause.
  - At the start of a task, before task code ran: the worker releases the claim with no attempt charged, and a new child runs the task.
  - After a body that finished: the child reports the result first, and the worker writes it as for any task. The task keeps its result, with no second run.
  - A body that the refusal ended (a core call of the body raised `CoreStopped` and the body let it out, or returned it as its error) has no result: the child reports nothing, and the task takes the crashed-child path (`WORKER_CRASHED`) and its retry policy. So does every other run of a shared child whose body still runs when the child exits.
  - A fork of a child exits at once and writes nothing to the worker.
- The Rust client has the fork state only. Every public call of the Rust client that runs on the core panics in a process that was forked without `exec` after the core's process runtime started in its parent, async and blocking forms alike, with both process ids and the fix; nothing ran. No Rust result type holds the fork. The Rust client has no stopped-core state: a panic in a core task of a Rust program is the program's panic.

| Raised | Cause | What to do |
| ------ | ----- | ---------- |
| `ForkedAfterCoreStart` | The process was forked without exec after the core started in its parent, for example a `multiprocessing` pool with the `fork` start method inside a task. `args` are `(core_pid, this_pid, advice)` | Start child processes that call horsies with the `spawn` or `forkserver` start method. Or start the core after the fork: the core starts at the first call that runs on it, so fork before the parent's first call |
| `CoreStopped` | A background task of the core panicked: a defect in horsies, not a failure of the database or of your code. The core then stops in this process. `args` are the `(subject, cause)` of the first panic | Restart the process: the core does not start again in it. Report the panic's subject and cause as a defect |

When the core ran the call and the call failed, `exception` is the core's class of the cause. Import each class from `horsies`. The declared type of `exception` does not change:

| Class | When | Fields |
| ----- | ---- | ------ |
| `CoreServerError` | The server answered with an error | `subject`, `sqlstate`, `message` (the server's primary message), `retryable` |
| `CoreConnectionError` | No answer from the server: a connect or a checkout failed or timed out, the connection was lost, or a reply did not come within its bound | `subject`, `message`, `retryable` |
| `CoreContractViolation` | A row, a stored value or a result broke the contract that the core reads it with. The same call meets it again | `subject`, `message` |
| `CorePreconditionFailed` | A precondition of the call does not hold: the settings, the client family or the schema of the database, an argument, or a key or a lock that another command holds | `subject`, `message`, `retryable` |

`CoreServerError` and `CoreConnectionError` share the base class `CoreDatabaseError`; the four classes share `CoreFailure`. `subject` names the core operation, for example `release_claims`. A lock that another session holds is `CoreServerError` with `sqlstate` `'55P03'`.

<!-- members: BrokerErrorCode -->
| Code | Description | Retryable |
| ---- | ----------- | --------- |
| `SCHEMA_INIT_FAILED` | Schema initialization failed | Varies |
| `ENQUEUE_FAILED` | The enqueue write failed or could not be confirmed, or the core of the process could not run it ([Calls That Run on the Core](#calls-that-run-on-the-core)); the row may or may not exist | Varies |
| `PAYLOAD_MISMATCH` | The task id exists, and the command or the idempotency key differs from the original send | No |
| `IDEMPOTENCY_KEY_CONFLICT` | The idempotency key is reserved by another task with a different command; `owner_task_id` names the owner task | No |
| `TASK_INFO_QUERY_FAILED` | Task-info or raw-result query failed | Varies |
| `MONITORING_QUERY_FAILED` | Monitoring or worker-state query failed | Varies |
| `DB_PING_FAILED` | `ping_database` reachability probe failed | Varies |
| `WORKER_PING_FAILED` | `ping_workers` failed: the reply subscribe or the NOTIFY round trip failed | Varies |
| `CLEANUP_FAILED` | Cleanup or retention operation failed | Varies |
| `CLOSE_FAILED` | Broker close failed | Varies |
| `LISTENER_START_FAILED` | LISTEN/NOTIFY listener failed to start | Varies |
| `LISTENER_SUBSCRIBE_FAILED` | LISTEN/NOTIFY subscribe failed | Varies |
| `NO_BROKER` | Operation needs a broker, but none is configured | No |
| `ASYNC_CONTEXT` | A sync method (`app.get_raw_result()`, `app.ping_database()`, `broker.close()`, ...) was called on a thread that runs an event loop; nothing ran. Call the `*_async` form | No |
| `NO_TYPE_AVAILABLE` | Typed decode at the `app` layer needs an `ok_type`, but `task_name` is not in the local task registry | No |
| `INVALID_JSON_PAYLOAD` | Stored result JSON is malformed, not a JSON object, has an invalid strict-serde envelope, or fails app-level typed slot decode | No |
| `INVALID_TASK_OPTIONS` | `enqueue_async` received task options that are not valid JSON or not a JSON object, a `retry_policy` that does not validate as `RetryPolicy` (strict types, no unknown fields), an `execution` that is not an `Execution` value, or a `cancel_grace_ms` that does not fit the class (100 to 10,000 ms for `SHARED_ASYNC`; absent, null, or 100 to 10,000 ms for `ISOLATED`; absent or null for `SHARED_THREAD`), or a `SHARED_ASYNC` or `SHARED_THREAD` class with no `timeout_ms` (absent or null). The message names each failing field. Nothing was written | No |
| `INVALID_INPUT` | An argument was refused before anything ran: a non-positive `timeout_seconds`, `min_responses` or history `limit`, a `target_worker_id` that is not a string, both `enqueued_at` and `enqueue_delay_seconds`, a future `sent_at` without either, a naive `sent_at`, `enqueued_at` or `good_until`, an `input_json` that does not parse, that repeats a key in an object, or that is not a JSON object or null, an empty task or queue name, a priority outside 1..100, a negative `enqueue_delay_seconds`, an empty `retention_class_key`, or an idempotency key that is empty or over 255 bytes. Nothing was written | No |
| `LISTENER_DISABLED` | The broker was built with `assume_initialized=True` and has no listener, so `ping_workers` cannot run. Nothing ran | No |
| `WAIT_INSIDE_TASK` | A raw result read (`app.get_raw_result()`, `broker.get_raw_result_record()` or an `*_async` form) was called in a worker child, where task bodies run. Nothing ran. Compose the tasks with a workflow: see [Waiting for a Result Inside a Task](/horsies/concepts/result-handling/#waiting-for-a-result-inside-a-task) | No |

### BrokerOperationError

`BrokerOperationError` is a union of frozen variants. The nine operation codes from `SCHEMA_INIT_FAILED` to `LISTENER_SUBSCRIBE_FAILED` share `BrokerOperationFailed`, which keeps the code in its `code` field. Every other code has a variant of its own. Every variant has `code`, `message` and `retryable`. Match on the variant; a `match` that omits a variant fails pyright's exhaustiveness check.

| Variant | `code` | Fields besides `message` | `retryable` | After the failure |
| ------- | ------ | ------------------------ | ----------- | ----------------- |
| `BrokerOperationFailed` | one of the nine operation codes | `exception` | the core's classification | A read changed nothing. A cleanup may have finished part of its rows |
| `BrokerEnqueueFailed` | `ENQUEUE_FAILED` | `exception` | the core's classification | The row may or may not exist. An enqueue again with the same task id and payload returns the existing task |
| `BrokerPayloadMismatch` | `PAYLOAD_MISMATCH` | – | `False` | Nothing was written |
| `BrokerIdempotencyKeyConflict` | `IDEMPOTENCY_KEY_CONFLICT` | `owner_task_id` | `False` | Nothing was written |
| `BrokerNotAttached` | `NO_BROKER` | – | `False` | Nothing ran |
| `BrokerNoTypeAvailable` | `NO_TYPE_AVAILABLE` | – | `False` | The stored result is unchanged |
| `BrokerInvalidJsonPayload` | `INVALID_JSON_PAYLOAD` | `exception` | `False` | The stored data is unchanged |
| `BrokerInvalidTaskOptions` | `INVALID_TASK_OPTIONS` | `exception` | `False` | Nothing was written |
| `BrokerInvalidInput` | `INVALID_INPUT` | `exception` | `False` | Nothing was written |
| `BrokerAsyncContext` | `ASYNC_CONTEXT` | – | `False` | Nothing ran |
| `BrokerListenerDisabled` | `LISTENER_DISABLED` | – | `False` | Nothing ran |
| `BrokerWaitInsideTask` | `WAIT_INSIDE_TASK` | – | `False` | Nothing ran |

| Field | Type | Description |
| ----- | ---- | ----------- |
| `message` | `str` | Human-readable description |
| `retryable` | `bool` | The classification of the cause where the variant has a cause from the database or the core: `True` when the condition may clear, for example a lost connection or `TooManyPendingCalls`. `False` on every other variant |
| `exception` | `BaseException` on `BrokerOperationFailed`; `BaseException \| NoCause` on the other variants that have it | The cause. `NoCause()` when a check found the failure without a Python exception |
| `owner_task_id` | `uuid.UUID` | The task that holds the idempotency key |

The repr of a variant shows its class name and its other fields. `message` and `exception` are not in the repr.

`broker_error_cause(err)` returns the cause of any variant, or `NoCause()` for a variant without one. Match on it before `raise ... from`, `exc_info=` or an error tracker call:

```python
from horsies import (
    BrokerEnqueueFailed,
    BrokerOperationError,
    BrokerOperationFailed,
    NoCause,
    broker_error_cause,
)


def should_retry(err: BrokerOperationError) -> bool:
    match err:
        case BrokerOperationFailed(retryable=retryable) | BrokerEnqueueFailed(retryable=retryable):
            return retryable
        case _:
            return False


def raise_broker_error(err: BrokerOperationError) -> None:
    match broker_error_cause(err):
        case NoCause():
            raise RuntimeError(err.message)
        case cause:
            raise RuntimeError(err.message) from cause
```

## Send Errors (TaskSendResult)

Send errors are returned via `TaskSendResult[TaskHandle[T]]` from `.send()`, `.send_async()`, `.resend()` and `.resend_async()`. These are separate from the result of the task: they indicate that enqueuing the task itself failed, not that task execution failed.

### TaskSendError

`TaskSendError` is a union with one frozen variant per `TaskSendErrorCode` member. Each variant has only the fields of its failure, and `err.code` names its member. Match on the variant; a `match` that omits a variant fails pyright's exhaustiveness check. The handling pattern is in [Sending Tasks](/horsies/tasks/sending-tasks/#resending-failed-sends).

| Variant | `code` | Fields |
| ------- | ------ | ------ |
| `TaskSendSuppressed` | `SEND_SUPPRESSED` | `message` |
| `TaskSendAsyncContext` | `ASYNC_CONTEXT` | `task_id`, `payload`, `message` |
| `TaskSendValidationFailed` | `VALIDATION_FAILED` | `message`, `exception` |
| `TaskSendEnqueueFailed` | `ENQUEUE_FAILED` | `task_id`, `payload`, `message`, `retryable`, `exception` |
| `TaskSendPayloadMismatch` | `PAYLOAD_MISMATCH` | `task_id`, `payload`, `message` |
| `TaskSendPayloadTooLarge` | `PAYLOAD_TOO_LARGE` | `message`, `size_bytes`, `limit_bytes` |
| `TaskSendIdempotencyKeyConflict` | `IDEMPOTENCY_KEY_CONFLICT` | `task_id`, `payload`, `message`, `owner_task_id`; `retryable` is `True` |

| Field | Type | Description |
| ----- | ---- | ----------- |
| `message` | `str` | Human-readable description |
| `task_id` | `uuid.UUID` | Task id of the prepared send |
| `payload` | `TaskSendPayload` | Serialized envelope that the resend methods send again |
| `retryable` | `bool` | `TaskSendEnqueueFailed`: the core's classification of the cause, `True` when the condition may clear, for example a lost connection or `TooManyPendingCalls`. `TaskSendIdempotencyKeyConflict`: a class variable that is always `True`, because the conflict clears when the reservation ends |
| `exception` | `BaseException \| NoCause` | The cause. `NoCause()` when a check found the failure without a Python exception |
| `owner_task_id` | `uuid.UUID` | The task that holds the idempotency key |
| `size_bytes` | `int` | Bytes of the UTF-8 text of the serialized input |
| `limit_bytes` | `int` | The configured `payload.reject_bytes` |

The repr of a variant shows its class name and its other fields. `payload`, `message` and `exception` are not in the repr.

`NoCause` is not an exception. Match on `exception` before `raise ... from`, `exc_info=` or an error tracker call:

```python
import logging

from horsies import NoCause, TaskSendEnqueueFailed

logger = logging.getLogger(__name__)


def log_enqueue_failure(err: TaskSendEnqueueFailed) -> None:
    match err.exception:
        case NoCause():
            logger.error('enqueue failed: %s', err.message)
        case cause:
            logger.error('enqueue failed: %s', err.message, exc_info=cause)
```

For a validation failure, the cause names the field, the message and the type, not the value. Text from your own validators is kept as written ([Validation Failure Text](#validation-failure-text)).

`TaskSendResendable` is `TaskSendEnqueueFailed | TaskSendAsyncContext | TaskSendIdempotencyKeyConflict`. It is the parameter type of `.resend()` and `.resend_async()`; another variant is a type error, and at run time it fails an assertion that names the variant. These three variants carry a `TaskSendPayload`, and the resend methods send that exact payload again: the same task id, command, `sent_at` and delay. If the first send was written, the broker finds the same command fingerprint and `sent_at` on that task id and answers with the task id instead of writing a second row, also when the task already finished (a resend reads the task history).

## Workflow Start Errors (WorkflowStartResult)

`spec.start()`, `start_async()`, `retry_start()` and `retry_start_async()` return `WorkflowStartResult[WorkflowHandle[T]]` = `Result[WorkflowHandle[T], WorkflowStartError]`. `WorkflowStartError` is a union with one frozen variant per code; the variants and their fields are on the [Workflow API](/horsies/concepts/workflows/workflow-api/) page.

The retry methods accept `ENQUEUE_FAILED` and `ASYNC_CONTEXT` (`WorkflowStartResendable`).

## Workflow Handle Errors (HandleResult)

`WorkflowHandle` methods that read or change the workflow row (`status()`, `results()`, `tasks()`, `cancel()`, `pause()`, `resume()` and their `*_async` forms) and `pause_workflow()` / `resume_workflow()` and their sync forms return `HandleResult[T]` = `Result[T, HandleOperationError]`. `get()`, `result_for()` and their `*_async` forms return a `ReadResult` instead: see [Read Errors](#read-errors-readresult).

`HandleOperationError` is a union with one frozen variant per `HandleErrorCode` member. Every variant has `code`, `message`, `retryable` and `workflow_id`. `message` and the cause do not show in `repr()`. Match on the variant. Any other exception, for example a defect in `pause_workflow()` or `resume_workflow()`, propagates.

`WorkflowHandle.for_id(text, broker)` returns `HandleInvalidWorkflowId` for a workflow id text that is not a UUID: `message`, `workflow_id_text` and `exception` (the `ValueError` of `uuid.UUID`). No handle is built. It is not a `HandleOperationError` variant and has no `HandleErrorCode`.

## Custom Error Codes

Domain-specific errors use string codes:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Err, JsonValue, Ok, Result, TaskError

@app.task("validate_order")
def validate_order(*, order_id: str) -> Result[dict[str, JsonValue], TaskError]:
    order = get_order(order_id)

    if order.total > 10000:
        return Err(TaskError(
            code="ORDER_LIMIT_EXCEEDED",
            message=f"Order {order_id} exceeds limit",
            data={"order_id": order_id, "total": order.total},
        ))

    return Ok({"status": "valid"})
```

Declare a task's codes with `error_codes=` to give each one its own value of the `error_code` metric label; another custom code counts as `undeclared` there. See [Declare Error Codes](/horsies/tasks/defining-tasks/#declare-error-codes).

With an `Enum` and `TaskError[E]`, the codes are typed from the body to the reader. A `match` on the code names each member and each built-in family:

```python
from enum import Enum

from horsies import (
    ContractCode,
    Err,
    Failed,
    Ok,
    OperationalErrorCode,
    OutcomeCode,
    ReadResult,
    RetrievalCode,
    Succeeded,
    TaskError,
)


class WelcomeCode(str, Enum):
    INVALID_EMAIL = "INVALID_EMAIL"


def welcome_text(read: ReadResult[str, TaskError[WelcomeCode]]) -> str:
    match read:
        case Ok(Succeeded(message)):
            return message
        case Ok(Failed(task_error)):
            match task_error.code:
                case WelcomeCode.INVALID_EMAIL:
                    return "the address is not valid"
                case OperationalErrorCode() | ContractCode() | RetrievalCode() | OutcomeCode():
                    return f"library failure: {task_error.code.value}"
        case Err(read_error):
            return f"no outcome: {read_error.code}"
```

Custom codes are auto-retried when listed in `auto_retry_for`:

```python
@app.task(
    "call_api",
    retry_policy=RetryPolicy.fixed([30, 60, 120], auto_retry_for=["RATE_LIMITED", "SERVICE_UNAVAILABLE"]),
)
def call_api(*, url: str) -> Result[dict[str, JsonValue], TaskError]:
    ...
```

## Reserved Built-In Code Enforcement

All string values used by the built-in error code families (`OperationalErrorCode`, `ContractCode`, `RetrievalCode`, `OutcomeCode`) are **reserved**. Custom error codes must not collide with these values.

**Runtime enforcement**: `TaskError(code="BROKER_ERROR", message="db down")` raises `ValueError` at construction time. Built-in codes must be passed as enum members. Custom codes must be plain `str`, not `str, Enum` subclasses:

```python
TaskError(code=OperationalErrorCode.BROKER_ERROR, message="db down")  # correct
TaskError(code="MY_CUSTOM_CODE", message="card declined")             # correct — custom code
TaskError(code="BROKER_ERROR", message="db down")                     # ValueError — reserved
```

`WORKFLOW_STOPPED` is also reserved, and it is not a task code: it names a workflow task that stopped before its body ran. `TaskError(code="WORKFLOW_STOPPED", ...)` raises `ValueError`, and a stored task error with this code does not decode. An `exception_mapper` value, a `default_unhandled_error_code` or an `auto_retry_for` entry with this name is refused like any other invalid code.

**Serialization**: built-in codes are serialized as tagged dicts `{"__builtin_task_code__": "BROKER_ERROR"}` so round-trip identity is preserved through standard `model_validate()` / `model_validate_json()`. Custom codes are serialized as plain strings. This tagged format eliminates wire-format ambiguity between built-in and custom codes, allowing `model_validate()` to work correctly everywhere — including nested models. The stored form (the `err` slot of a stored result) has the key `error_code` in place of `code`: `encode_task_error` writes it and `decode_task_error` reads it.

`horsies check` detects reserved-code collisions in statically visible configuration:

- **`exception_mapper` values** — if any mapped error code string matches a reserved built-in code, `horsies check` reports `HRS-212`.
- **`default_unhandled_error_code`** — if set to a reserved built-in code other than the library default `UNHANDLED_EXCEPTION`, `horsies check` reports `HRS-212`.

The library default `UNHANDLED_EXCEPTION` is intentionally a built-in code and is not flagged.

Example collision that `horsies check` catches:

```python
from pydantic import SecretStr

app = Horsies(
    config=AppConfig(
        broker=PostgresConfig(database_url=SecretStr('postgresql://...')),
        # HRS-212: 'BROKER_ERROR' collides with OperationalErrorCode.BROKER_ERROR
        exception_mapper={ValueError: 'BROKER_ERROR'},
    ),
)
```

Use a custom string that does not match any built-in code:

```python
exception_mapper={ValueError: 'VALIDATION_FAILED'}  # OK — not reserved
```

The full list of reserved strings is available at runtime via `BUILTIN_CODE_REGISTRY` from `horsies.core.models.tasks`.

## Validating with `horsies check`

Use the `horsies check` command to validate your configuration, task registry, and workflow definitions without starting any services. This is recommended for CI/CD pipelines to catch blocking errors before deployment.

```bash
horsies check myapp.instance:app
```

On success it prints the task count, the declared error codes of each task that declares some, and the tracing setting (`OpenTelemetry` for `AppConfig(tracing=OtelTracing())`, else `off`):

```text
ok: all validations passed
  3 task(s) registered
  declared error codes:
    charge_card: CARD_DECLINED, RATE_LIMITED
    send_receipt: BOUNCED
  tracing: off
```

With no declared code, the codes line is `  declared error codes: none`.

## HorsiesError Structure

Startup errors provide Rust-style formatting automatically. When raised, they display with source location and context:

```text
error[HRS-007]: cycle detected in workflow DAG
  --> /app/workflows/order.py:12
   |
12 | node_b = TaskNode(fn=process, waits_for=[node_a])
   | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
   = note: workflows must be acyclic directed graphs (DAG)

   = help:
        remove circular dependencies between tasks
```

Access fields programmatically when catching the exception:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies.core.errors import HorsiesError

try:
    spec = app.workflow(
        name="order_flow",
        tasks=[node_a, node_b],
        definition_key="myapp.order_flow.v1",
    )
except HorsiesError as e:
    e.code       # ErrorCode.WORKFLOW_CYCLE_DETECTED
    e.message    # "cycle detected in workflow DAG"
    e.location   # SourceLocation(file="/app/workflows/order.py", line=12)
    e.notes      # ["workflows must be acyclic directed graphs (DAG)"]
    e.help_text  # "remove circular dependencies between tasks"
```

## API Reference

### `Result[T, TaskError]`

The return type of a task body: `Ok[T] | Err[TaskError]`.

| Member | Field | Description |
| ------ | ----- | ----------- |
| `Ok(value)` | `ok_value: T` | The task succeeded with `value` |
| `Err(error)` | `err_value: TaskError` | The task failed with `error` |

### `TaskOutcome[T, TaskError]`

How a task ended: `Succeeded[T] | Failed[TaskError]`. A workflow node input from `args_from` and `WorkflowContext.result_for()` have this type.

| Class | Field | Description |
| ----- | ----- | ----------- |
| `Succeeded` | `value: T` | The task returned this value |
| `Failed` | `error: TaskError` | The task returned this error, or the library recorded it |

### `ReadResult[T, TaskError]`

The result of a read: `Ok[Succeeded[T]] | Ok[Failed[TaskError]] | Err[ReadError]`. The classes of `ReadError` are in [Read Errors](#read-errors-readresult).

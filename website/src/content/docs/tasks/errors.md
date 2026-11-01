---
title: Errors Reference
summary: Exhaustive reference of all error codes and types in horsies.
related: [error-handling, ../../concepts/result-handling]
tags: [errors, TaskError, BuiltInTaskCode, ErrorCode, reference]
---

# Errors Reference

Horsies has two error systems: **runtime errors** returned in `TaskResult`, and **blocking startup errors** raised during app initialization.

## Runtime Errors (TaskResult)

Runtime errors are returned via `TaskResult[T, TaskError]`. The `TaskError.error_code` field contains either a `BuiltInTaskCode` member or a user-defined string.

When a task reaches a terminal `FAILED` state with a `TaskResult(err=TaskError(...))`, the `error_code` is persisted to `horsies_tasks.error_code` for queryability. This column is `NULL` for successful and non-terminal tasks. Worker/process failures synthesize a `TaskResult(err=TaskError(...))` where possible, so `WORKER_CRASHED` and serialization failures are queryable. Access it via `TaskInfo.error_code` from `handle.info()`.

Each execution attempt is also recorded in `horsies_task_attempts` with per-attempt `error_code`, `error_message`, and outcome. See [Retrieving Results](retrieving-results#task-metadata-and-attempt-history) for API usage.

### TaskError

| Field | Type | Description |
| ----- | ---- | ----------- |
| `error_code` | `BuiltInTaskCode \| str \| None` | Library or domain error code |
| `message` | `str \| None` | Human-readable description |
| `data` | `Any \| None` | Additional context (task_id, etc.) |
| `exception` | `BaseException \| FlattenedException \| None` | Live exception in-process; flattened to a `FlattenedException` TypedDict (`type`, `module`, `message`, `repr`, `traceback`) when serialized to the wire |

`TaskError` refuses a key that it does not declare, at construction and at decode. `SubWorkflowError` adds `sub_workflow_id` and `sub_workflow_summary`, and it refuses any other key. A stored error with an unknown key is stored data that does not parse: see [decode-error surfaces](../../internals/serialization#decoding-consumer-side).

### BuiltInTaskCode (4-Family Split)

All library-defined error codes are grouped into four families. The umbrella type alias `BuiltInTaskCode` is the union of all four enums. User code should handle these explicitly.

#### OperationalErrorCode

Errors from task execution, broker, and worker internals.

<!-- members: OperationalErrorCode -->
| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `UNHANDLED_EXCEPTION` | Uncaught exception in task code, wrapped to `UNHANDLED_EXCEPTION` by library  | Yes, if in `auto_retry_for` |
| `TASK_EXCEPTION` | Task returned `None` instead of a `TaskResult`, or the worker caught an exception outside the task wrapper (for example `KeyboardInterrupt`). A raised exception in task code gives `UNHANDLED_EXCEPTION` | Yes, if in `auto_retry_for` |
| `WORKER_CRASHED` | Worker process died during execution | Yes, if in `auto_retry_for` |
| `BROKER_ERROR` | Database or broker failure during operation | No |
| `WORKER_RESOLUTION_ERROR` | Failed to resolve task by name | No |
| `WORKER_SERIALIZATION_ERROR` | Failed to serialize/deserialize task data | No |
| `RESULT_DESERIALIZATION_ERROR` | A stored result or a stored workflow error is corrupt or could not be deserialized | No |
| `WORKFLOW_ENQUEUE_FAILED` | Workflow node failed after READY->ENQUEUED transition during enqueue/build | No |
| `SUBWORKFLOW_LOAD_FAILED` | Subworkflow definition could not be loaded | No |

#### ContractCode

Errors from type/schema validation and structural contracts.

<!-- members: ContractCode -->
| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `RETURN_TYPE_MISMATCH` | Task return type doesn't match declaration | No |
| `NO_TYPE_AVAILABLE` | Outputless workflow per-node decode: terminal task name not in the local registry, so no `task_ok_type` to decode with | No |
| `WORKFLOW_CTX_MISSING_ID` | Workflow context is missing required ID | No |

`RETURN_TYPE_MISMATCH` does not store the returned value. `message` names the variant, the actual type, the expected type and the failing fields. `data` holds `variant`, `expected_type`, `actual_type`, `validation_errors` (at most 10 entries, each with `loc`, `msg` and `type`) and `validation_error_count`. `exception` is `None`. A validator whose error text holds a lone surrogate gives one `validation_errors` entry of type `lone_surrogate`, because pydantic cannot build that text.

#### RetrievalCode

Errors from `handle.get()` / `get_async()`. These indicate issues retrieving the result, not task execution failures.

<!-- members: RetrievalCode -->
| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `WAIT_TIMEOUT` | `get()` timed out; task may still be running | No |
| `TASK_NOT_FOUND` | Task ID doesn't exist in database | No |
| `WORKFLOW_NOT_FOUND` | Workflow ID doesn't exist in database, or retention removed the workflow after `get()` read its terminal status | No |
| `RESULT_NOT_AVAILABLE` | Result cache was never set | No |
| `RESULT_NOT_READY` | Result not yet available; task is still running | No |
| `EVENT_LOOP_LIMIT` | The broker serves its limit of open event loops (5) and this is a new one; not cached, clears when one of those loops closes | No |
| `ASYNC_CONTEXT` | Sync `get()` / `result_for()` was called on a thread that runs an event loop; nothing ran; not cached. Call `await handle.get_async()` / `result_for_async()` | No |

#### OutcomeCode

Terminal outcome codes for tasks and workflows.

<!-- members: OutcomeCode -->
| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `TASK_CANCELLED` | Task was cancelled before completion | No |
| `TASK_EXPIRED` | Task's `good_until` deadline passed before execution started (PENDING or CLAIMED) | No |
| `TASK_TIMEOUT` | Task exceeded its `timeout_ms` execution limit; the child process was killed | Yes, if in `auto_retry_for` |
| `WORKFLOW_PAUSED` | Workflow was paused | No |
| `WORKFLOW_EXPIRED` | A paused workflow passed `paused_workflow_auto_cancel_after` and expired; `data` holds the policy name and the age | No |
| `WORKFLOW_FAILED` | Workflow failed | No |
| `WORKFLOW_CANCELLED` | Workflow was cancelled | No |
| `UPSTREAM_SKIPPED` | Upstream task in workflow was skipped | No |
| `SUBWORKFLOW_FAILED` | Subworkflow failed | No |
| `WORKFLOW_SUCCESS_CASE_NOT_MET` | Workflow success condition was not satisfied | No |
| `WORKFLOW_OUTPUT_SKIPPED` | A `COMPLETED` workflow has no output: its output node, or a terminal node of an outputless workflow, is SKIPPED, and a success case does not require that node; `data` names the workflow and the node | No |

### Validation Failure Text

When a value fails pydantic validation, the library writes each failure as `<loc>: <msg> [<type>]`. Error messages, stored `TaskError` fields, flattened exceptions and log lines use this text. It names the field, the message and the error type. It does not contain the value.

- The text shows at most 10 failures, then `(+N more)`.
- For a task kwarg, `<loc>` starts with the kwarg name.
- When a message template uses a value from the input, the library writes `<input>` in its place. Example: `Input tag '<input>' found using 'kind' does not match any of the expected tags: 'cat', 'dog'`.
- Text that your own validator writes is kept as written. This applies to `value_error`, `assertion_error` and custom `PydanticCustomError` types. Do not put input values into validator messages that must not be stored.
- A key from the input can be part of `<loc>` in two cases: the validated type is a dict or a mapping, or contains one; or a model, `TypedDict` or dataclass with `extra='forbid'` refuses the key.

The cause object follows the same rule. Where the library catches a pydantic `ValidationError`, it keeps a `ValueValidationError` with the same text instead (`horsies.core.codec.ValueValidationError`, a subclass of `SerializationError` and of `ValueError`). For example, `TaskSendValidationFailed.exception` is a `ValueValidationError` when a kwarg fails its declared type. When task code raises a `ValidationError`, the stored `TaskError` keeps the same text in `message` and in the `repr` and `traceback` of the flattened exception.

### Broker Errors (BrokerResult)

`app.get_result_async()` and broker methods return `BrokerResult[T]` = `Result[T, BrokerOperationError]`. The outer `Err` is an infrastructure-level failure, distinct from the inner `TaskResult.err`.

Three broker methods return a subset of `BrokerOperationError`, their only failures:

| Method | Returns |
| ------ | ------- |
| `enqueue_async()` | `Result[str, BrokerEnqueueError]` |
| `enqueue()` | `Result[str, BrokerEnqueueError \| BrokerSyncCallError]` |
| `ensure_schema_initialized()` | `Result[None, BrokerOperationFailed \| BrokerEventLoopLimit]` |

`BrokerEnqueueError` is `BrokerEnqueueFailed | BrokerPayloadMismatch | BrokerIdempotencyKeyConflict | BrokerInvalidTaskOptions | BrokerInvalidInput | BrokerEventLoopLimit`. `BrokerSyncCallError` is `BrokerAsyncContext | BrokerLoopRunnerFailed`: the failures that the sync form adds, where nothing ran. Both are exported from `horsies`.

<!-- members: BrokerErrorCode -->
| Code | Description | Retryable |
| ---- | ----------- | --------- |
| `SCHEMA_INIT_FAILED` | Schema initialization failed | Varies |
| `ENQUEUE_FAILED` | The enqueue write failed or could not be confirmed; the row may or may not exist | Varies |
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
| `EVENT_LOOP_LIMIT` | The broker serves its limit of open event loops (5) and this call ran on a new one; nothing ran; clears when one of those loops closes | No |
| `ASYNC_CONTEXT` | A sync method (`app.get_result()`, `app.ping_database()`, `broker.close()`, ...) was called on a thread that runs an event loop; nothing ran. Call the `*_async` form | No |
| `LOOP_RUNNER_FAILED` | A sync method could not run the call on the broker's sync runner: the runner is stopped, its thread did not start, or scheduling failed. Nothing ran. `close()` and `close_async()` stop the runner, and a stopped runner does not start again: call the `*_async` form or use a new broker | No |
| `NO_TYPE_AVAILABLE` | Typed decode at the `app` layer needs an `ok_type`, but `task_name` is not in the local task registry | No |
| `INVALID_JSON_PAYLOAD` | Stored result JSON is malformed, not a JSON object, has an invalid strict-serde envelope, or fails app-level typed slot decode | No |
| `INVALID_TASK_OPTIONS` | `enqueue_async` received task options that are not valid JSON, or a `retry_policy` that does not validate as `RetryPolicy` (strict types, no unknown fields). The message names each failing field. Nothing was written | No |
| `INVALID_INPUT` | An argument was refused before anything ran: a non-positive `timeout_seconds`, `min_responses` or history `limit`, a `target_worker_id` that is not a string, both `enqueued_at` and `enqueue_delay_seconds`, a future `sent_at` without either, a naive `sent_at`, `enqueued_at` or `good_until`, malformed args or kwargs JSON, args that are not null or an empty array, kwargs that are not an object or null, an empty task or queue name, a priority outside 1..100, a negative `enqueue_delay_seconds`, an empty `retention_class_key`, or an idempotency key that is empty or over 255 bytes. Nothing was written | No |
| `LISTENER_DISABLED` | The broker was built with `assume_initialized=True` and has no listener, so `ping_workers` cannot run. Nothing ran | No |

#### BrokerOperationError

`BrokerOperationError` is a union of frozen variants. The nine operation codes from `SCHEMA_INIT_FAILED` to `LISTENER_SUBSCRIBE_FAILED` share `BrokerOperationFailed`, which keeps the code in its `code` field. Every other code has a variant of its own. Every variant has `code`, `message` and `retryable`. Match on the variant; a `match` that omits a variant fails pyright's exhaustiveness check.

| Variant | `code` | Fields besides `message` | `retryable` | After the failure |
| ------- | ------ | ------------------------ | ----------- | ----------------- |
| `BrokerOperationFailed` | one of the nine operation codes | `exception` | driver classification | A read changed nothing. A cleanup may have finished part of its rows |
| `BrokerEnqueueFailed` | `ENQUEUE_FAILED` | `exception` | driver classification | The row may or may not exist. An enqueue again with the same task id and payload returns the existing task |
| `BrokerPayloadMismatch` | `PAYLOAD_MISMATCH` | – | `False` | Nothing was written |
| `BrokerIdempotencyKeyConflict` | `IDEMPOTENCY_KEY_CONFLICT` | `owner_task_id` | `False` | Nothing was written |
| `BrokerNotAttached` | `NO_BROKER` | – | `False` | Nothing ran |
| `BrokerNoTypeAvailable` | `NO_TYPE_AVAILABLE` | – | `False` | The stored result is unchanged |
| `BrokerInvalidJsonPayload` | `INVALID_JSON_PAYLOAD` | `exception` | `False` | The stored data is unchanged |
| `BrokerInvalidTaskOptions` | `INVALID_TASK_OPTIONS` | `exception` | `False` | Nothing was written |
| `BrokerInvalidInput` | `INVALID_INPUT` | `exception` | `False` | Nothing was written |
| `BrokerEventLoopLimit` | `EVENT_LOOP_LIMIT` | `loop_limit` | `False` | Nothing ran |
| `BrokerAsyncContext` | `ASYNC_CONTEXT` | – | `False` | Nothing ran |
| `BrokerLoopRunnerFailed` | `LOOP_RUNNER_FAILED` | `exception` | `False` | Nothing ran |
| `BrokerListenerDisabled` | `LISTENER_DISABLED` | – | `False` | Nothing ran |

| Field | Type | Description |
| ----- | ---- | ----------- |
| `message` | `str` | Human-readable description |
| `retryable` | `bool` | Driver classification of the cause where the variant has a cause from the driver: `True` when the condition may clear, for example a lost connection. `False` on every other variant |
| `exception` | `BaseException` on `BrokerOperationFailed` and `BrokerLoopRunnerFailed`; `BaseException \| NoCause` on the other variants that have it | The cause. `NoCause()` when a check found the failure without a Python exception |
| `owner_task_id` | `str` | The task that holds the idempotency key |
| `loop_limit` | `EventLoopLimit` | The limit and the threads of the held event loops |

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

Failed-task results decode without an `ok_type`, so `BrokerErrorCode.NO_TYPE_AVAILABLE` only fires for the success path. Reading failed tasks across processes that don't import the user code still works.

### Send Errors (TaskSendResult)

Send errors are returned via `TaskSendResult[TaskHandle[T]]` from `.send()`, `.send_async()`, `.schedule()` and `.schedule_async()`. These are separate from `TaskResult` -- they indicate that enqueuing the task itself failed, not that task execution failed.

#### TaskSendError

`TaskSendError` is a union with one frozen variant per `TaskSendErrorCode` member. Each variant has only the fields of its failure, and `err.code` names its member. Match on the variant; a `match` that omits a variant fails pyright's exhaustiveness check. The handling pattern is in [Sending Tasks](./sending-tasks#retrying-failed-sends).

| Variant | `code` | Fields |
| ------- | ------ | ------ |
| `TaskSendSuppressed` | `SEND_SUPPRESSED` | `message` |
| `TaskSendAsyncContext` | `ASYNC_CONTEXT` | `task_id`, `payload`, `message` |
| `TaskSendValidationFailed` | `VALIDATION_FAILED` | `message`, `exception` |
| `TaskSendEnqueueFailed` | `ENQUEUE_FAILED` | `task_id`, `payload`, `message`, `retryable`, `exception` |
| `TaskSendPayloadMismatch` | `PAYLOAD_MISMATCH` | `task_id`, `payload`, `message` |
| `TaskSendPayloadTooLarge` | `PAYLOAD_TOO_LARGE` | `message`, `size_bytes`, `limit_bytes` |
| `TaskSendIdempotencyKeyConflict` | `IDEMPOTENCY_KEY_CONFLICT` | `task_id`, `payload`, `message`, `owner_task_id` |
| `TaskSendEventLoopLimit` | `EVENT_LOOP_LIMIT` | `message` |
| `TaskSendLoopRunnerFailed` | `LOOP_RUNNER_FAILED` | `task_id`, `payload`, `message`, `exception` |

| Field | Type | Description |
| ----- | ---- | ----------- |
| `message` | `str` | Human-readable description |
| `task_id` | `str` | Task id of the prepared send |
| `payload` | `TaskSendPayload` | Serialized envelope that the retry methods send again |
| `retryable` | `bool` | Driver classification of the cause: `True` when the condition may clear, for example a lost connection |
| `exception` | `BaseException \| NoCause` | The cause. `NoCause()` when a check found the failure without a Python exception. On `TaskSendLoopRunnerFailed` it is `BaseException`: always the `LoopRunnerError` |
| `owner_task_id` | `str` | The task that holds the idempotency key |
| `size_bytes` | `int` | Length of the serialized kwargs |
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

#### TaskSendErrorCode

<!-- members: TaskSendErrorCode -->
| Code | Description | Retry methods |
| ---- | ----------- | ------------- |
| `SEND_SUPPRESSED` | Send suppressed during worker import/discovery to prevent side effects | Refused |
| `ASYNC_CONTEXT` | Sync send/schedule (or sync retry variant) called inside a running event loop -- the blocking enqueue would stall the loop; `retry_*_async(err)` completes the same dispatch (or call the `*_async` entry point with the original args) | Accepted |
| `VALIDATION_FAILED` | Argument serialization or validation failed before enqueue, the broker refused the input before the write (`INVALID_INPUT`, `INVALID_TASK_OPTIONS`), or a retry method refused the payload (another task, or a schedule payload sent to a send retry) | Refused |
| `ENQUEUE_FAILED` | Broker/database failure during enqueue; the row may or may not exist | Accepted |
| `PAYLOAD_MISMATCH` | The task id exists, and the command or the idempotency key differs from the original send -- the payload was altered between send and retry | Refused |
| `PAYLOAD_TOO_LARGE` | The serialized kwargs exceed `payload.reject_bytes`; nothing was enqueued | Refused |
| `IDEMPOTENCY_KEY_CONFLICT` | The idempotency key is reserved by another task with a different command; nothing was enqueued. `owner_task_id` names the owner task. A retry after the reservation ends is accepted | Accepted |
| `EVENT_LOOP_LIMIT` | The broker serves its limit of open event loops (5) and this send ran on a new one; nothing was enqueued; clears when one of those loops closes | Refused |
| `LOOP_RUNNER_FAILED` | Sync send/schedule (or sync retry variant) could not run the enqueue on the broker's sync runner: the runner is stopped, its thread did not start, or scheduling failed. Nothing ran and nothing was written. A stopped runner does not start again, so a sync retry returns the same error; `retry_*_async(err)` completes the same dispatch | Accepted |

`TaskSendRetryable` is `TaskSendEnqueueFailed | TaskSendAsyncContext | TaskSendIdempotencyKeyConflict | TaskSendLoopRunnerFailed`. It is the parameter type of `.retry_send()`, `.retry_send_async()`, `.retry_schedule()` and `.retry_schedule_async()`; another variant is a type error, and at run time it fails an assertion that names the variant. These four variants carry a `TaskSendPayload` with an `enqueue_sha` field, and the retry methods send that exact payload again. The SHA guarantees same-payload idempotency.

#### Workflow Errors

Workflow-specific error codes are distributed across the four families above:

- **OperationalErrorCode**: `WORKFLOW_ENQUEUE_FAILED`, `SUBWORKFLOW_LOAD_FAILED`
- **ContractCode**: `WORKFLOW_CTX_MISSING_ID`
- **RetrievalCode**: `WORKFLOW_NOT_FOUND`
- **OutcomeCode**: `WORKFLOW_PAUSED`, `WORKFLOW_EXPIRED`, `WORKFLOW_FAILED`, `WORKFLOW_CANCELLED`, `UPSTREAM_SKIPPED`, `SUBWORKFLOW_FAILED`, `WORKFLOW_SUCCESS_CASE_NOT_MET`, `WORKFLOW_OUTPUT_SKIPPED`

### Workflow Start Errors (WorkflowStartResult)

`spec.start()`, `start_async()`, `retry_start()` and `retry_start_async()` return `WorkflowStartResult[WorkflowHandle[T]]` = `Result[WorkflowHandle[T], WorkflowStartError]`. `WorkflowStartError` is a union with one frozen variant per code; the variants and their fields are on the [Workflow API](/horsies/concepts/workflows/workflow-api/) page.

#### WorkflowStartErrorCode

<!-- members: WorkflowStartErrorCode -->
| Code | Retryable | When |
|---|---|---|
| `BROKER_NOT_CONFIGURED` | No | `spec.start()` called without broker |
| `VALIDATION_FAILED` | No | A start check (`WorkflowValidationError`) refused the start, or a retry of another workflow's error. Any other exception in the start checks is a defect and propagates. In the start transaction, an exception that is not a database or driver error is a defect and propagates, also a `SerializationError` |
| `WORKFLOW_ID_CONFLICT` | No | The workflow id exists for another workflow name; nothing was written. Use another id |
| `ENQUEUE_FAILED` | Maybe | Schema init or the start transaction failed with a database or driver error. Any other exception in the start transaction propagates |
| `LOOP_RUNNER_FAILED` | No | Sync `spec.start()` could not run the call on the broker's sync runner (stopped, thread did not start, or scheduling failed); nothing was written. A stopped runner does not start again: call `await spec.retry_start_async(err)` or use a new broker |
| `EVENT_LOOP_LIMIT` | No | The broker serves its limit of open event loops and this call ran on a new one; nothing was written |
| `ASYNC_CONTEXT` | No | Sync `spec.start()` / `retry_start()` was called on a thread that runs an event loop; nothing was written. Call `await spec.retry_start_async(err)` to complete the start with the error's id |

The retry methods accept `ENQUEUE_FAILED`, `ASYNC_CONTEXT` and `LOOP_RUNNER_FAILED` (`WorkflowStartRetryable`).

### Workflow Handle Errors (HandleResult)

`WorkflowHandle` methods that read or change the workflow row (`status()`, `results()`, `tasks()`, `cancel()`, `pause()`, `resume()` and their `*_async` forms) and `pause_workflow()` / `resume_workflow()` and their sync forms return `HandleResult[T]` = `Result[T, HandleOperationError]`. `get()` and `result_for()` return a `TaskResult` and use `RetrievalCode` instead.

`HandleOperationError` is a union with one frozen variant per `HandleErrorCode` member. Every variant has `code`, `message`, `retryable` and `workflow_id`. `message` and the cause do not show in `repr()`. Match on the variant. Any other exception, for example a defect in `pause_workflow()` or `resume_workflow()`, propagates.

#### HandleErrorCode

<!-- members: HandleErrorCode -->
| Code | Variant | Other fields | Retryable | When |
|---|---|---|---|---|
| `WORKFLOW_NOT_FOUND` | `HandleWorkflowNotFound` | – | No | No workflow has this id; nothing changed |
| `DB_OPERATION_FAILED` | `HandleDbOperationFailed` | `exception: BaseException` | Driver classification of the cause | A database or driver error. A read changed nothing; `cancel()`, `pause()` and `resume()` before its commit rolled back, or the commit outcome is unknown |
| `STORED_DATA_INVALID` | `HandleStoredDataInvalid` | `exception: BaseException` | No | A stored workflow status that is not a `WorkflowStatus` (`status()`, `cancel()`), or a subworkflow summary that is not valid (`tasks()`); nothing changed, and a repeat returns the same error |
| `RESUME_RECOVERY_FAILED` | `HandleResumeRecoveryFailed` | `exception: BaseException` | No | `resume()` committed, and the recovery pass for the workflow tree, which runs after the commit, failed with a database error. The workflow is `RUNNING`; the worker reaper runs the same pass over all workflows. A repeated `resume()` returns `Ok(False)` |
| `LOOP_RUNNER_FAILED` | `HandleLoopRunnerFailed` | `exception: BaseException` (the `LoopRunnerError`) | No | A sync method could not run the call on the broker's sync runner; nothing ran |
| `EVENT_LOOP_LIMIT` | `HandleEventLoopLimit` | `loop_limit: EventLoopLimit` | No | The broker serves its limit of open event loops and this call ran on a new one; nothing ran |
| `ASYNC_CONTEXT` | `HandleAsyncContext` | – | No | A sync handle method or `pause_workflow_sync` / `resume_workflow_sync` called on a thread that runs an event loop; nothing ran. Call the `*_async` form (sync `get()` / `result_for()` return `RetrievalCode.ASYNC_CONTEXT` instead) |

`get()` folds a status read failure into its `TaskResult`: `WORKFLOW_NOT_FOUND` becomes `RetrievalCode.WORKFLOW_NOT_FOUND`, and `DB_OPERATION_FAILED` and `STORED_DATA_INVALID` become `BROKER_ERROR`.

### User-Defined Error Codes

Domain-specific errors use string codes:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import TaskResult, TaskError, JsonValue

@app.task("validate_order")
def validate_order(*, order_id: str) -> TaskResult[dict[str, JsonValue], TaskError]:
    order = get_order(order_id)

    if order.total > 10000:
        return TaskResult(err=TaskError(
            error_code="ORDER_LIMIT_EXCEEDED",
            message=f"Order {order_id} exceeds limit",
            data={"order_id": order_id, "total": order.total},
        ))

    return TaskResult(ok={"status": "valid"})
```

User-defined codes are auto-retried when listed in `auto_retry_for`:

```python
@app.task(
    "call_api",
    retry_policy=RetryPolicy.fixed([30, 60, 120], auto_retry_for=["RATE_LIMITED", "SERVICE_UNAVAILABLE"]),
)
def call_api(*, url: str) -> TaskResult[dict[str, JsonValue], TaskError]:
    ...
```

## Startup Errors (HorsiesError)

Startup errors are **blocking exceptions** raised during app initialization, workflow validation, or task registration. If a startup error occurs, the worker or scheduler will fail to start and will not accept tasks.

These errors use `ErrorCode` (not `BuiltInTaskCode`) and indicate structural or configuration issues that must be resolved before the application can run.

### Validating with `horsies check`

Use the `horsies check` command to validate your configuration, task registry, and workflow definitions without starting any services. This is recommended for CI/CD pipelines to catch blocking errors before deployment.

```bash
horsies check myapp.instance:app
```

### PgBouncer Configuration Errors

These startup and operator messages usually mean the deployment is using
transaction-pooled PgBouncer without a direct/session-capable URL.

```text
session_database_url required when pgbouncer_transaction_mode=True
```

Set `PostgresConfig.session_database_url` to the direct/session-capable
Postgres URL. Managed providers, such as PlanetScale Postgres, may expose
separate pooled and direct connection strings; verify the current ports and URLs
in your provider dashboard or docs.

```text
Postgres LISTEN failed. If database_url points to PgBouncer transaction pooling
```

The worker listener could not establish a persistent PostgreSQL session. Workers
do not support PgBouncer-only transaction-pool deployment.

```text
LISTEN unavailable; falling back to polling
```

Producer-side result waits can poll when notifications are unavailable. This is
not the supported worker dispatch mode.

```text
session_database_url appears to be transaction-pooled; LISTEN notification was not delivered
```

`horsies check --live` accepted the session URL at the SQL level, but a
`LISTEN` + `NOTIFY` behavior probe did not deliver a notification. Use a direct
Postgres URL or a session-pooling URL for `session_database_url`.

See [PgBouncer Troubleshooting](../troubleshooting/pgbouncer) for the full
deployment pattern.

### Reserved Built-In Code Enforcement

All string values used by the built-in error code families (`OperationalErrorCode`, `ContractCode`, `RetrievalCode`, `OutcomeCode`) are **reserved**. User-defined error codes must not collide with these values.

**Runtime enforcement**: `TaskError(error_code="BROKER_ERROR")` raises `ValueError` at construction time. Built-in codes must be passed as enum members. User-defined codes must be plain `str`, not `str, Enum` subclasses:

```python
TaskError(error_code=OperationalErrorCode.BROKER_ERROR)  # correct
TaskError(error_code="MY_CUSTOM_CODE")                   # correct — user string
TaskError(error_code="BROKER_ERROR")                     # ValueError — reserved
```

**Serialization**: built-in codes are serialized as tagged dicts `{"__builtin_task_code__": "BROKER_ERROR"}` so round-trip identity is preserved through standard `model_validate()` / `model_validate_json()`. User strings are serialized as plain strings. This tagged format eliminates wire-format ambiguity between built-in and user codes, allowing `model_validate()` to work correctly everywhere — including nested models.

`horsies check` detects reserved-code collisions in statically visible configuration:

- **`exception_mapper` values** — if any mapped error code string matches a reserved built-in code, `horsies check` reports `HRS-212`.
- **`default_unhandled_error_code`** — if set to a reserved built-in code other than the library default `UNHANDLED_EXCEPTION`, `horsies check` reports `HRS-212`.

The library default `UNHANDLED_EXCEPTION` is intentionally a built-in code and is not flagged.

Example collision that `horsies check` catches:

```python
from pydantic import SecretStr

app = Horsies(
    config=AppConfig(
        broker=PostgresConfig(database_url=SecretStr('postgresql+psycopg://...')),
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

### ErrorCode Categories

| Range | Category | Description |
| ----- | -------- | ----------- |
| HRS-001-HRS-099 | Workflow Validation | Invalid workflow specification |
| HRS-100-HRS-199 | Task Definition | Invalid task decorator usage |
| HRS-200-HRS-299 | Configuration | Invalid app/broker configuration |
| HRS-300-HRS-399 | Registry | Task registration failures |

### Workflow Validation (HRS-001-HRS-099)

<!-- members: ErrorCode part -->
| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-001 | `WORKFLOW_NO_NAME` | Workflow has no name |
| HRS-002 | `WORKFLOW_NO_NODES` | Workflow has no nodes |
| HRS-003 | `WORKFLOW_INVALID_NODE_ID` | Invalid node ID reference |
| HRS-004 | `WORKFLOW_DUPLICATE_NODE_ID` | Duplicate node ID |
| HRS-005 | `WORKFLOW_NO_ROOT_TASKS` | No root tasks in workflow |
| HRS-006 | `WORKFLOW_INVALID_DEPENDENCY` | Invalid dependency reference |
| HRS-007 | `WORKFLOW_CYCLE_DETECTED` | Cycle detected in workflow DAG |
| HRS-008 | `WORKFLOW_INVALID_ARGS_FROM` | Invalid args_from reference |
| HRS-009 | `WORKFLOW_INVALID_CTX_FROM` | Invalid ctx_from reference |
| HRS-010 | `WORKFLOW_CTX_PARAM_MISSING` | Context parameter missing |
| HRS-011 | `WORKFLOW_INVALID_OUTPUT` | Invalid output specification |
| HRS-012 | `WORKFLOW_INVALID_SUCCESS_POLICY` | Invalid success policy |
| HRS-013 | `WORKFLOW_INVALID_JOIN` | Invalid join configuration |
| HRS-014 | `WORKFLOW_UNRESOLVED_QUEUE` | Queue name not resolved |
| HRS-015 | `WORKFLOW_UNRESOLVED_PRIORITY` | Priority not resolved |
| HRS-016 | `WORKFLOW_NO_DEFINITION_KEY` | Workflow definition/spec missing `definition_key` |
| HRS-017 | `WORKFLOW_DUPLICATE_DEFINITION_KEY` | Two definitions share the same `definition_key` |
| HRS-018 | `WORKFLOW_SUBWORKFLOW_APP_MISSING` | Subworkflow app reference missing |
| HRS-019 | `WORKFLOW_INVALID_KWARG_KEY` | Unknown kwargs or args_from key for callable |
| HRS-020 | `WORKFLOW_MISSING_REQUIRED_PARAMS` | Missing required parameters for task or subworkflow |
| HRS-021 | `WORKFLOW_KWARGS_ARGS_FROM_OVERLAP` | kwargs and args_from share one or more keys |
| HRS-022 | `WORKFLOW_SUBWORKFLOW_PARAMS_REQUIRE_BUILD_WITH` | Subworkflow params passed but build_with is not overridden |
| HRS-024 | `WORKFLOW_ARGS_FROM_TYPE_MISMATCH` | args_from source result type doesn't match target parameter |
| HRS-025 | `WORKFLOW_OUTPUT_TYPE_MISMATCH` | Output node type doesn't match WorkflowSpec generic |
| HRS-026 | `WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED` | Positional args are not supported for workflow nodes |
| HRS-027 | `WORKFLOW_CHECK_CASES_REQUIRED` | Parameterized workflow builder missing test cases |
| HRS-028 | `WORKFLOW_CHECK_CASE_INVALID` | Workflow builder test case is invalid |
| HRS-029 | `WORKFLOW_CHECK_BUILDER_EXCEPTION` | Workflow builder raised an exception or returned non-WorkflowSpec |
| HRS-030 | `WORKFLOW_CHECK_UNDECORATED_BUILDER` | Function returns WorkflowSpec but lacks @app.workflow_builder |
| HRS-031 | `WORKFLOW_KWARGS_NOT_SERIALIZABLE` | kwargs value fails JSON serialization, also a lone surrogate in a string or a dict key |
| HRS-032 | `WORKFLOW_INVALID_ID` | supplied `workflow_id` is not a UUID |
| HRS-033 | `WORKFLOW_INVALID_DEFINITION_CONFIG` | A `WorkflowDefinition` class defines `Meta`, or sets `config` to a value that is not a `WorkflowDefinitionConfig`; raised when the class is created |
| HRS-034 | `WORKFLOW_INVALID_PRIORITY` | A `TaskNode` sets a `priority` outside 1–100; raised when the spec is built |
| HRS-035 | `WORKFLOW_NAIVE_GOOD_UNTIL` | A `TaskNode` sets a naive `good_until` (no `tzinfo`); raised when the spec is built |

### Task Definition (HRS-100-HRS-199)

<!-- members: ErrorCode part -->
| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-100 | `TASK_NO_RETURN_TYPE` | Task function missing return type |
| HRS-101 | `TASK_INVALID_RETURN_TYPE` | Return type is not TaskResult |
| HRS-102 | `TASK_INVALID_OPTIONS` | Invalid task options |
| HRS-103 | `TASK_INVALID_QUEUE` | Invalid queue specification |
| HRS-104 | `TASK_PREDECORATED_NOT_SUPPORTED` | Task function is pre-decorated: it has a `__wrapped__` chain. Apply `@app.task()` to the raw function |
| HRS-105 | `TASK_STRICT_SIGNATURE_VIOLATION` | Task signature fails the strict check: a parameter that is not keyword-only (`*args`, `**kwargs`, positional), a type that is not allowed (for example `Any`, a bare container, `set`, `bytes`), or a return type that is not `TaskResult[OkT, TaskError]` |
| HRS-106 | `TASK_ASYNC_FUNCTION_NOT_SUPPORTED` | Task function is defined with `async def`; task functions must be sync |

### Configuration (HRS-200-HRS-299)

<!-- members: ErrorCode part -->
| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-200 | `CONFIG_INVALID_QUEUE_MODE` | Invalid queue mode or custom queue list: `custom_queues` in `DEFAULT` mode, missing or duplicate `custom_queues` in `CUSTOM` mode, or a `CustomQueueConfig.name` that is empty, has whitespace at the start or the end, or is longer than 100 characters |
| HRS-201 | `CONFIG_INVALID_CLUSTER_CAP` | Invalid cluster capacity |
| HRS-202 | `CONFIG_INVALID_PREFETCH` | Invalid prefetch setting |
| HRS-203 | `BROKER_INVALID_URL` | Invalid broker URL |
| HRS-204 | `CONFIG_INVALID_RECOVERY` | Invalid recovery configuration |
| HRS-205 | `CONFIG_INVALID_SCHEDULE` | Invalid schedule configuration |
| HRS-206 | `CLI_INVALID_ARGS` | Invalid CLI arguments |
| HRS-207 | `WORKER_INVALID_LOCATOR` | Invalid worker locator |
| HRS-208 | `CONFIG_INVALID_RESILIENCE` | Invalid resilience configuration |
| HRS-209 | `CONFIG_INVALID_EXCEPTION_MAPPER` | Invalid exception mapper |
| HRS-210 | `MODULE_EXEC_ERROR` | Module raised an error during import |
| HRS-211 | `BROKER_INIT_FAILED` | Broker failed to initialize |
| HRS-212 | `CHECK_RESERVED_CODE_COLLISION` | User config value collides with a reserved built-in error code |
| HRS-213 | `CONFIG_INVALID_BROKER_POOL` | Invalid worker pool bounds: `worker_pool_size` below 1, `worker_max_overflow` below 0, `worker_child_pool_min_size` below 0, `worker_child_pool_max_size` below 1, or a minimum above the maximum |
| HRS-214 | `CONFIG_INVALID_CHILD_HOOK` | `on_child_process_start` received a value that is not callable, or an async function |
| HRS-215 | `CONFIG_INVALID_KEEPALIVE` | Invalid TCP keepalive settings: a keepalive value below 1 while `tcp_keepalives` is on, or a negative `tcp_user_timeout_ms` |
| HRS-216 | `CONFIG_INVALID_RETENTION` | Invalid retention configuration |
| HRS-217 | `CHECK_LIVE_ASYNC_CONTEXT` | `check(live=True)` was called on a thread that runs an event loop; the live check did not run |

### Registry (HRS-300-HRS-399)

<!-- members: ErrorCode part -->
| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-300 | `TASK_NOT_REGISTERED` | Task not found in registry |
| HRS-301 | `TASK_DUPLICATE_NAME` | Duplicate task name |

### HorsiesError Structure

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

### TaskResult[T, TaskError]

| Property/Method | Type | Description |
| --------------- | ---- | ----------- |
| `is_ok()` | `bool` | True if success |
| `is_err()` | `bool` | True if error |
| `ok` | `T \| None` | Success value or `None` |
| `err` | `TaskError \| None` | Error value or `None` |
| `ok_value` | `T` | Success value; raises `ValueError` if error |
| `err_value` | `TaskError` | Error value; raises `ValueError` if success |

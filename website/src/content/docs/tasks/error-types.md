---
title: Error Types
section: python
summary: The Python types of task errors, broker errors, send errors, workflow start and handle errors, and startup errors.
related: [errors, error-handling, retrieving-results, sending-tasks]
tags: [errors, TaskError, TaskResult, BrokerOperationError, TaskSendError, HorsiesError]
---

# Error Types

The Python types that carry the error codes of the [Errors Reference](/horsies/tasks/errors/). A task returns `TaskResult[T, TaskError]`; a broker call returns `BrokerResult[T]`; a send returns `TaskSendResult`; a workflow start returns `WorkflowStartResult`; a workflow handle call returns `HandleResult`. Startup errors raise `HorsiesError`.

## TaskError

| Field | Type | Description |
| ----- | ---- | ----------- |
| `error_code` | `BuiltInTaskCode \| str` | Required. Library or domain error code |
| `message` | `str \| None` | Human-readable description |
| `data` | `JsonInput` on write, `JsonValue` on read | Additional context (task_id, etc.) |
| `exception` | `BaseException \| FlattenedException \| None` | Live exception in-process; flattened to a `FlattenedException` TypedDict (`type`, `module`, `message`, `repr`, `traceback`) when serialized to the wire |

`error_code` must not be null: `auto_retry_for` and outcome routing read it. `TaskError` refuses a key that it does not declare, at construction and at decode. `SubWorkflowError` adds `sub_workflow_id` and `sub_workflow_summary`, and it refuses any other key. A stored error with an unknown key, or without `error_code`, is stored data that does not parse: see [Decode Errors](/horsies/tasks/task-types/#decode-errors).

## Validation Failure Text

When a value fails pydantic validation, the library writes each failure as `<loc>: <msg> [<type>]`. Error messages, stored `TaskError` fields, flattened exceptions and log lines use this text. It names the field, the message and the error type. It does not contain the value.

- The text shows at most 10 failures, then `(+N more)`.
- For a task kwarg, `<loc>` starts with the kwarg name.
- When a message template uses a value from the input, the library writes `<input>` in its place. Example: `Input tag '<input>' found using 'kind' does not match any of the expected tags: 'cat', 'dog'`.
- Text that your own validator writes is kept as written. This applies to `value_error`, `assertion_error` and custom `PydanticCustomError` types. Do not put input values into validator messages that must not be stored.
- A key from the input can be part of `<loc>` in two cases: the validated type is a dict or a mapping, or contains one; or a model, `TypedDict` or dataclass with `extra='forbid'` refuses the key.

The cause object follows the same rule. Where the library catches a pydantic `ValidationError`, it keeps a `ValueValidationError` with the same text instead (`horsies.core.codec.ValueValidationError`, a subclass of `SerializationError` and of `ValueError`). For example, `TaskSendValidationFailed.exception` is a `ValueValidationError` when a kwarg fails its declared type. When task code raises a `ValidationError`, the stored `TaskError` keeps the same text in `message` and in the `repr` and `traceback` of the flattened exception.

## Broker Errors (BrokerResult)

`app.get_result_async()` and broker methods return `BrokerResult[T]` = `Result[T, BrokerOperationError]`. The outer `Err` is an infrastructure-level failure, distinct from the inner `TaskResult.err`.

Three broker methods return a subset of `BrokerOperationError`, their only failures:

| Method | Returns |
| ------ | ------- |
| `enqueue_async()` | `Result[uuid.UUID, BrokerEnqueueError]` |
| `enqueue()` | `Result[uuid.UUID, BrokerEnqueueError \| BrokerSyncCallError]` |
| `ensure_schema_initialized()` | `Result[None, BrokerOperationFailed \| BrokerEventLoopLimit]` |

`enqueue()` and `enqueue_async()` require `sent_at`, the call time of the send, which the row stores. A send that repeats a task id is the same send when the stored command fingerprint and `sent_at` equal its own, and answers with that task id; otherwise it is `PAYLOAD_MISMATCH`. A resend therefore passes the `sent_at` of the first send.

They also require `send_kind` (`SendKind` from `horsies.core.brokers.enqueue_task`): `SendKind.FIRST` for the first send of a task id, `SendKind.REPEAT` for a resend. A repeat reads the task history first, so a resend of a task that finished answers with its task id, or `PAYLOAD_MISMATCH`, and the task does not run again. A first send does not read history.

`BrokerEnqueueError` is `BrokerEnqueueFailed | BrokerPayloadMismatch | BrokerIdempotencyKeyConflict | BrokerInvalidTaskOptions | BrokerInvalidInput | BrokerEventLoopLimit`. `BrokerSyncCallError` is `BrokerAsyncContext | BrokerLoopRunnerFailed`: the failures that the sync form adds, where nothing ran. Both are exported from `horsies`.

### Calls That Run on the Core

These calls run on the core of the process: `enqueue()`, `enqueue_async()`, `ensure_schema_initialized()`, `get_task_info()`, `get_task_info_async()`, `get_raw_result_record()`, `get_raw_result_record_async()`, `ping_database_async()`, `ping_workers_async()`, and the task sends (`send()`, `send_async()`, `schedule()`, `schedule_async()` and the four retry methods). The core has one set of pools per broker, for every thread and event loop.

- They do not return `EVENT_LOOP_LIMIT`.
- The sync forms `enqueue()`, `get_task_info()` and `get_raw_result_record()` call the core on the calling thread. They do not return `LOOP_RUNNER_FAILED`. They still return `ASYNC_CONTEXT` on a thread that runs an event loop.
- `BrokerEnqueueError`, `BrokerSyncCallError`, `TaskSendError` and the return type of `ensure_schema_initialized()` keep the two variants, so an exhaustive `match` names them.

When the core of the process cannot run the call, the call returns its failure code (`ENQUEUE_FAILED`, `TASK_INFO_QUERY_FAILED`, `DB_PING_FAILED`, `WORKER_PING_FAILED` or `SCHEMA_INIT_FAILED`) with the core error as `exception`. A send is different for two of the core errors: see [Process-State Refusals](#process-state-refusals). The core errors are in `horsies._core`:

| Core error | When | `retryable` | After the failure |
| ---------- | ---- | ----------- | ----------------- |
| `TooManyPendingCalls` | 65,536 core calls are in flight on the calling event loop | `True` | Nothing ran |
| `CoreCallTimedOut` | A sync form did not end within 30 minutes. For `get_raw_result_record()` with a `timeout_ms`, the bound is `timeout_ms` plus 30 minutes; with no `timeout_ms`, the call has no bound | `True` | A read changed nothing. After `enqueue()` or a sync send, the row may or may not exist |
| `CoreStopped` | A core background task panicked, so the core is stopped in this process. The message names the first panic. A send raises it | `False` | Nothing ran |
| `CoreStartFailed` | The core runtime could not start | `False` | Nothing ran |
| `ForkedAfterCoreStart` | The process was forked without exec after the core started in its parent. A send raises it | `False` | Nothing ran |

#### Process-State Refusals

`ForkedAfterCoreStart` and `CoreStopped` are states of the process, not of one call. The core refuses every later call in the process, so a retry in the same process gets the same refusal.

- The task sends raise them: `send()`, `send_async()`, `schedule()`, `schedule_async()`, `retry_send()`, `retry_send_async()`, `retry_schedule()` and `retry_schedule_async()`.
- `enqueue()` and `enqueue_async()` raise them.
- These calls never return them as `ENQUEUE_FAILED`. The send result types do not change.
- Nothing was sent.

| Raised | Cause | What to do |
| ------ | ----- | ---------- |
| `ForkedAfterCoreStart` | The process was forked without exec after the core started in its parent, for example a `multiprocessing` pool with the `fork` start method inside a task. `args` are `(core_pid, this_pid, advice)` | Start child processes that call horsies with the `spawn` or `forkserver` start method. Or start the core after the fork: the core starts at the first call that runs on it, so fork before the parent's first send or read |
| `CoreStopped` | A core background task panicked. `args` are the `(subject, cause)` of the first panic | Restart the process: the core does not start again in it. The panic is a defect: report its subject and cause |

When the core ran the call and the call failed, `exception` is the core's class of the cause, from `horsies._core`. The declared type of `exception` does not change:

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
| `ENQUEUE_FAILED` | The enqueue write failed or could not be confirmed, or the core of the process could not run it ([Calls That Run on the Core](#calls-that-run-on-the-core)); the row may or may not exist. `ForkedAfterCoreStart` and `CoreStopped` are raised instead ([Process-State Refusals](#process-state-refusals)) | Varies |
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
| `EVENT_LOOP_LIMIT` | Not returned by a broker method: the broker's calls run on the core ([Calls That Run on the Core](#calls-that-run-on-the-core)). The workflow start and the workflow resume return their own `EVENT_LOOP_LIMIT` | No |
| `ASYNC_CONTEXT` | A sync method (`app.get_result()`, `app.ping_database()`, `broker.close()`, ...) was called on a thread that runs an event loop; nothing ran. Call the `*_async` form | No |
| `LOOP_RUNNER_FAILED` | A sync method of the app (`app.get_result()`, `app.get_task_info()`, `app.ping_database()`, `app.ping_workers()`, `app.list_worker_states()`, `app.get_worker_state()`, `app.get_worker_state_history()`, `app.get_class_coverage()`, `app.get_class_report()`) or `broker.close()` could not run the call on the broker's sync runner: the runner is stopped, its thread did not start, or scheduling failed. Nothing ran. `close()` and `close_async()` stop the runner, and a stopped runner does not start again: call the `*_async` form or use a new broker. `broker.enqueue()`, `broker.get_task_info()` and `broker.get_raw_result_record()` do not use the runner | No |
| `NO_TYPE_AVAILABLE` | Typed decode at the `app` layer needs an `ok_type`, but `task_name` is not in the local task registry | No |
| `INVALID_JSON_PAYLOAD` | Stored result JSON is malformed, not a JSON object, has an invalid strict-serde envelope, or fails app-level typed slot decode | No |
| `INVALID_TASK_OPTIONS` | `enqueue_async` received task options that are not valid JSON or not a JSON object, a `retry_policy` that does not validate as `RetryPolicy` (strict types, no unknown fields), an `execution` that is not an `Execution` value, or a `cancel_grace_ms` that does not fit the class (100 to 10,000 ms for `SHARED_ASYNC`, absent or null for the others). The message names each failing field. Nothing was written | No |
| `INVALID_INPUT` | An argument was refused before anything ran: a non-positive `timeout_seconds`, `min_responses` or history `limit`, a `target_worker_id` that is not a string, both `enqueued_at` and `enqueue_delay_seconds`, a future `sent_at` without either, a naive `sent_at`, `enqueued_at` or `good_until`, an `input_json` that does not parse, that repeats a key in an object, or that is not a JSON object or null, an empty task or queue name, a priority outside 1..100, a negative `enqueue_delay_seconds`, an empty `retention_class_key`, or an idempotency key that is empty or over 255 bytes. Nothing was written | No |
| `LISTENER_DISABLED` | The broker was built with `assume_initialized=True` and has no listener, so `ping_workers` cannot run. Nothing ran | No |
| `WAIT_INSIDE_TASK` | A raw result read (`app.get_raw_result()`, `broker.get_raw_result_record()` or an `*_async` form) was called in a worker child, where task bodies run. Nothing ran. Compose the tasks with a workflow: see [Waiting for a Result Inside a Task](/horsies/concepts/result-handling/#waiting-for-a-result-inside-a-task) | No |

### BrokerOperationError

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
| `BrokerWaitInsideTask` | `WAIT_INSIDE_TASK` | – | `False` | Nothing ran |

| Field | Type | Description |
| ----- | ---- | ----------- |
| `message` | `str` | Human-readable description |
| `retryable` | `bool` | The classification of the cause where the variant has a cause from the database or the core: `True` when the condition may clear, for example a lost connection or `TooManyPendingCalls`. `False` on every other variant |
| `exception` | `BaseException` on `BrokerOperationFailed` and `BrokerLoopRunnerFailed`; `BaseException \| NoCause` on the other variants that have it | The cause. `NoCause()` when a check found the failure without a Python exception |
| `owner_task_id` | `uuid.UUID` | The task that holds the idempotency key |
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

## Send Errors (TaskSendResult)

Send errors are returned via `TaskSendResult[TaskHandle[T]]` from `.send()`, `.send_async()`, `.schedule()` and `.schedule_async()`. These are separate from `TaskResult` -- they indicate that enqueuing the task itself failed, not that task execution failed. A send raises `ForkedAfterCoreStart` and `CoreStopped` instead of returning them: see [Process-State Refusals](#process-state-refusals).

### TaskSendError

`TaskSendError` is a union with one frozen variant per `TaskSendErrorCode` member. Each variant has only the fields of its failure, and `err.code` names its member. Match on the variant; a `match` that omits a variant fails pyright's exhaustiveness check. The handling pattern is in [Sending Tasks](/horsies/tasks/sending-tasks/#retrying-failed-sends).

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
| `task_id` | `uuid.UUID` | Task id of the prepared send |
| `payload` | `TaskSendPayload` | Serialized envelope that the retry methods send again |
| `retryable` | `bool` | The core's classification of the cause: `True` when the condition may clear, for example a lost connection or `TooManyPendingCalls` |
| `exception` | `BaseException \| NoCause` | The cause. `NoCause()` when a check found the failure without a Python exception. On `TaskSendLoopRunnerFailed` it is `BaseException`: always the `LoopRunnerError` |
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

`TaskSendRetryable` is `TaskSendEnqueueFailed | TaskSendAsyncContext | TaskSendIdempotencyKeyConflict | TaskSendLoopRunnerFailed`. It is the parameter type of `.retry_send()`, `.retry_send_async()`, `.retry_schedule()` and `.retry_schedule_async()`; another variant is a type error, and at run time it fails an assertion that names the variant. These four variants carry a `TaskSendPayload`, and the retry methods send that exact payload again: the same task id, command and `sent_at`. If the first send was written, the broker finds the same command fingerprint and `sent_at` on that task id and answers with the task id instead of writing a second row, also when the task already finished (a retry reads the task history).

## Workflow Start Errors (WorkflowStartResult)

`spec.start()`, `start_async()`, `retry_start()` and `retry_start_async()` return `WorkflowStartResult[WorkflowHandle[T]]` = `Result[WorkflowHandle[T], WorkflowStartError]`. `WorkflowStartError` is a union with one frozen variant per code; the variants and their fields are on the [Workflow API](/horsies/concepts/workflows/workflow-api/) page.

The retry methods accept `ENQUEUE_FAILED`, `ASYNC_CONTEXT` and `LOOP_RUNNER_FAILED` (`WorkflowStartRetryable`).

## Workflow Handle Errors (HandleResult)

`WorkflowHandle` methods that read or change the workflow row (`status()`, `results()`, `tasks()`, `cancel()`, `pause()`, `resume()` and their `*_async` forms) and `pause_workflow()` / `resume_workflow()` and their sync forms return `HandleResult[T]` = `Result[T, HandleOperationError]`. `get()` and `result_for()` return a `TaskResult` and use `RetrievalCode` instead.

`HandleOperationError` is a union with one frozen variant per `HandleErrorCode` member. Every variant has `code`, `message`, `retryable` and `workflow_id`. `message` and the cause do not show in `repr()`. Match on the variant. Any other exception, for example a defect in `pause_workflow()` or `resume_workflow()`, propagates.

## User-Defined Error Codes

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

## Reserved Built-In Code Enforcement

All string values used by the built-in error code families (`OperationalErrorCode`, `ContractCode`, `RetrievalCode`, `OutcomeCode`) are **reserved**. User-defined error codes must not collide with these values.

**Runtime enforcement**: `TaskError(error_code="BROKER_ERROR", message="db down")` raises `ValueError` at construction time. Built-in codes must be passed as enum members. User-defined codes must be plain `str`, not `str, Enum` subclasses:

```python
TaskError(error_code=OperationalErrorCode.BROKER_ERROR, message="db down")  # correct
TaskError(error_code="MY_CUSTOM_CODE", message="card declined")             # correct — user string
TaskError(error_code="BROKER_ERROR", message="db down")                     # ValueError — reserved
```

`WORKFLOW_STOPPED` is also reserved, and it is not a task code: it names a workflow task that stopped before its body ran. `TaskError(error_code="WORKFLOW_STOPPED", ...)` raises `ValueError`, and a stored task error with this code does not decode. An `exception_mapper` value, a `default_unhandled_error_code` or an `auto_retry_for` entry with this name is refused like any other invalid code.

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

### TaskResult[T, TaskError]

| Property/Method | Type | Description |
| --------------- | ---- | ----------- |
| `is_ok()` | `bool` | True if success |
| `is_err()` | `bool` | True if error |
| `ok` | `T \| None` | Success value or `None` |
| `err` | `TaskError \| None` | Error value or `None` |
| `ok_value` | `T` | Success value; raises `ValueError` if error |
| `err_value` | `TaskError` | Error value; raises `ValueError` if success |

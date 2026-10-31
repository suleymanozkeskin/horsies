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

### BuiltInTaskCode (4-Family Split)

All library-defined error codes are grouped into four families. The umbrella type alias `BuiltInTaskCode` is the union of all four enums. User code should handle these explicitly.

#### OperationalErrorCode

Errors from task execution, broker, and worker internals.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `UNHANDLED_EXCEPTION` | Uncaught exception in task code, wrapped to `UNHANDLED_EXCEPTION` by library  | Yes, if in `auto_retry_for` |
| `TASK_EXCEPTION` | Task returned `None` instead of a `TaskResult`, or the worker caught an exception outside the task wrapper (for example `KeyboardInterrupt`). A raised exception in task code gives `UNHANDLED_EXCEPTION` | Yes, if in `auto_retry_for` |
| `WORKER_CRASHED` | Worker process died during execution | Yes, if in `auto_retry_for` |
| `BROKER_ERROR` | Database or broker failure during operation | No |
| `WORKER_RESOLUTION_ERROR` | Failed to resolve task by name | No |
| `WORKER_SERIALIZATION_ERROR` | Failed to serialize/deserialize task data | No |
| `RESULT_DESERIALIZATION_ERROR` | Stored result JSON is corrupt or could not be deserialized | No |
| `WORKFLOW_ENQUEUE_FAILED` | Workflow node failed after READY->ENQUEUED transition during enqueue/build | No |
| `SUBWORKFLOW_LOAD_FAILED` | Subworkflow definition could not be loaded | No |

#### ContractCode

Errors from type/schema validation and structural contracts.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `RETURN_TYPE_MISMATCH` | Task return type doesn't match declaration | No |
| `NO_TYPE_AVAILABLE` | Outputless workflow per-node decode: terminal task name not in the local registry, so no `task_ok_type` to decode with | No |
| `WORKFLOW_CTX_MISSING_ID` | Workflow context is missing required ID | No |

`RETURN_TYPE_MISMATCH` does not store the returned value. `message` names the variant, the actual type, the expected type and the failing fields. `data` holds `variant`, `expected_type`, `actual_type`, `validation_errors` (at most 10 entries, each with `loc`, `msg` and `type`) and `validation_error_count`. `exception` is `None`.

#### RetrievalCode

Errors from `handle.get()` / `get_async()`. These indicate issues retrieving the result, not task execution failures.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `WAIT_TIMEOUT` | `get()` timed out; task may still be running | No |
| `TASK_NOT_FOUND` | Task ID doesn't exist in database | No |
| `WORKFLOW_NOT_FOUND` | Workflow ID doesn't exist in database | No |
| `RESULT_NOT_AVAILABLE` | Result cache was never set | No |
| `RESULT_NOT_READY` | Result not yet available; task is still running | No |
| `EVENT_LOOP_LIMIT` | The broker serves its limit of open event loops (5) and this is a new one; not cached, clears when one of those loops closes | No |
| `ASYNC_CONTEXT` | Sync `get()` / `result_for()` was called on a thread that runs an event loop; nothing ran; not cached. Call `await handle.get_async()` / `result_for_async()` | No |

#### OutcomeCode

Terminal outcome codes for tasks and workflows.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `TASK_CANCELLED` | Task was cancelled before completion | No |
| `TASK_EXPIRED` | Task's `good_until` deadline passed before execution started (PENDING or CLAIMED) | No |
| `TASK_TIMEOUT` | Task exceeded its `timeout_ms` execution limit; the child process was killed | Yes, if in `auto_retry_for` |
| `WORKFLOW_PAUSED` | Workflow was paused | No |
| `WORKFLOW_FAILED` | Workflow failed | No |
| `WORKFLOW_CANCELLED` | Workflow was cancelled | No |
| `UPSTREAM_SKIPPED` | Upstream task in workflow was skipped | No |
| `SUBWORKFLOW_FAILED` | Subworkflow failed | No |
| `WORKFLOW_SUCCESS_CASE_NOT_MET` | Workflow success condition was not satisfied | No |

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

| Code | Description | Retryable |
| ---- | ----------- | --------- |
| `SCHEMA_INIT_FAILED` | Schema initialization failed | Varies |
| `ENQUEUE_FAILED` | Enqueue or schedule path failed | Yes when the underlying broker error is retryable |
| `PAYLOAD_MISMATCH` | The task id exists, and the command or the idempotency key differs from the original send | No |
| `IDEMPOTENCY_KEY_CONFLICT` | The idempotency key is reserved by another task with a different command; `detail` is an `IdempotencyKeyOwner` that names the owner task | No |
| `TASK_INFO_QUERY_FAILED` | Task-info or raw-result query failed | Varies |
| `MONITORING_QUERY_FAILED` | Monitoring or worker-state query failed | Varies |
| `DB_PING_FAILED` | `ping_database` reachability probe failed | Varies |
| `WORKER_PING_FAILED` | `ping_workers` failed (listener unavailable, bad timeout) | No |
| `CLEANUP_FAILED` | Cleanup or retention operation failed | Varies |
| `CLOSE_FAILED` | Broker close failed | Varies |
| `LISTENER_START_FAILED` | LISTEN/NOTIFY listener failed to start | No |
| `LISTENER_SUBSCRIBE_FAILED` | LISTEN/NOTIFY subscribe failed | No |
| `NO_BROKER` | Operation needs a broker, but none is configured | No |
| `EVENT_LOOP_LIMIT` | The broker serves its limit of open event loops (5) and this call ran on a new one; nothing ran; clears when one of those loops closes | No |
| `ASYNC_CONTEXT` | A sync method (`app.get_result()`, `app.ping_database()`, `broker.close()`, ...) was called on a thread that runs an event loop; nothing ran. Call the `*_async` form | No |
| `NO_TYPE_AVAILABLE` | Typed decode at the `app` layer needs an `ok_type`, but `task_name` is not in the local task registry | No |
| `INVALID_JSON_PAYLOAD` | Stored result JSON is malformed, not a JSON object, has an invalid strict-serde envelope, or fails app-level typed slot decode | No |
| `INVALID_TASK_OPTIONS` | `enqueue_async` received a `retry_policy` that does not validate as `RetryPolicy` (strict types, no unknown fields). The message names each failing field. Nothing was written | No |

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

| Field | Type | Description |
| ----- | ---- | ----------- |
| `message` | `str` | Human-readable description |
| `task_id` | `str` | Task id of the prepared send |
| `payload` | `TaskSendPayload` | Serialized envelope that the retry methods send again |
| `retryable` | `bool` | Driver classification of the cause: `True` when the condition may clear, for example a lost connection |
| `exception` | `BaseException \| NoCause` | The cause. `NoCause()` when a check found the failure without a Python exception |
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

| Code | Description | Retry methods |
| ---- | ----------- | ------------- |
| `SEND_SUPPRESSED` | Send suppressed during worker import/discovery to prevent side effects | Refused |
| `ASYNC_CONTEXT` | Sync send/schedule (or sync retry variant) called inside a running event loop -- the blocking enqueue would stall the loop; `retry_*_async(err)` completes the same dispatch (or call the `*_async` entry point with the original args) | Accepted |
| `VALIDATION_FAILED` | Argument serialization or validation failed before enqueue, or a retry method refused the payload (another task, or a schedule payload sent to a send retry) | Refused |
| `ENQUEUE_FAILED` | Broker/database failure during enqueue; the row may or may not exist | Accepted |
| `PAYLOAD_MISMATCH` | The task id exists, and the command or the idempotency key differs from the original send -- the payload was altered between send and retry | Refused |
| `PAYLOAD_TOO_LARGE` | The serialized kwargs exceed `payload.reject_bytes`; nothing was enqueued | Refused |
| `IDEMPOTENCY_KEY_CONFLICT` | The idempotency key is reserved by another task with a different command; nothing was enqueued. `owner_task_id` names the owner task. A retry after the reservation ends is accepted | Accepted |
| `EVENT_LOOP_LIMIT` | The broker serves its limit of open event loops (5) and this send ran on a new one; nothing was enqueued; clears when one of those loops closes | Refused |

`TaskSendRetryable` is `TaskSendEnqueueFailed | TaskSendAsyncContext | TaskSendIdempotencyKeyConflict`. It is the parameter type of `.retry_send()`, `.retry_send_async()`, `.retry_schedule()` and `.retry_schedule_async()`; another variant is a type error, and at run time it fails an assertion that names the variant. These three variants carry a `TaskSendPayload` with an `enqueue_sha` field, and the retry methods send that exact payload again. The SHA guarantees same-payload idempotency.

#### Workflow Errors

Workflow-specific error codes are distributed across the four families above:

- **OperationalErrorCode**: `WORKFLOW_ENQUEUE_FAILED`, `SUBWORKFLOW_LOAD_FAILED`
- **ContractCode**: `WORKFLOW_CTX_MISSING_ID`
- **RetrievalCode**: `WORKFLOW_NOT_FOUND`
- **OutcomeCode**: `WORKFLOW_PAUSED`, `WORKFLOW_FAILED`, `WORKFLOW_CANCELLED`, `UPSTREAM_SKIPPED`, `SUBWORKFLOW_FAILED`, `WORKFLOW_SUCCESS_CASE_NOT_MET`

### User-Defined Error Codes

Domain-specific errors use string codes:

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
app = Horsies(
    config=AppConfig(
        broker=PostgresConfig(database_url='postgresql+psycopg://...'),
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
| HRS-031 | `WORKFLOW_KWARGS_NOT_SERIALIZABLE` | kwargs value fails JSON serialization |
| HRS-032 | `WORKFLOW_INVALID_ID` | supplied `workflow_id` is not a UUID |
| HRS-033 | `WORKFLOW_INVALID_DEFINITION_CONFIG` | A `WorkflowDefinition` class defines `Meta`, or sets `config` to a value that is not a `WorkflowDefinitionConfig`; raised when the class is created |

### Task Definition (HRS-100-HRS-199)

| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-100 | `TASK_NO_RETURN_TYPE` | Task function missing return type |
| HRS-101 | `TASK_INVALID_RETURN_TYPE` | Return type is not TaskResult |
| HRS-102 | `TASK_INVALID_OPTIONS` | Invalid task options |
| HRS-103 | `TASK_INVALID_QUEUE` | Invalid queue specification |
| HRS-104 | `TASK_PREDECORATED_NOT_SUPPORTED` | Task function is already decorated by another app instance |
| HRS-106 | `TASK_ASYNC_FUNCTION_NOT_SUPPORTED` | Task function is defined with `async def`; task functions must be sync |

### Configuration (HRS-200-HRS-299)

| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-200 | `CONFIG_INVALID_QUEUE_MODE` | Invalid queue mode |
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
| HRS-216 | `CONFIG_INVALID_RETENTION` | Invalid retention configuration |
| HRS-217 | `CHECK_LIVE_ASYNC_CONTEXT` | `check(live=True)` was called on a thread that runs an event loop; the live check did not run |

### Registry (HRS-300-HRS-399)

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

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

When a task reaches a terminal `FAILED` state with a `TaskResult(err=TaskError(...))`, the `error_code` is persisted to `horsies_tasks.error_code` for queryability. This column is `NULL` for successful tasks, non-terminal tasks, and worker-level failures that never produced a `TaskResult`. Access it via `TaskInfo.error_code` from `handle.info()`.

Each execution attempt is also recorded in `horsies_task_attempts` with per-attempt `error_code`, `error_message`, and outcome. See [Retrieving Results](retrieving-results#task-metadata-and-attempt-history) for API usage.

### TaskError

| Field | Type | Description |
| ----- | ---- | ----------- |
| `error_code` | `BuiltInTaskCode \| str \| None` | Library or domain error code |
| `message` | `str \| None` | Human-readable description |
| `data` | `Any \| None` | Additional context (task_id, etc.) |
| `exception` | `dict[str, Any] \| BaseException \| None` | Original exception if applicable |

### BuiltInTaskCode (4-Family Split)

All library-defined error codes are grouped into four families. The umbrella type alias `BuiltInTaskCode` is the union of all four enums. User code should handle these explicitly.

#### OperationalErrorCode

Errors from task execution, broker, and worker internals.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `UNHANDLED_EXCEPTION` | Uncaught exception in task code, wrapped to `UNHANDLED_EXCEPTION` by library  | Yes, if in `auto_retry_for` |
| `TASK_EXCEPTION` | Task raised exception or returned invalid value | Yes, if in `auto_retry_for` |
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
| `PYDANTIC_HYDRATION_ERROR` | Task succeeded but its return value could not be rehydrated to the declared type | No |
| `WORKFLOW_CTX_MISSING_ID` | Workflow context is missing required ID | No |

#### RetrievalCode

Errors from `handle.get()` / `get_async()`. These indicate issues retrieving the result, not task execution failures.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `WAIT_TIMEOUT` | `get()` timed out; task may still be running | No |
| `TASK_NOT_FOUND` | Task ID doesn't exist in database | No |
| `WORKFLOW_NOT_FOUND` | Workflow ID doesn't exist in database | No |
| `RESULT_NOT_AVAILABLE` | Result cache was never set | No |
| `RESULT_NOT_READY` | Result not yet available; task is still running | No |

#### OutcomeCode

Terminal outcome codes for tasks and workflows.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `TASK_CANCELLED` | Task was cancelled before completion | No |
| `TASK_EXPIRED` | Task's `good_until` deadline passed before execution started (PENDING or CLAIMED) | No |
| `WORKFLOW_PAUSED` | Workflow was paused | No |
| `WORKFLOW_FAILED` | Workflow failed | No |
| `WORKFLOW_CANCELLED` | Workflow was cancelled | No |
| `UPSTREAM_SKIPPED` | Upstream task in workflow was skipped | No |
| `SUBWORKFLOW_FAILED` | Subworkflow failed | No |
| `WORKFLOW_SUCCESS_CASE_NOT_MET` | Workflow success condition was not satisfied | No |

### Send Errors (TaskSendResult)

Send errors are returned via `TaskSendResult[TaskHandle[T]]` from `.send()`, `.send_async()`, and `.schedule()`. These are separate from `TaskResult` -- they indicate that enqueuing the task itself failed, not that task execution failed.

#### TaskSendError

| Field | Type | Description |
| ----- | ---- | ----------- |
| `code` | `TaskSendErrorCode` | Failure category |
| `message` | `str` | Human-readable description |
| `retryable` | `bool` | Whether the caller can retry with the same payload |
| `task_id` | `str \| None` | Generated task ID (`None` for `SEND_SUPPRESSED`, `VALIDATION_FAILED`) |
| `payload` | `TaskSendPayload \| None` | Serialized envelope for idempotent retry |
| `exception` | `BaseException \| None` | The original cause, if any |

#### TaskSendErrorCode

| Code | Description | Retryable |
| ---- | ----------- | --------- |
| `SEND_SUPPRESSED` | Send suppressed during worker import/discovery to prevent side effects | No |
| `VALIDATION_FAILED` | Argument serialization or validation failed before enqueue | No |
| `ENQUEUE_FAILED` | Broker/database failure during enqueue (transient) | Yes |
| `PAYLOAD_MISMATCH` | Retry payload SHA does not match -- payload was altered between send and retry | No |

`ENQUEUE_FAILED` errors carry a `TaskSendPayload` with an `enqueue_sha` field. Pass the error to `.retry_send(error)` or `.retry_send_async(error)` to replay the exact payload. The SHA guarantees same-payload idempotency.

#### Workflow Errors

Workflow-specific error codes are distributed across the four families above:

- **OperationalErrorCode**: `WORKFLOW_ENQUEUE_FAILED`, `SUBWORKFLOW_LOAD_FAILED`
- **ContractCode**: `WORKFLOW_CTX_MISSING_ID`
- **RetrievalCode**: `WORKFLOW_NOT_FOUND`
- **OutcomeCode**: `WORKFLOW_PAUSED`, `WORKFLOW_FAILED`, `WORKFLOW_CANCELLED`, `UPSTREAM_SKIPPED`, `SUBWORKFLOW_FAILED`, `WORKFLOW_SUCCESS_CASE_NOT_MET`

### User-Defined Error Codes

Domain-specific errors use string codes:

```python
from horsies import TaskResult, TaskError

@app.task("validate_order")
def validate_order(order_id: str) -> TaskResult[dict, TaskError]:
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
def call_api(url: str) -> TaskResult[dict, TaskError]:
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
| HRS-023 | `WORKFLOW_SUBWORKFLOW_BUILD_WITH_BINDING` | Subworkflow build_with binding error (duplicate param binding) |
| HRS-024 | `WORKFLOW_ARGS_FROM_TYPE_MISMATCH` | args_from source result type doesn't match target parameter |
| HRS-025 | `WORKFLOW_OUTPUT_TYPE_MISMATCH` | Output node type doesn't match WorkflowSpec generic |
| HRS-026 | `WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED` | Positional args are not supported for workflow nodes |
| HRS-027 | `WORKFLOW_CHECK_CASES_REQUIRED` | Parameterized workflow builder missing test cases |
| HRS-028 | `WORKFLOW_CHECK_CASE_INVALID` | Workflow builder test case is invalid |
| HRS-029 | `WORKFLOW_CHECK_BUILDER_EXCEPTION` | Workflow builder raised an exception or returned non-WorkflowSpec |
| HRS-030 | `WORKFLOW_CHECK_UNDECORATED_BUILDER` | Function returns WorkflowSpec but lacks @app.workflow_builder |
| HRS-031 | `WORKFLOW_KWARGS_NOT_SERIALIZABLE` | kwargs value fails JSON serialization |

### Task Definition (HRS-100-HRS-199)

| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-100 | `TASK_NO_RETURN_TYPE` | Task function missing return type |
| HRS-101 | `TASK_INVALID_RETURN_TYPE` | Return type is not TaskResult |
| HRS-102 | `TASK_INVALID_OPTIONS` | Invalid task options |
| HRS-103 | `TASK_INVALID_QUEUE` | Invalid queue specification |
| HRS-104 | `TASK_PREDECORATED_NOT_SUPPORTED` | Task function is already decorated by another app instance |

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

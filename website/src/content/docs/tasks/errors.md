---
title: Errors Reference
summary: Exhaustive reference of all error codes and types in horsies.
related: [error-handling, ../../concepts/result-handling]
tags: [errors, TaskError, LibraryErrorCode, ErrorCode, reference]
---

# Errors Reference

Horsies has two error systems: **runtime errors** returned in `TaskResult`, and **blocking startup errors** raised during app initialization.

## Runtime Errors (TaskResult)

Runtime errors are returned via `TaskResult[T, TaskError]`. The `TaskError.error_code` field contains either a `LibraryErrorCode` or a user-defined string.

### TaskError

| Field | Type | Description |
| ----- | ---- | ----------- |
| `error_code` | `LibraryErrorCode \| str \| None` | Library or domain error code |
| `message` | `str \| None` | Human-readable description |
| `data` | `Any \| None` | Additional context (task_id, etc.) |
| `exception` | `dict \| BaseException \| None` | Original exception if applicable |

### LibraryErrorCode

All library-defined error codes. User code should handle these explicitly.

#### Execution Errors

Errors from task execution.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `UNHANDLED_EXCEPTION` | Uncaught exception in task code, wrapped to `UNHANDLED_EXCEPTION` by library  | Yes, if in `auto_retry_for` |
| `TASK_EXCEPTION` | Task raised exception or returned invalid value | Yes, if in `auto_retry_for` |
| `WORKER_CRASHED` | Worker process died during execution | Yes, if in `auto_retry_for` |

#### Retrieval Errors

Errors from `handle.get()` / `get_async()`. These indicate issues retrieving the result, not task execution failures.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `WAIT_TIMEOUT` | `get()` timed out; task may still be running | No |
| `TASK_NOT_FOUND` | Task ID doesn't exist in database | No |
| `TASK_CANCELLED` | Task was cancelled before completion | No |
| `RESULT_NOT_AVAILABLE` | Result cache was never set | No |

#### Broker Errors

Errors from broker/database operations.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `BROKER_ERROR` | Database or broker failure during operation | No |

#### Worker Errors

Errors from worker internals.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `WORKER_RESOLUTION_ERROR` | Failed to resolve task by name | No |
| `WORKER_SERIALIZATION_ERROR` | Failed to serialize/deserialize task data | No |

#### Validation Errors

Errors from type/schema validation.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `RETURN_TYPE_MISMATCH` | Task return type doesn't match declaration | No |
| `PYDANTIC_HYDRATION_ERROR` | Failed to hydrate Pydantic model from result | No |

#### Lifecycle Errors

Errors from task lifecycle management.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `SEND_SUPPRESSED` | Task send was suppressed due to prevent import side effects | No |

#### Workflow Errors

Errors specific to workflow execution.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `UPSTREAM_SKIPPED` | Upstream task in workflow was skipped | No |
| `WORKFLOW_CTX_MISSING_ID` | Workflow context is missing required ID | No |
| `WORKFLOW_SUCCESS_CASE_NOT_MET` | Workflow success condition was not satisfied | No |

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
    retry_policy=RetryPolicy.fixed([30, 60, 120]),
    auto_retry_for=["RATE_LIMITED", "SERVICE_UNAVAILABLE"],
)
def call_api(url: str) -> TaskResult[dict, TaskError]:
    ...
```

## Startup Errors (HorsiesError)

Startup errors are **blocking exceptions** raised during app initialization, workflow validation, or task registration. If a startup error occurs, the worker or scheduler will fail to start and will not accept tasks.

These errors use `ErrorCode` (not `LibraryErrorCode`) and indicate structural or configuration issues that must be resolved before the application can run.

### Validating with `horsies check`

Use the `horsies check` command to validate your configuration, task registry, and workflow definitions without starting any services. This is recommended for CI/CD pipelines to catch blocking errors before deployment.

```bash
horsies check myapp.instance:app
```

### ErrorCode Categories

| Range | Category | Description |
| ----- | -------- | ----------- |
| E001-E099 | Workflow Validation | Invalid workflow specification |
| E100-E199 | Task Definition | Invalid task decorator usage |
| E200-E299 | Configuration | Invalid app/broker configuration |
| E300-E399 | Registry | Task registration failures |

### Workflow Validation (E001-E099)

| Code | Name | Description |
| ---- | ---- | ----------- |
| E001 | `WORKFLOW_NO_NAME` | Workflow has no name |
| E002 | `WORKFLOW_NO_NODES` | Workflow has no nodes |
| E003 | `WORKFLOW_INVALID_NODE_ID` | Invalid node ID reference |
| E004 | `WORKFLOW_DUPLICATE_NODE_ID` | Duplicate node ID |
| E005 | `WORKFLOW_NO_ROOT_TASKS` | No root tasks in workflow |
| E006 | `WORKFLOW_INVALID_DEPENDENCY` | Invalid dependency reference |
| E007 | `WORKFLOW_CYCLE_DETECTED` | Cycle detected in workflow DAG |
| E008 | `WORKFLOW_INVALID_ARGS_FROM` | Invalid args_from reference |
| E009 | `WORKFLOW_INVALID_CTX_FROM` | Invalid ctx_from reference |
| E010 | `WORKFLOW_CTX_PARAM_MISSING` | Context parameter missing |
| E011 | `WORKFLOW_INVALID_OUTPUT` | Invalid output specification |
| E012 | `WORKFLOW_INVALID_SUCCESS_POLICY` | Invalid success policy |
| E013 | `WORKFLOW_INVALID_JOIN` | Invalid join configuration |
| E014 | `WORKFLOW_UNRESOLVED_QUEUE` | Queue name not resolved |
| E015 | `WORKFLOW_UNRESOLVED_PRIORITY` | Priority not resolved |
| E016 | `WORKFLOW_ARGS_WITH_INJECTION` | Positional args used with args_from or workflow_ctx_from |
| E019 | `WORKFLOW_INVALID_KWARG_KEY` | Unknown kwargs or args_from key for callable |
| E020 | `WORKFLOW_MISSING_REQUIRED_PARAMS` | Missing required parameters for task or subworkflow |
| E017 | `WORKFLOW_INVALID_SUBWORKFLOW_RETRY_MODE` | Invalid subworkflow retry mode |
| E018 | `WORKFLOW_SUBWORKFLOW_APP_MISSING` | Subworkflow app reference missing |

### Task Definition (E100-E199)

| Code | Name | Description |
| ---- | ---- | ----------- |
| E100 | `TASK_NO_RETURN_TYPE` | Task function missing return type |
| E101 | `TASK_INVALID_RETURN_TYPE` | Return type is not TaskResult |
| E102 | `TASK_INVALID_OPTIONS` | Invalid task options |
| E103 | `TASK_INVALID_QUEUE` | Invalid queue specification |

### Configuration (E200-E299)

| Code | Name | Description |
| ---- | ---- | ----------- |
| E200 | `CONFIG_INVALID_QUEUE_MODE` | Invalid queue mode |
| E201 | `CONFIG_INVALID_CLUSTER_CAP` | Invalid cluster capacity |
| E202 | `CONFIG_INVALID_PREFETCH` | Invalid prefetch setting |
| E203 | `BROKER_INVALID_URL` | Invalid broker URL |
| E204 | `CONFIG_INVALID_RECOVERY` | Invalid recovery configuration |
| E205 | `CONFIG_INVALID_SCHEDULE` | Invalid schedule configuration |
| E206 | `CLI_INVALID_ARGS` | Invalid CLI arguments |
| E207 | `WORKER_INVALID_LOCATOR` | Invalid worker locator |

### Registry (E300-E399)

| Code | Name | Description |
| ---- | ---- | ----------- |
| E300 | `TASK_NOT_REGISTERED` | Task not found in registry |
| E301 | `TASK_DUPLICATE_NAME` | Duplicate task name |

### HorsiesError Structure

Startup errors provide Rust-style formatting automatically. When raised, they display with source location and context:

```text
error[E007]: cycle detected in workflow DAG
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
    spec = app.workflow("order_flow", tasks=[node_a, node_b])
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
| `unwrap()` | `T` | Same as `ok_value` |
| `unwrap_err()` | `TaskError` | Same as `err_value` |

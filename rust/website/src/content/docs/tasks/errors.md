---
title: Errors Reference
summary: Reference for runtime task errors, send errors, and startup validation errors.
related: [error-handling, ../../concepts/result-handling]
tags: [errors, TaskError, BuiltInTaskCode, ErrorCode, reference]
---

# Errors Reference

Horsies has three relevant error surfaces:

- runtime task/workflow results returned in `TaskResult<T>`
- send-time errors returned in `TaskSendResult<T>`
- startup or validation errors returned as `HorsiesError`

## Runtime Errors (TaskResult)

Runtime errors are returned via `TaskResult<T>`. The `TaskError.error_code` field contains either a `TaskErrorCode::BuiltIn(BuiltInTaskCode)` or a `TaskErrorCode::User(String)`.

When a task reaches terminal `Failed`, its `error_code` moves to
`horsies_task_history`. Access it through `TaskInfo.error_code` from
`handle.info()`. The API resolves live and history storage.

Each execution attempt is first recorded in `horsies_task_attempts`. The
terminal move archives the attempt snapshot in the history row and removes the
live attempt rows in the same transaction.

### TaskError

| Field | Type | Description |
| ----- | ---- | ----------- |
| `error_code` | `Option<TaskErrorCode>` | Library or domain error code |
| `message` | `Option<String>` | Human-readable description |
| `data` | `Option<serde_json::Value>` | Additional context |
| `cause` | `Option<serde_json::Value>` | Original cause (serialized as `"exception"` on wire) |

### TaskErrorCode

```rust
pub enum TaskErrorCode {
    BuiltIn(BuiltInTaskCode),
    User(String),
}
```

Built-in codes are serialized as `{"__builtin_task_code__": "CODE_NAME"}`. User strings are serialized as plain strings.

### BuiltInTaskCode (4-Family Split)

All library-defined error codes are grouped into four families.

#### OperationalErrorCode

Errors from task execution, broker, and worker internals.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `UnhandledError` | Uncaught panic or error in task code | Yes, if in `auto_retry_for` |
| `TaskError` | Task function returned an error | Yes, if in `auto_retry_for` |
| `WorkerCrashed` | Worker died during execution | Yes, if in `auto_retry_for` |
| `BrokerError` | Database or broker failure | No |
| `WorkerResolutionError` | Failed to resolve task by name | No |
| `WorkerSerializationError` | Failed to serialize/deserialize task data | No |
| `ResultDeserializationError` | Stored result JSON is corrupt | No |
| `WorkflowEnqueueFailed` | Workflow node failed during enqueue | No |
| `SubworkflowLoadFailed` | Subworkflow definition could not be loaded | No |

#### ContractCode

Errors from type/schema validation and structural contracts.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `ArgumentTypeMismatch` | Arguments don't match expected type | No |
| `ReturnTypeMismatch` | Return type doesn't match declaration | No |
| `WorkflowCtxMissingId` | Workflow context is missing required ID | No |

#### RetrievalCode

Errors from `handle.get()`. These indicate issues retrieving the result, not task execution failures.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `WaitTimeout` | `get()` timed out; task may still be running | No |
| `TaskNotFound` | Task ID doesn't exist in database | No |
| `WorkflowNotFound` | Workflow ID doesn't exist in database | No |
| `ResultNotAvailable` | Result cache was never set | No |
| `ResultNotReady` | Result not yet available; task is still running | No |

#### OutcomeCode

Terminal outcome codes for tasks and workflows.

| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `TaskCancelled` | Task was cancelled before completion | No |
| `TaskExpired` | `good_until` deadline passed before execution started | No |
| `WorkflowPaused` | Workflow was paused | No |
| `WorkflowFailed` | Workflow failed | No |
| `WorkflowCancelled` | Workflow was cancelled | No |
| `UpstreamSkipped` | Upstream task in workflow was skipped | No |
| `SubworkflowFailed` | Subworkflow failed | No |
| `WorkflowSuccessCaseNotMet` | Workflow success condition was not satisfied | No |
| `WorkflowStopped` | Workflow was stopped | No |
| `SendSuppressed` | Send was suppressed (e.g. during check phase) | No |

### Send Errors (TaskSendResult)

Send errors are returned via `TaskSendResult<TaskHandle<T>>`. These are separate from `TaskResult` — they indicate that enqueuing the task itself failed, not that task execution failed.

#### TaskSendError

| Field | Type | Description |
| ----- | ---- | ----------- |
| `code` | `TaskSendErrorCode` | Failure category |
| `message` | `String` | Human-readable description |
| `retryable` | `bool` | Whether the caller can retry with the same payload |
| `task_id` | `Option<Uuid>` | Generated task ID |
| `payload` | `Option<TaskSendPayload>` | Serialized envelope for idempotent retry |

#### TaskSendErrorCode

| Code | Description | Retryable |
| ---- | ----------- | --------- |
| `SendSuppressed` | Send suppressed during check phase | No |
| `ValidationFailed` | Argument serialization or validation failed | No |
| `EnqueueFailed` | Broker/database failure during enqueue (transient) | Yes |
| `PayloadMismatch` | Retry payload SHA does not match | No |

### User-Defined Error Codes

Domain-specific errors use string codes via `TaskErrorCode::User`:

```rust
#[task("validate_order")]
async fn validate_order(input: OrderInput) -> Result<OrderResult, TaskError> {
    if input.total > 10000 {
        return Err(TaskError::new(
            "ORDER_LIMIT_EXCEEDED",
            format!("Order {} exceeds limit", input.order_id),
        ));
    }
    Ok(OrderResult { status: "valid".into() })
}
```

User-defined codes are auto-retried when listed in `auto_retry_for`:

```rust
#[task(
    "call_api",
    auto_retry_for = ["RATE_LIMITED", "SERVICE_UNAVAILABLE"],
    retry_policy = RetryPolicy::fixed(vec![30, 60, 120], true)?
)]
async fn call_api(input: ApiInput) -> Result<ApiResponse, TaskError> {
    // ...
}
```

## Startup Errors (HorsiesError)

Startup errors are returned during app initialization, workflow validation, or task registration. If a startup error occurs, the worker or scheduler will fail to start.

These errors use `ErrorCode` (not `BuiltInTaskCode`) and indicate structural or configuration issues.

### HorsiesError Structure

```rust
pub struct HorsiesError {
    message: String,
    code: Option<ErrorCode>,
    notes: Vec<String>,
}
```

### ErrorCode Categories

| Range | Category | Description |
| ----- | -------- | ----------- |
| HRS-001–HRS-099 | Workflow Validation | Invalid workflow specification |
| HRS-100–HRS-199 | Task Definition | Invalid task macro usage |
| HRS-200–HRS-299 | Configuration | Invalid app, broker, CLI, or worker configuration |
| HRS-300–HRS-399 | Registry | Task/workflow registry failures |

### Workflow Validation (HRS-001–HRS-099)

| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-001 | `WorkflowNoName` | Workflow has no name |
| HRS-002 | `WorkflowNoNodes` | Workflow has no nodes |
| HRS-003 | `WorkflowInvalidNodeId` | Invalid node ID reference |
| HRS-004 | `WorkflowDuplicateNodeId` | Duplicate node ID |
| HRS-005 | `WorkflowNoRootTasks` | No root tasks in workflow |
| HRS-006 | `WorkflowInvalidDependency` | Invalid dependency reference |
| HRS-007 | `WorkflowCycleDetected` | Cycle detected in workflow DAG |
| HRS-008 | `WorkflowInvalidArgsFrom` | Invalid `args_from` binding |
| HRS-009 | `WorkflowInvalidCtxFrom` | Invalid `workflow_ctx_from` binding |
| HRS-010 | `WorkflowCtxParamMissing` | Task expects workflow context but binding is missing |
| HRS-011 | `WorkflowInvalidOutput` | Invalid workflow output node |
| HRS-012 | `WorkflowInvalidSuccessPolicy` | Invalid success policy configuration |
| HRS-013 | `WorkflowInvalidJoin` | Invalid join configuration |
| HRS-014 | `WorkflowUnresolvedQueue` | Queue could not be resolved for a task node |
| HRS-015 | `WorkflowUnresolvedPriority` | Priority could not be resolved for a task node |
| HRS-016 | `WorkflowNoDefinitionKey` | Missing workflow definition key where required |
| HRS-017 | `WorkflowDuplicateDefinitionKey` | Duplicate workflow definition key |
| HRS-018 | `WorkflowSubworkflowAppMissing` | Subworkflow app/registry context missing |
| HRS-019 | `WorkflowInvalidKwargKey` | Invalid kwarg key in injected workflow data |
| HRS-020 | `WorkflowMissingRequiredParams` | Workflow input is missing required params |
| HRS-025 | `WorkflowOutputTypeMismatch` | Workflow output type does not match declared type |
| HRS-027 | `WorkflowCheckCasesRequired` | Workflow check builder requires cases |
| HRS-028 | `WorkflowCheckCaseInvalid` | Invalid workflow check case |
| HRS-029 | `WorkflowCheckBuilderException` | Workflow check builder raised an exception |
| HRS-030 | `WorkflowCheckUndecoratedBuilder` | Workflow check builder was not decorated correctly |
| HRS-032 | `WorkflowArgsWithInjection` | Workflow mixes whole args with injected params in an invalid way |

### Task Definition (HRS-100–HRS-199)

| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-100 | `TaskNoReturnType` | Task is missing a return type |
| HRS-101 | `TaskInvalidReturnType` | Task return type is not supported |
| HRS-102 | `TaskInvalidOptions` | Invalid task options |
| HRS-103 | `TaskInvalidQueue` | Invalid task queue configuration |

### Configuration (HRS-200–HRS-299)

| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-200 | `ConfigInvalidQueueMode` | Invalid queue mode configuration |
| HRS-201 | `ConfigInvalidClusterCap` | Invalid cluster capacity configuration |
| HRS-202 | `ConfigInvalidPrefetch` | Invalid prefetch configuration |
| HRS-203 | `BrokerInvalidUrl` | Invalid broker URL |
| HRS-204 | `ConfigInvalidRecovery` | Invalid recovery configuration |
| HRS-205 | `ConfigInvalidSchedule` | Invalid schedule configuration |
| HRS-206 | `CliInvalidArgs` | Invalid CLI arguments |
| HRS-207 | `WorkerInvalidLocator` | Invalid worker locator configuration |
| HRS-211 | `BrokerInitFailed` | Broker initialization failed |
| HRS-212 | `CheckReservedCodeCollision` | Reserved built-in code collides with user retry configuration |

### Registry (HRS-300–HRS-399)

| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-300 | `TaskNotRegistered` | Task not found in registry |
| HRS-301 | `TaskDuplicateName` | Duplicate task name |
| HRS-302 | `WorkflowUnregisteredTask` | Workflow references a task that was not registered |

### Validation

Use `app.check()` for static validation or `app.check_live()` to additionally connect to PostgreSQL, ensure the schema, and verify connectivity:

```rust
// Static validation only
app.check()?;

// Static + schema ensure + database ping
app.check_live().await?;
```

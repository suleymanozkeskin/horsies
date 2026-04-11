---
title: Result Handling
summary: TaskResult pattern, error types, and explicit error handling.
related: [task-lifecycle, ../../tasks/retrieving-results]
tags: [concepts, results, errors, TaskResult]
---

## TaskResult\<T\>

Every task must return `Result<T, TaskError>`. On the wire and when retrieved, results are wrapped in `TaskResult<T>`:

```rust
pub enum TaskResult<T> {
    Ok(T),
    Err(TaskError),
}
```

A `TaskResult` contains exactly one of:

- `Ok(value)`: The success value (type `T`)
- `Err(error)`: The error value (type `TaskError`)

```rust
use horsies::{task, TaskError};

#[task("divide")]
async fn divide(input: DivideInput) -> Result<f64, TaskError> {
    if input.b == 0 {
        return Err(TaskError::new(
            "DIVISION_BY_ZERO",
            "Cannot divide by zero",
        ));
    }
    Ok(input.a as f64 / input.b as f64)
}
```

## Checking Results

```rust
use std::time::Duration;
use horsies::TaskResult;

match divide::send(input).await {
    Ok(handle) => {
        let result: TaskResult<f64> = handle.get(Some(Duration::from_secs(5))).await;

        match result {
            TaskResult::Ok(value) => println!("Result: {}", value),
            TaskResult::Err(err) => {
                println!("Error: {:?} - {:?}", err.error_code, err.message);
            }
        }
    }
    Err(send_err) => {
        println!("Send failed: {} - {}", send_err.code, send_err.message);
    }
}
```

## TaskError Structure

`TaskError` is a struct with optional fields:

| Field | Type | Purpose |
|-------|------|---------|
| `error_code` | `Option<TaskErrorCode>` | Identifies the error type |
| `message` | `Option<String>` | Human-readable description |
| `data` | `Option<serde_json::Value>` | Additional context |
| `cause` | `Option<serde_json::Value>` | Original cause (serialized as `"exception"` on wire) |

```rust
Err(TaskError {
    error_code: Some(TaskErrorCode::User("VALIDATION_FAILED".into())),
    message: Some("Input validation failed".into()),
    data: Some(serde_json::json!({"field": "email", "reason": "invalid format"})),
    cause: None,
})
```

Convenience constructors:

```rust
// User-defined error
TaskError::new("INVALID_AMOUNT", "Amount must be positive")

// Built-in error
TaskError::builtin(OperationalErrorCode::BrokerError, "connection lost")
```

## Built-In Error Codes

When the library itself encounters an error (not your task code), it uses one of four error code families via `TaskErrorCode::BuiltIn(BuiltInTaskCode)`.

### OperationalErrorCode

| Code | When |
| ---- | ---- |
| `UnhandledError` | Task panicked or hit an uncaught error |
| `TaskError` | Task function returned an error |
| `WorkerCrashed` | Worker died (detected via heartbeat) |
| `BrokerError` | Broker failed while retrieving the result |
| `WorkerResolutionError` | Worker couldn't find the task in registry |
| `WorkerSerializationError` | Result couldn't be serialized |
| `ResultDeserializationError` | Stored result JSON is corrupt or could not be deserialized |
| `WorkflowEnqueueFailed` | Workflow node failed during enqueue/build |
| `SubworkflowLoadFailed` | Subworkflow definition could not be loaded |

### ContractCode

| Code | When |
| ---- | ---- |
| `ArgumentTypeMismatch` | Arguments don't match the expected type |
| `ReturnTypeMismatch` | Returned value doesn't match declared type |
| `WorkflowCtxMissingId` | Workflow context is missing required ID |

### RetrievalCode

Returned by `handle.get()` for issues retrieving results:

| Code | When |
| ---- | ---- |
| `WaitTimeout` | Timeout elapsed while waiting for result (task may still be running) |
| `TaskNotFound` | Task ID doesn't exist in database |
| `WorkflowNotFound` | Workflow ID doesn't exist in database |
| `ResultNotAvailable` | Result cache is empty |
| `ResultNotReady` | Result not yet available; task is still running |

### OutcomeCode

| Code | When |
| ---- | ---- |
| `TaskCancelled` | Task was cancelled before completion |
| `TaskExpired` | Task expired before execution started |
| `WorkflowPaused` | Workflow was paused |
| `WorkflowFailed` | Workflow failed |
| `WorkflowCancelled` | Workflow was cancelled |
| `UpstreamSkipped` | Upstream task in workflow was skipped |
| `SubworkflowFailed` | Subworkflow failed |
| `WorkflowSuccessCaseNotMet` | Workflow success condition was not satisfied |
| `WorkflowStopped` | Workflow was stopped |
| `SendSuppressed` | Send was suppressed (e.g. during check phase) |

## Domain Errors vs Library Errors

**Domain errors**: Your task returns an error for business logic reasons.

```rust
#[task("transfer_funds")]
async fn transfer_funds(input: TransferInput) -> Result<String, TaskError> {
    if input.amount <= 0.0 {
        // This is a domain error - expected, handled
        return Err(TaskError::new(
            "INVALID_AMOUNT",
            "Amount must be positive",
        ));
    }
    Ok("Transfer complete".into())
}
```

**Built-in errors**: Something went wrong in the infrastructure.

```rust
#[task("buggy_task")]
async fn buggy_task() -> Result<String, TaskError> {
    panic!("Oops"); // Becomes UnhandledError
}
```

Both cases result in `TaskResult::Err(...)`, but the error codes differ.

## Convenience Methods

```rust
let result = handle.get(None).await;

// Check state
result.is_ok()        // true if success
result.is_err()       // true if error
result.is_transient() // true if retrieval/broker error (may resolve on retry)
result.is_terminal()  // true if final (not transient)

// Access values (panics if wrong variant)
result.unwrap()       // Returns Ok value
result.unwrap_err()   // Returns TaskError

// Safe access
result.ok()           // Option<T>

// Transform
result.map(|v| v.to_string())     // Map success value
result.and_then(|v| /* ... */)    // Chain operations
```

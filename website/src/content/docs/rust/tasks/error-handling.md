---
title: Error Handling
summary: Patterns for handling TaskResult errors with explicit control flow.
related: [errors, retrieving-results, retry-policy]
tags: [tasks, errors, TaskResult, patterns]
---

# Error Handling

Handle errors explicitly through pattern matching and explicit returns. Avoid panicking for flow control — panics break control flow and obscure error paths.

For exhaustive error code reference, see [errors](errors.md).

This document means to be a pattern guide rather than definitive way to handle your errors.
Adjust to your needs.

## Why and When

### Error Categories

| Category | Source | Examples | Auto-Retry? |
| -------- | ------ | -------- | ----------- |
| Retrieval/transient errors | `handle.get()` or broker reads | `WaitTimeout`, `TaskNotFound`, `BrokerError` | No |
| Execution errors | Task panicked or worker/runtime failed | `UnhandledError`, `WorkerCrashed` | If in `auto_retry_for` |
| Domain errors | Your task returned `TaskError::new(...)` | `"RATE_LIMITED"`, `"VALIDATION_FAILED"` | If in `auto_retry_for` |

### When to Handle Errors

Handle errors when:

- The error is a **retrieval error** (task may still complete)
- The error is **not in `auto_retry_for`** (won't be retried)
- The task has **exhausted retries** (final failure)

## How To

### Configure Automatic Retries

The library handles retries automatically when `auto_retry_for` matches the error:

```rust
#[task(
    "fetch_api_data",
    auto_retry_for = ["RATE_LIMITED", "TIMEOUT"],
    retry_policy = RetryPolicy::exponential(30, 3, true)?
)]
async fn fetch_api_data(input: ApiInput) -> Result<ApiResponse, TaskError> {
    // If this returns a RATE_LIMITED error, the worker automatically
    // retries up to 3 times with exponential backoff
    match call_api(&input.url).await {
        Ok(response) => Ok(response),
        Err(e) if e.is_rate_limited() => {
            Err(TaskError::new("RATE_LIMITED", "Rate limited"))
        }
        Err(e) if e.is_timeout() => {
            Err(TaskError::new("TIMEOUT", "Request timed out"))
        }
        Err(e) => Err(TaskError::new("API_ERROR", e.to_string())),
    }
}
```

See [retry-policy](./retry-policy) for configuration details.

### Handling Upstream Failures in Chained Tasks

Tasks wired with `args_from` in workflows receive the deserialized upstream result. When using `allow_failed_deps`, the task may receive an error result from the upstream node:

```rust
#[task("parse_product_page")]
async fn parse_product_page(
    page_result: TaskResult<String>,
) -> Result<ProductRecord, TaskError> {
    match page_result {
        TaskResult::Err(err) => {
            Err(TaskError::new(
                "UPSTREAM_FAILED",
                format!("Cannot parse: fetch failed - {:?}", err.error_code),
            ))
        }
        TaskResult::Ok(html) => {
            Ok(ProductRecord {
                product_id: "product-123".into(),
                name: "Widget Pro".into(),
                price_cents: 1999,
            })
        }
    }
}
```

This pattern only runs when the downstream task actually executes. With `JoinType::All` and `allow_failed_deps = false` (default), any failed dependency causes the task to be Skipped. See [Workflow Semantics](../../concepts/workflows/workflow-semantics) for join rules and failure propagation.

### Handle Errors with Pattern Matching

```rust
use std::time::Duration;
use horsies::TaskResult;

async fn process_task(input: MyInput) -> Option<String> {
    let handle = match my_task::send(input).await {
        Ok(h) => h,
        Err(send_err) => {
            println!("Send failed: {} - {}", send_err.code, send_err.message);
            return None;
        }
    };

    let result = handle.get(Some(Duration::from_secs(5))).await;

    match result {
        TaskResult::Ok(value) => Some(value),
        TaskResult::Err(err) => {
            if err.is_transient() {
                // Task may still be running, check status again
                println!("Transient error, task may complete later");
            } else {
                handle_error(&err);
            }
            None
        }
    }
}
```

## Things to Avoid

**Don't panic for flow control.**

```rust
// Wrong - panics break control flow
if result.is_err() {
    panic!("Task failed: {:?}", result.unwrap_err().error_code);
}

// Correct - explicit return and handling
if result.is_err() {
    handle_error(&result.unwrap_err());
    return None;
}
```

**Don't ignore errors after logging.**

```rust
// Wrong - logs but continues as if nothing happened
if result.is_err() {
    println!("{:?}", result.unwrap_err().message);
}
// Code continues...

// Correct - handle and return explicitly
match result {
    TaskResult::Err(err) => {
        handle_error(&err);
        return Err(err);
    }
    TaskResult::Ok(value) => { /* use value */ }
}
```

**Don't manually retry errors that should be auto-retried.**

```rust
// Wrong - duplicates library retry logic
if error.error_code == Some(TaskErrorCode::User("RATE_LIMITED".into())) {
    manually_schedule_retry(task_id);
}

// Correct - configure auto_retry_for on the task definition
#[task(
    "my_task",
    auto_retry_for = ["RATE_LIMITED"],
    retry_policy = RetryPolicy::fixed(vec![60, 120, 300], true)?
)]
async fn my_task() -> Result<String, TaskError> { ... }
```

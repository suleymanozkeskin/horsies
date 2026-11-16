---
title: Defining Tasks
summary: How to define tasks with the #[task] and #[blocking_task] proc macros.
related: [sending-tasks, retrieving-results, ../../concepts/result-handling]
tags: [tasks, macro, definition]
---

# Defining Tasks

Define tasks by annotating functions with `#[task]` or `#[blocking_task]` and returning `Result<T, TaskError>`.

`#[task]` requires an `async fn`. For CPU-bound synchronous work, use `#[blocking_task]` (runs via `tokio::task::spawn_blocking`).

## How To

### Basic Task

```rust
use horsies::{task, TaskError};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct ProcessInput { order_id: i64 }

#[task("process_order")]
async fn process_order(input: ProcessInput) -> Result<String, TaskError> {
    Ok(format!("Order {} processed", input.order_id))
}
```

### Return Success

```rust
#[task("compute")]
async fn compute(input: ComputeInput) -> Result<i64, TaskError> {
    Ok(input.x * 2)
}
```

For void tasks, return `()`:

```rust
#[task("fire_and_forget")]
async fn fire_and_forget() -> Result<(), TaskError> {
    do_something();
    Ok(())
}
```

### Return Errors

Domain errors (expected failures):

```rust
#[task("validate_input")]
async fn validate_input(data: InputData) -> Result<InputData, TaskError> {
    if data.email.is_empty() {
        return Err(TaskError::new(
            "MISSING_EMAIL",
            "Email is required",
        ));
    }
    Ok(data)
}
```

Panics are caught and wrapped automatically:

```rust
#[task("might_crash")]
async fn might_crash() -> Result<String, TaskError> {
    panic!("Something went wrong");
    // Becomes TaskResult::Err(TaskError {
    //     error_code: OperationalErrorCode::UnhandledError,
    //     message: "Unhandled panic in task might_crash: ...",
    // })
}
```

### Blocking Tasks

For CPU-bound work that should not block the tokio runtime:

```rust
use horsies::blocking_task;

#[blocking_task("heavy_computation")]
fn heavy_computation(input: ComputeInput) -> Result<f64, TaskError> {
    // Runs via tokio::task::spawn_blocking
    let result = expensive_cpu_work(input.data);
    Ok(result)
}
```

### Configure Retries

```rust
use horsies::RetryPolicy;

#[task(
    "some_api_call",
    auto_retry_for = ["TIMEOUT", "CONNECTION_ERROR"],
    retry_policy = RetryPolicy::exponential(30, 5, true)?
)]
async fn some_api_call() -> Result<String, TaskError> {
    match call_external_api() {
        Ok(result) => Ok(result),
        Err(e) if e.is_timeout() => {
            Err(TaskError::new("TIMEOUT", "Request timed out"))
        }
        Err(e) => {
            Err(TaskError::new("CONNECTION_ERROR", e.to_string()))
        }
    }
}
```

For backoff strategies, jitter, and auto-retry triggers, see [Retry Policy](./retry-policy).

### Assign to Queue (Custom Mode)

```rust
#[task("urgent_task", queue = "critical")]
async fn urgent_task() -> Result<String, TaskError> {
    Ok("done".into())
}
```

### Access TaskRuntime

Tasks can optionally receive a `TaskRuntime` as their first parameter to access shared state or start workflows:

```rust
use horsies::{task, TaskError, TaskRuntime};

#[task("with_runtime")]
async fn with_runtime(rt: TaskRuntime, input: MyInput) -> Result<String, TaskError> {
    // Access shared state
    let db = rt.state::<DbPool>()?;

    // Start a workflow from within a task
    let handle = rt.start::<OutputType>(spec).await?;

    Ok("done".into())
}
```

### Register Tasks

Each `#[task]` macro generates a companion module with a `register` function:

```rust
// Register individual tasks
process_order::register(&mut app)?;
validate_input::register(&mut app)?;

// Or use discover() with a list of registrar closures
app.discover(vec![
    |app| process_order::register(app).map(|_| ()),
    |app| validate_input::register(app).map(|_| ()),
])?;
```

After registration, the generated module helpers such as `process_order::send(...)`,
`process_order::with_options(...)`, and `process_order::node()?` become usable.

## Things to Avoid

**Don't use generics or lifetime parameters.**

```rust
// Wrong - generics not supported
#[task("bad_task")]
async fn bad_task<T: Serialize>(input: T) -> Result<T, TaskError> { ... }

// Correct - use concrete types
#[task("good_task")]
async fn good_task(input: MyInput) -> Result<MyOutput, TaskError> { ... }
```

**Don't reuse task names.**

```rust
#[task("duplicate_name")]
async fn task_one() -> Result<String, TaskError> { ... }

#[task("duplicate_name")]  // Registration will fail
async fn task_two() -> Result<String, TaskError> { ... }
```

**Don't define tasks as methods.**

```rust
// Wrong - self parameter not supported
impl MyStruct {
    #[task("bad")]
    async fn bad(&self) -> Result<String, TaskError> { ... }
}
```

## API Reference

### `#[task("name", ...)]`

| Attribute | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `"name"` | string literal | Yes | Unique task identifier |
| `queue` | string literal | No | Target queue (Custom mode only) |
| `auto_retry_for` | array of string literals | No | Error codes that trigger retry |
| `retry_policy` | expression | No | `RetryPolicy` for timing and backoff |

`good_until` is intentionally not accepted on `#[task]`: it would be evaluated when the task is registered, not each time the task is sent. For ad-hoc sends, use `TaskSendOptions` from [Sending Tasks](./sending-tasks). For workflow tasks, set `.good_until(deadline)` on the workflow node.

### Generated module

For `#[task("my_task")]` on `fn my_task`, generates:

| Function | Description |
| -------- | ----------- |
| `my_task::register(&mut app)` | Register task, returns `TaskFunction<A, T>` |
| `my_task::send(args).await` | Send task for immediate execution |
| `my_task::schedule(delay, args).await` | Send task with delay |
| `my_task::with_options(options).send(args).await` | Send with per-send options |
| `my_task::handle(&rt)` | Get `TaskFunction` from `TaskRuntime` |
| `my_task::node()` | Get `TaskNode<T, A>` for workflow composition |
| `my_task::params::field_name()` | Typed field token for `arg_from` / `set` in workflows |

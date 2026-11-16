---
title: Producing Tasks
summary: Define tasks and send them for execution.
related: [01-configuring-horsies, 03-defining-workflows]
tags: [quickstart, tasks]
---

## Defining a Task

Mark an async function with the `#[task]` attribute macro to define it. Provide a task name and optionally a target queue. Registration on the app is a separate step via `task_name::register(&mut app)`.

```rust
use horsies::{task, TaskError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderItem {
    sku: String,
    quantity: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    items: Vec<OrderItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValidatedOrder {
    order_id: String,
    items: Vec<OrderItem>,
    validated_at: chrono::DateTime<chrono::Utc>,
}

#[task("validate_order", queue = "standard")]
async fn validate_order(order: Order) -> Result<ValidatedOrder, TaskError> {
    if order.order_id.is_empty() {
        return Err(TaskError::new(
            "INVALID_ORDER_ID",
            "Order ID is required",
        ));
    }

    if order.items.is_empty() {
        return Err(TaskError::new(
            "EMPTY_ORDER",
            "Order must contain at least one item",
        ));
    }

    Ok(ValidatedOrder {
        order_id: order.order_id,
        items: order.items,
        validated_at: chrono::Utc::now(),
    })
}
```

Every task returns `Result<T, TaskError>`. Use `Ok(value)` for success and `Err(TaskError::new(...))` for failure.

## Sending a Task

`send()` dispatches the task to its queue and returns a `TaskSendResult<TaskHandle<T>>` — either `Ok(TaskHandle)` on success or `Err(TaskSendError)` on failure:

```rust
use std::time::Duration;
use horsies::TaskResult;

match validate_order::send(order).await {
    Ok(handle) => {
        match handle.get(Some(Duration::from_secs(5))).await {
            TaskResult::Ok(validated) => {
                println!("Validated: {}", validated.order_id);
            }
            TaskResult::Err(err) => {
                println!("Failed: {:?} - {:?}", err.error_code, err.message);
            }
        }
    }
    Err(send_err) => {
        println!("Send failed: {} - {}", send_err.code, send_err.message);
    }
}
```

`timeout` controls the maximum wait time. If the task does not complete within the timeout, `get()` returns a `TaskResult::Err` with `RetrievalCode::WaitTimeout`. The task may still be running. Match it like this:

```rust
use horsies::{TaskResult, TaskErrorCode, BuiltInTaskCode, RetrievalCode};

match handle.get(Some(Duration::from_secs(5))).await {
    TaskResult::Err(err) if err.error_code == Some(TaskErrorCode::from(RetrievalCode::WaitTimeout)) => {
        println!("Timed out, task may still be running");
    }
    // ...
}
```

## Delayed Execution

`schedule()` dispatches the task after a delay:

```rust
use std::time::Duration;

match validate_order::schedule(Duration::from_secs(5), order).await {
    Ok(handle) => println!("Scheduled: {}", handle.task_id()),
    Err(err) => println!("Schedule failed: {}", err.code),
}
```

## Retrying Failed Sends

If a send fails with a retryable error, use `retry_send` on the `TaskFunction` you received at registration:

```rust
// Keep the TaskFunction from registration
let validate_order_task = validate_order::register(&mut app)?;

// Later, when sending:
match validate_order_task.send(order.clone()).await {
    Ok(handle) => {
        match handle.get(Some(Duration::from_secs(30))).await {
            TaskResult::Ok(validated) => {
                println!("Validated: {}", validated.order_id);
            }
            TaskResult::Err(err) => {
                println!("Task failed: {:?}", err.error_code);
            }
        }
    }
    Err(send_err) if send_err.retryable => {
        // Retry using the preserved payload from the error
        match validate_order_task.retry_send(&send_err).await {
            Ok(handle) => println!("Retry succeeded: {}", handle.task_id()),
            Err(retry_err) => println!("Retry failed: {}", retry_err.code),
        }
    }
    Err(send_err) => {
        println!("Non-retryable error: {}", send_err.code);
    }
}
```

:::note[Too verbose?]
Set `resend_on_transient_err` on `AppConfig` to automatically retry transient enqueue failures with exponential backoff — no manual `retry_send` needed. See more in detail <a href="/horsies-rust/configuration/app-config/#fields" target="_blank">here</a>.
:::

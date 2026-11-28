---
title: Producing Tasks
summary: Define the Acme order tasks with a code enum, send them, read each outcome, and delay a send.
related: [01-configuring-horsies, 03-defining-workflows, ../tasks/defining-tasks, ../tasks/sending-tasks, ../tasks/retrieving-results]
tags: [rust, quickstart, tasks, send, TaskOutcome]
---

# Producing Tasks

This page adds two tasks of Acme Clothing: `validate_order` and `send_shipping_sms`. It sends them and reads what each one returns.

## Add the Crate for Serde

The result of a task is stored as JSON, so it derives `Serialize` and `Deserialize`:

```bash
cargo add serde --features derive
```

## The Domain File

In `src/domain.rs`:

```rust
use horsies::CustomErrorCode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, CustomErrorCode)]
pub enum AcmeCode {
    OrderNotFound, // ORDER_NOT_FOUND
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderValidation {
    pub order_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShippingNotice {
    pub order_id: String,
    pub tracking_code: String,
}
```

`AcmeCode` is the code enum of every Acme task. Each variant is a code in `UPPER_SNAKE_CASE`. The next pages add variants to it. Add `pub mod domain;` to `src/lib.rs`.

## Define a Task

In `src/tasks/orders.rs`:

<!-- rust-check: skip partial -- uses the domain module of the example package -->
```rust
use horsies::{task, Horsies, HorsiesError, TaskError};

use crate::domain::{AcmeCode, OrderValidation};

#[task("validate_order", queue = "fulfillment")]
pub async fn validate_order(order_id: String) -> Result<OrderValidation, TaskError<AcmeCode>> {
    if order_id.is_empty() {
        return Err(TaskError::custom(AcmeCode::OrderNotFound, "the order id is empty"));
    }
    Ok(OrderValidation { order_id })
}

pub fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _validate_order = validate_order::register(app)?;
    Ok(())
}
```

- A task returns `Result<T, TaskError<E>>`. `Ok(value)` is success. `Err(TaskError::custom(code, message))` is a failure with a code that the task declares.
- `E` is the code enum. A code that the enum does not name does not compile.
- The task names its queue. The queue is one of the queues of the app.

In `src/tasks/mod.rs`, add `pub mod orders;`.

A second task goes into `src/tasks/notify.rs`, next to `send_welcome_email`:

<!-- rust-check: skip partial -- uses the domain module of the example package -->
```rust
use horsies::{task, TaskError};

use crate::domain::{AcmeCode, ShippingNotice};

#[task("send_shipping_sms", queue = "notifications")]
pub async fn send_shipping_sms(
    order_id: String,
    tracking_code: String,
) -> Result<ShippingNotice, TaskError<AcmeCode>> {
    Ok(ShippingNotice { order_id, tracking_code })
}
```

Register it in the `register` function of the same file:

<!-- rust-check: skip partial -- replaces the register function of notify.rs -->
```rust
pub fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _send_welcome_email = send_welcome_email::register(app)?;
    let _send_shipping_sms = send_shipping_sms::register(app)?;
    Ok(())
}
```

`build_app` already lists `tasks::orders::register` (page 1), so the app now has three tasks.

## Send a Task and Read the Outcome

In `src/bin/place_order.rs`:

<!-- rust-check: skip partial -- names the library crate of the example package -->
```rust
use std::time::Duration;

use getting_started::app::build_app;
use getting_started::domain::AcmeCode;
use getting_started::tasks::orders::validate_order;
use horsies::{TaskErrorCode, TaskOutcome, WaitLimit};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = build_app()?;
    let wait = WaitLimit::Within(Duration::from_secs(5));

    for order_id in ["ACME-1001", ""] {
        let handle = match validate_order::send(&app, order_id.to_owned()).await {
            Ok(handle) => handle,
            Err(send_error) => {
                println!("not sent: {send_error}");
                continue;
            }
        };
        match handle.get(wait).await {
            Ok(TaskOutcome::Succeeded(validation)) => println!("validated {}", validation.order_id),
            Ok(TaskOutcome::Failed(error)) => match error.code() {
                TaskErrorCode::Custom(AcmeCode::OrderNotFound) => println!("order not found"),
                TaskErrorCode::BuiltIn(_) => println!("library code {}", error.code()),
            },
            Err(read_error) => println!("not read: {read_error}"),
        }
    }
    Ok(())
}
```

Start the worker in one terminal and run the sender in another:

```bash
cargo run --bin getting-started -- worker --profile default
cargo run --bin place_order
```

```text
validated ACME-1001
order not found
```

`validate_order::send(&app, order_id)` takes the app, then the parameters of the task with their types. It returns `Result<TaskHandle<..>, TaskSendError>`.

- `Ok(handle)` means the task is stored.
- `Err(send_error)` means the task is not stored, or it may not be. `send_error.code()` is the catalog code, and `send_error.retryable()` tells whether the same send can work later.

## Read the Outcome

`handle.get(wait)` returns `Result<TaskOutcome<T, E>, ReadError>`:

| Result | Meaning |
|--------|---------|
| `Ok(TaskOutcome::Succeeded(value))` | The task returned `Ok(value)` |
| `Ok(TaskOutcome::Failed(error))` | The task ended with an error: a code of `AcmeCode`, or a code that the library records |
| `Err(read_error)` | The read gave no outcome, for example `ReadError::WaitTimeout` |

`WaitLimit` sets the longest wait:

- `WaitLimit::Within(duration)` waits up to `duration`. A `duration` of zero reads once.
- `WaitLimit::NoLimit` waits until the task ends.

A `WaitTimeout` does not stop the task. The task can still run, and a later `get` can find its outcome.

The match on `AcmeCode` is exhaustive. A new variant of the enum does not compile here until it has an arm. See [Retrieving Results](/horsies/rust/tasks/retrieving-results/).

## Delayed Execution

A send option delays the start of the task:

<!-- rust-check: skip partial -- a fragment of main in place_order.rs, above -->
```rust
use getting_started::tasks::notify::send_shipping_sms;
use horsies::{TaskSendOptions, WholeSeconds};

let delayed = send_shipping_sms::with_options(TaskSendOptions::new().delay(WholeSeconds::from_secs(60)))
    .send(&app, "ACME-1001".to_owned(), "TRK-0042".to_owned())
    .await;
match delayed {
    Ok(handle) => println!("queued {}", handle.task_id()),
    Err(send_error) => println!("not sent: {send_error}"),
}
```

- The task starts no earlier than 60 seconds after the send.
- The code does not read the result. The text message needs no answer.
- `TaskSendOptions` also sets a deadline (`good_until`), an idempotency key and the retention of the task record. See [Sending Tasks](/horsies/rust/tasks/sending-tasks/).

## Send Again After a Failed Send

A send that fails with `retryable() == true` can be sent again with the same task id. Use `resend(&send_error)` on the `TaskFunction` that `register` returns. The task is stored once, also when the first send did reach the database.

With `AppConfig.resend_on_transient_error`, the library sends again by itself. See [Sending Tasks](/horsies/rust/tasks/sending-tasks/#resend-on-transient-errors).

## Blocking Code

The sends and reads on this page are `async`. A program with no async runtime uses the blocking forms of `horsies::blocking`. See [Blocking API](/horsies/rust/tasks/blocking-api/).

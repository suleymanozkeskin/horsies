---
title: Send from an HTTP Route
summary: Send a task from an axum handler, answer 202 with the task id, map each send error to a status, read the task by its id, and see what resend_on_transient_error changes.
related: [../tasks/sending-tasks, ../tasks/errors, ../tasks/retrieving-results, ./async-frameworks]
tags: [rust, axum, send, idempotency, TaskSendError, resend_on_transient_error]
---

# Send from an HTTP Route

A handler sends a task and answers at once. The task runs later in a worker. The handler answers 202 with the task id. A second handler reads the task by that id.

A send returns a `Result`. The handler must handle the `Err` side.

This page uses the Acme Clothing tasks `authorize_payment` and `send_shipping_sms`. The files are `src/app.rs` (`build_app`), `src/domain.rs`, `src/tasks/payments.rs`, `src/tasks/notify.rs` and the new file `src/bin/api.rs`.

## Why and When

Read this page when a web handler starts work that must not block the request.

- The handler sends the task and does not wait for it.
- The caller gets the task id and asks for the outcome later.
- A failed send becomes an HTTP status. The handler never hides it.

For the split between the service binary and the worker binary, see [Async Frameworks](/horsies/rust/guides/async-frameworks/).

## Setup

```bash
cargo add axum
cargo add tokio --features rt-multi-thread,macros,net
cargo add serde --features derive
cargo add serde_json uuid tracing
```

`Horsies` is not `Clone`. The service builds the app one time and puts it in an `Arc` as the router state.

## How To

### Send and Answer 202

Create `src/bin/api.rs`:

<!-- rust-check: skip partial -- names the library crate of the example package -->
```rust
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use horsies::{Horsies, TaskSendError};
use serde::Deserialize;
use serde_json::json;

use shop::app::build_app;
use shop::tasks::payments::authorize_payment;

type SharedApp = Arc<Horsies>;

#[derive(Deserialize)]
struct AuthorizePaymentRequest {
    amount_cents: i32,
}

fn send_error_response(error: &TaskSendError) -> Response {
    let code = error.code().to_string();
    match error {
        TaskSendError::EnqueueFailed { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "error": code }))).into_response()
        }
        TaskSendError::IdempotencyKeyConflict { owner_task_id, .. } => (
            StatusCode::CONFLICT,
            Json(json!({ "error": code, "owner_task_id": owner_task_id.to_string() })),
        )
            .into_response(),
        TaskSendError::ValidationFailed { .. } => {
            (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({ "error": code }))).into_response()
        }
        TaskSendError::PayloadTooLarge { .. } => {
            (StatusCode::PAYLOAD_TOO_LARGE, Json(json!({ "error": code }))).into_response()
        }
        TaskSendError::Suppressed { .. }
        | TaskSendError::AsyncContext { .. }
        | TaskSendError::PayloadMismatch { .. } => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": code }))).into_response()
        }
    }
}

async fn authorize(
    State(app): State<SharedApp>,
    Path(order_id): Path<String>,
    Json(body): Json<AuthorizePaymentRequest>,
) -> Response {
    match authorize_payment::send(&app, order_id, body.amount_cents).await {
        Ok(handle) => (
            StatusCode::ACCEPTED,
            Json(json!({ "task_id": handle.task_id().to_string() })),
        )
            .into_response(),
        Err(error) => send_error_response(&error),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = Arc::new(build_app()?);
    let router = Router::new()
        .route("/orders/{order_id}/payment", post(authorize))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    axum::serve(listener, router).await?;
    Ok(())
}
```

```bash
cargo run --bin api
```

- `authorize_payment::send` takes the app, then the parameters of the task with their types. It returns `Ok(handle)` or `Err(TaskSendError)`. `handle.task_id()` is the id of the stored task.
- `TaskSendError` is a closed enum. The `match` in `send_error_response` is exhaustive. A new variant is a compile error here until it has an arm.
- The response holds the code of the error and no input value.
- `main` is the main of the service binary. It does not run the worker.

### What Each Send Error Means for the Queue

The status of a failed send tells the caller what to do. It also says what the queue holds.

| Send error | Status | Queue state | Caller action |
|------------|--------|-------------|---------------|
| `EnqueueFailed` | 503 | The row may exist or not. The database failed during the send | If `retryable()` is true, repeat the request with the same idempotency key. If it is false, a repeat does not clear the cause |
| `IdempotencyKeyConflict` | 409 | Nothing written | Send the same command, or use another key |
| `ValidationFailed` | 422 | Nothing written | Change the input |
| `PayloadTooLarge` | 413 | Nothing written | Send a smaller input |
| `Suppressed`, `AsyncContext`, `PayloadMismatch` | 500 | Nothing written | None. These are defects of the service or its setup |

A 503 does not mean that nothing is queued. The row may exist whatever the value of `retryable()`. A send whose commit reply was lost has a stored row. A repeat of the request without a key makes a second task. The next section removes that risk.

### Take the Idempotency Key from the Request

A client that repeats a request after a 503 sends the same `Idempotency-Key` header each time. The handler passes the header to the send:

<!-- rust-check: skip partial -- replaces the handler of the block above -->
```rust
use axum::http::HeaderMap;
use horsies::TaskSendOptions;

async fn authorize(
    State(app): State<SharedApp>,
    Path(order_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<AuthorizePaymentRequest>,
) -> Response {
    let key = match headers.get("idempotency-key").map(|value| value.to_str()) {
        Some(Ok(key)) => key.to_owned(),
        Some(Err(_not_text)) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "IDEMPOTENCY_KEY_NOT_TEXT" })),
            )
                .into_response();
        }
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "IDEMPOTENCY_KEY_MISSING" })),
            )
                .into_response();
        }
    };
    let options = TaskSendOptions::new().idempotency_key(key);
    match authorize_payment::with_options(options)
        .send(&app, order_id, body.amount_cents)
        .await
    {
        Ok(handle) => (
            StatusCode::ACCEPTED,
            Json(json!({ "task_id": handle.task_id().to_string() })),
        )
            .into_response(),
        Err(error) => send_error_response(&error),
    }
}
```

- A repeat with the same key and the same command returns `Ok(handle)` of the first task. The handler answers 202 with the same task id.
- A send with the same key and another command is `Err(TaskSendError::IdempotencyKeyConflict)`. The handler answers 409. `owner_task_id` names the task that holds the key.
- The conflict is not retryable. A repeat of the same call returns the same conflict while the other task holds the key. `resend` refuses it. The caller sends the same command, or uses another key.
- The reservation of a key ends `reservation_window` after its task finishes. A send with the key after that time starts a new task.
- A key must have 1 to 255 UTF-8 bytes. A key outside the size is `Err(TaskSendError::ValidationFailed)`.

### Send Without Reading the Result

Some sends have no result that the handler needs. The handler logs the failed send and answers as usual. The send of a shipping text is an example.

Add to `src/bin/api.rs`:

<!-- rust-check: skip partial -- adds a handler to the file of the blocks above -->
```rust
use shop::tasks::notify::send_shipping_sms;

#[derive(Deserialize)]
struct ShippedRequest {
    tracking_code: String,
}

async fn mark_shipped(
    State(app): State<SharedApp>,
    Path(order_id): Path<String>,
    Json(body): Json<ShippedRequest>,
) -> Json<serde_json::Value> {
    // Stand-in: the order row is updated here, in the shop database.
    match send_shipping_sms::send(&app, order_id.clone(), body.tracking_code).await {
        Ok(_handle) => {}
        Err(error) => {
            tracing::warn!(%order_id, code = %error.code(), "shipping sms not queued");
        }
    }
    Json(json!({ "status": "shipped" }))
}
```

Register the handler with `.route("/orders/{order_id}/shipped", post(mark_shipped))`.

- The `Ok` arm drops the handle. No code reads the task.
- The `Err` arm logs the order id and the code. The handler still answers 200, because the order is shipped.
- A logged `EnqueueFailed` can still have a stored task. The text can be sent. Do not use this form for a send that must happen exactly one time. Use a key for that send.

### Read the Task by Its Id

The status handler reads the task from the database. It does not need the handle of the send. Any process of the service can answer it.

Add to `src/bin/api.rs`:

<!-- rust-check: skip partial -- adds a handler to the file of the blocks above -->
```rust
use std::time::Duration;

use horsies::{ReadError, TaskOutcome, WaitLimit};
use uuid::Uuid;

use shop::domain::{AcmeCode, PaymentAuthorization};

async fn payment_status(State(app): State<SharedApp>, Path(task_id): Path<Uuid>) -> Response {
    let wait = WaitLimit::Within(Duration::ZERO);
    match app
        .get_result::<PaymentAuthorization, AcmeCode>(task_id, wait)
        .await
    {
        Ok(TaskOutcome::Succeeded(authorization)) => {
            Json(json!({ "state": "succeeded", "authorization": authorization })).into_response()
        }
        Ok(TaskOutcome::Failed(error)) => {
            Json(json!({ "state": "failed", "code": error.code().to_string() })).into_response()
        }
        Err(ReadError::WaitTimeout { .. }) => {
            Json(json!({ "state": "pending" })).into_response()
        }
        Err(ReadError::TaskNotFound { .. }) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "TASK_NOT_FOUND" })),
        )
            .into_response(),
        Err(unread) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": unread.to_string() })),
        )
            .into_response(),
    }
}
```

Register the handler with `.route("/payments/{task_id}", get(payment_status))`, and add `get` to the `axum::routing` import.

- `WaitLimit::Within(Duration::ZERO)` reads one time and does not wait. A task that has not ended gives `Err(ReadError::WaitTimeout)`. The handler answers `pending`.
- The three arms of a read are `Ok(TaskOutcome::Succeeded(value))`, `Ok(TaskOutcome::Failed(error))` and `Err(read_error)`. The last arm names the two read errors that this handler answers on its own.
- `Failed` holds the error that the task returned, or a code that the library recorded, for example `TASK_TIMEOUT`. `error.code()` is a `TaskErrorCode<AcmeCode>`.
- `ReadError::TaskNotFound` means that no stored task has this id. A task from a 503 send may not exist.
- The read takes the output type and the code type of the task. The types are not stored with the id.

### Resend Transient Errors in the Library

`resend_on_transient_error` in `AppConfig` is off by default. Turn it on to let the library send again before the handler sees an error:

<!-- rust-check: skip partial -- shows the one changed field of the app config -->
```rust
let config = AppConfig {
    resend_on_transient_error: true,
    ..AppConfig::from_database_url(DATABASE_URL)
};
```

The setting changes what the handler sees:

- Only `EnqueueFailed` with `retryable: true` is sent again. Every other error returns at once.
- The library sends at most 3 more times. The waits are 200 ms, 400 ms and 800 ms.
- Each resend uses the same task id and payload, and the same idempotency key. A first attempt that was stored answers with its task id. The task is not queued two times.
- The handler gets `Ok(handle)` after one of the attempts, or the `EnqueueFailed` of the last attempt. The handler still maps that error to 503.
- A send that must resend waits at least 1,400 ms in total, plus the time of each attempt. The wait does not block the runtime. Set the timeout of the HTTP client and of the proxy above this time.

## Things to Avoid

**Do not call the blocking views in a handler.** On a runtime thread they return `TaskSendError::AsyncContext` and write nothing. Use the async `send`.

**Do not answer 503 and tell the caller that nothing is queued.** Only `EnqueueFailed` leaves the queue state open. Use a key to make a repeat safe.

**Do not repeat a send after a key conflict.** The same call returns the same conflict.

**Do not wait for the task in the send handler.** A handler that waits holds the request for the time of the task. Answer 202 and read in the status handler.

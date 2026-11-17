---
title: Async Frameworks
summary: Send tasks and read results from a tokio web service such as axum, and keep the service apart from the worker's child mode.
related: [../tasks/blocking-api, ../tasks/sending-tasks, ../monitoring/web-ui-mount, ../cli]
tags: [rust, tokio, axum, async, ASYNC_CONTEXT, web service]
---

# Async Frameworks

A web service on tokio (axum, actix-web, a gRPC server) sends tasks and reads results with the async API. The app connects at its first operation and needs no start or stop step.

## Why and When

- `Horsies::new` does no I/O. Build the app once when the service starts, register the tasks, then serve.
- The async methods run on the service's own runtime. The blocking views refuse to run there with `ASYNC_CONTEXT`.
- A worker starts its binary again for each child, and every line of `main` before child mode runs in each child. Keep the service out of that path.

## Send from a Handler

```rust
use std::convert::Infallible;

use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;
use horsies::{task, AppConfig, Horsies, HorsiesError, TaskError, TaskSendError};

#[task("send_receipt")]
async fn send_receipt(order_id: String) -> Result<(), TaskError<Infallible>> {
    let _sent = order_id;
    Ok(())
}

fn build_app() -> Result<Horsies, HorsiesError> {
    let mut app = Horsies::new(AppConfig::for_database_url(
        "postgresql://app:secret@db.example.com:5432/horsies",
    ))?;
    let _receipt = send_receipt::register(&mut app)?;
    Ok(app)
}

async fn receipt_route(Path(order_id): Path<String>) -> Result<String, StatusCode> {
    match send_receipt::send(order_id).await {
        Ok(handle) => Ok(handle.task_id().to_string()),
        Err(TaskSendError::EnqueueFailed { retryable: true, .. }) => {
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
        Err(_refused) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _app = build_app()?;
    let router = Router::new().route("/orders/{order_id}/receipt", post(receipt_route));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    axum::serve(listener, router).await?;
    Ok(())
}
```

- `task_name::send` sends through the function of the first `task_name::register` in the process, so a handler needs no state. A send before `register` is `ValidationFailed`.
- A `TaskFunction` is `Clone`, `Send` and `Sync`: put it in the router state to send through a function that the handler gets as an argument.
- Each handler awaits the async form. A send, a read and a workflow start run their statements on the app's pool.
- The error of a send is a closed catalog: see [Sending Tasks](/horsies/rust/tasks/sending-tasks/#tasksenderror).

## Keep the Service Apart from the Worker

The worker starts its binary again for each child. Run the service from a binary or a command that is not the worker's:

| Binary | `main` |
|--------|--------|
| The worker binary | Builds the app and calls `horsies::cli::run(app)` from a plain `fn main`. See [CLI Reference](/horsies/rust/cli/#run-the-app) |
| The service binary | Builds the same app, with the same task registrations, then starts its runtime and serves. It never runs the `worker` command |

A binary that serves both calls `horsies::child_mode()` first and serves the child, then runs the service or the worker by its own arguments. See [A Binary That Owns Its `main`](/horsies/rust/cli/#a-binary-that-owns-its-main).

## Things to Avoid

**Do not call the blocking views in a handler.** On a thread where a tokio runtime is current they run nothing and return `ASYNC_CONTEXT`. See [Blocking API](/horsies/rust/tasks/blocking-api/#on-a-runtime-thread).

```rust
// Wrong: returns TaskSendError::AsyncContext and writes nothing
let handle = receipt.blocking().send(input);

// Correct
let handle = receipt.send(input).await;
```

**Do not call `horsies::cli::run` inside `#[tokio::main]`.** It blocks its thread, and inside a runtime it prints why and returns exit code 1.

**Do not start the service in `main` before child mode.** Each worker child runs `main` again and would start the service again.

**Do not wait for a task result in a task body.** A wait in a worker child is `WaitInsideTask`. Compose the tasks with a workflow: see [Typed Node Builder](/horsies/rust/concepts/workflows/typed-node-builder/#take-a-dependencys-result).

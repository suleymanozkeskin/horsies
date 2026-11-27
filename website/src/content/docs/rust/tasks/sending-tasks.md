---
title: Sending Tasks
summary: Send a task now or after a delay, with per-send options, and handle the closed catalog of TaskSendError.
related: [defining-tasks, retrieving-results, ../configuration/app-config, ../../tasks/errors]
tags: [rust, send, delay, TaskFunction, TaskSendError, idempotency, retention]
---

# Sending Tasks

`register` returns the `TaskFunction<A, T, E>` of a task: input `A`, output `T`, code type `E`. Its sends return `Result<TaskHandle<T, E>, TaskSendError>`. For a task of `#[horsies::task]`, `A` is the generated `Input` struct, one field per parameter (`send_receipt::Input { order_id }`, stored as `{"order_id": "..."}`). The generated `send_receipt::send(&app, ...)` takes the app and the parameters themselves.

## Send a Task

```rust
use horsies::{task, Horsies, TaskError, TaskSendError, TaskSendOptions, WholeSeconds};

#[task("send_receipt")]
async fn send_receipt(order_id: String) -> Result<(), TaskError<std::convert::Infallible>> {
    let _ = order_id;
    Ok(())
}

async fn send_receipts(app: &mut Horsies) -> Result<(), Box<dyn std::error::Error>> {
    let receipt = send_receipt::register(app)?;

    let now = receipt
        .send(send_receipt::Input { order_id: "order-1042".to_owned() })
        .await?;
    let later = receipt
        .send_with_options(
            TaskSendOptions::new().delay(WholeSeconds::from_secs(300)),
            send_receipt::Input { order_id: "order-1043".to_owned() },
        )
        .await?;

    // The generated module sends through the app that it names.
    let also_now: Result<_, TaskSendError> = send_receipt::send(app, "order-1044".to_owned()).await;

    println!("{} {} {}", now.task_id(), later.task_id(), also_now.is_ok());
    Ok(())
}
```

| Method | Sends |
|--------|-------|
| `send(input)` | Now |
| `send_with_options(options, input)` | With `TaskSendOptions`: now, or after the delay of `TaskSendOptions::delay` |
| `with_options(options)` | A `TaskFunctionSendOptions` with `send(input)` |

`task_name()`, `queue_name()`, `priority()` and `task_options()` read the task's registration.

The generated module of a task has two sends:

| Generated send | Sends |
|----------------|-------|
| `send_receipt::send(&app, order_id)` | Now, through `app` |
| `send_receipt::with_options(options).send(&app, order_id)` | With `TaskSendOptions`, through `app` |

The module holds no state. Each send finds the task's typed function in the app that it names, so two apps in one process share no registration. A send through an app that does not register the task returns `ValidationFailed` with the cause `PreconditionFailed`. Nothing is written.

## What a Send Does

A send runs these steps in order, and stops at the first refusal:

1. A generated send finds the task in the app that it names: an app that does not register the task refuses with `ValidationFailed`.
2. The suppression check: a suppressed send returns `Suppressed`.
3. The retention of the send.
4. The read-back of the input: encode it, decode the text with `A`, encode that value again. Two different texts refuse the send with `ValidationFailed` before any statement.
5. The payload size: an input over `payload.reject_bytes` is `PayloadTooLarge`.
6. A new task id and the sent instant.
7. One enqueue through the core.

In a worker child, a send goes through the child's own producer handle, whose pool the worker sizes. Anywhere else it goes through the app's handle.

## Send Options

```rust
use chrono::{Duration, Utc};
use horsies::{SendRetention, TaskSendOptions, WholeSeconds};

fn invoice_options(invoice_id: u64) -> TaskSendOptions {
    TaskSendOptions::new()
        .delay(WholeSeconds::from_secs(60))
        .good_until(Utc::now() + Duration::hours(1))
        .idempotency_key(format!("invoice-{invoice_id}"))
        .retention(SendRetention::Forever)
}
```

| Option | Does |
|--------|------|
| `delay(WholeSeconds)` | The task starts no earlier than the delay after the send. The stored row has `enqueue_delay_seconds`. Without it, the task starts when a worker claims it |
| `good_until(deadline)` | The task expires when no worker started it by `deadline` |
| `idempotency_key(key)` | Stores the task once per key within the app's reservation window (24 hours): a second send of the same command answers with the task that holds the key |
| `retention(SendRetention)` | The retention of the task's history record |
| `retain_forever()` | `retention(SendRetention::Forever)` |

`SendRetention`:

| Variant | History record kept |
|---------|---------------------|
| `FromQueueOrDefault` (default) | For the period that `RetentionConfig.queue_retention` maps the task's queue to, else 30 days |
| `Forever` | Forever |
| `Period(RetentionDays)` | For a period of whole days that this deployment registers: the 30-day default, a period of `queue_retention`, or one of `retention_periods`. Build it with `RetentionDays::parse_period(chrono::Duration::days(n))`. Another period is refused with the allowed periods |

The meaning of the reservation window and of retention is on [Retention Config](/horsies/configuration/retention-config/).

## TaskSendError

`TaskSendError` is the closed catalog of the send codes. Each variant has only the facts of its state.

| Variant | Code | Fields | Retry methods |
|---------|------|--------|---------------|
| `Suppressed` | `SEND_SUPPRESSED` | `message` | Refused |
| `AsyncContext` | `ASYNC_CONTEXT` | `task_id`, `payload`, `message` | Accepted |
| `ValidationFailed` | `VALIDATION_FAILED` | `message` | Refused |
| `EnqueueFailed` | `ENQUEUE_FAILED` | `task_id`, `payload`, `message`, `retryable` | Accepted |
| `PayloadMismatch` | `PAYLOAD_MISMATCH` | `task_id`, `payload`, `message` | Refused |
| `PayloadTooLarge` | `PAYLOAD_TOO_LARGE` | `message`, `size_bytes`, `limit_bytes` | Refused |
| `IdempotencyKeyConflict` | `IDEMPOTENCY_KEY_CONFLICT` | `task_id`, `payload`, `message`, `owner_task_id` | Refused |

- `ValidationFailed` with the cause `PreconditionFailed` is also the answer of a generated send through an app that does not register the task.
- `EnqueueFailed`: the row may or may not exist. `retryable` is the classification of the cause.
- `AsyncContext` is the error of a blocking send on a thread that runs a runtime. The async sends do not return it. See [Blocking API](/horsies/rust/tasks/blocking-api/#on-a-runtime-thread).
- `PayloadMismatch`: the task id exists with another command or idempotency key; nothing was written.
- `IdempotencyKeyConflict`: another task (`owner_task_id`) holds the key with another command; nothing was written. A repeat of the same send does not complete it: send the same command as the owner task, or use another key. A send with the key after the reservation ends starts a new task.
- A send in a process that was forked without `exec` after the core's process runtime started panics, as every call that runs on the core: see [Process-State Refusals](/horsies/tasks/error-types/#process-state-refusals).

The meaning of each code is on the [Errors Reference](/horsies/tasks/errors/#send-error-codes).

### Retry a Failed Send

<!-- rust-check: alone -->
```rust
use horsies::{task, TaskError, TaskFunction, TaskHandle, TaskSendError};

#[task("send_receipt")]
async fn send_receipt(order_id: String) -> Result<(), TaskError<std::convert::Infallible>> {
    let _ = order_id;
    Ok(())
}

async fn send_with_one_retry(
    receipt: &TaskFunction<send_receipt::Input, (), std::convert::Infallible>,
    order_id: String,
) -> Result<TaskHandle<(), std::convert::Infallible>, TaskSendError> {
    match receipt.send(send_receipt::Input { order_id }).await {
        Err(error @ TaskSendError::EnqueueFailed { retryable: true, .. }) => receipt.retry_send(&error).await,
        other => other,
    }
}
```

`retry_send(&error)` sends again the task of a failed send: the same task id and the same payload, as a repeat. The payload of a send with a delay keeps its delay. A task that an earlier call wrote answers with its task id, also after it finished, and does not run again. It refuses with `ValidationFailed`:

- an error that carries no payload (only `AsyncContext` and `EnqueueFailed` are sent again; `IdempotencyKeyConflict` is refused);
- the error of another task.

### Resend on Transient Errors

With `AppConfig.resend_on_transient_err`, a send that fails with a retryable `EnqueueFailed` is sent again with the same task id and payload, as a repeat. The number of resends, the waits and the answer of a repeat are on [Atomic Enqueue and Topology](/horsies/operations/atomic-enqueue-and-topology/#resend-transient-failures).

## Payload Size

`AppConfig.payload` is a `PayloadPolicy`:

| Field | Type | Does |
|-------|------|------|
| `warn_bytes` | `Option<u64>` | Logs a warning when a serialized payload exceeds this many bytes, once per task name and payload kind per process; `None` turns it off |
| `reject_bytes` | `Option<u64>` | Refuses a send whose serialized input exceeds this many bytes with `PayloadTooLarge`, before any row is written; `None` (default) turns it off. Results are never refused |

The size is counted in bytes of the UTF-8 text.

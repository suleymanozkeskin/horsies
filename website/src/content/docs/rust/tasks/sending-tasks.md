---
title: Sending Tasks
summary: Send a task now or after a delay, with per-send options, and handle the closed catalog of TaskSendError.
related: [defining-tasks, retrieving-results, ../configuration/app-config, ../../tasks/errors]
tags: [rust, send, schedule, TaskFunction, TaskSendError, idempotency, retention]
---

# Sending Tasks

`register` returns the `TaskFunction<A, T, E>` of a task: input `A`, output `T`, code type `E`. Its sends return `Result<TaskHandle<T, E>, TaskSendError>`.

## Send a Task

```rust
use horsies::{task, Horsies, TaskError, TaskSendError, WholeSeconds};

#[task("send_receipt")]
async fn send_receipt(order_id: String) -> Result<(), TaskError<std::convert::Infallible>> {
    let _ = order_id;
    Ok(())
}

async fn send_receipts(app: &mut Horsies) -> Result<(), Box<dyn std::error::Error>> {
    let receipt = send_receipt::register(app)?;

    let now = receipt.send("order-1042".to_owned()).await?;
    let later = receipt.schedule(WholeSeconds::from_secs(300), "order-1043".to_owned()).await?;

    // The generated module sends through the function of the first register.
    let also_now: Result<_, TaskSendError> = send_receipt::send("order-1044".to_owned()).await;

    println!("{} {} {}", now.task_id(), later.task_id(), also_now.is_ok());
    Ok(())
}
```

| Method | Sends |
|--------|-------|
| `send(input)` | Now |
| `schedule(delay, input)` | To start after `delay: WholeSeconds` |
| `send_with_options(options, input)` | Now, with `TaskSendOptions` |
| `schedule_with_options(options, delay, input)` | After `delay`, with `TaskSendOptions` |
| `with_options(options)` | A `TaskFunctionSendOptions` with `send(input)` and `schedule(delay, input)` |

`task_name()`, `queue_name()`, `priority()` and `task_options()` read the task's registration.

## What a Send Does

A send runs these steps in order, and stops at the first refusal:

1. The suppression check: a suppressed send returns `SendSuppressed`.
2. The retention of the send.
3. The read-back of the input: encode it, decode the text with `A`, encode that value again. Two different texts refuse the send with `ValidationFailed` before any statement.
4. The payload size: an input over `payload.reject_bytes` is `PayloadTooLarge`.
5. A new task id and the sent instant.
6. One enqueue through the core.

In a worker child, a send goes through the child's own producer handle, whose pool the worker sizes. Anywhere else it goes through the app's handle.

## Send Options

```rust
use chrono::{Duration, Utc};
use horsies::{SendRetention, TaskSendOptions};

fn invoice_options(invoice_id: u64) -> TaskSendOptions {
    TaskSendOptions::new()
        .good_until(Utc::now() + Duration::hours(1))
        .idempotency_key(format!("invoice-{invoice_id}"))
        .retention(SendRetention::Forever)
}
```

| Option | Does |
|--------|------|
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
| `SendSuppressed` | `SEND_SUPPRESSED` | `message` | Refused |
| `AsyncContext` | `ASYNC_CONTEXT` | `task_id`, `payload`, `message` | Accepted |
| `ValidationFailed` | `VALIDATION_FAILED` | `message` | Refused |
| `EnqueueFailed` | `ENQUEUE_FAILED` | `task_id`, `payload`, `message`, `retryable` | Accepted |
| `PayloadMismatch` | `PAYLOAD_MISMATCH` | `task_id`, `payload`, `message` | Refused |
| `PayloadTooLarge` | `PAYLOAD_TOO_LARGE` | `message`, `size_bytes`, `limit_bytes` | Refused |
| `IdempotencyKeyConflict` | `IDEMPOTENCY_KEY_CONFLICT` | `task_id`, `payload`, `message`, `owner_task_id` | Accepted |

- `EnqueueFailed`: the row may or may not exist. `retryable` is the classification of the cause.
- `AsyncContext` is the error of a blocking send on a thread that runs a runtime. The async sends do not return it.
- `PayloadMismatch`: the task id exists with another command or idempotency key; nothing was written.
- `IdempotencyKeyConflict`: another task (`owner_task_id`) holds the key with another command; nothing was written.

The meaning of each code is on the [Errors Reference](/horsies/tasks/errors/#send-error-codes).

### Retry a Failed Send

```rust
use horsies::{TaskFunction, TaskHandle, TaskSendError};

async fn send_with_one_retry(
    receipt: &TaskFunction<String, (), std::convert::Infallible>,
    order_id: String,
) -> Result<TaskHandle<(), std::convert::Infallible>, TaskSendError> {
    match receipt.send(order_id).await {
        Err(error @ TaskSendError::EnqueueFailed { retryable: true, .. }) => receipt.retry_send(&error).await,
        other => other,
    }
}
```

`retry_send(&error)` and `retry_schedule(&error)` send again the task of a failed `send` or `schedule`: the same task id and the same payload, as a repeat. A task that an earlier call wrote answers with its task id, also after it finished, and does not run again. Both refuse with `ValidationFailed`:

- an error that carries no payload (only `AsyncContext`, `EnqueueFailed` and `IdempotencyKeyConflict` carry one);
- the error of another task;
- the error of the other method (a `schedule` error to `retry_send`, a `send` error to `retry_schedule`).

### Resend on Transient Errors

With `AppConfig.resend_on_transient_err`, a send that fails with a retryable `EnqueueFailed` is sent again with the same task id and payload, as a repeat. The number of resends, the waits and the answer of a repeat are on [Atomic Enqueue and Topology](/horsies/operations/atomic-enqueue-and-topology/#resend-transient-failures).

## Payload Size

`AppConfig.payload` is a `PayloadPolicy`:

| Field | Type | Does |
|-------|------|------|
| `warn_bytes` | `Option<u64>` | Logs a warning when a serialized payload exceeds this many bytes, once per task name and payload kind per process; `None` turns it off |
| `reject_bytes` | `Option<u64>` | Refuses a send whose serialized input exceeds this many bytes with `PayloadTooLarge`, before any row is written; `None` (default) turns it off. Results are never refused |

The size is counted in bytes of the UTF-8 text.

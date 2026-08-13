---
title: Sending Tasks
summary: How to enqueue tasks for background execution.
related: [defining-tasks, retrieving-results]
tags: [tasks, send, async, scheduling]
---

# Sending Tasks

Enqueue tasks with `::send()` or `::schedule()`. Both return `TaskSendResult<TaskHandle<T>>` — either `Ok(TaskHandle)` on success or `Err(TaskSendError)` on failure.

## How To

### Send a Task

```rust
match my_task::send(input).await {
    Ok(handle) => println!("Task submitted: {}", handle.task_id()),
    Err(send_err) => println!("Send failed: {} - {}", send_err.code, send_err.message),
}
```

### Delay Execution

```rust
use std::time::Duration;

match my_task::schedule(Duration::from_secs(60), input).await {
    Ok(handle) => println!("Scheduled: {}", handle.task_id()),
    Err(err) => println!("Schedule failed: {}", err.code),
}
```

### Set a Deadline

Use `TaskSendOptions` when the deadline is dynamic for this particular send:

```rust
use chrono::{Duration, Utc};
use horsies::TaskSendOptions;

let deadline = Utc::now() + Duration::minutes(5);

let handle = my_task::with_options(
    TaskSendOptions::new().good_until(deadline),
)
.send(input)
.await?;
```

For scheduled sends:

```rust
let handle = my_task::with_options(
    TaskSendOptions::new().good_until(deadline),
)
.schedule(std::time::Duration::from_secs(60), input)
.await?;
```

`good_until` is an absolute UTC deadline. It is not relative to the schedule delay.
If the scheduled task has not started by that instant, it expires instead of running.

For workflow tasks, prefer setting the deadline on the node when building the workflow spec:

```rust
let node = my_task::node()?
    .set_input(input)?
    .good_until(deadline);
```

### Choose task-history retention

Every send gets a retention class. The precedence is:

1. An explicit send choice.
2. The queue's `queue_retention` mapping.
3. `standard_30d`.

Use `retention_class(...)` for a finite class:

```rust
use horsies::TaskSendOptions;

let handle = audit_task::with_options(
    TaskSendOptions::new().retention_class("audit_1y"),
)
.send(input)
.await?;
```

Use `retain_forever()` for the explicit forever choice:

```rust
let handle = audit_task::with_options(
    TaskSendOptions::new().retain_forever(),
)
.send(input)
.await?;
```

The payload stores the explicit forever choice as a missing finite
`retention_class_key`. The database resolves it to the `forever` class. Do not
pass the string `"forever"` to `retention_class(...)`.

An unknown class returns `ValidationFailed`. No task row is written. The class
is fixed at enqueue. Retry methods replay the same class.

See [Retention Config](../../configuration/retention-config).

### Use an idempotency key

An idempotency key deduplicates enqueue commands for one task name.

```rust
let handle = charge_card::with_options(
    TaskSendOptions::new().idempotency_key("checkout-4182"),
)
.send(input)
.await?;
```

The key is scoped by task name. The key must be non-empty. It can use at most
255 UTF-8 bytes.

The default reservation window is 24 hours. A matching command replays the
existing task ID. A different command with the same scoped key returns a key
conflict. The command fingerprint includes task name, queue, payload, options,
retention class, deadline, and rerun lineage.

Delayed sends carry the same key. `retry_send` and `retry_schedule` also carry
it. Do not change the stored error payload before a retry.

### Wait for Result

```rust
use std::time::Duration;

match my_task::send(input).await {
    Ok(handle) => {
        // Wait with timeout
        let result = handle.get(Some(Duration::from_secs(5))).await;

        // Wait indefinitely
        let result = handle.get(None).await;
    }
    Err(err) => println!("Send failed: {}", err.code),
}
```

### Fire and Forget

```rust
// Send without waiting for result — drop the handle
let _ = my_task::send(input).await;
```

### Pass Complex Arguments

Arguments must implement `Serialize` and `Deserialize`:

```rust
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct OrderInput {
    id: i64,
    items: Vec<String>,
    metadata: HashMap<String, serde_json::Value>,
}

match process_order::send(OrderInput {
    id: 123,
    items: vec!["a".into(), "b".into()],
    metadata: HashMap::new(),
}).await {
    Ok(handle) => { /* ... */ }
    Err(err) => println!("Send failed: {}", err.code),
}
```

## Retrying Failed Sends

When `send()` fails with `EnqueueFailed` (a transient broker error), use the retry methods to replay the exact same payload. The `enqueue_sha` on the stored `TaskSendPayload` guarantees the retry carries the identical serialized payload.

```rust
match my_task::send(input).await {
    Ok(handle) => { /* ... */ }
    Err(send_err) if send_err.retryable => {
        let task_fn = my_task::handle(&rt)?;
        match task_fn.retry_send(&send_err).await {
            Ok(handle) => println!("Retry succeeded: {}", handle.task_id()),
            Err(retry_err) => println!("Retry failed: {}", retry_err.code),
        }
    }
    Err(send_err) => {
        println!("Permanent failure: {}", send_err.code);
    }
}
```

### Automatic Retry via Config

Set `resend_on_transient_err = true` in `AppConfig` to have the library automatically retry transient enqueue failures (up to 3 times with exponential backoff):

```rust
let config = AppConfig {
    resend_on_transient_err: true,
    ..AppConfig::for_database_url("postgresql://...")
};
```

## API Reference

### `task_name::send(args) -> TaskSendResult<TaskHandle<T>>`

Enqueue task for immediate execution.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `args` | `A: Serialize` | Task arguments |

### `task_name::schedule(delay, args) -> TaskSendResult<TaskHandle<T>>`

Enqueue task for delayed execution.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `delay` | `Duration` | Time to wait before task becomes claimable |
| `args` | `A: Serialize` | Task arguments |

### `TaskFunction<A, T>`

| Method | Returns | Description |
| ------ | ------- | ----------- |
| `.send(args)` | `TaskSendResult<TaskHandle<T>>` | Enqueue immediately |
| `.schedule(delay, args)` | `TaskSendResult<TaskHandle<T>>` | Enqueue with delay |
| `.with_options(options)` | `TaskFunctionSendOptions<'_, A, T>` | Bind per-send options |
| `.retry_send(&err)` | `TaskSendResult<TaskHandle<T>>` | Retry failed send |
| `.retry_schedule(&err)` | `TaskSendResult<TaskHandle<T>>` | Retry failed schedule |
| `.task_name()` | `&str` | Task name |
| `.queue_name()` | `&str` | Assigned queue |
| `.priority()` | `u32` | Effective priority |

### `TaskSendOptions`

```rust
TaskSendOptions::new()
    .good_until(deadline)
    .idempotency_key("checkout-4182")
    .retention_class("audit_1y")
```

`good_until` is the last valid time for the task to begin execution. A task whose deadline passes while still `Pending` or `Claimed` transitions to `Expired` without running user code.

| Method | Meaning |
|---|---|
| `.good_until(deadline)` | Set the last valid start time |
| `.idempotency_key(key)` | Set a task-name-scoped enqueue key |
| `.retention_class(key)` | Select a configured finite class |
| `.retain_forever()` | Keep the terminal record forever |

Omitting retention uses the queue mapping. It uses `standard_30d` when the
queue has no mapping.

### `TaskSendResult<T>`

Type alias: `Result<T, TaskSendError>`.

### `TaskSendError`

| Field | Type | Description |
| ----- | ---- | ----------- |
| `code` | `TaskSendErrorCode` | Failure category |
| `message` | `String` | Human-readable description |
| `retryable` | `bool` | Whether the caller can retry with the same payload |
| `task_id` | `Option<Uuid>` | Generated task ID |
| `payload` | `Option<TaskSendPayload>` | Serialized envelope for replay |

### `TaskSendErrorCode`

| Code | Description | Retryable |
| ---- | ----------- | --------- |
| `SendSuppressed` | Send suppressed during check phase | No |
| `ValidationFailed` | Argument serialization or validation failed | No |
| `EnqueueFailed` | Broker/database failure during enqueue | Yes |
| `PayloadMismatch` | Retry payload SHA does not match | No |
| `PayloadTooLarge` | Serialized input exceeded the configured limit | No |

### `TaskHandle<T>`

| Method | Returns | Description |
| ------ | ------- | ----------- |
| `.task_id()` | `Uuid` | Unique task identifier |
| `.get(timeout)` | `TaskResult<T>` | Wait for result |
| `.info(include_result, include_failed_reason, include_attempts)` | `BrokerResult<Option<TaskInfo>>` | Fetch task metadata |

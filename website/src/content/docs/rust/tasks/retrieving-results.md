---
title: Retrieving Results
summary: Read a task's outcome as Result<TaskOutcome<T, E>, ReadError>, read its info, and read by task id on the app.
related: [sending-tasks, errors, ../../concepts/task-lifecycle]
tags: [rust, result, TaskHandle, TaskOutcome, ReadError, WaitLimit, TaskInfo]
---

# Retrieving Results

A read of a task's result returns `Result<TaskOutcome<T, E>, ReadError>`. The outer error is the read failing: the wait limit passed, the task is not held, the stored data does not read, the database failed. The inner value is what the task ended with.

## Read the Outcome

```rust
use std::time::Duration;

use horsies::{ReadError, TaskHandle, TaskOutcome, WaitLimit};

async fn print_outcome(handle: &TaskHandle<String, std::convert::Infallible>) -> Result<(), ReadError> {
    match handle.get(WaitLimit::Within(Duration::from_secs(30))).await? {
        TaskOutcome::Succeeded(order_id) => println!("validated {order_id}"),
        TaskOutcome::Failed(error) => println!("failed with {}", error.code().as_str()),
    }
    Ok(())
}
```

`TaskHandle::get(wait)` waits while the task has not finished, up to `wait`:

| `WaitLimit` | Wait |
|-------------|------|
| `WaitLimit::NoLimit` | Until the task ends |
| `WaitLimit::Within(duration)` | Up to `duration`. A zero `duration` reads once and does not wait |

The read checks the limit first. A `Within` limit above `MAX_WAIT_LIMIT_MS` (4,294,967,295 ms, about 49.7 days) is `ReadError::InvalidInput`. This check comes before a kept read, before `WaitInsideTask` and before `AsyncContext`. No statement runs, and the handle keeps nothing.

`TaskOutcome<T, E>`:

| Variant | The task |
|---------|----------|
| `Succeeded(T)` | Returned this value |
| `Failed(TaskError<E>)` | Returned this error, or the library recorded it: a built-in code such as `TASK_CANCELLED`, `TASK_EXPIRED`, `TASK_TIMEOUT` or `WORKER_CRASHED` |

## ReadError

| Variant | Code | When | A later read |
|---------|------|------|--------------|
| `WaitTimeout { task_id, status }` | `WAIT_TIMEOUT` | The task had not finished when the limit passed | May find it finished |
| `TaskNotFound { task_id, reason }` | `TASK_NOT_FOUND` | Neither the live table nor the history holds the task. `reason` is a `TaskAbsenceReason`; see below. A handle built by id can race the commit of its send | May find it |
| `ResultNotAvailable { task_id, status }` | `RESULT_NOT_AVAILABLE` | The task reached a terminal status and stored no result | Kept: returns the same |
| `ResultDeserializationError { task_id, cause }` | `RESULT_DESERIALIZATION_ERROR` | The stored result does not read as the result of this task's types; `cause` names no value | Kept: returns the same |
| `UnknownCustomCode { task_id, stored_code, code_type }` | `UNKNOWN_CUSTOM_CODE` | The stored error has a custom code that `E` does not name; `stored_code` is the stored text | Kept: returns the same |
| `InvalidInput { task_id, cause }` | `INVALID_INPUT` | The `Within` limit is above `MAX_WAIT_LIMIT_MS`. `cause` is `WaitLimitRefused`. Nothing was read | Refuses the same limit again. A limit within the bound reads |
| `WaitInsideTask { message }` | `WAIT_INSIDE_TASK` | A result wait in a process that runs task bodies. Nothing was read. Compose the tasks with a workflow | – |
| `AsyncContext { message }` | `ASYNC_CONTEXT` | A blocking read on a thread where a tokio runtime is current. Nothing was read. See [Blocking API](/horsies/rust/tasks/blocking-api/#on-a-runtime-thread) | – |
| `DatabaseError { database_code, message, retryable }` | `DATABASE_ERROR` | The database or the connection failed; `database_code` is the code of the failed operation, and `retryable` is the classification of the cause | May succeed |

`ReadError::code()` returns the built-in code of the failure.

`TaskAbsenceReason` tells why the task is held nowhere:

| Variant | Meaning |
|---------|---------|
| `PredatesRetainedHistory` | The task id's UUIDv7 time is before the end of a history leaf that retention removed: retention can have removed the task |
| `WithinRetainedHistory` | No removed leaf can have held the task: it did not exist, or its leaf was removed out of band |
| `NotClassified` | The id is not a UUIDv7, so it carries no time |

The handle keeps its first final read: an outcome, `ResultNotAvailable`, `ResultDeserializationError` and `UnknownCustomCode`. A later `get` returns it with no read. Clones of a handle share one task and one kept read.

## Task Info

```rust
use horsies::{ReadError, TaskHandle, TaskInfoParts};

async fn print_attempts(handle: &TaskHandle<String, std::convert::Infallible>) -> Result<(), ReadError> {
    let parts = TaskInfoParts { result: false, failed_reason: true, attempts: true };
    let info = handle.info(parts).await?;
    println!("{info:?}");
    Ok(())
}
```

`info(parts)` returns the task's `TaskInfo`: its status and times, plus the parts asked for in `TaskInfoParts`:

| Field | Reads |
|-------|-------|
| `result` | The stored result of a terminal task, as `TaskResult<JsonText>`: the value as its stored text (`decode::<T>()` reads it with the task's type), or the stored error |
| `failed_reason` | The failed reason of a failed task |
| `attempts` | Every attempt of the task, newest first |

The info is not kept: a status changes. Its errors are `TaskNotFound` and `DatabaseError`.

## Reads by Task Id

The app reads a task that another process sent:

| Method | Returns |
|--------|---------|
| `app.task_handle::<T, E>(task_id)` | A `TaskHandle<T, E>` of the task. Starts nothing |
| `app.get_result::<T, E>(task_id, wait)` | `Result<TaskOutcome<T, E>, ReadError>`, as `TaskHandle::get`. Nothing is kept between calls |
| `app.get_raw_result(task_id, wait)` | `Result<StoredResult, ReadError>`: the stored result, read with no types |
| `app.get_task_info(task_id, parts)` | `Result<TaskInfo, ReadError>`: the live row or the history record, attempts newest first |

`StoredResult` has `task_id`, `task_name`, `status` (the terminal status) and `envelope_json`: the stored result envelope as JSON text, `{"__h_task_result__": true, "ok": <value>, "err": <error>}`. It is for tools that read results without the task's types. `get_raw_result` returns `InvalidInput` and `WaitInsideTask` (both before any statement), `TaskNotFound`, `WaitTimeout`, `ResultNotAvailable` and `DatabaseError`. `get_result` and `get_raw_result` check the limit first, as `TaskHandle::get` does.

In a worker child a read goes through the child's producer handle, as a send does.

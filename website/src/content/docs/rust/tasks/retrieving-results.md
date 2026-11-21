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
    let limit = WaitLimit::parse(Some(Duration::from_secs(30))).expect("30 s is within the limit");
    match handle.get(limit).await? {
        TaskOutcome::Succeeded(order_id) => println!("validated {order_id}"),
        TaskOutcome::Failed(error) => println!("failed with {}", error.code().text()),
    }
    Ok(())
}
```

`TaskHandle::get(limit)` waits while the task has not finished, up to `limit`:

| `WaitLimit` | Wait |
|-------------|------|
| `WaitLimit::NoLimit` | Until the task ends |
| `WaitLimit::parse(Some(duration))` | Up to `duration`. A limit above `MAX_WAIT_LIMIT_MS` (4,294,967,295 ms) is refused with `WaitLimitRefused` before any read |
| `WaitLimit::parse(None)` | `NoLimit` |

`TaskOutcome<T, E>`:

| Variant | The task |
|---------|----------|
| `Succeeded(T)` | Returned this value |
| `Failed(TaskError<E>)` | Returned this error, or the library recorded it: a library code such as `TASK_CANCELLED`, `TASK_EXPIRED`, `TASK_TIMEOUT` or `WORKER_CRASHED` |

## ReadError

| Variant | Code | When | A later read |
|---------|------|------|--------------|
| `WaitTimedOut { task_id, status }` | `WAIT_TIMEOUT` | The task had not finished when the limit passed | May find it finished |
| `TaskNotFound { task_id, reason }` | `TASK_NOT_FOUND` | Neither the live table nor the history holds the task. `reason` is a `TaskAbsenceReason`; see below. A handle built by id can race the commit of its send | May find it |
| `ResultNotAvailable { task_id, status }` | `RESULT_NOT_AVAILABLE` | The task reached a terminal status and stored no result | Kept: returns the same |
| `StoredResultUnreadable { task_id, cause }` | `RESULT_DESERIALIZATION_ERROR` | The stored result does not read as the result of this task's types; `cause` names no value | Kept: returns the same |
| `UnknownUserCode { task_id, code, code_type }` | `RESULT_DESERIALIZATION_ERROR` | The stored error has a user code that `E` does not name | Kept: returns the same |
| `WaitInsideTask { message }` | `WAIT_INSIDE_TASK` | A result wait in a process that runs task bodies. Nothing was read. Compose the tasks with a workflow | – |
| `AsyncContext { message }` | `ASYNC_CONTEXT` | A blocking read on a thread where a tokio runtime is current. Nothing was read. See [Blocking API](/horsies/rust/tasks/blocking-api/#on-a-runtime-thread) | – |
| `Database { code, message, retryable }` | `BROKER_ERROR` | The database or the connection failed; `retryable` is the classification of the cause | May succeed |

`ReadError::code()` returns the library code of the failure.

`TaskAbsenceReason` tells why the task is held nowhere:

| Variant | Meaning |
|---------|---------|
| `PredatesRetainedHistory` | The task id's UUIDv7 time is before the end of a history leaf that retention removed: retention can have removed the task |
| `WithinRetainedHistory` | No removed leaf can have held the task: it did not exist, or its leaf was removed out of band |
| `NotClassified` | The id is not a UUIDv7, so it carries no time |

The handle keeps its first final read: an outcome, `ResultNotAvailable`, `StoredResultUnreadable` and `UnknownUserCode`. A later `get` returns it with no read. Clones of a handle share one task and one kept read.

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

The info is not kept: a status changes. Its errors are `TaskNotFound` and `Database`.

## Reads by Task Id

The app reads a task that another process sent:

| Method | Returns |
|--------|---------|
| `app.task_handle_of::<T, E>(task_id)` | A `TaskHandle<T, E>` of the task. Starts nothing |
| `app.get_result::<T, E>(task_id, limit)` | `Result<TaskOutcome<T, E>, ReadError>`, as `TaskHandle::get`. Nothing is kept between calls |
| `app.get_raw_result(task_id, limit)` | `Result<StoredResult, ReadError>`: the stored result, read with no types |
| `app.get_task_info(task_id, parts)` | `Result<TaskInfo, ReadError>`: the live row or the history record, attempts newest first |

`StoredResult` has `task_id`, `task_name`, `status` (the terminal status) and `envelope_json`: the stored result envelope as JSON text, `{"__h_task_result__": true, "ok": <value>, "err": <error>}`. It is for tools that read results without the task's types. `get_raw_result` returns `WaitInsideTask` (before any statement), `TaskNotFound`, `WaitTimedOut`, `ResultNotAvailable` and `Database`.

In a worker child a read goes through the child's producer handle, as a send does.

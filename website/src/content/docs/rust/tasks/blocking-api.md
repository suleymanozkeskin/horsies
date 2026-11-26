---
title: Blocking API
summary: Send tasks, read results, start and control workflows, and run the health reads and the live check from a program with no tokio runtime, through the blocking views of horsies::blocking.
related: [sending-tasks, retrieving-results, ../concepts/workflows/workflow-api, ../monitoring/health-reads, ../guides/async-frameworks]
tags: [rust, blocking, TaskFunctionView, TaskHandleView, HorsiesView, WorkflowFunctionView, WorkflowTemplateView, WorkflowHandleView, ASYNC_CONTEXT]
---

# Blocking API

A program with no tokio runtime calls the blocking views of `horsies::blocking`. A view method has the name, the arguments and the result type of the async method. It runs the async method on the process runtime of the core, which drives the app's connections, and blocks the calling thread until the method ends.

## Send and Read

```rust
use std::convert::Infallible;

use horsies::{task, AppConfig, Horsies, TaskError, TaskOutcome, WaitLimit};

#[task("add_numbers")]
async fn add_numbers(a: i64, b: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(a + b)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = Horsies::new(AppConfig::for_database_url(
        "postgresql://app:secret@db.example.com:5432/horsies",
    ))?;
    let add = add_numbers::register(&mut app)?;

    let handle = add.blocking().send(add_numbers::Input { a: 2, b: 3 })?;
    match handle.blocking().get(WaitLimit::NoLimit)? {
        TaskOutcome::Succeeded(sum) => println!("sum = {sum}"),
        TaskOutcome::Failed(error) => println!("failed with {}", error.code().text()),
    }
    Ok(())
}
```

## Start a Workflow and Read Its Result

With `DailyReport` and `register_daily_report` of [Workflow API](/horsies/rust/concepts/workflows/workflow-api/#define-and-start-a-workflow):

<!-- rust-check: skip partial -- register_daily_report is defined on the Workflow API page -->
```rust
use horsies::{AppConfig, Horsies, TaskOutcome, WaitLimit};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = Horsies::new(AppConfig::for_database_url(
        "postgresql://app:secret@db.example.com:5432/horsies",
    ))?;
    let daily_report = register_daily_report(&mut app)?;

    let handle = daily_report.blocking().start()?;
    match handle.blocking().get(WaitLimit::NoLimit)? {
        TaskOutcome::Succeeded(length) => println!("published {length} bytes"),
        TaskOutcome::Failed(error) => println!("the workflow ended with {}", error.code().text()),
    }
    let status = handle.blocking().status()?;
    println!("status: {status:?}");
    Ok(())
}
```

## Views

| View | From | Methods |
|------|------|---------|
| `TaskFunctionView` | `task_function.blocking()` | `send`, `send_with_options`, `with_options`, `retry_send` |
| `TaskFunctionSendOptionsView` | `task_function.blocking().with_options(options)` | `send` |
| `TaskHandleView` | `handle.blocking()` | `get`, `info` |
| `HorsiesView` | `app.blocking()` | `get_result`, `get_raw_result`, `get_task_info`, `ping_database`, `ping_workers`, `list_worker_states`, `get_worker_state`, `get_worker_state_history`, `get_class_coverage`, `get_class_report`, `check_live`, `start`, `start_with_id`, `start_workflow`, `start_workflow_with` |
| `WorkflowFunctionView` | `workflow_function.blocking()` | `start`, `start_with_id`, `retry_start` |
| `WorkflowTemplateView` | `workflow_template.blocking()` | `start`, `start_with_id` |
| `WorkflowHandleView` | `workflow_handle.blocking()` | `get`, `result_for`, `status`, `results`, `tasks`, `cancel`, `pause`, `resume` |

The starts of a definition by its type are methods of the app view: `app.blocking().start_workflow::<D>()` and `app.blocking().start_workflow_with::<D>(params)`.

The sends, their options and `TaskSendError` are on [Sending Tasks](/horsies/rust/tasks/sending-tasks/); the reads and `ReadError` on [Retrieving Results](/horsies/rust/tasks/retrieving-results/); the workflow starts, `WorkflowStartError`, the handle and its errors on [Workflow API](/horsies/rust/concepts/workflows/workflow-api/); the health reads on [Health Reads](/horsies/rust/monitoring/health-reads/).

The app has no blocking form of `get_stale_tasks` and `get_expired_tasks`, and of the `horsies::monitoring` functions.

## On a Runtime Thread

A blocking call on a thread where a tokio runtime is current (`tokio::runtime::Handle::try_current()` succeeds) would stall that runtime. It runs nothing and returns the `ASYNC_CONTEXT` member of its catalog:

| Call | Error |
|------|-------|
| A send or a retry | `TaskSendError::AsyncContext` with the prepared `task_id` and `payload`, and nothing written |
| `get`, `info`, `get_result`, `get_raw_result`, `get_task_info` | `ReadError::AsyncContext { message }` |
| A health read | `BrokerOperationError::AsyncContext`, not retryable |
| `check_live` | `CheckReport` with `HRS-217` (`CheckLiveAsyncContext`), after the static check passed |
| A workflow start or `retry_start` | `WorkflowStartError::AsyncContext` with the `workflow_id` of the start, not retryable, and nothing written |
| `status`, `results`, `tasks`, `cancel`, `pause`, `resume` | `HandleOperationError::AsyncContext`, not retryable |
| `WorkflowHandleView::get`, `result_for` | `WorkflowReadError::AsyncContext { message }` |

`retry_send(&error).await` with a `TaskSendError::AsyncContext` completes the same send from async code, with the same task id and payload. See [Retry a Failed Send](/horsies/rust/tasks/sending-tasks/#retry-a-failed-send).

`retry_start(&error).await` with a `WorkflowStartError::AsyncContext` error completes the same start from async code, with the error's `workflow_id`.

## Order of the Checks

- A refused input is refused before the thread check: a suppressed send is `SendSuppressed`, and an input that does not read back is `ValidationFailed`, on any thread.
- A read that takes a `WaitLimit` checks the limit first, on any thread: a `Within` limit above `MAX_WAIT_LIMIT_MS` (4,294,967,295 ms) is `ReadError::InvalidInput` (`get`, `get_result`, `get_raw_result`) or `WorkflowReadError::InvalidInput` (`WorkflowHandleView::get`). Nothing runs.
- `TaskHandleView::get` then refuses a wait in a worker child with `WaitInsideTask`, then returns a kept read with no I/O, then checks the thread. An `InvalidInput` or `AsyncContext` answer is not kept.
- A workflow start checks the thread first, as Python's `WorkflowSpec.start`: on a runtime thread the build of a template does not run, and a start by type of a definition that the app does not register is `AsyncContext` too.
- `WorkflowFunctionView::retry_start` checks its error first: an error of another workflow name, or with a code that `retry_start` does not take, is `ValidationFailed` on any thread.
- `WorkflowHandleView::get` refuses a wait in a worker child with `WaitInsideTask` after the limit check. `result_for` is not a wait.
- The thread of a `#[horsies::blocking_task]` body in a worker child drives no async task, so a blocking send or a blocking workflow start works there. A result wait in a worker child is `WaitInsideTask`.

## When the Process Runtime Did Not Start

When the operating system refused the threads of the core's process runtime, the call returns the database failure of its catalog with the cause, `retryable` (the next call tries the start again). Nothing was sent or started:

| Call | Error |
|------|-------|
| A send or a retry | `TaskSendError::EnqueueFailed` with the task id and payload |
| A read | `ReadError::Database` |
| A health read | `BrokerOperationError::OperationFailed` with the code of the read |
| `check_live` | `CheckReport` with the connectivity refusal |
| A workflow start | `WorkflowStartError::EnqueueFailed` with the `workflow_id` of the start |
| `status`, `results`, `tasks`, `cancel`, `pause`, `resume` | `HandleOperationError::DbOperationFailed` |
| `WorkflowHandleView::get`, `result_for` | `WorkflowReadError::Database` with `DbOperationFailed` |

In a process that was forked without `exec` after the process runtime started, every call panics: see [Process-State Refusals](/horsies/tasks/error-types/#process-state-refusals).

## Things to Avoid

**Do not call the blocking views in async code.** They return `ASYNC_CONTEXT` and run nothing. Call the async method with `.await`. See [Async Frameworks](/horsies/rust/guides/async-frameworks/).

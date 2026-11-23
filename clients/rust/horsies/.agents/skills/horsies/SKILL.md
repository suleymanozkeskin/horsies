---
name: horsies
description: Guidance for the horsies Rust crate, the Rust client of the horsies task queue and workflow engine (PostgreSQL broker). Use when code has `use horsies::...`, `#[horsies::task]`, `#[horsies::blocking_task]`, `TaskError<E>`, `TaskOutcome`, `WorkflowDefinition`, `WorkflowSpecBuilder` or `horsies::cli::run`, or when the user works on horsies tasks, workflows, configuration, workers, scheduling or monitoring in Rust.
---

# horsies (Rust) — Quick Reference

The `horsies` crate is the Rust client of the horsies core (`horsies-core`).
The Python package runs on the same core, so the behaviour and the
guarantees are the same for both clients.

This file is an introductory reference. Open a reference file of this
folder only when the task needs it:

| File | When to open |
|---|---|
| `tasks.md` | `#[horsies::task]`, the stored input, `TaskError<E>`, `TaskRuntime`, the cancel at the deadline, the fencing token, sends, `TaskOutcome` and `ReadError`, the blocking API, rerun |
| `workflows.md` | `WorkflowDefinition`, `WorkflowSpecBuilder`, `TaskNode`, `arg_from`, the workflow context, sub-workflows, starts, `WorkflowHandle` |
| `configs.md` | `Horsies::new`, `AppConfig`, queues, worker profiles, child start hooks, schedules, `check`, `horsies::cli::run`, the shipped `horsies` binary |
| `monitoring.md` | `horsies::monitoring` queries, `cancel_task`, workflow actions, the axum mount `horsies::web::router`, `horsies web` |

All public items are at the crate root (`use horsies::Name`), except the
modules `horsies::blocking`, `horsies::cli`, `horsies::monitoring`,
`horsies::rerun` and `horsies::web`.

For the full API reference, run `horsies get-docs` (the shipped binary) to
download the Rust pages and the shared pages of the installed version, or
read `https://suleymanozkeskin.github.io/horsies/rust/llms.txt`.

## Deployment facts

- Use a dedicated PostgreSQL instance for the broker. Ownership,
  finalization and workflow transitions are transactional state changes in
  that database.
- A failover under asynchronous replication can lose committed sends,
  claims, task ends and workflow progress, so a task body can run again.
  Use synchronous replication for the horsies instance, or make each effect
  outside horsies idempotent. The reasons, the connections of each URL and
  the failover limits are on the docs page `operations/deployment-topology`.
- A database belongs to one client. The core refuses a database that the
  Python client installed.
- 0.8.0 starts on a new database. The core refuses a database of
  `horsies-rust` 0.1.0-alpha (migrations up to 65) and changes nothing in
  it. Drain the old fleet, then start 0.8.0 on a new database.
- Task bodies never run in the worker process. The worker starts the app's
  own binary again in child mode for each child. Every line of `main`
  before `horsies::cli::run(app)` runs in each child too.
- Pre-1.0: breaking API changes may land in minor releases.

## Crate features

| Feature | Default | Gives |
|---|---|---|
| `cli` | on | `horsies::cli::run` and the shipped `horsies` binary |
| `web` | off | `horsies::web::router` (axum mount) and `horsies web` |
| `tokio-postgres` | off | `horsies::rerun::TokioPostgresCaller` |
| `sqlx` | off | `horsies::rerun::SqlxCaller` |
| `otel` | off | `horsies::otel`: OpenTelemetry spans of the sends, the workflow starts and the task bodies, and the producer metrics, with the global providers of `opentelemetry` 0.33 |

Minimum Rust version: 1.91.

## Define a Task

```rust
use std::convert::Infallible;

use horsies::{task, TaskError};

#[task("add_numbers")]
async fn add_numbers(a: i64, b: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(a + b)
}
```

- A task is an `async fn` with `#[horsies::task]`, or a `fn` with
  `#[horsies::blocking_task]`.
- It returns `Result<T, TaskError<E>>`. `T` is the output type. `E` is the
  code type of the task's own errors (`#[derive(horsies::UserErrorCode)]`),
  or `std::convert::Infallible` when the task has no codes of its own.
- Every parameter is stored under its name, as Python stores keyword
  arguments: `add_numbers` stores `{"a": 2, "b": 3}`, and a single
  parameter `order: Order` stores `{"order": {...}}`. A task with no
  parameters stores `{}`. See `tasks.md`, The Stored Input.
- The attribute generates a module with the name of the function:
  `add_numbers::register(&mut app)` returns the typed
  `TaskFunction<add_numbers::Input, i64, Infallible>`.

## Run the App

The app's own binary runs the worker, the scheduler and the check. Build the
app in one function and hand it to `horsies::cli::run` from a plain
`fn main`:

<!-- rust-check: alone -->
```rust
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::process::ExitCode;

use horsies::{
    task, AppConfig, Horsies, IsolatedChildCount, IsolatedChildren,
    IsolatedSetting, TaskError, WorkerProfile,
};

#[task("add_numbers")]
async fn add_numbers(a: i64, b: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(a + b)
}

fn build_app() -> Result<Horsies, Box<dyn std::error::Error>> {
    let profile = WorkerProfile {
        isolated: IsolatedSetting::IsolatedChildren(IsolatedChildren::new(
            IsolatedChildCount::try_from(4)?,
        )),
        ..WorkerProfile::default()
    };
    let config = AppConfig {
        worker_profiles: BTreeMap::from([("default".to_owned(), profile)]),
        ..AppConfig::for_database_url("postgresql://app:secret@db.example.com:5432/horsies")
    };
    let mut app = Horsies::new(config)?;
    let _add = add_numbers::register(&mut app)?;
    Ok(app)
}

fn main() -> ExitCode {
    match build_app() {
        Ok(app) => horsies::cli::run(app),
        Err(refused) => {
            eprintln!("{refused}");
            ExitCode::FAILURE
        }
    }
}
```

```bash
./myapp worker --profile default
./myapp scheduler
./myapp check --live
```

- `Horsies::new` does no I/O. The app connects at its first operation.
- A Rust worker profile does not set `preload`. The default,
  `ChildPreload::NoPreload`, is the only value that a Rust worker takes;
  `Horsies::new` refuses `Library` and `App` with `HRS-218`.
- `run` blocks its thread. Inside a tokio runtime it prints why and
  returns exit code 1.

See `configs.md` for worker profiles, a binary that owns its `main`, and
the exit codes.

## Send a Task and Read the Result

A producer process builds the same app and registers the same tasks:

<!-- rust-check: alone -->
```rust
use std::convert::Infallible;
use std::time::Duration;

use horsies::{task, AppConfig, Horsies, TaskError, TaskOutcome, WaitLimit};

#[task("add_numbers")]
async fn add_numbers(a: i64, b: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(a + b)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = Horsies::new(AppConfig::for_database_url(
        "postgresql://app:secret@db.example.com:5432/horsies",
    ))?;
    let add = add_numbers::register(&mut app)?;

    let handle = add.send(add_numbers::Input { a: 2, b: 3 }).await?;
    let limit = WaitLimit::parse(Some(Duration::from_secs(30)))?;
    match handle.get(limit).await? {
        TaskOutcome::Succeeded(sum) => println!("sum = {sum}"),
        TaskOutcome::Failed(error) => println!("failed with {}", error.code().text()),
    }
    Ok(())
}
```

- `send` returns `Result<TaskHandle<T, E>, TaskSendError>`.
- `get` returns `Result<TaskOutcome<T, E>, ReadError>`. The outer error is
  the read that failed (the wait limit passed, the task is held nowhere,
  the stored data does not read, the database failed). The inner value is
  what the task ended with.
- The generated `add_numbers::send(2, 3)` takes the parameters and sends
  through the function of the first `register` in the process.
- A result wait in a worker child, the process that runs task bodies, is
  refused with `WaitInsideTask` before any statement. Compose tasks with a
  workflow.

## Blocking Calls

A program with no tokio runtime uses the blocking views:

<!-- rust-check: alone -->
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

On a thread that runs a tokio runtime, a blocking call runs nothing and
returns `ASYNC_CONTEXT` (`TaskSendError::AsyncContext`,
`ReadError::AsyncContext`). See `tasks.md`, Blocking API.

## Define a Workflow

```rust
use std::convert::Infallible;

use horsies::{
    task, HorsiesError, TaskError, TaskOutcome, WorkflowDefConfig, WorkflowDefinition,
    WorkflowSpecBuilder,
};

#[task("fetch_report")]
async fn fetch_report(region: String) -> Result<String, TaskError<Infallible>> {
    Ok(format!("report of {region}"))
}

#[task("publish_report")]
async fn publish_report(
    report: TaskOutcome<String, Infallible>,
) -> Result<usize, TaskError<Infallible>> {
    match report {
        TaskOutcome::Succeeded(text) => Ok(text.len()),
        TaskOutcome::Failed(_error) => Ok(0),
    }
}

struct DailyReport;

impl WorkflowDefinition for DailyReport {
    type Output = usize;
    type Params = ();

    fn name() -> &'static str {
        "daily_report"
    }

    fn definition_key() -> &'static str {
        "reports.daily_report.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let fetched = builder.task(
            fetch_report::node()?.set(fetch_report::params::region(), "eu".to_owned())?,
        );
        let published = builder.task(
            publish_report::node()?.arg_from(publish_report::params::report(), fetched),
        );
        Ok(WorkflowDefConfig::new().output(published))
    }
}
```

- `arg_from` gives the dependency's outcome to a parameter of type
  `TaskOutcome<S, E>`, and the node waits for the dependency.
- Register the definition in `build_app` with
  `app.register_workflow_definition::<DailyReport>()?`, after the tasks.
- Start it with `function.start().await` or
  `horsies::start_workflow::<DailyReport>().await`. See `workflows.md`.

## Result Types

| Operation | Returns |
|---|---|
| Task body | `Result<T, TaskError<E>>` |
| `send`, `schedule`, `retry_send`, `retry_schedule` | `Result<TaskHandle<T, E>, TaskSendError>` |
| `TaskHandle::get`, `Horsies::get_result` | `Result<TaskOutcome<T, E>, ReadError>` |
| `TaskHandle::info`, `Horsies::get_task_info` | `Result<TaskInfo, ReadError>` |
| Workflow start | `Result<WorkflowHandle<T>, WorkflowStartError>` |
| `WorkflowHandle::get` | `Result<TaskOutcome<T, UserCode>, WorkflowReadError>` |
| `WorkflowHandle::status`, `cancel`, `pause`, `resume` | `HandleResult<_>` (`Result<_, HandleOperationError>`) |
| Health reads on the app | `Result<_, BrokerOperationError>` |
| Config, registration, `provide` | `Result<_, HorsiesError>` (`HRS-xxx` codes) |
| `check`, `check_live` | `Result<CheckPassed, CheckReport>` |

Every error type is a closed catalog. Match on the variant or on the code.
`retryable: true` means the cause may clear; it does not mean that a repeat
is safe. The codes and their meaning are on the shared docs page
`tasks/errors`.

A fork is never an error value. Every public call that runs on the core,
async or blocking, panics in a process that was forked without `exec`
after the core's process runtime started in its parent: the sends, the
reads, `check_live`, the workflow starts, `WorkflowHandle`,
`horsies::monitoring`, `rerun_task`, `run_worker` and `run_scheduler`.
The panic names the call, both process ids and the fix; nothing ran. Start
child processes with `exec` (`std::process::Command`), or fork before the
parent's first call that runs on the core. Python raises
`ForkedAfterCoreStart` in the same state. `rerun_task_in` runs on the
caller's transaction, not on the core's runtime, so it runs in a fork.

## Rerun a Terminal Task

A terminal record is immutable. A rerun is a new task with a new id and a
recorded lineage: `app.rerun_task(command, policy)` in a transaction of the
app, or `horsies::rerun::rerun_task_in` in a transaction that the caller
owns. See `tasks.md`, Rerun a Terminal Task.

## CLI

```bash
./myapp worker --profile <name> [--loglevel LEVEL] [--metrics-listen HOST:PORT]   # the app's own binary
./myapp scheduler [--loglevel LEVEL]
./myapp check [--live] [--loglevel LEVEL]
./myapp metrics --listen HOST:PORT [--loglevel LEVEL]   # the fleet metrics at GET /metrics
horsies web --database-url URL                       # the shipped binary (feature web)
horsies get-docs [--output DIR] [--ref main]
horsies skills install --target DIR [--force]        # copy this skill folder to DIR/horsies/
```

The shipped `horsies` binary refuses `worker`, `scheduler` and `check` with
exit code 2: the tasks are compiled into the app's own binary. See
`configs.md`, CLI.

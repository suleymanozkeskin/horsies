---
title: Getting Started
summary: Add the horsies crate, define a task, build the app in one binary that is the worker and the producer, run a worker, and send the task.
related: [../tasks/defining-tasks, ../configuration/app-config, ../workers/worker-setup, ../cli, ../tasks/sending-tasks]
tags: [rust, quick start, install, first task, worker]
---

# Getting Started

One binary of a Rust app is the producer, the worker and each child of the worker. `main` builds the app, registers the tasks, and hands the command line to `horsies::cli::run`, which runs the worker, the scheduler or the check. This page builds an app with one task, runs a worker, and sends the task.

## Prerequisites

- Rust 1.91 or later.
- A PostgreSQL database for the app. A database belongs to one client: the core refuses a database that the Python client installed.

## Add the Crate

```bash
cargo add horsies
cargo add tokio --features rt-multi-thread,macros
```

The default feature `cli` gives `horsies::cli::run`. The other features are `web` (the monitoring UI), `tokio-postgres` and `sqlx` (rerun in the caller's transaction). See [Overview](/horsies/rust/).

## Write the App

```rust
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::process::ExitCode;
use std::time::Duration;

use horsies::{
    task, AppConfig, Horsies, IsolatedChildren, TaskError, TaskOutcome, WaitLimit,
    WorkerProfile,
};

const DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/horsies";

#[task("greet")]
async fn greet(name: String) -> Result<String, TaskError<Infallible>> {
    Ok(format!("hello, {name}"))
}

/// Every process builds the same app: the producer, the worker and each child.
fn build_app() -> Result<Horsies, Box<dyn std::error::Error>> {
    let profile = WorkerProfile {
        isolated: IsolatedChildren::new(2).into(),
        ..WorkerProfile::default()
    };
    let config = AppConfig {
        worker_profiles: BTreeMap::from([("default".to_owned(), profile)]),
        ..AppConfig::for_database_url(DATABASE_URL)
    };
    let mut app = Horsies::new(config)?;
    let _greet = greet::register(&mut app)?;
    Ok(app)
}

async fn send_greeting(app: &Horsies) -> Result<(), Box<dyn std::error::Error>> {
    let handle = greet::send(app, "world".to_owned()).await?;
    match handle.get(WaitLimit::Within(Duration::from_secs(30))).await? {
        TaskOutcome::Succeeded(greeting) => println!("{greeting}"),
        TaskOutcome::Failed(error) => println!("greet failed: {}", error.code().text()),
    }
    Ok(())
}

fn run_send(app: Horsies) -> ExitCode {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("the runtime did not start: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(send_greeting(&app)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn main() -> ExitCode {
    let app = match build_app() {
        Ok(app) => app,
        Err(refused) => {
            eprintln!("{refused}");
            return ExitCode::FAILURE;
        }
    };
    match std::env::args_os().nth(1) {
        Some(command) if command == "send" => run_send(app),
        Some(_) | None => horsies::cli::run(app),
    }
}
```

- `#[task("greet")]` defines the task `greet`. Its parameter `name` is stored under its name: the input `{"name": "world"}`. See [Defining Tasks](/horsies/rust/tasks/defining-tasks/).
- `Horsies::new` does no I/O. The app connects at its first operation and installs the schema there. See [App Config](/horsies/rust/configuration/app-config/).
- The worker profile `default` has two isolated children. It does not set `preload`: the default, `ChildPreload::NoPreload`, is the only value that a Rust worker takes. See [Worker Setup](/horsies/rust/workers/worker-setup/).
- `greet::send(app, ...)` sends through the app that registered `greet`. `get` waits for the outcome. See [Sending Tasks](/horsies/rust/tasks/sending-tasks/) and [Retrieving Results](/horsies/rust/tasks/retrieving-results/).
- The worker starts this binary again for each child, with arguments of its own. `horsies::cli::run` serves them. Every line of `main` before it runs in each child too: build the app there and start nothing else.

## Run It

Start a worker of the profile `default`:

```bash
cargo run -- worker --profile default
```

In a second terminal, send the task:

```bash
cargo run -- send
```

```text
hello, world
```

`cargo run -- check` checks the app without a database. The commands, their options and their exit codes are on [CLI Reference](/horsies/rust/cli/).

## Next Steps

| Topic | Page |
|-------|------|
| Task attributes, execution classes, retries, `TaskRuntime` | [Defining Tasks](/horsies/rust/tasks/defining-tasks/) |
| Errors of a task and their codes | [Task Errors](/horsies/rust/tasks/errors/) |
| Workflows | [Workflow API](/horsies/rust/concepts/workflows/workflow-api/) |
| Schedules | [Schedule Config](/horsies/rust/scheduling/schedule-config/) |
| A program with no tokio runtime | [Blocking API](/horsies/rust/tasks/blocking-api/) |
| Tests of task bodies | [Testing](/horsies/rust/operations/testing/) |

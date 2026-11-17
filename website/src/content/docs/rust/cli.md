---
title: CLI Reference
summary: horsies::cli::run(app) runs the worker, the scheduler or the check of a Rust app from the app's own binary, and serves the worker children of that binary.
related: [configuration/app-config, ../workers/worker-architecture, ../scheduling/scheduler-overview, ../workers/heartbeats-recovery]
tags: [rust, cli, worker, scheduler, check]
---

# CLI Reference

A Rust app runs its worker, its scheduler and its check from its own binary. `horsies::cli::run(app)` reads the arguments of the process and runs the command. The tasks are registered in the binary, so a command takes no app path. `run` is in the `cli` feature, which is on by default.

What a worker, a scheduler and a check do is on the shared pages: [Worker Architecture](/horsies/workers/worker-architecture/) and [Scheduler Overview](/horsies/scheduling/scheduler-overview/).

## Run the App

```rust
use std::process::ExitCode;

use horsies::{AppConfig, Horsies, HorsiesError};

fn build_app() -> Result<Horsies, HorsiesError> {
    let config = AppConfig::for_database_url("postgresql://app:secret@db.example.com:5432/horsies");
    Horsies::new(config)
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

Register the tasks, provide the state and add the child start hooks in `build_app`: see [App Config](/horsies/rust/configuration/app-config/#registration-and-state).

- The worker starts this binary again for each child, with the argument `--horsies-child`. `run` reads that argument first and serves the child. Every line of `main` before `run` runs in each child too.
- `run` blocks the calling thread until the command ends. Call it from a plain `fn main`. Inside a tokio runtime it prints why and returns exit code 1.

## Commands

| Command | Does | Python command |
|---------|------|----------------|
| `<binary> worker --profile <name> [--loglevel LEVEL]` | Starts a worker of one profile of `AppConfig.worker_profiles` | [`horsies worker`](/horsies/cli/#horsies-worker) |
| `<binary> scheduler [--loglevel LEVEL]` | Starts the scheduler of `AppConfig.schedule` | [`horsies scheduler`](/horsies/cli/#horsies-scheduler) |
| `<binary> check [--live] [--loglevel LEVEL]` | Checks the app without starting a service | [`horsies check`](/horsies/cli/#horsies-check) |

An unknown command, or no command, is an argument error: exit code 2.

### `worker`

```bash
./myapp worker --profile default
./myapp worker --profile batch --loglevel debug
```

| Option | Default | Description |
|--------|---------|-------------|
| `--profile NAME` | required | The profile of `AppConfig.worker_profiles` that holds the settings of the worker: children, recycling, claim limits, drain timeout |
| `--loglevel LEVEL` | `INFO` | See [Logs](#logs) |

- A missing or unknown `--profile` exits with code 1, and nothing starts. The refusal lists the defined profile names.
- The app check runs before the worker starts, without the schedules. A refused check prints its report on stderr and exits with code 1.
- `SIGTERM` or `SIGINT` stops the worker. It drains within the drain timeout of the profile and exits with code 0. See [Graceful Shutdown](/horsies/workers/worker-architecture/#graceful-shutdown).

### `scheduler`

```bash
./myapp scheduler
```

| Option | Default | Description |
|--------|---------|-------------|
| `--loglevel LEVEL` | `INFO` | See [Logs](#logs) |

- The scheduler exits with code 1 when the app config has no `schedule` section, or when `schedule.enabled` is `false`.
- The app check runs before the scheduler starts, with the schedules. A refused check prints its report on stderr and exits with code 1.
- `SIGTERM` or `SIGINT` stops the scheduler, and it exits with code 0.

### `check`

```bash
./myapp check
./myapp check --live
```

| Option | Default | Description |
|--------|---------|-------------|
| `--live` | off | Also connect to the database, and compare each worker profile with the connection limit of the server |
| `--loglevel LEVEL` | `WARNING` | See [Logs](#logs) |

A check that passes prints its text on stdout and exits with code 0. A refused check prints its report on stderr and exits with code 1. `--live` installs no schema.

## Logs

`--loglevel` takes `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL`, in any case.

- `run` installs one `tracing` subscriber that writes to stderr at the level of `--loglevel`. When `RUST_LOG` is set to a valid filter, it replaces the level.
- `tracing` has no level above `error`, so `CRITICAL` logs errors.
- A subscriber that the binary installed before `run` is kept.
- Each worker child installs the same subscriber, at the `--loglevel` of the worker.
- The logs have ANSI color on a terminal. `FORCE_COLOR` (set, not empty, not `0`) turns color on, and wins over `NO_COLOR`. `NO_COLOR` (set, any value) turns color off.

## Removed Flags

The flags of the `0.1.0-alpha` commands exit with code 2, with the message `<flag> was removed in 0.8.0: <replacement>`. A flag counts in its own form and in its `=value` form, only after its own command.

| Command | Flag | Replacement |
|---------|------|-------------|
| `worker` | `-q`, `--queues` | A worker serves every queue of the app. Set the queues with `AppConfig.queue_mode` and `AppConfig.custom_queues` |
| `worker` | `-c`, `--concurrency` | The children of a profile of `AppConfig.worker_profiles` (`WorkerProfile.isolated`, `shared_async`, `shared_thread`), and `--profile <name>` |
| `worker` | `--max-claim-batch` | `WorkerProfile.claim.per_queue_per_pass`, and `--profile <name>` |
| `worker` | `--max-claim-per-worker` | `WorkerProfile.claim.claimed_per_worker`, and `--profile <name>` |
| `worker` | `--coalesce-notifies` | None |
| `scheduler` | `--check-interval` | `AppConfig.schedule.check_interval_seconds` |
| `scheduler` | `--dry-run` | The `check` command |

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | The worker or the scheduler stopped at `SIGTERM` or `SIGINT`, the check passed, or `--help` printed the usage |
| 1 | Any other end: a refused check, a refused profile, a start that failed, a worker or a scheduler that failed, or `run` inside a tokio runtime |
| 2 | Arguments that do not parse, or a removed flag |
| 77 | `worker`: the worker could not write runner heartbeats, and it had not stopped 120 seconds after it started to stop. See [When the Worker Cannot Write Heartbeats](/horsies/workers/heartbeats-recovery/#when-the-worker-cannot-write-heartbeats) |

A worker child exits with the code of `ChildHostExit`. The worker reads it to name how the child ended.

| Code | `ChildHostExit` | Meaning |
|------|-----------------|---------|
| 0 | `Stopped` | The worker sent its stop frame |
| 71 | `ParentGone` | The worker is gone |
| 72 | `BadParentFrame` | The worker sent a malformed frame, or a frame over the bounds |
| 73 | `BadArguments` | The child mode arguments cannot be used |
| 75 | `BootFailed` | The child start failed, after a report that names the cause |
| 78 | `ProtocolMismatch` | The worker speaks another protocol version |
| 173 | `ChildHookFailed` | A child start hook failed, panicked or ran past its bound |

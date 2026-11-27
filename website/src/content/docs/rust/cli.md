---
title: CLI Reference
summary: horsies::cli::run(app) runs the worker, the scheduler, the check or the fleet metrics server of a Rust app from the app's own binary and serves its worker children; the shipped horsies binary has web, get-docs and skills install.
related: [configuration/app-config, ../workers/worker-architecture, ../scheduling/scheduler-overview, ../workers/heartbeats-recovery, ../monitoring/web-ui-deployment]
tags: [rust, cli, worker, scheduler, check, metrics, web, get-docs, skills]
---

# CLI Reference

A Rust app has two command-line programs:

| Program | Commands | Why |
|---------|----------|-----|
| The app's own binary, through `horsies::cli::run(app)` | `worker`, `scheduler`, `check`, `metrics`, and child mode | The tasks are compiled into this binary, and the worker starts it again for each child |
| The shipped `horsies` binary | `web`, `get-docs`, `skills install` | These commands need no task of the app |

Both are in the `cli` feature of the crate, which is on by default.

What a worker, a scheduler and a check do is on the shared pages: [Worker Architecture](/horsies/workers/worker-architecture/) and [Scheduler Overview](/horsies/scheduling/scheduler-overview/).

## Run the App

```rust
use std::process::ExitCode;

use horsies::{AppConfig, Horsies, ValidationReport};

fn build_app() -> Result<Horsies, ValidationReport> {
    let config = AppConfig::from_database_url("postgresql://app:secret@db.example.com:5432/horsies");
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

The contract of `run(app: Horsies) -> ExitCode`:

1. Child mode first. The worker starts this binary again for each child, with the arguments `--horsies-child <fd> <pid>`. `run` reads them before any other argument, serves the child until the worker stops it, and returns the child's exit code (see [Exit Codes](#exit-codes)). A child start with arguments that cannot be used exits with code 73.
2. Every line of `main` before `run` runs in each child too. Build the app there (config, registrations, state, start hooks), and start nothing else: each child would start it again.
3. `run` blocks the calling thread until the command ends. Call it from a plain `fn main`. Inside a tokio runtime it prints why and returns exit code 1.
4. Any other process runs the command of its arguments: `worker`, `scheduler`, `check` or `metrics`. `run` installs the logs of the command (see [Logs](#logs)) and returns its exit code.

## A Binary That Owns Its `main`

A binary with a command line of its own (for example a service that also runs other loops) calls the parts of `run` itself: `horsies::child_mode()` first, then a method of the app for its service.

```rust
use std::process::ExitCode;

use horsies::{AppConfig, ChildHostExit, ChildMode, Horsies, ValidationReport, WorkerRunOptions};

fn build_app() -> Result<Horsies, ValidationReport> {
    let config = AppConfig::from_database_url("postgresql://app:secret@db.example.com:5432/horsies");
    Horsies::new(config)
}

fn main() -> ExitCode {
    let app = match build_app() {
        Ok(app) => app,
        Err(refused) => {
            eprintln!("{refused}");
            return ExitCode::FAILURE;
        }
    };
    match horsies::child_mode() {
        ChildMode::Child(args) => app.serve_child(args),
        ChildMode::BadArguments(error) => {
            eprintln!("child mode cannot start: {error}");
            ExitCode::from(u8::try_from(ChildHostExit::BadArguments.exit_code()).unwrap_or(u8::MAX))
        }
        ChildMode::Worker => run_worker(app),
    }
}

fn run_worker(app: Horsies) -> ExitCode {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("the runtime did not start: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(app.run_worker(WorkerRunOptions::profile("default"))) {
        Ok(_stopped) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
```

- Call `serve_child` outside any tokio runtime: it blocks its thread on the core's process runtime.
- `serve_child` installs no `tracing` subscriber; the child keeps the one that `main` installed before the call.

| Command of `run` | Method |
|------------------|--------|
| child mode | `horsies::child_mode() -> ChildMode`, then `Horsies::serve_child(self, ChildArgs) -> ExitCode` |
| `worker --profile <name>` | `Horsies::run_worker(self, WorkerRunOptions) -> Result<WorkerStopped, WorkerRunError>` (async) |
| `scheduler` | `Horsies::run_scheduler(&self) -> Result<SchedulerStopped, SchedulerRunError>` (async), or `Horsies::scheduler(&self)` for a `PreparedScheduler` with `run()` and `stop_handle()` |
| `check` | `Horsies::check(&self) -> Result<CheckPassed, ValidationReport>` |
| `check --live` | `Horsies::check_live(&self) -> Result<CheckPassed, ValidationReport>` (async) |

`run_worker` and `run_scheduler` stop at `SIGTERM` or `SIGINT`, as the commands do.

## Commands

| Command | Does | Python command |
|---------|------|----------------|
| `<binary> worker --profile <name> [--log-level LEVEL] [--metrics-listen HOST:PORT]` | Starts a worker of one profile of `AppConfig.worker_profiles` | [`horsies worker`](/horsies/cli/#horsies-worker) |
| `<binary> scheduler [--log-level LEVEL]` | Starts the scheduler of `AppConfig.schedule` | [`horsies scheduler`](/horsies/cli/#horsies-scheduler) |
| `<binary> check [--live] [--log-level LEVEL]` | Checks the app without starting a service | [`horsies check`](/horsies/cli/#horsies-check) |
| `<binary> metrics --metrics-listen HOST:PORT [--log-level LEVEL]` | Serves the fleet metrics of the app's database at `GET /metrics` | [`horsies metrics`](/horsies/cli/#horsies-metrics) |

An unknown command, or no command, is an argument error: exit code 2.

### `worker`

```bash
./myapp worker --profile default
./myapp worker --profile batch --log-level debug
./myapp worker --profile default --metrics-listen 0.0.0.0:9464
```

| Option | Default | Description |
|--------|---------|-------------|
| `--profile NAME` | required | The profile of `AppConfig.worker_profiles` that holds the settings of the worker: children, recycling, claim limits, drain timeout |
| `--log-level LEVEL` | `INFO` | See [Logs](#logs) |
| `--metrics-listen HOST:PORT` | none | Serve the worker metrics (`GET /metrics`) and the health probes (`GET /livez`, `GET /readyz`) on `HOST:PORT`, or `[IPV6]:PORT`. There is no default: without it the worker serves no endpoint. See [Metrics](/horsies/observability/metrics/) |

- A `--metrics-listen` value that does not parse is an argument error: exit code 2. An address that does not bind does not stop the worker: the endpoint binds again with a backoff. `WorkerRunOptions::with_metrics_listen(ListenAddress)` gives the address to `Horsies::run_worker` in a binary that owns its `main`.

- A missing or unknown `--profile` exits with code 1, and nothing starts. The refusal lists the defined profile names.
- The app check runs before the worker starts, without the schedules. A refused check prints its report on stderr and exits with code 1.
- `SIGTERM` or `SIGINT` stops the worker. It drains within the drain timeout of the profile and exits with code 0. See [Graceful Shutdown](/horsies/workers/worker-architecture/#graceful-shutdown).

### `scheduler`

```bash
./myapp scheduler
```

| Option | Default | Description |
|--------|---------|-------------|
| `--log-level LEVEL` | `INFO` | See [Logs](#logs) |

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
| `--log-level LEVEL` | `WARNING` | See [Logs](#logs) |

A check that passes prints its text on stdout and exits with code 0. A refused check prints its report on stderr and exits with code 1. `--live` installs no schema.

### `metrics`

```bash
./myapp metrics --metrics-listen 0.0.0.0:9465
```

| Option | Default | Description |
|--------|---------|-------------|
| `--metrics-listen HOST:PORT` | required | The address to serve on: `HOST:PORT`, or `[IPV6]:PORT`. The port is 1 to 65535. There is no default |
| `--log-level LEVEL` | `INFO` | See [Logs](#logs) |

- The command serves the fleet metrics of the app's database at `GET /metrics`, in the Prometheus text format. What the metrics hold and when the command reads them: [Fleet Collector](/horsies/observability/fleet-collector/).
- The connection runs no schema step and opens no listener.
- An address that does not parse is an argument error: exit code 2. The command exits with code 0 at `SIGTERM` or `SIGINT`, and with code 1 when the config does not build a handle or the address does not bind. A database that does not answer does not end it.

## Logs

`--log-level` takes `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL`, in any case.

- `run` installs one `tracing` subscriber that writes to stderr at the level of `--log-level`. When `RUST_LOG` is set to a valid filter, it replaces the level.
- `tracing` has no level above `error`, so `CRITICAL` logs errors.
- A subscriber that the binary installed before `run` is kept.
- Each worker child installs the same subscriber, at the `--log-level` of the worker.
- The logs have ANSI color on a terminal. `FORCE_COLOR` (set, not empty, not `0`) turns color on, and wins over `NO_COLOR`. `NO_COLOR` (set, any value) turns color off.
- Each task body runs inside the `tracing` span `run` (target `horsies::attempt`) with the fields `horsies.task.id`, `horsies.task.name` and `horsies.task.attempt`, so each record of the body carries them. The filter of `run` passes this span at every `--log-level`. The fields of the core's own records are on [Logs](/horsies/observability/logs/).

### JSON Lines

`run_with_log_format(app, LogFormat::JsonLogs)` runs the app as `run` does, with one JSON object per log record, in the runner and in each worker child:

```rust
use std::process::ExitCode;

use horsies::cli::LogFormat;
use horsies::{AppConfig, Horsies, ValidationReport};

fn build_app() -> Result<Horsies, ValidationReport> {
    let config = AppConfig::from_database_url("postgresql://app:secret@db.example.com:5432/horsies");
    Horsies::new(config)
}

fn main() -> ExitCode {
    match build_app() {
        Ok(app) => horsies::cli::run_with_log_format(app, LogFormat::JsonLogs),
        Err(refused) => {
            eprintln!("{refused}");
            ExitCode::FAILURE
        }
    }
}
```

| Key | Value |
|-----|-------|
| `timestamp` | The time of the record |
| `level` | The level |
| `target` | The target of the event |
| The fields of the event | At the top level, `message` among them |
| `span` | The innermost span of the record, with its name and fields |
| `spans` | Every span of the record, outermost first |

`run(app)` is `run_with_log_format(app, LogFormat::TextLogs)`. With the feature `otel`, the subscriber of both also has the layer of `horsies::otel`: see [Tracing Setup](/horsies/rust/observability/tracing-setup/).

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

The exit codes of `run`, the codes of the app's own binary:

| Code | Meaning |
|------|---------|
| 0 | The worker or the scheduler stopped at `SIGTERM` or `SIGINT`, the check passed, or `--help` printed the usage |
| 1 | Any other end: a refused check, a refused profile, a start that failed, a worker or a scheduler that failed, or `run` inside a tokio runtime |
| 2 | Arguments that do not parse, or a removed flag |
| 77 | `worker`: the worker could not write runner heartbeats, and it had not stopped 120 seconds after it started to stop. See [When the Worker Cannot Write Heartbeats](/horsies/workers/heartbeats-recovery/#when-the-worker-cannot-write-heartbeats) |
| 79 | `worker`: the worker held a task, and its liveness lock was not held for 60 seconds. See [Worker Liveness Lock](/horsies/workers/heartbeats-recovery/#worker-liveness-lock) |

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

## The `horsies` Binary

The crate ships one binary, `horsies`. `web` needs the `web` feature:

```bash
cargo install horsies --features web
```

| Command | Does | Python command |
|---------|------|----------------|
| `horsies web [CONFIG] [OPTIONS]` | Serves the monitoring web UI of one database | [`horsies web`](/horsies/cli/#horsies-web) |
| `horsies get-docs [--output DIR] [--ref main]` | Fetches the docs of this release for AI agents | [`horsies get-docs`](/horsies/cli/#horsies-get-docs) |
| `horsies skills install --target DIR [--force]` | Copies the agent skill folder of this release | [`horsies skills install`](/horsies/cli/#horsies-skills-install) |

`horsies worker`, `horsies scheduler` and `horsies check` exit with code 2: the tasks of an app are compiled into the app's own binary, so these commands run there. The message names `horsies::cli::run(app)` and the command of that binary, for example `` `<your binary> worker` ``. `horsies --help` names them the same way.

### `horsies web`

```bash
horsies web horsies.toml
horsies web --database-url postgresql://app:secret@localhost:5432/horsies
horsies web --database-url postgresql://app:secret@pooler.example.com:6432/horsies \
  --session-database-url postgresql://app:secret@db.example.com:5432/horsies \
  --pgbouncer-transaction-mode
horsies web horsies.toml --host 0.0.0.0 --authorization trusted-header --enable-actions
```

| Option | Default | Description |
|--------|---------|-------------|
| `CONFIG` | none | A TOML file of `AppConfig`; its `broker` section names the database. Give exactly one of `CONFIG` and `--database-url` |
| `--database-url URL` | none | Monitor this database, with the default of every other setting |
| `--session-database-url URL` | none | A direct or session-capable URL when `--database-url` is pooled. Needs `--database-url` |
| `--pgbouncer-transaction-mode` | off | `--database-url` is a PgBouncer transaction-pool endpoint. Needs `--database-url` and `--session-database-url` |
| `--host HOST` | `127.0.0.1` | Bind address |
| `--port PORT` | `8600` | Bind port; `0` asks the operating system for a free port |
| `--authorization MODE` | `none` | `none` (a loopback host only) or `trusted-header` |
| `--trusted-header NAME` | `X-Forwarded-User` | The identity header that the proxy sets, in `trusted-header` mode |
| `--enable-actions` | off | Allow task cancel and workflow pause, resume and cancel. Without it the UI is view only |
| `--custom-css-url URL` | none | A stylesheet URL that the page loads after its own stylesheet |
| `--trace-link TEMPLATE` | none | The link of a task's trace id in the task detail: an `http` or `https` URL with one `{trace_id}`. A template that breaks a rule exits 2 and names the rule |
| `--log-level LEVEL` | `INFO` | See [Logs](#logs) |

A config file:

```toml
[database]
database_url = "postgresql://app:secret@db.example.com:5432/horsies"
```

- The config file is read up to 1 MiB. A key that no config section has is refused by name, with exit code 1.
- The server never installs or migrates the schema (`SchemaMode::ObserveOnly`).
- `--authorization none` on a host that is not `localhost` or a loopback address exits with code 2. A blank `--trusted-header`, or a value that is not a header name, exits with code 2. `--authorization trusted-header` prints a warning: the proxy must strip or overwrite that header on each incoming request. The access rules are on the shared page: [Bind rules](/horsies/monitoring/web-ui-deployment/#bind-rules) and [Trusted-header mode](/horsies/monitoring/web-ui-deployment/#trusted-header-mode).
- The binary has no task or workflow of an app. A resume of a paused workflow that owes a sub-workflow child needs the app's workflow definitions: serve the UI from the app's own binary with `horsies::web::router` (feature `web`).
- `SIGTERM` or `SIGINT` stops the server; requests in flight end first, and it exits with code 0.
- Built without the `web` feature, `horsies web` prints ``horsies web requires the `web` cargo feature`` and exits with code 1.

### `horsies get-docs`

```bash
horsies get-docs
horsies get-docs --output docs/horsies --ref main
```

| Option | Default | Description |
|--------|---------|-------------|
| `--output DIR` | `.horsies-docs` | The output folder |
| `--ref main` | none | Fetch the `main` branch instead of the release tag. Required for a version that is not a release version |

- The docs are those of the tag `v<crate version>`, for example `v0.8.0`. A crate version that is not three dot-separated numbers (a pre-release) has no tag: without `--ref main` the command exits with code 1. The `main` branch can differ from the installed version.
- Kept: every page under `rust/` and every page with `section: shared`. The Python API pages and the site pages are dropped. `llms.txt` of the Rust docs is written to the output folder.
- The fetch is a git sparse checkout, then, when that fails, the tarball of the ref through `curl` and `tar`. Each git or curl call is stopped after 60 seconds.
- The output folder is replaced only after the new copy is complete. On success the command prints `Done. N files written to DIR/` and exits with code 0; on a failure it prints the cause and exits with code 1.

### `horsies skills install`

```bash
horsies skills install --target .claude/skills
horsies skills install --target .claude/skills --force
```

| Option | Default | Description |
|--------|---------|-------------|
| `--target DIR` | required | The skills folder of the agent tool. The skill is written to `DIR/horsies/` |
| `--force` | off | Replace a copy that differs from the skill files of this release |

The skill files are compiled into the binary. The command creates `DIR` when it does not exist, and writes the new copy beside the destination before it moves it into place.

| Case | Result | Exit code |
|------|--------|-----------|
| `DIR/horsies/` is absent | Folder written | 0 |
| `DIR/horsies/` has the same files | Nothing written | 0 |
| `DIR/horsies/` differs (edited, missing or extra files) | Nothing written. The message lists the differing paths | 1 |
| `DIR/horsies/` differs and `--force` is set | Folder replaced | 0 |
| `DIR` or `DIR/horsies` is a file | Nothing written | 1 |
| A filesystem call fails | The message names the path, the cause and what is at the destination | 1 |

### Exit Codes of `horsies`

| Code | Meaning |
|------|---------|
| 0 | The command succeeded, or `SIGTERM` or `SIGINT` stopped `web` |
| 1 | The command failed |
| 2 | Arguments that do not parse, `worker`, `scheduler` or `check`, or an argument that `web` refuses before it starts |

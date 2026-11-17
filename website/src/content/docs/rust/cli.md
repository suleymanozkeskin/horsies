---
title: CLI Reference
summary: Commands of the horsies binary, and the library methods that run a worker, the scheduler, and the definition check.
related: [../workers/worker-architecture, ../scheduling/scheduler-overview, ./monitoring/web-ui-deployment, ./operations/cutover-runbook]
tags: [cli, worker, scheduler, monitoring, commands]
---

The `horsies` crate ships a clap-based binary named `horsies`. The binary runs
the commands that need no task registry: the monitoring web UI, the
documentation fetch, the task-history cutover, and archive transcodes.

Rust tasks are registered at compile time. A worker, the scheduler, and the
definition check therefore run inside the application's own binary, through
library methods. See [Application Binary](#application-binary). One entry point
for these modes is tracked in
[#656](https://github.com/suleymanozkeskin/horsies/issues/656).

## Commands

| Command | Result |
| ------- | ------ |
| [`horsies web`](#horsies-web) | Serves the monitoring web UI. Needs the `web` Cargo feature. |
| [`horsies get-docs`](#horsies-get-docs) | Writes the documentation to a local directory as Markdown files. |
| [`horsies cutover`](#horsies-cutover) | Runs the stages of the offline task-history cutover. |
| [`horsies transcode`](#horsies-transcode) | Runs replacement-partition archive transcodes. |
| [`horsies worker`, `horsies scheduler`, `horsies check`](#refused-commands) | Refused. Exit code 1. |

### Refused Commands

`horsies --help` lists `worker`, `scheduler`, and `check`, and the binary
parses their arguments. The binary then writes this line to stderr and exits
with code 1:

```text
worker, scheduler, and application checks require an application-specific binary with its task registry linked; use that binary's command surface
```

Run these modes from the application binary. See
[Application Binary](#application-binary).

### horsies web

Start the optional monitoring dashboard. Build the binary with the `web` Cargo
feature. Without the feature, the command writes
``horsies web requires the `web` cargo feature`` to stderr and exits with
code 1.

```bash
horsies web CONFIG [OPTIONS]
horsies web --database-url URL [OPTIONS]
```

`CONFIG` is a TOML application configuration. `--database-url` selects the
standalone database-URL form. The two forms are mutually exclusive.

The server is observe-only. It does not run migrations or the task-history
fleet gate.

**Options:**

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--database-url URL` | -- | Runtime query URL instead of `CONFIG` |
| `--session-database-url URL` | -- | Direct or session URL for `LISTEN/NOTIFY` |
| `--pgbouncer-transaction-mode` | false | Treat the runtime URL as a PgBouncer transaction pool |
| `--host HOST` | `127.0.0.1` | Bind interface or host |
| `--port PORT` | `8600` | Bind port; zero requests an assigned port |
| `--auth MODE` | `none` | `none` or `trusted-header` |
| `--trusted-header NAME` | `X-Forwarded-User` | Trusted proxy identity header |
| `--enable-actions` | false | Enable task and workflow actions |
| `--custom-css-url URL` | -- | Load a stylesheet after the bundled styles |
| `--loglevel LEVEL` | `info` | debug, info, warning, error |

`--auth none` requires a loopback host. Trusted-header mode requires a reverse
proxy that strips or overwrites the identity header on every request.

```bash
# Local view-only dashboard
horsies web ./config/horsies.toml

# Trusted proxy with actions
horsies web ./config/horsies.toml \
  --host 127.0.0.1 \
  --auth trusted-header \
  --trusted-header X-Forwarded-User \
  --enable-actions

# PgBouncer transaction-pool deployment
horsies web \
  --database-url postgresql://app:secret@pool.example.com/horsies \
  --session-database-url postgresql://app:secret@db.example.com/horsies \
  --pgbouncer-transaction-mode
```

See [Deployment and Authentication](../monitoring/web-ui-deployment/) for the
router factory, auth policies, custom CSS, and schema states.

### horsies get-docs

Fetch the full documentation locally as markdown files, for AI agents (Claude
Code, Cursor, Copilot, etc.) that read docs without web requests.

```bash
horsies get-docs [OPTIONS]
```

**Options:**

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--output DIR` | .horsies-docs | Output directory |

**Examples:**

```bash
# Fetch docs to default location
horsies get-docs

# Custom output directory
horsies get-docs --output my-docs/

# Update existing docs (idempotent -- overwrites cleanly)
horsies get-docs
```

Uses git sparse checkout when git is available, falls back to tarball download
otherwise. No app instance or database connection required. On success, the
command prints `fetched N documentation files`.

### horsies cutover

Run the stages of the offline task-history cutover.

```bash
horsies cutover --database-url URL STAGE [OPTIONS]
```

`--database-url` is a direct PostgreSQL URL. Every stage that reads or writes
the database requires it. [Task-history cutover](../operations/cutover-runbook/)
gives the stages, their order, their options, and the backup procedure.

### horsies transcode

Run replacement-partition archive transcodes.

```bash
horsies transcode --database-url URL SUBCOMMAND [OPTIONS]
```

`--database-url` is a direct PostgreSQL URL. Every subcommand requires it.

| Subcommand | Options | Result |
| ---------- | ------- | ------ |
| `begin` | `--session-id` | Begins an archive-maintenance session |
| `plan` | `--job-id`, component and version options | Inventories the component and creates a durable, reversible job |
| `copy` | `--job-id`, `--batch-size` | Copies all remaining batches; commits each batch separately |
| `verify` | `--job-id` | Verifies the full replacement content and records identity tokens |
| `swap` | `--job-id` | Runs bounded, non-queuing swap attempts |
| `finalize` | `--job-id` | Drops the backup relations and retires the source decoder version |
| `finish` | `--session-id` | Ends maintenance after every job of the session is complete |
| `status` | `--job-id` | Prints the durable job and maintenance facts |
| `run` | `--session-id`, `--job-id`, component and version options, `--batch-size` | Runs `begin`, `plan`, `copy`, `verify`, `swap`, `finalize`, and `finish` in order |

| Option | Type | Default |
| ------ | ---- | ------- |
| `--session-id` | UUID | -- |
| `--job-id` | UUID | -- |
| `--component` | `history-row`, `result`, `attempts`, or `rerun-input` | -- |
| `--source-version` | `i16` | -- |
| `--target-version` | `i16` | -- |
| `--source-codec` | string | -- |
| `--target-codec` | string | -- |
| `--batch-size` | `i64` | `10000` |

The component and version options are `--component`, `--source-version`,
`--target-version`, `--source-codec`, and `--target-codec`. All five are
required for `plan` and `run`.

## Application Binary

Call these methods on `Horsies` after every task is registered.

| Method | Result |
| ------ | ------ |
| `app.check()` | Static validation of configuration, the task registry, and workflows. Returns `Result<(), HorsiesError>`. |
| `app.check_live().await` | `check()`, then connects to PostgreSQL, initializes the Horsies schema, and runs `SELECT 1`. Returns `AppResult<()>`. |
| `app.check_with(live).await` | `check_live()` when `live` is `true`, else `check()`. |
| `app.run_worker().await` | Runs a worker with `WorkerConfig::default()`. Consumes `app`. |
| `app.run_worker_with(config).await` | Runs a worker with `config`. Consumes `app`. |
| `app.run_scheduler().await` | Runs the scheduler. Consumes `app`. Returns an error when `AppConfig.schedule` is not set. |

The validation phases are in
[Startup Validation](../configuration/app-config/#startup-validation-appcheck).
The `WorkerConfig` fields are in [Concurrency](../workers/concurrency/).

`send_welcome_email` below is a `#[task]` function. See
[Defining Tasks](../tasks/defining-tasks/).

```rust
use horsies::{init_tracing, AppConfig, Horsies, LogLevel, WorkerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing(LogLevel::Info);

    let mut app = Horsies::new(AppConfig::for_database_url(
        "postgresql://user:password@localhost:5432/mydb",
    ))?;
    send_welcome_email::register(&mut app)?;

    app.check_live().await?;

    let worker_config = WorkerConfig {
        concurrency: 8,
        ..Default::default()
    };
    app.run_worker_with(worker_config).await?;
    Ok(())
}
```

A binary that runs the scheduler calls `app.run_scheduler().await?` in place of
`run_worker_with`.

## Agent Skills (Repository)

If you are using an AI coding agent from a source checkout, Horsies also ships
guidance-oriented skill files in:

`crates/horsies/.agents/skills/`

Available files:

- `SKILL.md` -- quick orientation and routing
- `practical-summary.md` -- condensed practical reference
- `tasks.md` -- task authoring, send/retry, serialization, error handling
- `workflows.md` -- DAG construction, handles, failure semantics, validation
- `configs.md` -- configuration, scheduling, CLI checks, environment variables

These files are documentation-focused (no bundled scripts) and are intended for
on-demand loading by agents that support markdown skill files.

## Process Signals

| Process | SIGINT (Ctrl+C) | SIGTERM |
| ------- | --------------- | ------- |
| `run_worker`, `run_worker_with` | Graceful shutdown | Graceful shutdown |
| `run_scheduler` | Graceful shutdown | Graceful shutdown |
| `horsies web` | Graceful shutdown | No handler; the default signal action ends the process |

SIGTERM handling is Unix-only. The worker shutdown steps are in
[Worker Architecture](/horsies/workers/worker-architecture/#graceful-shutdown). The
scheduler shutdown is in
[Scheduler Overview](/horsies/scheduling/scheduler-overview/#graceful-shutdown).

## Exit Codes

These codes apply to the `horsies` binary. An application binary sets its own
exit code.

| Code | Meaning |
| ---- | ------- |
| 0 | The command completed, or `--help` printed the usage |
| 1 | The command failed, or the command is `worker`, `scheduler`, or `check`; the error is on stderr |
| 2 | Invalid command-line arguments, or unsafe or invalid `horsies web` security arguments |

## Environment Variables

The binary reads no `DATABASE_URL` variable and loads no `.env` file. Pass the
database URL with `--database-url`, or pass a `CONFIG` file to `horsies web`.

| Variable | Effect |
| -------- | ------ |
| `RUST_LOG` | Log filter. A valid value takes precedence over `--loglevel` and over the `init_tracing` level. |
| `FORCE_COLOR` | Set, non-empty, and not `0`: log output is colored. Takes precedence over `NO_COLOR`. |
| `NO_COLOR` | Set: log output is not colored. |

With neither color variable set, log color follows whether stderr is a
terminal. These variables apply to `horsies web` and to an application binary
that calls `init_tracing`. `cutover`, `transcode`, and `get-docs` install no log
subscriber.

```bash
# Fine-grained log control
RUST_LOG=horsies=debug,sqlx=warn horsies web ./config/horsies.toml
```

## Deployment

### Systemd

```ini
[Unit]
Description=Horsies Worker
After=postgresql.service

[Service]
Type=simple
User=app
WorkingDirectory=/app
ExecStart=/usr/local/bin/myapp-worker
Restart=always
Environment=DATABASE_URL=postgresql://user:pass@localhost/db

[Install]
WantedBy=multi-user.target
```

### Docker

```dockerfile
FROM rust:1.75 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/myapp-worker /usr/local/bin/
CMD ["myapp-worker"]
```

### Procfile

```text
worker: ./target/release/myapp-worker
scheduler: ./target/release/myapp-scheduler
```

---
title: CLI Reference
summary: Command-line interface for workers, the scheduler, and monitoring.
related: [../workers/worker-architecture, ../scheduling/scheduler-overview, ./monitoring/web-ui-deployment]
tags: [cli, worker, scheduler, monitoring, commands]
---

Horsies provides a clap-based CLI (`horsies`) for running workers, the scheduler, and validation checks. In Rust, tasks are registered at compile time, so the CLI takes a configuration source (config file path or database URL) instead of a Python module path.

## Commands

### horsies worker

Start a task worker.

```bash
horsies worker [CONFIG] [OPTIONS]
```

**Arguments:**

| Argument | Description |
| -------- | ----------- |
| `CONFIG` | Path to a config file (TOML/JSON) or database URL (positional, optional) |

**Options:**

| Option | Default | Description |
| ------ | ------- | ----------- |
| `-m`, `--module CONFIG` | -- | Config source (alternative to positional argument; takes precedence) |
| `-q`, `--queues QUEUES` | `default` | Comma-separated queue names |
| `-c`, `--concurrency N` | CPU count | Maximum concurrent tasks |
| `--max-claim-batch N` | 2 | Max claims per queue per pass |
| `--max-claim-per-worker N` | 0 | Max total claimed tasks (0=auto) |
| `--coalesce-notifies N` | 100 | NOTIFY messages to drain per wake |
| `--loglevel LEVEL` | info | debug, info, warning, error |

**Examples:**

```bash
# Start worker with a config file
horsies worker ./config/horsies.toml

# Start worker with a database URL
horsies worker -m "postgresql://user:pass@localhost/mydb"

# Custom concurrency and queues
horsies worker ./config/horsies.toml -q high,low --concurrency 8

# Debug logging
horsies worker ./config/horsies.toml --loglevel debug

# Production settings
horsies worker ./config/horsies.toml --concurrency 8 --max-claim-batch 4 --loglevel warning
```

### Programmatic Alternative

In most Rust projects, you start the worker directly in your binary:

```rust
use horsies::{Horsies, AppConfig, WorkerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = Horsies::new(AppConfig::for_database_url("postgresql://..."))?;

    // Register tasks
    my_task::register(&mut app)?;

    // Start worker with defaults
    app.run_worker().await?;

    // Or with custom config
    let worker_config = WorkerConfig {
        concurrency: 8,
        queues: vec!["high".into(), "low".into()],
        ..Default::default()
    };
    app.run_worker_with(worker_config).await?;

    Ok(())
}
```

### horsies scheduler

Start the scheduler service.

```bash
horsies scheduler [CONFIG] [OPTIONS]
```

**Arguments:**

| Argument | Description |
| -------- | ----------- |
| `CONFIG` | Path to a config file or database URL |

**Options:**

| Option | Default | Description |
| ------ | ------- | ----------- |
| `-m`, `--module CONFIG` | -- | Config source (alternative to positional argument) |
| `--loglevel LEVEL` | info | debug, info, warning, error |
| `--check-interval N` | from config | Override check interval in seconds (1-60) |
| `--dry-run` | false | Validate schedules without starting the loop |

**Examples:**

```bash
# Start scheduler
horsies scheduler ./config/horsies.toml

# Debug logging
horsies scheduler ./config/horsies.toml --loglevel debug

# Validate schedules without running
horsies scheduler ./config/horsies.toml --dry-run
```

**Programmatic alternative:**

```rust
app.run_scheduler().await?;
```

### horsies check

Validate configuration, task registration, and optionally broker connectivity without starting services.

For details on validation phases, see [Startup Validation](../configuration/app-config#startup-validation-appcheck).

```bash
horsies check [CONFIG] [OPTIONS]
```

**Arguments:**

| Argument | Description |
| -------- | ----------- |
| `CONFIG` | Path to a config file or database URL |

**Options:**

| Option | Default | Description |
| ------ | ------- | ----------- |
| `-m`, `--module CONFIG` | -- | Config source (alternative to positional argument) |
| `--loglevel LEVEL` | warning | debug, info, warning, error |
| `--live` | false | Also connect to PostgreSQL, ensure the Horsies schema, and run `SELECT 1` |

**Examples:**

```bash
# Validate config and task registration
horsies check ./config/horsies.toml

# Include broker connectivity check
horsies check ./config/horsies.toml --live
```

**Programmatic alternative:**

```rust
app.check()?;
// or with live DB check:
app.check_live().await?;
```

### horsies web

Start the optional monitoring dashboard. Build the binary with the `web` Cargo
feature first.

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

See [Deployment and Authentication](./monitoring/web-ui-deployment/) for the
router factory, auth policies, custom CSS, and schema states.

### horsies get-docs

Fetch the full documentation locally as markdown files. Useful for AI agents (Claude Code, Cursor, Copilot, etc.) that need to read docs without web requests.

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

Uses git sparse checkout when git is available, falls back to tarball download otherwise. No app instance or database connection required.

## Agent Skills (Repository)

If you are using an AI coding agent from a source checkout, Horsies also ships
guidance-oriented skill files in:

`horsies/.agents/skills/`

Available files:

- `SKILL.md` -- quick orientation and routing
- `tasks.md` -- task authoring, send/retry, serialization, error handling
- `workflows.md` -- DAG construction, handles, failure semantics, validation
- `configs.md` -- configuration, scheduling, CLI checks, environment variables

These files are documentation-focused (no bundled scripts) and are intended for
on-demand loading by agents that support markdown skill files.

## Process Signals

Both commands handle graceful shutdown:

| Signal | Behavior |
| ------ | -------- |
| `SIGTERM` | Graceful shutdown |
| `SIGINT` (Ctrl+C) | Graceful shutdown |

Workers wait for running tasks to complete before exiting.

## Exit Codes

| Code | Meaning |
| ---- | ------- |
| 0 | Clean shutdown |
| 1 | Error (check logs) |
| 2 | Unsafe or invalid `horsies web` security arguments |

## Environment Variables

```bash
export DATABASE_URL=postgresql://...
horsies worker "$DATABASE_URL"
```

The CLI does not inject `DATABASE_URL` into config files or load `.env` automatically. Use a config file path as the positional argument, or pass the database URL directly as the config source.

Logging can be controlled with `RUST_LOG`:

```bash
# Fine-grained log control
RUST_LOG=horsies=debug,sqlx=warn horsies worker ./config/horsies.toml
```

When `RUST_LOG` is set, it takes precedence over the `--loglevel` flag.

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

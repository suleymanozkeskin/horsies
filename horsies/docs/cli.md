---
title: CLI Reference
summary: Command-line interface for running workers and the scheduler.
related: [workers/worker-architecture, scheduling/scheduler-overview]
tags: [cli, worker, scheduler, commands]
---

## Commands

### horsies worker

Start a task worker.

```bash
horsies worker <module> --profile NAME [OPTIONS]
```

**Arguments:**

| Argument | Description |
| -------- | ----------- |
| `module` | Python file or dotted module path containing Horsies instance |

**Options:**

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--profile NAME` | (required) | Name of the worker profile in `AppConfig.worker_profiles`. The profile holds the settings of the worker: children, recycling, claim limits, drain timeout and preload. See [Worker Profiles](workers/worker-profiles) |
| `--loglevel LEVEL` | INFO | DEBUG, INFO, WARNING, ERROR, CRITICAL |

There is no default profile. A missing or unknown `--profile` exits with code 1 and lists the defined profile names. The flags `--processes`, `--max-claim-batch`, `--max-claim-per-worker`, `--max-tasks-per-child`, `--max-memory-per-child-mb`, `--drain-timeout` and `--child-preload` were removed in 0.7.0; each one exits with code 2 and names the profile field that replaces it.

**Examples:**

```bash
# Start a worker with the 'default' profile (module:app format, recommended)
horsies worker myapp.instance:app --profile default

# Debug logging
horsies worker myapp.instance:app --profile default --loglevel=DEBUG

# Using file path (also supported)
horsies worker app/configs/instance.py:app --profile default
```

### horsies scheduler

Start the scheduler service.

```bash
horsies scheduler <module> [OPTIONS]
```

**Arguments:**

| Argument | Description |
| -------- | ----------- |
| `module` | Python file or dotted module path containing Horsies instance |

**Options:**

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--loglevel LEVEL` | INFO | DEBUG, INFO, WARNING, ERROR, CRITICAL |

**Examples:**

```bash
# Basic scheduler
horsies scheduler myapp.instance:app

# Debug logging
horsies scheduler myapp.instance:app --loglevel=DEBUG
```

### horsies check

Validate configuration, task registration, and optionally broker connectivity without starting services.

```bash
horsies check <module> [OPTIONS]
```

**Arguments:**

| Argument | Description |
| -------- | ----------- |
| `module` | Dotted module path or file path containing Horsies instance |

**Options:**

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--loglevel LEVEL` | WARNING | DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `--live` | false | Also test broker connectivity (SELECT 1), and compare each worker profile with the server connection limit |

**Examples:**

```bash
# Validate tasks and config
horsies check myapp.instance:app

# Include broker connectivity check
horsies check myapp.instance:app --live
```

### horsies web

Serve the monitoring web UI. Requires the `horsies[web]` extra. For the UI itself, see the [web UI overview](monitoring/web-ui-overview).

```bash
horsies web [app_path] [OPTIONS]
```

**Arguments:**

| Argument | Description |
| -------- | ----------- |
| `app_path` | App locator (for example `myapp.tasks:app`). Omit it when you use `--database-url` |

Pass either `app_path` or `--database-url`, not both. With `--database-url` the UI has no task registry and does not run schema migrations.

**Options:**

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--database-url URL` | none | Monitor a database without importing an app module |
| `--session-database-url URL` | none | Direct or session-capable URL when the main URL is pooled |
| `--pgbouncer-transaction-mode` | false | Disable prepared statements for a transaction-pooled URL |
| `--host HOST` | 127.0.0.1 | Bind address |
| `--port N` | 8600 | Bind port |
| `--auth MODE` | none | `none` (loopback hosts only) or `trusted-header` |
| `--trusted-header NAME` | X-Forwarded-User | Identity header that the proxy sets |
| `--enable-actions` | false | Allow task cancel, and workflow pause, resume and cancel (default is view-only) |
| `--loglevel LEVEL` | INFO | DEBUG, INFO, WARNING, ERROR, CRITICAL |

**Examples:**

```bash
# From your app module
horsies web myapp.tasks:app

# Monitor a database directly
horsies web --database-url postgresql+psycopg://user:pw@host/db

# Behind a proxy that authenticates callers and sets the header
horsies web myapp.tasks:app --host 0.0.0.0 \
    --auth trusted-header --trusted-header X-Forwarded-User --enable-actions
```

### horsies get-docs

Fetch documentation locally for AI agents.

```bash
horsies get-docs [OPTIONS]
```

**Options:**

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--output DIR` | .horsies-docs | Output directory for downloaded docs |
| `--ref main` | (none) | Download the `main` branch docs. Without it, the docs of the release tag `v{installed version}` are downloaded, and a development version fails |

**Examples:**

```bash
# Default output directory
horsies get-docs

# Custom output directory
horsies get-docs --output docs/horsies
```

### horsies skills install

Copy the agent skill folder from the installed package into a skills directory.

```bash
horsies skills install --target DIR [--force]
```

**Options:**

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--target DIR` | (required) | Skills directory of your agent tool. The skill is written to `DIR/horsies/` |
| `--force` | false | Replace an existing copy that differs from the packaged files |

**Examples:**

```bash
# Claude Code, project scope
horsies skills install --target .claude/skills

# Replace a copy that you or an older version changed
horsies skills install --target .claude/skills --force
```

Each agent tool reads a different folder, so `--target` has no default. The command creates `DIR` when it does not exist.

**Outcomes:**

| Case | Result | Exit code |
| ---- | ------ | --------- |
| `DIR/horsies/` is absent | Folder written | 0 |
| `DIR/horsies/` has the same files as the package | Nothing written | 0 |
| `DIR/horsies/` differs (edited, missing or extra files) | Nothing written. The message lists the differing paths | 1 |
| `DIR/horsies/` differs and `--force` is set | Folder replaced. Extra files are removed | 0 |
| `DIR` or `DIR/horsies` is a file | Nothing written | 1 |
| The package has no skill folder, or a filesystem call fails | Message names the path and the cause | 1 |

## Module Formats

The recommended format is a dotted module path with an explicit variable name:

```bash
horsies worker myapp.instance:app --profile default
```

Where `app` is the variable name of your `Horsies` instance in the module.

File paths are also supported:

```bash
horsies worker app/configs/instance.py:app --profile default
```

**Do not** mix dotted module notation with a `.py` extension (e.g., `myapp.instance.py`). The CLI will reject this with a helpful error.

If the module contains exactly one `Horsies` instance, the `:name` suffix can be omitted — the CLI will auto-discover it. If multiple instances exist, the suffix is required.

## Module Discovery

```python
# instance.py
from horsies import Horsies, AppConfig, PostgresConfig

app = Horsies(AppConfig(...))  # Auto-discovered

@app.task("my_task")
def my_task():
    ...

app.discover_tasks(["myapp.tasks"])
```

Requirements:

- Module must have exactly one `Horsies` instance (or use `:name` suffix)
- Instance can have any variable name

## Process Signals

Both commands handle graceful shutdown:

| Signal | Behavior |
| ------ | -------- |
| `SIGTERM` | Graceful shutdown |
| `SIGINT` (Ctrl+C) | Graceful shutdown |

Workers wait up to the `drain_timeout` of their profile (default 300 seconds) for running tasks, then kill their children.

## Exit Codes

| Code | Meaning |
| ---- | ------- |
| 0 | Clean shutdown |
| 1 | Error (check logs), including a missing or unknown `--profile` |
| 2 | Invalid `horsies web` arguments, a removed worker flag, or an argparse error |
| 77 | `horsies worker`: the worker could not write runner heartbeats, and it had not stopped 120 seconds after it started to stop. See [When the Worker Cannot Write Heartbeats](workers/heartbeats-recovery#when-the-worker-cannot-write-heartbeats) |

## Environment Variables

```bash
DATABASE_URL=postgresql+psycopg://... horsies worker myapp.instance:app --profile default
```

Use in config:

```python
import os

from pydantic import SecretStr

config = AppConfig(
    broker=PostgresConfig(database_url=SecretStr(os.environ["DATABASE_URL"])),
)
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
ExecStart=/usr/bin/horsies worker myapp.instance:app --profile default
Restart=always

[Install]
WantedBy=multi-user.target
```

### Docker

```dockerfile
FROM python:3.13

WORKDIR /app
COPY . .
RUN pip install horsies

CMD ["horsies", "worker", "myapp.instance:app", "--profile", "default"]
```

### Procfile

```text
worker: horsies worker myapp.instance:app --profile default
scheduler: horsies scheduler myapp.instance:app
```

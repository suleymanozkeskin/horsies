---
title: CLI Reference
summary: Command-line interface for running workers and the scheduler.
related: [../workers/worker-architecture, ../scheduling/scheduler-overview]
tags: [cli, worker, scheduler, commands]
---

## Commands

### horsies worker

Start a task worker.

```bash
horsies worker <module> [OPTIONS]
```

**Arguments:**

| Argument | Description |
| -------- | ----------- |
| `module` | Python file or dotted module path containing Horsies instance |

**Options:**

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--processes N` | 1 | Number of worker processes |
| `--loglevel LEVEL` | INFO | DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `--max-claim-batch N` | 2 | Max claims per queue per pass |
| `--max-claim-per-worker N` | 0 | Max total claimed tasks (0=auto) |

**Examples:**

```bash
# Basic worker (module:app format, recommended)
horsies worker myapp.instance:app

# With 8 processes
horsies worker myapp.instance:app --processes=8

# Debug logging
horsies worker myapp.instance:app --loglevel=DEBUG

# Production settings
horsies worker myapp.instance:app --processes=8 --max-claim-batch=4 --loglevel=WARNING

# Using file path (also supported)
horsies worker app/configs/instance.py:app
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

Validate configuration, task registration, workflow structure, and optionally broker connectivity without starting services. Runs the same validation that workers and schedulers perform at startup.

For details on validation phases, `@app.workflow_builder`, the guarantee model, and CI usage, see [Startup Validation](../configuration/app-config#startup-validation-appcheck).

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
| `--live` | false | Also test broker connectivity (SELECT 1) |

**Examples:**

```bash
# Validate tasks, config, and workflows
horsies check myapp.instance:app

# Include broker connectivity check
horsies check myapp.instance:app --live
```

## Module Formats

The recommended format is a dotted module path with an explicit variable name:

```bash
horsies worker myapp.instance:app
```

Where `app` is the variable name of your `Horsies` instance in the module.

File paths are also supported:

```bash
horsies worker app/configs/instance.py:app
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

Workers wait for running tasks to complete before exiting.

## Exit Codes

| Code | Meaning |
| ---- | ------- |
| 0 | Clean shutdown |
| 1 | Error (check logs) |

## Environment Variables

```bash
DATABASE_URL=postgresql+psycopg://... horsies worker myapp.instance:app
```

Use in config:

```python
import os

config = AppConfig(
    broker=PostgresConfig(database_url=os.environ["DATABASE_URL"]),
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
ExecStart=/usr/bin/horsies worker myapp.instance:app --processes=8
Restart=always

[Install]
WantedBy=multi-user.target
```

### Docker

```dockerfile
FROM python:3.11

WORKDIR /app
COPY . .
RUN pip install horsies

CMD ["horsies", "worker", "myapp.instance:app", "--processes=8"]
```

### Procfile

```text
worker: horsies worker myapp.instance:app --processes=8
scheduler: horsies scheduler myapp.instance:app
```

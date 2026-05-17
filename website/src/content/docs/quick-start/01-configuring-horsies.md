---
title: Configuring Horsies
summary: Set up the app instance and broker connection.
related: [02-producing-tasks]
tags: [quickstart, configuration]
---

## Prerequisites

- PostgreSQL 12+
- Python 3.13+

## Installation

```bash
uv add horsies
```

## Basic Configuration

```python
from horsies import Horsies, AppConfig, PostgresConfig

config = AppConfig(
    broker=PostgresConfig(
        database_url="postgresql+psycopg://user:password@localhost:5432/mydb",
    ),
)

app = Horsies(config)
```

## PgBouncer / Transaction Pooling

If your provider gives you a transaction-pooled PgBouncer URL, keep ordinary SQL
traffic on that pooled URL and give Horsies a second direct/session-capable URL
for schema setup and `LISTEN`/`NOTIFY`.

```python
from horsies import Horsies, AppConfig, PostgresConfig

config = AppConfig(
    broker=PostgresConfig(
        database_url=os.environ["DATABASE_URL_POOLED"],
        session_database_url=os.environ["DATABASE_URL_DIRECT"],
        pgbouncer_transaction_mode=True,
    ),
)

app = Horsies(config)
```

Managed providers, such as PlanetScale Postgres, may show separate pooled and
direct connection strings. Verify the current ports and URLs in your provider
dashboard or docs. Workers do not support a PgBouncer-only URL because
PostgreSQL `LISTEN` requires a persistent session.

## Custom Queues with Priorities

Different operations have different urgency levels can be defined with priority values.
1-100 where 1 is priority numero uno.

```python
from horsies import Horsies, AppConfig, PostgresConfig, QueueMode, CustomQueueConfig

config = AppConfig(
    queue_mode=QueueMode.CUSTOM,
    custom_queues=[
        CustomQueueConfig(name="urgent", priority=1, max_concurrency=10),
        CustomQueueConfig(name="standard", priority=50, max_concurrency=20),
        CustomQueueConfig(name="low", priority=100, max_concurrency=5),
    ],
    broker=PostgresConfig(
        database_url="postgresql+psycopg://user:password@localhost:5432/db_name",
    ),
)

app = Horsies(config) # use this app instance for decorating task
```

| Queue | Priority | Use Case |
|-------|----------|----------|
| `urgent` | 1 | The most important queue |
| `standard` | 50 | Things in between |
| `low` | 100 | The least important, can wait |

## Task Discovery

```python
app.discover_tasks([
    "myapp.tasks",
    "myapp.workflows",
])
```

`discover_tasks` records module paths for later import by the worker. Dotted module paths use `importlib.import_module()`, while `.py` entries are imported by file path — it does **not** recursively scan submodules.

To discover tasks in `myapp.tasks.scraping`, either list it explicitly:

```python
app.discover_tasks([
    "myapp.tasks",
    "myapp.tasks.scraping",
    "myapp.workflows",
])
```

Or export the decorated functions from `myapp.tasks.__init__.py` so importing `myapp.tasks` triggers the decorator registration.

## Running the Worker

```bash
horsies worker myapp.config:app --processes=8 --loglevel=INFO
```

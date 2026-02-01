---
title: Configuring Horsies
summary: Set up the app instance and broker connection.
related: [02-producing-tasks]
tags: [quickstart, configuration]
---

## Prerequisites

- PostgreSQL 12+
- Python 3.10+

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

## Running the Worker

```bash
horsies worker myapp.config:app --processes=8 --loglevel=INFO
```

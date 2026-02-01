---
title: Broker Config
summary: PostgreSQL connection settings via PostgresConfig.
related: [app-config, ../../internals/postgres-broker]
tags: [configuration, PostgresConfig, database]
---

## Basic Usage

```python
from horsies import PostgresConfig

broker = PostgresConfig(
    database_url="postgresql+psycopg://user:password@localhost:5432/mydb",
)
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database_url` | `str` | required | SQLAlchemy connection URL |
| `pool_size` | `int` | 5 | Connection pool size |
| `max_overflow` | `int` | 10 | Additional connections beyond pool_size |
| `pool_timeout` | `int` | 30 | Seconds to wait for connection |
| `pool_recycle` | `int` | 1800 | Recycle connections after N seconds |

## Connection URL Format

```
postgresql+psycopg://user:password@host:port/database
```

Components:

- `postgresql+psycopg` - Driver (psycopg3)
- `user:password` - Credentials
- `host:port` - Server location (default port: 5432)
- `database` - Database name

## Examples

### Local Development

```python
PostgresConfig(
    database_url="postgresql+psycopg://postgres:postgres@localhost:5432/horsies_dev",
)
```

### Production with Connection Pool

```python
PostgresConfig(
    database_url="postgresql+psycopg://app:secret@db.example.com:5432/production",
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
)
```

### From Environment Variable

```python
import os

PostgresConfig(
    database_url=os.environ["DATABASE_URL"],
)
```

## Connection Pooling

The broker uses SQLAlchemy's async connection pool:

- `pool_size`: Base number of persistent connections
- `max_overflow`: Additional connections created under load (temporary)
- `pool_timeout`: How long to wait if all connections are busy
- `pool_recycle`: Close and recreate connections after this many seconds

### Sizing Guidelines

| Deployment | pool_size | max_overflow |
|------------|-----------|--------------|
| Development | 2-5 | 5 |
| Small production | 5-10 | 10-20 |
| High traffic | 10-20 | 20-40 |

Consider: `pool_size + max_overflow` should not exceed PostgreSQL's `max_connections` (divided by number of worker processes).

## Multiple Components

The broker creates two connection types:

1. **Async engine** (SQLAlchemy): For queries, inserts, updates
2. **LISTEN/NOTIFY** (psycopg): For real-time notifications

Both share the same `database_url`.

## Health Monitoring

The broker includes automatic health checks:

- Proactive connection monitoring
- Auto-reconnect on connection loss
- Exponential backoff for reconnection attempts

## Schema Initialization

On first use, the broker creates required tables:

- `horsies_tasks` - Task storage
- `horsies_heartbeats` - Liveness tracking
- `horsies_worker_states` - Worker monitoring
- `horsies_schedule_state` - Scheduler state

Uses PostgreSQL advisory locks to prevent race conditions during schema creation.

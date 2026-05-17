---
title: PgBouncer Troubleshooting
summary: Fix PgBouncer transaction pooling issues with Horsies and Syce.
related: [../configuration/broker-config, ../monitoring/syce-overview, ../tasks/errors]
tags: [troubleshooting, pgbouncer, planetscale, postgres]
---

# PgBouncer Troubleshooting

Horsies supports PgBouncer transaction pooling only with a second
session-capable PostgreSQL URL.

```python
from horsies import AppConfig, PostgresConfig

config = AppConfig(
    broker=PostgresConfig(
        database_url=os.environ["DATABASE_URL_POOLED"],
        session_database_url=os.environ["DATABASE_URL_DIRECT"],
        pgbouncer_transaction_mode=True,
    ),
)
```

Managed providers, such as PlanetScale Postgres, may show separate pooled and
direct connection strings. Verify the current ports and URLs in your provider
dashboard or docs before copying them into Horsies.

## Error: session_database_url required

```text
session_database_url required when pgbouncer_transaction_mode=True
```

`pgbouncer_transaction_mode=True` tells Horsies that `database_url` may point to
transaction pooling. Workers still need PostgreSQL `LISTEN`, and `LISTEN`
requires a persistent session. Set `session_database_url` to a direct Postgres
URL or a session-pooling URL.

## Error: Postgres LISTEN failed

```text
Postgres LISTEN failed. If database_url points to PgBouncer transaction pooling,
set PostgresConfig.session_database_url to a direct/session-capable Postgres URL.
```

The worker could not subscribe to PostgreSQL notifications. Check that
`session_database_url` does not point to transaction-pooled PgBouncer. Horsies
workers intentionally fail startup here; poll-only workers are not supported.

## Warning: LISTEN unavailable

```text
LISTEN unavailable; falling back to polling
```

Producer-side `handle.get()` and workflow waits can poll when notifications are
unavailable. This keeps result retrieval working, but it is not worker dispatch
support. Fix the session URL if workers or Syce need real-time notifications.

## Live Check: Notification Not Delivered

```text
session_database_url appears to be transaction-pooled; LISTEN notification was not delivered
```

The URL accepted `LISTEN` and `NOTIFY` SQL, but the live behavior probe did not
receive a notification within the bounded timeout. This is the common signature
of transaction pooling. Use the direct Postgres URL for `session_database_url`.

## Prepared Statement Errors

If the pooled URL is used without `pgbouncer_transaction_mode=True`, operators
may see prepared-statement or statement-cache errors from the pooler. Enable the
flag:

```python
PostgresConfig(
    database_url=os.environ["DATABASE_URL_POOLED"],
    session_database_url=os.environ["DATABASE_URL_DIRECT"],
    pgbouncer_transaction_mode=True,
)
```

This disables psycopg prepared statements for Horsies runtime SQL. In Syce, pass
`--pgbouncer-transaction-mode` to disable SQLx's PostgreSQL statement cache.

```bash
syce \
  --database-url "$DATABASE_URL_POOLED" \
  --session-database-url "$DATABASE_URL_DIRECT" \
  --pgbouncer-transaction-mode
```

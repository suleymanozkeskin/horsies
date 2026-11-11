---
title: PgBouncer Troubleshooting
summary: Fix PgBouncer transaction pooling issues with Horsies and Syce, and size the PgBouncer pool for the workers.
related: [../configuration/broker-config, ../configuration/remote-postgres, ../workers/worker-profiles, ../monitoring/syce-overview, ../tasks/errors]
tags: [troubleshooting, pgbouncer, planetscale, postgres]
---

# PgBouncer Troubleshooting

Horsies supports PgBouncer transaction pooling only with a second
session-capable PostgreSQL URL.

```python
from pydantic import SecretStr
from horsies import AppConfig, PostgresConfig

config = AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr(os.environ["DATABASE_URL_POOLED"]),
        session_database_url=SecretStr(os.environ["DATABASE_URL_DIRECT"]),
        pgbouncer_transaction_mode=True,
    ),
)
```

Managed providers, such as PlanetScale Postgres, may show separate pooled and
direct connection strings. Verify the current ports and URLs in your provider
dashboard or docs before copying them into Horsies.

## Pool Size

With `pgbouncer_transaction_mode=True`, `horsies check --live` and the worker
boot do not compare connections with the server (`HRS-219`): a client
connection to PgBouncer holds a server connection only during a transaction.
PgBouncer opens at most `default_pool_size` server connections for each
database and user pair. A transaction that finds them all in use waits in
PgBouncer.

Each worker opens up to this many client connections to the pooled
`database_url`:

```text
worker_pool_size + worker_max_overflow                  # parent pool
+ 1                                                     # runner heartbeat
+ isolated children × isolated.pool max_size            # isolated child pools
+ shared async children × shared_async.pool max_size    # shared async child pools
+ shared thread children × shared_thread.pool max_size  # shared thread child pools
```

The 2 listener connections go to `session_database_url`, not through
PgBouncer. Each `max_size` is the resolved value of its mode; see
[Connections](/horsies/workers/worker-profiles/#connections).

- Set `max_client_conn` to at least the sum of this number over all workers,
  plus the producers and the other clients of the pooled URL.
- A `default_pool_size` equal to that sum never makes a transaction wait in
  PgBouncer. A smaller value uses fewer server connections, and a
  transaction waits when every server connection is in use.

<!-- PLACEHOLDER default_pool_size rule: replace this comment with the measured rule for default_pool_size. -->

## Error: session_database_url required

```text
session_database_url required when pgbouncer_transaction_mode=True
```

`pgbouncer_transaction_mode=True` tells Horsies that `database_url` may point to
transaction pooling. Workers still need PostgreSQL `LISTEN`, and `LISTEN`
requires a persistent session. Partition detach and finalize also use session
advisory locks. Set `session_database_url` to a direct Postgres URL or a
session-pooling URL.

## Error: session URL matches the transaction URL

```text
session_database_url must not use the transaction-pool URL
```

Use two different URLs. Put the transaction-pool URL in `database_url`. Put a
direct or session-pooled URL in `session_database_url`.

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
    database_url=SecretStr(os.environ["DATABASE_URL_POOLED"]),
    session_database_url=SecretStr(os.environ["DATABASE_URL_DIRECT"]),
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

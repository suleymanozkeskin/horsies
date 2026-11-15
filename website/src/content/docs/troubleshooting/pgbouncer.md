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
pool max_size                                           # pool of the worker process
+ 1                                                     # runner heartbeat
+ isolated children × isolated.pool max_size            # isolated child pools
+ shared async children × shared_async.pool max_size    # shared async child pools
+ shared thread children × shared_thread.pool max_size  # shared thread child pools
```

Task code that sends through the app adds the `task_send_pool` of its child
kind for each event loop that it sends from, also to the pooled
`database_url`. The 2 listener connections go to `session_database_url`, not
through PgBouncer. Each `max_size` is the resolved value; see
[Connections](/horsies/workers/worker-profiles/#connections).

A larger pool of the worker process adds client connections, not server
connections: a client holds a server connection only from `BEGIN` to
`COMMIT`. If `default_pool_size` is below the server connections that the
workers use at once, the wait for a connection moves from the pool of the
worker process to PgBouncer, and the larger pool gives no gain.

- Set `max_client_conn` to at least the sum of this number over all workers,
  plus the producers and the other clients of the pooled URL.
- A `default_pool_size` equal to that sum never makes a transaction wait in
  PgBouncer. A smaller value uses fewer server connections, and a
  transaction waits when every server connection is in use.

### `default_pool_size`

PgBouncer in transaction mode gives a client a server connection for one
transaction. Most writes of a worker are one statement in autocommit, so each
is a transaction of its own: the RUNNING update and the finalizing marker of
each task (in its child), the claim, and the batched write of plain successes
(in the worker process). Such a transaction holds a server connection while
PgBouncer forwards the statement and Postgres runs it. The round trip between
the worker and PgBouncer is not part of it.

```text
server connections in use = statements per second of all workers
                            × (RTT between PgBouncer and Postgres + server time of one statement)
statements per second of one worker = 2 × tasks per second            # RUNNING update and marker, in the children
                                      + statements of the worker process # 0.05 to 1.5 per plain success, 2 per failure
```

Set `default_pool_size` to about 8 times this average, summed over the
workers behind PgBouncer, and not above the sum of their client pools. The
average hides bursts: tasks that start together end together and write
together.

Measured with each worker 3.5 ms from Postgres 17 (in a 4-vCPU VM), tasks of
0.1 s, statements counted at the client, and the statements that run at once
read from `pg_stat_activity` every 0.5 s:

| Worker | Tasks/s | Statements/s | Client connections busy, mean | Statements running at once in Postgres, mean / max | Server time of one statement |
|--------|---------|--------------|-------------------------------|----------------------------------------------------|------------------------------|
| 1 shared async child, 100 seats, pool 10 | 790-810 | 2,000-2,100 | 9.5-9.7 | 0.8-1.1 / 3-5 | 0.4-0.55 ms |
| 1 shared async child, 1000 seats, pool 40 | 1,700-1,760 | 3,560-3,730 | 39 | 3.6-4.5 / 27-30 | 1.0-1.3 ms |

With PgBouncer next to Postgres (RTT about 0), the formula gives the mean
column: about a tenth of the client connections that the worker keeps busy.
8 times the mean is above the max column of these runs. These runs went
direct to Postgres; the numbers apply to PgBouncer through the formula, not
through a run behind it.

When the database CPU is the limit, statements queue inside Postgres, and the
number that run at once grows with the connections that send them. 4 shared
async children of 250 seats at 1,740 tasks/s ran 15-19 statements at once on
average (4-5 ms each), with the database VM at 345-356 % of 4 cores. More
server connections then add no throughput.

- A transaction of more than one statement holds its server connection across
  the round trips between the worker and PgBouncer too: the RUNNING update of
  a workflow task (`BEGIN`, update, node, `COMMIT`: 4 round trips) and the
  other writes of a workflow task that run as one transaction. Count each as
  its round trips × the RTT between the worker and PgBouncer.
- The 2 listener connections of each worker go to `session_database_url`, not
  through PgBouncer: `LISTEN` needs a session, and transaction pooling does not
  keep one. They are not part of `default_pool_size` or of the client
  connections above. They use 2 direct Postgres slots per worker. For a worker
  without PgBouncer, `HRS-219` counts them as `listener 2`; with
  `pgbouncer_transaction_mode=True` it compares nothing.

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

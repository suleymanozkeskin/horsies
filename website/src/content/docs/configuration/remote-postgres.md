---
title: Remote PostgreSQL
summary: Deploy workers against a remote or managed PostgreSQL — connection budgeting, pooled multi-worker setups, prefetch, and health-check tuning.
related: [broker-config, ../workers/concurrency, ../troubleshooting/pgbouncer]
tags: [configuration, deployment, postgres, pgbouncer, connections, latency]
---

# Remote PostgreSQL

Running workers against a remote PostgreSQL — a managed provider or a
database in another network — changes two constraints that are invisible
locally: every connection counts against a server-side cap, and every
statement pays the network round trip. This page covers sizing workers
against `max_connections`, running multiple workers through a transaction
pooler, and the configuration that recovers throughput at high RTT.

## Connection Budget

Each worker holds, under load:

```text
worker_pool_size + worker_max_overflow   # coordinator engine
+ 2                                      # LISTEN connections (tasks, ping)
+ processes × worker_child_pool_max_size # per-child pools
```

At defaults (`worker_pool_size=3`, `worker_max_overflow=2`,
`worker_child_pool_max_size=2`) with `--processes=8`:

```text
3 + 2 + 2 + 8 × 2 = 23 connections per worker
```

Producers hold their own engine pool (`pool_size + max_overflow`, default
5 + 10) per process.

Size against the server's effective capacity, not its nominal
`max_connections`: subtract superuser-reserved slots and any connections
held by the provider's infrastructure. A managed instance with
`max_connections=50` fits one 8-process worker on direct connections —
not two.

When workers exhaust the server's slots, connection attempts fail with:

```text
FATAL: remaining connection slots are reserved for roles with the SUPERUSER attribute
```

Workers retry through this, but tasks claimed during the outage wait for
[recovery](../workers/heartbeats-recovery) instead of completing promptly.
If this appears in worker logs, the deployment needs a pooler or fewer
direct consumers.

## Multiple Workers: Transaction Pooling

Run multiple workers against a slot-capped server through a transaction-mode
pooler (PgBouncer or the provider's built-in equivalent). Point
`database_url` at the pooled endpoint and `session_database_url` at a direct
endpoint — `LISTEN` requires a real session and does not survive transaction
pooling:

```python
import os

from horsies import AppConfig, PostgresConfig

config = AppConfig(
    broker=PostgresConfig(
        database_url=os.environ['DATABASE_URL_POOLED'],
        session_database_url=os.environ['DATABASE_URL_DIRECT'],
        pgbouncer_transaction_mode=True,
    ),
)
```

Direct slots consumed per worker drop to the 2 `LISTEN` connections;
everything else multiplexes over the pooler's server pool. See
[PgBouncer Troubleshooting](../troubleshooting/pgbouncer) for
provider-specific endpoints and failure modes.

Transaction pooling taxes per-worker throughput: a server connection is
pinned for the whole transaction, and at high RTT the statements of one
transaction arrive a round trip apart, so server connections sit idle
in transaction between them. Expect a single pooled worker to run
~25–35% slower than the same worker on a direct connection. The pooler
still wins past one worker — it is the only way to run several workers
inside a small slot budget.

## Prefetch Hides Round Trips

With `prefetch_buffer=0` (the default), a worker claims new tasks only
when a process slot is free, and each claim pass costs several round
trips. At 30ms+ RTT this serializes into the task rate and caps
throughput near `processes` tasks per second regardless of task duration.

Set a prefetch buffer and claim lease for remote deployments:

```python
from horsies import AppConfig, PostgresConfig

config = AppConfig(
    prefetch_buffer=24,        # claim ahead: 2–3× processes
    claim_lease_ms=120_000,    # required when prefetch_buffer > 0
    broker=PostgresConfig(database_url=os.environ['DATABASE_URL']),
)
```

At 33ms RTT this raises drain throughput roughly 2–3× over the default.
Soft-cap semantics and fairness trade-offs are covered in
[Concurrency](../workers/concurrency).

## Health Checks at High RTT

Two settings health-check pooled connections on every checkout, costing
one round trip each:

| Setting | Default | Scope |
|---------|---------|-------|
| `pool_pre_ping` | `True` | coordinator and producer engines |
| `worker_child_pool_check` | `True` | per-child psycopg pools |

On a low-latency network the cost is unmeasurable. At 30ms+ RTT the
checks consume roughly 20% of the per-task statement budget. Disable both
for busy remote workers:

```python
from horsies import AppConfig, PostgresConfig

config = AppConfig(
    broker=PostgresConfig(
        database_url=os.environ['DATABASE_URL'],
        pool_pre_ping=False,
        worker_child_pool_check=False,
    ),
)
```

Without checks, a connection that died while idle surfaces as a failed
first statement instead of a transparent reconnect. Coordinator
operations retry through the worker's resilience machinery; a failed
pre-execution transition leaves the task `CLAIMED` until the
[reaper](../workers/heartbeats-recovery) requeues it
(`claimed_stale_threshold_ms`, default 120s). Keep the checks enabled for
workers that sit idle for long stretches behind NAT or load balancers
that kill idle connections — those are the deployments where stale
connections actually occur.

## Things to Avoid

**Don't run multiple direct-connection workers against a small `max_connections`.**
Each worker needs its full budget at load; two workers on a 50-slot
instance exhaust it and stall both. Use the pooled setup above.

**Don't enable prefetch without a claim lease.**
`prefetch_buffer > 0` requires `claim_lease_ms`; prefetched claims without
a lease have no expiry for other workers to reclaim.

**Don't point `session_database_url` at the pooled endpoint.**
`LISTEN` registrations vanish when the pooler reassigns the server
connection; workers stop receiving wake notifications and fall back to
polling intervals.

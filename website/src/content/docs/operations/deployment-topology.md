---
title: Deployment Topology
section: shared
summary: Where to run the horsies database - a dedicated PostgreSQL instance and why, the transaction boundary with the application, the connections of each URL, and what a failover under asynchronous replication can lose.
related: [atomic-enqueue-and-topology, ../configuration/remote-postgres, ../troubleshooting/pgbouncer, ../workers/heartbeats-recovery, ../configuration/retention-config, ../tasks/retry-policy]
tags: [operations, topology, deployment, postgres, dedicated instance, failover, replication, synchronous_commit, synchronous_standby_names]
---

# Deployment Topology

PostgreSQL holds the correctness contract of horsies. A claim takes a new claim generation, and each later write of the attempt is fenced on it. The end of a task is one transaction. Recovery reads rows and timestamps, so no task state exists only in a process. Run horsies on a dedicated PostgreSQL instance, and give that instance replication settings that keep each committed transaction.

## Why and When

Read this page before the first production deployment of horsies. Read it again before a change to the replication or failover settings of the horsies database.

## Use a Dedicated Instance

Run horsies on its own PostgreSQL instance, apart from the instance of the application database. Each row of the table is one reason:

| Resource | What horsies uses | On a dedicated instance |
| -------- | ----------------- | ----------------------- |
| Connections | A worker with 8 isolated children holds 16 connections at the defaults. One of them is the session of its liveness lock. A producer holds a pool of up to `pool_size + max_overflow` connections | The connections count against the `max_connections` of the horsies instance only |
| Writes and vacuum | Each task writes and deletes rows in tables with a high rate of change: the claim, the heartbeats, the attempt, the move to history. Partition maintenance creates and drops partitions every `partition_maintenance_interval_s` | The WAL, the autovacuum workers and the table settings of [Autovacuum Tuning](/horsies/operations/autovacuum-tuning/) serve horsies only |
| Commit durability | Each claim pass, attempt start and task end is a commit | The settings of [Settings That Lose No Commit](#settings-that-lose-no-commit) apply to horsies only. They add no standby round trip to the commits of the application |
| Access | Write access to `horsies_tasks` is the privilege to run tasks | The roles of the instance limit who can write the horsies tables. See [Trust Boundary](/horsies/internals/database-schema/#trust-boundary) |

A send never joins the transaction of its caller, so a dedicated instance changes no send guarantee. See [The Transaction Boundary](#the-transaction-boundary).

## The Transaction Boundary

A send and a workflow start take a connection from the broker pool and commit alone. The application write and the enqueue are two transactions on every topology. On a dedicated instance, they are also on two servers.

Use the pattern of [Atomic Enqueue and Topology](/horsies/operations/atomic-enqueue-and-topology/):

1. Commit the application write. Then send. See [Send After Commit](/horsies/operations/atomic-enqueue-and-topology/#send-after-commit).
2. Set `resend_on_transient_err`. A retryable enqueue failure is then sent again inside the send call. See [Resend Transient Failures](/horsies/operations/atomic-enqueue-and-topology/#resend-transient-failures).
3. Pass an idempotency key that you derive from the application write. A repeat of the send then returns the first task. See [Make the Send Repeatable](/horsies/operations/atomic-enqueue-and-topology/#make-the-send-repeatable-with-an-idempotency-key).

## Connections

Each URL of the broker config serves a different set of connections:

| URL | Connections | Pooler mode |
| --- | ----------- | ----------- |
| `database_url` | The pool of the worker process, the runner heartbeat connection, the pools of the children, and the pool of a producer | Direct, session mode, or transaction mode with `pgbouncer_transaction_mode=True` |
| `session_database_url`, else `database_url` | The listener (`LISTEN`), the worker liveness lock, and the session work: the schema install and partition maintenance | Direct or session mode. Never transaction mode: these connections keep session state |

A session-mode pooler holds one server connection for each client connection. It does not decrease the number of server connections.

The sizes and the rules are on these pages:

- The connections of one worker and of one producer: [Connection Budget](/horsies/configuration/remote-postgres/#connection-budget).
- The pool sizes of a worker profile: [Connections](/horsies/workers/worker-profiles/#connections).
- The liveness lock: [Worker Liveness Lock](/horsies/workers/heartbeats-recovery/#worker-liveness-lock).
- Several workers through a transaction pooler: [Multiple Workers: Transaction Pooling](/horsies/configuration/remote-postgres/#multiple-workers-transaction-pooling) and [Pool Size](/horsies/troubleshooting/pgbouncer/#pool-size).

## Failover

A failover promotes a standby to primary. Under asynchronous replication, the standby can lack the last transactions that the old primary committed and acknowledged. After the failover, these transactions do not exist. Horsies cannot detect the loss, because the new primary is its only record.

A commit whose reply did not arrive is a different case. The write fails with a retryable connection error, and it is sent again on a new connection:

- The worker sends a failed write of a task end again, up to the retry bound of its stage.
- A send returns a retryable error. `resend_on_transient_err`, or the retry method of the client, sends it again with the same task id.

The repeat applies the write once. A write that reached the new primary is not applied a second time.

### Settings That Lose No Commit

- Name the standby that the failover promotes in `synchronous_standby_names` on the primary.
- Keep `synchronous_commit` at `on` (the PostgreSQL default), or set it to `remote_apply`. A commit then returns only after the standby has the commit in durable storage.
- Let the failover manager promote only a synchronous standby. On a managed service, use a high-availability mode that the provider states loses no committed transaction at failover.
- Fence the old primary at failover. A process that still reaches the old primary writes there, and the new primary does not get these writes.

Horsies does not set `synchronous_commit`. The value of the server, the database or the role applies to each horsies connection.

With synchronous replication, each commit waits for one round trip to the standby. Each claim pass, attempt start and task end is a commit, so each of them takes this round trip. Put the standby near the primary. While no synchronous standby is connected, each commit waits.

### What an Asynchronous Failover Can Lose

Each row is one transaction that the old primary committed and the standby did not receive.

| Lost transaction | State on the new primary | What follows |
| ---------------- | ------------------------ | ------------ |
| A send. The task row and the key reservation of its idempotency key are one statement | No row holds the task id or the key | A repeat of the send with the same key is a first send: it enqueues a new task. If a worker ran the first task before the failover, the task body runs again. Without a repeat, no task exists for the send |
| A workflow start | No row holds the workflow id | A start with the same workflow id starts a new workflow. Tasks of the first run that ran before the failover run again |
| A claim, or the start of an attempt | The row is `PENDING`, or `CLAIMED` by the same worker | The row is claimed again: a `PENDING` row at once, a `CLAIMED` row when its claim lease ends or when recovery returns it to `PENDING`. A second run can start while the first run still runs. The end of the first run fails its fence (`RUNNING`, owner, claim generation) and writes nothing |
| The end of a task: the attempt, the history record and the delete of the live row, in one transaction | The row is `RUNNING` again, owned by the worker that ran it. When earlier writes were lost too, the row is in an earlier state | No heartbeat covers the row, because the worker no longer holds the task. With `auto_fail_stale_running=True`, recovery ends the attempt with `WORKER_CRASHED`: after `running_stale_threshold_ms`, or after `finalizing_stale_threshold_ms` when the finalizing mark of the row was kept. See the next list |
| The workflow progress of a finished task: its node, the enqueue of its dependents, the completion check | The task is in history. Its node is not finished, and its phase-2 record is still pending | Phase-2 recovery runs the progress again after its grace window. It enqueues the dependents again, so a dependent that ran before the failover runs again |

After a lost task end, recovery applies the retry policy of the task:

- With `WORKER_CRASHED` in `auto_retry_for` and retries remaining, the task is retried. Its body runs again.
- Otherwise the task ends `FAILED` with `WORKER_CRASHED`, although its body ran and returned.
- A workflow task that ends `FAILED` applies the `on_error` policy of its workflow.
- With `auto_fail_stale_running=False`, the row stays `RUNNING` until a [manual recovery](/horsies/workers/heartbeats-recovery/#manual-recovery).

### The Worker Liveness Lock

The liveness lock is session state on the primary, not a row, so it does not replicate. After a failover, no session on the new primary holds the lock of a worker.

- Each worker opens a new connection to the session URL and takes its lock again, with a new generation. A reaper judges a worker gone only when it finds the lock free at one generation for 75 seconds. A worker that takes its lock again before that is not judged gone.
- With `auto_fail_stale_running=True` (the default), a worker that holds a task and has no confirmed lock for 60 seconds kills each child that holds a task and exits with code 79. A supervisor restarts it.
- The tasks of that worker are recovered as after a worker crash. A `RUNNING` row ends its attempt with `WORKER_CRASHED`, with a retry when its retry policy allows it. A `CLAIMED` row goes back to `PENDING`.
- The worker confirms its lock every 5 seconds. After a lost connection, it tries a new connection at least every 5 seconds. A failover that keeps a worker from its lock for 60 seconds after its last confirmation ends the running tasks of that worker.

The rules of the lock are on [Worker Liveness Lock](/horsies/workers/heartbeats-recovery/#worker-liveness-lock).

### Design for Asynchronous Replication

When the horsies instance uses asynchronous replication, design for a task body that runs two times:

- Make each effect outside horsies idempotent. Key the effect by a value of the task input. A send that is repeated after a lost send can carry a new task id.
- Put `WORKER_CRASHED` in `auto_retry_for` of a task that must run again, not end `FAILED`, after a lost task end. See [Retry Policy](/horsies/tasks/retry-policy/).
- Keep the idempotency key of each send with its application write. After a failover, repeat the sends of the period before it with the same keys. A send whose task exists returns that task. A lost send enqueues its task again. This holds only inside the reservation window: see [Guarantees That End with Retention](/horsies/configuration/retention-config/#guarantees-that-end-with-retention).
- Start each workflow with a deterministic workflow id. After a failover, repeat the starts of the period before it with the same ids.

## Things to Avoid

**Don't run horsies in the instance of the application database.** The connections, the write load and the replication settings of horsies then share the limits of the application.

**Don't key an external effect by the task id alone.** After a lost send, a repeat of the send can carry a new task id, and the effect then occurs two times.

**Don't let a failover promote an asynchronous standby without a plan for the lost transactions.** Use [Settings That Lose No Commit](#settings-that-lose-no-commit), or design each task body for a second run.

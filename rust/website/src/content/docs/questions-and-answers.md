---
title: Questions & Answers
summary: Common questions about design trade-offs, scaling, and failure behavior.
related: [concepts/architecture, workers/concurrency, workers/heartbeats-recovery, tasks/retry-policy, internals/serialization]
tags: [faq, design]
---

## Why horsies?

Horsies aims to provide things that traditional task queue libraries don't. Shortly but not exhaustively:

- **Strict typing** — typed task contracts, typed workflow wiring, typed result retrieval. Mistakes are caught at registration and compile time, not in production.
- **Errors as values** — tasks return `Result<T, TaskError>` with a consistently structured error taxonomy.
- **Defensive approach** — explicit registration, `app.check()` at startup, opt-in retry policies when you need them.

## Why errors as values?

Every task **must** return `Result<T, TaskError>` — a uniform contract with a structured error taxonomy. The `#[task]` macro enforces this at compile time: your code will not compile if the return type doesn't match.

`Result<T, TaskError>` is what you write in your task function. `TaskResult<T>` is what you get back from `handle.get()`. It wraps both your task's result and retrieval-level failures (timeout, task not found, broker errors) that aren't your task's fault. One return type, one match expression, regardless of what went wrong.

This leads developers to actually think about error cases on both the definition and call site. Same applies for coding agents.

See [error handling](../tasks/error-handling) for the full taxonomy.

## Why PostgreSQL only?

Correctness and performance.

### Correctness:

Every guarantee horsies makes is a Postgres primitive:

- **Claiming**: one server-side function under `FOR UPDATE SKIP LOCKED`; a claim-generation fence rejects stale attempts, including a worker re-claiming its own requeued task. Double execution and phantom retries are impossible states, not tuned-away ones.
- **Finalization**: row lock → immutable attempt-history append → state transition, one transaction.
- **Recovery**: all state is rows and timestamps; the reaper reconstructs and repairs after a worker dies mid-flight — nothing in-flight exists only in a broker's memory.
- **Workflows**: fan-in resolution, completion checks, subworkflow cascades, and orphan self-heal are multi-row transitions under a documented lock order.
- **Dispatch**: LISTEN/NOTIFY push — no polling loop between enqueue and execution.
- **Inspection**: task history is plain tables — SQL, `EXPLAIN`, your existing backups. See [operational indexes](../internals/operational-indexes) for query-shape guidance.

A message broker can approximate the first three with visibility timeouts and acks; it cannot express them as invariants. That is the reason for the Postgres requirement, operating one less service is absolutely not a selling point. In fact, we strongly recommend running a dedicated Postgres instance for your worker.

### Postgres is performant:

It scales with your Postgres instance (a PlanetScale Postgres and a Heroku Postgres will not perform the same); even with a cross-machine deployment, app server and managed Postgres in the same region, holds per-statement p99 in the low single-digit milliseconds across the claim/dispatch/finalize hot path.

Measured numbers: [performance](../internals/performance).

## Where do terminal tasks go?

Terminal tasks move from `horsies_tasks` to `horsies_task_history`. The move is
part of the terminalization transaction. The live table contains only
`PENDING`, `CLAIMED`, and `RUNNING` rows.

History is partitioned by retention class and UTC day. Result and task-info
reads search live storage and history. Attempt rows become a verified snapshot
in the history row.

See [Database Schema](../internals/database-schema).

## How do I keep a task record forever?

Use `TaskSendOptions::new().retain_forever()` for that send. Do not pass the
string `"forever"` as a finite class.

Use `queue_retention` with a `None` value to keep all tasks sent to one queue
forever. An unmapped queue uses the 30-day default class.

See [Retention Config](../configuration/retention-config).

## Can I rerun a terminal task?

Yes. `rerun_task` creates a new task with a new UUID. It records the direct
source and the rerun root. It does not mutate the source history row.

The source must have a retained inline input envelope. Enable
`PostgresConfig.retain_rerun_input_default` before the original enqueue when
later rerun is required.

See [Retrieving Results](../tasks/retrieving-results#rerun-a-retained-terminal-task).

## How do I upgrade an existing database to task history?

A database at migration 0032 needs an offline cutover. Migration 0032 is the
schema-v26 compatibility boundary. Stop every process. Take a named backup.
Apply migrations 0033–0042. Run the stage order. Restart only after validation
writes the cutover attestation.

Fresh databases start at migration 0042 and need no cutover.

See the [Task-history Cutover](../operations/cutover-runbook) runbook.

## Is it ergonomic for devs?

Yes. The `#[task]` and `#[blocking_task]` proc macros generate typed companion modules with `register`, `send`, `schedule`, `node`, and `params`. Workflow wiring is explicit and typed, so mistakes are caught at registration time instead of runtime string matching.

All task inputs must implement `Serialize + Deserialize`. Tasks must be explicitly registered at startup. No auto-discovery, no magic.

## How does it handle retries?

It has a clear retry policy which can be set by developers.

Every retry policy requires an explicit list of error codes to retry on. This gives you fine-grained control. Supports `Fixed` and `Exponential` backoff strategies with optional jitter.

See [retry policy](../tasks/retry-policy).

## How does horsies handle panics?

They don't crash the worker and they don't disappear.

Any panic inside a task is caught (via `tokio::spawn` for async tasks, `catch_unwind` for blocking tasks), wrapped into a structured `TaskError`, and stored as a normal error result. The worker continues processing other tasks.

## What validation happens before the app starts?

`app.check()` runs a multi-phase validation covering:

- Config validation (queue settings, broker config, resilience bounds)
- Schedule validation (patterns, task references)
- Task retry policy consistency (valid intervals, no collisions with built-in error codes)
- Queue metadata for every registered task in Custom mode
- Workflow task references (every node must reference a registered task)
- Workflow queue validity (every node must target a valid queue)
- Workflow input completeness (nodes expecting input must have `set_input` or `arg_from`)
- Workflow `definition_key` presence and uniqueness
- Declared child workflow edge validation

The goal: fail at startup, not in production.

See [App Config](../configuration/app-config) for `app.check()` details.

## Does horsies work with PgBouncer or managed Postgres poolers?

Yes, in transaction-pool mode. Because transaction pooling cannot preserve session state for `LISTEN/NOTIFY`, the broker takes two URLs: a runtime URL that may point at the pooler and a `session_database_url` that points at a direct or session-pooled endpoint. Use `PostgresConfig::from_pgbouncer_urls(runtime_url, session_url)` to wire both at once.

The runtime pool disables SQLx's local prepared-statement cache and requires the pooler to have `max_prepared_statements > 0` so PgBouncer can track protocol-level prepared statements. Schema initialization, workers, and listeners use the session URL; workers also run a one-shot `LISTEN/NOTIFY` delivery probe at startup to fail fast if the session URL is accidentally transaction-pooled.

See [broker config — PgBouncer Transaction Pooling](../configuration/broker-config#pgbouncer-transaction-pooling).

## Does it have a scheduler?

Yes. Runs in-process via `app.run_scheduler()`. It supports intervals with human-readable typed models, not cron expressions.

See [scheduler](../scheduling/scheduler-overview).

##  Does horsies support worker side orchestration and execution?

Yes, horsies provides DAG workflows. Stack your tasks as nodes in the workflow, decide the policy by filling `TaskNode` details. You can even use workflows within workflows — a node itself can be a workflow.

E.g. `join: [All, Any, Quorum]`, `waits_for` (which nodes must be completed prior to this step in the pipeline).

See [workflows](../concepts/workflows/workflow-api) and [subworkflows](../concepts/workflows/subworkflows).

## Does it have monitoring?

There is a terminal-based TUI called Syce, capable of displaying the status of your workers, tasks, and workflows in detail.

See [syce](../monitoring/syce-overview).

## Does horsies provide guidance files for coding agents?

Yes. In source checkouts, horsies includes markdown skill files under:

`horsies/.agents/skills/`

These cover:

- quick routing (`SKILL.md`)
- tasks (`tasks.md`)
- workflows (`workflows.md`)
- configuration and operations (`configs.md`)
- practical summary (`practical-summary.md`)

They are best-practice references for agents and developers, and complement the public docs plus `llms.txt`.

## Does it support queue based concurrency control in the same deployed instance?

Yes. You do not need to waste a separate instance for each queue. Deploy workers only when you need more capacity, not when you want to have separate queue limitations.

## How does execution work?

You can have as many workers as you like. They consume tasks from the specified database.

Each worker is single-process and tokio-based:

- Async tasks run via `tokio::spawn`
- Blocking tasks run via `tokio::task::spawn_blocking`
- Workers heartbeat through the lifecycle of a task
- The library uses these heartbeats to keep track of health and take action

## Can I run multiple workers?

Yes. Multiple workers coordinate through the database:

- `FOR UPDATE SKIP LOCKED` prevents double-claiming
- Advisory locks serialize claim rounds
- `cluster_wide_cap` limits total in-flight tasks across all workers
- Heartbeats detect crashed workers and reclaim their tasks

See [worker architecture](../workers/worker-architecture) and [heartbeats & recovery](../workers/heartbeats-recovery).

## Is it production-ready?

Horsies is in alpha. The API may change between releases.
Fundamentals will likely remain the same.

## What about the Python version?

Horsies was originally written in Python. The Rust version aims for the same feature set and very similar semantics, but they are not wire-compatible.

Today, Rust and Python do **not** guarantee shared-database interoperability. Task and workflow payload serialization differs between the two implementations, and some built-in error codes also differ.

The practical guidance is:

- Use the Python version with Python workers and its own database.
- Use the Rust version with Rust workers and its own database.
- Treat them as sibling implementations with similar concepts, not as two runtimes that can safely process each other's rows from the same PostgreSQL database.

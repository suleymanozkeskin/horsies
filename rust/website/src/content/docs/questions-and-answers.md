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

**Because PostgreSQL is a powerful database which can cover the needs of most applications.**

The PostgreSQL broker is a deliberate architectural decision, not a compromise. It trades peak throughput — which most applications rarely need — for structured data control in the queue system, which is always better to have. Your tasks, results, state, and coordination live in a real database with schemas, indexes, transactions, and queryability.

PostgreSQL handles task storage, LISTEN/NOTIFY for real-time dispatch, advisory locks for coordination, and heartbeat tracking. All in a single database with a single source of truth.

If you have throughput levels which your PostgreSQL instance can't handle, use a dedicated broker.

Here `moderate` and `high` throughput is also relative to your postgres instance ( e.g. you will not get the same performance from a PlanetScale Postgres vs Heroku Postgres )

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

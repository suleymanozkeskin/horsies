---
title: Architecture
summary: System design, components, and data flow.
related: [task-lifecycle, ../../workers/worker-architecture, ../../internals/database-schema]
tags: [concepts, architecture, design]
---

# Architecture

Horsies is a PostgreSQL-backed distributed task queue and workflow engine with real-time dispatch via LISTEN/NOTIFY. Producers enqueue tasks, workers claim and execute them using tokio, and results flow back through the database.

## Overview

The system consists of four main components:

1. **Producer**: Application code that sends tasks via `.send()`
2. **PostgreSQL Broker**: Stores tasks, results, and coordinates via LISTEN/NOTIFY
3. **Worker**: Claims and executes tasks in a tokio runtime
4. **Scheduler**: (Optional) Enqueues tasks on a schedule

## Component Relationships

```text
Producer                  PostgreSQL                    Worker(s)
   │                         │                             │
   │  .send()                │                             │
   ├────────────────────────>│ INSERT task                 │
   │                         │                             │
   │                         │ NOTIFY task_new ───────────>│
   │                         │                             │
   │                         │<──────── CLAIM task ────────┤
   │                         │                             │
   │                         │                             │ Execute
   │                         │                             │
   │                         │<──────── UPDATE result ─────┤
   │                         │                             │
   │  .get()                 │ NOTIFY task_done            │
   │<────────────────────────│                             │
```

## Design Decisions

### PostgreSQL as the Only Backend

Unlike systems that require a separate message broker (RabbitMQ, Redis, NATS), horsies uses PostgreSQL for everything:

- **Task storage**: The `horsies_tasks` table holds all task data
- **Message passing**: LISTEN/NOTIFY replaces a separate message broker
- **Coordination**: Advisory locks prevent race conditions
- **State tracking**: Heartbeats and worker states are database tables

### Tokio-Based Worker

Workers use a single-process, tokio-based async runtime:

- Async tasks run via `tokio::spawn`
- Blocking tasks (CPU-bound) run via `tokio::task::spawn_blocking` with `catch_unwind` for panic safety
- Per-worker concurrency gated by a `tokio::sync::Semaphore`; cluster-wide and queue level concurrency enforced at claim time via the database
- Panics are isolated per-task — a panicking task produces a `TaskError` without taking down the worker

### Structured Result Contract

All tasks return `Result<T, TaskError>`, mandating consistent handling of tasks and their errors across the codebase.

- **Built-in errors**: `TaskErrorCode::BuiltIn` covers infrastructure failures — `RetrievalCode::WaitTimeout`, `OperationalErrorCode::BrokerError`, etc. These are produced by the library itself.
- **User-defined errors**: `TaskError::new("YOUR_CODE", "message")` for domain-specific failures. Your error codes are preserved through serialization and available for programmatic matching on the consumer side.

### Real-Time via LISTEN/NOTIFY

Workers primarily receive tasks via NOTIFY, not polling:

- PostgreSQL triggers send NOTIFY on task INSERT
- Workers subscribe via `PgListener` and wake immediately
- Result waiters listen on the `task_done` channel
- A configurable fallback poll interval (`notify_poll_interval_ms`, default 5s) ensures progress if NOTIFY is temporarily unavailable. Runtime recovery comes from a combination of sqlx reconnect behavior and Horsies worker retry/poll fallback logic

## User-Facing Components

| Component | Description |
| --------- | ----------- |
| `Horsies` | Application instance — configure and register tasks here |
| `#[task]` / `#[blocking_task]` | Proc macros to define tasks |
| `TaskFunction<A, T>` | Typed handle for sending tasks |
| `TaskHandle<T>` | Returned by `.send()` — use to retrieve results |
| `TaskResult<T>` | Return type wrapper — contains success or error |
| `Worker` | Started via `app.run_worker()` |
| `Scheduler` | Started via `app.run_scheduler()` |

## Data Flow

### Task Submission

1. Producer calls `task_name::send(args).await`
2. Queue and arguments are validated
3. Task row inserted into `horsies_tasks` table with status `PENDING`
4. PostgreSQL trigger fires NOTIFY to wake workers
5. `TaskHandle<T>` returned to producer

### Task Execution

1. Worker receives NOTIFY and wakes
2. Worker claims task atomically via `FOR UPDATE SKIP LOCKED`
3. Task dispatched via `tokio::spawn` (async) or `spawn_blocking` (blocking)
4. Task function executes
5. Result stored in database, status updated to `COMPLETED` or `FAILED`

### Result Retrieval

1. Producer calls `handle.get(timeout).await`
2. If result cached on handle, return immediately
3. Otherwise, listen for `task_done` notification or poll database
4. Return `TaskResult<T>` with success value or error

## Concurrency Model

- **Cluster level**: Optional `cluster_wide_cap` limits total in-flight work across the cluster
- **Worker level**: `concurrency` setting limits concurrent executions per worker
- **Queue level**: `max_concurrency` per custom queue (Custom mode only)
- **Claiming**: `max_claim_batch` prevents one worker from starving others

See [Concurrency](../../workers/concurrency) for details.

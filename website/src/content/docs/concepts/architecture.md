---
title: Architecture
summary: System design, components, and data flow.
related: [task-lifecycle, ../../workers/worker-architecture, ../../internals/postgres-broker]
tags: [concepts, architecture, design]
---

# Architecture

Horsies is a PostgreSQL-backed distributed task queue and workflow engine with real-time dispatch via LISTEN/NOTIFY. Producers enqueue tasks, workers claim and execute them in isolated processes, and results flow back through the database.
Async APIs (`send_async`, `get_async`, `start_async`, etc.) are producer-side I/O helpers; they do not change where tasks execute.

## Overview

The system consists of four main components:

1. **Producer**: Application code that sends tasks via `.send()` or `.send_async()`
2. **PostgreSQL Broker**: Stores tasks, results, and coordinates via LISTEN/NOTIFY
3. **Worker**: Claims and executes tasks in a process pool
4. **Scheduler**: (Optional) Enqueues tasks on a schedule

## Component Relationships

```text
Producer                  PostgreSQL                    Worker(s)
   │                         │                             │
   │  .send() / .send_async()│                             │
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
   │  .get() / .get_async()  │ NOTIFY task_done            │
   │<────────────────────────│                             │
```

## Design Decisions

### PostgreSQL as the Only Backend

Unlike Celery (which typically uses Redis or RabbitMQ), horsies uses PostgreSQL for everything:

- **Task storage**: The `tasks` table holds all task data
- **Message passing**: LISTEN/NOTIFY replaces a separate message broker
- **Coordination**: Advisory locks prevent race conditions
- **State tracking**: Heartbeats and worker states are database tables

### Process Pool Execution

Workers use Python's `ProcessPoolExecutor` rather than threads:

- Each task runs in an isolated child process
- GIL contention is avoided for CPU-bound tasks
- Crashed tasks don't take down the worker
- Child processes send their own heartbeats

### Type-Safe Results

All tasks must return `TaskResult[T, TaskError]`:

- Forces explicit error handling at definition time
- Distinguishes domain errors (returned by task) from infrastructure errors (thrown by library)
- Enables typed result retrieval

### Real-Time via LISTEN/NOTIFY

Workers don't poll for tasks:

- PostgreSQL triggers send NOTIFY on task INSERT
- Workers subscribe and wake immediately
- Result waiters listen on `task_done` channel
- Fallback polling handles edge cases (lost notifications)

## User-Facing Components

| Component | Description |
| --------- | ----------- |
| `Horsies` | Application instance - configure and register tasks here |
| `@app.task` | Decorator to define tasks |
| `TaskHandle` | Returned by `.send()` - use to retrieve results |
| `TaskResult` | Return type of all tasks - contains success or error |
| `Worker` (CLI) | Started via `horsies worker` command |
| `Scheduler` (CLI) | Started via `horsies scheduler` command |

## Data Flow

### Task Submission

1. Producer calls `task.send(args)`
2. Queue and arguments are validated
3. Task row inserted into `tasks` table with status `PENDING`
4. PostgreSQL trigger fires NOTIFY to wake workers
5. `TaskHandle` returned to producer

### Task Execution

1. Worker receives NOTIFY and wakes
2. Worker claims task atomically
3. Task dispatched to child process in the pool
4. Child process executes task function
5. Result stored in database, status updated to `COMPLETED` or `FAILED`

### Result Retrieval

1. Producer calls `handle.get()`
2. If result cached on handle, return immediately
3. Otherwise, listen for `task_done` notification or poll database
4. Return `TaskResult` with success value or error

## Concurrency Model

- **Cluster level**: Optional `cluster_wide_cap` limits total RUNNING tasks
- **Worker level**: `processes` setting limits concurrent executions per worker
- **Queue level**: `max_concurrency` per custom queue (CUSTOM mode only)
- **Claiming**: `max_claim_batch` prevents one worker from starving others

See [Concurrency](../../workers/concurrency) for details.

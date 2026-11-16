---
title: Worker Architecture
summary: Tokio-based concurrency model for claiming and executing tasks.
related: [concurrency, heartbeats-recovery, ../../concepts/architecture]
tags: [workers, architecture, tokio, concurrency]
---

## Overview

<!-- todo:diagram-needed - Worker architecture -->

```text
+----------------------------------------------------------+
|  Worker Process (single binary)                          |
|  - Subscribes to NOTIFY channels                         |
|  - Claims tasks from PostgreSQL                          |
|  - Dispatches to tokio tasks                             |
|  - Writes results back                                   |
|                                                          |
|  +----------------------------------------------------+  |
|  |  Tokio Runtime + Semaphore(concurrency)            |  |
|  |  +----------+ +----------+ +----------+            |  |
|  |  | Task A   | | Task B   | | (idle)   |  ...      |  |
|  |  | (spawn)  | | (spawn)  | |          |            |  |
|  |  +----------+ +----------+ +----------+            |  |
|  +----------------------------------------------------+  |
+----------------------------------------------------------+
```

## Concurrency Model

Workers use the tokio async runtime with a `tokio::sync::Semaphore` for concurrency control:

- **Single process**: One OS process per worker, no child processes
- **Async tasks**: Each task runs in a `tokio::spawn` green thread
- **Blocking tasks**: CPU-bound work runs via `tokio::task::spawn_blocking`
- **Concurrency**: Configurable via `WorkerConfig.concurrency`

Benefits:

- No process pool overhead or inter-process serialization
- Lightweight task spawning (thousands of concurrent tasks)
- Shared memory between tasks (broker connection pool)
- Panics in tasks are caught without killing the worker

## Execution Flow

### 1. Startup

```rust
// Programmatic startup
app.run_worker().await?;

// Or with custom configuration
use horsies::WorkerConfig;

let worker_config = WorkerConfig {
    concurrency: 8,
    ..Default::default()
};
app.run_worker_with(worker_config).await?;
```

1. Validate configuration
2. Connect to PostgreSQL (broker pool + LISTEN/NOTIFY)
3. Create semaphore with configured concurrency
4. Subscribe to NOTIFY channels

### 2. Main Loop

```text
while not cancelled:
    claim_and_dispatch_all()
    wait_for_notify_or_poll()
```

1. **Claim pass**: Query for available tasks, claim them
2. **Dispatch**: Spawn claimed tasks into tokio
3. **Wait**: Listen for NOTIFY until new tasks arrive (with poll fallback)
4. Repeat

### 3. Claiming

```sql
WITH next AS (
  SELECT id FROM tasks
  WHERE queue_name = :queue
    AND status = 'PENDING'
    AND enqueued_at <= now()
    AND (good_until IS NULL OR good_until > now())
  ORDER BY priority ASC, enqueued_at ASC
  FOR UPDATE SKIP LOCKED
  LIMIT :limit
)
UPDATE tasks SET status='CLAIMED', claimed_at=now()
FROM next WHERE tasks.id = next.id
RETURNING tasks.id
```

Key features:

- `FOR UPDATE SKIP LOCKED`: Non-blocking concurrent claiming
- Priority ordering: Lower priority number = higher priority
- FIFO within same priority
- Expiry check: Skip tasks past `good_until`
- A second expiry guard runs when moving `Claimed` to `Running`; if `good_until` passed in the buffer, the task becomes `Expired` and user code does not run

### 4. Task Execution

Inside the spawned tokio task:

1. Resolve task from registry by name
2. Deserialize arguments via serde
3. Start heartbeat (background tokio task)
4. Call task function (`async` or `spawn_blocking` for blocking tasks)
5. Serialize result
6. Stop heartbeat

### 5. Result Handling

After task completes:

1. Parse result (Ok or Err)
2. Check for retry eligibility
3. For a retry, update the live task and keep it pending
4. For a terminal outcome, write task history and remove the live row atomically
5. Send NOTIFY for result waiters

## Worker Configuration

From `WorkerConfig`:

| Field | Description |
| ----- | ----------- |
| `queues` | Queue names to process |
| `concurrency` | Maximum concurrent tasks |
| `max_claim_batch` | Max claims per queue per pass |
| `max_claim_per_worker` | Total claim limit |
| `queue_priorities` | Priority ordering for queues |
| `queue_max_concurrency` | Per-queue concurrency limits |
| `coalesce_notifies` | NOTIFY messages to drain per wake |
| `loglevel` | Log level for this worker |

## Graceful Shutdown

On SIGTERM/SIGINT:

1. Stop accepting new claims
2. Wait for running tasks to complete
3. Close database connections

```rust
// The worker handles signals automatically.
// Ctrl+C or SIGTERM triggers graceful shutdown.
app.run_worker().await?;
```

## Resilience

Workers recover from transient infrastructure failures without operator intervention. Configure via `WorkerResilienceConfig` in `AppConfig`.

### What It Handles

- **Database outages**: Worker retries on connection loss during startup, claiming, and result writes. No crash, no manual restart.
- **Silent NOTIFY channels**: If the PostgreSQL NOTIFY mechanism stops delivering (broken listener connection, dropped trigger), the worker falls back to periodic polling so tasks are still picked up.
- **Task panics**: Panics in `tokio::spawn` tasks are caught. The task is marked FAILED. The worker continues operating.

### Configuration

```rust
use horsies::WorkerResilienceConfig;

let config = AppConfig {
    resilience: WorkerResilienceConfig {
        db_retry_initial_ms: 500,      // initial backoff on DB errors
        db_retry_max_ms: 30_000,       // backoff cap
        db_retry_max_attempts: 0,      // 0 = retry forever
        notify_poll_interval_ms: 5_000, // poll fallback when NOTIFY is silent
    },
    ..AppConfig::for_database_url("postgresql://...")
};
```

| Field | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `db_retry_initial_ms` | `u64` | `500` | Initial backoff delay (100-60,000ms) |
| `db_retry_max_ms` | `u64` | `30000` | Maximum backoff delay (500-300,000ms) |
| `db_retry_max_attempts` | `u64` | `0` | Max retry attempts; `0` = infinite (0-10,000) |
| `notify_poll_interval_ms` | `u64` | `5000` | Poll interval when NOTIFY is silent (1,000-300,000ms) |

Backoff uses exponential growth (`initial_ms * 2^attempt`) with jitter, capped at `db_retry_max_ms`. Backoff resets after each successful loop iteration.

## Error Handling

| Error Type | Handling |
| ---------- | -------- |
| Task returns `Err(TaskError)` | Stored as FAILED with error code |
| Panic in task | Caught, wrapped as UNHANDLED_ERROR |
| Serialization error | WORKER_SERIALIZATION_ERROR |
| Task not found in registry | WORKER_RESOLUTION_ERROR |
| Worker crash (detected via heartbeat) | Marked FAILED by reaper |

## Multiple Workers

Run multiple worker instances for horizontal scaling:

```rust
// On machine 1: binary with concurrency=8
let worker_config = WorkerConfig { concurrency: 8, ..Default::default() };
app.run_worker_with(worker_config).await?;

// On machine 2: same binary, same config
```

- Each worker claims independently
- `SKIP LOCKED` prevents duplicate claims
- Advisory locks serialize certain operations
- `cluster_wide_cap` limits total concurrency

## Schema Setup

Workers ensure the Horsies schema during startup before entering the claim loop. If you want an explicit preflight step in provisioning scripts or standalone tools, call `broker.migrate().await?` or `broker.ensure_schema_initialized().await?` yourself.

```rust
let broker = PostgresBroker::connect_with(config).await?;
broker.ensure_schema_initialized().await?;
```

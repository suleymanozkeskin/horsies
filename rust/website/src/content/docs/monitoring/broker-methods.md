---
title: Broker Monitoring Methods
summary: Async methods on PostgresBroker for inspecting task and worker health.
related: [syce-overview, ../workers/heartbeats-recovery]
tags: [monitoring, broker, api]
---

`PostgresBroker` exposes async methods for querying task and worker health directly from the database. These are useful for building custom monitoring, alerting, or cleanup scripts.

Connectivity, result, and raw monitoring methods return
`Result<T, BrokerError>`. History-aware task-info and worker-state methods
return `BrokerResult<T>`, whose error is `BrokerOperationError`.
Task ID parameters use `uuid::Uuid`.

```rust
use horsies::{Horsies, AppConfig};

let app = Horsies::new(AppConfig::for_database_url(
    "postgresql://user:pass@localhost:5432/mydb"
))?;

let broker = app.get_broker().await?;
```

## Methods

### `get_result(task_id, timeout) -> Result<TaskResult<T>, BrokerError>`

Retrieve a task's result by ID, waiting if necessary. This is the broker-level equivalent of `TaskHandle.get()` -- use it when you need to fetch a result by task ID without holding a `TaskHandle` (e.g. in HTTP endpoints that receive a task ID from the client).

Uses PostgreSQL `LISTEN/NOTIFY` with a 1-second polling fallback.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task_id` | `Uuid` | -- | Task ID to retrieve result for |
| `timeout` | `Option<Duration>` | `None` | Max wait time; `None` waits indefinitely |

**Returns:** `Result<TaskResult<T>, BrokerError>`. Error codes on the error branch of `TaskResult`:
- `WAIT_TIMEOUT` -- timed out; task may still be running
- `TASK_NOT_FOUND` -- task ID does not exist
- `TASK_CANCELLED` -- task was cancelled
- `BROKER_ERROR` -- database/infrastructure failure

```rust
use std::time::Duration;

let task_id = uuid::Uuid::parse_str("018f47a2-65cb-7cc4-98d1-9f8659d23c31")?;
let result = broker.get_result::<i64>(task_id, Some(Duration::from_secs(5))).await?;
match result {
    Ok(value) => println!("Result: {}", value),
    Err(err) => println!("Task failed: {:?}", err.error_code),
}
```

### `get_stale_tasks(stale_threshold_minutes) -> Result<Vec<StaleTaskRow>, BrokerError>`

Find RUNNING tasks whose workers have not sent a heartbeat within the threshold. Indicates a crashed or unresponsive worker.

**Returns:** List of stale task records with fields: `id`, `worker_hostname`, `worker_pid`, `worker_process_name`, `last_heartbeat`, `started_at`, `task_name`.

```rust
let stale_tasks = broker.get_stale_tasks(5).await?;
for task in &stale_tasks {
    println!("Task {} on {} -- last heartbeat: {:?}",
        task.id, task.worker_hostname, task.last_heartbeat);
}
```

### `list_worker_states() -> BrokerResult<Vec<WorkerStateSnapshot>>`

Read the latest state snapshot for each worker. Idle workers remain visible.

**Returns:** The worker identity, snapshot time, process limits, queues, running
and claimed counts, resource use, and worker start time.

```rust
let states = broker.list_worker_states().await?;
for worker in &states {
    println!(
        "{} -- {} running, {} claimed",
        worker.worker_id,
        worker.tasks_running,
        worker.tasks_claimed,
    );
}
```

### `get_expired_tasks() -> Result<Vec<ExpiredTaskRow>, BrokerError>`

Find `Pending` tasks that exceeded their `good_until` deadline before being picked up. This query does not include tasks that expired after being claimed but before user code started.

**Returns:** Expired live-task rows with `id`, `task_name`, `queue_name`,
`priority`, `sent_at`, and `good_until`.

```rust
let expired = broker.get_expired_tasks().await?;
for task in &expired {
    println!(
        "Task {} ({}) passed deadline {:?}",
        task.id,
        task.task_name,
        task.good_until,
    );
}
```

### `get_task_info(task_id, include_result, include_failed_reason) -> BrokerResult<Option<TaskInfo>>`

Fetch metadata for a single task by ID. Returns `Ok(None)` if the task does not exist.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task_id` | `Uuid` | -- | Task ID to query |
| `include_result` | `bool` | `false` | Include `TaskResult` for terminal tasks |
| `include_failed_reason` | `bool` | `false` | Include worker-level `failed_reason` |

**Returns:** `BrokerResult<Option<TaskInfo>>`

```rust
let task_id = uuid::Uuid::parse_str("018f47a2-65cb-7cc4-98d1-9f8659d23c31")?;
if let Some(info) = broker.get_task_info(task_id, true, true).await? {
    println!("{} {}/{}", info.task_name, info.retry_count, info.max_retries);
    if let Some(next_retry) = info.next_retry_at {
        println!("Next retry at: {:?}", next_retry);
    }
}
```

### `get_task_attempts(task_id) -> Result<Vec<TaskAttemptRow>, BrokerError>`

Retrieve the per-attempt execution history for a task. Returns one row per finished execution attempt (success, failure, or worker crash).

| Parameter | Type | Description |
|-----------|------|-------------|
| `task_id` | `Uuid` | Task ID to query |

**Returns:** `Result<Vec<TaskAttemptRow>, BrokerError>`

```rust
let task_id = uuid::Uuid::parse_str("018f47a2-65cb-7cc4-98d1-9f8659d23c31")?;
let attempts = broker.get_task_attempts(task_id).await?;
for attempt in &attempts {
    println!("Attempt {}: {} at {:?}", attempt.attempt, attempt.outcome, attempt.finished_at);
}
```

### `health_check() -> Result<(), BrokerError>`

Verify database connectivity by running `SELECT 1`.

```rust
broker.health_check().await?;
println!("Broker is healthy");
```

## Not Public Broker Methods

Some stale-task recovery functions exist inside the worker recovery implementation, but they are not public `PostgresBroker` methods in the Rust API surface. If you need manual stale-task intervention, use worker automation or targeted operational SQL instead.

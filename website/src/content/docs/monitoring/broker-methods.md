---
title: Broker Monitoring Methods
summary: Async methods on PostgresBroker for inspecting task and worker health.
related: [syce-overview, ../workers/heartbeats-recovery]
tags: [monitoring, broker, api]
---

`PostgresBroker` exposes async methods for querying task and worker health directly from the database. These are useful for building custom monitoring, alerting, or cleanup scripts.

All broker methods return `BrokerResult[T]` — either `Ok(value)` on success or `Err(BrokerOperationError)` on failure. The error carries a `retryable` flag indicating whether the failure is transient (connection blip) or permanent (schema drift, code bug).

```python
from horsies import Horsies, AppConfig, PostgresConfig, is_ok, is_err

app = Horsies(AppConfig(
    broker=PostgresConfig(
        database_url="postgresql+psycopg://user:pass@localhost:5432/mydb",
    ),
))

broker = app.get_broker()
```

## Methods

### `get_raw_result_record_async(task_id: str, timeout_ms: int | None = None) -> BrokerResult[RawResultRecord | None]`

Fetch the raw stored result envelope for a task by ID, waiting if necessary. This is the broker's infrastructure-level result-fetch surface: it does no typed decoding and imports no user code. Callers that want a typed `TaskResult[T, TaskError]` should use [`app.get_result_async`](#app-level-typed-result-apis) instead — it composes a typed decode on top of this call.

Uses PostgreSQL `LISTEN/NOTIFY` with a 1-second polling fallback.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task_id` | `str` | — | Task ID to retrieve result for |
| `timeout_ms` | `int \| None` | `None` | Max wait time in milliseconds; `None` waits indefinitely |

**Returns:** `BrokerResult[RawResultRecord | None]` — `Ok(None)` if no task row exists for that ID; `Err(BrokerOperationError)` on infrastructure failure. The `RawResultRecord` carries `task_id`, `task_name`, `status`, and `raw_result` (the decoded JSON envelope, or `None` for cancelled tasks, non-terminal timeout snapshots, or terminal rows with no payload).

Sync variant: `get_raw_result_record(task_id, timeout_ms)` (runs the async version in a background loop).

```python
# Async — broker-level raw fetch (no typed decode)
result = await broker.get_raw_result_record_async("task-uuid-here", timeout_ms=5000)
if is_ok(result):
    record = result.ok_value
    if record is None:
        print("Task does not exist")
    elif record.raw_result is None:
        print(f"status={record.status}, no result payload yet")
    else:
        print(f"status={record.status}, task_name={record.task_name}")
        print(f"raw envelope: {record.raw_result}")
elif is_err(result):
    print(f"Broker error: {result.err_value.code} - {result.err_value.message}")
```

### App-level typed result APIs

For most callers, prefer `app.get_result_async(task_id, timeout_ms)` over the raw record. It returns `BrokerResult[TaskResult[Any, TaskError]]` — the outer `BrokerResult` surfaces infrastructure failures (`INVALID_JSON_PAYLOAD`, `NO_TYPE_AVAILABLE`), while the inner `TaskResult` carries the typed domain result decoded via the local task catalog's `task_ok_type`.

```python
# Async — typed decode at the app layer
outer = await app.get_result_async("task-uuid-here", timeout_ms=5000)
if is_err(outer):
    print(f"Infrastructure error: {outer.err_value.code}")
else:
    task_result = outer.ok_value  # TaskResult[Any, TaskError]
    if task_result.is_ok():
        print(f"Result: {task_result.ok_value}")
    else:
        print(f"Task error: {task_result.err_value.error_code}")

# Sync
outer = app.get_result("task-uuid-here", timeout_ms=5000)
```

### `get_stale_tasks(stale_threshold_minutes: int = 2) -> BrokerResult[list[dict[str, Any]]]`

Find RUNNING tasks whose workers have not sent a heartbeat within the threshold. Indicates a crashed or unresponsive worker.

**Returns:** `Ok(list[dict])` with keys: `id`, `worker_hostname`, `worker_pid`, `worker_process_name`, `last_heartbeat`, `started_at`, `task_name`.

```python
result = await broker.get_stale_tasks(stale_threshold_minutes=5)
if is_ok(result):
    for task in result.ok_value:
        print(f"Task {task['id']} on {task['worker_hostname']} — last heartbeat: {task['last_heartbeat']}")
```

### Worker health and load

For per-worker load, configuration, and liveness, use the typed [Worker & Database Health](/monitoring/worker-health/) API (`app.list_worker_states_async`, `app.ping_workers_async`, `app.ping_database_async`). It reads the worker-state timeseries — so it includes idle workers — and returns typed `WorkerStateSnapshot` / `WorkerPong` models rather than raw dicts.

### `get_expired_tasks() -> BrokerResult[list[dict[str, Any]]]`

Find PENDING tasks that exceeded their `good_until` deadline before being picked up.

**Returns:** `Ok(list[dict])` with keys: `id`, `task_name`, `queue_name`, `good_until`, `expired_for`.

```python
result = await broker.get_expired_tasks()
if is_ok(result):
    for task in result.ok_value:
        print(f"Task {task['id']} ({task['task_name']}) expired {task['expired_for']} ago")
```

### `get_task_info(task_id: str, include_result: bool = False, include_failed_reason: bool = False) -> BrokerResult[TaskInfo | None]`

Fetch metadata for a single task by ID. Returns `Ok(None)` if the task does not exist, `Err(BrokerOperationError)` on infrastructure failure.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task_id` | `str` | — | Task ID to query |
| `include_result` | `bool` | `False` | Include the raw stored result envelope for terminal tasks |
| `include_failed_reason` | `bool` | `False` | Include worker-level `failed_reason` |

**Returns:** `BrokerResult[TaskInfo | None]`. At the broker layer, `include_result=True` populates `TaskInfo.raw_result` only. Use `app.get_task_info_async(...)` when you want the local app to also populate `decoded_result` and `result_decoded`.
Sync wrapper: `broker.get_task_info(...)`.

```python
result = await broker.get_task_info_async(
    task_id,
    include_result=True,
    include_failed_reason=True,
)
if is_err(result):
    print(f"Query failed: {result.err_value.message}")
elif result.ok_value is not None:
    info = result.ok_value
    print(f"{info.task_name} {info.retry_count}/{info.max_retries}")
    if info.next_retry_at:
        print(f"Next retry at: {info.next_retry_at}")
```

### `mark_stale_tasks_as_failed(stale_threshold_ms: int = 300_000, finalizing_stale_threshold_ms: int = 300_000) -> BrokerResult[int]`

Handle RUNNING tasks with no heartbeat within the threshold. Tasks with a retry policy listing `WORKER_CRASHED` in `auto_retry_for` (and retries remaining) are scheduled for retry; all others are marked FAILED with `WORKER_CRASHED` error code.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stale_threshold_ms` | `int` | `300_000` (5 min) | Milliseconds without heartbeat to consider crashed |
| `finalizing_stale_threshold_ms` | `int` | `300_000` (5 min) | Milliseconds a completed child may remain finalizing before recovery |

**Returns:** `Ok(int)` — number of tasks processed (retried or failed).

```python
result = await broker.mark_stale_tasks_as_failed(stale_threshold_ms=300_000)
if is_ok(result):
    print(f"Marked {result.ok_value} crashed tasks as FAILED")
```

### `requeue_stale_claimed(stale_threshold_ms: int = 120_000) -> BrokerResult[int]`

Requeue tasks stuck in CLAIMED status when the claiming worker has stopped sending heartbeats.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stale_threshold_ms` | `int` | `120_000` (2 min) | Milliseconds without heartbeat to consider stale |

**Returns:** `Ok(int)` — number of tasks requeued.

```python
result = await broker.requeue_stale_claimed(stale_threshold_ms=120_000)
if is_ok(result):
    print(f"Requeued {result.ok_value} stale claimed tasks")
```

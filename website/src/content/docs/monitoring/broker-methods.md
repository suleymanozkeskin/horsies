---
title: Broker Monitoring Methods
summary: Async methods on PostgresBroker for inspecting task and worker health.
related: [syce-overview, ../workers/heartbeats-recovery]
tags: [monitoring, broker, api]
---

`PostgresBroker` exposes async methods for querying task and worker health directly from the database. These are useful for building custom monitoring, alerting, or cleanup scripts.

Access the broker instance via `app.broker`:

```python
from horsies import Horsies, AppConfig, PostgresConfig

app = Horsies(AppConfig(
    broker=PostgresConfig(
        database_url="postgresql+psycopg://user:pass@localhost:5432/mydb",
    ),
))

broker = app.broker
```

## Methods

### `get_stale_tasks(stale_threshold_minutes: int = 2) -> list[dict[str, Any]]`

Find RUNNING tasks whose workers have not sent a heartbeat within the threshold. Indicates a crashed or unresponsive worker.

**Returns:** List of dicts with keys: `id`, `worker_hostname`, `worker_pid`, `last_heartbeat`.

```python
stale = await broker.get_stale_tasks(stale_threshold_minutes=5)
for task in stale:
    print(f"Task {task['id']} on {task['worker_hostname']} — last heartbeat: {task['last_heartbeat']}")
```

### `get_worker_stats() -> list[dict[str, Any]]`

Group RUNNING tasks by worker to show load distribution and health.

**Returns:** List of dicts with keys: `worker_hostname`, `worker_pid`, `active_tasks`, `oldest_task_start`.

```python
stats = await broker.get_worker_stats()
for worker in stats:
    print(f"{worker['worker_hostname']}:{worker['worker_pid']} — {worker['active_tasks']} active")
```

### `get_expired_tasks() -> list[dict[str, Any]]`

Find PENDING tasks that exceeded their `good_until` deadline before being picked up.

**Returns:** List of dicts with keys: `id`, `task_name`, `queue_name`, `good_until`, `expired_for`.

```python
expired = await broker.get_expired_tasks()
for task in expired:
    print(f"Task {task['id']} ({task['task_name']}) expired {task['expired_for']} ago")
```

### `get_task_info(task_id: str, include_result: bool = False, include_failed_reason: bool = False) -> TaskInfo | None`

Fetch metadata for a single task by ID. Returns `None` if the task does not exist.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task_id` | `str` | — | Task ID to query |
| `include_result` | `bool` | `False` | Include `TaskResult` for terminal tasks |
| `include_failed_reason` | `bool` | `False` | Include worker-level `failed_reason` |

**Returns:** `TaskInfo | None`
Sync wrapper: `broker.get_task_info(...)`.

```python
info = await broker.get_task_info(
    task_id,
    include_result=True,
    include_failed_reason=True,
)
if info:
    print(f"{info.task_name} {info.retry_count}/{info.max_retries}")
    if info.next_retry_at:
        print(f"Next retry at: {info.next_retry_at}")
```

### `mark_stale_tasks_as_failed(stale_threshold_ms: int = 300_000) -> int`

Mark RUNNING tasks as FAILED when no heartbeat has been received within the threshold. Sets the result to a `TaskResult` with `WORKER_CRASHED` error code.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stale_threshold_ms` | `int` | `300_000` (5 min) | Milliseconds without heartbeat to consider crashed |

**Returns:** Number of tasks marked as failed.

```python
count = await broker.mark_stale_tasks_as_failed(stale_threshold_ms=300_000)
print(f"Marked {count} crashed tasks as FAILED")
```

### `requeue_stale_claimed(stale_threshold_ms: int = 120_000) -> int`

Requeue tasks stuck in CLAIMED status when the claiming worker has stopped sending heartbeats.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stale_threshold_ms` | `int` | `120_000` (2 min) | Milliseconds without heartbeat to consider stale |

**Returns:** Number of tasks requeued.

```python
count = await broker.requeue_stale_claimed(stale_threshold_ms=120_000)
print(f"Requeued {count} stale claimed tasks")
```

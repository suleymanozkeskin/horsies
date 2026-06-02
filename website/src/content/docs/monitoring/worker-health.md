---
title: Worker & Database Health
summary: Active liveness probes for Postgres and workers, plus typed worker-state snapshots.
related: [broker-methods, ../workers/heartbeats-recovery]
tags: [monitoring, health, liveness, worker, api]
---

The health API answers three operational questions: is Postgres reachable, which workers are responsive right now, and what is each worker doing. All methods exist on the `Horsies` app (async and sync) and return `BrokerResult[T]` — `Ok(value)` on success, `Err(BrokerOperationError)` with a `retryable` flag on failure.

## Mental model

- `ping_database()` — "Can this app reach Postgres right now?"
- `ping_workers()` — "Which workers can respond right now?"
- `list_worker_states()` — "What did workers last report about themselves?"
- Comparing `list_worker_states()` with `ping_workers()` tells you which known workers are currently non-responsive ( e.g. reported recently, but did not answer a ping )

`ping_*` are active, right-now probes. `list_worker_states` is the last self-reported snapshot, not a live check.

```python
from horsies import Horsies, AppConfig, PostgresConfig, is_ok, is_err

app = Horsies(AppConfig(
    broker=PostgresConfig(
        database_url="postgresql+psycopg://user:pass@localhost:5432/mydb",
    ),
))
```

## Why and When

Use `ping_database` and `ping_workers` for active, right-now liveness — a Kubernetes readiness gate, a `/health` endpoint, or pre-dispatch checks. Use the worker-state reads for load and configuration introspection (dashboards, capacity decisions).

## Is Postgres alive?

`ping_database_async` runs `SELECT 1` through the live broker pool, so success also confirms a pooled connection could be checked out. It reports round-trip latency.

```python
result = await app.ping_database_async()
if is_ok(result):
    print(f"Postgres reachable in {result.ok_value.latency_ms:.1f} ms")
else:
    print(f"Unreachable: {result.err_value.code}")  # DB_PING_FAILED
```

## Which workers are alive?

`ping_workers_async` broadcasts a ping and collects replies. A reply proves the worker's event loop is responsive **and** that the worker can reach Postgres (the reply travels through the worker's own connection). Pass `target_worker_id` to probe one worker; it returns as soon as that worker replies.

```python
result = await app.ping_workers_async(timeout_seconds=2.0)
if is_ok(result):
    for pong in result.ok_value:
        print(f"{pong.worker_id} on {pong.hostname}:{pong.pid} — {pong.round_trip_ms:.1f} ms")
```

**Stop conditions:**

- `min_responses=N` — return as soon as `N` distinct workers reply. Use `min_responses=1` for a fast fail-open liveness gate: a healthy fleet answers in milliseconds, and only a degraded fleet pays the full `timeout_seconds`. This removes the latency floor for a high-frequency `/health` probe.
- `target_worker_id` — return on the first reply from that worker.
- neither — wait the full `timeout_seconds` and enumerate every responder (the live count is not known in advance).

```python
# Fast liveness gate: returns on the first pong (~ms when healthy).
result = await app.ping_workers_async(min_responses=1, timeout_seconds=2.0)
healthy = is_ok(result) and len(result.ok_value) >= 1
```

## What is each worker doing?

`list_worker_states_async` returns the latest snapshot per worker, **including idle workers**. Each `WorkerStateSnapshot` carries identity, queue/concurrency configuration, current `tasks_running`/`tasks_claimed`, and psutil memory/CPU.

```python
result = await app.list_worker_states_async()
if is_ok(result):
    for snap in result.ok_value:
        print(f"{snap.worker_id}: {snap.tasks_running} running, {snap.tasks_claimed} claimed")
```

`get_worker_state_async(worker_id)` returns one worker's latest snapshot (or `None`). `get_worker_state_history_async(worker_id, limit=...)` returns the snapshot timeseries newest-first; `limit=None` returns all retained rows.

```python
history = await app.get_worker_state_history_async("worker-uuid", limit=20)
if is_ok(history):
    for snap in history.ok_value:
        print(f"{snap.snapshot_at}: cpu={snap.cpu_percent}% mem={snap.memory_usage_mb}MB")
```

## Liveness pattern: known fleet vs responsive now

Combine the two surfaces to find workers that exist but stopped responding. A worker present in `list_worker_states` but absent from `ping_workers` is non-responsive.

```python
states = await app.list_worker_states_async()
pongs = await app.ping_workers_async(timeout_seconds=2.0)
if is_ok(states) and is_ok(pongs):
    known = {s.worker_id for s in states.ok_value}
    responsive = {p.worker_id for p in pongs.ok_value}
    suspect = known - responsive
    if suspect:
        print(f"Non-responsive workers: {suspect}")
```

## API Reference

Each method has a sync variant with the same name minus the `_async` suffix (it runs the async version in a background loop).

### `ping_database_async() -> BrokerResult[DatabasePing]`

`SELECT 1` through the live pool. **Returns:** `Ok(DatabasePing)` with `latency_ms: float`; `Err` with code `DB_PING_FAILED` on failure.

### `ping_workers_async(*, target_worker_id: str | None = None, timeout_seconds: float = 2.0, min_responses: int | None = None) -> BrokerResult[list[WorkerPong]]`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `target_worker_id` | `str \| None` | `None` | `None` broadcasts to all workers; a value probes one worker and returns on its reply |
| `timeout_seconds` | `float` | `2.0` | Collection window; must be positive |
| `min_responses` | `int \| None` | `None` | Return as soon as this many distinct workers reply. `None` waits the full window. Must be `>= 1` when set |

**Returns:** `Ok(list[WorkerPong])` — one entry per responding (distinct) worker, possibly empty. Each `WorkerPong` has `worker_id`, `hostname`, `pid`, `round_trip_ms`. `Err` with code `WORKER_PING_FAILED` on non-positive `timeout_seconds`, `min_responses < 1`, or reply-channel failure.

### `list_worker_states_async() -> BrokerResult[list[WorkerStateSnapshot]]`

Latest snapshot per worker, including idle workers. **Returns:** `Ok(list[WorkerStateSnapshot])`; `Err` with code `MONITORING_QUERY_FAILED` on failure.

### `get_worker_state_async(worker_id: str) -> BrokerResult[WorkerStateSnapshot | None]`

Latest snapshot for one worker. **Returns:** `Ok(WorkerStateSnapshot)`, `Ok(None)` if unknown, or `Err(MONITORING_QUERY_FAILED)`.

### `get_worker_state_history_async(worker_id: str, *, limit: int | None = None) -> BrokerResult[list[WorkerStateSnapshot]]`

Snapshot timeseries for one worker, newest first.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `worker_id` | `str` | — | Worker instance id |
| `limit` | `int \| None` | `None` | Max rows; `None` returns all retained rows. Must be positive when set |

**Returns:** `Ok(list[WorkerStateSnapshot])`; `Err(MONITORING_QUERY_FAILED)` on failure or non-positive `limit`.

### `WorkerStateSnapshot` fields

| Field | Type | Description |
|-------|------|-------------|
| `worker_id` | `str` | Worker instance id |
| `snapshot_at` | `datetime` | When the snapshot was taken |
| `hostname` | `str` | Worker host |
| `pid` | `int` | Worker process id |
| `processes` | `int` | Process pool size |
| `max_claim_batch` | `int` | Claim batch size |
| `max_claim_per_worker` | `int` | Per-worker claim cap |
| `cluster_wide_cap` | `int \| None` | Cluster-wide concurrency cap, if set |
| `queues` | `list[str]` | Queues this worker serves |
| `queue_priorities` | `dict[str, int] \| None` | Per-queue priorities (CUSTOM mode) |
| `queue_max_concurrency` | `dict[str, int] \| None` | Per-queue concurrency (CUSTOM mode) |
| `recovery_config` | `dict[str, Any] \| None` | Recovery configuration snapshot |
| `tasks_running` | `int` | RUNNING tasks at snapshot time |
| `tasks_claimed` | `int` | CLAIMED tasks at snapshot time |
| `memory_usage_mb` | `float \| None` | Resident memory (psutil) |
| `memory_percent` | `float \| None` | Memory percent (psutil) |
| `cpu_percent` | `float \| None` | CPU percent (psutil) |
| `worker_started_at` | `datetime` | Worker process start time |

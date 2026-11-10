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
- `get_class_coverage()` — "Does a live worker serve every class that has pending tasks?"
- `get_class_report()` — "How long do pending tasks of each class wait, and how much capacity is free?"

`ping_*` are active, right-now probes. `list_worker_states` is the last self-reported snapshot, not a live check.

```python
from pydantic import SecretStr
from horsies import Horsies, AppConfig, PostgresConfig, is_ok, is_err

app = Horsies(AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr("postgresql+psycopg://user:pass@localhost:5432/mydb"),
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

### Which slots are down?

`WorkerStateSnapshot.slot_health` says whether any slot of the worker was down when it wrote the snapshot. A slot is down when its last child start failed for a cause that can clear, such as `EMFILE`. It takes no task, and the worker starts a new child for it after a growing wait. See [Child Start Failures](/horsies/workers/worker-architecture/#child-start-failures).

| `slot_health` | Meaning |
| ------------- | ------- |
| `AllSlotsServing()` | No slot was down |
| `SlotsDown(slots)` | The worker is degraded. `slots` holds one `DownSlot` per slot that was down, in slot order |
| `SlotHealthNotRecorded()` | The snapshot comes from a worker version that did not record slot health |

```python
from horsies import AllSlotsServing, SlotHealthNotRecorded, SlotsDown, is_ok

result = await app.list_worker_states_async()
if is_ok(result):
    for snap in result.ok_value:
        match snap.slot_health:
            case SlotsDown(slots=down):
                for slot in down:
                    print(f"{snap.worker_id} slot {slot.slot}: {slot.cause} since {slot.down_since}")
            case AllSlotsServing() | SlotHealthNotRecorded():
                pass
```

`DownSlot` fields: `slot` (index), `cause` (`ResourceShortage(errno_name)`, `TaskLoopCreateFailed()`, `ThreadStartRefused()` or `SpawnFailed()`), `detail` (error text of the last failed start, at most 2400 characters), `down_since` (aware `datetime`), `failed_boots` (failed starts since the slot went down, at least 1).

### Which classes does a worker serve?

`WorkerStateSnapshot.class_capacity` holds the claim budget of each execution class that the worker runs children for. A budget's `capacity` counts its children (`ISOLATED`) or seats (a shared class) in service, and `serves` lists the classes whose rows it claims. A class that no budget serves is not claimed by the worker. See [Budgets per Execution Class](/horsies/workers/concurrency/#budgets-per-execution-class).

| `class_capacity` | Meaning |
| ---------------- | ------- |
| `ClassBudgets(budgets)` | One `ClassBudget(budget, capacity, serves)` per budget, in `Execution` order. Empty when the worker serves no class |
| `ClassCapacityNotRecorded()` | The snapshot comes from a worker version that did not record its budgets |

```python
from horsies import ClassBudgets, ClassCapacityNotRecorded, is_ok

result = await app.list_worker_states_async()
if is_ok(result):
    for snap in result.ok_value:
        match snap.class_capacity:
            case ClassBudgets(budgets=budgets):
                for budget in budgets:
                    served = ", ".join(member.value for member in budget.serves)
                    print(f"{snap.worker_id} {budget.budget.value}: {budget.capacity} in service, serves {served}")
            case ClassCapacityNotRecorded():
                pass
```

`get_worker_state_async(worker_id)` returns one worker's latest snapshot (or `None`). `get_worker_state_history_async(worker_id, limit=...)` returns the snapshot timeseries newest-first; `limit=None` returns all retained rows.

```python
history = await app.get_worker_state_history_async("worker-uuid", limit=20)
if is_ok(history):
    for snap in history.ok_value:
        print(f"{snap.snapshot_at}: cpu={snap.cpu_percent}% mem={snap.memory_usage_mb}MB")
```

## Does a live worker serve every pending class?

A pending stream is the `PENDING` rows of one queue and one execution class. A worker is live while its latest snapshot is at most `WORKER_STATE_LIVE_WINDOW_S` (120) seconds old. A live worker serves a stream when the queue is one of its `queues` and one of its budgets serves the class. `get_class_coverage_async` gives each pending stream one state:

| `state` | Meaning |
| ------- | ------- |
| `StreamServed(workers, capacity)` | Live workers serve the stream. `capacity` sums the capacity in service of the budget that serves the class on each of them |
| `StreamWithoutCapacity(workers)` | Live workers serve the stream, and every child or seat of their serving budgets was down at their snapshot |
| `StreamUnserved()` | No live worker serves the stream. Its rows wait until a worker that serves the class runs on the queue |

```python
from horsies import StreamServed, StreamUnserved, StreamWithoutCapacity, is_ok

result = await app.get_class_coverage_async()
if is_ok(result):
    for stream in result.ok_value.streams:
        match stream.state:
            case StreamUnserved():
                print(f"{stream.queue_name}/{stream.execution.value}: no live worker serves it")
            case StreamWithoutCapacity(workers=workers):
                print(f"{stream.queue_name}/{stream.execution.value}: {workers} worker(s), no child in service")
            case StreamServed():
                pass
```

A delayed send and a retry that waits for its interval are `PENDING` too, so they count before they are due.

### The worker logs it

Each reaper pass runs the same check, one check interval after the worker starts. The worker logs an unserved stream at error level and a stream without capacity at warning level, for example:

```
PENDING ISOLATED tasks on queue 'default' wait: no live worker serves this class. Start a worker with isolated children on this queue. A shared task that went back to the queue as ISOLATED also waits here.
```

Each worker logs one stream at most once per 5 minutes (`CLASS_COVERAGE_LOG_INTERVAL_S`). A check that fails logs one warning per 5 minutes and changes nothing.

### What to do

- **Unserved `ISOLATED`**: start a worker with isolated children (`WorkerProfile(isolated=IsolatedChildren(...))`) that serves the queue. A row of a shared class goes back to the queue as `ISOLATED` when its task, as the worker registered it, has another class; such rows need isolated children too.
- **Unserved shared class**: start a worker with children of that class, or with isolated children, that serves the queue.
- **Without capacity**: the serving workers' slots are down. Read their `slot_health` (above) for the cause. Each slot boots again after a growing wait.
- **A worker that reports less often than every 120 seconds** (`worker_state_snapshot_interval_ms` above 120,000) is not live between its snapshots, so the check reports its streams as unserved. Keep the interval at or below 120 seconds.

## How long do pending tasks wait, and what capacity is free?

`get_class_report_async` returns the pending streams with the wait of their oldest due row and their coverage, and the budget load of each live worker.

- `PendingStream.wait` is `OldestWaiting(enqueued_at, wait)` when at least one row is due, or `NoneDue(next_due_at)` when every row is due later. A row is due when its `enqueued_at` (its send time, or the time its retry became due) is at or before the read.
- `WorkerClassLoad.budgets` holds one `BudgetLoad` per budget: `capacity` and `serves` from the worker's latest snapshot, `in_flight` (the worker's `RUNNING` rows and `CLAIMED` rows with a live lease of the classes it serves, counted at the read, as the claim counts them) and `free` (`capacity - in_flight`, at least 0; a prefetch buffer can hold more rows than the capacity).

```python
from horsies import OldestWaiting, NoneDue, is_ok

result = await app.get_class_report_async()
if is_ok(result):
    report = result.ok_value
    for stream in report.streams:
        match stream.wait:
            case OldestWaiting(wait=wait):
                print(f"{stream.queue_name}/{stream.execution.value}: oldest waits {wait}")
            case NoneDue(next_due_at=due):
                print(f"{stream.queue_name}/{stream.execution.value}: next due {due}")
    for worker in report.workers:
        for budget in worker.budgets:
            print(f"{worker.worker_id} {budget.budget.value}: {budget.free}/{budget.capacity} free")
```

Both reads use the claim index. The coverage read takes one index probe per pending (queue, class); the report takes one per (queue, class, priority), so its cost grows with the priorities in use, not with the backlog. With 98,000 pending rows over 5 queues, 3 classes and 100 priorities on PostgreSQL 17, the coverage read used 32 buffers and the stream heads of the report 2,379 buffers (6.5 ms).

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

**Returns:** `Ok(list[WorkerPong])` — one entry per responding (distinct) worker, possibly empty. Each `WorkerPong` has `worker_id`, `hostname`, `pid`, `round_trip_ms`. `Err` with code `INVALID_INPUT` on non-positive `timeout_seconds` or `min_responses < 1`; `Err` with code `WORKER_PING_FAILED` on reply-channel failure.

### `list_worker_states_async() -> BrokerResult[list[WorkerStateSnapshot]]`

Latest snapshot per worker, including idle workers. **Returns:** `Ok(list[WorkerStateSnapshot])`; `Err` with code `MONITORING_QUERY_FAILED` on failure, also when a stored `slots_down` or `class_capacity` value does not have the shape that the worker writes. The message names the worker.

### `get_worker_state_async(worker_id: str) -> BrokerResult[WorkerStateSnapshot | None]`

Latest snapshot for one worker. **Returns:** `Ok(WorkerStateSnapshot)`, `Ok(None)` if unknown, or `Err(MONITORING_QUERY_FAILED)`.

### `get_worker_state_history_async(worker_id: str, *, limit: int | None = None) -> BrokerResult[list[WorkerStateSnapshot]]`

Snapshot timeseries for one worker, newest first.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `worker_id` | `str` | — | Worker instance id |
| `limit` | `int \| None` | `None` | Max rows; `None` returns all retained rows. Must be positive when set |

**Returns:** `Ok(list[WorkerStateSnapshot])`; `Err(INVALID_INPUT)` on non-positive `limit`; `Err(MONITORING_QUERY_FAILED)` on query failure.

### `get_class_coverage_async() -> BrokerResult[ClassCoverage]`

The fleet check. **Returns:** `Ok(ClassCoverage)` with `checked_at` (database time), `live_workers` (the workers whose latest snapshot was live) and `streams`: one `StreamCoverage(queue_name, execution, state)` per (queue, class) with `PENDING` rows, ordered by queue name, then by `Execution` order. `Err(MONITORING_QUERY_FAILED)` on a database error, and with `retryable=False` when a live snapshot's `class_capacity` is unreadable or missing (the message names the worker), or when a read reaches its bound: 20,000 snapshot rows inside the live window, or 3,000 pending streams.

### `get_class_report_async() -> BrokerResult[ClassReport]`

**Returns:** `Ok(ClassReport)` with `checked_at`, `streams` (one `PendingStream(queue_name, execution, wait, state)` per pending stream, ordered as above) and `workers` (one `WorkerClassLoad(worker_id, snapshot_at, budgets)` per live worker, by worker id). `Err(MONITORING_QUERY_FAILED)` as for `get_class_coverage_async`, also when a live worker holds rows of a class that its recorded budgets do not serve, or when more than 10,000 (queue, class, priority) groups have `PENDING` rows.

### `WorkerStateSnapshot` fields

| Field | Type | Description |
|-------|------|-------------|
| `worker_id` | `str` | Worker instance id |
| `snapshot_at` | `datetime` | When the snapshot was taken |
| `hostname` | `str` | Worker host |
| `pid` | `int` | Worker process id |
| `processes` | `int` | Child process count: the number of children of the worker profile |
| `max_claim_batch` | `int` | Claim batch size |
| `max_claim_per_worker` | `int` | Per-worker claim cap |
| `cluster_wide_cap` | `int \| None` | Cluster-wide concurrency cap, if set |
| `queues` | `list[str]` | Queues this worker serves |
| `queue_priorities` | `dict[str, int] \| None` | Per-queue priorities (CUSTOM mode) |
| `queue_max_concurrency` | `dict[str, int] \| None` | Per-queue concurrency (CUSTOM mode) |
| `recovery_config` | `dict[str, Any] \| None` | Recovery configuration snapshot, plus the embedded health payloads below |
| `tasks_running` | `int` | RUNNING tasks at snapshot time |
| `tasks_claimed` | `int` | CLAIMED tasks at snapshot time |
| `memory_usage_mb` | `float \| None` | Parent worker process resident memory (psutil) |
| `memory_percent` | `float \| None` | Memory percent (psutil) |
| `cpu_percent` | `float \| None` | CPU percent (psutil) |
| `children_memory_mb` | `float \| None` | Summed resident memory of the worker's child processes |
| `worker_started_at` | `datetime` | Worker process start time |
| `slot_health` | `SlotHealth` | `AllSlotsServing()`, `SlotsDown(slots)` or `SlotHealthNotRecorded()` |
| `class_capacity` | `ClassCapacity` | `ClassBudgets(budgets)` or `ClassCapacityNotRecorded()` |

### Embedded health payloads

`recovery_config` carries four operational payloads beside the
configuration snapshot, refreshed with each snapshot:

- `partition_coverage` — the last coverage-ensure outcome: `state`
  (`ensured` / `failed`), covered-through markers, and on failure the
  stage, class, and typed refusal verbatim. Coverage health fails
  **before** fewer than two future partitions remain, so the alarm
  precedes the lapse.
- `partition_pruning` — the last pruning pass:
  `{"finalized": int, "detached": int, "dropped": int, "refusals": [...],
  "errors": [...]}`. `refusals` and `errors` are reason-carrying lists,
  not counts — a skipped partition arrives with the reason it was
  skipped, and remains a candidate on the next pass.
- `phase2_recovery` — the crashed-worker progression pass summary
  (considered / applied / retained / failed counts, quarantined rows,
  `raised_counted` rows whose progression raised a persistent error, and
  the quarantine function's refusals verbatim).
- `workflow_recovery` — the last workflow recovery pass: `recovered`,
  `errors`, and per case the rows selected, refusals and errors. The
  top-level `errors` adds every case and the failures that no case counted.
  `failures_counted` and `limit_reached` per case show the
  [recovery limit](../configuration/recovery-config#workflow-recovery-limit).
  `over_failure_limit` counts the `RUNNING` workflows at or over the limit:
  a live node holds such a workflow, and so does a limit write that fails.

`memory_usage_mb` is the **parent** process only. Task code runs in the worker's
children, so per-child memory growth is invisible there; `children_memory_mb`
captures it. The platform memory quota (e.g. Heroku R14) is measured against the
whole process tree. `children_memory_mb` sums RSS, so with a
[preload parent](../workers/worker-architecture#preload-parent) it counts each
shared page once per child, and it reads above real use. Use it to see one child
grow, and read the container or cgroup total for the footprint that the quota
enforces. To bound it, see
[process recycling](../workers/concurrency#process-recycling).

---
title: Health Reads
section: python
summary: The health methods of the Python app - database and worker pings, worker state snapshots, the stale and expired task lists, and the execution class coverage and report - with their signatures and result types.
related: [worker-health, broker-methods, ../rust/monitoring/health-reads]
tags: [monitoring, health, liveness, worker, api]
---

# Health Reads

The health methods are on the `Horsies` app. Each method returns `DatabaseResult[T]`: `Ok(value)` on success, or `Err(DatabaseOperationError)` with a `retryable` flag on failure. What each read means, and what to do with its result, is on [Worker & Database Health](/horsies/monitoring/worker-health/).

Each method has a sync variant with the same name without the `_async` suffix. The sync variant runs the read of the core on the calling thread, with the GIL released. On a thread that runs an event loop it returns `Err(DatabaseAsyncContext)` (`ASYNC_CONTEXT`) and does no I/O.

```python
from pydantic import SecretStr
from horsies import Horsies, AppConfig, PostgresConfig, is_ok, is_err

app = Horsies(AppConfig(
    database=PostgresConfig(
        database_url=SecretStr("postgresql://user:pass@localhost:5432/mydb"),
    ),
))
```

## How To

### Ping the Database

```python
result = await app.ping_database_async()
if is_ok(result):
    print(f"Postgres reachable in {result.ok_value.latency_ms:.1f} ms")
else:
    print(f"Unreachable: {result.err_value.code}")  # DATABASE_PING_FAILED
```

### Ping the Workers

```python
result = await app.ping_workers_async(wait_ms=2_000)
if is_ok(result):
    for pong in result.ok_value:
        print(f"{pong.worker_id} on {pong.hostname}:{pong.pid} — {pong.round_trip_ms:.1f} ms")
```

`wait_ms` is at most 60,000 (`WORKER_PING_WAIT_BOUND`, 60 s). A larger value is refused with `INVALID_INPUT` (`DatabaseInvalidInput`) before the ping is sent.

`MinResponsesAtLeast(1)` is a fast liveness gate:

```python
from horsies import MinResponsesAtLeast

# Returns on the first pong (milliseconds when the fleet is healthy).
result = await app.ping_workers_async(min_responses=MinResponsesAtLeast(1), wait_ms=2_000)
healthy = is_ok(result) and len(result.ok_value) >= 1
```

`PingTargetWorker(worker_id)` pings one worker and returns on its reply:

```python
from horsies import PingTargetWorker

result = await app.ping_workers_async(target=PingTargetWorker("worker-uuid"), wait_ms=2_000)
answered = is_ok(result) and len(result.ok_value) == 1
```

### Read the Worker States

```python
result = await app.list_worker_states_async()
if is_ok(result):
    for snap in result.ok_value:
        print(f"{snap.worker_id}: {snap.tasks_running} running, {snap.tasks_claimed} claimed")
```

Match `slot_health` to find the slots that are down:

```python
from horsies import SlotHealthAllServing, SlotHealthSlotsDown, is_ok

result = await app.list_worker_states_async()
if is_ok(result):
    for snap in result.ok_value:
        match snap.slot_health:
            case SlotHealthSlotsDown(slots=down):
                for slot in down:
                    print(f"{snap.worker_id} slot {slot.slot}: {slot.cause} since {slot.down_since}")
            case SlotHealthAllServing():
                pass
```

Read `execution_class_budgets` to see the execution classes that a worker serves:

```python
from horsies import ExecutionClassBudgets, is_ok

result = await app.list_worker_states_async()
if is_ok(result):
    for snap in result.ok_value:
        match snap.execution_class_budgets:
            case ExecutionClassBudgets(budgets=budgets):
                for budget in budgets:
                    served = ", ".join(member.value for member in budget.serves)
                    print(f"{snap.worker_id} {budget.budget.value}: {budget.capacity} in service, serves {served}")
```

Match `profile` to see the worker profile of each worker:

```python
from horsies import SnapshotProfileNamed, SnapshotProfileNotRecorded, is_ok

result = await app.list_worker_states_async()
if is_ok(result):
    for snap in result.ok_value:
        match snap.profile:
            case SnapshotProfileNamed(name=name):
                print(f"{snap.worker_id} runs the profile {name}")
            case SnapshotProfileNotRecorded():
                print(f"{snap.worker_id}: the snapshot holds no profile name")
```

Read the snapshots of one worker, newest first. A read returns at most 10,000 snapshots (`WORKER_HISTORY_ROW_BOUND`). A `WorkerHistoryLimitNewest(count)` above the bound is refused with `INVALID_INPUT`. `WorkerHistoryLimitAllRetained()` reads every retained snapshot, and is refused with `INVALID_INPUT` when the worker holds more than the bound: no snapshots are returned in part. Read such a worker with `WorkerHistoryLimitNewest(10_000)` or less:

```python
from horsies import WorkerHistoryLimitNewest

history = await app.get_worker_state_history_async(
    "worker-uuid", limit=WorkerHistoryLimitNewest(20),
)
if is_ok(history):
    for snap in history.ok_value:
        print(f"{snap.snapshot_at}: cpu={snap.cpu_percent}% mem={snap.memory_usage_mb}MB")
```

### List the Stale and Expired Tasks

A stale task is RUNNING, and its latest runner heartbeat, or its start when it has none, is older than the threshold. An expired task is PENDING past its `good_until`. Both reads return at most `TASK_LIST_ROW_BOUND` (10,000) rows:

```python
from horsies import BoundedTaskListComplete, BoundedTaskListTruncated, is_ok

result = await app.get_stale_tasks_async(threshold_minutes=5)
if is_ok(result):
    match result.ok_value:
        case BoundedTaskListComplete(rows=rows):
            pass
        case BoundedTaskListTruncated(rows=rows, bound=bound):
            print(f"the first {bound} stale tasks; more match")
    for row in rows:
        print(f"{row.task_id} {row.task_name} on {row.worker_hostname}: last beat {row.last_heartbeat_at}")

expired = await app.get_expired_tasks_async()
if is_ok(expired):
    for row in expired.ok_value.rows:
        print(f"{row.task_id} {row.task_name}: expired {row.expired_for} ago")
```

The reads do not change the tasks. The reaper recovers stale tasks; no worker claims an expired task.

### Check the Execution Class Coverage

```python
from horsies import StreamCoverageStateServed, StreamCoverageStateUnserved, StreamCoverageStateNoCapacity, is_ok

result = await app.get_execution_class_coverage_async()
if is_ok(result):
    for stream in result.ok_value.streams:
        match stream.coverage:
            case StreamCoverageStateUnserved():
                print(f"{stream.queue}/{stream.execution.value}: no live worker serves it")
            case StreamCoverageStateNoCapacity(workers=workers):
                print(f"{stream.queue}/{stream.execution.value}: {workers} worker(s), no child in service")
            case StreamCoverageStateServed():
                pass
```

### Read the Execution Class Report

```python
from horsies import StreamWaitDue, StreamWaitNoneDue, is_ok

result = await app.get_execution_class_report_async()
if is_ok(result):
    report = result.ok_value
    for stream in report.streams:
        match stream.wait:
            case StreamWaitDue(wait=wait):
                print(f"{stream.queue}/{stream.execution.value}: oldest waits {wait}")
            case StreamWaitNoneDue(next_due_at=due):
                print(f"{stream.queue}/{stream.execution.value}: next due {due}")
    for worker in report.workers:
        for budget in worker.budgets:
            print(f"{worker.worker_id} {budget.budget.value}: {budget.free}/{budget.capacity} free")
```

### Find the Workers That Do Not Answer

A worker that `list_worker_states_async` lists and `ping_workers_async` does not return did not answer:

```python
states = await app.list_worker_states_async()
pongs = await app.ping_workers_async(wait_ms=2_000)
if is_ok(states) and is_ok(pongs):
    known = {s.worker_id for s in states.ok_value}
    responsive = {p.worker_id for p in pongs.ok_value}
    suspect = known - responsive
    if suspect:
        print(f"Non-responsive workers: {suspect}")
```

## API Reference

### `ping_database_async() -> DatabaseResult[DatabasePing]`

**Returns:** `Ok(DatabasePing)` with `latency_ms: float`; `Err` with code `DATABASE_PING_FAILED` on failure.

### `ping_workers_async(*, target: PingTarget = PingTargetEveryWorker(), wait_ms: int = 2_000, min_responses: MinResponses = MinResponsesFullWindow()) -> DatabaseResult[list[WorkerPong]]`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `target` | `PingTargetEveryWorker() \| PingTargetWorker(worker_id)` | `PingTargetEveryWorker()` | `PingTargetEveryWorker()` sends the ping to all workers; `PingTargetWorker(worker_id)` pings one worker and returns on its reply |
| `wait_ms` | `int` | `2_000` | The collection window, in milliseconds; 1 to 60,000 (`WORKER_PING_WAIT_BOUND`) |
| `min_responses` | `MinResponsesFullWindow() \| MinResponsesAtLeast(count)` | `MinResponsesFullWindow()` | `MinResponsesAtLeast(count)` returns as soon as `count` distinct workers reply; `count` is 1 or more. `MinResponsesFullWindow()` waits the full window |

**Returns:** `Ok(list[WorkerPong])`: one entry per distinct worker that replied, possibly empty. Each `WorkerPong` has `worker_id`, `hostname`, `pid`, `round_trip_ms`. `Err` with code `INVALID_INPUT` on a `wait_ms` that is not an `int` or is outside 1 to 60,000, or a `count` below 1; `Err` with code `WORKER_PING_FAILED` on a reply-channel failure.

### `list_worker_states_async() -> DatabaseResult[list[WorkerStateSnapshot]]`

**Returns:** `Ok(list[WorkerStateSnapshot])`, the latest snapshot of each worker; `Err` with code `MONITORING_QUERY_FAILED` on failure, also when a stored `slots_down` or `class_capacity` column value does not have the shape that the worker writes. The message names the worker.

### `get_worker_state_async(worker_id: str) -> DatabaseResult[WorkerStateSnapshot | None]`

**Returns:** `Ok(WorkerStateSnapshot)`, `Ok(None)` if the worker is unknown, or `Err(MONITORING_QUERY_FAILED)`.

### `get_worker_state_history_async(worker_id: str, *, limit: WorkerHistoryLimit = WorkerHistoryLimitAllRetained()) -> DatabaseResult[list[WorkerStateSnapshot]]`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `worker_id` | `str` | — | Worker instance id |
| `limit` | `WorkerHistoryLimitAllRetained() \| WorkerHistoryLimitNewest(count)` | `WorkerHistoryLimitAllRetained()` | `WorkerHistoryLimitAllRetained()` returns all retained rows; `WorkerHistoryLimitNewest(count)` returns the newest `count` rows. `count` is 1 to 10,000 (`WORKER_HISTORY_ROW_BOUND`). `WorkerHistoryLimitAllRetained()` is refused when the worker holds more than 10,000 retained rows |

**Returns:** `Ok(list[WorkerStateSnapshot])`, newest first; `Err(INVALID_INPUT)` on a `count` that is not in 1 to 10,000, or on `WorkerHistoryLimitAllRetained()` for a worker that holds more than 10,000 retained rows; `Err(MONITORING_QUERY_FAILED)` on a query failure.

### `get_stale_tasks_async(threshold_minutes: int) -> DatabaseResult[BoundedTaskList[StaleTaskRow]]`

| Parameter | Type | Description |
|-----------|------|-------------|
| `threshold_minutes` | `int` | Heartbeat age after which a RUNNING task is stale: whole minutes from 0 to 525,600 (one year) |

**Returns:** `Ok(BoundedTaskListComplete(rows))` with every stale task, or `Ok(BoundedTaskListTruncated(rows, bound))` with the first 10,000 when more match. Rows come ordered by the latest heartbeat, rows with none first, then by task id. `Err(INVALID_INPUT)` on a `threshold_minutes` that is not an `int` from 0 to 525,600; nothing is read. `Err(MONITORING_QUERY_FAILED)` on a query failure.

### `get_expired_tasks_async() -> DatabaseResult[BoundedTaskList[ExpiredTaskRow]]`

**Returns:** `Ok(BoundedTaskListComplete(rows))` with every PENDING task past its `good_until`, or `Ok(BoundedTaskListTruncated(rows, bound))` with the first 10,000 when more match. Rows come ordered by `good_until`, oldest first, then by task id. `Err(MONITORING_QUERY_FAILED)` on a query failure.

### `get_execution_class_coverage_async() -> DatabaseResult[ExecutionClassCoverage]`

**Returns:** `Ok(ExecutionClassCoverage)`; `Err(MONITORING_QUERY_FAILED)` for the failures on [Does a live worker serve every pending class?](/horsies/monitoring/worker-health/#does-a-live-worker-serve-every-pending-class).

### `get_execution_class_report_async() -> DatabaseResult[ExecutionClassReport]`

**Returns:** `Ok(ExecutionClassReport)`; `Err(MONITORING_QUERY_FAILED)` for the failures on [How long do pending tasks wait?](/horsies/monitoring/worker-health/#how-long-do-pending-tasks-wait-and-what-capacity-is-free).

### Result Types

| Type | Fields |
|------|--------|
| `DatabasePing` | `latency_ms: float` |
| `WorkerPong` | `worker_id: str`, `hostname: str`, `pid: int`, `round_trip_ms: float` |
| `StaleTaskRow` | `task_id: UUID`, `task_name: str`, `worker_hostname: str \| None`, `worker_pid: int \| None`, `worker_process_name: str \| None`, `started_at: datetime`, `last_heartbeat_at: datetime \| None` (`None` when the task has no runner heartbeat since its start) |
| `ExpiredTaskRow` | `task_id: UUID`, `task_name: str`, `queue_name: str`, `priority: int`, `sent_at: datetime`, `enqueued_at: datetime`, `good_until: datetime`, `expired_for: timedelta` (on the database clock) |
| `BoundedTaskListComplete` | `rows: tuple[Row, ...]`: every matching row |
| `BoundedTaskListTruncated` | `rows: tuple[Row, ...]`, `bound: int`: the first `bound` matching rows; more rows match |
| `ExecutionClassCoverage` | `checked_at: datetime`, `live_workers: int`, `streams: tuple[StreamCoverage, ...]` |
| `StreamCoverage` | `queue: str`, `execution: Execution`, `coverage: StreamCoverageStateServed \| StreamCoverageStateNoCapacity \| StreamCoverageStateUnserved` |
| `StreamCoverageStateServed` | `workers: int`, `capacity: int` |
| `StreamCoverageStateNoCapacity` | `workers: int` |
| `StreamCoverageStateUnserved` | (no fields) |
| `ExecutionClassReport` | `checked_at: datetime`, `live_window: timedelta`, `streams: tuple[PendingStream, ...]`, `workers: tuple[WorkerExecutionClassLoad, ...]` |
| `PendingStream` | `queue: str`, `execution: Execution`, `coverage` (as in `StreamCoverage`), `wait: StreamWaitDue \| StreamWaitNoneDue` |
| `StreamWaitDue` | `oldest_due_at: datetime`, `wait: timedelta` |
| `StreamWaitNoneDue` | `next_due_at: datetime` |
| `WorkerExecutionClassLoad` | `worker_id: str`, `snapshot_at: datetime`, `budgets: tuple[BudgetLoad, ...]` |
| `BudgetLoad` | `budget: Execution`, `capacity: int`, `serves: tuple[Execution, ...]`, `in_flight: int`, and the property `free: int` |

The state names map to the states on the shared page: `StreamCoverageStateServed` is served, `StreamCoverageStateNoCapacity` is without capacity, `StreamCoverageStateUnserved` is unserved.

### `WorkerStateSnapshot` Types

The meaning of each field is on [Snapshot Fields](/horsies/monitoring/worker-health/#snapshot-fields).

| Field | Type |
|-------|------|
| `worker_id` | `str` |
| `snapshot_at` | `datetime` |
| `hostname` | `str` |
| `pid` | `int` |
| `child_count` | `int` |
| `claim_limits` | `ClaimLimits(per_queue_per_pass, claimed_per_worker)`: each `TaskLimit(tasks)` or `NoTaskLimit()` |
| `cluster_wide_cap` | `ClusterCap \| NoClusterCap` |
| `queues` | `list[str]` |
| `queue_priorities` | `dict[str, int]` |
| `queue_max_concurrency` | `dict[str, int]` |
| `recovery_config` | `dict[str, Any]` |
| `tasks_running` | `int` |
| `tasks_claimed` | `int` |
| `memory_usage_mb` | `float \| None` |
| `memory_percent` | `float \| None` |
| `cpu_percent` | `float \| None` |
| `children_memory_mb` | `float \| None` |
| `worker_started_at` | `datetime` |
| `slot_health` | `SlotHealthAllServing()` or `SlotHealthSlotsDown(slots)`, where `slots` holds one `DownSlot` per slot that was down, in slot order |
| `execution_class_budgets` | `ExecutionClassBudgets(budgets)`, where `budgets` holds one `ExecutionClassBudget(budget, capacity, serves)` per budget, in `Execution` order |
| `profile` | `SnapshotProfileNamed(name)` or `SnapshotProfileNotRecorded()` |

`DownSlot` fields: `slot` (index), `cause` (`ResourceShortage(errno)` or `ThreadStartRefused()`), `detail` (`str`), `down_since` (aware `datetime`), `failed_boots` (`int`, at least 1).

---
title: Health Reads
summary: The health reads on the app - database and worker pings, worker state snapshots, stale and expired tasks, and the execution class reads.
related: [../tasks/retrieving-results, ../../monitoring/worker-health, ../../workers/heartbeats-recovery]
tags: [rust, monitoring, health, ping, worker state, stale tasks]
---

# Health Reads

The app has the health reads of the Python app. Each read connects the app's handle, installs the schema once per handle as a send does, and reads only. What each read means is on [Worker & Database Health](/horsies/monitoring/worker-health/).

```rust
use std::num::NonZeroUsize;
use std::time::Duration;

use horsies::{BrokerOperationError, Horsies, PingTarget};

async fn check_health(app: &Horsies) -> Result<(), BrokerOperationError> {
    let ping = app.ping_database().await?;
    let pongs = app
        .ping_workers(PingTarget::EveryWorker, Duration::from_secs(2), NonZeroUsize::new(1))
        .await?;
    println!("{ping:?}, {} workers answered", pongs.len());
    Ok(())
}
```

## Reads

| Method | Returns | Failure code |
|--------|---------|--------------|
| `ping_database()` | `DatabasePing`: the latency of a timed `SELECT 1` round trip | `DB_PING_FAILED` |
| `ping_workers(target, timeout, min_responses)` | `Vec<WorkerPong>`: the distinct replies by worker id | `INVALID_INPUT` for a zero `timeout`, with no statement; `LISTENER_DISABLED` for an app whose schema mode has no listener; `WORKER_PING_FAILED` |
| `list_worker_states()` | `Vec<WorkerStateSnapshot>`: the latest snapshot of every worker that reported one, idle workers included | `MONITORING_QUERY_FAILED` |
| `get_worker_state(worker_id)` | `Option<WorkerStateSnapshot>`: `None` when the worker never reported one | `MONITORING_QUERY_FAILED` |
| `get_worker_state_history(worker_id, limit)` | `Vec<WorkerStateSnapshot>`, newest first | `MONITORING_QUERY_FAILED` |
| `get_stale_tasks(threshold)` | `BoundedTaskList<StaleTaskRow>` | `MONITORING_QUERY_FAILED` |
| `get_expired_tasks()` | `BoundedTaskList<ExpiredTaskRow>` | `MONITORING_QUERY_FAILED` |
| `get_class_coverage()` | `ClassCoverage`: for each pending stream of an execution class, whether a live worker can run it | `MONITORING_QUERY_FAILED` |
| `get_class_report()` | `ClassReport`: the load of each execution class over the live workers | `MONITORING_QUERY_FAILED` |

Every read returns `Result<_, BrokerOperationError>`, an enum with one variant per state and the methods `code()`, `message()` and `retryable()`. A failed read is `OperationFailed` with the `BrokerOperationFailedCode` of the read, a refused argument is `InvalidInput`, and an app with no listener is `ListenerDisabled`. A failed connect or schema install has the code of the read. A stored worker snapshot of another shape is `MONITORING_QUERY_FAILED`, and the message names the worker and the column.

## Arguments

`ping_workers` collects replies until the first of these holds:

| Argument | Stop condition |
|----------|----------------|
| `target: PingTarget::Worker(worker_id)` | That worker replied |
| `min_responses: Some(n)` | `n` distinct workers replied |
| `timeout` | The time passed. With `PingTarget::EveryWorker` and no `min_responses`, the read waits the whole `timeout` and lists every worker that replied |

A worker that `list_worker_states` lists and `ping_workers` does not return did not answer.

`get_worker_state_history` takes a `WorkerHistoryLimit`: `AllRetained` (every retained snapshot) or `Newest(NonZeroU32)` (the newest `n`).

`get_stale_tasks` takes a `StaleMinutes`. `StaleMinutes::parse(minutes)` accepts 0 to `MAX_STALE_THRESHOLD_MINUTES` (525,600, one year) whole minutes and refuses another value with `StaleThresholdRefusal`. A RUNNING task is stale when its latest runner beat, or its start when it has no beat, is older than the threshold. The read does not recover the task: the reaper does.

`get_expired_tasks` lists PENDING tasks past their `good_until`, oldest deadline first. No worker claims them.

## Bounded Task Lists

`get_stale_tasks` and `get_expired_tasks` return at most `TASK_LIST_BOUND` (10,000) rows:

| `BoundedTaskList<T>` | Holds |
|----------------------|-------|
| `Complete(rows)` | Every matching row |
| `Truncated { rows, bound }` | The first `bound` matching rows; more rows match |

---
title: Heartbeats & Recovery
summary: Detecting and recovering from worker crashes via heartbeats.
related: [../../configuration/recovery-config, worker-architecture]
tags: [workers, heartbeats, recovery, crash-detection]
---

## Overview

When a worker dies mid-task, the task would be stuck forever without detection. Heartbeats solve this:

1. Workers send periodic heartbeats for their tasks
2. A reaper checks for missing heartbeats
3. Stale tasks are automatically recovered

## Heartbeat Types

### Claimer Heartbeat

Sent by the main worker process for CLAIMED tasks:

- Indicates worker is alive and will soon start the task
- Sent at `claimer_heartbeat_interval_ms` interval
- Covers the gap between claim and execution start

### Runner Heartbeat

Written by the worker process for the RUNNING tasks that its children hold:

- Indicates that the task's child process runs it
- One thread of the worker writes one statement per `runner_heartbeat_interval_ms` for all of these tasks. The thread does not run on the worker's event loop
- The child writes the first beat, in the transaction that sets the row RUNNING
- Task code cannot stop it: C code that holds the GIL in a child does not reach the worker process

The statement beats a row only when all of these are true:

- The row is RUNNING and owned by this worker, at the claim generation of the dispatch
- The child marked no result as finalizing on the row
- The child process runs: it did not exit, and the worker sent it no kill

So the beats of a task end when the worker kills its child (for example at `timeout_ms`), when the child exits, when the child marks its result, and when the worker reads the result.

### When the Worker Cannot Write Heartbeats

The worker stops only when the reaper would fail its tasks, so the rule follows its recovery config. The worker decides it once, at start.

With `auto_fail_stale_running=True` (the default), the worker fails closed, so the reaper does not fail a task that still runs. If the heartbeat thread dies, or no heartbeat lands for half of `running_stale_threshold_ms`, the worker:

1. Logs the cause at critical level
2. Kills every child that holds a task, with its session
3. Stops, and exits with code 1, so that a supervisor restarts it

If the worker has not stopped 120 seconds later, it exits with code 77. A failed write is tried again within that half, after 0.25 s and then after doubling waits of at most 5 s.

The half counts from when the last heartbeat landed. Heartbeats land one `runner_heartbeat_interval_ms` apart, plus the time the next one takes to land. So `RecoveryConfig` refuses, with `HRS-204`, a `running_stale_threshold_ms` below `2 × (runner_heartbeat_interval_ms + 2 s)` while `auto_fail_stale_running` is on. At that smallest threshold, a heartbeat that takes more than 2 s to land stops the worker. The reaper may count a heartbeat from the start of its write. So after a heartbeat whose write took almost the whole half, the worker stops 0.5 s before `running_stale_threshold_ms` has passed from the start of that write.

Each killed task takes the [child crash](#child-process-crash) path when the worker can still reach the database, and stale RUNNING recovery when it cannot.

With `auto_fail_stale_running=False`, or with no recovery config, no reaper fails a RUNNING row, so a missing heartbeat puts no running task at risk. The worker keeps running. It logs failed writes and late heartbeats, and a dead heartbeat thread once.

Failed writes are logged at the first failure of a run, then at most once per 60 seconds.

## Heartbeat Flow

<!-- todo:diagram-needed - Heartbeat sequence diagram -->

```text
CLAIMED                          RUNNING
   │                               │
   │  Claimer heartbeat            │  Runner heartbeat
   │  (from main process)          │  (from main process)
   │                               │
   ├──── HB ────┐                  ├──── HB ────┐
   │            │                  │            │
   ├──── HB ────┤  30s interval    ├──── HB ────┤  30s interval
   │            │                  │            │
   └────────────┴──────────────────┴────────────┴───>
```

## Stale Detection

The reaper periodically checks for stale tasks:

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Simplified logic
for task in tasks.filter(status='CLAIMED'):
    last_hb = get_latest_heartbeat(task, role='claimer')
    if now - last_hb > claimed_stale_threshold:
        requeue(task)  # Safe - code never ran

for task in tasks.filter(status='RUNNING'):
    last_hb = get_latest_heartbeat(task, role='runner')
    if task.finalizing_at is recent:
        continue  # Child finished; parent may still be committing the result
    if now - last_hb > running_stale_threshold:
        fail(task)  # Not safe to requeue
```

## Recovery Actions

### Stale CLAIMED → PENDING

When a CLAIMED task has no recent claimer heartbeat:

- **Safe to requeue**: User code never started
- Task reset to PENDING
- Another worker will claim it
- No data corruption risk

### Stale RUNNING Recovery

When a RUNNING task has no recent runner heartbeat:

- **Not safe to blindly requeue**: Code was executing, may have partial side effects
- If the task has a retry policy with `WORKER_CRASHED` in `auto_retry_for` and retries remaining: scheduled for retry (returns to PENDING with `next_retry_at`)
- Otherwise: marked as FAILED with `WORKER_CRASHED` error
- Recent `finalizing_at` suppresses recovery so a completed child is not failed
  while the parent is still writing terminal state

`running_stale_threshold_ms` measures missing heartbeats, not task wall-clock
duration. A task can run longer than the threshold while its child holds it and
the worker writes its runner heartbeats.

### Child Process Crash

Each task runs in its own child process. When a child exits abnormally (a segfault in a C extension, a kill by the operating system such as an out-of-memory kill, `os._exit()` in task code, or the kill after `timeout_ms`), the worker detects this at once, without waiting for heartbeats. It recovers the task of that child and starts a new child in that slot:

<!-- members: TaskStatus subset -->
- CLAIMED task (user code not started): returned to PENDING
- RUNNING task (user code started): if the task has a retry policy with `WORKER_CRASHED` in `auto_retry_for` and retries remaining, scheduled for retry; otherwise marked as FAILED with `WORKER_CRASHED` error

The worker writes this only after it sees the child exit. If the child does not exit within 30 seconds of a kill by the worker, the worker writes nothing and logs at critical level. A RUNNING task then goes through the stale RUNNING recovery above, and a CLAIMED task is claimed again when its claim lease ends.

Other children and their tasks continue. The worker also kills the subprocesses that the lost child leaves in its session. A task that exceeded `timeout_ms` fails with `TASK_TIMEOUT` instead, also only after the worker sees the child exit.

### Workflow Task Recovery

When a worker crashes during a **workflow** task, the reaper marks `tasks.status = FAILED`, but the worker dies before calling `on_workflow_task_complete()`. This leaves `workflow_tasks.status` stuck in `RUNNING` while the underlying task is already terminal.

The recovery loop detects this mismatch automatically:

1. Finds `workflow_tasks` rows in non-terminal status where the linked `tasks` row is terminal (`COMPLETED`, `FAILED`, or `CANCELLED`)
2. Deserializes the `TaskResult` from the task's stored result
3. If no result is stored, synthesizes an error result:
   - `WORKER_CRASHED` for failed tasks
   - `TASK_CANCELLED` for cancelled tasks
   - `RESULT_NOT_AVAILABLE` for completed tasks with missing results
4. Triggers the normal completion path: updates `workflow_tasks` status, applies `on_error` policy, propagates to dependents, and checks workflow completion

This runs before workflow finalization, so dependents are resolved in the same recovery pass.

## Configuration

<!-- check: skip elided -- ... stands for a config shown on other pages -->
```python
from horsies.core.models.recovery import RecoveryConfig

config = AppConfig(
    broker=PostgresConfig(...),
    recovery=RecoveryConfig(
        # Claimer detection
        auto_requeue_stale_claimed=True,
        claimed_stale_threshold_ms=120_000,     # 2 minutes
        claimer_heartbeat_interval_ms=30_000,   # 30 seconds

        # Runner detection
        auto_fail_stale_running=True,
        running_stale_threshold_ms=300_000,     # 5 minutes
        finalizing_stale_threshold_ms=300_000,  # 5 minutes
        runner_heartbeat_interval_ms=30_000,    # 30 seconds

        # Check frequency
        check_interval_ms=30_000,               # 30 seconds
    ),
)
```

## Timing Guidelines

### Rule: Thresholds Hold the Heartbeats

- With `auto_fail_stale_running=True` (the default): `running_stale_threshold_ms >= 2 × (runner_heartbeat_interval_ms + 2_000)`. The worker stops when no heartbeat lands for half of the threshold, and that half must hold one interval plus the time the next heartbeat takes to land.
- With `auto_fail_stale_running=False`: `running_stale_threshold_ms >= 2 × runner_heartbeat_interval_ms`.
- `finalizing_stale_threshold_ms >= 2 × runner_heartbeat_interval_ms`.
- `claimed_stale_threshold_ms >= 2 × claimer_heartbeat_interval_ms`.

A config that breaks a rule raises `ConfigurationError` with code `HRS-204`, which names the fields and the smallest valid value.

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# Valid
RecoveryConfig(
    runner_heartbeat_interval_ms=30_000,  # 30s
    running_stale_threshold_ms=64_000,    # 64s = 2 x (30s + 2s)
    finalizing_stale_threshold_ms=60_000, # 60s (2x)
)

# Invalid - raises ConfigurationError (HRS-204)
RecoveryConfig(
    runner_heartbeat_interval_ms=30_000,  # 30s
    running_stale_threshold_ms=60_000,    # half is 30s: no time for a heartbeat to land
)
```

### For CPU-Heavy Tasks

A task that holds the GIL does not delay its runner heartbeats, because the worker process writes them. CPU-bound tasks need no extra margin in the thresholds.

### For Quick Tasks

Fast tasks can use tighter detection:

```python
RecoveryConfig(
    runner_heartbeat_interval_ms=10_000,   # 10 seconds
    running_stale_threshold_ms=40_000,     # 40 seconds
)
```

The worker stops when no heartbeat lands for 20 seconds, so a heartbeat may take up to 10 seconds to land.

### Recovery Time After a Worker Stops

A child process crash is detected at once (see [Child Process Crash](#child-process-crash)). When the whole worker process or its machine stops, its children stop too, and its RUNNING tasks are recovered by the reaper, which runs inside workers. The reaper treats a task as stale `running_stale_threshold_ms` after its last runner heartbeat and acts on it at its next pass (`check_interval_ms`). With the defaults (heartbeat every 30 s, threshold 300 s, reaper pass every 30 s), recovery happens 4.5 to 5.5 minutes after the worker stops. A `WORKER_CRASHED` retry then waits its retry policy delay, counted from recovery. If no worker is running, recovery waits until a worker starts.

Until then, the `RUNNING` rows of the dead worker count against each queue's `max_concurrency`. A new worker has free children, but a queue at its limit waits for the reaper.

To recover faster, lower `runner_heartbeat_interval_ms` and `running_stale_threshold_ms` together. A lower threshold also makes a worker stop sooner when it cannot reach the database (after half of the threshold, when `auto_fail_stale_running` is on; see [When the Worker Cannot Write Heartbeats](#when-the-worker-cannot-write-heartbeats)), and raises the risk of [false positives](#false-positives-tasks-marked-stale-but-running).

## Database Schema

Heartbeats are stored in the `horsies_heartbeats` table:

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | int | Auto-increment ID |
| `task_id` | str | Task being tracked |
| `sender_id` | str | Worker id. The first runner beat, which the child writes, has `<worker id>:<child pid>` |
| `role` | str | 'claimer' or 'runner' |
| `sent_at` | datetime | Heartbeat timestamp |
| `hostname` | str | Machine hostname |
| `pid` | int | Process ID of the sender |

## Manual Recovery

Query stale tasks:

```python
from horsies import Err, Ok

broker = app.get_broker()

# Find stale RUNNING tasks. get_stale_tasks() returns Ok(list of rows) or Err(BrokerOperationError)
match await broker.get_stale_tasks(stale_threshold_minutes=5):
    case Err(query_error):
        print(f"Query failed: {query_error.message}")
    case Ok(stale):
        for task in stale:
            print(f"Stale: {task['id']} on {task['worker_hostname']}")
```

Force recovery:

```python
# Manually fail stale RUNNING
failed = await broker.mark_stale_tasks_as_failed(stale_threshold_ms=300_000)
print(f"Failed {failed} stale tasks")

# Manually requeue stale CLAIMED
requeued = await broker.requeue_stale_claimed(stale_threshold_ms=120_000)
print(f"Requeued {requeued} stale tasks")
```

## Disabling Recovery

Not recommended, but possible:

```python
RecoveryConfig(
    auto_requeue_stale_claimed=False,
    auto_fail_stale_running=False,
)
```

Tasks will remain stuck until manually resolved.

## Partitioned Storage and Cleanup

`horsies_heartbeats` is partitioned by hour. Workers create partitions ahead
of writes — at startup and on a periodic maintenance pass every
`partition_maintenance_interval_s` (default 900s), keeping
`heartbeat_leaf_horizon_hours` (default 6) complete future partitions
available.

The same pass prunes: per finite retention class, every partition past its
horizon is detached (`DETACH PARTITION CONCURRENTLY`, under a 5 s statement
timeout) and dropped whole — there is no row-delete pass and no row-by-row
scan. A refusal — recovery evidence still pinning a partition, or a reader
holding the detach past its timeout — skips that partition and reports it
with its reason on the worker health surface, and the partition is retried
on every pass until the blocker clears. The `forever` history class is
never pruned.

Detach latency is a rule, not a constant: the age of the longest in-flight
transaction that could still touch the parent, plus a few milliseconds. It
does not scale with partition size, and readers are never blocked — the
concurrent detach waits so that queries do not. In production the wait is
capped at 5 s: a longer-lived reader produces a contained timeout and a
retry on the next pass, never a stalled worker.

`heartbeat_retention_hours` was removed in 0.5.0 — setting it fails
validation naming the successor, because a row-delete window no longer
exists.

Partition coverage requires the worker role to hold `CREATE` on the partition
parents. A deployment that withholds that privilege must create partitions
from an external cron instead; coverage health is published on the worker
health surface and fails **before** fewer than two future partitions remain,
so the alarm precedes the lapse rather than reporting it.

## Troubleshooting

### False Positives (Tasks Marked Stale But Running)

With `auto_fail_stale_running` on, a worker that cannot write runner heartbeats kills its tasks before the reaper acts on them. A task can still be marked stale while it runs when the worker process itself cannot run its threads:

- The worker process is paused, for example by SIGSTOP or a frozen container, while its children run
- C code in the worker process, such as a C extension call in a workflow's `build_with()`, holds the GIL for longer than `running_stale_threshold_ms` minus one heartbeat interval. Python code does not: it lets other threads run

Increase thresholds:

```python
RecoveryConfig(
    running_stale_threshold_ms=600_000,  # 10 minutes
)
```

### Worker Exits With a Runner Heartbeat Error

The log names the cause: the heartbeat thread died, or no heartbeat landed for half of `running_stale_threshold_ms`, with the last database error. Check the database connectivity of the worker host. Under a slow database, raise `running_stale_threshold_ms`: the worker stops after half of it, and a heartbeat may take that half less one `runner_heartbeat_interval_ms` to land.

### Tasks Not Recovering

Check:

- `auto_requeue_stale_claimed` / `auto_fail_stale_running` enabled?
- Reaper loop running? (Check worker logs)
- Database connectivity?

---
title: Heartbeats & Recovery
section: shared
summary: Detecting and recovering from worker crashes via heartbeats and the worker liveness lock.
related: [../../configuration/recovery-config, worker-architecture, ../../concepts/fencing-token]
tags: [workers, heartbeats, recovery, crash-detection]
---

## Overview

When a worker dies mid-task, the task would be stuck forever without detection. Heartbeats solve this:

1. Workers send periodic heartbeats for their tasks
2. A reaper checks for missing heartbeats
3. Stale tasks are automatically recovered

Each worker process also holds a liveness lock. When its process ends, the reaper recovers its tasks without waiting for the heartbeat thresholds (see [Worker Liveness Lock](#worker-liveness-lock)).

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
- The child writes no beat. A task gets its first beat at the first pass of the thread after its start; the passes are one `runner_heartbeat_interval_ms` apart
- Task code cannot stop it: C code that holds the GIL in a child does not reach the worker process

The statement beats a row only when all of these are true:

- The row is RUNNING and owned by this worker, at the claim generation of the dispatch
- The child marked no result as finalizing on the row
- The child process runs: it did not exit, and the worker sent it no kill

So the beats of a task end when the worker kills its child (for example at `timeout_ms`), when the child exits, when the child marks its result, and when the worker reads the result. An `async def` task in an isolated child that returns while asyncio tasks that it started still run is marked only after that work ended, so its beats cover that work (see [Async Tasks](/horsies/tasks/defining-tasks/#async-tasks)).

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

## Worker Liveness Lock

Each worker process holds a session advisory lock on one connection of its own to the session URL (`session_database_url`, else `database_url`). The key comes from the worker id, which is new at each process start. The worker takes the lock before its first claim and holds it until its stop ends. The server releases the lock when the session of the process ends: when the process exits or is killed, its operating system closes the connection.

Each acquisition of the lock writes a new generation for the worker id, in the same transaction, to the table `horsies_worker_liveness`. The worker counts the lock as held again only after that transaction committed.

The worker confirms the lock every 5 seconds. After a lost connection, it opens a new one and takes the lock again. A lock that another session of the worker still holds, because the server has not yet ended the lost session, counts as held. After a lock statement of the worker failed or did not end, such a lock does not count until the worker takes the lock itself: the session of that statement can hold the lock with no generation. When the lock is not held for 60 seconds:

- With `auto_fail_stale_running=True` (the default) and a task held, the worker fails closed: it kills every child that holds a task and exits with code 79 at once, with no drain. A supervisor restarts it.
- With no task held, no task is at risk. The worker logs it, runs on, and keeps taking the lock again. A task that it starts before the lock is held again makes it fail closed.
- With `auto_fail_stale_running=False`, no reaper recovers a RUNNING row, so the worker logs it and runs on.

A worker that is alive but stalled, for example a process paused by `SIGSTOP`, keeps its connection and so keeps its lock. Its children run on, and the task of such a child can be retried while it runs. The late writes of that attempt to horsies tables are refused; for its writes to other systems, use the [fencing token](/horsies/concepts/fencing-token/).

Each reaper pass reads the owners of CLAIMED and RUNNING rows whose lock no session holds, each with the generation of its last acquisition. The read takes no lock. An owner is gone when all of these are true:

- Two passes of one reaper find its lock free at least 75 seconds apart (60 seconds and a margin of 15 seconds), both at the same generation.
- No pass of that reaper between them finds the lock held, or finds another generation.
- A read of the generation after the second pass still finds that generation.

A worker that takes its lock again between two passes writes a new generation, so the reaper never counts two losses of the lock, with a hold between them, as one loss. An owner with no recorded generation is not judged gone by this rule: the heartbeat thresholds apply to its rows. At each retention sweep (`retention_sweep_interval_s`), the reaper deletes the generation rows of workers that own no CLAIMED or RUNNING row and whose lock no session holds.

The reaper then recovers the rows that the gone worker claimed before the first of the two passes:

- Its RUNNING rows, with `auto_fail_stale_running=True`, as [Stale RUNNING Recovery](#stale-running-recovery) states, whatever their heartbeats and their `finalizing_at`. The `WORKER_CRASHED` error names the time from which the lock was free.
- Its CLAIMED rows, with `auto_requeue_stale_claimed=True`, back to PENDING as [Stale CLAIMED → PENDING](#stale-claimed--pending) states, whatever their heartbeats.

Each statement checks again, under the row lock, that the row still belongs to the gone worker: at the claim generation that the reaper read for a RUNNING row, and claimed before the first of the two passes for a CLAIMED row. The lock takes one connection per worker; see [Connections](/horsies/workers/worker-profiles/#connections).

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

```text
# Simplified logic
for task in tasks.filter(status='CLAIMED'):
    last_hb = get_latest_heartbeat(task, role='claimer')
    if now - last_hb > claimed_stale_threshold:
        requeue(task)  # Safe - code never ran

for task in tasks.filter(status='RUNNING'):
    # Only the beats of this attempt: an earlier attempt's beats are older than started_at
    last_hb = get_latest_heartbeat(task, role='runner', since=task.started_at) or task.started_at
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
- Only the runner heartbeats sent at or after the task's `started_at` count. A
  task with no such heartbeat counts from `started_at`. A heartbeat of an
  earlier attempt of a retried task never counts for the new attempt

`running_stale_threshold_ms` measures missing heartbeats, not task wall-clock
duration. A task can run longer than the threshold while its child holds it and
the worker writes its runner heartbeats.

A worker that stops after a task returned and before the outcome of the task
committed loses that outcome. This is the case for an outcome that waits for a
connection or for its batch, and for a batch statement that did not commit.
The row stays RUNNING with its finalizing mark, and the reaper treats it as a
crashed attempt after `finalizing_stale_threshold_ms`: `WORKER_CRASHED`, with a
retry when the retry policy allows it. The body of the task ran, so its side
effects happened.

### Child Process Crash

Each task runs in its own child process. When a child exits abnormally (a segfault in a C extension, a kill by the operating system such as an out-of-memory kill, `os._exit()` in task code, or the kill after `timeout_ms`), the worker detects this at once, without waiting for heartbeats. It recovers the task of that child and starts a new child in that slot:

<!-- members: TaskStatus subset -->
- CLAIMED task (user code not started): returned to PENDING
- RUNNING task (user code started): if the task has a retry policy with `WORKER_CRASHED` in `auto_retry_for` and retries remaining, scheduled for retry; otherwise marked as FAILED with `WORKER_CRASHED` error

The worker writes this only after it sees the child exit. If the child does not exit within 30 seconds of a kill by the worker, the worker writes nothing and logs at critical level. A RUNNING task then goes through the stale RUNNING recovery above, and a CLAIMED task is claimed again when its claim lease ends.

Other children and their tasks continue. The worker also kills the subprocesses that the lost child leaves in its session. A task that exceeded `timeout_ms` fails with `TASK_TIMEOUT` instead, also only after the worker sees the child exit.

### Workflow Task Recovery

When a worker crashes during a **workflow** task, the reaper marks `tasks.status = FAILED`, but the worker dies before its phase 2 (`horsies_workflow_node_finish`). This leaves `workflow_tasks.status` stuck in `RUNNING` while the underlying task is already terminal.

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

The `recovery` section of the app config sets the heartbeat intervals, the stale thresholds and the reaper interval. The fields, their defaults and their ranges are on [Recovery Config](/horsies/configuration/recovery-config/#fields). The API is on the client pages: [Recovery Configuration](/horsies/configuration/app-config/#recovery-configuration) (Python) and [AppConfig](/horsies/rust/configuration/app-config/#appconfig) (Rust).

## Timing Guidelines

### Rule: Thresholds Hold the Heartbeats

- With `auto_fail_stale_running=True` (the default): `running_stale_threshold_ms >= 2 × (runner_heartbeat_interval_ms + 2_000)`. The worker stops when no heartbeat lands for half of the threshold, and that half must hold one interval plus the time the next heartbeat takes to land.
- With `auto_fail_stale_running=False`: `running_stale_threshold_ms >= 2 × runner_heartbeat_interval_ms`.
- `finalizing_stale_threshold_ms >= 2 × runner_heartbeat_interval_ms`.
- `claimed_stale_threshold_ms >= 2 × claimer_heartbeat_interval_ms`.

A config that breaks a rule is refused with code `HRS-204`, which names the fields and the smallest valid value.

For example, with `runner_heartbeat_interval_ms` at 30,000 and `auto_fail_stale_running` on:

- `running_stale_threshold_ms` at 64,000 is valid: 2 × (30 s + 2 s).
- `running_stale_threshold_ms` at 60,000 is refused: its half, 30 s, leaves no time for a heartbeat to land.
- `finalizing_stale_threshold_ms` at 60,000 is valid: 2 × 30 s.

The Python example is in [Threshold Rules](/horsies/configuration/app-config/#threshold-rules).

### For CPU-Heavy Tasks

A task that holds the GIL does not delay its runner heartbeats, because the worker process writes them. CPU-bound tasks need no extra margin in the thresholds.

### For Quick Tasks

Fast tasks can use tighter detection: for example, `runner_heartbeat_interval_ms` at 10,000 (10 seconds) and `running_stale_threshold_ms` at 40,000 (40 seconds). The worker stops when no heartbeat lands for 20 seconds, so a heartbeat may take up to 10 seconds to land. The Python example is in [Quick Tasks](/horsies/configuration/app-config/#quick-tasks).

### Recovery Time After a Worker Stops

A child process crash is detected at once (see [Child Process Crash](#child-process-crash)). When the whole worker process or its machine stops, its children stop too, and its CLAIMED and RUNNING tasks are recovered by the reaper, which runs inside workers.

When the worker process ends and its machine runs on, the server releases its [liveness lock](#worker-liveness-lock) at once. A reaper finds the lock free at its next pass (`check_interval_ms`), and recovers the tasks at its first pass 75 seconds or more after that. With the default reaper pass every 30 s, the passes are 90 seconds apart, so recovery happens about 1.5 to 2 minutes after the worker stops.

When the server does not see the session end, for example when the machine of the worker lost power or its network, the lock stays held until the server's TCP keepalive or `tcp_user_timeout` ends the session. Set `tcp_keepalives_idle`, `tcp_keepalives_interval` and `tcp_keepalives_count`, or `tcp_user_timeout`, on the server to bound that time. Until then the heartbeat thresholds apply: the reaper treats a task as stale `running_stale_threshold_ms` after its last runner heartbeat and acts on it at its next pass. With the defaults (heartbeat every 30 s, threshold 300 s, reaper pass every 30 s), recovery happens 4.5 to 5.5 minutes after the worker stops.

A `WORKER_CRASHED` retry then waits its retry policy delay, counted from recovery. If no worker is running, recovery waits until a worker starts.

Until then, the `RUNNING` rows of the dead worker count against each queue's `max_concurrency`. A new worker has free children, but a queue at its limit waits for the reaper.

To recover faster, lower `runner_heartbeat_interval_ms` and `running_stale_threshold_ms` together. A lower threshold also makes a worker stop sooner when it cannot reach the database (after half of the threshold, when `auto_fail_stale_running` is on; see [When the Worker Cannot Write Heartbeats](#when-the-worker-cannot-write-heartbeats)), and raises the risk of [false positives](#false-positives-tasks-marked-stale-but-running).

## Database Schema

Heartbeats are stored in the `horsies_heartbeats` table:

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | int | Auto-increment ID |
| `task_id` | str | Task being tracked |
| `sender_id` | str | Worker id |
| `role` | str | 'claimer' or 'runner' |
| `sent_at` | datetime | Heartbeat timestamp |
| `hostname` | str | Machine hostname |
| `pid` | int | Process ID of the sender |

## Manual Recovery

Three calls do by hand what the reaper does, each with a threshold that the call takes:

- The stale read lists the RUNNING tasks whose latest runner heartbeat since their start, or their start when they have none, is older than the threshold. It changes nothing.
- The RUNNING pass recovers stale RUNNING tasks as [Stale RUNNING Recovery](#stale-running-recovery) states, and returns the number of tasks that it retried or failed.
- The CLAIMED pass returns stale CLAIMED tasks to PENDING as [Stale CLAIMED → PENDING](#stale-claimed--pending) states, and returns the number of tasks that it requeued.

The API is on the client pages: [Broker Methods](/horsies/monitoring/broker-methods/#methods) (Python: the three calls) and [Health Reads](/horsies/rust/monitoring/health-reads/) (Rust: the stale read).

## Disabling Recovery

See [Disabling Recovery](/horsies/configuration/recovery-config/#disabling-recovery).

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

Raise `running_stale_threshold_ms`, for example to 600,000 (10 minutes).

The [liveness lock](#worker-liveness-lock) does not recover the tasks of a paused worker: it keeps its lock. When the reaper retries such a task while the first attempt still runs, the [fencing token](/horsies/concepts/fencing-token/) lets your store refuse the late writes of the first attempt. With `auto_fail_stale_running=True`, it does not recover the tasks of a live worker either. A worker whose lock is not held for 60 seconds ends before a reaper can judge it gone. A worker that takes its lock again writes a new generation, so two losses of its lock do not count as one.

### Worker Exits With Code 79

The worker held a task, and its liveness lock was not held for 60 seconds. The log names the last cause: a connection that ended, or a new connection or lock statement that failed. Check the connectivity of the worker host to the session URL.

### Worker Exits With a Runner Heartbeat Error

The log names the cause: the heartbeat thread died, or no heartbeat landed for half of `running_stale_threshold_ms`, with the last database error. Check the database connectivity of the worker host. Under a slow database, raise `running_stale_threshold_ms`: the worker stops after half of it, and a heartbeat may take that half less one `runner_heartbeat_interval_ms` to land.

### Tasks Not Recovering

Check:

- `auto_requeue_stale_claimed` / `auto_fail_stale_running` enabled?
- Reaper loop running? (Check worker logs)
- Database connectivity?

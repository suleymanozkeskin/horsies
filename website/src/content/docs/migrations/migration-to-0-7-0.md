---
title: Migrating to 0.7.0
summary: What to change to upgrade from 0.6.x to 0.7.0, in upgrade order - psycopg 3.3.6, worker profiles in place of worker flags, the snapshot interval cap, new error code cases, result waits inside a task, workflow limits, a stop of every 0.6.x worker before schema v55, exit codes, and the changes in behaviour after the upgrade.
related: [../changelog, ../workers/worker-profiles, ../workers/execution-modes, ../cli, ../configuration/recovery-config, migration-to-0-6-0]
tags: [migration, 0.7.0, breaking-changes, worker, profiles, cli, schema, execution]
---

## Upgrade order

1. Install horsies 0.7.0 with psycopg 3.3.6 or later. See [Dependencies](#dependencies).
2. Change the config: [worker profiles](#worker-settings-move-into-profiles), [the running stale threshold](#running_stale_threshold_ms-holds-one-heartbeat-interval-plus-2-s) and [the snapshot interval](#worker_state_snapshot_interval_ms-is-at-most-60000-ms).
3. Change the code: [matches on error codes](#matches-on-error-codes), [result waits inside a task](#a-result-wait-inside-a-task-is-refused) and [workflow limits](#workflow-limits).
4. Run `horsies check <app>`, and `horsies check <app> --live` against the target database. See [New validation](#new-validation).
5. Stop every 0.6.x worker, then start the 0.7.0 processes. See [Stop 0.6.x workers first](#stop-06x-workers-first).
6. Update the start commands and the supervisor for the [exit codes](#exit-codes).
7. Read [After the upgrade](#after-the-upgrade) for the changes in behaviour.

## Dependencies

horsies 0.7.0 requires psycopg 3.3.6 or later. The `pg-binary` and `pg-c` extras require `psycopg-binary` or `psycopg-c` 3.3.6 or later. Before 3.3.6, psycopg waited with no limit for an interrupted query to end. The 5-second limit on the cancel request also needs libpq 17 or later. See [psycopg and libpq](/horsies/configuration/broker-config/#psycopg-and-libpq).

## Worker settings move into profiles

`horsies worker` no longer takes execution flags. The settings live in named worker profiles in `AppConfig.worker_profiles`, and a worker starts with `--profile <name>`. `--loglevel` and the app locator stay.

1. Add one profile for each set of flags that your deployment starts workers with.
2. Replace the flags on each `horsies worker` command with `--profile <name>`.
3. Run `horsies check <app>`, and `horsies check <app> --live` against the target database.

`--profile` is required. There is no default profile. A removed flag exits with code 2 and a message that names the field that replaces it:

```text
horsies worker: error: --processes was removed in 0.7.0: worker settings live in a worker profile. Set WorkerProfile(isolated=IsolatedChildren(count=N)) in AppConfig.worker_profiles, and start the worker with --profile <name>.
```

## Flag to profile field

| 0.6.x flag | Default | 0.7.0 profile field |
|------------|---------|---------------------|
| `--processes N` | `1` | `isolated=IsolatedChildren(count=N)` (required, no default) |
| `--max-tasks-per-child N` | `100` | `isolated=IsolatedChildren(count=..., recycle=IsolatedRecycle(after=TaskLimit(tasks=N)))` |
| `--max-tasks-per-child 0` | | `recycle=IsolatedRecycle(after=NoTaskLimit())` |
| `--max-memory-per-child-mb N` | off | `recycle=IsolatedRecycle(above=MemoryLimit(mb=N))` |
| no `--max-memory-per-child-mb` | | `recycle=IsolatedRecycle(above=NoMemoryLimit())` (the default) |
| `--max-claim-batch N` | `0` | `claim=ClaimLimits(per_queue_per_pass=TaskLimit(tasks=N))` |
| `--max-claim-batch 0` | | `claim=ClaimLimits(per_queue_per_pass=NoTaskLimit())` (the default) |
| `--max-claim-per-worker N` | `0` | `claim=ClaimLimits(claimed_per_worker=TaskLimit(tasks=N))` |
| `--max-claim-per-worker 0` | | `claim=ClaimLimits(claimed_per_worker=NoTaskLimit())` (the default) |
| `--drain-timeout SECONDS` | `300` | `drain_timeout=timedelta(seconds=SECONDS)` |
| `--child-preload library\|app\|none` | `library` | `preload=ChildPreload.LIBRARY`, `ChildPreload.APP` or `ChildPreload.NONE` |

The defaults of the profile fields are the defaults of the old flags, except the number of children: `IsolatedChildren.count` has no default.

## Example

A 0.6.x command:

```bash
horsies worker myapp.instance:app --processes=8 --max-tasks-per-child=500 \
    --max-memory-per-child-mb=1024 --drain-timeout=120 --child-preload=app
```

The 0.7.0 profile and command:

```python
from datetime import timedelta

from pydantic import SecretStr

from horsies import (
    AppConfig,
    ChildPreload,
    Horsies,
    IsolatedChildren,
    IsolatedRecycle,
    MemoryLimit,
    PostgresConfig,
    TaskLimit,
    WorkerProfile,
)

app = Horsies(AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr('postgresql+psycopg://user:pass@localhost:5432/mydb'),
    ),
    worker_profiles={
        'default': WorkerProfile(
            isolated=IsolatedChildren(
                count=8,
                recycle=IsolatedRecycle(
                    after=TaskLimit(tasks=500),
                    above=MemoryLimit(mb=1024),
                ),
            ),
            drain_timeout=timedelta(seconds=120),
            preload=ChildPreload.APP,
        ),
    },
))
```

```bash
horsies worker myapp.instance:app --profile default
```

## `running_stale_threshold_ms` holds one heartbeat interval plus 2 s

With `auto_fail_stale_running=True` (the default), a 0.7.0 worker stops when
no runner heartbeat lands for half of `RecoveryConfig.running_stale_threshold_ms`.
That half must hold one `runner_heartbeat_interval_ms` plus 2 s for the next
heartbeat to land. So the threshold is at least
`2 × (runner_heartbeat_interval_ms + 2,000 ms)`: 64,000 ms at the default
30,000 ms interval. 0.6.x accepted `2 × runner_heartbeat_interval_ms`. A
smaller value raises `ConfigurationError` with `HRS-204` when the config is
built, and names the smallest valid value. With
`auto_fail_stale_running=False`, the 0.6.x bound stays. See
[When the Worker Cannot Write Heartbeats](/horsies/workers/heartbeats-recovery/#when-the-worker-cannot-write-heartbeats).

## `worker_state_snapshot_interval_ms` is at most 60,000 ms

0.6.x accepted `RecoveryConfig.worker_state_snapshot_interval_ms` up to
300,000 ms. In 0.7.0 the cap is 60,000 ms, half of the 120 s window in which a
worker reads as live. A larger value raises `ConfigurationError` with
`HRS-204` when the config is built. Set the interval to 60,000 ms or less. See
[Recovery Config](/horsies/configuration/recovery-config/).

## Matches on error codes

An exhaustive `match` on these types needs a change:

| Type | Change |
|------|--------|
| `ErrorCode` | Remove `TASK_ASYNC_FUNCTION_NOT_SUPPORTED` (`HRS-106`). `@app.task()` accepts an `async def` function, and the number is retired |
| `OperationalErrorCode` | Add `SUBWORKFLOW_DEPTH_LIMIT_REACHED`, `TASK_BLOCKED_LOOP` and `NEIGHBOUR_BLOCKED_LOOP` |
| `RetrievalCode` | Add `WAIT_INSIDE_TASK` |
| `BrokerErrorCode` | Add `WAIT_INSIDE_TASK` |
| `BrokerOperationError` | Add `BrokerWaitInsideTask` |

The new code strings are reserved. An `exception_mapper` or a `default_unhandled_error_code` with one of these values fails `horsies check` with `HRS-212`. A 0.6.x process cannot decode an error stored with one of the new `OperationalErrorCode` values, so upgrade every process that reads task results.

New fields without a default: `WorkerStateSnapshot.slot_health`, `WorkerStateSnapshot.class_capacity` and the monitoring `LeafTaskInfo.execution`. Code that builds one of these types, for example a test fake, must pass them. See [Worker & Database Health](/horsies/monitoring/worker-health/).

## A result wait inside a task is refused

In a worker child, a wait for the result of another task or workflow returns an error at once, and waits for nothing:

| Call | Returns |
|------|---------|
| `TaskHandle.get()` and `get_async()` on a sent task, `app.get_result()` and `get_result_async()`, `WorkflowHandle.get()` and `get_async()` | `RetrievalCode.WAIT_INSIDE_TASK` |
| `app.get_raw_result()`, `broker.get_raw_result_record()` and their `*_async` forms | `Err(BrokerWaitInsideTask)` |

The refusal also covers the threads that a task starts and the `on_child_process_start` hooks. `send()`, a workflow start, `TaskHandle.info()` and `workflow_ctx.result_for()` still work. Compose the steps as a workflow. See [Waiting for a Result Inside a Task](/horsies/concepts/result-handling/#waiting-for-a-result-inside-a-task).

## Workflow limits

- A child workflow has at most depth 32. A `SubWorkflowNode` whose child would have depth 33 fails with `SUBWORKFLOW_DEPTH_LIMIT_REACHED`, and the `on_error` policy of its workflow applies.
- A `WorkflowSpec` holds at most 10,000 nodes. A larger spec raises `HRS-038` when it is built. Split the nodes into `SubWorkflowNode`s: each child workflow has its own limit.
- `horsies check` reports `HRS-036` for a nesting deeper than 32, and `HRS-037` for a definition that includes itself with the same kwargs, when no `SubWorkflowNode` on the path has `waits_for`.

See [Limits](/horsies/concepts/workflows/subworkflows/#66-limits).

## New validation

These were not checked before 0.7.0, because the values were command-line flags:

- `horsies check` validates every profile. A claim limit above the capacity of the worker (its seats plus `prefetch_buffer`; one seat per isolated child) is refused with `HRS-218`; before, it had no effect.
- `horsies check --live` reports `HRS-219` when one worker of a profile can open more Postgres connections than the server has usable slots.
- At boot the worker warns when the per-child memory limits add up to more than the memory that the worker can use: the container (cgroup) limit when it is below the host RAM, else the host RAM, and when one worker can open more connections than the server has usable slots.

Each bound of the old flags still applies: at least one child, a task limit of at least 1, a memory limit of at least 1 MB, and a drain timeout above 0. New upper bounds: 1024 isolated children, 2,147,483,647 tasks for a task limit, 16,777,216 MB for a memory limit, and 24 hours for the drain timeout.

See [Worker Profiles](/horsies/workers/worker-profiles/) for every field and for the validation table.

## Stop 0.6.x workers first

The first 0.7.0 process that starts migrates the schema from v52 to v55:

- v53 adds `horsies_worker_states.slots_down`.
- v54 adds `horsies_tasks.execution`, and builds `idx_horsies_tasks_claim_pending_class` concurrently.
- v55 replaces the claim function with one that takes a budget per execution class, and drops the earlier one. It adds `horsies_worker_states.class_capacity`, builds `idx_horsies_tasks_claim_expired_class` concurrently, and drops `idx_horsies_tasks_claim_pending` and `idx_horsies_tasks_claim_expired_ordered` concurrently.

A 0.6.x worker that still runs fails each claim pass and exits. Stop every 0.6.x worker first, then start the 0.7.0 workers.

A 0.6.x process refuses a schema newer than its own when it sets up its schema: `database schema v55 is newer than this build supports (v52)`. Upgrade the producers, the schedulers and the web UI too. Rows that a 0.6.x process writes read `ISOLATED`.

## Exit codes

| Code | Process | Cause | In 0.6.x |
|------|---------|-------|----------|
| 1 | `horsies worker` | `--profile` is missing, or names no profile | No such option |
| 2 | `horsies worker` | A removed worker flag | The flag was valid |
| 1 | `horsies worker` | The worker cannot write runner heartbeats, with `auto_fail_stale_running=True` | The reaper failed the running tasks |
| 77 | `horsies worker` | As above, and the worker has not stopped 120 seconds later | No such code |
| 76 | worker child | A forked child that does not reparent to the worker | Code 74, which also meant a stop with executor threads left |

See [Exit Codes](/horsies/cli/#exit-codes).

## After the upgrade

### Scheduled runs apply the options of their task

A scheduled run stores the options of its task: `timeout_ms`, `retry_policy`, `execution` and `cancel_grace_ms`. In 0.6.x a scheduled run had no timeout and no retries, and ran as `ISOLATED`. A scheduled task with `timeout_ms` now stops at its timeout, and a failure retries by its policy.

The enqueue digest of every slot changes. A slot that a 0.6.x scheduler enqueued and did not record gets `PAYLOAD_MISMATCH` when 0.7.0 fires it. This happens when the 0.6.x scheduler stopped before it saved the schedule state. The scheduler then counts the slot as fired: it logs one warning, records the run with the task id of the slot, and moves to the next slot. That run keeps its 0.6.x row: no timeout, no retries, `ISOLATED`. See [Task Options](/horsies/scheduling/scheduler-overview/#task-options).

### A keyed send that a 0.6.x process sent is a conflict

The stored task options have two more keys, `execution` and `cancel_grace_ms`, so the command of every send changes. A send with an `idempotency_key` that a 0.6.x process already sent returns `IDEMPOTENCY_KEY_CONFLICT`, not the task of the 0.6.x send. A send after the reservation of the key ends is accepted.

### Enqueue refuses task options that are not an object

`enqueue_async` refuses with `INVALID_TASK_OPTIONS`:

- task options that are not a JSON object;
- an `execution` that is not an `Execution` value;
- a `cancel_grace_ms` that does not fit the class: from 100 to 10,000 ms for `SHARED_ASYNC`, absent or null for the other classes.

Sends of an `@app.task` function write valid options. This affects only code that passes its own options text to the broker.

### A child that cannot start does not always stop the worker

A child that fails its start for a cause that can clear takes only its slot down: `EMFILE`, `ENFILE`, `ENOMEM` or `EAGAIN` in a start step, an event loop that cannot be created, or a spawn or a thread that the OS refuses. The worker runs on degraded, and starts a new child for the slot after a growing wait. In 0.6.x each such failure counted one of three attempts, and the third stopped the worker. A worker with every slot down still stops after three failed starts. Read `WorkerStateSnapshot.slot_health` to see the slots that are down. See [Child Start Failures](/horsies/workers/worker-architecture/#child-start-failures).

### The worker process writes the runner heartbeats

One thread of the worker writes the runner heartbeats of its RUNNING tasks, over one connection of its own. A worker opens one more Postgres connection than in 0.6.x. A task that holds the GIL in C code no longer stops its heartbeats. See [When the Worker Cannot Write Heartbeats](/horsies/workers/heartbeats-recovery/#when-the-worker-cannot-write-heartbeats).

### A timed-out task moves after its child has exited

The worker writes `TASK_TIMEOUT`, the retry and the next workflow step only after the child of the task has exited. So a retry never runs next to the attempt that it replaces. When the child does not exit within 30 seconds of the kill, the worker writes nothing and logs at critical level, and stale-task recovery takes the task.

## Execution modes are new

A profile can start shared async children and shared thread children:

- `shared_async=SharedAsyncChildren(count=N, seats=K)`: each child runs up to K `Execution.SHARED_ASYNC` tasks at once, as coroutines.
- `shared_thread=SharedThreadChildren(count=N, seats=K)`: each child runs up to K `Execution.SHARED_THREAD` tasks at once, on threads.

They are new in 0.7.0 and have no flag before them. A profile without them runs every task in isolated children, as 0.6.x did, also a task marked `SHARED_ASYNC` or `SHARED_THREAD`. See [Choosing an Execution Mode](/horsies/workers/execution-modes/).

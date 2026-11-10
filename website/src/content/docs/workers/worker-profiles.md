---
title: Worker Profiles
summary: Named worker settings in AppConfig - isolated and shared async children, recycling, claim limits, drain timeout and preload - and what horsies check and worker boot validate.
related: [worker-architecture, shared-async-tasks, concurrency, ../cli, ../configuration/app-config, ../migrations/migration-to-0-7-0]
tags: [workers, profiles, configuration, concurrency, recycling, validation]
---

# Worker Profiles

A worker profile is a named set of worker settings in `AppConfig.worker_profiles`. `horsies worker <app> --profile <name>` starts a worker with the settings of that profile. The settings are part of the app config, so `horsies check` validates every profile before a worker starts.

## Why and When

An app that runs a worker defines at least one profile. Define more than one when the workers of one app run with different settings, for example a small profile for a shared host and a large one for a batch machine. An app that only sends tasks, runs the scheduler, or serves `horsies web` needs no profile.

## Define a Profile

```python
from datetime import timedelta

from pydantic import SecretStr

from horsies import (
    AppConfig,
    ChildPreload,
    ClaimLimits,
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
        'default': WorkerProfile(isolated=IsolatedChildren(count=4)),
        'batch': WorkerProfile(
            isolated=IsolatedChildren(
                count=8,
                recycle=IsolatedRecycle(
                    after=TaskLimit(tasks=500),
                    above=MemoryLimit(mb=1024),
                ),
            ),
            claim=ClaimLimits(per_queue_per_pass=TaskLimit(tasks=4)),
            drain_timeout=timedelta(seconds=120),
            preload=ChildPreload.APP,
        ),
    },
))
```

```bash
horsies worker myapp.instance:app --profile default
horsies worker myapp.instance:app --profile batch
```

`--profile` is required. There is no default profile. A missing or unknown name stops the command before the app check and before any child starts. The message lists the defined names:

```text
error[HRS-206]: --profile is required
   = note: a worker takes its settings from a named profile in AppConfig.worker_profiles; there is no default profile
   = note: defined profiles: batch, default

   = help:
        start the worker with: horsies worker <app> --profile batch
```

## Execution Modes

A profile sets a number of children for each execution mode. A child never changes its mode.

| Mode | Field | A child runs |
|------|-------|--------------|
| isolated | `isolated=IsolatedChildren(count=N)` | one task at a time, in its own process. See [Worker Architecture](worker-architecture) |
| shared async | `shared_async=SharedAsyncChildren(count=N, seats=K)` | up to K `SHARED_ASYNC` tasks at once, as coroutines on one event loop. See [Shared Async Tasks](shared-async-tasks) |

The number of children of a worker is the sum of the per-mode counts. There is no separate total. A profile with only shared async children is valid. A worker with no shared async children runs `SHARED_ASYNC` tasks in isolated children.

```python
from horsies import SharedAsyncChildren, SharedRecycle

io_profile = WorkerProfile(
    isolated=IsolatedChildren(count=2),
    shared_async=SharedAsyncChildren(
        count=4,
        seats=25,
        recycle=SharedRecycle(
            after_per_seat=TaskLimit(tasks=200),
            above=MemoryLimit(mb=2048),
        ),
    ),
)
```

## API Reference

### `WorkerProfile`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `isolated` | `IsolatedChildren \| NoIsolatedChildren` | `NoIsolatedChildren()` | The isolated children |
| `shared_async` | `SharedAsyncChildren \| NoSharedAsyncChildren` | `NoSharedAsyncChildren()` | The shared async children. A profile needs at least one child of either mode |
| `claim` | `ClaimLimits` | `ClaimLimits()` | Limits on the tasks that the worker claims |
| `drain_timeout` | `timedelta` | 300 seconds | How long a stopping worker waits for running tasks. Above 0, at most 24 hours. See [Graceful Shutdown](worker-architecture#graceful-shutdown) |
| `preload` | `ChildPreload` | `ChildPreload.LIBRARY` | What a Linux worker imports once before it forks children. See [Preload Parent](worker-architecture#preload-parent) |

`WorkerProfile.child_count()` returns the number of children of the profile. `WorkerProfile.seat_count()` returns the tasks that they run at once: one per isolated child, and `seats` per shared async child.

### `IsolatedChildren`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `count` | `int` | required | Number of isolated children, 1 to 1024 |
| `recycle` | `IsolatedRecycle` | `IsolatedRecycle()` | When a child is replaced |

`NoIsolatedChildren()` states that the profile starts no isolated children.

### `IsolatedRecycle`

The worker replaces a child with a new one when either limit is reached. Each child has its own counters, so children are replaced at different times. See [Process Recycling](concurrency#process-recycling).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `after` | `TaskLimit \| NoTaskLimit` | `TaskLimit(tasks=100)` | Replace a child after it completes this many tasks |
| `above` | `MemoryLimit \| NoMemoryLimit` | `NoMemoryLimit()` | Replace a child when its own RSS reaches this many MB |

### `SharedAsyncChildren`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `count` | `int` | required | Number of shared async children, 1 to 1024 |
| `seats` | `int` | required | Tasks that one child runs at once, 1 to 1000. A seat that waits on the network holds a socket, and a process often has a limit of 1024 open files |
| `recycle` | `SharedRecycle` | `SharedRecycle()` | When a child is replaced |

`NoSharedAsyncChildren()` states that the profile starts no shared async children.

### `SharedRecycle`

A shared async child that reaches a limit closes: it takes no new task, its running tasks finish, then it stops and a new child starts. See [Recycling](shared-async-tasks#recycling).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `after_per_seat` | `TaskLimit \| NoTaskLimit` | `TaskLimit(tasks=100)` | Replace a child after it completes `after_per_seat.tasks × seats` tasks |
| `above` | `MemoryLimit \| NoMemoryLimit` | `NoMemoryLimit()` | Replace a child when its own RSS, read after each task, reaches this many MB |

### `ClaimLimits`

The capacity of a worker is its number of seats (`seat_count()`) plus `AppConfig.prefetch_buffer`. A worker never holds more tasks than its capacity, so validation refuses a limit above it. See [Claiming Controls](concurrency#claiming-controls).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `per_queue_per_pass` | `TaskLimit \| NoTaskLimit` | `NoTaskLimit()` | The most tasks that one claim pass takes from one queue. With no limit, a pass fills the free capacity |
| `claimed_per_worker` | `TaskLimit \| NoTaskLimit` | `NoTaskLimit()` | The most CLAIMED tasks (claimed, not yet running) that the worker holds. With no limit, the capacity is the limit |

### Limits

| Type | Field | Bounds | Meaning |
|------|-------|--------|---------|
| `TaskLimit` | `tasks: int` | 1 to 2,147,483,647 | A limit of `tasks` tasks |
| `NoTaskLimit` | none | none | No limit on the number of tasks |
| `MemoryLimit` | `mb: int` | 1 to 16,777,216 | A limit of `mb` MB of resident memory (RSS) |
| `NoMemoryLimit` | none | none | No limit on the resident memory of a child |

### `ChildPreload`

<!-- members: ChildPreload -->
| Member | Value | What a Linux worker imports before it forks children |
|--------|-------|-------------------------------------------------------|
| `LIBRARY` | `library` | Library code. Each child imports the app itself |
| `APP` | `app` | Library and app. Children share module state from the app's import |
| `NONE` | `none` | Nothing. Every child starts with `exec` |

Other platforms start every child with `exec`.

## Validation

Validation runs in two places. `horsies check` validates every profile with the facts that need no machine. A worker validates again at boot, and adds facts that only the machine and the server know.

| Check | `horsies check` | `horsies check --live` | Worker boot |
|-------|-----------------|------------------------|-------------|
| Bounds of each value (count, limits, drain timeout) | error `HRS-210` | error `HRS-210` | error: the app config does not build |
| Profile name: 1 to 64 letters, digits, `_`, `.`, `-`; does not start with `.` or `-` | error `HRS-218` | error `HRS-218` | error: the app config does not build |
| At least one child | error `HRS-218` | error `HRS-218` | error: the app config does not build |
| Claim limit above the capacity (seats plus `prefetch_buffer`) | error `HRS-218` | error `HRS-218` | error: the app config does not build |
| `--profile` names a defined profile | not checked | not checked | error `HRS-206`, exit 1 |
| Postgres connections of one worker against the server | not checked | error `HRS-219` | warning |
| Sum of the per-child memory limits against the usable memory (container limit or host RAM) | not checked | not checked | warning |
| Memory limit against the measured child baseline RSS | not checked | not checked | refusal, exit 1 |

The app config is built when the worker imports the app module, so a worker boot validates every profile again, the chosen one included. A worker does not start from a config that has an invalid profile.

A value out of bounds fails where it is built, before the profile is in the dict, so its error (`HRS-210`, from the import of the app module) names the type and the field, for example `IsolatedChildren` `count`. An `HRS-218` error names the profile and the field path:

```text
error[HRS-218]: worker_profiles['batch'].claim.claimed_per_worker: 10 tasks is above the capacity of the worker (4)
   = note: capacity = 4 children + prefetch_buffer 0
   = note: the worker never holds more tasks than its capacity, so this limit has no effect

   = help:
        set the limit to 4 or less, or use NoTaskLimit()
```

### Connections

One worker can open this many Postgres connections:

```text
worker_pool_size + worker_max_overflow   # parent pool
+ 2                                      # listener
+ 1                                      # runner heartbeat
+ children × worker_child_pool_max_size  # child pools
```

The worker process writes the runner heartbeats of its RUNNING tasks over one connection of its own. `children` counts both modes: an isolated child opens a sync pool, and a shared async child opens an async pool of the same size, whatever its seats. At the defaults, a profile of 8 children needs `3 + 2 + 2 + 1 + 8 × 2 = 24`.

The server has `max_connections` minus `superuser_reserved_connections` minus `reserved_connections` (PostgreSQL 16 and later) usable slots. `horsies check --live` reports `HRS-219` for each profile above that number. At boot the worker reads the same settings before it builds its children and logs a warning. With `pgbouncer_transaction_mode=True` the pooled connections do not map to server connections, and nothing is compared. See [Remote PostgreSQL](../configuration/remote-postgres#connection-budget).

The comparison is for one worker. Several workers, producers and other clients share the same slots.

### Memory

At boot the worker compares the sum of `count × MemoryLimit.mb` over the modes with the memory that it can use, and logs a warning when the sum is above it. The worker starts in every case: a deployment can move between machine sizes. A profile with a mode that has children and `NoMemoryLimit()` has no sum to compare.

The memory limit of each mode is also compared with the measured baseline RSS of a booted child of that mode, and a limit at or below it refuses the start.

The memory that the worker can use is the smaller of two values:

| Source | Read from | Named in the warning |
|--------|-----------|----------------------|
| Container limit | cgroup v2 `/sys/fs/cgroup/memory.max`; when that file does not exist, cgroup v1 `/sys/fs/cgroup/memory/memory.limit_in_bytes` | `container limit` |
| Host RAM | `psutil.virtual_memory().total` | `host RAM` |

- A cgroup value of `max`, or no cgroup file, means no container limit.
- A cgroup limit at or above the host RAM, such as the cgroup v1 value for no limit, leaves the host RAM.
- A cgroup file that exists but cannot be read or parsed gives a separate warning, and the host RAM is used.

```text
Worker profile 'default': the per-child memory limits add up to 2048 MB, above the 1024 MB container limit. Children can use more memory than the worker has before they are recycled.
```

## Things to Avoid

**Don't set a claim limit above the capacity.** It has no effect, and validation refuses it.

```python
# Wrong: 2 children and prefetch_buffer 0 hold at most 2 tasks
claim_too_high = WorkerProfile(
    isolated=IsolatedChildren(count=2),
    claim=ClaimLimits(claimed_per_worker=TaskLimit(tasks=10)),
)

# Correct
claim_at_capacity = WorkerProfile(
    isolated=IsolatedChildren(count=2),
    claim=ClaimLimits(claimed_per_worker=TaskLimit(tasks=2)),
)
```

**Don't size the memory limits for one machine and ignore the boot warning on another.** The warning names the sum, the usable memory of the worker that started, and its source.

## Migrating from Worker Flags

The worker flags `--processes`, `--max-claim-batch`, `--max-claim-per-worker`, `--max-tasks-per-child`, `--max-memory-per-child-mb`, `--drain-timeout` and `--child-preload` are removed. Each one exits with a message that names the profile field that replaces it. See [Migrating to 0.7.0](../migrations/migration-to-0-7-0) for the table.

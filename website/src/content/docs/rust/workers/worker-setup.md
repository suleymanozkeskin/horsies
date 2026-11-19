---
title: Worker Setup
summary: Define the worker profiles of a Rust app with WorkerProfile and its bounded types, add child start hooks, and run a worker with run_worker.
related: [../cli, ../configuration/app-config, ../../workers/execution-modes, ../../workers/concurrency, ../../workers/worker-architecture]
tags: [rust, worker, WorkerProfile, on_child_start, run_worker, WorkerRunError]
---

# Worker Setup

A worker runs one named profile of `AppConfig.worker_profiles`. The profile holds every setting of the worker: the children of each execution class, their recycling, the pools, the claim limits and the drain timeout. The worker starts the app's own binary again in child mode for each child, and the task bodies run there. The process model is on [Worker Architecture](/horsies/workers/worker-architecture/).

## Define a Profile

```rust
use std::collections::BTreeMap;
use std::time::Duration;

use horsies::{
    AppConfig, DrainTimeout, IsolatedChildCount, IsolatedChildren, IsolatedRecycle,
    IsolatedSetting, MemoryLimitSetting, MemoryMb, SharedAsyncChildCount, SharedAsyncChildren,
    SharedAsyncSeats, SharedAsyncSetting, TaskCount, TaskLimitSetting, WorkerProfile,
};

fn with_profiles(config: AppConfig) -> Result<AppConfig, Box<dyn std::error::Error>> {
    let isolated = IsolatedChildren {
        recycle: IsolatedRecycle {
            after: TaskLimitSetting::TaskLimit { tasks: TaskCount::try_from(500)? },
            above: MemoryLimitSetting::MemoryLimit { mb: MemoryMb::try_from(1024)? },
        },
        ..IsolatedChildren::new(IsolatedChildCount::try_from(2)?)
    };
    let default_profile = WorkerProfile {
        isolated: IsolatedSetting::IsolatedChildren(isolated),
        shared_async: SharedAsyncSetting::SharedAsyncChildren(SharedAsyncChildren::new(
            SharedAsyncChildCount::try_from(4)?,
            SharedAsyncSeats::try_from(25)?,
        )),
        drain_timeout: DrainTimeout::try_from(Duration::from_secs(120))?,
        ..WorkerProfile::default()
    };
    Ok(AppConfig {
        worker_profiles: BTreeMap::from([("default".to_owned(), default_profile)]),
        ..config
    })
}
```

```bash
./myapp worker --profile default
```

- `WorkerProfile::default()` has no children. `Horsies::new` refuses a profile with no child.
- Do not set `preload`. Its default, `ChildPreload::NoPreload`, is the only value that a Rust worker takes: a Rust worker starts each child with `exec` of its own binary. `Horsies::new` refuses `ChildPreload::Library` and `ChildPreload::App` with `HRS-218`. See [App Config](/horsies/rust/configuration/app-config/#rules-that-only-a-rust-app-has).
- A task runs on the children of its execution class. The routing of each class, and the classes to choose by the work of a task, are on [Choosing an Execution Mode](/horsies/workers/execution-modes/).

## WorkerProfile

| Field | Type | Default |
|-------|------|---------|
| `isolated` | `IsolatedSetting`: `IsolatedChildren(IsolatedChildren)` or `NoIsolatedChildren {}` | `NoIsolatedChildren {}` |
| `shared_async` | `SharedAsyncSetting`: `SharedAsyncChildren(SharedAsyncChildren)` or `NoSharedAsyncChildren {}` | `NoSharedAsyncChildren {}` |
| `shared_thread` | `SharedThreadSetting`: `SharedThreadChildren(SharedThreadChildren)` or `NoSharedThreadChildren {}` | `NoSharedThreadChildren {}` |
| `pool` | `ParentPool`: the connection pool of the worker process | Its size follows the seats |
| `claim` | `ClaimLimits` | No limits |
| `drain_timeout` | `DrainTimeout`: how long a stopping worker waits for running tasks | 300 s |
| `preload` | `ChildPreload`: `Library`, `App` or `NoPreload`. A Rust app takes only `NoPreload` | `NoPreload` |
| `parent_cpu` | `ParentCpu`: `Shared` or `OwnCore` | `Shared` |

`child_count()` and `seat_count()` return the children and the seats of the profile.

### Children

| Type | Fields | Constructor |
|------|--------|-------------|
| `IsolatedChildren` | `count`, `recycle: IsolatedRecycle`, `pool: IsolatedPool`, `task_send_pool: PoolSize` | `IsolatedChildren::new(count)` |
| `SharedAsyncChildren` | `count`, `seats`, `recycle: SharedRecycle`, `pool: SharedPool`, `executor: ExecutorThreadsSetting`, `task_send_pool: PoolSize` | `SharedAsyncChildren::new(count, seats)` |
| `SharedThreadChildren` | `count`, `seats`, `recycle: SharedRecycle`, `pool: SharedPool`, `task_send_pool: PoolSize` | `SharedThreadChildren::new(count, seats)` |

The constructors take the default recycle, pools and executor. An isolated child runs one task at a time. A shared async child runs up to `seats` async tasks at once on one runtime. A shared thread child runs up to `seats` blocking tasks at once, each on a thread of its own. See [Concurrency](/horsies/workers/concurrency/#worker-level-children).

### Recycling

| Type | Fields | Default |
|------|--------|---------|
| `IsolatedRecycle` | `after: TaskLimitSetting`: replace a child after it completes this many tasks; `above: MemoryLimitSetting`: replace a child when its RSS reaches this limit | `after` 100 tasks, `above` no limit |
| `SharedRecycle` | `after_per_seat: TaskLimitSetting`: replace a child after `tasks × seats` tasks; `above: MemoryLimitSetting` | `after_per_seat` 100 tasks, `above` no limit |

- `TaskLimitSetting` is `TaskLimit { tasks: TaskCount }` or `NoTaskLimit {}`.
- `MemoryLimitSetting` is `MemoryLimit { mb: MemoryMb }` or `NoMemoryLimit {}`.
- A child is recycled when either limit is reached. The worker start fails when a memory limit is at or below the measured child baseline.

The rules are on [Concurrency](/horsies/workers/concurrency/#process-recycling).

### Pools

| Type | Fields | Default |
|------|--------|---------|
| `IsolatedPool` | `min_size: PoolMinSize`, `max_size: PoolSize`, `check: bool` | 0 open, at most 1 connection, no check |
| `SharedPool` | `min_size: PoolMinSize`, `max_size: PoolSizeSetting`, `check: bool` | 0 open, a size from the seats, no check |
| `ParentPool` | `max_size: PoolSizeSetting` | A size from the seats |
| `PoolSize` | `connections: PoolConnectionCount`. `PoolSize::new(connections)` | `task_send_pool`: 2 connections |

- `PoolSizeSetting` is `PoolSize { connections }` or `PoolSizeFromSeats {}`.
- `ExecutorThreadsSetting` is `ExecutorThreads { threads }` or `ExecutorThreadsFromSeats {}` (the default): the threads of the default executor of a shared async child.
- `check: true` checks each connection when a write takes it from the pool.
- `task_send_pool` is the pool that a send from task code opens in its child.

### Claim Limits and Drain

| Item | Does |
|------|------|
| `ClaimLimits.per_queue_per_pass` | `TaskLimitSetting`: the most tasks that one claim pass takes from one queue |
| `ClaimLimits.claimed_per_worker` | `TaskLimitSetting`: the most CLAIMED tasks that the worker holds |
| `DrainTimeout::try_from(duration)` | Above 0 and at most 24 hours; `DrainTimeoutRefused` (`Zero`, `AboveMaximum`) otherwise. At the deadline the worker kills the children that still run a task |
| `ParentCpu::OwnCore` | The worker process runs on one physical core and the children on the other CPUs. Linux only: the start fails on another platform or with fewer than 2 physical cores |

The claim limits are on [Concurrency](/horsies/workers/concurrency/#claiming-controls). Set the drain timeout below the stop window of the supervisor.

### Bounded Counts

Each count is a `BoundedU32<MIN, MAX>`. `try_from(u32)` returns `OutOfBounds` (`value`, `min`, `max`) for a value outside the bounds; `from_constant(value)` builds one in a `const` item, where a value outside fails the build. `get()` returns the number.

| Type | Bounds |
|------|--------|
| `IsolatedChildCount`, `SharedAsyncChildCount`, `SharedThreadChildCount` | 1 to 1,024 |
| `SharedAsyncSeats` | 1 to 1,000 |
| `SharedThreadSeats` | 1 to 256 |
| `TaskCount` | 1 to 2,147,483,647 |
| `MemoryMb` | 1 to 16,777,216 |
| `PoolConnectionCount` | 1 to 1,000 |
| `PoolMinSize` | 0 to 1,000 |
| `ExecutorThreadCount` | 1 to 256 |

```rust
use horsies::{SharedAsyncChildCount, SharedAsyncSeats};

const ASYNC_CHILDREN: SharedAsyncChildCount = SharedAsyncChildCount::from_constant(2);
const ASYNC_SEATS: SharedAsyncSeats = SharedAsyncSeats::from_constant(20);
```

## Child Start Hooks

`app.on_child_start(hook)` runs an async hook once in every worker child, a replacement child included.

```rust
use horsies::Horsies;

async fn check_cache_reachable() -> Result<(), String> {
    Ok(())
}

fn add_hooks(app: &mut Horsies) {
    app.on_child_start(|| async { check_cache_reachable().await });
}
```

- The hook is `FnOnce() -> Future<Output = Result<(), E>>`, with `E: Display`.
- It runs on the child's runtime, after the child connected its pool and before it takes its first task.
- Hooks run in registration order. The first failure ends the rest.
- Each hook runs for at most `CHILD_START_HOOK_BOUND` (10 s).
- A hook that returns an error, panics or runs past its bound ends the child with exit code 173 (`ChildHostExit::ChildHookFailed`). The worker stops with `CHILD_HOOK_FAILED`. The logged cause names the hook by its position and its type name.
- Only worker children run the hooks: the worker process, the scheduler and a producer never call them.
- Work that a hook starts and leaves running belongs to no task.

To give task bodies a value that each child builds, use `provide` in `main`: every child runs `main` again. See [App Config](/horsies/rust/configuration/app-config/#registration-and-state).

## Run a Worker

`<binary> worker --profile <name>` runs a worker through `horsies::cli::run(app)`. The command, its options and its exit codes are on [CLI Reference](/horsies/rust/cli/#worker). A binary that owns its `main` calls `run_worker`:

| Item | Does |
|------|------|
| `WorkerStart::profile(name)` | A worker of the profile `name`, whose children log at `INFO` |
| `.with_log_level(WorkerLogLevel)` | The log level of the children: `Debug`, `Info`, `Warning`, `Error`, `Critical` |
| `app.run_worker(start).await` | `Result<WorkerStopped, WorkerRunError>`. Runs until `SIGTERM` or `SIGINT`, then drains |

`run_worker` runs these steps in order: the role of the app becomes `Worker`; the check without the schedules; the worker manifest of the app; the profile and the connection settings; then the worker process of the core, which connects, starts the children and claims. It returns `Ok(WorkerStopped)` only at a stop request.

| `WorkerRunError` | When |
|------------------|------|
| `CheckRefused(CheckReport)` | The check refused the app. Nothing started |
| `ManifestBound { path, min, max }` | A config value is outside the bound of its worker manifest field. Nothing started |
| `ChildCommandRefused { cause }` | The command that starts this binary in child mode cannot be built. Nothing started |
| `ProfileRefused { refusals }` | The profile set fails its checks, or the start names a profile that the app does not define. Nothing started |
| `ConnectionSettingsRefused { cause }` | The connection settings fail the broker config check. Nothing started |
| `RuntimeUnavailable(RuntimeStartFailed)` | The operating system refused the threads of the core's process runtime. Nothing started; a later run tries the start again. In a fork, `run_worker` panics: see [Process-State Refusals](/horsies/tasks/error-types/#process-state-refusals) |
| `BootFailed { cause, message }` | The start failed; the worker claimed nothing. `cause` is a `WorkerBootCause` |
| `Failed { cause, message }` | The worker was ready, then failed; its running tasks drained first. `cause` is a `WorkerFailCause` |

`WorkerBootCause::as_str()` and `WorkerFailCause::as_str()` return the stored text of the cause, for example `CHILD_HOOK_FAILED` and `RUNNER_HEARTBEAT_LOST`.

## Things to Avoid

**Do not start servers, threads or loops in `main` before `run`.** Each worker child runs `main` again and starts them again.

**Do not set `preload` to `Library` or `App`.** `Horsies::new` refuses `ChildPreload::Library` and `ChildPreload::App` with `HRS-218`. Leave `preload` at its default, `ChildPreload::NoPreload`.

**Do not set a memory limit at or below the child baseline.** The worker start fails. See [Memory Recycling](/horsies/workers/concurrency/#memory-recycling).

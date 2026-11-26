---
title: Worker Setup
summary: Define the worker profiles of a Rust app with WorkerProfile and plain numbers, add child start hooks, and run a worker with run_worker.
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
    AppConfig, IsolatedChildren, IsolatedRecycle, MemoryLimitSetting, SharedAsyncChildren,
    TaskLimitSetting, WorkerProfile,
};

fn with_profiles(config: AppConfig) -> AppConfig {
    let isolated = IsolatedChildren {
        recycle: IsolatedRecycle {
            after: TaskLimitSetting::TaskLimit { tasks: 500 },
            above: MemoryLimitSetting::MemoryLimit { mb: 1024 },
        },
        ..IsolatedChildren::new(2)
    };
    let default_profile = WorkerProfile {
        isolated: isolated.into(),
        shared_async: SharedAsyncChildren::new(4, 25).into(),
        drain_timeout: Duration::from_secs(120),
        ..WorkerProfile::default()
    };
    AppConfig {
        worker_profiles: BTreeMap::from([("default".to_owned(), default_profile)]),
        ..config
    }
}
```

```bash
./myapp worker --profile default
```

- Each count, size and task limit is a plain number. `Horsies::new` checks each value against its [bounds](#bounds). It refuses the config with every refusal in one error, `HRS-218`, and each refusal names the field path of its value.
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
| `drain_timeout` | `Duration`: how long a stopping worker waits for running tasks | 300 s |
| `preload` | `ChildPreload`: `Library`, `App` or `NoPreload`. A Rust app takes only `NoPreload` | `NoPreload` |
| `parent_cpu` | `ParentCpu`: `Shared` or `OwnCore` | `Shared` |

`From` builds each setting from its children: `IsolatedChildren::new(2).into()` is `IsolatedSetting::IsolatedChildren(IsolatedChildren::new(2))`.

`profile.checked(name)` returns the checked profile, with `child_count()` and `seat_count()`, or one `ProfileValueRefused` for each value outside its bounds. `Horsies::new` does this check for each profile.

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

- `TaskLimitSetting` is `TaskLimit { tasks }` or `NoTaskLimit {}`.
- `MemoryLimitSetting` is `MemoryLimit { mb }` or `NoMemoryLimit {}`.
- A child is recycled when either limit is reached. The worker start fails when a memory limit is at or below the measured child baseline.

The rules are on [Concurrency](/horsies/workers/concurrency/#process-recycling).

### Pools

| Type | Fields | Default |
|------|--------|---------|
| `IsolatedPool` | `min_size`, `max_size: PoolSize`, `check: bool` | 0 open, at most 1 connection, no check |
| `SharedPool` | `min_size`, `max_size: PoolSizeSetting`, `check: bool` | 0 open, a size from the seats, no check |
| `ParentPool` | `max_size: PoolSizeSetting` | A size from the seats |
| `PoolSize` | `connections`. `PoolSize::new(connections)` | `task_send_pool`: 2 connections |

- `PoolSizeSetting` is `PoolSize { connections }` or `PoolSizeFromSeats {}`.
- `ExecutorThreadsSetting` is `ExecutorThreads { threads }` or `ExecutorThreadsFromSeats {}` (the default): the threads of the default executor of a shared async child.
- `check: true` checks each connection when a write takes it from the pool.
- `task_send_pool` is the pool that a send from task code opens in its child.

### Claim Limits and Drain

| Item | Does |
|------|------|
| `ClaimLimits.per_queue_per_pass` | `TaskLimitSetting`: the most tasks that one claim pass takes from one queue |
| `ClaimLimits.claimed_per_worker` | `TaskLimitSetting`: the most CLAIMED tasks that the worker holds |
| `drain_timeout` | Above 0 and at most 24 hours. At the deadline the worker kills the children that still run a task |
| `ParentCpu::OwnCore` | The worker process runs on one physical core and the children on the other CPUs. Linux only: the start fails on another platform or with fewer than 2 physical cores |

The claim limits are on [Concurrency](/horsies/workers/concurrency/#claiming-controls). Set the drain timeout below the stop window of the supervisor.

### Bounds

`Horsies::new` refuses each value outside its bounds. The refusal names the field path and the bounds, for example `worker_profiles['default'].isolated.count: 0 is outside the bounds 1 to 1024`.

| Field | Bounds |
|-------|--------|
| `count` of `IsolatedChildren`, `SharedAsyncChildren` and `SharedThreadChildren` | 1 to 1,024 |
| `SharedAsyncChildren.seats` | 1 to 1,000 |
| `SharedThreadChildren.seats` | 1 to 256 |
| `TaskLimit { tasks }` | 1 to 2,147,483,647 |
| `MemoryLimit { mb }` | 1 to 16,777,216 |
| `connections` of `PoolSize` and of `PoolSizeSetting::PoolSize` | 1 to 1,000 |
| `min_size` of `IsolatedPool` and `SharedPool` | 0 to 1,000 |
| `ExecutorThreads { threads }` | 1 to 256 |
| `drain_timeout` | Above 0, at most 24 hours |

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
- Only task children run the hooks: the worker process, the handler child, the scheduler and a producer never call them. The handler child runs the hooks of `on_handler_child_start` ([Event Handlers](/horsies/rust/monitoring/event-handlers/#handler-child-start-hooks)).
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

# horsies (Rust) — Configuration

Reference for the app, its configuration, worker profiles, child start
hooks, schedules, the check and the command-line programs of the `horsies`
crate. The meaning, the default and the range of each config field are on
the shared docs pages; this file states the Rust API.

## The App

`Horsies` is the app of a process: its configuration, its task and workflow
registrations, its state, and the core handle that connects at the first
operation. Every process of a deployment builds the same app in `main`.

| Constructor | Schema mode | The handle |
|---|---|---|
| `Horsies::new(config)` | `SchemaMode::MigrateAndValidate` | Installs the schema at the first operation, or checks it |
| `Horsies::with_schema_mode(config, SchemaMode::ObserveOnly)` | `ObserveOnly` | Runs no schema DDL. For monitoring tools |
| `Horsies::with_schema_mode(config, SchemaMode::AssumeInitialized)` | `AssumeInitialized` | Runs no schema DDL and uses no listener |
| `Horsies::with_handle(config, handle)` | The mode of `handle` | Uses an `Arc<CoreHandle>` that another app shares |

- The constructors do no I/O. A config that does not validate is refused
  with every refusal in one `HorsiesError`.
- `HorsiesError` has `code` (`Option<ErrorCode>`, `HRS-xxx`), `message`,
  `notes` and `help_text`. The codes are on the shared docs page
  `tasks/errors` ("Config error codes").

## `AppConfig`

`AppConfig::for_database_url(url)` builds the config with every default.
Change fields with struct update syntax.

| Field | Type | Shared docs page |
|---|---|---|
| `queue_mode` | `QueueMode` (`Default`, `Custom`) | `concepts/queue-modes` |
| `custom_queues` | `Option<Vec<CustomQueueConfig>>` (`name`, `priority`, `max_concurrency`) | `concepts/queue-modes` |
| `broker` | `PostgresConfig` | `configuration/broker-config` |
| `cluster_wide_cap` | `Option<u32>` | `workers/concurrency` |
| `prefetch_buffer` | `u32` | `workers/concurrency` |
| `claim_lease_ms` | `Option<u32>` | `workers/concurrency` |
| `max_claim_renew_age_ms` | `u32` | `workers/heartbeats-recovery` |
| `payload` | `PayloadPolicy` (`warn_bytes`, `reject_bytes`: `Option<u64>`) | Rust page `rust/tasks/sending-tasks` |
| `recovery` | `RecoveryConfig` | `configuration/recovery-config` |
| `retention` | `RetentionConfig` | `configuration/retention-config` |
| `resilience` | `WorkerResilienceConfig` | `workers/worker-architecture` |
| `worker_profiles` | `BTreeMap<String, WorkerProfile>` | `workers/concurrency` |
| `schedule` | `Option<ScheduleConfig>` | `scheduling/scheduler-overview` |
| `resend_on_transient_err` | `bool` | `operations/atomic-enqueue-and-topology` |

```rust
use horsies::{AppConfig, CustomQueueConfig, Horsies, HorsiesError, QueueMode};

fn build_app(database_url: &str) -> Result<Horsies, HorsiesError> {
    let config = AppConfig {
        queue_mode: QueueMode::Custom,
        custom_queues: Some(vec![
            CustomQueueConfig { name: "critical".to_owned(), priority: 1, max_concurrency: None },
            CustomQueueConfig { name: "standard".to_owned(), priority: 50, max_concurrency: Some(10) },
        ]),
        ..AppConfig::for_database_url(database_url)
    };
    Horsies::new(config)
}
```

### `PostgresConfig`

`PostgresConfig::from_url(url)` builds the broker config with every
default. Three fields have Rust types where the Python client has numbers:

| Field | Type |
|---|---|
| `pool_recycle` | `PoolRecycle::Never` or `PoolRecycle::AfterSeconds { seconds: WholeSeconds }` |
| `tcp_keepalives` | `TcpKeepalivesConfig::Off` or `TcpKeepalivesConfig::On { idle_seconds, interval_seconds, probes }` |
| `tcp_user_timeout` | `TcpUserTimeoutConfig::SystemDefault` or `TcpUserTimeoutConfig::After { milliseconds }` |

The other fields have the names and meanings of the shared page
`configuration/broker-config` (for example `database_url`,
`session_database_url`, `pgbouncer_transaction_mode`, `pool_size`,
`max_overflow`).

### Rules that only a Rust app has

| Config | Refusal of `Horsies::new` |
|---|---|
| `broker.pool_size` outside 1 to 10,000, or `broker.max_overflow` above 10,000 | `HorsiesError` "invalid broker config: broker.<field>: ..." with no code |
| A worker profile whose `preload` is `ChildPreload::Library` or `ChildPreload::App` | `HRS-218` with the path `worker_profiles.<name>.preload`. A Rust worker starts each child with `exec` of its own binary. Do not set `preload`: the default is `ChildPreload::NoPreload` |

### Queues

| Method | Returns |
|---|---|
| `get_valid_queue_names()` | `Vec<String>`: `default` in `Default` mode, the configured queues in `Custom` mode |
| `validate_queue(name)` | `Result<(), HorsiesError>`: refuses with `HRS-103` and the valid names |
| `effective_priority(name, task_priority)` | `u32`: `task_priority` when given, else the queue's priority, else 100 |

- In `Default` mode a task that names a queue is refused, `default`
  included (`HRS-200`).
- In `Custom` mode a task that names no queue is refused (`HRS-200`), and
  an unknown queue is refused with `HRS-103`.

## Worker Profiles

A worker runs one named profile of `AppConfig.worker_profiles`. The profile
holds every worker setting: the children of each execution class, their
recycling, the pools, the claim limits and the drain timeout.

```rust
use std::collections::BTreeMap;

use horsies::{
    AppConfig, IsolatedChildCount, IsolatedChildren, IsolatedRecycle,
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
        ..WorkerProfile::default()
    };
    Ok(AppConfig {
        worker_profiles: BTreeMap::from([("default".to_owned(), default_profile)]),
        ..config
    })
}
```

| `WorkerProfile` field | Type |
|---|---|
| `isolated` | `IsolatedSetting::IsolatedChildren(IsolatedChildren)` or `NoIsolatedChildren {}` |
| `shared_async` | `SharedAsyncSetting::SharedAsyncChildren(SharedAsyncChildren)` or `NoSharedAsyncChildren {}` |
| `shared_thread` | `SharedThreadSetting::SharedThreadChildren(SharedThreadChildren)` or `NoSharedThreadChildren {}` |
| `pool` | `ParentPool` (`max_size: PoolSizeSetting`) |
| `claim` | `ClaimLimits` (`per_queue_per_pass`, `claimed_per_worker`: `TaskLimitSetting`) |
| `drain_timeout` | `DrainTimeout`: `DrainTimeout::try_from(Duration)`, above 0 and at most 24 hours, default 300 s |
| `preload` | `ChildPreload`, default `NoPreload`: the only value that a Rust app takes |
| `parent_cpu` | `ParentCpu::Shared` (default) or `OwnCore` (Linux) |

- `WorkerProfile::default()` has no children. A profile needs at least one
  child.
- The counts are bounded types: build them with `try_from(u32)`, which
  returns `OutOfBounds` for a value outside the bounds.
  `IsolatedChildCount`, `SharedAsyncChildCount` and `SharedThreadChildCount`
  are 1 to 1,024; `SharedAsyncSeats` 1 to 1,000; `SharedThreadSeats` 1 to
  256.
- `IsolatedChildren::new(count)`, `SharedAsyncChildren::new(count, seats)`
  and `SharedThreadChildren::new(count, seats)` take the default recycle
  and pools. `IsolatedRecycle` has `after` (default 100 tasks) and `above`
  (default no memory limit); `SharedRecycle` has `after_per_seat` and
  `above`.
- A child is recycled when either limit is reached. A memory limit at or
  below the measured child baseline fails the worker start.
- A `shared_async` task runs on a shared async child when the profile has
  one, else in an isolated child. The routing and the sizing are on the
  shared docs pages `workers/execution-modes` and `workers/concurrency`.

## Registration, State and Child Start Hooks

| Method | Does |
|---|---|
| `task_name::register(&mut app)` | Registers a task of `#[horsies::task]` (see `tasks.md`) |
| `register(name, task)`, `register_with_queue(name, task, queue)` | Registers a task built with `async_task_fn!` or `blocking_task_fn!` |
| `register_workflow_definition::<D>()`, `workflow_template::<D>()` | Registers a workflow definition (see `workflows.md`) |
| `provide(value)` | Gives a value of type `T` to task bodies, which read it with `TaskRuntime::state::<T>()` |
| `discover(registrars)` | Runs each `fn(&mut Horsies) -> Result<(), HorsiesError>` on the app in order; stops at the first refusal |
| `on_child_start(hook)` | Runs an async hook once in every worker child |
| `task_runtime()` | The `TaskRuntime` that task bodies take |

```rust
use horsies::{Horsies, HorsiesError};

async fn check_cache_reachable() -> Result<(), String> {
    Ok(())
}

fn register_hooks(app: &mut Horsies) -> Result<(), HorsiesError> {
    app.on_child_start(|| async { check_cache_reachable().await });
    Ok(())
}
```

`on_child_start`:

- The hook runs on the child's runtime, after the child connected its pool
  and before it takes its first task. A replacement child runs it too.
- Hooks run in registration order. The first failure ends the rest.
- Each hook runs for at most `CHILD_START_HOOK_BOUND` (10 s).
- A hook that returns an error, panics or runs past its bound ends the
  child with exit code 173. The worker stops with `CHILD_HOOK_FAILED`. The
  logged cause names the hook by its position and its type name.
- Only worker children run the hooks: the worker process, the scheduler
  and a producer never call them.
- Work that a hook starts and leaves running belongs to no task.

## Send Suppression and Role

| Item | Does |
|---|---|
| `suppress_sends(true)` / `suppress_sends(false)` | Suppresses the sends of this process, or stops it |
| `are_sends_suppressed()` | `true` after `suppress_sends(true)`, or when `HORSIES_SUPPRESS_SENDS` (`SUPPRESS_SENDS_ENV`) is `1`. The variable is read at each call |
| `role()`, `set_role(AppRole)` | `AppRole::Producer` (a new app), `Worker` or `Scheduler`. The CLI runner sets it |

A suppressed send returns `TaskSendError::SendSuppressed` and writes
nothing.

## Schedules

`AppConfig.schedule` is an `Option<ScheduleConfig>`.

```rust
use std::convert::Infallible;

use chrono::NaiveTime;
use horsies::{
    task, AppConfig, DailySchedule, IntervalSchedule, ScheduleConfig, SchedulePattern, TaskError,
    TaskSchedule,
};

#[task("build_report")]
async fn build_report(region: String) -> Result<String, TaskError<Infallible>> {
    Ok(format!("report of {region}"))
}

fn with_schedules(config: AppConfig) -> Result<AppConfig, Box<dyn std::error::Error>> {
    let three_am = NaiveTime::from_hms_opt(3, 0, 0).ok_or("03:00:00 is a time of day")?;
    let nightly_report = TaskSchedule::new(
        "nightly-eu-report",
        "build_report",
        SchedulePattern::Daily(DailySchedule { time: three_am }),
    )
    .input(&build_report::Input { region: "eu".to_owned() })?
    .timezone("Europe/Berlin");
    let cache_cleanup = TaskSchedule::new(
        "cache-cleanup",
        "clean_cache",
        SchedulePattern::Interval(IntervalSchedule { minutes: Some(15), ..IntervalSchedule::default() }),
    );
    Ok(AppConfig {
        schedule: Some(ScheduleConfig::new(vec![nightly_report, cache_cleanup])),
        ..config
    })
}
```

`clean_cache` is a task with no parameters: its schedule takes no `input`.

| Item | Fields and builders |
|---|---|
| `ScheduleConfig` | `enabled` (default `true`), `schedules`, `check_interval_seconds` (1 to 60). `ScheduleConfig::new(schedules)`, `.enabled(bool)`, `.check_interval_seconds(n)` |
| `TaskSchedule::new(name, task_name, pattern)` | `.input(&value)` (`Result<TaskSchedule, JsonTextRefused>`), `.queue(name)`, `.enabled(bool)`, `.timezone(name)` (default `UTC`), `.catch_up_missed(bool)`, `.max_catch_up_runs(n)` (default 100) |
| `SchedulePattern` | `Interval(IntervalSchedule { seconds, minutes, hours, days })`, `Hourly(HourlySchedule { minute, second })`, `Daily(DailySchedule { time })`, `Weekly(WeeklySchedule { days, time })`, `Monthly(MonthlySchedule { day, time })`, `Cron(CronSchedule)` |

- `input` is the task input in the stored form: one member per task
  parameter, by name. A schedule without input stores `{}`.
- `.input(&value)` takes the task's `Input`, such as
  `build_report::Input { region }`, and stores its JSON text
  (`Option<JsonText>`), written by the strict writer: an integer keeps
  every digit (`u128::MAX`). `JsonTextRefused` is a value that does not
  write as strict JSON (a non-finite float, deeper than 300 levels).
- A config text of any serde format (TOML, JSON) gives its `input` member
  as its value, written by the same strict writer. In a config file, an
  integer of `input` must fit `i64` or `u64`; set an input with a wider
  integer in code with `.input(&value)`.
- A time of day is a chrono `NaiveTime` in whole seconds, in the
  schedule's time zone. `Weekday` and `Month` are enums of the crate.
- The scheduler checks each enabled schedule before it connects: its task
  is registered, its queue is a queue of the app, its input reads back with
  the task's input type, and its size is within the payload policy.
- The pattern rules and the run semantics are on the shared docs page
  `scheduling/scheduler-overview`.

## The Check

| Method | Returns |
|---|---|
| `app.check()` | `Result<CheckPassed, CheckReport>`. No I/O |
| `app.check_live().await` | `Result<CheckPassed, CheckReport>`. Connects and reads; installs no schema |

The check stops at the first phase that refuses:

1. Workflows: each definition without params and each check case of a
   workflow builder, built and checked against the registered definitions
   and the queues, with its sub-workflows.
2. Task options of each task registered by hand.
3. Schedules (not for a worker): each enabled schedule's task, queue,
   input read-back and input size.

`check_live` adds:

4. Connectivity: `SELECT 1` on `database_url`, then on
   `session_database_url` when it differs, then, in PgBouncer transaction
   mode, a LISTEN probe on the session URL. A refusal is `HRS-203`.
5. Worker connections: each worker profile against the connection limit of
   the server (`HRS-219`).

`CheckPassed::task_count()` is the number of registered tasks.
`CheckPassed::declared_error_codes()` is a `BTreeMap` from task name to
the declared error codes (`UserErrorCode::CODES`) of each task that
declares at least one. The success text lists them under
`declared error codes:`, or prints `declared error codes: none`.
`CheckReport::errors()` holds the refusals; `phase()` is `check`,
`worker-startup` or `scheduler-startup`. Both implement `Display`.

## CLI

A Rust app has two command-line programs:

| Program | Commands |
|---|---|
| The app's own binary, through `horsies::cli::run(app)` | `worker`, `scheduler`, `check`, `metrics`, and child mode |
| The shipped `horsies` binary | `web`, `get-docs`, `skills install` |

### `horsies::cli::run(app)`

```rust
use std::process::ExitCode;

use horsies::{AppConfig, Horsies, HorsiesError};

fn build_app() -> Result<Horsies, HorsiesError> {
    let config = AppConfig::for_database_url("postgresql://app:secret@db.example.com:5432/horsies");
    Horsies::new(config)
}

fn main() -> ExitCode {
    match build_app() {
        Ok(app) => horsies::cli::run(app),
        Err(refused) => {
            eprintln!("{refused}");
            ExitCode::FAILURE
        }
    }
}
```

1. Child mode first. The worker starts this binary again for each child,
   with `--horsies-child <fd> <pid>`. `run` serves the child and returns
   its exit code.
2. Every line of `main` before `run` runs in each child too. Build the app
   there (config, registrations, state, start hooks), and start nothing
   else.
3. `run` blocks the calling thread until the command ends. Call it from a
   plain `fn main`. Inside a tokio runtime it prints why and returns exit
   code 1.
4. Any other process runs the command of its arguments.

| Command | Does |
|---|---|
| `<binary> worker --profile <name> [--loglevel LEVEL] [--metrics-listen HOST:PORT]` | Starts a worker of one profile. A missing or unknown `--profile` exits 1 and lists the profile names. The check without the schedules runs first. `--metrics-listen` serves `GET /metrics`, `GET /livez` and `GET /readyz` (no default; `WorkerStart::with_metrics_endpoint(ListenAddress)` in a binary that owns its `main`) |
| `<binary> scheduler [--loglevel LEVEL]` | Starts the scheduler of `AppConfig.schedule`. Exits 1 when there is no schedule section or `schedule.enabled` is `false` |
| `<binary> check [--live] [--loglevel LEVEL]` | Checks the app. `--loglevel` default `WARNING` |
| `<binary> metrics --listen HOST:PORT [--loglevel LEVEL]` | Serves the fleet metrics of the app's database at `GET /metrics` (Prometheus text format 0.0.4). `--listen` is required: `HOST:PORT` or `[IPV6]:PORT`. Reads at start, then at most once per 15 s; a scrape never reads. Exits 1 when the config does not build a handle or the address does not bind |

- `SIGTERM` or `SIGINT` stops the worker and the scheduler. The worker
  drains within the profile's `drain_timeout`.
- `--loglevel` takes `DEBUG`, `INFO` (default), `WARNING`, `ERROR` or
  `CRITICAL`. `RUST_LOG` with a valid filter replaces it. A subscriber
  that the binary installed before `run` is kept. `FORCE_COLOR` and
  `NO_COLOR` control color.
- The flags of the `0.1.0-alpha` commands (`-q`/`--queues`,
  `-c`/`--concurrency`, `--max-claim-batch`, `--max-claim-per-worker`,
  `--coalesce-notifies`, `--check-interval`, `--dry-run`) exit 2 with
  what replaces each.
- `horsies::cli::run_with_log_format(app, LogFormat::Json)` writes one JSON
  object per record (`timestamp`, `level`, `target`, the event's fields at
  the top level, `span`, `spans`) in the runner and in each child.
  `run(app)` is `run_with_log_format(app, LogFormat::Text)`.
- Each task body runs inside the `tracing` span `run` (target
  `horsies::attempt`, `horsies::ATTEMPT_SPAN_TARGET`) with the fields
  `horsies.task.id`, `horsies.task.name` and `horsies.task.attempt`; the
  filter of `run` passes it at every level. The core's records of an
  attempt carry `task_id`, `attempt` and `trace_id`.

| Exit code of `run` | Meaning |
|---|---|
| 0 | Stopped at `SIGTERM` or `SIGINT`, the check passed, or `--help` |
| 1 | Any other end: a refused check or profile, a failed start, a failed worker or scheduler, `run` inside a tokio runtime |
| 2 | Arguments that do not parse, or a removed flag |
| 77 | `worker`: the worker could not write runner heartbeats and had not stopped 120 s after it started to stop |

A worker child exits with a `ChildHostExit` code: 0 `Stopped`, 71
`ParentGone`, 72 `BadParentFrame`, 73 `BadArguments`, 75 `BootFailed`, 78
`ProtocolMismatch`, 173 `ChildHookFailed`.

### A binary that owns its `main`

```rust
use std::process::ExitCode;

use horsies::{AppConfig, ChildHostExit, ChildMode, Horsies, HorsiesError, WorkerStart};

fn build_app() -> Result<Horsies, HorsiesError> {
    let config = AppConfig::for_database_url("postgresql://app:secret@db.example.com:5432/horsies");
    Horsies::new(config)
}

fn main() -> ExitCode {
    let app = match build_app() {
        Ok(app) => app,
        Err(refused) => {
            eprintln!("{refused}");
            return ExitCode::FAILURE;
        }
    };
    match horsies::child_mode() {
        ChildMode::Child(args) => app.serve_child(args),
        ChildMode::BadArguments(error) => {
            eprintln!("child mode cannot start: {error}");
            ExitCode::from(u8::try_from(ChildHostExit::BadArguments.code()).unwrap_or(u8::MAX))
        }
        ChildMode::Worker => run_worker(app),
    }
}

fn run_worker(app: Horsies) -> ExitCode {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("the runtime did not start: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(app.run_worker(WorkerStart::profile("default"))) {
        Ok(_stopped) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
```

| Command of `run` | Method |
|---|---|
| child mode | `horsies::child_mode() -> ChildMode`, then `Horsies::serve_child(self, ChildArgs) -> ExitCode`. Call it outside any tokio runtime |
| `worker --profile <name>` | `Horsies::run_worker(self, WorkerStart) -> Result<WorkerStopped, WorkerRunError>` (async). `WorkerStart::profile(name).with_log_level(level)` |
| `scheduler` | `Horsies::run_scheduler(&self) -> Result<SchedulerStopped, SchedulerRunError>` (async), or `Horsies::scheduler(&self)` for a `PreparedScheduler` with `run()` and `stop_handle()` |
| `check` | `Horsies::check(&self)` |
| `check --live` | `Horsies::check_live(&self)` (async) |

- `serve_child` installs no `tracing` subscriber. The child keeps the one
  that `main` installed.
- `run_worker` and `run_scheduler` stop at `SIGTERM` or `SIGINT`.
- `WorkerRunError`: `CheckRefused`, `ManifestBound`, `ChildCommandRefused`,
  `ProfileRefused`, `ConnectionSettingsRefused`,
  `RuntimeUnavailable(RuntimeStartFailed)`, `BootFailed { cause }`,
  `Failed { cause }`.
- `SchedulerRunError`: `NotConfigured`, `Disabled`, `CheckRefused`,
  `ManifestRefused`, `StartRefused`, `StartFailed { retryable }`,
  `RuntimeUnavailable(RuntimeStartFailed)`.
- `RuntimeStartFailed`: the operating system refused the threads of the
  core's process runtime; a later run tries again. A fork is not one: the
  run panics (`SKILL.md`, Result Types).

### The shipped `horsies` binary

| Command | Does |
|---|---|
| `horsies web [CONFIG] [OPTIONS]` | Serves the monitoring web UI of one database (feature `web`; see `monitoring.md`) |
| `horsies get-docs [--output DIR] [--ref main]` | Fetches the Rust pages and the shared pages of this release (`v<crate version>`). A pre-release version needs `--ref main` |
| `horsies skills install --target DIR [--force]` | Writes this skill folder to `DIR/horsies/`. A copy that differs is replaced only with `--force` |

`horsies worker`, `horsies scheduler` and `horsies check` exit with code 2:
the tasks of an app are compiled into the app's own binary. Exit codes of
`horsies`: 0 success (or `web` stopped by a signal), 1 failure, 2 argument
errors.

## Things to Avoid

**Do not call `horsies::cli::run` inside `#[tokio::main]`.** It blocks its
thread and refuses inside a runtime with exit code 1.

**Do not start servers, threads or loops in `main` before `run`.** Each
worker child runs `main` again.

**Do not set `preload` to `Library` or `App`.** `Horsies::new` refuses
them with `HRS-218`. Leave `preload` at its default, `ChildPreload::NoPreload`.

## All Key Imports

```rust
use horsies::{
    // App
    Horsies, HorsiesError, ErrorCode, AppRole, SchemaMode, SUPPRESS_SENDS_ENV, CoreHandle,
    // Config
    AppConfig, PostgresConfig, QueueMode, CustomQueueConfig, PayloadPolicy, RecoveryConfig,
    RetentionConfig, WorkerResilienceConfig, PoolRecycle, TcpKeepalivesConfig,
    TcpUserTimeoutConfig,
    // Worker profiles
    WorkerProfile, ChildPreload, ParentCpu, IsolatedSetting, IsolatedChildren, IsolatedRecycle,
    SharedAsyncSetting, SharedAsyncChildren, SharedThreadSetting, SharedThreadChildren,
    SharedRecycle, ClaimLimits, ParentPool, DrainTimeout, TaskLimitSetting, MemoryLimitSetting,
    IsolatedChildCount, SharedAsyncChildCount, SharedAsyncSeats, SharedThreadChildCount,
    SharedThreadSeats, TaskCount, MemoryMb, OutOfBounds,
    // Schedules
    ScheduleConfig, TaskSchedule, SchedulePattern, IntervalSchedule, HourlySchedule,
    DailySchedule, WeeklySchedule, MonthlySchedule, CronSchedule, Weekday, Month,
    // Check and services
    CheckPassed, CheckReport, WorkerStart, WorkerStopped, WorkerRunError, SchedulerStopped,
    SchedulerRunError, PreparedScheduler, ChildMode, ChildArgs, ChildHostExit,
    CHILD_START_HOOK_BOUND,
};
```

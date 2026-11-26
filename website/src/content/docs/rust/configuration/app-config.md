---
title: App Config
summary: Build the app from an AppConfig, choose its schema mode, and register tasks, queues and state.
related: [../tasks/defining-tasks, ../../configuration/broker-config, ../../configuration/recovery-config, ../../configuration/retention-config]
tags: [rust, configuration, AppConfig, Horsies, SchemaMode]
---

# App Config

`Horsies` is the app of a Rust process: its configuration, its task registrations, its state, and the core handle that connects at the first operation. Every process of a deployment builds the same app in `main`.

## Build the App

```rust
use horsies::{AppConfig, Horsies, ValidationReport};

fn build_app() -> Result<Horsies, ValidationReport> {
    let config = AppConfig::for_database_url("postgresql://app:secret@db.example.com:5432/horsies");
    Horsies::new(config)
}
```

`Horsies::new` does no I/O. It validates the config, builds the core's producer manifest, and returns an app whose handle connects at the first operation (a send, a read). A config that does not validate is refused with a `ValidationReport` of the phase `config`: `refusals()` holds every refusal of the validation. `ValidationReport` converts into one `HorsiesError`, so `?` works in a function that returns `HorsiesError`.

## Schema Modes

| Constructor | Schema mode | The handle |
|-------------|-------------|------------|
| `Horsies::new(config)` | `SchemaMode::MigrateAndValidate` | Installs the schema at the first operation, or checks it |
| `Horsies::with_schema_mode(config, SchemaMode::ObserveOnly)` | `ObserveOnly` | Runs no schema DDL. For monitoring tools |
| `Horsies::with_schema_mode(config, SchemaMode::AssumeInitialized)` | `AssumeInitialized` | Runs no schema DDL and uses no listener |
| `Horsies::with_core_connection(config, connection)` | the mode of `connection` | Uses an `Arc<CoreConnection>` that another app or the caller shares. The connection settings are those of `connection` |

See [Broker Config](/horsies/configuration/broker-config/#schema-initialization) for the install.

## AppConfig

`AppConfig` is the core's one config schema, which the Python client reads too. `AppConfig::for_database_url(url)` builds it with every default. The meaning, the default and the range of each field are on the shared pages.

| Field | Type | Meaning |
|-------|------|---------|
| `queue_mode` | `QueueMode` (`Default`, `Custom`) | [Queue Modes](/horsies/concepts/queue-modes/) |
| `custom_queues` | `Option<Vec<CustomQueueConfig>>` (`name`, `priority`, `max_concurrency`) | [Queue Modes](/horsies/concepts/queue-modes/) |
| `broker` | `PostgresConfig` | [Broker Config](/horsies/configuration/broker-config/) |
| `cluster_wide_cap` | `ClusterCapSetting::ClusterCap { tasks }` or `ClusterCapSetting::NoClusterCap` (default) | [Concurrency](/horsies/workers/concurrency/) |
| `prefetch_buffer` | `u32` | [Concurrency](/horsies/workers/concurrency/) |
| `claim_lease_ms` | `Option<u32>` | [Concurrency](/horsies/workers/concurrency/) |
| `max_claim_renew_age_ms` | `u32` | [Heartbeats & Recovery](/horsies/workers/heartbeats-recovery/) |
| `payload` | `PayloadPolicy` | [Sending Tasks](/horsies/rust/tasks/sending-tasks/#payload-size) |
| `recovery` | `RecoveryConfig` | [Recovery Config](/horsies/configuration/recovery-config/) |
| `retention` | `RetentionConfig` | [Retention Config](/horsies/configuration/retention-config/) |
| `resilience` | `WorkerResilienceConfig` | [Worker Architecture](/horsies/workers/worker-architecture/) |
| `worker_profiles` | `BTreeMap<String, WorkerProfile>` | [Concurrency](/horsies/workers/concurrency/) |
| `schedule` | `Option<ScheduleConfig>` | [Scheduler Overview](/horsies/scheduling/scheduler-overview/) |
| `resend_on_transient_error` | `bool` | [Sending Tasks](/horsies/rust/tasks/sending-tasks/#resend-on-transient-errors) |

```rust
use horsies::{AppConfig, CustomQueueConfig, Horsies, QueueMode, ValidationReport};

fn build_app(database_url: &str) -> Result<Horsies, ValidationReport> {
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

### PostgresConfig

`PostgresConfig::from_url(url)` builds the broker config with every default. Four fields are enums, as the Python classes:

| Field | Type | Python classes |
|-------|------|----------------|
| `session_database` | `SessionDatabase::SameAsDatabaseUrl` (default) or `SessionDatabase::Separate { url }` | `SessionDatabaseSameAsDatabaseUrl`, `SessionDatabaseSeparate` |
| `pool_recycle` | `PoolRecycle::Never` or `PoolRecycle::AfterSeconds { seconds: WholeSeconds }` | `PoolRecycleNever`, `PoolRecycleAfterSeconds` |
| `tcp_keepalives` | `TcpKeepalivesConfig::Off` or `TcpKeepalivesConfig::On { idle_seconds, interval_seconds, probes }` | `TcpKeepalivesOff`, `TcpKeepalivesOn` |
| `tcp_user_timeout` | `TcpUserTimeoutConfig::SystemDefault` or `TcpUserTimeoutConfig::After { milliseconds }` | `TcpUserTimeoutSystemDefault`, `TcpUserTimeoutAfter` |

The keepalive and user timeout values are plain `u32` numbers. `Horsies::new` refuses a value outside its bound with `HRS-215` (`CONFIG_INVALID_KEEPALIVE`), and the message names the field, for example `tcp_keepalives.probes`. In a TOML file, a separate session database is `session_database = { kind = "SEPARATE", url = "postgresql://..." }`.

The other fields have the names and the meanings of the [Broker Config](/horsies/configuration/broker-config/) table.

#### Pool Size

`pool_size` (default 5) and `max_overflow` (default 10) size the general broker pool: `pool_size` persistent connections, and up to `max_overflow` more under load. The Python client has the same defaults.

- The general broker serves producers, web processes, schedulers and app-level APIs.
- It does not size the pools of a worker. The worker process opens the `pool` of its worker profile, and task code in a worker child opens the `task_send_pool` of its child kind. See [Worker Setup](/horsies/rust/workers/worker-setup/#pools).

| Deployment | pool_size | max_overflow |
|------------|-----------|--------------|
| Development | 2-5 | 5 |
| Small production | 5-10 | 10-20 |
| High traffic | 10-20 | 20-40 |

Raise both values for a producer that sends at a high rate:

```rust
use horsies::{AppConfig, Horsies, PostgresConfig, ValidationReport};

fn build_high_traffic_app(database_url: &str) -> Result<Horsies, ValidationReport> {
    let config = AppConfig {
        broker: PostgresConfig {
            pool_size: 20,
            max_overflow: 40,
            ..PostgresConfig::from_url(database_url)
        },
        ..AppConfig::for_database_url(database_url)
    };
    Horsies::new(config)
}
```

## Rules That Only a Rust App Has

`Horsies::new` refuses these configs:

| Config | Refusal |
|--------|---------|
| `broker.pool_size` outside 1 to 10,000, or `broker.max_overflow` above 10,000 | `HorsiesError` "invalid broker config: broker.<field>: ..." with no code |
| A worker profile whose `preload` is `ChildPreload::Library` or `ChildPreload::App` | `HRS-218` (`ConfigInvalidWorkerProfile`) with the path `worker_profiles.<name>.preload`. A Rust worker starts each child with `exec` of its own binary, so there is nothing to preload. A profile that does not set `preload` (`..WorkerProfile::default()`, or a config file without the key) has `ChildPreload::NoPreload` |

## Queues

| Method | Returns |
|--------|---------|
| `get_valid_queue_names()` | `Vec<String>`: `default` in `Default` mode, the configured queues in `Custom` mode |
| `validate_queue(queue)` | `Result<(), HorsiesError>`: refuses with `HRS-103` (`TaskInvalidQueue`) and the valid names |
| `effective_priority(queue_name, task_priority)` | `u32`: `task_priority` when given, else the queue's configured priority, else 100 |

A task registration follows the queue rules of the Python client:

- In `Default` mode, a task that names a queue is refused, `default` included (`HRS-200`).
- In `Custom` mode, a task that names no queue is refused (`HRS-200`), and an unknown queue is refused with `HRS-103`.

## Registration and State

| Method | Does |
|--------|------|
| `register(name, task)` | Registers a task built with `async_task_fn!` or `blocking_task_fn!`, on the queue that its options name. The `#[horsies::task]` attribute generates a `register` function that calls it. Refuses an empty or repeated name, a definition-time `good_until`, and a queue that the queue mode does not admit; after a refusal nothing is registered |
| `register_with_queue(name, task, queue)` | Registers on `queue` in place of the queue of the options |
| `provide(value)` | Provides a value of type `T` to task bodies, which read it with `TaskRuntime::state::<T>()`. Refuses a second value of one type (wrap it in a newtype) and a value provided after a body read the state |
| `discover(registrars)` | Runs each registrar function on the app in order and stops at the first refusal |
| `task_runtime()` | The `TaskRuntime` that task bodies take as their first parameter |
| `workflow_builder(name)` | A `WorkflowSpecBuilder` whose task nodes resolve against the tasks of this app. See [Workflow API](/horsies/rust/concepts/workflows/workflow-api/) |
| `start_workflow::<D>()`, `start_workflow_with::<D>(params)` | Starts the definition `D` through its registration on this app. `VALIDATION_FAILED` when this app does not register `D`. See [Workflow API](/horsies/rust/concepts/workflows/workflow-api/) |

Each process builds its own state: every worker child runs `main` again. See [Defining Tasks](/horsies/rust/tasks/defining-tasks/).

The registrations belong to the app. The generated sends (`name::send(&app, ...)`) and the workflow builders of an app find a task only in that app, so two apps in one process share no registration.

## Role and Send Suppression

| Item | Does |
|------|------|
| `role()`, `set_role(AppRole)` | The role of the process: `AppRole::Producer` (a new app), `Worker` or `Scheduler`. A worker skips the schedule checks |
| `suppress_sends(bool)` | Suppresses the sends of this process (`true`) or stops suppressing them (`false`) |
| `are_sends_suppressed()` | `true` when `suppress_sends(true)` was called, or when the environment variable `HORSIES_SUPPRESS_SENDS` (`SUPPRESS_SENDS_ENV`) is `1`. The variable is read at each call |

A suppressed send returns `TaskSendError::SendSuppressed` and writes nothing.

## Refusals

A refusal of the config, of a registration or of `provide` is a `HorsiesError`: an `ErrorCode` (`HRS-xxx`) and its `ErrorCategory` where the refusal has one, the message, notes and help text. A `ValidationReport` collects the refusals of one phase. Both clients print the same codes; the codes are in the [Errors Reference](/horsies/tasks/errors/#config-error-codes-horsieserror).

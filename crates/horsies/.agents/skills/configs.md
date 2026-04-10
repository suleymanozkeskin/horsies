---
name: horsies-rust-configs
description: Configuration and runtime guidance for horsies-rust, including AppConfig, PostgresConfig, queue modes, recovery and resilience tuning, scheduling, and validation checks. Use when setting up, tuning, or troubleshooting runtime configuration.
---

# horsies-rust — Configuration

Detailed reference for configuration types, validation rules, and runtime setup.

## `AppConfig`

Root configuration passed to the unified `horsies::Horsies::new(config)`.

```rust
use horsies::{AppConfig, Horsies};

let config = AppConfig::for_database_url("postgresql://user:pass@host/db");
let app = Horsies::new(config)?;
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `queue_mode` | `QueueMode` | `Default` | `Default` or `Custom` |
| `custom_queues` | `Option<Vec<CustomQueueConfig>>` | `None` | Required if `Custom` mode |
| `broker` | `PostgresConfig` | required | Database connection config |
| `cluster_wide_cap` | `Option<u32>` | `None` | Max RUNNING tasks across cluster |
| `prefetch_buffer` | `u32` | `0` | 0 = hard cap; >0 = soft cap with lease |
| `claim_lease_ms` | `Option<u32>` | `None` | Claim lease duration; None = default 60s |
| `max_claim_renew_age_ms` | `u64` | `180_000` | Max age of CLAIMED task for heartbeat renewal |
| `recovery` | `RecoveryConfig` | `RecoveryConfig::default()` | Stale task detection and retention |
| `resilience` | `WorkerResilienceConfig` | default | Worker retry behavior |
| `schedule` | `Option<ScheduleConfig>` | `None` | Recurring task schedules |
| `resend_on_transient_err` | `bool` | `false` | Auto-retry transient ENQUEUE_FAILED for sends and starts |

### Validation (at `Horsies::new()`)

- `Default` mode: `custom_queues` must be `None`.
- `Custom` mode: `custom_queues` must be non-empty with unique names.
- `cluster_wide_cap` must be > 0 when set.
- `prefetch_buffer > 0` requires explicit `claim_lease_ms > 0`.
- Effective lease must be >= 2x `recovery.claimer_heartbeat_interval_ms`.

## `PostgresConfig`

```rust
let config = PostgresConfig::from_url("postgresql://user:pass@localhost:5432/mydb");
```

| Field | Type | Default | Description |
|---|---|---|---|
| `database_url` | `String` | required | PostgreSQL connection URL |
| `pool_size` | `u32` | `30` | Connection pool size |
| `pool_timeout` | `u32` | `30` | Timeout in seconds for acquiring a pooled connection |

## `QueueMode`

```rust
pub enum QueueMode {
    Default,  // single "default" queue
    Custom,   // named queues via custom_queues
}
```

### Default mode

Single `"default"` queue. Tasks registered without explicit queue.

### Custom mode

Named queues configured via `custom_queues`. Tasks must specify their queue at registration:

```rust
app.register_with_queue("my_task", task, "critical")?;
// Or via the unified task builder:
app.task::<A, T>("my_task", task)?.queue("critical").register()?;
```

## `CustomQueueConfig`

```rust
let config = AppConfig {
    queue_mode: QueueMode::Custom,
    custom_queues: Some(vec![
        CustomQueueConfig { name: "critical".into(), priority: 1, max_concurrency: 10 },
        CustomQueueConfig { name: "background".into(), priority: 50, max_concurrency: 3 },
    ]),
    ..AppConfig::for_database_url("postgresql://localhost/mydb")
};
```

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `String` | required | Unique queue name |
| `priority` | `u32` | `1` | 1 = highest, 100 = lowest |
| `max_concurrency` | `u32` | `5` | Max simultaneous RUNNING tasks |

Lower priority number = claimed first.

## `RecoveryConfig`

Controls stale task detection and retention.

| Field | Type | Default | Description |
|---|---|---|---|
| `auto_requeue_stale_claimed` | `bool` | `true` | Requeue tasks stuck in CLAIMED |
| `claimed_stale_threshold_ms` | `u64` | `120_000` | Ms before CLAIMED is stale |
| `auto_fail_stale_running` | `bool` | `true` | Fail tasks stuck in RUNNING |
| `running_stale_threshold_ms` | `u64` | `300_000` | Ms before RUNNING is stale |
| `check_interval_ms` | `u64` | `30_000` | Reaper poll cadence |
| `runner_heartbeat_interval_ms` | `u64` | `30_000` | Heartbeat from running task |
| `claimer_heartbeat_interval_ms` | `u64` | `30_000` | Heartbeat for CLAIMED tasks |
| `heartbeat_retention_hours` | `Option<u32>` | `Some(24)` | Prune old heartbeat rows |
| `worker_state_retention_hours` | `Option<u32>` | `Some(168)` | Prune old worker_state rows |
| `terminal_record_retention_hours` | `Option<u32>` | `Some(720)` | Prune terminal task/workflow rows |

### Constraints

- `running_stale_threshold_ms >= runner_heartbeat_interval_ms * 2`
- `claimed_stale_threshold_ms >= claimer_heartbeat_interval_ms * 2`

The 2x factor ensures a task can miss one heartbeat cycle without being incorrectly marked stale.

## `WorkerResilienceConfig`

Controls worker retry behavior on transient DB failures.

| Field | Type | Default | Description |
|---|---|---|---|
| `db_retry_initial_ms` | `u64` | `500` | Initial backoff |
| `db_retry_max_ms` | `u64` | `30_000` | Max backoff cap |
| `db_retry_max_attempts` | `u32` | `0` | Max retries; 0 = infinite |
| `notify_poll_interval_ms` | `u64` | `5_000` | Fallback poll interval |

### Constraint

`db_retry_max_ms >= db_retry_initial_ms`.

## `resend_on_transient_err`

When `true` on `AppConfig`, enables automatic retry of transient `ENQUEUE_FAILED` errors for:
- `TaskFunction::send()` and `TaskFunction::schedule()`
- `WorkflowFunction::start()` and low-level `start_workflow_with_retry()`

Retry parameters (hardcoded, matches Python):
- 3 retries after initial attempt (4 total)
- Initial backoff: 200ms
- Max backoff: 2000ms
- Exponential backoff, no jitter

Same payload identity across all retry attempts (task_id, sent_at, SHA fixed once).

## Scheduling

### `ScheduleConfig`

```rust
let config = AppConfig {
    schedule: Some(ScheduleConfig::new(vec![task_schedule]).check_interval_seconds(1)),
    ..AppConfig::for_database_url("postgresql://localhost/mydb")
};
```

All `TaskSchedule.name` values must be unique.

### `TaskSchedule`

```rust
TaskSchedule::new(
    "sync_inventory",
    "sync_inventory",
    SchedulePattern::Interval(IntervalSchedule { seconds: Some(30), ..Default::default() }),
)
```

### Schedule Patterns

- `IntervalSchedule` — `{ seconds, minutes, hours, days }` (at least one set)
- `HourlySchedule` — `{ minute, second }`
- `DailySchedule` — `{ time: NaiveTime }`
- `WeeklySchedule` — `{ days: Vec<Weekday>, time: NaiveTime }`
- `MonthlySchedule` — `{ day, time: NaiveTime }`

## `Horsies::check()`

Phased validation. Fail-fast — each phase short-circuits on errors.

```rust
let errors = app.check();
if !errors.is_empty() {
    for err in &errors {
        eprintln!("ERROR: {}", err);
    }
}
```

### Phases

| Phase | What | Rust status |
|---|---|---|
| 1 | Config — validated at `Horsies::new()` | Done (implicit) |
| 2 | Schedule validation — task names, queue names | Done |
| 2.5 | Runtime policy safety — registered task `task_options`, `retry_policy`, `auto_retry_for`, reserved code collisions | Done |
| 2.6 | Custom-mode task queue metadata validation | Done |
| 2.8 | Registered workflow nodes reference registered tasks | Done |
| 2.9 | Workflow node queues are resolved and valid | Done |
| 2.10 | Workflow nodes that require input have args, kwargs, or `args_from` | Done |
| 3 | Workflow `definition_key` presence (HRS-016) | Done |
| 3.2 | Checked workflow builder representative cases | Done |
| 3.5 | Duplicate `definition_key` detection (HRS-017) | Done |

### Runtime policy safety (Phase 2.5)

Validates registered task metadata:
- `task_options` identity / queue consistency
- `retry_policy.validate()` (intervals, max_retries)
- `retry_policy` requiring non-empty `auto_retry_for`
- Reserved built-in collision detection for user-provided retry codes (HRS-212)

### Workflow wiring safety (Phases 2.8–2.10)

Validates registered workflow specs and checked builder output specs:
- referenced task names are registered
- node queues resolve from explicit node overrides or registered task defaults
- resolved queues are valid for the configured queue mode
- non-unit tasks have an input source: `args_json`, `kwargs_json`, or `args_from`

The same queue/input validation also runs at workflow start through the shared
workflow registry resolver, so invalid dynamic specs fail before worker
deserialization.

### `Horsies::check_live()`

Extends `Horsies::check()` with a live broker connectivity check:

```rust
app.check()?;
app.check_live().await?;
```

Uses `PostgresBroker::health_check()` through the lazy broker owned by the
top-level app.

### Error codes

| Code | Description | Phase |
|---|---|---|
| `HRS-001–HRS-020`, `HRS-025`, `HRS-027–HRS-030`, `HRS-032` | Workflow validation errors | 2.8–3.5 |
| `HRS-014` | Workflow queue unresolved or invalid | 2.9, builder validation, start-time validation |
| `HRS-016` | Missing `definition_key` | 3, 3.2 |
| `HRS-017` | Duplicate `definition_key` | 3.5 |
| `HRS-020` | Workflow node missing required input | 2.10, builder validation, start-time validation |
| `HRS-027` | Parameterized builder without cases | 3.2 |
| `HRS-029` | Builder exception / wrong return type | 3.2 |
| `HRS-100–HRS-103` | Task definition errors | 2.5 |
| `HRS-102` | Invalid task_options / retry policy | 2.5 |
| `HRS-200–HRS-211` | Configuration errors | 1 |
| `HRS-203` | Broker connectivity failure (live only) | 4 |
| `HRS-212` | Reserved code collision in `auto_retry_for` | 2.5 |
| `HRS-301` | Registry duplicate name | Registration |
| `HRS-302` | Workflow references unregistered task | 2.8, builder validation |

## `PostgresBroker`

Database connection and task operations. Normally accessed through the
unified app (`app.get_broker().await`), not constructed directly.

```rust
// Via unified app (preferred)
let broker = app.get_broker().await?;

// Direct construction (advanced/internal)
use horsies::Broker;
let broker = Broker::connect("postgresql://...").await?;
```

### Key methods

| Method | Description |
|---|---|
| `send_task(&resolved, args, kwargs, opts)` | Enqueue a task (low-level) |
| `schedule_task(&resolved, args, kwargs, delay, opts)` | Enqueue with delay |
| `get_result(task_id, timeout)` | Wait for task result |
| `get_task_info(task_id, ...)` | Fetch task metadata |
| `retry_send(&payload, task_id)` | Replay a failed send |
| `claim(queue, batch_size, worker_id, lease)` | Claim tasks for processing |
| `pool()` | Access underlying PgPool |
| `health_check()` | Verify DB connectivity |

## Worker

```rust
// Via unified app (preferred)
app.run_worker().await?;
// Or with custom config:
app.run_worker_with(worker_config).await?;
```

Advanced: direct `horsies::Worker` construction is available for custom runtime integration but is not the primary path.

## All Key Config Imports

```rust
use horsies::{
    Horsies, AppConfig, PostgresConfig, QueueMode, CustomQueueConfig,
    RecoveryConfig, WorkerResilienceConfig,
    ScheduleConfig, TaskSchedule, SchedulePattern,
    IntervalSchedule, HourlySchedule, DailySchedule,
    WeeklySchedule, MonthlySchedule, Weekday,
    ResolvedEnqueue, ErrorCode, HorsiesError, ValidationReport,
};
```

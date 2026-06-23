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
| `max_claim_renew_age_ms` | `u32` | `180_000` | Max age of CLAIMED task for heartbeat renewal |
| `recovery` | `RecoveryConfig` | `RecoveryConfig::default()` | Stale task detection and retention |
| `resilience` | `WorkerResilienceConfig` | default | Worker retry behavior |
| `schedule` | `Option<ScheduleConfig>` | `None` | Recurring task schedules |
| `resend_on_transient_err` | `bool` | `false` | Auto-retry transient ENQUEUE_FAILED for sends and starts |

### Validation (at `Horsies::new()`)

- `Default` mode: `custom_queues` must be `None`.
- `Custom` mode: `custom_queues` must be non-empty with unique names.
- `cluster_wide_cap` must be > 0 when set.
- `cluster_wide_cap` is incompatible with `prefetch_buffer > 0` (cluster cap requires hard-cap mode).
- `prefetch_buffer > 0` requires explicit `claim_lease_ms > 0`.
- Effective lease must be >= 2x `recovery.claimer_heartbeat_interval_ms`.
- `max_claim_renew_age_ms` must be >= the effective claim lease.

## `PostgresConfig`

```rust
let config = PostgresConfig::from_url("postgresql://user:pass@localhost:5432/mydb");
```

| Field | Type | Default | Description |
|---|---|---|---|
| `database_url` | `String` | required | Runtime PostgreSQL connection URL. May point at a PgBouncer transaction-pool endpoint when `pgbouncer_transaction_mode = true`. |
| `session_database_url` | `Option<String>` | `None` | Direct or session-pooled URL used for schema initialization and `LISTEN/NOTIFY`. Required when `pgbouncer_transaction_mode = true`. |
| `pgbouncer_transaction_mode` | `bool` | `false` | Configure the runtime pool for PgBouncer transaction pooling. Disables SQLx's local prepared-statement cache; the pooler must have protocol-level prepared-statement tracking enabled (`max_prepared_statements > 0`). |
| `pool_pre_ping` | `bool` | `true` | Pre-ping each connection before use. |
| `pool_size` | `u32` | `30` | Runtime connection pool size. |
| `max_overflow` | `u32` | `30` | Additional runtime connections beyond `pool_size`. |
| `pool_timeout` | `u32` | `30` | Seconds to wait for acquiring a pooled connection. |
| `pool_recycle` | `u32` | `1800` | Seconds before a connection is recycled. |
| `echo` | `bool` | `false` | Log SQL statements. |

### PgBouncer transaction pooling

PgBouncer transaction pooling can carry normal task and workflow SQL, but it
cannot preserve session state for `LISTEN/NOTIFY`. When deploying behind a
transaction-pool endpoint, configure two URLs:

```rust
let broker = PostgresConfig::from_pgbouncer_urls(
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
    std::env::var("SESSION_DATABASE_URL").expect("SESSION_DATABASE_URL must be set"),
);
```

`from_pgbouncer_urls(runtime_url, session_url)` sets `database_url`,
`session_database_url`, and `pgbouncer_transaction_mode = true` in one call.

- The runtime pool uses `database_url` (the transaction-pool endpoint) and
  disables SQLx's local prepared-statement cache. The pooler must have
  `max_prepared_statements > 0`; if prepared-statement tracking is off, the
  broker returns a clear `PgBouncer transaction mode requires protocol prepared-statement tracking`
  error during connect.
- Schema initialization, workers, result listeners, and workflow listeners use
  `session_database_url`. When the session URL differs from `database_url`,
  the session-capable pool is capped at 4 connections instead of inheriting
  `pool_size`.
- Workers run a bounded `LISTEN` + `NOTIFY` delivery probe once at startup in
  PgBouncer transaction mode. If the session URL is accidentally
  transaction-pooled, worker startup fails with a message indicating the URL
  cannot preserve `LISTEN/NOTIFY` state.
- `effective_session_database_url()` returns `session_database_url` when set,
  otherwise falls back to `database_url`.

Validation at `PostgresConfig::validate()` (called transitively through
`Horsies::new()`):

- `pgbouncer_transaction_mode = true` without `session_database_url` →
  `PostgresConfigError::MissingSessionDatabaseUrl`.
- Either URL with a non-`postgresql://` / `postgres://` scheme →
  `InvalidUrlScheme` / `InvalidSessionUrlScheme`.

Different managed providers expose direct and pooled Postgres endpoints
differently (separate ports vs separate hostnames). Check your provider's
current connection documentation when deriving the two URLs.

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
        CustomQueueConfig { name: "critical".into(), priority: 1, max_concurrency: Some(10) },
        CustomQueueConfig { name: "background".into(), priority: 50, max_concurrency: Some(3) },
        // Uncapped: this queue is omitted from the per-queue concurrency cap.
        CustomQueueConfig { name: "bulk".into(), priority: 90, max_concurrency: None },
    ]),
    ..AppConfig::for_database_url("postgresql://localhost/mydb")
};
```

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `String` | required | Unique queue name |
| `priority` | `u32` | `1` | 1 = highest, 100 = lowest |
| `max_concurrency` | `Option<u32>` | `Some(5)` | Max simultaneous RUNNING tasks. `None` = explicit uncapped sentinel (queue excluded from the per-queue cap map); `Some(0)` is valid and pauses claiming for the queue |

Lower priority number = claimed first.

## `RecoveryConfig`

Controls stale task detection and retention.

| Field | Type | Default | Description |
|---|---|---|---|
| `auto_requeue_stale_claimed` | `bool` | `true` | Requeue tasks stuck in CLAIMED |
| `claimed_stale_threshold_ms` | `u64` | `120_000` | Ms before CLAIMED is stale |
| `auto_fail_stale_running` | `bool` | `true` | Fail tasks stuck in RUNNING |
| `running_stale_threshold_ms` | `u64` | `300_000` | Ms before RUNNING is stale |
| `finalizing_stale_threshold_ms` | `u64` | `300_000` | Ms before a task whose two-phase finalize stalled is recovered |
| `crashed_worker_recovery_grace_ms` | `u64` | `10_000` | Grace before the reaper recovers a just-terminal workflow task whose Phase 2 may still be in flight; `0` disables, max `3_600_000` |
| `check_interval_ms` | `u64` | `30_000` | Reaper poll cadence |
| `runner_heartbeat_interval_ms` | `u64` | `30_000` | Heartbeat from running task |
| `claimer_heartbeat_interval_ms` | `u64` | `30_000` | Heartbeat for CLAIMED tasks |
| `heartbeat_retention_hours` | `Option<u32>` | `Some(24)` | Prune old heartbeat rows |
| `worker_state_retention_hours` | `Option<u32>` | `Some(168)` | Prune old worker_state rows |
| `terminal_record_retention_hours` | `Option<u32>` | `Some(720)` | Prune terminal task/workflow rows |

### Constraints

- `running_stale_threshold_ms >= runner_heartbeat_interval_ms * 2`
- `claimed_stale_threshold_ms >= claimer_heartbeat_interval_ms * 2`
- `finalizing_stale_threshold_ms >= runner_heartbeat_interval_ms * 2`
- All `*_ms` fields must be `>= 1000`.
- `crashed_worker_recovery_grace_ms <= 3_600_000` (`0` disables it; no minimum).

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
.kwargs(serde_json::json!({ "region": "eu" }))
.queue("background")
```

`TaskSchedule` fields (builder methods in parentheses): `name`, `task_name`,
`pattern`, `args` (`.args()`), `kwargs` (`.kwargs()`), `queue_name` (`.queue()`),
`enabled` (`.enabled()`, default `true`), `timezone` (default `"UTC"`),
`catch_up_missed`, `max_catch_up_runs`. Both `args` and `kwargs` are
`serde_json::Value` (default `Null`). Unlike Python (which removed positional
`args` in #144), `TaskSchedule.args` remains functional in Rust — the scheduler
serializes both `args` and `kwargs` into the enqueue envelope.

`check()` dry-runs each enabled schedule's `args`/`kwargs` against the task's
declared input type (see Phase 2 below); a type mismatch is reported at
check-time, not just at execution.

### Schedule Patterns

- `IntervalSchedule` — `{ seconds, minutes, hours, days }` (at least one set)
- `HourlySchedule` — `{ minute, second }`
- `DailySchedule` — `{ time: NaiveTime }`
- `WeeklySchedule` — `{ days: Vec<Weekday>, time: NaiveTime }`
- `MonthlySchedule` — `{ day, time: NaiveTime }`
- `CronSchedule` — `{ minute: Vec<CronNumericTerm>, hour: Vec<CronNumericTerm>, month: Vec<CronEnumTerm<Month>>, day: DaySelector }`. Cron-style fields built from `CronNumericTerm` (`Every`/`Step`/`Values`/`Range`), `CronEnumTerm<Month>`, and `DaySelector` (`EveryDay`/`ByMonthDay`/`ByWeekday`/`EitherDay`/`BothDays`). Wire-format byte-identical to Python's cron schedule.

## `Horsies::check()`

Phased validation. Each phase appends to a single `ValidationReport`; all
phases run and their failures are aggregated into one combined `Err` (offline
phases do not short-circuit), so a single `check()` surfaces every problem.

`check()` aggregates all phase failures into a single `ValidationReport` and
returns one combined `Err`, or `Ok(())` when clean:

```rust
match app.check() {
    Ok(()) => println!("config OK"),
    Err(err) => eprintln!("check failed:\n{}", err),
}
// or, to abort on failure:
app.check()?;
```

### Phases

| Phase | What | Rust status |
|---|---|---|
| 1 | Config — validated at `Horsies::new()` | Done (implicit) |
| 2 | Schedule validation — task names, queue names, and each enabled schedule's `args`/`kwargs` dry-run against the task's declared input type | Done |
| 2.5 | Runtime policy safety — registered task `task_options`, `retry_policy`, `auto_retry_for`, reserved code collisions | Done |
| 2.6 | Custom-mode task queue metadata validation | Done |
| 2.8 | Registered workflow nodes reference registered tasks | Done |
| 2.9 | Workflow node queues are resolved and valid | Done |
| 2.10 | Workflow nodes that require input have args, kwargs, or `args_from` | Done |
| 2.11 | Workflow node static kwargs dry-run against the task's declared input type (skips nodes with `args_from` or node-level `workflow_ctx_from`); runs for registered specs and builder `run_case` specs | Done |
| 3 | Workflow `definition_key` presence (HRS-016) | Done |
| 3.1 | Declared child workflow keys resolve to a registered child | Done |
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
- a node's fully-static kwargs deserialize into the task's declared input type
  (Phase 2.11; nodes with `args_from` or node-level `workflow_ctx_from` are
  skipped because their static payload is intentionally partial)

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
top-level app. `check_with(live: bool)` runs the offline checks and, when
`live = true`, the connectivity check in one call.

### Error codes

| Code | Description | Phase |
|---|---|---|
| `HRS-001–HRS-020`, `HRS-025`, `HRS-027–HRS-030`, `HRS-032` | Workflow validation errors | 2.8–3.5 |
| `HRS-014` | Workflow queue unresolved or invalid | 2.9, builder validation, start-time validation |
| `HRS-016` | Missing `definition_key` | 3, 3.2 |
| `HRS-017` | Duplicate `definition_key` | 3.5 |
| `HRS-019` | Workflow node static kwargs do not match the task's declared input type | 2.11, builder validation |
| `HRS-020` | Workflow node missing required input | 2.10, builder validation, start-time validation |
| `HRS-205` | Schedule invalid — incl. `args`/`kwargs` not matching the task's declared input type | 2 |
| `HRS-027` | Parameterized builder without cases | 3.2 |
| `HRS-029` | Builder exception / wrong return type | 3.2 |
| `HRS-100–HRS-103` | Task definition errors | 2.5 |
| `HRS-102` | Invalid task_options / retry policy | 2.5 |
| `HRS-200–HRS-211` | Configuration errors | 1 |
| `HRS-203` | Broker connectivity failure | live check (`check_live` / `check_with(true)`) |
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
    CronSchedule, CronNumericTerm, CronEnumTerm, DaySelector, CronOrdinal, Month,
    ResolvedEnqueue, ErrorCode, HorsiesError, ValidationReport,
};
```

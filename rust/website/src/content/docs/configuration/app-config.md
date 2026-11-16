---
title: AppConfig
summary: Root configuration for a horsies application.
related: [broker-config, recovery-config, retention-config, ../../concepts/queue-modes]
tags: [configuration, AppConfig]
---

## Basic Usage

```rust
use horsies::{
    Horsies, AppConfig,
};

let config = AppConfig::for_database_url("postgresql://user:pass@localhost:5432/mydb");
let mut app = Horsies::new(config)?;
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `queue_mode` | `QueueMode` | `Default` | Queue operating mode |
| `custom_queues` | `Option<Vec<CustomQueueConfig>>` | `None` | Queue definitions (Custom mode only) |
| `broker` | `PostgresConfig` | required | Database connection settings |
| `cluster_wide_cap` | `Option<u32>` | `None` | Max in-flight tasks cluster-wide |
| `prefetch_buffer` | `u32` | `0` | 0 = hard cap mode, >0 = soft cap with prefetch |
| `claim_lease_ms` | `Option<u32>` | `None` | Claim lease duration (required if prefetch_buffer > 0; optional override in hard cap mode) |
| `max_claim_renew_age_ms` | `u32` | `180000` | Max age (ms) of a CLAIMED task that heartbeat will renew. Older claims are left to expire, preventing indefinite renewal of orphaned tasks. Must be >= effective claim lease |
| `payload` | `PayloadPolicy` | defaults | Payload-size guardrail (warn at 1 MiB, rejection off) |
| `recovery` | `RecoveryConfig` | defaults | Crash recovery settings |
| `retention` | `RetentionConfig` | defaults | Data lifetime, partition coverage, and cleanup settings |
| `resilience` | `WorkerResilienceConfig` | defaults | Worker retry/backoff and notify polling |
| `schedule` | `Option<ScheduleConfig>` | `None` | Scheduled task configuration |
| `resend_on_transient_err` | `bool` | `false` | Auto-retry transient enqueue/start failures for task sends, scheduled sends, and workflow starts |

## Queue Mode Configuration

### DEFAULT Mode

```rust
let config = AppConfig {
    queue_mode: QueueMode::Default,
    custom_queues: None,
    ..AppConfig::for_database_url("postgresql://...")
};
```

### CUSTOM Mode

```rust
use horsies::CustomQueueConfig;

let config = AppConfig {
    queue_mode: QueueMode::Custom,
    custom_queues: Some(vec![
        CustomQueueConfig { name: "high".into(), priority: 1, max_concurrency: 10 },
        CustomQueueConfig { name: "low".into(), priority: 100, max_concurrency: 3 },
    ]),
    ..AppConfig::for_database_url("postgresql://...")
};
```

See [Queue Modes](../../concepts/queue-modes) for details.

## Cluster-Wide Concurrency

Limit total in-flight tasks across all workers:

```rust
let config = AppConfig {
    cluster_wide_cap: Some(100), // Max 100 in-flight tasks (RUNNING + CLAIMED)
    ..AppConfig::for_database_url("postgresql://...")
};
```

Set to `None` (default) for unlimited.

**Note:** When `cluster_wide_cap` is set, the system operates in hard cap mode (counts RUNNING + CLAIMED tasks). This ensures strict enforcement and fair distribution across workers.

## Prefetch Configuration

Control whether workers can prefetch tasks beyond their running capacity:

```rust
// Hard cap mode (default) - strict enforcement, fair distribution
let config = AppConfig {
    prefetch_buffer: 0, // No prefetch, workers only claim what they can run
    ..AppConfig::for_database_url("postgresql://...")
};

// Soft cap mode - allows prefetch with lease expiry
let config = AppConfig {
    prefetch_buffer: 4,             // Prefetch up to 4 extra tasks per worker
    claim_lease_ms: Some(5000),     // Prefetched claims expire after 5 seconds
    ..AppConfig::for_database_url("postgresql://...")
};
```

**Important:** `cluster_wide_cap` cannot be used with `prefetch_buffer > 0`. If you need a global cap, hard cap mode is required.

See [Concurrency](../../workers/concurrency) for detailed explanation of hard vs soft cap modes.

## Recovery Configuration

Override crash recovery defaults:

```rust
use horsies::RecoveryConfig;

let config = AppConfig {
    recovery: RecoveryConfig {
        auto_requeue_stale_claimed: true,
        claimed_stale_threshold_ms: 120_000,  // 2 minutes
        auto_fail_stale_running: true,
        running_stale_threshold_ms: 300_000,  // 5 minutes
        ..RecoveryConfig::default()
    },
    ..AppConfig::for_database_url("postgresql://...")
};
```

See [Recovery Config](../recovery-config) for all options.

## Retention Configuration

Set record lifetime and partition coverage under `retention`:

```rust
use std::collections::HashMap;

use chrono::Duration;
use horsies::{RetentionClassConfig, RetentionConfig};

let config = AppConfig {
    retention: RetentionConfig {
        queue_retention: HashMap::from([
            ("emails".to_owned(), Some(Duration::days(7))),
            ("audit".to_owned(), None),
        ]),
        retention_classes: vec![RetentionClassConfig {
            key: "audit_1y".to_owned(),
            duration: Duration::days(365),
        }],
        ..RetentionConfig::default()
    },
    ..AppConfig::for_database_url("postgresql://...")
};
```

`None` in `queue_retention` means forever. A missing queue mapping uses the
30-day default class. See [Retention Config](../retention-config).

## Payload Guardrail

`payload` bounds serialized task payloads at the encode boundaries. The check compares the length of the already-serialized JSON — one integer comparison per enqueue/result, no extra serialization pass.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `warn_bytes` | `Option<u64>` | `Some(1_048_576)` (1 MiB) | Log a structured warning when a payload exceeds this size, rate-limited to once per (task_name, kind) per process. `None` disables |
| `reject_bytes` | `Option<u64>` | `None` (off) | Fail an enqueue closed (`PAYLOAD_TOO_LARGE`, nothing written) when its payload exceeds this size. Results are never rejected — the work is done, and destroying it would convert a size concern into data loss |

Per-boundary coverage:

| Boundary | Warn | Reject |
|----------|------|--------|
| `send` / `schedule` (args + kwargs) | yes | yes |
| Scheduler slot enqueue (static schedule args/kwargs) | yes | yes (slot not enqueued, logged via the schedule's enqueue-failure path) |
| Worker terminal result (success payload and error envelope) | yes | never |
| Workflow-node enqueue (args_from-merged kwargs, measured on the final kwargs JSON incl. injected workflow context) | yes | no — rejecting a mid-workflow node needs a designed node-failure path; a size limit must not strand a running workflow |

```rust
use horsies::PayloadPolicy;

let config = AppConfig {
    payload: PayloadPolicy {
        warn_bytes: Some(512 * 1024),      // warn at 512 KiB
        reject_bytes: Some(4 * 1024 * 1024), // reject enqueues over 4 MiB
    },
    ..AppConfig::for_database_url("postgresql://...")
};
```

## Resilience Configuration

Configure worker retry/backoff and NOTIFY fallback polling:

```rust
use horsies::WorkerResilienceConfig;

let config = AppConfig {
    resilience: WorkerResilienceConfig {
        db_retry_initial_ms: 500,
        db_retry_max_ms: 30_000,
        db_retry_max_attempts: 0, // 0 = infinite
        notify_poll_interval_ms: 5_000,
    },
    ..AppConfig::for_database_url("postgresql://...")
};
```

## Schedule Configuration

Enable scheduled tasks:

```rust
use horsies::{ScheduleConfig, TaskSchedule, SchedulePattern, DailySchedule};
use chrono::NaiveTime;

let config = AppConfig {
    schedule: Some(ScheduleConfig::new(vec![
        TaskSchedule::new(
            "daily-cleanup",
            "cleanup_old_data",
            SchedulePattern::Daily(DailySchedule {
                time: NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
            }),
        ),
    ])),
    ..AppConfig::for_database_url("postgresql://...")
};
```

See [Scheduler Overview](../../scheduling/scheduler-overview) for details.

## Validation

`AppConfig` is validated when you construct `Horsies` or run `app.check()`:

- CUSTOM mode requires non-empty `custom_queues`
- DEFAULT mode must not have `custom_queues`
- Queue names must be unique
- `cluster_wide_cap` must be positive if set
- `prefetch_buffer` must be non-negative
- `claim_lease_ms` is required when `prefetch_buffer > 0`
- `claim_lease_ms` is optional when `prefetch_buffer = 0` (overrides the default 60s lease)
- `cluster_wide_cap` cannot be combined with `prefetch_buffer > 0`
- retention class keys and durations must be valid
- `queue_retention` keys must name configured queues

Multiple validation errors within the same phase are collected and reported together (compiler-style), rather than stopping at the first error.

## Startup Validation (`app.check()`)

Use `app.check()` to run static validation before starting a worker or scheduler. Use `app.check_live()` to additionally connect to PostgreSQL, ensure the schema, and verify broker connectivity.

```rust
// Static validation — returns Result<(), HorsiesError>
app.check()?;

// Static + broker connectivity check
app.check_live().await?;
```

**Phases:**

| Phase | What it validates | Gating |
|-------|-------------------|--------|
| 1. Config | `AppConfig`, `RecoveryConfig`, `ScheduleConfig` consistency | Validated at construction (implicit pass) |
| 2. Task registry | All registered tasks have valid names and queue assignments | Errors stop progression to later phases |
| 3. Workflows | `WorkflowSpec` DAG validation (cycles, missing deps, type mismatches) | Collected alongside Phase 2 |
| 4. Broker (if `check_live`) | Connects to PostgreSQL, ensures the Horsies schema, and runs `SELECT 1` | Only runs if earlier phases pass |

**Returns** `Ok(())` when all validations pass, or `Err(HorsiesError)` with diagnostic messages.

**CLI equivalent:**

```bash
horsies check ./config/horsies.toml       # Static validation
horsies check ./config/horsies.toml --live # Static + broker connectivity
```

## Logging Configuration

Log the configuration (with masked password):

```rust
use tracing::info;

info!("AppConfig:\n{}", config.format_for_logging());
```

Output:

```
AppConfig:
  queue_mode: DEFAULT
  broker:
    database_url: postgresql://user:***@localhost/db
    pool_size: 30
    max_overflow: 30
  recovery:
    auto_requeue_stale_claimed: true
    ...
```

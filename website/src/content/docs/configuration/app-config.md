---
title: AppConfig
summary: Root configuration for a horsies application.
related: [broker-config, recovery-config, ../../concepts/queue-modes]
tags: [configuration, AppConfig]
---

## Basic Usage

```python
from horsies import Horsies, AppConfig, PostgresConfig, QueueMode

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(database_url="postgresql+psycopg://..."),
)
app = Horsies(config)
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `queue_mode` | `QueueMode` | `DEFAULT` | Queue operating mode |
| `custom_queues` | `list[CustomQueueConfig]` | `None` | Queue definitions (CUSTOM mode only) |
| `broker` | `PostgresConfig` | required | Database connection settings |
| `cluster_wide_cap` | `int` | `None` | Max in-flight tasks cluster-wide |
| `prefetch_buffer` | `int` | `0` | 0 = hard cap mode, >0 = soft cap with prefetch |
| `claim_lease_ms` | `int` | `None` | Claim lease duration (required if prefetch_buffer > 0; must be None when prefetch_buffer = 0) |
| `recovery` | `RecoveryConfig` | defaults | Crash recovery settings |
| `resilience` | `WorkerResilienceConfig` | defaults | Worker retry/backoff and notify polling |
| `schedule` | `ScheduleConfig` | `None` | Scheduled task configuration |

## Queue Mode Configuration

### DEFAULT Mode

```python
config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(...),
    # custom_queues must be None or omitted
)
```

### CUSTOM Mode

```python
from horsies import CustomQueueConfig

config = AppConfig(
    queue_mode=QueueMode.CUSTOM,
    custom_queues=[
        CustomQueueConfig(name="high", priority=1, max_concurrency=10),
        CustomQueueConfig(name="low", priority=100, max_concurrency=3),
    ],
    broker=PostgresConfig(...),
)
```

See [Queue Modes](../../concepts/queue-modes) for details.

## Cluster-Wide Concurrency

Limit total in-flight tasks across all workers:

```python
config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(...),
    cluster_wide_cap=100,  # Max 100 in-flight tasks (RUNNING + CLAIMED)
)
```

Set to `None` (default) for unlimited.

**Note:** When `cluster_wide_cap` is set, the system operates in hard cap mode (counts RUNNING + CLAIMED tasks). This ensures strict enforcement and fair distribution across workers.

## Prefetch Configuration

Control whether workers can prefetch tasks beyond their running capacity:

```python
# Hard cap mode (default) - strict enforcement, fair distribution
config = AppConfig(
    broker=PostgresConfig(...),
    prefetch_buffer=0,  # No prefetch, workers only claim what they can run
)

# Soft cap mode - allows prefetch with lease expiry
config = AppConfig(
    broker=PostgresConfig(...),
    prefetch_buffer=4,      # Prefetch up to 4 extra tasks per worker
    claim_lease_ms=5000,    # Prefetched claims expire after 5 seconds
)
```

**Important:** `cluster_wide_cap` cannot be used with `prefetch_buffer > 0`. If you need a global cap, hard cap mode is required.

See [Concurrency](../../workers/concurrency) for detailed explanation of hard vs soft cap modes.

## Recovery Configuration

Override crash recovery defaults:

```python
from horsies import RecoveryConfig

config = AppConfig(
    broker=PostgresConfig(...),
    recovery=RecoveryConfig(
        auto_requeue_stale_claimed=True,
        claimed_stale_threshold_ms=120_000,  # 2 minutes
        auto_fail_stale_running=True,
        running_stale_threshold_ms=300_000,  # 5 minutes
    ),
)
```

See [Recovery Config](recovery-config.md) for all options.

## Resilience Configuration

Configure worker retry/backoff and NOTIFY fallback polling:

```python
from horsies import WorkerResilienceConfig

config = AppConfig(
    broker=PostgresConfig(...),
    resilience=WorkerResilienceConfig(
        db_retry_initial_ms=500,
        db_retry_max_ms=30_000,
        db_retry_max_attempts=0,  # 0 = infinite
        notify_poll_interval_ms=5_000,
    ),
)
```

## Schedule Configuration

Enable scheduled tasks:

```python
from horsies import ScheduleConfig, TaskSchedule, DailySchedule
from datetime import time

config = AppConfig(
    broker=PostgresConfig(...),
    schedule=ScheduleConfig(
        enabled=True,
        check_interval_seconds=1,
        schedules=[
            TaskSchedule(
                name="daily-cleanup",
                task_name="cleanup_old_data",
                pattern=DailySchedule(time=time(3, 0, 0)),
            ),
        ],
    ),
)
```

See [Scheduler Overview](../../scheduling/scheduler-overview) for details.

## Validation

`AppConfig` validates consistency at creation:

- CUSTOM mode requires non-empty `custom_queues`
- DEFAULT mode must not have `custom_queues`
- Queue names must be unique
- `cluster_wide_cap` must be positive if set
- `prefetch_buffer` must be non-negative
- `claim_lease_ms` is required when `prefetch_buffer > 0`
- `claim_lease_ms` must be None when `prefetch_buffer = 0`
- `cluster_wide_cap` cannot be combined with `prefetch_buffer > 0`

Multiple validation errors within the same phase are collected and reported together (compiler-style), rather than stopping at the first error.

## Startup Validation (`app.check()`)

Use `app.check()` to run phased validation before starting a worker or scheduler. This is the programmatic equivalent of the [`horsies check`](../cli#horsies-check) CLI command.

```python
errors = app.check(live=True)
if errors:
    for err in errors:
        print(err)
else:
    print("All validations passed")
```

**Signature:**

```python
def check(self, *, live: bool = False) -> list[HorsiesError]
```

**Phases:**

| Phase | What it validates | Gating |
|-------|-------------------|--------|
| 1. Config | `AppConfig`, `RecoveryConfig`, `ScheduleConfig` consistency | Validated at construction (implicit pass) |
| 2. Task imports | Imports all discovered task modules, collects import/registration errors | Errors stop progression to Phase 4 |
| 3. Workflows | `WorkflowSpec` DAG validation (triggered during module imports) | Collected alongside Phase 2 |
| 4. Broker (if `live=True`) | Connects to PostgreSQL and runs `SELECT 1` | Only runs if Phases 2-3 pass |

**Returns** an empty list when all validations pass, or a list of `HorsiesError` instances with Rust-style formatting.

**CLI equivalent:**

```bash
horsies check myapp.instance:app        # Phases 1-3
horsies check myapp.instance:app --live  # Phases 1-4
```

## Logging Configuration

Log the configuration (with masked password):

```python
import logging
logger = logging.getLogger("myapp")

config.log_config(logger)
```

Output:

```
AppConfig:
  queue_mode: DEFAULT
  broker:
    database_url: postgresql+psycopg://user:***@localhost/db
    pool_size: 5
    max_overflow: 10
  recovery:
    auto_requeue_stale_claimed: True
    ...
```

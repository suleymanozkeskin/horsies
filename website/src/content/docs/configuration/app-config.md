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
| 2. Task imports | Imports all discovered task modules, collects import/registration errors | Errors stop progression to Phases 4-6 |
| 3. Workflows | `WorkflowSpec` DAG validation (triggered during module imports) | Collected alongside Phase 2 |
| 4. Workflow builders | Executes `@app.workflow_builder` functions, validates returned specs, detects undecorated builders | Errors stop progression to Phases 5-6 |
| 5. Runtime policy safety | Validates retry/exception-mapper policy metadata after imports | Errors stop progression to Phase 6 |
| 6. Broker (if `live=True`) | Connects to PostgreSQL and runs `SELECT 1` | Only runs if Phases 2-5 pass |

**Returns** an empty list when all validations pass, or a list of `HorsiesError` instances with Rust-style formatting.

**CLI equivalent:**

```bash
horsies check myapp.instance:app        # Phases 1-5
horsies check myapp.instance:app --live  # Phases 1-6
```

### `@app.workflow_builder`

Register a workflow builder function for check-phase validation. During `horsies check`, registered builders are executed under send suppression (no tasks are actually enqueued) to validate the `WorkflowSpec` they produce.

```python
# Zero-parameter builder — called with no arguments during check
@app.workflow_builder()
def build_etl_pipeline() -> WorkflowSpec:
    ...

# Parameterized builder — requires cases= for check to exercise
@app.workflow_builder(cases=[
    {'region': 'us-east'},
    {'region': 'eu-west'},
])
def build_regional_pipeline(region: str) -> WorkflowSpec:
    ...
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `cases` | `list[dict[str, Any]] \| None` | Kwarg dicts to invoke the builder with during check. Required when the builder has parameters without defaults. |

**What check validates per builder invocation:**

- Builder does not raise exceptions (E029)
- Builder returns a `WorkflowSpec` instance (E029)
- The returned `WorkflowSpec` passes full DAG validation (node IDs, dependencies, kwargs, args_from types, etc.)

### Guarantee Model

`horsies check` has two validation paths with different guarantee levels:

**Strong guarantee (decorated builders):** Functions registered with `@app.workflow_builder` are executed during check. Every `WorkflowSpec` they produce is fully validated — DAG structure, kwargs against function signatures, `args_from` type compatibility, missing required params, and more. For the exercised builder cases, this catches structural workflow validity errors before runtime.

**Best-effort (undecorated builder detection):** Check also scans discovered task modules for top-level functions whose return type annotation is `WorkflowSpec` but lack the `@app.workflow_builder` decorator. These produce E030 check errors. This detection is heuristic:

- It only scans modules directly listed in `discover_tasks()`, not sub-modules of discovered packages.
- Functions re-exported in `__init__.py` from sub-modules may not be detected.

E030 is a safety net, not an absolute proof that no undecorated builders exist.

### CI Playbook

For deterministic, high-confidence workflow validity in CI:

1. Decorate every workflow entrypoint with `@app.workflow_builder(...)`.
2. For parameterized builders, provide `cases=` that cover all meaningful branches and shapes.
3. Ensure all builder modules are imported by the app used in `horsies check` (no hidden or dynamic registration paths).
4. Run `horsies check` in CI and fail the pipeline on any errors.
5. Treat E030 as additional lint signal, not the primary guarantee mechanism.

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

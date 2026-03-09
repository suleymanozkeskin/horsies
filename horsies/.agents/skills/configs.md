---
name: horsies-configs
description: Configuration and operations guidance for horsies, including AppConfig, PostgresConfig, queue modes, recovery and resilience tuning, scheduling, CLI checks, and environment variables. Use when setting up, tuning, or troubleshooting runtime configuration.
---

# horsies — Configuration

Detailed reference for all configuration types, validation rules, CLI commands,
and environment variables.

## AppConfig

Root configuration passed to `Horsies(config=AppConfig(...))`.
Pydantic `BaseModel`, frozen after construction.

```python
from horsies import Horsies, AppConfig, PostgresConfig

app = Horsies(config=AppConfig(
    broker=PostgresConfig(database_url="postgresql+psycopg://user:pass@host/db"),
))
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `queue_mode` | `QueueMode` | `DEFAULT` | `DEFAULT` or `CUSTOM` |
| `custom_queues` | `list[CustomQueueConfig] \| None` | `None` | Required if `CUSTOM` mode |
| `broker` | `PostgresConfig` | required | Database connection config |
| `cluster_wide_cap` | `int \| None` | `None` | Max RUNNING tasks across cluster; None = unlimited |
| `prefetch_buffer` | `int` | `0` | 0 = hard cap; >0 = soft cap with lease |
| `claim_lease_ms` | `int \| None` | `None` | Claim lease duration; None uses default 60s |
| `max_claim_renew_age_ms` | `int` | `180_000` (3 min) | Max age of CLAIMED task that heartbeat will renew |
| `recovery` | `RecoveryConfig` | `RecoveryConfig()` | Stale task detection and retention |
| `resilience` | `WorkerResilienceConfig` | `WorkerResilienceConfig()` | Worker retry behavior |
| `schedule` | `ScheduleConfig \| None` | `None` | Recurring task schedules |
| `exception_mapper` | `ExceptionMapper` | `{}` | Global exception-to-error-code mapping |
| `default_unhandled_error_code` | `str` | `"UNHANDLED_EXCEPTION"` | Must be `UPPER_SNAKE_CASE` |
| `resend_on_transient_err` | `bool` | `False` | Auto-retry transient ENQUEUE_FAILED |

### Validation Rules

**Queue mode (E200):**
- `DEFAULT`: `custom_queues` must be `None`.
- `CUSTOM`: `custom_queues` must be non-None, non-empty, with unique names.

**Cluster cap (E201):**
- Must be > 0 when set.
- Mutually exclusive with `prefetch_buffer > 0` (cluster cap requires hard cap mode).

**Prefetch (E202):**
- Must be >= 0.
- When > 0, `claim_lease_ms` must be explicitly set (not None) and > 0.
- Effective lease must be >= 2x `recovery.claimer_heartbeat_interval_ms`.
- `max_claim_renew_age_ms` must be > 0 and >= effective lease.

**Exception mapper (E209):**
- Keys must be `BaseException` subclasses.
- Values must match `^[A-Z][A-Z0-9_]*$`.
- Values that look like exception class names (e.g., `"TimeoutError"`) are rejected.

## Horsies App Methods

Methods on the `Horsies` instance beyond `@app.task(...)` and `app.workflow(...)`:

```python
# Register task modules for worker discovery (records paths, no I/O)
app.discover_tasks(["myapp.tasks", "myapp.jobs.tasks"])

# Glob patterns — expand first, then discover
paths = app.expand_module_globs(["src/**/*_tasks.py"])
app.discover_tasks(paths)

# Get the configured PostgresBroker (for monitoring/introspection)
broker = app.get_broker()

# List registered task names
names = app.list_tasks()  # list[str]
```

### Direct Broker Methods

`app.get_broker()` returns a `PostgresBroker` with methods for result retrieval and monitoring. All methods have sync and async variants.

**Result retrieval** (use when you have a task ID but no `TaskHandle`):

```python
broker = app.get_broker()

# Wait for a task result by ID — returns TaskResult[Any, TaskError], never raises
result = await broker.get_result_async("task-uuid", timeout_ms=5000)
result = broker.get_result("task-uuid", timeout_ms=5000)  # sync

# Fetch task metadata by ID — returns BrokerResult[TaskInfo | None]
info = await broker.get_task_info_async("task-uuid", include_result=True)
info = broker.get_task_info("task-uuid", include_result=True)  # sync
```

**Monitoring** (async only): `get_stale_tasks()`, `get_worker_stats()`, `get_expired_tasks()`, `mark_stale_tasks_as_failed()`, `requeue_stale_claimed()`. See website docs `monitoring/broker-methods` for full signatures.

## PostgresConfig

```python
from horsies import PostgresConfig

config = PostgresConfig(
    database_url="postgresql+psycopg://user:pass@localhost:5432/mydb",
)
```

| Field | Type | Default | Description |
|---|---|---|---|
| `database_url` | `str` | required | Must start with `"postgresql+psycopg"` (E203 otherwise) |
| `pool_size` | `int` | `30` | Connection pool size |
| `max_overflow` | `int` | `30` | Extra connections beyond pool_size |
| `pool_timeout` | `int` | `30` | Seconds to wait for a connection |
| `pool_recycle` | `int` | `1800` | Seconds before connections are recycled |
| `pool_pre_ping` | `bool` | `True` | Pre-ping connections before use |
| `echo` | `bool` | `False` | Echo SQL (debug only) |

Driver must be psycopg3 (async). `postgresql+psycopg2://` is rejected.

## QueueMode

```python
class QueueMode(Enum):
    DEFAULT = 'default'
    CUSTOM = 'custom'
```

### DEFAULT

- Single `"default"` queue.
- No `queue_name` on `@app.task(...)` — passing one raises E103.

### CUSTOM

- Named queues via `custom_queues`.
- Every `@app.task(...)` must include `queue_name` matching a configured queue.

## CustomQueueConfig

```python
from horsies import CustomQueueConfig

config = AppConfig(
    queue_mode=QueueMode.CUSTOM,
    custom_queues=[
        CustomQueueConfig(name="critical", priority=1, max_concurrency=10),
        CustomQueueConfig(name="background", priority=50, max_concurrency=3),
    ],
    broker=PostgresConfig(database_url="..."),
)
```

| Field | Type | Default | Range | Description |
|---|---|---|---|---|
| `name` | `str` | required | — | Unique queue name |
| `priority` | `int` | `1` | 1–100 | 1 = highest, 100 = lowest |
| `max_concurrency` | `int` | `5` | — | Max simultaneous RUNNING tasks for this queue |

Lower priority number = claimed first. `cluster_wide_cap` still applies as an upper bound.

## RecoveryConfig

Controls stale task detection, automatic recovery, and data retention.

| Field | Type | Default | Range | Description |
|---|---|---|---|---|
| `auto_requeue_stale_claimed` | `bool` | `True` | — | Requeue tasks stuck in CLAIMED |
| `claimed_stale_threshold_ms` | `int` | `120_000` | 1s–1hr | Ms before CLAIMED task is stale |
| `auto_fail_stale_running` | `bool` | `True` | — | Fail tasks stuck in RUNNING |
| `running_stale_threshold_ms` | `int` | `300_000` | 1s–2hr | Ms before RUNNING task is stale |
| `check_interval_ms` | `int` | `30_000` | 1s–10min | Reaper poll cadence |
| `runner_heartbeat_interval_ms` | `int` | `30_000` | 1s–2min | Heartbeat from running task process |
| `claimer_heartbeat_interval_ms` | `int` | `30_000` | 1s–2min | Heartbeat for CLAIMED tasks |
| `heartbeat_retention_hours` | `int \| None` | `24` | 1–8760; None disables | Prune old heartbeat rows |
| `worker_state_retention_hours` | `int \| None` | `168` (7d) | 1–8760; None disables | Prune old worker_state rows |
| `terminal_record_retention_hours` | `int \| None` | `720` (30d) | 1–43800; None disables | Prune terminal task/workflow rows |

### Constraints (E204)

- `running_stale_threshold_ms >= runner_heartbeat_interval_ms * 2`
- `claimed_stale_threshold_ms >= claimer_heartbeat_interval_ms * 2`

The 2x factor ensures a task can miss one full heartbeat cycle without being incorrectly marked stale.

### What the reaper does

Runs on `check_interval_ms` cadence:
1. CLAIMED tasks without heartbeat for `claimed_stale_threshold_ms` → requeued to PENDING (if enabled).
2. RUNNING tasks without heartbeat for `running_stale_threshold_ms` → marked FAILED (if enabled).
3. Hourly retention pruning: deletes old heartbeat, worker_state, and terminal rows based on retention settings.

**CPU/GIL-heavy tasks:** Increase `running_stale_threshold_ms`. GIL-bound tasks may not send heartbeats at the configured interval. Rule of thumb: >= 3–5x worst-case heartbeat gap.

## WorkerResilienceConfig

Controls worker retry behavior on transient DB failures.

| Field | Type | Default | Range | Description |
|---|---|---|---|---|
| `db_retry_initial_ms` | `int` | `500` | 100ms–60s | Initial backoff |
| `db_retry_max_ms` | `int` | `30_000` | 500ms–5min | Max backoff cap |
| `db_retry_max_attempts` | `int` | `0` | 0–10000 | Max retries; 0 = infinite |
| `notify_poll_interval_ms` | `int` | `5_000` | 1s–5min | Fallback poll when NOTIFY is silent |

### Constraint (E208)

`db_retry_max_ms >= db_retry_initial_ms`.

### Retry behavior

Exponential backoff with ±25% jitter:
```
delay = min(db_retry_max_ms, db_retry_initial_ms * 2^(attempt-1))
```

`notify_poll_interval_ms` is the safety-net polling interval. Primary dispatch uses PostgreSQL `LISTEN/NOTIFY`.

## Scheduling

### ScheduleConfig

| Field | Type | Default | Range | Description |
|---|---|---|---|---|
| `enabled` | `bool` | `True` | — | Master on/off |
| `schedules` | `list[TaskSchedule]` | `[]` | — | Scheduled task definitions |
| `check_interval_seconds` | `int` | `1` | 1–60 | How often scheduler checks for due runs |

All `TaskSchedule.name` values must be unique (E205).

### TaskSchedule

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Unique schedule identifier |
| `task_name` | `str` | required | Must match a registered task |
| `pattern` | `SchedulePattern` | required | When to run |
| `args` | `tuple` | `()` | Positional args to task |
| `kwargs` | `dict` | `{}` | Keyword args to task |
| `queue_name` | `str \| None` | `None` | Target queue |
| `enabled` | `bool` | `True` | Per-schedule on/off |
| `timezone` | `str` | `"UTC"` | IANA timezone |
| `catch_up_missed` | `bool` | `False` | Execute missed runs on restart |
| `max_catch_up_runs` | `int` | `100` | 1–10000; max runs per tick during catch-up |

### Schedule Patterns

**IntervalSchedule** — run every N time units:

```python
IntervalSchedule(seconds=30)
IntervalSchedule(hours=1, minutes=30)  # total = 90 minutes
```

Fields: `seconds`, `minutes`, `hours`, `days` — all `int | None`. At least one must be set (E205).

**HourlySchedule** — every hour at fixed offset:

```python
HourlySchedule(minute=30, second=0)  # every hour at XX:30:00
```

**DailySchedule** — every day at fixed time:

```python
DailySchedule(time=time(3, 0, 0))  # daily at 03:00
```

**WeeklySchedule** — specific weekdays at fixed time:

```python
WeeklySchedule(days=[Weekday.MONDAY, Weekday.FRIDAY], time=time(9, 0))
```

`days` must have no duplicates (E205).

**MonthlySchedule** — fixed day of month:

```python
MonthlySchedule(day=15, time=time(15, 0))  # 15th at 15:00
```

If `day` > days in month (e.g., day=31 in Feb), that month is skipped.

### Weekday

```python
class Weekday(str, Enum):
    MONDAY = 'monday'
    TUESDAY = 'tuesday'
    WEDNESDAY = 'wednesday'
    THURSDAY = 'thursday'
    FRIDAY = 'friday'
    SATURDAY = 'saturday'
    SUNDAY = 'sunday'
```

## `resend_on_transient_err`

When `True` on `AppConfig`, enables automatic retry of transient `ENQUEUE_FAILED` errors for both task sends and workflow starts.

**Retry parameters** (hardcoded, not user-configurable):
- 3 retries after initial attempt (4 total)
- Initial backoff: 200ms
- Max backoff: 2000ms
- Exponential backoff (no jitter)

Only retries errors with `retryable=True`. Permanent errors (validation, serialization) return immediately.

`app.workflow()` copies this setting onto each `WorkflowSpec` automatically.

## `horsies check` — Phased Validation

```bash
horsies check myapp.config:app [--live]
```

Phases are fail-fast: errors in phase N stop later phases.

| Phase | What | Errors |
|---|---|---|
| 1 — Config | `AppConfig` Pydantic validators (implicit — already validated at construction) | E200–E209 |
| 2 — Imports | Import each discovered module | ImportError, E210 |
| 3 — Workflows | `WorkflowSpec` DAG validation during imports | E001–E031 |
| 3.1 — Builders | Execute `@app.workflow_builder` functions | E027–E030 |
| 3.2 — Undecorated | Scan for functions returning WorkflowSpec without decorator | E030 |
| 3.5 — Policies | Re-validate exception_mapper after imports | E209 |
| 4 — Broker | `SELECT 1` against PostgreSQL (`--live` only) | E211, Connection error |

**Guarantee model:**
- **Strong:** `@app.workflow_builder` functions are fully executed and validated.
- **Best-effort:** Undecorated builder detection (E030) only scans directly discovered modules.

Worker and scheduler also run `app.check(live=False)` at startup.

## CLI Commands

### Module locator format

```
myapp.config:app      # dotted module + attribute (recommended)
myapp.config          # dotted module, auto-discover Horsies instance
app/config.py:app     # file path + attribute
app/config.py         # file path, auto-discover
```

Auto-discovery: exactly one `Horsies` instance → use it. Zero or multiple → E206.

### `horsies worker`

```bash
horsies worker <module> [--processes N] [--loglevel LEVEL] [--max-claim-batch N] [--max-claim-per-worker N]
```

| Flag | Default | Description |
|---|---|---|
| `--processes` | `1` | Worker process count |
| `--loglevel` | `INFO` | Log level |
| `--max-claim-batch` | `2` | Max tasks per queue per claim pass |
| `--max-claim-per-worker` | `0` | Max claimed per worker; 0 = auto |

Startup: logging → import app → `app.check()` → schema init with retry → start processes.
SIGTERM/SIGINT: graceful shutdown, waits for running tasks.

### `horsies scheduler`

```bash
horsies scheduler <module> [--loglevel LEVEL]
```

Requires `app.config.schedule` to be set and enabled. Same startup sequence as worker.

### `horsies check`

```bash
horsies check <module> [--live] [--loglevel LEVEL]
```

`--live` adds Phase 4 (broker connectivity). Default `--loglevel` is `WARNING`.

### `horsies get-docs`

```bash
horsies get-docs [--output DIR]
```

Downloads docs as local markdown to `DIR` (default `.horsies-docs/`). For AI agents to read without web requests.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Error |

## Environment Variables

| Variable | Effect |
|---|---|
| `HORSIES_VERBOSE=1` | Show full Python traceback alongside Rust-style errors |
| `HORSIES_PLAIN_ERRORS=1` | Fall back to default Python exception formatting |
| `HORSIES_FORCE_COLOR=1` | Force ANSI colors even when not a TTY |
| `NO_COLOR` | Disable ANSI colors (https://no-color.org/) |

Color priority: `HORSIES_FORCE_COLOR` → `NO_COLOR` → `isatty()` auto-detect.

## All Public Imports

```python
from horsies import (
    # App
    Horsies, AppConfig,
    # Broker
    PostgresConfig,
    # Queue
    QueueMode, CustomQueueConfig,
    # Recovery / Resilience
    RecoveryConfig, WorkerResilienceConfig,
    # Scheduling
    ScheduleConfig, TaskSchedule,
    Weekday, IntervalSchedule, HourlySchedule,
    DailySchedule, WeeklySchedule, MonthlySchedule,
    # Exception mapper
    ExceptionMapper,
    # Retry
    RetryPolicy,
)
```

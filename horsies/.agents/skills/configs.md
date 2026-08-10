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
from pydantic import SecretStr

from horsies import Horsies, AppConfig, PostgresConfig

app = Horsies(config=AppConfig(
    broker=PostgresConfig(database_url=SecretStr("postgresql+psycopg://user:pass@host/db")),
))
```

`database_url` and `session_database_url` are `SecretStr` fields: a plain
string validates at runtime, but strict type checkers reject it — wrap with
`pydantic.SecretStr` explicitly.

```python
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
| `payload` | `PayloadPolicy` | `PayloadPolicy()` | Payload-size guardrail (warn/reject thresholds) |
| `recovery` | `RecoveryConfig` | `RecoveryConfig()` | Stale task detection and retention |
| `resilience` | `WorkerResilienceConfig` | `WorkerResilienceConfig()` | Worker retry behavior |
| `schedule` | `ScheduleConfig \| None` | `None` | Recurring task schedules |
| `exception_mapper` | `ExceptionMapper` | `{}` | Global exception-to-error-code mapping |
| `default_unhandled_error_code` | `str` | `"UNHANDLED_EXCEPTION"` | Must be `UPPER_SNAKE_CASE` |
| `resend_on_transient_err` | `bool` | `False` | Auto-retry transient ENQUEUE_FAILED |

### Validation Rules

**Queue mode (HRS-200):**
- `DEFAULT`: `custom_queues` must be `None`.
- `CUSTOM`: `custom_queues` must be non-None, non-empty, with unique names.

**Cluster cap (HRS-201):**
- Must be > 0 when set.
- Mutually exclusive with `prefetch_buffer > 0` (cluster cap requires hard cap mode).

**Prefetch (HRS-202):**
- Must be >= 0.
- When > 0, `claim_lease_ms` must be explicitly set (not None) and > 0.
- Effective lease must be >= 2x `recovery.claimer_heartbeat_interval_ms`.
- `max_claim_renew_age_ms` must be > 0 and >= effective lease.

**Exception mapper (HRS-209):**
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

# Per-child-process hook: sync zero-arg fn, runs once in every worker child
# (after task imports, before horsies' own child pool). Use for disposing
# fork-inherited app engines + rebinding worker-specific pool policy.
@app.on_child_process_start
def reset_db_for_child() -> None: ...

hooks = app.get_child_process_start_hooks()  # list[ChildProcessStartHook]
```

`on_child_process_start` rules: deduped by function identity; async functions
rejected (`HRS-214`); hooks fire on every child start including executor
restarts, so bodies must be idempotent. Fail-closed: a raising or hung hook
(10s budget) exits the child with a dedicated code and the worker STOPS with
the hook named — it does not restart-loop. Engine rebind requires indirection
(task code reads via a `get_engine()` accessor; `Engine.poolclass` is fixed at
construction). See website docs `workers/child-process-hooks`.

### Direct Broker Methods

`app.get_broker()` returns a `PostgresBroker` with methods for result retrieval and monitoring. All methods have sync and async variants.

**Result retrieval** (use when you have a task ID but no `TaskHandle`):

```python
broker = app.get_broker()

# Typed result by ID — app-level API (broker.get_result(_async) was removed in 0.1.2).
# Returns BrokerResult[TaskResult[Any, TaskError]]: outer surfaces infrastructure
# failures (INVALID_JSON_PAYLOAD / NO_TYPE_AVAILABLE), inner is the typed result.
outer = await app.get_result_async("task-uuid", timeout_ms=5000)
outer = app.get_result("task-uuid", timeout_ms=5000)  # sync

# Raw stored envelope, no typed decode — returns BrokerResult[RawResultRecord | None]
raw = await broker.get_raw_result_record_async("task-uuid", timeout_ms=5000)

# Fetch task metadata by ID — returns BrokerResult[TaskInfo | None]
info = await broker.get_task_info_async("task-uuid", include_result=True)
info = broker.get_task_info("task-uuid", include_result=True)  # sync
```

**Monitoring** (async only): `get_stale_tasks()`, `get_expired_tasks()`, `mark_stale_tasks_as_failed()`, `requeue_stale_claimed()`. See website docs `monitoring/broker-methods` for full signatures.

**Health/liveness** (app, async + sync): `ping_database_async()` (DatabasePing latency), `ping_workers_async(target_worker_id=None, timeout_seconds=2.0, min_responses=None)` (list[WorkerPong] — active ping-pong; min_responses=1 = fast liveness gate, returns on first reply), `list_worker_states_async()` / `get_worker_state_async(worker_id)` / `get_worker_state_history_async(worker_id, limit=None)` (WorkerStateSnapshot, includes idle workers). Replaces the retired `get_worker_stats()`. `WorkerStateSnapshot.memory_usage_mb` is the parent process only; `children_memory_mb` is the summed RSS of executor children (the memory-quota driver — use it to size `max_tasks_per_child` / `max_memory_per_child_mb`). See website docs `monitoring/worker-health`. 

## PostgresConfig

```python
from horsies import PostgresConfig

config = PostgresConfig(
    database_url=SecretStr("postgresql+psycopg://user:pass@localhost:5432/mydb"),
)
```

| Field | Type | Default | Description |
|---|---|---|---|
| `database_url` | `str` | required | Must start with `"postgresql+psycopg"` (HRS-203 otherwise) |
| `session_database_url` | `str | None` | `None` | Direct/session-capable URL for LISTEN and schema setup; required when `database_url` is transaction-pooled |
| `pgbouncer_transaction_mode` | `bool` | `False` | Disable prepared statements for transaction-pooled `database_url`; requires `session_database_url` |
| `pool_size` | `int` | `5` | Connection pool size (raise for high-throughput producers) |
| `max_overflow` | `int` | `10` | Extra connections beyond pool_size |
| `retain_rerun_input_default` | `bool` | `False` | App-level default for enqueue-time rerun-input retention; a per-task value overrides it and the resolved value is snapshotted at enqueue. A task enqueued without a retained input cannot be rerun later |
| `worker_pool_size` | `int | None` | `3` | Worker coordinator pool size; `None` inherits `pool_size` |
| `worker_max_overflow` | `int | None` | `2` | Worker coordinator overflow; `None` inherits `max_overflow` |
| `worker_child_pool_min_size` | `int` | `0` | Minimum connections kept by each child worker process |
| `worker_child_pool_max_size` | `int` | `2` | Maximum connections allowed per child worker process |
| `worker_child_pool_check` | `bool` | `True` | Health-check child pool connections on checkout; one round trip per checkout, disable alongside `pool_pre_ping` on high-RTT links |
| `pool_timeout` | `int` | `30` | Seconds to wait for a connection |
| `pool_recycle` | `int` | `1800` | Seconds before connections are recycled |
| `pool_pre_ping` | `bool` | `True` | Pre-ping connections before use |
| `tcp_keepalives` | `bool` | `True` | Enable libpq TCP keepalives on broker and child-process connections |
| `tcp_keepalives_idle` | `int` | `30` | Idle seconds before the first keepalive probe (libpq `keepalives_idle`) |
| `tcp_keepalives_interval` | `int` | `10` | Seconds between keepalive probes (libpq `keepalives_interval`) |
| `tcp_keepalives_count` | `int` | `3` | Unacknowledged probes before the connection is dropped (libpq `keepalives_count`) |
| `echo` | `bool` | `False` | Echo SQL (debug only) |

Driver must be psycopg3 (async). `postgresql+psycopg2://` is rejected.

`tcp_keepalives` (default on) keeps idle pooled sockets warm so a server-side
or middlebox idle-reap is detected and recycled at the socket layer instead of
surfacing as a mid-query `OperationalError`. `pool_pre_ping` and `pool_recycle`
are checkout-time guards and cannot catch a connection that dies in-flight. The
keepalives apply to the broker engine pool and each child-process pool; the
LISTEN/NOTIFY listener sets its own keepalives independently. Without them, the
OS idle default (often 7200s) outlives a pooler's idle-reap window — e.g.
PlanetScale's PgBouncer reaps idle connections within ~1–2h. Disabling is rarely
needed; lower `tcp_keepalives_idle` only for poolers that reap faster than 30s.

Worker processes use a separate connection profile by default. The worker
coordinator uses `worker_pool_size + worker_max_overflow`; each child process
creates its own post-fork psycopg pool bounded by
`worker_child_pool_min_size..worker_child_pool_max_size`. Direct Postgres
budget for one worker is approximately:

```text
worker_pool_size + worker_max_overflow
  + 2 listener/session connections
  + processes * worker_child_pool_max_size
  + task-code database connections
```

Child processes are warmed before the parent opens long-lived database sockets,
but child DB connections are lazy by default (`worker_child_pool_min_size=0`).
Replacement executors created after startup use a non-inheriting process start
method so live parent database sockets are not forked into new children.

### Child process recycling

Two knobs recycle executor children to bound memory. Both force the `spawn`
start method (the stdlib count budget and the memory feature are incompatible
with `fork`), so children re-import the app — higher baseline RSS and slower
child startup on Linux. Recycling is per-child and staggered: each child has its
own counter/threshold and is replaced individually, never in lockstep.

**Count — `WorkerConfig.max_tasks_per_child`** (CLI `--max-tasks-per-child`,
default `100`): recycle each child after N tasks, returning retained memory
(allocator high-water, C-extension caches, leaks) to the OS.

- `100` (default): recycle every 100 tasks. No universal value — raise for
  high-throughput / connection-constrained apps (each recycle rebuilds the
  child DB pool), lower for memory-heavy tasks. Size against
  `children_memory_mb` (below).
- `0`: disabled — children live for the worker's lifetime, uses `fork` on Linux
  (unless memory recycling is on).
- `>= 2` required (`1` rejected: warmup consumes one executor call).

**Memory — `WorkerConfig.max_memory_per_child_mb`** (CLI
`--max-memory-per-child-mb`, default off/`None`): recycle a child once its own
RSS is at or above N MB after a real task finishes. The child samples RSS and
exits cleanly via the stdlib `exit_pid` marker; the pool replaces only that
child (clean replacement path, never `BrokenProcessPool`).

- **CPython-only** — built on private `ProcessPoolExecutor` internals, asserted
  at startup; the worker refuses to start on other implementations or untested
  CPython minors.
- Retention guardrail, **not** a mid-task sandbox: the check is post-task, so a
  task can still exceed the threshold (or OOM) while running.
- Startup **baseline guard**: each child samples its settled baseline RSS (after
  `gc.collect()`); the worker hard-fails if the threshold is at or below the
  warmed baseline (the app does not fit the per-child budget) and warns within
  80% of it.
- Positive int only — no fixed floor, the baseline guard is the real floor.

**Using both (OR semantics).** When both are set, a child exits when *either*
limit is reached. Recommended default for memory-quota deployments (containers,
PaaS dynos): memory as the primary guard, count as a high backstop for slow
leaks / connection churn that do not surface as RSS in time, tuned from
`children_memory_mb`:

```bash
horsies worker app.instance:app --processes=6 --max-tasks-per-child=500 --max-memory-per-child-mb=180
```

Memory-only — disable count explicitly:

```bash
horsies worker app.instance:app --processes=6 --max-tasks-per-child=0 --max-memory-per-child-mb=180
```

Sizing: the per-child peak is `threshold + task_peak` (RSS is sampled after the
task that crosses the threshold, so the child holds the threshold plus that
task's working set), and children peak together. Budget `(threshold + task_peak)
× processes + parent + headroom < quota`; tune from `children_memory_mb`.
Example — 1024 MB quota, 4 procs, ~80 MB task: threshold=300 → ~1249 MB peak
(over quota); threshold=200 → ~719 MB peak (within quota).

## QueueMode

```python
class QueueMode(Enum):
    DEFAULT = 'default'
    CUSTOM = 'custom'
```

### DEFAULT

- Single `"default"` queue.
- No `queue_name` on `@app.task(...)` — passing one raises HRS-103.

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
| `max_concurrency` | `int \| None` | `5` | `>= 0` | Max simultaneous RUNNING tasks for this queue. `None` = uncapped (omitted from the worker's per-queue counting entirely); `0` = pause claiming; negative rejected |

Lower priority number = claimed first. `cluster_wide_cap` still applies as an upper bound.

## PayloadPolicy

Size guardrail for serialized payloads, checked as one integer comparison
against the already-serialized JSON string (no extra serialization pass).

| Field | Type | Default | Range | Description |
|---|---|---|---|---|
| `warn_bytes` | `int \| None` | `1_048_576` (1 MiB) | >=1; None disables | Warn when a serialized payload exceeds this size; rate-limited to once per (task_name, kind) per process |
| `reject_bytes` | `int \| None` | `None` (off) | >=1; None disables | Reject an enqueue whose serialized kwargs exceed this size before anything is written |

Coverage: `.send()`/`.schedule()` kwargs warn and reject
(`Err(PAYLOAD_TOO_LARGE)`); scheduled fires warn and reject (slot fails
via the schedule's enqueue-failure logging); running-flow workflow-node
enqueues (including `args_from` injection) warn only; task results warn
only — results are never rejected. Subworkflow child-start kwargs are not
yet checked.

```python
from horsies import PayloadPolicy

AppConfig(
    broker=broker,
    payload=PayloadPolicy(warn_bytes=1_048_576, reject_bytes=33_554_432),
)
```

## RecoveryConfig

Controls stale task detection, automatic recovery, and data retention.

| Field | Type | Default | Range | Description |
|---|---|---|---|---|
| `auto_requeue_stale_claimed` | `bool` | `True` | — | Requeue tasks stuck in CLAIMED |
| `claimed_stale_threshold_ms` | `int` | `120_000` | 1s–1hr | Ms before CLAIMED task is stale |
| `auto_terminate_orphaned_workflow_tasks` | `bool` | `True` | — | Cancel CLAIMED workflow tasks with no live workflow_task linkage (orphans) instead of requeuing forever |
| `auto_fail_stale_running` | `bool` | `True` | — | Fail tasks stuck in RUNNING |
| `running_stale_threshold_ms` | `int` | `300_000` | 1s–2hr | Ms before RUNNING task is stale |
| `finalizing_stale_threshold_ms` | `int` | `300_000` | 1s–2hr | Ms a completed child may remain finalizing before recovery |
| `crashed_worker_recovery_grace_ms` | `int` | `10_000` | 0–1hr | Grace before recovering a terminal workflow task whose progression was not applied; `0` disables; not heartbeat-coupled |
| `check_interval_ms` | `int` | `30_000` | 1s–10min | Reaper poll cadence |
| `runner_heartbeat_interval_ms` | `int` | `30_000` | 1s–2min | Heartbeat from running task process |
| `claimer_heartbeat_interval_ms` | `int` | `30_000` | 1s–2min | Heartbeat for CLAIMED tasks |
| `worker_state_snapshot_interval_ms` | `int` | `30_000` | 1s–5min | How often each worker inserts a monitoring snapshot row into `horsies_worker_states` |
| `phase2_quarantine_after_attempts` | `int` | `25` | 3–1000 | Recovery passes an unresolvable workflow-progression row may retain before its evidence moves to the quarantine table and discovery stops retrying it |
| `paused_workflow_auto_cancel_after` | `timedelta \| None` | `None` | positive duration | Age past which a PAUSED workflow is expired by policy (`WorkflowStatus.EXPIRED`); `None` disables the sweep |
| `worker_state_retention_hours` | `int \| None` | `168` (7d) | 1–8760; None disables | Prune old worker_state rows |
| `terminal_record_retention_hours` | `int \| None` | `720` (30d) | 1–43800; None disables | Prune terminal **workflow** rows (`horsies_workflows`, `horsies_workflow_tasks`) |
| `history_leaf_horizon_days` | `int` | `3` | 2–14 | Complete future daily history partitions kept ahead of writes; 2 is the coverage-health red line |
| `heartbeat_leaf_horizon_hours` | `int` | `6` | 2–48 | Complete future hourly heartbeat partitions kept ahead of writes |
| `partition_maintenance_interval_s` | `int` | `900` | 60–3600 | Seconds between coverage-ensure passes |
| `retention_sweep_interval_s` | `int` | `300` (5 min) | 30s–24h | Seconds between retention sweep passes |
| `retention_delete_batch_size` | `int` | `500` | 50–10000 | Rows per workflow-retention DELETE batch; each batch commits independently |

**Removed in 0.5.0 — setting either fails config construction naming the
successor:** `heartbeat_retention_hours` (heartbeats live in hourly partitions
that drop whole) and `queue_terminal_record_retention_hours` (terminal task
records age by the retention class assigned at enqueue).

**Terminal task records are not covered by any of these fields.** They move to
the `horsies_task_history` archive at terminalization and age by the retention
class assigned at enqueue — the default is the immutable 30-day class, and
only an explicit `None` keeps a record forever. Whole partitions are dropped;
rows are never deleted individually.

Task records and their workflow record age **independently**: each task record
ages by its own class from its own terminal instant, and the workflow row ages
by `terminal_record_retention_hours`. Deleting a workflow also removes any
unconsumed progression evidence still waiting in its outbox — retention is
never held up by a stalled consumer.

### Constraints (HRS-204)

- `running_stale_threshold_ms >= runner_heartbeat_interval_ms * 2`
- `finalizing_stale_threshold_ms >= runner_heartbeat_interval_ms * 2`
- `claimed_stale_threshold_ms >= claimer_heartbeat_interval_ms * 2`

The 2x factor ensures a task can miss one full heartbeat cycle without being incorrectly marked stale.

### What the reaper does

Runs on `check_interval_ms` cadence:
1. CLAIMED tasks without heartbeat for `claimed_stale_threshold_ms` → requeued to PENDING (if enabled).
2. RUNNING tasks without heartbeat for `running_stale_threshold_ms` → retry on `WORKER_CRASHED` policy or mark FAILED (if enabled). Recent `finalizing_at` and live parent worker state suppress this recovery.
3. Retention pruning every `retention_sweep_interval_s` seconds (default 5 min): deletes old heartbeat, worker_state, and terminal rows based on retention settings; a deleted task's `task_attempts` history is purged in the same statement. Deletes run in `retention_delete_batch_size`-row batches (default 500, one transaction each) under a 60s per-pass budget; a larger backlog drains across consecutive passes.

**CPU/GIL-heavy tasks:** Increase `running_stale_threshold_ms`. GIL-bound tasks may not send heartbeats at the configured interval. Rule of thumb: >= 3–5x worst-case heartbeat gap. The stale threshold is based on missing runner heartbeats, not total task duration.

## WorkerResilienceConfig

Controls worker retry behavior on transient DB failures.

| Field | Type | Default | Range | Description |
|---|---|---|---|---|
| `db_retry_initial_ms` | `int` | `500` | 100ms–60s | Initial backoff |
| `db_retry_max_ms` | `int` | `30_000` | 500ms–5min | Max backoff cap |
| `db_retry_max_attempts` | `int` | `0` | 0–10000 | Max retries; 0 = infinite |
| `notify_poll_interval_ms` | `int` | `5_000` | 1s–5min | Fallback poll when NOTIFY is silent |

### Constraint (HRS-208)

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

All `TaskSchedule.name` values must be unique (HRS-205).

### TaskSchedule

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Unique schedule identifier |
| `task_name` | `str` | required | Must match a registered task |
| `pattern` | `SchedulePattern` | required | When to run |
| `kwargs` | `dict` | `{}` | Keyword args to task — schedules are kwargs-only (`extra='forbid'`; `args`/unknown fields rejected at construction) |
| `queue_name` | `str \| None` | `None` | Target queue |
| `enabled` | `bool` | `True` | Per-schedule on/off |
| `timezone` | `str` | `"UTC"` | IANA timezone (validated at scheduler startup; invalid names fail boot) |
| `catch_up_missed` | `bool` | `False` | `True`: replay missed runs on restart. `False`: drop missed runs, fire only the most recent due slot |
| `max_catch_up_runs` | `int` | `100` | 1–10000; max runs per tick during catch-up |

### Schedule Patterns

Use the smallest pattern that describes the schedule. `HourlySchedule`,
`DailySchedule`, `WeeklySchedule`, and `MonthlySchedule` are simpler when they
fit. Use `CronSchedule` for wall-clock aligned field combinations such as
"every 4 hours at minute 15".

**IntervalSchedule** — run every N time units:

```python
IntervalSchedule(seconds=30)
IntervalSchedule(hours=1, minutes=30)  # total = 90 minutes
```

Fields: `seconds`, `minutes`, `hours`, `days` — all `int | None`. At least one must be set (HRS-205).

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

`days` must have no duplicates (HRS-205).

**MonthlySchedule** — fixed day of month:

```python
MonthlySchedule(day=15, time=time(15, 0))  # 15th at 15:00
```

If `day` > days in month (e.g., day=31 in Feb), that month is skipped.

**CronSchedule** — typed cron-style schedule:

```python
from horsies import CronEvery, CronSchedule, CronStep, CronValues, EveryDay

# Equivalent shape to "15 */4 * * *": 00:15, 04:15, 08:15, ...
CronSchedule(
    minute=[CronValues(values=[15])],
    hour=[CronStep(step=4)],
    month=[CronEvery()],
    day=EveryDay(),
)
```

`CronSchedule` fires at second `:00` and models the five cron fields as
typed objects instead of accepting a cron expression string:

| Field | Type | Domain |
|---|---|---|
| `minute` | `list[CronNumericTerm]` | 0-59 |
| `hour` | `list[CronNumericTerm]` | 0-23 |
| `month` | `list[CronMonthTerm]` | `Month` enum |
| `day` | `DaySelector` | day-of-month / day-of-week choice |

Numeric terms (`minute`, `hour`, day-of-month):

| Term | Meaning |
|---|---|
| `CronEvery()` | every value in the field |
| `CronStep(step=n)` | every nth value, anchored at the domain start |
| `CronValues(values=[...])` | explicit integer values |
| `CronRange(start=a, end=b, step=1)` | inclusive integer range |

Enum terms (`month`, day-of-week):

| Term | Meaning |
|---|---|
| `CronEvery()` | every enum value |
| `CronEnumValues[Month \| Weekday](values=[...])` | explicit enum values |
| `CronEnumRange[Month \| Weekday](start=..., end=..., step=1)` | inclusive enum range by canonical order |
| `CronEnumStep[Month \| Weekday](step=n)` | every nth enum value, anchored at the domain start |

`CronStep`/`CronEnumStep` are anchored at the field's first value. For example,
`CronStep(step=4)` on `hour` means 0, 4, 8, 12, 16, 20. Steps larger than the
field span are rejected as likely mistakes: minute 59, hour 23, day-of-month
30, month 11, weekday 6. Use `CronValues`/`CronEnumValues` for a single value.

`CronRange` and `CronEnumRange` do not wrap. `FRIDAY -> MONDAY` and
`DECEMBER -> FEBRUARY` are invalid; split them into multiple terms.

`day` is explicit so cron's ambiguous "day-of-month OR day-of-week" behavior
cannot happen by accident:

| Selector | Meaning |
|---|---|
| `EveryDay()` | no day restriction |
| `ByMonthDay(day_of_month=[...])` | match day-of-month only |
| `ByWeekday(day_of_week=[...])` | match weekday only |
| `EitherDay(day_of_month=[...], day_of_week=[...])` | match either side (OR) |
| `BothDays(day_of_month=[...], day_of_week=[...])` | match both sides (AND) |

Example: the 13th or Friday vs Friday the 13th:

```python
from horsies import CronEnumValues, CronValues, EitherDay, BothDays, Weekday

EitherDay(
    day_of_month=[CronValues(values=[13])],
    day_of_week=[CronEnumValues[Weekday](values=[Weekday.FRIDAY])],
)

BothDays(
    day_of_month=[CronValues(values=[13])],
    day_of_week=[CronEnumValues[Weekday](values=[Weekday.FRIDAY])],
)
```

Construction-time validation rejects out-of-domain values, empty term lists,
wrap-around ranges, invalid step spans, and unsatisfiable month/day combinations
such as February 30. `EitherDay` can still contain an impossible
day-of-month because the weekday side can satisfy the schedule; `BothDays`
cannot.

### Weekday and Month

```python
class Weekday(str, Enum):
    MONDAY = 'monday'
    TUESDAY = 'tuesday'
    WEDNESDAY = 'wednesday'
    THURSDAY = 'thursday'
    FRIDAY = 'friday'
    SATURDAY = 'saturday'
    SUNDAY = 'sunday'

class Month(str, Enum):
    JANUARY = 'january'
    FEBRUARY = 'february'
    MARCH = 'march'
    APRIL = 'april'
    MAY = 'may'
    JUNE = 'june'
    JULY = 'july'
    AUGUST = 'august'
    SEPTEMBER = 'september'
    OCTOBER = 'october'
    NOVEMBER = 'november'
    DECEMBER = 'december'
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
| 1 — Config | `AppConfig` Pydantic validators (implicit — already validated at construction) | HRS-200–HRS-209 |
| 2 — Imports | Import each discovered module | ImportError, HRS-210 |
| 3 — Workflows | `WorkflowSpec` DAG validation during imports | HRS-001–HRS-031 |
| 3.1 — Builders | Execute `@app.workflow_builder` functions | HRS-027–HRS-030 |
| 3.2 — Undecorated | Scan for functions returning WorkflowSpec without decorator | HRS-030 |
| 3.5 — Policies | Re-validate exception_mapper after imports | HRS-209 |
| 4 — Broker | `SELECT 1` against PostgreSQL (`--live` only) | HRS-211, Connection error |

**Guarantee model:**
- **Strong:** `@app.workflow_builder` functions are fully executed and validated.
- **Best-effort:** Undecorated builder detection (HRS-030) only scans directly discovered modules.

Worker and scheduler also run `app.check(live=False)` at startup.

## CLI Commands

### Module locator format

```
myapp.config:app      # dotted module + attribute (recommended)
myapp.config          # dotted module, auto-discover Horsies instance
app/config.py:app     # file path + attribute
app/config.py         # file path, auto-discover
```

Auto-discovery: exactly one `Horsies` instance → use it. Zero or multiple → HRS-206.

### `horsies worker`

```bash
horsies worker <module> [--processes N] [--loglevel LEVEL] [--max-claim-batch N] [--max-claim-per-worker N] [--max-tasks-per-child N] [--max-memory-per-child-mb N]
```

| Flag | Default | Description |
|---|---|---|
| `--processes` | `1` | Worker process count |
| `--loglevel` | `INFO` | Log level |
| `--max-claim-batch` | `2` | Max tasks per queue per claim pass |
| `--max-claim-per-worker` | `0` | Max claimed per worker; 0 = auto |
| `--max-tasks-per-child` | `100` | Recycle a child after N tasks (`>= 2`, or `0` to disable); forces `spawn`. See [Child process recycling](#child-process-recycling) |
| `--max-memory-per-child-mb` | _(off)_ | Recycle a child once its RSS reaches N MB (CPython-only, forces `spawn`); fails startup if N is at/below the warmed baseline. See [Child process recycling](#child-process-recycling) |

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

### `horsies web`

```bash
horsies web <module> [--host 127.0.0.1] [--port 8600] [--auth none|trusted-header] [--enable-actions]
horsies web --database-url "postgresql+psycopg://..." [--session-database-url URL]
```

Serves the monitoring dashboard (requires the `web` extra). Fail-closed:
non-loopback hosts require `--auth trusted-header`; actions are off unless
`--enable-actions`. Never executes DDL. See `monitoring.md` for the full
reference including the registry-less capability boundary.

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
    # Payload
    PayloadPolicy,
    # Recovery / Resilience
    RecoveryConfig, WorkerResilienceConfig,
    # Scheduling
    ScheduleConfig, TaskSchedule,
    SchedulePattern,
    Weekday, Month, IntervalSchedule, HourlySchedule,
    DailySchedule, WeeklySchedule, MonthlySchedule,
    CronSchedule, CronEvery, CronStep, CronValues, CronRange,
    CronEnumValues, CronEnumRange, CronEnumStep,
    DaySelector, EveryDay, ByMonthDay, ByWeekday, EitherDay, BothDays,
    # Exception mapper
    ExceptionMapper,
    # Retry
    RetryPolicy,
)
```

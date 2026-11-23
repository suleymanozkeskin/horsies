# horsies — Monitoring

Detailed reference for the typed monitoring query API, task actions, and the
web dashboard. The dashboard requires the `web` extra
(`pip install horsies[web]`); `horsies.monitoring` is core and needs no extra.

## Two Ways to Run the Dashboard

### Mounted — inside a host FastAPI application

```python
from fastapi import FastAPI
from horsies.web import AllowAll, create_monitoring_app

from myapp.tasks import app as horsies_app

api = FastAPI()
api.mount(
    "/monitoring",
    create_monitoring_app(horsies_app, auth_policy=AllowAll()),
)
```

- `auth_policy` is keyword-only with **no default** — every deployment states
  its policy. `AllowAll()` means *this mount is already guarded by the host
  application*, not "no authentication".
- Mounts at any path; the served SPA detects its mount point per request.
- Uses the host app's broker as configured — the monitoring layer constructs
  nothing in mounted mode.

### Standalone — `horsies web`

```bash
horsies web myapp.tasks:app                    # app path: full feature set
horsies web --database-url "postgresql://..."   # registry-less
```

| Flag | Default | Notes |
|---|---|---|
| `--host` | `127.0.0.1` | Non-loopback REFUSES to start without `--auth trusted-header` |
| `--port` | `8600` | |
| `--auth` | `none` | `none` (loopback only) or `trusted-header` |
| `--trusted-header` | `X-Forwarded-User` | The reverse proxy MUST strip/set this header itself, or the mode is spoofable |
| `--enable-actions` | off | Off → view-only policy; on → actions allowed |
| `--trace-link` | — | `http`/`https` URL with one `{trace_id}`: the task detail links the trace id; a bad template exits 2 (`MonitoringUIConfig(trace_link=TraceLinkTemplate(template=...))` when mounting; `HRS-221`) |
| `--session-database-url` | — | Direct URL for LISTEN when `--database-url` is transaction-pooled |
| `--pgbouncer-transaction-mode` | off | Pair with `--session-database-url` |

The app-path form runs the same startup validation as `horsies worker` /
`horsies scheduler` (imports discovered task modules; a failing check refuses
to serve). The `--database-url` form is registry-less by design:

| Capability | app path | `--database-url` |
|---|---|---|
| All reads, live updates | yes | yes |
| Task cancel | yes | yes |
| Workflow pause / cancel | yes | yes |
| Workflow **resume** when a pending node uses `args_from` | yes | **no** — encoding an upstream result into a fresh task row needs the source task's registered return type |

## Worker Metrics Endpoint — `--metrics-listen`

- `horsies worker <app> --profile <name> --metrics-listen HOST:PORT`
  (`[IPV6]:PORT`; host 1 to 253 bytes, port 1 to 65535; no default, no
  config field) serves, on a thread of its own:
  - `GET /metrics`: the worker metrics, Prometheus text format 0.0.4 (no
    OpenMetrics);
  - `GET /livez`: 503 only when the claim loop is past the deadline of its
    next turn (its wait bound plus 30 s); 200 during the start and the
    drain;
  - `GET /readyz`: 200 when the pools have no failure, a child serves and
    the liveness lock is held; 503 during the start and the stop.
- A bad value exits 2 before the start. A bind failure never stops the
  worker: the endpoint binds again with a 1 s to 60 s backoff; the probes
  answer once it binds; `horsies_worker_states.telemetry` shows `down`.
- Bounds: 16 connections, 5 s to read a request head, 8 KiB of request
  (431 above).
- Labels: `task` = the registered tasks (`(unregistered)` else), `queue` =
  the app's queues (`(unknown)` else), `error_code` = `""`, the built-in
  codes and the task's declared codes (`undeclared` else). A name that
  cannot be a label value (empty, the fallback text, past 100,000 names) is
  folded into the fallback, with one WARNING line at the start.
- At most 10,000 series per metric; past it, the sample is dropped and
  counted in `horsies_telemetry_series_dropped_total{metric}`.
- See website docs `observability/metrics` and
  `observability/metric-reference`.

## Fleet Metrics — `horsies metrics` and `GET /metrics`

```bash
horsies metrics myapp.tasks:app --listen 0.0.0.0:9465   # --listen is required, no default
```

- Serves the fleet gauges of the app's database at `GET /metrics`
  (Prometheus text format 0.0.4 only; no OpenMetrics): live tasks by queue,
  class and status; `horsies_queue_pending_more_than` past 1,000 `PENDING`
  rows of a queue and class; the age of the oldest due `PENDING` task; live
  workers, seats, seats in use, unserved classes, workers with telemetry
  down; workflows; time since the last run of each schedule; the schema
  version; and the time, errors and `failed` state of each query.
- Reads at start, then at most once per 15 s; a scrape never reads. Each
  statement has a 2 s limit. A failed query leaves its gauges with no
  series and sets `horsies_exporter_collect_failed{query} 1`.
- Label values: the queues and schedules of `app.config`; another name is
  `(unknown)`. Run one collector per database, or aggregate with `max`.
- Exit codes: 2 for an address that does not parse; 0 at `SIGTERM` or
  `SIGINT`; 1 when the app does not load, the broker does not build, or the
  address does not bind.
- A mounted `create_monitoring_app` and `horsies web` also serve
  `GET /metrics`, behind `can_view`; the request reads when the last read
  is 15 s old or more.
- `deploy/grafana/horsies-dashboard.json` and
  `deploy/prometheus/horsies-alerts.yaml` in the repository read these
  series. See website docs `observability/fleet-collector` and
  `observability/dashboards-and-alerts`.

## Authorization

<!-- check: skip partial -- uses names that the page does not define -->
```python
class MonitoringAuthPolicy(Protocol):
    async def can_view(self, request: Request) -> bool: ...
    async def can_act(self, request: Request) -> bool: ...
```

Built-ins: `AllowAll`, `ViewOnly`, `TrustedHeader(header_name, allow_actions=...)`.
Every `/api` route requires `can_view` (false → 403). Mutating routes also
require `can_act` **and** the header `X-Horsies-Intent: action` (CSRF guard —
not authentication). Implement the protocol to delegate to a host session/RBAC.

## Schema Compatibility — the No-DDL Rule

The monitoring layer NEVER executes DDL. Brokers it constructs skip schema
migrations; instead the highest successful version in `horsies_migrations` is
probed (cached 60 s):

<!-- members: SchemaState -->
| State | Meaning | Reads | Actions |
|---|---|---|---|
| `MATCH` | stored == expected | served | allowed (policy permitting) |
| `MISMATCH` | version differs | served | 409 `SCHEMA_INCOMPATIBLE`, server-side |
| `ABSENT` | a successful probe found no horsies schema | — | refused; the tool never initializes a database |
| `ZERO_X` | a successful probe found a horsies 0.x schema (`horsies_schema_version` without `horsies_migrations`, or a version below 1000) | — | refused; the remedy is a new database, not a worker start |
| `UNKNOWN` | the probe has never succeeded (unreachable DB) | — | refused (`SCHEMA_UNKNOWN`); never conflated with ABSENT |

`GET /api/meta` reports `schema_version`, `expected_schema_version`,
`schema_compatible`, `schema_state` (the `SchemaState` value), `actions_enabled`, `can_act`, `actions_disabled_reason`.

## Query API — `horsies.monitoring`

All functions are async, take `broker: PostgresBroker` first, and return
`MonitoringResult[T] = Result[T, MonitoringQueryError]`. **An absent row is
`Ok(None)`, not an error;** `get_task_detail` returns
`Ok(TaskDetailNotFound(task_id, reason))` instead. `MonitoringQueryError` is a union of frozen
variants, one per `MonitoringQueryErrorCode` member; match on the variant.
Every variant has `code`, `message` and `retryable`. A query is a read, so no
error changes stored data. HTTP is the status of the web read routes.

<!-- members: MonitoringQueryErrorCode -->
| Code | Variant | Fields | `retryable` | HTTP | When |
|---|---|---|---|---|---|
| `DB_OPERATION_FAILED` | `MonitoringQueryDbOperationFailed` | `exception` (the core's class of the cause: `CoreServerError`, `CoreConnectionError`, ...; or `TooManyPendingCalls`, `CoreCallTimedOut`, `CoreStartFailed`) | the core's classification | 503 | The database operation failed |
| `HISTORY_CONTRACT_VIOLATION` | `MonitoringQueryHistoryContractViolation` | `exception` (the `HistoryContractError`) | `False` | 500 | `task_stats`, `list_tasks`, `get_task_detail`, `get_workflow_run` or `get_workflow_node` read history data that does not decode, for example a history status that is not a `TaskStatus` member; or the detail read found the task live inside its own snapshot, where the live read had missed it (the staged function and the live read disagree); a repeat gives the same result until the stored row or the server changes |

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
task_stats(broker, *, window, task_names, queues, workers, error_codes, error_categories, retried_only)
task_facets(broker, *, window, statuses, retried_only)          # + error_categories scoping of the code list
task_breakdown(broker, *, window, group_by, ..., limit)         # limit: BreakdownLimit; group_by: 'worker' | 'task_name' | 'queue'
list_tasks(broker, *, window, statuses, ..., sort_by, sort_dir, page)  # page: TaskPage
get_task_detail(broker, task_id)                        # task_id: uuid.UUID; Ok(TaskDetailNotFound) when absent; leaf.trace_id: 32 hex digits or None
list_workflow_names(broker)                             # at most 10,000 names; a cut is logged as a warning
list_workflow_runs(broker, *, name, status, limit)      # limit: WorkflowRunsLimit
get_workflow_run(broker, workflow_id)                   # full DAG: nodes, edges, failed_indices, trace_id of the start, timing
get_workflow_node(broker, workflow_id, task_index)
list_schedules(broker)                                  # next run, then name; at most 10,000; a cut is logged
```

- `task_breakdown` groups: each `GroupRow.group` is `BreakdownValueGroup(value)`
  or, for `group_by='worker'` only, `BreakdownUnclaimedGroup()` (tasks no worker
  claimed). A worker whose id is `'unknown'` is a value group of its own.
  `Breakdown.total` is a `BreakdownCounts` over every group.
- Filter lists: OR within a dimension, AND across dimensions; empty = no filter.
- `error_categories` (`ErrorCategory`: OPERATIONAL / CONTRACT / RETRIEVAL /
  OUTCOME / DOMAIN) expands server-side from the library's own code registry —
  DOMAIN is the complement of every built-in code.
- Worker/health surfaces reuse the app APIs: `list_worker_states_async`,
  `ping_workers_async`, `ping_database_async`, `get_worker_state_history_async`,
  `get_class_report_async`.
- A snapshot's `slot_health` is `AllSlotsServing()` or `SlotsDown(slots)` (the
  worker is degraded). Each `DownSlot` has `slot`, `cause`
  (`ResourceShortage(errno_name)` or `ThreadStartRefused()`), `detail` (at
  most 2400 characters), `down_since` and `failed_boots`. A slot is down
  when its child start failed for a cause that can clear; it takes no task and
  boots again after a growing wait. Match both variants.
- A snapshot's `class_capacity` is `ClassBudgets(budgets)`. Each
  `ClassBudget` has `budget` (an `Execution`), `capacity` (children or seats
  in service) and `serves` (the classes whose rows it claims). Empty
  `budgets`: the worker serves no class.
- A snapshot's `queue_priorities`, `queue_max_concurrency` and
  `recovery_config` are `dict`, empty when the worker had none set.
- Fleet check: `app.get_class_coverage_async()` →
  `ClassCoverage(checked_at, live_workers, streams)`. One `StreamCoverage`
  per (queue, class) with PENDING rows; `state` is
  `StreamServed(workers, capacity)`, `StreamWithoutCapacity(workers)` (every
  serving child or seat is down) or `StreamUnserved()` (no live worker has
  the queue and a budget that serves the class). Live = latest snapshot at
  most `WORKER_STATE_LIVE_WINDOW_S` (120) s old;
  `worker_state_snapshot_interval_ms` is at most half of it (60,000 ms), so a
  running worker stays live when a snapshot write is late by up to half the
  window. Each reaper pass runs it and
  logs unserved at error, without-capacity at warning, once per 5 min per
  (queue, class) per worker. Fix unserved `ISOLATED` with a worker that has
  isolated children (shared rows go back to the queue as `ISOLATED` when
  their task's class changed); a shared class with children of that class or
  isolated children.
- `app.get_class_report_async()` → `ClassReport(checked_at, streams,
  workers)`. `PendingStream.wait` is `OldestWaiting(enqueued_at, wait)` or
  `NoneDue(next_due_at)`; `WorkerClassLoad.budgets` holds `BudgetLoad(budget,
  capacity, serves, in_flight)` with `.free` = `max(0, capacity -
  in_flight)`. Both reads fail with `MONITORING_QUERY_FAILED`
  (`retryable=False`) when a live snapshot has an unreadable
  `class_capacity`, naming the worker. Web: `GET /api/workers/classes`.
- A snapshot's `recovery_config` embeds three health payloads:
  `partition_coverage` (coverage-ensure outcome; fails before fewer than two
  future partitions remain), `partition_pruning`
  (`{"finalized", "detached", "dropped", "refusals", "errors"}` — the last
  two are reason-carrying lists, not counts; a refused partition is retried
  every pass), `phase2_recovery` (pass summary with quarantined counts,
  `raised_counted` and refusals), and `workflow_recovery` (`recovered`,
  `errors`, `over_failure_limit`, and per case `failures_counted` and
  `limit_reached`).

### Reads span live and history

A task that reaches a terminal status leaves `horsies_tasks` for the
`horsies_task_history` archive in the same transaction. Detail, result, and
attempt-history reads resolve on **both** sides, so a moved task still
answers. `get_task_detail` and `get_workflow_node` each send one statement
(`horsies_monitoring_task_detail`, `horsies_monitoring_workflow_node`), so
the live read and the history read see the same rows.

`TaskDetailNotFound.reason` is a `TaskAbsenceReason`. The web route answers
404 for each reason. `get_task_detail` derives it from the detached history
leaves: a UUIDv7 id born before the latest upper bound of a detached leaf of a
period class (plus 5 s) predates retained history; any other UUIDv7 id is
within it. A path id that is not a UUID answers 400 with
`{"code": "INVALID_INPUT", "detail": ...}` before any read.

<!-- members: TaskAbsenceReason -->
| Reason | Meaning |
|---|---|
| `PREDATES_RETAINED_HISTORY` | The id's UUIDv7 birth is before the end of the latest history period that retention removed (plus 5 s); retention can have removed the task |
| `WITHIN_RETAINED_HISTORY` | The id's UUIDv7 birth is later: the task did not exist, or its history partition was removed out of band |
| `NOT_CLASSIFIED` | The id is not a UUIDv7, or the database has no staged history read |

List and aggregate reads are **window-scoped** and take a
`MonitoringWindow`. Only `resolve_monitoring_window(since=..., until=...)`
builds one, and it returns `MonitoringWindow | WindowRefused`: the default
is the last 24 hours (`MONITORING_WINDOW_DEFAULT`), the maximum span is 30
days (`MONITORING_WINDOW_MAX`), and a request over the maximum is refused
with the bound named — never silently clamped. Bounds must be
timezone-aware and increasing. The window carries the bound, so a read
never checks it again; building a `MonitoringWindow` directly with other
bounds is a defect and raises `AssertionError`.

`TaskListPage.total` follows a stated contract: **exact when any filter is
active, a planner estimate on the unfiltered view.** Do not present an
unfiltered total as an exact count.

`list_tasks` orders by the sort key in the database collation (NULL keys
last in both directions), then a live task before an ended task with the
same value, then the task id ascending. The order is total, so a page does
not reorder between polls.

### Phase timing (the lifecycle record)

The lifecycle record keeps the phase times of each attempt row and the
events of each workflow run (migration 1001). `get_workflow_run` composes the
timing in the core from the rows of one statement; no statement runs per
node or per task. Each model holds times, not durations: subtract the two
times of a segment. Every union is told apart by `kind`.

- `TaskAttemptInfo.phases`: `PhasesNotCapturedInfo()` (a row written before
  the record, or a version-1 archive in history) or
  `PhasesCapturedInfo(enqueued_at, claimed_at, claims, finalizing,
  recorded_by)`. `claims` is `ClaimsCountedInfo(count, first_claimed_at)` or
  `ClaimsNotCountedInfo()`; a count above 1 holds lost leases and released
  claims. `finalizing` is `FinalizingMarkedInfo(finalizing_at)` or
  `FinalizingNotMarkedInfo()`. `recorded_by` is
  `RecordedByWorkerInfo(placement)` (`PlacementPlacedInfo(placed_at)`: the
  claim time plus the measured slot wait, or `PlacementNotObservedInfo()`) or
  `RecordedByRecoveryInfo(last_seen_at)` (stale RUNNING recovery wrote the
  row).
- `WorkflowNodeInfo.timing` is tagged by its coverage label:
  `NodeTimingExactInfo(segments, endpoint)` (every boundary is a recorded
  time), `NodeTimingPartialInfo(reasons, segments, endpoint)` (at least one
  `unknown` segment), `NodeTimingUnavailableInfo(reason)` or
  `NodeTimingInconsistentInfo(conflict)` (the record of the node contradicts
  itself; no segment).
- A `TimingSegmentInfo` has `phase`, `started_at`, `end`
  (`SegmentEndedInfo(at)` or `SegmentOpenInfo()`) and `source`
  (`SourceNodeInfo()`, `SourceTaskInfo(generation, task_id)` or
  `SourceAttemptInfo(generation, task_id, attempt)`). `generation` counts the
  tasks of the node from 1: a pause abandon and a resume give it a new task.
- Phases: `pending`, `ready`, `enqueued`, `retry_wait`,
  `claimed` (`PhaseClaimedInfo(claim)`: `ClaimEarlierInfo(count)` for the
  claims before the claim that holds the attempt, `ClaimRecordedInfo(placement)`
  for the claim of the attempt row, `ClaimNotRecordedInfo()` for an attempt
  with no row), `running`, `finalizing`, `advancing` (the task is in history
  and the workflow has not applied the node), `unknown`
  (`PhaseUnknownInfo(reason)`).
- `endpoint` is `EndpointTerminalInfo(status, at)` or
  `EndpointNotTerminalInfo()`. It is a marker, not a segment: a node that a
  workflow cancel, a failure or the recovery limit ended while its task went
  on keeps the segments of that task.
- A conflict is `ConflictClockOrderInfo(earlier, earlier_at, later,
  later_at)` (two `TimingBoundary` values in the wrong order: a step of the
  database clock), `ConflictMissingRecordInfo(record)` or
  `ConflictUnexpectedEventInfo(event_id, event)`. The core does not reorder
  times and does not guess a duration.
- `WorkflowRunDetail.timing`: `RunTimingUnavailableInfo(as_of,
  captured_since, reason)` (every node is unavailable with the same reason)
  or `RunTimingComposedInfo(as_of, captured_since, coverage,
  range_started_at, range_ended_at, open_segments, pauses)`. `as_of` is the
  database time of the read (an open segment ends there for drawing);
  `captured_since` is the install time of migration 1001. `coverage` is
  `RunCoverageExactInfo()`, `RunCoveragePartialInfo(reasons)` or
  `RunCoverageInconsistentInfo(conflicts)` (`RunConflictNodeInfo(task_index)`,
  `RunConflictPauseInfo(conflict)`). A pause ends `PauseResumedInfo(at)`,
  `PauseRunEndedInfo(at)` or `PauseOpenInfo()`.

<!-- members: UnknownReason -->
| Reason | The `unknown` segment |
|---|---|
| `ATTEMPT_NOT_CAPTURED` | Is an attempt row with no phase record |
| `CLAIMS_NOT_COUNTED` | Is the queue wait of an attempt whose claims were not counted |
| `HISTORY_PRUNED` | Runs to the move of a task whose history row retention removed |
| `WORKER_LOST` | Runs from the last sign of life of a recovered attempt to its recovery |

<!-- members: UnavailableReason -->
| Reason | The run is unavailable because |
|---|---|
| `RUN_BEFORE_CAPTURE` | It began before `captured_since` |
| `TIMING_BOUND_EXCEEDED` | It has more than 100,000 events |

A history class shorter than the workflow retention removes task rows first:
the node then shows `HISTORY_PRUNED`.

## Task Actions — `cancel_task`

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
cancel_task(broker, task_id: uuid.UUID, *, include_running=False) -> Result[TaskCancelled, TaskActionError]
```

- Single-transaction compare-and-set. A committed CANCELLED cannot be
  overwritten by claim, finalize, auto-retry, or reaper paths.
- Cancel eligibility: PENDING, CLAIMED; RUNNING only with
  `include_running=True` — the row flips durably but **the process keeps
  executing**: side effects still happen and no attempt row is recorded for
  that run. There is no cross-process kill.
- Workflow-bound rows are REFUSED (`TASK_IS_WORKFLOW_TASK`) — workflow rows
  are managed only by the workflow primitives (`handle.pause/resume/cancel`).
- There is no manual retry action: a terminal record is immutable, and
  re-execution is a new request through the rerun contract.
- `TaskActionError` is a union of frozen variants, one per
  `TaskActionErrorCode` member; match on the variant. Every variant has
  `code`, `message`, `retryable` and `task_id` (a `uuid.UUID`). HTTP is the
  status of `POST /api/tasks/{task_id}/cancel`. A path id that is not a UUID
  answers 400 with `{"code": "INVALID_INPUT", "detail": ...}` before the
  action runs.

<!-- members: TaskActionErrorCode -->
| Code | Variant | Fields | `retryable` | HTTP | When |
|---|---|---|---|---|---|
| `TASK_NOT_FOUND` | `TaskActionNotFound` | — | `False` | 404 | No live or history row has the id; nothing changed |
| `TASK_NOT_CANCELLABLE` | `TaskActionNotCancellable` | `current_status: TaskStatus` | `False` | 409 | The task is terminal, or `RUNNING` without `include_running=True`; nothing changed |
| `TASK_IS_WORKFLOW_TASK` | `TaskActionWorkflowTask` | `current_status: TaskStatus` | `False` | 400 | The task belongs to a workflow; nothing changed |
| `DB_OPERATION_FAILED` | `TaskActionDbOperationFailed` | `exception` (the core's class of the cause, or the refusal of the core boundary) | the core's classification | 503 | The cancel statement failed: it wrote nothing, or it committed and its reply was lost. A repeated cancel tells which: a committed cancel answers `TASK_NOT_CANCELLABLE` with `CANCELLED` |
| `STORED_DATA_INVALID` | `TaskActionStoredDataInvalid` | `exception` (the `OutcomeDecodeError`) | `False` | 500 | Stored data does not parse: the cancel outcome row does not decode, for example a history status that is not a `TaskStatus` member; nothing changed |

`cancel_task` raises `RuntimeError` when the cancel outcome contradicts the
cancel function (a lost claim, or no observed status). That is a defect, not
an outcome.

## Live Updates

The web app subscribes on the process listener of its broker (no connection
of its own; subscriptions start at the first stream) to `horsies_task_status`,
`horsies_workflow_status`, and `horsies_worker_state`, coalesces notifies
(250 ms), and fans out over `GET /api/events` (SSE). After a listener
reconnect each stream gets one event with no ids per topic; a broker with
`assume_initialized=True` has no listener, and its stream sends `degraded`.
Events are invalidation signals — `{"topic", "ids"}` — never data. Client
polling is a fallback that activates only while the stream is down; the
schedules table always polls (its table has no trigger). A task's start
(RUNNING) sends no event; its creation, other status changes and its end do.

## Deployment Footprint

- Per web process: the broker pool (defaults `pool_size=5 + max_overflow=10`)
  plus the process listener connection of the session URL — **flat
  regardless of viewer count**; excess requests queue on the pool.
- Public view-only deployments: identical viewers issue identical aggregate
  queries; put a ~1 s shared cache in front (`Cache-Control: max-age=1` via a
  proxy) to make audience size irrelevant to database load.
- Under PgBouncer transaction pooling, provide the session-capable URL for
  LISTEN (`session_database_url` / `--session-database-url`).

## All Public Imports

```python
from horsies.monitoring import (
    # Queries
    task_stats, task_facets, task_breakdown, list_tasks, get_task_detail,
    list_workflow_names, list_workflow_runs, get_workflow_run,
    get_workflow_node, list_schedules,
    # Actions
    cancel_task, TaskCancelled,
    TaskActionError, TaskActionErrorCode,
    TaskActionNotFound, TaskActionNotCancellable, TaskActionWorkflowTask,
    TaskActionDbOperationFailed, TaskActionStoredDataInvalid,
    # Result / errors
    MonitoringResult, MonitoringQueryError, MonitoringQueryErrorCode,
    MonitoringQueryDbOperationFailed, MonitoringQueryHistoryContractViolation,
    # Vocabularies
    ErrorCategory, TaskSortField, TaskGroupBy, SortDirection,
    # Response models (unstable pre-1.0)
    TaskSummary, TaskListPage, TaskDetail, LeafTaskInfo, TaskAttemptInfo,
    StatusCount, FacetValue, ErrorFacet, Facets, GroupRow, Breakdown,
    BreakdownCounts, BreakdownGroup, BreakdownValueGroup, BreakdownUnclaimedGroup,
    WorkflowRunSummary, WorkflowNodeInfo, WorkflowEdge, WorkflowRunDetail,
    WorkflowTaskDetail, WorkerStateInfo, WorkerPingInfo, LivenessReport,
    ScheduleStateInfo, WorkerHistoryPoint, DownSlotInfo, ClassReportInfo,
    PendingStreamInfo, WorkerClassLoadInfo, BudgetLoadInfo, StreamServedInfo,
    StreamNoCapacityInfo, StreamUnservedInfo, StreamWaitingInfo,
    StreamNoneDueInfo,
    # Phase timing (the lifecycle record)
    AttemptPhasesInfo, PhasesCapturedInfo, PhasesNotCapturedInfo,
    AttemptClaimsInfo, ClaimsCountedInfo, ClaimsNotCountedInfo,
    AttemptPlacementInfo, PlacementPlacedInfo, PlacementNotObservedInfo,
    AttemptFinalizingInfo, FinalizingMarkedInfo, FinalizingNotMarkedInfo,
    AttemptRecordedByInfo, RecordedByWorkerInfo, RecordedByRecoveryInfo,
    NodeTimingInfo, NodeTimingExactInfo, NodeTimingPartialInfo,
    NodeTimingUnavailableInfo, NodeTimingInconsistentInfo, TimingSegmentInfo,
    SegmentPhaseInfo, PhasePendingInfo, PhaseReadyInfo, PhaseEnqueuedInfo,
    PhaseRetryWaitInfo, PhaseClaimedInfo, PhaseRunningInfo, PhaseFinalizingInfo,
    PhaseAdvancingInfo, PhaseUnknownInfo, SegmentClaimInfo, ClaimEarlierInfo,
    ClaimRecordedInfo, ClaimNotRecordedInfo, SegmentEndInfo, SegmentEndedInfo,
    SegmentOpenInfo, SegmentSourceInfo, SourceNodeInfo, SourceTaskInfo,
    SourceAttemptInfo, NodeEndpointInfo, EndpointTerminalInfo,
    EndpointNotTerminalInfo, TimingConflictInfo, ConflictClockOrderInfo,
    ConflictMissingRecordInfo, ConflictUnexpectedEventInfo, RunTimingInfo,
    RunTimingUnavailableInfo, RunTimingComposedInfo, RunCoverageInfo,
    RunCoverageExactInfo, RunCoveragePartialInfo, RunCoverageInconsistentInfo,
    RunConflictInfo, RunConflictNodeInfo, RunConflictPauseInfo,
    PauseIntervalInfo, PauseEndInfo, PauseResumedInfo, PauseRunEndedInfo,
    PauseOpenInfo, UnknownReason, UnavailableReason, TimingBoundary,
    MissingRecord, WorkflowEventKind,
    # Page, limit and absence types
    TaskPage, BreakdownLimit, WorkflowRunsLimit,
    TaskDetailNotFound, TaskAbsenceReason, PageRefused,
    # History window
    MonitoringWindow, WindowRefused, resolve_monitoring_window,
    MONITORING_WINDOW_DEFAULT, MONITORING_WINDOW_MAX,
)

from horsies.web import (  # requires horsies[web]
    create_monitoring_app, MonitoringUIConfig,
    AllowAll, ViewOnly, TrustedHeader,
)
```

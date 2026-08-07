---
name: horsies-monitoring
description: Monitoring guidance for horsies, including the horsies.monitoring query API, the task cancel action, the horsies[web] dashboard, create_monitoring_app, the horsies web CLI, authorization policies, and schema-compatibility states. Use when embedding the dashboard, operating it, or building on the monitoring API.
---

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
horsies web --database-url "postgresql+psycopg://..."   # registry-less
```

| Flag | Default | Notes |
|---|---|---|
| `--host` | `127.0.0.1` | Non-loopback REFUSES to start without `--auth trusted-header` |
| `--port` | `8600` | |
| `--auth` | `none` | `none` (loopback only) or `trusted-header` |
| `--trusted-header` | `X-Forwarded-User` | The reverse proxy MUST strip/set this header itself, or the mode is spoofable |
| `--enable-actions` | off | Off → view-only policy; on → actions allowed |
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

## Authorization

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
migrations; instead the stored schema version is probed (cached 60 s):

| State | Meaning | Reads | Actions |
|---|---|---|---|
| `MATCH` | stored == expected | served | allowed (policy permitting) |
| `MISMATCH` | version differs | served | 409 `SCHEMA_INCOMPATIBLE`, server-side |
| `ABSENT` | a successful probe found no horsies schema | — | refused; the tool never initializes a database |
| `UNKNOWN` | the probe has never succeeded (unreachable DB) | — | refused (`SCHEMA_UNKNOWN`); never conflated with ABSENT |

`GET /api/meta` reports `schema_version`, `expected_schema_version`,
`schema_compatible`, `actions_enabled`, `can_act`, `actions_disabled_reason`.

## Query API — `horsies.monitoring`

All functions are async, take `broker: PostgresBroker` first, and return
`MonitoringResult[T] = Result[T, MonitoringQueryError]`. **An absent row is
`Ok(None)`, not an error** — `Err` means the database operation failed
(`retryable` marks transient connection errors).

```python
task_stats(broker, *, task_names, queues, workers, error_codes, error_categories, retried_only)
task_facets(broker, *, statuses, retried_only)          # + error_categories scoping of the code list
task_breakdown(broker, *, group_by, ..., limit)         # group_by: 'worker' | 'task_name' | 'queue'
list_tasks(broker, *, statuses, ..., sort_by, sort_dir, offset, limit)
get_task_detail(broker, task_id)                        # Ok(None) when absent
list_workflow_names(broker)
list_workflow_runs(broker, *, name, status, limit)
get_workflow_run(broker, workflow_id)                   # full DAG: nodes, edges, failed_indices
get_workflow_node(broker, workflow_id, task_index)
list_schedules(broker)
```

- Filter lists: OR within a dimension, AND across dimensions; empty = no filter.
- `error_categories` (`ErrorCategory`: OPERATIONAL / CONTRACT / RETRIEVAL /
  OUTCOME / DOMAIN) expands server-side from the library's own code registry —
  DOMAIN is the complement of every built-in code.
- Worker/health surfaces reuse the app APIs: `list_worker_states_async`,
  `ping_workers_async`, `ping_database_async`, `get_worker_state_history_async`.

## Task Actions — `cancel_task`

```python
cancel_task(broker, task_id, *, include_running=False) -> Result[TaskCancelled, TaskActionError]
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
- `TaskActionErrorCode`: `TASK_NOT_FOUND`, `TASK_NOT_CANCELLABLE`,
  `TASK_IS_WORKFLOW_TASK`, `DB_OPERATION_FAILED`. State-conflict errors
  carry `current_status`.

## Live Updates

The web process holds ONE `PostgresListener` (~2 connections, lazy start) on
`horsies_task_status`, `horsies_workflow_status`, and `horsies_worker_state`,
coalesces notifies (250 ms), and fans out over `GET /api/events` (SSE).
Events are invalidation signals — `{"topic", "ids"}` — never data. Client
polling is a fallback that activates only while the stream is down; the
schedules table always polls (its table has no trigger).

## Deployment Footprint

- Per web process: the broker pool (defaults `pool_size=5 + max_overflow=10`)
  plus ~2 listener connections — **flat regardless of viewer count**; excess
  requests queue on the pool.
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
    # Result / errors
    MonitoringResult, MonitoringQueryError, MonitoringQueryErrorCode,
    # Vocabularies
    ErrorCategory, TaskSortField, TaskGroupBy, SortDirection,
    # Response models (unstable pre-1.0)
    TaskSummary, TaskListPage, TaskDetail, LeafTaskInfo, TaskAttemptInfo,
    StatusCount, FacetValue, ErrorFacet, Facets, GroupRow, Breakdown,
    WorkflowRunSummary, WorkflowNodeInfo, WorkflowEdge, WorkflowRunDetail,
    WorkflowTaskDetail, WorkerStateInfo, WorkerPingInfo, LivenessReport,
    ScheduleStateInfo, WorkerHistoryPoint,
)

from horsies.web import (  # requires horsies[web]
    create_monitoring_app, MonitoringUIConfig,
    AllowAll, ViewOnly, TrustedHeader,
)
```

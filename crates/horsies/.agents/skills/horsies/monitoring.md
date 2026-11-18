# horsies (Rust) — Monitoring

Reference for the health reads on the app, the typed monitoring queries of
`horsies::monitoring`, the task and workflow actions, and the web UI: the
axum mount `horsies::web::router` (feature `web`) and the shipped
`horsies web` command. The core serves every route, guard and event of the
web UI, so the Python app and the Rust app answer alike.

## Health Reads on the App

```rust
use std::num::NonZeroUsize;
use std::time::Duration;

use horsies::{BrokerOperationError, Horsies, PingTarget};

async fn check_health(app: &Horsies) -> Result<(), BrokerOperationError> {
    let ping = app.ping_database().await?;
    let pongs = app
        .ping_workers(PingTarget::EveryWorker, Duration::from_secs(2), NonZeroUsize::new(1))
        .await?;
    println!("database {} ms, {} workers answered", ping.latency_ms, pongs.len());
    Ok(())
}
```

| Method | Returns |
|---|---|
| `ping_database()` | `DatabasePing` (`latency_ms`) |
| `ping_workers(target, timeout, min_responses)` | `Vec<WorkerPong>` (`worker_id`, `hostname`, `pid`, `round_trip_ms`). `target` is `PingTarget::EveryWorker` or `PingTarget::Worker(worker_id)` |
| `list_worker_states()` | `Vec<WorkerStateSnapshot>`: the latest snapshot of each worker |
| `get_worker_state(worker_id)` | `Option<WorkerStateSnapshot>` |
| `get_worker_state_history(worker_id, limit)` | `Vec<WorkerStateSnapshot>`, newest first. `WorkerHistoryLimit::AllRetained` or `Newest(NonZeroU32)` |
| `get_stale_tasks(StaleMinutes)` | `BoundedTaskList<StaleTaskRow>`. `StaleMinutes::parse(0..=525_600)` |
| `get_expired_tasks()` | `BoundedTaskList<ExpiredTaskRow>`: PENDING tasks past their `good_until` |
| `get_class_coverage()` | `ClassCoverage`: whether a live worker can run each pending execution class |
| `get_class_report()` | `ClassReport`: the load of each execution class |

- Every read returns `Result<_, BrokerOperationError>`: one variant per
  state, with `code()`, `message()` and `retryable()`. A failed read is
  `OperationFailed` with its `BrokerOperationFailedCode`; a refused
  argument is `InvalidInput`; an app with no listener is
  `ListenerDisabled`. Each read connects the app's handle and installs the
  schema once per handle, as a send does.
- `ping_workers` stops at the first of: the target worker replied,
  `min_responses` workers replied, `timeout` passed. A zero `timeout` is
  `INVALID_INPUT`. An app with no listener (`SchemaMode::AssumeInitialized`)
  gets `LISTENER_DISABLED`.
- `BoundedTaskList` is `Complete(rows)` or `Truncated { rows, bound }`, at
  most `TASK_LIST_BOUND` (10,000) rows.
- `app.blocking()` has the same reads, except `get_stale_tasks` and
  `get_expired_tasks`.
- The meaning of each read is on the shared docs page
  `monitoring/worker-health`.

## Monitoring Queries — `horsies::monitoring`

Each function takes the app in place of a broker, connects the app's
handle, and runs the core's query. No function installs the schema.

```rust
use horsies::monitoring::{
    list_tasks, resolve_monitoring_window, task_stats, SortDirection, TaskFilters,
    TaskListQuery, TaskSortField, TaskStatsQuery,
};
use horsies::{Horsies, TaskStatus};

async fn failed_tasks_of_last_day(app: &Horsies) -> Result<(), Box<dyn std::error::Error>> {
    let window = resolve_monitoring_window(None, None, None)?;
    for count in task_stats(app, &TaskStatsQuery::new(window)).await? {
        println!("{}: {}", count.status, count.count);
    }
    let filters = TaskFilters { statuses: vec![TaskStatus::Failed], ..TaskFilters::default() };
    let query = TaskListQuery::new(window)
        .with_filters(filters)
        .with_sort(TaskSortField::FailedAt, SortDirection::Descending)
        .with_pagination(0, 50)?;
    let page = list_tasks(app, &query).await?;
    for task in &page.rows {
        println!("{} {} {:?}", task.id, task.task_name, task.error_code);
    }
    Ok(())
}
```

| Function | Returns (`MonitoringResult<_>`) |
|---|---|
| `task_stats(&app, &TaskStatsQuery)` | `Vec<StatusCount>`: each of the seven statuses, live table plus history window |
| `task_facets(&app, &TaskFacetsQuery)` | `Facets`: workers, task names, queues and error codes with counts |
| `task_breakdown(&app, &TaskBreakdownQuery)` | `Breakdown`: counts per `TaskGroupBy` (`Worker`, `TaskName`, `Queue`) |
| `list_tasks(&app, &TaskListQuery)` | `TaskListPage` (`rows: Vec<TaskSummary>`, `total`) |
| `get_task_detail(&app, task_id)` | `TaskDetailRead`: `Found(TaskDetail)` or `NotFound(TaskDetailNotFound { task_id, reason })` |
| `list_workflow_names(&app)` | `Vec<String>`, at most 10,000 |
| `list_workflow_runs(&app, &WorkflowRunsQuery)` | `Vec<WorkflowRunSummary>`, newest first |
| `get_workflow_run(&app, workflow_id)` | `Option<WorkflowRunDetail>`: nodes and edges; `None` when no run has the id |
| `get_workflow_node(&app, workflow_id, task_index)` | `Option<WorkflowTaskDetail>` |
| `list_schedules(&app)` | `Vec<ScheduleStateInfo>`, soonest next run first, at most 10,000 |

Query builders:

| Query | Builders |
|---|---|
| `TaskStatsQuery::new(window)` | `.with_filters(filters)` (the statuses filter is not used: the read counts every status) |
| `TaskFacetsQuery::new(window)` | `.with_filters`, `.with_statuses`, `.with_error_categories` |
| `TaskBreakdownQuery::new(window, group_by)` | `.with_filters`, `.with_limit(1..=500)` (default 50) |
| `TaskListQuery::new(window)` | `.with_filters`, `.with_sort(TaskSortField, SortDirection)`, `.with_pagination(offset, limit)`: limit 1 to 200, `offset + limit` at most 500 (`MAX_TASK_PAGE_REACH`) |
| `WorkflowRunsQuery::new()` | `.with_name(Option<String>)`, `.with_status(Option<String>)`, `.with_limit(n)` (default 30) |

- `TaskFilters` has `statuses`, `task_names`, `queues`, `workers`,
  `error_codes`, `error_categories` and `retried_only`.
- A refused limit or offset is `PaginationRefused`.
- `resolve_monitoring_window(since, until, now)` is the only constructor of
  a `HistoryWindow`. The default is the last 24 hours
  (`MONITORING_WINDOW_DEFAULT`). A span over 30 days
  (`MONITORING_WINDOW_MAX`), or bounds that do not increase, are
  `WindowRefused`.
- `TaskListPage.total` is exact when any filter is active. With no filter
  it is the live estimate plus the planner's estimate of the history
  window.
- A detail read resolves on the live table and the history, so a task that
  ended still answers. `TaskDetailNotFound.reason` is a
  `TaskAbsenceReason` (see `tasks.md`).

`MonitoringQueryError` is an enum, one variant per state, each with
`message` and `source`; `code()`, `message()`, `retryable()`,
`error_source()` and `failure_cause()` read them:

| Variant | When |
|---|---|
| `DbOperationFailed { retryable, .. }` | The database or the connect failed. `retryable` is the classification of the cause |
| `HistoryContractViolation` | A history row does not decode (`task_stats`, `list_tasks`, `get_task_detail`, `get_workflow_node`). Not retryable |

`MonitoringQueryErrorCode::EventLoopLimit` is Python's only; no variant
has it.

## Task Cancel

```rust
use horsies::monitoring::{cancel_task, TaskActionError};
use horsies::Horsies;
use uuid::Uuid;

async fn cancel_pending(app: &Horsies, task_id: Uuid) -> Result<bool, String> {
    match cancel_task(app, task_id, false).await {
        Ok(_cancelled) => Ok(true),
        Err(TaskActionError::NotCancellable { .. } | TaskActionError::NotFound { .. }) => Ok(false),
        Err(
            error @ (TaskActionError::WorkflowTask { .. }
            | TaskActionError::DbOperationFailed { .. }
            | TaskActionError::StoredDataInvalid { .. }),
        ) => Err(error.message().to_owned()),
    }
}
```

`cancel_task(&app, task_id, include_running)` returns
`TaskActionResult<TaskCancelled>` (`task_id`, `was_status`):

- One compare-and-set in one transaction. A committed `CANCELLED` is final:
  claim, finalize, retry and recovery cannot overwrite it.
- PENDING and CLAIMED are cancelled. RUNNING is cancelled only with
  `include_running = true`: the row becomes `CANCELLED`, but the task code
  keeps running until it returns, its side effects still happen, and no
  attempt row is recorded for that run.
- A waiting `TaskHandle::get` returns `Failed` with `TASK_CANCELLED`.
- A workflow task is refused: use `WorkflowHandle::cancel`, `pause` and
  `resume`.

`TaskActionError` is an enum, one variant per state, each with `message`
and `task_id`; `code()`, `message()`, `retryable()`, `task_id()` and
`failure_cause()` read them:

| Variant | HTTP | When |
|---|---|---|
| `NotFound` | 404 | No live or history row has the id. Nothing changed |
| `NotCancellable { current_status, .. }` | 409 | Terminal, or RUNNING without `include_running`; `current_status` is the status found. Nothing changed |
| `WorkflowTask { current_status, .. }` | 400 | The task belongs to a workflow. Nothing changed |
| `DbOperationFailed { retryable, cause, .. }` | 503 | The statement or the connect failed: nothing was written, or the commit reply was lost. A repeated cancel tells which: a committed cancel answers `NotCancellable` with `CANCELLED` |
| `StoredDataInvalid` | 500 | Stored data does not parse. Nothing changed |

`TaskActionErrorCode::EventLoopLimit` is Python's only; no variant has it.

There is no manual retry action. Re-execution is a rerun (see `tasks.md`).

## Actions as the Web Routes Answer

These functions write and return an `ActionOutcome`: the HTTP status
(`status_code()`) and the body (`body()`: `ActionBody`) that the web route
returns.

| Function | Web route |
|---|---|
| `cancel_task_action(&app, task_id, include_running)` | `POST /api/tasks/{id}/cancel` |
| `pause_workflow_action(&app, workflow_id)` | `POST /api/workflows/{id}/pause` |
| `resume_workflow_action(&app, workflow_id)` | `POST /api/workflows/{id}/resume` |
| `cancel_workflow_action(&app, workflow_id)` | `POST /api/workflows/{id}/cancel` |

`resume_workflow_action` builds the sub-workflow children that the resume
owes from the workflow definitions registered on `app`. A failed connect is
503 with the cause. The rules of each action are on the shared docs page
`monitoring/action-semantics`.

## Web UI: the Axum Mount (feature `web`)

```rust
use axum::Router;
use horsies::web::{router, MonitoringConfig, TrustedHeader};
use horsies::Horsies;

fn monitoring_routes(app: &Horsies) -> Result<Router, Box<dyn std::error::Error>> {
    let policy = TrustedHeader::new("X-Forwarded-User", true)?;
    let monitoring = router(app, MonitoringConfig::default(), policy);
    Ok(Router::new().nest("/monitoring", monitoring))
}
```

`router(&app, config, policy) -> axum::Router` serves every path of the
mount: the page and its assets, `/api/...` and the event stream
(`GET /api/events`).

- Building the router does no I/O. The core handler is built at the first
  request on the app's handle. A request whose broker cannot be built is
  answered 503 with the cause; the next request tries again.
- The mount runs no schema step. For a monitoring-only process, build the
  app with `Horsies::with_schema_mode(config, SchemaMode::ObserveOnly)`.
- A workflow resume builds the children it owes from the workflow
  definitions registered on `app` at the first request. Mount the router
  from the app's own binary, after the registrations, when the UI must
  resume such workflows.
- `MonitoringConfig { ui: MonitoringUiConfig { custom_css_url }, actions }`.
  `MonitoringConfig::default()` has `Actions::Enabled`. With
  `Actions::Disabled` every action route is 403 without a policy call.

### Access policy

| Policy | `can_view` | `can_act` |
|---|---|---|
| `AllowAll` | Yes | Yes. The host owns authentication: mount the router behind it |
| `ViewOnly` | Yes | No |
| `TrustedHeader::new(header_name, allow_actions)` | The header has a value that is not blank | `allow_actions` and the header check. `new` returns `Result<_, InvalidHeaderName>` |

```rust
use horsies::web::{MonitoringAuthPolicy, MonitoringRequest};

struct AdminHeader;

impl MonitoringAuthPolicy for AdminHeader {
    async fn can_view(&self, request: MonitoringRequest<'_>) -> bool {
        request.headers.contains_key("x-admin-user")
    }

    async fn can_act(&self, request: MonitoringRequest<'_>) -> bool {
        request.headers.contains_key("x-admin-user")
    }
}
```

- `MonitoringAuthPolicy` is `Send + Sync + 'static`. Write its methods as
  `async fn`; each future must be `Send`.
- `MonitoringRequest` has `method`, `uri` (the path inside the mount),
  `version`, `headers` and `extensions` (what the host's own layers put
  there).
- The mount asks only the verdicts that the route needs: `can_view` for
  every read route, the event stream and `/api/meta`; then `can_act` for an
  action route while actions are enabled, and for `/api/meta`. The page and
  its assets ask nothing.
- An action route also needs the header `X-Horsies-Intent: action`
  (`INTENT_HEADER`, `INTENT_VALUE`). It is a guard against cross-site
  requests, not authentication.
- A trusted header is safe only when the reverse proxy strips or
  overwrites it on each incoming request.

## Web UI: `horsies web`

The shipped binary serves the web UI of one database (built with the `web`
feature: `cargo install horsies --features web`).

```bash
horsies web horsies.toml
horsies web --database-url postgresql://app:secret@localhost:5432/horsies
horsies web horsies.toml --host 0.0.0.0 --auth trusted-header --enable-actions
```

| Option | Default | Description |
|---|---|---|
| `CONFIG` | none | A TOML file of `AppConfig`; its `[broker]` section names the database. Give exactly one of `CONFIG` and `--database-url` |
| `--database-url URL` | none | Monitor this database, with the default of every other setting |
| `--session-database-url URL` | none | A direct or session-capable URL when `--database-url` is pooled |
| `--pgbouncer-transaction-mode` | off | `--database-url` is a PgBouncer transaction-pool endpoint. Needs `--session-database-url` |
| `--host HOST` | `127.0.0.1` | Bind address |
| `--port PORT` | `8600` | Bind port; `0` asks the operating system for a free port |
| `--auth MODE` | `none` | `none` (a loopback host only) or `trusted-header` |
| `--trusted-header NAME` | `X-Forwarded-User` | The identity header that the proxy sets |
| `--enable-actions` | off | Allow task cancel and workflow pause, resume and cancel |
| `--custom-css-url URL` | none | A stylesheet that the page loads after its own |
| `--loglevel LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

- The server never installs or migrates the schema (`ObserveOnly`).
- `--auth none` on a host that is not a loopback address exits with code 2.
- The binary has no task or workflow of an app. A resume of a paused
  workflow that owes a sub-workflow child needs the app's definitions:
  serve the UI from the app's own binary with `horsies::web::router`.
- Without the `web` feature, `horsies web` exits with code 1.
- The bind and access rules are on the shared docs page
  `monitoring/web-ui-deployment`.

## Things to Avoid

**Do not treat an unfiltered `TaskListPage.total` as an exact count.** It
is an estimate.

**Do not cancel a RUNNING task to stop its side effects.** The cancel
changes the row; the task code runs on.

**Do not expose `AllowAll` without the host's own authentication.** It
allows every read and every action.

## All Key Imports

```rust
use horsies::{
    // Health reads
    PingTarget, WorkerHistoryLimit, DatabasePing, WorkerPong, WorkerStateSnapshot,
    StaleMinutes, StaleThresholdRefusal, StaleTaskRow, ExpiredTaskRow, BoundedTaskList,
    ClassCoverage, ClassReport, BrokerOperationError, BrokerErrorCode, TASK_LIST_BOUND,
    MAX_STALE_THRESHOLD_MINUTES,
};
use horsies::monitoring::{
    // Queries
    task_stats, task_facets, task_breakdown, list_tasks, get_task_detail, list_workflow_names,
    list_workflow_runs, get_workflow_run, get_workflow_node, list_schedules,
    TaskStatsQuery, TaskFacetsQuery, TaskBreakdownQuery, TaskListQuery, WorkflowRunsQuery,
    TaskFilters, TaskSortField, SortDirection, TaskGroupBy, ErrorCategory, PaginationRefused,
    HistoryWindow, WindowRefused, resolve_monitoring_window, MONITORING_WINDOW_DEFAULT,
    MONITORING_WINDOW_MAX, MAX_TASK_PAGE_REACH,
    // Results and errors
    MonitoringResult, MonitoringQueryError, MonitoringQueryErrorCode, StatusCount, Facets,
    Breakdown, TaskListPage, TaskSummary, TaskDetailRead, TaskDetail, TaskDetailNotFound,
    WorkflowRunSummary, WorkflowRunDetail, WorkflowTaskDetail, ScheduleStateInfo,
    // Actions
    cancel_task, cancel_task_action, pause_workflow_action, resume_workflow_action,
    cancel_workflow_action, TaskCancelled, TaskActionError, TaskActionErrorCode,
    TaskActionResult, ActionOutcome, ActionBody, ActionResponse,
};
use horsies::web::{  // feature `web`
    router, Actions, MonitoringConfig, MonitoringUiConfig, MonitoringAuthPolicy,
    MonitoringRequest, AllowAll, ViewOnly, TrustedHeader, INTENT_HEADER, INTENT_VALUE,
};
```

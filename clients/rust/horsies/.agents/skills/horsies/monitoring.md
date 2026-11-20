# horsies (Rust) — Monitoring

Reference for the health reads on the app, the typed monitoring queries of
`horsies::monitoring`, the task and workflow actions, and the web UI: the
axum mount `horsies::web::router` (feature `web`) and the shipped
`horsies web` command; and the event handlers of `horsies::events`. The core serves every route, guard and event of the
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
  `ListenerDisabled`. The message is Python's: `ping_database failed:
  <cause>`, `ping_workers failed: <cause>`, `<read> failed: <cause>`.
  `OperationFailed` carries its `cause`, a `BrokerOperationSource` (the
  `BrokerError`, a refused class read, a snapshot that does not read, a
  refused subscription); `InvalidInput` carries `BrokerRefusalCause::Check`.
  Each read connects the app's handle and installs the schema once per
  handle, as a send does.
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
| `list_workflow_names(&app)` | `Vec<String>`, at most `LIST_ROW_BOUND` (10,000) |
| `list_workflow_runs(&app, &WorkflowRunsQuery)` | `Vec<WorkflowRunSummary>`, newest first |
| `get_workflow_run(&app, workflow_id)` | `Option<WorkflowRunDetail>`: nodes and edges; `None` when no run has the id |
| `get_workflow_node(&app, workflow_id, task_index)` | `Option<WorkflowTaskDetail>` |
| `list_schedules(&app)` | `Vec<ScheduleStateInfo>`, soonest next run first, at most `LIST_ROW_BOUND` (10,000) |

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
| `HistoryContractViolation` | A history row does not decode (`task_stats`, `list_tasks`, `get_task_detail`, `get_workflow_node`); `source` is a `HistoryContractSource` that holds the error. Not retryable |

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
| `DbOperationFailed { retryable, cause, .. }` | 503 | The statement or the connect failed; `cause` is the `BrokerError`. Nothing was written, or the commit reply was lost. A repeated cancel tells which: a committed cancel answers `NotCancellable` with `CANCELLED` |
| `StoredDataInvalid { cause, .. }` | 500 | Stored data does not parse; `cause` is the `OutcomeDecodeError`. Nothing changed |

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
mount: the page and its assets, `/api/...`, the event stream
(`GET /api/events`), and `GET /metrics`: the fleet metrics of the app's
database behind the view verdict (the same gauges as `<binary> metrics`,
read when the last read is 15 s old or more).

- Building the router does no I/O. The core handler is built at the first
  request on the app's handle. A request whose broker cannot be built is
  answered 503 with the cause; the next request tries again.
- The mount runs no schema step. For a monitoring-only process, build the
  app with `Horsies::with_schema_mode(config, SchemaMode::ObserveOnly)`.
- A workflow resume builds the children it owes from the workflow
  definitions registered on `app` at the first request. Mount the router
  from the app's own binary, after the registrations, when the UI must
  resume such workflows.
- `MonitoringConfig { ui: MonitoringUiConfig { custom_css_url, trace_link }, actions }`.
  `trace_link` is `TraceLink::NoTraceLink` (the task detail shows the trace
  id as text) or `TraceLink::Template(TraceLinkTemplate::parse(text)?)`: an
  `http` or `https` URL with one `{trace_id}`, at most 2,048 bytes of
  printable ASCII. `horsies web --trace-link TEMPLATE` sets it.
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

## Worker Metrics Endpoint — `--metrics-listen`

- `<binary> worker --profile <name> --metrics-listen HOST:PORT`
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

## OpenTelemetry (feature `otel`)

- Spans: `send <task_name>` (PRODUCER) around every database call of a
  send; `start <workflow_name>` (PRODUCER) per workflow start; in a worker
  child, the `tracing` span `run <task_name>` (CONSUMER, target
  `horsies::attempt`) around each task body, whose OpenTelemetry parent is
  the send span, extracted from the carrier that the task row stores.
- The client uses the global tracer provider, meter provider and
  propagator of `opentelemetry` 0.33. Set them first in `main`: `main`
  runs in each worker child before `horsies::cli::run`. Without a provider
  no span is recorded; without a propagator that writes keys a send stores
  no carrier.
- `horsies::cli::run` adds `horsies::otel::layer()` to its subscriber. A
  binary with its own subscriber adds the layer and lets its filter pass
  `horsies::attempt` at `INFO`.
- A child exits after `run` returns: shut the tracer provider down after
  `run` so a batch processor exports the last spans.
- Attempt status: OK for a value; ERROR with `error.type` for an error, a
  panic (`UNHANDLED_EXCEPTION`) or a deadline that came first
  (`TASK_TIMEOUT`). An isolated task that the worker kills at its timeout
  has no ended span.
- Producer metrics: `horsies.task.sent` (task, queue, result `ok` or the
  send error code), `horsies.task.send.duration` (queue).

See the website docs `observability/tracing` and
`rust/observability/tracing-setup`.
## Event Handlers — `horsies::events`

```rust
use horsies::events::{AttemptEnded, HandlerError, TaskClaimed};
use horsies::{Horsies, HorsiesError};

async fn record_attempt_end(event: AttemptEnded) -> Result<(), HandlerError> {
    println!("task {} attempt {} ended", event.task_id, event.attempt.get());
    Ok(())
}

fn record_claim(event: TaskClaimed) -> Result<(), HandlerError> {
    println!("task {} claimed", event.task_id);
    Ok(())
}

fn add_handlers(app: &mut Horsies) -> Result<(), HorsiesError> {
    app.on_event::<AttemptEnded>("audit.attempt_ended", record_attempt_end)?;
    app.on_event_blocking::<TaskClaimed>("audit.task_claimed", record_claim)?;
    app.on_handler_child_start(|| async { Ok::<(), String>(()) });
    Ok(())
}
```

- A handler takes the events of one type: `TaskClaimed`, `ClaimReleased`,
  `AttemptPlaced`, `AttemptNotStarted`, `AttemptEnded`, `AttemptRecorded`,
  `TaskRecovered`, `ChildStarted`, `ChildReplaced`, `SlotDown`,
  `SlotRecovered`.
- Register handlers in `main` before `horsies::cli::run(app)`. With a
  handler, the worker starts one handler child (this binary in child
  mode); handlers never run in the worker process.
- Each handler runs on a thread of its own with a queue of
  `HANDLER_QUEUE_EVENTS` (10,000). A full queue drops the event for that
  handler and counts `horsies.telemetry.handler.dropped{handler}`.
- A returned `HandlerError` or a panic counts
  `horsies.telemetry.handler.errors{handler}`, is logged with the name, and
  the handler takes its next event.
- `name`: 1 to 255 characters, not `(unknown)`, once per app; at most 32
  handlers. A refusal is a `HorsiesError`.
- `on_handler_child_start(hook)`: once per handler child, before the
  handlers start, 10 s per hook. A failure keeps the handler child down; the
  worker runs on.
- Delivery is at most once. The history tables are the record of a task.
- With the feature `otel`, the handler child also makes the span of each
  attempt that the worker ended (a timeout kill `TASK_TIMEOUT`, a
  blocked-runtime kill, a lost child `WORKER_CRASHED`), whose span in the
  task child never ended: `run <task_name>`, CONSUMER, parent from the stored
  carrier, start at the start that the child reported (a shared run that
  its child ended at its deadline), else at the placement, status ERROR
  with `error.type`,
  `horsies.span.source = "worker"`. For a workflow task the handler child
  reads the node by the task id (one bounded core read, only for these
  attempts, on one reader task with a queue of at most 1,000 spans: never
  on the event path) and sets `horsies.workflow.*`. A failed read, a full
  queue and a span still queued at the stop leave them out; the worker
  metric `horsies.spans.worker_ended.read_failures{reason}` (`read_failed`,
  `not_read`) counts them. It is the default (the worker then starts the
  handler child with no handler);
  `app.set_worker_ended_spans(horsies::otel::WorkerEndedSpans::NoSpans)`
  turns it off. Set the global tracer provider in `main` or in
  `on_handler_child_start`.

## Things to Avoid

**Do not block in an async event handler.** It takes no event while it
blocks, so its queue fills and drops. Use `on_event_blocking` for a
blocking call.

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
use horsies::events::{
    // Event handlers
    TaskClaimed, ClaimReleased, AttemptPlaced, AttemptNotStarted, AttemptEnded,
    AttemptRecorded, TaskRecovered, ChildStarted, ChildReplaced, SlotDown, SlotRecovered,
    AttemptEnd, AttemptRecord, HandlerError, HANDLER_QUEUE_EVENTS, HANDLER_STATS_INTERVAL,
};
use horsies::web::{  // feature `web`
    router, Actions, MonitoringConfig, MonitoringUiConfig, MonitoringAuthPolicy,
    MonitoringRequest, AllowAll, ViewOnly, TrustedHeader, INTENT_HEADER, INTENT_VALUE,
};
```

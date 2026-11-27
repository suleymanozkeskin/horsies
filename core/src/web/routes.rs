//! Monitoring read, action, and event routes.

use std::convert::Infallible;
use std::time::Duration;

use async_stream::stream;
use axum::extract::{Path, RawQuery, Request, State};
use axum::http::{header, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, NaiveDateTime, TimeDelta, Utc};
use uuid::Uuid;

use crate::broker::health::ListedWorkerState;
use crate::core::defaults::WORKER_STATE_LIVE_WINDOW_SECONDS;
use crate::monitoring::{
    cancel_task_action, cancel_workflow_action, get_task_detail, get_workflow_node,
    get_workflow_run, list_schedules, list_tasks, list_workflow_names, list_workflow_runs,
    pause_workflow_action, resolve_monitoring_window, resume_workflow_action, task_breakdown,
    task_facets, task_status_counts, CancelTaskBody, DatabaseLiveness, ExecutionClassReport,
    LivenessReport, TaskAbsenceReason, TaskBreakdownQuery, TaskDetailNotFound, TaskDetailRead,
    TaskFacetsQuery, TaskFilters, TaskListQuery, TaskStatusCountsQuery, WorkerHistoryPoint,
    WorkerStateInfo, WorkflowRunsQuery,
};
use crate::monitoring::{ActionBody, ActionOutcome, INTERNAL_SERVER_ERROR_TEXT};

use super::app::WebState;
use super::common::{
    broker_read_failed, pagination_refused, parse_error_categories, parse_f64, parse_fastapi_bool,
    parse_group_by, parse_i64, parse_path_integer, parse_path_uuid, parse_sort_direction,
    parse_sort_field, parse_statuses, query_failed, validate_f64_range, validate_i64_range,
    ApiError, PathInteger, QueryValues,
};
use super::events::{data_frame, degraded_frame};
use super::request_body::{read_action_body, WorkflowActionBody};
use super::stdlib_uuid_text::parse_stdlib_uuid_text;

pub(crate) fn read_router() -> Router<WebState> {
    Router::new()
        .route("/tasks/status-counts", get(read_task_stats))
        .route("/tasks/facets", get(read_task_facets))
        .route("/tasks/breakdown", get(read_task_breakdown))
        .route("/tasks", get(read_tasks))
        .route("/tasks/{task_id}", get(read_task))
        .route("/workflows/names", get(read_workflow_names))
        .route("/workflows", get(read_workflows))
        .route("/workflows/{workflow_id}", get(read_workflow))
        .route(
            "/workflows/{workflow_id}/tasks/{node_index}",
            get(read_workflow_task),
        )
        .route("/workers/ping", get(read_worker_ping))
        .route("/workers/schedules", get(read_schedules))
        .route("/workers/classes", get(read_class_report))
        .route("/workers", get(read_workers))
        .route("/workers/{worker_id}/history", get(read_worker_history))
        .route("/events", get(read_events))
}

pub(crate) fn action_router() -> Router<WebState> {
    Router::new()
        .route("/tasks/{task_id}/cancel", post(cancel_task))
        .route("/workflows/{workflow_id}/pause", post(pause_workflow))
        .route("/workflows/{workflow_id}/resume", post(resume_workflow))
        .route("/workflows/{workflow_id}/cancel", post(cancel_workflow))
}

#[derive(Debug, Default)]
struct WindowParams {
    since: Option<String>,
    until: Option<String>,
}

impl WindowParams {
    fn from_values(values: &QueryValues) -> Self {
        Self {
            since: values.last("since").map(str::to_owned),
            until: values.last("until").map(str::to_owned),
        }
    }

    fn resolve(self) -> Result<crate::core::history::reads::pages::MonitoringWindow, ApiError> {
        resolve_monitoring_window(
            parse_window_bound("since", self.since.as_deref())?,
            parse_window_bound("until", self.until.as_deref())?,
            None,
        )
        .map_err(|error| ApiError::bad_request(error.reason))
    }
}

fn parse_window_bound(
    field: &'static str,
    value: Option<&str>,
) -> Result<Option<DateTime<Utc>>, ApiError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
        return Ok(Some(parsed.with_timezone(&Utc)));
    }
    let naive = [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dt%H:%M:%S%.f",
        "%Y-%m-%d_%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dt%H:%M",
        "%Y-%m-%d_%H:%M",
        "%Y-%m-%d %H:%M",
    ]
    .into_iter()
    .any(|format| NaiveDateTime::parse_from_str(value, format).is_ok());
    if naive {
        return Err(ApiError::bad_request(format!(
            "{field} must be timezone-aware"
        )));
    }
    Err(ApiError::query_validation(
        field,
        "datetime_parsing",
        "Input should be a valid datetime or date, invalid datetime separator, expected `T`, `t`, `_` or space",
        value,
    ))
}

#[derive(Debug, Default)]
struct TaskStatsParams {
    window: WindowParams,
    task_name: Vec<String>,
    queue: Vec<String>,
    worker: Vec<String>,
    error_code: Vec<String>,
    error_category: Vec<String>,
    retried_only: bool,
}

impl TaskStatsParams {
    fn from_values(values: &QueryValues) -> Result<Self, ApiError> {
        Ok(Self {
            window: WindowParams::from_values(values),
            task_name: values.all("task_name"),
            queue: values.all("queue"),
            worker: values.all("worker"),
            error_code: values.all("error_code"),
            error_category: values.all("error_category"),
            retried_only: parse_fastapi_bool(values, "retried_only", false)?,
        })
    }

    fn filters(&self) -> Result<TaskFilters, ApiError> {
        Ok(TaskFilters {
            statuses: Vec::new(),
            task_names: self.task_name.clone(),
            queues: self.queue.clone(),
            workers: self.worker.clone(),
            error_codes: self.error_code.clone(),
            error_categories: parse_error_categories(self.error_category.clone())?,
            retried_only: self.retried_only,
        })
    }

    fn window(&self) -> Result<crate::core::history::reads::pages::MonitoringWindow, ApiError> {
        WindowParams {
            since: self.window.since.clone(),
            until: self.window.until.clone(),
        }
        .resolve()
    }
}

async fn read_task_stats(
    State(state): State<WebState>,
    RawQuery(raw): RawQuery,
) -> Result<Json<Vec<crate::monitoring::StatusCount>>, ApiError> {
    let cache_key = raw.clone().unwrap_or_default();
    let values = QueryValues::parse(raw.as_deref());
    let params = TaskStatsParams::from_values(&values)?;
    let query = TaskStatusCountsQuery::new(params.window()?).with_filters(params.filters()?);
    state
        .task_stats_cache
        .get_or_try_init(cache_key, || task_status_counts(&state.broker, &query))
        .await
        .map(Json)
        .map_err(|error| query_failed("Task stats", &error))
}

#[derive(Debug, Default)]
struct TaskFacetsParams {
    window: WindowParams,
    statuses: Vec<String>,
    error_category: Vec<String>,
    retried_only: bool,
}

impl TaskFacetsParams {
    fn from_values(values: &QueryValues) -> Result<Self, ApiError> {
        Ok(Self {
            window: WindowParams::from_values(values),
            statuses: values.all("status"),
            error_category: values.all("error_category"),
            retried_only: parse_fastapi_bool(values, "retried_only", false)?,
        })
    }
}

async fn read_task_facets(
    State(state): State<WebState>,
    RawQuery(raw): RawQuery,
) -> Result<Json<crate::monitoring::Facets>, ApiError> {
    let cache_key = raw.clone().unwrap_or_default();
    let values = QueryValues::parse(raw.as_deref());
    let params = TaskFacetsParams::from_values(&values)?;
    let window = params.window.resolve()?;
    let query = TaskFacetsQuery::new(window)
        .with_statuses(parse_statuses(params.statuses)?)
        .with_error_categories(parse_error_categories(params.error_category)?)
        .retried_only(params.retried_only);
    state
        .task_facets_cache
        .get_or_try_init(cache_key, || task_facets(&state.broker, &query))
        .await
        .map(Json)
        .map_err(|error| query_failed("Task facets", &error))
}

#[derive(Debug)]
struct TaskBreakdownParams {
    window: WindowParams,
    group_by: String,
    statuses: Vec<String>,
    task_name: Vec<String>,
    queue: Vec<String>,
    worker: Vec<String>,
    error_code: Vec<String>,
    error_category: Vec<String>,
    retried_only: bool,
    limit: i64,
}

impl TaskBreakdownParams {
    fn from_values(values: &QueryValues) -> Result<Self, ApiError> {
        let limit = parse_i64(values, "limit", 50)?;
        Ok(Self {
            window: WindowParams::from_values(values),
            group_by: values.last("group_by").unwrap_or("worker").to_owned(),
            statuses: values.all("status"),
            task_name: values.all("task_name"),
            queue: values.all("queue"),
            worker: values.all("worker"),
            error_code: values.all("error_code"),
            error_category: values.all("error_category"),
            retried_only: parse_fastapi_bool(values, "retried_only", false)?,
            limit: validate_i64_range(values, "limit", limit, 1, 500)?,
        })
    }
}

async fn read_task_breakdown(
    State(state): State<WebState>,
    RawQuery(raw): RawQuery,
) -> Result<Json<crate::monitoring::Breakdown>, ApiError> {
    let cache_key = raw.clone().unwrap_or_default();
    let values = QueryValues::parse(raw.as_deref());
    let params = TaskBreakdownParams::from_values(&values)?;
    let window = params.window.resolve()?;
    let filters = TaskFilters {
        statuses: parse_statuses(params.statuses)?,
        task_names: params.task_name,
        queues: params.queue,
        workers: params.worker,
        error_codes: params.error_code,
        error_categories: parse_error_categories(params.error_category)?,
        retried_only: params.retried_only,
    };
    let query = TaskBreakdownQuery::new(window, parse_group_by(&params.group_by)?)
        .with_filters(filters)
        .with_limit(params.limit)
        .map_err(|error| pagination_refused(error, &values, "limit", params.limit))?;
    state
        .task_breakdown_cache
        .get_or_try_init(cache_key, || task_breakdown(&state.broker, &query))
        .await
        .map(Json)
        .map_err(|error| query_failed("Task breakdown", &error))
}

#[derive(Debug)]
struct TaskListParams {
    window: WindowParams,
    statuses: Vec<String>,
    task_name: Vec<String>,
    queue: Vec<String>,
    worker: Vec<String>,
    error_code: Vec<String>,
    error_category: Vec<String>,
    retried_only: bool,
    sort_field: String,
    sort_direction: String,
    offset: i64,
    limit: i64,
}

impl TaskListParams {
    fn from_values(values: &QueryValues) -> Result<Self, ApiError> {
        let offset = parse_i64(values, "offset", 0)?;
        let limit = parse_i64(values, "limit", 50)?;
        Ok(Self {
            window: WindowParams::from_values(values),
            statuses: values.all("status"),
            task_name: values.all("task_name"),
            queue: values.all("queue"),
            worker: values.all("worker"),
            error_code: values.all("error_code"),
            error_category: values.all("error_category"),
            retried_only: parse_fastapi_bool(values, "retried_only", false)?,
            sort_field: values
                .last("sort_field")
                .unwrap_or("enqueued_at")
                .to_owned(),
            sort_direction: values
                .last("sort_direction")
                .unwrap_or("DESCENDING")
                .to_owned(),
            offset: validate_i64_range(values, "offset", offset, 0, i64::MAX)?,
            limit: validate_i64_range(values, "limit", limit, 1, 200)?,
        })
    }
}

async fn read_tasks(
    State(state): State<WebState>,
    RawQuery(raw): RawQuery,
) -> Result<Json<crate::monitoring::TaskListPage>, ApiError> {
    let values = QueryValues::parse(raw.as_deref());
    let params = TaskListParams::from_values(&values)?;
    let reach = i128::from(params.offset) + i128::from(params.limit);
    if reach > 500 {
        return Err(ApiError::bad_request(format!(
            "offset + limit must be <= 500; got {reach}"
        )));
    }
    let window = params.window.resolve()?;
    let filters = TaskFilters {
        statuses: parse_statuses(params.statuses)?,
        task_names: params.task_name,
        queues: params.queue,
        workers: params.worker,
        error_codes: params.error_code,
        error_categories: parse_error_categories(params.error_category)?,
        retried_only: params.retried_only,
    };
    let query = TaskListQuery::new(window)
        .with_filters(filters)
        .with_sort(
            parse_sort_field(&params.sort_field)?,
            parse_sort_direction(&params.sort_direction)?,
        )
        .with_pagination(params.offset, params.limit)
        .map_err(|error| pagination_refused(error, &values, "limit", params.limit))?;
    list_tasks(&state.broker, &query)
        .await
        .map(Json)
        .map_err(|error| query_failed("Task list", &error))
}

async fn read_task(
    State(state): State<WebState>,
    Path(task_id): Path<String>,
) -> Result<Json<crate::monitoring::TaskDetail>, ApiError> {
    let task_id = parse_path_uuid("task_id", &task_id)?;
    match get_task_detail(&state.broker, task_id)
        .await
        .map_err(|error| query_failed("Task detail", &error))?
    {
        TaskDetailRead::Found(detail) => Ok(Json(*detail)),
        TaskDetailRead::NotFound(TaskDetailNotFound { reason, .. }) => Err(ApiError::detail(
            StatusCode::NOT_FOUND,
            task_not_found_text(reason),
        )),
    }
}

/// The 404 detail text for a task id that no table holds.
pub(super) fn task_not_found_text(reason: TaskAbsenceReason) -> &'static str {
    match reason {
        TaskAbsenceReason::PredatesRetainedHistory => {
            "Task not found. Its id is older than the retained task history."
        }
        TaskAbsenceReason::WithinRetainedHistory | TaskAbsenceReason::NotClassified => {
            "Task not found."
        }
    }
}

async fn read_workflow_names(State(state): State<WebState>) -> Result<Json<Vec<String>>, ApiError> {
    list_workflow_names(&state.broker)
        .await
        .map(Json)
        .map_err(|error| query_failed("Workflow names", &error))
}

/// The query of `GET /api/workflows`. The query parameter `name` keeps its
/// HTTP name; it is the workflow name filter.
#[derive(Debug)]
struct WorkflowParams {
    workflow_name: Option<String>,
    status: Option<String>,
    limit: i64,
}

impl WorkflowParams {
    fn from_values(values: &QueryValues) -> Result<Self, ApiError> {
        let limit = parse_i64(values, "limit", 30)?;
        Ok(Self {
            workflow_name: values.last("workflow_name").map(str::to_owned),
            status: values.last("status").map(str::to_owned),
            limit: validate_i64_range(values, "limit", limit, 1, 200)?,
        })
    }
}

async fn read_workflows(
    State(state): State<WebState>,
    RawQuery(raw): RawQuery,
) -> Result<Json<Vec<crate::monitoring::WorkflowRunSummary>>, ApiError> {
    let values = QueryValues::parse(raw.as_deref());
    let params = WorkflowParams::from_values(&values)?;
    let query = WorkflowRunsQuery::new()
        .with_workflow_name(params.workflow_name)
        .with_status(params.status)
        .with_limit(params.limit)
        .map_err(|error| pagination_refused(error, &values, "limit", params.limit))?;
    list_workflow_runs(&state.broker, &query)
        .await
        .map(Json)
        .map_err(|error| query_failed("Workflow runs", &error))
}

/// The 404 of a workflow run that the read did not find.
fn workflow_run_not_found() -> ApiError {
    ApiError::detail(StatusCode::NOT_FOUND, "Workflow run not found.")
}

/// The 404 of a workflow node that the read did not find.
fn workflow_task_not_found() -> ApiError {
    ApiError::detail(StatusCode::NOT_FOUND, "Workflow task not found.")
}

/// One run's DAG. The Python route takes the id as text, and
/// `get_workflow_run` reads it with `uuid.UUID(text)`
/// ([`parse_stdlib_uuid_text`]): a text that it reads names the run of that
/// UUID, and any other text names no run: 404, with no database call.
async fn read_workflow(
    State(state): State<WebState>,
    Path(workflow_id): Path<String>,
) -> Result<Json<crate::monitoring::WorkflowRunDetail>, ApiError> {
    let Some(workflow_id) = parse_stdlib_uuid_text(&workflow_id) else {
        return Err(workflow_run_not_found());
    };
    get_workflow_run(&state.broker, workflow_id)
        .await
        .map_err(|error| query_failed("Workflow run", &error))?
        .map(Json)
        .ok_or_else(workflow_run_not_found)
}

/// One node's detail. The Python route takes the id as text and the index
/// as an integer: an index text that pydantic does not read as an integer
/// is the 422 of FastAPI. `get_workflow_node` reads the id text with
/// `uuid.UUID(text)` ([`parse_stdlib_uuid_text`]). An id text that it
/// refuses, or an integer index outside the 32-bit node index column, names
/// no node: 404, with no database call.
async fn read_workflow_task(
    State(state): State<WebState>,
    Path((workflow_id, node_index)): Path<(String, String)>,
) -> Result<Json<crate::monitoring::WorkflowNodeDetail>, ApiError> {
    let node_index = match parse_path_integer(&node_index) {
        PathInteger::Fits(node_index) => Some(node_index),
        PathInteger::OutsideI32 => None,
        PathInteger::NotAnInteger => {
            return Err(ApiError::Validation(vec![ApiError::path_issue(
                "node_index",
                "int_parsing",
                "Input should be a valid integer, unable to parse string as an integer",
                node_index.as_str(),
            )]))
        }
        PathInteger::TooLong => {
            return Err(ApiError::Validation(vec![ApiError::path_issue(
                "node_index",
                "int_parsing_size",
                "Unable to parse input string as an integer, exceeded maximum size",
                node_index.as_str(),
            )]))
        }
    };
    let (Some(workflow_id), Some(node_index)) = (parse_stdlib_uuid_text(&workflow_id), node_index)
    else {
        return Err(workflow_task_not_found());
    };
    get_workflow_node(&state.broker, workflow_id, node_index)
        .await
        .map_err(|error| query_failed("Workflow task", &error))?
        .map(Json)
        .ok_or_else(workflow_task_not_found)
}

#[derive(Debug)]
struct PingParams {
    timeout_seconds: f64,
}

impl PingParams {
    fn from_values(values: &QueryValues) -> Result<Self, ApiError> {
        let timeout = parse_f64(values, "timeout_seconds", 2.0)?;
        Ok(Self {
            timeout_seconds: validate_f64_range(values, "timeout_seconds", timeout, 0.1, 10.0)?,
        })
    }
}

async fn read_worker_ping(
    State(state): State<WebState>,
    RawQuery(raw): RawQuery,
) -> Result<Json<LivenessReport>, ApiError> {
    let values = QueryValues::parse(raw.as_deref());
    let params = PingParams::from_values(&values)?;
    let database = match state.broker.ping_database().await {
        Ok(ping) => DatabaseLiveness::Ping {
            latency_ms: ping.latency_ms,
        },
        Err(_) => DatabaseLiveness::Unreachable,
    };
    let workers = state
        .broker
        .ping_workers(None, Duration::from_secs_f64(params.timeout_seconds), None)
        .await
        .map_err(|error| broker_read_failed("Worker ping", &error))?;
    Ok(Json(LivenessReport { database, workers }))
}

async fn read_schedules(
    State(state): State<WebState>,
) -> Result<Json<Vec<crate::monitoring::ScheduleStateInfo>>, ApiError> {
    list_schedules(&state.broker)
        .await
        .map(Json)
        .map_err(|error| query_failed("Schedule state", &error))
}

/// Pending streams per (queue, class) with their oldest wait and coverage,
/// and the budget load of each live worker. A failed read that may clear is
/// 503; a read that stored data or a bound refused is 500
/// ([`broker_read_failed`]).
async fn read_class_report(
    State(state): State<WebState>,
) -> Result<Json<ExecutionClassReport>, ApiError> {
    state
        .broker
        .get_execution_class_report()
        .await
        .map(Json)
        .map_err(|error| broker_read_failed("Execution class report query", &error))
}

/// A snapshot older than this is stale: the worker stopped reporting. The
/// fleet check counts the same workers as live (a snapshot at most this
/// old).
const STALE_SNAPSHOT_THRESHOLD: TimeDelta =
    TimeDelta::seconds(WORKER_STATE_LIVE_WINDOW_SECONDS as i64);

/// One entry of the worker list.
///
/// The snapshot age, the staleness and the uptime are measured to `read_at`,
/// the database clock of the list statement: `snapshot_at` and
/// `worker_started_at` are database times too. Each is exact to the
/// microsecond, and the staleness compares the exact age.
pub(super) fn worker_state(listed: ListedWorkerState) -> WorkerStateInfo {
    let ListedWorkerState { snapshot, read_at } = listed;
    let age = read_at - snapshot.snapshot_at;
    WorkerStateInfo {
        worker_id: snapshot.worker_id,
        hostname: snapshot.hostname,
        pid: snapshot.pid,
        snapshot_at: snapshot.snapshot_at,
        snapshot_age_seconds: Some(age),
        stale: age > STALE_SNAPSHOT_THRESHOLD,
        worker_started_at: snapshot.worker_started_at,
        uptime_seconds: Some(read_at - snapshot.worker_started_at),
        child_count: snapshot.child_count,
        queues: snapshot.queues,
        queue_max_concurrency: snapshot.queue_max_concurrency,
        tasks_running: snapshot.tasks_running,
        tasks_claimed: snapshot.tasks_claimed,
        // The web JSON holds the cap as a number, or null for no cap.
        cluster_wide_cap: snapshot
            .cluster_wide_cap
            .claim_parameter()
            .map(|tasks| i32::try_from(tasks).expect("a stored cap came from an integer column")),
        memory_usage_mb: snapshot.memory_usage_mb,
        memory_percent: snapshot.memory_percent,
        cpu_percent: snapshot.cpu_percent,
        slot_health: snapshot.slot_health,
        profile: snapshot.profile,
    }
}

async fn read_workers(
    State(state): State<WebState>,
) -> Result<Json<Vec<WorkerStateInfo>>, ApiError> {
    let listed = state
        .broker
        .list_worker_states_with_read_time()
        .await
        .map_err(|error| broker_read_failed("Worker state query", &error))?;
    Ok(Json(listed.into_iter().map(worker_state).collect()))
}

#[derive(Debug)]
struct HistoryParams {
    limit: i64,
}

impl HistoryParams {
    fn from_values(values: &QueryValues) -> Result<Self, ApiError> {
        let limit = parse_i64(values, "limit", 120)?;
        Ok(Self {
            limit: validate_i64_range(values, "limit", limit, 1, 1000)?,
        })
    }
}

async fn read_worker_history(
    State(state): State<WebState>,
    Path(worker_id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Result<Json<Vec<WorkerHistoryPoint>>, ApiError> {
    let values = QueryValues::parse(raw.as_deref());
    let params = HistoryParams::from_values(&values)?;
    state
        .broker
        .get_worker_state_history(&worker_id, Some(params.limit))
        .await
        .map_err(|error| broker_read_failed("Worker history query", &error))
        .map(|snapshots| {
            Json(
                snapshots
                    .into_iter()
                    .map(|snapshot| WorkerHistoryPoint {
                        snapshot_at: snapshot.snapshot_at,
                        tasks_running: snapshot.tasks_running,
                        tasks_claimed: snapshot.tasks_claimed,
                        cpu_percent: snapshot.cpu_percent,
                        memory_usage_mb: snapshot.memory_usage_mb,
                        memory_percent: snapshot.memory_percent,
                    })
                    .collect(),
            )
        })
}

async fn read_events(State(state): State<WebState>) -> Response {
    let heartbeat = state.heartbeat;
    let mut subscription = match state.events.subscribe().await {
        Ok(subscription) => subscription,
        Err(refused) => {
            return ApiError::unavailable(format!("The event stream is refused: {refused}."))
                .into_response()
        }
    };
    let events = stream! {
        while let Some(event) = subscription.recv().await {
            match event {
                Some(event) => yield Ok::<Event, Infallible>(Event::default().data(data_frame(&event))),
                None => {
                    yield Ok::<Event, Infallible>(Event::default().data(degraded_frame()));
                    break;
                }
            }
        }
    };
    (
        [
            (header::CACHE_CONTROL, "no-cache"),
            (header::HeaderName::from_static("x-accel-buffering"), "no"),
        ],
        Sse::new(events).keep_alive(KeepAlive::new().interval(heartbeat).text("heartbeat")),
    )
        .into_response()
}

/// The response of an action, with its log line: `monitoring action route=...
/// id=... http=... result=...`, at INFO on `horsies.web`. Every body is JSON
/// but [`ActionBody::InternalServerError`], which is
/// [`INTERNAL_SERVER_ERROR_TEXT`] as `text/plain; charset=utf-8`.
fn action_response(route: &'static str, entity_id: Uuid, outcome: ActionOutcome) -> Response {
    tracing::info!(
        "monitoring action route={route} id={entity_id} http={} result={}",
        outcome.status_code(),
        outcome.result_word(),
    );
    let status =
        StatusCode::from_u16(outcome.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match outcome.into_body() {
        ActionBody::InternalServerError => (status, INTERNAL_SERVER_ERROR_TEXT).into_response(),
        body @ (ActionBody::Success(_)
        | ActionBody::Conflict { .. }
        | ActionBody::Code { .. }
        | ActionBody::Detail { .. }) => (status, Json(body)).into_response(),
    }
}

/// Cancel one task. The body is read first ([`read_action_body`]: no body,
/// or `{"policy": "PENDING_OR_CLAIMED_ONLY" | "INCLUDE_RUNNING"}`), then the
/// path id.
async fn cancel_task(
    State(state): State<WebState>,
    Path(task_id): Path<String>,
    request: Request,
) -> Response {
    let body: CancelTaskBody = match read_action_body(request).await {
        Ok(body) => body,
        Err(refused) => return refused.into_response(),
    };
    let task_uuid = match parse_path_uuid("task_id", &task_id) {
        Ok(task_uuid) => task_uuid,
        Err(refused) => return refused.into_response(),
    };
    action_response(
        "tasks.cancel",
        task_uuid,
        cancel_task_action(&state.broker, task_uuid, body.policy).await,
    )
}

/// The input of a workflow action route, read before any statement: the
/// body first ([`read_action_body`]: no body, or `{}`), then the path id.
/// Path text that is not a UUID is refused with 400 `INVALID_INPUT`, as the
/// task cancel route refuses its id (Python `require_path_uuid`).
async fn workflow_action_input(workflow_id: &str, request: Request) -> Result<Uuid, ApiError> {
    let WorkflowActionBody {} = read_action_body(request).await?;
    parse_path_uuid("workflow_id", workflow_id)
}

async fn pause_workflow(
    State(state): State<WebState>,
    Path(workflow_id): Path<String>,
    request: Request,
) -> Response {
    let workflow_id = match workflow_action_input(&workflow_id, request).await {
        Ok(workflow_id) => workflow_id,
        Err(refused) => return refused.into_response(),
    };
    action_response(
        "workflows.pause",
        workflow_id,
        pause_workflow_action(&state.broker, workflow_id).await,
    )
}

async fn resume_workflow(
    State(state): State<WebState>,
    Path(workflow_id): Path<String>,
    request: Request,
) -> Response {
    let workflow_id = match workflow_action_input(&workflow_id, request).await {
        Ok(workflow_id) => workflow_id,
        Err(refused) => return refused.into_response(),
    };
    action_response(
        "workflows.resume",
        workflow_id,
        resume_workflow_action(&state.broker, workflow_id, state.children.as_ref()).await,
    )
}

async fn cancel_workflow(
    State(state): State<WebState>,
    Path(workflow_id): Path<String>,
    request: Request,
) -> Response {
    let workflow_id = match workflow_action_input(&workflow_id, request).await {
        Ok(workflow_id) => workflow_id,
        Err(refused) => return refused.into_response(),
    };
    action_response(
        "workflows.cancel",
        workflow_id,
        cancel_workflow_action(&state.broker, workflow_id).await,
    )
}

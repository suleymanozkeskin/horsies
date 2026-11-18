//! The monitoring reads through the core (`horsies_core::monitoring`).
//!
//! A read returns the JSON text of its core model (`MonitoringJson`); the
//! Python client validates it into its Pydantic model with one
//! `model_validate_json`, and the web handler writes the same text. An
//! expected failure is `MonitoringFailed` with its catalog code. The task
//! detail of an id that no table holds is `TaskDetailAbsent`. Every read
//! sends one statement in autocommit and writes nothing; it does not install
//! the schema, as Python's monitoring reads do not.

use std::future::Future;
use std::sync::Arc;

use horsies_core::catalog::CodeEnum;
use horsies_core::monitoring::{
    get_task_detail, get_workflow_node, get_workflow_run, list_schedules, list_tasks,
    list_workflow_names, list_workflow_runs, task_breakdown, task_facets, task_stats,
    ErrorCategory, HistoryWindow, MonitoringQueryError, MonitoringQueryErrorCode, SortDirection,
    TaskBreakdownQuery as CoreTaskBreakdownQuery, TaskDetailRead,
    TaskFacetsQuery as CoreTaskFacetsQuery, TaskFilters, TaskGroupBy,
    TaskListQuery as CoreTaskListQuery, TaskSortField, TaskStatsQuery as CoreTaskStatsQuery,
    WorkflowRunsQuery as CoreWorkflowRunsQuery,
};
use horsies_core::{BrokerError, PostgresBroker, TaskStatus};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::core_failures::FailureException;
use crate::handle::BrokerConnection;
use crate::operations::arguments::utc_instant;

/// The JSON text of the core model of one monitoring read.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct MonitoringJson {
    pub json: String,
}

/// An expected failure of a monitoring read. Nothing changed: a read writes
/// nothing.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct MonitoringFailed {
    /// A member of `MonitoringQueryErrorCode`, as its wire value.
    pub code: String,
    /// The cause, with the read that failed.
    pub message: String,
    pub retryable: bool,
    /// The cause, with the read as its subject (for example
    /// `task list query`).
    pub exception: FailureException,
}

/// The subject of the exception of a failed detail read.
const DETAIL_SUBJECT: &str = "task detail query";

/// Neither the live table nor the history holds the task.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct TaskDetailAbsent {
    pub task_id: Uuid,
    /// A member of `TaskAbsenceReason`, as its wire value.
    pub reason: String,
}

/// The outcome of a list or aggregate read.
#[derive(Debug)]
pub enum ReadOutcome {
    Json(MonitoringJson),
    Failed(MonitoringFailed),
}

impl<'py> IntoPyObject<'py> for ReadOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Json(json) => json.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The outcome of the task detail read.
#[derive(Debug)]
pub enum DetailOutcome {
    Json(MonitoringJson),
    Absent(TaskDetailAbsent),
    Failed(MonitoringFailed),
}

impl<'py> IntoPyObject<'py> for DetailOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Json(json) => json.into_bound_py_any(py),
            Self::Absent(absent) => absent.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// One frozen task list read: the window, the filters, the sort and the
/// page. The constructor checks every value; a value outside its rule
/// raises `ValueError`, and a naive bound raises `TypeError`.
#[pyclass(module = "horsies._core", frozen)]
pub struct TaskListQuery {
    query: CoreTaskListQuery,
}

impl TaskListQuery {
    /// The core query.
    pub fn query(&self) -> CoreTaskListQuery {
        self.query.clone()
    }
}

#[pymethods]
impl TaskListQuery {
    #[new]
    #[pyo3(signature = (
        *,
        window_lower,
        window_upper,
        statuses,
        task_names,
        queues,
        workers,
        error_codes,
        error_categories,
        retried_only,
        sort_by,
        sort_dir,
        offset,
        limit,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        window_lower: &Bound<'_, PyAny>,
        window_upper: &Bound<'_, PyAny>,
        statuses: Vec<String>,
        task_names: Vec<String>,
        queues: Vec<String>,
        workers: Vec<String>,
        error_codes: Vec<String>,
        error_categories: Vec<String>,
        retried_only: bool,
        sort_by: &str,
        sort_dir: &str,
        offset: i64,
        limit: i64,
    ) -> PyResult<Self> {
        let window = history_window(window_lower, window_upper)?;
        let filters = TaskFilters {
            statuses: task_statuses(&statuses).map_err(PyValueError::new_err)?,
            task_names,
            queues,
            workers,
            error_codes,
            error_categories: error_categories_of(&error_categories)
                .map_err(PyValueError::new_err)?,
            retried_only,
        };
        let field = sort_field(sort_by).map_err(PyValueError::new_err)?;
        let direction = sort_direction(sort_dir).map_err(PyValueError::new_err)?;
        let query = CoreTaskListQuery::new(window)
            .with_filters(filters)
            .with_sort(field, direction)
            .with_pagination(offset, limit)
            .map_err(|refused| PyValueError::new_err(refused.reason))?;
        Ok(Self { query })
    }
}

/// One frozen task facets read: the window, the statuses, the error
/// categories that narrow the code list, and the retried filter. A value
/// outside its rule raises `ValueError`, and a naive bound raises
/// `TypeError`.
#[pyclass(module = "horsies._core", frozen)]
pub struct TaskFacetsQuery {
    query: CoreTaskFacetsQuery,
}

impl TaskFacetsQuery {
    /// The core query.
    pub fn query(&self) -> CoreTaskFacetsQuery {
        self.query.clone()
    }
}

#[pymethods]
impl TaskFacetsQuery {
    #[new]
    #[pyo3(signature = (*, window_lower, window_upper, statuses, error_categories, retried_only))]
    fn new(
        window_lower: &Bound<'_, PyAny>,
        window_upper: &Bound<'_, PyAny>,
        statuses: Vec<String>,
        error_categories: Vec<String>,
        retried_only: bool,
    ) -> PyResult<Self> {
        let window = history_window(window_lower, window_upper)?;
        let query = CoreTaskFacetsQuery::new(window)
            .with_statuses(task_statuses(&statuses).map_err(PyValueError::new_err)?)
            .with_error_categories(
                error_categories_of(&error_categories).map_err(PyValueError::new_err)?,
            )
            .retried_only(retried_only);
        Ok(Self { query })
    }
}

/// One frozen task stats read: the window and every filter but the status
/// (the stats are the status selector). A value outside its rule raises
/// `ValueError`, and a naive bound raises `TypeError`.
#[pyclass(module = "horsies._core", frozen)]
pub struct TaskStatsQuery {
    query: CoreTaskStatsQuery,
}

impl TaskStatsQuery {
    /// The core query.
    pub fn query(&self) -> CoreTaskStatsQuery {
        self.query.clone()
    }
}

#[pymethods]
impl TaskStatsQuery {
    #[new]
    #[pyo3(signature = (
        *,
        window_lower,
        window_upper,
        task_names,
        queues,
        workers,
        error_codes,
        error_categories,
        retried_only,
    ))]
    fn new(
        window_lower: &Bound<'_, PyAny>,
        window_upper: &Bound<'_, PyAny>,
        task_names: Vec<String>,
        queues: Vec<String>,
        workers: Vec<String>,
        error_codes: Vec<String>,
        error_categories: Vec<String>,
        retried_only: bool,
    ) -> PyResult<Self> {
        let window = history_window(window_lower, window_upper)?;
        let filters = TaskFilters {
            statuses: Vec::new(),
            task_names,
            queues,
            workers,
            error_codes,
            error_categories: error_categories_of(&error_categories)
                .map_err(PyValueError::new_err)?,
            retried_only,
        };
        Ok(Self {
            query: CoreTaskStatsQuery::new(window).with_filters(filters),
        })
    }
}

/// One frozen task breakdown read: the window, the group column, the
/// filters and the group limit. A value outside its rule raises
/// `ValueError`, and a naive bound raises `TypeError`.
#[pyclass(module = "horsies._core", frozen)]
pub struct TaskBreakdownQuery {
    query: CoreTaskBreakdownQuery,
}

impl TaskBreakdownQuery {
    /// The core query.
    pub fn query(&self) -> CoreTaskBreakdownQuery {
        self.query.clone()
    }
}

#[pymethods]
impl TaskBreakdownQuery {
    #[new]
    #[pyo3(signature = (
        *,
        window_lower,
        window_upper,
        group_by,
        statuses,
        task_names,
        queues,
        workers,
        error_codes,
        error_categories,
        retried_only,
        limit,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        window_lower: &Bound<'_, PyAny>,
        window_upper: &Bound<'_, PyAny>,
        group_by: &str,
        statuses: Vec<String>,
        task_names: Vec<String>,
        queues: Vec<String>,
        workers: Vec<String>,
        error_codes: Vec<String>,
        error_categories: Vec<String>,
        retried_only: bool,
        limit: i64,
    ) -> PyResult<Self> {
        let window = history_window(window_lower, window_upper)?;
        let group = group_column(group_by).map_err(PyValueError::new_err)?;
        let filters = TaskFilters {
            statuses: task_statuses(&statuses).map_err(PyValueError::new_err)?,
            task_names,
            queues,
            workers,
            error_codes,
            error_categories: error_categories_of(&error_categories)
                .map_err(PyValueError::new_err)?,
            retried_only,
        };
        let query = CoreTaskBreakdownQuery::new(window, group)
            .with_filters(filters)
            .with_limit(limit)
            .map_err(|refused| PyValueError::new_err(refused.reason))?;
        Ok(Self { query })
    }
}

/// One frozen workflow runs read: the exact name and status filters (`None`
/// for none) and the limit. A limit outside 1 to 200 raises `ValueError`.
#[pyclass(module = "horsies._core", frozen)]
pub struct WorkflowRunsQuery {
    query: CoreWorkflowRunsQuery,
}

impl WorkflowRunsQuery {
    /// The core query.
    pub fn query(&self) -> CoreWorkflowRunsQuery {
        self.query.clone()
    }
}

#[pymethods]
impl WorkflowRunsQuery {
    #[new]
    #[pyo3(signature = (*, name, status, limit))]
    fn new(name: Option<String>, status: Option<String>, limit: i64) -> PyResult<Self> {
        let query = CoreWorkflowRunsQuery::new()
            .with_name(name)
            .with_status(status)
            .with_limit(limit)
            .map_err(|refused| PyValueError::new_err(refused.reason))?;
        Ok(Self { query })
    }
}

/// The group column of its wire value: `worker`, `task_name` or `queue`.
/// Pure.
fn group_column(value: &str) -> Result<TaskGroupBy, String> {
    TaskGroupBy::ALL
        .into_iter()
        .find(|group| group.as_str() == value)
        .ok_or_else(|| format!("group_by: {value:?} is not 'worker', 'task_name' or 'queue'"))
}

/// The history window of two aware bounds. A naive bound raises
/// `TypeError`; bounds that are not increasing, or more than the maximum
/// apart, raise `ValueError`.
pub fn history_window(
    lower: &Bound<'_, PyAny>,
    upper: &Bound<'_, PyAny>,
) -> PyResult<HistoryWindow> {
    HistoryWindow::new(utc_instant(lower)?, utc_instant(upper)?)
        .map_err(|refused| PyValueError::new_err(refused.to_string()))
}

/// The task statuses of their wire values, or the first value that is not
/// one. Pure.
pub fn task_statuses(values: &[String]) -> Result<Vec<TaskStatus>, String> {
    values
        .iter()
        .map(|value| {
            value
                .parse::<TaskStatus>()
                .map_err(|_| format!("statuses: {value:?} is not a task status"))
        })
        .collect()
}

/// The error categories of their wire values, or the first value that is
/// not one. Pure.
pub fn error_categories_of(values: &[String]) -> Result<Vec<ErrorCategory>, String> {
    values
        .iter()
        .map(|value| {
            ErrorCategory::ALL
                .into_iter()
                .find(|category| category.as_str() == value.as_str())
                .ok_or_else(|| format!("error_categories: {value:?} is not an error category"))
        })
        .collect()
}

/// The sort key of its wire value. Pure.
fn sort_field(value: &str) -> Result<TaskSortField, String> {
    TaskSortField::ALL
        .into_iter()
        .find(|field| field.as_str() == value)
        .ok_or_else(|| format!("sort_by: {value:?} is not a sort key"))
}

/// The sort direction of its wire value, `asc` or `desc`. Pure.
fn sort_direction(value: &str) -> Result<SortDirection, String> {
    SortDirection::ALL
        .into_iter()
        .find(|direction| direction.as_str() == value)
        .ok_or_else(|| format!("sort_dir: {value:?} is not 'asc' or 'desc'"))
}

/// The record of a failure of the read `operation` that the core reports.
pub fn read_failed(operation: &'static str, error: MonitoringQueryError) -> MonitoringFailed {
    MonitoringFailed {
        code: error.code().wire_value().to_owned(),
        message: error.message().to_owned(),
        retryable: error.retryable(),
        exception: FailureException::of(operation, &error),
    }
}

/// The record of a failed connect of the handle's broker: no statement ran.
/// The message names the read, as the core's read failures do.
pub fn connect_failed(operation: &'static str, error: &BrokerError) -> MonitoringFailed {
    MonitoringFailed {
        code: MonitoringQueryErrorCode::DbOperationFailed
            .wire_value()
            .to_owned(),
        message: format!("{operation} failed: {error}"),
        retryable: error.is_retryable(),
        exception: FailureException::of(operation, error),
    }
}

/// The record of the JSON text of the core model `model`. A model that does
/// not encode is a defect of the core: the operation panics with the
/// model's name.
pub fn model_json(model: &str, encoded: Result<String, serde_json::Error>) -> MonitoringJson {
    match encoded {
        Ok(json) => MonitoringJson { json },
        Err(error) => panic!("the monitoring model {model} does not encode as JSON: {error}"),
    }
}

/// The JSON text of a read's core model, or the error of the read.
type EncodedRead = Result<Result<String, serde_json::Error>, MonitoringQueryError>;

/// Connect the handle's broker, run `read` on it, and give the JSON text of
/// the model `model`, or the failure of the connect or of the read.
async fn json_read<F, Fut>(
    connection: BrokerConnection,
    operation: &'static str,
    model: &'static str,
    read: F,
) -> ReadOutcome
where
    F: FnOnce(Arc<PostgresBroker>) -> Fut,
    Fut: Future<Output = EncodedRead>,
{
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return ReadOutcome::Failed(connect_failed(operation, &error)),
    };
    match read(broker).await {
        Ok(encoded) => ReadOutcome::Json(model_json(model, encoded)),
        Err(error) => ReadOutcome::Failed(read_failed(operation, error)),
    }
}

/// The count of each task status: the JSON of `Vec<StatusCount>`.
pub async fn task_stats_operation(
    connection: BrokerConnection,
    query: CoreTaskStatsQuery,
) -> ReadOutcome {
    json_read(
        connection,
        "task stats query",
        "Vec<StatusCount>",
        move |broker| async move {
            task_stats(&broker, &query)
                .await
                .map(|counts| serde_json::to_string(&counts))
        },
    )
    .await
}

/// The task counts per group: the JSON of `Breakdown`.
pub async fn task_breakdown_operation(
    connection: BrokerConnection,
    query: CoreTaskBreakdownQuery,
) -> ReadOutcome {
    json_read(
        connection,
        "task breakdown query",
        "Breakdown",
        move |broker| async move {
            task_breakdown(&broker, &query)
                .await
                .map(|breakdown| serde_json::to_string(&breakdown))
        },
    )
    .await
}

/// The names of the root runs: the JSON of `Vec<String>`.
pub async fn workflow_names_operation(connection: BrokerConnection) -> ReadOutcome {
    json_read(
        connection,
        "workflow names query",
        "Vec<String>",
        |broker| async move {
            list_workflow_names(&broker)
                .await
                .map(|names| serde_json::to_string(&names))
        },
    )
    .await
}

/// The newest root runs: the JSON of `Vec<WorkflowRunSummary>`.
pub async fn workflow_runs_operation(
    connection: BrokerConnection,
    query: CoreWorkflowRunsQuery,
) -> ReadOutcome {
    json_read(
        connection,
        "workflow runs query",
        "Vec<WorkflowRunSummary>",
        move |broker| async move {
            list_workflow_runs(&broker, &query)
                .await
                .map(|runs| serde_json::to_string(&runs))
        },
    )
    .await
}

/// One run with its nodes and edges: the JSON of
/// `Option<WorkflowRunDetail>` (`null` when no run has the id).
pub async fn workflow_run_operation(
    connection: BrokerConnection,
    workflow_id: Uuid,
) -> ReadOutcome {
    json_read(
        connection,
        "workflow run detail query",
        "Option<WorkflowRunDetail>",
        move |broker| async move {
            get_workflow_run(&broker, workflow_id)
                .await
                .map(|run| serde_json::to_string(&run))
        },
    )
    .await
}

/// One node with its task: the JSON of `Option<WorkflowTaskDetail>`
/// (`null` when the run has no node at the index).
pub async fn workflow_node_operation(
    connection: BrokerConnection,
    workflow_id: Uuid,
    task_index: i32,
) -> ReadOutcome {
    json_read(
        connection,
        "workflow node detail query",
        "Option<WorkflowTaskDetail>",
        move |broker| async move {
            get_workflow_node(&broker, workflow_id, task_index)
                .await
                .map(|node| serde_json::to_string(&node))
        },
    )
    .await
}

/// Every schedule state: the JSON of `Vec<ScheduleStateInfo>`.
pub async fn schedules_operation(connection: BrokerConnection) -> ReadOutcome {
    json_read(
        connection,
        "schedule state query",
        "Vec<ScheduleStateInfo>",
        |broker| async move {
            list_schedules(&broker)
                .await
                .map(|schedules| serde_json::to_string(&schedules))
        },
    )
    .await
}

/// One page of the task list.
pub async fn list_tasks_operation(
    connection: BrokerConnection,
    query: CoreTaskListQuery,
) -> ReadOutcome {
    let operation = "task list query";
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return ReadOutcome::Failed(connect_failed(operation, &error)),
    };
    match list_tasks(&broker, &query).await {
        Ok(page) => ReadOutcome::Json(model_json("TaskListPage", serde_json::to_string(&page))),
        Err(error) => ReadOutcome::Failed(read_failed(operation, error)),
    }
}

/// The facets of one scope.
pub async fn task_facets_operation(
    connection: BrokerConnection,
    query: CoreTaskFacetsQuery,
) -> ReadOutcome {
    let operation = "task facets query";
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return ReadOutcome::Failed(connect_failed(operation, &error)),
    };
    match task_facets(&broker, &query).await {
        Ok(facets) => ReadOutcome::Json(model_json("Facets", serde_json::to_string(&facets))),
        Err(error) => ReadOutcome::Failed(read_failed(operation, error)),
    }
}

/// The outcome of a detail read. Pure.
fn detail_outcome(read: Result<TaskDetailRead, MonitoringQueryError>) -> DetailOutcome {
    match read {
        Ok(TaskDetailRead::Found(detail)) => {
            DetailOutcome::Json(model_json("TaskDetail", serde_json::to_string(&detail)))
        }
        Ok(TaskDetailRead::NotFound(absent)) => DetailOutcome::Absent(TaskDetailAbsent {
            task_id: absent.task_id,
            reason: absent.reason.wire_value().to_owned(),
        }),
        Err(error) => DetailOutcome::Failed(read_failed(DETAIL_SUBJECT, error)),
    }
}

/// One task with its attempts, or the reason it is absent.
pub async fn task_detail_operation(connection: BrokerConnection, task_id: Uuid) -> DetailOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return DetailOutcome::Failed(connect_failed(DETAIL_SUBJECT, &error)),
    };
    detail_outcome(get_task_detail(&broker, task_id).await)
}

#[cfg(test)]
mod tests {
    use horsies_core::monitoring::{TaskAbsenceReason, TaskDetailNotFound};

    use super::*;

    #[test]
    fn statuses_and_categories_are_read_by_their_wire_values() {
        assert_eq!(
            task_statuses(&["PENDING".to_owned(), "FAILED".to_owned()]),
            Ok(vec![TaskStatus::Pending, TaskStatus::Failed])
        );
        let refused = task_statuses(&["pending".to_owned()]).expect_err("the wire value is upper");
        assert!(refused.contains("\"pending\""), "{refused}");
        assert_eq!(
            error_categories_of(&["DOMAIN".to_owned(), "OUTCOME".to_owned()]),
            Ok(vec![ErrorCategory::Domain, ErrorCategory::Outcome])
        );
        assert!(error_categories_of(&["OTHER".to_owned()]).is_err());
    }

    #[test]
    fn every_group_column_has_its_wire_value() {
        for group in TaskGroupBy::ALL {
            assert_eq!(group_column(group.as_str()), Ok(group));
        }
        assert!(group_column("status").is_err());
    }

    #[test]
    fn every_sort_key_and_direction_has_its_wire_value() {
        for field in TaskSortField::ALL {
            assert_eq!(sort_field(field.as_str()), Ok(field));
        }
        for direction in SortDirection::ALL {
            assert_eq!(sort_direction(direction.as_str()), Ok(direction));
        }
        assert!(sort_field("id").is_err());
        assert!(sort_direction("DESC").is_err());
    }

    #[test]
    fn an_absent_task_keeps_its_id_and_the_wire_value_of_its_reason() {
        let task_id = Uuid::from_u128(7);
        let outcome = detail_outcome(Ok(TaskDetailRead::NotFound(TaskDetailNotFound {
            task_id,
            reason: TaskAbsenceReason::PredatesRetainedHistory,
        })));
        match outcome {
            DetailOutcome::Absent(absent) => {
                assert_eq!(absent.task_id, task_id);
                assert_eq!(absent.reason, "PREDATES_RETAINED_HISTORY");
            }
            other => panic!("expected the absence, got {other:?}"),
        }
    }

    #[test]
    fn a_connect_failure_is_a_database_failure_that_names_the_read() {
        let failed = connect_failed(
            "task list query",
            &BrokerError::ClaimContract("planned failure".to_owned()),
        );
        assert_eq!(failed.code, "DB_OPERATION_FAILED");
        assert!(
            failed.message.starts_with("task list query failed: "),
            "{}",
            failed.message
        );
        assert!(!failed.retryable);
    }
}

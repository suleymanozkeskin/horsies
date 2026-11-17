//! The monitoring reads through the core (`horsies_core::monitoring`).
//!
//! A read returns the JSON text of its core model (`MonitoringJson`); the
//! Python client validates it into its Pydantic model with one
//! `model_validate_json`, and the web handler writes the same text. An
//! expected failure is `MonitoringFailed` with its catalog code. The task
//! detail of an id that no table holds is `TaskDetailAbsent`. Every read
//! sends one statement in autocommit and writes nothing; it does not install
//! the schema, as Python's monitoring reads do not.

use horsies_core::catalog::CodeEnum;
use horsies_core::monitoring::{
    get_task_detail, list_tasks, task_facets, ErrorCategory, HistoryWindow, MonitoringQueryError,
    MonitoringQueryErrorCode, SortDirection, TaskDetailRead,
    TaskFacetsQuery as CoreTaskFacetsQuery, TaskFilters, TaskListQuery as CoreTaskListQuery,
    TaskSortField,
};
use horsies_core::{BrokerError, TaskStatus};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

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
}

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

/// The record of a read failure that the core reports.
pub fn read_failed(error: MonitoringQueryError) -> MonitoringFailed {
    MonitoringFailed {
        code: error.code.wire_value().to_owned(),
        message: error.message,
        retryable: error.retryable,
    }
}

/// The record of a failed connect of the handle's broker: no statement ran.
/// The message names the read, as the core's read failures do.
pub fn connect_failed(operation: &str, error: &BrokerError) -> MonitoringFailed {
    MonitoringFailed {
        code: MonitoringQueryErrorCode::DbOperationFailed
            .wire_value()
            .to_owned(),
        message: format!("{operation} failed: {error}"),
        retryable: error.is_retryable(),
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
        Err(error) => ReadOutcome::Failed(read_failed(error)),
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
        Err(error) => ReadOutcome::Failed(read_failed(error)),
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
        Err(error) => DetailOutcome::Failed(read_failed(error)),
    }
}

/// One task with its attempts, or the reason it is absent.
pub async fn task_detail_operation(connection: BrokerConnection, task_id: Uuid) -> DetailOutcome {
    let operation = "task detail query";
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return DetailOutcome::Failed(connect_failed(operation, &error)),
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

//! The stale and expired task lists through the core:
//! `horsies_list_stale_tasks` and `horsies_list_expired_tasks`
//! (`PostgresBroker::get_stale_tasks`, `PostgresBroker::get_expired_tasks`).
//!
//! The outcome is `TaskListComplete`, `TaskListTruncated` (the first `bound`
//! rows; more rows match), or `BrokerFailure` with `INVALID_INPUT` (a stale
//! threshold that the rule refuses; nothing ran) or `MONITORING_QUERY_FAILED`.
//! Each row is the `dict` of Python's public API, with its keys in Python's
//! order and the task id as `uuid.UUID`. Each list sends one statement in
//! autocommit and writes nothing.

use chrono::TimeDelta;
use horsies_core::{
    BoundedTaskList, BrokerError, BrokerErrorCode, ExpiredTaskRow, StaleMinutes, StaleTaskRow,
    StaleThresholdRefusal,
};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyInt, PyList};
use pyo3::IntoPyObjectExt;

use crate::handle::BrokerConnection;
use crate::records::BrokerFailure;

/// Every matching row.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct TaskListComplete {
    /// One `dict` per row.
    pub rows: Py<PyList>,
}

/// The first `bound` matching rows; more rows match.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct TaskListTruncated {
    /// One `dict` per row.
    pub rows: Py<PyList>,
    pub bound: usize,
}

/// A row of a task list as the `dict` of Python's public API.
pub trait TaskListEntry {
    /// The `dict` of the row, with its keys in Python's order.
    fn entry<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>>;
}

impl TaskListEntry for StaleTaskRow {
    fn entry<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let entry = PyDict::new(py);
        entry.set_item("id", self.id)?;
        entry.set_item("worker_hostname", &self.worker_hostname)?;
        entry.set_item("worker_pid", self.worker_pid)?;
        entry.set_item("worker_process_name", &self.worker_process_name)?;
        entry.set_item("last_heartbeat", self.last_heartbeat)?;
        entry.set_item("started_at", self.started_at)?;
        entry.set_item("task_name", &self.task_name)?;
        Ok(entry)
    }
}

impl TaskListEntry for ExpiredTaskRow {
    fn entry<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let entry = PyDict::new(py);
        entry.set_item("id", self.id)?;
        entry.set_item("task_name", &self.task_name)?;
        entry.set_item("queue_name", &self.queue_name)?;
        entry.set_item("priority", self.priority)?;
        entry.set_item("sent_at", self.sent_at)?;
        entry.set_item("enqueued_at", self.enqueued_at)?;
        entry.set_item("good_until", self.good_until)?;
        entry.set_item("expired_for", TimeDelta::microseconds(self.expired_for_us))?;
        Ok(entry)
    }
}

/// The outcome of one task list read.
pub enum TaskListOutcome<T> {
    Listed(BoundedTaskList<T>),
    Failed(BrokerFailure),
}

fn entries<T: TaskListEntry>(py: Python<'_>, rows: &[T]) -> PyResult<Py<PyList>> {
    let dicts = rows
        .iter()
        .map(|row| row.entry(py))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(PyList::new(py, dicts)?.unbind())
}

impl<'py, T: TaskListEntry> IntoPyObject<'py> for TaskListOutcome<T> {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Listed(BoundedTaskList::Complete(rows)) => TaskListComplete {
                rows: entries(py, &rows)?,
            }
            .into_bound_py_any(py),
            Self::Listed(BoundedTaskList::Truncated { rows, bound }) => TaskListTruncated {
                rows: entries(py, &rows)?,
                bound,
            }
            .into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The stale threshold of a Python value, by Python's rule
/// (`_stale_threshold_minutes`): a `bool` and a value that is not an `int`
/// are refused before the range; an `int` beyond 64 bits is below 0 or
/// above the maximum by its sign. Reads the value only.
pub fn stale_threshold(
    value: &Bound<'_, PyAny>,
) -> PyResult<Result<StaleMinutes, StaleThresholdRefusal>> {
    if value.is_instance_of::<PyBool>() {
        return Ok(Err(StaleThresholdRefusal::Bool));
    }
    if !value.is_instance_of::<PyInt>() {
        let type_name = value.get_type().name()?.to_string();
        return Ok(Err(StaleThresholdRefusal::NotAnInteger { type_name }));
    }
    match value.extract::<i64>() {
        Ok(minutes) => Ok(StaleMinutes::parse(minutes)),
        Err(_) => match value.lt(0)? {
            true => Ok(Err(StaleThresholdRefusal::BelowZero)),
            false => Ok(Err(StaleThresholdRefusal::AboveMaximum)),
        },
    }
}

/// The failure of a list whose broker did not connect: no statement ran.
/// The message names the read, as the core's read failures do.
pub(crate) fn connect_failed(operation: &str, error: &BrokerError) -> BrokerFailure {
    BrokerFailure {
        code: BrokerErrorCode::MonitoringQueryFailed.to_string(),
        message: format!("{operation} failed: {error}"),
        retryable: error.is_retryable(),
    }
}

/// The RUNNING tasks whose latest runner beat, or their start, is older
/// than `threshold`.
pub async fn stale_tasks_operation(
    connection: BrokerConnection,
    threshold: StaleMinutes,
) -> TaskListOutcome<StaleTaskRow> {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return TaskListOutcome::Failed(connect_failed("get_stale_tasks", &error)),
    };
    match broker.get_stale_tasks(threshold.minutes()).await {
        Ok(list) => TaskListOutcome::Listed(list),
        Err(error) => TaskListOutcome::Failed(BrokerFailure::from(error)),
    }
}

/// The PENDING tasks whose `good_until` passed.
pub async fn expired_tasks_operation(
    connection: BrokerConnection,
) -> TaskListOutcome<ExpiredTaskRow> {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return TaskListOutcome::Failed(connect_failed("get_expired_tasks", &error)),
    };
    match broker.get_expired_tasks().await {
        Ok(list) => TaskListOutcome::Listed(list),
        Err(error) => TaskListOutcome::Failed(BrokerFailure::from(error)),
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::MAX_STALE_THRESHOLD_MINUTES;

    use super::*;

    fn threshold_of(source: &str) -> Result<StaleMinutes, StaleThresholdRefusal> {
        Python::initialize();
        Python::attach(|py| {
            let value = py
                .eval(&std::ffi::CString::new(source).expect("no NUL"), None, None)
                .expect("the expression evaluates");
            stale_threshold(&value).expect("the value reads")
        })
    }

    #[test]
    fn python_values_follow_python_rule_of_the_threshold() {
        assert_eq!(threshold_of("0").map(StaleMinutes::minutes), Ok(0));
        assert_eq!(
            threshold_of("525600").map(StaleMinutes::minutes),
            Ok(MAX_STALE_THRESHOLD_MINUTES)
        );
        assert_eq!(threshold_of("True"), Err(StaleThresholdRefusal::Bool));
        assert_eq!(
            threshold_of("'5'"),
            Err(StaleThresholdRefusal::NotAnInteger {
                type_name: "str".to_owned()
            })
        );
        assert_eq!(
            threshold_of("2.0"),
            Err(StaleThresholdRefusal::NotAnInteger {
                type_name: "float".to_owned()
            })
        );
        assert_eq!(threshold_of("-1"), Err(StaleThresholdRefusal::BelowZero));
        assert_eq!(
            threshold_of("525601"),
            Err(StaleThresholdRefusal::AboveMaximum)
        );
        assert_eq!(
            threshold_of("10 ** 5000"),
            Err(StaleThresholdRefusal::AboveMaximum)
        );
        assert_eq!(
            threshold_of("-(10 ** 5000)"),
            Err(StaleThresholdRefusal::BelowZero)
        );
    }

    #[test]
    fn a_connect_failure_names_the_list_and_is_a_monitoring_failure() {
        let failed = connect_failed(
            "get_expired_tasks",
            &BrokerError::ClaimContract("planned failure".to_owned()),
        );
        assert_eq!(failed.code, "MONITORING_QUERY_FAILED");
        assert!(
            failed.message.starts_with("get_expired_tasks failed: "),
            "{}",
            failed.message
        );
        assert!(!failed.retryable);
    }
}

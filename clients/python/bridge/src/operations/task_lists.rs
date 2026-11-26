//! The stale and expired task lists through the core:
//! `horsies_list_stale_tasks` and `horsies_list_expired_tasks`
//! (`PostgresBroker::get_stale_tasks`, `PostgresBroker::get_expired_tasks`).
//!
//! The outcome of each list is its own complete or truncated class (the
//! first `bound` rows; more rows match), or the generated broker error class
//! with `INVALID_INPUT` (a stale threshold that the rule refuses; nothing
//! ran) or `MONITORING_QUERY_FAILED`. Each row is a record with the fields
//! of the core's row; the task id is a `uuid.UUID`. Each list sends one
//! statement in autocommit and writes nothing.

use chrono::{DateTime, TimeDelta, Utc};
use horsies_core::{
    BoundedTaskList, BrokerError, BrokerOperationError, BrokerOperationSource, ExpiredTaskRow,
    StaleMinutes, StaleTaskRow, StaleThresholdRefusal,
};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyInt, PyList};
use pyo3::IntoPyObjectExt;

use crate::handle::BrokerConnection;
use crate::records::BrokerFailure;

/// One RUNNING task of the stale list.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StaleTaskRecord {
    pub task_id: uuid::Uuid,
    pub task_name: String,
    pub worker_hostname: Option<String>,
    pub worker_pid: Option<i32>,
    pub worker_process_name: Option<String>,
    pub started_at: DateTime<Utc>,
    /// The latest runner beat since `started_at`; `None` when the task has
    /// no such beat.
    pub last_heartbeat_at: Option<DateTime<Utc>>,
}

/// One PENDING task of the expired list.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ExpiredTaskRecord {
    pub task_id: uuid::Uuid,
    pub task_name: String,
    pub queue_name: String,
    pub priority: i32,
    pub sent_at: DateTime<Utc>,
    pub enqueued_at: DateTime<Utc>,
    pub good_until: DateTime<Utc>,
    /// How long ago `good_until` passed, on the database clock.
    pub expired_for: TimeDelta,
}

/// Every matching row of the stale list.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct StaleTaskListComplete {
    /// One `StaleTaskRecord` per row.
    pub rows: Py<PyList>,
}

/// The first `bound` matching rows of the stale list; more rows match.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct StaleTaskListTruncated {
    /// One `StaleTaskRecord` per row.
    pub rows: Py<PyList>,
    pub bound: usize,
}

/// Every matching row of the expired list.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct ExpiredTaskListComplete {
    /// One `ExpiredTaskRecord` per row.
    pub rows: Py<PyList>,
}

/// The first `bound` matching rows of the expired list; more rows match.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct ExpiredTaskListTruncated {
    /// One `ExpiredTaskRecord` per row.
    pub rows: Py<PyList>,
    pub bound: usize,
}

impl From<StaleTaskRow> for StaleTaskRecord {
    fn from(row: StaleTaskRow) -> Self {
        Self {
            task_id: row.task_id,
            task_name: row.task_name,
            worker_hostname: row.worker_hostname,
            worker_pid: row.worker_pid,
            worker_process_name: row.worker_process_name,
            started_at: row.started_at,
            last_heartbeat_at: row.last_heartbeat_at,
        }
    }
}

impl From<ExpiredTaskRow> for ExpiredTaskRecord {
    fn from(row: ExpiredTaskRow) -> Self {
        Self {
            task_id: row.task_id,
            task_name: row.task_name,
            queue_name: row.queue_name,
            priority: row.priority,
            sent_at: row.sent_at,
            enqueued_at: row.enqueued_at,
            good_until: row.good_until,
            // The core reads the interval from whole microseconds of an
            // `i64`, which a `TimeDelta` always holds.
            expired_for: TimeDelta::from_std(row.expired_for)
                .expect("an interval of i64 microseconds fits a TimeDelta"),
        }
    }
}

/// A row of a task list and the Python classes of its list.
pub trait TaskListRow: Sized {
    /// The record of the row.
    type Record: for<'py> IntoPyObject<'py> + From<Self>;

    /// The Python object of every matching row, `rows` the list of records.
    fn complete(py: Python<'_>, rows: Py<PyList>) -> PyResult<Bound<'_, PyAny>>;

    /// The Python object of the first `bound` matching rows.
    fn truncated(py: Python<'_>, rows: Py<PyList>, bound: usize) -> PyResult<Bound<'_, PyAny>>;
}

impl TaskListRow for StaleTaskRow {
    type Record = StaleTaskRecord;

    fn complete(py: Python<'_>, rows: Py<PyList>) -> PyResult<Bound<'_, PyAny>> {
        StaleTaskListComplete { rows }.into_bound_py_any(py)
    }

    fn truncated(py: Python<'_>, rows: Py<PyList>, bound: usize) -> PyResult<Bound<'_, PyAny>> {
        StaleTaskListTruncated { rows, bound }.into_bound_py_any(py)
    }
}

impl TaskListRow for ExpiredTaskRow {
    type Record = ExpiredTaskRecord;

    fn complete(py: Python<'_>, rows: Py<PyList>) -> PyResult<Bound<'_, PyAny>> {
        ExpiredTaskListComplete { rows }.into_bound_py_any(py)
    }

    fn truncated(py: Python<'_>, rows: Py<PyList>, bound: usize) -> PyResult<Bound<'_, PyAny>> {
        ExpiredTaskListTruncated { rows, bound }.into_bound_py_any(py)
    }
}

/// The outcome of one task list read.
pub enum TaskListOutcome<T> {
    Listed(BoundedTaskList<T>),
    Failed(BrokerFailure),
}

/// The records of `rows`, in list order.
fn records<T: TaskListRow>(py: Python<'_>, rows: Vec<T>) -> PyResult<Py<PyList>> {
    let records = rows.into_iter().map(T::Record::from).collect::<Vec<_>>();
    Ok(PyList::new(py, records)?.unbind())
}

impl<'py, T: TaskListRow> IntoPyObject<'py> for TaskListOutcome<T> {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Listed(BoundedTaskList::Complete(rows)) => T::complete(py, records(py, rows)?),
            Self::Listed(BoundedTaskList::Truncated { rows, bound }) => {
                T::truncated(py, records(py, rows)?, bound)
            }
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The stale threshold of a Python value, by Python's rule
/// (`threshold_minutes`): a `bool` and a value that is not an `int`
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
/// `MONITORING_QUERY_FAILED` with Python's message of the read (`{operation}
/// failed: {error}`); the read is the subject of the exception.
pub(crate) fn connect_failed(operation: &'static str, error: BrokerError) -> BrokerFailure {
    BrokerFailure::of_operation_error(
        operation,
        BrokerOperationError::monitoring_query_failed(
            operation,
            BrokerOperationSource::Broker(error),
        ),
    )
}

/// The RUNNING tasks whose latest runner beat, or their start, is older
/// than `threshold`.
pub async fn stale_tasks_operation(
    connection: BrokerConnection,
    threshold: StaleMinutes,
) -> TaskListOutcome<StaleTaskRow> {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return TaskListOutcome::Failed(connect_failed("get_stale_tasks", error)),
    };
    match broker.get_stale_tasks(threshold.minutes()).await {
        Ok(list) => TaskListOutcome::Listed(list),
        Err(error) => {
            TaskListOutcome::Failed(BrokerFailure::of_operation_error("get_stale_tasks", error))
        }
    }
}

/// The PENDING tasks whose `good_until` passed.
pub async fn expired_tasks_operation(
    connection: BrokerConnection,
) -> TaskListOutcome<ExpiredTaskRow> {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return TaskListOutcome::Failed(connect_failed("get_expired_tasks", error)),
    };
    match broker.get_expired_tasks().await {
        Ok(list) => TaskListOutcome::Listed(list),
        Err(error) => TaskListOutcome::Failed(BrokerFailure::of_operation_error(
            "get_expired_tasks",
            error,
        )),
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
            BrokerError::ClaimContract("planned failure".to_owned()),
        );
        assert_eq!(failed.subject, "get_expired_tasks");
        assert_eq!(
            failed.error.code(),
            horsies_core::BrokerErrorCode::MonitoringQueryFailed
        );
        assert_eq!(
            failed.error.message(),
            "get_expired_tasks failed: claim contract violation: planned failure"
        );
        assert!(!failed.error.retryable());
    }

    /// The task id of the list rows of these tests.
    const TASK_ID: &str = "0198c0de-0000-7000-8000-000000000001";

    /// 2026-01-01T00:00:00Z, the time columns of the list rows.
    fn started() -> chrono::DateTime<chrono::Utc> {
        use chrono::TimeZone;
        chrono::Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("a valid instant")
    }

    /// Run the Python `checks` with `outcome` bound to the Python object of
    /// `list`, and the modules `uuid` and `datetime`.
    fn check_outcome<T: TaskListRow>(list: BoundedTaskList<T>, checks: &std::ffi::CStr) {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let globals = pyo3::types::PyDict::new(py);
            globals.set_item("outcome", TaskListOutcome::Listed(list).into_pyobject(py)?)?;
            globals.set_item("uuid", py.import("uuid")?)?;
            globals.set_item("datetime", py.import("datetime")?)?;
            py.run(checks, Some(&globals), None)
        })
        .expect("the checks pass");
    }

    fn stale_row() -> StaleTaskRow {
        StaleTaskRow {
            task_id: uuid::Uuid::parse_str(TASK_ID).expect("a valid task id"),
            task_name: "slow_task".to_owned(),
            worker_hostname: Some("host-1".to_owned()),
            worker_pid: Some(1234),
            worker_process_name: Some("child-1".to_owned()),
            started_at: started(),
            last_heartbeat_at: None,
        }
    }

    /// A complete stale list is its class, with one record per row: the
    /// task id as `uuid.UUID`, the fields of the core's row.
    #[test]
    fn a_complete_stale_list_holds_one_record_per_row() {
        check_outcome(
            BoundedTaskList::Complete(vec![stale_row()]),
            c"started = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
assert type(outcome).__name__ == 'StaleTaskListComplete', type(outcome)
[row] = outcome.rows
assert type(row).__name__ == 'StaleTaskRecord', type(row)
assert row.task_id == uuid.UUID('0198c0de-0000-7000-8000-000000000001'), row.task_id
assert type(row.task_id) is uuid.UUID, type(row.task_id)
assert row.task_name == 'slow_task'
assert row.worker_hostname == 'host-1'
assert row.worker_pid == 1234
assert row.worker_process_name == 'child-1'
assert row.started_at == started
assert row.last_heartbeat_at is None
",
        );
    }

    /// A truncated expired list is its class, with the bound, and each row
    /// has `expired_for` as a `timedelta`.
    #[test]
    fn a_truncated_expired_list_holds_its_bound_and_records() {
        let row = ExpiredTaskRow {
            task_id: uuid::Uuid::parse_str(TASK_ID).expect("a valid task id"),
            task_name: "slow_task".to_owned(),
            queue_name: "default".to_owned(),
            priority: 100,
            sent_at: started(),
            enqueued_at: started(),
            good_until: started(),
            expired_for: std::time::Duration::from_secs(300),
        };

        check_outcome(
            BoundedTaskList::Truncated {
                rows: vec![row],
                bound: 1,
            },
            c"started = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
assert type(outcome).__name__ == 'ExpiredTaskListTruncated', type(outcome)
assert outcome.bound == 1
[row] = outcome.rows
assert type(row).__name__ == 'ExpiredTaskRecord', type(row)
assert row.task_id == uuid.UUID('0198c0de-0000-7000-8000-000000000001'), row.task_id
assert row.task_name == 'slow_task'
assert row.queue_name == 'default'
assert row.priority == 100
assert row.sent_at == started
assert row.enqueued_at == started
assert row.good_until == started
assert row.expired_for == datetime.timedelta(minutes=5), row.expired_for
",
        );
    }
}

//! The stale and expired task lists through the core:
//! `horsies_list_stale_tasks` and `horsies_list_expired_tasks`
//! (`PostgresBroker::get_stale_tasks`, `PostgresBroker::get_expired_tasks`).
//!
//! The outcome is `TaskListComplete`, `TaskListTruncated` (the first `bound`
//! rows; more rows match), or the generated broker error class with `INVALID_INPUT` (a stale
//! threshold that the rule refuses; nothing ran) or `MONITORING_QUERY_FAILED`.
//! Each row is the `dict` of Python's public API, with its keys in Python's
//! order and the task id as `uuid.UUID`. Each list sends one statement in
//! autocommit and writes nothing.

use chrono::TimeDelta;
use horsies_core::{
    BoundedTaskList, BrokerError, BrokerOperationError, BrokerOperationSource, ExpiredTaskRow,
    StaleMinutes, StaleTaskRow, StaleThresholdRefusal,
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

    /// Run the Python `checks` with `entry` bound to the dict of `row`, and
    /// the modules `uuid` and `datetime`.
    fn check_entry(row: &impl TaskListEntry, checks: &std::ffi::CStr) {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let globals = PyDict::new(py);
            globals.set_item("entry", row.entry(py)?)?;
            globals.set_item("uuid", py.import("uuid")?)?;
            globals.set_item("datetime", py.import("datetime")?)?;
            py.run(checks, Some(&globals), None)
        })
        .expect("the checks pass");
    }

    /// Python `tests/unit/test_broker_task_lists.py::TestStaleList::test_rows_become_the_dicts_of_the_client_statement`:
    /// a stale row is the dict of Python's public API, with its keys in
    /// Python's order and the task id as `uuid.UUID`.
    #[test]
    fn a_stale_row_is_the_dict_of_the_python_api() {
        let row = StaleTaskRow {
            id: uuid::Uuid::parse_str(TASK_ID).expect("a valid task id"),
            task_name: "slow_task".to_owned(),
            worker_hostname: Some("host-1".to_owned()),
            worker_pid: Some(1234),
            worker_process_name: Some("child-1".to_owned()),
            started_at: started(),
            last_heartbeat: None,
        };

        check_entry(
            &row,
            c"started = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
assert entry == {
    'id': uuid.UUID('0198c0de-0000-7000-8000-000000000001'),
    'worker_hostname': 'host-1',
    'worker_pid': 1234,
    'worker_process_name': 'child-1',
    'last_heartbeat': None,
    'started_at': started,
    'task_name': 'slow_task',
}, entry
assert list(entry) == [
    'id',
    'worker_hostname',
    'worker_pid',
    'worker_process_name',
    'last_heartbeat',
    'started_at',
    'task_name',
], list(entry)
assert type(entry['id']) is uuid.UUID, type(entry['id'])
",
        );
    }

    /// Python `tests/unit/test_broker_task_lists.py::TestExpiredList::test_rows_become_the_dicts_of_the_client_statement`:
    /// an expired row is the dict of Python's public API, with its keys in
    /// Python's order, the task id as `uuid.UUID` and `expired_for` as a
    /// `timedelta`.
    #[test]
    fn an_expired_row_is_the_dict_of_the_python_api() {
        let row = ExpiredTaskRow {
            id: uuid::Uuid::parse_str(TASK_ID).expect("a valid task id"),
            task_name: "slow_task".to_owned(),
            queue_name: "default".to_owned(),
            priority: 100,
            sent_at: started(),
            enqueued_at: started(),
            good_until: started(),
            expired_for_us: 300_000_000,
        };

        check_entry(
            &row,
            c"started = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
assert list(entry) == [
    'id',
    'task_name',
    'queue_name',
    'priority',
    'sent_at',
    'enqueued_at',
    'good_until',
    'expired_for',
], list(entry)
assert entry == {
    'id': uuid.UUID('0198c0de-0000-7000-8000-000000000001'),
    'task_name': 'slow_task',
    'queue_name': 'default',
    'priority': 100,
    'sent_at': started,
    'enqueued_at': started,
    'good_until': started,
    'expired_for': datetime.timedelta(minutes=5),
}, entry
assert type(entry['id']) is uuid.UUID, type(entry['id'])
",
        );
    }
}

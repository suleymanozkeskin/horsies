//! The task info read: one task's live row, or its history record, with the
//! parts the caller asked for.

use chrono::{DateTime, Utc};
use horsies_core::catalog::CodeEnum;
use horsies_core::{BrokerErrorCode, ExecutionClass, TaskAttemptInfo, TaskInfo, TaskInfoRead};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::handle::BrokerConnection;
use crate::records::BrokerFailure;

/// The optional parts of a task info read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskInfoParts {
    pub result: bool,
    pub failed_reason: bool,
    pub attempts: bool,
}

/// One execution attempt of a task.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct TaskAttemptRecord {
    pub task_id: Uuid,
    pub attempt: i32,
    /// A member of `TaskAttemptOutcome`, as its wire value.
    pub outcome: String,
    pub will_retry: bool,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub failed_reason: Option<String>,
    pub worker_id: Option<String>,
    pub worker_hostname: Option<String>,
    pub worker_pid: Option<i32>,
    pub worker_process_name: Option<String>,
}

impl From<TaskAttemptInfo> for TaskAttemptRecord {
    fn from(attempt: TaskAttemptInfo) -> Self {
        Self {
            task_id: attempt.task_id,
            attempt: attempt.attempt,
            outcome: attempt.outcome.wire_value().to_owned(),
            will_retry: attempt.will_retry,
            started_at: attempt.started_at,
            finished_at: attempt.finished_at,
            error_code: attempt.error_code,
            error_message: attempt.error_message,
            failed_reason: attempt.failed_reason,
            worker_id: attempt.worker_id,
            worker_hostname: attempt.worker_hostname,
            worker_pid: attempt.worker_pid,
            worker_process_name: attempt.worker_process_name,
        }
    }
}

/// The metadata of one task, with the fields of Python's `TaskInfo`.
/// `result_json` is the stored result envelope as JSON text, when the read
/// asked for it and the task has one; `attempts` is a list when the read
/// asked for attempts. `execution` is the stored class of a live task
/// (`ISOLATED`, `SHARED_ASYNC` or `SHARED_THREAD`), and `None` for a task
/// read from history, which does not keep the class (Python
/// `TaskInfo.execution`).
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct TaskInfoRecord {
    pub task_id: Uuid,
    pub task_name: String,
    /// A member of `TaskStatus`, as its wire value.
    pub status: String,
    pub queue_name: String,
    pub priority: i32,
    pub retry_count: u32,
    pub max_retries: u32,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub sent_at: Option<DateTime<Utc>>,
    pub enqueued_at: DateTime<Utc>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub worker_hostname: Option<String>,
    pub worker_pid: Option<u32>,
    pub worker_process_name: Option<String>,
    pub error_code: Option<String>,
    pub failed_reason: Option<String>,
    pub result_json: Option<String>,
    pub attempts: Option<Vec<TaskAttemptRecord>>,
    pub execution: Option<String>,
}

impl TaskInfoRecord {
    /// The record of a core `TaskInfo`. Fails only when the result does not
    /// serialize, which a value read from JSON cannot do.
    fn from_core(info: TaskInfo) -> Result<Self, serde_json::Error> {
        let result_json = match &info.result {
            Some(result) => Some(serde_json::to_string(result)?),
            None => None,
        };
        let attempts = info
            .attempts
            .map(|attempts| attempts.into_iter().map(TaskAttemptRecord::from).collect());
        Ok(Self {
            task_id: info.task_id,
            task_name: info.task_name,
            status: info.status.wire_value().to_owned(),
            queue_name: info.queue_name,
            priority: info.priority,
            retry_count: info.retry_count,
            max_retries: info.max_retries,
            next_retry_at: info.next_retry_at,
            sent_at: info.sent_at,
            enqueued_at: info.enqueued_at,
            claimed_at: info.claimed_at,
            started_at: info.started_at,
            completed_at: info.completed_at,
            failed_at: info.failed_at,
            worker_hostname: info.worker_hostname,
            worker_pid: info.worker_pid,
            worker_process_name: info.worker_process_name,
            error_code: info.error_code,
            failed_reason: info.failed_reason,
            result_json,
            attempts,
            execution: execution_of(info.execution),
        })
    }
}

/// The reported class of a task: the stored class name of a live task, or
/// `None` for a task read from history. Pure.
fn execution_of(execution: ExecutionClass) -> Option<String> {
    match execution {
        ExecutionClass::Live(class) => Some(class.as_str().to_owned()),
        ExecutionClass::NotKeptInHistory => None,
    }
}

/// Neither the live table nor the task history holds the task. The core's
/// absence reason is not kept: Python's `TASK_NOT_FOUND` carries the task id
/// only.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct TaskInfoNotFound {
    pub task_id: Uuid,
}

/// The outcome of one task info read.
pub enum TaskInfoOutcome {
    /// Boxed: the record is many times larger than the other variants.
    Found(Box<TaskInfoRecord>),
    NotFound(TaskInfoNotFound),
    Failed(BrokerFailure),
}

impl<'py> IntoPyObject<'py> for TaskInfoOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Found(record) => (*record).into_bound_py_any(py),
            Self::NotFound(absent) => absent.into_bound_py_any(py),
            Self::Failed(failure) => failure.into_bound_py_any(py),
        }
    }
}

/// Read the info of one task. Installs the schema once per handle first, as
/// Python's `get_task_info_async`; a failed connect or install is
/// `TASK_INFO_QUERY_FAILED`, as Python reports any failure of the read.
/// Then reads only.
pub async fn task_info_operation(
    connection: BrokerConnection,
    task_id: Uuid,
    parts: TaskInfoParts,
) -> TaskInfoOutcome {
    let broker = match connection.initialized_broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return TaskInfoOutcome::Failed(BrokerFailure::from_broker_error(
                BrokerErrorCode::TaskInfoQueryFailed,
                &error,
            ))
        }
    };
    let read = broker
        .get_task_info_with_attempts(task_id, parts.result, parts.failed_reason, parts.attempts)
        .await;
    match read {
        Ok(TaskInfoRead::Found(info)) => match TaskInfoRecord::from_core(*info) {
            Ok(record) => TaskInfoOutcome::Found(Box::new(record)),
            Err(error) => TaskInfoOutcome::Failed(BrokerFailure {
                code: BrokerErrorCode::InvalidJsonPayload.to_string(),
                message: format!("the stored result of task {task_id} does not serialize: {error}"),
                retryable: false,
            }),
        },
        Ok(TaskInfoRead::Absent { reason: _ }) => {
            TaskInfoOutcome::NotFound(TaskInfoNotFound { task_id })
        }
        Err(error) => TaskInfoOutcome::Failed(BrokerFailure::from(error)),
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::Execution;

    use super::*;

    #[test]
    fn a_live_task_reports_its_stored_class() {
        for (class, name) in [
            (Execution::Isolated, "ISOLATED"),
            (Execution::SharedAsync, "SHARED_ASYNC"),
            (Execution::SharedThread, "SHARED_THREAD"),
        ] {
            assert_eq!(
                execution_of(ExecutionClass::Live(class)).as_deref(),
                Some(name)
            );
        }
    }

    #[test]
    fn a_task_read_from_history_reports_no_class() {
        assert_eq!(execution_of(ExecutionClass::NotKeptInHistory), None);
    }
}

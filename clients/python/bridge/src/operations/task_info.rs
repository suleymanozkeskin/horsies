//! The task info read: one task's live row, or its history record, with the
//! parts the caller asked for.

use chrono::{DateTime, Utc};
use horsies_core::catalog::CodeEnum;
use horsies_core::{ExecutionClass, StoredEnvelopeText, TaskAttemptInfo, TaskInfo, TaskInfoRead};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::handle::BrokerConnection;
use crate::records::BrokerFailure;

/// The subject of the exception of a failed read.
const SUBJECT: &str = "task_info";

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
/// `result_json` is the stored result envelope as JSON text, as stored, when
/// the read asked for it and the task has one; `attempts` is a list when the
/// read asked for attempts. `execution` is the stored class of a live task
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
    pub queue: String,
    pub priority: i32,
    pub retry_count: u32,
    pub max_retries: u32,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub sent_at: Option<DateTime<Utc>>,
    pub due_at: DateTime<Utc>,
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
    /// The record of a core `TaskInfo`. The stored result passes as its
    /// stored text: the typed decode is the app's (Python
    /// `Horsies.get_task_info_async`). Pure.
    fn from_core(info: TaskInfo<StoredEnvelopeText>) -> Self {
        let result_json = info.result.map(|envelope| envelope.as_str().to_owned());
        let attempts = info
            .attempts
            .map(|attempts| attempts.into_iter().map(TaskAttemptRecord::from).collect());
        Self {
            task_id: info.task_id,
            task_name: info.task_name,
            status: info.status.wire_value().to_owned(),
            queue: info.queue,
            priority: info.priority,
            retry_count: info.retry_count,
            max_retries: info.max_retries,
            next_retry_at: info.next_retry_at,
            sent_at: info.sent_at,
            due_at: info.due_at,
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
        }
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
            return TaskInfoOutcome::Failed(BrokerFailure::of_operation_error(
                SUBJECT,
                horsies_core::BrokerOperationError::task_info_query_failed(
                    task_id,
                    horsies_core::BrokerOperationSource::Broker(error),
                ),
            ))
        }
    };
    let read = broker
        .get_task_info_with_attempts(task_id, parts.result, parts.failed_reason, parts.attempts)
        .await;
    match read {
        Ok(TaskInfoRead::Found(info)) => {
            TaskInfoOutcome::Found(Box::new(TaskInfoRecord::from_core(*info)))
        }
        Ok(TaskInfoRead::Absent { reason: _ }) => {
            TaskInfoOutcome::NotFound(TaskInfoNotFound { task_id })
        }
        Err(error) => TaskInfoOutcome::Failed(BrokerFailure::of_operation_error(SUBJECT, error)),
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::{Execution, TaskStatus};

    use super::*;

    fn history_info(result: Option<StoredEnvelopeText>) -> TaskInfo<StoredEnvelopeText> {
        TaskInfo {
            task_id: Uuid::nil(),
            task_name: "failed_task".to_owned(),
            status: TaskStatus::Failed,
            queue: "default".to_owned(),
            priority: 100,
            retry_count: 0,
            max_retries: 0,
            next_retry_at: None,
            sent_at: None,
            due_at: Utc::now(),
            claimed_at: None,
            started_at: None,
            completed_at: None,
            failed_at: Some(Utc::now()),
            worker_hostname: None,
            worker_pid: None,
            worker_process_name: None,
            error_code: Some("E2".to_owned()),
            failed_reason: None,
            result,
            attempts: None,
            execution: ExecutionClass::NotKeptInHistory,
        }
    }

    /// The stored result passes as stored: an error without its null keys
    /// keeps its keys, and nothing is written back.
    #[test]
    fn the_stored_result_passes_as_its_stored_text() {
        let stored = r#"{"__h_task_result__":true,"ok":null,"err":{"error_code":"E2"}}"#;
        let envelope = StoredEnvelopeText::parse(stored.to_owned())
            .expect("the text is strict JSON")
            .expect("the text is an object");
        let record = TaskInfoRecord::from_core(history_info(Some(envelope)));
        assert_eq!(record.result_json.as_deref(), Some(stored));
        assert_eq!(
            TaskInfoRecord::from_core(history_info(None)).result_json,
            None
        );
    }

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

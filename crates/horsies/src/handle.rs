//! The handle of a sent task: the result read as
//! `Result<TaskOutcome<T, E>, ReadError>`, and the task info (Python
//! `TaskHandle.get`, `info`, `_record_to_task_result`).
//!
//! The outer error is the read failing: the wait limit passed, the task is
//! not held, the stored data does not read, the database failed. The inner
//! value is what the task ended with: its value, its own error, or a library
//! code that the core recorded (`TASK_CANCELLED`, `TASK_EXPIRED`,
//! `TASK_TIMEOUT`, `WORKER_CRASHED`, ...).
//!
//! The handle keeps the first final read, as Python does: an outcome,
//! `RESULT_NOT_AVAILABLE`, a stored result that does not read, and a stored
//! user code that `E` does not name. A wait that timed out, a task not
//! found, a refused wait and a database failure are not kept: a later read
//! may succeed.

use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

use horsies_core::catalog::CodeEnum;
use horsies_core::core::codec::{task_result_from_json_str, JsonReadError};
use horsies_core::handle::CoreHandle;
use horsies_core::{
    process_role, wait_inside_task_message, BrokerError, BrokerErrorCode, BrokerOperationError,
    BuiltInTaskCode, OperationalErrorCode, OutcomeCode, ProcessRole, RawResultRead,
    RawResultRecord, RetrievalCode, TaskInfoRead, TaskResult,
};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::task::body::value_free_decode_text;
use crate::{TaskError, UserErrorCode};

/// Why neither the live table nor the task history holds a task.
pub use horsies_core::TaskAbsenceReason;
/// The limit of a result wait: `WaitLimit::parse(Some(duration))` refuses
/// a limit above `MAX_WAIT_LIMIT_MS` (about 49.7 days) before any read.
pub use horsies_core::{BoundedWait, WaitLimit, WaitLimitRefused, MAX_WAIT_LIMIT_MS};
/// The info of a task and its attempts, and the status of a task.
pub use horsies_core::{TaskAttemptInfo, TaskInfo, TaskStatus};

/// What a task ended with.
#[derive(Debug, Clone, PartialEq)]
pub enum TaskOutcome<T, E> {
    /// The task returned this value.
    Succeeded(T),
    /// The task returned this error, or the library recorded it: a library
    /// code such as `TASK_CANCELLED`, `TASK_EXPIRED`, `TASK_TIMEOUT` or
    /// `WORKER_CRASHED`.
    Failed(TaskError<E>),
}

/// Why a read gave no outcome.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadError {
    /// `WAIT_TIMEOUT`: the task had not finished when the wait limit passed.
    /// A later read may find it finished.
    #[error(
        "timed out waiting for task {task_id}; its status is {} and it may still run",
        .status.wire_value()
    )]
    WaitTimedOut { task_id: Uuid, status: TaskStatus },
    /// `TASK_NOT_FOUND`: neither the live table nor the task history holds
    /// the task. `reason` tells a task that retention can have removed
    /// (`PredatesRetainedHistory`) from an id that no retained history held
    /// (`WithinRetainedHistory`). A handle built by id can race the commit of
    /// its send, so a later read may find it.
    #[error("task {task_id} not found in database ({})", .reason.as_str())]
    TaskNotFound {
        task_id: Uuid,
        reason: TaskAbsenceReason,
    },
    /// `RESULT_NOT_AVAILABLE`: the task reached a terminal status and stored
    /// no result.
    #[error(
        "task {task_id} reached terminal status {} but stored no result payload",
        .status.wire_value()
    )]
    ResultNotAvailable { task_id: Uuid, status: TaskStatus },
    /// `RESULT_DESERIALIZATION_ERROR`: the stored result does not read as
    /// the result of this task's types. `cause` names no value.
    #[error("the stored result of task {task_id} does not read: {cause}")]
    StoredResultUnreadable { task_id: Uuid, cause: String },
    /// The stored error has a user code that `E` does not name: the task
    /// that stored it has another code type, or another version of it.
    #[error(
        "the stored error of task {task_id} has user code {code}, which {code_type} does not name"
    )]
    UnknownUserCode {
        task_id: Uuid,
        code: String,
        code_type: &'static str,
    },
    /// `WAIT_INSIDE_TASK`: a result wait in a process that runs task bodies.
    /// Nothing was read.
    #[error("{message}")]
    WaitInsideTask { message: String },
    /// `BROKER_ERROR`: the database or the connection failed; `retryable`
    /// is the classification of the cause.
    #[error("the read failed ({code}): {message}")]
    Database {
        code: BrokerErrorCode,
        message: String,
        retryable: bool,
    },
}

impl ReadError {
    /// The library code of this failure, as Python's `TaskHandle.get`
    /// stores it. A user code that `E` does not name is a stored result
    /// that does not read: `RESULT_DESERIALIZATION_ERROR`.
    pub fn code(&self) -> BuiltInTaskCode {
        match self {
            Self::WaitTimedOut { .. } => RetrievalCode::WaitTimeout.into(),
            Self::TaskNotFound { .. } => RetrievalCode::TaskNotFound.into(),
            Self::ResultNotAvailable { .. } => RetrievalCode::ResultNotAvailable.into(),
            Self::StoredResultUnreadable { .. } | Self::UnknownUserCode { .. } => {
                OperationalErrorCode::ResultDeserializationError.into()
            }
            Self::WaitInsideTask { .. } => RetrievalCode::WaitInsideTask.into(),
            Self::Database { .. } => OperationalErrorCode::BrokerError.into(),
        }
    }

    /// Whether the handle keeps this failure: a fact of the stored data,
    /// not of the moment of the read.
    fn is_final(&self) -> bool {
        match self {
            Self::ResultNotAvailable { .. }
            | Self::StoredResultUnreadable { .. }
            | Self::UnknownUserCode { .. } => true,
            Self::WaitTimedOut { .. }
            | Self::TaskNotFound { .. }
            | Self::WaitInsideTask { .. }
            | Self::Database { .. } => false,
        }
    }

    /// The failure of a connect or a schema install before the read.
    pub(crate) fn of_broker(error: &BrokerError) -> Self {
        Self::Database {
            code: BrokerErrorCode::TaskInfoQueryFailed,
            message: error.to_string(),
            retryable: error.is_retryable(),
        }
    }

    /// The failure of a read. A stored payload that does not read keeps its
    /// own variant.
    pub(crate) fn of_operation(task_id: Uuid, error: BrokerOperationError) -> Self {
        match error.code {
            BrokerErrorCode::InvalidJsonPayload => Self::StoredResultUnreadable {
                task_id,
                cause: error.message,
            },
            code => Self::Database {
                code,
                message: error.message,
                retryable: error.retryable,
            },
        }
    }

    /// The refusal of a result wait where task bodies run, for `operation`.
    pub(crate) fn wait_inside_task(operation: &str) -> Self {
        Self::WaitInsideTask {
            message: wait_inside_task_message(operation),
        }
    }
}

/// The parts of the task info to read besides its status and times.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskInfoParts {
    /// The stored result of a terminal task.
    pub result: bool,
    /// The failed reason of a failed task.
    pub failed_reason: bool,
    /// Every attempt of the task, newest first.
    pub attempts: bool,
}

/// The handle of one sent task, with its output type `T` and code type `E`.
/// Clones share one task and one kept read.
pub struct TaskHandle<T, E> {
    task_id: Uuid,
    reads: Arc<CoreHandle>,
    kept: Arc<OnceLock<Result<TaskOutcome<T, E>, ReadError>>>,
    types: PhantomData<fn() -> (T, E)>,
}

impl<T, E> TaskHandle<T, E> {
    /// The handle of `task_id`, read through `reads`: the handle that sent
    /// the task, or the app's handle for a read by id.
    pub(crate) fn new(task_id: Uuid, reads: Arc<CoreHandle>) -> Self {
        Self {
            task_id,
            reads,
            kept: Arc::new(OnceLock::new()),
            types: PhantomData,
        }
    }

    /// The id of the task.
    pub fn task_id(&self) -> Uuid {
        self.task_id
    }
}

impl<T, E> TaskHandle<T, E>
where
    T: DeserializeOwned + Clone,
    E: UserErrorCode,
{
    /// The outcome of the task, waiting while it has not finished, up to
    /// `limit` (`WaitLimit::NoLimit` waits until it ends).
    ///
    /// The first final read is kept, and later calls return it with no
    /// read. In a process that runs task bodies the wait is refused with
    /// `WaitInsideTask` before any statement.
    ///
    /// Errors: [`ReadError`].
    pub async fn get(&self, limit: WaitLimit) -> Result<TaskOutcome<T, E>, ReadError> {
        if let Some(kept) = self.kept.get() {
            return kept.clone();
        }
        if process_role() == ProcessRole::RunsTaskBodies {
            return Err(ReadError::wait_inside_task(&format!(
                "TaskHandle.get() for task {}",
                self.task_id
            )));
        }
        let read = self.read(limit).await;
        match &read {
            Ok(_) => self.keep(read),
            Err(failure) if failure.is_final() => self.keep(read),
            Err(_) => read,
        }
    }

    /// The info of the task with the `parts` asked for. Not kept: a status
    /// changes.
    ///
    /// Errors: `TaskNotFound` for a task held nowhere; `Database`.
    pub async fn info(&self, parts: TaskInfoParts) -> Result<TaskInfo, ReadError> {
        read_task_info(&self.reads, self.task_id, parts).await
    }

    async fn read(&self, limit: WaitLimit) -> Result<TaskOutcome<T, E>, ReadError> {
        let broker = self
            .reads
            .initialized_broker()
            .await
            .map_err(|error| ReadError::of_broker(&error))?;
        let record = broker
            .get_raw_result_record(self.task_id, limit_duration(limit))
            .await
            .map_err(|error| ReadError::of_operation(self.task_id, error))?;
        match record {
            RawResultRead::Absent { reason } => Err(ReadError::TaskNotFound {
                task_id: self.task_id,
                reason,
            }),
            RawResultRead::Found(record) => outcome_of_record(record),
        }
    }

    /// Keep `read` when no other read was kept first, and return the kept
    /// read.
    fn keep(
        &self,
        read: Result<TaskOutcome<T, E>, ReadError>,
    ) -> Result<TaskOutcome<T, E>, ReadError> {
        let _first = self.kept.set(read.clone());
        self.kept.get().cloned().unwrap_or(read)
    }
}

/// The task info of `task_id` through `reads`. Python `info`: the info of
/// the live row or of the history record, attempts newest first.
pub(crate) async fn read_task_info(
    reads: &CoreHandle,
    task_id: Uuid,
    parts: TaskInfoParts,
) -> Result<TaskInfo, ReadError> {
    let broker = reads
        .initialized_broker()
        .await
        .map_err(|error| ReadError::of_broker(&error))?;
    let read = broker
        .get_task_info_with_attempts(task_id, parts.result, parts.failed_reason, parts.attempts)
        .await
        .map_err(|error| ReadError::of_operation(task_id, error))?;
    match read {
        TaskInfoRead::Found(info) => Ok(*info),
        TaskInfoRead::Absent { reason } => Err(ReadError::TaskNotFound { task_id, reason }),
    }
}

/// The duration of a wait limit, as the core's read takes it.
pub(crate) fn limit_duration(limit: WaitLimit) -> Option<std::time::Duration> {
    match limit {
        WaitLimit::NoLimit => None,
        WaitLimit::Within(bounded) => Some(bounded.duration()),
    }
}

/// The outcome of a stored record (Python `_record_to_task_result`). Pure.
///
/// - `CANCELLED`: `TASK_CANCELLED` with the task id in `data`.
/// - no result, terminal status: `ResultNotAvailable`.
/// - no result, another status: the wait limit passed, `WaitTimedOut`.
/// - a result: the envelope read with `T` and `E`.
pub(crate) fn outcome_of_record<T, E>(
    record: RawResultRecord,
) -> Result<TaskOutcome<T, E>, ReadError>
where
    T: DeserializeOwned,
    E: UserErrorCode,
{
    let task_id = record.task_id;
    let envelope = match (record.status, record.raw_result) {
        (TaskStatus::Cancelled, _) => {
            return Ok(TaskOutcome::Failed(
                TaskError::library(
                    OutcomeCode::TaskCancelled,
                    format!("Task {task_id} was cancelled before completion"),
                )
                .with_data(serde_json::json!({"task_id": task_id.to_string()})),
            ))
        }
        (status @ (TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Expired), None) => {
            return Err(ReadError::ResultNotAvailable { task_id, status })
        }
        (status @ (TaskStatus::Pending | TaskStatus::Claimed | TaskStatus::Running), None) => {
            return Err(ReadError::WaitTimedOut { task_id, status })
        }
        (_, Some(envelope)) => envelope,
    };
    let text = serde_json::Value::Object(envelope).to_string();
    let stored = task_result_from_json_str::<T>(&text).map_err(|error| {
        ReadError::StoredResultUnreadable {
            task_id,
            cause: unreadable_cause(&error),
        }
    })?;
    match stored {
        TaskResult::Ok(value) => Ok(TaskOutcome::Succeeded(value)),
        TaskResult::Err(error) => TaskError::from_stored(error)
            .map(TaskOutcome::Failed)
            .map_err(|unknown| ReadError::UnknownUserCode {
                task_id,
                code: unknown.code,
                code_type: unknown.code_type,
            }),
    }
}

/// The cause of a stored result that does not read, with no value.
fn unreadable_cause(error: &JsonReadError) -> String {
    match error {
        JsonReadError::Refused(refused) => format!("the text breaks the strict rule: {refused}"),
        JsonReadError::Decode(decode) => value_free_decode_text(decode),
    }
}

impl<T, E> Clone for TaskHandle<T, E> {
    fn clone(&self) -> Self {
        Self {
            task_id: self.task_id,
            reads: Arc::clone(&self.reads),
            kept: Arc::clone(&self.kept),
            types: PhantomData,
        }
    }
}

impl<T, E> std::fmt::Debug for TaskHandle<T, E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskHandle")
            .field("task_id", &self.task_id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::{json, Value};

    use super::*;
    use crate::{AppConfig, Horsies};

    #[derive(Debug, Clone, PartialEq)]
    enum ChargeCode {
        Declined,
    }

    impl UserErrorCode for ChargeCode {
        const CODES: &'static [&'static str] = &["DECLINED"];

        fn code(&self) -> &'static str {
            match self {
                Self::Declined => "DECLINED",
            }
        }

        fn from_code(text: &str) -> Option<Self> {
            match text {
                "DECLINED" => Some(Self::Declined),
                _ => None,
            }
        }
    }

    const MARKER: &str = "secret-marker-41";

    fn task() -> Uuid {
        Uuid::from_u128(5)
    }

    fn record(status: TaskStatus, raw_result: Option<Value>) -> RawResultRecord {
        RawResultRecord {
            task_id: task(),
            task_name: "charge".to_owned(),
            status,
            raw_result: raw_result.map(|value| match value {
                Value::Object(map) => map,
                other => panic!("an envelope is an object: {other}"),
            }),
        }
    }

    fn outcome(record: RawResultRecord) -> Result<TaskOutcome<u32, ChargeCode>, ReadError> {
        outcome_of_record(record)
    }

    fn error_envelope(code: &str) -> Value {
        json!({
            "__h_task_result__": true,
            "ok": null,
            "err": {"exception": null, "error_code": code, "data": null, "message": "no"}
        })
    }

    #[test]
    fn a_stored_value_is_succeeded() {
        let read = outcome(record(
            TaskStatus::Completed,
            Some(json!({"__h_task_result__": true, "ok": 3, "err": null})),
        ));
        assert_eq!(read, Ok(TaskOutcome::Succeeded(3)));
    }

    #[test]
    fn a_stored_user_error_is_failed_with_the_code_of_the_type() {
        let Ok(TaskOutcome::Failed(error)) =
            outcome(record(TaskStatus::Failed, Some(error_envelope("DECLINED"))))
        else {
            panic!("a stored user error is a failed outcome");
        };
        assert_eq!(
            error.code(),
            &crate::TaskErrorCode::User(ChargeCode::Declined)
        );
    }

    #[test]
    fn a_stored_library_code_is_failed() {
        let envelope = json!({
            "__h_task_result__": true,
            "ok": null,
            "err": {
                "exception": null,
                "error_code": {"__builtin_task_code__": "TASK_TIMEOUT"},
                "data": null,
                "message": "deadline"
            }
        });
        let Ok(TaskOutcome::Failed(error)) = outcome(record(TaskStatus::Failed, Some(envelope)))
        else {
            panic!("a stored library error is a failed outcome");
        };
        assert_eq!(error.code().text(), "TASK_TIMEOUT");
    }

    #[test]
    fn a_cancelled_task_is_failed_with_task_cancelled() {
        let Ok(TaskOutcome::Failed(error)) = outcome(record(TaskStatus::Cancelled, None)) else {
            panic!("a cancelled task is a failed outcome");
        };
        assert_eq!(error.code().text(), "TASK_CANCELLED");
        assert_eq!(error.data(), Some(&json!({"task_id": task().to_string()})));
    }

    #[test]
    fn a_stored_user_code_that_the_type_does_not_name_is_unknown() {
        assert_eq!(
            outcome(record(
                TaskStatus::Failed,
                Some(error_envelope("OUT_OF_STOCK"))
            )),
            Err(ReadError::UnknownUserCode {
                task_id: task(),
                code: "OUT_OF_STOCK".to_owned(),
                code_type: std::any::type_name::<ChargeCode>(),
            })
        );
    }

    #[test]
    fn a_stored_value_of_another_type_is_unreadable_without_the_value() {
        let read = outcome(record(
            TaskStatus::Completed,
            Some(json!({"__h_task_result__": true, "ok": MARKER, "err": null})),
        ));
        let Err(ReadError::StoredResultUnreadable { cause, .. }) = read else {
            panic!("a value of another type is unreadable");
        };
        assert!(!cause.contains(MARKER), "{cause}");
    }

    #[test]
    fn a_terminal_task_without_a_result_is_not_available_and_an_unfinished_one_timed_out() {
        assert_eq!(
            outcome(record(TaskStatus::Expired, None)),
            Err(ReadError::ResultNotAvailable {
                task_id: task(),
                status: TaskStatus::Expired
            })
        );
        assert_eq!(
            outcome(record(TaskStatus::Claimed, None)),
            Err(ReadError::WaitTimedOut {
                task_id: task(),
                status: TaskStatus::Claimed
            })
        );
    }

    #[test]
    fn only_the_failures_of_the_stored_data_are_kept() {
        let kept = [
            ReadError::ResultNotAvailable {
                task_id: task(),
                status: TaskStatus::Failed,
            },
            ReadError::StoredResultUnreadable {
                task_id: task(),
                cause: "x".to_owned(),
            },
            ReadError::UnknownUserCode {
                task_id: task(),
                code: "X".to_owned(),
                code_type: "T",
            },
        ];
        assert!(kept.iter().all(ReadError::is_final));
        let not_kept = [
            ReadError::WaitTimedOut {
                task_id: task(),
                status: TaskStatus::Running,
            },
            ReadError::TaskNotFound {
                task_id: task(),
                reason: TaskAbsenceReason::WithinRetainedHistory,
            },
            ReadError::wait_inside_task("TaskHandle.get()"),
            ReadError::Database {
                code: BrokerErrorCode::TaskInfoQueryFailed,
                message: "down".to_owned(),
                retryable: true,
            },
        ];
        assert!(!not_kept.iter().any(ReadError::is_final));
    }

    /// Decision on #1405: the reason of the history read reaches the read
    /// error, and the code stays Python's `TASK_NOT_FOUND`.
    #[test]
    fn a_task_not_found_names_its_absence_reason() {
        let error = ReadError::TaskNotFound {
            task_id: task(),
            reason: TaskAbsenceReason::PredatesRetainedHistory,
        };
        assert_eq!(error.code().to_string(), "TASK_NOT_FOUND");
        assert!(
            error.to_string().ends_with("(PREDATES_RETAINED_HISTORY)"),
            "{error}"
        );
    }

    #[test]
    fn each_read_error_has_python_s_code() {
        let codes: Vec<String> = [
            ReadError::WaitTimedOut {
                task_id: task(),
                status: TaskStatus::Running,
            },
            ReadError::TaskNotFound {
                task_id: task(),
                reason: TaskAbsenceReason::WithinRetainedHistory,
            },
            ReadError::ResultNotAvailable {
                task_id: task(),
                status: TaskStatus::Failed,
            },
            ReadError::wait_inside_task("TaskHandle.get()"),
        ]
        .iter()
        .map(|error| error.code().value())
        .collect();
        assert_eq!(
            codes,
            vec![
                "WAIT_TIMEOUT",
                "TASK_NOT_FOUND",
                "RESULT_NOT_AVAILABLE",
                "WAIT_INSIDE_TASK"
            ]
        );
    }

    #[test]
    fn an_invalid_payload_read_is_unreadable_and_another_failure_is_database() {
        let unreadable = ReadError::of_operation(
            task(),
            BrokerOperationError {
                code: BrokerErrorCode::InvalidJsonPayload,
                message: "digest mismatch".to_owned(),
                retryable: false,
            },
        );
        assert!(matches!(
            unreadable,
            ReadError::StoredResultUnreadable { .. }
        ));
        let database = ReadError::of_operation(
            task(),
            BrokerOperationError {
                code: BrokerErrorCode::TaskInfoQueryFailed,
                message: "timeout".to_owned(),
                retryable: true,
            },
        );
        assert!(matches!(
            database,
            ReadError::Database {
                retryable: true,
                ..
            }
        ));
    }

    #[test]
    fn a_wait_limit_above_the_bound_is_refused_and_no_limit_has_no_duration() {
        assert!(WaitLimit::parse(Some(Duration::MAX)).is_err());
        assert_eq!(limit_duration(WaitLimit::NoLimit), None);
        let limit = WaitLimit::parse(Some(Duration::from_secs(2))).unwrap();
        assert_eq!(limit_duration(limit), Some(Duration::from_secs(2)));
    }

    /// A kept read is returned with no statement: the app's handle never
    /// connects to the address, which has no database.
    #[tokio::test]
    async fn a_kept_outcome_is_returned_again_and_clones_share_it() {
        let app = Horsies::new(AppConfig::for_database_url(
            "postgresql://127.0.0.1:1/nothing",
        ))
        .unwrap();
        let handle: TaskHandle<u32, ChargeCode> = app.task_handle_of(task());
        let kept = handle.keep(Ok(TaskOutcome::Succeeded(4)));
        assert_eq!(kept, Ok(TaskOutcome::Succeeded(4)));
        let again = handle.keep(Ok(TaskOutcome::Succeeded(5)));
        assert_eq!(again, Ok(TaskOutcome::Succeeded(4)));
        let clone = handle.clone();
        assert_eq!(
            clone.get(WaitLimit::NoLimit).await,
            Ok(TaskOutcome::Succeeded(4))
        );
    }

    #[test]
    fn no_value_reaches_the_message_of_a_map_of_another_shape() {
        let read = outcome(record(
            TaskStatus::Completed,
            Some(json!({"__h_task_result__": true, "ok": {"k": MARKER}, "err": null})),
        ));
        let Err(error) = read else {
            panic!("an object is not a u32");
        };
        assert!(!error.to_string().contains(MARKER));
    }
}

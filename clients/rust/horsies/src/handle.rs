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
//! found, a wait limit above the bound, a refused wait and a database
//! failure are not kept: a later read may succeed.

use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

use horsies_core::catalog::CodeEnum;
use horsies_core::core::codec::{
    from_json_bytes_with, task_result_from_json_str, JsonReadError, TextRule,
};
use horsies_core::core::task::result::{
    read_task_result_text, TaskResultText, TaskResultTextRefused,
};
use horsies_core::core::task::StoredErrorRefused;
use horsies_core::handle::CoreHandle;
use horsies_core::{
    process_role, wait_inside_task_message, BoundedWait, BrokerError, BuiltInTaskCode,
    DatabaseErrorCode, DatabaseOperationError, JsonText, OperationalCode, OutcomeCode, ProcessRole,
    RawResultRead, RawResultRecord, RetrievalCode, StoredEnvelopeText, TaskInfoRead, TaskResult,
};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::process_state::refuse_in_a_fork;
use crate::task::body::value_free_decode_text;
use crate::{TaskError, UserErrorCode};

/// Why neither the live table nor the task history holds a task.
pub use horsies_core::TaskAbsenceReason;
/// The info of an attempt of a task, and the status of a task.
pub use horsies_core::{TaskAttemptInfo, TaskStatus};
/// The limit of a result wait. A read refuses a `WaitLimit::Within` limit
/// above `MAX_WAIT_LIMIT_MS` (about 49.7 days) with `InvalidInput`, before
/// any statement.
pub use horsies_core::{WaitLimit, WaitLimitRefused, MAX_WAIT_LIMIT_MS};

/// The info of a task. Its `result` is the stored result read as a
/// `TaskResult`: the `ok` slot as its stored text (`JsonText::decode` reads
/// it with the task's type), the `err` slot as a stored task error. The
/// core's read returns the envelope as stored, and the client reads it
/// (Python `Horsies.get_task_info_async`).
pub type TaskInfo = horsies_core::TaskInfo<TaskResult<JsonText>>;

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
    /// `UNKNOWN_CUSTOM_CODE`: the stored error has a user code that `E`
    /// does not name: the task that stored it has another code type, or
    /// another version of it.
    #[error(
        "the stored error of task {task_id} has user code {code}, which {code_type} does not name"
    )]
    UnknownUserCode {
        task_id: Uuid,
        code: String,
        code_type: &'static str,
    },
    /// `INVALID_INPUT`: the `WaitLimit::Within` limit is above
    /// `MAX_WAIT_LIMIT_MS`. It is the first check of a read: nothing was
    /// read and nothing is kept. The same limit is refused again.
    #[error("invalid input for the read of task {task_id}: {cause}")]
    InvalidInput {
        task_id: Uuid,
        cause: WaitLimitRefused,
    },
    /// `WAIT_INSIDE_TASK`: a result wait in a process that runs task bodies.
    /// Nothing was read.
    #[error("{message}")]
    WaitInsideTask { message: String },
    /// `ASYNC_CONTEXT`: a blocking read on a thread that runs a tokio
    /// runtime; its wait would stall the runtime. Nothing was read; the
    /// async form reads.
    #[error("{message}")]
    AsyncContext { message: String },
    /// `DATABASE_ERROR`: the database or the connection failed; `retryable`
    /// is the classification of the cause.
    #[error("the read failed ({code}): {message}")]
    Database {
        code: DatabaseErrorCode,
        message: String,
        retryable: bool,
    },
}

impl ReadError {
    /// The library code of this failure, as Python's `TaskHandle.get`
    /// stores it. A user code that `E` does not name is
    /// `UNKNOWN_CUSTOM_CODE`.
    pub fn code(&self) -> BuiltInTaskCode {
        match self {
            Self::WaitTimedOut { .. } => RetrievalCode::WaitTimeout.into(),
            Self::TaskNotFound { .. } => RetrievalCode::TaskNotFound.into(),
            Self::ResultNotAvailable { .. } => RetrievalCode::ResultNotAvailable.into(),
            Self::StoredResultUnreadable { .. } => {
                OperationalCode::ResultDeserializationError.into()
            }
            Self::UnknownUserCode { .. } => RetrievalCode::UnknownCustomCode.into(),
            Self::InvalidInput { .. } => RetrievalCode::InvalidInput.into(),
            Self::WaitInsideTask { .. } => RetrievalCode::WaitInsideTask.into(),
            Self::AsyncContext { .. } => RetrievalCode::AsyncContext.into(),
            Self::Database { .. } => OperationalCode::DatabaseError.into(),
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
            | Self::InvalidInput { .. }
            | Self::WaitInsideTask { .. }
            | Self::AsyncContext { .. }
            | Self::Database { .. } => false,
        }
    }

    /// The failure of a connect or a schema install before the read.
    pub(crate) fn of_broker(error: &BrokerError) -> Self {
        Self::Database {
            code: DatabaseErrorCode::TaskInfoQueryFailed,
            message: error.to_string(),
            retryable: error.is_retryable(),
        }
    }

    /// The failure of a read. A stored payload that does not read keeps its
    /// own variant.
    pub(crate) fn of_operation(task_id: Uuid, error: DatabaseOperationError) -> Self {
        match error {
            DatabaseOperationError::InvalidJsonPayload { message, .. } => {
                Self::StoredResultUnreadable {
                    task_id,
                    cause: message,
                }
            }
            other => Self::Database {
                code: other.code(),
                message: other.message().to_owned(),
                retryable: other.retryable(),
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
///
/// # Panics
///
/// Every method that runs on the core, async or blocking, panics in a
/// process that was forked without `exec` after the core's process runtime
/// started in its parent: a fork is a state of the process, never an error
/// value of a call. Nothing ran.
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
    /// The limit is the first check: a `Within` limit above
    /// `MAX_WAIT_LIMIT_MS` is `InvalidInput`, also when a read is kept. The
    /// first final read is kept, and later calls return it with no read. In
    /// a process that runs task bodies the wait is refused with
    /// `WaitInsideTask` before any statement.
    ///
    /// Errors: [`ReadError`].
    pub async fn get(&self, limit: WaitLimit) -> Result<TaskOutcome<T, E>, ReadError> {
        let wait = checked_task_wait(self.task_id, limit)?;
        if let Some(kept) = self.kept.get() {
            return kept.clone();
        }
        if process_role() == ProcessRole::RunsTaskBodies {
            return Err(ReadError::wait_inside_task(&format!(
                "TaskHandle.get() for task {}",
                self.task_id
            )));
        }
        let read = self.read(wait).await;
        match &read {
            Ok(_) => self.keep(read),
            Err(failure) if failure.is_final() => self.keep(read),
            Err(_) => read,
        }
    }

    /// The info of the task with the `parts` asked for. Not kept: a status
    /// changes.
    ///
    /// Errors: `TaskNotFound` for a task held nowhere; `StoredResultUnreadable`
    /// for a stored result that is not a task result; `Database`.
    pub async fn info(&self, parts: TaskInfoParts) -> Result<TaskInfo, ReadError> {
        read_task_info(&self.reads, self.task_id, parts).await
    }

    /// The result read of the task, up to `wait`: a limit that
    /// [`checked_wait`] returned.
    ///
    /// # Panics
    ///
    /// In a fork, without `exec`, of the process that started the core's
    /// process runtime. Nothing ran.
    async fn read(
        &self,
        wait: Option<std::time::Duration>,
    ) -> Result<TaskOutcome<T, E>, ReadError> {
        refuse_in_a_fork(format_args!("TaskHandle::get for task {}", self.task_id));
        let broker = self
            .reads
            .initialized_broker()
            .await
            .map_err(|error| ReadError::of_broker(&error))?;
        let record = broker
            .get_raw_result_record(self.task_id, wait)
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

    /// The read that the handle kept, when one was kept: a later `get`
    /// returns it with no read.
    pub(crate) fn kept(&self) -> Option<Result<TaskOutcome<T, E>, ReadError>> {
        self.kept.get().cloned()
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
///
/// # Panics
///
/// In a fork, without `exec`, of the process that started the core's
/// process runtime. Nothing ran.
pub(crate) async fn read_task_info(
    reads: &CoreHandle,
    task_id: Uuid,
    parts: TaskInfoParts,
) -> Result<TaskInfo, ReadError> {
    refuse_in_a_fork(format_args!("the info read of task {task_id}"));
    let broker = reads
        .initialized_broker()
        .await
        .map_err(|error| ReadError::of_broker(&error))?;
    let read = broker
        .get_task_info_with_attempts(task_id, parts.result, parts.failed_reason, parts.attempts)
        .await
        .map_err(|error| ReadError::of_operation(task_id, error))?;
    match read {
        TaskInfoRead::Found(info) => task_info_of_stored(*info),
        TaskInfoRead::Absent { reason } => Err(ReadError::TaskNotFound { task_id, reason }),
    }
}

/// The task info with its stored result read as a `TaskResult` (Python
/// `Horsies.get_task_info_async`, which decodes the envelope that the
/// broker's read returns). Pure.
///
/// Errors: `StoredResultUnreadable` for a stored result that is not a task
/// result envelope, or whose error does not read; the cause names no value.
fn task_info_of_stored(
    info: horsies_core::TaskInfo<StoredEnvelopeText>,
) -> Result<TaskInfo, ReadError> {
    let task_id = info.task_id;
    info.try_map_result(|envelope| {
        task_result_from_json_str::<JsonText>(envelope.as_str()).map_err(|error| {
            ReadError::StoredResultUnreadable {
                task_id,
                cause: unreadable_cause(&error),
            }
        })
    })
}

/// A wait limit as the core's reads take it: `None` waits until the end,
/// `Some` waits at most that long. A read calls it first, before any
/// statement. Pure.
///
/// Errors: [`WaitLimitRefused`] for a `Within` limit above
/// `MAX_WAIT_LIMIT_MS`.
pub(crate) fn checked_wait(
    limit: WaitLimit,
) -> Result<Option<std::time::Duration>, WaitLimitRefused> {
    match limit {
        WaitLimit::NoLimit => Ok(None),
        WaitLimit::Within(duration) => {
            BoundedWait::try_from(duration).map(|bounded| Some(bounded.duration()))
        }
    }
}

/// [`checked_wait`] for a read of task `task_id`. Pure.
///
/// Errors: `InvalidInput` with the task id and the refusal.
pub(crate) fn checked_task_wait(
    task_id: Uuid,
    limit: WaitLimit,
) -> Result<Option<std::time::Duration>, ReadError> {
    checked_wait(limit).map_err(|cause| ReadError::InvalidInput { task_id, cause })
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
    let stored = read_stored_result::<T>(envelope.as_str()).map_err(|refused| {
        ReadError::StoredResultUnreadable {
            task_id,
            cause: refused.cause,
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

/// Why a stored result envelope does not read with the value type. The
/// cause names no value of the text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredResultRefused {
    pub(crate) cause: String,
}

/// A stored result envelope read with the value type `T` (Python
/// `decode_task_result`). The core reads the shape of the envelope and its
/// `err` slot (`read_task_result_text`); the `ok` slot decodes as `T` from
/// its own text, under the rules of a user value (no engine key, at most
/// 200 levels). So an integer outside the `i64` and `u64` range reads as
/// `T` asks (`u128`, `i128`), as Python reads any integer: a read through
/// `serde_json::Value` holds it as an `f64`, which `T` refuses. Pure.
///
/// Errors: [`StoredResultRefused`] for a text that is not a task result
/// envelope, an `err` slot that is not a stored task error, and an `ok`
/// slot that breaks the rules of a user value or does not decode as `T`.
pub(crate) fn read_stored_result<T: DeserializeOwned>(
    text: &str,
) -> Result<TaskResult<T>, StoredResultRefused> {
    match read_task_result_text(text) {
        Err(refused) => Err(StoredResultRefused {
            cause: envelope_refused_cause(&refused),
        }),
        Ok(TaskResultText::Err(error)) => Ok(TaskResult::Err(error)),
        Ok(TaskResultText::Ok(value)) => {
            from_json_bytes_with::<T>(value.get().as_bytes(), TextRule::UserValue)
                .map(TaskResult::Ok)
                .map_err(|error| StoredResultRefused {
                    cause: unreadable_cause(&error),
                })
        }
    }
}

/// The cause of an envelope that the core's read refuses, with no value: a
/// shape refusal names its rule or a key; a decode failure keeps only its
/// kind; a field of the stored error that does not parse is named without
/// the parse text, which can hold the value. Pure.
fn envelope_refused_cause(refused: &TaskResultTextRefused) -> String {
    match refused {
        TaskResultTextRefused::NotStrictJson(rule) => {
            format!("the text breaks the strict rule: {rule}")
        }
        TaskResultTextRefused::Undecodable(error) => value_free_decode_text(error),
        TaskResultTextRefused::ErrorRefused(StoredErrorRefused::FieldInvalid { field, .. }) => {
            format!("field {field} of a stored task error does not parse")
        }
        TaskResultTextRefused::ErrorRefused(
            StoredErrorRefused::NotJson(_)
            | StoredErrorRefused::NotAnObject
            | StoredErrorRefused::UnknownKey(_)
            | StoredErrorRefused::MissingField(_)
            | StoredErrorRefused::UserValue { .. },
        )
        | TaskResultTextRefused::NotAnObject
        | TaskResultTextRefused::MarkerMissing
        | TaskResultTextRefused::ExtraKey { .. }
        | TaskResultTextRefused::MissingSlot
        | TaskResultTextRefused::BothSlotsSet => refused.to_string(),
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

    use horsies_core::StoredEnvelopeText;
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
            raw_result: raw_result.map(|value| {
                StoredEnvelopeText::parse(value.to_string())
                    .unwrap()
                    .expect("an envelope is an object")
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
        assert_eq!(
            error
                .data()
                .map(|data| data.decode::<serde_json::Value>().unwrap()),
            Some(json!({"task_id": task().to_string()}))
        );
        // A result stored for a cancelled task is not read.
        let Ok(TaskOutcome::Failed(stored)) = outcome(record(
            TaskStatus::Cancelled,
            Some(json!({"__h_task_result__": true, "ok": 3, "err": null})),
        )) else {
            panic!("a cancelled task with a stored result is a failed outcome");
        };
        assert_eq!(stored.code().text(), "TASK_CANCELLED");
    }

    /// A stored result that is not a result envelope (the legacy
    /// `__task_result__` shape, or an object with no marker) is unreadable,
    /// not decoded (Python `test_legacy_pydantic_result_envelope_rejected`,
    /// `test_invalid_task_result_structure_returns_err`).
    #[test]
    fn a_stored_result_that_is_not_an_envelope_is_unreadable() {
        let legacy = json!({
            "__task_result__": true,
            "ok": {
                "__pydantic_model__": true,
                "module": "nonexistent.module.that.does.not.exist",
                "qualname": "FakeModel",
                "data": {"field": "value"},
            },
            "err": null,
        });
        for stored in [legacy, json!({"random": 1})] {
            let read = outcome(record(TaskStatus::Completed, Some(stored.clone())));
            assert!(
                matches!(read, Err(ReadError::StoredResultUnreadable { .. })),
                "{stored}: {read:?}"
            );
        }
    }

    /// An EXPIRED task reads its stored `TASK_EXPIRED` error, as a COMPLETED
    /// or a FAILED task reads its stored result.
    #[test]
    fn an_expired_task_reads_its_stored_error() {
        let envelope = json!({
            "__h_task_result__": true,
            "ok": null,
            "err": {
                "exception": null,
                "error_code": {"__builtin_task_code__": "TASK_EXPIRED"},
                "data": null,
                "message": "expired"
            }
        });
        let Ok(TaskOutcome::Failed(error)) = outcome(record(TaskStatus::Expired, Some(envelope)))
        else {
            panic!("an expired task with a stored error is a failed outcome");
        };
        assert_eq!(error.code().text(), "TASK_EXPIRED");
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

    /// The record of a completed task whose stored envelope is `text`.
    fn completed_text(text: &str) -> RawResultRecord {
        RawResultRecord {
            task_id: task(),
            task_name: "charge".to_owned(),
            status: TaskStatus::Completed,
            raw_result: Some(
                StoredEnvelopeText::parse(text.to_owned())
                    .unwrap()
                    .expect("an envelope is an object"),
            ),
        }
    }

    /// The value decodes from its own text: an integer outside the `i64`
    /// and `u64` range reads as `u128` or `i128`, as Python reads it. A read
    /// through `serde_json::Value` holds it as an `f64`, which both refuse.
    #[test]
    fn a_stored_integer_outside_i64_and_u64_reads_as_its_type() {
        let max = outcome_of_record::<u128, ChargeCode>(completed_text(
            r#"{"__h_task_result__":true,"ok":340282366920938463463374607431768211455,"err":null}"#,
        ));
        assert_eq!(max, Ok(TaskOutcome::Succeeded(u128::MAX)));
        let below = outcome_of_record::<i128, ChargeCode>(completed_text(
            r#"{"__h_task_result__":true,"ok":-9223372036854775809,"err":null}"#,
        ));
        assert_eq!(below, Ok(TaskOutcome::Succeeded(i128::from(i64::MIN) - 1)));
    }

    /// The value keeps the rules of a user value: a key that only the engine
    /// writes, at any depth, and more than 200 levels are refused; 200
    /// levels read.
    #[test]
    fn a_stored_value_keeps_the_rules_of_a_user_value() {
        let read = |ok: &str| {
            outcome_of_record::<Value, ChargeCode>(completed_text(&format!(
                r#"{{"__h_task_result__":true,"ok":{ok},"err":null}}"#
            )))
        };
        assert!(matches!(
            read(r#"{"a": [{"__h_marker": 1}]}"#),
            Err(ReadError::StoredResultUnreadable { .. })
        ));
        let nested = |levels: usize| format!("{}{}", "[".repeat(levels), "]".repeat(levels));
        assert!(read(&nested(200)).is_ok());
        assert!(matches!(
            read(&nested(201)),
            Err(ReadError::StoredResultUnreadable { .. })
        ));
    }

    /// A field of the stored error that does not parse is named without its
    /// parse text, which holds the value.
    #[test]
    fn a_stored_error_field_that_does_not_parse_is_unreadable_without_the_value() {
        let read = outcome(record(
            TaskStatus::Failed,
            Some(json!({
                "__h_task_result__": true,
                "ok": null,
                "err": {"exception": MARKER, "error_code": "DECLINED", "data": null, "message": "no"}
            })),
        ));
        let Err(ReadError::StoredResultUnreadable { cause, .. }) = read else {
            panic!("a stored error with a field that does not parse is unreadable");
        };
        assert!(!cause.contains(MARKER), "{cause}");
        assert!(cause.contains("field exception"), "{cause}");
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
            ReadError::InvalidInput {
                task_id: task(),
                cause: WaitLimitRefused,
            },
            ReadError::wait_inside_task("TaskHandle.get()"),
            ReadError::AsyncContext {
                message: "blocking get on a runtime thread".to_owned(),
            },
            ReadError::Database {
                code: DatabaseErrorCode::TaskInfoQueryFailed,
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
            ReadError::InvalidInput {
                task_id: task(),
                cause: WaitLimitRefused,
            },
            ReadError::wait_inside_task("TaskHandle.get()"),
            ReadError::AsyncContext {
                message: "blocking get on a runtime thread".to_owned(),
            },
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
                "INVALID_INPUT",
                "WAIT_INSIDE_TASK",
                "ASYNC_CONTEXT"
            ]
        );
    }

    /// The text of `InvalidInput` names the task and the bound.
    #[test]
    fn an_invalid_input_names_the_task_and_the_bound() {
        let error = ReadError::InvalidInput {
            task_id: task(),
            cause: WaitLimitRefused,
        };
        assert_eq!(
            error.to_string(),
            format!(
                "invalid input for the read of task {}: the wait limit is above 4294967295 ms \
                 (about 49.7 days)",
                task()
            ),
        );
    }

    #[test]
    fn an_invalid_payload_read_is_unreadable_and_another_failure_is_database() {
        let unreadable = ReadError::of_operation(
            task(),
            DatabaseOperationError::InvalidJsonPayload {
                message: "digest mismatch".to_owned(),
                cause: Box::new(horsies_core::BrokerCheckCause::Check),
            },
        );
        assert!(matches!(
            unreadable,
            ReadError::StoredResultUnreadable { .. }
        ));
        let database = ReadError::of_operation(
            task(),
            DatabaseOperationError::task_info_query_failed(
                task(),
                horsies_core::BrokerOperationSource::Broker(
                    horsies_core::BrokerError::ListenerClosed,
                ),
            ),
        );
        assert!(matches!(
            database,
            ReadError::Database {
                retryable: true,
                ..
            }
        ));
    }

    /// A task info as the core's read returns it: the stored result as its
    /// stored text.
    fn stored_info(result: Option<Value>) -> horsies_core::TaskInfo<StoredEnvelopeText> {
        horsies_core::TaskInfo {
            task_id: task(),
            task_name: "charge".to_owned(),
            status: TaskStatus::Failed,
            queue_name: "default".to_owned(),
            priority: 100,
            retry_count: 1,
            max_retries: 2,
            next_retry_at: None,
            sent_at: None,
            enqueued_at: chrono::Utc::now(),
            claimed_at: None,
            started_at: None,
            completed_at: None,
            failed_at: Some(chrono::Utc::now()),
            worker_hostname: None,
            worker_pid: None,
            worker_process_name: None,
            error_code: None,
            failed_reason: None,
            result: result.map(|value| {
                StoredEnvelopeText::parse(value.to_string())
                    .unwrap()
                    .expect("an envelope is an object")
            }),
            attempts: None,
            execution: horsies_core::ExecutionClass::NotKeptInHistory,
        }
    }

    /// Python `Horsies.get_task_info_async`: the client reads the stored
    /// envelope that the core returns; the other fields pass unchanged.
    #[test]
    fn a_task_info_result_is_read_by_the_client() {
        let info = task_info_of_stored(stored_info(Some(json!({
            "__h_task_result__": true,
            "ok": 3,
            "err": null
        }))))
        .expect("a stored value reads");
        let Some(TaskResult::Ok(value)) = info.result else {
            panic!("a stored value is an ok result");
        };
        assert_eq!(value.get(), "3");
        assert_eq!(info.task_name, "charge");
        assert_eq!(info.retry_count, 1);

        let library = task_info_of_stored(stored_info(Some(json!({
            "__h_task_result__": true,
            "ok": null,
            "err": {
                "exception": null,
                "error_code": {"__builtin_task_code__": "TASK_EXPIRED"},
                "data": null,
                "message": "expired"
            }
        }))))
        .expect("a stored library error reads");
        assert!(matches!(library.result, Some(TaskResult::Err(_))));

        let none = task_info_of_stored(stored_info(None)).expect("no result reads");
        assert!(none.result.is_none());

        // The value keeps its stored text: an integer outside the `i64` and
        // `u64` range keeps every digit.
        for wide in [
            "340282366920938463463374607431768211455",
            "-9223372036854775809",
        ] {
            let text = format!(r#"{{"__h_task_result__":true,"ok":{wide},"err":null}}"#);
            let mut stored = stored_info(None);
            stored.result = StoredEnvelopeText::parse(text).unwrap();
            let info = task_info_of_stored(stored).expect("a wide value reads");
            let Some(TaskResult::Ok(value)) = info.result else {
                panic!("a stored value is an ok result");
            };
            assert_eq!(value.get(), wide);
        }
    }

    /// The core returns any stored object; the client's typed read refuses
    /// one that is not a task result, and a built-in name stored as a plain
    /// code, with a cause that names no value.
    #[test]
    fn a_task_info_result_that_is_not_a_task_result_is_unreadable() {
        for stored in [
            json!({"__h_task_result__": true, "ok": null, "err": {"error_code": "TASK_EXPIRED"}}),
            json!({"not": MARKER}),
        ] {
            let Err(ReadError::StoredResultUnreadable { task_id, cause }) =
                task_info_of_stored(stored_info(Some(stored.clone())))
            else {
                panic!("{stored}: the client reads only a task result");
            };
            assert_eq!(task_id, task());
            assert!(!cause.contains(MARKER), "{cause}");
        }
    }

    /// One millisecond above the bound of a wait.
    fn above_the_bound() -> WaitLimit {
        WaitLimit::Within(Duration::from_millis(MAX_WAIT_LIMIT_MS + 1))
    }

    #[test]
    fn no_limit_is_no_core_wait_and_a_limit_up_to_the_bound_is_its_duration() {
        assert_eq!(checked_wait(WaitLimit::NoLimit), Ok(None));
        for duration in [
            Duration::ZERO,
            Duration::from_secs(2),
            Duration::from_millis(MAX_WAIT_LIMIT_MS),
        ] {
            assert_eq!(
                checked_wait(WaitLimit::Within(duration)),
                Ok(Some(duration))
            );
        }
    }

    #[test]
    fn a_limit_above_the_bound_is_invalid_input_with_the_task_id() {
        assert_eq!(
            checked_wait(WaitLimit::Within(Duration::MAX)),
            Err(WaitLimitRefused)
        );
        assert_eq!(
            checked_task_wait(task(), above_the_bound()),
            Err(ReadError::InvalidInput {
                task_id: task(),
                cause: WaitLimitRefused,
            }),
        );
    }

    /// The limit is checked before any statement: the app's address has
    /// no database, so a read that ran would fail as `Database`. Nothing
    /// is kept.
    #[tokio::test]
    async fn a_get_above_the_bound_is_invalid_input_before_any_statement() {
        let app = Horsies::new(AppConfig::for_database_url(
            "postgresql://127.0.0.1:1/nothing",
        ))
        .unwrap();
        let handle: TaskHandle<u32, ChargeCode> = app.task_handle_of(task());
        let refused = handle.get(above_the_bound()).await.unwrap_err();
        assert_eq!(
            refused,
            ReadError::InvalidInput {
                task_id: task(),
                cause: WaitLimitRefused,
            },
        );
        assert_eq!(refused.code().value(), "INVALID_INPUT");
        assert!(handle.kept().is_none());
    }

    /// A kept read does not hide a limit above the bound, as in Python and
    /// TypeScript; a later read within the bound returns the kept read.
    #[tokio::test]
    async fn a_kept_read_does_not_hide_a_limit_above_the_bound() {
        let app = Horsies::new(AppConfig::for_database_url(
            "postgresql://127.0.0.1:1/nothing",
        ))
        .unwrap();
        let handle: TaskHandle<u32, ChargeCode> = app.task_handle_of(task());
        let _kept = handle.keep(Ok(TaskOutcome::Succeeded(4)));
        assert!(matches!(
            handle.get(above_the_bound()).await,
            Err(ReadError::InvalidInput { .. })
        ));
        assert_eq!(
            handle
                .get(WaitLimit::Within(Duration::from_millis(MAX_WAIT_LIMIT_MS)))
                .await,
            Ok(TaskOutcome::Succeeded(4))
        );
    }

    /// `Horsies::get_result` and `Horsies::get_raw_result` check the limit
    /// first, as `TaskHandle::get`.
    #[tokio::test]
    async fn the_reads_by_task_id_above_the_bound_are_invalid_input_before_any_statement() {
        let app = Horsies::new(AppConfig::for_database_url(
            "postgresql://127.0.0.1:1/nothing",
        ))
        .unwrap();
        let typed = app
            .get_result::<u32, ChargeCode>(task(), above_the_bound())
            .await;
        assert!(
            matches!(typed, Err(ReadError::InvalidInput { task_id, .. }) if task_id == task()),
            "{typed:?}"
        );
        let raw = app.get_raw_result(task(), above_the_bound()).await;
        assert!(
            matches!(raw, Err(ReadError::InvalidInput { task_id, .. }) if task_id == task()),
            "{raw:?}"
        );
    }

    /// A handle prints and crosses threads: Debug, Send and Sync.
    #[test]
    fn a_task_handle_is_debug_send_and_sync() {
        fn assert_debug_send_sync<V: std::fmt::Debug + Send + Sync>() {}
        assert_debug_send_sync::<TaskHandle<u32, ChargeCode>>();
        assert_debug_send_sync::<TaskHandle<Value, std::convert::Infallible>>();
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

/// Typed error types for PostgresBroker operations.
///
/// Mirrors Python's `python/horsies/core/brokers/result_types.py`.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::core::codec::{check_json_text, StrictJsonError};
use crate::core::history::reads::detail::TaskAbsenceReason;
use crate::core::{TaskInfo, TaskStatus};
use crate::error_catalog::ErrorEnum;
use crate::failure_cause::FailureCause;

/// Categorized broker operation failure codes.
///
/// The members are Python's, in Python's order
/// (`python/horsies/core/brokers/result_types.py`). The nine operation codes from
/// `SCHEMA_INIT_FAILED` to `LISTENER_SUBSCRIBE_FAILED` share one variant in
/// Python, `BrokerOperationFailed`. A client produces `NO_BROKER` and
/// `NO_TYPE_AVAILABLE`; only the Python client produces `EVENT_LOOP_LIMIT`,
/// `ASYNC_CONTEXT` and `LOOP_RUNNER_FAILED`.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, horsies_core_derive::CodeCatalog,
)]
#[code_catalog(module = "broker", base = "str_enum")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BrokerErrorCode {
    SchemaInitFailed,
    EnqueueFailed,
    PayloadMismatch,
    /// The idempotency key is reserved by another task with a different
    /// command. Nothing was written.
    IdempotencyKeyConflict,
    TaskInfoQueryFailed,
    MonitoringQueryFailed,
    DbPingFailed,
    WorkerPingFailed,
    CleanupFailed,
    CloseFailed,
    ListenerStartFailed,
    ListenerSubscribeFailed,
    /// The task handle has no broker, so it cannot read task info. Nothing
    /// ran. A client produces it.
    NoBroker,
    /// A typed result was requested, but the process has no registered type
    /// for the task. The stored result is unchanged. A client produces it.
    NoTypeAvailable,
    /// Stored or wire JSON failed strict validation.
    InvalidJsonPayload,
    /// The task options passed to enqueue are not valid JSON, or carry a
    /// retry limit that cannot be stored; nothing was written.
    InvalidTaskOptions,
    /// The broker already holds resource sets for its limit of open event
    /// loops, and this call ran on a new one. Nothing ran. Python client
    /// only; removed before 0.8.0.
    EventLoopLimit,
    /// A sync facade was called on a thread that runs an event loop. Its wait
    /// would stall that loop, so nothing ran. Python client only.
    AsyncContext,
    /// A sync facade could not run the call on the broker's sync runner.
    /// Nothing ran. Python client only; removed before 0.8.0.
    LoopRunnerFailed,
    /// An argument of the call was refused before anything ran. Nothing was
    /// written, and the same arguments are refused again.
    InvalidInput,
    /// The broker has no listener, so an operation that needs one cannot run.
    /// Nothing ran. It does not clear for this broker.
    ListenerDisabled,
    /// A result wait was called in a process that runs task bodies. A wait
    /// there can hold the slot that the awaited task needs, so nothing ran.
    WaitInsideTask,
}

impl std::fmt::Display for BrokerErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = serde_plain::to_string(self).unwrap_or_else(|_| format!("{:?}", self));
        write!(f, "{}", s)
    }
}

/// The codes of the broker operations whose failure is one variant,
/// [`BrokerOperationError::OperationFailed`], that keeps the code (Python
/// `BrokerOperationFailedCode`, a `Literal` subset of `BrokerErrorCode`).
/// The members are Python's, in Python's order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BrokerOperationFailedCode {
    SchemaInitFailed,
    TaskInfoQueryFailed,
    MonitoringQueryFailed,
    DbPingFailed,
    WorkerPingFailed,
    CleanupFailed,
    CloseFailed,
    ListenerStartFailed,
    ListenerSubscribeFailed,
}

impl BrokerOperationFailedCode {
    /// Every member, in Python's order.
    pub const ALL: [Self; 9] = [
        Self::SchemaInitFailed,
        Self::TaskInfoQueryFailed,
        Self::MonitoringQueryFailed,
        Self::DbPingFailed,
        Self::WorkerPingFailed,
        Self::CleanupFailed,
        Self::CloseFailed,
        Self::ListenerStartFailed,
        Self::ListenerSubscribeFailed,
    ];

    /// The member of the whole catalog. Pure.
    pub const fn broker_code(self) -> BrokerErrorCode {
        match self {
            Self::SchemaInitFailed => BrokerErrorCode::SchemaInitFailed,
            Self::TaskInfoQueryFailed => BrokerErrorCode::TaskInfoQueryFailed,
            Self::MonitoringQueryFailed => BrokerErrorCode::MonitoringQueryFailed,
            Self::DbPingFailed => BrokerErrorCode::DbPingFailed,
            Self::WorkerPingFailed => BrokerErrorCode::WorkerPingFailed,
            Self::CleanupFailed => BrokerErrorCode::CleanupFailed,
            Self::CloseFailed => BrokerErrorCode::CloseFailed,
            Self::ListenerStartFailed => BrokerErrorCode::ListenerStartFailed,
            Self::ListenerSubscribeFailed => BrokerErrorCode::ListenerSubscribeFailed,
        }
    }
}

/// The failure of a broker operation: one variant per Python class of
/// `python/horsies/core/brokers/result_types.py`, each with the facts of its
/// state only. Carried inside `Err(...)` of a [`BrokerResult`]. Each
/// variant's doc is its Python class doc (`ErrorCatalog`).
///
/// The classes that only the Python client produces are not variants:
/// `BrokerNotAttached`, `BrokerNoTypeAvailable`, `BrokerEventLoopLimit` and
/// `BrokerLoopRunnerFailed`.
///
/// `cause` is what the operation met: the server's error, no answer, a
/// broken read contract, or a failed precondition (a refused argument
/// included). A variant without `cause` has the cause of its state
/// ([`Self::failure_cause`]). A variant without `retryable` never clears.
#[derive(Debug, Clone, horsies_core_derive::ErrorCatalog)]
#[error_catalog(
    module = "broker_errors",
    code = "BrokerErrorCode",
    union = "BrokerCoreError",
    union_doc = "Every failure of a broker operation that the core gives: one class per state."
)]
pub enum BrokerOperationError {
    /// A broker operation failed with a database, driver or connection error.
    ///
    /// ``code`` names the operation. ``retryable`` is the driver classification
    /// of the cause: ``True`` when the condition may clear, for example a lost
    /// connection. A read changed nothing. A cleanup may have finished part of
    /// its rows; no write of the library's own data is left half done.
    #[error_class(
        class = "BrokerOperationFailed",
        code_field = "code",
        code_subset = "BrokerOperationFailedCode",
        exception = "BaseException"
    )]
    OperationFailed {
        code: BrokerOperationFailedCode,
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "Bool")]
        retryable: bool,
        #[error_slot(skip)]
        cause: FailureCause,
    },
    /// The enqueue write failed or could not be confirmed. The row may or may not exist.
    ///
    /// ``retryable`` is the driver classification of the cause. An enqueue
    /// again with the same task id and payload is safe: the broker returns the
    /// existing task for a duplicate. ``exception`` is ``NoCause()`` when the
    /// broker found the failure by a data check (the conflicting row was gone).
    #[error_class(
        class = "BrokerEnqueueFailed",
        code = "EnqueueFailed",
        exception = "BaseException | NoCause"
    )]
    EnqueueFailed {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "Bool")]
        retryable: bool,
        #[error_slot(skip)]
        cause: FailureCause,
    },
    /// The task id holds a different command or ``sent_at``, or its live row a
    /// different idempotency key. Nothing was written. The same enqueue fails
    /// again.
    #[error_class(
        class = "BrokerPayloadMismatch",
        code = "PayloadMismatch",
        never_retryable
    )]
    PayloadMismatch {
        #[error_slot(python = "Str", hidden)]
        message: String,
    },
    /// The idempotency key is reserved by another task with a different command.
    ///
    /// Nothing was written. ``owner_task_id`` names the task that holds the
    /// key. While the owner task is live, the reservation stays. After the
    /// owner task finishes, the reservation ends when the reservation window
    /// has passed (24 h by default, at most 30 days); an enqueue with the same
    /// key is then accepted.
    #[error_class(
        class = "BrokerIdempotencyKeyConflict",
        code = "IdempotencyKeyConflict",
        never_retryable
    )]
    IdempotencyKeyConflict {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "Uuid")]
        owner_task_id: Uuid,
    },
    /// Stored JSON failed strict validation. The stored data is unchanged.
    ///
    /// The same read fails again. ``exception`` is ``NoCause()`` when a shape
    /// check found the failure without a Python exception.
    #[error_class(
        class = "BrokerInvalidJsonPayload",
        code = "InvalidJsonPayload",
        never_retryable,
        exception = "BaseException | NoCause"
    )]
    InvalidJsonPayload {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(skip)]
        cause: FailureCause,
    },
    /// The task options are not valid JSON or carry a retry limit that cannot be stored.
    ///
    /// Nothing was written. The same options are refused again. ``exception``
    /// is ``NoCause()`` when a check refused the options without a Python
    /// exception.
    #[error_class(
        class = "BrokerInvalidTaskOptions",
        code = "InvalidTaskOptions",
        never_retryable,
        exception = "BaseException | NoCause"
    )]
    InvalidTaskOptions {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(skip)]
        cause: FailureCause,
    },
    /// An argument of the call was refused before anything ran. Nothing was written.
    ///
    /// The message names the argument and the rule. The same arguments are
    /// refused again. ``exception`` is ``NoCause()`` when a check refused the
    /// argument without a Python exception.
    #[error_class(
        class = "BrokerInvalidInput",
        code = "InvalidInput",
        never_retryable,
        exception = "BaseException | NoCause"
    )]
    InvalidInput {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(skip)]
        cause: FailureCause,
    },
    /// A sync facade was called on a thread that runs an event loop. Nothing ran.
    ///
    /// A sync call on that thread returns the same error; call the
    /// ``*_async`` form.
    #[error_class(class = "BrokerAsyncContext", code = "AsyncContext", never_retryable)]
    AsyncContext {
        #[error_slot(python = "Str", hidden)]
        message: String,
    },
    /// The broker has no listener, so the operation cannot run. Nothing ran.
    ///
    /// A broker built with ``assume_initialized=True`` has no listener. Every
    /// call that needs one returns this error for that broker.
    #[error_class(
        class = "BrokerListenerDisabled",
        code = "ListenerDisabled",
        never_retryable
    )]
    ListenerDisabled {
        #[error_slot(python = "Str", hidden)]
        message: String,
    },
    /// A result wait was called in a process that runs task bodies. Nothing ran.
    ///
    /// Every result wait in that process returns this error. Compose the tasks
    /// with a workflow, or send the other task and return.
    #[error_class(
        class = "BrokerWaitInsideTask",
        code = "WaitInsideTask",
        never_retryable
    )]
    WaitInsideTask {
        #[error_slot(python = "Str", hidden)]
        message: String,
    },
}

impl From<BrokerOperationFailedCode> for BrokerErrorCode {
    fn from(code: BrokerOperationFailedCode) -> Self {
        code.broker_code()
    }
}

impl BrokerOperationError {
    /// The catalog code of the variant: the code of its Python class
    /// (`ErrorCatalog`). Pure.
    pub fn code(&self) -> BrokerErrorCode {
        ErrorEnum::error_code(self)
    }

    /// What failed and why. Pure.
    pub fn message(&self) -> &str {
        match self {
            Self::OperationFailed { message, .. }
            | Self::EnqueueFailed { message, .. }
            | Self::PayloadMismatch { message }
            | Self::IdempotencyKeyConflict { message, .. }
            | Self::InvalidJsonPayload { message, .. }
            | Self::InvalidTaskOptions { message, .. }
            | Self::InvalidInput { message, .. }
            | Self::AsyncContext { message }
            | Self::ListenerDisabled { message }
            | Self::WaitInsideTask { message } => message,
        }
    }

    /// Whether a repeat of the same call can succeed: the classification of
    /// the cause where the variant has one, else `false`. Pure.
    pub fn retryable(&self) -> bool {
        match self {
            Self::OperationFailed { retryable, .. } | Self::EnqueueFailed { retryable, .. } => {
                *retryable
            }
            Self::PayloadMismatch { .. }
            | Self::IdempotencyKeyConflict { .. }
            | Self::InvalidJsonPayload { .. }
            | Self::InvalidTaskOptions { .. }
            | Self::InvalidInput { .. }
            | Self::AsyncContext { .. }
            | Self::ListenerDisabled { .. }
            | Self::WaitInsideTask { .. } => false,
        }
    }

    /// The cause of the failure: the variant's `cause`, else the cause of
    /// its state. A refused key, a refused thread, a missing listener and a
    /// wait in a task process are failed preconditions. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::OperationFailed { cause, .. }
            | Self::EnqueueFailed { cause, .. }
            | Self::InvalidJsonPayload { cause, .. }
            | Self::InvalidTaskOptions { cause, .. }
            | Self::InvalidInput { cause, .. } => cause.clone(),
            Self::PayloadMismatch { .. }
            | Self::IdempotencyKeyConflict { .. }
            | Self::AsyncContext { .. }
            | Self::ListenerDisabled { .. }
            | Self::WaitInsideTask { .. } => FailureCause::PreconditionFailed,
        }
    }
}

impl std::fmt::Display for BrokerOperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {} (retryable={})",
            self.code(),
            self.message(),
            self.retryable(),
        )
    }
}

impl std::error::Error for BrokerOperationError {}

/// Result type for broker operations.
///
/// `Ok(T)` on success, `Err(BrokerOperationError)` on infrastructure failure.
pub type BrokerResult<T> = Result<T, BrokerOperationError>;

/// Raw broker-level snapshot of a task's stored result envelope.
///
/// The broker verifies history envelopes and checks that the stored text is
/// a JSON object, but deliberately performs no task-specific `Ok` decoding.
/// Typed handles layer their task result type on top of this record.
#[derive(Debug, Clone, PartialEq)]
pub struct RawResultRecord {
    pub task_id: Uuid,
    pub task_name: String,
    pub status: TaskStatus,
    pub raw_result: Option<StoredEnvelopeText>,
}

/// The stored result envelope of a task, as the text that the row stores.
///
/// The text passed the strict read rule and is one JSON object. It is not
/// parsed into a value: a `serde_json::Value` holds an integer outside the
/// `i64` and `u64` range as an `f64`, so Python's text of `2**64` would read
/// back as a float (`tests/fixtures/stored_forms/README.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEnvelopeText(String);

/// Why a stored result text is not a result envelope text.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoredEnvelopeRefused {
    /// The text breaks the strict read rule.
    #[error("result JSON is refused: {0}")]
    NotStrictJson(#[from] StrictJsonError),
    /// The text is another JSON value than an object or `null`; the kind is
    /// named.
    #[error("result JSON must be an object, got {0}")]
    NotAnObject(&'static str),
}

impl StoredEnvelopeText {
    /// Read a stored result text: `None` for the JSON `null`, which is no
    /// result; the text for a JSON object; a refusal for a text that breaks
    /// the strict read rule or holds another JSON value. Pure.
    pub fn parse(text: String) -> Result<Option<Self>, StoredEnvelopeRefused> {
        check_json_text(&text)?;
        // The strict read passed, so the first byte after the whitespace
        // starts the one JSON value of the text.
        match text.trim_start().as_bytes().first() {
            Some(b'{') => Ok(Some(Self(text))),
            Some(b'n') => Ok(None),
            Some(b't' | b'f') => Err(StoredEnvelopeRefused::NotAnObject("boolean")),
            Some(b'"') => Err(StoredEnvelopeRefused::NotAnObject("string")),
            Some(b'[') => Err(StoredEnvelopeRefused::NotAnObject("array")),
            Some(_) => Err(StoredEnvelopeRefused::NotAnObject("number")),
            None => unreachable!("the strict read refuses a text that holds no value"),
        }
    }

    /// The envelope text, as stored.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// What a raw result read found: the record of the task, or why neither
/// the live table nor the task history holds it.
#[derive(Debug, Clone, PartialEq)]
pub enum RawResultRead {
    Found(RawResultRecord),
    /// `reason` tells a task that retention can have removed from an id
    /// that no retained history held.
    Absent {
        reason: TaskAbsenceReason,
    },
}

#[cfg(test)]
impl RawResultRead {
    /// The record of a read that found the task. Panics on an absent task.
    pub(crate) fn expect_found(self) -> RawResultRecord {
        match self {
            Self::Found(record) => record,
            Self::Absent { reason } => panic!("the task is absent: {}", reason.as_str()),
        }
    }

    pub(crate) fn is_absent(&self) -> bool {
        matches!(self, Self::Absent { .. })
    }
}

/// What a task info read found: the info of the task, or why neither the
/// live table nor the task history holds it.
#[derive(Debug, Clone)]
pub enum TaskInfoRead {
    /// Boxed: the info is many times larger than the reason. The stored
    /// result is the envelope text as stored, with no typed decode.
    Found(Box<TaskInfo<StoredEnvelopeText>>),
    /// `reason` as in [`RawResultRead::Absent`].
    Absent { reason: TaskAbsenceReason },
}

#[cfg(test)]
impl TaskInfoRead {
    /// The info of a read that found the task. Panics on an absent task.
    pub(crate) fn expect_found(self) -> TaskInfo<StoredEnvelopeText> {
        match self {
            Self::Found(info) => *info,
            Self::Absent { reason } => panic!("the task is absent: {}", reason.as_str()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_error_code_serde() {
        let code = BrokerErrorCode::EnqueueFailed;
        let json = serde_json::to_string(&code).unwrap();
        assert_eq!(json, "\"ENQUEUE_FAILED\"");
        let back: BrokerErrorCode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, code);
    }

    #[test]
    fn broker_operation_error_display() {
        let err = BrokerOperationError::OperationFailed {
            code: BrokerOperationFailedCode::SchemaInitFailed,
            message: "connection refused".to_owned(),
            retryable: true,
            cause: FailureCause::Connection,
        };
        let s = format!("{}", err);
        assert!(s.contains("SCHEMA_INIT_FAILED"));
        assert!(s.contains("connection refused"));
        assert!(s.contains("retryable=true"));
    }

    /// Each variant has the code of its Python class; the operation codes
    /// keep theirs.
    #[test]
    fn each_variant_has_the_code_of_its_python_class() {
        let cause = FailureCause::PreconditionFailed;
        let message = || "m".to_owned();
        let cases = [
            (
                BrokerOperationError::OperationFailed {
                    code: BrokerOperationFailedCode::WorkerPingFailed,
                    message: message(),
                    retryable: true,
                    cause: cause.clone(),
                },
                BrokerErrorCode::WorkerPingFailed,
            ),
            (
                BrokerOperationError::EnqueueFailed {
                    message: message(),
                    retryable: true,
                    cause: cause.clone(),
                },
                BrokerErrorCode::EnqueueFailed,
            ),
            (
                BrokerOperationError::PayloadMismatch { message: message() },
                BrokerErrorCode::PayloadMismatch,
            ),
            (
                BrokerOperationError::IdempotencyKeyConflict {
                    message: message(),
                    owner_task_id: Uuid::from_u128(7),
                },
                BrokerErrorCode::IdempotencyKeyConflict,
            ),
            (
                BrokerOperationError::InvalidJsonPayload {
                    message: message(),
                    cause: cause.clone(),
                },
                BrokerErrorCode::InvalidJsonPayload,
            ),
            (
                BrokerOperationError::InvalidTaskOptions {
                    message: message(),
                    cause: cause.clone(),
                },
                BrokerErrorCode::InvalidTaskOptions,
            ),
            (
                BrokerOperationError::InvalidInput {
                    message: message(),
                    cause: cause.clone(),
                },
                BrokerErrorCode::InvalidInput,
            ),
            (
                BrokerOperationError::AsyncContext { message: message() },
                BrokerErrorCode::AsyncContext,
            ),
            (
                BrokerOperationError::ListenerDisabled { message: message() },
                BrokerErrorCode::ListenerDisabled,
            ),
            (
                BrokerOperationError::WaitInsideTask { message: message() },
                BrokerErrorCode::WaitInsideTask,
            ),
        ];
        for (error, code) in cases {
            assert_eq!(error.code(), code);
            assert_eq!(error.message(), "m");
        }
    }

    /// Only the two variants whose cause can clear carry `retryable`; every
    /// other variant never clears.
    #[test]
    fn only_a_failed_operation_or_enqueue_can_be_retryable() {
        let failed = BrokerOperationError::OperationFailed {
            code: BrokerOperationFailedCode::DbPingFailed,
            message: "lost".to_owned(),
            retryable: true,
            cause: FailureCause::Connection,
        };
        let refused = BrokerOperationError::InvalidInput {
            message: "bad".to_owned(),
            cause: FailureCause::PreconditionFailed,
        };
        let async_context = BrokerOperationError::AsyncContext {
            message: "loop".to_owned(),
        };
        assert!(failed.retryable());
        assert!(!refused.retryable());
        assert!(!async_context.retryable());
        assert_eq!(failed.failure_cause(), FailureCause::Connection);
        assert_eq!(
            async_context.failure_cause(),
            FailureCause::PreconditionFailed
        );
    }

    /// The operation codes are Python's `BrokerOperationFailedCode`, in its
    /// order.
    #[test]
    fn the_operation_codes_are_the_nine_of_python() {
        let codes = [
            BrokerOperationFailedCode::SchemaInitFailed,
            BrokerOperationFailedCode::TaskInfoQueryFailed,
            BrokerOperationFailedCode::MonitoringQueryFailed,
            BrokerOperationFailedCode::DbPingFailed,
            BrokerOperationFailedCode::WorkerPingFailed,
            BrokerOperationFailedCode::CleanupFailed,
            BrokerOperationFailedCode::CloseFailed,
            BrokerOperationFailedCode::ListenerStartFailed,
            BrokerOperationFailedCode::ListenerSubscribeFailed,
        ]
        .map(|code| code.broker_code().to_string());
        assert_eq!(
            codes,
            [
                "SCHEMA_INIT_FAILED",
                "TASK_INFO_QUERY_FAILED",
                "MONITORING_QUERY_FAILED",
                "DB_PING_FAILED",
                "WORKER_PING_FAILED",
                "CLEANUP_FAILED",
                "CLOSE_FAILED",
                "LISTENER_START_FAILED",
                "LISTENER_SUBSCRIBE_FAILED",
            ]
        );
    }

    #[test]
    fn invalid_input_has_python_wire_name() {
        assert_eq!(
            serde_json::to_string(&BrokerErrorCode::InvalidInput).unwrap(),
            "\"INVALID_INPUT\""
        );
        assert_eq!(BrokerErrorCode::InvalidInput.to_string(), "INVALID_INPUT");
    }

    #[test]
    fn listener_disabled_has_python_wire_name() {
        assert_eq!(
            serde_json::to_string(&BrokerErrorCode::ListenerDisabled).unwrap(),
            "\"LISTENER_DISABLED\""
        );
    }

    #[test]
    fn all_codes_round_trip() {
        let codes = [
            BrokerErrorCode::SchemaInitFailed,
            BrokerErrorCode::EnqueueFailed,
            BrokerErrorCode::PayloadMismatch,
            BrokerErrorCode::IdempotencyKeyConflict,
            BrokerErrorCode::TaskInfoQueryFailed,
            BrokerErrorCode::MonitoringQueryFailed,
            BrokerErrorCode::DbPingFailed,
            BrokerErrorCode::WorkerPingFailed,
            BrokerErrorCode::CleanupFailed,
            BrokerErrorCode::CloseFailed,
            BrokerErrorCode::ListenerStartFailed,
            BrokerErrorCode::ListenerSubscribeFailed,
            BrokerErrorCode::NoBroker,
            BrokerErrorCode::NoTypeAvailable,
            BrokerErrorCode::InvalidJsonPayload,
            BrokerErrorCode::InvalidTaskOptions,
            BrokerErrorCode::EventLoopLimit,
            BrokerErrorCode::AsyncContext,
            BrokerErrorCode::LoopRunnerFailed,
            BrokerErrorCode::InvalidInput,
            BrokerErrorCode::ListenerDisabled,
            BrokerErrorCode::WaitInsideTask,
        ];
        for code in codes {
            let json = serde_json::to_string(&code).unwrap();
            let back: BrokerErrorCode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, code);
        }
    }
}

#[cfg(test)]
mod stored_envelope_tests {
    use super::*;

    /// The text is kept as stored: an integer above `u128::MAX` keeps every
    /// digit, and the member order and spacing stay.
    #[test]
    fn an_object_is_kept_as_its_stored_text() {
        let text = r#"{"__h_task_result__":true, "ok":340282366920938463463374607431768211456,"err":null}"#;
        let envelope = StoredEnvelopeText::parse(text.to_owned())
            .unwrap()
            .expect("an object is an envelope");
        assert_eq!(envelope.as_str(), text);
    }

    #[test]
    fn the_json_null_is_no_result() {
        assert_eq!(StoredEnvelopeText::parse(" null".to_owned()), Ok(None));
    }

    #[test]
    fn another_json_value_is_refused_with_its_kind() {
        for (text, kind) in [
            ("3", "number"),
            ("-1.5", "number"),
            (r#""x""#, "string"),
            ("[]", "array"),
            ("true", "boolean"),
            (" false", "boolean"),
        ] {
            assert_eq!(
                StoredEnvelopeText::parse(text.to_owned()),
                Err(StoredEnvelopeRefused::NotAnObject(kind)),
                "{text}"
            );
        }
    }

    #[test]
    fn a_text_that_breaks_the_strict_rule_is_refused() {
        assert!(matches!(
            StoredEnvelopeText::parse(r#"{"ok":1,"ok":2}"#.to_owned()),
            Err(StoredEnvelopeRefused::NotStrictJson(
                StrictJsonError::DuplicateKey { .. }
            ))
        ));
    }
}

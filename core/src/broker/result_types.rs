/// Typed error types for PostgresBroker operations.
///
/// Mirrors Python's `clients/python/horsies/core/brokers/result_types.py`.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::broker::class_coverage::ClassReadRefused;
use crate::broker::health::WorkerStateUnreadable;
use crate::broker::process_listener::ListenError;
use crate::broker::{BrokerError, EnqueueRefusal, EnqueueRefusalCause};
use crate::core::codec::{check_json_text, StrictJsonError};
use crate::core::history::enqueue::SendPart;
use crate::core::history::reads::detail::TaskAbsenceReason;
use crate::core::{TaskInfo, TaskStatus};
use crate::error_catalog::ErrorEnum;
use crate::failure_cause::FailureCause;

/// Categorized broker operation failure codes.
///
/// The members are Python's, in Python's order
/// (`clients/python/horsies/core/brokers/result_types.py`). The nine operation codes from
/// `SCHEMA_INIT_FAILED` to `LISTENER_SUBSCRIBE_FAILED` share one variant in
/// Python, `BrokerOperationFailed`. A client produces `NO_BROKER` and
/// `NO_TYPE_AVAILABLE`; only the Python client produces `ASYNC_CONTEXT`.
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
    /// A sync facade was called on a thread that runs an event loop. Its wait
    /// would stall that loop, so nothing ran. Python client only.
    AsyncContext,
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
/// `clients/python/horsies/core/brokers/result_types.py`, each with the facts of its
/// state only. Carried inside `Err(...)` of a [`BrokerResult`]. Each
/// variant's doc is its Python class doc (`ErrorCatalog`).
///
/// The classes that only the Python client produces are not variants:
/// `BrokerNotAttached` and `BrokerNoTypeAvailable`.
///
/// `cause` is the typed cause of the variant's `exception`
/// (`ErrorClassValues`): the [`BrokerOperationSource`] that a failed
/// operation met, the [`BrokerCheckCause`] of a failed enqueue or of stored
/// JSON that does not read, or the [`BrokerRefusalCause`] of a refused
/// argument, input or task options. A variant without `cause` has the
/// cause of its state ([`Self::failure_cause`]). A variant without
/// `retryable` never clears. The constructors of the operation codes build
/// Python's message of each operation; [`Self::enqueue_error`] decides the
/// code of a failed enqueue.
#[derive(Debug, horsies_core_derive::ErrorCatalog, horsies_core_derive::ErrorClassValues)]
#[error_catalog(
    module = "broker_errors",
    code = "BrokerErrorCode",
    union = "BrokerCoreError",
    union_doc = "Every failure of a broker operation that the core gives: one class per state."
)]
pub enum BrokerOperationError {
    /// A broker operation failed with a database, driver or connection error.
    ///
    /// ``code`` names the operation. ``retryable`` is the core's classification
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
        /// What the operation met.
        #[error_slot(cause)]
        cause: Box<BrokerOperationSource>,
    },
    /// The enqueue write failed or could not be confirmed. The row may or may not exist.
    ///
    /// ``retryable`` is the core's classification of the cause. An enqueue
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
        /// The broker error of the write, or the check that found the
        /// conflicting row gone.
        #[error_slot(cause)]
        cause: Box<BrokerCheckCause>,
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
        /// The error that refused the value, or the check of the core.
        #[error_slot(cause)]
        cause: Box<BrokerCheckCause>,
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
        /// The check of the core, the text that does not read as JSON, or
        /// the rule that refused the value.
        #[error_slot(cause)]
        cause: BrokerRefusalCause,
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
        /// The check of the core, the text that does not read as JSON, or
        /// the rule that refused the value.
        #[error_slot(cause)]
        cause: BrokerRefusalCause,
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

    /// The cause of the failure: the cause of the variant's `cause`, else
    /// the cause of its state. A check of the core that refused stored JSON
    /// is a broken read contract; a check that found an enqueue whose row
    /// was gone is a failed precondition. A refused argument, input or task
    /// options, a refused key, a refused thread, a missing listener and a
    /// wait in a task process are failed preconditions. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::OperationFailed { cause, .. } => cause.failure_cause(),
            Self::InvalidJsonPayload { cause, .. } => {
                cause.failure_cause_or(FailureCause::ContractViolation)
            }
            Self::EnqueueFailed { cause, .. } => {
                cause.failure_cause_or(FailureCause::PreconditionFailed)
            }
            Self::InvalidTaskOptions { .. }
            | Self::InvalidInput { .. }
            | Self::PayloadMismatch { .. }
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

impl std::error::Error for BrokerOperationError {
    /// The variant's `cause`, when it has one and it is an error. A refused
    /// argument, input or task options has no error.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::OperationFailed { cause, .. } => Some(&**cause),
            Self::EnqueueFailed { cause, .. } | Self::InvalidJsonPayload { cause, .. } => {
                cause.error()
            }
            Self::InvalidTaskOptions { .. }
            | Self::InvalidInput { .. }
            | Self::PayloadMismatch { .. }
            | Self::IdempotencyKeyConflict { .. }
            | Self::AsyncContext { .. }
            | Self::ListenerDisabled { .. }
            | Self::WaitInsideTask { .. } => None,
        }
    }
}

impl BrokerOperationError {
    /// `code` failed with `source`, with the message `message`; the retry
    /// rule is the source's. The constructors below give Python's message of
    /// each operation; a client uses this one for a failure of its own (a
    /// process runtime that did not start, the connect before a read). Pure.
    pub fn operation_failed(
        code: BrokerOperationFailedCode,
        message: String,
        source: BrokerOperationSource,
    ) -> Self {
        Self::OperationFailed {
            code,
            message,
            retryable: source.is_retryable(),
            cause: Box::new(source),
        }
    }

    /// `SCHEMA_INIT_FAILED`: the schema install failed with `error`
    /// (Python: `Schema initialization failed: {error}`). Pure.
    pub fn schema_init_failed(error: BrokerError) -> Self {
        Self::operation_failed(
            BrokerOperationFailedCode::SchemaInitFailed,
            format!("Schema initialization failed: {error}"),
            BrokerOperationSource::Broker(error),
        )
    }

    /// `TASK_INFO_QUERY_FAILED` of `get_task_info` for `task_id` (Python:
    /// `get_task_info failed for {task_id}: {source}`). Pure.
    pub fn task_info_query_failed(task_id: Uuid, source: BrokerOperationSource) -> Self {
        Self::operation_failed(
            BrokerOperationFailedCode::TaskInfoQueryFailed,
            format!("get_task_info failed for {task_id}: {source}"),
            source,
        )
    }

    /// `INVALID_JSON_PAYLOAD` of `get_task_info` for `task_id`: a stored
    /// result that fails its check, with the check's text `detail` (Python:
    /// `get_task_info failed for {task_id}: {detail}`). Pure.
    pub fn task_info_invalid_json(
        task_id: Uuid,
        detail: &dyn std::fmt::Display,
        cause: BrokerCheckCause,
    ) -> Self {
        Self::InvalidJsonPayload {
            message: format!("get_task_info failed for {task_id}: {detail}"),
            cause: Box::new(cause),
        }
    }

    /// `TASK_INFO_QUERY_FAILED` of `get_raw_result_record` for `task_id`
    /// (Python: `Broker error while retrieving task {task_id}: {error}`).
    /// Pure.
    pub fn raw_result_query_failed(task_id: Uuid, error: BrokerError) -> Self {
        Self::operation_failed(
            BrokerOperationFailedCode::TaskInfoQueryFailed,
            format!("Broker error while retrieving task {task_id}: {error}"),
            BrokerOperationSource::Broker(error),
        )
    }

    /// `MONITORING_QUERY_FAILED` of the read `operation` (Python:
    /// `{operation} failed: {source}`). Pure.
    pub fn monitoring_query_failed(operation: &str, source: BrokerOperationSource) -> Self {
        Self::operation_failed(
            BrokerOperationFailedCode::MonitoringQueryFailed,
            format!("{operation} failed: {source}"),
            source,
        )
    }

    /// `MONITORING_QUERY_FAILED` of the class read `operation` that stored
    /// data or a bound refused, not retryable (Python: `{operation} refused:
    /// {refused}`). Pure.
    pub fn monitoring_query_refused(operation: &str, refused: ClassReadRefused) -> Self {
        Self::operation_failed(
            BrokerOperationFailedCode::MonitoringQueryFailed,
            format!("{operation} refused: {refused}"),
            BrokerOperationSource::ClassRead(refused),
        )
    }

    /// `DB_PING_FAILED`: the ping met `error` (Python: `ping_database failed:
    /// {error}`). Pure.
    pub fn db_ping_failed(error: BrokerError) -> Self {
        Self::operation_failed(
            BrokerOperationFailedCode::DbPingFailed,
            format!("ping_database failed: {error}"),
            BrokerOperationSource::Broker(error),
        )
    }

    /// `WORKER_PING_FAILED`: the step `step` of the ping met `source`, with
    /// the step's Python message ([`WorkerPingStep::message`]). Pure.
    pub fn worker_ping_failed(step: WorkerPingStep, source: BrokerOperationSource) -> Self {
        Self::operation_failed(
            BrokerOperationFailedCode::WorkerPingFailed,
            step.message(&source),
            source,
        )
    }

    /// `INVALID_INPUT`: a check of the core refused an argument before
    /// anything ran; `message` names the argument and the rule. Pure.
    pub fn invalid_input(message: String) -> Self {
        Self::InvalidInput {
            message,
            cause: BrokerRefusalCause::Check,
        }
    }

    /// The class of a failed enqueue of the task `task_name`: the code that
    /// Python's broker gave `error`, with its message and its cause. Pure.
    ///
    /// - The core refused the send before any statement: by the refused
    ///   part ([`Self::enqueue_refused`]).
    /// - A value that does not serialize, or a reservation window out of
    ///   range: `INVALID_INPUT` with the error's text and no exception (a
    ///   check of the core).
    /// - The task id holds another command or key: `PAYLOAD_MISMATCH` with
    ///   the error's text.
    /// - Another task holds the key with another command:
    ///   `IDEMPOTENCY_KEY_CONFLICT` with the error's text and the owner.
    /// - The conflicting row was gone before the compare: `ENQUEUE_FAILED`,
    ///   not retryable, with the error's text (Python's) and no exception:
    ///   the broker found it by a data check.
    /// - Every other error: [`Self::enqueue_failed`].
    pub fn enqueue_error(task_name: &str, error: BrokerError) -> Self {
        match error {
            BrokerError::EnqueueContract(refusal) => Self::enqueue_refused(task_name, &refusal),
            BrokerError::Serialization(_) | BrokerError::InvalidIdempotencyReservationWindow(_) => {
                Self::InvalidInput {
                    message: error.to_string(),
                    cause: BrokerRefusalCause::Check,
                }
            }
            BrokerError::PayloadMismatch { .. } => Self::PayloadMismatch {
                message: error.to_string(),
            },
            BrokerError::IdempotencyKeyConflict {
                task_id: owner_task_id,
                ..
            } => Self::IdempotencyKeyConflict {
                message: error.to_string(),
                owner_task_id,
            },
            BrokerError::EnqueueConflictUnverifiable { .. } => Self::EnqueueFailed {
                message: error.to_string(),
                retryable: false,
                cause: Box::new(BrokerCheckCause::Check),
            },
            BrokerError::InvalidWorkflowNodeStatus
            | BrokerError::IncompleteTaskHistoryCutover
            | BrokerError::InvalidStatus(_)
            | BrokerError::InvalidExecutionClass(_)
            | BrokerError::ConnectionFailed(_)
            | BrokerError::InvalidConnectionSettings(_)
            | BrokerError::RetentionClassKey(_)
            | BrokerError::ClientFamilyMismatch { .. }
            | BrokerError::UnknownStoredFamily(_)
            | BrokerError::SchemaRefused(_)
            | BrokerError::InstallLock(_)
            | BrokerError::InstallCoverage(_)
            | BrokerError::ListenerClosed
            | BrokerError::TerminalizationContract(_)
            | BrokerError::HistoryReadContract(_)
            | BrokerError::InvalidClaimArgument(_)
            | BrokerError::ClaimContract(_)
            | BrokerError::Statement(_)
            | BrokerError::PoolSettings(_)
            | BrokerError::RowDecode(_)
            | BrokerError::ProcessRuntime(_) => Self::enqueue_failed(task_name, error),
        }
    }

    /// `INVALID_INPUT` (the input, or a fact of the call that the input
    /// carries) or `INVALID_TASK_OPTIONS`, by the part that the core refused
    /// before any statement. Nothing was written. Pure.
    ///
    /// A rule of the version-1 command or of the scoped idempotency key has
    /// Python's message (`enqueue input refused for {task_name}: {rule}`,
    /// `idempotency key refused for {task_name}: {rule}`) and the rule as
    /// the cause (Python: `ValueError`). A text that does not read as JSON
    /// has the reader's error as the cause (Python: `SerializationError`),
    /// and a rule that refused a value that reads has no exception; both
    /// have the text of the core's error.
    pub fn enqueue_refused(task_name: &str, refusal: &EnqueueRefusal) -> Self {
        let contract_text = || BrokerError::EnqueueContract(refusal.clone()).to_string();
        let (message, cause) = match &refusal.cause {
            EnqueueRefusalCause::JsonUnreadable(reader_error) => (
                contract_text(),
                BrokerRefusalCause::JsonUnreadable(reader_error.clone()),
            ),
            EnqueueRefusalCause::RuleRefused => (contract_text(), BrokerRefusalCause::Check),
            EnqueueRefusalCause::CommandRuleRefused(rule) => (
                format!("enqueue input refused for {task_name}: {rule}"),
                BrokerRefusalCause::ValueRule(rule.clone()),
            ),
            EnqueueRefusalCause::KeyRuleRefused(rule) => (
                format!("idempotency key refused for {task_name}: {rule}"),
                BrokerRefusalCause::ValueRule(rule.clone()),
            ),
        };
        match refusal.part {
            SendPart::Input => Self::InvalidInput { message, cause },
            SendPart::TaskOptions => Self::InvalidTaskOptions { message, cause },
        }
    }

    /// `ENQUEUE_FAILED` of the task `task_name` with `error`: Python's
    /// message (`Failed to enqueue task {task_name}: {error}`), the error's
    /// retry rule, and the error as the cause. The row may or may not
    /// exist. Pure.
    pub fn enqueue_failed(task_name: &str, error: BrokerError) -> Self {
        Self::EnqueueFailed {
            message: format!("Failed to enqueue task {task_name}: {error}"),
            retryable: error.is_retryable(),
            cause: Box::new(BrokerCheckCause::Error(error)),
        }
    }
}

/// A step of a worker ping that can fail, with the start of Python's
/// message of its `WORKER_PING_FAILED`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerPingStep {
    /// The NOTIFY of the ping (or the connect before it).
    Notify,
    /// The subscription of the reply channel.
    ReplySubscribe,
    /// The encode of the ping request.
    PayloadEncode,
    /// The wait for the replies.
    ReplyWait,
}

impl WorkerPingStep {
    /// Python's message of a failure of this step with the cause `source`.
    /// Pure.
    pub fn message(self, source: &dyn std::fmt::Display) -> String {
        match self {
            Self::Notify => format!("ping_workers failed: {source}"),
            Self::ReplySubscribe => format!("ping_workers reply subscribe failed: {source}"),
            Self::PayloadEncode => format!("ping_workers payload encode failed: {source}"),
            Self::ReplyWait => format!("ping_workers reply wait failed: {source}"),
        }
    }
}

/// What a failed broker operation met: the cause of
/// [`BrokerOperationError::OperationFailed`]. The client builds the
/// exception of the class from it: the core's class of the cause, with the
/// cause's own text (`ExceptionCause`).
#[derive(Debug, thiserror::Error)]
pub enum BrokerOperationSource {
    /// A connect, a statement or a row decode of the broker failed.
    #[error(transparent)]
    Broker(#[from] BrokerError),
    /// A class read met stored data that it cannot decide on, or a bound
    /// that it reached. The same rows give the same answer.
    #[error(transparent)]
    ClassRead(#[from] ClassReadRefused),
    /// A stored worker snapshot row does not read. The same row gives the
    /// same answer.
    #[error(transparent)]
    Snapshot(#[from] WorkerStateUnreadable),
    /// The broker's listener refused the reply subscription of a worker
    /// ping.
    #[error(transparent)]
    Listen(#[from] ListenError),
    /// The task was absent from the live table and live in the history read
    /// on each of the `passes` reads of a task info: a send of the id
    /// committed between the two statements every time. A later read can
    /// succeed.
    #[error(
        "task {task_id} was absent from the live table and live in the history read on \
         {passes} passes in a row"
    )]
    TaskInfoMoved { task_id: Uuid, passes: u32 },
    /// A client could not read an argument as the value that the operation
    /// takes (Python: the exception of the conversion, for example the
    /// window of a worker ping that is not a number of seconds). The text is
    /// the client's. The same argument gives the same failure.
    #[error("{0}")]
    ArgumentUnreadable(String),
}

impl BrokerOperationSource {
    /// Whether the condition may clear: the error's own rule; stored data
    /// that does not read never clears; a task that moved may be read
    /// again. Pure.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Broker(error) => error.is_retryable(),
            Self::Listen(error) => error.is_retryable(),
            Self::ClassRead(_) | Self::Snapshot(_) | Self::ArgumentUnreadable(_) => false,
            Self::TaskInfoMoved { .. } => true,
        }
    }
}

/// The cause of a failed enqueue or of stored JSON that does not read,
/// whose `exception` may be `NoCause()`: the broker error that the
/// operation met, or a check of the core that found the failure with no
/// error. Python builds `NoCause()` for a check.
#[derive(Debug, thiserror::Error)]
pub enum BrokerCheckCause {
    /// The error that refused the value.
    #[error(transparent)]
    Error(#[from] BrokerError),
    /// A check of the core refused the value; no error was raised.
    #[error("a check of the core refused the value")]
    Check,
}

impl BrokerCheckCause {
    /// The cause of the error, or `check` for a check of the core. Pure.
    pub fn failure_cause_or(&self, check: FailureCause) -> FailureCause {
        match self {
            Self::Error(error) => error.failure_cause(),
            Self::Check => check,
        }
    }

    /// The error, when the cause is one. Pure.
    pub fn error(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Error(error) => Some(error),
            Self::Check => None,
        }
    }
}

/// The cause of a refused argument, input or task options: what the client
/// builds the `exception` of `INVALID_INPUT` and `INVALID_TASK_OPTIONS`
/// from. Python's broker gave `NoCause()` for a check, the JSON reader's
/// `SerializationError` for a text that does not read, and the
/// `ValueError` of the version-1 command or of the scoped idempotency key
/// for a rule of either.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerRefusalCause {
    /// A check of the core refused the value; no error was raised.
    Check,
    /// The refused text does not read as JSON: the reader's error text.
    JsonUnreadable(String),
    /// A rule of the version-1 command or of the scoped idempotency key
    /// refused a value: the rule's text.
    ValueRule(String),
}

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
        let err = BrokerOperationError::schema_init_failed(BrokerError::ConnectionFailed(
            "connection refused".to_owned(),
        ));
        let s = format!("{}", err);
        assert!(s.contains("SCHEMA_INIT_FAILED"));
        assert!(s.contains("connection refused"));
        assert!(s.contains("retryable=true"));
    }

    /// Each variant has the code of its Python class; the operation codes
    /// keep theirs.
    #[test]
    fn each_variant_has_the_code_of_its_python_class() {
        let message = || "m".to_owned();
        let cases = [
            (
                BrokerOperationError::operation_failed(
                    BrokerOperationFailedCode::WorkerPingFailed,
                    message(),
                    BrokerOperationSource::Broker(BrokerError::ListenerClosed),
                ),
                BrokerErrorCode::WorkerPingFailed,
            ),
            (
                BrokerOperationError::EnqueueFailed {
                    message: message(),
                    retryable: true,
                    cause: Box::new(BrokerCheckCause::Check),
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
                    cause: Box::new(BrokerCheckCause::Check),
                },
                BrokerErrorCode::InvalidJsonPayload,
            ),
            (
                BrokerOperationError::InvalidTaskOptions {
                    message: message(),
                    cause: BrokerRefusalCause::Check,
                },
                BrokerErrorCode::InvalidTaskOptions,
            ),
            (
                BrokerOperationError::InvalidInput {
                    message: message(),
                    cause: BrokerRefusalCause::Check,
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
        let failed =
            BrokerOperationError::db_ping_failed(BrokerError::ConnectionFailed("lost".to_owned()));
        let refused = BrokerOperationError::invalid_input("bad".to_owned());
        let async_context = BrokerOperationError::AsyncContext {
            message: "loop".to_owned(),
        };
        assert!(failed.retryable());
        assert!(!refused.retryable());
        assert!(!async_context.retryable());
        assert_eq!(failed.failure_cause(), FailureCause::Connection);
        assert_eq!(refused.failure_cause(), FailureCause::PreconditionFailed);
        assert_eq!(
            async_context.failure_cause(),
            FailureCause::PreconditionFailed
        );
    }

    /// Each constructor of an operation code gives Python's message of the
    /// operation, the code, and the retry rule of its cause.
    #[test]
    fn each_operation_code_constructor_gives_python_s_message() {
        let task_id = Uuid::from_u128(7);
        let refused = || BrokerError::ConnectionFailed("refused".to_owned());
        let cases = [
            (
                BrokerOperationError::schema_init_failed(refused()),
                BrokerErrorCode::SchemaInitFailed,
                "Schema initialization failed: connection failed: refused".to_owned(),
            ),
            (
                BrokerOperationError::task_info_query_failed(
                    task_id,
                    BrokerOperationSource::Broker(refused()),
                ),
                BrokerErrorCode::TaskInfoQueryFailed,
                format!("get_task_info failed for {task_id}: connection failed: refused"),
            ),
            (
                BrokerOperationError::raw_result_query_failed(task_id, refused()),
                BrokerErrorCode::TaskInfoQueryFailed,
                format!("Broker error while retrieving task {task_id}: connection failed: refused"),
            ),
            (
                BrokerOperationError::monitoring_query_failed(
                    "list_worker_states",
                    BrokerOperationSource::Broker(refused()),
                ),
                BrokerErrorCode::MonitoringQueryFailed,
                "list_worker_states failed: connection failed: refused".to_owned(),
            ),
            (
                BrokerOperationError::db_ping_failed(refused()),
                BrokerErrorCode::DbPingFailed,
                "ping_database failed: connection failed: refused".to_owned(),
            ),
            (
                BrokerOperationError::worker_ping_failed(
                    WorkerPingStep::Notify,
                    BrokerOperationSource::Broker(refused()),
                ),
                BrokerErrorCode::WorkerPingFailed,
                "ping_workers failed: connection failed: refused".to_owned(),
            ),
        ];
        for (error, code, message) in cases {
            assert_eq!(error.code(), code);
            assert_eq!(error.message(), message);
            assert!(error.retryable());
            assert_eq!(error.failure_cause(), FailureCause::Connection);
            assert!(std::error::Error::source(&error).is_some());
        }
        let refused_argument = BrokerOperationError::invalid_input("bad".to_owned());
        assert!(std::error::Error::source(&refused_argument).is_none());
    }

    /// Python's message of each failing step of a worker ping.
    #[test]
    fn each_worker_ping_step_has_python_s_message() {
        assert_eq!(
            WorkerPingStep::Notify.message(&"x"),
            "ping_workers failed: x"
        );
        assert_eq!(
            WorkerPingStep::ReplySubscribe.message(&"x"),
            "ping_workers reply subscribe failed: x"
        );
        assert_eq!(
            WorkerPingStep::PayloadEncode.message(&"x"),
            "ping_workers payload encode failed: x"
        );
        assert_eq!(
            WorkerPingStep::ReplyWait.message(&"x"),
            "ping_workers reply wait failed: x"
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
            BrokerErrorCode::AsyncContext,
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

/// Typed error types for task send operations.
///
/// Mirrors Python's `clients/python/horsies/core/models/task_send_types.py`: one variant per
/// code, and the core maps every enqueue error to its variant once
/// ([`send_error_from_enqueue`]).
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::{BrokerError, BrokerRefusalCause};
use crate::core::config::EnqueueRetention;
use crate::error_catalog::{
    ErrorEnum, ExceptionCause, ExceptionValue, RecordFieldValue, RecordValue,
};
use crate::FailureCause;

/// Categorized task send failure codes.
///
/// The members are Python's, in Python's order
/// (`clients/python/horsies/core/models/task_send_types.py`). [`TaskSendError`] has
/// one variant per member.
#[derive(Debug, Clone, PartialEq, Eq, Hash, horsies_core_derive::CodeCatalog)]
#[code_catalog(module = "task_send", base = "str_enum")]
pub enum TaskSendErrorCode {
    /// Send called while suppression is active (worker/scheduler import phase,
    /// `check()`, or `HORSIES_SUPPRESS_SENDS=1`). Non-retryable.
    SendSuppressed,
    /// A sync (Python) or blocking (Rust) send or resend was called
    /// on a thread that runs an event loop (Python) or a tokio runtime
    /// (Rust); the blocking enqueue would stall it, so nothing was written.
    /// A blocking resend on the same thread returns this code again; the
    /// async forms complete the dispatch with the same task id.
    #[code_catalog(blocking_form_only)]
    AsyncContext,
    /// Programmer error (bad args, serialization failure, unresolved queue).
    ValidationFailed,
    /// Infrastructure error (schema init, DB transaction). Conditionally retryable.
    EnqueueFailed,
    /// The task id exists with a different command or idempotency key.
    /// Nothing was written. The resend methods refuse it.
    PayloadMismatch,
    /// The idempotency key is reserved by another task with a different
    /// command. Nothing was written. Non-retryable: send the same command, or
    /// use another key. The resend methods refuse it.
    IdempotencyKeyConflict,
    /// Serialized payload exceeded `payload.reject_bytes`; nothing was
    /// written. Non-retryable — shrink the payload (pass a reference to
    /// external storage) or raise the configured limit.
    PayloadTooLarge,
}

impl std::fmt::Display for TaskSendErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SendSuppressed => write!(f, "SEND_SUPPRESSED"),
            Self::AsyncContext => write!(f, "ASYNC_CONTEXT"),
            Self::ValidationFailed => write!(f, "VALIDATION_FAILED"),
            Self::EnqueueFailed => write!(f, "ENQUEUE_FAILED"),
            Self::PayloadMismatch => write!(f, "PAYLOAD_MISMATCH"),
            Self::IdempotencyKeyConflict => write!(f, "IDEMPOTENCY_KEY_CONFLICT"),
            Self::PayloadTooLarge => write!(f, "PAYLOAD_TOO_LARGE"),
        }
    }
}

/// Serialized envelope for an idempotent resend.
///
/// All value fields are pre-serialized to avoid round-trip issues.
/// The resend methods extract this payload directly — no user input needed.
/// A replay carries the same command and `sent_at`, so the exact-id resend
/// check accepts it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSendPayload {
    pub task_name: String,
    pub queue_name: String,
    pub priority: i32,
    /// The encoded task input as one JSON value.
    pub input_json: String,
    pub sent_at: DateTime<Utc>,
    pub good_until: Option<DateTime<Utc>>,
    pub enqueue_delay_seconds: Option<i64>,
    pub task_options: Option<String>,
    pub idempotency_key: Option<String>,
    /// The retention of the send: the default class, forever or a period.
    pub retention: crate::core::config::EnqueueRetention,
}

impl TaskSendPayload {
    /// The fields of the Python record `TaskSendPayload`
    /// (`error_catalog::TASK_SEND_PAYLOAD`), by name, in the record's order.
    /// The input is its text (Python's `None` stores the same `{}`), and
    /// the retention is its class key, or `None` for forever. Pure.
    pub fn record_values(&self) -> Vec<RecordFieldValue<'_>> {
        fn field<'a>(name: &'static str, value: RecordValue<'a>) -> RecordFieldValue<'a> {
            RecordFieldValue { name, value }
        }
        let retention_class_key = match self.retention {
            EnqueueRetention::Forever => None,
            EnqueueRetention::Default | EnqueueRetention::Period(_) => {
                Some(self.retention.class_key())
            }
        };
        vec![
            field("task_name", RecordValue::Str(&self.task_name)),
            field("queue_name", RecordValue::Str(&self.queue_name)),
            field("priority", RecordValue::Int(i64::from(self.priority))),
            field(
                "input_json",
                RecordValue::OptionalStr(Some(self.input_json.as_str())),
            ),
            field("sent_at", RecordValue::DateTime(self.sent_at)),
            field("good_until", RecordValue::OptionalDateTime(self.good_until)),
            field(
                "enqueue_delay_seconds",
                RecordValue::OptionalInt(self.enqueue_delay_seconds),
            ),
            field(
                "task_options",
                RecordValue::OptionalStr(self.task_options.as_deref()),
            ),
            field(
                "idempotency_key",
                RecordValue::OptionalStr(self.idempotency_key.as_deref()),
            ),
            field(
                "retention_class_key",
                RecordValue::OptionalRetentionClassKey(retention_class_key),
            ),
        ]
    }
}

/// Error from task send operations: one variant per
/// [`TaskSendErrorCode`] that the core or a client's blocking form
/// produces, each with only the facts of its state (Python
/// `clients/python/horsies/core/models/task_send_types.py`).
///
/// The `///` lines of each variant are the docstring of its generated
/// Python class (`horsies._catalog.task_send_errors`); Rust notes are `//`
/// comments.
#[derive(
    Debug, Clone, horsies_core_derive::ErrorCatalog, horsies_core_derive::ErrorClassValues,
)]
#[error_catalog(
    module = "task_send_errors",
    code = "TaskSendErrorCode",
    union = "TaskSendCoreError",
    union_doc = "Every failure of a send that the core records."
)]
pub enum TaskSendError {
    /// Sends are suppressed in this process. Nothing was serialized or written.
    ///
    /// Suppression is active during the worker and scheduler import phase,
    /// during ``check()``, and under ``HORSIES_SUPPRESS_SENDS=1``. The resend
    /// methods refuse this error.
    #[error_class(class = "TaskSendSuppressed", code = "SendSuppressed")]
    Suppressed {
        #[error_slot(python = "Str", hidden)]
        message: String,
    },
    /// A sync send ran inside a running event loop. Nothing was written.
    ///
    /// The payload is prepared. ``resend_async(err)`` completes the same
    /// dispatch with the same task id. A sync resend from the same loop returns
    /// this error again.
    // The Rust client's blocking forms give it on a thread that runs a tokio
    // runtime; the core's enqueue never does.
    #[error_class(class = "TaskSendAsyncContext", code = "AsyncContext", never_retryable)]
    AsyncContext {
        #[error_slot(python = "Uuid")]
        task_id: Uuid,
        #[error_slot(python = "TaskSendPayload", hidden)]
        payload: Box<TaskSendPayload>,
        #[error_slot(python = "Str", hidden)]
        message: String,
    },
    /// The input was refused before anything was written.
    ///
    /// ``cause`` is the exception of the cause, or ``NoCause()`` when a check refused the
    /// input without a Python exception. The resend methods refuse this error.
    // `cause` is what refused it.
    #[error_class(
        class = "TaskSendValidationFailed",
        code = "ValidationFailed",
        exception = "BaseException | NoCause"
    )]
    ValidationFailed {
        #[error_slot(python = "Str", hidden)]
        message: String,
        /// What refused the input: a rule, a text that does not read as JSON,
        /// or a check of the core.
        #[error_slot(cause)]
        cause: BrokerRefusalCause,
    },
    /// The broker or the database failed during enqueue. The row may or may not exist.
    ///
    /// ``retryable`` is the core's classification of the cause. ``resend(err)``
    /// sends the same task id and payload again; the broker returns the existing
    /// task for a duplicate. ``cause`` is ``NoCause()`` when the broker found
    /// the failure by a data check.
    // `cause` is what the enqueue met.
    #[error_class(
        class = "TaskSendEnqueueFailed",
        code = "EnqueueFailed",
        exception = "BaseException | NoCause"
    )]
    EnqueueFailed {
        #[error_slot(python = "Uuid")]
        task_id: Uuid,
        #[error_slot(python = "TaskSendPayload", hidden)]
        payload: Box<TaskSendPayload>,
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "Bool")]
        retryable: bool,
        /// The exception of the failure.
        #[error_slot(cause)]
        cause: Box<EnqueueFailureCause>,
    },
    /// The task id exists with a different command or idempotency key. Nothing was written.
    ///
    /// The message names which one differs. The resend methods refuse this error.
    #[error_class(class = "TaskSendPayloadMismatch", code = "PayloadMismatch")]
    PayloadMismatch {
        #[error_slot(python = "Uuid")]
        task_id: Uuid,
        #[error_slot(python = "TaskSendPayload", hidden)]
        payload: Box<TaskSendPayload>,
        #[error_slot(python = "Str", hidden)]
        message: String,
    },
    /// The serialized kwargs exceed ``payload.reject_bytes``. Nothing was written.
    ///
    /// ``size_bytes`` is the length of the serialized kwargs and ``limit_bytes``
    /// the configured limit. The resend methods refuse this error.
    #[error_class(class = "TaskSendPayloadTooLarge", code = "PayloadTooLarge")]
    PayloadTooLarge {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "Int")]
        size_bytes: u64,
        #[error_slot(python = "Int")]
        limit_bytes: u64,
    },
    /// The idempotency key is reserved by another task with a different command.
    ///
    /// Nothing was written. ``owner_task_id`` names the task that holds the key.
    /// ``retryable`` is ``False``: a repeat of the same send does not complete
    /// it. While the owner task is live, the repeat meets the same conflict.
    /// After the reservation ends, the repeat starts a second task under the
    /// key. To complete the send, send the same command as the owner task, or
    /// use another key. The resend methods refuse this error.
    #[error_class(
        class = "TaskSendIdempotencyKeyConflict",
        code = "IdempotencyKeyConflict",
        never_retryable
    )]
    IdempotencyKeyConflict {
        #[error_slot(python = "Uuid")]
        task_id: Uuid,
        #[error_slot(python = "TaskSendPayload", hidden)]
        payload: Box<TaskSendPayload>,
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "Uuid")]
        owner_task_id: Uuid,
    },
}

/// What a resend needs: the task id and the payload of the first send.
#[derive(Debug, Clone, Copy)]
pub struct ResendTarget<'a> {
    pub task_id: Uuid,
    pub payload: &'a TaskSendPayload,
}

impl TaskSendError {
    /// The code of this variant (the derive's one mapping,
    /// [`ErrorEnum::error_code`]).
    pub fn code(&self) -> TaskSendErrorCode {
        ErrorEnum::error_code(self)
    }

    /// Whether the condition may clear, so the same send can succeed later.
    /// Only an enqueue failure carries a driver classification; every
    /// refusal, the key conflict included, stays. An async context does
    /// not clear on its thread (its class has `retryable = False`): the
    /// async forms complete the send.
    pub fn retryable(&self) -> bool {
        match self {
            Self::EnqueueFailed { retryable, .. } => *retryable,
            Self::AsyncContext { .. }
            | Self::IdempotencyKeyConflict { .. }
            | Self::Suppressed { .. }
            | Self::ValidationFailed { .. }
            | Self::PayloadMismatch { .. }
            | Self::PayloadTooLarge { .. } => false,
        }
    }

    /// The human-readable description. It never holds input values.
    pub fn message(&self) -> &str {
        match self {
            Self::Suppressed { message }
            | Self::AsyncContext { message, .. }
            | Self::ValidationFailed { message, .. }
            | Self::EnqueueFailed { message, .. }
            | Self::PayloadMismatch { message, .. }
            | Self::PayloadTooLarge { message, .. }
            | Self::IdempotencyKeyConflict { message, .. } => message.as_str(),
        }
    }

    /// What the send met. A variant without `cause` has the cause of its
    /// state: a refusal of the call, an existing row or a held key is a
    /// failed precondition. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::EnqueueFailed { cause, .. } => cause.failure_cause(),
            Self::Suppressed { .. }
            | Self::ValidationFailed { .. }
            | Self::AsyncContext { .. }
            | Self::PayloadMismatch { .. }
            | Self::PayloadTooLarge { .. }
            | Self::IdempotencyKeyConflict { .. } => FailureCause::PreconditionFailed,
        }
    }

    /// The task id of the send, when the send got one.
    pub fn task_id(&self) -> Option<Uuid> {
        match self {
            Self::AsyncContext { task_id, .. }
            | Self::EnqueueFailed { task_id, .. }
            | Self::PayloadMismatch { task_id, .. }
            | Self::IdempotencyKeyConflict { task_id, .. } => Some(*task_id),
            Self::Suppressed { .. }
            | Self::ValidationFailed { .. }
            | Self::PayloadTooLarge { .. } => None,
        }
    }

    /// The task id and payload when the resend methods accept this error,
    /// else `None`. Python's accepted set (`TaskSendResendable`,
    /// `clients/python/horsies/core/models/task_send_types.py`): an async context
    /// and an enqueue failure. A refusal is never replayed, because the same
    /// input meets the same refusal. A key conflict is a refusal: a repeat of
    /// the same send meets the same conflict, or starts a second task under
    /// the key once the reservation ends.
    pub fn resend_target(&self) -> Option<ResendTarget<'_>> {
        match self {
            Self::AsyncContext {
                task_id, payload, ..
            }
            | Self::EnqueueFailed {
                task_id, payload, ..
            } => Some(ResendTarget {
                task_id: *task_id,
                payload: payload.as_ref(),
            }),
            Self::Suppressed { .. }
            | Self::ValidationFailed { .. }
            | Self::PayloadMismatch { .. }
            | Self::PayloadTooLarge { .. }
            | Self::IdempotencyKeyConflict { .. } => None,
        }
    }
}

impl std::fmt::Display for TaskSendError {
    /// Redacted display — never leak the input in logs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TaskSendError(code={}, retryable={}, task_id={:?})",
            self.code(),
            self.retryable(),
            self.task_id(),
        )
    }
}

impl std::error::Error for TaskSendError {}

/// The cause of `ENQUEUE_FAILED`: the `exception` of the generated class
/// `TaskSendEnqueueFailed`.
///
/// Python stores the exception of the broker class of the failure
/// ([`crate::DatabaseOperationError::enqueue_error`]). Its two forms are the
/// core's class of the error and `NoCause()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueFailureCause {
    /// An error of the enqueue: the core's class of what it met, with the
    /// error's own text and retry rule. [`send_error_from_enqueue`] sets
    /// `text` to the `message` of the send error and `retryable` to its
    /// `retryable`.
    Error {
        text: String,
        retryable: bool,
        failure: FailureCause,
    },
    /// A check of the core found the failure; no error was raised: a
    /// conflict whose row was gone before the compare
    /// ([`BrokerError::EnqueueConflictUnverifiable`]).
    Check,
}

impl EnqueueFailureCause {
    /// What the enqueue met; a check is a failed precondition. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::Error { failure, .. } => failure.clone(),
            Self::Check => FailureCause::PreconditionFailed,
        }
    }

    /// The cause of the failed enqueue `error`: its text, its retry rule and
    /// its [`BrokerError::failure_cause`]. Pure.
    fn of_error(error: &BrokerError) -> Self {
        Self::Error {
            text: error.to_string(),
            retryable: error.is_retryable(),
            failure: error.failure_cause(),
        }
    }
}

/// An error of the enqueue is the core's class of its cause, with its text
/// and its retry rule; a check of the core is `NoCause()`.
impl ExceptionCause for EnqueueFailureCause {
    fn exception_value(&self) -> ExceptionValue {
        match self {
            Self::Error {
                text,
                retryable,
                failure,
            } => ExceptionValue::CoreFailure {
                text: text.clone(),
                retryable: *retryable,
                cause: failure.clone(),
            },
            Self::Check => ExceptionValue::NoCause,
        }
    }
}

/// The send error for a failed enqueue, by Python's rule
/// (`clients/python/horsies/core/task_decorator.py:1248-1310`):
/// - a refusal of the input, the options or the call (`EnqueueContract`,
///   `Serialization`, `InvalidIdempotencyReservationWindow`) is
///   `VALIDATION_FAILED`, not retryable;
/// - a task id that exists with another command or key is
///   `PAYLOAD_MISMATCH`;
/// - a key reserved by another task is `IDEMPOTENCY_KEY_CONFLICT`, naming
///   the owner;
/// - every other error is `ENQUEUE_FAILED` with the error's own retryable
///   flag; a conflict whose row vanished before the compare and a retention
///   class key that no class derives are not retryable.
///
/// `cause` of `VALIDATION_FAILED` is the form of Python's `exception`
/// ([`BrokerRefusalCause`]); `cause` of `ENQUEUE_FAILED` is
/// [`EnqueueFailureCause`]. A payload mismatch and a key conflict have no
/// `cause`.
pub fn send_error_from_enqueue(
    task_id: Uuid,
    payload: TaskSendPayload,
    error: &BrokerError,
) -> TaskSendError {
    let message = error.to_string();
    match error {
        BrokerError::EnqueueContract(refusal) => TaskSendError::ValidationFailed {
            message,
            cause: BrokerRefusalCause::of_enqueue_refusal(&refusal.cause),
        },
        BrokerError::Serialization(_) | BrokerError::InvalidIdempotencyReservationWindow(_) => {
            TaskSendError::ValidationFailed {
                message,
                cause: BrokerRefusalCause::Check,
            }
        }
        BrokerError::EnqueueConflictUnverifiable { .. } => TaskSendError::EnqueueFailed {
            task_id,
            payload: Box::new(payload),
            message,
            retryable: error.is_retryable(),
            cause: Box::new(EnqueueFailureCause::Check),
        },
        BrokerError::PayloadMismatch { .. } => TaskSendError::PayloadMismatch {
            task_id,
            payload: Box::new(payload),
            message,
        },
        BrokerError::IdempotencyKeyConflict {
            task_id: owner_task_id,
            ..
        } => TaskSendError::IdempotencyKeyConflict {
            task_id,
            payload: Box::new(payload),
            message,
            owner_task_id: *owner_task_id,
        },
        BrokerError::SchemaRefused(_)
        | BrokerError::InvalidWorkflowNodeStatus
        | BrokerError::IncompleteTaskHistoryCutover
        | BrokerError::InvalidStatus(_)
        | BrokerError::InvalidExecutionClass(_)
        | BrokerError::ConnectionFailed(_)
        | BrokerError::InvalidConnectionSettings(_)
        | BrokerError::RetentionClassKey(_)
        | BrokerError::ListenerClosed
        | BrokerError::InstallLock(_)
        | BrokerError::InstallCoverage(_)
        | BrokerError::ClientFamilyMismatch { .. }
        | BrokerError::UnknownStoredFamily(_)
        | BrokerError::TerminalizationContract(_)
        | BrokerError::HistoryReadContract(_)
        | BrokerError::InvalidClaimArgument(_)
        | BrokerError::ClaimContract(_)
        | BrokerError::Statement(_)
        | BrokerError::PoolSettings(_)
        | BrokerError::ProcessRuntime(_)
        | BrokerError::RowDecode(_) => TaskSendError::EnqueueFailed {
            task_id,
            payload: Box::new(payload),
            message,
            retryable: error.is_retryable(),
            cause: Box::new(EnqueueFailureCause::of_error(error)),
        },
    }
}

/// Result type for task send operations.
///
/// `Ok(T)` on success (typically a client's task handle),
/// `Err(TaskSendError)` on failure.
pub type TaskSendResult<T> = Result<T, TaskSendError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error_catalog::{ErrorClassValues, ErrorEnum, SlotData};

    #[test]
    fn error_code_display() {
        assert_eq!(
            TaskSendErrorCode::SendSuppressed.to_string(),
            "SEND_SUPPRESSED"
        );
        assert_eq!(
            TaskSendErrorCode::EnqueueFailed.to_string(),
            "ENQUEUE_FAILED"
        );
    }

    fn sample_payload() -> TaskSendPayload {
        TaskSendPayload {
            task_name: "my_task".to_owned(),
            queue_name: "default".to_owned(),
            priority: 100,
            input_json: "\"sensitive data\"".to_owned(),
            sent_at: Utc::now(),
            good_until: None,
            enqueue_delay_seconds: None,
            task_options: None,
            idempotency_key: None,
            retention: crate::core::config::EnqueueRetention::Default,
        }
    }

    #[test]
    fn task_send_error_display_redacted() {
        let err = TaskSendError::EnqueueFailed {
            task_id: Uuid::nil(),
            payload: Box::new(sample_payload()),
            message: "connection refused".to_owned(),
            retryable: true,
            cause: Box::new(EnqueueFailureCause::Error {
                text: "connection refused".to_owned(),
                retryable: true,
                failure: FailureCause::Connection,
            }),
        };
        let display = format!("{}", err);
        // Must NOT contain the sensitive input
        assert!(!display.contains("sensitive data"));
        assert!(display.contains("ENQUEUE_FAILED"));
        assert!(display.contains("00000000-0000-0000-0000-000000000000"));
    }

    #[test]
    fn idempotency_key_conflict_has_python_wire_name() {
        assert_eq!(
            TaskSendErrorCode::IdempotencyKeyConflict.to_string(),
            "IDEMPOTENCY_KEY_CONFLICT"
        );
    }

    /// A key conflict names the owner, is not retryable and is refused for a
    /// resend. Reverting `Self::IdempotencyKeyConflict { .. } => false` in
    /// `retryable()` fails the `retryable` assertion; reverting the `None` arm
    /// in `resend_target()` fails the `resend_target` assertion.
    #[test]
    fn a_key_conflict_names_the_owner_is_not_retryable_and_is_refused_for_a_resend() {
        let task_id = Uuid::from_u128(1);
        let owner = Uuid::from_u128(2);
        let error = send_error_from_enqueue(
            task_id,
            sample_payload(),
            &BrokerError::IdempotencyKeyConflict {
                task_name: "my_task".to_owned(),
                task_id: owner,
            },
        );

        assert_eq!(error.code(), TaskSendErrorCode::IdempotencyKeyConflict);
        assert!(matches!(
            error,
            TaskSendError::IdempotencyKeyConflict { owner_task_id, .. } if owner_task_id == owner
        ));
        assert!(error.resend_target().is_none());
        assert!(!error.retryable());
    }

    /// The fixed `retryable` of a class in the catalog is the value of
    /// `retryable()` for a value of that class: the key conflict and the
    /// async context are the only classes with a fixed value, and neither
    /// clears.
    #[test]
    fn the_fixed_retry_slot_of_a_class_is_the_retry_rule_of_its_value() {
        use crate::error_catalog::ErrorSlot;

        let conflict = TaskSendError::IdempotencyKeyConflict {
            task_id: Uuid::from_u128(1),
            payload: Box::new(sample_payload()),
            message: "the key is held".to_owned(),
            owner_task_id: Uuid::from_u128(2),
        };
        let async_context = TaskSendError::AsyncContext {
            task_id: Uuid::from_u128(3),
            payload: Box::new(sample_payload()),
            message: "blocking send on a runtime thread".to_owned(),
        };
        let fixed: Vec<(&str, &ErrorSlot)> = TaskSendError::ERROR_CATALOG
            .classes
            .iter()
            .flat_map(|class| {
                class
                    .slots
                    .iter()
                    .filter(|slot| match slot {
                        ErrorSlot::NeverRetryable | ErrorSlot::AlwaysRetryable => true,
                        ErrorSlot::CodeMember { .. }
                        | ErrorSlot::CodeSubset { .. }
                        | ErrorSlot::Field { .. }
                        | ErrorSlot::Exception { .. } => false,
                    })
                    .map(|slot| (class.python_class, slot))
            })
            .collect();

        assert_eq!(
            fixed,
            [
                (async_context.python_class(), &ErrorSlot::NeverRetryable),
                (conflict.python_class(), &ErrorSlot::NeverRetryable),
            ]
        );
        assert!(!conflict.retryable());
        assert!(!async_context.retryable());
    }

    /// A refused input is never replayed: the same input meets the same
    /// refusal (Python `test_send_idempotency_conflict.py`, refusal rows).
    /// A rule of the version-1 command (an empty retention class key) and a
    /// rule of the scoped key (an empty key) are refusals of the same kind:
    /// Python passes their `ValueError`, the Rust client its precondition
    /// cause.
    #[test]
    fn a_refusal_is_validation_failed_and_refused_for_a_resend() {
        use crate::core::history::enqueue::EnqueuePreparationError;
        use crate::core::history::identity::keys::IdempotencyKeyError;

        for refusal in [
            BrokerError::EnqueueContract(crate::broker::EnqueueRefusal::by_rule(
                crate::core::history::enqueue::SendPart::TaskOptions,
                "bad options".to_owned(),
            )),
            BrokerError::EnqueueContract(crate::broker::EnqueueRefusal::empty_retention_class_key()),
            BrokerError::EnqueueContract(crate::broker::EnqueueRefusal::from(
                EnqueuePreparationError::Idempotency(IdempotencyKeyError::Empty {
                    label: "idempotency key",
                }),
            )),
            BrokerError::InvalidIdempotencyReservationWindow("0s".to_owned()),
        ] {
            let error = send_error_from_enqueue(Uuid::from_u128(1), sample_payload(), &refusal);
            assert_eq!(
                error.code(),
                TaskSendErrorCode::ValidationFailed,
                "{refusal}"
            );
            assert!(!error.retryable(), "{refusal}");
            assert!(error.resend_target().is_none(), "{refusal}");
            assert_eq!(error.failure_cause(), refusal.failure_cause(), "{refusal}");
            assert_eq!(
                error.failure_cause(),
                FailureCause::PreconditionFailed,
                "{refusal}"
            );
        }
    }

    /// Python `TaskSendResendable` holds `TaskSendAsyncContext`: a blocking
    /// send on a runtime thread keeps its prepared task id and payload, and
    /// an async resend sends them.
    #[test]
    fn an_async_context_keeps_its_payload_and_is_accepted_for_a_resend() {
        let task_id = Uuid::from_u128(3);
        let payload = sample_payload();
        let sent_at = payload.sent_at;
        let error = TaskSendError::AsyncContext {
            task_id,
            payload: Box::new(payload),
            message: "blocking send on a runtime thread".to_owned(),
        };

        assert_eq!(error.code(), TaskSendErrorCode::AsyncContext);
        assert_eq!(error.code().to_string(), "ASYNC_CONTEXT");
        assert!(!error.retryable());
        assert_eq!(error.task_id(), Some(task_id));
        assert_eq!(error.message(), "blocking send on a runtime thread");
        let target = error.resend_target().expect("an async context is accepted");
        assert_eq!(target.task_id, task_id);
        assert_eq!(target.payload.sent_at, sent_at);
        assert!(!format!("{error}").contains("sensitive data"));
    }

    #[test]
    fn a_payload_mismatch_is_refused_for_a_resend() {
        let task_id = Uuid::from_u128(1);
        let error = send_error_from_enqueue(
            task_id,
            sample_payload(),
            &BrokerError::PayloadMismatch { task_id },
        );

        assert_eq!(error.code(), TaskSendErrorCode::PayloadMismatch);
        assert!(!error.retryable());
        assert!(error.resend_target().is_none());
    }

    /// A failed enqueue statement that did not reach the server.
    fn not_sent(cause: crate::db::NotSentCause) -> BrokerError {
        BrokerError::Statement(crate::db::StatementFailure::NotSent {
            statement: "enqueue_task",
            cause,
        })
    }

    /// Python `test_task_decorator.py::test_resend_reuses_task_id_and_payload`:
    /// a resend carries the first send's task id and payload.
    #[test]
    fn a_database_failure_keeps_its_flag_and_is_accepted_for_a_resend() {
        let task_id = Uuid::from_u128(7);
        let payload = sample_payload();
        let sent_at = payload.sent_at;
        let error = send_error_from_enqueue(
            task_id,
            payload,
            &not_sent(crate::db::NotSentCause::Checkout(
                crate::db::CheckoutFailure::TimedOut {
                    role: crate::db::PoolRole::Main,
                    waited: std::time::Duration::from_secs(30),
                },
            )),
        );

        assert_eq!(error.code(), TaskSendErrorCode::EnqueueFailed);
        assert!(error.retryable());
        assert_eq!(error.failure_cause(), FailureCause::Connection);
        let target = error
            .resend_target()
            .expect("an enqueue failure is accepted");
        assert_eq!(target.task_id, task_id);
        assert_eq!(target.payload.sent_at, sent_at);
    }

    /// A statement-pooled `database_url` is refused at the first checkout of
    /// the core pool; the send reports it as `ENQUEUE_FAILED`, not retryable.
    #[test]
    fn a_statement_pooled_url_is_an_enqueue_failure_that_is_not_retryable() {
        let error = send_error_from_enqueue(
            Uuid::from_u128(1),
            sample_payload(),
            &BrokerError::Statement(crate::db::StatementFailure::NotSent {
                statement: "enqueue",
                cause: crate::db::NotSentCause::Checkout(
                    crate::db::CheckoutFailure::StatementPooledUrl {
                        masked_url: crate::db::MaskedUrl::for_tests(
                            "postgresql://horsies:***@pgbouncer:6432/app",
                        ),
                    },
                ),
            }),
        );

        assert_eq!(error.code(), TaskSendErrorCode::EnqueueFailed);
        assert!(!error.retryable());
    }

    /// Python `test_broker_postgres.py::TestEnqueueCatch::test_schema_init_operator_state_stays_enqueue_failed`:
    /// a schema state that an operator must repair (a newer schema, a cutover
    /// that is not complete, a lost install lock) fails the send as
    /// `ENQUEUE_FAILED`, not retryable, and never as a refusal of the input.
    #[test]
    fn a_schema_state_for_the_operator_is_an_enqueue_failure_that_is_not_retryable() {
        for state in [
            BrokerError::SchemaRefused(crate::broker::migrations::SchemaRefusal::SchemaNewer {
                stored: 1099,
                own: 1000,
            }),
            BrokerError::IncompleteTaskHistoryCutover,
            BrokerError::InstallLock(crate::broker::error::InstallLockFailure::OwnershipLost),
        ] {
            let error = send_error_from_enqueue(Uuid::from_u128(1), sample_payload(), &state);

            assert_eq!(error.code(), TaskSendErrorCode::EnqueueFailed, "{state}");
            assert!(!error.retryable(), "{state}");
            assert_eq!(error.message(), state.to_string());
        }
    }

    /// Python `test_broker_postgres.py::TestEnqueueCatch::test_write_error_keeps_its_classification`:
    /// a failed write is `ENQUEUE_FAILED` with the retry flag of its own
    /// cause, in both directions.
    #[test]
    fn a_failed_write_keeps_the_retry_flag_of_its_cause() {
        for (failure, retryable) in [
            (
                not_sent(crate::db::NotSentCause::Checkout(
                    crate::db::CheckoutFailure::TimedOut {
                        role: crate::db::PoolRole::Main,
                        waited: std::time::Duration::from_secs(30),
                    },
                )),
                true,
            ),
            (not_sent(crate::db::NotSentCause::ConnectionClosed), true),
            (
                not_sent(crate::db::NotSentCause::Checkout(
                    crate::db::CheckoutFailure::PoolClosed {
                        role: crate::db::PoolRole::Main,
                    },
                )),
                false,
            ),
            (
                BrokerError::RowDecode(crate::broker::RowDecodeError::NoRow {
                    statement: "enqueue_task",
                }),
                false,
            ),
        ] {
            let error = send_error_from_enqueue(Uuid::from_u128(1), sample_payload(), &failure);

            assert_eq!(error.code(), TaskSendErrorCode::EnqueueFailed, "{failure}");
            assert_eq!(error.retryable(), retryable, "{failure}");
        }
    }

    #[test]
    fn a_vanished_conflict_row_is_an_enqueue_failure_that_is_not_retryable() {
        let error = send_error_from_enqueue(
            Uuid::from_u128(1),
            sample_payload(),
            &BrokerError::EnqueueConflictUnverifiable {
                task_id: Uuid::from_u128(1),
                task_name: "my_task".to_owned(),
            },
        );

        assert_eq!(error.code(), TaskSendErrorCode::EnqueueFailed);
        assert!(!error.retryable());
    }

    /// The instance slot names of the class `class` in the catalog, in
    /// order. The class values (`code` of one member, the fixed
    /// `retryable` of a class) are not instance slots.
    fn catalog_slot_names(class: &str) -> Vec<&'static str> {
        use crate::error_catalog::ErrorSlot;

        let found = TaskSendError::ERROR_CATALOG
            .classes
            .iter()
            .find(|candidate| candidate.python_class == class)
            .expect("the class is in the catalog");
        found
            .slots
            .iter()
            .filter_map(|slot| match slot {
                ErrorSlot::CodeMember { .. }
                | ErrorSlot::NeverRetryable
                | ErrorSlot::AlwaysRetryable => None,
                ErrorSlot::CodeSubset { .. } => Some("code"),
                ErrorSlot::Field { name, .. } => Some(*name),
                ErrorSlot::Exception { .. } => Some("cause"),
            })
            .collect()
    }

    /// The slot values of `error` by name, after a check that the names
    /// and their order are the catalog's slots of its class.
    fn named_slots(error: &TaskSendError) -> Vec<(&'static str, SlotData<'_>)> {
        let named: Vec<(&'static str, SlotData<'_>)> = error
            .slot_values()
            .into_iter()
            .map(|slot| (slot.name, slot.value))
            .collect();
        let names: Vec<&'static str> = named.iter().map(|(name, _)| *name).collect();
        assert_eq!(names, catalog_slot_names(error.python_class()));
        named
    }

    /// The `cause` slot of `error`.
    fn exception_of(error: &TaskSendError) -> ExceptionValue {
        let slot = error
            .slot_values()
            .into_iter()
            .find(|slot| slot.name == "cause")
            .expect("the class has a cause slot");
        let SlotData::Exception(exception) = slot.value else {
            panic!("the cause slot holds {:?}", slot.value);
        };
        exception
    }

    /// The `exception` that Python stores for the failed enqueue of
    /// `error`: the broker's class of the failure
    /// (`DatabaseOperationError::enqueue_error`), whose `exception`
    /// `_map_broker_err` copies into the send error.
    fn python_exception(error: BrokerError) -> ExceptionValue {
        let broker = crate::DatabaseOperationError::enqueue_error("my_task", error);
        let slot = crate::error_catalog::ErrorClassValues::slot_values(&broker)
            .into_iter()
            .find(|slot| slot.name == "cause")
            .expect("the broker class has a cause slot");
        let SlotData::Exception(exception) = slot.value else {
            panic!("the cause slot holds {:?}", slot.value);
        };
        exception
    }

    /// A server error: the cause that no broker error of these tests gives.
    fn server_cause() -> FailureCause {
        FailureCause::Server(crate::ServerErrorFacts::of_code(
            "40001",
            "could not serialize access".to_owned(),
        ))
    }

    #[test]
    fn a_suppressed_send_has_its_message() {
        let error = TaskSendError::Suppressed {
            message: "sends are suppressed".to_owned(),
        };

        assert_eq!(
            named_slots(&error),
            [("message", SlotData::Str("sends are suppressed"))]
        );
    }

    #[test]
    fn an_async_context_has_its_task_id_payload_and_message() {
        let payload = sample_payload();
        let error = TaskSendError::AsyncContext {
            task_id: Uuid::from_u128(3),
            payload: Box::new(payload.clone()),
            message: "inside a runtime".to_owned(),
        };

        assert_eq!(
            named_slots(&error),
            [
                ("task_id", SlotData::Uuid(Uuid::from_u128(3))),
                ("payload", SlotData::TaskSendPayload(&payload)),
                ("message", SlotData::Str("inside a runtime")),
            ]
        );
    }

    /// The cause of a refusal is the form of Python's `exception`: `NoCause()`
    /// for a check, the client's `SerializationError` with the reader's text,
    /// the client's `ValueError` with the rule's text.
    #[test]
    fn a_validation_failure_has_the_exception_of_its_refusal_cause() {
        for (cause, exception) in [
            (BrokerRefusalCause::Check, ExceptionValue::NoCause),
            (
                BrokerRefusalCause::JsonUnreadable("EOF at 1".to_owned()),
                ExceptionValue::ClientClass {
                    class: crate::error_catalog::ClientExceptionClass::SerializationError,
                    text: "EOF at 1".to_owned(),
                },
            ),
            (
                BrokerRefusalCause::ValueRule("a rule".to_owned()),
                ExceptionValue::ClientClass {
                    class: crate::error_catalog::ClientExceptionClass::ValueError,
                    text: "a rule".to_owned(),
                },
            ),
        ] {
            let error = TaskSendError::ValidationFailed {
                message: "refused".to_owned(),
                cause: cause.clone(),
            };
            assert_eq!(
                named_slots(&error),
                [
                    ("message", SlotData::Str("refused")),
                    ("cause", SlotData::Exception(exception)),
                ],
                "{cause:?}"
            );
            assert_eq!(error.failure_cause(), FailureCause::PreconditionFailed);
        }
    }

    /// The exception of a validation failure that the core makes of a broker
    /// error is Python's form for the same error.
    #[test]
    fn a_validation_failure_of_a_broker_error_has_pythons_exception() {
        use crate::broker::{EnqueueRefusal, EnqueueRefusalCause};
        use crate::core::history::enqueue::{EnqueuePreparationError, SendPart};
        use crate::core::history::identity::keys::IdempotencyKeyError;

        let errors: [fn() -> BrokerError; 6] = [
            || BrokerError::Serialization(serde_json::from_str::<u8>("x").expect_err("not JSON")),
            || BrokerError::InvalidIdempotencyReservationWindow("0s".to_owned()),
            || {
                BrokerError::EnqueueContract(EnqueueRefusal::by_rule(
                    SendPart::TaskOptions,
                    "bad options".to_owned(),
                ))
            },
            || BrokerError::EnqueueContract(EnqueueRefusal::empty_retention_class_key()),
            || {
                BrokerError::EnqueueContract(EnqueueRefusal::from(
                    EnqueuePreparationError::Idempotency(IdempotencyKeyError::Empty {
                        label: "idempotency key",
                    }),
                ))
            },
            || {
                BrokerError::EnqueueContract(EnqueueRefusal {
                    part: SendPart::Input,
                    message: "the input does not read".to_owned(),
                    cause: EnqueueRefusalCause::JsonUnreadable("EOF at 1".to_owned()),
                })
            },
        ];
        for broker_error in errors {
            let error =
                send_error_from_enqueue(Uuid::from_u128(1), sample_payload(), &broker_error());
            assert_eq!(error.code(), TaskSendErrorCode::ValidationFailed);
            assert_eq!(exception_of(&error), python_exception(broker_error()));
        }
    }

    /// An error of the enqueue is the core's class of what it met, with the
    /// cause's text and retry rule; a check is `NoCause()`.
    #[test]
    fn an_enqueue_failure_has_the_exception_of_its_cause() {
        let payload = sample_payload();
        for (cause, exception, failure_cause) in [
            (
                EnqueueFailureCause::Error {
                    text: "the write failed".to_owned(),
                    retryable: true,
                    failure: server_cause(),
                },
                ExceptionValue::CoreFailure {
                    text: "the write failed".to_owned(),
                    retryable: true,
                    cause: server_cause(),
                },
                server_cause(),
            ),
            (
                EnqueueFailureCause::Error {
                    text: "the row is wrong".to_owned(),
                    retryable: false,
                    failure: FailureCause::ContractViolation,
                },
                ExceptionValue::CoreFailure {
                    text: "the row is wrong".to_owned(),
                    retryable: false,
                    cause: FailureCause::ContractViolation,
                },
                FailureCause::ContractViolation,
            ),
            (
                EnqueueFailureCause::Check,
                ExceptionValue::NoCause,
                FailureCause::PreconditionFailed,
            ),
        ] {
            let error = TaskSendError::EnqueueFailed {
                task_id: Uuid::from_u128(4),
                payload: Box::new(payload.clone()),
                message: "the write failed".to_owned(),
                retryable: true,
                cause: Box::new(cause.clone()),
            };
            assert_eq!(
                named_slots(&error),
                [
                    ("task_id", SlotData::Uuid(Uuid::from_u128(4))),
                    ("payload", SlotData::TaskSendPayload(&payload)),
                    ("message", SlotData::Str("the write failed")),
                    ("retryable", SlotData::Bool(true)),
                    ("cause", SlotData::Exception(exception)),
                ],
                "{cause:?}"
            );
            assert_eq!(error.failure_cause(), failure_cause, "{cause:?}");
        }
    }

    /// The exception of an enqueue failure that the core makes of a broker
    /// error is Python's form for the same error, the conflict whose row was
    /// gone included.
    #[test]
    fn an_enqueue_failure_of_a_broker_error_has_pythons_exception() {
        let errors: [fn() -> BrokerError; 5] = [
            || not_sent(crate::db::NotSentCause::ConnectionClosed),
            || BrokerError::ConnectionFailed("connection refused".to_owned()),
            || BrokerError::InvalidStatus("BOGUS".to_owned()),
            || BrokerError::InvalidWorkflowNodeStatus,
            || BrokerError::EnqueueConflictUnverifiable {
                task_id: Uuid::from_u128(1),
                task_name: "my_task".to_owned(),
            },
        ];
        for broker_error in errors {
            let error =
                send_error_from_enqueue(Uuid::from_u128(1), sample_payload(), &broker_error());
            assert_eq!(error.code(), TaskSendErrorCode::EnqueueFailed);
            assert_eq!(exception_of(&error), python_exception(broker_error()));
        }
    }

    #[test]
    fn a_payload_mismatch_has_its_task_id_payload_and_message() {
        let payload = sample_payload();
        let error = TaskSendError::PayloadMismatch {
            task_id: Uuid::from_u128(5),
            payload: Box::new(payload.clone()),
            message: "another command".to_owned(),
        };

        assert_eq!(
            named_slots(&error),
            [
                ("task_id", SlotData::Uuid(Uuid::from_u128(5))),
                ("payload", SlotData::TaskSendPayload(&payload)),
                ("message", SlotData::Str("another command")),
            ]
        );
    }

    #[test]
    fn a_payload_too_large_has_its_two_byte_counts() {
        let error = TaskSendError::PayloadTooLarge {
            message: "too large".to_owned(),
            size_bytes: u64::MAX,
            limit_bytes: 100,
        };

        assert_eq!(
            named_slots(&error),
            [
                ("message", SlotData::Str("too large")),
                ("size_bytes", SlotData::Int(i128::from(u64::MAX))),
                ("limit_bytes", SlotData::Int(100)),
            ]
        );
    }

    #[test]
    fn a_key_conflict_has_its_task_id_payload_message_and_owner() {
        let payload = sample_payload();
        let error = TaskSendError::IdempotencyKeyConflict {
            task_id: Uuid::from_u128(6),
            payload: Box::new(payload.clone()),
            message: "the key is held".to_owned(),
            owner_task_id: Uuid::from_u128(7),
        };

        assert_eq!(
            named_slots(&error),
            [
                ("task_id", SlotData::Uuid(Uuid::from_u128(6))),
                ("payload", SlotData::TaskSendPayload(&payload)),
                ("message", SlotData::Str("the key is held")),
                ("owner_task_id", SlotData::Uuid(Uuid::from_u128(7))),
            ]
        );
    }

    /// A refusal of the core before any statement, of the input part.
    fn input_refusal(cause: crate::broker::EnqueueRefusalCause) -> BrokerError {
        BrokerError::EnqueueContract(crate::broker::EnqueueRefusal {
            part: crate::core::history::enqueue::SendPart::Input,
            message: "the input is refused".to_owned(),
            cause,
        })
    }

    /// The exception of the send error that [`send_error_from_enqueue`] makes
    /// of `error`, after a check that it is Python's form for the same error.
    fn exact_exception(error: fn() -> BrokerError) -> ExceptionValue {
        let send_error = send_error_from_enqueue(Uuid::from_u128(1), sample_payload(), &error());
        let exception = exception_of(&send_error);
        assert_eq!(exception, python_exception(error()));
        exception
    }

    /// Python: `EnqueueCommandV1` raises the rule as a `ValueError`.
    #[test]
    fn a_command_rule_refusal_is_a_value_error_with_the_rule() {
        let error = || {
            input_refusal(crate::broker::EnqueueRefusalCause::CommandRuleRefused(
                "retention_class_key must be non-empty".to_owned(),
            ))
        };

        assert_eq!(
            send_error_from_enqueue(Uuid::from_u128(1), sample_payload(), &error()).code(),
            TaskSendErrorCode::ValidationFailed
        );
        assert_eq!(
            exact_exception(error),
            ExceptionValue::ClientClass {
                class: crate::error_catalog::ClientExceptionClass::ValueError,
                text: "retention_class_key must be non-empty".to_owned(),
            }
        );
    }

    /// Python: `ScopedIdempotencyKey` raises the rule as a `ValueError`.
    #[test]
    fn a_key_rule_refusal_is_a_value_error_with_the_rule() {
        let error = || {
            input_refusal(crate::broker::EnqueueRefusalCause::KeyRuleRefused(
                "idempotency key must be non-empty".to_owned(),
            ))
        };

        assert_eq!(
            send_error_from_enqueue(Uuid::from_u128(1), sample_payload(), &error()).code(),
            TaskSendErrorCode::ValidationFailed
        );
        assert_eq!(
            exact_exception(error),
            ExceptionValue::ClientClass {
                class: crate::error_catalog::ClientExceptionClass::ValueError,
                text: "idempotency key must be non-empty".to_owned(),
            }
        );
    }

    /// Python: the JSON reader's `SerializationError` with its text.
    #[test]
    fn an_unreadable_json_text_is_a_serialization_error_with_the_reader_text() {
        let error = || {
            input_refusal(crate::broker::EnqueueRefusalCause::JsonUnreadable(
                "EOF while parsing a value at line 1 column 0".to_owned(),
            ))
        };

        assert_eq!(
            send_error_from_enqueue(Uuid::from_u128(1), sample_payload(), &error()).code(),
            TaskSendErrorCode::ValidationFailed
        );
        assert_eq!(
            exact_exception(error),
            ExceptionValue::ClientClass {
                class: crate::error_catalog::ClientExceptionClass::SerializationError,
                text: "EOF while parsing a value at line 1 column 0".to_owned(),
            }
        );
    }

    /// Python: `NoCause()` for a check of the core: a rule that refused a
    /// value that reads, a value that does not serialize, a reservation
    /// window out of range.
    #[test]
    fn a_check_of_the_core_is_no_cause() {
        let checks: [fn() -> BrokerError; 3] = [
            || {
                BrokerError::EnqueueContract(crate::broker::EnqueueRefusal::by_rule(
                    crate::core::history::enqueue::SendPart::TaskOptions,
                    "bad options".to_owned(),
                ))
            },
            || BrokerError::Serialization(serde_json::from_str::<u8>("x").expect_err("not JSON")),
            || BrokerError::InvalidIdempotencyReservationWindow("0s".to_owned()),
        ];
        for error in checks {
            assert_eq!(
                send_error_from_enqueue(Uuid::from_u128(1), sample_payload(), &error()).code(),
                TaskSendErrorCode::ValidationFailed
            );
            assert_eq!(exact_exception(error), ExceptionValue::NoCause);
        }
    }

    /// Python: `NoCause()`; the broker found the gone row by a data check.
    #[test]
    fn an_unverifiable_conflict_is_no_cause() {
        let error = || BrokerError::EnqueueConflictUnverifiable {
            task_id: Uuid::from_u128(1),
            task_name: "my_task".to_owned(),
        };

        assert_eq!(
            send_error_from_enqueue(Uuid::from_u128(1), sample_payload(), &error()).code(),
            TaskSendErrorCode::EnqueueFailed
        );
        assert_eq!(exact_exception(error), ExceptionValue::NoCause);
    }

    /// Python: the core's class of the error, with its text and its retry
    /// rule.
    #[test]
    fn a_connection_failure_is_the_core_class_of_the_error() {
        let error = || BrokerError::ConnectionFailed("connection refused".to_owned());

        assert_eq!(
            send_error_from_enqueue(Uuid::from_u128(1), sample_payload(), &error()).code(),
            TaskSendErrorCode::EnqueueFailed
        );
        assert_eq!(
            exact_exception(error),
            ExceptionValue::CoreFailure {
                text: "connection failed: connection refused".to_owned(),
                retryable: true,
                cause: FailureCause::Connection,
            }
        );
    }

    /// The classes of a task id with another command and of a key that
    /// another task holds have no `cause` slot.
    #[test]
    fn a_payload_mismatch_and_a_key_conflict_have_no_exception_slot() {
        for error in [
            BrokerError::PayloadMismatch {
                task_id: Uuid::from_u128(1),
            },
            BrokerError::IdempotencyKeyConflict {
                task_name: "my_task".to_owned(),
                task_id: Uuid::from_u128(2),
            },
        ] {
            let send_error = send_error_from_enqueue(Uuid::from_u128(1), sample_payload(), &error);
            assert!(
                send_error
                    .slot_values()
                    .iter()
                    .all(|slot| slot.name != "cause"),
                "{error}"
            );
        }
    }
}

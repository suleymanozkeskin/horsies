/// Typed error types for task send/schedule operations.
///
/// Mirrors Python's `horsies/core/models/task_send_types.py`: one variant per
/// code, and the core maps every enqueue error to its variant once
/// ([`send_error_from_enqueue`]).
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::BrokerError;

/// Categorized task send failure codes.
///
/// The members are Python's, in Python's order
/// (`horsies/core/models/task_send_types.py`). `ASYNC_CONTEXT`,
/// `EVENT_LOOP_LIMIT` and `LOOP_RUNNER_FAILED` are produced by the Python
/// client only; [`TaskSendError`] has no variant for them.
#[derive(Debug, Clone, PartialEq, Eq, Hash, horsies_macros::CodeCatalog)]
#[code_catalog(module = "task_send", base = "str_enum")]
pub enum TaskSendErrorCode {
    /// Send called while suppression is active (worker/scheduler import phase,
    /// `check()`, or `HORSIES_SUPPRESS_SENDS=1`). Non-retryable.
    SendSuppressed,
    /// A sync send or schedule (or a sync retry) was called from inside a
    /// running event loop; the blocking enqueue would stall the loop, so
    /// nothing was written. A sync retry from the same loop returns this code
    /// again; the `*_async` forms complete the dispatch. Python client only.
    AsyncContext,
    /// Programmer error (bad args, serialization failure, unresolved queue).
    ValidationFailed,
    /// Infrastructure error (schema init, DB transaction). Conditionally retryable.
    EnqueueFailed,
    /// The task id exists with a different command or idempotency key.
    /// Nothing was written. The retry methods refuse it.
    PayloadMismatch,
    /// The idempotency key is reserved by another task with a different
    /// command. Nothing was written. The retry methods accept it: a retry
    /// after the reservation ends is accepted, one before returns the same
    /// error.
    IdempotencyKeyConflict,
    /// Serialized payload exceeded `payload.reject_bytes`; nothing was
    /// written. Non-retryable — shrink the payload (pass a reference to
    /// external storage) or raise the configured limit.
    PayloadTooLarge,
    /// The broker already holds resource sets for its limit of open event
    /// loops, and this send ran on a new one; nothing was written. The retry
    /// methods refuse it. Python client only; removed before 0.8.0.
    EventLoopLimit,
    /// A sync send or schedule could not run the enqueue on the broker's sync
    /// runner: the runner is stopped, its thread did not start, or scheduling
    /// failed. Nothing was written. Python client only; removed before 0.8.0.
    LoopRunnerFailed,
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
            Self::EventLoopLimit => write!(f, "EVENT_LOOP_LIMIT"),
            Self::LoopRunnerFailed => write!(f, "LOOP_RUNNER_FAILED"),
        }
    }
}

/// Serialized envelope for idempotent retry.
///
/// All value fields are pre-serialized to avoid round-trip issues.
/// The retry methods extract this payload directly — no user input needed.
/// A replay carries the same command and `sent_at`, so the exact-id resend
/// check accepts it.
#[derive(Debug, Clone)]
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

/// Error from task send/schedule operations: one variant per
/// [`TaskSendErrorCode`] that the core produces, each with only the facts of
/// its state (Python `horsies/core/models/task_send_types.py:107-252`).
#[derive(Debug, Clone)]
pub enum TaskSendError {
    /// Sends are suppressed in this process. Nothing was serialized or
    /// written. The retry methods refuse it.
    SendSuppressed { message: String },
    /// The input or the options were refused before anything was written.
    /// The retry methods refuse it.
    ValidationFailed { message: String },
    /// The database failed during enqueue. The row may or may not exist.
    /// `retryable` is the driver classification of the cause. A resend of
    /// the same task id and payload is safe: the broker returns the existing
    /// task for a duplicate.
    EnqueueFailed {
        task_id: Uuid,
        payload: Box<TaskSendPayload>,
        message: String,
        retryable: bool,
    },
    /// The task id exists with a different command or idempotency key.
    /// Nothing was written. The retry methods refuse it.
    PayloadMismatch {
        task_id: Uuid,
        payload: Box<TaskSendPayload>,
        message: String,
    },
    /// The serialized input exceeds `payload.reject_bytes`. Nothing was
    /// written. The retry methods refuse it.
    PayloadTooLarge {
        message: String,
        size_bytes: u64,
        limit_bytes: u64,
    },
    /// The idempotency key is reserved by `owner_task_id` with a different
    /// command. Nothing was written. The retry methods accept it.
    IdempotencyKeyConflict {
        task_id: Uuid,
        payload: Box<TaskSendPayload>,
        message: String,
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
    /// The code of this variant.
    pub fn code(&self) -> TaskSendErrorCode {
        match self {
            Self::SendSuppressed { .. } => TaskSendErrorCode::SendSuppressed,
            Self::ValidationFailed { .. } => TaskSendErrorCode::ValidationFailed,
            Self::EnqueueFailed { .. } => TaskSendErrorCode::EnqueueFailed,
            Self::PayloadMismatch { .. } => TaskSendErrorCode::PayloadMismatch,
            Self::PayloadTooLarge { .. } => TaskSendErrorCode::PayloadTooLarge,
            Self::IdempotencyKeyConflict { .. } => TaskSendErrorCode::IdempotencyKeyConflict,
        }
    }

    /// Whether the condition may clear, so the same send can succeed later.
    /// Only an enqueue failure carries a driver classification; a key
    /// conflict clears when the reservation ends; every refusal stays.
    pub fn retryable(&self) -> bool {
        match self {
            Self::EnqueueFailed { retryable, .. } => *retryable,
            Self::IdempotencyKeyConflict { .. } => true,
            Self::SendSuppressed { .. }
            | Self::ValidationFailed { .. }
            | Self::PayloadMismatch { .. }
            | Self::PayloadTooLarge { .. } => false,
        }
    }

    /// The human-readable description. It never holds input values.
    pub fn message(&self) -> &str {
        match self {
            Self::SendSuppressed { message }
            | Self::ValidationFailed { message }
            | Self::EnqueueFailed { message, .. }
            | Self::PayloadMismatch { message, .. }
            | Self::PayloadTooLarge { message, .. }
            | Self::IdempotencyKeyConflict { message, .. } => message.as_str(),
        }
    }

    /// The task id of the send, when the send got one.
    pub fn task_id(&self) -> Option<Uuid> {
        match self {
            Self::EnqueueFailed { task_id, .. }
            | Self::PayloadMismatch { task_id, .. }
            | Self::IdempotencyKeyConflict { task_id, .. } => Some(*task_id),
            Self::SendSuppressed { .. }
            | Self::ValidationFailed { .. }
            | Self::PayloadTooLarge { .. } => None,
        }
    }

    /// The task id and payload when the retry methods accept this error,
    /// else `None`. Python's accepted set (`TaskSendRetryable`,
    /// `horsies/core/models/task_send_types.py:255-260`) without the
    /// Python-only codes: an enqueue failure and a key conflict. A refusal
    /// is never replayed, because the same input meets the same refusal.
    pub fn resend_target(&self) -> Option<ResendTarget<'_>> {
        match self {
            Self::EnqueueFailed {
                task_id, payload, ..
            }
            | Self::IdempotencyKeyConflict {
                task_id, payload, ..
            } => Some(ResendTarget {
                task_id: *task_id,
                payload: payload.as_ref(),
            }),
            Self::SendSuppressed { .. }
            | Self::ValidationFailed { .. }
            | Self::PayloadMismatch { .. }
            | Self::PayloadTooLarge { .. } => None,
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

/// The send error for a failed enqueue, by Python's rule
/// (`horsies/core/task_decorator.py:1248-1310`):
/// - a refusal of the input, the options or the call (`EnqueueContract`,
///   `Serialization`, `InvalidIdempotencyReservationWindow`) is
///   `VALIDATION_FAILED`, not retryable;
/// - a task id that exists with another command or key is
///   `PAYLOAD_MISMATCH`;
/// - a key reserved by another task is `IDEMPOTENCY_KEY_CONFLICT`, naming
///   the owner;
/// - every other error is `ENQUEUE_FAILED` with the error's own retryable
///   flag; a conflict whose row vanished before the compare is not
///   retryable.
pub fn send_error_from_enqueue(
    task_id: Uuid,
    payload: TaskSendPayload,
    error: &BrokerError,
) -> TaskSendError {
    let message = error.to_string();
    match error {
        BrokerError::EnqueueContract(_)
        | BrokerError::Serialization(_)
        | BrokerError::InvalidIdempotencyReservationWindow(_) => {
            TaskSendError::ValidationFailed { message }
        }
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
        BrokerError::Database(_)
        | BrokerError::Migration(_)
        | BrokerError::InvalidWorkflowNodeStatus
        | BrokerError::IncompleteTaskHistoryCutover
        | BrokerError::SchemaVersionMismatch { .. }
        | BrokerError::InvalidStatus(_)
        | BrokerError::InvalidExecutionClass(_)
        | BrokerError::ConnectionFailed(_)
        | BrokerError::InvalidConnectionSettings(_)
        | BrokerError::EnqueueConflictUnverifiable { .. }
        | BrokerError::ListenerClosed
        | BrokerError::InstallLock(_)
        | BrokerError::ClientFamilyMismatch { .. }
        | BrokerError::TerminalizationContract(_)
        | BrokerError::HistoryReadContract(_)
        | BrokerError::InvalidClaimArgument(_)
        | BrokerError::Statement(_)
        | BrokerError::PoolSettings(_)
        | BrokerError::RowDecode(_) => TaskSendError::EnqueueFailed {
            task_id,
            payload: Box::new(payload),
            message,
            retryable: error.is_retryable(),
        },
    }
}

/// Result type for task send/schedule operations.
///
/// `Ok(T)` on success (typically `TaskHandle<T>`),
/// `Err(TaskSendError)` on failure.
pub type TaskSendResult<T> = Result<T, TaskSendError>;

#[cfg(test)]
mod tests {
    use super::*;

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

    /// Python `test_send_idempotency_conflict.py::TestKeyReservedByAnotherTask::
    /// test_conflict_names_the_owner_and_retry_is_accepted`.
    #[test]
    fn a_key_conflict_names_the_owner_and_is_accepted_for_a_resend() {
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
        let target = error.resend_target().expect("a key conflict is accepted");
        assert_eq!(target.task_id, task_id);
    }

    /// A refused input is never replayed: the same input meets the same
    /// refusal (Python `test_send_idempotency_conflict.py`, refusal rows).
    #[test]
    fn a_refusal_is_validation_failed_and_refused_for_a_resend() {
        for refusal in [
            BrokerError::EnqueueContract("bad options".to_owned()),
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
        }
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

    /// Python `test_task_decorator.py::test_retry_send_reuses_task_id_and_payload`:
    /// a resend carries the first send's task id and payload.
    #[test]
    fn a_database_failure_keeps_its_flag_and_is_accepted_for_a_resend() {
        let task_id = Uuid::from_u128(7);
        let payload = sample_payload();
        let sent_at = payload.sent_at;
        let error = send_error_from_enqueue(
            task_id,
            payload,
            &BrokerError::Database(sqlx::Error::PoolTimedOut),
        );

        assert_eq!(error.code(), TaskSendErrorCode::EnqueueFailed);
        assert!(error.retryable());
        let target = error
            .resend_target()
            .expect("an enqueue failure is accepted");
        assert_eq!(target.task_id, task_id);
        assert_eq!(target.payload.sent_at, sent_at);
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
}

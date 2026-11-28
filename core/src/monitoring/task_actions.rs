//! Administrative task cancellation: one `horsies_cancel_task` statement
//! ([`PostgresBroker::cancel_task`]), with the answer as a task action
//! result.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::broker::{BrokerError, CancelAnswer, CancelTaskError, PostgresBroker};
use crate::core::lifecycle::OutcomeDecodeError;
use crate::core::types::status::TaskStatus;
use crate::db::StatementFailure;
use crate::failure_cause::FailureCause;

use super::models::CancelPolicy;

/// Why a task action did not apply.
///
/// Each member names one variant of `TaskActionError`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_core_derive::CodeCatalog,
)]
#[code_catalog(module = "monitoring", values = "equal_text")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskActionErrorCode {
    TaskNotFound,
    TaskNotCancellable,
    TaskIsWorkflowTask,
    DatabaseOperationFailed,
    /// The cancel statement went to the server and its reply was lost: the
    /// cancel may have committed. A repeated cancel tells which, because a
    /// committed cancel answers `TaskNotCancellable` with `CANCELLED`.
    OutcomeUnknown,
    /// Stored task data does not parse: a history status that is not a task
    /// status, or a cancel outcome row that does not decode. Nothing changed:
    /// the cancel refused the task. Not retryable: a repeat gives the same
    /// result until the stored row changes.
    StoredDataInvalid,
}

/// A refused or failed task action: one variant per class of Python's task
/// action catalog, each with the facts of its state. Each variant's doc is
/// its Python class doc (`ErrorCatalog`). A variant with an `exception`
/// carries its typed cause, which the client builds that exception from
/// (`ErrorClassValues`).
#[derive(
    Debug,
    thiserror::Error,
    horsies_core_derive::ErrorCatalog,
    horsies_core_derive::ErrorClassValues,
)]
#[error_catalog(
    module = "monitoring_errors",
    code = "TaskActionErrorCode",
    union = "TaskActionCoreError",
    union_doc = "Every failure of a task action that the core gives: one class per state."
)]
pub enum TaskActionError {
    /// No task has this id. Nothing changed.
    ///
    /// Neither the live table nor history holds the task (retention can
    /// remove it). Not retryable: the same call returns this error until a
    /// task with this id exists. A repeat does not create it.
    #[error("{message}")]
    #[error_class(class = "TaskActionNotFound", code = "TaskNotFound", never_retryable)]
    NotFound {
        #[error_slot(slot_type = "Str", hidden)]
        message: String,
        #[error_slot(slot_type = "Uuid")]
        task_id: Uuid,
    },
    /// The task's status does not admit the cancel. Nothing changed.
    ///
    /// ``current_status`` is the status that the cancel found: a terminal
    /// status, or ``RUNNING`` with the policy ``PENDING_OR_CLAIMED_ONLY``. A
    /// call with the policy ``INCLUDE_RUNNING`` cancels a ``RUNNING`` task.
    #[error("{message}")]
    #[error_class(
        class = "TaskActionNotCancellable",
        code = "TaskNotCancellable",
        never_retryable
    )]
    NotCancellable {
        #[error_slot(slot_type = "Str", hidden)]
        message: String,
        #[error_slot(slot_type = "Uuid")]
        task_id: Uuid,
        #[error_slot(slot_type = "TaskStatus")]
        current_status: TaskStatus,
    },
    /// The task belongs to a workflow. Nothing changed.
    ///
    /// Only the workflow primitives drive a workflow task: pause, resume and
    /// cancel on its workflow. ``current_status`` is the task's status.
    #[error("{message}")]
    #[error_class(
        class = "TaskActionIsWorkflowTask",
        code = "TaskIsWorkflowTask",
        never_retryable
    )]
    IsWorkflowTask {
        #[error_slot(slot_type = "Str", hidden)]
        message: String,
        #[error_slot(slot_type = "Uuid")]
        task_id: Uuid,
        #[error_slot(slot_type = "TaskStatus")]
        current_status: TaskStatus,
    },
    /// The cancel failed with a database or driver error, or the core of this
    /// process could not run it. Nothing changed.
    ///
    /// ``retryable`` is the core's classification of the cause: ``True`` when
    /// the condition may clear, for example a lost connection. ``cause``
    /// is the core's class of the cause (a ``CoreFailure`` leaf class, with the
    /// subject ``cancel_task``), or the ``CoreCallRefusal`` of the core
    /// boundary. The statement was not sent, or the server refused it: the
    /// task is as it was. A reply that was lost after the statement was sent
    /// is ``TaskActionOutcomeUnknown``.
    #[error("{message}")]
    #[error_class(
        class = "TaskActionDatabaseOperationFailed",
        code = "DatabaseOperationFailed",
        cause = "required"
    )]
    DatabaseOperationFailed {
        #[error_slot(slot_type = "Str", hidden)]
        message: String,
        #[error_slot(slot_type = "Bool")]
        retryable: bool,
        #[error_slot(slot_type = "Uuid")]
        task_id: Uuid,
        /// The failed connect, client family check or statement.
        #[error_slot(cause)]
        #[source]
        cause: Box<BrokerError>,
    },
    /// The cancel statement went to the server and its reply was lost. The
    /// cancel may have committed.
    ///
    /// A repeated cancel tells which: a committed cancel answers
    /// ``TaskActionNotCancellable`` with ``CANCELLED``, and a cancel that
    /// did not commit cancels the task. ``retryable`` is the core's
    /// classification of the cause (``True``: the lost connection may
    /// clear). ``cause`` is the core's class of the cause (a ``CoreFailure``
    /// leaf class, with the subject ``cancel_task``).
    #[error("{message}")]
    #[error_class(
        class = "TaskActionOutcomeUnknown",
        code = "OutcomeUnknown",
        cause = "required"
    )]
    OutcomeUnknown {
        #[error_slot(slot_type = "Str", hidden)]
        message: String,
        #[error_slot(slot_type = "Bool")]
        retryable: bool,
        #[error_slot(slot_type = "Uuid")]
        task_id: Uuid,
        /// The statement whose reply was lost.
        #[error_slot(cause)]
        #[source]
        cause: Box<BrokerError>,
    },
    /// Stored task data does not parse. Nothing changed: the cancel refused the task.
    ///
    /// ``cause`` is an ``OutcomeDecodeError`` with the core's message, for
    /// example a history status that is not a task status. Not retryable: the
    /// same call returns this error until the stored row is repaired. A repeat
    /// does not repair it.
    #[error("{message}")]
    #[error_class(
        class = "TaskActionStoredDataInvalid",
        code = "StoredDataInvalid",
        never_retryable,
        cause = "required"
    )]
    StoredDataInvalid {
        #[error_slot(slot_type = "Str", hidden)]
        message: String,
        #[error_slot(slot_type = "Uuid")]
        task_id: Uuid,
        /// The outcome row that does not decode.
        #[error_slot(cause)]
        #[source]
        cause: OutcomeDecodeError,
    },
}

impl TaskActionError {
    /// The cancel of `task_id` met `cause`: a failed connect, client family
    /// check, or a statement that was not sent or that the server refused.
    /// Nothing was written. `retryable` is the cause's retry rule. Pure.
    pub fn cancel_failed(task_id: Uuid, cause: BrokerError) -> Self {
        Self::DatabaseOperationFailed {
            message: format!("Task cancel failed: {cause}"),
            retryable: cause.is_retryable(),
            task_id,
            cause: Box::new(cause),
        }
    }

    /// The cancel statement of `task_id` was sent and its reply was lost
    /// (`failure`): the cancel may have committed. `retryable` is the
    /// cause's retry rule. Pure.
    pub fn cancel_outcome_unknown(task_id: Uuid, failure: StatementFailure) -> Self {
        let cause = BrokerError::Statement(failure);
        Self::OutcomeUnknown {
            message: format!("Task cancel failed: {cause}"),
            retryable: cause.is_retryable(),
            task_id,
            cause: Box::new(cause),
        }
    }

    /// The catalog code of the variant. Pure.
    pub fn code(&self) -> TaskActionErrorCode {
        crate::error_catalog::ErrorEnum::error_code(self)
    }

    /// What was refused or failed, and why. Pure.
    pub fn message(&self) -> &str {
        match self {
            Self::NotFound { message, .. }
            | Self::NotCancellable { message, .. }
            | Self::IsWorkflowTask { message, .. }
            | Self::DatabaseOperationFailed { message, .. }
            | Self::OutcomeUnknown { message, .. }
            | Self::StoredDataInvalid { message, .. } => message,
        }
    }

    /// Whether a repeat of the same action can succeed: the classification
    /// of the cause of a database failure; every refusal stays. Pure.
    pub fn retryable(&self) -> bool {
        match self {
            Self::DatabaseOperationFailed { retryable, .. }
            | Self::OutcomeUnknown { retryable, .. } => *retryable,
            Self::NotFound { .. }
            | Self::NotCancellable { .. }
            | Self::IsWorkflowTask { .. }
            | Self::StoredDataInvalid { .. } => false,
        }
    }

    /// The task of the action. Pure.
    pub fn task_id(&self) -> Uuid {
        match self {
            Self::NotFound { task_id, .. }
            | Self::NotCancellable { task_id, .. }
            | Self::IsWorkflowTask { task_id, .. }
            | Self::DatabaseOperationFailed { task_id, .. }
            | Self::OutcomeUnknown { task_id, .. }
            | Self::StoredDataInvalid { task_id, .. } => *task_id,
        }
    }

    /// The cause of the failure: the cause of a database failure; stored
    /// data that does not parse broke the read contract; a refusal is a
    /// precondition that does not hold. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::DatabaseOperationFailed { cause, .. } | Self::OutcomeUnknown { cause, .. } => {
                cause.failure_cause()
            }
            Self::StoredDataInvalid { .. } => FailureCause::ContractViolation,
            Self::NotFound { .. } | Self::NotCancellable { .. } | Self::IsWorkflowTask { .. } => {
                FailureCause::PreconditionFailed
            }
        }
    }
}

/// A task that the cancel moved to CANCELLED: `was_status` is the status
/// it left.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCancelled {
    pub task_id: Uuid,
    pub was_status: TaskStatus,
}

pub type TaskActionResult<T> = Result<T, TaskActionError>;

fn not_found(task_id: Uuid) -> TaskActionError {
    TaskActionError::NotFound {
        message: format!("Task {task_id} does not exist."),
        task_id,
    }
}

fn workflow_bound(task_id: Uuid, current_status: TaskStatus) -> TaskActionError {
    TaskActionError::IsWorkflowTask {
        message: format!("Task {task_id} belongs to a workflow; use the workflow actions instead."),
        task_id,
        current_status,
    }
}

fn state_conflict(task_id: Uuid, current_status: TaskStatus) -> TaskActionError {
    TaskActionError::NotCancellable {
        message: format!("Task {task_id} cannot be cancelled from status {current_status}."),
        task_id,
        current_status,
    }
}

/// Stored data that does not parse. Nothing changed: the cancel refused the
/// task.
fn stored_data_invalid(task_id: Uuid, cause: OutcomeDecodeError) -> TaskActionError {
    TaskActionError::StoredDataInvalid {
        message: format!("Task cancel failed: stored data does not parse: {cause}"),
        task_id,
        cause,
    }
}

/// Move a non-workflow live task to CANCELLED (Python
/// `horsies.monitoring.task_actions.cancel_task`).
///
/// PENDING and CLAIMED are eligible. RUNNING is eligible only with
/// [`CancelPolicy::IncludeRunning`]: the same statement records the running
/// attempt with the outcome `CANCELLED`, and the worker that runs it gets the
/// cancel notice and gives the cancel to the task code. A workflow task, live or
/// finished, is `TaskIsWorkflowTask`; a finished plain task is
/// `TaskNotCancellable` with its terminal status; a task held nowhere is
/// `TaskNotFound`.
///
/// Panics: as [`PostgresBroker::cancel_task`], on an outcome that
/// contradicts the function.
pub async fn cancel_task(
    broker: &PostgresBroker,
    task_id: Uuid,
    policy: CancelPolicy,
) -> TaskActionResult<TaskCancelled> {
    // The SQL function keeps its schema parameter `p_include_running`.
    let include_running = match policy {
        CancelPolicy::IncludeRunning => true,
        CancelPolicy::PendingOrClaimedOnly => false,
    };
    match broker.cancel_task(task_id, include_running).await {
        Ok(CancelAnswer::Cancelled { was_status }) => Ok(TaskCancelled {
            task_id,
            was_status,
        }),
        Ok(CancelAnswer::WorkflowTask { current_status }) => {
            Err(workflow_bound(task_id, current_status))
        }
        Ok(CancelAnswer::NotCancellable { current_status }) => {
            Err(state_conflict(task_id, current_status))
        }
        Ok(CancelAnswer::TaskAbsent) => Err(not_found(task_id)),
        Err(CancelTaskError::Broker(error)) => Err(TaskActionError::cancel_failed(task_id, error)),
        Err(CancelTaskError::OutcomeUnknown(failure)) => {
            Err(TaskActionError::cancel_outcome_unknown(task_id, failure))
        }
        Err(CancelTaskError::StoredDataInvalid(error)) => Err(stored_data_invalid(task_id, error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TASK: Uuid = Uuid::from_u128(0x3f9c_1a2b_4d5e_4f60_8a71_b2c3_d4e5_f607);

    /// The texts of the refusals, each with the task id in its text form.
    #[test]
    fn each_refusal_has_python_s_message() {
        assert_eq!(
            not_found(TASK).message(),
            "Task 3f9c1a2b-4d5e-4f60-8a71-b2c3d4e5f607 does not exist."
        );
        assert_eq!(
            workflow_bound(TASK, TaskStatus::Failed).message(),
            "Task 3f9c1a2b-4d5e-4f60-8a71-b2c3d4e5f607 belongs to a workflow; \
             use the workflow actions instead."
        );
        assert_eq!(
            state_conflict(TASK, TaskStatus::Cancelled).message(),
            "Task 3f9c1a2b-4d5e-4f60-8a71-b2c3d4e5f607 cannot be cancelled from status CANCELLED."
        );
    }

    /// The texts of the failures: the cause's text after the action.
    #[test]
    fn each_failure_has_python_s_message_and_keeps_its_cause() {
        let failed = TaskActionError::cancel_failed(
            TASK,
            BrokerError::ConnectionFailed("connection refused".to_owned()),
        );
        assert_eq!(
            failed.message(),
            "Task cancel failed: connection failed: connection refused"
        );
        assert!(failed.retryable());
        assert_eq!(failed.failure_cause(), FailureCause::Connection);
        assert!(matches!(
            &failed,
            TaskActionError::DatabaseOperationFailed { cause, .. }
                if matches!(**cause, BrokerError::ConnectionFailed(_))
        ));

        let unknown = TaskActionError::cancel_outcome_unknown(
            TASK,
            StatementFailure::OutcomeUnknown {
                statement: "cancel_task",
                cause: crate::db::UnknownOutcomeCause::NoReplyWithinBound {
                    bound: std::time::Duration::from_secs(1),
                    grace: std::time::Duration::from_secs(1),
                    cancel: crate::db::CancelRequestOutcome::Sent,
                },
            },
        );
        assert_eq!(unknown.code(), TaskActionErrorCode::OutcomeUnknown);
        assert!(unknown
            .message()
            .starts_with("Task cancel failed: database error: "));
        assert!(unknown.retryable());
        assert_eq!(unknown.failure_cause(), FailureCause::Connection);
        assert_eq!(unknown.task_id(), TASK);
        assert_eq!(failed.code(), TaskActionErrorCode::DatabaseOperationFailed);

        let invalid = stored_data_invalid(
            TASK,
            OutcomeDecodeError("observed_status NOT_KNOWN".to_owned()),
        );
        assert_eq!(
            invalid.message(),
            "Task cancel failed: stored data does not parse: \
             terminalization outcome decode failed: observed_status NOT_KNOWN"
        );
        assert!(!invalid.retryable());
        assert_eq!(invalid.failure_cause(), FailureCause::ContractViolation);
    }
}

//! Administrative task cancellation: one `horsies_cancel_task` statement
//! ([`PostgresBroker::cancel_task`]), with the answer as a task action
//! result.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::broker::{BrokerError, CancelAnswer, CancelTaskError, PostgresBroker};
use crate::core::types::status::TaskStatus;
use crate::failure_cause::FailureCause;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_core_derive::CodeCatalog,
)]
#[code_catalog(module = "monitoring", base = "str_enum")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskActionErrorCode {
    TaskNotFound,
    TaskNotCancellable,
    TaskIsWorkflowTask,
    DbOperationFailed,
    /// The broker already holds resource sets for its limit of open event
    /// loops, and this call ran on a new one. Nothing changed. Python client
    /// only; removed before 0.8.0.
    EventLoopLimit,
    /// Stored task data does not parse: a history status that is not a task
    /// status, or a cancel outcome row that does not decode. Nothing changed:
    /// the cancel refused the task. Not retryable: a repeat gives the same
    /// result until the stored row changes (Python
    /// `python/horsies/monitoring/task_actions.py`, `TaskActionStoredDataInvalid`).
    StoredDataInvalid,
}

/// A refused or failed task action: one variant per class of Python's task
/// action catalog that the core produces, each with the facts of its state.
/// Python's `TaskActionEventLoopLimit` is a class of the Python client only,
/// so it is not a variant.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TaskActionError {
    /// Python `TaskActionNotFound`: neither the live table nor the history
    /// holds the task. Nothing changed.
    #[error("{message}")]
    NotFound { message: String, task_id: Uuid },
    /// Python `TaskActionNotCancellable`: the status that the cancel found
    /// does not admit it (a terminal status, or `RUNNING` without
    /// `include_running`). Nothing changed.
    #[error("{message}")]
    NotCancellable {
        message: String,
        task_id: Uuid,
        current_status: TaskStatus,
    },
    /// Python `TaskActionWorkflowTask`: the task belongs to a workflow; only
    /// the workflow actions drive it. Nothing changed.
    #[error("{message}")]
    WorkflowTask {
        message: String,
        task_id: Uuid,
        current_status: TaskStatus,
    },
    /// Python `TaskActionDbOperationFailed`: the connect, the client family
    /// check or the statement failed. Nothing was written, or the
    /// statement's outcome is unknown. `retryable` is the classification of
    /// the cause.
    #[error("{message}")]
    DbOperationFailed {
        message: String,
        retryable: bool,
        task_id: Uuid,
        cause: FailureCause,
    },
    /// Python `TaskActionStoredDataInvalid`: stored task data does not parse.
    /// Nothing changed: the cancel refused the task. Not retryable.
    #[error("{message}")]
    StoredDataInvalid { message: String, task_id: Uuid },
}

impl TaskActionError {
    /// The catalog code of the variant. Pure.
    pub fn code(&self) -> TaskActionErrorCode {
        match self {
            Self::NotFound { .. } => TaskActionErrorCode::TaskNotFound,
            Self::NotCancellable { .. } => TaskActionErrorCode::TaskNotCancellable,
            Self::WorkflowTask { .. } => TaskActionErrorCode::TaskIsWorkflowTask,
            Self::DbOperationFailed { .. } => TaskActionErrorCode::DbOperationFailed,
            Self::StoredDataInvalid { .. } => TaskActionErrorCode::StoredDataInvalid,
        }
    }

    /// What was refused or failed, and why. Pure.
    pub fn message(&self) -> &str {
        match self {
            Self::NotFound { message, .. }
            | Self::NotCancellable { message, .. }
            | Self::WorkflowTask { message, .. }
            | Self::DbOperationFailed { message, .. }
            | Self::StoredDataInvalid { message, .. } => message,
        }
    }

    /// Whether a repeat of the same action can succeed: the classification
    /// of the cause of a database failure; every refusal stays. Pure.
    pub fn retryable(&self) -> bool {
        match self {
            Self::DbOperationFailed { retryable, .. } => *retryable,
            Self::NotFound { .. }
            | Self::NotCancellable { .. }
            | Self::WorkflowTask { .. }
            | Self::StoredDataInvalid { .. } => false,
        }
    }

    /// The task of the action. Pure.
    pub fn task_id(&self) -> Uuid {
        match self {
            Self::NotFound { task_id, .. }
            | Self::NotCancellable { task_id, .. }
            | Self::WorkflowTask { task_id, .. }
            | Self::DbOperationFailed { task_id, .. }
            | Self::StoredDataInvalid { task_id, .. } => *task_id,
        }
    }

    /// The cause of the failure: the cause of a database failure; stored
    /// data that does not parse broke the read contract; a refusal is a
    /// precondition that does not hold. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::DbOperationFailed { cause, .. } => cause.clone(),
            Self::StoredDataInvalid { .. } => FailureCause::ContractViolation,
            Self::NotFound { .. } | Self::NotCancellable { .. } | Self::WorkflowTask { .. } => {
                FailureCause::PreconditionFailed
            }
        }
    }
}

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
    TaskActionError::WorkflowTask {
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

/// A failed connect, client family check or statement: nothing was
/// written, or the statement's outcome is unknown.
fn broker_failure(task_id: Uuid, error: &BrokerError) -> TaskActionError {
    TaskActionError::DbOperationFailed {
        message: format!("Task cancel failed: {error}"),
        retryable: error.is_retryable(),
        task_id,
        cause: error.failure_cause(),
    }
}

/// Stored data that does not parse. Nothing changed: the cancel refused the
/// task.
fn stored_data_invalid(task_id: Uuid, detail: impl Into<String>) -> TaskActionError {
    TaskActionError::StoredDataInvalid {
        message: format!(
            "Task cancel failed: stored data does not parse: {}",
            detail.into()
        ),
        task_id,
    }
}

/// Move a non-workflow live task to CANCELLED (Python
/// `horsies.monitoring.task_actions.cancel_task`).
///
/// PENDING and CLAIMED are eligible. RUNNING is eligible only when
/// `include_running` is true. A workflow task, live or finished, is
/// `TaskIsWorkflowTask`; a finished plain task is `TaskNotCancellable` with
/// its terminal status; a task held nowhere is `TaskNotFound`.
///
/// Panics: as [`PostgresBroker::cancel_task`], on an outcome that
/// contradicts the function.
pub async fn cancel_task(
    broker: &PostgresBroker,
    task_id: Uuid,
    include_running: bool,
) -> TaskActionResult<TaskCancelled> {
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
        Err(CancelTaskError::Broker(error)) => Err(broker_failure(task_id, &error)),
        Err(CancelTaskError::StoredDataInvalid(error)) => {
            Err(stored_data_invalid(task_id, error.to_string()))
        }
    }
}

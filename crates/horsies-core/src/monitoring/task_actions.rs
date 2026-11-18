//! Administrative task cancellation: one `horsies_cancel_task` statement
//! ([`PostgresBroker::cancel_task`]), with the answer as a task action
//! result.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::broker::{BrokerError, CancelAnswer, CancelTaskError, PostgresBroker};
use crate::core::types::status::TaskStatus;

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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct TaskActionError {
    pub code: TaskActionErrorCode,
    pub message: String,
    pub retryable: bool,
    pub task_id: Uuid,
    pub current_status: Option<TaskStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCancelled {
    pub task_id: Uuid,
    pub was_status: TaskStatus,
}

pub type TaskActionResult<T> = Result<T, TaskActionError>;

fn not_found(task_id: Uuid) -> TaskActionError {
    TaskActionError {
        code: TaskActionErrorCode::TaskNotFound,
        message: format!("Task {task_id} does not exist."),
        retryable: false,
        task_id,
        current_status: None,
    }
}

fn workflow_bound(task_id: Uuid, current_status: Option<TaskStatus>) -> TaskActionError {
    TaskActionError {
        code: TaskActionErrorCode::TaskIsWorkflowTask,
        message: format!("Task {task_id} belongs to a workflow; use the workflow actions instead."),
        retryable: false,
        task_id,
        current_status,
    }
}

fn state_conflict(
    task_id: Uuid,
    status_value: &str,
    current_status: Option<TaskStatus>,
) -> TaskActionError {
    TaskActionError {
        code: TaskActionErrorCode::TaskNotCancellable,
        message: format!("Task {task_id} cannot be cancelled from status {status_value}."),
        retryable: false,
        task_id,
        current_status,
    }
}

/// A failed connect, client family check or statement: nothing was
/// written, or the statement's outcome is unknown.
fn broker_failure(task_id: Uuid, error: &BrokerError) -> TaskActionError {
    TaskActionError {
        code: TaskActionErrorCode::DbOperationFailed,
        message: format!("Task cancel failed: {error}"),
        retryable: error.is_retryable(),
        task_id,
        current_status: None,
    }
}

/// Stored data that does not parse. Nothing changed: the cancel refused the
/// task.
fn stored_data_invalid(task_id: Uuid, detail: impl Into<String>) -> TaskActionError {
    TaskActionError {
        code: TaskActionErrorCode::StoredDataInvalid,
        message: format!(
            "Task cancel failed: stored data does not parse: {}",
            detail.into()
        ),
        retryable: false,
        task_id,
        current_status: None,
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
            Err(workflow_bound(task_id, Some(current_status)))
        }
        Ok(CancelAnswer::NotCancellable { current_status }) => Err(state_conflict(
            task_id,
            &current_status.to_string(),
            Some(current_status),
        )),
        Ok(CancelAnswer::TaskAbsent) => Err(not_found(task_id)),
        Err(CancelTaskError::Broker(error)) => Err(broker_failure(task_id, &error)),
        Err(CancelTaskError::StoredDataInvalid(error)) => {
            Err(stored_data_invalid(task_id, error.to_string()))
        }
    }
}

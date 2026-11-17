/// Typed error types for WorkflowHandle operations.
use uuid::Uuid;

use crate::failure_cause::FailureCause;
///
/// Mirrors Python's `python/horsies/core/models/workflow/handle_types.py`.
///
/// Two error strategies by return type:
///
/// 1. **Wrap strategy** — methods that return non-TaskResult types
///    (status, cancel, pause, resume, results, tasks) return
///    `HandleResult<T>`. Infrastructure errors are `Err(HandleOperationError)`.
///
/// 2. **Fold strategy** — methods that return `TaskResult`
///    (`WorkflowHandle::get`, `WorkflowHandle::result_for`) keep that
///    shape. Infrastructure/query failures fold into
///    `TaskResult::Err(TaskError(...))` instead of bubbling out as
///    `HandleResult::Err(...)`.
///
/// Categorized handle operation failure codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, horsies_macros::CodeCatalog)]
#[code_catalog(module = "workflow_handle", base = "str_enum")]
pub enum HandleErrorCode {
    /// Workflow ID does not exist in the database.
    WorkflowNotFound,
    /// Database query or commit failed.
    DbOperationFailed,
    /// A stored value of the workflow does not parse (an unknown status, a stored JSON value). Nothing
    /// changed, and the same call returns the same error. Not retryable.
    StoredDataInvalid,
    /// `resume` resumed the workflow, and the recovery pass for its tree, which runs after that, failed.
    /// The workflow is RUNNING. Not retryable.
    ResumeRecoveryFailed,
    /// Internal polling/event-loop runner failed.
    LoopRunnerFailed,
    /// The broker already holds resource sets for its limit of open event loops, and this call ran on a
    /// new one. Nothing ran. Python client only; removed before 0.8.0.
    EventLoopLimit,
    /// A sync handle method was called on a thread that runs an event loop. Its wait would stall that
    /// loop, so nothing ran. Python client only.
    AsyncContext,
}

impl std::fmt::Display for HandleErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WorkflowNotFound => write!(f, "WORKFLOW_NOT_FOUND"),
            Self::DbOperationFailed => write!(f, "DB_OPERATION_FAILED"),
            Self::StoredDataInvalid => write!(f, "STORED_DATA_INVALID"),
            Self::ResumeRecoveryFailed => write!(f, "RESUME_RECOVERY_FAILED"),
            Self::LoopRunnerFailed => write!(f, "LOOP_RUNNER_FAILED"),
            Self::EventLoopLimit => write!(f, "EVENT_LOOP_LIMIT"),
            Self::AsyncContext => write!(f, "ASYNC_CONTEXT"),
        }
    }
}

/// Error payload for handle operations.
///
/// Carried inside `Err(...)` of a `HandleResult<T>`.
#[derive(Debug, Clone)]
pub struct HandleOperationError {
    /// Which failure category.
    pub code: HandleErrorCode,
    /// Human-readable description.
    pub message: String,
    /// Whether the caller can safely retry.
    pub retryable: bool,
    /// The workflow this handle refers to.
    pub workflow_id: Uuid,
    /// What the operation met: the server's error, no answer, a broken read
    /// contract, or a failed precondition (an absent workflow included).
    pub cause: FailureCause,
}

impl std::fmt::Display for HandleOperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {} (workflow_id={}, retryable={})",
            self.code, self.message, self.workflow_id, self.retryable,
        )
    }
}

impl std::error::Error for HandleOperationError {}

/// Result type for wrap-strategy workflow handle operations.
///
/// Used by methods like `status()`, `results()`, `tasks()`, `cancel()`,
/// `pause()`, and `resume()`.
///
/// `Ok(T)` on success, `Err(HandleOperationError)` on failure.
pub type HandleResult<T> = Result<T, HandleOperationError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_display() {
        assert_eq!(
            HandleErrorCode::WorkflowNotFound.to_string(),
            "WORKFLOW_NOT_FOUND"
        );
        assert_eq!(
            HandleErrorCode::DbOperationFailed.to_string(),
            "DB_OPERATION_FAILED"
        );
        assert_eq!(
            HandleErrorCode::StoredDataInvalid.to_string(),
            "STORED_DATA_INVALID"
        );
        assert_eq!(
            HandleErrorCode::ResumeRecoveryFailed.to_string(),
            "RESUME_RECOVERY_FAILED"
        );
    }

    #[test]
    fn handle_operation_error_display() {
        let err = HandleOperationError {
            code: HandleErrorCode::DbOperationFailed,
            message: "connection refused".to_owned(),
            retryable: true,
            workflow_id: uuid::Uuid::nil(),
            cause: FailureCause::Connection,
        };
        let s = format!("{}", err);
        assert!(s.contains("DB_OPERATION_FAILED"));
        assert!(s.contains(&uuid::Uuid::nil().to_string()));
        assert!(s.contains("retryable=true"));
    }
}

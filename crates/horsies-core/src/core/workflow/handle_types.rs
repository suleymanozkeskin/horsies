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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, horsies_core_derive::CodeCatalog)]
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
    /// A sync or blocking handle method was called on a thread that runs an event loop or a tokio
    /// runtime. Its wait would stall that loop, so nothing ran.
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

/// The failure of a workflow handle operation: one variant per Python class
/// of `python/horsies/core/models/workflow/handle_types.py` that the core or
/// a client of the core produces, each with the facts of its state only.
/// Carried inside `Err(...)` of a [`HandleResult`].
///
/// The classes that only the Python client produces are not variants:
/// `HandleLoopRunnerFailed` and `HandleEventLoopLimit`. A variant without
/// `cause` has the precondition cause of its state
/// ([`Self::failure_cause`]); a variant without `retryable` never clears.
#[derive(Debug, Clone)]
pub enum HandleOperationError {
    /// Python `HandleWorkflowNotFound`: no workflow has the id. Nothing
    /// changed.
    WorkflowNotFound { message: String, workflow_id: Uuid },
    /// Python `HandleDbOperationFailed`: a query or a commit failed.
    DbOperationFailed {
        message: String,
        retryable: bool,
        workflow_id: Uuid,
        cause: FailureCause,
    },
    /// Python `HandleStoredDataInvalid`: a stored value of the workflow does
    /// not parse. Nothing changed, and the same call fails again.
    StoredDataInvalid {
        message: String,
        workflow_id: Uuid,
        cause: FailureCause,
    },
    /// Python `HandleResumeRecoveryFailed`: the resume committed, and the
    /// recovery pass for its tree failed. The workflow is RUNNING.
    ResumeRecoveryFailed {
        message: String,
        workflow_id: Uuid,
        cause: FailureCause,
    },
    /// Python `HandleAsyncContext`: a sync or blocking handle method on a
    /// thread that runs an event loop or a tokio runtime. Nothing ran.
    AsyncContext { message: String, workflow_id: Uuid },
}

impl HandleOperationError {
    /// The catalog code of the variant. Pure.
    pub fn code(&self) -> HandleErrorCode {
        match self {
            Self::WorkflowNotFound { .. } => HandleErrorCode::WorkflowNotFound,
            Self::DbOperationFailed { .. } => HandleErrorCode::DbOperationFailed,
            Self::StoredDataInvalid { .. } => HandleErrorCode::StoredDataInvalid,
            Self::ResumeRecoveryFailed { .. } => HandleErrorCode::ResumeRecoveryFailed,
            Self::AsyncContext { .. } => HandleErrorCode::AsyncContext,
        }
    }

    /// What failed and why. Pure.
    pub fn message(&self) -> &str {
        match self {
            Self::WorkflowNotFound { message, .. }
            | Self::DbOperationFailed { message, .. }
            | Self::StoredDataInvalid { message, .. }
            | Self::ResumeRecoveryFailed { message, .. }
            | Self::AsyncContext { message, .. } => message,
        }
    }

    /// Whether a repeat of the call can succeed: the classification of the
    /// cause of `DbOperationFailed`, else `false`. Pure.
    pub fn retryable(&self) -> bool {
        match self {
            Self::DbOperationFailed { retryable, .. } => *retryable,
            Self::WorkflowNotFound { .. }
            | Self::StoredDataInvalid { .. }
            | Self::ResumeRecoveryFailed { .. }
            | Self::AsyncContext { .. } => false,
        }
    }

    /// The workflow of the handle. Pure.
    pub fn workflow_id(&self) -> Uuid {
        match self {
            Self::WorkflowNotFound { workflow_id, .. }
            | Self::DbOperationFailed { workflow_id, .. }
            | Self::StoredDataInvalid { workflow_id, .. }
            | Self::ResumeRecoveryFailed { workflow_id, .. }
            | Self::AsyncContext { workflow_id, .. } => *workflow_id,
        }
    }

    /// The cause of the failure: the variant's `cause`, else a failed
    /// precondition (an absent workflow, a refused thread). Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::DbOperationFailed { cause, .. }
            | Self::StoredDataInvalid { cause, .. }
            | Self::ResumeRecoveryFailed { cause, .. } => cause.clone(),
            Self::WorkflowNotFound { .. } | Self::AsyncContext { .. } => {
                FailureCause::PreconditionFailed
            }
        }
    }
}

impl std::fmt::Display for HandleOperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {} (workflow_id={}, retryable={})",
            self.code(),
            self.message(),
            self.workflow_id(),
            self.retryable(),
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
        let err = HandleOperationError::DbOperationFailed {
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

    /// Each variant has the code of its Python class; only
    /// `DbOperationFailed` can be retryable, and a variant without a cause
    /// has a failed precondition.
    #[test]
    fn each_variant_has_the_code_of_its_python_class() {
        let workflow_id = uuid::Uuid::from_u128(9);
        let cases = [
            (
                HandleOperationError::WorkflowNotFound {
                    message: "m".to_owned(),
                    workflow_id,
                },
                HandleErrorCode::WorkflowNotFound,
                FailureCause::PreconditionFailed,
            ),
            (
                HandleOperationError::DbOperationFailed {
                    message: "m".to_owned(),
                    retryable: false,
                    workflow_id,
                    cause: FailureCause::Connection,
                },
                HandleErrorCode::DbOperationFailed,
                FailureCause::Connection,
            ),
            (
                HandleOperationError::StoredDataInvalid {
                    message: "m".to_owned(),
                    workflow_id,
                    cause: FailureCause::ContractViolation,
                },
                HandleErrorCode::StoredDataInvalid,
                FailureCause::ContractViolation,
            ),
            (
                HandleOperationError::ResumeRecoveryFailed {
                    message: "m".to_owned(),
                    workflow_id,
                    cause: FailureCause::Connection,
                },
                HandleErrorCode::ResumeRecoveryFailed,
                FailureCause::Connection,
            ),
            (
                HandleOperationError::AsyncContext {
                    message: "m".to_owned(),
                    workflow_id,
                },
                HandleErrorCode::AsyncContext,
                FailureCause::PreconditionFailed,
            ),
        ];
        for (error, code, cause) in cases {
            assert_eq!(error.code(), code);
            assert_eq!(error.message(), "m");
            assert_eq!(error.workflow_id(), workflow_id);
            assert_eq!(error.failure_cause(), cause);
            assert!(!error.retryable());
        }
    }
}

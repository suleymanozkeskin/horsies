/// Typed error types for workflow start operations.
///
/// Mirrors Python's `python/horsies/core/workflows/start_types.py`.
use uuid::Uuid;

use crate::FailureCause;

/// Categorized workflow start failure codes.
///
/// The members are Python's, in Python's order
/// (`python/horsies/core/workflows/start_types.py`). `LOOP_RUNNER_FAILED`
/// and `EVENT_LOOP_LIMIT` are produced by the Python client only.
#[derive(Debug, Clone, PartialEq, Eq, Hash, horsies_core_derive::CodeCatalog)]
#[code_catalog(module = "workflow_start", base = "str_enum")]
pub enum WorkflowStartErrorCode {
    /// Config error: no broker attached.
    BrokerNotConfigured,
    /// Programmer error: bad DAG, bad args, serialization failure.
    ValidationFailed,
    /// The workflow id exists for another workflow name; nothing was written. Not retryable.
    WorkflowIdConflict,
    /// Infrastructure error: schema init, DB transaction. Conditionally retryable.
    EnqueueFailed,
    /// A sync start could not run the call on the broker's sync runner: the runner is stopped, its
    /// thread did not start, or scheduling failed. Nothing ran. Python client only; removed before
    /// 0.8.0.
    LoopRunnerFailed,
    /// The broker already holds resource sets for its limit of open event loops, and this call ran on a
    /// new one; nothing was written. Python client only; removed before 0.8.0.
    EventLoopLimit,
    /// A sync or blocking start was called on a thread that runs an event loop or a tokio
    /// runtime; its wait would stall that loop, so nothing was written. `retry_start` accepts it.
    AsyncContext,
}

impl std::fmt::Display for WorkflowStartErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BrokerNotConfigured => write!(f, "BROKER_NOT_CONFIGURED"),
            Self::ValidationFailed => write!(f, "VALIDATION_FAILED"),
            Self::WorkflowIdConflict => write!(f, "WORKFLOW_ID_CONFLICT"),
            Self::EnqueueFailed => write!(f, "ENQUEUE_FAILED"),
            Self::LoopRunnerFailed => write!(f, "LOOP_RUNNER_FAILED"),
            Self::EventLoopLimit => write!(f, "EVENT_LOOP_LIMIT"),
            Self::AsyncContext => write!(f, "ASYNC_CONTEXT"),
        }
    }
}

/// The failure of a workflow start: one variant per Python class of
/// `python/horsies/core/workflows/start_types.py` that the core or a client
/// of the core produces, each with the facts of its state only. Carried
/// inside `Err(...)` of a [`WorkflowStartResult`].
///
/// The classes that only the Python client produces are not variants:
/// `WorkflowStartBrokerNotConfigured`, `WorkflowStartLoopRunnerFailed` and
/// `WorkflowStartEventLoopLimit`.
#[derive(Debug, Clone)]
pub enum WorkflowStartError {
    /// Python `WorkflowStartValidationFailed`: the spec, its arguments or
    /// the error passed to a retry was refused. Nothing was written.
    /// `workflow_id` is the id of the start: the caller's id, or the id
    /// minted for it, as in Python. `cause` is what refused it (Python
    /// `exception`).
    ValidationFailed {
        message: String,
        workflow_name: String,
        workflow_id: Uuid,
        cause: FailureCause,
    },
    /// Python `WorkflowStartIdConflict`: the id holds the workflow
    /// `existing_workflow_name`. Nothing was written.
    IdConflict {
        message: String,
        workflow_name: String,
        workflow_id: Uuid,
        existing_workflow_name: String,
    },
    /// Python `WorkflowStartEnqueueFailed`: the connect, the schema install
    /// or the start failed. Nothing was committed, or the commit reply was
    /// lost: a start again with the same id finds the workflow. `cause` is
    /// what the start met (Python `exception`).
    EnqueueFailed {
        message: String,
        retryable: bool,
        workflow_name: String,
        workflow_id: Uuid,
        cause: FailureCause,
    },
    /// Python `WorkflowStartAsyncContext`: a sync or blocking start on a
    /// thread that runs an event loop or a tokio runtime. Nothing was
    /// written; `retry_start` completes the start with `workflow_id`.
    AsyncContext {
        message: String,
        workflow_name: String,
        workflow_id: Uuid,
    },
}

impl WorkflowStartError {
    /// The catalog code of the variant. Pure.
    pub fn code(&self) -> WorkflowStartErrorCode {
        match self {
            Self::ValidationFailed { .. } => WorkflowStartErrorCode::ValidationFailed,
            Self::IdConflict { .. } => WorkflowStartErrorCode::WorkflowIdConflict,
            Self::EnqueueFailed { .. } => WorkflowStartErrorCode::EnqueueFailed,
            Self::AsyncContext { .. } => WorkflowStartErrorCode::AsyncContext,
        }
    }

    /// What failed and why. Pure.
    pub fn message(&self) -> &str {
        match self {
            Self::ValidationFailed { message, .. }
            | Self::IdConflict { message, .. }
            | Self::EnqueueFailed { message, .. }
            | Self::AsyncContext { message, .. } => message,
        }
    }

    /// Whether a start again can succeed: the classification of the cause
    /// of `EnqueueFailed`, else `false`. Pure.
    pub fn retryable(&self) -> bool {
        match self {
            Self::EnqueueFailed { retryable, .. } => *retryable,
            Self::ValidationFailed { .. } | Self::IdConflict { .. } | Self::AsyncContext { .. } => {
                false
            }
        }
    }

    /// The name of the workflow spec of the start. Pure.
    pub fn workflow_name(&self) -> &str {
        match self {
            Self::ValidationFailed { workflow_name, .. }
            | Self::IdConflict { workflow_name, .. }
            | Self::EnqueueFailed { workflow_name, .. }
            | Self::AsyncContext { workflow_name, .. } => workflow_name,
        }
    }

    /// The id of the start: the caller's id, or the id minted for it. Pure.
    pub fn workflow_id(&self) -> Uuid {
        match self {
            Self::ValidationFailed { workflow_id, .. }
            | Self::IdConflict { workflow_id, .. }
            | Self::EnqueueFailed { workflow_id, .. }
            | Self::AsyncContext { workflow_id, .. } => *workflow_id,
        }
    }

    /// What the start met. A variant without `cause` has the cause of its
    /// state: an id that another workflow holds, or a blocking start on a
    /// runtime thread, is a failed precondition. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::ValidationFailed { cause, .. } | Self::EnqueueFailed { cause, .. } => {
                cause.clone()
            }
            Self::IdConflict { .. } | Self::AsyncContext { .. } => FailureCause::PreconditionFailed,
        }
    }
}

impl std::fmt::Display for WorkflowStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {} (workflow={}, id={}, retryable={})",
            self.code(),
            self.message(),
            self.workflow_name(),
            self.workflow_id(),
            self.retryable(),
        )
    }
}

impl std::error::Error for WorkflowStartError {}

/// Result type for workflow start operations.
///
/// `Ok(T)` on success (typically `WorkflowHandle<T>`),
/// `Err(WorkflowStartError)` on failure.
pub type WorkflowStartResult<T> = Result<T, WorkflowStartError>;

/// Validate whether a `WorkflowStartError` is eligible for retry_start.
///
/// `EnqueueFailed` and `AsyncContext` errors can be retried: each carries the
/// id of a start that wrote nothing or may have written. The error's
/// workflow_name must match the spec being retried (prevents cross-workflow
/// retry).
///
/// Returns `Ok(workflow_id)` if retry is valid, `Err(WorkflowStartError)`
/// with `ValidationFailed` code otherwise.
///
/// Mirrors Python's `WorkflowSpec._validate_start_retry(error)`, which also
/// accepts the Python-only `LOOP_RUNNER_FAILED`.
pub fn validate_start_retry(
    error: &WorkflowStartError,
    spec_name: &str,
) -> Result<Uuid, WorkflowStartError> {
    let (workflow_name, workflow_id) = match error {
        WorkflowStartError::EnqueueFailed {
            workflow_name,
            workflow_id,
            ..
        }
        | WorkflowStartError::AsyncContext {
            workflow_name,
            workflow_id,
            ..
        } => (workflow_name, *workflow_id),
        WorkflowStartError::ValidationFailed { .. } | WorkflowStartError::IdConflict { .. } => {
            return Err(WorkflowStartError::ValidationFailed {
                message: format!(
                    "retry_start only accepts ENQUEUE_FAILED and ASYNC_CONTEXT errors, got {}",
                    error.code(),
                ),
                workflow_name: spec_name.to_owned(),
                workflow_id: error.workflow_id(),
                cause: FailureCause::PreconditionFailed,
            });
        }
    };
    if workflow_name != spec_name {
        return Err(WorkflowStartError::ValidationFailed {
            message: format!(
                "cross-workflow retry rejected: error from '{workflow_name}', spec is \
                 '{spec_name}'",
            ),
            workflow_name: spec_name.to_owned(),
            workflow_id,
            cause: FailureCause::PreconditionFailed,
        });
    }
    Ok(workflow_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_display() {
        assert_eq!(
            WorkflowStartErrorCode::BrokerNotConfigured.to_string(),
            "BROKER_NOT_CONFIGURED",
        );
        assert_eq!(
            WorkflowStartErrorCode::EnqueueFailed.to_string(),
            "ENQUEUE_FAILED",
        );
        assert_eq!(
            WorkflowStartErrorCode::WorkflowIdConflict.to_string(),
            "WORKFLOW_ID_CONFLICT",
        );
    }

    #[test]
    fn workflow_start_error_display() {
        let workflow_id = Uuid::new_v4();
        let err = WorkflowStartError::EnqueueFailed {
            message: "connection refused".to_owned(),
            retryable: true,
            workflow_name: "my_pipeline".to_owned(),
            workflow_id,
            cause: FailureCause::Connection,
        };
        let s = format!("{}", err);
        assert!(s.contains("ENQUEUE_FAILED"));
        assert!(s.contains("my_pipeline"));
        assert!(s.contains(&workflow_id.to_string()));
    }

    /// Each variant has the code of its Python class and the id of its
    /// start; only `EnqueueFailed` can be retryable. A variant without
    /// `cause` has a failed precondition as its cause.
    #[test]
    fn each_variant_has_the_code_of_its_python_class() {
        let workflow_id = Uuid::new_v4();
        let name = || "wf".to_owned();
        let cases = [
            (
                WorkflowStartError::ValidationFailed {
                    message: "m".to_owned(),
                    workflow_name: name(),
                    workflow_id,
                    cause: FailureCause::ContractViolation,
                },
                WorkflowStartErrorCode::ValidationFailed,
                FailureCause::ContractViolation,
            ),
            (
                WorkflowStartError::IdConflict {
                    message: "m".to_owned(),
                    workflow_name: name(),
                    workflow_id,
                    existing_workflow_name: "other".to_owned(),
                },
                WorkflowStartErrorCode::WorkflowIdConflict,
                FailureCause::PreconditionFailed,
            ),
            (
                WorkflowStartError::EnqueueFailed {
                    message: "m".to_owned(),
                    retryable: false,
                    workflow_name: name(),
                    workflow_id,
                    cause: FailureCause::Connection,
                },
                WorkflowStartErrorCode::EnqueueFailed,
                FailureCause::Connection,
            ),
            (
                WorkflowStartError::AsyncContext {
                    message: "m".to_owned(),
                    workflow_name: name(),
                    workflow_id,
                },
                WorkflowStartErrorCode::AsyncContext,
                FailureCause::PreconditionFailed,
            ),
        ];
        for (error, code, cause) in cases {
            assert_eq!(error.code(), code);
            assert_eq!(error.message(), "m");
            assert_eq!(error.workflow_name(), "wf");
            assert_eq!(error.workflow_id(), workflow_id);
            assert_eq!(error.failure_cause(), cause);
            assert!(!error.retryable());
        }
    }

    // -- validate_start_retry (ported from Python test_workflow_retry_start.py) --

    #[test]
    fn validate_retry_accepts_enqueue_failed() {
        let workflow_id = Uuid::new_v4();
        let error = WorkflowStartError::EnqueueFailed {
            message: "DB connection lost".to_owned(),
            retryable: true,
            workflow_name: "test-wf".to_owned(),
            workflow_id,
            cause: FailureCause::Connection,
        };
        let result = validate_start_retry(&error, "test-wf");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), workflow_id);
    }

    #[test]
    fn validate_retry_accepts_async_context() {
        let workflow_id = Uuid::new_v4();
        let error = WorkflowStartError::AsyncContext {
            message: "a blocking start on a runtime thread".to_owned(),
            workflow_name: "test-wf".to_owned(),
            workflow_id,
        };
        let result = validate_start_retry(&error, "test-wf");
        assert_eq!(result.unwrap(), workflow_id);
    }

    #[test]
    fn validate_retry_rejects_async_context_of_another_workflow() {
        let error = WorkflowStartError::AsyncContext {
            message: "a blocking start on a runtime thread".to_owned(),
            workflow_name: "other-workflow".to_owned(),
            workflow_id: Uuid::new_v4(),
        };
        let result = validate_start_retry(&error, "my-workflow");
        let err = result.unwrap_err();
        assert_eq!(err.code(), WorkflowStartErrorCode::ValidationFailed);
        assert!(err.message().contains("cross-workflow"));
    }

    #[test]
    fn validate_retry_rejects_validation_failed() {
        let error = WorkflowStartError::ValidationFailed {
            message: "Bad DAG".to_owned(),
            workflow_name: "test-wf".to_owned(),
            workflow_id: Uuid::new_v4(),
            cause: FailureCause::PreconditionFailed,
        };
        let result = validate_start_retry(&error, "test-wf");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), WorkflowStartErrorCode::ValidationFailed);
        assert!(err.message().contains("ENQUEUE_FAILED"));
        assert_eq!(err.workflow_id(), error.workflow_id());
        assert_eq!(err.failure_cause(), FailureCause::PreconditionFailed);
    }

    #[test]
    fn validate_retry_rejects_workflow_id_conflict() {
        let error = WorkflowStartError::IdConflict {
            message: "id belongs to another workflow".to_owned(),
            workflow_name: "test-wf".to_owned(),
            workflow_id: Uuid::new_v4(),
            existing_workflow_name: "other-wf".to_owned(),
        };
        let result = validate_start_retry(&error, "test-wf");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code(),
            WorkflowStartErrorCode::ValidationFailed,
        );
    }

    #[test]
    fn validate_retry_rejects_cross_workflow_name() {
        let error = WorkflowStartError::EnqueueFailed {
            message: "DB connection lost".to_owned(),
            retryable: true,
            workflow_name: "other-workflow".to_owned(),
            workflow_id: Uuid::new_v4(),
            cause: FailureCause::Connection,
        };
        let result = validate_start_retry(&error, "my-workflow");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), WorkflowStartErrorCode::ValidationFailed);
        assert!(err.message().contains("cross-workflow"));
    }
}

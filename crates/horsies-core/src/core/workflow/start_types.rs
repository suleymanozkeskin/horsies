/// Typed error types for workflow start operations.
///
/// Mirrors Python's `python/horsies/core/workflows/start_types.py`.
use uuid::Uuid;

/// Categorized workflow start failure codes.
///
/// The members are Python's, in Python's order
/// (`python/horsies/core/workflows/start_types.py`). `LOOP_RUNNER_FAILED`
/// and `EVENT_LOOP_LIMIT` are produced by the Python client only.
#[derive(Debug, Clone, PartialEq, Eq, Hash, horsies_macros::CodeCatalog)]
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

/// Error payload for workflow start operations.
///
/// Carried inside `Err(...)` of a `WorkflowStartResult<T>`.
#[derive(Debug, Clone)]
pub struct WorkflowStartError {
    /// Which failure category.
    pub code: WorkflowStartErrorCode,
    /// Human-readable description.
    pub message: String,
    /// Whether the caller can safely retry.
    pub retryable: bool,
    /// Workflow spec name (always available).
    pub workflow_name: String,
    /// Generated workflow ID (`None` when validation failed before minting).
    pub workflow_id: Option<Uuid>,
}

impl std::fmt::Display for WorkflowStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {} (workflow={}, id={}, retryable={})",
            self.code,
            self.message,
            self.workflow_name,
            self.workflow_id
                .map_or_else(|| "<unminted>".to_owned(), |id| id.to_string()),
            self.retryable,
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
    match error.code {
        WorkflowStartErrorCode::EnqueueFailed | WorkflowStartErrorCode::AsyncContext => {}
        WorkflowStartErrorCode::BrokerNotConfigured
        | WorkflowStartErrorCode::ValidationFailed
        | WorkflowStartErrorCode::WorkflowIdConflict
        | WorkflowStartErrorCode::LoopRunnerFailed
        | WorkflowStartErrorCode::EventLoopLimit => {
            return Err(WorkflowStartError {
                code: WorkflowStartErrorCode::ValidationFailed,
                message: format!(
                    "retry_start only accepts ENQUEUE_FAILED and ASYNC_CONTEXT errors, got {}",
                    error.code,
                ),
                retryable: false,
                workflow_name: spec_name.to_owned(),
                workflow_id: error.workflow_id.clone(),
            });
        }
    }

    if error.workflow_name != spec_name {
        return Err(WorkflowStartError {
            code: WorkflowStartErrorCode::ValidationFailed,
            message: format!(
                "cross-workflow retry rejected: error from '{}', spec is '{}'",
                error.workflow_name, spec_name,
            ),
            retryable: false,
            workflow_name: spec_name.to_owned(),
            workflow_id: error.workflow_id.clone(),
        });
    }

    error.workflow_id.ok_or_else(|| WorkflowStartError {
        code: WorkflowStartErrorCode::ValidationFailed,
        message: "retry_start requires an error with a minted workflow ID".to_owned(),
        retryable: false,
        workflow_name: spec_name.to_owned(),
        workflow_id: None,
    })
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
        let err = WorkflowStartError {
            code: WorkflowStartErrorCode::EnqueueFailed,
            message: "connection refused".to_owned(),
            retryable: true,
            workflow_name: "my_pipeline".to_owned(),
            workflow_id: Some(workflow_id),
        };
        let s = format!("{}", err);
        assert!(s.contains("ENQUEUE_FAILED"));
        assert!(s.contains("my_pipeline"));
        assert!(s.contains(&workflow_id.to_string()));
    }

    // -- validate_start_retry (ported from Python test_workflow_retry_start.py) --

    #[test]
    fn validate_retry_accepts_enqueue_failed() {
        let workflow_id = Uuid::new_v4();
        let error = WorkflowStartError {
            code: WorkflowStartErrorCode::EnqueueFailed,
            message: "DB connection lost".to_owned(),
            retryable: true,
            workflow_name: "test-wf".to_owned(),
            workflow_id: Some(workflow_id),
        };
        let result = validate_start_retry(&error, "test-wf");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), workflow_id);
    }

    #[test]
    fn validate_retry_accepts_async_context() {
        let workflow_id = Uuid::new_v4();
        let error = WorkflowStartError {
            code: WorkflowStartErrorCode::AsyncContext,
            message: "a blocking start on a runtime thread".to_owned(),
            retryable: false,
            workflow_name: "test-wf".to_owned(),
            workflow_id: Some(workflow_id),
        };
        let result = validate_start_retry(&error, "test-wf");
        assert_eq!(result.unwrap(), workflow_id);
    }

    #[test]
    fn validate_retry_rejects_async_context_of_another_workflow() {
        let error = WorkflowStartError {
            code: WorkflowStartErrorCode::AsyncContext,
            message: "a blocking start on a runtime thread".to_owned(),
            retryable: false,
            workflow_name: "other-workflow".to_owned(),
            workflow_id: Some(Uuid::new_v4()),
        };
        let result = validate_start_retry(&error, "my-workflow");
        let err = result.unwrap_err();
        assert_eq!(err.code, WorkflowStartErrorCode::ValidationFailed);
        assert!(err.message.contains("cross-workflow"));
    }

    #[test]
    fn validate_retry_rejects_broker_not_configured() {
        let error = WorkflowStartError {
            code: WorkflowStartErrorCode::BrokerNotConfigured,
            message: "No broker".to_owned(),
            retryable: false,
            workflow_name: "test-wf".to_owned(),
            workflow_id: Some(Uuid::new_v4()),
        };
        let result = validate_start_retry(&error, "test-wf");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, WorkflowStartErrorCode::ValidationFailed);
        assert!(err.message.contains("ENQUEUE_FAILED"));
    }

    #[test]
    fn validate_retry_rejects_validation_failed() {
        let error = WorkflowStartError {
            code: WorkflowStartErrorCode::ValidationFailed,
            message: "Bad DAG".to_owned(),
            retryable: false,
            workflow_name: "test-wf".to_owned(),
            workflow_id: Some(Uuid::new_v4()),
        };
        let result = validate_start_retry(&error, "test-wf");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code,
            WorkflowStartErrorCode::ValidationFailed,
        );
    }

    #[test]
    fn validate_retry_rejects_workflow_id_conflict() {
        let error = WorkflowStartError {
            code: WorkflowStartErrorCode::WorkflowIdConflict,
            message: "id belongs to another workflow".to_owned(),
            retryable: false,
            workflow_name: "test-wf".to_owned(),
            workflow_id: Some(Uuid::new_v4()),
        };
        let result = validate_start_retry(&error, "test-wf");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code,
            WorkflowStartErrorCode::ValidationFailed,
        );
    }

    #[test]
    fn validate_retry_rejects_cross_workflow_name() {
        let error = WorkflowStartError {
            code: WorkflowStartErrorCode::EnqueueFailed,
            message: "DB connection lost".to_owned(),
            retryable: true,
            workflow_name: "other-workflow".to_owned(),
            workflow_id: Some(Uuid::new_v4()),
        };
        let result = validate_start_retry(&error, "my-workflow");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, WorkflowStartErrorCode::ValidationFailed);
        assert!(err.message.contains("cross-workflow"));
    }
}

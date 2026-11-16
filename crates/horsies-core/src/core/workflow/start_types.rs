/// Typed error types for workflow start operations.
///
/// Mirrors Python's `horsies/core/workflows/start_types.py`.
use uuid::Uuid;

/// Categorized workflow start failure codes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkflowStartErrorCode {
    /// Config error: no broker attached.
    BrokerNotConfigured,
    /// Programmer error: bad DAG, bad args, serialization failure.
    ValidationFailed,
    /// Infrastructure error: schema init, DB transaction. Conditionally retryable.
    EnqueueFailed,
    /// Bug / catch-all.
    InternalFailed,
}

impl std::fmt::Display for WorkflowStartErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BrokerNotConfigured => write!(f, "BROKER_NOT_CONFIGURED"),
            Self::ValidationFailed => write!(f, "VALIDATION_FAILED"),
            Self::EnqueueFailed => write!(f, "ENQUEUE_FAILED"),
            Self::InternalFailed => write!(f, "INTERNAL_FAILED"),
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
/// Only `EnqueueFailed` errors can be retried. The error's workflow_name
/// must match the spec being retried (prevents cross-workflow retry).
///
/// Returns `Ok(workflow_id)` if retry is valid, `Err(WorkflowStartError)`
/// with `ValidationFailed` code otherwise.
///
/// Mirrors Python's `WorkflowSpec._validate_start_retry(error)`.
pub fn validate_start_retry(
    error: &WorkflowStartError,
    spec_name: &str,
) -> Result<Uuid, WorkflowStartError> {
    if error.code != WorkflowStartErrorCode::EnqueueFailed {
        return Err(WorkflowStartError {
            code: WorkflowStartErrorCode::ValidationFailed,
            message: format!(
                "retry_start only accepts ENQUEUE_FAILED errors, got {}",
                error.code,
            ),
            retryable: false,
            workflow_name: spec_name.to_owned(),
            workflow_id: error.workflow_id.clone(),
        });
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
    fn validate_retry_rejects_internal_failed() {
        let error = WorkflowStartError {
            code: WorkflowStartErrorCode::InternalFailed,
            message: "Bug".to_owned(),
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

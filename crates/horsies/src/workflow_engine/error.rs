use crate::broker::BrokerError;
use crate::core::TaskError;

/// Errors produced by workflow engine operations.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowError {
    /// Database connection or query error.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Broker-level error (enqueue, claim, etc.).
    #[error("broker error: {0}")]
    Broker(#[from] BrokerError),

    /// JSON serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Workflow not found in the database.
    #[error("workflow not found: {workflow_id}")]
    WorkflowNotFound { workflow_id: String },

    /// Timeout waiting for workflow completion.
    #[error("timeout waiting for workflow result: {workflow_id}")]
    WorkflowTimeout { workflow_id: String },

    /// Task-level error propagated from a workflow task.
    #[error("workflow task error: {0}")]
    WorkflowError(TaskError),

    /// Unrecognized status string from the database.
    #[error("invalid status: {0}")]
    InvalidStatus(String),

    /// Workflow validation error (unresolved queue, invalid node config, etc.).
    #[error("workflow validation error: {0}")]
    Validation(String),
}

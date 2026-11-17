use crate::broker::BrokerError;
use uuid::Uuid;

use crate::workflow_engine::steps::StepContractError;

/// Errors produced by workflow engine operations.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowError {
    /// Database connection or query error.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Broker-level error (enqueue, claim, etc.), and the failures of the
    /// workflow steps on the core statement layer (`BrokerError::Statement`,
    /// `BrokerError::PoolSettings`).
    #[error("broker error: {0}")]
    Broker(#[from] BrokerError),

    /// JSON serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Workflow not found in the database.
    #[error("workflow not found: {workflow_id}")]
    WorkflowNotFound { workflow_id: Uuid },

    /// Unrecognized status string from the database.
    #[error("invalid status: {0}")]
    InvalidStatus(String),

    /// Workflow validation error (unresolved queue, invalid node config, etc.).
    #[error("workflow validation error: {0}")]
    Validation(String),

    /// A row of a workflow step function that breaks the outcome contract of
    /// its function. Not retryable: the same call meets it again.
    #[error("workflow step contract: {0}")]
    StepContract(#[from] StepContractError),
}

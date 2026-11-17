use crate::broker::BrokerError;
use uuid::Uuid;

use crate::workflow_engine::child_spec::ChildSpecUnavailable;
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

    /// The client's child spec source gave no answer for a child that a step
    /// builds; the step rolled back.
    #[error("child spec source: {0}")]
    ChildSpecSource(#[from] ChildSpecUnavailable),
}

impl WorkflowError {
    /// Whether the condition may clear, so the same call can succeed later. Pure.
    ///
    /// - `Database`: the core's SQLSTATE rule ([`crate::broker::is_retryable_sqlx_error`]).
    /// - `Broker`: the broker error's rule ([`BrokerError::is_retryable`]).
    /// - `ChildSpecSource`: a build that did not run, yes; an interrupted
    ///   build or one that raised, no ([`ChildSpecUnavailable::is_retryable`]).
    /// - Every other variant: no. The same call meets the same stored data,
    ///   the same refused argument or the same contract break again.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Database(error) => crate::broker::is_retryable_sqlx_error(error),
            Self::Broker(error) => error.is_retryable(),
            Self::ChildSpecSource(error) => error.is_retryable(),
            Self::Serialization(_)
            | Self::WorkflowNotFound { .. }
            | Self::InvalidStatus(_)
            | Self::Validation(_)
            | Self::StepContract(_) => false,
        }
    }
}

#[cfg(test)]
mod tests;

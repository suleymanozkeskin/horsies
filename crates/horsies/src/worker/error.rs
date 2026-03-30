use crate::broker::{is_retryable_sqlx_error, BrokerError};

/// Errors produced by worker operations.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// Broker or database error.
    #[error("broker error: {0}")]
    Broker(#[from] BrokerError),

    /// Direct database error from worker-owned queries.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Worker configuration error.
    #[error("configuration error: {0}")]
    Config(String),
}

impl WorkerError {
    /// Whether this error is transient and the operation can be retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Broker(e) => e.is_retryable(),
            Self::Database(e) => is_retryable_sqlx_error(e),
            Self::Config(_) => false,
        }
    }
}

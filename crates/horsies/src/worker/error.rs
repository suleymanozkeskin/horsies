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

    /// A worker-lifetime service loop (claimer heartbeat, reaper, workflow
    /// recovery, worker state) died without a shutdown request. Fail-fatal:
    /// a worker claiming with a dead service loop silently loses lease
    /// renewal, stale-task recovery, or liveness advertising (C11-inverse).
    #[error("service loop '{service}' {reason}; worker stopped")]
    ServiceLoopDied {
        service: &'static str,
        reason: String,
    },
}

impl WorkerError {
    /// Whether this error is transient and the operation can be retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Broker(e) => e.is_retryable(),
            Self::Database(e) => is_retryable_sqlx_error(e),
            Self::Config(_) => false,
            Self::ServiceLoopDied { .. } => false,
        }
    }
}

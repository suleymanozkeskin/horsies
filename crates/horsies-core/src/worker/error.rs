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

    /// Task-history coverage or partition contract error.
    #[error("task-history error: {0}")]
    History(#[from] crate::core::history::errors::HistoryError),

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

    /// One attempt of the worker's database start steps did not finish
    /// within `bound`. Retryable: the start runs again after a backoff.
    #[error("worker start attempt did not finish within {bound:?}")]
    StartTimedOut { bound: std::time::Duration },

    /// The runner heartbeats failed closed: no beat landed within half of
    /// `running_stale_threshold_ms`. The watchdog killed every child that
    /// held a task and stopped the worker (Python `RunnerHeartbeatLostError`).
    #[error("runner heartbeats failed closed: {cause}; worker stopped")]
    RunnerHeartbeatLost { cause: String },
}

impl WorkerError {
    /// Whether this error is transient and the operation can be retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Broker(e) => e.is_retryable(),
            Self::Database(e) => is_retryable_sqlx_error(e),
            Self::History(crate::core::history::errors::HistoryError::Database(e)) => {
                is_retryable_sqlx_error(e)
            }
            Self::History(_) => false,
            Self::Config(_) => false,
            Self::ServiceLoopDied { .. } => false,
            Self::StartTimedOut { .. } => true,
            Self::RunnerHeartbeatLost { .. } => false,
        }
    }
}

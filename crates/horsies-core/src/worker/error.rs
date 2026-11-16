use crate::broker::{is_retryable_sqlx_error, BrokerError};
use crate::worker::executor::placement::PlacementRefused;
use crate::worker::executor::slots::{BaselineOverLimit, ChildrenStartFailed, PoolFailure};
use crate::worker::executor::supervisor::SpawnerStartError;

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

    /// The profile's `parent_cpu = OWN_CORE` cannot be planned on this
    /// machine. The worker started no child.
    #[error("worker child start failed: {0}")]
    PlacementRefused(PlacementRefused),

    /// The spawner thread of the children could not start.
    #[error("worker child start failed: {0}")]
    SpawnerStart(SpawnerStartError),

    /// An `on_child_process_start` hook failed in a child at the start
    /// (Python `ChildHookFailedError`). Never retried: every new child runs
    /// the hook again.
    #[error("{}", crate::worker::worker::CHILD_HOOK_FAILURE_MESSAGE)]
    ChildHookFailed,

    /// The start of the children failed: no child could start, the threads
    /// of the worker could not be placed, or a first baseline is at or above
    /// its memory limit (Python `ChildStartFailedError`,
    /// `MemoryBaselineExceedsThresholdError`).
    #[error(transparent)]
    ChildrenStartFailed(ChildrenStartFailed),

    /// After the start, no replacement child can start (Python
    /// `ChildStartFailedError` from the slot failure loop). Running tasks
    /// drained first.
    #[error("worker child start failed: {0}")]
    PoolFailed(PoolFailure),

    /// The first child of a kind that was down at the first start has a
    /// baseline at or above the memory limit of its kind. Its tasks drained
    /// first.
    #[error("{0}")]
    BaselineOverLimit(BaselineOverLimit),
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
            Self::PlacementRefused(_)
            | Self::SpawnerStart(_)
            | Self::ChildHookFailed
            | Self::ChildrenStartFailed(_)
            | Self::PoolFailed(_)
            | Self::BaselineOverLimit(_) => false,
        }
    }
}

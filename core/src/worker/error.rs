use crate::broker::{BrokerError, ListenError, SessionCheckFailure};
use crate::worker::executor::placement::PlacementRefused;
use crate::worker::executor::slots::{BaselineOverLimit, ChildrenStartFailed, PoolFailure};
use crate::worker::executor::supervisor::SpawnerStartError;
use crate::worker::liveness::LivenessStartError;

/// Errors produced by worker operations.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// Broker or database error.
    #[error("broker error: {0}")]
    Broker(#[from] BrokerError),

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

    /// The last attempt of the worker start found the partition coverage
    /// gate held by another process, with no heartbeat leaf for the present
    /// instant, and the retry budget of the start
    /// (`WorkerResilienceConfig::database_retry_max_attempts`) was spent. `waited`
    /// is the time from the first attempt of the start to this error, and
    /// `attempts` counts the attempts. Retryable: the other process creates
    /// the leaves and releases the gate.
    #[error(
        "the partition coverage gate (coverage_gate_busy) is still held by another process, \
         and no heartbeat leaf covers the present instant, after the worker start waited {} ms \
         over {attempts} attempts",
        .waited.as_millis()
    )]
    CoverageGateBusy {
        waited: std::time::Duration,
        attempts: u64,
    },

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

    /// The preload process of an `app` preload profile refused, or did not
    /// report ready (Python `PreloadRefused`). The worker started no child.
    /// A `library` profile uses `exec` instead.
    #[error("worker child start failed: the preload parent refused: {reason}")]
    PreloadRefused { reason: String },

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

    /// The session check of the worker boot did not pass. A session URL that
    /// does not keep session state is not retryable; a database that cannot
    /// be reached is.
    #[error("session URL check failed: {0}")]
    SessionCheck(SessionCheckFailure),

    /// The broker's listener refused a claim wake or ping subscription of the
    /// worker. Retryable when the refusal is transient.
    #[error("listener subscription failed: {0}")]
    Listener(ListenError),

    /// The worker could not take its liveness lock at the start
    /// ([`crate::worker::liveness`]). Retryable when the failure can clear.
    #[error("worker liveness lock failed: {0}")]
    LivenessLock(LivenessStartError),
}

impl From<crate::broker::CoveragePassError> for WorkerError {
    fn from(error: crate::broker::CoveragePassError) -> Self {
        match error {
            crate::broker::CoveragePassError::Broker(error) => Self::Broker(error),
            crate::broker::CoveragePassError::History(error) => Self::History(error),
        }
    }
}

impl WorkerError {
    /// Whether this error is transient and the operation can be retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Broker(e) => e.is_retryable(),
            Self::History(error) => error.is_retryable(),
            Self::Config(_) => false,
            Self::ServiceLoopDied { .. } => false,
            Self::StartTimedOut { .. } => true,
            Self::CoverageGateBusy { .. } => true,
            Self::RunnerHeartbeatLost { .. } => false,
            Self::PlacementRefused(_)
            | Self::SpawnerStart(_)
            | Self::PreloadRefused { .. }
            | Self::ChildHookFailed
            | Self::ChildrenStartFailed(_)
            | Self::PoolFailed(_)
            | Self::BaselineOverLimit(_) => false,
            Self::SessionCheck(failure) => failure.is_retryable(),
            Self::Listener(refused) => refused.is_retryable(),
            Self::LivenessLock(failed) => failed.is_retryable(),
        }
    }
}

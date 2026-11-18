//! How a worker run ended, as a closed set for a client: the stop, a failed
//! start, or a failure after the start (Python's exit contract of `horsies
//! worker`: 0 only on an operator stop, else 1).
//!
//! [`WorkerError`] carries database errors and free text; a client needs
//! only the closed cause and the message to choose its exit code and its
//! log line. [`run_end_of`] maps every error to one cause. The message is
//! the error's text, with its subject and cause.

use crate::broker::{BrokerError, SessionCheckFailure};
use crate::worker::error::WorkerError;
use crate::worker::executor::slots::ChildrenStartFailed;
use crate::worker::liveness::LivenessStartError;

/// Where the run was when it ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunPhase {
    /// Before the claim loop: the children, the database start, the
    /// listener and the service loops.
    Starting,
    /// The worker was ready and claimed.
    Serving,
}

/// Why the start of a worker failed. The worker claimed nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerBootCause {
    /// The schema step refused the database's schema.
    SchemaRefused,
    /// The database belongs to the other client family.
    ClientFamilyRefused,
    /// The session URL does not keep a transaction (PgBouncer statement
    /// pooling).
    StatementPooledUrl,
    /// The session URL does not keep session state (LISTEN gets no NOTIFY).
    SessionNotCapable,
    /// One attempt of the database start did not end within its bound, and
    /// no retry was left.
    StartTimedOut,
    /// A database error that can clear, after the last retry of the start.
    DatabaseUnavailable,
    /// A database error that does not clear on a retry.
    DatabaseRefused,
    /// No child of the profile could start: the boot attempts of a slot ran
    /// out, or a slot repeated its defects.
    BootAttemptsExhausted,
    /// An `on_child_process_start` hook failed in a child.
    ChildHookFailed,
    /// The preload process of an `app` preload profile refused or did not
    /// start.
    PreloadRefused,
    /// A first child's memory baseline is at or above its limit.
    BaselineOverLimit,
    /// The CPU placement of the profile cannot be planned or applied.
    PlacementRefused,
    /// The spawner thread of the children did not start.
    SpawnerStartFailed,
    /// The core refused a setting of the worker (for example the partition
    /// coverage of the retention section, or the runner heartbeat pool).
    ConfigRefused,
    /// The listener refused a claim wake or ping subscription.
    ListenerRefused,
}

impl WorkerBootCause {
    /// The wire value at the client boundary.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SchemaRefused => "SCHEMA_REFUSED",
            Self::ClientFamilyRefused => "CLIENT_FAMILY_REFUSED",
            Self::StatementPooledUrl => "STATEMENT_POOLED_URL",
            Self::SessionNotCapable => "SESSION_NOT_CAPABLE",
            Self::StartTimedOut => "START_TIMED_OUT",
            Self::DatabaseUnavailable => "DATABASE_UNAVAILABLE",
            Self::DatabaseRefused => "DATABASE_REFUSED",
            Self::BootAttemptsExhausted => "BOOT_ATTEMPTS_EXHAUSTED",
            Self::ChildHookFailed => "CHILD_HOOK_FAILED",
            Self::PreloadRefused => "PRELOAD_REFUSED",
            Self::BaselineOverLimit => "BASELINE_OVER_LIMIT",
            Self::PlacementRefused => "PLACEMENT_REFUSED",
            Self::SpawnerStartFailed => "SPAWNER_START_FAILED",
            Self::ConfigRefused => "CONFIG_REFUSED",
            Self::ListenerRefused => "LISTENER_REFUSED",
        }
    }
}

/// Why a worker that was ready stopped. Its running tasks drained first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerFailCause {
    /// A service loop ended with no stop request.
    ServiceLoopDied,
    /// The runner heartbeats failed closed.
    RunnerHeartbeatLost,
    /// No replacement child can start.
    PoolFailed,
    /// The first child of a kind that was down at the start has a memory
    /// baseline at or above its limit.
    BaselineOverLimit,
    /// The claim loop met an error that ends the worker.
    ClaimFailed,
}

impl WorkerFailCause {
    /// The wire value at the client boundary.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ServiceLoopDied => "SERVICE_LOOP_DIED",
            Self::RunnerHeartbeatLost => "RUNNER_HEARTBEAT_LOST",
            Self::PoolFailed => "POOL_FAILED",
            Self::BaselineOverLimit => "BASELINE_OVER_LIMIT",
            Self::ClaimFailed => "CLAIM_FAILED",
        }
    }
}

/// How a worker run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerRunEnd {
    /// The run ended at a stop request (or a failed start hook in a
    /// replacement child, which stops the worker gracefully).
    Stopped,
    /// The start failed; `message` is the error with its subject and cause.
    BootFailed {
        cause: WorkerBootCause,
        message: String,
    },
    /// The worker was ready, then failed; `message` as above.
    Failed {
        cause: WorkerFailCause,
        message: String,
    },
}

/// The end of a run that returned `result` in `phase`. Pure.
pub fn run_end_of(result: Result<(), WorkerError>, phase: RunPhase) -> WorkerRunEnd {
    let error = match result {
        Ok(()) => return WorkerRunEnd::Stopped,
        Err(error) => error,
    };
    let message = error.to_string();
    let fail_cause = match (&error, phase) {
        (WorkerError::ServiceLoopDied { .. }, _) => Some(WorkerFailCause::ServiceLoopDied),
        (WorkerError::RunnerHeartbeatLost { .. }, _) => Some(WorkerFailCause::RunnerHeartbeatLost),
        (WorkerError::PoolFailed(_), _) => Some(WorkerFailCause::PoolFailed),
        (WorkerError::BaselineOverLimit(_), _) => Some(WorkerFailCause::BaselineOverLimit),
        (_, RunPhase::Serving) => Some(WorkerFailCause::ClaimFailed),
        (_, RunPhase::Starting) => None,
    };
    match fail_cause {
        Some(cause) => WorkerRunEnd::Failed { cause, message },
        None => WorkerRunEnd::BootFailed {
            cause: boot_cause(&error),
            message,
        },
    }
}

/// The cause of an error of the start. Pure.
fn boot_cause(error: &WorkerError) -> WorkerBootCause {
    match error {
        WorkerError::Broker(BrokerError::SchemaRefused(_)) => WorkerBootCause::SchemaRefused,
        WorkerError::Broker(BrokerError::ClientFamilyMismatch { .. }) => {
            WorkerBootCause::ClientFamilyRefused
        }
        WorkerError::Broker(_) | WorkerError::Database(_) | WorkerError::History(_) => {
            match error.is_retryable() {
                true => WorkerBootCause::DatabaseUnavailable,
                false => WorkerBootCause::DatabaseRefused,
            }
        }
        WorkerError::Config(_) => WorkerBootCause::ConfigRefused,
        WorkerError::StartTimedOut { .. } => WorkerBootCause::StartTimedOut,
        WorkerError::PlacementRefused(_) => WorkerBootCause::PlacementRefused,
        WorkerError::SpawnerStart(_) => WorkerBootCause::SpawnerStartFailed,
        WorkerError::PreloadRefused { .. } => WorkerBootCause::PreloadRefused,
        WorkerError::ChildHookFailed => WorkerBootCause::ChildHookFailed,
        WorkerError::ChildrenStartFailed(ChildrenStartFailed::Pool(_)) => {
            WorkerBootCause::BootAttemptsExhausted
        }
        WorkerError::ChildrenStartFailed(ChildrenStartFailed::WorkerNotPlaced(_)) => {
            WorkerBootCause::PlacementRefused
        }
        WorkerError::ChildrenStartFailed(ChildrenStartFailed::BaselineOverLimit(_))
        | WorkerError::BaselineOverLimit(_) => WorkerBootCause::BaselineOverLimit,
        WorkerError::SessionCheck(SessionCheckFailure::NotCapable { .. }) => {
            WorkerBootCause::SessionNotCapable
        }
        WorkerError::SessionCheck(failure) => match failure.is_retryable() {
            true => WorkerBootCause::DatabaseUnavailable,
            false => WorkerBootCause::StatementPooledUrl,
        },
        WorkerError::Listener(_) => WorkerBootCause::ListenerRefused,
        WorkerError::LivenessLock(LivenessStartError::Lock(_)) => match error.is_retryable() {
            true => WorkerBootCause::DatabaseUnavailable,
            false => WorkerBootCause::DatabaseRefused,
        },
        WorkerError::LivenessLock(
            LivenessStartError::Plan(_) | LivenessStartError::WatchThread(_),
        ) => WorkerBootCause::ConfigRefused,
        // `run_end_of` maps these before it asks for a boot cause.
        WorkerError::ServiceLoopDied { .. }
        | WorkerError::RunnerHeartbeatLost { .. }
        | WorkerError::PoolFailed(_) => {
            unreachable!("a failure after the start is not a boot cause: {error}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_ended_run_with_no_error_is_a_stop() {
        assert_eq!(run_end_of(Ok(()), RunPhase::Serving), WorkerRunEnd::Stopped);
        assert_eq!(
            run_end_of(Ok(()), RunPhase::Starting),
            WorkerRunEnd::Stopped
        );
    }

    #[test]
    fn an_error_before_the_claim_loop_is_a_boot_failure_with_its_cause() {
        let cases = [
            (
                WorkerError::StartTimedOut {
                    bound: std::time::Duration::from_secs(30),
                },
                WorkerBootCause::StartTimedOut,
            ),
            (
                WorkerError::ChildHookFailed,
                WorkerBootCause::ChildHookFailed,
            ),
            (
                WorkerError::PreloadRefused {
                    reason: "import failed".to_owned(),
                },
                WorkerBootCause::PreloadRefused,
            ),
            (
                WorkerError::Config("coverage refused".to_owned()),
                WorkerBootCause::ConfigRefused,
            ),
        ];
        for (error, cause) in cases {
            let message = error.to_string();
            assert_eq!(
                run_end_of(Err(error), RunPhase::Starting),
                WorkerRunEnd::BootFailed { cause, message }
            );
        }
    }

    #[test]
    fn an_error_of_the_claim_loop_after_the_start_is_a_claim_failure() {
        let error = WorkerError::Config("a claim setting".to_owned());
        let message = error.to_string();
        assert_eq!(
            run_end_of(Err(error), RunPhase::Serving),
            WorkerRunEnd::Failed {
                cause: WorkerFailCause::ClaimFailed,
                message,
            }
        );
    }

    #[test]
    fn a_failure_of_a_ready_worker_keeps_its_cause() {
        let cases = [
            (
                WorkerError::ServiceLoopDied {
                    service: "reaper",
                    reason: "returned".to_owned(),
                },
                WorkerFailCause::ServiceLoopDied,
            ),
            (
                WorkerError::RunnerHeartbeatLost {
                    cause: "no beat".to_owned(),
                },
                WorkerFailCause::RunnerHeartbeatLost,
            ),
        ];
        for (error, cause) in cases {
            let message = error.to_string();
            assert_eq!(
                run_end_of(Err(error), RunPhase::Serving),
                WorkerRunEnd::Failed { cause, message }
            );
        }
    }

    #[test]
    fn every_wire_value_is_distinct_screaming_snake_case() {
        let boot = [
            WorkerBootCause::SchemaRefused,
            WorkerBootCause::ClientFamilyRefused,
            WorkerBootCause::StatementPooledUrl,
            WorkerBootCause::SessionNotCapable,
            WorkerBootCause::StartTimedOut,
            WorkerBootCause::DatabaseUnavailable,
            WorkerBootCause::DatabaseRefused,
            WorkerBootCause::BootAttemptsExhausted,
            WorkerBootCause::ChildHookFailed,
            WorkerBootCause::PreloadRefused,
            WorkerBootCause::BaselineOverLimit,
            WorkerBootCause::PlacementRefused,
            WorkerBootCause::SpawnerStartFailed,
            WorkerBootCause::ConfigRefused,
            WorkerBootCause::ListenerRefused,
        ]
        .map(WorkerBootCause::as_str);
        let unique: std::collections::BTreeSet<&str> = boot.iter().copied().collect();
        assert_eq!(unique.len(), boot.len());
        for value in boot {
            assert!(value.chars().all(|c| c.is_ascii_uppercase() || c == '_'));
        }
    }
}

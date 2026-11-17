//! The start of a run in a worker's child through the core:
//! `horsies_start_attempt` (`PostgresBroker::start_attempt`).
//!
//! A Python child (isolated, shared async, or a seat of a shared thread
//! child) starts each run with one call on its `CoreChildHandle`. The call
//! carries the time left before the deadline of the run, when it has one:
//! the core checks it after the pool gave the connection, as Python's
//! `start_check`. The outcome is `AttemptStarted`, `AttemptMissed` (the
//! path after the miss stays with the caller), `DeadlineBeforeStart`, or
//! `StartFailed`.

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use horsies_core::core::lifecycle::OwnedClaim;
use horsies_core::{BrokerError, StartAttempt, StartOutcome, StartWindow, WindowedStart};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::handle::BrokerConnection;

/// The row is RUNNING with its first runner beat (and, for a workflow task,
/// its node RUNNING), from `started_at` on the database clock.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct AttemptStarted {
    pub task_id: Uuid,
    pub started_at: DateTime<Utc>,
}

/// The start wrote nothing. `reason` is the first reason that holds, in the
/// function's order: `DEADLINE_PASSED`, `NOT_HELD`, `LEASE_EXPIRED`,
/// `WORKFLOW_PAUSED`, `WORKFLOW_CANCELLED` or `NODE_NOT_LINKED` (Python
/// `StartMiss`). The path after the miss is the caller's.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct AttemptMissed {
    pub task_id: Uuid,
    pub reason: String,
}

/// The deadline of the run passed while the start waited for a pooled
/// connection. No statement ran: the row stays CLAIMED, and the timeout of
/// the worker returns it to the queue.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct DeadlineBeforeStart {
    pub task_id: Uuid,
}

/// The start failed: a failed connect, client family check or checkout
/// (nothing sent), a failed statement (it wrote nothing, or it committed
/// and its reply was lost, so the row may be RUNNING), or a row outside the
/// function's contract (not retryable). Python ends the run
/// `OWNERSHIP_UNCONFIRMED`.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StartFailed {
    pub task_id: Uuid,
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// The outcome of one start.
pub enum StartRecord {
    Started(AttemptStarted),
    Missed(AttemptMissed),
    DeadlineBefore(DeadlineBeforeStart),
    Failed(StartFailed),
}

impl<'py> IntoPyObject<'py> for StartRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Started(started) => started.into_bound_py_any(py),
            Self::Missed(missed) => missed.into_bound_py_any(py),
            Self::DeadlineBefore(passed) => passed.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The window of a start from the time left before the deadline of its run
/// at the call: `None` is a run with no deadline. Reads the monotonic
/// clock once.
pub fn start_window(deadline_in_ms: Option<u64>) -> StartWindow {
    match deadline_in_ms {
        Some(left_ms) => match Instant::now().checked_add(Duration::from_millis(left_ms)) {
            Some(deadline) => StartWindow::Until(deadline),
            // A deadline past the clock's range never passes.
            None => StartWindow::Open,
        },
        None => StartWindow::Open,
    }
}

/// The start command of one call. Pure.
pub fn start_command(
    task_id: Uuid,
    worker_id: String,
    claimed_at: DateTime<Utc>,
    worker_pid: i32,
    worker_hostname: String,
) -> StartAttempt {
    StartAttempt::of_process(
        task_id,
        OwnedClaim {
            worker_id,
            claimed_at,
        },
        worker_pid,
        worker_hostname,
    )
}

fn failed(task_id: Uuid, error: &BrokerError) -> StartRecord {
    StartRecord::Failed(StartFailed {
        task_id,
        message: error.to_string(),
        retryable: error.is_retryable(),
    })
}

/// The record of a start. Pure.
fn start_record(task_id: Uuid, start: Result<WindowedStart, BrokerError>) -> StartRecord {
    match start {
        Ok(WindowedStart::Ran(StartOutcome::Started { started_at })) => {
            StartRecord::Started(AttemptStarted {
                task_id,
                started_at,
            })
        }
        Ok(WindowedStart::Ran(StartOutcome::Missed(miss))) => StartRecord::Missed(AttemptMissed {
            task_id,
            reason: miss.as_str().to_owned(),
        }),
        Ok(WindowedStart::WindowClosed) => {
            StartRecord::DeadlineBefore(DeadlineBeforeStart { task_id })
        }
        Err(error) => failed(task_id, &error),
    }
}

/// Start one claimed task. Writes, in one statement; a caller that leaves
/// early does not stop it, and the row may then be RUNNING.
pub async fn start_attempt_operation(
    connection: BrokerConnection,
    command: StartAttempt,
    window: StartWindow,
) -> StartRecord {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return failed(command.task_id, &error),
    };
    start_record(
        command.task_id,
        broker.start_attempt(&command, window).await,
    )
}

#[cfg(test)]
mod tests {
    use horsies_core::StartMiss;

    use super::*;

    fn task() -> Uuid {
        Uuid::from_u128(7)
    }

    #[test]
    fn each_start_outcome_gives_its_record() {
        let started_at = Utc::now();
        assert!(matches!(
            start_record(
                task(),
                Ok(WindowedStart::Ran(StartOutcome::Started { started_at }))
            ),
            StartRecord::Started(AttemptStarted { started_at: at, .. }) if at == started_at
        ));
        let StartRecord::Missed(missed) = start_record(
            task(),
            Ok(WindowedStart::Ran(StartOutcome::Missed(
                StartMiss::LeaseExpired,
            ))),
        ) else {
            panic!("a miss gives AttemptMissed");
        };
        assert_eq!(missed.reason, "LEASE_EXPIRED");
        assert!(matches!(
            start_record(task(), Ok(WindowedStart::WindowClosed)),
            StartRecord::DeadlineBefore(DeadlineBeforeStart { task_id }) if task_id == task()
        ));
    }

    #[test]
    fn a_failed_start_keeps_its_cause_and_retry_rule() {
        let StartRecord::Failed(failed) = start_record(
            task(),
            Err(BrokerError::ClaimContract(
                "horsies_start_attempt returned 0 rows".to_owned(),
            )),
        ) else {
            panic!("a failed start gives StartFailed");
        };
        assert!(failed.message.contains("0 rows"), "{}", failed.message);
        assert!(!failed.retryable);
    }

    #[test]
    fn a_run_with_no_deadline_has_an_open_window() {
        assert_eq!(start_window(None), StartWindow::Open);
        assert!(matches!(start_window(Some(1_000)), StartWindow::Until(_)));
        // `u64::MAX` ms is about 1.8e16 s. Where `Instant` holds that much
        // (an `i64` of seconds on macOS and Linux) the window ends that far
        // ahead; where the sum overflows, the window is open. In both cases
        // no run sees it close.
        let longer_than_any_run = Duration::from_secs(1_000 * 365 * 24 * 3_600);
        match start_window(Some(u64::MAX)) {
            StartWindow::Open => {}
            StartWindow::Until(deadline) => {
                assert!(deadline > Instant::now() + longer_than_any_run);
            }
        }
    }
}

//! The stop of a claimed workflow task whose start missed, through the core:
//! `horsies_workflow_task_stop_before_start`
//! (`PostgresBroker::stop_workflow_task_before_start`).
//!
//! A Python child sends it after `start_attempt` missed for a workflow
//! reason, with the orphan self-heal of the run. The function judges the
//! task again under its own locks. The outcome is `StoppedBeforeStart` (the
//! step that ran and the outcome of its move), `ResumedBeforeStart` (a
//! resume came first: the caller runs the start once more), or
//! `StopBeforeStartFailed`.

use chrono::{DateTime, Utc};
use horsies_core::core::lifecycle::OwnedClaim;
use horsies_core::{BrokerError, OrphanSelfHeal, StopBeforeStart, StopMove};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::handle::BrokerConnection;

/// The stop ran a step that ends the start of this claim: `outcome` is the
/// step (`EXPIRED`, `ABANDONED`, `CANCELLED`, `ORPHAN_CANCELLED`,
/// `NODE_NOT_LINKED`, `NOT_HELD`) and `move_outcome` the outcome of its move
/// (`APPLIED`, `ALREADY_APPLIED`, `LOST_CLAIM`, `SOURCE_STATE_CONFLICT`,
/// `TASK_ABSENT`), or `None` for `NODE_NOT_LINKED`, which writes nothing.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StoppedBeforeStart {
    pub task_id: Uuid,
    pub outcome: &'static str,
    pub move_outcome: Option<&'static str>,
}

/// A resume came first: the claim is still startable and a node links it
/// live in a RUNNING workflow. Nothing is written; the caller runs the start
/// once more.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ResumedBeforeStart {
    pub task_id: Uuid,
}

/// The stop failed: a failed connect, client family check or checkout
/// (nothing sent), a failed statement (it wrote nothing, or it committed and
/// its reply was lost: the same stop sent again reports a committed move as
/// `ALREADY_APPLIED`, or the row as not held), or a row outside the
/// function's contract (not retryable).
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StopBeforeStartFailed {
    pub task_id: Uuid,
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// The outcome of one stop.
pub enum StopRecord {
    Stopped(StoppedBeforeStart),
    Resumed(ResumedBeforeStart),
    Failed(StopBeforeStartFailed),
}

impl<'py> IntoPyObject<'py> for StopRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Stopped(stopped) => stopped.into_bound_py_any(py),
            Self::Resumed(resumed) => resumed.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The claim fence of one stop. Pure.
pub fn stop_fence(worker_id: String, claimed_at: DateTime<Utc>) -> OwnedClaim {
    OwnedClaim {
        worker_id,
        claimed_at,
    }
}

/// The orphan self-heal of the `orphan_self_heal` argument. Pure.
pub fn self_heal_of(orphan_self_heal: bool) -> OrphanSelfHeal {
    OrphanSelfHeal::of_recovery_flag(orphan_self_heal)
}

fn failed(task_id: Uuid, error: &BrokerError) -> StopRecord {
    StopRecord::Failed(StopBeforeStartFailed {
        task_id,
        message: error.to_string(),
        retryable: error.is_retryable(),
    })
}

/// The record of a stop. Pure.
fn stop_record(task_id: Uuid, stop: Result<StopBeforeStart, BrokerError>) -> StopRecord {
    match stop {
        Ok(StopBeforeStart::Resumed) => StopRecord::Resumed(ResumedBeforeStart { task_id }),
        Ok(stopped) => StopRecord::Stopped(StoppedBeforeStart {
            task_id,
            outcome: stopped.outcome_str(),
            move_outcome: stopped.move_outcome().map(StopMove::as_str),
        }),
        Err(error) => failed(task_id, &error),
    }
}

/// Stop one claimed workflow task whose start missed. Writes, in one
/// statement; a caller that leaves early does not stop it, and sees its
/// outcome as unknown.
pub async fn stop_before_start_operation(
    connection: BrokerConnection,
    task_id: Uuid,
    fence: OwnedClaim,
    orphan_self_heal: OrphanSelfHeal,
) -> StopRecord {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return failed(task_id, &error),
    };
    stop_record(
        task_id,
        broker
            .stop_workflow_task_before_start(task_id, &fence, orphan_self_heal)
            .await,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task() -> Uuid {
        Uuid::from_u128(11)
    }

    #[test]
    fn each_step_gives_its_record() {
        let StopRecord::Stopped(stopped) = stop_record(
            task(),
            Ok(StopBeforeStart::OrphanCancelled(StopMove::Applied)),
        ) else {
            panic!("an orphan cancel gives StoppedBeforeStart");
        };
        assert_eq!(stopped.task_id, task());
        assert_eq!(stopped.outcome, "ORPHAN_CANCELLED");
        assert_eq!(stopped.move_outcome, Some("APPLIED"));

        let StopRecord::Stopped(not_linked) =
            stop_record(task(), Ok(StopBeforeStart::NodeNotLinked))
        else {
            panic!("a node that is not linked gives StoppedBeforeStart");
        };
        assert_eq!(not_linked.outcome, "NODE_NOT_LINKED");
        assert_eq!(not_linked.move_outcome, None);

        assert!(matches!(
            stop_record(task(), Ok(StopBeforeStart::Resumed)),
            StopRecord::Resumed(ResumedBeforeStart { task_id }) if task_id == task()
        ));
    }

    #[test]
    fn a_failed_stop_keeps_its_cause_and_retry_rule() {
        let StopRecord::Failed(failed) = stop_record(
            task(),
            Err(BrokerError::TerminalizationContract(
                "horsies_workflow_task_stop_before_start returned 0 rows".to_owned(),
            )),
        ) else {
            panic!("a failed stop gives StopBeforeStartFailed");
        };
        assert_eq!(failed.task_id, task());
        assert!(failed.message.contains("0 rows"), "{}", failed.message);
        assert!(!failed.retryable);
    }

    #[test]
    fn the_argument_gives_the_self_heal() {
        assert_eq!(self_heal_of(true), OrphanSelfHeal::On);
        assert_eq!(self_heal_of(false), OrphanSelfHeal::Off);
    }
}

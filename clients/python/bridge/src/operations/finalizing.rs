//! The finalizing marker of a run in a worker's child through the core:
//! `horsies_mark_finalizing` (`PostgresBroker::mark_finalizing`).
//!
//! A Python child writes the marker once the body of a run that set its row
//! RUNNING has ended, before the parent finalizes the attempt. The marker is
//! best effort: Python logs a skipped or failed write and goes on. The
//! outcome is `FinalizingMarked`, `FinalizingNotHeld` or
//! `MarkFinalizingFailed`.

use chrono::{DateTime, Utc};
use horsies_core::core::lifecycle::OwnedClaim;
use horsies_core::{BrokerError, FinalizingMark};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::handle::BrokerConnection;

/// The marker is written: the row is RUNNING under this worker's claim
/// generation.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct FinalizingMarked {
    pub task_id: Uuid,
}

/// The marker wrote nothing: the row is absent, not RUNNING, or held by
/// another worker or another claim generation (Python logs "the claim
/// changed").
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct FinalizingNotHeld {
    pub task_id: Uuid,
}

/// The marker failed: a failed connect, client family check or checkout
/// (nothing sent), a failed statement (it wrote nothing, or it committed and
/// its reply was lost, so the marker may be written), or a row outside the
/// function's contract (not retryable).
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct MarkFinalizingFailed {
    pub task_id: Uuid,
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// The outcome of one marker.
pub enum FinalizingRecord {
    Marked(FinalizingMarked),
    NotHeld(FinalizingNotHeld),
    Failed(MarkFinalizingFailed),
}

impl<'py> IntoPyObject<'py> for FinalizingRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Marked(marked) => marked.into_bound_py_any(py),
            Self::NotHeld(not_held) => not_held.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The claim fence of one marker. Pure.
pub fn marker_fence(worker_id: String, claimed_at: DateTime<Utc>) -> OwnedClaim {
    OwnedClaim {
        worker_id,
        claimed_at,
    }
}

fn failed(task_id: Uuid, error: &BrokerError) -> FinalizingRecord {
    FinalizingRecord::Failed(MarkFinalizingFailed {
        task_id,
        message: error.to_string(),
        retryable: error.is_retryable(),
    })
}

/// The record of a marker. Pure.
fn finalizing_record(
    task_id: Uuid,
    marked: Result<FinalizingMark, BrokerError>,
) -> FinalizingRecord {
    match marked {
        Ok(FinalizingMark::Marked) => FinalizingRecord::Marked(FinalizingMarked { task_id }),
        Ok(FinalizingMark::NotHeld) => FinalizingRecord::NotHeld(FinalizingNotHeld { task_id }),
        Err(error) => failed(task_id, &error),
    }
}

/// Write the finalizing marker of one run. Writes, in one statement; a
/// caller that leaves early does not stop it, and the marker may then be
/// written.
pub async fn mark_finalizing_operation(
    connection: BrokerConnection,
    task_id: Uuid,
    fence: OwnedClaim,
) -> FinalizingRecord {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return failed(task_id, &error),
    };
    finalizing_record(task_id, broker.mark_finalizing(task_id, &fence).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task() -> Uuid {
        Uuid::from_u128(7)
    }

    #[test]
    fn each_marker_outcome_gives_its_record() {
        assert!(matches!(
            finalizing_record(task(), Ok(FinalizingMark::Marked)),
            FinalizingRecord::Marked(FinalizingMarked { task_id }) if task_id == task()
        ));
        assert!(matches!(
            finalizing_record(task(), Ok(FinalizingMark::NotHeld)),
            FinalizingRecord::NotHeld(FinalizingNotHeld { task_id }) if task_id == task()
        ));
    }

    #[test]
    fn a_failed_marker_keeps_its_cause_and_retry_rule() {
        let FinalizingRecord::Failed(failed) = finalizing_record(
            task(),
            Err(BrokerError::ClaimContract(
                "horsies_mark_finalizing returned 0 rows".to_owned(),
            )),
        ) else {
            panic!("a failed marker gives MarkFinalizingFailed");
        };
        assert_eq!(failed.task_id, task());
        assert!(failed.message.contains("0 rows"), "{}", failed.message);
        assert!(!failed.retryable);
    }
}

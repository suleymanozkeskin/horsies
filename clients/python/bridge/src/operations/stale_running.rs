//! The stale RUNNING recovery pass through the core
//! (`PostgresBroker::mark_stale_running_as_failed`): one
//! `horsies_stale_running_scan`, then one `horsies_finish_stale_attempt` per
//! candidate.
//!
//! The outcome is one class per way the pass ended: `StaleRunningRecovered`,
//! `StaleRunningScanFull` (the scan read a full batch; stale rows can remain
//! for the next pass), or `StaleRunningScanFailed`. The Python client maps
//! them to the result of `PostgresBroker.mark_stale_tasks_as_failed`
//! (`CLEANUP_FAILED` for a failure). A candidate whose statement fails is
//! logged by the core and skipped, as Python does.

use horsies_core::{BrokerError, FailureCause, StaleRunningPass, StaleRunningThresholds};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use super::arguments::threshold_ms;
use crate::core_failures::FailureException;
use crate::handle::BrokerConnection;

/// The pass ended: the scan read fewer candidates than its batch.
/// `recovered` counts the candidates whose crash retry or failure
/// committed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StaleRunningRecovered {
    pub recovered: u64,
}

/// The pass ended, and its scan read a full batch of `scan_batch`
/// candidates: stale rows can remain for the next pass. `recovered` counts
/// the candidates whose crash retry or failure committed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StaleRunningScanFull {
    pub recovered: u64,
    pub scan_batch: u32,
}

/// The pass did not run: a threshold outside 0 to 2 147 483 647 ms (not
/// retryable), a failed connect, a refused or failed client family check,
/// or a failed scan. Nothing was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StaleRunningScanFailed {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
    /// The cause, with the subject `mark_stale_running`.
    pub exception: FailureException,
}

/// The subject of the exception of a failed pass.
const SUBJECT: &str = "mark_stale_running";

impl StaleRunningScanFailed {
    fn from_broker_error(error: &BrokerError) -> Self {
        Self {
            message: error.to_string(),
            retryable: error.is_retryable(),
            exception: FailureException::of(SUBJECT, error),
        }
    }
}

/// The outcome of one stale RUNNING pass.
pub enum StaleRunningOutcome {
    Recovered(StaleRunningRecovered),
    ScanFull(StaleRunningScanFull),
    Failed(StaleRunningScanFailed),
}

impl<'py> IntoPyObject<'py> for StaleRunningOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Recovered(recovered) => recovered.into_bound_py_any(py),
            Self::ScanFull(full) => full.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The thresholds of a pass, or the refusal as text. Pure.
fn stale_running_thresholds(
    stale_after_ms: i64,
    finalizing_stale_after_ms: i64,
) -> Result<StaleRunningThresholds, String> {
    Ok(StaleRunningThresholds {
        stale_after: threshold_ms("stale threshold", stale_after_ms)?,
        finalizing_stale_after: threshold_ms(
            "finalizing stale threshold",
            finalizing_stale_after_ms,
        )?,
    })
}

/// The outcome of a pass. Pure.
fn pass_outcome(pass: Result<StaleRunningPass, BrokerError>) -> StaleRunningOutcome {
    match pass {
        Ok(StaleRunningPass::Complete { recovered }) => {
            StaleRunningOutcome::Recovered(StaleRunningRecovered { recovered })
        }
        Ok(StaleRunningPass::ScanFull {
            recovered,
            scan_batch,
        }) => StaleRunningOutcome::ScanFull(StaleRunningScanFull {
            recovered,
            scan_batch,
        }),
        Err(error) => {
            StaleRunningOutcome::Failed(StaleRunningScanFailed::from_broker_error(&error))
        }
    }
}

/// Run one stale RUNNING pass with the two thresholds. Writes; a caller
/// that leaves early does not stop the pass, and sees its outcome as
/// unknown.
pub async fn mark_stale_running_operation(
    connection: BrokerConnection,
    stale_after_ms: i64,
    finalizing_stale_after_ms: i64,
) -> StaleRunningOutcome {
    let thresholds = match stale_running_thresholds(stale_after_ms, finalizing_stale_after_ms) {
        Ok(thresholds) => thresholds,
        Err(message) => {
            return StaleRunningOutcome::Failed(StaleRunningScanFailed {
                exception: FailureException::new(
                    SUBJECT,
                    message.clone(),
                    false,
                    FailureCause::PreconditionFailed,
                ),
                message,
                retryable: false,
            })
        }
    };
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return StaleRunningOutcome::Failed(StaleRunningScanFailed::from_broker_error(&error))
        }
    };
    pass_outcome(broker.mark_stale_running_as_failed(thresholds).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_thresholds_are_kept_and_each_refusal_names_its_threshold() {
        let thresholds =
            stale_running_thresholds(300_000, 60_000).expect("two thresholds in range");
        assert_eq!(thresholds.stale_after.get(), 300_000);
        assert_eq!(thresholds.finalizing_stale_after.get(), 60_000);

        let negative = stale_running_thresholds(-1, 60_000).expect_err("a negative threshold");
        assert!(
            negative.contains("the stale threshold of -1 ms"),
            "{negative}"
        );
        let above = stale_running_thresholds(300_000, i64::from(i32::MAX) + 1)
            .expect_err("above the integer limit");
        assert!(above.contains("finalizing stale threshold"), "{above}");
        assert!(above.contains("2147483648 ms"), "{above}");
    }

    #[test]
    fn each_end_of_a_pass_has_its_own_record() {
        assert!(matches!(
            pass_outcome(Ok(StaleRunningPass::Complete { recovered: 2 })),
            StaleRunningOutcome::Recovered(StaleRunningRecovered { recovered: 2 })
        ));
        assert!(matches!(
            pass_outcome(Ok(StaleRunningPass::ScanFull {
                recovered: 900,
                scan_batch: 1_000
            })),
            StaleRunningOutcome::ScanFull(StaleRunningScanFull {
                recovered: 900,
                scan_batch: 1_000
            })
        ));
    }

    #[test]
    fn a_failed_scan_keeps_its_cause_and_retry_rule() {
        let StaleRunningOutcome::Failed(failed) = pass_outcome(Err(BrokerError::ClaimContract(
            "planned failure".to_owned(),
        ))) else {
            panic!("a failed scan gives StaleRunningScanFailed");
        };
        assert!(
            failed.message.contains("planned failure"),
            "{}",
            failed.message
        );
        assert!(!failed.retryable);
    }
}

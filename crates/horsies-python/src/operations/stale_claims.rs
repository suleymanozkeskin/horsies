//! The stale CLAIMED requeue pass through the core: `horsies_requeue_stale_claims`
//! in bounded batches (`PostgresBroker::requeue_stale_claimed`).
//!
//! The outcome is one class per way the pass ended: `StaleClaimsDrained`,
//! `StaleClaimsBoundReached` (the pass stopped at its batch bound; rows can
//! remain), or `StaleClaimsRequeueFailed` with the rows that the batches
//! before the failure requeued. The Python client maps them to the result of
//! `PostgresBroker.requeue_stale_claimed` (`CLEANUP_FAILED` for a failure).

use horsies_core::{
    FailureCause, StaleClaimsRequeueFailed as PassFailed, StaleClaimsRequeuePass, ThresholdMs,
};

use super::arguments::threshold_ms;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::core_failures::FailureException;
use crate::handle::BrokerConnection;

/// The pass ended at a short batch: no eligible row was left. Every batch
/// committed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StaleClaimsDrained {
    pub requeued: u64,
}

/// The pass ran its bound of full batches and stopped; eligible rows can
/// remain for the next pass. Every batch committed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StaleClaimsBoundReached {
    pub requeued: u64,
}

/// The pass stopped at a failure. The batches before it committed and
/// requeued `requeued_before` rows; the failed batch wrote nothing, or its
/// outcome is unknown. A threshold outside 0 to 2 147 483 647 ms, a failed
/// connect and a refused client family end the pass before any batch, with
/// `requeued_before` 0.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StaleClaimsRequeueFailed {
    pub requeued_before: u64,
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
    /// The cause, with the subject `requeue_stale_claimed`.
    pub exception: FailureException,
}

/// The subject of the exception of a failed pass.
const SUBJECT: &str = "requeue_stale_claimed";

impl From<PassFailed> for StaleClaimsRequeueFailed {
    fn from(failed: PassFailed) -> Self {
        Self {
            requeued_before: failed.requeued_before,
            message: failed.error.to_string(),
            retryable: failed.error.is_retryable(),
            exception: FailureException::of(SUBJECT, &failed.error),
        }
    }
}

/// The outcome of one requeue pass.
pub enum RequeueOutcome {
    Drained(StaleClaimsDrained),
    BoundReached(StaleClaimsBoundReached),
    Failed(StaleClaimsRequeueFailed),
}

impl<'py> IntoPyObject<'py> for RequeueOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Drained(drained) => drained.into_bound_py_any(py),
            Self::BoundReached(bounded) => bounded.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The threshold of a pass, or the refusal as text. Pure.
fn stale_threshold(value_ms: i64) -> Result<ThresholdMs, String> {
    threshold_ms("stale threshold", value_ms)
}

/// The record of a pass whose threshold was refused: nothing ran, and the
/// same threshold is refused again.
fn refused_threshold(message: String) -> StaleClaimsRequeueFailed {
    StaleClaimsRequeueFailed {
        requeued_before: 0,
        exception: FailureException::new(
            SUBJECT,
            message.clone(),
            false,
            FailureCause::PreconditionFailed,
        ),
        message,
        retryable: false,
    }
}

/// The outcome of a pass. Pure.
fn pass_outcome(pass: Result<StaleClaimsRequeuePass, PassFailed>) -> RequeueOutcome {
    match pass {
        Ok(StaleClaimsRequeuePass::Drained { requeued }) => {
            RequeueOutcome::Drained(StaleClaimsDrained { requeued })
        }
        Ok(StaleClaimsRequeuePass::BoundReached { requeued }) => {
            RequeueOutcome::BoundReached(StaleClaimsBoundReached { requeued })
        }
        Err(failed) => RequeueOutcome::Failed(StaleClaimsRequeueFailed::from(failed)),
    }
}

/// Run one stale CLAIMED requeue pass with the threshold `stale_after_ms`.
/// Writes; a caller that leaves early does not stop the pass, and sees its
/// outcome as unknown.
pub async fn requeue_stale_claimed_operation(
    connection: BrokerConnection,
    stale_after_ms: i64,
) -> RequeueOutcome {
    let stale_after = match stale_threshold(stale_after_ms) {
        Ok(stale_after) => stale_after,
        Err(message) => return RequeueOutcome::Failed(refused_threshold(message)),
    };
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return RequeueOutcome::Failed(StaleClaimsRequeueFailed::from(PassFailed {
                requeued_before: 0,
                error,
            }))
        }
    };
    pass_outcome(broker.requeue_stale_claimed(stale_after).await)
}

#[cfg(test)]
mod tests {
    use horsies_core::BrokerError;

    use super::*;

    #[test]
    fn a_threshold_from_zero_to_the_integer_limit_is_kept() {
        assert_eq!(stale_threshold(0).map(ThresholdMs::get), Ok(0));
        assert_eq!(
            stale_threshold(i64::from(i32::MAX)).map(ThresholdMs::get),
            Ok(i32::MAX)
        );
    }

    #[test]
    fn a_negative_threshold_or_one_above_the_integer_limit_is_refused() {
        let negative = stale_threshold(-1).expect_err("a negative threshold");
        assert!(negative.contains("-1 ms"), "{negative}");
        let above = stale_threshold(i64::from(i32::MAX) + 1).expect_err("above the limit");
        assert!(above.contains("2147483648 ms"), "{above}");
    }

    #[test]
    fn each_end_of_a_pass_has_its_own_record() {
        assert!(matches!(
            pass_outcome(Ok(StaleClaimsRequeuePass::Drained { requeued: 3 })),
            RequeueOutcome::Drained(StaleClaimsDrained { requeued: 3 })
        ));
        assert!(matches!(
            pass_outcome(Ok(StaleClaimsRequeuePass::BoundReached {
                requeued: 100_000
            })),
            RequeueOutcome::BoundReached(StaleClaimsBoundReached { requeued: 100_000 })
        ));
    }

    #[test]
    fn a_failure_keeps_the_rows_requeued_before_it_and_its_cause() {
        let outcome = pass_outcome(Err(PassFailed {
            requeued_before: 500,
            error: BrokerError::ClaimContract("planned failure".to_owned()),
        }));
        let RequeueOutcome::Failed(failed) = outcome else {
            panic!("a failed pass gives the failure record");
        };
        assert_eq!(failed.requeued_before, 500);
        assert!(
            failed.message.contains("planned failure"),
            "{}",
            failed.message
        );
        assert!(!failed.retryable);
    }

    #[test]
    fn a_refused_threshold_is_not_retryable_and_ran_nothing() {
        let failed = refused_threshold("refused".to_owned());
        assert_eq!(failed.requeued_before, 0);
        assert!(!failed.retryable);
    }
}

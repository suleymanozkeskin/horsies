//! The paused workflow expiry of the reaper through the core
//! (`PostgresBroker::expire_paused_workflows`): one read of the candidates,
//! then one `horsies_workflow_expire_paused` per candidate.
//!
//! The outcome is `PausedExpired` or `PausedExpiryFailed`. The Python reaper
//! logs a failure as its paused-workflow expiry error, as it logs the
//! exception of its own statements today; the next pass runs again.

use std::num::NonZeroU32;

use horsies_core::{FailureCause, PausedWorkflowAge, WorkflowError};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::core_failures::FailureException;
use crate::handle::BrokerConnection;

/// The call ended. `expired` counts the workflows that it expired.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct PausedExpired {
    pub expired: u64,
}

/// The call failed: an age or batch size that the core refuses (not
/// retryable), a failed connect, a refused or failed client family check,
/// the candidate read, or a candidate's statement. The candidates before a
/// failed one are expired; a statement whose reply was lost may have
/// committed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct PausedExpiryFailed {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
    /// The cause, with the subject `expire_paused_workflows`.
    pub exception: FailureException,
}

/// The subject of the exception of a failed call.
const SUBJECT: &str = "expire_paused_workflows";

/// The outcome of one paused expiry call.
pub enum PausedExpiryOutcome {
    Expired(PausedExpired),
    Failed(PausedExpiryFailed),
}

impl<'py> IntoPyObject<'py> for PausedExpiryOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Expired(expired) => expired.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The age and batch size of a call, or the refusal as text. Pure.
fn expiry_arguments(
    older_than_us: i64,
    batch_size: u32,
) -> Result<(PausedWorkflowAge, NonZeroU32), String> {
    let age = PausedWorkflowAge::parse(chrono::Duration::microseconds(older_than_us))
        .map_err(|refused| format!("the paused expiry age is refused: {refused}"))?;
    let batch_size = NonZeroU32::new(batch_size)
        .ok_or_else(|| "the paused expiry needs a batch size of 1 or more, not 0".to_owned())?;
    Ok((age, batch_size))
}

/// The outcome of a call. Pure.
fn expiry_outcome(answer: Result<u64, WorkflowError>) -> PausedExpiryOutcome {
    match answer {
        Ok(expired) => PausedExpiryOutcome::Expired(PausedExpired { expired }),
        Err(error) => PausedExpiryOutcome::Failed(PausedExpiryFailed {
            message: format!("paused workflow expiry failed: {error}"),
            retryable: error.is_retryable(),
            exception: FailureException::of(SUBJECT, &error),
        }),
    }
}

/// Expire the PAUSED workflows older than `older_than_us` microseconds, at
/// most `batch_size`. Writes; each candidate commits alone, and a caller
/// that leaves early does not stop the call.
pub async fn expire_paused_workflows_operation(
    connection: BrokerConnection,
    older_than_us: i64,
    batch_size: u32,
) -> PausedExpiryOutcome {
    let (age, batch_size) = match expiry_arguments(older_than_us, batch_size) {
        Ok(arguments) => arguments,
        Err(message) => {
            return PausedExpiryOutcome::Failed(PausedExpiryFailed {
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
            return PausedExpiryOutcome::Failed(PausedExpiryFailed {
                message: format!("paused workflow expiry failed: {error}"),
                retryable: error.is_retryable(),
                exception: FailureException::of(SUBJECT, &error),
            })
        }
    };
    expiry_outcome(broker.expire_paused_workflows(age, batch_size).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_age_and_batch_are_kept_and_each_refusal_is_named() {
        let (age, batch) = expiry_arguments(3_600_000_000, 50).expect("an hour, 50 rows");
        assert_eq!(age.micros(), 3_600_000_000);
        assert_eq!(batch.get(), 50);

        let zero_age = expiry_arguments(0, 50).expect_err("an age of zero");
        assert!(zero_age.contains("age is refused"), "{zero_age}");
        let negative = expiry_arguments(-1, 50).expect_err("a negative age");
        assert!(negative.contains("age is refused"), "{negative}");
        let (longest, _) =
            expiry_arguments(PausedWorkflowAge::MAX_MICROS, 50).expect("the longest age");
        assert_eq!(longest.micros(), PausedWorkflowAge::MAX_MICROS);
        let too_long = expiry_arguments(PausedWorkflowAge::MAX_MICROS + 1, 50)
            .expect_err("an age above the bound");
        assert!(too_long.contains("age is refused"), "{too_long}");
        assert!(too_long.contains("longer than"), "{too_long}");
        let zero_batch = expiry_arguments(1, 0).expect_err("a batch of zero");
        assert!(zero_batch.contains("batch size"), "{zero_batch}");
    }

    #[test]
    fn an_ended_call_counts_its_workflows() {
        assert!(matches!(
            expiry_outcome(Ok(3)),
            PausedExpiryOutcome::Expired(PausedExpired { expired: 3 })
        ));
    }

    #[test]
    fn a_failure_keeps_its_cause_and_retry_rule() {
        let PausedExpiryOutcome::Failed(failed) =
            expiry_outcome(Err(WorkflowError::Validation("planned refusal".to_owned())))
        else {
            panic!("a failure gives PausedExpiryFailed");
        };
        assert!(
            failed.message.contains("planned refusal"),
            "{}",
            failed.message
        );
        assert!(!failed.retryable);
    }
}

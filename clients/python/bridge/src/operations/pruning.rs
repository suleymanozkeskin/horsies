//! The pruning pass through the core (`PostgresBroker::prune_expired_partitions`):
//! one candidate read (`horsies_prune_candidates`), the finalization of each
//! interrupted detach, then the heartbeat and the period-class sweeps (detach
//! and drop), on the session-work core pool.
//!
//! The outcome is `PrunePassRecord` (the facts that the Python reaper logs and
//! keeps in its pruning health) or `PruneFailed` (the candidate read failed,
//! or the pass could not start). A leaf's error or refusal is contained and
//! listed in the record. The per-leaf typed outcomes of the core pass do not
//! cross: no Python caller reads them.

use horsies_core::{PrunePass, PrunePassError, PruneReach};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::core_failures::FailureException;
use crate::handle::BrokerConnection;

/// One pruning pass: the finalized leaves, the counts of the swept leaves
/// left detached and dropped, the refusals (`leaf: Outcome` and
/// `finalize leaf: Outcome`, with the Python outcome class name), the errors
/// (`leaf: cause`, `heartbeat leaf: cause`, `finalize leaf: cause`), and
/// whether a per-pass bound left expired leaves for the next pass
/// (`WITHIN_BOUNDS` or `TRUNCATED`).
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct PrunePassRecord {
    pub finalized_leaves: Vec<String>,
    pub detached: usize,
    pub dropped: usize,
    pub refusals: Vec<String>,
    pub errors: Vec<String>,
    pub reach: String,
}

impl From<PrunePass> for PrunePassRecord {
    fn from(pass: PrunePass) -> Self {
        let detached = pass.detached_count();
        let dropped = pass.dropped_count();
        Self {
            finalized_leaves: pass.finalized_leaves,
            detached,
            dropped,
            refusals: pass.refusals,
            errors: pass.errors,
            reach: reach_text(pass.reach).to_owned(),
        }
    }
}

/// The text of a pass reach, as Python's `PruneReach` values. Pure.
fn reach_text(reach: PruneReach) -> &'static str {
    match reach {
        PruneReach::WithinBounds => "WITHIN_BOUNDS",
        PruneReach::Truncated => "TRUNCATED",
    }
}

/// The pass has no report: the connect or the client family check failed
/// (nothing was written), or the candidate read failed or broke its
/// function's contract (nothing was detached or dropped).
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct PruneFailed {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
    /// The cause, with the subject `prune_expired_partitions`.
    pub exception: FailureException,
}

/// The subject of the exception of a pass with no report.
const SUBJECT: &str = "prune_expired_partitions";

/// The outcome of one pruning pass.
pub enum PruneOutcomeRecord {
    Pass(PrunePassRecord),
    Failed(PruneFailed),
}

impl<'py> IntoPyObject<'py> for PruneOutcomeRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Pass(pass) => pass.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The record of a pass's result. Pure.
fn outcome_record(outcome: Result<PrunePass, PrunePassError>) -> PruneOutcomeRecord {
    match outcome {
        Ok(pass) => PruneOutcomeRecord::Pass(pass.into()),
        Err(error) => PruneOutcomeRecord::Failed(PruneFailed {
            message: error.to_string(),
            retryable: error.is_retryable(),
            exception: FailureException::of(SUBJECT, &error),
        }),
    }
}

/// Run one pruning pass. Writes (DDL and catalog rows); a caller that leaves
/// early does not stop the pass, and sees its outcome as unknown.
pub async fn prune_expired_partitions_operation(
    connection: BrokerConnection,
) -> PruneOutcomeRecord {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return PruneOutcomeRecord::Failed(PruneFailed {
                message: error.to_string(),
                retryable: error.is_retryable(),
                exception: FailureException::of(SUBJECT, &error),
            })
        }
    };
    outcome_record(broker.prune_expired_partitions().await)
}

#[cfg(test)]
mod tests {
    use horsies_core::{BrokerError, HistoryError};

    use super::*;

    fn pass(reach: PruneReach) -> PrunePass {
        PrunePass {
            finalized_leaves: vec!["horsies_task_history_standard_30d_2026_10_06".to_owned()],
            heartbeat_swept: Vec::new(),
            history_swept: Vec::new(),
            refusals: vec![
                "horsies_task_history_standard_30d_2026_10_07: LeafPendingBlocked".to_owned(),
            ],
            errors: vec!["horsies_task_history_standard_30d_2026_10_08: database error".to_owned()],
            reach,
        }
    }

    #[test]
    fn a_pass_keeps_its_facts_and_the_python_reach_text() {
        let PruneOutcomeRecord::Pass(record) = outcome_record(Ok(pass(PruneReach::Truncated)))
        else {
            panic!("a pass gives PrunePassRecord");
        };
        assert_eq!(
            record.finalized_leaves,
            vec!["horsies_task_history_standard_30d_2026_10_06".to_owned()]
        );
        assert_eq!(record.detached, 0);
        assert_eq!(record.dropped, 0);
        assert_eq!(
            record.refusals,
            vec!["horsies_task_history_standard_30d_2026_10_07: LeafPendingBlocked".to_owned()]
        );
        assert_eq!(record.errors.len(), 1);
        assert_eq!(record.reach, "TRUNCATED");
        assert_eq!(reach_text(PruneReach::WithinBounds), "WITHIN_BOUNDS");
    }

    #[test]
    fn a_failed_pass_keeps_its_cause_and_retry_rule() {
        let PruneOutcomeRecord::Failed(contract) = outcome_record(Err(PrunePassError::History(
            HistoryError::contract("candidate row has no class kind"),
        ))) else {
            panic!("a history error gives PruneFailed");
        };
        assert!(
            contract.message.contains("candidate row has no class kind"),
            "{}",
            contract.message
        );
        assert!(!contract.retryable);

        let PruneOutcomeRecord::Failed(connect) = outcome_record(Err(PrunePassError::Broker(
            BrokerError::ConnectionFailed("refused".to_owned()),
        ))) else {
            panic!("a broker error gives PruneFailed");
        };
        assert!(connect.retryable);
    }
}

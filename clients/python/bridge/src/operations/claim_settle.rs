//! The claim-time settle of a claim pass through the core
//! (`PostgresBroker::filter_non_runnable_workflow_tasks`):
//! `horsies_workflow_claimed_tasks_settle`, one statement in autocommit per
//! 1000 claims. Under the lock of each workflow row the function abandons
//! this worker's claims of PAUSED workflows (the node is READY again) and
//! cancels its claims of CANCELLED workflows (the node is skipped).
//!
//! The outcome is `ClaimsSettled(blocked_task_ids)`, the tasks never to
//! dispatch, in input order, or `SettleFailed`.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use super::arguments::utc_instant;
use crate::core_failures::FailureException;
use crate::handle::BrokerConnection;

/// The claims of one settle: the worker and each claimed workflow task with
/// its claim generation. A naive `datetime` raises `TypeError`.
#[pyclass(module = "horsies._core", frozen)]
pub struct SettleRequest {
    parts: Arc<SettleParts>,
}

/// The values of one settle.
pub struct SettleParts {
    pub worker_id: String,
    pub claims: Vec<(Uuid, DateTime<Utc>)>,
}

impl SettleRequest {
    pub fn parts(&self) -> Arc<SettleParts> {
        Arc::clone(&self.parts)
    }
}

#[pymethods]
impl SettleRequest {
    #[new]
    #[pyo3(signature = (*, worker_id, claims))]
    fn new(worker_id: String, claims: Vec<(Uuid, Bound<'_, PyAny>)>) -> PyResult<Self> {
        let claims = claims
            .into_iter()
            .map(|(task_id, claimed_at)| Ok((task_id, utc_instant(&claimed_at)?)))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self {
            parts: Arc::new(SettleParts { worker_id, claims }),
        })
    }
}

/// The settle committed: the tasks of PAUSED and CANCELLED workflows, never
/// to dispatch, in input order. Every other claim dispatches.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ClaimsSettled {
    pub blocked_task_ids: Vec<Uuid>,
}

/// The settle failed: the client family check, a statement, or rows outside
/// the function's contract (not retryable). The statements before the failed
/// one stay committed; a lost reply may have committed the moves of the
/// failed one.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SettleFailed {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
    /// The cause, with the subject `settle_claimed_workflow_tasks`.
    pub exception: FailureException,
}

/// The subject of the exception of a failed settle_claimed_workflow_tasks.
const SUBJECT: &str = "settle_claimed_workflow_tasks";

/// The outcome of one settle.
pub enum SettleOutcome {
    Settled(ClaimsSettled),
    Failed(SettleFailed),
}

impl<'py> IntoPyObject<'py> for SettleOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Settled(settled) => settled.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The record of a failed settle. Pure.
fn settle_failed(error: &horsies_core::BrokerError) -> SettleOutcome {
    SettleOutcome::Failed(SettleFailed {
        message: format!("claim-time settle of workflow tasks failed: {error}"),
        retryable: error.is_retryable(),
        exception: FailureException::of(SUBJECT, error),
    })
}

/// Settle the claims of one claim pass. Writes; a caller that leaves early
/// does not stop it.
pub async fn settle_claimed_workflow_tasks_operation(
    connection: BrokerConnection,
    parts: Arc<SettleParts>,
) -> SettleOutcome {
    if parts.claims.is_empty() {
        return SettleOutcome::Settled(ClaimsSettled {
            blocked_task_ids: Vec::new(),
        });
    }
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return settle_failed(&error),
    };
    if let Err(error) = broker.ensure_client_family_for_write().await {
        return settle_failed(&error);
    }
    match broker
        .filter_non_runnable_workflow_tasks(&parts.claims, &parts.worker_id)
        .await
    {
        Ok(blocked_task_ids) => SettleOutcome::Settled(ClaimsSettled { blocked_task_ids }),
        Err(error) => settle_failed(&error),
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::BrokerError;

    use super::*;

    #[test]
    fn a_failure_keeps_its_retry_rule_and_names_the_settle() {
        match settle_failed(&BrokerError::ClaimContract(
            "row 1 names another task".to_owned(),
        )) {
            SettleOutcome::Failed(failed) => {
                assert!(!failed.retryable);
                assert!(failed.message.starts_with("claim-time settle"));
            }
            SettleOutcome::Settled(_) => panic!("a failure is SettleFailed"),
        }
    }
}

//! The release operation: the return of claimed rows that no child started,
//! through the core (`horsies_release_claims`, `PostgresBroker::release_claims`).
//!
//! The Python worker builds one frozen `ReleaseClaimsRequest` per call: the
//! claims of one worker, at most `MAX_RELEASED_CLAIMS` of them, each task once,
//! each with the claim generation of its dispatch. A longer list is the
//! client's to split. The outcome is `ClaimsReleased`, with one row outcome
//! per claim in the request order (`ReleasedRow` or `NotHeldRow`), or
//! `ReleaseFailed`.

use std::sync::Arc;

use horsies_core::{BrokerError, ReleaseBatch, ReleaseClaims, ReleaseOutcome, ReleasePlacement};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use super::arguments::utc_instant;
use crate::core_failures::FailureException;
use crate::handle::BrokerConnection;

/// One frozen release. A value that the Python client should have refused
/// first raises `ValueError` (more than `MAX_RELEASED_CLAIMS` claims, a
/// repeated task id, an unknown placement) or `TypeError` (a naive
/// `datetime`).
#[pyclass(module = "horsies._core", frozen)]
pub struct ReleaseClaimsRequest {
    command: Arc<ReleaseClaims>,
}

impl ReleaseClaimsRequest {
    pub fn command(&self) -> Arc<ReleaseClaims> {
        Arc::clone(&self.command)
    }
}

#[pymethods]
impl ReleaseClaimsRequest {
    #[new]
    #[pyo3(signature = (*, worker_id, claims, placement))]
    fn new(
        worker_id: String,
        claims: Vec<(Uuid, Bound<'_, PyAny>)>,
        placement: &str,
    ) -> PyResult<Self> {
        let placement = placement_of(placement).map_err(PyValueError::new_err)?;
        let claims = claims
            .into_iter()
            .map(|(task_id, claimed_at)| Ok((task_id, utc_instant(&claimed_at)?)))
            .collect::<PyResult<Vec<_>>>()?;
        let batch = ReleaseBatch::new(&claims)
            .map_err(|refused| PyValueError::new_err(refused.to_string()))?;
        Ok(Self {
            command: Arc::new(ReleaseClaims {
                worker_id,
                batch,
                placement,
            }),
        })
    }
}

/// The placement of a release: `SAME_CLASS` or `ISOLATED`. Pure.
fn placement_of(text: &str) -> Result<ReleasePlacement, String> {
    match text {
        "SAME_CLASS" => Ok(ReleasePlacement::SameClass),
        "ISOLATED" => Ok(ReleasePlacement::Isolated),
        other => Err(format!(
            "placement must be 'SAME_CLASS' or 'ISOLATED', not {other:?}"
        )),
    }
}

/// The row was CLAIMED by the worker at its claim generation and is PENDING
/// again, with every claim and run field cleared.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ReleasedRow {
    pub task_id: Uuid,
}

/// The row is absent, or is not CLAIMED by the worker at this claim
/// generation (a row claimed again included). It was not touched.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct NotHeldRow {
    pub task_id: Uuid,
}

/// What the release did with one row.
#[derive(Clone, Debug)]
pub enum ReleaseRow {
    Released(ReleasedRow),
    NotHeld(NotHeldRow),
}

impl From<(Uuid, ReleaseOutcome)> for ReleaseRow {
    fn from((task_id, outcome): (Uuid, ReleaseOutcome)) -> Self {
        match outcome {
            ReleaseOutcome::Released => Self::Released(ReleasedRow { task_id }),
            ReleaseOutcome::NotHeld => Self::NotHeld(NotHeldRow { task_id }),
        }
    }
}

impl<'py> IntoPyObject<'py> for ReleaseRow {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Released(released) => released.into_bound_py_any(py),
            Self::NotHeld(not_held) => not_held.into_bound_py_any(py),
        }
    }
}

/// The release statement ran: one row outcome per claim, in the request
/// order. An empty request sent nothing.
#[pyclass(module = "horsies._core", frozen)]
pub struct ClaimsReleased {
    rows: Vec<ReleaseRow>,
}

#[pymethods]
impl ClaimsReleased {
    /// `ReleasedRow` or `NotHeldRow` per claim, in the request order.
    #[getter]
    fn rows<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyAny>>> {
        self.rows
            .iter()
            .cloned()
            .map(|row| row.into_bound_py_any(py))
            .collect()
    }
}

/// The release failed. The statement wrote nothing, or it committed and its
/// reply was lost; a release of the same claims again answers `NotHeldRow`
/// for each row that the lost reply released.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ReleaseFailed {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
    /// The cause, with the subject `release_claims`.
    pub exception: FailureException,
}

/// The subject of the exception of a failed release.
const SUBJECT: &str = "release_claims";

impl ReleaseFailed {
    fn from_broker_error(error: &BrokerError) -> Self {
        Self {
            message: error.to_string(),
            retryable: error.is_retryable(),
            exception: FailureException::of(SUBJECT, error),
        }
    }
}

/// The outcome of one release.
pub enum ReleaseCallOutcome {
    Released(ClaimsReleased),
    Failed(ReleaseFailed),
}

impl<'py> IntoPyObject<'py> for ReleaseCallOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Released(released) => released.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The outcome of a release. Pure.
fn release_outcome(
    released: Result<Vec<(Uuid, ReleaseOutcome)>, BrokerError>,
) -> ReleaseCallOutcome {
    match released {
        Ok(rows) => ReleaseCallOutcome::Released(ClaimsReleased {
            rows: rows.into_iter().map(ReleaseRow::from).collect(),
        }),
        Err(error) => ReleaseCallOutcome::Failed(ReleaseFailed::from_broker_error(&error)),
    }
}

/// Release the claims of one request. Writes; a caller that leaves early
/// does not stop the statement, and sees its outcome as unknown.
pub async fn release_claims_operation(
    connection: BrokerConnection,
    command: Arc<ReleaseClaims>,
) -> ReleaseCallOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return ReleaseCallOutcome::Failed(ReleaseFailed::from_broker_error(&error)),
    };
    release_outcome(broker.release_claims(&command).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    #[test]
    fn the_two_placements_are_read_and_any_other_text_is_refused() {
        assert_eq!(placement_of("SAME_CLASS"), Ok(ReleasePlacement::SameClass));
        assert_eq!(placement_of("ISOLATED"), Ok(ReleasePlacement::Isolated));
        let refused = placement_of("isolated").expect_err("placement texts are upper case");
        assert!(refused.contains("\"isolated\""), "{refused}");
    }

    #[test]
    fn each_row_outcome_has_its_own_record_in_the_request_order() {
        let ReleaseCallOutcome::Released(released) = release_outcome(Ok(vec![
            (task(2), ReleaseOutcome::Released),
            (task(1), ReleaseOutcome::NotHeld),
        ])) else {
            panic!("a release that ran gives ClaimsReleased");
        };
        let rows: Vec<(Uuid, bool)> = released
            .rows
            .iter()
            .map(|row| match row {
                ReleaseRow::Released(released) => (released.task_id, true),
                ReleaseRow::NotHeld(not_held) => (not_held.task_id, false),
            })
            .collect();
        assert_eq!(rows, vec![(task(2), true), (task(1), false)]);
    }

    #[test]
    fn a_failed_release_keeps_its_cause_and_retry_rule() {
        let outcome = release_outcome(Err(BrokerError::ClaimContract(
            "ordinality 3 has no answer".to_owned(),
        )));
        let ReleaseCallOutcome::Failed(failed) = outcome else {
            panic!("a failed release gives ReleaseFailed");
        };
        assert!(
            failed.message.contains("ordinality 3"),
            "{}",
            failed.message
        );
        assert!(!failed.retryable);
    }
}

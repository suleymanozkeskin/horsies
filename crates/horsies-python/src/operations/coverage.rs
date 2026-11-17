//! The partition coverage pass through the core
//! (`PostgresBroker::ensure_partition_coverage`): the probe
//! (`horsies_coverage_probe`) and, for a damaged set, the class registration,
//! one leaf transaction per damaged leaf and the final publication, on the
//! session-work core pool.
//!
//! The Python worker builds one frozen `CoverageRequest` per pass: the two
//! horizons, the declared retention periods in whole days, and how a damaged
//! pass is serialized (`GATED` at the worker boot, `PER_LEAF_LOCKS` in the
//! reaper). The outcome is `CoverageEnsuredRecord`, `CoverageEnsureFailedRecord`
//! (a contained refusal: the pass ran and names where it stopped), or
//! `CoverageFailed` (the pass has no outcome). The Python client maps them to
//! `CoverageEnsured`, `CoverageEnsureFailed`, and the error it raises today.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use horsies_core::{
    CoverageEnsureFailed, CoverageEnsured, CoverageOutcome, CoveragePassError, CoverageRequest,
    CoverageSerialization, HistoryCoverage, RetentionDays,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::handle::BrokerConnection;

/// One frozen coverage pass: its request and its serialization.
pub struct CoveragePass {
    pub request: CoverageRequest,
    pub serialization: CoverageSerialization,
}

/// One frozen coverage pass. A value that the Python client should have
/// refused first raises `ValueError`: a declared period that is not 1 to
/// 1825 whole days, a horizon below two leaves or above the `smallint`
/// range, or another serialization.
#[pyclass(module = "horsies._core", name = "CoverageRequest", frozen)]
pub struct CoverageRequestArgument {
    pass: Arc<CoveragePass>,
}

impl CoverageRequestArgument {
    pub fn pass(&self) -> Arc<CoveragePass> {
        Arc::clone(&self.pass)
    }
}

#[pymethods]
impl CoverageRequestArgument {
    #[new]
    #[pyo3(signature = (*, history_horizon_days, heartbeat_horizon_hours, declared_period_days, serialization))]
    fn new(
        history_horizon_days: u32,
        heartbeat_horizon_hours: u32,
        declared_period_days: Vec<i64>,
        serialization: &str,
    ) -> PyResult<Self> {
        let pass = coverage_pass(
            history_horizon_days,
            heartbeat_horizon_hours,
            &declared_period_days,
            serialization,
        )
        .map_err(PyValueError::new_err)?;
        Ok(Self {
            pass: Arc::new(pass),
        })
    }
}

/// The serialization of a pass: `GATED` or `PER_LEAF_LOCKS`. Pure.
fn serialization_of(text: &str) -> Result<CoverageSerialization, String> {
    match text {
        "GATED" => Ok(CoverageSerialization::Gated),
        "PER_LEAF_LOCKS" => Ok(CoverageSerialization::PerLeafLocks),
        other => Err(format!(
            "serialization must be 'GATED' or 'PER_LEAF_LOCKS', not {other:?}"
        )),
    }
}

/// The checked pass of the request values, or the refusal as text. Pure.
fn coverage_pass(
    history_horizon_days: u32,
    heartbeat_horizon_hours: u32,
    declared_period_days: &[i64],
    serialization: &str,
) -> Result<CoveragePass, String> {
    let serialization = serialization_of(serialization)?;
    let periods = declared_period_days
        .iter()
        .map(|days| {
            let period = Duration::try_days(*days)
                .ok_or_else(|| format!("declared period of {days} days is out of range"))?;
            RetentionDays::parse_period(period)
                .map_err(|refused| format!("declared period of {days} days: {refused}"))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let request = CoverageRequest::new(history_horizon_days, heartbeat_horizon_hours, &periods)
        .map_err(|refused| refused.to_string())?;
    Ok(CoveragePass {
        request,
        serialization,
    })
}

/// No history class has an end of coverage: only the forever class has
/// leaves, and its leaf has an open upper bound.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct CoveredForeverRecord {}

/// The first instant (UTC) at which a history class has no leaf.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct CoveredUntilRecord {
    pub at: DateTime<Utc>,
}

/// How far the history leaves reach.
#[derive(Clone, Debug)]
enum HistoryCoverageRecord {
    Forever(CoveredForeverRecord),
    Until(CoveredUntilRecord),
}

impl From<HistoryCoverage> for HistoryCoverageRecord {
    fn from(coverage: HistoryCoverage) -> Self {
        match coverage {
            HistoryCoverage::Forever => Self::Forever(CoveredForeverRecord {}),
            HistoryCoverage::Until { at } => Self::Until(CoveredUntilRecord { at }),
        }
    }
}

/// The pass ended with the expected leaves conformant: what it created,
/// whether it published the readers, and how far coverage reaches.
/// `absent_leaves` names cataloged leaves whose relation is gone.
#[pyclass(module = "horsies._core", frozen)]
pub struct CoverageEnsuredRecord {
    created_history_leaves: u64,
    created_heartbeat_leaves: u64,
    republished: bool,
    heartbeat_covered_now: bool,
    history_covered_through: HistoryCoverageRecord,
    heartbeats_covered_through: DateTime<Utc>,
    absent_leaves: Vec<String>,
}

#[pymethods]
impl CoverageEnsuredRecord {
    #[getter]
    fn created_history_leaves(&self) -> u64 {
        self.created_history_leaves
    }

    #[getter]
    fn created_heartbeat_leaves(&self) -> u64 {
        self.created_heartbeat_leaves
    }

    #[getter]
    fn republished(&self) -> bool {
        self.republished
    }

    #[getter]
    fn heartbeat_covered_now(&self) -> bool {
        self.heartbeat_covered_now
    }

    /// `CoveredForeverRecord` or `CoveredUntilRecord`.
    #[getter]
    fn history_covered_through<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match &self.history_covered_through {
            HistoryCoverageRecord::Forever(forever) => forever.clone().into_bound_py_any(py),
            HistoryCoverageRecord::Until(until) => until.clone().into_bound_py_any(py),
        }
    }

    #[getter]
    fn heartbeats_covered_through(&self) -> DateTime<Utc> {
        self.heartbeats_covered_through
    }

    #[getter]
    fn absent_leaves(&self) -> Vec<String> {
        self.absent_leaves.clone()
    }
}

impl From<CoverageEnsured> for CoverageEnsuredRecord {
    fn from(ensured: CoverageEnsured) -> Self {
        Self {
            created_history_leaves: ensured.created_history_leaves,
            created_heartbeat_leaves: ensured.created_heartbeat_leaves,
            republished: ensured.republished,
            heartbeat_covered_now: ensured.heartbeat_covered_now,
            history_covered_through: ensured.history_covered_through.into(),
            heartbeats_covered_through: ensured.heartbeats_covered_through,
            absent_leaves: ensured.absent_leaves,
        }
    }
}

/// The pass stopped at a refusal; `stage`, `class_key` and `refusal` name
/// where and why. `absent_leaves` names cataloged leaves whose relation is
/// gone, as far as the pass observed them.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct CoverageEnsureFailedRecord {
    pub stage: String,
    pub class_key: Option<String>,
    pub refusal: String,
    pub heartbeat_covered_now: bool,
    pub absent_leaves: Vec<String>,
}

impl From<CoverageEnsureFailed> for CoverageEnsureFailedRecord {
    fn from(failed: CoverageEnsureFailed) -> Self {
        Self {
            stage: failed.stage.to_owned(),
            class_key: failed.class_key,
            refusal: failed.refusal,
            heartbeat_covered_now: failed.heartbeat_covered_now,
            absent_leaves: failed.absent_leaves,
        }
    }
}

/// The pass has no outcome: the connect or the client family check failed
/// (nothing was written), or a probe, the coverage gate, the class
/// registration or the final publication failed, or a row broke its
/// function's contract (the leaf transactions that committed before stay).
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct CoverageFailed {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// The outcome of one coverage pass.
pub enum CoverageOutcomeRecord {
    Ensured(CoverageEnsuredRecord),
    EnsureFailed(CoverageEnsureFailedRecord),
    Failed(CoverageFailed),
}

impl<'py> IntoPyObject<'py> for CoverageOutcomeRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Ensured(ensured) => ensured.into_bound_py_any(py),
            Self::EnsureFailed(failed) => failed.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The record of a pass's result. Pure.
fn outcome_record(outcome: Result<CoverageOutcome, CoveragePassError>) -> CoverageOutcomeRecord {
    match outcome {
        Ok(CoverageOutcome::Ensured(ensured)) => CoverageOutcomeRecord::Ensured(ensured.into()),
        Ok(CoverageOutcome::Failed(failed)) => CoverageOutcomeRecord::EnsureFailed(failed.into()),
        Err(error) => CoverageOutcomeRecord::Failed(CoverageFailed {
            message: error.to_string(),
            retryable: error.is_retryable(),
        }),
    }
}

/// Run one coverage pass. Writes (DDL and catalog rows); a caller that
/// leaves early does not stop the pass, and sees its outcome as unknown.
pub async fn ensure_partition_coverage_operation(
    connection: BrokerConnection,
    pass: Arc<CoveragePass>,
) -> CoverageOutcomeRecord {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return CoverageOutcomeRecord::Failed(CoverageFailed {
                message: error.to_string(),
                retryable: error.is_retryable(),
            })
        }
    };
    outcome_record(
        broker
            .ensure_partition_coverage(&pass.request, pass.serialization)
            .await,
    )
}

#[cfg(test)]
mod tests {
    use horsies_core::{BrokerError, HistoryError};

    use super::*;

    #[test]
    fn the_two_serializations_are_read_and_any_other_text_is_refused() {
        assert_eq!(serialization_of("GATED"), Ok(CoverageSerialization::Gated));
        assert_eq!(
            serialization_of("PER_LEAF_LOCKS"),
            Ok(CoverageSerialization::PerLeafLocks)
        );
        let refused = serialization_of("UNGATED").expect_err("another serialization");
        assert!(refused.contains("\"UNGATED\""), "{refused}");
    }

    #[test]
    fn a_pass_keeps_its_serialization_and_refuses_each_bad_value() {
        let pass = coverage_pass(2, 2, &[7, 30], "PER_LEAF_LOCKS").expect("values in range");
        assert_eq!(pass.serialization, CoverageSerialization::PerLeafLocks);

        let period = coverage_pass(2, 2, &[0], "GATED").expect_err("a period of 0 days");
        assert!(period.contains("declared period of 0 days"), "{period}");
        let long = coverage_pass(2, 2, &[1826], "GATED").expect_err("a period above 1825 days");
        assert!(long.contains("1826 days"), "{long}");
        let huge = coverage_pass(2, 2, &[i64::MAX], "GATED").expect_err("no duration of that size");
        assert!(huge.contains("out of range"), "{huge}");
        assert!(
            coverage_pass(1, 2, &[], "GATED").is_err(),
            "one history leaf"
        );
        assert!(
            coverage_pass(2, 1, &[], "GATED").is_err(),
            "one heartbeat leaf"
        );
    }

    fn at(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("an RFC 3339 instant")
            .with_timezone(&Utc)
    }

    #[test]
    fn each_end_of_a_pass_has_its_own_record() {
        let ensured = outcome_record(Ok(CoverageOutcome::Ensured(CoverageEnsured {
            created_history_leaves: 3,
            created_heartbeat_leaves: 2,
            republished: true,
            heartbeat_covered_now: true,
            history_covered_through: HistoryCoverage::Until {
                at: at("2026-11-20T00:00:00Z"),
            },
            heartbeats_covered_through: at("2026-11-17T16:00:00Z"),
            absent_leaves: vec!["gone_leaf".to_owned()],
        })));
        let CoverageOutcomeRecord::Ensured(record) = ensured else {
            panic!("an ensured pass gives CoverageEnsuredRecord");
        };
        assert_eq!(record.created_history_leaves, 3);
        assert_eq!(record.created_heartbeat_leaves, 2);
        assert!(record.republished);
        assert!(matches!(
            record.history_covered_through,
            HistoryCoverageRecord::Until(CoveredUntilRecord { at: until })
                if until == at("2026-11-20T00:00:00Z")
        ));
        assert_eq!(record.absent_leaves, vec!["gone_leaf".to_owned()]);

        let failed = outcome_record(Ok(CoverageOutcome::Failed(CoverageEnsureFailed {
            stage: "coverage_gate_busy",
            class_key: Some("standard_30d".to_owned()),
            refusal: "another worker owns the partition coverage gate".to_owned(),
            heartbeat_covered_now: false,
            absent_leaves: Vec::new(),
        })));
        let CoverageOutcomeRecord::EnsureFailed(record) = failed else {
            panic!("a contained refusal gives CoverageEnsureFailedRecord");
        };
        assert_eq!(record.stage, "coverage_gate_busy");
        assert_eq!(record.class_key.as_deref(), Some("standard_30d"));
        assert!(!record.heartbeat_covered_now);
    }

    #[test]
    fn a_failed_pass_keeps_its_cause_and_retry_rule() {
        let CoverageOutcomeRecord::Failed(contract) = outcome_record(Err(
            CoveragePassError::History(HistoryError::contract("probe returned no row")),
        )) else {
            panic!("a history error gives CoverageFailed");
        };
        assert!(
            contract.message.contains("probe returned no row"),
            "{}",
            contract.message
        );
        assert!(!contract.retryable);

        let CoverageOutcomeRecord::Failed(family) = outcome_record(Err(CoveragePassError::Broker(
            BrokerError::ConnectionFailed("refused".to_owned()),
        ))) else {
            panic!("a broker error gives CoverageFailed");
        };
        assert!(family.retryable);
    }

    #[test]
    fn the_forever_coverage_has_its_own_record() {
        assert!(matches!(
            HistoryCoverageRecord::from(HistoryCoverage::Forever),
            HistoryCoverageRecord::Forever(_)
        ));
    }
}

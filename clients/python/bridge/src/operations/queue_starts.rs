//! The count of the soft-cap start check through the core
//! (`PostgresBroker::count_queue_starts`).
//!
//! In soft-cap mode the Python worker holds the claimed rows of capped
//! queues until a child of the row's route is free, and counts those queues
//! before it starts held rows. It builds one frozen `QueueStartsRequest` per
//! check: the worker, the capped queues that have held rows, and the claims
//! it started whose outcome write has not ended. The outcome is
//! `QueueStartsRecord` or `QueueStartsFailed`.

use std::collections::HashMap;
use std::sync::Arc;

use horsies_core::{BrokerError, QueueStartCheck, QueueStarts};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use super::arguments::utc_instant;
use crate::core_failures::FailureException;
use crate::handle::BrokerConnection;

/// One frozen start check. `started` holds each started claim as (task id,
/// claim generation). A naive `datetime` raises `TypeError`.
#[pyclass(module = "horsies._core", frozen)]
pub struct QueueStartsRequest {
    check: Arc<QueueStartCheck>,
}

impl QueueStartsRequest {
    pub fn check(&self) -> Arc<QueueStartCheck> {
        Arc::clone(&self.check)
    }
}

#[pymethods]
impl QueueStartsRequest {
    #[new]
    #[pyo3(signature = (*, worker_id, queues, started))]
    fn new(
        worker_id: String,
        queues: Vec<String>,
        started: Vec<(Uuid, Bound<'_, PyAny>)>,
    ) -> PyResult<Self> {
        let started = started
            .into_iter()
            .map(|(task_id, claimed_at)| Ok((task_id, utc_instant(&claimed_at)?)))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self {
            check: Arc::new(QueueStartCheck {
                worker_id,
                queues,
                started,
            }),
        })
    }
}

/// The count of each queue of the check: its RUNNING rows on every worker,
/// plus the started claims of the check that are still CLAIMED. A queue
/// with no counted row is absent from `starts`: its count is 0.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct QueueStartsRecord {
    pub starts: HashMap<String, u64>,
}

/// The count failed: a failed connect, a failed statement, or rows outside
/// the statement's contract (not retryable). It wrote nothing.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct QueueStartsFailed {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
    /// The cause, with the subject `count_queue_starts`.
    pub exception: FailureException,
}

/// The subject of the exception of a failed count_queue_starts.
const SUBJECT: &str = "count_queue_starts";

/// The outcome of one start check.
pub enum QueueStartsOutcome {
    Counted(QueueStartsRecord),
    Failed(QueueStartsFailed),
}

impl<'py> IntoPyObject<'py> for QueueStartsOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Counted(counted) => counted.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

fn failed(error: &BrokerError) -> QueueStartsOutcome {
    QueueStartsOutcome::Failed(QueueStartsFailed {
        message: error.to_string(),
        retryable: error.is_retryable(),
        exception: FailureException::of(SUBJECT, error),
    })
}

/// The outcome of a count. Pure.
fn starts_outcome(counted: Result<QueueStarts, BrokerError>) -> QueueStartsOutcome {
    match counted {
        Ok(starts) => QueueStartsOutcome::Counted(QueueStartsRecord {
            starts: starts.into_counts(),
        }),
        Err(error) => failed(&error),
    }
}

/// Count the queues of one start check. Reads only; a caller that leaves
/// early drops it, and nothing changed.
pub async fn count_queue_starts_operation(
    connection: BrokerConnection,
    check: Arc<QueueStartCheck>,
) -> QueueStartsOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return failed(&error),
    };
    starts_outcome(broker.count_queue_starts(&check).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_count_keeps_each_counted_queue_and_leaves_out_the_rest() {
        let counts = HashMap::from([("emails".to_owned(), 3), ("reports".to_owned(), 1)]);
        let QueueStartsOutcome::Counted(record) = starts_outcome(Ok(QueueStarts::new(counts)))
        else {
            panic!("a count that ran gives QueueStartsRecord");
        };
        assert_eq!(record.starts.get("emails"), Some(&3));
        assert_eq!(record.starts.get("reports"), Some(&1));
        assert_eq!(record.starts.get("uncounted"), None);
    }

    #[test]
    fn a_count_outside_the_contract_is_a_failure_that_is_not_retryable() {
        let QueueStartsOutcome::Failed(failed) = starts_outcome(Err(BrokerError::ClaimContract(
            "count_queue_starts counted queue \"emails\" twice".to_owned(),
        ))) else {
            panic!("a failed count gives QueueStartsFailed");
        };
        assert!(failed.message.contains("twice"), "{}", failed.message);
        assert!(!failed.retryable);
    }
}

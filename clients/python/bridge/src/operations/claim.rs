//! One claim pass through the core: `horsies_claim`
//! (`PostgresBroker::claim_pass`).
//!
//! The Python worker builds one frozen `ClaimPassRequest` per pass from its
//! claim plan: the queues in claim order, their priorities and caps, the
//! budget of each execution class and the class each served row class
//! claims into, the limits, the lease and the advisory keys of the cap
//! accounting. The outcome is `ClaimPassRecord` (the claimed rows in
//! dispatch order and the rows the take missed) or `ClaimPassFailed`. The
//! dispatch of the claimed rows stays with the caller.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use horsies_core::{
    BrokerError, ClaimClasses, ClaimPass, ClaimPassRequest as CoreClaimPassRequest, ClaimTake,
    ClaimedRow, Execution,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::core_failures::FailureException;
use crate::handle::BrokerConnection;

/// The execution class of a class name: `ISOLATED`, `SHARED_ASYNC` or
/// `SHARED_THREAD`. Pure.
fn execution_of(text: &str) -> Result<Execution, String> {
    Execution::ALL
        .into_iter()
        .find(|class| class.as_str() == text)
        .ok_or_else(|| {
            format!(
                "an execution class is 'ISOLATED', 'SHARED_ASYNC' or 'SHARED_THREAD', not {text:?}"
            )
        })
}

/// The budgets and routes of a pass, from their class names. Pure.
///
/// Errors: an unknown class name, or a route to a budget class that the
/// pass does not hold.
fn claim_classes(
    budgets: HashMap<String, u32>,
    routes: HashMap<String, String>,
) -> Result<ClaimClasses, String> {
    let budgets = budgets
        .into_iter()
        .map(|(class, capacity)| Ok((execution_of(&class)?, capacity)))
        .collect::<Result<HashMap<Execution, u32>, String>>()?;
    let routes = routes
        .into_iter()
        .map(|(row_class, budget_class)| {
            Ok((execution_of(&row_class)?, execution_of(&budget_class)?))
        })
        .collect::<Result<HashMap<Execution, Execution>, String>>()?;
    ClaimClasses::new(budgets, routes).map_err(|unheld| unheld.to_string())
}

/// One frozen claim pass. `queue_max_concurrency` holds the capped queues
/// only; `cluster_wide_cap` `None` means no cluster cap. A count that is
/// negative or does not fit in 32 bits raises `OverflowError`; an unknown
/// class name, a route to a budget the pass does not hold, or a lease of 0
/// raises `ValueError`.
#[pyclass(module = "horsies._core", frozen)]
pub struct ClaimPassRequest {
    request: Arc<CoreClaimPassRequest>,
}

impl ClaimPassRequest {
    pub fn request(&self) -> Arc<CoreClaimPassRequest> {
        Arc::clone(&self.request)
    }
}

#[pymethods]
impl ClaimPassRequest {
    #[new]
    #[pyo3(signature = (
        *,
        worker_id,
        queues,
        queue_priority,
        queue_max_concurrency,
        hard_cap_mode,
        budgets,
        routes,
        prefetch_buffer,
        max_claim_per_worker,
        max_claim_batch,
        cluster_wide_cap,
        claim_lease_ms,
        lock_keys
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        worker_id: String,
        queues: Vec<String>,
        queue_priority: HashMap<String, i32>,
        queue_max_concurrency: HashMap<String, u32>,
        hard_cap_mode: bool,
        budgets: HashMap<String, u32>,
        routes: HashMap<String, String>,
        prefetch_buffer: u32,
        max_claim_per_worker: u32,
        max_claim_batch: u32,
        cluster_wide_cap: Option<u32>,
        claim_lease_ms: u32,
        lock_keys: Vec<i64>,
    ) -> PyResult<Self> {
        let classes = claim_classes(budgets, routes).map_err(PyValueError::new_err)?;
        let claim_lease_ms = NonZeroU32::new(claim_lease_ms).ok_or_else(|| {
            PyValueError::new_err("claim_lease_ms must be above 0: every claim has a lease")
        })?;
        Ok(Self {
            request: Arc::new(CoreClaimPassRequest {
                worker_id,
                queues,
                queue_priority,
                queue_max_concurrency,
                hard_cap_mode,
                classes,
                prefetch_buffer,
                max_claim_per_worker,
                max_claim_batch,
                cluster_wide_cap,
                claim_lease_ms,
                lock_keys,
            }),
        })
    }
}

/// One row the pass claimed: everything the worker needs to dispatch it and
/// to write a failed attempt with no read before the write. `claimed_at` is
/// the claim generation of this claim.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ClaimedRowRecord {
    pub id: Uuid,
    pub task_name: String,
    pub queue_name: String,
    /// The encoded task input as one JSON value, as the send wrote it.
    pub input: String,
    pub task_options: Option<String>,
    /// `ISOLATED`, `SHARED_ASYNC` or `SHARED_THREAD`.
    pub execution: String,
    pub is_workflow_task: bool,
    pub claimed_at: DateTime<Utc>,
    pub retry_count: i32,
    pub max_retries: i32,
}

impl From<ClaimedRow> for ClaimedRowRecord {
    fn from(row: ClaimedRow) -> Self {
        Self {
            id: row.id,
            task_name: row.task_name,
            queue_name: row.queue_name,
            input: row.input,
            task_options: row.task_options,
            execution: row.execution.as_str().to_owned(),
            is_workflow_task: row.is_workflow_task,
            claimed_at: row.claimed_at,
            retry_count: row.retry_count,
            max_retries: row.max_retries,
        }
    }
}

/// The pass ran: the rows it claimed, in dispatch order, and
/// `short_of_plan`, the rows that its plan chose and its take did not get
/// (another transaction held or took them; 0 when the take got every
/// planned row).
#[pyclass(module = "horsies._core", frozen)]
pub struct ClaimPassRecord {
    claimed: Vec<ClaimedRowRecord>,
    #[pyo3(get)]
    short_of_plan: u32,
}

#[pymethods]
impl ClaimPassRecord {
    /// `ClaimedRowRecord` per claimed row, in dispatch order.
    #[getter]
    fn claimed(&self) -> Vec<ClaimedRowRecord> {
        self.claimed.clone()
    }
}

/// The pass failed. The statement wrote nothing, or it committed and its
/// reply was lost: the rows it claimed then return to the queue when their
/// lease expires.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ClaimPassFailed {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
    /// The cause, with the subject `claim_pass`.
    pub exception: FailureException,
}

/// The subject of the exception of a failed claim_pass.
const SUBJECT: &str = "claim_pass";

/// The outcome of one claim pass.
pub enum ClaimPassOutcome {
    Claimed(ClaimPassRecord),
    Failed(ClaimPassFailed),
}

impl<'py> IntoPyObject<'py> for ClaimPassOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Claimed(claimed) => claimed.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

fn failed(error: &BrokerError) -> ClaimPassOutcome {
    ClaimPassOutcome::Failed(ClaimPassFailed {
        message: error.to_string(),
        retryable: error.is_retryable(),
        exception: FailureException::of(SUBJECT, error),
    })
}

/// The outcome of a pass. Pure.
fn pass_outcome(pass: Result<ClaimPass, BrokerError>) -> ClaimPassOutcome {
    match pass {
        Ok(pass) => ClaimPassOutcome::Claimed(ClaimPassRecord {
            short_of_plan: match pass.take {
                ClaimTake::Full => 0,
                ClaimTake::Short { short_by } => short_by.get(),
            },
            claimed: pass
                .claimed
                .into_iter()
                .map(ClaimedRowRecord::from)
                .collect(),
        }),
        Err(error) => failed(&error),
    }
}

/// Run one claim pass. Writes, in one statement; a caller that leaves early
/// does not stop it, and the rows it claimed return to the queue when their
/// lease expires.
pub async fn claim_pass_operation(
    connection: BrokerConnection,
    request: Arc<CoreClaimPassRequest>,
) -> ClaimPassOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return failed(&error),
    };
    pass_outcome(broker.claim_pass(&request).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_three_class_names_are_read_and_any_other_text_is_refused() {
        assert_eq!(execution_of("ISOLATED"), Ok(Execution::Isolated));
        assert_eq!(execution_of("SHARED_ASYNC"), Ok(Execution::SharedAsync));
        assert_eq!(execution_of("SHARED_THREAD"), Ok(Execution::SharedThread));
        let refused = execution_of("isolated").expect_err("class names are upper case");
        assert!(refused.contains("\"isolated\""), "{refused}");
    }

    #[test]
    fn a_route_to_a_budget_the_pass_does_not_hold_is_refused() {
        let budgets = HashMap::from([("ISOLATED".to_owned(), 4)]);
        let held = HashMap::from([("SHARED_ASYNC".to_owned(), "ISOLATED".to_owned())]);
        assert!(claim_classes(budgets.clone(), held).is_ok());
        let unheld = HashMap::from([("ISOLATED".to_owned(), "SHARED_THREAD".to_owned())]);
        let refused = claim_classes(budgets, unheld).expect_err("an unheld budget");
        assert!(refused.contains("SHARED_THREAD"), "{refused}");
    }

    fn claimed_row(n: u128) -> ClaimedRow {
        ClaimedRow {
            id: Uuid::from_u128(n),
            task_name: "t".to_owned(),
            queue_name: "q".to_owned(),
            input: "{}".to_owned(),
            task_options: None,
            execution: Execution::SharedAsync,
            is_workflow_task: false,
            claimed_at: Utc::now(),
            retry_count: 0,
            max_retries: 3,
            trace: horsies_core::events::RunTrace::NoCarrier,
            trace_id: horsies_core::telemetry::trace_carrier::StoredTraceId::NoTraceId,
            workflow: horsies_core::frame_protocol::RunWorkflow::NotInWorkflow,
            priority: horsies_core::events::Priority::try_from(100).expect("100 is a priority"),
            enqueued_at: Utc::now(),
            read: horsies_core::ClaimRead::now(),
        }
    }

    #[test]
    fn a_pass_keeps_its_rows_in_order_and_counts_the_rows_it_missed() {
        let ClaimPassOutcome::Claimed(full) = pass_outcome(Ok(ClaimPass {
            claimed: vec![claimed_row(2), claimed_row(1)],
            take: ClaimTake::Full,
        })) else {
            panic!("a pass that ran gives ClaimPassRecord");
        };
        let ids: Vec<Uuid> = full.claimed.iter().map(|row| row.id).collect();
        assert_eq!(ids, vec![Uuid::from_u128(2), Uuid::from_u128(1)]);
        assert_eq!(full.claimed[0].execution, "SHARED_ASYNC");
        assert_eq!(full.short_of_plan, 0);

        let ClaimPassOutcome::Claimed(short) = pass_outcome(Ok(ClaimPass {
            claimed: Vec::new(),
            take: ClaimTake::Short {
                short_by: NonZeroU32::new(3).expect("3 is above 0"),
            },
        })) else {
            panic!("a pass that ran gives ClaimPassRecord");
        };
        assert_eq!(short.short_of_plan, 3);
    }

    #[test]
    fn a_failed_pass_keeps_its_cause_and_retry_rule() {
        let ClaimPassOutcome::Failed(failed) =
            pass_outcome(Err(BrokerError::InvalidClaimArgument(
                "max_claim_batch = 4294967295 exceeds the claim function's integer range"
                    .to_owned(),
            )))
        else {
            panic!("a failed pass gives ClaimPassFailed");
        };
        assert!(
            failed.message.contains("max_claim_batch"),
            "{}",
            failed.message
        );
        assert!(!failed.retryable);
    }
}

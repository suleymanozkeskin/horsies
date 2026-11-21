//! The failed attempt of a claimed task through the core:
//! `horsies_finish_failed_attempt` (`PostgresBroker::finish_failed_attempt`,
//! `PostgresBroker::finish_ended_attempt`).
//!
//! The Python worker builds one frozen `FinishFailedAttemptRequest` per
//! write: the task, its claim fence, the retry that the task's policy asks
//! for (`NoRetry` or `RetryAfter`, decided by the Python worker), the
//! terminal failure and the attempt row. Two claimed-row rules:
//! - `finish_failed_attempt_async` (`REFUSE`): the child reported a result,
//!   so the row must be RUNNING;
//! - `finish_ended_attempt_async` (`RELEASE`): the child ended without a
//!   result; a row still CLAIMED by this claim goes back to the queue with
//!   no attempt row (`AttemptClaimReleased`).
//!
//! The outcome is one class per arm: `AttemptFailureApplied` (with its
//! `retry` refusal and what is owed `after`), `AttemptRetryScheduled`, the
//! four refusals of `attempt_records`, `AttemptClaimReleased` (rule
//! `RELEASE` only), or `AttemptWriteFailed`.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use horsies_core::core::lifecycle::{
    AfterFailure, FailedAttemptOutcome, OwnedClaim, RetryRefusal, UnreportedAttemptOutcome,
};
use horsies_core::{
    AttemptFailure, AttemptFailureKind, BrokerError, FinishFailedAttempt, RetryPlacement,
    RetryRequest,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use super::arguments::utc_instant;
use super::attempt_records::{
    observed_fields, AttemptWriteFailed, PhaseTwoOwedRecord, RefusalRecord,
};
use crate::handle::BrokerConnection;

/// The function writes the terminal failure.
#[pyclass(module = "horsies._core", frozen)]
pub struct NoRetry {}

#[pymethods]
impl NoRetry {
    #[new]
    fn new() -> Self {
        Self {}
    }
}

/// The function writes the retry, unless its next attempt would start at or
/// after `good_until` on the database clock. `expected_retry_count` is the
/// count of the claim that the delay was computed for; it is part of the
/// fence. `placement` is `SAME_CLASS` or `ISOLATED`; another text raises
/// `ValueError`.
#[pyclass(module = "horsies._core", frozen)]
pub struct RetryAfter {
    request: RetryRequest,
}

#[pymethods]
impl RetryAfter {
    #[new]
    #[pyo3(signature = (*, expected_retry_count, delay_s, placement))]
    fn new(expected_retry_count: i32, delay_s: f64, placement: &str) -> PyResult<Self> {
        Ok(Self {
            request: RetryRequest::RetryAfter {
                expected_retry_count,
                delay_seconds: delay_s,
                placement: retry_placement_of(placement).map_err(PyValueError::new_err)?,
            },
        })
    }
}

/// The retry argument of a request: one of the two retry classes.
#[derive(FromPyObject)]
pub enum RetryArgument<'py> {
    No(Bound<'py, NoRetry>),
    After(Bound<'py, RetryAfter>),
}

impl RetryArgument<'_> {
    fn request(self) -> RetryRequest {
        match self {
            Self::No(_) => RetryRequest::NoRetry,
            Self::After(after) => after.get().request,
        }
    }
}

/// The placement of a retry: `SAME_CLASS` or `ISOLATED`. Pure.
fn retry_placement_of(text: &str) -> Result<RetryPlacement, String> {
    match text {
        "SAME_CLASS" => Ok(RetryPlacement::SameClass),
        "ISOLATED" => Ok(RetryPlacement::Isolated),
        other => Err(format!(
            "placement must be 'SAME_CLASS' or 'ISOLATED', not {other:?}"
        )),
    }
}

/// The attempt outcome of a failed attempt: `FAILED` or `WORKER_FAILURE`.
/// Pure.
fn attempt_failure_kind_of(text: &str) -> Result<AttemptFailureKind, String> {
    match text {
        "FAILED" => Ok(AttemptFailureKind::Failed),
        "WORKER_FAILURE" => Ok(AttemptFailureKind::WorkerFailure),
        other => Err(format!(
            "attempt_outcome must be 'FAILED' or 'WORKER_FAILURE', not {other:?}"
        )),
    }
}

/// One frozen failed attempt of a RUNNING task. `result_json`, `error_code`
/// and `failed_reason` are the terminal failure of the task; `error_code`
/// is also the code of the attempt row. A naive `claimed_at` raises
/// `TypeError`; another `attempt_outcome` text raises `ValueError`.
#[pyclass(module = "horsies._core", frozen)]
pub struct FinishFailedAttemptRequest {
    command: Arc<FinishFailedAttempt>,
}

impl FinishFailedAttemptRequest {
    pub fn command(&self) -> Arc<FinishFailedAttempt> {
        Arc::clone(&self.command)
    }
}

#[pymethods]
impl FinishFailedAttemptRequest {
    #[new]
    #[pyo3(signature = (
        *,
        task_id,
        worker_id,
        claimed_at,
        retry,
        result_json,
        error_code,
        failed_reason,
        attempt_outcome,
        attempt_error_message,
        attempt_failed_reason
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        task_id: Uuid,
        worker_id: String,
        claimed_at: &Bound<'_, PyAny>,
        retry: RetryArgument<'_>,
        result_json: String,
        error_code: Option<String>,
        failed_reason: Option<String>,
        attempt_outcome: &str,
        attempt_error_message: Option<String>,
        attempt_failed_reason: Option<String>,
    ) -> PyResult<Self> {
        let kind = attempt_failure_kind_of(attempt_outcome).map_err(PyValueError::new_err)?;
        Ok(Self {
            command: Arc::new(FinishFailedAttempt {
                task_id,
                fence: OwnedClaim {
                    worker_id,
                    claimed_at: utc_instant(claimed_at)?,
                },
                retry: retry.request(),
                result_json,
                error_code,
                failed_reason,
                attempt: AttemptFailure {
                    kind,
                    error_message: attempt_error_message,
                    failed_reason: attempt_failed_reason,
                },
            }),
        })
    }

    /// The task id of this write.
    #[getter]
    fn task_id(&self) -> Uuid {
        self.command.task_id
    }
}

/// The caller asked for no retry.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct RetryNotRequested {}

/// The caller asked for a retry, and its next attempt would start at or
/// after `good_until` on the database clock.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RetryPastDeadline {
    pub next_retry_at: DateTime<Utc>,
    pub good_until: DateTime<Utc>,
}

/// A plain task: the statement sent its capacity wake. Nothing is owed.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct CapacityWoken {}

/// Why a terminal failure was written instead of a retry.
#[derive(Clone, Debug)]
pub enum RetryRefusalRecord {
    NotRequested(RetryNotRequested),
    PastDeadline(RetryPastDeadline),
}

impl<'py> IntoPyObject<'py> for RetryRefusalRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::NotRequested(not_requested) => not_requested.into_bound_py_any(py),
            Self::PastDeadline(past) => past.into_bound_py_any(py),
        }
    }
}

/// What is owed after a terminal failure.
#[derive(Clone, Debug)]
pub enum AfterFailureRecord {
    CapacityWoken(CapacityWoken),
    PhaseTwoOwed(PhaseTwoOwedRecord),
}

impl<'py> IntoPyObject<'py> for AfterFailureRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::CapacityWoken(woken) => woken.into_bound_py_any(py),
            Self::PhaseTwoOwed(owed) => owed.into_bound_py_any(py),
        }
    }
}

/// The terminal failure and its attempt row committed. `retry` is
/// `RetryNotRequested` or `RetryPastDeadline`; `after` is `CapacityWoken`
/// (a plain task) or `PhaseTwoOwedRecord` (a workflow task).
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct AttemptFailureApplied {
    #[pyo3(get)]
    pub task_id: Uuid,
    #[pyo3(get)]
    pub terminal_at: DateTime<Utc>,
    #[pyo3(get)]
    pub observed_status: Option<String>,
    #[pyo3(get)]
    pub observed_worker_id: Option<String>,
    #[pyo3(get)]
    pub observed_claimed_at: Option<DateTime<Utc>>,
    retry: RetryRefusalRecord,
    after: AfterFailureRecord,
}

#[pymethods]
impl AttemptFailureApplied {
    /// `RetryNotRequested` or `RetryPastDeadline`.
    #[getter]
    fn retry<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.retry.clone().into_bound_py_any(py)
    }

    /// `CapacityWoken` or `PhaseTwoOwedRecord`.
    #[getter]
    fn after<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.after.clone().into_bound_py_any(py)
    }
}

/// The retry and its attempt row (`will_retry`) committed. `retry_count` is
/// the count the row now holds; the caller wakes the queue when the retry is
/// due.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct AttemptRetryScheduled {
    pub task_id: Uuid,
    pub retry_count: i32,
    pub next_retry_at: DateTime<Utc>,
    pub queue_name: String,
    pub observed_status: Option<String>,
    pub observed_worker_id: Option<String>,
    pub observed_claimed_at: Option<DateTime<Utc>>,
}

/// Rule `RELEASE`: the row was still CLAIMED by this claim generation; no
/// child started it, and it is PENDING again with every claim and run field
/// cleared and no attempt row.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct AttemptClaimReleased {
    pub task_id: Uuid,
    pub queue_name: String,
    pub observed_status: Option<String>,
    pub observed_worker_id: Option<String>,
    pub observed_claimed_at: Option<DateTime<Utc>>,
}

/// The outcome of one failed-attempt write.
pub enum FailedAttemptRecord {
    FailureApplied(AttemptFailureApplied),
    RetryScheduled(AttemptRetryScheduled),
    Refused(RefusalRecord),
    ClaimReleased(AttemptClaimReleased),
    Failed(AttemptWriteFailed),
}

impl<'py> IntoPyObject<'py> for FailedAttemptRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::FailureApplied(applied) => applied.into_bound_py_any(py),
            Self::RetryScheduled(scheduled) => scheduled.into_bound_py_any(py),
            Self::Refused(refused) => refused.into_bound_py_any(py),
            Self::ClaimReleased(released) => released.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The record of one failed-attempt outcome. Pure.
fn failed_attempt_record(outcome: &FailedAttemptOutcome) -> FailedAttemptRecord {
    match outcome {
        FailedAttemptOutcome::FailureApplied {
            task_id,
            terminal_at,
            observed,
            retry,
            after,
        } => {
            let (observed_status, observed_worker_id, observed_claimed_at) =
                observed_fields(observed);
            FailedAttemptRecord::FailureApplied(AttemptFailureApplied {
                task_id: *task_id,
                terminal_at: *terminal_at,
                observed_status,
                observed_worker_id,
                observed_claimed_at,
                retry: match retry {
                    RetryRefusal::NotRequested => {
                        RetryRefusalRecord::NotRequested(RetryNotRequested {})
                    }
                    RetryRefusal::PastDeadline {
                        next_retry_at,
                        good_until,
                    } => RetryRefusalRecord::PastDeadline(RetryPastDeadline {
                        next_retry_at: *next_retry_at,
                        good_until: *good_until,
                    }),
                },
                after: match after {
                    AfterFailure::CapacityWoken => {
                        AfterFailureRecord::CapacityWoken(CapacityWoken {})
                    }
                    AfterFailure::PhaseTwoOwed(owed) => {
                        AfterFailureRecord::PhaseTwoOwed(PhaseTwoOwedRecord::from(owed))
                    }
                },
            })
        }
        FailedAttemptOutcome::RetryScheduled {
            task_id,
            retry_count,
            next_retry_at,
            queue_name,
            observed,
        } => {
            let (observed_status, observed_worker_id, observed_claimed_at) =
                observed_fields(observed);
            FailedAttemptRecord::RetryScheduled(AttemptRetryScheduled {
                task_id: *task_id,
                retry_count: *retry_count,
                next_retry_at: *next_retry_at,
                queue_name: queue_name.clone(),
                observed_status,
                observed_worker_id,
                observed_claimed_at,
            })
        }
        FailedAttemptOutcome::Refused(refusal) => {
            FailedAttemptRecord::Refused(RefusalRecord::from(refusal))
        }
    }
}

/// The record of one write under the rule `RELEASE`. Pure.
fn ended_attempt_record(outcome: &UnreportedAttemptOutcome) -> FailedAttemptRecord {
    match outcome {
        UnreportedAttemptOutcome::Released(released) => {
            let (observed_status, observed_worker_id, observed_claimed_at) =
                observed_fields(&released.observed);
            FailedAttemptRecord::ClaimReleased(AttemptClaimReleased {
                task_id: released.task_id,
                queue_name: released.queue_name.clone(),
                observed_status,
                observed_worker_id,
                observed_claimed_at,
            })
        }
        UnreportedAttemptOutcome::Failed(failed) => failed_attempt_record(failed),
    }
}

/// The failure of the write `subject` of `task_id` that met `error`.
fn write_failed(subject: &'static str, task_id: Uuid, error: &BrokerError) -> FailedAttemptRecord {
    FailedAttemptRecord::Failed(AttemptWriteFailed::new(subject, task_id, error))
}

/// The subject of the exception of a failed `finish_failed_attempt`.
const FAILED_ATTEMPT_SUBJECT: &str = "finish_failed_attempt";

/// The subject of the exception of a failed `finish_ended_attempt`.
const ENDED_ATTEMPT_SUBJECT: &str = "finish_ended_attempt";

/// Write the failed attempt of a child that reported a result (rule
/// `REFUSE`). Writes, in one statement; a caller that leaves early does not
/// stop it, and sees its outcome as unknown.
pub async fn finish_failed_attempt_operation(
    connection: BrokerConnection,
    command: Arc<FinishFailedAttempt>,
) -> FailedAttemptRecord {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return write_failed(FAILED_ATTEMPT_SUBJECT, command.task_id, &error),
    };
    match broker.finish_failed_attempt(&command).await {
        Ok(outcome) => failed_attempt_record(&outcome),
        Err(error) => write_failed(FAILED_ATTEMPT_SUBJECT, command.task_id, &error),
    }
}

/// Write the end of an attempt whose child ended without a result (rule
/// `RELEASE`). Writes, in one statement; a caller that leaves early does
/// not stop it, and sees its outcome as unknown.
pub async fn finish_ended_attempt_operation(
    connection: BrokerConnection,
    command: Arc<FinishFailedAttempt>,
) -> FailedAttemptRecord {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return write_failed(ENDED_ATTEMPT_SUBJECT, command.task_id, &error),
    };
    match broker.finish_ended_attempt(&command).await {
        Ok(outcome) => ended_attempt_record(&outcome),
        Err(error) => write_failed(ENDED_ATTEMPT_SUBJECT, command.task_id, &error),
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::core::lifecycle::{
        AttemptWriteRefusal, ObservedTaskState, PhaseTwoOwed, PhaseTwoStep, ReleasedClaim,
    };
    use horsies_core::TaskStatus;

    use super::*;

    fn task() -> Uuid {
        Uuid::from_u128(11)
    }

    fn observed(status: TaskStatus) -> ObservedTaskState {
        ObservedTaskState {
            status: Some(status),
            worker_id: Some("worker-a".to_owned()),
            claimed_at: Some(Utc::now()),
        }
    }

    #[test]
    fn the_placements_and_attempt_outcomes_are_read_and_any_other_text_is_refused() {
        assert_eq!(
            retry_placement_of("SAME_CLASS"),
            Ok(RetryPlacement::SameClass)
        );
        assert_eq!(retry_placement_of("ISOLATED"), Ok(RetryPlacement::Isolated));
        assert!(retry_placement_of("isolated").is_err());
        assert_eq!(
            attempt_failure_kind_of("FAILED"),
            Ok(AttemptFailureKind::Failed)
        );
        assert_eq!(
            attempt_failure_kind_of("WORKER_FAILURE"),
            Ok(AttemptFailureKind::WorkerFailure)
        );
        let refused = attempt_failure_kind_of("CANCELLED").expect_err("not an attempt failure");
        assert!(refused.contains("\"CANCELLED\""), "{refused}");
    }

    #[test]
    fn an_applied_failure_keeps_its_retry_refusal_and_what_is_owed() {
        let now = Utc::now();
        let record = failed_attempt_record(&FailedAttemptOutcome::FailureApplied {
            task_id: task(),
            terminal_at: now,
            observed: observed(TaskStatus::Running),
            retry: RetryRefusal::PastDeadline {
                next_retry_at: now,
                good_until: now,
            },
            after: AfterFailure::PhaseTwoOwed(PhaseTwoOwed {
                task_name: "t".to_owned(),
                queue_name: "q".to_owned(),
                step: PhaseTwoStep::NoDependents,
            }),
        });
        let FailedAttemptRecord::FailureApplied(applied) = record else {
            panic!("an applied failure gives AttemptFailureApplied");
        };
        assert_eq!(applied.observed_status.as_deref(), Some("RUNNING"));
        assert!(matches!(applied.retry, RetryRefusalRecord::PastDeadline(_)));
        assert!(matches!(
            applied.after,
            AfterFailureRecord::PhaseTwoOwed(PhaseTwoOwedRecord { ref queue_name, .. })
                if queue_name == "q"
        ));

        let plain = failed_attempt_record(&FailedAttemptOutcome::FailureApplied {
            task_id: task(),
            terminal_at: now,
            observed: observed(TaskStatus::Running),
            retry: RetryRefusal::NotRequested,
            after: AfterFailure::CapacityWoken,
        });
        let FailedAttemptRecord::FailureApplied(plain) = plain else {
            panic!("an applied failure gives AttemptFailureApplied");
        };
        assert!(matches!(plain.retry, RetryRefusalRecord::NotRequested(_)));
        assert!(matches!(plain.after, AfterFailureRecord::CapacityWoken(_)));
    }

    #[test]
    fn a_retry_a_refusal_and_a_release_have_their_own_records() {
        let now = Utc::now();
        assert!(matches!(
            failed_attempt_record(&FailedAttemptOutcome::RetryScheduled {
                task_id: task(),
                retry_count: 2,
                next_retry_at: now,
                queue_name: "q".to_owned(),
                observed: observed(TaskStatus::Running),
            }),
            FailedAttemptRecord::RetryScheduled(AttemptRetryScheduled { retry_count: 2, .. })
        ));
        assert!(matches!(
            failed_attempt_record(&FailedAttemptOutcome::Refused(
                AttemptWriteRefusal::TaskAbsent { task_id: task() }
            )),
            FailedAttemptRecord::Refused(RefusalRecord::TaskAbsent(_))
        ));
        assert!(matches!(
            ended_attempt_record(&UnreportedAttemptOutcome::Released(ReleasedClaim {
                task_id: task(),
                queue_name: "q".to_owned(),
                observed: observed(TaskStatus::Claimed),
            })),
            FailedAttemptRecord::ClaimReleased(AttemptClaimReleased { ref observed_status, .. })
                if observed_status.as_deref() == Some("CLAIMED")
        ));
    }

    #[test]
    fn a_failed_write_keeps_its_cause_and_retry_rule() {
        let FailedAttemptRecord::Failed(failed) = write_failed(
            FAILED_ATTEMPT_SUBJECT,
            task(),
            &BrokerError::TerminalizationContract("planned failure".to_owned()),
        ) else {
            panic!("a failure gives AttemptWriteFailed");
        };
        assert_eq!(failed.task_id, task());
        assert!(
            failed.message.contains("planned failure"),
            "{}",
            failed.message
        );
        assert!(!failed.retryable);
    }
}

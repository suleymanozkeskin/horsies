//! The expiry and orphan sweep of task rows through the core
//! (`horsies_core::broker::expiry_and_orphans`):
//!
//! - the pending expiry pass (`PostgresBroker::expire_pending_tasks`):
//!   `PendingExpiryDrained`, `PendingExpiryBoundReached` (the pass stopped at
//!   its batch bound; due rows can remain), or `PendingExpiryFailed` with the
//!   rows that the batches before the failure expired;
//! - one page of the orphan sweep
//!   (`PostgresBroker::audit_orphaned_workflow_tasks`): `OrphanPageAudited`,
//!   `OrphanAuditLockBusy` (another sweep holds the cursor, SQLSTATE 55P03),
//!   or `OrphanAuditFailed`;
//! - the expiry of a claimed row before its start
//!   (`PostgresBroker::expire_owned_claim`): `TaskRowEnded`, one of the four
//!   refusals of `attempt_records`, or `AttemptWriteFailed`.
//!
//! The Python client maps them to the results of its callers: `CLEANUP_FAILED`
//! for a failure of a pass or a page, and the shared lifecycle outcomes for
//! the single-row writes.

use chrono::{DateTime, Utc};
use horsies_core::{
    BrokerError, OrphanAuditPage, PendingExpiryFailed as PassFailed, PendingExpiryPass, TaskRowEnd,
};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use super::attempt_records::{observed_fields, AttemptWriteFailed, RefusalRecord};
use crate::handle::BrokerConnection;

/// The pending expiry pass ended at a short batch: no due row was left. Every
/// batch committed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct PendingExpiryDrained {
    pub expired: u64,
}

/// The pending expiry pass ran its bound of full batches and stopped; due
/// rows can remain for the next pass. Every batch committed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct PendingExpiryBoundReached {
    pub expired: u64,
}

/// The pending expiry pass stopped at a failure. The batches before it
/// committed and expired `expired_before` rows; the failed batch committed
/// nothing, or its outcome is unknown. A failed connect and a refused client
/// family end the pass before any batch, with `expired_before` 0.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct PendingExpiryFailed {
    pub expired_before: u64,
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

impl From<PassFailed> for PendingExpiryFailed {
    fn from(failed: PassFailed) -> Self {
        Self {
            expired_before: failed.expired_before,
            message: failed.error.to_string(),
            retryable: failed.error.is_retryable(),
        }
    }
}

/// The outcome of one pending expiry pass.
pub enum PendingExpiryOutcome {
    Drained(PendingExpiryDrained),
    BoundReached(PendingExpiryBoundReached),
    Failed(PendingExpiryFailed),
}

impl<'py> IntoPyObject<'py> for PendingExpiryOutcome {
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

/// The outcome of a pass. Pure.
fn pending_expiry_outcome(pass: Result<PendingExpiryPass, PassFailed>) -> PendingExpiryOutcome {
    match pass {
        Ok(PendingExpiryPass::Drained { expired }) => {
            PendingExpiryOutcome::Drained(PendingExpiryDrained { expired })
        }
        Ok(PendingExpiryPass::BoundReached { expired }) => {
            PendingExpiryOutcome::BoundReached(PendingExpiryBoundReached { expired })
        }
        Err(failed) => PendingExpiryOutcome::Failed(PendingExpiryFailed::from(failed)),
    }
}

/// Run one pending expiry pass. Writes; a caller that leaves early does not
/// stop the pass, and sees its outcome as unknown.
pub async fn expire_pending_tasks_operation(connection: BrokerConnection) -> PendingExpiryOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return pending_expiry_outcome(Err(PassFailed {
                expired_before: 0,
                error,
            }))
        }
    };
    pending_expiry_outcome(broker.expire_pending_tasks().await)
}

/// The orphan page committed: `rows_selected` workflow tasks scanned, and
/// `candidates_returned` of them, the ones with no runnable node link, moved
/// to history as CANCELLED (`cancelled` is the same count).
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct OrphanPageAudited {
    pub rows_selected: u32,
    pub candidates_returned: u32,
    pub cancelled: u32,
}

/// Another sweep holds the orphan cursor: the page was refused at once
/// (SQLSTATE 55P03) and wrote nothing.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct OrphanAuditLockBusy {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// The page did not commit: a failed connect or client family check, a
/// failed statement, or a page outside the function's contract (rolled
/// back). After a lost `COMMIT` reply its outcome is unknown.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct OrphanAuditFailed {
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// The outcome of one orphan page.
pub enum OrphanAuditOutcome {
    Audited(OrphanPageAudited),
    LockBusy(OrphanAuditLockBusy),
    Failed(OrphanAuditFailed),
}

impl<'py> IntoPyObject<'py> for OrphanAuditOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Audited(audited) => audited.into_bound_py_any(py),
            Self::LockBusy(busy) => busy.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The outcome of a page. Pure.
fn orphan_audit_outcome(page: Result<OrphanAuditPage, BrokerError>) -> OrphanAuditOutcome {
    match page {
        Ok(page) => OrphanAuditOutcome::Audited(OrphanPageAudited {
            rows_selected: page.rows_selected,
            candidates_returned: page.candidates_returned,
            cancelled: page.cancelled,
        }),
        Err(error) => match error.is_lock_not_available() {
            true => OrphanAuditOutcome::LockBusy(OrphanAuditLockBusy {
                message: error.to_string(),
                retryable: error.is_retryable(),
            }),
            false => OrphanAuditOutcome::Failed(OrphanAuditFailed {
                message: error.to_string(),
                retryable: error.is_retryable(),
            }),
        },
    }
}

/// Run one page of the orphan sweep. Writes; a caller that leaves early does
/// not stop the page, and sees its outcome as unknown.
pub async fn audit_orphaned_workflow_tasks_operation(
    connection: BrokerConnection,
) -> OrphanAuditOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return orphan_audit_outcome(Err(error)),
    };
    orphan_audit_outcome(broker.audit_orphaned_workflow_tasks().await)
}

/// The row moved to history: `kind` is `EXPIRE_CLAIMED`, the expiry of a
/// claimed row before its start. The observed fields are the row as the
/// statement locked it.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct TaskRowEnded {
    pub task_id: Uuid,
    pub terminal_at: DateTime<Utc>,
    pub kind: String,
    pub observed_status: Option<String>,
    pub observed_worker_id: Option<String>,
    pub observed_claimed_at: Option<DateTime<Utc>>,
}

/// The outcome of one single-row expiry.
pub enum TaskRowEndRecord {
    Ended(TaskRowEnded),
    Refused(RefusalRecord),
    Failed(AttemptWriteFailed),
}

impl<'py> IntoPyObject<'py> for TaskRowEndRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Ended(ended) => ended.into_bound_py_any(py),
            Self::Refused(refused) => refused.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The record of one single-row write. Pure.
fn task_row_end_record(task_id: Uuid, end: Result<TaskRowEnd, BrokerError>) -> TaskRowEndRecord {
    match end {
        Ok(TaskRowEnd::Ended {
            task_id,
            terminal_at,
            kind,
            observed,
        }) => {
            let (observed_status, observed_worker_id, observed_claimed_at) =
                observed_fields(&observed);
            TaskRowEndRecord::Ended(TaskRowEnded {
                task_id,
                terminal_at,
                kind: kind.as_str().to_owned(),
                observed_status,
                observed_worker_id,
                observed_claimed_at,
            })
        }
        Ok(TaskRowEnd::Refused(refusal)) => {
            TaskRowEndRecord::Refused(RefusalRecord::from(&refusal))
        }
        Err(error) => TaskRowEndRecord::Failed(AttemptWriteFailed::new(task_id, &error)),
    }
}

/// Expire one claimed row of `worker_id` whose deadline passed before its
/// start. Writes, in one statement; a caller that leaves early does not stop
/// it, and sees its outcome as unknown.
pub async fn expire_owned_claim_operation(
    connection: BrokerConnection,
    task_id: Uuid,
    worker_id: String,
) -> TaskRowEndRecord {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return task_row_end_record(task_id, Err(error)),
    };
    task_row_end_record(
        task_id,
        broker.expire_owned_claim(task_id, &worker_id).await,
    )
}

#[cfg(test)]
mod tests {
    use horsies_core::core::lifecycle::outcomes::ObservedClaim;
    use horsies_core::core::lifecycle::{
        AttemptWriteRefusal, GuardEvidence, ObservedTaskState, TerminalizationKind,
    };
    use horsies_core::TaskStatus;

    use super::*;

    fn task() -> Uuid {
        Uuid::from_u128(13)
    }

    #[test]
    fn each_end_of_a_pending_expiry_pass_has_its_own_record() {
        assert!(matches!(
            pending_expiry_outcome(Ok(PendingExpiryPass::Drained { expired: 3 })),
            PendingExpiryOutcome::Drained(PendingExpiryDrained { expired: 3 })
        ));
        assert!(matches!(
            pending_expiry_outcome(Ok(PendingExpiryPass::BoundReached { expired: 100_000 })),
            PendingExpiryOutcome::BoundReached(PendingExpiryBoundReached { expired: 100_000 })
        ));
        let PendingExpiryOutcome::Failed(failed) = pending_expiry_outcome(Err(PassFailed {
            expired_before: 500,
            error: BrokerError::TerminalizationContract("planned failure".to_owned()),
        })) else {
            panic!("a failed pass gives PendingExpiryFailed");
        };
        assert_eq!(failed.expired_before, 500);
        assert!(
            failed.message.contains("planned failure"),
            "{}",
            failed.message
        );
        assert!(!failed.retryable);
    }

    #[test]
    fn an_orphan_page_and_its_failure_have_their_own_records() {
        assert!(matches!(
            orphan_audit_outcome(Ok(OrphanAuditPage {
                rows_selected: 4,
                candidates_returned: 1,
                cancelled: 1,
            })),
            OrphanAuditOutcome::Audited(OrphanPageAudited {
                rows_selected: 4,
                candidates_returned: 1,
                cancelled: 1,
            })
        ));
        let OrphanAuditOutcome::Failed(failed) = orphan_audit_outcome(Err(
            BrokerError::TerminalizationContract("planned failure".to_owned()),
        )) else {
            panic!("a contract failure is not a busy lock");
        };
        assert!(
            failed.message.contains("planned failure"),
            "{}",
            failed.message
        );
        assert!(!failed.retryable);
    }

    #[test]
    fn a_single_row_write_is_an_end_a_refusal_or_a_failure() {
        let observed = ObservedTaskState {
            status: Some(TaskStatus::Claimed),
            worker_id: Some("worker-a".to_owned()),
            claimed_at: Some(Utc::now()),
        };
        let TaskRowEndRecord::Ended(ended) = task_row_end_record(
            task(),
            Ok(TaskRowEnd::Ended {
                task_id: task(),
                terminal_at: Utc::now(),
                kind: TerminalizationKind::ExpireClaimed,
                observed: observed.clone(),
            }),
        ) else {
            panic!("an end gives TaskRowEnded");
        };
        assert_eq!(ended.kind, "EXPIRE_CLAIMED");
        assert_eq!(ended.observed_status.as_deref(), Some("CLAIMED"));

        assert!(matches!(
            task_row_end_record(
                task(),
                Ok(TaskRowEnd::Refused(
                    AttemptWriteRefusal::SourceStateConflict {
                        task_id: task(),
                        observed,
                        evidence: GuardEvidence::Claim(ObservedClaim {
                            worker_id: None,
                            claimed_at: None,
                        }),
                    }
                ))
            ),
            TaskRowEndRecord::Refused(RefusalRecord::SourceStateConflict(_))
        ));
        let TaskRowEndRecord::Failed(failed) = task_row_end_record(
            task(),
            Err(BrokerError::TerminalizationContract(
                "planned failure".to_owned(),
            )),
        ) else {
            panic!("a failure gives AttemptWriteFailed");
        };
        assert_eq!(failed.task_id, task());
        assert!(!failed.retryable);
    }
}

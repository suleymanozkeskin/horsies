//! The expiry and orphan cancel of task rows, through the functions of the
//! section "Expiry and orphan cancel" of
//! `core/baseline/0004_lifecycle_functions.sql`:
//!
//! - `horsies_expire_pending_tasks`: one batch of PENDING rows whose
//!   deadline passed. [`expire_pending_pass`] runs batches until one is
//!   short, at most [`PENDING_EXPIRY_MAX_BATCHES_PER_PASS`].
//! - `horsies_cancel_orphaned_tasks`: one page of the orphan sweep.
//!   [`cancel_orphan_page`] reads the page's cursor counts in the same
//!   transaction.
//! - `horsies_expire_owned_claim`: the expiry of one CLAIMED row whose
//!   deadline passed before its start ([`expire_owned_claim`]).
//! - `horsies_cancel_owned_orphan`: the cancel of one CLAIMED workflow task
//!   that its claim holds and that no node links ([`cancel_owned_orphan`]).
//!
//! Every statement runs on a core pool, checked out as reserved work. The
//! core reaper, the core child host and the Python client's handles run the
//! same functions here: `PostgresBroker.expire_pending_tasks` and
//! `PostgresBroker.audit_orphaned_workflow_tasks`
//! (`clients/python/horsies/core/brokers/postgres.py`).

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::child_writes::WriteDeadline;
use crate::broker::core_rows::{column, one_row, roll_back, StatementLog};
use crate::broker::error::BrokerError;
use crate::broker::stale_recovery::RecoveredTask;
use crate::broker::terminalization::{
    terminalize, terminalize_moved_and_commit_if, terminalize_moved_in_core_transaction,
    terminalize_within,
};
use crate::core::lifecycle::{
    AttemptWriteRefusal, BatchSize, MovedRow, ObservedTaskState, OwnedClaim,
    TerminalizationCommand, TerminalizationKind, TerminalizationOutcome, WorkerOwned,
};
use crate::core::{OutcomeCode, TaskError, TaskResult};
use crate::db::{
    CheckoutClass, CorePool, IsolationLevel, NotSentCause, StatementBound, StatementFailure,
    TypedStatement,
};

/// Rows of one pending expiry batch (Python `_EXPIRE_BATCH_SIZE`).
pub const PENDING_EXPIRY_BATCH_ROWS: i32 = 500;

/// Batches of one pending expiry pass at most (Python
/// `_EXPIRE_MAX_BATCHES_PER_PASS`). The bound limits the work of one pass and
/// the volume of its status notifications; the rest waits for the next pass.
pub const PENDING_EXPIRY_MAX_BATCHES_PER_PASS: u32 = 200;

/// Rows that one orphan audit page scans at most (Python
/// `_ORPHAN_BATCH_SIZE`).
pub const ORPHAN_AUDIT_PAGE_ROWS: i32 = 500;

/// The message of the stored error of an expiry (Python
/// `expire_pending_tasks` and `_expire_claimed_task_before_start`).
const EXPIRY_MESSAGE: &str = "Task expired: good_until deadline passed before execution started";

/// The error code that an expiry stores.
const EXPIRY_ERROR_CODE: &str = "TASK_EXPIRED";

/// The cursor counts of the page that `horsies_cancel_orphaned_tasks` just
/// recorded (Python `READ_ORPHAN_TASK_SCAN_STATS_SQL`).
static ORPHAN_SCAN_COUNTS: TypedStatement = TypedStatement::new(
    "orphan_scan_counts",
    "SELECT last_scan_rows, last_candidate_rows
     FROM horsies_recovery_scan_cursors
     WHERE scan_name = 'orphan_workflow_tasks'",
    &[],
);

/// How a pending expiry pass ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingExpiryPass {
    /// A batch came back short: no due row was left when it ran.
    Drained { expired: u64 },
    /// The pass ran [`PENDING_EXPIRY_MAX_BATCHES_PER_PASS`] full batches and
    /// stopped. Due rows can remain for the next pass (Python warns then).
    BoundReached { expired: u64 },
}

impl PendingExpiryPass {
    /// The rows that the pass expired.
    pub const fn expired(self) -> u64 {
        match self {
            Self::Drained { expired } | Self::BoundReached { expired } => expired,
        }
    }
}

/// A pending expiry pass stopped at a failure. The batches before it
/// committed and expired `expired_before` rows; the failed batch committed
/// nothing, or its `COMMIT` reply was lost (`error` tells which). No later
/// batch ran. A failure before the first batch (the pools, the client family
/// check, the stored result) has `expired_before` 0.
#[derive(Debug, thiserror::Error)]
#[error("the pending expiry failed after expiring {expired_before} task(s): {error}")]
pub struct PendingExpiryFailed {
    pub expired_before: u64,
    pub error: BrokerError,
}

/// What one orphan audit page did (Python `OrphanTaskAuditReport` without its
/// duration, which each caller measures).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrphanAuditPage {
    /// PENDING or CLAIMED workflow tasks that the page scanned.
    pub rows_selected: u32,
    /// Scanned rows with no runnable node link that the page cancelled.
    pub candidates_returned: u32,
    /// The rows that moved to history as CANCELLED; equal to
    /// `candidates_returned`, which the page checks.
    pub cancelled: u32,
}

/// What a single-row expiry or orphan cancel did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskRowEnd {
    /// The row moved to history with `kind` (`EXPIRE_CLAIMED` or
    /// `CANCEL_ORPHAN`); `observed` is the row as the statement locked it.
    Ended {
        task_id: Uuid,
        terminal_at: DateTime<Utc>,
        kind: TerminalizationKind,
        observed: ObservedTaskState,
    },
    /// Nothing was written: a replay of the writer's committed effect, or a
    /// refusal with what the statement saw under its locks.
    Refused(AttemptWriteRefusal),
}

/// The stored result of a pending expiry: `TASK_EXPIRED` with Python's
/// message and no data (Python `expire_pending_tasks`). Pure.
///
/// Errors: [`BrokerError::Serialization`] when the result does not encode.
pub(crate) fn pending_expiry_result() -> Result<String, BrokerError> {
    let task_error = TaskError::built_in(OutcomeCode::TaskExpired, EXPIRY_MESSAGE);
    Ok(serde_json::to_string(
        &TaskResult::<serde_json::Value>::Err(task_error),
    )?)
}

/// The stored result of the expiry of a claimed row before its start:
/// `TASK_EXPIRED` with Python's message, and the task and worker ids as
/// `data` (Python `_expire_claimed_task_before_start`). Pure.
///
/// Errors: [`BrokerError::Serialization`] when the result does not encode.
pub(crate) fn prestart_expiry_result(
    task_id: Uuid,
    worker_id: &str,
) -> Result<String, BrokerError> {
    let mut task_error = TaskError::built_in(OutcomeCode::TaskExpired, EXPIRY_MESSAGE);
    task_error.data = Some(crate::core::codec::JsonText::of_engine_value(
        &serde_json::json!({
            "task_id": task_id.to_string(),
            "worker_id": worker_id,
        }),
    ));
    Ok(serde_json::to_string(
        &TaskResult::<serde_json::Value>::Err(task_error),
    )?)
}

/// One pending expiry pass on `pool`: `horsies_expire_pending_tasks` in
/// batches of [`PENDING_EXPIRY_BATCH_ROWS`] rows, earliest deadline first,
/// until a batch is short, and at most [`PENDING_EXPIRY_MAX_BATCHES_PER_PASS`]
/// batches. Each batch is one transaction of its own (`BEGIN`, the batch,
/// `COMMIT`) on a connection checked out as reserved work, and commits only
/// when every outcome is `APPLIED`. No attempt row is written: the task never
/// started. `expired` gets the rows of each batch once it committed, in
/// batch order. Logs nothing; each caller reports the end of the pass.
///
/// Errors: [`PendingExpiryFailed`] with the rows of the batches before the
/// failed one. Its `error` is [`BrokerError::Statement`],
/// [`BrokerError::TerminalizationContract`] for a row outside the wire
/// contract or an outcome other than `APPLIED` (the batch rolled back), or
/// [`BrokerError::Serialization`] for the stored result (nothing ran).
pub(crate) async fn expire_pending_pass(
    pool: &CorePool,
    log: StatementLog,
    expired: impl FnMut(&[RecoveredTask]),
) -> Result<PendingExpiryPass, PendingExpiryFailed> {
    let not_started = |error: BrokerError| PendingExpiryFailed {
        expired_before: 0,
        error,
    };
    let command = TerminalizationCommand::ExpirePendingTasks {
        batch_size: pending_expiry_batch(),
        result_json: pending_expiry_result().map_err(not_started)?,
        error_code: EXPIRY_ERROR_CODE.to_owned(),
    };
    expire_pass_with(
        PENDING_EXPIRY_BATCH_ROWS.unsigned_abs(),
        PENDING_EXPIRY_MAX_BATCHES_PER_PASS,
        || terminalize_moved_and_commit_if(pool, log, &command, expired_tasks),
        expired,
    )
    .await
}

/// The batch size of a pending expiry batch.
fn pending_expiry_batch() -> BatchSize {
    match BatchSize::new(PENDING_EXPIRY_BATCH_ROWS) {
        Ok(batch) => batch,
        Err(refused) => panic!("PENDING_EXPIRY_BATCH_ROWS is not a batch size: {refused}"),
    }
}

/// The loop of a pending expiry pass: at most `max_batches` calls of
/// `expire_batch`, each of which expires at most `full_batch` rows and
/// returns them; `expired` gets the rows of each batch. Stops at the first
/// short batch, at the first failure, or after `max_batches` full batches.
async fn expire_pass_with<F, Fut>(
    full_batch: u32,
    max_batches: u32,
    mut expire_batch: F,
    mut expired: impl FnMut(&[RecoveredTask]),
) -> Result<PendingExpiryPass, PendingExpiryFailed>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Vec<RecoveredTask>, BrokerError>>,
{
    let mut count: u64 = 0;
    for _ in 0..max_batches {
        let rows = match expire_batch().await {
            Ok(rows) => rows,
            Err(error) => {
                return Err(PendingExpiryFailed {
                    expired_before: count,
                    error,
                })
            }
        };
        expired(&rows);
        count += u64::try_from(rows.len()).unwrap_or(u64::MAX);
        let short = u32::try_from(rows.len()).is_ok_and(|rows| rows < full_batch);
        if short {
            return Ok(PendingExpiryPass::Drained { expired: count });
        }
    }
    Ok(PendingExpiryPass::BoundReached { expired: count })
}

/// The tasks that a pending expiry batch moved, from its rows. The batch
/// reports only the rows it moved (Python `expire_pending_tasks`), so any
/// outcome other than `APPLIED` breaks the wire contract
/// ([`refuse_unapplied`]). Pure.
///
/// Errors: [`BrokerError::TerminalizationContract`].
pub(crate) fn expired_tasks(rows: &[MovedRow]) -> Result<Vec<RecoveredTask>, BrokerError> {
    moved_tasks("horsies_expire_pending_tasks", rows)
}

/// The task of each row of a discovery batch of `operation`, each of whose
/// outcomes must be `APPLIED`. Pure.
///
/// Errors: as [`refuse_unapplied`].
fn moved_tasks(operation: &str, rows: &[MovedRow]) -> Result<Vec<RecoveredTask>, BrokerError> {
    refuse_unapplied(operation, rows.iter().map(|row| &row.outcome))?;
    Ok(rows
        .iter()
        .map(|row| RecoveredTask {
            task_id: row.outcome.task_id(),
            task_name: row.task_name.clone(),
            queue_name: row.queue_name.clone(),
        })
        .collect())
}

/// Refuse the outcomes of a discovery batch of `operation` unless each one
/// is `APPLIED`. A discovery batch reports one row per row it moved (Python
/// `expire_pending_tasks` and `audit_orphaned_workflow_tasks`). Pure.
///
/// Errors: [`BrokerError::TerminalizationContract`] naming `operation`, the
/// task and the first outcome other than `APPLIED`.
pub(crate) fn refuse_unapplied<'a>(
    operation: &str,
    outcomes: impl IntoIterator<Item = &'a TerminalizationOutcome>,
) -> Result<(), BrokerError> {
    for outcome in outcomes {
        match outcome {
            TerminalizationOutcome::Applied { .. } => {}
            TerminalizationOutcome::AlreadyApplied { .. }
            | TerminalizationOutcome::LostClaim { .. }
            | TerminalizationOutcome::SourceStateConflict { .. }
            | TerminalizationOutcome::TaskAbsent { .. } => {
                return Err(BrokerError::TerminalizationContract(format!(
                    "{operation}: task {} reported {outcome:?}; \
                     a discovery batch reports moved rows only",
                    outcome.task_id(),
                )));
            }
        }
    }
    Ok(())
}

/// One orphan audit page on `pool` (Python `audit_orphaned_workflow_tasks`):
/// `horsies_cancel_orphaned_tasks` with a page of [`ORPHAN_AUDIT_PAGE_ROWS`]
/// rows, then the page's cursor counts, in one transaction on a connection
/// checked out as reserved work (`BEGIN`, the page, the counts, `COMMIT`).
/// The page cancels the scanned PENDING and CLAIMED workflow tasks that no
/// runnable node links (`CANCEL_ORPHAN_SWEEP`) and moves the cursor.
/// `cancelled` gets the rows of the page once it committed.
///
/// Errors (nothing committed, except after a `COMMIT` whose reply was lost):
/// - [`BrokerError::Statement`]: the checkout, `BEGIN`, the page, the counts
///   or `COMMIT` failed. A second sweep that holds the cursor refuses the
///   page at once with SQLSTATE 55P03
///   ([`BrokerError::is_lock_not_available`]).
/// - [`BrokerError::TerminalizationContract`]: an outcome other than
///   `APPLIED`, a negative count, or a candidate count that differs from the
///   outcomes; the transaction is rolled back.
/// - [`BrokerError::RowDecode`]: the counts do not decode.
pub(crate) async fn cancel_orphan_page(
    pool: &CorePool,
    log: StatementLog,
    mut cancelled: impl FnMut(&[RecoveredTask]),
) -> Result<OrphanAuditPage, BrokerError> {
    let command = TerminalizationCommand::CancelOrphanedTasks {
        batch_size: orphan_audit_batch(),
    };
    let mut connection = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .map_err(|cause| {
            BrokerError::Statement(StatementFailure::NotSent {
                statement: "cancel_orphaned_tasks",
                cause: NotSentCause::Checkout(cause),
            })
        })?;
    let mut transaction = connection.begin(IsolationLevel::ServerDefault).await?;
    // Every early return below drops the transaction before its COMMIT: a
    // dropped transaction that is open or aborted discards its connection,
    // so nothing of it commits.
    let moved = terminalize_moved_in_core_transaction(&mut transaction, log, &command).await?;
    log.log(&ORPHAN_SCAN_COUNTS);
    let rows = transaction
        .query(
            &ORPHAN_SCAN_COUNTS.bind().finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await?;
    let counts = one_row(rows, &ORPHAN_SCAN_COUNTS)?;
    let scanned: i32 = column(&counts, &ORPHAN_SCAN_COUNTS, "last_scan_rows")?;
    let candidates: i32 = column(&counts, &ORPHAN_SCAN_COUNTS, "last_candidate_rows")?;
    match orphan_audit_page(scanned, candidates, &moved) {
        Ok((page, tasks)) => {
            transaction.commit().await?;
            cancelled(&tasks);
            Ok(page)
        }
        Err(refused) => {
            roll_back(transaction).await?;
            Err(refused)
        }
    }
}

/// The batch size of an orphan audit page.
fn orphan_audit_batch() -> BatchSize {
    match BatchSize::new(ORPHAN_AUDIT_PAGE_ROWS) {
        Ok(batch) => batch,
        Err(refused) => panic!("ORPHAN_AUDIT_PAGE_ROWS is not a batch size: {refused}"),
    }
}

/// The page of an orphan audit from its cursor counts and rows (Python
/// `audit_orphaned_workflow_tasks`), with the task of each row. Pure.
///
/// Errors: [`BrokerError::TerminalizationContract`] for an outcome other
/// than `APPLIED`, a negative count, or a candidate count that differs from
/// the number of rows.
fn orphan_audit_page(
    scanned: i32,
    candidates: i32,
    rows: &[MovedRow],
) -> Result<(OrphanAuditPage, Vec<RecoveredTask>), BrokerError> {
    let tasks = moved_tasks("horsies_cancel_orphaned_tasks", rows)?;
    let (Ok(rows_selected), Ok(candidates_returned)) =
        (u32::try_from(scanned), u32::try_from(candidates))
    else {
        return Err(BrokerError::TerminalizationContract(format!(
            "horsies_cancel_orphaned_tasks: the cursor counts are negative \
             (scanned {scanned}, candidates {candidates})"
        )));
    };
    match usize::try_from(candidates_returned) {
        Ok(count) if count == tasks.len() => Ok((
            OrphanAuditPage {
                rows_selected,
                candidates_returned,
                cancelled: candidates_returned,
            },
            tasks,
        )),
        Ok(_) | Err(_) => Err(BrokerError::TerminalizationContract(format!(
            "horsies_cancel_orphaned_tasks: the cursor candidate count {candidates_returned} \
             differs from the {} outcome rows",
            tasks.len()
        ))),
    }
}

/// Expire one CLAIMED row whose deadline passed before its start
/// (`horsies_expire_owned_claim`, Python `_expire_claimed_task_before_start`):
/// one statement alone on `pool`, as a child write within `deadline`, the
/// deadline of the start attempt whose miss path it is
/// ([`crate::broker::child_writes::query_within`]). The fence is the worker,
/// not the claim generation: once the deadline passed, the expiry is right
/// for every generation of this worker. A row whose deadline has not passed
/// is refused with the `DEADLINE` guard; any other row is refused as the
/// shared miss classifier says. The stored error is Python's
/// ([`prestart_expiry_result`]).
///
/// Errors: [`BrokerError::Serialization`] for the stored result (nothing
/// sent); [`BrokerError::Statement`] (nothing sent when `deadline` passed
/// first; nothing written, a cancel at the bound included; or
/// `OutcomeUnknown`: the same call sent again reports a committed expiry as
/// `AlreadyApplied`); [`BrokerError::TerminalizationContract`] for a row
/// outside the wire contract.
pub(crate) async fn expire_owned_claim(
    pool: &CorePool,
    log: StatementLog,
    task_id: Uuid,
    worker_id: &str,
    deadline: WriteDeadline,
) -> Result<TaskRowEnd, BrokerError> {
    let command = TerminalizationCommand::ExpireOwnedClaim {
        task_id,
        fence: WorkerOwned {
            worker_id: worker_id.to_owned(),
        },
        result_json: prestart_expiry_result(task_id, worker_id)?,
        error_code: EXPIRY_ERROR_CODE.to_owned(),
    };
    single_row_end(
        terminalize_within(pool, log, &command, deadline).await?,
        task_id,
    )
}

/// Cancel one CLAIMED workflow task that `fence` holds and that no runnable
/// node links (`horsies_cancel_owned_orphan`, the orphan self-heal of
/// Python's finalize): one statement alone on `pool`, checked out as
/// reserved work. The task moves to history as CANCELLED (`CANCEL_ORPHAN`,
/// `WORKFLOW_CHECK_FAILED`). A task with a runnable node link is refused with
/// the `WORKFLOW_LINK_STATE` guard; any other row is refused as the shared
/// miss classifier says.
///
/// Errors: [`BrokerError::Statement`] (nothing written, or `OutcomeUnknown`:
/// the same call sent again reports a committed cancel as `AlreadyApplied`);
/// [`BrokerError::TerminalizationContract`] for a row outside the wire
/// contract.
pub(crate) async fn cancel_owned_orphan(
    pool: &CorePool,
    log: StatementLog,
    task_id: Uuid,
    fence: &OwnedClaim,
) -> Result<TaskRowEnd, BrokerError> {
    let command = TerminalizationCommand::CancelOwnedOrphan {
        task_id,
        fence: fence.clone(),
    };
    single_row_end(terminalize(pool, log, &command).await?, task_id)
}

/// The end of a single-row command from its one outcome; `terminalize`
/// enforced the count of one. Pure.
///
/// Errors: [`BrokerError::TerminalizationContract`] when the outcome names
/// another task.
fn single_row_end(
    outcomes: Vec<TerminalizationOutcome>,
    task_id: Uuid,
) -> Result<TaskRowEnd, BrokerError> {
    let [outcome] = <[TerminalizationOutcome; 1]>::try_from(outcomes).map_err(|outcomes| {
        BrokerError::TerminalizationContract(format!(
            "task {task_id}: expected exactly one outcome row, got {}",
            outcomes.len()
        ))
    })?;
    if outcome.task_id() != task_id {
        return Err(BrokerError::TerminalizationContract(format!(
            "task {task_id}: the outcome row names task {}",
            outcome.task_id()
        )));
    }
    Ok(task_row_end(outcome))
}

/// A single-row outcome as a [`TaskRowEnd`]. Pure.
fn task_row_end(outcome: TerminalizationOutcome) -> TaskRowEnd {
    match outcome {
        TerminalizationOutcome::Applied {
            task_id,
            terminal_at,
            kind,
            observed,
            ..
        } => TaskRowEnd::Ended {
            task_id,
            terminal_at,
            kind,
            observed,
        },
        TerminalizationOutcome::AlreadyApplied {
            task_id,
            terminal_at,
            kind,
            observed,
            ..
        } => TaskRowEnd::Refused(AttemptWriteRefusal::AlreadyApplied {
            task_id,
            terminal_at,
            kind,
            observed,
        }),
        TerminalizationOutcome::LostClaim {
            task_id, observed, ..
        } => TaskRowEnd::Refused(AttemptWriteRefusal::LostClaim { task_id, observed }),
        TerminalizationOutcome::SourceStateConflict {
            task_id,
            observed,
            evidence,
            ..
        } => TaskRowEnd::Refused(AttemptWriteRefusal::SourceStateConflict {
            task_id,
            observed,
            evidence,
        }),
        TerminalizationOutcome::TaskAbsent { task_id, .. } => {
            TaskRowEnd::Refused(AttemptWriteRefusal::TaskAbsent { task_id })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::lifecycle::outcomes::ObservedWorkflowLink;
    use crate::core::lifecycle::GuardEvidence;
    use crate::core::TaskStatus;

    fn task() -> Uuid {
        Uuid::from_u128(41)
    }

    fn observed(status: TaskStatus) -> ObservedTaskState {
        ObservedTaskState {
            status: Some(status),
            worker_id: Some("worker-a".to_owned()),
            claimed_at: Some(Utc::now()),
        }
    }

    fn applied() -> TerminalizationOutcome {
        TerminalizationOutcome::Applied {
            task_id: task(),
            ordinality: None,
            terminal_at: Utc::now(),
            kind: TerminalizationKind::CancelOrphanSweep,
            observed: observed(TaskStatus::Pending),
        }
    }

    /// A moved row of `outcome`, of task `orphan.task` on queue `default`.
    fn moved(outcome: TerminalizationOutcome) -> MovedRow {
        MovedRow {
            outcome,
            task_name: "orphan.task".to_owned(),
            queue_name: "default".to_owned(),
        }
    }

    /// `count` expired rows, each with its own id.
    fn expired_rows(count: u64) -> Vec<RecoveredTask> {
        (0..count)
            .map(|_| RecoveredTask {
                task_id: Uuid::new_v4(),
                task_name: "expired.task".to_owned(),
                queue_name: "default".to_owned(),
            })
            .collect()
    }

    /// The pass of `expire_pass_with` over planned batch answers (a count
    /// of rows, or a failure), the number of batches it ran, and the row
    /// count of each batch that the pass handed on.
    async fn planned_pass(
        full_batch: u32,
        max_batches: u32,
        answers: Vec<Result<u64, BrokerError>>,
    ) -> (
        Result<PendingExpiryPass, PendingExpiryFailed>,
        usize,
        Vec<usize>,
    ) {
        let mut answers = std::collections::VecDeque::from(answers);
        let mut batches = 0;
        let mut handed_on = Vec::new();
        let pass = expire_pass_with(
            full_batch,
            max_batches,
            || {
                batches += 1;
                let answer = answers.pop_front().expect("a planned batch answer");
                std::future::ready(answer.map(expired_rows))
            },
            |rows| handed_on.push(rows.len()),
        )
        .await;
        (pass, batches, handed_on)
    }

    #[tokio::test]
    async fn a_short_batch_ends_the_pending_expiry_pass_as_drained() {
        let (pass, batches, handed_on) =
            planned_pass(500, 200, vec![Ok(500), Ok(500), Ok(2)]).await;
        assert_eq!(
            pass.expect("no batch failed"),
            PendingExpiryPass::Drained { expired: 1_002 }
        );
        assert_eq!(batches, 3);
        assert_eq!(handed_on, vec![500, 500, 2]);
    }

    #[tokio::test]
    async fn full_batches_up_to_the_bound_end_the_pending_expiry_pass_with_the_bound_reached() {
        let (pass, batches, handed_on) = planned_pass(2, 3, vec![Ok(2), Ok(2), Ok(2)]).await;
        assert_eq!(
            pass.expect("no batch failed"),
            PendingExpiryPass::BoundReached { expired: 6 }
        );
        assert_eq!(batches, 3);
        assert_eq!(handed_on, vec![2, 2, 2]);
    }

    #[tokio::test]
    async fn a_failed_batch_stops_the_pending_expiry_pass_with_the_rows_before_it() {
        let (pass, batches, handed_on) = planned_pass(
            2,
            5,
            vec![
                Ok(2),
                Err(BrokerError::TerminalizationContract("planned".to_owned())),
            ],
        )
        .await;
        let failed = pass.expect_err("the second batch failed");
        assert_eq!(failed.expired_before, 2);
        assert!(failed.to_string().contains("planned"), "{failed}");
        assert_eq!(batches, 2);
        // The rows of the committed batch are handed on; the failed batch
        // has none.
        assert_eq!(handed_on, vec![2]);
    }

    #[test]
    fn an_expiry_stores_python_s_task_expired_result() {
        let pending: serde_json::Value =
            serde_json::from_str(&pending_expiry_result().expect("the result encodes"))
                .expect("the result is JSON");
        assert_eq!(pending["err"]["message"], EXPIRY_MESSAGE);
        assert_eq!(pending["err"]["data"], serde_json::Value::Null);
        assert!(
            pending.to_string().contains("\"TASK_EXPIRED\""),
            "{pending}"
        );

        let prestart: serde_json::Value = serde_json::from_str(
            &prestart_expiry_result(task(), "worker-a").expect("the result encodes"),
        )
        .expect("the result is JSON");
        assert_eq!(prestart["err"]["message"], EXPIRY_MESSAGE);
        assert_eq!(
            prestart["err"]["data"],
            serde_json::json!({"task_id": task().to_string(), "worker_id": "worker-a"})
        );
    }

    #[test]
    fn a_discovery_outcome_other_than_applied_is_a_contract_error() {
        let refused = expired_tasks(&[moved(TerminalizationOutcome::TaskAbsent {
            task_id: task(),
            ordinality: None,
        })]);
        assert!(
            matches!(refused, Err(BrokerError::TerminalizationContract(_))),
            "{refused:?}"
        );
        assert_eq!(
            expired_tasks(&[]).expect("no outcome is no move"),
            Vec::new()
        );
        let two = expired_tasks(&[moved(applied()), moved(applied())]).expect("two moves");
        assert_eq!(
            two,
            vec![
                RecoveredTask {
                    task_id: task(),
                    task_name: "orphan.task".to_owned(),
                    queue_name: "default".to_owned(),
                };
                2
            ]
        );
    }

    #[test]
    fn an_orphan_page_counts_its_outcomes_and_refuses_counts_that_disagree() {
        let (page, cancelled) =
            orphan_audit_page(3, 2, &[moved(applied()), moved(applied())]).expect("a page");
        assert_eq!(
            page,
            OrphanAuditPage {
                rows_selected: 3,
                candidates_returned: 2,
                cancelled: 2,
            }
        );
        assert_eq!(
            cancelled
                .iter()
                .map(|task| task.task_id)
                .collect::<Vec<_>>(),
            vec![task(), task()]
        );
        assert!(matches!(
            orphan_audit_page(3, 1, &[moved(applied()), moved(applied())]),
            Err(BrokerError::TerminalizationContract(_))
        ));
        assert!(matches!(
            orphan_audit_page(-1, 0, &[]),
            Err(BrokerError::TerminalizationContract(_))
        ));
        assert!(matches!(
            orphan_audit_page(
                1,
                1,
                &[moved(TerminalizationOutcome::LostClaim {
                    task_id: task(),
                    ordinality: None,
                    observed: observed(TaskStatus::Claimed),
                })]
            ),
            Err(BrokerError::TerminalizationContract(_))
        ));
    }

    #[test]
    fn a_single_row_outcome_is_an_end_or_a_refusal() {
        assert!(matches!(
            task_row_end(applied()),
            TaskRowEnd::Ended {
                kind: TerminalizationKind::CancelOrphanSweep,
                ..
            }
        ));
        let conflict = task_row_end(TerminalizationOutcome::SourceStateConflict {
            task_id: task(),
            ordinality: None,
            observed: observed(TaskStatus::Claimed),
            evidence: GuardEvidence::WorkflowLink(ObservedWorkflowLink {
                node_status: Some("RUNNING".to_owned()),
            }),
        });
        assert!(matches!(
            conflict,
            TaskRowEnd::Refused(AttemptWriteRefusal::SourceStateConflict { .. })
        ));
        assert_eq!(
            task_row_end(TerminalizationOutcome::TaskAbsent {
                task_id: task(),
                ordinality: None,
            }),
            TaskRowEnd::Refused(AttemptWriteRefusal::TaskAbsent { task_id: task() })
        );
    }

    #[test]
    fn a_single_row_outcome_of_another_task_is_a_contract_error() {
        let other = TerminalizationOutcome::TaskAbsent {
            task_id: Uuid::from_u128(42),
            ordinality: None,
        };
        assert!(matches!(
            single_row_end(vec![other], task()),
            Err(BrokerError::TerminalizationContract(_))
        ));
        assert!(matches!(
            single_row_end(Vec::new(), task()),
            Err(BrokerError::TerminalizationContract(_))
        ));
    }
}

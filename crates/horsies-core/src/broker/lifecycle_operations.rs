//! Lifecycle operations of a client handle on its main core pool, one 0004
//! function each (`crates/horsies-core/baseline/0004_lifecycle_functions.sql`):
//!
//! - [`PostgresBroker::requeue_stale_claimed`]: the stale CLAIMED requeue
//!   pass, `horsies_requeue_stale_claims` in bounded batches.
//! - [`PostgresBroker::release_claims`]: `horsies_release_claims`, the return
//!   of claimed rows that no child started.
//! - [`PostgresBroker::mark_stale_running_as_failed`]: one stale RUNNING
//!   pass, `horsies_stale_running_scan` and one `horsies_finish_stale_attempt`
//!   per candidate.
//! - [`PostgresBroker::finish_failed_attempt`] and
//!   [`PostgresBroker::finish_ended_attempt`]: `horsies_finish_failed_attempt`
//!   under the claimed-row rules `REFUSE` and `RELEASE`.
//! - [`PostgresBroker::complete_successes`]: `horsies_complete_tasks_fused`,
//!   the plain successes of one worker (1 to 64 rows) in one statement.
//! - [`PostgresBroker::finish_workflow_success`]:
//!   `horsies_finish_workflow_success`, phase 1 of a workflow task's success.
//! - [`PostgresBroker::claim_pass`]: `horsies_claim`, one claim pass of one
//!   worker.
//! - [`PostgresBroker::count_queue_starts`]: the count of the soft-cap
//!   start check (a plain read, not a 0004 function).
//! - [`PostgresBroker::start_attempt`]: `horsies_start_attempt`, the start
//!   of a run in a worker's child, with the window of its deadline.
//! - [`PostgresBroker::mark_finalizing`]: `horsies_mark_finalizing`, the
//!   finalizing marker of a run whose body ended in a worker's child.
//! - [`PostgresBroker::cancel_task`]: `horsies_cancel_task`, the
//!   administrative cancel of one plain task (Python
//!   `horsies.monitoring.task_actions.cancel_task`, and the core's monitoring
//!   cancel).
//! - [`PostgresBroker::expire_pending_tasks`]: one pending expiry pass,
//!   `horsies_expire_pending_tasks` in bounded batches.
//! - [`PostgresBroker::audit_orphaned_workflow_tasks`]: one page of the
//!   orphan sweep, `horsies_cancel_orphaned_tasks` with its cursor counts.
//! - [`PostgresBroker::expire_owned_claim`]: `horsies_expire_owned_claim`,
//!   the expiry of a claimed row whose deadline passed before its start.
//! - [`PostgresBroker::cancel_owned_orphan`]: `horsies_cancel_owned_orphan`,
//!   the cancel of a claimed workflow task that no node links.
//!
//! These are the entry points of the Python client while its own worker
//! still runs the parent loop: the client calls them through `horsies._core`
//! in place of its own statements. The core worker calls the same statement
//! functions on its worker pool.
//!
//! Each write checks the client family once per handle before its first
//! write ([`PostgresBroker::ensure_client_family_for_write`]) and holds a
//! write token, so [`PostgresBroker::close`] waits for it. The read checks
//! no family and holds no token.

use uuid::Uuid;

use crate::broker::attempt_writes::{self, FinishFailedAttempt, FinishWorkflowSuccess};
use crate::broker::child_writes::WriteDeadline;
use crate::broker::claims::{
    claim_pass, count_queue_starts, release_claims, start_attempt_within, ClaimPass,
    ClaimPassRequest, QueueStartCheck, QueueStarts, ReleaseClaims, ReleaseOutcome, StartAttempt,
    StartWindow, WindowedStart,
};
use crate::broker::error::BrokerError;
use crate::broker::expiry_and_orphans::{
    cancel_orphan_page, cancel_owned_orphan, expire_owned_claim, expire_pending_pass,
    OrphanAuditPage, PendingExpiryFailed, PendingExpiryPass, TaskRowEnd,
};
use crate::broker::postgres::PostgresBroker;
use crate::broker::stale_recovery::{
    requeue_stale_claimed_pass, StaleClaimsRequeueFailed, StaleClaimsRequeuePass, StaleRunningPass,
    StaleRunningThresholds, ThresholdMs, STALE_RUNNING_SCAN_BATCH,
};
use crate::core::lifecycle::{
    decode_outcome_row, FailedAttemptOutcome, GuardEvidence, ObservedTaskState,
    ObservedWorkflowTask, OutcomeDecodeError, OwnedClaim, TerminalizationOutcome,
    UnreportedAttemptOutcome, WorkflowSuccessOutcome,
};
use crate::core::types::status::TaskStatus;
use crate::db::{CheckoutClass, ParameterType, StatementBound, Transience, TypedStatement};
use crate::worker::execution::{mark_task_finalizing, FinalizingMark};
use crate::worker::finalize_batch::statement::complete_plain_successes;
use crate::worker::finalize_batch::{BatchOutcomes, BatchWriteError, SuccessBatch};
use crate::worker::recovery::stale_running_pass;

/// `horsies_cancel_task`. Parameters in order: task id, whether a RUNNING
/// row may be cancelled.
static CANCEL_TASK: TypedStatement = TypedStatement::new(
    "cancel_task",
    "SELECT * FROM horsies_cancel_task($1, $2)",
    &[ParameterType::Uuid, ParameterType::Bool],
);

/// What one administrative cancel answered (Python `_cancel_answer`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelAnswer {
    /// The plain task moved to history as CANCELLED; `was_status` is the
    /// status it left (PENDING, CLAIMED, or RUNNING with the opt-in).
    Cancelled { was_status: TaskStatus },
    /// The task belongs to a workflow, live or finished. Nothing changed.
    WorkflowTask { current_status: TaskStatus },
    /// The task's status does not admit the cancel: a terminal status (an
    /// earlier administrative cancel included), or RUNNING without the
    /// opt-in. Nothing changed.
    NotCancellable { current_status: TaskStatus },
    /// Neither the live table nor history holds the task (retention can
    /// remove it). Nothing changed.
    TaskAbsent,
}

/// Why an administrative cancel has no answer.
#[derive(Debug, thiserror::Error)]
pub enum CancelTaskError {
    /// The pools could not open, the client family check refused or could
    /// not read ([`BrokerError::PoolSettings`],
    /// [`BrokerError::ClientFamilyMismatch`], [`BrokerError::Statement`]):
    /// nothing was sent. Or the statement failed
    /// ([`BrokerError::Statement`]): it wrote nothing, or, for
    /// `OutcomeUnknown`, it may have committed; a repeated cancel tells
    /// which, because a committed cancel answers
    /// [`CancelAnswer::NotCancellable`] with CANCELLED.
    #[error(transparent)]
    Broker(BrokerError),
    /// The outcome row does not decode: stored data that does not parse,
    /// for example a history status that is not a task status. The function
    /// refused the task, so nothing changed; a repeat gives the same answer
    /// until the stored row changes.
    #[error(transparent)]
    StoredDataInvalid(OutcomeDecodeError),
}

impl PostgresBroker {
    /// One stale CLAIMED requeue pass (Python
    /// `PostgresBroker.requeue_stale_claimed`): a CLAIMED row with no
    /// claimer beat newer than `stale_after`, claimed longer ago than
    /// `stale_after`, goes back to PENDING (a workflow task with no runnable
    /// node stays), in batches until a batch is short or the pass reaches
    /// its bound. Each batch commits alone. Sends no queue wake.
    ///
    /// Errors: [`StaleClaimsRequeueFailed`]: the pools could not open, the
    /// client family check refused or could not read (`requeued_before` 0),
    /// or a batch failed after the batches before it committed.
    pub async fn requeue_stale_claimed(
        &self,
        stale_after: ThresholdMs,
    ) -> Result<StaleClaimsRequeuePass, StaleClaimsRequeueFailed> {
        let not_started = |error: BrokerError| StaleClaimsRequeueFailed {
            requeued_before: 0,
            error,
        };
        self.ensure_client_family_for_write()
            .await
            .map_err(not_started)?;
        let (open, _write) = self.core_pools_for_write().map_err(not_started)?;
        requeue_stale_claimed_pass(open.main(), self.statement_log(), stale_after).await
    }

    /// Return claimed rows that no child started to the queue (Python
    /// `horsies.core.worker.release.release_claims`, one call of at most
    /// [`crate::MAX_RELEASED_CLAIMS`] rows). A row that is
    /// CLAIMED by the command's worker at its claim generation goes back to
    /// PENDING with every claim and run field cleared; any other row is not
    /// touched. `ISOLATED` also writes the `ISOLATED` class and wakes each
    /// queue of a released row once. No attempt row is written.
    ///
    /// One statement in autocommit; an empty batch sends nothing and checks
    /// nothing. Returns one outcome per claim, in the batch order.
    ///
    /// Errors: [`BrokerError::PoolSettings`] and the client family check's
    /// errors (nothing sent); [`BrokerError::Statement`] (nothing written,
    /// or `OutcomeUnknown`: the release may have committed; the same
    /// release sent again answers `NotHeld` for each released row);
    /// [`BrokerError::RowDecode`] and [`BrokerError::ClaimContract`] for
    /// rows outside the function's contract.
    pub async fn release_claims(
        &self,
        command: &ReleaseClaims,
    ) -> Result<Vec<(Uuid, ReleaseOutcome)>, BrokerError> {
        if command.batch.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_client_family_for_write().await?;
        let (open, _write) = self.core_pools_for_write()?;
        release_claims(open.main(), self.statement_log(), command).await
    }

    /// One stale RUNNING pass (Python `PostgresBroker.mark_stale_tasks_as_failed`):
    /// `horsies_stale_running_scan` reads at most 1000 candidates with no
    /// lock; each candidate is then one `horsies_finish_stale_attempt`, which
    /// judges the row again under its locks and writes the crash retry that
    /// the task's policy admits for `WORKER_CRASHED`, or the terminal failure
    /// (`FAIL_STALE`), or nothing. A scheduled retry wakes its queue (best
    /// effort). Each statement commits alone, on the main core pool, checked
    /// out as reserved work.
    ///
    /// A candidate whose statement fails is logged and skipped, and the pass
    /// goes on, as Python does. [`StaleRunningPass::ScanFull`] says that the
    /// scan read a full batch, so stale rows can remain for the next pass.
    ///
    /// Errors: [`BrokerError::PoolSettings`] and the client family check's
    /// errors (nothing sent); the scan's [`BrokerError::Statement`] or
    /// [`BrokerError::RowDecode`] (nothing written).
    pub async fn mark_stale_running_as_failed(
        &self,
        thresholds: StaleRunningThresholds,
    ) -> Result<StaleRunningPass, BrokerError> {
        self.ensure_client_family_for_write().await?;
        let (open, _write) = self.core_pools_for_write()?;
        stale_running_pass(
            open.main(),
            self.statement_log(),
            thresholds,
            STALE_RUNNING_SCAN_BATCH,
        )
        .await
    }

    /// Write the failed attempt of a task whose child reported a result
    /// (Python `horsies.core.worker.failed_attempt.finish_failed_attempt`,
    /// claimed-row rule `REFUSE`): `horsies_finish_failed_attempt`, one
    /// statement in autocommit on the main core pool, checked out as
    /// reserved work. Under the full fence (worker, claim generation, and
    /// the retry count of a retry) it writes the attempt row and the retry,
    /// or the terminal failure with its capacity wake (a plain task), or
    /// nothing on a refusal. The retry request is the caller's.
    ///
    /// Errors: [`BrokerError::PoolSettings`] and the client family check's
    /// errors (nothing sent); [`BrokerError::Statement`] (nothing written,
    /// or `OutcomeUnknown`: the same command sent again reports a committed
    /// failure as `AlreadyApplied` and a committed retry as `LostClaim`);
    /// [`BrokerError::TerminalizationContract`] for a row outside the wire
    /// contract.
    pub async fn finish_failed_attempt(
        &self,
        command: &FinishFailedAttempt,
    ) -> Result<FailedAttemptOutcome, BrokerError> {
        self.ensure_client_family_for_write().await?;
        let (open, _write) = self.core_pools_for_write()?;
        attempt_writes::finish_failed_attempt(open.main(), self.statement_log(), command).await
    }

    /// Write the end of an attempt whose child ended without a result
    /// (Python `horsies.core.worker.failed_attempt.finish_ended_attempt`,
    /// claimed-row rule `RELEASE`): a row still CLAIMED by this worker at
    /// this claim generation goes back to the queue with no attempt row
    /// (`Released`); any other row is written or refused as
    /// [`PostgresBroker::finish_failed_attempt`] does. One statement, as
    /// there.
    ///
    /// Errors: as [`PostgresBroker::finish_failed_attempt`]; after
    /// `OutcomeUnknown` a committed release is reported as a refusal.
    pub async fn finish_ended_attempt(
        &self,
        command: &FinishFailedAttempt,
    ) -> Result<UnreportedAttemptOutcome, BrokerError> {
        self.ensure_client_family_for_write().await?;
        let (open, _write) = self.core_pools_for_write()?;
        attempt_writes::finish_unreported_attempt(open.main(), self.statement_log(), command).await
    }

    /// Write the plain successes of one worker (Python
    /// `horsies.core.lifecycle.persistence.apply_fused_batch`):
    /// `horsies_complete_tasks_fused`, one statement in autocommit for 1 to
    /// 64 rows, on the main core pool, checked out as batch work. Each row
    /// keeps its own fence (`worker_id` and its claim generation) and
    /// outcome; an `Applied` row committed with the statement, a refused row
    /// wrote nothing; the rows of one queue send one capacity wake. A batch
    /// of one row runs the same function with one row; Python runs
    /// `horsies_complete_task_fused` for it, with the same effects.
    ///
    /// Errors: [`BatchWriteError::NotSent`] (the pools, the client family
    /// check, or the checkout failed: no row changed);
    /// [`BatchWriteError::Refused`] (no row changed);
    /// [`BatchWriteError::CommitUnknown`] (each row may have committed; a
    /// replay of a row finds it `AlreadyApplied` when it did);
    /// [`BatchWriteError::ContractBroken`] (rows outside the function's
    /// contract).
    pub async fn complete_successes(
        &self,
        worker_id: &str,
        batch: &SuccessBatch,
    ) -> Result<BatchOutcomes, BatchWriteError> {
        let not_sent = |error: BrokerError| BatchWriteError::NotSent {
            transience: match error.is_retryable() {
                true => Transience::Transient,
                false => Transience::Permanent,
            },
            detail: error.to_string(),
            cause: error.failure_cause(),
        };
        self.ensure_client_family_for_write()
            .await
            .map_err(not_sent)?;
        let (open, _write) = self.core_pools_for_write().map_err(not_sent)?;
        complete_plain_successes(open.main(), self.statement_log(), worker_id, batch).await
    }

    /// Write phase 1 of a workflow task's success (Python
    /// `horsies.core.worker.workflow_success.finish_workflow_success`):
    /// `horsies_finish_workflow_success`, one statement in autocommit on the
    /// main core pool, checked out as reserved work. Under the full fence it
    /// writes the attempt row, the move to history and the phase-2 record of
    /// the node, or nothing on a refusal. Sends no capacity wake: phase 2
    /// sends it, and phase 2 stays with the caller.
    ///
    /// Errors: [`BrokerError::PoolSettings`] and the client family check's
    /// errors (nothing sent); [`BrokerError::Statement`] (nothing written, or
    /// `OutcomeUnknown`: the same command sent again reports a committed
    /// success as `AlreadyApplied`); [`BrokerError::TerminalizationContract`]
    /// for a row outside the wire contract.
    pub async fn finish_workflow_success(
        &self,
        command: &FinishWorkflowSuccess,
    ) -> Result<WorkflowSuccessOutcome, BrokerError> {
        self.ensure_client_family_for_write().await?;
        let (open, _write) = self.core_pools_for_write()?;
        attempt_writes::finish_workflow_success(open.main(), self.statement_log(), command).await
    }

    /// One claim pass of one worker (Python `ClaimMixin._claim_and_dispatch_all`,
    /// the claim statement only): `horsies_claim`, one statement in
    /// autocommit on the main core pool, checked out as batch work, so its
    /// cap locks end with the statement. It claims rows (CLAIMED, a new claim
    /// generation, a lease) within the class budgets, the per-worker limit,
    /// the queue caps and the cluster cap, and writes nothing else. The
    /// dispatch of the claimed rows is the caller's.
    ///
    /// Errors: [`BrokerError::PoolSettings`] and the client family check's
    /// errors (nothing sent); [`BrokerError::InvalidClaimArgument`] for a
    /// count above the function's integer range (nothing sent);
    /// [`BrokerError::Statement`] (after `OutcomeUnknown` the pass may have
    /// claimed rows; they return to the queue when their lease expires);
    /// [`BrokerError::RowDecode`] and [`BrokerError::ClaimContract`] for rows
    /// outside the function's contract.
    pub async fn claim_pass(&self, request: &ClaimPassRequest) -> Result<ClaimPass, BrokerError> {
        self.ensure_client_family_for_write().await?;
        let (open, _write) = self.core_pools_for_write()?;
        claim_pass(open.main(), self.statement_log(), request).await
    }

    /// Count the capped queues of one soft-cap start check of one worker
    /// (Python `ClaimMixin._start_held_rows`, the count statement only): one
    /// statement on the main core pool, checked out as batch work. Each queue
    /// of `check` counts its RUNNING rows on every worker, plus the started
    /// claims of `check` that are still CLAIMED by its worker at their claim
    /// generation. A queue with no counted row is absent. Reads only and
    /// locks nothing, so a start of another worker in the same instant is
    /// not seen. A check with no queue sends nothing, as Python sends no
    /// statement when no row is held.
    ///
    /// Errors: [`BrokerError::PoolSettings`] (nothing sent);
    /// [`BrokerError::Statement`]; [`BrokerError::RowDecode`] and
    /// [`BrokerError::ClaimContract`] for rows outside the statement's
    /// contract (a queue counted twice, a queue that the check did not
    /// name, a negative count).
    pub async fn count_queue_starts(
        &self,
        check: &QueueStartCheck,
    ) -> Result<QueueStarts, BrokerError> {
        if check.queues.is_empty() {
            return Ok(QueueStarts::default());
        }
        let open = self.core_pools_for_read()?;
        count_queue_starts(open.main(), self.statement_log(), check).await
    }

    /// Start one claimed task in a worker's child (Python
    /// `child_runner._confirm_ownership_and_set_running` and
    /// `shared_lifecycle._set_running_on_a_connection`, the start statement
    /// only): `horsies_start_attempt`, one statement in autocommit on the
    /// main core pool, checked out as reserved work. Under the claim fence
    /// it writes the RUNNING row, the first runner beat and, for a workflow
    /// task, the node, or nothing on a miss. When `window` has closed by the
    /// time the pool gives the connection, no statement runs and the row
    /// stays CLAIMED ([`WindowedStart::WindowClosed`]). The path after a miss
    /// (the expiry, the workflow stop) is the caller's, within the rest of
    /// the same start attempt.
    ///
    /// The start is a child write within `deadline`, the deadline of its
    /// start attempt ([`WriteDeadline::start_attempt`],
    /// [`crate::broker::child_writes`]): the checkout waits at most 30 s, and
    /// the statement gets a cancel request after 10 s or at the deadline.
    /// A start cancelled at its bound does not commit later.
    ///
    /// Errors: [`BrokerError::PoolSettings`] and the client family check's
    /// errors (nothing sent); [`BrokerError::Statement`] (nothing sent for a
    /// failed checkout or a deadline that passed first; nothing written,
    /// a cancel at the bound included; or `OutcomeUnknown`: the row may be
    /// RUNNING, and the same start sent again misses with `NotHeld`);
    /// [`BrokerError::RowDecode`] and [`BrokerError::ClaimContract`] for a
    /// row outside the function's contract.
    pub async fn start_attempt(
        &self,
        command: &StartAttempt,
        window: StartWindow,
        deadline: WriteDeadline,
    ) -> Result<WindowedStart, BrokerError> {
        self.ensure_client_family_for_write().await?;
        let (open, _write) = self.core_pools_for_write()?;
        start_attempt_within(open.main(), self.statement_log(), command, window, deadline).await
    }

    /// Write the finalizing marker of a run whose body ended in a worker's
    /// child (Python `child_runner._mark_task_finalizing` and
    /// `shared_lifecycle.mark_task_finalizing`): `horsies_mark_finalizing`,
    /// one statement in autocommit on the main core pool, checked out as
    /// reserved work. When the row is RUNNING under `fence` (this worker,
    /// this claim generation) it stamps `finalizing_at` and
    /// `finalizing_by_worker_id` ([`FinalizingMark::Marked`]); otherwise it
    /// writes nothing ([`FinalizingMark::NotHeld`]). The wait for the
    /// connection and the statement end within
    /// [`crate::broker::child_writes::FINALIZING_MARKER_BOUND`]; a marker
    /// cancelled at the bound is not written later.
    ///
    /// Errors: [`BrokerError::PoolSettings`] and the client family check's
    /// errors (nothing sent); [`BrokerError::Statement`] (nothing sent when
    /// the bound passed first; nothing written, a cancel at the bound
    /// included; or `OutcomeUnknown`: the marker may be written, and the same
    /// call sent again writes it again); [`BrokerError::RowDecode`] and
    /// [`BrokerError::ClaimContract`] for a row outside the function's
    /// contract.
    pub async fn mark_finalizing(
        &self,
        task_id: Uuid,
        fence: &OwnedClaim,
    ) -> Result<FinalizingMark, BrokerError> {
        self.ensure_client_family_for_write().await?;
        mark_task_finalizing(self, task_id, &fence.worker_id, fence.claimed_at).await
    }

    /// Cancel one plain task (Python `horsies.monitoring.task_actions.cancel_task`):
    /// `horsies_cancel_task`, one statement in autocommit on the main core
    /// pool, checked out as batch work (monitoring work, never reserved).
    ///
    /// The function takes the task advisory lock, then the row lock. A
    /// PENDING or CLAIMED plain task (RUNNING only with `include_running`)
    /// moves to history as CANCELLED (`CANCEL_ADMIN`, error code
    /// `TASK_CANCELLED`) with no attempt and no result; `task_done` and
    /// `horsies_task_status` fire. A RUNNING task's code keeps running on its
    /// worker. Every other task is refused with no write
    /// ([`CancelAnswer`]).
    ///
    /// Errors: [`CancelTaskError`].
    ///
    /// Panics: when the function returns other than one row, or a row that
    /// contradicts it (a lost claim: the cancel has no claim fence; an
    /// applied, replayed or refused cancel with no observed status). These
    /// are defects of the function, as Python's `RuntimeError` and
    /// `AssertionError`.
    pub async fn cancel_task(
        &self,
        task_id: Uuid,
        include_running: bool,
    ) -> Result<CancelAnswer, CancelTaskError> {
        self.ensure_client_family_for_write()
            .await
            .map_err(CancelTaskError::Broker)?;
        let (open, _write) = self
            .core_pools_for_write()
            .map_err(CancelTaskError::Broker)?;
        let statement = &CANCEL_TASK;
        let bound = statement
            .bind()
            .value(&task_id)
            .value(&include_running)
            .finish();
        self.statement_log().log(statement);
        let rows = open
            .main()
            .query(
                CheckoutClass::Batch,
                &bound,
                StatementBound::ConnectionTimeouts,
            )
            .await
            .map_err(|failure| CancelTaskError::Broker(BrokerError::from(failure)))?;
        let [row] = rows.as_slice() else {
            panic!(
                "horsies_cancel_task returned {} rows for task {task_id}; it returns one",
                rows.len()
            );
        };
        let outcome = decode_outcome_row(row).map_err(CancelTaskError::StoredDataInvalid)?;
        Ok(cancel_answer(task_id, &outcome))
    }

    /// One pending expiry pass (Python `PostgresBroker.expire_pending_tasks`):
    /// `horsies_expire_pending_tasks` in batches of 500 rows on the main core
    /// pool, until a batch is short, at most 200 batches. Each batch is one
    /// transaction checked out as reserved work and commits alone; a PENDING
    /// row whose deadline passed moves to history as EXPIRED with
    /// `TASK_EXPIRED`. Sends no queue wake.
    ///
    /// Errors: [`PendingExpiryFailed`]: the pools could not open, the client
    /// family check refused or could not read (`expired_before` 0), or a
    /// batch failed after the batches before it committed.
    pub async fn expire_pending_tasks(&self) -> Result<PendingExpiryPass, PendingExpiryFailed> {
        let not_started = |error: BrokerError| PendingExpiryFailed {
            expired_before: 0,
            error,
        };
        self.ensure_client_family_for_write()
            .await
            .map_err(not_started)?;
        let (open, _write) = self.core_pools_for_write().map_err(not_started)?;
        expire_pending_pass(open.main(), self.statement_log()).await
    }

    /// One page of the orphan sweep (Python
    /// `PostgresBroker.audit_orphaned_workflow_tasks`):
    /// `horsies_cancel_orphaned_tasks` with a page of 500 rows and the page's
    /// cursor counts, in one transaction on the main core pool, checked out
    /// as reserved work. The page cancels the scanned PENDING and CLAIMED
    /// workflow tasks that no runnable node links (`CANCEL_ORPHAN_SWEEP`).
    ///
    /// Errors: [`BrokerError::PoolSettings`] and the client family check's
    /// errors (nothing sent); [`BrokerError::Statement`] (nothing committed,
    /// or `OutcomeUnknown` at `COMMIT`; a second sweep that holds the cursor
    /// refuses the page at once, [`BrokerError::is_lock_not_available`]);
    /// [`BrokerError::TerminalizationContract`] and [`BrokerError::RowDecode`]
    /// for a page outside the function's contract (rolled back).
    pub async fn audit_orphaned_workflow_tasks(&self) -> Result<OrphanAuditPage, BrokerError> {
        self.ensure_client_family_for_write().await?;
        let (open, _write) = self.core_pools_for_write()?;
        cancel_orphan_page(open.main(), self.statement_log()).await
    }

    /// Expire one CLAIMED task of `worker_id` whose deadline passed before its
    /// start (the child miss path of Python, `_expire_claimed_task_before_start`):
    /// `horsies_expire_owned_claim`, one statement in autocommit on the main
    /// core pool, checked out as reserved work, within `deadline`, the rest
    /// of the start attempt whose miss path it is
    /// ([`WriteDeadline::rest_of_start_attempt`]). The row moves to history as
    /// EXPIRED (`EXPIRE_CLAIMED`) with Python's stored error
    /// ([`TaskRowEnd::Ended`]); a row whose deadline has not passed, or that
    /// this worker does not hold, is refused with no write.
    ///
    /// Errors: [`BrokerError::PoolSettings`] and the client family check's
    /// errors (nothing sent); [`BrokerError::Serialization`] (nothing sent);
    /// [`BrokerError::Statement`] (nothing sent when `deadline` passed
    /// first; nothing written, a cancel at the bound included; or
    /// `OutcomeUnknown`: the same call sent again reports a committed expiry
    /// as `AlreadyApplied`); [`BrokerError::TerminalizationContract`] for a
    /// row outside the wire contract.
    pub async fn expire_owned_claim(
        &self,
        task_id: Uuid,
        worker_id: &str,
        deadline: WriteDeadline,
    ) -> Result<TaskRowEnd, BrokerError> {
        self.ensure_client_family_for_write().await?;
        let (open, _write) = self.core_pools_for_write()?;
        expire_owned_claim(
            open.main(),
            self.statement_log(),
            task_id,
            worker_id,
            deadline,
        )
        .await
    }

    /// Cancel one CLAIMED workflow task that `fence` holds and that no
    /// runnable node links (the orphan self-heal of Python's finalize):
    /// `horsies_cancel_owned_orphan`, one statement in autocommit on the main
    /// core pool, checked out as reserved work. The task moves to history as
    /// CANCELLED (`CANCEL_ORPHAN`, `WORKFLOW_CHECK_FAILED`)
    /// ([`TaskRowEnd::Ended`]); a task with a runnable node link, or that the
    /// fence does not hold, is refused with no write.
    ///
    /// Errors: [`BrokerError::PoolSettings`] and the client family check's
    /// errors (nothing sent); [`BrokerError::Statement`] (nothing written, or
    /// `OutcomeUnknown`: the same call sent again reports a committed cancel
    /// as `AlreadyApplied`); [`BrokerError::TerminalizationContract`] for a
    /// row outside the wire contract.
    pub async fn cancel_owned_orphan(
        &self,
        task_id: Uuid,
        fence: &OwnedClaim,
    ) -> Result<TaskRowEnd, BrokerError> {
        self.ensure_client_family_for_write().await?;
        let (open, _write) = self.core_pools_for_write()?;
        cancel_owned_orphan(open.main(), self.statement_log(), task_id, fence).await
    }
}

/// The answer of an administrative cancel from its decoded outcome (Python
/// `_cancel_answer`). Pure. A finished task is refused as it was when it was
/// live: a workflow task as a workflow task, a plain task as not
/// cancellable from its terminal status (an earlier cancel included,
/// `AlreadyApplied`).
///
/// Panics: a lost claim, or an applied, replayed or refused cancel with no
/// observed status; `horsies_cancel_task` returns neither.
fn cancel_answer(task_id: Uuid, outcome: &TerminalizationOutcome) -> CancelAnswer {
    match outcome {
        TerminalizationOutcome::Applied {
            observed:
                ObservedTaskState {
                    status: Some(was_status),
                    ..
                },
            ..
        } => CancelAnswer::Cancelled {
            was_status: *was_status,
        },
        TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::WorkflowTask(ObservedWorkflowTask { observed_status }),
            ..
        } => CancelAnswer::WorkflowTask {
            current_status: *observed_status,
        },
        TerminalizationOutcome::SourceStateConflict {
            observed:
                ObservedTaskState {
                    status: Some(current_status),
                    ..
                },
            ..
        }
        | TerminalizationOutcome::AlreadyApplied {
            observed:
                ObservedTaskState {
                    status: Some(current_status),
                    ..
                },
            ..
        } => CancelAnswer::NotCancellable {
            current_status: *current_status,
        },
        TerminalizationOutcome::TaskAbsent { .. } => CancelAnswer::TaskAbsent,
        TerminalizationOutcome::Applied { .. }
        | TerminalizationOutcome::AlreadyApplied { .. }
        | TerminalizationOutcome::SourceStateConflict { .. } => {
            panic!("administrative cancellation of task {task_id} reported no observed status")
        }
        TerminalizationOutcome::LostClaim { .. } => panic!(
            "administrative cancellation of task {task_id} reported a lost claim; the cancel \
             has no claim fence"
        ),
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::core::lifecycle::outcomes::ObservedForeignTerminalization;
    use crate::core::lifecycle::TerminalizationKind;

    fn task() -> Uuid {
        Uuid::from_u128(7)
    }

    fn observed(status: Option<TaskStatus>) -> ObservedTaskState {
        ObservedTaskState {
            status,
            worker_id: None,
            claimed_at: None,
        }
    }

    #[tokio::test]
    async fn a_start_check_with_no_queue_sends_nothing() {
        // No server listens on port 1: a check that reached the pool would
        // fail instead of answering.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(1))
            .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies")
            .expect("a lazy pool opens no connection");
        let broker = PostgresBroker::from_pool(pool, crate::ClientFamily::Rust);
        let check = QueueStartCheck {
            worker_id: "worker-1".to_owned(),
            queues: Vec::new(),
            started: vec![(task(), Utc::now())],
        };
        let starts = broker
            .count_queue_starts(&check)
            .await
            .expect("a check with no queue sends nothing, so it cannot fail");
        assert_eq!(starts, QueueStarts::default());
    }

    #[test]
    fn an_applied_cancel_names_the_status_it_left() {
        let answer = cancel_answer(
            task(),
            &TerminalizationOutcome::Applied {
                task_id: task(),
                ordinality: None,
                terminal_at: Utc::now(),
                kind: TerminalizationKind::CancelAdmin,
                observed: observed(Some(TaskStatus::Claimed)),
            },
        );
        assert_eq!(
            answer,
            CancelAnswer::Cancelled {
                was_status: TaskStatus::Claimed
            }
        );
    }

    #[test]
    fn a_workflow_task_is_refused_with_its_status_live_or_finished() {
        for status in [TaskStatus::Pending, TaskStatus::Failed] {
            let answer = cancel_answer(
                task(),
                &TerminalizationOutcome::SourceStateConflict {
                    task_id: task(),
                    ordinality: None,
                    observed: observed(Some(status)),
                    evidence: GuardEvidence::WorkflowTask(ObservedWorkflowTask {
                        observed_status: status,
                    }),
                },
            );
            assert_eq!(
                answer,
                CancelAnswer::WorkflowTask {
                    current_status: status
                }
            );
        }
    }

    #[test]
    fn a_status_refusal_another_end_and_an_earlier_cancel_are_not_cancellable() {
        let running = TerminalizationOutcome::SourceStateConflict {
            task_id: task(),
            ordinality: None,
            observed: observed(Some(TaskStatus::Running)),
            evidence: GuardEvidence::Claim(crate::core::lifecycle::outcomes::ObservedClaim {
                worker_id: None,
                claimed_at: None,
            }),
        };
        let completed = TerminalizationOutcome::SourceStateConflict {
            task_id: task(),
            ordinality: None,
            observed: observed(Some(TaskStatus::Completed)),
            evidence: GuardEvidence::ForeignTerminalization(ObservedForeignTerminalization {
                observed_status: TaskStatus::Completed,
                committed_kind: TerminalizationKind::CompleteFused,
                terminal_at: Some(Utc::now()),
            }),
        };
        let cancelled = TerminalizationOutcome::AlreadyApplied {
            task_id: task(),
            ordinality: None,
            terminal_at: Utc::now(),
            kind: TerminalizationKind::CancelAdmin,
            observed: observed(Some(TaskStatus::Cancelled)),
        };
        for (outcome, status) in [
            (running, TaskStatus::Running),
            (completed, TaskStatus::Completed),
            (cancelled, TaskStatus::Cancelled),
        ] {
            assert_eq!(
                cancel_answer(task(), &outcome),
                CancelAnswer::NotCancellable {
                    current_status: status
                }
            );
        }
    }

    #[test]
    fn a_task_held_nowhere_is_absent() {
        let answer = cancel_answer(
            task(),
            &TerminalizationOutcome::TaskAbsent {
                task_id: task(),
                ordinality: None,
            },
        );
        assert_eq!(answer, CancelAnswer::TaskAbsent);
    }

    #[test]
    #[should_panic(expected = "reported a lost claim")]
    fn a_lost_claim_contradicts_the_cancel() {
        let _never = cancel_answer(
            task(),
            &TerminalizationOutcome::LostClaim {
                task_id: task(),
                ordinality: None,
                observed: observed(Some(TaskStatus::Pending)),
            },
        );
    }

    #[test]
    #[should_panic(expected = "reported no observed status")]
    fn an_applied_cancel_without_an_observed_status_contradicts_the_cancel() {
        let _never = cancel_answer(
            task(),
            &TerminalizationOutcome::Applied {
                task_id: task(),
                ordinality: None,
                terminal_at: Utc::now(),
                kind: TerminalizationKind::CancelAdmin,
                observed: observed(None),
            },
        );
    }
}

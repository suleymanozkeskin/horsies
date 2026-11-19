//! Workflow operations of a client handle on its main core pool, one 0005
//! function each (`crates/horsies-core/baseline/0005_workflow_functions.sql`):
//!
//! - [`PostgresBroker::pause_workflow`]: `horsies_workflow_pause`, the pause of
//!   a RUNNING workflow and its RUNNING descendants.
//! - [`PostgresBroker::cancel_workflow`]: `horsies_workflow_cancel`, the cancel
//!   of a workflow tree.
//! - [`PostgresBroker::resume_client_workflow`]: `horsies_workflow_resume`, the
//!   resume of a PAUSED tree, with its child starts from the client's source.
//! - [`PostgresBroker::expire_paused_workflows`]: `horsies_workflow_expire_paused`
//!   per candidate, the paused expiry of the reaper.
//! - [`PostgresBroker::delete_expired_workflows`]: the workflow retention
//!   delete of the reaper, in bounded batches (not a 0005 function: one
//!   statement, `crate::worker::recovery::DELETE_EXPIRED_WORKFLOWS_SQL`).
//!
//! These are the entry points of the Python client's workflow handle: it
//! calls them through `horsies._core` in place of its own statements. Each
//! checks the client family once per handle before its first write and holds
//! a write token while its statement runs, so [`PostgresBroker::close`] waits
//! for it.

use std::num::NonZeroU32;
use std::time::Duration;

use uuid::Uuid;

use crate::broker::error::BrokerError;
use crate::broker::postgres::PostgresBroker;
use crate::core::config::retention::PausedWorkflowAge;
use crate::core::workflow::handle_types::HandleResult;
use crate::db::{CheckoutClass, ParameterType, StatementBound, TypedStatement};
use crate::worker::recovery::DELETE_EXPIRED_WORKFLOWS_SQL;
use crate::workflow_engine::child_spec::ChildSpecSource;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::lifecycle;

/// The workflow retention delete: one batch of terminal workflows older than
/// `$1` hours (text) with no live backing task, at most `$2` rows by node
/// count (the first candidate always), with their node rows. Its row count
/// counts workflows.
static DELETE_EXPIRED_WORKFLOWS: TypedStatement = TypedStatement::new(
    "delete_expired_workflows",
    DELETE_EXPIRED_WORKFLOWS_SQL,
    &[ParameterType::Text, ParameterType::Int8],
);

/// The bounds of one workflow retention pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkflowRetentionPass {
    /// A terminal workflow older than this many hours is deleted.
    pub retention_hours: NonZeroU32,
    /// The most rows of one batch, counted by nodes (the first candidate of a
    /// batch always goes).
    pub batch_size: NonZeroU32,
    /// After this time no further batch starts; the first batch always runs.
    pub budget: Duration,
}

/// A workflow retention pass that failed.
#[derive(Debug)]
pub struct WorkflowRetentionFailed {
    /// The workflows deleted by the batches that committed before the failure.
    pub deleted_before: u64,
    /// The failure of the client family check, the checkout or the batch. After
    /// `OutcomeUnknown` the failed batch may have committed.
    pub error: BrokerError,
}

impl PostgresBroker {
    /// Pause a workflow tree: `horsies_workflow_pause`, one statement in
    /// autocommit (Python `pause_workflow`,
    /// `python/horsies/core/workflows/lifecycle.py`).
    ///
    /// Outcomes:
    /// - `Ok(true)`: the workflow was RUNNING and is PAUSED with its RUNNING
    ///   descendants; the claimed tasks of the paused tree are abandoned and
    ///   their nodes are READY with no task.
    /// - `Ok(false)`: the workflow exists and is not RUNNING; nothing was
    ///   written.
    /// - `WORKFLOW_NOT_FOUND`: no workflow has this id; nothing was written.
    /// - `DB_OPERATION_FAILED`: the client family check refused the handle, or
    ///   the statement failed; retryable by the core's rule. A lost reply may
    ///   have committed.
    ///
    /// Panics: a row outside the function's contract (a defect).
    pub async fn pause_workflow(&self, workflow_id: Uuid) -> HandleResult<bool> {
        lifecycle::pause_workflow(self, workflow_id).await
    }

    /// Cancel a workflow tree: `horsies_workflow_cancel`, one statement in
    /// autocommit (Python `WorkflowHandle.cancel_async`,
    /// `python/horsies/core/models/workflow/handle.py`).
    ///
    /// Outcomes:
    /// - `Ok(true)`: the workflow and every PENDING, RUNNING or PAUSED
    ///   descendant are CANCELLED; the tasks behind ENQUEUED nodes moved to
    ///   history as cancelled, and the nodes that did not start are SKIPPED.
    /// - `Ok(false)`: the workflow is COMPLETED, FAILED, CANCELLED or EXPIRED;
    ///   nothing was written.
    /// - `WORKFLOW_NOT_FOUND`, `DB_OPERATION_FAILED`: as
    ///   [`PostgresBroker::pause_workflow`].
    ///
    /// Panics: a row outside the function's contract (a defect).
    pub async fn cancel_workflow(&self, workflow_id: Uuid) -> HandleResult<bool> {
        lifecycle::cancel_workflow(self, workflow_id).await
    }

    /// Resume a workflow tree of a client that builds its children through
    /// `source` (Python `resume_workflow`,
    /// `python/horsies/core/workflows/lifecycle.py`): `horsies_workflow_resume`
    /// in a transaction with the child starts it owes, then the parent
    /// advances after the commit, then the recovery pass of the tree.
    ///
    /// Outcomes:
    /// - `Ok(true)`: the workflow was PAUSED and its tree is RUNNING.
    /// - `Ok(false)`: the workflow exists and is not PAUSED; nothing was
    ///   written.
    /// - `WORKFLOW_NOT_FOUND`: no workflow has this id; nothing was written.
    /// - `DB_OPERATION_FAILED`: the client family check, a statement, or a
    ///   child build that `source` did not run; nothing committed, or the
    ///   `COMMIT` reply was lost.
    /// - `STORED_DATA_INVALID`: an owed child met a value that does not
    ///   encode; nothing committed.
    /// - `RESUME_RECOVERY_FAILED`: the resume committed, and the parent
    ///   advance or the recovery pass after it failed.
    ///
    /// Panics: a row outside a function's contract, or a child tree that the
    /// core refuses (a defect).
    pub async fn resume_client_workflow(
        &self,
        workflow_id: Uuid,
        source: &dyn ChildSpecSource,
    ) -> HandleResult<bool> {
        lifecycle::resume_client_workflow(self, workflow_id, source).await
    }

    /// Expire the PAUSED workflows older than `older_than`, at most
    /// `batch_size` per call (Python `expire_paused_workflows`,
    /// `python/horsies/core/workflows/lifecycle.py`): one read of the
    /// candidates, oldest first, then one `horsies_workflow_expire_paused` per
    /// candidate in autocommit, on the main core pool with the reserve. A
    /// resume that committed first wins.
    ///
    /// The client family is checked once per handle before the first
    /// statement.
    ///
    /// Returns: the count of workflows that this call expired.
    ///
    /// Errors: the errors of
    /// [`crate::workflow_engine::lifecycle::expire_paused_workflows`], and a
    /// refused or failed client family check as [`WorkflowError::Broker`]. The
    /// candidates before a failed one are expired; the rest wait for the next
    /// call.
    pub async fn expire_paused_workflows(
        &self,
        older_than: PausedWorkflowAge,
        batch_size: NonZeroU32,
    ) -> Result<u64, WorkflowError> {
        self.ensure_client_family_for_write()
            .await
            .map_err(WorkflowError::Broker)?;
        lifecycle::expire_paused_workflows(self, older_than, i64::from(batch_size.get())).await
    }

    /// One workflow retention pass (the workflow part of the retention
    /// cleanup):
    /// batches of `pass.batch_size` until a batch deletes no workflow (the node
    /// budget keeps batches short while a backlog remains, so only an empty
    /// batch means drained), or until `pass.budget` has passed after a batch.
    /// The first batch always runs.
    ///
    /// Each batch is one statement in autocommit on the main core pool with the
    /// reserve ([`CheckoutClass::Reserved`]): the reaper calls this. The client
    /// family is checked once per handle before the first batch, and the write
    /// token is held for the pass.
    ///
    /// Returns: the count of workflows deleted.
    ///
    /// Errors: [`WorkflowRetentionFailed`] with the count of the batches that
    /// committed before the failure; the backlog resumes on the next pass.
    pub async fn delete_expired_workflows(
        &self,
        pass: WorkflowRetentionPass,
    ) -> Result<u64, WorkflowRetentionFailed> {
        let not_started = |error: BrokerError| WorkflowRetentionFailed {
            deleted_before: 0,
            error,
        };
        self.ensure_client_family_for_write()
            .await
            .map_err(not_started)?;
        let (open, _write) = self.core_pools_for_write().map_err(not_started)?;
        let hours = pass.retention_hours.get().to_string();
        let batch_size = i64::from(pass.batch_size.get());
        let started = tokio::time::Instant::now();
        let mut deleted: u64 = 0;
        loop {
            let bound = DELETE_EXPIRED_WORKFLOWS
                .bind()
                .value(&hours)
                .value(&batch_size)
                .finish();
            self.statement_log().log(&DELETE_EXPIRED_WORKFLOWS);
            let batch = open
                .main()
                .execute(
                    CheckoutClass::Reserved,
                    &bound,
                    StatementBound::ConnectionTimeouts,
                )
                .await
                .map_err(|failure| WorkflowRetentionFailed {
                    deleted_before: deleted,
                    error: BrokerError::from(failure),
                })?;
            deleted += batch;
            if batch == 0 {
                return Ok(deleted);
            }
            if started.elapsed() >= pass.budget {
                tracing::info!(
                    total_deleted = deleted,
                    "retention pass time budget reached; remaining backlog resumes next pass",
                );
                return Ok(deleted);
            }
        }
    }
}

//! Workflow operations of a client handle on its main core pool, one 0005
//! function each (`crates/horsies-core/baseline/0005_workflow_functions.sql`):
//!
//! - [`PostgresBroker::pause_workflow`]: `horsies_workflow_pause`, the pause of
//!   a RUNNING workflow and its RUNNING descendants.
//! - [`PostgresBroker::cancel_workflow`]: `horsies_workflow_cancel`, the cancel
//!   of a workflow tree.
//!
//! These are the entry points of the Python client's workflow handle: it
//! calls them through `horsies._core` in place of its own statements. Each
//! checks the client family once per handle before its first write and holds
//! a write token while its statement runs, so [`PostgresBroker::close`] waits
//! for it.

use uuid::Uuid;

use crate::broker::postgres::PostgresBroker;
use crate::core::workflow::handle_types::HandleResult;
use crate::workflow_engine::lifecycle;

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
}

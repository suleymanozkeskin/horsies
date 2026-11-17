//! The reads of a workflow handle on its main core pool, as the Python client's
//! `WorkflowHandle` sends them (`python/horsies/core/models/workflow/handle.py`):
//! the status, the wait for the end, the node results, one node result, and the
//! nodes. Each read returns the stored values as text; the client decodes a result
//! envelope with its own output type.
//!
//! The reads write nothing and run no client family check: a read of a database of
//! another family answers what it stores.

use std::time::Duration;

use uuid::Uuid;

use crate::broker::postgres::PostgresBroker;
use crate::core::workflow::handle_types::HandleResult;
use crate::core::WorkflowStatus;
use crate::workflow_engine::lifecycle::handle_operation_error;
use crate::workflow_engine::query::{
    self, NodeResultsRead, NodesRead, StoredNodeResult, WorkflowWait,
};

impl PostgresBroker {
    /// The status of a workflow: one read (Python `WorkflowHandle.status_async`).
    ///
    /// Outcomes: the status; `WORKFLOW_NOT_FOUND`; `STORED_DATA_INVALID` (not retryable) for a
    /// stored status outside the enum; `DB_OPERATION_FAILED` for a failed read, retryable by the
    /// core's rule.
    ///
    /// Panics: a row that does not decode (a defect).
    pub async fn workflow_status(&self, workflow_id: Uuid) -> HandleResult<WorkflowStatus> {
        query::get_workflow_status(self, workflow_id)
            .await
            .map_err(|error| handle_operation_error(&error, workflow_id))
    }

    /// Wait for the end of a workflow, up to `timeout` (none: no limit), with its stored values
    /// as text (Python `WorkflowHandle.get_async` before its decode). A finished workflow is one
    /// read; the wait subscribes on the broker's listener, or polls without one.
    ///
    /// Outcomes: [`WorkflowWait`]; `STORED_DATA_INVALID` for a stored status outside the enum;
    /// `DB_OPERATION_FAILED` for a failed read or a listener that stopped during the wait.
    ///
    /// Panics: a row that does not decode (a defect).
    pub async fn wait_workflow_end(
        &self,
        workflow_id: Uuid,
        timeout: Option<Duration>,
    ) -> HandleResult<WorkflowWait> {
        query::wait_workflow_end(self, workflow_id, timeout)
            .await
            .map_err(|error| handle_operation_error(&error, workflow_id))
    }

    /// The nodes of a workflow that store a result: one read (Python
    /// `WorkflowHandle.results_async`).
    ///
    /// Outcomes: [`NodeResultsRead`]; `DB_OPERATION_FAILED` for a failed read.
    ///
    /// Panics: a row that does not decode (a defect).
    pub async fn workflow_node_results(&self, workflow_id: Uuid) -> HandleResult<NodeResultsRead> {
        query::read_workflow_node_results(self, workflow_id)
            .await
            .map_err(|error| handle_operation_error(&error, workflow_id))
    }

    /// The stored result of one node: one read (Python `WorkflowHandle.result_for_async`).
    ///
    /// Outcomes: the node's stored result; `None` when no node of this id stores a result or no
    /// workflow has the id (the read does not tell the two apart); `DB_OPERATION_FAILED` for a
    /// failed read.
    ///
    /// Panics: a row that does not decode (a defect).
    pub async fn workflow_node_result(
        &self,
        workflow_id: Uuid,
        node_id: &str,
    ) -> HandleResult<Option<StoredNodeResult>> {
        query::read_workflow_node_result(self, workflow_id, node_id)
            .await
            .map_err(|error| handle_operation_error(&error, workflow_id))
    }

    /// The nodes of a workflow in index order: one read (Python `WorkflowHandle.tasks_async`).
    ///
    /// Outcomes: [`NodesRead`]; `DB_OPERATION_FAILED` for a failed read.
    ///
    /// Panics: a row that does not decode (a defect).
    pub async fn workflow_nodes(&self, workflow_id: Uuid) -> HandleResult<NodesRead> {
        query::read_workflow_nodes(self, workflow_id)
            .await
            .map_err(|error| handle_operation_error(&error, workflow_id))
    }
}

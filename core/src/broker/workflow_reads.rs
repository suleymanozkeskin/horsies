//! The reads of a workflow handle on its main core pool, as the Python client's
//! `WorkflowHandle` sends them (`clients/python/horsies/core/models/workflow/handle.py`):
//! the status, the wait for the end, the node results, one node result, and the
//! nodes. Each read returns the stored values as text; the client decodes a result
//! envelope with its own output type.
//!
//! The reads write nothing and run no client family check: a read of a database of
//! another family answers what it stores. The message of a failure starts with the label of the
//! read's [`HandleOperation`], as Python's handle names it.

use std::time::Duration;

use uuid::Uuid;

use crate::broker::postgres::PostgresBroker;
use crate::core::workflow::handle_types::{HandleOperation, HandleResult};
use crate::core::WorkflowStatus;
use crate::workflow_engine::lifecycle::workflow_handle_error;
use crate::workflow_engine::query::{
    self, NodeResultRead, NodeResultsRead, NodesRead, StoredNodeResult, WorkflowWait,
};

impl PostgresBroker {
    /// The status of a workflow: one read (Python `WorkflowHandle.status_async`).
    ///
    /// Outcomes: the status; `WORKFLOW_NOT_FOUND`; `STORED_DATA_INVALID` (not retryable) for a
    /// stored status outside the enum; `DATABASE_OPERATION_FAILED` for a failed read, retryable by the
    /// core's rule.
    ///
    /// Panics: a row that does not decode (a defect).
    pub async fn workflow_status(&self, workflow_id: Uuid) -> HandleResult<WorkflowStatus> {
        query::get_workflow_status(self, workflow_id)
            .await
            .map_err(|error| workflow_handle_error(&HandleOperation::Status, error, workflow_id))
    }

    /// Wait for the end of a workflow, up to `timeout` (none: no limit), with its stored values
    /// as text (Python `WorkflowHandle.get_async` before its decode). A finished workflow is one
    /// read; the wait subscribes on the broker's listener, or polls without one.
    ///
    /// Outcomes: [`WorkflowWait`]; `STORED_DATA_INVALID` for a stored status outside the enum;
    /// `DATABASE_OPERATION_FAILED` for a failed read or a listener that stopped during the wait.
    ///
    /// Panics: a row that does not decode (a defect).
    pub async fn wait_workflow_end(
        &self,
        workflow_id: Uuid,
        timeout: Option<Duration>,
    ) -> HandleResult<WorkflowWait> {
        query::wait_workflow_end(self, workflow_id, timeout)
            .await
            .map_err(|error| workflow_handle_error(&HandleOperation::Get, error, workflow_id))
    }

    /// The nodes of a workflow that store a result: one read (Python
    /// `WorkflowHandle.results_async`).
    ///
    /// Outcomes: [`NodeResultsRead`]; `DATABASE_OPERATION_FAILED` for a failed read.
    ///
    /// Panics: a row that does not decode (a defect).
    pub async fn workflow_node_results(&self, workflow_id: Uuid) -> HandleResult<NodeResultsRead> {
        query::read_workflow_node_results(self, workflow_id)
            .await
            .map_err(|error| workflow_handle_error(&HandleOperation::Results, error, workflow_id))
    }

    /// The result of one node: one read (Python `WorkflowHandle.result_for_async`).
    ///
    /// Outcomes: [`NodeResultRead`]: the node's stored result, `NotStored` for a node that has none
    /// yet, `NodeAbsent` for a workflow with no node of this id, `WorkflowAbsent` for an id that no
    /// workflow has; `DATABASE_OPERATION_FAILED` for a failed read, with a message that names the
    /// node.
    ///
    /// Panics: a row that does not decode (a defect).
    pub async fn workflow_node_result_read(
        &self,
        workflow_id: Uuid,
        node_id: &str,
    ) -> HandleResult<NodeResultRead> {
        query::read_workflow_node_result(self, workflow_id, node_id)
            .await
            .map_err(|error| {
                let operation = HandleOperation::ResultFor {
                    node_id: node_id.to_owned(),
                };
                workflow_handle_error(&operation, error, workflow_id)
            })
    }

    /// The stored result of one node: [`Self::workflow_node_result_read`] with the three states
    /// without a result collapsed into `None`. A caller that must tell them apart reads
    /// `workflow_node_result_read`.
    ///
    /// Outcomes: the node's stored result; `None` when no node of this id stores a result, the
    /// workflow has no such node, or no workflow has the id; `DATABASE_OPERATION_FAILED`.
    ///
    /// Panics: a row that does not decode (a defect).
    pub async fn workflow_node_result(
        &self,
        workflow_id: Uuid,
        node_id: &str,
    ) -> HandleResult<Option<StoredNodeResult>> {
        Ok(
            match self.workflow_node_result_read(workflow_id, node_id).await? {
                NodeResultRead::Stored(stored) => Some(stored),
                NodeResultRead::WorkflowAbsent
                | NodeResultRead::NodeAbsent
                | NodeResultRead::NotStored => None,
            },
        )
    }

    /// The nodes of a workflow in index order: one read (Python `WorkflowHandle.tasks_async`).
    ///
    /// Outcomes: [`NodesRead`]; `DATABASE_OPERATION_FAILED` for a failed read.
    ///
    /// Panics: a row that does not decode (a defect).
    pub async fn workflow_nodes(&self, workflow_id: Uuid) -> HandleResult<NodesRead> {
        query::read_workflow_nodes(self, workflow_id)
            .await
            .map_err(|error| workflow_handle_error(&HandleOperation::Tasks, error, workflow_id))
    }
}

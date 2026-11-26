//! The node read of a task by its id (`PostgresBroker::read_task_workflow_node`):
//! the workflow facts of the span of an attempt that the worker ended, which
//! the handler child reads only for such an attempt of a workflow task.
//!
//! The outcome is `TaskWorkflowNodeLinked`, `TaskWorkflowNodeNotLinked`, or
//! the generated broker error class with `TASK_INFO_QUERY_FAILED`. No schema
//! step runs. The read is bounded by the core
//! (`TASK_WORKFLOW_NODE_READ_END`).

use horsies_core::{
    DatabaseOperationError, DatabaseOperationFailedCode, BrokerOperationSource, TaskWorkflowNode,
};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::handle::BrokerConnection;
use crate::records::BrokerFailure;

/// The subject of the exception of a failed read.
const SUBJECT: &str = "task_workflow_node";

/// The node at `node_index` of the workflow `workflow_id` links the task;
/// `root_workflow_id` is the top-level workflow of its tree.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskWorkflowNodeLinked {
    pub workflow_id: Uuid,
    /// From 0 to `MAX_WORKFLOW_NODE_INDEX`.
    pub node_index: u32,
    pub root_workflow_id: Uuid,
}

/// No workflow node links the task now.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskWorkflowNodeNotLinked {
    pub task_id: Uuid,
}

/// The outcome of one node read.
pub enum TaskWorkflowNodeOutcome {
    Linked(TaskWorkflowNodeLinked),
    NotLinked(TaskWorkflowNodeNotLinked),
    Failed(BrokerFailure),
}

impl<'py> IntoPyObject<'py> for TaskWorkflowNodeOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Linked(linked) => linked.into_bound_py_any(py),
            Self::NotLinked(not_linked) => not_linked.into_bound_py_any(py),
            Self::Failed(failure) => failure.into_pyobject(py),
        }
    }
}

/// The record of the node that the core read for `task_id`. Pure.
pub fn node_outcome(task_id: Uuid, node: TaskWorkflowNode) -> TaskWorkflowNodeOutcome {
    match node {
        TaskWorkflowNode::Linked {
            workflow_id,
            node_index,
            root_workflow_id,
        } => TaskWorkflowNodeOutcome::Linked(TaskWorkflowNodeLinked {
            workflow_id,
            node_index: node_index.get(),
            root_workflow_id,
        }),
        TaskWorkflowNode::NotLinked => {
            TaskWorkflowNodeOutcome::NotLinked(TaskWorkflowNodeNotLinked { task_id })
        }
    }
}

/// The failure of the read of `task_id` with `source`: `TASK_INFO_QUERY_FAILED`,
/// retryable as its source. Pure.
fn read_failed(task_id: Uuid, source: BrokerOperationSource) -> TaskWorkflowNodeOutcome {
    TaskWorkflowNodeOutcome::Failed(BrokerFailure::of_operation_error(
        SUBJECT,
        DatabaseOperationError::operation_failed(
            DatabaseOperationFailedCode::TaskInfoQueryFailed,
            format!("task_workflow_node failed for {task_id}: {source}"),
            source,
        ),
    ))
}

/// Connect the handle's broker (no schema step) and read the node of
/// `task_id`. A failed connect or read is `TASK_INFO_QUERY_FAILED`.
pub async fn task_workflow_node_operation(
    connection: BrokerConnection,
    task_id: Uuid,
) -> TaskWorkflowNodeOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return read_failed(task_id, BrokerOperationSource::Broker(error)),
    };
    match broker.read_task_workflow_node(task_id).await {
        Ok(node) => node_outcome(task_id, node),
        Err(error) => read_failed(task_id, BrokerOperationSource::Broker(error)),
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::WorkflowNodeIndex;

    use super::*;

    #[test]
    fn a_linked_node_and_no_node_give_their_records() {
        let task_id = Uuid::from_u128(7);
        let workflow_id = Uuid::from_u128(0xf007);
        let root_workflow_id = Uuid::from_u128(0xf001);
        let linked = node_outcome(
            task_id,
            TaskWorkflowNode::Linked {
                workflow_id,
                node_index: WorkflowNodeIndex::try_from(3).expect("3 is a node index"),
                root_workflow_id,
            },
        );
        assert!(matches!(
            linked,
            TaskWorkflowNodeOutcome::Linked(TaskWorkflowNodeLinked { node_index: 3, .. })
        ));
        assert!(matches!(
            node_outcome(task_id, TaskWorkflowNode::NotLinked),
            TaskWorkflowNodeOutcome::NotLinked(TaskWorkflowNodeNotLinked { .. })
        ));
    }
}

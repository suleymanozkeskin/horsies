//! The reads of a workflow handle through the core
//! (`PostgresBroker::workflow_status`, `wait_workflow_end`,
//! `workflow_node_results`, `workflow_node_result`, `workflow_nodes`).
//!
//! Each read returns the stored values as text, one record class per answer.
//! The Python `WorkflowHandle` decodes a result envelope with its own output
//! type, as before the switch. A failed read is the generated class of the
//! core's `HandleOperationError` (`HandleDbOperationFailed` or
//! `HandleStoredDataInvalid`, `HandleFailure` in `workflow_lifecycle`) with
//! the core's message; no workflow is `WorkflowAbsent`. A row that does not
//! decode panics in the core and raises `PanicException`.

use std::time::Duration;

use chrono::{DateTime, Utc};
use horsies_core::catalog::CodeEnum;
use horsies_core::{
    BrokerError, HandleOperation, HandleOperationError, HandleResult, NodeResultsRead, NodesRead,
    StoredNode, StoredNodeResult, WorkflowEnd, WorkflowStatus, WorkflowWait,
};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::handle::BrokerConnection;
use crate::operations::workflow_lifecycle::{HandleFailure, WorkflowAbsent};

/// The stored status of a workflow.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowStatusRead {
    pub workflow_id: Uuid,
    /// A wire value of the workflow status enum.
    pub status: String,
}

/// The workflow is COMPLETED, with its stored result envelope (NULL only when
/// a write outside the library stored it so).
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowCompletedRead {
    pub workflow_id: Uuid,
    pub result_json: Option<String>,
}

/// The workflow is FAILED, CANCELLED or EXPIRED, with its stored error.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowEndedRead {
    pub workflow_id: Uuid,
    /// `FAILED`, `CANCELLED` or `EXPIRED`.
    pub status: String,
    pub error_json: Option<String>,
}

/// The workflow is PAUSED.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkflowPausedRead {
    pub workflow_id: Uuid,
}

/// The workflow is PENDING or RUNNING after the timeout.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WaitTimedOut {
    pub workflow_id: Uuid,
}

/// One node that stores a result.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StoredNodeResultRecord {
    pub node_id: Option<String>,
    pub task_name: String,
    /// The stored result envelope.
    pub result_json: String,
    pub is_sub_workflow: bool,
    pub sub_definition_key: Option<String>,
}

impl From<StoredNodeResult> for StoredNodeResultRecord {
    fn from(node: StoredNodeResult) -> Self {
        Self {
            node_id: node.node_id,
            task_name: node.task_name,
            result_json: node.result,
            is_sub_workflow: node.is_subworkflow,
            sub_definition_key: node.sub_definition_key,
        }
    }
}

/// The nodes of a workflow that store a result; empty when none does.
#[pyclass(module = "horsies._core", frozen)]
pub struct WorkflowNodeResultsRead {
    #[pyo3(get)]
    workflow_id: Uuid,
    nodes: Vec<StoredNodeResultRecord>,
}

#[pymethods]
impl WorkflowNodeResultsRead {
    /// `StoredNodeResultRecord` per node that stores a result.
    #[getter]
    fn nodes(&self) -> Vec<StoredNodeResultRecord> {
        self.nodes.clone()
    }
}

/// No node of this id stores a result, or no workflow has the id; the read
/// does not tell the two apart.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct NodeResultNotStored {
    pub workflow_id: Uuid,
    pub node_id: String,
}

/// One node of a workflow, with its stored values as text.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StoredNodeRecord {
    pub node_id: Option<String>,
    pub task_index: i32,
    pub task_name: String,
    /// The stored node status.
    pub status: String,
    pub result_json: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub sub_workflow_id: Option<Uuid>,
    pub sub_workflow_summary_json: Option<String>,
    pub is_sub_workflow: bool,
    pub sub_definition_key: Option<String>,
}

impl From<StoredNode> for StoredNodeRecord {
    fn from(node: StoredNode) -> Self {
        Self {
            node_id: node.node_id,
            task_index: node.task_index,
            task_name: node.task_name,
            status: node.status,
            result_json: node.result,
            started_at: node.started_at,
            completed_at: node.completed_at,
            sub_workflow_id: node.sub_workflow_id,
            sub_workflow_summary_json: node.sub_workflow_summary,
            is_sub_workflow: node.is_subworkflow,
            sub_definition_key: node.sub_definition_key,
        }
    }
}

/// The nodes of a workflow in index order; empty when it has none.
#[pyclass(module = "horsies._core", frozen)]
pub struct WorkflowNodesRead {
    #[pyo3(get)]
    workflow_id: Uuid,
    nodes: Vec<StoredNodeRecord>,
}

#[pymethods]
impl WorkflowNodesRead {
    /// `StoredNodeRecord` per node, in index order.
    #[getter]
    fn nodes(&self) -> Vec<StoredNodeRecord> {
        self.nodes.clone()
    }
}

/// The answer of one handle read.
pub enum WorkflowReadOutcome {
    Status(WorkflowStatusRead),
    Completed(WorkflowCompletedRead),
    Ended(WorkflowEndedRead),
    Paused(WorkflowPausedRead),
    TimedOut(WaitTimedOut),
    NodeResults(WorkflowNodeResultsRead),
    NodeResult(StoredNodeResultRecord),
    NodeResultNotStored(NodeResultNotStored),
    Nodes(WorkflowNodesRead),
    Absent(WorkflowAbsent),
    Failed(HandleFailure),
}

impl<'py> IntoPyObject<'py> for WorkflowReadOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Status(read) => read.into_bound_py_any(py),
            Self::Completed(read) => read.into_bound_py_any(py),
            Self::Ended(read) => read.into_bound_py_any(py),
            Self::Paused(read) => read.into_bound_py_any(py),
            Self::TimedOut(read) => read.into_bound_py_any(py),
            Self::NodeResults(read) => read.into_bound_py_any(py),
            Self::NodeResult(read) => read.into_bound_py_any(py),
            Self::NodeResultNotStored(read) => read.into_bound_py_any(py),
            Self::Nodes(read) => read.into_bound_py_any(py),
            Self::Absent(absent) => absent.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_pyobject(py),
        }
    }
}

/// The record of a handle error of a read. Pure.
///
/// Panics: a code that a read never gives (`RESUME_RECOVERY_FAILED`) or that
/// only a sync or blocking form gives (`ASYNC_CONTEXT`): a defect of the
/// core.
fn failed_read(
    operation: HandleOperation,
    workflow_id: Uuid,
    error: HandleOperationError,
) -> WorkflowReadOutcome {
    match error {
        HandleOperationError::WorkflowNotFound { .. } => {
            WorkflowReadOutcome::Absent(WorkflowAbsent { workflow_id })
        }
        HandleOperationError::DbOperationFailed { .. }
        | HandleOperationError::StoredDataInvalid { .. } => {
            WorkflowReadOutcome::Failed(HandleFailure { operation, error })
        }
        HandleOperationError::ResumeRecoveryFailed { .. }
        | HandleOperationError::AsyncContext { .. } => panic!(
            "a read of workflow {workflow_id} answered {} (a defect): {}",
            error.code(),
            error.message()
        ),
    }
}

/// The record of a failed connect of the read `operation`. Pure.
fn connect_failed(
    operation: HandleOperation,
    workflow_id: Uuid,
    error: BrokerError,
) -> WorkflowReadOutcome {
    WorkflowReadOutcome::Failed(HandleFailure::connect_failed(operation, workflow_id, error))
}

/// The record of a status read. Pure.
fn status_outcome(workflow_id: Uuid, answer: HandleResult<WorkflowStatus>) -> WorkflowReadOutcome {
    match answer {
        Ok(status) => WorkflowReadOutcome::Status(WorkflowStatusRead {
            workflow_id,
            status: status.wire_value().to_owned(),
        }),
        Err(error) => failed_read(HandleOperation::Status, workflow_id, error),
    }
}

/// The record of a wait. Pure.
fn wait_outcome(workflow_id: Uuid, answer: HandleResult<WorkflowWait>) -> WorkflowReadOutcome {
    match answer {
        Ok(WorkflowWait::Ended(WorkflowEnd::Completed { result })) => {
            WorkflowReadOutcome::Completed(WorkflowCompletedRead {
                workflow_id,
                result_json: result,
            })
        }
        Ok(WorkflowWait::Ended(WorkflowEnd::Ended { status, error })) => {
            WorkflowReadOutcome::Ended(WorkflowEndedRead {
                workflow_id,
                status: status.wire_value().to_owned(),
                error_json: error,
            })
        }
        Ok(WorkflowWait::Ended(WorkflowEnd::Paused)) => {
            WorkflowReadOutcome::Paused(WorkflowPausedRead { workflow_id })
        }
        Ok(WorkflowWait::NotFound) => WorkflowReadOutcome::Absent(WorkflowAbsent { workflow_id }),
        Ok(WorkflowWait::TimedOut) => WorkflowReadOutcome::TimedOut(WaitTimedOut { workflow_id }),
        Err(error) => failed_read(HandleOperation::Get, workflow_id, error),
    }
}

/// The record of a node results read. Pure.
fn node_results_outcome(
    workflow_id: Uuid,
    answer: HandleResult<NodeResultsRead>,
) -> WorkflowReadOutcome {
    match answer {
        Ok(NodeResultsRead::WorkflowAbsent) => {
            WorkflowReadOutcome::Absent(WorkflowAbsent { workflow_id })
        }
        Ok(NodeResultsRead::Nodes(nodes)) => {
            WorkflowReadOutcome::NodeResults(WorkflowNodeResultsRead {
                workflow_id,
                nodes: nodes
                    .into_iter()
                    .map(StoredNodeResultRecord::from)
                    .collect(),
            })
        }
        Err(error) => failed_read(HandleOperation::Results, workflow_id, error),
    }
}

/// The record of a one-node result read. Pure.
fn node_result_outcome(
    workflow_id: Uuid,
    node_id: String,
    answer: HandleResult<Option<StoredNodeResult>>,
) -> WorkflowReadOutcome {
    match answer {
        Ok(Some(node)) => WorkflowReadOutcome::NodeResult(StoredNodeResultRecord::from(node)),
        Ok(None) => WorkflowReadOutcome::NodeResultNotStored(NodeResultNotStored {
            workflow_id,
            node_id,
        }),
        Err(error) => failed_read(HandleOperation::ResultFor { node_id }, workflow_id, error),
    }
}

/// The record of a nodes read. Pure.
fn nodes_outcome(workflow_id: Uuid, answer: HandleResult<NodesRead>) -> WorkflowReadOutcome {
    match answer {
        Ok(NodesRead::WorkflowAbsent) => {
            WorkflowReadOutcome::Absent(WorkflowAbsent { workflow_id })
        }
        Ok(NodesRead::Nodes(nodes)) => WorkflowReadOutcome::Nodes(WorkflowNodesRead {
            workflow_id,
            nodes: nodes.into_iter().map(StoredNodeRecord::from).collect(),
        }),
        Err(error) => failed_read(HandleOperation::Tasks, workflow_id, error),
    }
}

/// The status of one workflow. Reads only.
pub async fn workflow_status_operation(
    connection: BrokerConnection,
    workflow_id: Uuid,
) -> WorkflowReadOutcome {
    match connection.broker().await {
        Ok(broker) => status_outcome(workflow_id, broker.workflow_status(workflow_id).await),
        Err(error) => connect_failed(HandleOperation::Status, workflow_id, error),
    }
}

/// Wait for the end of one workflow, up to `timeout_ms` (none: no limit).
/// Reads only; a caller that leaves drops the wait and its subscription.
pub async fn wait_workflow_result_operation(
    connection: BrokerConnection,
    workflow_id: Uuid,
    timeout_ms: Option<u64>,
) -> WorkflowReadOutcome {
    let timeout = timeout_ms.map(Duration::from_millis);
    match connection.broker().await {
        Ok(broker) => wait_outcome(
            workflow_id,
            broker.wait_workflow_end(workflow_id, timeout).await,
        ),
        Err(error) => connect_failed(HandleOperation::Get, workflow_id, error),
    }
}

/// The nodes of one workflow that store a result. Reads only.
pub async fn workflow_node_results_operation(
    connection: BrokerConnection,
    workflow_id: Uuid,
) -> WorkflowReadOutcome {
    match connection.broker().await {
        Ok(broker) => {
            node_results_outcome(workflow_id, broker.workflow_node_results(workflow_id).await)
        }
        Err(error) => connect_failed(HandleOperation::Results, workflow_id, error),
    }
}

/// The stored result of one node. Reads only.
pub async fn workflow_node_result_operation(
    connection: BrokerConnection,
    workflow_id: Uuid,
    node_id: String,
) -> WorkflowReadOutcome {
    match connection.broker().await {
        Ok(broker) => {
            let answer = broker.workflow_node_result(workflow_id, &node_id).await;
            node_result_outcome(workflow_id, node_id, answer)
        }
        Err(error) => connect_failed(HandleOperation::ResultFor { node_id }, workflow_id, error),
    }
}

/// The nodes of one workflow. Reads only.
pub async fn workflow_nodes_operation(
    connection: BrokerConnection,
    workflow_id: Uuid,
) -> WorkflowReadOutcome {
    match connection.broker().await {
        Ok(broker) => nodes_outcome(workflow_id, broker.workflow_nodes(workflow_id).await),
        Err(error) => connect_failed(HandleOperation::Tasks, workflow_id, error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use horsies_core::{HandleErrorCode, TerminalErrorStatus};

    fn workflow() -> Uuid {
        Uuid::from_u128(11)
    }

    fn handle_error(code: HandleErrorCode, retryable: bool) -> HandleOperationError {
        let message = format!("planned {code}");
        let workflow_id = workflow();
        let cause = || {
            horsies_core::WorkflowError::Broker(BrokerError::ConnectionFailed("refused".to_owned()))
        };
        match code {
            HandleErrorCode::WorkflowNotFound => HandleOperationError::WorkflowNotFound {
                message,
                workflow_id,
            },
            HandleErrorCode::DbOperationFailed => HandleOperationError::DbOperationFailed {
                message,
                retryable,
                workflow_id,
                cause: cause(),
            },
            HandleErrorCode::StoredDataInvalid => HandleOperationError::StoredDataInvalid {
                message,
                workflow_id,
                cause: horsies_core::WorkflowError::InvalidStatus("BOGUS".to_owned()),
            },
            HandleErrorCode::ResumeRecoveryFailed => HandleOperationError::ResumeRecoveryFailed {
                message,
                workflow_id,
                cause: cause(),
            },
            HandleErrorCode::AsyncContext => HandleOperationError::AsyncContext {
                message,
                workflow_id,
            },
        }
    }

    #[test]
    fn each_end_of_a_wait_has_its_own_record() {
        assert!(matches!(
            wait_outcome(
                workflow(),
                Ok(WorkflowWait::Ended(WorkflowEnd::Completed {
                    result: Some("{}".to_owned())
                }))
            ),
            WorkflowReadOutcome::Completed(WorkflowCompletedRead { result_json: Some(ref text), .. })
                if text == "{}"
        ));
        let WorkflowReadOutcome::Ended(ended) = wait_outcome(
            workflow(),
            Ok(WorkflowWait::Ended(WorkflowEnd::Ended {
                status: TerminalErrorStatus::Expired,
                error: None,
            })),
        ) else {
            panic!("an EXPIRED workflow gives WorkflowEndedRead");
        };
        assert_eq!(ended.status, "EXPIRED");
        assert_eq!(ended.error_json, None);
        assert!(matches!(
            wait_outcome(workflow(), Ok(WorkflowWait::Ended(WorkflowEnd::Paused))),
            WorkflowReadOutcome::Paused(_)
        ));
        assert!(matches!(
            wait_outcome(workflow(), Ok(WorkflowWait::NotFound)),
            WorkflowReadOutcome::Absent(_)
        ));
        assert!(matches!(
            wait_outcome(workflow(), Ok(WorkflowWait::TimedOut)),
            WorkflowReadOutcome::TimedOut(_)
        ));
    }

    #[test]
    fn a_status_keeps_its_wire_value() {
        let WorkflowReadOutcome::Status(read) =
            status_outcome(workflow(), Ok(WorkflowStatus::Paused))
        else {
            panic!("a status gives WorkflowStatusRead");
        };
        assert_eq!(read.status, "PAUSED");
    }

    #[test]
    fn an_absent_workflow_and_a_workflow_with_no_node_are_two_records() {
        assert!(matches!(
            nodes_outcome(workflow(), Ok(NodesRead::WorkflowAbsent)),
            WorkflowReadOutcome::Absent(_)
        ));
        let WorkflowReadOutcome::Nodes(read) =
            nodes_outcome(workflow(), Ok(NodesRead::Nodes(vec![])))
        else {
            panic!("a workflow with no node gives WorkflowNodesRead");
        };
        assert!(read.nodes.is_empty());
        assert!(matches!(
            node_results_outcome(workflow(), Ok(NodeResultsRead::WorkflowAbsent)),
            WorkflowReadOutcome::Absent(_)
        ));
    }

    #[test]
    fn a_node_with_no_stored_result_names_the_node() {
        let WorkflowReadOutcome::NodeResultNotStored(read) =
            node_result_outcome(workflow(), "a".to_owned(), Ok(None))
        else {
            panic!("no stored result gives NodeResultNotStored");
        };
        assert_eq!(read.node_id, "a");
    }

    /// A failure stays the core's value, with the read's operation: Python
    /// reads its generated class with the operation's subject.
    #[test]
    fn a_failure_keeps_the_core_s_value_and_its_operation() {
        let WorkflowReadOutcome::Failed(failed) = status_outcome(
            workflow(),
            Err(handle_error(HandleErrorCode::StoredDataInvalid, false)),
        ) else {
            panic!("a stored status outside the enum gives a failure");
        };
        assert_eq!(failed.operation, HandleOperation::Status);
        assert_eq!(failed.error.code(), HandleErrorCode::StoredDataInvalid);
        assert!(!failed.error.retryable());
        let WorkflowReadOutcome::Failed(failed) = node_result_outcome(
            workflow(),
            "a".to_owned(),
            Err(handle_error(HandleErrorCode::DbOperationFailed, true)),
        ) else {
            panic!("a failed read gives a failure");
        };
        assert_eq!(
            failed.operation,
            HandleOperation::ResultFor {
                node_id: "a".to_owned()
            }
        );
        assert!(failed.error.retryable());
        let WorkflowReadOutcome::Failed(failed) = connect_failed(
            HandleOperation::Get,
            workflow(),
            BrokerError::ConnectionFailed("refused".to_owned()),
        ) else {
            panic!("a failed connect gives a failure");
        };
        assert_eq!(failed.operation, HandleOperation::Get);
        assert_eq!(
            failed.error.message(),
            format!(
                "Get failed for workflow {}: connection failed: refused",
                workflow()
            )
        );
        assert!(matches!(
            status_outcome(
                workflow(),
                Err(handle_error(HandleErrorCode::WorkflowNotFound, false))
            ),
            WorkflowReadOutcome::Absent(_)
        ));
    }

    /// A failed connect of a node result read names the node, in Python
    /// 0.7's text, and keeps the read's subject.
    #[test]
    fn a_failed_node_result_connect_names_the_node() {
        let WorkflowReadOutcome::Failed(failed) = connect_failed(
            HandleOperation::ResultFor {
                node_id: "fetch".to_owned(),
            },
            workflow(),
            BrokerError::ConnectionFailed("refused".to_owned()),
        ) else {
            panic!("a failed connect gives a failure");
        };
        assert_eq!(failed.operation.subject(), "workflow_node_result");
        assert_eq!(
            failed.error.message(),
            format!(
                "DB query failed for result_for node fetch in workflow {}: connection failed: \
                 refused",
                workflow()
            )
        );
        assert!(failed.error.retryable());
    }

    #[test]
    #[should_panic(expected = "a defect")]
    fn a_code_that_no_read_gives_is_a_defect() {
        let _ = nodes_outcome(
            workflow(),
            Err(handle_error(HandleErrorCode::ResumeRecoveryFailed, false)),
        );
    }
}

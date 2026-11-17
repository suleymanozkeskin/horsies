//! The handle of a started workflow (Python `WorkflowHandle`): its status,
//! its result, the results of its nodes, and pause, resume and cancel.
//!
//! The core reads the stored values as text; the handle decodes them with
//! its types. Two error forms, as Python's:
//!
//! - `get` and `result_for` return `Result<TaskOutcome<..>, WorkflowReadError>`:
//!   the outer error is the read failing (the wait limit passed, the
//!   workflow is not found, the stored value does not read, the database
//!   failed); the inner value is the outcome. A workflow holds the codes of
//!   many tasks, so `get` reads user codes as checked text (`UserCode`);
//!   `result_for` reads one node with that node's types.
//! - `status`, `results`, `tasks`, `cancel`, `pause` and `resume` return
//!   `HandleResult`, with Python's handle codes.

use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use horsies_core::core::codec::from_json_str;
use horsies_core::handle::CoreHandle;
use horsies_core::{
    process_role, resume_workflow, wait_inside_task_message, BuiltInTaskCode, HandleErrorCode,
    HandleOperationError, HandleResult, NodeResultsRead, NodesRead, OperationalErrorCode,
    OutcomeCode, PostgresBroker, ProcessRole, RetrievalCode, StoredNode, SubWorkflowSummary,
    TaskResult, TerminalErrorStatus, UserCode, WaitLimit, WorkflowEnd, WorkflowStatus,
    WorkflowTaskStatus, WorkflowWait,
};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::dependency::OutputlessResults;
use super::node::NodeKey;
use super::registry::WorkflowRegistry;
use crate::handle::limit_duration;
use crate::task::body::value_free_decode_text;
use crate::{TaskError, TaskOutcome, UnknownUserCode, UserErrorCode};

/// The marker key of the result of a workflow with no output node.
const OUTPUTLESS_TERMINALS_KEY: &str = "__h_outputless_terminals__";

/// Why a workflow read gave no outcome.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WorkflowReadError {
    /// `WAIT_TIMEOUT`: the workflow had not ended when the wait limit
    /// passed. A later read may find it ended.
    #[error("timed out waiting for workflow {workflow_id}; it may still run")]
    WaitTimedOut { workflow_id: Uuid },
    /// `WORKFLOW_NOT_FOUND`: no workflow has the id.
    #[error("workflow {workflow_id} not found")]
    WorkflowNotFound { workflow_id: Uuid },
    /// `RESULT_NOT_READY`: the node stores no result yet, or the workflow
    /// has no node of this id.
    #[error("node '{node_id}' of workflow {workflow_id} stores no result")]
    ResultNotReady { workflow_id: Uuid, node_id: String },
    /// `RESULT_DESERIALIZATION_ERROR`: the stored value does not read with
    /// the handle's types. `cause` names no value.
    #[error("the stored value of workflow {workflow_id} does not read: {cause}")]
    StoredResultUnreadable { workflow_id: Uuid, cause: String },
    /// The stored error has a user code that the asked code type does not
    /// name.
    #[error("the stored error of workflow {workflow_id}: {cause}")]
    UnknownUserCode {
        workflow_id: Uuid,
        cause: UnknownUserCode,
    },
    /// `WAIT_INSIDE_TASK`: a result wait in a process that runs task bodies.
    /// Nothing was read.
    #[error("{message}")]
    WaitInsideTask { message: String },
    /// `BROKER_ERROR`: the database or the connection failed, with the
    /// handle code; `retryable` is the classification of the cause.
    #[error("the read failed ({code}): {message}")]
    Database {
        code: HandleErrorCode,
        message: String,
        retryable: bool,
    },
}

impl WorkflowReadError {
    /// The library code of this failure, as Python's handle stores it.
    pub fn code(&self) -> BuiltInTaskCode {
        match self {
            Self::WaitTimedOut { .. } => RetrievalCode::WaitTimeout.into(),
            Self::WorkflowNotFound { .. } => RetrievalCode::WorkflowNotFound.into(),
            Self::ResultNotReady { .. } => RetrievalCode::ResultNotReady.into(),
            Self::StoredResultUnreadable { .. } | Self::UnknownUserCode { .. } => {
                OperationalErrorCode::ResultDeserializationError.into()
            }
            Self::WaitInsideTask { .. } => RetrievalCode::WaitInsideTask.into(),
            Self::Database { .. } => OperationalErrorCode::BrokerError.into(),
        }
    }

    fn of_handle_error(error: HandleOperationError) -> Self {
        match error.code {
            HandleErrorCode::WorkflowNotFound => Self::WorkflowNotFound {
                workflow_id: error.workflow_id,
            },
            HandleErrorCode::StoredDataInvalid => Self::StoredResultUnreadable {
                workflow_id: error.workflow_id,
                cause: error.message,
            },
            code @ (HandleErrorCode::DbOperationFailed
            | HandleErrorCode::ResumeRecoveryFailed
            | HandleErrorCode::LoopRunnerFailed
            | HandleErrorCode::EventLoopLimit
            | HandleErrorCode::AsyncContext) => Self::Database {
                code,
                message: error.message,
                retryable: error.retryable,
            },
        }
    }
}

/// The stored result of one node, read with no type: its outcome with the
/// value as JSON and the user codes as text, or why it does not read.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeResult {
    Outcome(TaskOutcome<serde_json::Value, UserCode>),
    /// The stored text does not read as a result envelope.
    Unreadable {
        cause: String,
    },
}

/// One node of a workflow (Python `WorkflowTaskInfo`).
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowNodeInfo {
    /// The node id; `None` for a node stored without one.
    pub node_id: Option<String>,
    pub index: i32,
    /// The task name, or the child's workflow name.
    pub name: String,
    pub status: WorkflowTaskStatus,
    /// The stored result, when the node has one.
    pub result: Option<NodeResult>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    /// The child workflow of a sub-workflow node.
    pub sub_workflow_id: Option<Uuid>,
    /// The summary of the ended child of a sub-workflow node.
    pub sub_workflow_summary: Option<SubWorkflowSummary>,
}

/// The handle of one started workflow whose result type is `T`. Clones
/// share one workflow.
pub struct WorkflowHandle<T> {
    workflow_id: Uuid,
    reads: Arc<CoreHandle>,
    workflows: Arc<WorkflowRegistry>,
    types: PhantomData<fn() -> T>,
}

impl<T> Clone for WorkflowHandle<T> {
    fn clone(&self) -> Self {
        Self {
            workflow_id: self.workflow_id,
            reads: Arc::clone(&self.reads),
            workflows: Arc::clone(&self.workflows),
            types: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for WorkflowHandle<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowHandle")
            .field("workflow_id", &self.workflow_id)
            .finish_non_exhaustive()
    }
}

impl<T> WorkflowHandle<T> {
    /// The handle of `workflow_id`, read through `reads`; a resume builds
    /// the owed children from `workflows`.
    pub(crate) fn new(
        workflow_id: Uuid,
        reads: Arc<CoreHandle>,
        workflows: Arc<WorkflowRegistry>,
    ) -> Self {
        Self {
            workflow_id,
            reads,
            workflows,
            types: PhantomData,
        }
    }

    /// The workflow id.
    pub fn workflow_id(&self) -> Uuid {
        self.workflow_id
    }

    /// The connected broker of the handle, or `DB_OPERATION_FAILED`.
    async fn broker(&self) -> HandleResult<Arc<PostgresBroker>> {
        self.reads
            .initialized_broker()
            .await
            .map_err(|error| HandleOperationError {
                code: HandleErrorCode::DbOperationFailed,
                message: error.to_string(),
                retryable: error.is_retryable(),
                workflow_id: self.workflow_id,
            })
    }

    /// The status of the workflow (Python `status`).
    ///
    /// Errors: `WORKFLOW_NOT_FOUND`; `STORED_DATA_INVALID`;
    /// `DB_OPERATION_FAILED`.
    pub async fn status(&self) -> HandleResult<WorkflowStatus> {
        self.broker().await?.workflow_status(self.workflow_id).await
    }

    /// The stored result of each node that has one, by node id (Python
    /// `results`). A node stored without a node id is left out.
    ///
    /// Errors: `WORKFLOW_NOT_FOUND`; `DB_OPERATION_FAILED`.
    pub async fn results(&self) -> HandleResult<BTreeMap<String, NodeResult>> {
        match self
            .broker()
            .await?
            .workflow_node_results(self.workflow_id)
            .await?
        {
            NodeResultsRead::WorkflowAbsent => Err(self.not_found()),
            NodeResultsRead::Nodes(nodes) => Ok(nodes
                .into_iter()
                .filter_map(|node| {
                    node.node_id
                        .map(|node_id| (node_id, node_result(&node.result)))
                })
                .collect()),
        }
    }

    /// Every node of the workflow in index order (Python `tasks`).
    ///
    /// Errors: `WORKFLOW_NOT_FOUND`; `STORED_DATA_INVALID` for a stored
    /// node status or summary that does not read; `DB_OPERATION_FAILED`.
    pub async fn tasks(&self) -> HandleResult<Vec<WorkflowNodeInfo>> {
        match self
            .broker()
            .await?
            .workflow_nodes(self.workflow_id)
            .await?
        {
            NodesRead::WorkflowAbsent => Err(self.not_found()),
            NodesRead::Nodes(nodes) => nodes.into_iter().map(|node| self.node_info(node)).collect(),
        }
    }

    /// Cancel the workflow and its descendants (Python `cancel`). A
    /// workflow that already ended is left as it is.
    ///
    /// Errors: `WORKFLOW_NOT_FOUND`; `DB_OPERATION_FAILED`.
    pub async fn cancel(&self) -> HandleResult<()> {
        self.broker()
            .await?
            .cancel_workflow(self.workflow_id)
            .await
            .map(|_cancelled_now| ())
    }

    /// Pause a RUNNING workflow (Python `pause`): `true` when it paused,
    /// `false` when it is not RUNNING.
    ///
    /// Errors: `WORKFLOW_NOT_FOUND`; `DB_OPERATION_FAILED`.
    pub async fn pause(&self) -> HandleResult<bool> {
        self.broker().await?.pause_workflow(self.workflow_id).await
    }

    /// Resume a PAUSED workflow (Python `resume`): `true` when it resumed,
    /// `false` when it is not PAUSED. The children that the resume owes
    /// build from the app's registered definitions.
    ///
    /// Errors: `WORKFLOW_NOT_FOUND`; `DB_OPERATION_FAILED`;
    /// `STORED_DATA_INVALID`; `RESUME_RECOVERY_FAILED` (the resume
    /// committed, the work after it failed).
    pub async fn resume(&self) -> HandleResult<bool> {
        let broker = self.broker().await?;
        let config = self.workflows.config();
        resume_workflow(
            &broker,
            self.workflow_id,
            &self.workflows.core_registry(),
            &config.payload,
            &config.retention,
        )
        .await
    }

    /// The outcome of the node `key`, read with its output type `V` and the
    /// code type `E` (Python `result_for`). Not a wait.
    ///
    /// Errors: `ResultNotReady` while the node stores no result;
    /// `StoredResultUnreadable`; `UnknownUserCode`; `Database`.
    pub async fn result_for<V, E>(
        &self,
        key: &NodeKey<V>,
    ) -> Result<TaskOutcome<V, E>, WorkflowReadError>
    where
        V: DeserializeOwned,
        E: UserErrorCode,
    {
        let broker = self
            .broker()
            .await
            .map_err(WorkflowReadError::of_handle_error)?;
        let stored = broker
            .workflow_node_result(self.workflow_id, key.node_id())
            .await
            .map_err(WorkflowReadError::of_handle_error)?;
        let Some(stored) = stored else {
            return Err(WorkflowReadError::ResultNotReady {
                workflow_id: self.workflow_id,
                node_id: key.node_id().to_owned(),
            });
        };
        envelope_outcome(self.workflow_id, &stored.result)
    }

    fn not_found(&self) -> HandleOperationError {
        HandleOperationError {
            code: HandleErrorCode::WorkflowNotFound,
            message: format!("workflow {} not found", self.workflow_id),
            retryable: false,
            workflow_id: self.workflow_id,
        }
    }

    fn stored_data_invalid(&self, message: String) -> HandleOperationError {
        HandleOperationError {
            code: HandleErrorCode::StoredDataInvalid,
            message,
            retryable: false,
            workflow_id: self.workflow_id,
        }
    }

    fn node_info(&self, node: StoredNode) -> HandleResult<WorkflowNodeInfo> {
        let status = serde_json::from_value::<WorkflowTaskStatus>(serde_json::Value::String(
            node.status.clone(),
        ))
        .map_err(|_| {
            self.stored_data_invalid(format!(
                "node {} has the stored status {:?}, which is not a node status",
                node.task_index, node.status
            ))
        })?;
        let sub_workflow_summary = match node.sub_workflow_summary.as_deref() {
            None => None,
            Some(text) => Some(stored_summary(text).map_err(|cause| {
                self.stored_data_invalid(format!(
                    "node {} has a stored sub-workflow summary that does not read: {cause}",
                    node.task_index
                ))
            })?),
        };
        Ok(WorkflowNodeInfo {
            node_id: node.node_id,
            index: node.task_index,
            name: node.task_name,
            status,
            result: node.result.as_deref().map(node_result),
            started_at: node.started_at,
            completed_at: node.completed_at,
            sub_workflow_id: node.sub_workflow_id,
            sub_workflow_summary,
        })
    }
}

impl<T> WorkflowHandle<T>
where
    T: DeserializeOwned + 'static,
{
    /// The result of the workflow, waiting while it runs, up to `limit`
    /// (`WaitLimit::NoLimit` waits until it ends) (Python `get`).
    ///
    /// The inner value is the workflow's outcome: its output read as `T`
    /// (`OutputlessResults` for a workflow with no output node), or its
    /// error: the stored error of a FAILED, CANCELLED or EXPIRED workflow,
    /// the code of its status when it stored none, and `WORKFLOW_PAUSED` for
    /// a PAUSED workflow, which a resume can continue.
    ///
    /// In a process that runs task bodies the wait is refused with
    /// `WaitInsideTask` before any statement.
    ///
    /// Errors: [`WorkflowReadError`].
    pub async fn get(
        &self,
        limit: WaitLimit,
    ) -> Result<TaskOutcome<T, UserCode>, WorkflowReadError> {
        if process_role() == ProcessRole::RunsTaskBodies {
            return Err(WorkflowReadError::WaitInsideTask {
                message: wait_inside_task_message(&format!(
                    "WorkflowHandle.get() for workflow {}",
                    self.workflow_id
                )),
            });
        }
        let broker = self
            .broker()
            .await
            .map_err(WorkflowReadError::of_handle_error)?;
        let wait = broker
            .wait_workflow_end(self.workflow_id, limit_duration(limit))
            .await
            .map_err(WorkflowReadError::of_handle_error)?;
        match wait {
            WorkflowWait::TimedOut => Err(WorkflowReadError::WaitTimedOut {
                workflow_id: self.workflow_id,
            }),
            WorkflowWait::NotFound => Err(WorkflowReadError::WorkflowNotFound {
                workflow_id: self.workflow_id,
            }),
            WorkflowWait::Ended(end) => end_outcome(self.workflow_id, end),
        }
    }
}

/// The outcome of an ended workflow (Python `get` after its wait). Pure.
fn end_outcome<T>(
    workflow_id: Uuid,
    end: WorkflowEnd,
) -> Result<TaskOutcome<T, UserCode>, WorkflowReadError>
where
    T: DeserializeOwned + 'static,
{
    let unreadable =
        |cause: String| WorkflowReadError::StoredResultUnreadable { workflow_id, cause };
    match end {
        WorkflowEnd::Paused => Ok(TaskOutcome::Failed(TaskError::library(
            OutcomeCode::WorkflowPaused,
            "workflow is paused awaiting intervention",
        ))),
        WorkflowEnd::Ended { status, error } => match error.as_deref() {
            None => Ok(TaskOutcome::Failed(TaskError::library(
                status_code(status),
                status_message(status),
            ))),
            Some("") => Err(unreadable("Workflow error payload is empty".to_owned())),
            Some(text) => stored_error(workflow_id, text).map(TaskOutcome::Failed),
        },
        WorkflowEnd::Completed { result } => match result.as_deref() {
            None => Err(unreadable(format!(
                "Workflow {workflow_id} is COMPLETED and its stored result is NULL"
            ))),
            Some("") => Err(unreadable(format!(
                "Workflow {workflow_id} is COMPLETED and its stored result is empty"
            ))),
            Some(text) => completed_outcome(workflow_id, text),
        },
    }
}

/// The outcome of a COMPLETED workflow's stored result. The outputless
/// marker of the stored envelope and the handle's type must agree: a
/// workflow with no output node is read as `OutputlessResults`, any other
/// as its output type (Python fails a mismatch closed). Pure.
fn completed_outcome<T>(
    workflow_id: Uuid,
    text: &str,
) -> Result<TaskOutcome<T, UserCode>, WorkflowReadError>
where
    T: DeserializeOwned + 'static,
{
    let unreadable =
        |cause: String| WorkflowReadError::StoredResultUnreadable { workflow_id, cause };
    let value: serde_json::Value = from_json_str(text)
        .map_err(|error| unreadable(format!("the result is not JSON: {error}")))?;
    let wire_outputless =
        value.get(OUTPUTLESS_TERMINALS_KEY) == Some(&serde_json::Value::Bool(true));
    let handle_outputless = TypeId::of::<T>() == TypeId::of::<OutputlessResults>();
    match (wire_outputless, handle_outputless) {
        (false, false) => envelope_outcome(workflow_id, text),
        (true, true) => outputless_outcome(workflow_id, value),
        (wire, handle) => Err(unreadable(format!(
            "outputless/typed envelope mismatch: wire_outputless={wire}, \
             handle_outputless={handle}; the workflow definition changed between the start and \
             the read, or the handle has another output type"
        ))),
    }
}

/// The outcome of one stored result envelope, read with `V` and `E`. Pure.
fn envelope_outcome<V, E>(
    workflow_id: Uuid,
    text: &str,
) -> Result<TaskOutcome<V, E>, WorkflowReadError>
where
    V: DeserializeOwned,
    E: UserErrorCode,
{
    let stored = from_json_str::<TaskResult<V>>(text).map_err(|error| {
        WorkflowReadError::StoredResultUnreadable {
            workflow_id,
            cause: error.to_string(),
        }
    })?;
    match stored {
        TaskResult::Ok(value) => Ok(TaskOutcome::Succeeded(value)),
        TaskResult::Err(error) => TaskError::<E>::from_stored(error)
            .map(TaskOutcome::Failed)
            .map_err(|cause| WorkflowReadError::UnknownUserCode { workflow_id, cause }),
    }
}

/// The outcome of the outputless envelope of a COMPLETED workflow: its
/// error, or the outcome of each node in its `results_by_id`. A node entry
/// that is `null` or does not read is a `RESULT_DESERIALIZATION_ERROR`
/// outcome naming the node; the other entries stay readable (Python's
/// handle). Pure.
fn outputless_outcome<T>(
    workflow_id: Uuid,
    value: serde_json::Value,
) -> Result<TaskOutcome<T, UserCode>, WorkflowReadError>
where
    T: 'static,
{
    let unreadable =
        |cause: String| WorkflowReadError::StoredResultUnreadable { workflow_id, cause };
    let serde_json::Value::Object(mut envelope) = value else {
        return Err(unreadable(
            "the outputless result is not an object".to_owned(),
        ));
    };
    if envelope.get("__h_task_result__") != Some(&serde_json::Value::Bool(true)) {
        return Err(unreadable(format!(
            "Workflow {workflow_id} result missing __h_task_result__ envelope marker"
        )));
    }
    if let Some(error) = envelope.remove("err").filter(|slot| !slot.is_null()) {
        return stored_error(workflow_id, &error.to_string()).map(TaskOutcome::Failed);
    }
    let Some(serde_json::Value::Object(mut ok)) = envelope.remove("ok") else {
        return Err(unreadable(
            "Outputless workflow envelope ok slot malformed".to_owned(),
        ));
    };
    let Some(serde_json::Value::Object(entries)) = ok.remove("results_by_id") else {
        return Err(unreadable(
            "Outputless workflow envelope missing or malformed results_by_id".to_owned(),
        ));
    };
    let by_node_id = entries
        .into_iter()
        .map(|(node_id, entry)| {
            let outcome = outputless_entry(workflow_id, &node_id, entry);
            (node_id, outcome)
        })
        .collect();
    let results: Box<dyn Any> = Box::new(OutputlessResults::new(by_node_id));
    match results.downcast::<T>() {
        Ok(results) => Ok(TaskOutcome::Succeeded(*results)),
        Err(_) => unreachable!("the caller reads an outputless result only as OutputlessResults"),
    }
}

/// The outcome of one entry of an outputless result. Pure.
fn outputless_entry(
    workflow_id: Uuid,
    node_id: &str,
    entry: serde_json::Value,
) -> TaskOutcome<serde_json::Value, UserCode> {
    let missing = |cause: String| {
        TaskOutcome::Failed(TaskError::library(
            OperationalErrorCode::ResultDeserializationError,
            format!("Outputless workflow {workflow_id} node {node_id:?} {cause}"),
        ))
    };
    match entry {
        serde_json::Value::Null => missing("missing payload".to_owned()),
        entry => serde_json::from_value::<TaskOutcome<serde_json::Value, UserCode>>(entry)
            .unwrap_or_else(|error| {
                missing(format!("does not read: {}", value_free_decode_text(&error)))
            }),
    }
}

/// The stored error of a workflow, read with user codes as text (Python
/// `_decode_stored_workflow_error`). Pure.
fn stored_error(workflow_id: Uuid, text: &str) -> Result<TaskError<UserCode>, WorkflowReadError> {
    let unreadable =
        |cause: String| WorkflowReadError::StoredResultUnreadable { workflow_id, cause };
    let value: serde_json::Value = from_json_str(text)
        .map_err(|error| unreadable(format!("Workflow error payload corrupt: {error}")))?;
    let stored = match value {
        value @ serde_json::Value::Object(_) => horsies_core::TaskError::from_stored_value(value)
            .map_err(|refused| {
            unreadable(format!(
                "Workflow error payload validation failed: {refused}"
            ))
        })?,
        _ => {
            return Err(unreadable(
                "Workflow error payload is not a JSON object".to_owned(),
            ))
        }
    };
    TaskError::<UserCode>::from_stored(stored)
        .map_err(|cause| WorkflowReadError::UnknownUserCode { workflow_id, cause })
}

/// The stored result of a node, read with no type. Pure.
fn node_result(text: &str) -> NodeResult {
    match from_json_str::<TaskResult<serde_json::Value>>(text) {
        Err(error) => NodeResult::Unreadable {
            cause: error.to_string(),
        },
        Ok(TaskResult::Ok(value)) => NodeResult::Outcome(TaskOutcome::Succeeded(value)),
        Ok(TaskResult::Err(error)) => match TaskError::<UserCode>::from_stored(error) {
            Ok(error) => NodeResult::Outcome(TaskOutcome::Failed(error)),
            Err(cause) => NodeResult::Unreadable {
                cause: cause.to_string(),
            },
        },
    }
}

/// A stored sub-workflow summary. Pure.
fn stored_summary(text: &str) -> Result<SubWorkflowSummary, String> {
    let value: serde_json::Value = from_json_str(text).map_err(|error| error.to_string())?;
    SubWorkflowSummary::from_stored_value(&value).map_err(|refused| refused.to_string())
}

/// The code of an ended status with no stored error (Python
/// `_terminal_outcome_code`).
fn status_code(status: TerminalErrorStatus) -> OutcomeCode {
    match status {
        TerminalErrorStatus::Failed => OutcomeCode::WorkflowFailed,
        TerminalErrorStatus::Cancelled => OutcomeCode::WorkflowCancelled,
        TerminalErrorStatus::Expired => OutcomeCode::WorkflowExpired,
    }
}

/// Python's message of an ended status with no stored error.
fn status_message(status: TerminalErrorStatus) -> &'static str {
    match status {
        TerminalErrorStatus::Failed => "Workflow failed",
        TerminalErrorStatus::Cancelled => "Workflow cancelled",
        TerminalErrorStatus::Expired => "Workflow expired",
    }
}

#[cfg(test)]
mod tests;

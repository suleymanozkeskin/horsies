//! The operations of a workflow handle on a core handle: the status, the
//! node results, the nodes, the wait for the end, the result of one node,
//! the cancel and the pause. The resume is in `start_resume`: it needs the
//! child spec source.
//!
//! - Each operation goes through the handle of this process
//!   (`ProducerHandle::of_process`): in a task child the producer handle
//!   that the child host published, with the child's `task_send_pool`
//!   connections; else the given core handle. The Rust client's workflow
//!   handle chooses the same way. The child spec source of a resume is the
//!   given handle's.
//! - Each operation installs the schema once per handle first, as the Rust
//!   client's handle does; a failed connect or install is
//!   `DB_OPERATION_FAILED` (`HandleOperationError::connect_failed`).
//! - A failed operation is a raw value of the handle catalog
//!   (`HandleWorkflowNotFound`, `HandleDbOperationFailed`,
//!   `HandleStoredDataInvalid`, `HandleResumeRecoveryFailed`), as the core
//!   gives it. It is the value of the envelope, not a throw.
//! - Every stored text (a node's result envelope, the workflow's result and
//!   error, a sub-workflow summary) passes the core's strict JSON check
//!   (`check_json_text`) here. A text that the check refuses crosses as
//!   `refused` with the cause, never as text: the package's reader trusts
//!   only checked text.
//! - A node status crosses as the stored text; the package maps it.
//! - Reads change nothing: a stop of the core in flight leaves no change.
//!   A cancel or a pause in flight at a stop has an unknown outcome.
//! - A workflow id that breaks a boundary rule resolves with
//!   `CLIENT_DEFECT` at once, and nothing starts. The package refuses a wait
//!   inside a task before the call; the addon does not check it.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use horsies_core::core::codec::check_json_text;
use horsies_core::{
    HandleOperation, HandleOperationError, HandleResult, NodeResultsRead as CoreNodeResultsRead,
    NodesRead as CoreNodesRead, OutputNodeRead as CoreOutputNodeRead, PostgresBroker, StoredNode,
    StoredNodeResult, TerminalErrorStatus, WorkflowEnd, WorkflowStatus, WorkflowWait,
};
use napi::bindgen_prelude::{Object, ToNapiValue};
use napi::Env;
use napi_derive::napi;
use uuid::Uuid;

use crate::call::guard::CallKind;
use crate::call::operation::{guarded_start, spawn_operation};
use crate::call::thrown::NativeThrown;
use crate::handle::core_handle::CoreHandle;
use crate::sends::early_end::thrown_now;
use crate::sends::producer_handle::ProducerHandle;
use crate::sends::task_reads::{RecordedInstant, RecordedText, WaitBound};
use crate::values::boundary::{text_of_uuid, u32_of_number, uuid_of_text, BoundaryRefusal};
use crate::values::catalog_values::RawCatalogError;

/// The status of a workflow.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkflowStatusName {
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled,
    Expired,
}

impl WorkflowStatusName {
    /// The name of the core's status. Pure.
    fn of(status: WorkflowStatus) -> Self {
        match status {
            WorkflowStatus::Pending => Self::Pending,
            WorkflowStatus::Running => Self::Running,
            WorkflowStatus::Paused => Self::Paused,
            WorkflowStatus::Completed => Self::Completed,
            WorkflowStatus::Failed => Self::Failed,
            WorkflowStatus::Cancelled => Self::Cancelled,
            WorkflowStatus::Expired => Self::Expired,
        }
    }
}

/// The status of a workflow that ended with an error.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorEndStatusName {
    Failed,
    Cancelled,
    Expired,
}

impl ErrorEndStatusName {
    /// The name of the core's status. Pure.
    fn of(status: TerminalErrorStatus) -> Self {
        match status {
            TerminalErrorStatus::Failed => Self::Failed,
            TerminalErrorStatus::Cancelled => Self::Cancelled,
            TerminalErrorStatus::Expired => Self::Expired,
        }
    }
}

/// A stored text after the core's strict JSON check: the text, or the cause
/// of the refusal.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckedText {
    Checked { text: String },
    Refused { cause: String },
}

impl CheckedText {
    /// The fact of a stored text, by the core's strict JSON check. Pure.
    fn of(text: String) -> Self {
        match check_json_text(&text) {
            Ok(()) => Self::Checked { text },
            Err(refused) => Self::Refused {
                cause: refused.to_string(),
            },
        }
    }
}

/// A stored text that a row may not hold (`NULL`), after the strict check
/// when it holds one.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoredTextFact {
    None,
    Checked { text: String },
    Refused { cause: String },
}

impl StoredTextFact {
    /// The fact of a stored text that may be `NULL`. Pure.
    fn of(text: Option<String>) -> Self {
        match text.map(CheckedText::of) {
            None => Self::None,
            Some(CheckedText::Checked { text }) => Self::Checked { text },
            Some(CheckedText::Refused { cause }) => Self::Refused { cause },
        }
    }
}

/// The status read of a workflow.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowStatusRead {
    Status { status: WorkflowStatusName },
    Failed { error: RawCatalogError },
}

/// One stored node result: the node id (none for a node stored without
/// one) and the result envelope.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredNodeResultRecord {
    pub node_id: RecordedText,
    pub result: CheckedText,
}

impl StoredNodeResultRecord {
    /// The record of a core node result. Pure.
    fn of(node: StoredNodeResult) -> Self {
        Self {
            node_id: RecordedText::of(node.node_id),
            result: CheckedText::of(node.result),
        }
    }
}

/// The stored results of the nodes that have one, in index order.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeResultsRead {
    Nodes { nodes: Vec<StoredNodeResultRecord> },
    WorkflowAbsent,
    Failed { error: RawCatalogError },
}

/// One stored node of a workflow.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct StoredNodeRecord {
    pub node_id: RecordedText,
    pub index: f64,
    /// The task name, or the child's workflow name.
    pub name: String,
    /// The stored status text.
    pub status: String,
    pub result: StoredTextFact,
    pub started_at: RecordedInstant,
    pub completed_at: RecordedInstant,
    /// The child workflow id of a sub-workflow node.
    pub sub_workflow_id: RecordedText,
    /// The summary of the ended child of a sub-workflow node.
    pub sub_workflow_summary: StoredTextFact,
}

impl StoredNodeRecord {
    /// The record of a core node row. Pure.
    fn of(node: StoredNode) -> Self {
        Self {
            node_id: RecordedText::of(node.node_id),
            index: f64::from(node.task_index),
            name: node.task_name,
            status: node.status,
            result: StoredTextFact::of(node.result),
            started_at: RecordedInstant::of(node.started_at),
            completed_at: RecordedInstant::of(node.completed_at),
            sub_workflow_id: RecordedText::of(node.sub_workflow_id.map(text_of_uuid)),
            sub_workflow_summary: StoredTextFact::of(node.sub_workflow_summary),
        }
    }
}

/// Every node of a workflow, in index order.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum NodesRead {
    Nodes { nodes: Vec<StoredNodeRecord> },
    WorkflowAbsent,
    Failed { error: RawCatalogError },
}

/// The outcome of a handle operation with no value.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandleDone {
    Done,
    Failed { error: RawCatalogError },
}

/// The outcome of a pause or a resume: whether the status changed.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandleChange {
    Changed { changed: bool },
    Failed { error: RawCatalogError },
}

/// What a wait for the end of a workflow found.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowWaitRead {
    /// COMPLETED, with the stored result envelope.
    Completed {
        result: StoredTextFact,
    },
    /// FAILED, CANCELLED or EXPIRED, with the stored error object.
    EndedWithError {
        status: ErrorEndStatusName,
        error: StoredTextFact,
    },
    /// PAUSED: the workflow waits for a resume.
    Paused,
    /// PENDING or RUNNING after the wait.
    TimedOut,
    /// No workflow has the id.
    NotFound,
    Failed {
        error: RawCatalogError,
    },
}

/// The stored result of one node by its id. Not a wait.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeResultRead {
    Stored {
        result: CheckedText,
    },
    /// No node of the id stores a result yet, or no workflow has the id:
    /// the core's read does not tell the two apart.
    NotReady,
    Failed {
        error: RawCatalogError,
    },
}

/// Run the operation that `run` makes of `producer` and the request that
/// `check` gives, and give a Promise of its envelope. A request that `check`
/// refuses resolves with its thrown code at once, and nothing starts.
///
/// Failure exits: the `napi::Error` of a deferred that Node-API cannot
/// create (the Promise does not exist then).
pub fn spawn_checked<'env, Request, T, Operation>(
    env: &'env Env,
    subject: &'static str,
    kind: CallKind,
    producer: ProducerHandle,
    check: impl FnOnce() -> Result<Request, NativeThrown>,
    run: impl FnOnce(ProducerHandle, Request) -> Operation,
) -> napi::Result<Object<'env>>
where
    T: ToNapiValue + Send + 'static,
    Operation: Future<Output = T> + Send + 'static,
{
    match guarded_start(subject, check) {
        Err(thrown) => thrown_now::<T>(env, thrown),
        Ok(request) => spawn_operation(env, subject, kind, move |_core| run(producer, request)),
    }
}

/// The workflow id of a lower-case hyphenated UUID text, or the refusal.
/// Pure.
pub fn workflow_id_of(text: &str) -> Result<Uuid, BoundaryRefusal> {
    uuid_of_text("workflowId", text)
}

/// The connected broker of `producer`, with the schema installed once per
/// handle, or `DB_OPERATION_FAILED` of `operation` on `workflow_id` with
/// the broker error as its cause.
pub async fn broker_for(
    producer: &ProducerHandle,
    operation: &HandleOperation,
    workflow_id: Uuid,
) -> HandleResult<Arc<PostgresBroker>> {
    producer
        .lazy_handle()
        .initialized_broker()
        .await
        .map_err(|error| HandleOperationError::connect_failed(operation, workflow_id, error))
}

/// The change of a pause or a resume `subject`. Pure.
pub fn change_of(subject: &str, changed: HandleResult<bool>) -> HandleChange {
    match changed {
        Ok(changed) => HandleChange::Changed { changed },
        Err(error) => HandleChange::Failed {
            error: RawCatalogError::of(subject, &error),
        },
    }
}

/// The time limit of a wait, or the refusal of milliseconds that are not a
/// whole number from 0 to 4294967295. The package refuses a wait above the
/// core's bound before the call. Pure.
fn wait_limit(wait: &WaitBound) -> Result<Option<Duration>, BoundaryRefusal> {
    match wait {
        WaitBound::UntilEnd => Ok(None),
        WaitBound::UpTo { milliseconds } => Ok(Some(Duration::from_millis(u64::from(
            u32_of_number("wait.milliseconds", *milliseconds)?,
        )))),
    }
}

/// Read the status of the workflow `workflowId`.
#[napi(ts_return_type = "Promise<NativeEnvelope<WorkflowStatusRead>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn workflow_status<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    workflow_id: String,
) -> napi::Result<Object<'env>> {
    spawn_checked(
        env,
        HandleOperation::Status.subject(),
        CallKind::Read,
        ProducerHandle::of_process(handle),
        || workflow_id_of(&workflow_id).map_err(NativeThrown::from),
        status_operation,
    )
}

/// One status read.
pub(crate) async fn status_operation(
    producer: ProducerHandle,
    workflow_id: Uuid,
) -> WorkflowStatusRead {
    let read = match broker_for(&producer, &HandleOperation::Status, workflow_id).await {
        Ok(broker) => broker.workflow_status(workflow_id).await,
        Err(error) => Err(error),
    };
    status_read_of(read)
}

/// The status read of the core's answer. Pure.
fn status_read_of(read: HandleResult<WorkflowStatus>) -> WorkflowStatusRead {
    match read {
        Ok(status) => WorkflowStatusRead::Status {
            status: WorkflowStatusName::of(status),
        },
        Err(error) => WorkflowStatusRead::Failed {
            error: RawCatalogError::of(HandleOperation::Status.subject(), &error),
        },
    }
}

/// The stored output node of a workflow: the facts with which the package
/// finds the type of the workflow's result, with no build.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputNodeFact {
    /// No workflow has the id.
    WorkflowAbsent,
    /// The workflow stores no output node.
    NoOutput,
    /// The output index names no node row: stored data that breaks the
    /// workflow's spec.
    NodeMissing {
        task_index: i32,
    },
    /// The output node runs the task `taskName`.
    Task {
        task_name: String,
    },
    /// The output node runs a child workflow, recorded once it started.
    SubWorkflow {
        child_workflow_id: RecordedText,
    },
    Failed {
        error: RawCatalogError,
    },
}

/// Read the stored output node of the workflow `workflowId`.
#[napi(ts_return_type = "Promise<NativeEnvelope<OutputNodeFact>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn workflow_output_node<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    workflow_id: String,
) -> napi::Result<Object<'env>> {
    spawn_checked(
        env,
        HandleOperation::Get.subject(),
        CallKind::Read,
        handle.connection(),
        || workflow_id_of(&workflow_id).map_err(NativeThrown::from),
        output_node_operation,
    )
}

/// One output node read.
async fn output_node_operation(connection: BrokerConnection, workflow_id: Uuid) -> OutputNodeFact {
    let read = match broker_for(&connection, &HandleOperation::Get, workflow_id).await {
        Ok(broker) => broker.workflow_output_node(workflow_id).await,
        Err(error) => Err(error),
    };
    output_node_fact_of(read)
}

/// The output node fact of the core's answer. Pure.
fn output_node_fact_of(read: HandleResult<CoreOutputNodeRead>) -> OutputNodeFact {
    match read {
        Ok(CoreOutputNodeRead::WorkflowAbsent) => OutputNodeFact::WorkflowAbsent,
        Ok(CoreOutputNodeRead::NoOutput) => OutputNodeFact::NoOutput,
        Ok(CoreOutputNodeRead::NodeMissing { task_index }) => {
            OutputNodeFact::NodeMissing { task_index }
        }
        Ok(CoreOutputNodeRead::Task { task_name, .. }) => OutputNodeFact::Task { task_name },
        Ok(CoreOutputNodeRead::SubWorkflow {
            child_workflow_id, ..
        }) => OutputNodeFact::SubWorkflow {
            child_workflow_id: RecordedText::of(child_workflow_id.map(text_of_uuid)),
        },
        Err(error) => OutputNodeFact::Failed {
            error: RawCatalogError::of(HandleOperation::Get.subject(), &error),
        },
    }
}

/// Read the stored result of each node of the workflow `workflowId` that
/// has one.
#[napi(ts_return_type = "Promise<NativeEnvelope<NodeResultsRead>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn workflow_node_results<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    workflow_id: String,
) -> napi::Result<Object<'env>> {
    spawn_checked(
        env,
        HandleOperation::Results.subject(),
        CallKind::Read,
        ProducerHandle::of_process(handle),
        || workflow_id_of(&workflow_id).map_err(NativeThrown::from),
        node_results_operation,
    )
}

/// One read of the node results.
async fn node_results_operation(producer: ProducerHandle, workflow_id: Uuid) -> NodeResultsRead {
    let read = match broker_for(&producer, &HandleOperation::Results, workflow_id).await {
        Ok(broker) => broker.workflow_node_results(workflow_id).await,
        Err(error) => Err(error),
    };
    node_results_read_of(read)
}

/// The node results of the core's answer, each text checked. Pure.
fn node_results_read_of(read: HandleResult<CoreNodeResultsRead>) -> NodeResultsRead {
    match read {
        Ok(CoreNodeResultsRead::Nodes(nodes)) => NodeResultsRead::Nodes {
            nodes: nodes.into_iter().map(StoredNodeResultRecord::of).collect(),
        },
        Ok(CoreNodeResultsRead::WorkflowAbsent) => NodeResultsRead::WorkflowAbsent,
        Err(error) => NodeResultsRead::Failed {
            error: RawCatalogError::of(HandleOperation::Results.subject(), &error),
        },
    }
}

/// Read every node of the workflow `workflowId`, in index order.
#[napi(ts_return_type = "Promise<NativeEnvelope<NodesRead>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn workflow_nodes<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    workflow_id: String,
) -> napi::Result<Object<'env>> {
    spawn_checked(
        env,
        HandleOperation::Tasks.subject(),
        CallKind::Read,
        ProducerHandle::of_process(handle),
        || workflow_id_of(&workflow_id).map_err(NativeThrown::from),
        nodes_operation,
    )
}

/// One read of the nodes.
async fn nodes_operation(producer: ProducerHandle, workflow_id: Uuid) -> NodesRead {
    let read = match broker_for(&producer, &HandleOperation::Tasks, workflow_id).await {
        Ok(broker) => broker.workflow_nodes(workflow_id).await,
        Err(error) => Err(error),
    };
    nodes_read_of(read)
}

/// The nodes of the core's answer, each text checked. Pure.
fn nodes_read_of(read: HandleResult<CoreNodesRead>) -> NodesRead {
    match read {
        Ok(CoreNodesRead::Nodes(nodes)) => NodesRead::Nodes {
            nodes: nodes.into_iter().map(StoredNodeRecord::of).collect(),
        },
        Ok(CoreNodesRead::WorkflowAbsent) => NodesRead::WorkflowAbsent,
        Err(error) => NodesRead::Failed {
            error: RawCatalogError::of(HandleOperation::Tasks.subject(), &error),
        },
    }
}

/// Cancel the workflow `workflowId` and its descendants. A workflow that
/// already ended is left as it is, and the cancel is `done` too.
#[napi(ts_return_type = "Promise<NativeEnvelope<HandleDone>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn cancel_workflow<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    workflow_id: String,
) -> napi::Result<Object<'env>> {
    spawn_checked(
        env,
        HandleOperation::Cancel.subject(),
        CallKind::Write,
        ProducerHandle::of_process(handle),
        || workflow_id_of(&workflow_id).map_err(NativeThrown::from),
        cancel_operation,
    )
}

/// One cancel.
async fn cancel_operation(producer: ProducerHandle, workflow_id: Uuid) -> HandleDone {
    let cancelled = match broker_for(&producer, &HandleOperation::Cancel, workflow_id).await {
        Ok(broker) => broker.cancel_workflow(workflow_id).await,
        Err(error) => Err(error),
    };
    done_of(cancelled)
}

/// The outcome of a cancel. Whether the cancel changed the status is not
/// part of it, as in the Rust client's handle. Pure.
fn done_of(cancelled: HandleResult<bool>) -> HandleDone {
    match cancelled {
        Ok(_cancelled_now) => HandleDone::Done,
        Err(error) => HandleDone::Failed {
            error: RawCatalogError::of(HandleOperation::Cancel.subject(), &error),
        },
    }
}

/// Pause the RUNNING workflow `workflowId`: `changed: true` when it paused,
/// `false` when it is not RUNNING.
#[napi(ts_return_type = "Promise<NativeEnvelope<HandleChange>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn pause_workflow<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    workflow_id: String,
) -> napi::Result<Object<'env>> {
    spawn_checked(
        env,
        HandleOperation::Pause.subject(),
        CallKind::Write,
        ProducerHandle::of_process(handle),
        || workflow_id_of(&workflow_id).map_err(NativeThrown::from),
        pause_operation,
    )
}

/// One pause.
async fn pause_operation(producer: ProducerHandle, workflow_id: Uuid) -> HandleChange {
    let paused = match broker_for(&producer, &HandleOperation::Pause, workflow_id).await {
        Ok(broker) => broker.pause_workflow(workflow_id).await,
        Err(error) => Err(error),
    };
    change_of(HandleOperation::Pause.subject(), paused)
}

/// One checked wait: the workflow id and the time limit (none: until the
/// end).
struct WaitRequest {
    workflow_id: Uuid,
    limit: Option<Duration>,
}

/// Wait for the end of the workflow `workflowId` as `wait` says. A wait
/// that breaks a boundary rule resolves with `CLIENT_DEFECT` at once.
#[napi(ts_return_type = "Promise<NativeEnvelope<WorkflowWaitRead>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn wait_workflow_end<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    workflow_id: String,
    wait: WaitBound,
) -> napi::Result<Object<'env>> {
    spawn_checked(
        env,
        HandleOperation::Get.subject(),
        CallKind::Read,
        ProducerHandle::of_process(handle),
        || -> Result<WaitRequest, NativeThrown> {
            Ok(WaitRequest {
                workflow_id: workflow_id_of(&workflow_id)?,
                limit: wait_limit(&wait)?,
            })
        },
        wait_operation,
    )
}

/// One wait for the end.
async fn wait_operation(producer: ProducerHandle, request: WaitRequest) -> WorkflowWaitRead {
    let workflow_id = request.workflow_id;
    let read = match broker_for(&producer, &HandleOperation::Get, workflow_id).await {
        Ok(broker) => broker.wait_workflow_end(workflow_id, request.limit).await,
        Err(error) => Err(error),
    };
    wait_read_of(read)
}

/// What the core's wait found, each text checked. Pure.
fn wait_read_of(read: HandleResult<WorkflowWait>) -> WorkflowWaitRead {
    match read {
        Ok(WorkflowWait::Ended(WorkflowEnd::Completed { result })) => WorkflowWaitRead::Completed {
            result: StoredTextFact::of(result),
        },
        Ok(WorkflowWait::Ended(WorkflowEnd::Ended { status, error })) => {
            WorkflowWaitRead::EndedWithError {
                status: ErrorEndStatusName::of(status),
                error: StoredTextFact::of(error),
            }
        }
        Ok(WorkflowWait::Ended(WorkflowEnd::Paused)) => WorkflowWaitRead::Paused,
        Ok(WorkflowWait::TimedOut) => WorkflowWaitRead::TimedOut,
        Ok(WorkflowWait::NotFound) => WorkflowWaitRead::NotFound,
        Err(error) => WorkflowWaitRead::Failed {
            error: RawCatalogError::of(HandleOperation::Get.subject(), &error),
        },
    }
}

/// One checked node result read: the workflow id and the node id.
struct NodeRequest {
    workflow_id: Uuid,
    node_id: String,
}

/// Read the stored result of the node `nodeId` of the workflow
/// `workflowId`. Not a wait.
#[napi(ts_return_type = "Promise<NativeEnvelope<NodeResultRead>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn node_result<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    workflow_id: String,
    node_id: String,
) -> napi::Result<Object<'env>> {
    let subject = node_operation(&node_id).subject();
    spawn_checked(
        env,
        subject,
        CallKind::Read,
        ProducerHandle::of_process(handle),
        || -> Result<NodeRequest, NativeThrown> {
            Ok(NodeRequest {
                workflow_id: workflow_id_of(&workflow_id)?,
                node_id,
            })
        },
        node_result_operation,
    )
}

/// The handle operation of the result read of the node `node_id`. Pure.
fn node_operation(node_id: &str) -> HandleOperation {
    HandleOperation::ResultFor {
        node_id: node_id.to_owned(),
    }
}

/// One node result read.
async fn node_result_operation(producer: ProducerHandle, request: NodeRequest) -> NodeResultRead {
    let NodeRequest {
        workflow_id,
        node_id,
    } = request;
    let operation = node_operation(&node_id);
    let read = match broker_for(&producer, &operation, workflow_id).await {
        Ok(broker) => broker.workflow_node_result(workflow_id, &node_id).await,
        Err(error) => Err(error),
    };
    node_result_read_of(operation.subject(), read)
}

/// The node result of the core's answer, its text checked. Pure.
fn node_result_read_of(
    subject: &str,
    read: HandleResult<Option<StoredNodeResult>>,
) -> NodeResultRead {
    match read {
        Ok(Some(stored)) => NodeResultRead::Stored {
            result: CheckedText::of(stored.result),
        },
        Ok(None) => NodeResultRead::NotReady,
        Err(error) => NodeResultRead::Failed {
            error: RawCatalogError::of(subject, &error),
        },
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use horsies_core::BrokerError;

    use super::*;
    use crate::values::boundary::bigint_of_i64;

    #[test]
    fn each_stored_output_node_has_its_fact() {
        let child = Uuid::from_u128(7);
        assert_eq!(
            output_node_fact_of(Ok(CoreOutputNodeRead::WorkflowAbsent)),
            OutputNodeFact::WorkflowAbsent
        );
        assert_eq!(
            output_node_fact_of(Ok(CoreOutputNodeRead::NoOutput)),
            OutputNodeFact::NoOutput
        );
        assert_eq!(
            output_node_fact_of(Ok(CoreOutputNodeRead::NodeMissing { task_index: 3 })),
            OutputNodeFact::NodeMissing { task_index: 3 }
        );
        assert_eq!(
            output_node_fact_of(Ok(CoreOutputNodeRead::Task {
                task_index: 1,
                task_name: "app.charge".to_owned()
            })),
            OutputNodeFact::Task {
                task_name: "app.charge".to_owned()
            }
        );
        assert_eq!(
            output_node_fact_of(Ok(CoreOutputNodeRead::SubWorkflow {
                task_index: 0,
                child_workflow_id: Some(child)
            })),
            OutputNodeFact::SubWorkflow {
                child_workflow_id: RecordedText::Recorded {
                    text: text_of_uuid(child)
                }
            }
        );
        let failed = output_node_fact_of(Err(HandleOperationError::not_found(child)));
        assert!(
            matches!(&failed, OutputNodeFact::Failed { error } if error.class == "HandleWorkflowNotFound"),
            "{failed:?}"
        );
    }

    fn workflow() -> Uuid {
        Uuid::from_u128(11)
    }

    /// A text that the strict read refuses: an object that repeats a key.
    const REPEATED_KEY: &str = r#"{"ok":1,"ok":2}"#;

    fn class_of(error: &RawCatalogError) -> &str {
        error.class.as_str()
    }

    #[test]
    fn a_stored_text_is_checked_and_a_text_that_the_strict_read_refuses_gives_its_cause() {
        assert_eq!(
            CheckedText::of(r#"{"ok":18446744073709551616}"#.to_owned()),
            CheckedText::Checked {
                text: r#"{"ok":18446744073709551616}"#.to_owned()
            }
        );
        assert!(matches!(
            CheckedText::of(REPEATED_KEY.to_owned()),
            CheckedText::Refused { cause } if cause.contains("repeats the key")
        ));
    }

    #[test]
    fn a_null_text_is_none_and_an_empty_text_is_refused() {
        assert_eq!(StoredTextFact::of(None), StoredTextFact::None);
        assert!(matches!(
            StoredTextFact::of(Some(String::new())),
            StoredTextFact::Refused { .. }
        ));
        assert_eq!(
            StoredTextFact::of(Some("{}".to_owned())),
            StoredTextFact::Checked {
                text: "{}".to_owned()
            }
        );
    }

    #[test]
    fn a_status_has_its_name_and_a_failed_read_its_handle_class() {
        assert_eq!(
            status_read_of(Ok(WorkflowStatus::Paused)),
            WorkflowStatusRead::Status {
                status: WorkflowStatusName::Paused
            }
        );
        let WorkflowStatusRead::Failed { error } =
            status_read_of(Err(HandleOperationError::not_found(workflow())))
        else {
            panic!("a failed read is a catalog value");
        };
        assert_eq!(class_of(&error), "HandleWorkflowNotFound");
    }

    #[test]
    fn a_failed_connect_is_db_operation_failed() {
        let error = HandleOperationError::connect_failed(
            &HandleOperation::Status,
            workflow(),
            BrokerError::ListenerClosed,
        );
        let WorkflowStatusRead::Failed { error } = status_read_of(Err(error)) else {
            panic!("a failed connect is a catalog value");
        };
        assert_eq!(class_of(&error), "HandleDbOperationFailed");
    }

    #[test]
    fn node_results_keep_a_missing_node_id_and_check_each_text() {
        let read = node_results_read_of(Ok(CoreNodeResultsRead::Nodes(vec![
            StoredNodeResult {
                node_id: Some("charge".to_owned()),
                task_name: "app.charge".to_owned(),
                result: r#"{"ok":7}"#.to_owned(),
                is_subworkflow: false,
                sub_definition_key: None,
            },
            StoredNodeResult {
                node_id: None,
                task_name: "app.charge".to_owned(),
                result: REPEATED_KEY.to_owned(),
                is_subworkflow: false,
                sub_definition_key: None,
            },
        ])));
        let NodeResultsRead::Nodes { nodes } = read else {
            panic!("the workflow has nodes");
        };
        assert_eq!(
            nodes[0],
            StoredNodeResultRecord {
                node_id: RecordedText::Recorded {
                    text: "charge".to_owned()
                },
                result: CheckedText::Checked {
                    text: r#"{"ok":7}"#.to_owned()
                },
            }
        );
        assert_eq!(nodes[1].node_id, RecordedText::NotRecorded);
        assert!(matches!(nodes[1].result, CheckedText::Refused { .. }));
        assert_eq!(
            node_results_read_of(Ok(CoreNodeResultsRead::WorkflowAbsent)),
            NodeResultsRead::WorkflowAbsent
        );
    }

    #[test]
    fn a_sub_workflow_node_gives_its_child_id_times_and_checked_summary() {
        let started = Utc
            .with_ymd_and_hms(2026, 11, 25, 12, 0, 0)
            .single()
            .expect("a valid instant");
        let child = Uuid::from_u128(12);
        let read = nodes_read_of(Ok(CoreNodesRead::Nodes(vec![StoredNode {
            node_id: Some("child".to_owned()),
            task_index: 3,
            task_name: "report".to_owned(),
            status: "RUNNING".to_owned(),
            result: None,
            started_at: Some(started),
            completed_at: None,
            sub_workflow_id: Some(child),
            sub_workflow_summary: Some(String::new()),
            is_subworkflow: true,
            sub_definition_key: Some("tests.report.v1".to_owned()),
        }])));
        let NodesRead::Nodes { nodes } = read else {
            panic!("the workflow has nodes");
        };
        let node = &nodes[0];
        assert_eq!(node.index, 3.0);
        assert_eq!(node.status, "RUNNING");
        assert_eq!(node.result, StoredTextFact::None);
        assert_eq!(
            node.started_at,
            RecordedInstant::Recorded {
                epoch_micros: bigint_of_i64(started.timestamp_micros())
            }
        );
        assert_eq!(node.completed_at, RecordedInstant::NotRecorded);
        assert_eq!(
            node.sub_workflow_id,
            RecordedText::Recorded {
                text: text_of_uuid(child)
            }
        );
        assert!(matches!(
            node.sub_workflow_summary,
            StoredTextFact::Refused { .. }
        ));
    }

    #[test]
    fn each_end_of_a_wait_has_its_read() {
        assert_eq!(
            wait_read_of(Ok(WorkflowWait::Ended(WorkflowEnd::Completed {
                result: Some(r#"{"ok":1}"#.to_owned())
            }))),
            WorkflowWaitRead::Completed {
                result: StoredTextFact::Checked {
                    text: r#"{"ok":1}"#.to_owned()
                }
            }
        );
        assert_eq!(
            wait_read_of(Ok(WorkflowWait::Ended(WorkflowEnd::Ended {
                status: TerminalErrorStatus::Cancelled,
                error: None,
            }))),
            WorkflowWaitRead::EndedWithError {
                status: ErrorEndStatusName::Cancelled,
                error: StoredTextFact::None,
            }
        );
        assert_eq!(
            wait_read_of(Ok(WorkflowWait::Ended(WorkflowEnd::Paused))),
            WorkflowWaitRead::Paused
        );
        assert_eq!(
            wait_read_of(Ok(WorkflowWait::TimedOut)),
            WorkflowWaitRead::TimedOut
        );
        assert_eq!(
            wait_read_of(Ok(WorkflowWait::NotFound)),
            WorkflowWaitRead::NotFound
        );
        assert!(matches!(
            wait_read_of(Err(HandleOperationError::not_found(workflow()))),
            WorkflowWaitRead::Failed { error } if class_of(&error) == "HandleWorkflowNotFound"
        ));
    }

    #[test]
    fn a_stored_node_result_is_checked_and_none_is_not_ready() {
        let subject = node_operation("charge").subject();
        let stored = StoredNodeResult {
            node_id: Some("charge".to_owned()),
            task_name: "app.charge".to_owned(),
            result: REPEATED_KEY.to_owned(),
            is_subworkflow: false,
            sub_definition_key: None,
        };
        assert!(matches!(
            node_result_read_of(subject, Ok(Some(stored))),
            NodeResultRead::Stored {
                result: CheckedText::Refused { .. }
            }
        ));
        assert_eq!(
            node_result_read_of(subject, Ok(None)),
            NodeResultRead::NotReady
        );
    }

    #[test]
    fn a_cancel_is_done_and_a_pause_gives_its_change() {
        assert_eq!(done_of(Ok(false)), HandleDone::Done);
        assert_eq!(done_of(Ok(true)), HandleDone::Done);
        assert!(matches!(
            done_of(Err(HandleOperationError::not_found(workflow()))),
            HandleDone::Failed { error } if class_of(&error) == "HandleWorkflowNotFound"
        ));
        assert_eq!(
            change_of(HandleOperation::Pause.subject(), Ok(true)),
            HandleChange::Changed { changed: true }
        );
        assert!(matches!(
            change_of(
                HandleOperation::Pause.subject(),
                Err(HandleOperationError::not_found(workflow()))
            ),
            HandleChange::Failed { error } if class_of(&error) == "HandleWorkflowNotFound"
        ));
    }

    #[test]
    fn a_wait_bound_is_a_time_limit_or_a_boundary_refusal() {
        assert_eq!(wait_limit(&WaitBound::UntilEnd), Ok(None));
        assert_eq!(
            wait_limit(&WaitBound::UpTo {
                milliseconds: 1_500.0
            }),
            Ok(Some(Duration::from_millis(1_500)))
        );
        assert!(matches!(
            wait_limit(&WaitBound::UpTo { milliseconds: 1.5 }),
            Err(refusal) if refusal.subject == "wait.milliseconds"
        ));
    }

    #[test]
    fn a_workflow_id_in_another_form_is_a_boundary_refusal() {
        assert_eq!(
            workflow_id_of("00000000-0000-0000-0000-00000000000b"),
            Ok(workflow())
        );
        assert!(matches!(
            workflow_id_of("00000000-0000-0000-0000-00000000000B"),
            Err(refusal) if refusal.subject == "workflowId"
        ));
    }
}

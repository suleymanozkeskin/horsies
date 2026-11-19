//! The workflow start on the core statement layer: the whole tree in one call
//! of `horsies_workflow_start` (`0005_workflow_functions.sql`).
//!
//! The core builds the tree in memory: the top-level workflow, a pre-built
//! child workflow for every sub-workflow root (recursively, to the depth
//! limit), every node row with its typed facts, and a failed-node record for
//! each sub-workflow root whose child cannot start. The function inserts the
//! rows, fails the recorded nodes and runs their progression, mints the task
//! rows of the READY task nodes, and links each sub-workflow root to its
//! pre-built child.
//!
//! The same arrays describe the child tree of an owed child start
//! (`horsies_workflow_child_start`, `child_start.rs`).

use std::borrow::Cow;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::PostgresBroker;
use crate::core::workflow::node::SubWorkflowParams;
use crate::core::{AnyNode, OnError, OperationalErrorCode, TaskError, TaskResult, WorkflowSpec};
use crate::db::{BoundStatement, CheckoutClass, ParameterBinder, ParameterType, TypedStatement};
use crate::telemetry::trace_carrier::StoredTrace;
use crate::workflow_engine::args_merge::{
    child_build_input, child_static_input, merge_args_from_sync, read_args_from, ArgsFromMerge,
    ArgsFromRead, ChildBuildInput, ChildStaticInput, DepResult, MergedNodeKind,
};
use crate::workflow_engine::child_spec::{
    queue_class_key, ChildSpecRequest, ChildSpecSource, ClientChildSpec, ClientSpec, NodeFailure,
};
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::merge_good_until_into_options;
use crate::workflow_engine::start::{
    child_workflow_depth, node_task_facts, require_subworkflow_node_keys,
    stored_queue_and_priority, stored_workflow_ctx_from, ChildSpecError,
};
use crate::workflow_engine::steps::{
    run_alone, run_in_transaction, step_row_select, step_rows, ChildStartSource,
    OwedParentAdvances, StepContractError, StepDisposition,
};

/// `horsies_workflow_start`: the workflow rows of the tree (top-level first),
/// the start call time, the node rows, the failed-node records, and the
/// trace kind, carrier text and trace id (`StoredTrace::columns`).
pub(crate) static START: TypedStatement = TypedStatement::new(
    "horsies_workflow_start",
    step_row_select!(
        "horsies_workflow_start($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, \
         $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, \
         $33, $34, $35, $36, $37, $38)"
    ),
    &[
        // Workflow rows.
        ParameterType::UuidArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::Int4Array,
        ParameterType::TextArray,
        ParameterType::UuidArray,
        ParameterType::Int4Array,
        ParameterType::Int4Array,
        // Start call time.
        ParameterType::Timestamptz,
        // Node rows.
        ParameterType::UuidArray,
        ParameterType::Int4Array,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::Int4Array,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::BoolArray,
        ParameterType::TextArray,
        ParameterType::Int4Array,
        ParameterType::TextArray,
        ParameterType::Int4Array,
        ParameterType::TextArray,
        ParameterType::TimestamptzArray,
        ParameterType::TextArray,
        ParameterType::BoolArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        ParameterType::TextArray,
        // Failed-node records.
        ParameterType::UuidArray,
        ParameterType::Int4Array,
        ParameterType::TextArray,
        // Trace.
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Bytea,
    ],
);

/// The disposition of `horsies_workflow_start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StartDisposition {
    /// The tree was written.
    Started,
    /// A workflow with the top-level id exists; nothing was written. `detail`
    /// holds its stored name (NULL when the row is gone by the read).
    Exists,
}

impl StepDisposition for StartDisposition {
    fn from_text(text: &str) -> Option<Self> {
        match text {
            "STARTED" => Some(Self::Started),
            "EXISTS" => Some(Self::Exists),
            _ => None,
        }
    }
}

/// What the start call found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StartOutcome {
    /// The tree was written.
    Started,
    /// A workflow with the top-level id exists; nothing was written.
    /// `stored_name` is its name, or `None` when the row was gone by the read.
    Exists { stored_name: Option<String> },
}

/// What a committed start did, and the parent advances it owes after its
/// commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StartedTree {
    pub(crate) outcome: StartOutcome,
    /// The workflows that the owed child starts of a transaction-form start
    /// ended and that have a parent. Empty for the one-statement form.
    pub(crate) parent_advances: OwedParentAdvances,
}

/// The parallel arrays of `horsies_workflow_start` (and of the `TREE` kind of
/// `horsies_workflow_child_start`), one element per workflow row, node row or
/// failed-node record of the tree. Only [`client_start_arguments`] and
/// [`child_tree_arguments`] build it.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct StartArguments {
    workflow_ids: Vec<Uuid>,
    names: Vec<String>,
    definition_keys: Vec<Option<String>>,
    on_errors: Vec<String>,
    output_task_indexes: Vec<Option<i32>>,
    success_policies: Vec<Option<String>>,
    parent_workflow_ids: Vec<Option<Uuid>>,
    parent_task_indexes: Vec<Option<i32>>,
    depths: Vec<i32>,
    node_workflow_ids: Vec<Uuid>,
    node_task_indexes: Vec<i32>,
    node_ids: Vec<Option<String>>,
    node_task_names: Vec<String>,
    node_inputs: Vec<String>,
    node_queue_names: Vec<String>,
    node_priorities: Vec<i32>,
    node_dependencies: Vec<String>,
    node_args_from: Vec<Option<String>>,
    node_workflow_ctx_from: Vec<Option<String>>,
    node_allow_failed_deps: Vec<bool>,
    node_join_types: Vec<String>,
    node_min_successes: Vec<Option<i32>>,
    node_task_options: Vec<Option<String>>,
    node_max_retries: Vec<Option<i32>>,
    node_executions: Vec<Option<String>>,
    node_good_untils: Vec<Option<DateTime<Utc>>>,
    node_retention_class_keys: Vec<Option<String>>,
    node_is_subworkflow: Vec<bool>,
    node_sub_workflow_names: Vec<Option<String>>,
    node_sub_definition_keys: Vec<Option<String>>,
    node_sub_workflow_params: Vec<Option<String>>,
    failed_workflow_ids: Vec<Uuid>,
    failed_task_indexes: Vec<i32>,
    failed_results: Vec<String>,
}

/// One workflow of the tree, before its rows are pushed.
struct TreeWorkflow<'a> {
    spec: Cow<'a, WorkflowSpec>,
    /// The retention class key of each task queue of `spec`, by queue name,
    /// as the client resolved it.
    classes: Cow<'a, BTreeMap<String, String>>,
    workflow_id: Uuid,
    /// The parent workflow and the index of its sub-workflow node; `None`
    /// for the top-level workflow.
    parent: Option<(Uuid, i32)>,
    depth: i32,
}

/// What the host build made of the child of a sub-workflow node.
pub(crate) enum ChildBuildOutcome<'a> {
    /// The child spec, at its depth, with the retention class key of each of
    /// its task queues that the client resolved.
    Built {
        spec: WorkflowSpec,
        depth: i32,
        classes: Cow<'a, BTreeMap<String, String>>,
    },
    /// The child cannot start: the node fails with this error, and its
    /// workflow's `on_error` applies (Python's node-level failure,
    /// `_fail_enqueued_task` and `_fail_subworkflow_load`,
    /// `core/workflows/engine.py:474-619`).
    NodeFails(NodeFailure),
}

/// Characters of a build failure text that the node error keeps (Python:
/// `str(exc)[:200]`, `core/workflows/engine.py:1206`).
const BUILD_FAILURE_TEXT_CHARS: usize = 200;

/// Characters of the refusal text of an `args_from` value that the node
/// error keeps (Python: `str(refusal)[:200]`, `core/workflows/engine.py:1152`).
const ARGS_FROM_REFUSAL_TEXT_CHARS: usize = 200;

/// Builds the arguments of a start of a spec that a client sent as data:
/// the top-level workflow `workflow_id` of `spec`, a pre-built child for
/// every sub-workflow node without dependencies (recursively), and a
/// failed-node record for each such node whose child cannot start. Parents
/// come before their children; the top-level row is first. The children come
/// from `source`, and every task queue takes the class that the client
/// resolved. Does no I/O; it asks `source`, which must not write.
///
/// Errors: [`WorkflowError::Validation`] for a sub-workflow node of a built
/// child without a definition key, a node without a resolved queue or
/// priority, a node index above `i32::MAX`, task options that the enqueue
/// rules refuse, or a task queue that the client gave no class;
/// [`WorkflowError::Serialization`] for a value that does not encode;
/// [`WorkflowError::ChildSpecSource`] when `source` gave no answer for a
/// root's child.
pub(crate) async fn client_start_arguments(
    spec: &ClientSpec,
    workflow_id: Uuid,
    source: &dyn ChildSpecSource,
) -> Result<StartArguments, WorkflowError> {
    tree_arguments(
        TreeWorkflow {
            spec: Cow::Borrowed(&spec.spec),
            classes: Cow::Borrowed(&spec.queue_classes),
            workflow_id,
            parent: None,
            depth: 0,
        },
        source,
    )
    .await
}

/// The child workflow of an owed child start, before its rows are built.
pub(crate) struct ChildTree<'a> {
    pub(crate) spec: WorkflowSpec,
    pub(crate) classes: Cow<'a, BTreeMap<String, String>>,
    pub(crate) child_workflow_id: Uuid,
    pub(crate) parent_workflow_id: Uuid,
    pub(crate) parent_task_index: i32,
    pub(crate) depth: i32,
}

/// Builds the `TREE` arguments of an owed child start: the child
/// `child_workflow_id` under the node `parent_task_index` of
/// `parent_workflow_id`, at `depth`, with its pre-built children (from
/// `build`) and failed-node records as [`client_start_arguments`]. Errors as
/// [`client_start_arguments`].
pub(crate) async fn child_tree_arguments(
    child: ChildTree<'_>,
    build: &dyn ChildSpecSource,
) -> Result<StartArguments, WorkflowError> {
    tree_arguments(
        TreeWorkflow {
            spec: Cow::Owned(child.spec),
            classes: child.classes,
            workflow_id: child.child_workflow_id,
            parent: Some((child.parent_workflow_id, child.parent_task_index)),
            depth: child.depth,
        },
        build,
    )
    .await
}

/// The rows of the tree under `top`. Each workflow is pushed once, and a
/// child only below the depth limit, so the work list ends.
async fn tree_arguments<'a>(
    top: TreeWorkflow<'a>,
    build: &'a dyn ChildSpecSource,
) -> Result<StartArguments, WorkflowError> {
    let mut arguments = StartArguments::default();
    let mut pending = vec![top];
    while let Some(workflow) = pending.pop() {
        require_subworkflow_node_keys(&workflow.spec)?;
        arguments.push_workflow(&workflow)?;
        for node in &workflow.spec.tasks {
            arguments.push_node(workflow.workflow_id, node, &workflow.classes)?;
        }
        for node in workflow
            .spec
            .tasks
            .iter()
            .filter(|node| node.is_subworkflow && node.dependencies.is_empty())
        {
            let task_index = node_index(node.index)?;
            let built = build_child(
                ChildOfNode {
                    parent_workflow_id: workflow.workflow_id,
                    parent_workflow_name: &workflow.spec.name,
                    task_index,
                    parent_depth: workflow.depth,
                    definition_key: node.sub_definition_key.as_deref(),
                },
                // A root node has no dependencies, so it has no `args_from`.
                ChildInput {
                    node_input: node.input_json.clone(),
                    args_from: None,
                    sources: Vec::new(),
                },
                node.sub_workflow_params,
                build,
            )
            .await?;
            match built {
                ChildBuildOutcome::Built {
                    spec,
                    depth,
                    classes,
                } => pending.push(TreeWorkflow {
                    spec: Cow::Owned(spec),
                    classes,
                    workflow_id: Uuid::new_v4(),
                    parent: Some((workflow.workflow_id, task_index)),
                    depth,
                }),
                ChildBuildOutcome::NodeFails(failure) => {
                    tracing::error!(
                        workflow_id = %workflow.workflow_id,
                        task_index,
                        error = %failure,
                        "sub-workflow root not started: {failure}; its node fails",
                    );
                    arguments.push_failed_record(workflow.workflow_id, task_index, failure)?;
                }
            }
        }
    }
    Ok(arguments)
}

/// The sub-workflow node whose child the host build makes.
pub(crate) struct ChildOfNode<'a> {
    pub(crate) parent_workflow_id: Uuid,
    /// The name of the parent workflow: the node errors name the workflow by
    /// it (Python's `parent_workflow_name`, `core/workflows/engine.py:1014-1016`).
    pub(crate) parent_workflow_name: &'a str,
    pub(crate) task_index: i32,
    pub(crate) parent_depth: i32,
    pub(crate) definition_key: Option<&'a str>,
}

/// The stored facts of a sub-workflow node that its child's input is made
/// of: the node input, its `args_from` text, and the terminal dependencies
/// whose results the `args_from` members take. A root has no `args_from` and
/// no dependency.
pub(crate) struct ChildInput {
    pub(crate) node_input: String,
    pub(crate) args_from: Option<String>,
    pub(crate) sources: Vec<ChildStartSource>,
}

/// The host build of the child of a sub-workflow node, in Python's order
/// (`enqueue_subworkflow_task`, `core/workflows/engine.py:1019-1212`): the
/// depth limit, then the client's [`ChildSpecSource`], which loads the
/// child by its definition key, merges the `args_from` members into the node
/// input ([`merged_child_input`]) and runs the builder in the client's
/// language.
///
/// A child past the depth limit, and every node failure that the source
/// answers, are [`ChildBuildOutcome::NodeFails`].
///
/// Errors: [`WorkflowError::ChildSpecSource`] when the client's source gave
/// no answer.
pub(crate) async fn build_child<'a>(
    node: ChildOfNode<'_>,
    input: ChildInput,
    params: SubWorkflowParams,
    build: &'a dyn ChildSpecSource,
) -> Result<ChildBuildOutcome<'a>, WorkflowError> {
    let depth = match child_workflow_depth(
        node.parent_depth,
        node.parent_workflow_id,
        node.parent_workflow_name,
        node.task_index,
    ) {
        Ok(depth) => depth,
        Err(error) => return Ok(ChildBuildOutcome::NodeFails(NodeFailure::Core(error))),
    };
    let request = ChildSpecRequest {
        parent_workflow_id: node.parent_workflow_id,
        parent_workflow_name: node.parent_workflow_name.to_owned(),
        task_index: node.task_index,
        child_depth: depth,
        definition_key: node.definition_key.map(str::to_owned),
        params,
        node_input: input.node_input,
        args_from: input.args_from,
        sources: input.sources,
    };
    match build.child_spec(request).await? {
        ClientChildSpec::Built(client) => Ok(ChildBuildOutcome::Built {
            spec: client.spec,
            depth,
            classes: Cow::Owned(client.queue_classes),
        }),
        ClientChildSpec::NodeFails {
            message,
            failure_result,
        } => Ok(ChildBuildOutcome::NodeFails(NodeFailure::Client {
            message,
            failure_result,
        })),
    }
}

/// The input of the builder of a child: the node input with its
/// `args_from` members merged after its static members, each replaced by
/// its `inner` envelope ([`child_build_input`]). With no `args_from` members
/// the input is the stored input, or `{}` for a stored input with no static
/// members ([`child_static_input`]: a value that is not an object, as Python
/// reads it). A client's child spec source decodes it with its builder's
/// params type.
///
/// The node fails ([`NodeInput::NodeFails`]), in Python's order
/// (`core/workflows/child_build.py`, `_input_kwargs` and `_args_from_map`),
/// with `WORKER_SERIALIZATION_ERROR` and the workflow id and the node in
/// `data` unless named otherwise:
/// - a stored input that is empty, that the strict read refuses, or `null`:
///   `Corrupt kwargs JSON for subworkflow task <index>`;
/// - an `args_from` text that is empty, that the strict read refuses, or
///   `null`: `Corrupt args_from JSON for subworkflow task <index>`;
/// - an `args_from` value that is not a JSON integer
///   ([`read_args_from`]): `args_from has non-int values for subworkflow
///   task <index>`;
/// - an `args_from` key that names a static member: `WORKFLOW_ENQUEUE_FAILED`
///   (Python `core/workflows/engine.py:1111-1123`);
/// - a merged input from which the builder input cannot be made
///   ([`ChildSpecError::of_stored_input`], `WORKFLOW_ENQUEUE_FAILED`).
///
/// Python decodes the static members against the builder before it reads
/// `args_from`; here the builder decodes after this merge, so a node whose
/// static members and `args_from` are both refused fails for its
/// `args_from`.
///
/// Each static member and each stored dependency result keeps its stored
/// text ([`merge_args_from_sync`], [`child_build_input`]), so an integer
/// outside the `i64` and `u64` range reaches the builder as stored, as
/// Python reads it (`_input_kwargs`, `core/workflows/child_build.py`).
///
/// Errors: the errors of the merge: [`WorkflowError::Serialization`] for an
/// `args_from` entry that does not encode. The merge takes a stored input
/// that passed the strict read as an object, or `{}`, so its
/// [`WorkflowError::Validation`] does not occur here. Pure.
pub fn merged_child_input(request: &ChildSpecRequest) -> Result<NodeInput, WorkflowError> {
    let static_input = match child_static_input(&request.node_input) {
        Ok(ChildStaticInput::Members(text)) => text,
        Ok(ChildStaticInput::NoMembers) => NO_STATIC_MEMBERS,
        Err(corrupt) => {
            tracing::warn!(
                workflow_id = %request.parent_workflow_id,
                task_index = request.task_index,
                "JSON parse failed (subworkflow kwargs): {}",
                corrupt.cause,
            );
            return Ok(NodeInput::NodeFails(child_input_error(
                request,
                format!(
                    "Corrupt kwargs JSON for subworkflow task {}",
                    request.task_index
                ),
            )));
        }
    };
    let args_from = match read_args_from(request.args_from.as_deref()) {
        ArgsFromRead::NoMembers => None,
        ArgsFromRead::Indexes(map) => Some(serde_json::Value::Object(map)),
        ArgsFromRead::Corrupt { cause } => {
            tracing::warn!(
                workflow_id = %request.parent_workflow_id,
                task_index = request.task_index,
                "JSON parse failed (subworkflow args_from): {cause}",
            );
            return Ok(NodeInput::NodeFails(child_input_error(
                request,
                format!(
                    "Corrupt args_from JSON for subworkflow task {}",
                    request.task_index
                ),
            )));
        }
        ArgsFromRead::NotIndexes => {
            return Ok(NodeInput::NodeFails(child_input_error(
                request,
                format!(
                    "args_from has non-int values for subworkflow task {}",
                    request.task_index
                ),
            )));
        }
    };
    let dependency_results: std::collections::HashMap<i32, DepResult> = request
        .sources
        .iter()
        .map(|source| {
            (
                source.task_index,
                DepResult {
                    node_id: source.node_id.clone(),
                    task_name: source.task_name.clone(),
                    sub_definition_key: source.definition_key.clone(),
                    status: source.status.clone(),
                    result: source.result.clone(),
                },
            )
        })
        .collect();
    Ok(
        match merge_args_from_sync(static_input, &args_from, &dependency_results)? {
            ArgsFromMerge::Merged(input) => match child_build_input(input, args_from.as_ref()) {
                Ok(input) => NodeInput::Input(input),
                Err(refused) => NodeInput::NodeFails(build_failed_error(
                    request.parent_workflow_id,
                    &request.parent_workflow_name,
                    request.task_index,
                    &ChildSpecError::of_stored_input(refused),
                )),
            },
            ArgsFromMerge::StaticMemberConflict(conflict) => {
                NodeInput::NodeFails(conflict.node_error(
                    request.parent_workflow_id,
                    request.task_index,
                    MergedNodeKind::SubWorkflow,
                ))
            }
        },
    )
}

/// The builder input of a stored node input with no static members (Python
/// reads it as `{}`).
const NO_STATIC_MEMBERS: &str = "{}";

/// The error of the sub-workflow node of `request` whose stored input or
/// `args_from` Python refuses: `WORKER_SERIALIZATION_ERROR` with `message`,
/// and the workflow id and the node in `data` (Python `node_error`,
/// `core/workflows/child_build.py`). Pure.
fn child_input_error(request: &ChildSpecRequest, message: String) -> TaskError {
    TaskError {
        data: Some(crate::core::codec::JsonText::of_engine_value(
            &serde_json::json!({
                "workflow_id": request.parent_workflow_id.to_string(),
                "task_index": request.task_index,
            }),
        )),
        ..TaskError::builtin(OperationalErrorCode::WorkerSerializationError, message)
    }
}

/// The input that the builder of a child takes ([`merged_child_input`]).
#[derive(Debug, Clone)]
pub enum NodeInput {
    /// The encoded input, `args_from` members merged after the static
    /// members.
    Input(ChildBuildInput),
    /// The input cannot be built; the node fails with this error.
    NodeFails(TaskError),
}

/// An input member whose value the decode of the child's builder refused.
#[derive(Debug, Clone, Copy)]
pub struct RefusedMember<'a> {
    pub key: &'a str,
    /// The decode's text, with the path of the value.
    pub cause: &'a str,
}

/// The error of the sub-workflow node `task_index` of the workflow
/// `workflow_id` whose input member `refused` does not decode to the builder
/// parameter of that name, or that no builder parameter takes (Python
/// `decode_subworkflow_kwargs`, `core/codec/kwargs.py`):
/// `WORKER_SERIALIZATION_ERROR` with the workflow id
/// and the node in `data` (Python `_fail_enqueued_task`,
/// `core/workflows/engine.py:474-512`). The message:
/// - an `args_from` member of `input`:
///   `args_from value for '<key>' does not fit the type that <hook> declares (subworkflow task <index>): <cause, 200 characters>`
///   (Python `:1134-1157`, which names `build_with`); `hook` is the name of
///   the client's builder;
/// - a static member:
///   `Typed decode of subworkflow kwargs failed for task <index>: <cause>`
///   (Python `:1063-1080`).
///
/// Python decodes the static members before it checks the `args_from`
/// values; the builder decodes `input.text`, which holds the static members
/// first, so a refused `args_from` member means that every static member
/// decoded. Pure.
pub fn params_refused_error(
    workflow_id: Uuid,
    task_index: i32,
    refused: RefusedMember<'_>,
    input: &ChildBuildInput,
    hook: &str,
) -> TaskError {
    let message = match input
        .args_from_keys
        .iter()
        .any(|key| key.as_str() == refused.key)
    {
        true => {
            let cause: String = refused
                .cause
                .chars()
                .take(ARGS_FROM_REFUSAL_TEXT_CHARS)
                .collect();
            format!(
                "args_from value for '{}' does not fit the type that {hook} declares \
                 (subworkflow task {task_index}): {cause}",
                refused.key,
            )
        }
        false => format!(
            "Typed decode of subworkflow kwargs failed for task {task_index}: {}",
            refused.cause,
        ),
    };
    TaskError {
        data: Some(crate::core::codec::JsonText::of_engine_value(
            &serde_json::json!({
                "workflow_id": workflow_id.to_string(),
                "task_index": task_index,
            }),
        )),
        ..TaskError::builtin(OperationalErrorCode::WorkerSerializationError, message)
    }
}

/// The error of the sub-workflow node `task_index` of the workflow
/// `workflow_id` (named `workflow_name`) whose registered child gave no spec:
/// `WORKFLOW_ENQUEUE_FAILED`,
/// `Subworkflow build failed: <class>: <text, 200 characters> (workflow <name>:<index>)`,
/// with the workflow id and the node in `data` (Python
/// `core/workflows/engine.py:1196-1211`, `_fail_enqueued_task` at `:507-512`).
/// Pure.
pub fn build_failed_error(
    workflow_id: Uuid,
    workflow_name: &str,
    task_index: i32,
    error: &ChildSpecError,
) -> TaskError {
    child_build_failure(
        workflow_id,
        workflow_name,
        task_index,
        &child_spec_failure_text(error),
    )
}

/// The error of the sub-workflow node `task_index` of the workflow
/// `workflow_id` (named `workflow_name`) whose builder input does not decode
/// as the params type of the client's builder as a whole (a parameter with
/// no member, for one): `WORKFLOW_ENQUEUE_FAILED`,
/// `Subworkflow build failed: <decode text, 200 characters> (workflow <name>:<index>)`,
/// with the workflow id and the node in `data`. Python gives the class of
/// the exception that `build_with` raised (`TypeError: `) before the text; a
/// decode has no exception class, so its text follows the prefix. Pure.
pub fn params_decode_failed_error(
    workflow_id: Uuid,
    workflow_name: &str,
    task_index: i32,
    decode_text: &str,
) -> TaskError {
    let text: String = decode_text.chars().take(BUILD_FAILURE_TEXT_CHARS).collect();
    child_build_failure(workflow_id, workflow_name, task_index, &text)
}

/// `WORKFLOW_ENQUEUE_FAILED` of the sub-workflow node `task_index` of the
/// workflow `workflow_id` (named `workflow_name`):
/// `Subworkflow build failed: <failure> (workflow <name>:<index>)`, with the
/// workflow id and the node in `data` (Python `_fail_enqueued_task`,
/// `core/workflows/engine.py:507-512`). Pure.
fn child_build_failure(
    workflow_id: Uuid,
    workflow_name: &str,
    task_index: i32,
    failure: &str,
) -> TaskError {
    TaskError {
        data: Some(crate::core::codec::JsonText::of_engine_value(
            &serde_json::json!({
                "workflow_id": workflow_id.to_string(),
                "task_index": task_index,
            }),
        )),
        ..TaskError::builtin(
            OperationalErrorCode::WorkflowEnqueueFailed,
            format!("Subworkflow build failed: {failure} (workflow {workflow_name}:{task_index})"),
        )
    }
}

/// The text of a registered child that gave no spec: `<class>: <text, 200 characters>` (Python
/// `f'{type(exc).__name__}: {str(exc)[:200]}'`, `core/workflows/engine.py:1246`). Pure.
pub(crate) fn child_spec_failure_text(error: &ChildSpecError) -> String {
    let text: String = error.text.chars().take(BUILD_FAILURE_TEXT_CHARS).collect();
    format!("{}: {text}", error.error_type.as_str())
}

/// The stored result of a node that fails with `error`: the error in its
/// result envelope.
///
/// Errors: [`WorkflowError::Serialization`] when the error does not encode.
/// Pure.
pub fn node_failure_result(error: TaskError) -> Result<String, WorkflowError> {
    Ok(serde_json::to_string(
        &TaskResult::<serde_json::Value>::Err(error),
    )?)
}

/// The error of the sub-workflow node `task_index` of the workflow named `parent_workflow_name` whose
/// child definition is not registered under its definition key. A child is resolved only by its key
/// (Python `core/workflows/engine.py:1034-1045`). The message names the parent workflow and the node;
/// `data` holds the definition key (`null` when the node has none), as Python's `_fail_subworkflow_load`
/// does (`core/workflows/engine.py:577-581`). Pure.
pub fn subworkflow_load_failed_error(
    parent_workflow_name: &str,
    task_index: i32,
    definition_key: Option<&str>,
) -> TaskError {
    TaskError {
        data: Some(crate::core::codec::JsonText::of_engine_value(
            &serde_json::json!({ "definition_key": definition_key }),
        )),
        ..TaskError::builtin(
            OperationalErrorCode::SubworkflowLoadFailed,
            format!(
                "Failed to load subworkflow definition for {parent_workflow_name}:{task_index}"
            ),
        )
    }
}

/// A node index as the `integer` column stores it.
fn node_index(index: usize) -> Result<i32, WorkflowError> {
    i32::try_from(index).map_err(|_| {
        WorkflowError::Validation(format!("node index {index} is above the integer range"))
    })
}

impl StartArguments {
    /// Whether the tree holds a failed-node record. Only then can the start
    /// owe child starts.
    pub(crate) fn has_failed_records(&self) -> bool {
        !self.failed_workflow_ids.is_empty()
    }

    /// The id of the top row of the tree.
    pub(crate) fn top_workflow_id(&self) -> Option<Uuid> {
        self.workflow_ids.first().copied()
    }

    fn push_workflow(&mut self, workflow: &TreeWorkflow<'_>) -> Result<(), WorkflowError> {
        let spec = &workflow.spec;
        let output_task_index = spec.output_index.map(node_index).transpose()?;
        let success_policy = spec
            .success_policy
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        self.workflow_ids.push(workflow.workflow_id);
        self.names.push(spec.name.clone());
        self.definition_keys.push(spec.definition_key.clone());
        self.on_errors.push(
            match spec.on_error {
                OnError::Fail => "fail",
                OnError::Pause => "pause",
            }
            .to_owned(),
        );
        self.output_task_indexes.push(output_task_index);
        self.success_policies.push(success_policy);
        self.parent_workflow_ids
            .push(workflow.parent.map(|(parent_id, _)| parent_id));
        self.parent_task_indexes
            .push(workflow.parent.map(|(_, task_index)| task_index));
        self.depths.push(workflow.depth);
        Ok(())
    }

    fn push_failed_record(
        &mut self,
        workflow_id: Uuid,
        task_index: i32,
        failure: NodeFailure,
    ) -> Result<(), WorkflowError> {
        let result = failure.into_stored_result()?;
        self.failed_workflow_ids.push(workflow_id);
        self.failed_task_indexes.push(task_index);
        self.failed_results.push(result);
        Ok(())
    }

    fn push_node(
        &mut self,
        workflow_id: Uuid,
        node: &AnyNode,
        classes: &BTreeMap<String, String>,
    ) -> Result<(), WorkflowError> {
        let (queue, priority) = stored_queue_and_priority(node)?;
        let merged_task_options = merge_good_until_into_options(
            node.index,
            &node.task_name,
            node.good_until,
            node.task_options_json.as_deref(),
        )?;
        let task_facts = match node.is_subworkflow {
            true => None,
            false => Some(node_task_facts(
                node,
                merged_task_options.as_deref(),
                Some(queue_class_key(classes, queue)?),
            )?),
        };
        let dependencies = node
            .dependencies
            .iter()
            .map(|&dependency| node_index(dependency))
            .collect::<Result<Vec<i32>, WorkflowError>>()?;
        let args_from = match node.args_from.is_empty() {
            true => None,
            false => Some(serde_json::to_string(&node.args_from)?),
        };
        // NULL for no list and for an empty list: the node takes no context.
        let workflow_ctx_from = stored_workflow_ctx_from(node)
            .map(serde_json::to_string)
            .transpose()?;
        self.node_workflow_ids.push(workflow_id);
        self.node_task_indexes.push(node_index(node.index)?);
        self.node_ids.push(node.node_id.clone());
        self.node_task_names.push(node.task_name.clone());
        self.node_inputs.push(node.input_json.clone());
        self.node_queue_names.push(queue.to_owned());
        self.node_priorities.push(priority);
        self.node_dependencies
            .push(serde_json::to_string(&dependencies)?);
        self.node_args_from.push(args_from);
        self.node_workflow_ctx_from.push(workflow_ctx_from);
        self.node_allow_failed_deps.push(node.allow_failed_deps);
        self.node_join_types.push(node.join.to_string());
        self.node_min_successes.push(node.min_success);
        self.node_task_options.push(merged_task_options);
        self.node_max_retries
            .push(task_facts.as_ref().map(|facts| facts.max_retries));
        self.node_executions.push(
            task_facts
                .as_ref()
                .map(|facts| facts.facts.execution.as_str().to_owned()),
        );
        self.node_good_untils
            .push(task_facts.as_ref().and_then(|facts| facts.good_until));
        self.node_retention_class_keys.push(
            task_facts
                .as_ref()
                .map(|facts| facts.facts.retention_class_key.clone()),
        );
        self.node_is_subworkflow.push(node.is_subworkflow);
        self.node_sub_workflow_names
            .push(match node.is_subworkflow {
                true => Some(
                    node.task_name
                        .strip_prefix("__sub_workflow:")
                        .unwrap_or(&node.task_name)
                        .to_owned(),
                ),
                false => None,
            });
        self.node_sub_definition_keys
            .push(node.sub_definition_key.clone());
        self.node_sub_workflow_params
            .push(match node.is_subworkflow {
                true => Some(node.sub_workflow_params.as_stored().to_owned()),
                false => None,
            });
        Ok(())
    }
}

impl StartArguments {
    /// Binds the workflow rows: 9 parameters.
    pub(crate) fn bind_workflows<'a>(&'a self, binder: ParameterBinder<'a>) -> ParameterBinder<'a> {
        binder
            .value(&self.workflow_ids)
            .value(&self.names)
            .value(&self.definition_keys)
            .value(&self.on_errors)
            .value(&self.output_task_indexes)
            .value(&self.success_policies)
            .value(&self.parent_workflow_ids)
            .value(&self.parent_task_indexes)
            .value(&self.depths)
    }

    /// Binds the node rows: 22 parameters.
    pub(crate) fn bind_nodes<'a>(&'a self, binder: ParameterBinder<'a>) -> ParameterBinder<'a> {
        binder
            .value(&self.node_workflow_ids)
            .value(&self.node_task_indexes)
            .value(&self.node_ids)
            .value(&self.node_task_names)
            .value(&self.node_inputs)
            .value(&self.node_queue_names)
            .value(&self.node_priorities)
            .value(&self.node_dependencies)
            .value(&self.node_args_from)
            .value(&self.node_workflow_ctx_from)
            .value(&self.node_allow_failed_deps)
            .value(&self.node_join_types)
            .value(&self.node_min_successes)
            .value(&self.node_task_options)
            .value(&self.node_max_retries)
            .value(&self.node_executions)
            .value(&self.node_good_untils)
            .value(&self.node_retention_class_keys)
            .value(&self.node_is_subworkflow)
            .value(&self.node_sub_workflow_names)
            .value(&self.node_sub_definition_keys)
            .value(&self.node_sub_workflow_params)
    }

    /// Binds the failed-node records: 3 parameters.
    pub(crate) fn bind_failed_records<'a>(
        &'a self,
        binder: ParameterBinder<'a>,
    ) -> ParameterBinder<'a> {
        binder
            .value(&self.failed_workflow_ids)
            .value(&self.failed_task_indexes)
            .value(&self.failed_results)
    }
}

#[cfg(test)]
impl StartArguments {
    /// The parent pair and the depth of the top row, for the tests of other
    /// modules.
    pub(crate) fn top_row_for_test(&self) -> (Option<Uuid>, Option<i32>, i32) {
        (
            self.parent_workflow_ids[0],
            self.parent_task_indexes[0],
            self.depths[0],
        )
    }
}

/// The trace parameters of the start statement (`StoredTrace::columns`).
struct TraceColumns<'a> {
    kind: &'static str,
    carrier: Option<&'a str>,
    trace_id: Option<&'a [u8]>,
}

impl<'a> TraceColumns<'a> {
    fn of(trace: &'a StoredTrace) -> TraceColumns<'a> {
        let (kind, carrier, trace_id) = trace.columns();
        TraceColumns {
            kind,
            carrier,
            trace_id,
        }
    }
}

/// The bound start statement, with `sent_at` as the start call time and the
/// trace parameters of `trace`.
fn start_statement<'a>(
    arguments: &'a StartArguments,
    sent_at: &'a DateTime<Utc>,
    trace: &'a TraceColumns<'a>,
) -> BoundStatement<'a> {
    let binder = arguments.bind_workflows(START.bind()).value(sent_at);
    arguments
        .bind_failed_records(arguments.bind_nodes(binder))
        .value(&trace.kind)
        .value(&trace.carrier)
        .value(&trace.trace_id)
        .finish()
}

/// Starts the tree as producer work ([`CheckoutClass::Batch`]), with
/// `sent_at` as the start call time and `trace` as the trace of every
/// workflow and task row of the tree.
///
/// | Arguments | Statements |
/// |---|---|
/// | no failed-node record | `horsies_workflow_start`, one statement in autocommit |
/// | failed-node records | `BEGIN`; `horsies_workflow_start`; `horsies_workflow_child_start` per owed child start; `COMMIT` |
///
/// Without failed-node records the function owes no child start: every
/// sub-workflow root has its pre-built child. A failed record runs the
/// failure policy and promotion of its node, which can promote a
/// sub-workflow node, so that form runs in a transaction.
///
/// Errors: [`WorkflowError::Broker`] when a statement failed (nothing
/// committed, or `OutcomeUnknown`: a resend with the same id meets `EXISTS`);
/// [`WorkflowError::StepContract`] for a row outside the contract (owed child
/// starts of the one-statement form included); the refusal of the host build
/// of an owed child (nothing committed).
pub(crate) async fn start_tree(
    broker: &PostgresBroker,
    arguments: &StartArguments,
    sent_at: DateTime<Utc>,
    trace: &StoredTrace,
    build: &dyn ChildSpecSource,
) -> Result<StartedTree, WorkflowError> {
    let trace = TraceColumns::of(trace);
    let statement = start_statement(arguments, &sent_at, &trace);
    let (disposition, detail, parent_advances) = match arguments.has_failed_records() {
        false => {
            let rows = run_alone(broker, CheckoutClass::Batch, &statement).await?;
            let step = step_rows::<StartDisposition>(&rows, &START)?;
            match step.owed_child_starts.len() {
                0 => {}
                count => {
                    return Err(StepContractError::ChildStartsInAutocommit {
                        function: START.name(),
                        count,
                    }
                    .into())
                }
            }
            (
                step.outcome.disposition,
                step.outcome.detail,
                OwedParentAdvances::default(),
            )
        }
        true => {
            let committed = run_in_transaction::<StartDisposition>(
                broker,
                CheckoutClass::Batch,
                &statement,
                build,
            )
            .await?;
            (
                committed.disposition,
                committed.detail,
                committed.parent_advances,
            )
        }
    };
    Ok(StartedTree {
        outcome: match disposition {
            StartDisposition::Started => StartOutcome::Started,
            StartDisposition::Exists => StartOutcome::Exists {
                stored_name: detail,
            },
        },
        parent_advances,
    })
}

#[cfg(test)]
mod tests;

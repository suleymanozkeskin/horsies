//! A built workflow spec as data: the form in which a client that keeps its
//! workflow DSL in its own language (the Python client) sends a spec to the
//! core.
//!
//! The client builds and checks the spec with its DSL, then sends the facts
//! that the workflow row and the node rows store, as JSON text. The core
//! parses the text into a [`ClientSpec`] and builds the rows from it, as it
//! does for a spec of the Rust client. `horsies-codegen` renders the types as
//! frozen dataclasses in `_catalog/workflow_spec.py`
//! ([`crate::manifest_schema::WORKFLOW_SPEC_DATA`]).
//!
//! Rules:
//! - No field has a default, and an unknown field is refused.
//! - Each node is at its index: `nodes[i].index == i`. Every index that the
//!   spec names (dependencies, `args_from`, the output, the success policy)
//!   names a node of the spec.
//! - The task options of a task node are the client's text, with the node's
//!   deadline already merged into them (`good_until`), as the node row stores
//!   them.
//! - The retention class of each task queue is the class that the client's
//!   queue mapping gives it, as for a send: `queue_classes` holds every queue
//!   of the task nodes, and no other queue.
//!
//! A text that the parse refuses is a defect of the client, not a user
//! error: the client refuses user errors when it builds the spec. The
//! refusal names the field path and never a value.

use std::collections::{BTreeMap, HashMap};

use serde::Deserialize;

use crate::core::config::bounded::BoundedU32;
use crate::core::workflow::node::{AnyNode, JoinType, SubWorkflowParameters};
use crate::core::workflow::policy::{SuccessCase, SuccessPolicy};
use crate::core::workflow::spec::MAX_WORKFLOW_NODES;
use crate::core::{OnError, WorkflowSpec};
use crate::manifest::parse_manifest;
use crate::manifest_schema::{IntBound, ManifestValue, ValueType};
use crate::scheduler_manifest::RetentionClassKey;
use crate::worker_manifest::QueuePriority;
use crate::workflow_engine::child_spec::ClientSpec;

/// The one spec data version this core reads.
pub const WORKFLOW_SPEC_DATA_VERSION: u32 = 1;

/// The largest node index of a spec.
const MAX_NODE_INDEX: u32 = MAX_WORKFLOW_NODES as u32 - 1;

/// A spec that the parse refused. Nothing was written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("workflow spec data refused at {field_path}: {cause}")]
pub struct SpecDataRefused {
    /// The path of the field, for example `nodes[2].dependencies[0]`, or
    /// `.` for the document itself.
    pub field_path: String,
    /// What is wrong; never the value.
    pub cause: String,
}

impl SpecDataRefused {
    fn at(field_path: String, cause: impl Into<String>) -> Self {
        Self {
            field_path,
            cause: cause.into(),
        }
    }
}

/// The spec data version. Only the parser constructs it, and it accepts
/// only [`WORKFLOW_SPEC_DATA_VERSION`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct SpecDataVersion(BoundedU32<WORKFLOW_SPEC_DATA_VERSION, WORKFLOW_SPEC_DATA_VERSION>);

impl ManifestValue for SpecDataVersion {
    const VALUE_TYPE: ValueType = ValueType::Int(IntBound {
        name: "SPEC_DATA_VERSION",
        min: WORKFLOW_SPEC_DATA_VERSION as u64,
        max: WORKFLOW_SPEC_DATA_VERSION as u64,
    });
}

/// The index of a node in its spec: 0 to `MAX_WORKFLOW_NODES - 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct NodeIndex(BoundedU32<0, MAX_NODE_INDEX>);

impl NodeIndex {
    /// The index as a position in the node list.
    fn position(self) -> usize {
        // At most `MAX_NODE_INDEX`, which fits every `usize` of a target the
        // core builds for.
        self.0.get() as usize
    }
}

impl ManifestValue for NodeIndex {
    const VALUE_TYPE: ValueType = ValueType::Int(IntBound {
        name: "NODE_INDEX",
        min: 0,
        max: MAX_NODE_INDEX as u64,
    });
}

/// The successes that a quorum join needs: 1 to `MAX_WORKFLOW_NODES - 1`
/// (at most the dependencies of the node).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct MinSuccess(BoundedU32<1, MAX_NODE_INDEX>);

impl ManifestValue for MinSuccess {
    const VALUE_TYPE: ValueType = ValueType::Int(IntBound {
        name: "MIN_SUCCESS",
        min: 1,
        max: MAX_NODE_INDEX as u64,
    });
}

/// A built workflow spec: the facts that its workflow row and its node rows
/// store.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, horsies_core_derive::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSpecData {
    /// Always `SPEC_DATA_VERSION`, the one version this core reads.
    pub data_version: SpecDataVersion,
    /// The workflow name.
    pub name: String,
    pub definition_key: SpecDefinitionKey,
    pub on_error: SpecOnError,
    pub output: SpecOutput,
    pub success_policy: SpecSuccessPolicy,
    /// The retention class key of each queue of the task nodes, as the
    /// client's queue mapping resolves it: `forever`, or
    /// `standard_<days>d`.
    pub queue_classes: BTreeMap<String, RetentionClassKey>,
    /// The nodes, each at its index.
    pub nodes: Vec<SpecNode>,
}

/// The stable definition key of the workflow.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, horsies_core_derive::ManifestSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum SpecDefinitionKey {
    /// The spec has a definition key.
    Keyed { key: String },
    /// The spec has none. A start refuses such a spec; a child built by its
    /// definition may have none.
    Unkeyed,
}

/// What a failed node does to its workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, horsies_core_derive::ManifestSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SpecOnError {
    /// The workflow fails.
    Fail,
    /// The workflow pauses.
    Pause,
}

/// The node whose result is the workflow's result.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, horsies_core_derive::ManifestSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum SpecOutput {
    /// The result of the node at `index`.
    Node { index: NodeIndex },
    /// The workflow has no output node.
    NoOutput,
}

/// When the workflow succeeds.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, horsies_core_derive::ManifestSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum SpecSuccessPolicy {
    /// The workflow succeeds when one case has every required node
    /// COMPLETED; the `optional` nodes may fail.
    Cases {
        cases: Vec<SpecSuccessCase>,
        optional: Vec<NodeIndex>,
    },
    /// The default rule: every node succeeds.
    NoPolicy,
}

/// One case of a success policy.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, horsies_core_derive::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct SpecSuccessCase {
    /// The nodes that must be COMPLETED.
    pub required: Vec<NodeIndex>,
}

/// One node of the spec.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, horsies_core_derive::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct SpecNode {
    /// The position of the node in the spec.
    pub index: NodeIndex,
    /// The node id, unique in the spec.
    pub node_id: String,
    /// The task name of a task node; the child workflow's name of a
    /// sub-workflow node.
    pub task_name: String,
    /// The node input as one JSON value; the row stores the text as
    /// written.
    pub input_json: String,
    /// The nodes that this node waits for.
    pub dependencies: Vec<NodeIndex>,
    /// The input members that take the result of a dependency, by member
    /// name.
    pub args_from: BTreeMap<String, NodeIndex>,
    /// The node ids of the dependencies whose results the node's workflow
    /// context takes; empty when the node takes no context.
    pub workflow_context_from: Vec<String>,
    /// The node runs when a dependency failed.
    pub allow_failed_dependencies: bool,
    pub join: SpecJoin,
    pub body: SpecNodeBody,
}

/// When a node with dependencies becomes ready.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, horsies_core_derive::ManifestSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum SpecJoin {
    /// Every dependency is terminal.
    All,
    /// One dependency is COMPLETED.
    Any,
    /// `min_success` dependencies are COMPLETED.
    Quorum { min_success: MinSuccess },
}

/// What a node runs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, horsies_core_derive::ManifestSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum SpecNodeBody {
    /// A task.
    Task {
        /// The queue of the task, resolved from the client's queue config.
        queue: String,
        /// The priority on that queue: 1 is claimed first, 100 last.
        priority: QueuePriority,
        task_options: SpecTaskOptions,
    },
    /// A child workflow.
    SubWorkflow {
        /// The definition key that resolves the child.
        definition_key: String,
        params: SpecSubWorkflowParams,
    },
}

/// The task options of a task node.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, horsies_core_derive::ManifestSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum SpecTaskOptions {
    /// The options text as the node row stores it: the text that the client
    /// encoded when the task was defined, with the node's deadline merged.
    Encoded { json: String },
    /// The task has no options and the node no deadline.
    NoOptions,
}

/// Whether a sub-workflow node gives its child params for the child's
/// build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, horsies_core_derive::ManifestSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SpecSubWorkflowParams {
    /// The node gives params.
    Given,
    /// The node gives none.
    NotGiven,
}

/// Parse the JSON text of a [`WorkflowSpecData`] into the spec the core
/// builds rows from.
///
/// Pure: no I/O. Errors: [`SpecDataRefused`] with the field path for a text
/// that does not parse, an unknown or missing field, a value outside its
/// bounds, a node away from its index, an index that names no node, and a
/// task queue without a class or a class of no task queue.
pub fn parse_workflow_spec_data(text: &str) -> Result<ClientSpec, SpecDataRefused> {
    let data: WorkflowSpecData = parse_manifest(text).map_err(|refused| SpecDataRefused {
        field_path: refused.field_path,
        cause: refused.cause,
    })?;
    data.into_client_spec()
}

impl WorkflowSpecData {
    /// The spec and its queue classes, after the checks that the field types
    /// cannot hold. Errors: as [`parse_workflow_spec_data`].
    fn into_client_spec(self) -> Result<ClientSpec, SpecDataRefused> {
        let node_count = self.nodes.len();
        let output_index = match self.output {
            SpecOutput::Node { index } => {
                Some(named_node(index, node_count, "output.index".to_owned())?)
            }
            SpecOutput::NoOutput => None,
        };
        let success_policy = success_policy(self.success_policy, node_count)?;
        let mut task_queues: BTreeMap<String, ()> = BTreeMap::new();
        let mut tasks = Vec::with_capacity(node_count);
        for (position, node) in self.nodes.into_iter().enumerate() {
            let any_node = any_node(position, node, node_count)?;
            if let Some(queue) = &any_node.queue {
                task_queues.insert(queue.clone(), ());
            }
            tasks.push(any_node);
        }
        let queue_classes = queue_classes(self.queue_classes, &task_queues)?;
        let spec = WorkflowSpec {
            name: self.name,
            definition_key: match self.definition_key {
                SpecDefinitionKey::Keyed { key } => Some(key),
                SpecDefinitionKey::Unkeyed => None,
            },
            tasks,
            on_error: match self.on_error {
                SpecOnError::Fail => OnError::Fail,
                SpecOnError::Pause => OnError::Pause,
            },
            output_index,
            success_policy,
        };
        Ok(ClientSpec::new(spec, queue_classes))
    }
}

/// The position of `index` when it names one of the `node_count` nodes.
fn named_node(
    index: NodeIndex,
    node_count: usize,
    field_path: String,
) -> Result<usize, SpecDataRefused> {
    let position = index.position();
    match position < node_count {
        true => Ok(position),
        false => Err(SpecDataRefused::at(
            field_path,
            format!("names no node of the spec, which has {node_count} nodes"),
        )),
    }
}

/// The positions of `indexes`, each naming a node.
fn named_nodes(
    indexes: Vec<NodeIndex>,
    node_count: usize,
    field_path: &str,
) -> Result<Vec<usize>, SpecDataRefused> {
    indexes
        .into_iter()
        .enumerate()
        .map(|(item, index)| named_node(index, node_count, format!("{field_path}[{item}]")))
        .collect()
}

/// The core's success policy of the data. An empty `optional` list is no
/// optional list, as the client's stored form has none.
fn success_policy(
    policy: SpecSuccessPolicy,
    node_count: usize,
) -> Result<Option<SuccessPolicy>, SpecDataRefused> {
    match policy {
        SpecSuccessPolicy::NoPolicy => Ok(None),
        SpecSuccessPolicy::Cases { cases, optional } => {
            let cases = cases
                .into_iter()
                .enumerate()
                .map(|(item, case)| {
                    Ok(SuccessCase {
                        required_indices: named_nodes(
                            case.required,
                            node_count,
                            &format!("success_policy.cases[{item}].required"),
                        )?,
                    })
                })
                .collect::<Result<Vec<SuccessCase>, SpecDataRefused>>()?;
            let optional = named_nodes(optional, node_count, "success_policy.optional")?;
            Ok(Some(SuccessPolicy {
                cases,
                optional_indices: match optional.is_empty() {
                    true => None,
                    false => Some(optional),
                },
            }))
        }
    }
}

/// The core node of the data node at `position`.
fn any_node(
    position: usize,
    node: SpecNode,
    node_count: usize,
) -> Result<AnyNode, SpecDataRefused> {
    let path = format!("nodes[{position}]");
    if node.index.position() != position {
        return Err(SpecDataRefused::at(
            format!("{path}.index"),
            "a node is not at its index",
        ));
    }
    let dependencies = named_nodes(
        node.dependencies,
        node_count,
        &format!("{path}.dependencies"),
    )?;
    let args_from = node
        .args_from
        .into_iter()
        .map(|(member, index)| {
            let field_path = format!("{path}.args_from.{member}");
            named_node(index, node_count, field_path).map(|source| (member, source))
        })
        .collect::<Result<HashMap<String, usize>, SpecDataRefused>>()?;
    let (join, min_success) = match node.join {
        SpecJoin::All => (JoinType::All, None),
        SpecJoin::Any => (JoinType::Any, None),
        // At most `MAX_NODE_INDEX`, which fits an `i32`.
        SpecJoin::Quorum { min_success } => (JoinType::Quorum, Some(min_success.0.get() as i32)),
    };
    let body = NodeBody::of(node.body);
    Ok(AnyNode {
        task_name: node.task_name,
        input_json: node.input_json,
        dependencies,
        args_from,
        workflow_context_from: match node.workflow_context_from.is_empty() {
            true => None,
            false => Some(node.workflow_context_from),
        },
        queue: body.queue,
        priority: body.priority,
        allow_failed_dependencies: node.allow_failed_dependencies,
        join,
        min_success,
        // The deadline is merged into the task options already.
        good_until: None,
        index: position,
        node_id: Some(node.node_id),
        task_options_json: body.task_options_json,
        is_subworkflow: body.is_subworkflow,
        sub_definition_key: body.sub_definition_key,
        sub_workflow_params: body.sub_workflow_params,
    })
}

/// The fields of a core node that the body of a data node sets.
struct NodeBody {
    queue: Option<String>,
    priority: Option<i32>,
    task_options_json: Option<String>,
    is_subworkflow: bool,
    sub_definition_key: Option<String>,
    sub_workflow_params: SubWorkflowParameters,
}

impl NodeBody {
    /// Pure.
    fn of(body: SpecNodeBody) -> Self {
        match body {
            SpecNodeBody::Task {
                queue,
                priority,
                task_options,
            } => Self {
                queue: Some(queue),
                // At most `MAX_QUEUE_PRIORITY` (100), which fits an `i32`.
                priority: Some(priority.0.get() as i32),
                task_options_json: match task_options {
                    SpecTaskOptions::Encoded { json } => Some(json),
                    SpecTaskOptions::NoOptions => None,
                },
                is_subworkflow: false,
                sub_definition_key: None,
                sub_workflow_params: SubWorkflowParameters::NotGiven,
            },
            SpecNodeBody::SubWorkflow {
                definition_key,
                params,
            } => Self {
                queue: None,
                priority: None,
                task_options_json: None,
                is_subworkflow: true,
                sub_definition_key: Some(definition_key),
                sub_workflow_params: match params {
                    SpecSubWorkflowParams::Given => SubWorkflowParameters::Given,
                    SpecSubWorkflowParams::NotGiven => SubWorkflowParameters::NotGiven,
                },
            },
        }
    }
}

/// The stored class key of each task queue. `classes` must hold exactly the
/// queues of the task nodes.
fn queue_classes(
    classes: BTreeMap<String, RetentionClassKey>,
    task_queues: &BTreeMap<String, ()>,
) -> Result<BTreeMap<String, String>, SpecDataRefused> {
    if let Some(queue) = task_queues
        .keys()
        .find(|queue| !classes.contains_key(*queue))
    {
        return Err(SpecDataRefused::at(
            "queue_classes".to_owned(),
            format!("has no class for the task queue {queue:?}"),
        ));
    }
    if let Some(queue) = classes
        .keys()
        .find(|queue| !task_queues.contains_key(*queue))
    {
        return Err(SpecDataRefused::at(
            format!("queue_classes.{queue}"),
            "names a queue of no task node",
        ));
    }
    Ok(classes
        .into_iter()
        .map(|(queue, key)| (queue, key.retention().class_key()))
        .collect())
}

#[cfg(test)]
mod tests;

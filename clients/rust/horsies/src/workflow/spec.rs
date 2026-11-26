//! The workflow spec builder and the built spec (Python `WorkflowSpec`).
//!
//! A builder comes from an app (`Horsies::workflow_builder`) or from the
//! runtime of a task body (`TaskRuntime::workflow_builder`), and holds the
//! typed functions of that app. It resolves each task node against them
//! when the node is added: the queue, the options and the workflow context
//! flag of the task's registration, or the refusal `HRS-302` for a task
//! that the app does not register with the node's types.
//!
//! The builder takes nodes, each after the nodes it references: a node's
//! dependencies, `args_from` sources and context sources are nodes added
//! before it. So a spec has no cycle, and its first node is a root.
//! `build` checks the rest, in Python's two gates
//! (`WorkflowSpec.__post_init__`):
//!
//! 1. The name, the node count, the node ids, and the references: every
//!    reference names a node of this builder. An error here stops the
//!    build.
//! 2. Every other check; their errors are reported together: the context
//!    sources, the join rules, the priorities, the success policy, the
//!    static input against the `args_from` members, and the input of each
//!    node decoded with the input type of its task or child workflow (keys
//!    that bind to no field, missing fields, values of another type).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use horsies_core::core::workflow::spec::MAX_WORKFLOW_NODES;
use horsies_core::OnError;

use super::dependency::{placeholder_context_text, placeholder_result_text, OutputlessResults};
use super::input::NodeInput;
use super::node::{
    BuilderId, InputCheck, JoinType, NodeDeadline, NodeIdChoice, NodeKey, NodeLinks, NodePriority,
    NodeQueue, NodeRef, NodeRegistration, OutputType, TaskNode, TypedNodeRef,
};
use super::sub_workflow::{NodeParams, SubWorkflowNode};
use crate::send::TaskFunctions;
use crate::task::body::DecodeFailure;
use crate::{ErrorCode, HorsiesError, TaskFunctionNotFound, ValidationReport};

/// The longest node id (Python `NODE_ID_MAX_LENGTH`).
const MAX_NODE_ID_LENGTH: usize = 128;

/// The lowest and the highest priority of a task node.
const PRIORITY_RANGE: std::ops::RangeInclusive<u32> = 1..=100;

/// The input member that takes the workflow context (the task attribute's
/// `workflow_ctx`).
const WORKFLOW_CTX_FIELD: &str = "workflow_ctx";

// ---------------------------------------------------------------------------
// Success policy
// ---------------------------------------------------------------------------

/// One way for a workflow to succeed: every node of `required` COMPLETED.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuccessCase {
    required: Vec<NodeRef>,
}

impl SuccessCase {
    /// The case that requires each of `required`.
    pub fn new<I, R>(required: I) -> Self
    where
        I: IntoIterator<Item = R>,
        R: Into<NodeRef>,
    {
        Self {
            required: required.into_iter().map(Into::into).collect(),
        }
    }
}

/// When a workflow succeeds (Python `SuccessPolicy`): when one case has
/// every required node COMPLETED.
///
/// The optional nodes are declarative: they record which nodes may fail,
/// and they have no runtime effect. A node that no case requires does not
/// change the result, with or without [`Self::optional`]. The spec build
/// refuses an optional node of another workflow builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuccessPolicy {
    cases: Vec<SuccessCase>,
    optional: Vec<NodeRef>,
}

impl SuccessPolicy {
    /// The policy of `cases`, with no optional node.
    pub fn new(cases: Vec<SuccessCase>) -> Self {
        Self {
            cases,
            optional: Vec::new(),
        }
    }

    /// The policy with `optional` recorded as the nodes that may fail.
    /// Declarative: no runtime effect.
    pub fn optional<I, R>(mut self, optional: I) -> Self
    where
        I: IntoIterator<Item = R>,
        R: Into<NodeRef>,
    {
        self.optional = optional.into_iter().map(Into::into).collect();
        self
    }

    fn refs(&self) -> impl Iterator<Item = &NodeRef> {
        self.cases
            .iter()
            .flat_map(|case| case.required.iter())
            .chain(self.optional.iter())
    }
}

// ---------------------------------------------------------------------------
// The built spec
// ---------------------------------------------------------------------------

/// What a node of a built spec runs.
#[derive(Debug, Clone)]
pub(crate) enum SpecBody {
    Task {
        input: NodeInput,
        queue: String,
        priority: NodePriority,
        /// The task's stored options text, without the node's deadline.
        task_options: Option<String>,
        deadline: NodeDeadline,
    },
    SubWorkflow {
        definition_key: String,
        params: NodeParams,
    },
}

/// One node of a built spec.
#[derive(Debug, Clone)]
pub(crate) struct SpecNode {
    pub(crate) index: usize,
    pub(crate) node_id: String,
    /// The task name, or the child's workflow name.
    pub(crate) name: String,
    pub(crate) dependencies: Vec<usize>,
    pub(crate) args_from: BTreeMap<String, usize>,
    /// The node ids of the context sources.
    pub(crate) workflow_ctx_from: Vec<String>,
    pub(crate) allow_failed_deps: bool,
    pub(crate) join: JoinType,
    pub(crate) body: SpecBody,
    pub(crate) output: OutputType,
}

/// The node whose result is the workflow's result.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SpecOutput {
    Node {
        index: usize,
        output: OutputType,
    },
    /// The workflow's result is the outcome of each node
    /// ([`OutputlessResults`]).
    NoOutput,
}

/// A success policy by node index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpecSuccessPolicy {
    pub(crate) cases: Vec<Vec<usize>>,
    pub(crate) optional: Vec<usize>,
}

/// A built and checked workflow spec (Python `WorkflowSpec`). Immutable.
#[derive(Debug, Clone)]
pub struct WorkflowSpec {
    builder: BuilderId,
    pub(crate) name: String,
    pub(crate) definition_key: Option<String>,
    pub(crate) on_error: OnError,
    pub(crate) nodes: Vec<SpecNode>,
    pub(crate) output: SpecOutput,
    pub(crate) success_policy: Option<SpecSuccessPolicy>,
}

impl WorkflowSpec {
    /// The workflow name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The definition key, when the spec has one. A start refuses a spec
    /// without one.
    pub fn definition_key(&self) -> Option<&str> {
        self.definition_key.as_deref()
    }

    /// What a failed node does to the workflow.
    pub fn on_error(&self) -> OnError {
        self.on_error
    }

    /// The number of nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// The node ids, in node order.
    pub fn node_ids(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().map(|node| node.node_id.as_str())
    }

    /// The key of the result of `node`, with its node id as built; `None`
    /// for a reference of another builder.
    pub fn key<T>(&self, node: TypedNodeRef<T>) -> Option<NodeKey<T>> {
        let node = node.untyped();
        match node.builder() == self.builder {
            true => self
                .nodes
                .get(node.index())
                .map(|built| NodeKey::new(built.node_id.clone())),
            false => None,
        }
    }

    /// Refuse a spec whose output type is not `T`: the type of its output
    /// node, or [`OutputlessResults`] for a spec with no output node (Python
    /// `validate_workflow_generic_output_match`).
    ///
    /// Errors: `WORKFLOW_OUTPUT_TYPE_MISMATCH` (HRS-025).
    pub(crate) fn check_output_type<T: 'static>(&self) -> Result<(), HorsiesError> {
        let declared = OutputType::of::<T>();
        let (actual, matches) = match self.output {
            SpecOutput::Node { output, .. } => (output.name(), output == declared),
            SpecOutput::NoOutput => (
                "no output node",
                declared == OutputType::of::<OutputlessResults>(),
            ),
        };
        match matches {
            true => Ok(()),
            false => Err(HorsiesError::new(format!(
                "workflow '{}' is declared with output type {} but its output is {actual}",
                self.name,
                declared.name()
            ))
            .with_code(ErrorCode::WorkflowOutputTypeMismatch)
            .with_help(
                "declare the output node's type, or OutputlessResults for a workflow with no \
                 output node",
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// The builder
// ---------------------------------------------------------------------------

/// What a node of the builder runs.
enum BuilderBody {
    Task {
        input: NodeInput,
        queue: String,
        priority: NodePriority,
        task_options: Option<String>,
        accepts_workflow_ctx: bool,
        deadline: NodeDeadline,
        input_check: InputCheck,
    },
    SubWorkflow {
        definition_key: String,
        params: NodeParams,
        params_check: InputCheck,
    },
}

struct BuilderNode {
    name: String,
    links: NodeLinks,
    body: BuilderBody,
    output: OutputType,
}

/// The builder of a workflow spec (Python `app.workflow(...)`), made from
/// an app with `Horsies::workflow_builder` or from the runtime of a task
/// body with `TaskRuntime::workflow_builder`.
///
/// ```ignore
/// let mut builder = app.workflow_builder("checkout");
/// builder.definition_key("shop.checkout.v1");
/// let charge = builder.task(charge::node().set_input(order)?)?;
/// let notify = builder.task(notify::node().arg_from(notify::params::charged(), charge))?;
/// builder.output(notify);
/// let spec = builder.build()?;
/// ```
pub struct WorkflowSpecBuilder {
    id: BuilderId,
    name: String,
    definition_key: Option<String>,
    on_error: OnError,
    nodes: Vec<BuilderNode>,
    output: Option<NodeRef>,
    success_policy: Option<SuccessPolicy>,
    /// The typed functions of the builder's app: the task nodes resolve
    /// against them.
    tasks: Arc<TaskFunctions>,
}

impl std::fmt::Debug for WorkflowSpecBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowSpecBuilder")
            .field("name", &self.name)
            .field("definition_key", &self.definition_key)
            .field("nodes", &self.nodes.len())
            .finish_non_exhaustive()
    }
}

impl WorkflowSpecBuilder {
    /// A builder of the workflow `name` whose task nodes resolve against
    /// `tasks`, with `on_error` FAIL and no node. `Horsies::workflow_builder`
    /// and `TaskRuntime::workflow_builder` call it with the typed functions
    /// of their app.
    pub(crate) fn new(name: impl Into<String>, tasks: Arc<TaskFunctions>) -> Self {
        Self {
            id: BuilderId::next(),
            name: name.into(),
            definition_key: None,
            on_error: OnError::Fail,
            nodes: Vec::new(),
            output: None,
            success_policy: None,
            tasks,
        }
    }

    /// Add a task node; its reference has the task's output type. The node
    /// takes the queue (unless it names one), the options and the workflow
    /// context flag of its task's registration on the builder's app.
    ///
    /// Errors, and the node is not added: `WORKFLOW_UNREGISTERED_TASK`
    /// (HRS-302) when the builder's app does not register the task, or
    /// registers a task of this name with another input or output type.
    pub fn task<T: 'static, I: 'static>(
        &mut self,
        node: TaskNode<T, I>,
    ) -> Result<TypedNodeRef<T>, HorsiesError> {
        let registration = self
            .tasks
            .node_registration::<T, I>(&node.task_name)
            .map_err(|not_found| unregistered_task(&self.name, &not_found))?;
        Ok(self.add_task(node, registration))
    }

    /// Add `node`, a node of the task whose registration is `registration`.
    /// No check: [`Self::task`] found the registration.
    pub(crate) fn add_task<T: 'static, I>(
        &mut self,
        node: TaskNode<T, I>,
        registration: NodeRegistration,
    ) -> TypedNodeRef<T> {
        let queue = match node.queue {
            NodeQueue::OfTask => registration.queue_name,
            NodeQueue::Given(queue) => queue,
        };
        self.push(BuilderNode {
            name: node.task_name,
            links: node.links,
            body: BuilderBody::Task {
                input: node.input,
                queue,
                priority: node.priority,
                task_options: registration.task_options,
                accepts_workflow_ctx: registration.accepts_workflow_ctx,
                deadline: node.deadline,
                input_check: node.input_check,
            },
            output: OutputType::of::<T>(),
        })
    }

    /// Add a sub-workflow node; its reference has the child's output type.
    pub fn sub_workflow<P, T: 'static>(&mut self, node: SubWorkflowNode<P, T>) -> TypedNodeRef<T> {
        self.push(BuilderNode {
            name: node.workflow_name,
            links: node.links,
            body: BuilderBody::SubWorkflow {
                definition_key: node.definition_key,
                params: node.params,
                params_check: node.params_check,
            },
            output: OutputType::of::<T>(),
        })
    }

    fn push<T>(&mut self, node: BuilderNode) -> TypedNodeRef<T> {
        let reference = NodeRef::new(self.id, self.nodes.len());
        self.nodes.push(node);
        TypedNodeRef::new(reference)
    }

    /// Set the stable definition key, the identity under which a start and
    /// the engine find the workflow.
    pub fn definition_key(&mut self, key: impl Into<String>) -> &mut Self {
        self.definition_key = Some(key.into());
        self
    }

    /// Set what a failed node does to the workflow.
    pub fn on_error(&mut self, on_error: OnError) -> &mut Self {
        self.on_error = on_error;
        self
    }

    /// Set the node whose result is the workflow's result.
    pub fn output<R: Into<NodeRef>>(&mut self, node: R) -> &mut Self {
        self.output = Some(node.into());
        self
    }

    /// Set when the workflow succeeds.
    pub fn success_policy(&mut self, policy: SuccessPolicy) -> &mut Self {
        self.success_policy = Some(policy);
        self
    }

    /// Check the spec and build it.
    ///
    /// Errors: the refusals of the two gates (module docs), with Python's
    /// codes. One refusal keeps its code; several are one error with a note
    /// each.
    pub fn build(self) -> Result<WorkflowSpec, HorsiesError> {
        let mut first = ValidationReport::new("node_id_validation");
        let node_ids = self.node_ids(&mut first);
        first.into_result()?;
        let mut second = ValidationReport::new("workflow_validation");
        for (index, node) in self.nodes.iter().enumerate() {
            self.check_node(index, node, &node_ids, &mut second);
        }
        self.check_success_policy(&mut second);
        second.into_result()?;
        Ok(self.into_spec(&node_ids))
    }

    // -- gate 1 ------------------------------------------------------------

    /// The node id of each node, with the refusals of the first gate added
    /// to `report`.
    fn node_ids(&self, report: &mut ValidationReport) -> Vec<String> {
        if self.name.trim().is_empty() {
            report.add(
                HorsiesError::new("workflow name must not be empty")
                    .with_code(ErrorCode::WorkflowNoName),
            );
        }
        if self.nodes.is_empty() {
            report.add(
                HorsiesError::new(format!("workflow '{}' has no node", self.name))
                    .with_code(ErrorCode::WorkflowNoNodes),
            );
            return Vec::new();
        }
        if self.nodes.len() > MAX_WORKFLOW_NODES {
            report.add(
                HorsiesError::new(format!(
                    "WorkflowSpec has {} nodes, more than the limit of {MAX_WORKFLOW_NODES}",
                    self.nodes.len()
                ))
                .with_code(ErrorCode::WorkflowTooManyNodes)
                .with_help(
                    "split the nodes into sub-workflow nodes; each child workflow has its own \
                     limit",
                ),
            );
            return Vec::new();
        }
        let slug = slugify(&self.name);
        let node_ids: Vec<String> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| match &node.links.node_id {
                NodeIdChoice::Given(id) => id.clone(),
                NodeIdChoice::Auto => format!("{slug}:{index}"),
            })
            .collect();
        let mut seen: HashMap<&str, usize> = HashMap::new();
        for (index, id) in node_ids.iter().enumerate() {
            if !is_valid_node_id(id) {
                report.add(
                    HorsiesError::new(format!("invalid node_id {id:?} at index {index}"))
                        .with_code(ErrorCode::WorkflowInvalidNodeId)
                        .with_help(format!(
                            "a node_id matches [A-Za-z0-9_\\-:.]+ and has at most \
                             {MAX_NODE_ID_LENGTH} characters"
                        )),
                );
            }
            match seen.insert(id.as_str(), index) {
                None => {}
                Some(earlier) => report.add(
                    HorsiesError::new(format!(
                        "duplicate node_id '{id}' at indices {earlier} and {index}"
                    ))
                    .with_code(ErrorCode::WorkflowDuplicateNodeId),
                ),
            }
        }
        self.check_references(report);
        node_ids
    }

    /// Refuse a reference of another builder.
    fn check_references(&self, report: &mut ValidationReport) {
        for (index, node) in self.nodes.iter().enumerate() {
            let links = &node.links;
            let foreign = links
                .dependencies
                .iter()
                .chain(links.args_from.values())
                .chain(links.workflow_ctx_from.iter())
                .any(|reference| reference.builder() != self.id);
            if foreign {
                report.add(
                    HorsiesError::new(format!(
                        "node '{}' at index {index} references a node of another workflow builder",
                        node.name
                    ))
                    .with_code(ErrorCode::WorkflowInvalidDependency)
                    .with_help("use the references that this builder returned"),
                );
            }
        }
        if let Some(output) = &self.output {
            if output.builder() != self.id {
                report.add(
                    HorsiesError::new("the output references a node of another workflow builder")
                        .with_code(ErrorCode::WorkflowInvalidOutput),
                );
            }
        }
        if let Some(policy) = &self.success_policy {
            if policy
                .refs()
                .any(|reference| reference.builder() != self.id)
            {
                report.add(
                    HorsiesError::new(
                        "the success policy references a node of another workflow builder",
                    )
                    .with_code(ErrorCode::WorkflowInvalidSuccessPolicy),
                );
            }
        }
    }

    // -- gate 2 ------------------------------------------------------------

    fn check_node(
        &self,
        index: usize,
        node: &BuilderNode,
        node_ids: &[String],
        report: &mut ValidationReport,
    ) {
        let owner = format!("'{}' at index {index}", node.name);
        check_join(&owner, &node.links, report);
        for source in &node.links.workflow_ctx_from {
            if !node.links.dependencies.contains(source) {
                report.add(
                    HorsiesError::new("workflow_ctx_from references a node not in waits_for")
                        .with_code(ErrorCode::WorkflowInvalidCtxFrom)
                        .with_note(format!(
                            "node {owner} references '{}'",
                            node_ids[source.index()]
                        ))
                        .with_help("add the node to waits_for"),
                );
            }
        }
        match &node.body {
            BuilderBody::Task {
                input,
                priority,
                accepts_workflow_ctx,
                input_check,
                ..
            } => {
                if !node.links.workflow_ctx_from.is_empty() && !accepts_workflow_ctx {
                    report.add(
                        HorsiesError::new(
                            "workflow_ctx_from declared but the task takes no workflow context",
                        )
                        .with_code(ErrorCode::WorkflowCtxParamMissing)
                        .with_note(format!("node {owner} declares workflow_ctx_from"))
                        .with_help(
                            "add `workflow_ctx` to the task attribute and a `workflow_ctx: \
                             WorkflowContext` parameter, or remove workflow_ctx_from",
                        ),
                    );
                }
                if let NodePriority::Given(given) = priority {
                    if !PRIORITY_RANGE.contains(given) {
                        report.add(
                            HorsiesError::new(format!(
                                "node {owner} has priority {given}; a priority is 1 to 100"
                            ))
                            .with_code(ErrorCode::WorkflowInvalidPriority),
                        );
                    }
                }
                let takes_context =
                    *accepts_workflow_ctx && !node.links.workflow_ctx_from.is_empty();
                check_input(
                    &owner,
                    input,
                    &node.links,
                    takes_context,
                    *input_check,
                    report,
                );
            }
            BuilderBody::SubWorkflow {
                params,
                params_check,
                ..
            } => match params {
                NodeParams::NotGiven => {}
                NodeParams::Given(input) => {
                    check_input(&owner, input, &node.links, false, *params_check, report);
                }
            },
        }
    }

    fn check_success_policy(&self, report: &mut ValidationReport) {
        let Some(policy) = &self.success_policy else {
            return;
        };
        if policy.cases.is_empty() {
            report.add(
                HorsiesError::new("SuccessPolicy must have at least one SuccessCase")
                    .with_code(ErrorCode::WorkflowInvalidSuccessPolicy)
                    .with_help("add at least one SuccessCase"),
            );
        }
        for (position, case) in policy.cases.iter().enumerate() {
            if case.required.is_empty() {
                report.add(
                    HorsiesError::new(format!("SuccessCase[{position}] has no required node"))
                        .with_code(ErrorCode::WorkflowInvalidSuccessPolicy)
                        .with_help("add at least one node to the required list"),
                );
            }
        }
    }

    // -- the spec ----------------------------------------------------------

    fn into_spec(self, node_ids: &[String]) -> WorkflowSpec {
        let nodes = self
            .nodes
            .into_iter()
            .enumerate()
            .map(|(index, node)| spec_node(index, node, node_ids))
            .collect::<Vec<SpecNode>>();
        let output = match self.output {
            Some(reference) => SpecOutput::Node {
                index: reference.index(),
                output: nodes[reference.index()].output,
            },
            None => SpecOutput::NoOutput,
        };
        let success_policy = self.success_policy.map(|policy| SpecSuccessPolicy {
            cases: policy
                .cases
                .iter()
                .map(|case| case.required.iter().map(NodeRef::index).collect())
                .collect(),
            optional: policy.optional.iter().map(NodeRef::index).collect(),
        });
        WorkflowSpec {
            builder: self.id,
            name: self.name,
            definition_key: self.definition_key,
            on_error: self.on_error,
            nodes,
            output,
            success_policy,
        }
    }
}

/// The node of a spec from a node of the builder, whose references the
/// first gate checked.
fn spec_node(index: usize, node: BuilderNode, node_ids: &[String]) -> SpecNode {
    let links = node.links;
    SpecNode {
        index,
        node_id: node_ids[index].clone(),
        name: node.name,
        dependencies: links.dependencies.iter().map(NodeRef::index).collect(),
        args_from: links
            .args_from
            .iter()
            .map(|(member, source)| (member.clone(), source.index()))
            .collect(),
        workflow_ctx_from: links
            .workflow_ctx_from
            .iter()
            .map(|source| node_ids[source.index()].clone())
            .collect(),
        allow_failed_deps: links.allow_failed_deps,
        join: links.join,
        body: match node.body {
            BuilderBody::Task {
                input,
                queue,
                priority,
                task_options,
                deadline,
                ..
            } => SpecBody::Task {
                input,
                queue,
                priority,
                task_options,
                deadline,
            },
            BuilderBody::SubWorkflow {
                definition_key,
                params,
                ..
            } => SpecBody::SubWorkflow {
                definition_key,
                params,
            },
        },
        output: node.output,
    }
}

/// The refusal of a task node whose task the builder's app does not
/// register with the node's types (`HRS-302`). Pure.
fn unregistered_task(workflow_name: &str, not_found: &TaskFunctionNotFound) -> HorsiesError {
    HorsiesError::new(format!(
        "a node of workflow '{workflow_name}' was not added: {not_found}"
    ))
    .with_code(ErrorCode::WorkflowUnregisteredTask)
    .with_help(
        "register the task on the app of this workflow builder before the node is added, with \
         the types of the node",
    )
}

/// The join rules (Python `_collect_join_semantics_errors`): a quorum
/// needs at least 1 success and at most the node's dependencies.
fn check_join(owner: &str, links: &NodeLinks, report: &mut ValidationReport) {
    let JoinType::Quorum { min_success } = links.join else {
        return;
    };
    let dependencies = links.dependencies.len();
    if min_success < 1 {
        report.add(
            HorsiesError::new(format!(
                "node {owner} has min_success {min_success}; it must be 1 or more"
            ))
            .with_code(ErrorCode::WorkflowInvalidJoin),
        );
    } else if usize::try_from(min_success).map_or(true, |needed| needed > dependencies) {
        report.add(
            HorsiesError::new(format!(
                "node {owner} has min_success {min_success} and {dependencies} dependencies"
            ))
            .with_code(ErrorCode::WorkflowInvalidJoin)
            .with_help("add nodes to waits_for, or set min_success to at most their number"),
        );
    }
}

/// The checks of a node's input: no static member that an `args_from`
/// member also names, and the decode with the input type of the task or
/// child. The decode reads the input as the child of the worker gives it:
/// the static members, each `args_from` member as a result envelope, and
/// the workflow context when the task takes it. Each static member must
/// then bind to a field at every depth: a key that the decoded value does
/// not write back is refused with its key path (Python `extra='forbid'`).
fn check_input(
    owner: &str,
    input: &NodeInput,
    links: &NodeLinks,
    takes_context: bool,
    input_check: InputCheck,
    report: &mut ValidationReport,
) {
    let mut overlap: Vec<&str> = links
        .args_from
        .keys()
        .map(String::as_str)
        .filter(|member| input.contains(member))
        .collect();
    overlap.sort_unstable();
    if !overlap.is_empty() {
        report.add(
            HorsiesError::new("kwargs and args_from keys must be disjoint")
                .with_code(ErrorCode::WorkflowKwargsArgsFromOverlap)
                .with_note(format!("node {owner} has overlapping key(s): {overlap:?}"))
                .with_help("remove the overlapping key from either the input or args_from"),
        );
        return;
    }
    let result = placeholder_result_text();
    let context = placeholder_context_text();
    let mut members: Vec<(&str, &str)> = links
        .args_from
        .keys()
        .map(|member| (member.as_str(), result.as_str()))
        .collect();
    if takes_context {
        members.push((WORKFLOW_CTX_FIELD, context.as_str()));
    }
    let static_members: Vec<&str> = input.names().collect();
    if let Err(failure) = input_check(input.text_with(&members).as_bytes(), &static_members) {
        report.add(decode_refusal(owner, &failure));
        return;
    }
    // A node that joins on any or on a quorum may start before an `args_from`
    // source ended; that member is then absent, so its field needs a
    // default (Python `_collect_args_from_default_errors`).
    if matches!(links.join, JoinType::All) || links.args_from.is_empty() {
        return;
    }
    let without_results: Vec<(&str, &str)> = members
        .iter()
        .copied()
        .filter(|(member, _)| !links.args_from.contains_key(*member))
        .collect();
    // The static members were compared above; this decode looks for a
    // missing field of the input itself (depth 0) only.
    if let Err(failure) = input_check(input.text_with(&without_results).as_bytes(), &[]) {
        if let (0, Some(member)) = (failure.depth, missing_field(&failure)) {
            if links.args_from.contains_key(member) {
                report.add(
                    HorsiesError::new(format!(
                        "node {owner} joins on any or a quorum and takes args_from['{member}'], \
                         but its field '{member}' has no default"
                    ))
                    .with_code(ErrorCode::WorkflowInvalidJoin)
                    .with_help(format!(
                        "give '{member}' a default (an Option field), or join on all"
                    )),
                );
            }
        }
    }
}

/// The refusal of a node input that does not decode, or that holds a member
/// that binds to no field, with Python's code for each kind:
///
/// - A key of the input itself that binds to no field:
///   `WORKFLOW_INVALID_KWARG_KEY`, as Python's check of the parameter
///   names. Its failure has depth 1, the path of the key, or depth 0 from
///   the check of a task with no parameters, which names the key at the
///   input itself.
/// - A key below a member that binds to no field (depth 2 or more):
///   `WORKFLOW_KWARGS_NOT_SERIALIZABLE`, as Python's strict encode of the
///   member's value.
/// - A field of the input itself that has no value (depth 0, the path
///   `.`): `WORKFLOW_MISSING_REQUIRED_PARAMS`, as Python's check of the
///   parameters.
/// - A missing field inside a member's value (depth 1 or more: serde gives
///   it the path of the value that misses it, `options` or `parcels[0]`):
///   `WORKFLOW_KWARGS_NOT_SERIALIZABLE`, as Python's strict encode of the
///   member's value.
/// - A value of another type: `WORKFLOW_KWARGS_NOT_SERIALIZABLE`.
///
/// serde gives a key inside a value that it buffers (an internally tagged
/// or untagged enum, a flattened struct) the path of that value. A key that
/// binds to no field inside such a value of a member has depth 1 and gets
/// `WORKFLOW_INVALID_KWARG_KEY`.
fn decode_refusal(owner: &str, failure: &DecodeFailure) -> HorsiesError {
    let code = match (
        failure.text.starts_with("unknown field "),
        failure.depth,
        missing_field(failure),
    ) {
        (true, 0 | 1, _) => ErrorCode::WorkflowInvalidKwargKey,
        (true, _, _) => ErrorCode::WorkflowKwargsNotSerializable,
        (false, 0, Some(_)) => ErrorCode::WorkflowMissingRequiredParams,
        (false, _, Some(_)) => ErrorCode::WorkflowKwargsNotSerializable,
        (false, _, None) => ErrorCode::WorkflowKwargsNotSerializable,
    };
    HorsiesError::new(format!(
        "the input of node {owner} does not decode as its input type: {failure}"
    ))
    .with_code(code)
}

/// The field that a decode failure names as missing.
fn missing_field(failure: &DecodeFailure) -> Option<&str> {
    let rest = failure.text.strip_prefix("missing field `")?;
    rest.split_once('`').map(|(field, _)| field)
}

/// The workflow name as the first part of an auto node id (Python
/// `slugify`): a space becomes `_`; every other character outside
/// `[A-Za-z0-9_\-:.]` is removed; an empty result is `_`. Pure.
fn slugify(name: &str) -> String {
    let slug: String = name
        .chars()
        .filter_map(|character| match character {
            ' ' => Some('_'),
            other if other.is_ascii_alphanumeric() || matches!(other, '_' | '-' | ':' | '.') => {
                Some(other)
            }
            _ => None,
        })
        .collect();
    match slug.is_empty() {
        true => "_".to_owned(),
        false => slug,
    }
}

/// Whether `id` is a node id: `[A-Za-z0-9_\-:.]+`, at most 128 characters.
fn is_valid_node_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_NODE_ID_LENGTH
        && id.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | ':' | '.')
        })
}

#[cfg(test)]
mod tests;

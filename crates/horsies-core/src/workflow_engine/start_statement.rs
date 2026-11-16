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

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::PostgresBroker;
use crate::core::config::retention::RetentionConfig;
use crate::core::workflow::node::SubWorkflowParams;
use crate::core::{
    AnyNode, OnError, OperationalErrorCode, TaskError, TaskResult, WorkflowSpec,
    WorkflowSpecRegistry,
};
use crate::db::{BoundStatement, CheckoutClass, ParameterBinder, ParameterType, TypedStatement};
use crate::workflow_engine::engine::subworkflow_load_failed_error;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::merge_good_until_into_options;
use crate::workflow_engine::start::{
    child_workflow_depth, materialize_child_spec, node_task_facts, require_subworkflow_node_keys,
    stored_queue_and_priority, ChildSpecError,
};
use crate::workflow_engine::steps::{
    run_alone, run_in_transaction, step_row_select, step_rows, ChildBuild, OwedParentAdvances,
    StepContractError, StepDisposition,
};

/// `horsies_workflow_start`: the workflow rows of the tree (top-level first),
/// the start call time, the node rows, and the failed-node records.
pub(crate) static START: TypedStatement = TypedStatement::new(
    "horsies_workflow_start",
    step_row_select!(
        "horsies_workflow_start($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, \
         $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, \
         $33, $34, $35)"
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
/// failed-node record of the tree. Only [`start_arguments`] and
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
    workflow_id: Uuid,
    /// The parent workflow and the index of its sub-workflow node; `None`
    /// for the top-level workflow.
    parent: Option<(Uuid, i32)>,
    depth: i32,
}

/// What the host build made of the child of a sub-workflow node.
pub(crate) enum ChildBuildOutcome {
    /// The child spec, at its depth.
    Built { spec: WorkflowSpec, depth: i32 },
    /// The child cannot start: the node fails with this error, and its
    /// workflow's `on_error` applies (Python's node-level failure,
    /// `_fail_enqueued_task` and `_fail_subworkflow_load`,
    /// `core/workflows/engine.py:474-619`).
    NodeFails(TaskError),
}

/// Characters of a build failure text that the node error keeps (Python:
/// `str(exc)[:200]`, `core/workflows/engine.py:1206`).
const BUILD_FAILURE_TEXT_CHARS: usize = 200;

/// Builds the arguments of a start: the top-level workflow `workflow_id` of
/// `spec`, a pre-built child for every sub-workflow node without
/// dependencies (recursively), and a failed-node record for each such node
/// whose child cannot start. Parents come before their children; the
/// top-level row is first. Does no I/O; it runs the registered child builders
/// (`spec_builder`), which must have no side effects.
///
/// Errors: [`WorkflowError::Validation`] for a sub-workflow node of a built
/// child without a definition key, a node without a resolved queue or
/// priority, a node index above `i32::MAX`, or task options that the enqueue
/// rules refuse; [`WorkflowError::Serialization`] for a value that does not
/// encode.
pub(crate) fn start_arguments(
    spec: &WorkflowSpec,
    workflow_id: Uuid,
    registry: &WorkflowSpecRegistry,
    retention: &RetentionConfig,
) -> Result<StartArguments, WorkflowError> {
    tree_arguments(
        TreeWorkflow {
            spec: Cow::Borrowed(spec),
            workflow_id,
            parent: None,
            depth: 0,
        },
        registry,
        retention,
    )
}

/// Builds the `TREE` arguments of an owed child start: the child
/// `child_workflow_id` of `spec` under the node `parent_task_index` of
/// `parent_workflow_id`, at `depth`, with its pre-built children and
/// failed-node records as [`start_arguments`]. Errors as
/// [`start_arguments`].
pub(crate) fn child_tree_arguments(
    spec: &WorkflowSpec,
    child_workflow_id: Uuid,
    parent_workflow_id: Uuid,
    parent_task_index: i32,
    depth: i32,
    registry: &WorkflowSpecRegistry,
    retention: &RetentionConfig,
) -> Result<StartArguments, WorkflowError> {
    tree_arguments(
        TreeWorkflow {
            spec: Cow::Borrowed(spec),
            workflow_id: child_workflow_id,
            parent: Some((parent_workflow_id, parent_task_index)),
            depth,
        },
        registry,
        retention,
    )
}

/// The rows of the tree under `top`. Each workflow is pushed once, and a
/// child only below the depth limit, so the work list ends.
fn tree_arguments(
    top: TreeWorkflow<'_>,
    registry: &WorkflowSpecRegistry,
    retention: &RetentionConfig,
) -> Result<StartArguments, WorkflowError> {
    let mut arguments = StartArguments::default();
    let mut pending = vec![top];
    while let Some(workflow) = pending.pop() {
        require_subworkflow_node_keys(&workflow.spec)?;
        arguments.push_workflow(&workflow)?;
        for node in &workflow.spec.tasks {
            arguments.push_node(workflow.workflow_id, node, retention)?;
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
                || Ok(NodeInput::Input(node.input_json.clone())),
                node.sub_workflow_params,
                registry,
            )?;
            match built {
                ChildBuildOutcome::Built { spec, depth } => pending.push(TreeWorkflow {
                    spec: Cow::Owned(spec),
                    workflow_id: Uuid::new_v4(),
                    parent: Some((workflow.workflow_id, task_index)),
                    depth,
                }),
                ChildBuildOutcome::NodeFails(error) => {
                    tracing::error!(
                        workflow_id = %workflow.workflow_id,
                        task_index,
                        error = ?error.message,
                        "sub-workflow root not started; its node fails",
                    );
                    arguments.push_failed_record(workflow.workflow_id, task_index, error)?;
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

/// The host build of the child of a sub-workflow node, in Python's order
/// (`enqueue_subworkflow_task`, `core/workflows/engine.py:1019-1212`): the
/// depth limit, the load of the registered child by its definition key, the
/// node input (`input_of`, which the child start merges with the `args_from`
/// members), then the registered builder.
///
/// A child past the depth limit, one with no registration under its key, an
/// `args_from` key that names a member of the static input (from
/// `input_of`), and a builder that fails are [`ChildBuildOutcome::NodeFails`].
/// Errors: what `input_of` returns. Does no I/O.
pub(crate) fn build_child<F>(
    node: ChildOfNode<'_>,
    input_of: F,
    params: SubWorkflowParams,
    registry: &WorkflowSpecRegistry,
) -> Result<ChildBuildOutcome, WorkflowError>
where
    F: FnOnce() -> Result<NodeInput, WorkflowError>,
{
    let depth = match child_workflow_depth(
        node.parent_depth,
        node.parent_workflow_id,
        node.parent_workflow_name,
        node.task_index,
    ) {
        Ok(depth) => depth,
        Err(error) => return Ok(ChildBuildOutcome::NodeFails(error)),
    };
    let Some(registered) = node
        .definition_key
        .and_then(|key| registry.resolve_child_registration(key))
    else {
        return Ok(ChildBuildOutcome::NodeFails(subworkflow_load_failed_error(
            node.parent_workflow_name,
            node.task_index,
            node.definition_key,
        )));
    };
    let input = match input_of()? {
        NodeInput::Input(input) => input,
        NodeInput::NodeFails(error) => return Ok(ChildBuildOutcome::NodeFails(error)),
    };
    match materialize_child_spec(registered, params, &input, registry) {
        Ok(spec) => Ok(ChildBuildOutcome::Built { spec, depth }),
        Err(error) => Ok(ChildBuildOutcome::NodeFails(build_failed_error(
            node.parent_workflow_id,
            node.parent_workflow_name,
            node.task_index,
            &error,
        ))),
    }
}

/// The input that the registered builder of a child takes.
pub(crate) enum NodeInput {
    /// The encoded input, `args_from` members merged.
    Input(String),
    /// The input cannot be built; the node fails with this error.
    NodeFails(TaskError),
}

/// The error of the sub-workflow node `task_index` of the workflow
/// `workflow_id` (named `workflow_name`) whose registered child gave no spec:
/// `WORKFLOW_ENQUEUE_FAILED`,
/// `Subworkflow build failed: <class>: <text, 200 characters> (workflow <name>:<index>)`,
/// with the workflow id and the node in `data` (Python
/// `core/workflows/engine.py:1196-1211`, `_fail_enqueued_task` at `:507-512`).
pub(crate) fn build_failed_error(
    workflow_id: Uuid,
    workflow_name: &str,
    task_index: i32,
    error: &ChildSpecError,
) -> TaskError {
    TaskError {
        data: Some(serde_json::json!({
            "workflow_id": workflow_id.to_string(),
            "task_index": task_index,
        })),
        ..TaskError::builtin(
            OperationalErrorCode::WorkflowEnqueueFailed,
            format!(
                "Subworkflow build failed: {} (workflow {workflow_name}:{task_index})",
                child_spec_failure_text(error),
            ),
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
pub(crate) fn node_failure_result(error: TaskError) -> Result<String, WorkflowError> {
    Ok(serde_json::to_string(
        &TaskResult::<serde_json::Value>::Err(error),
    )?)
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
        error: TaskError,
    ) -> Result<(), WorkflowError> {
        let result = node_failure_result(error)?;
        self.failed_workflow_ids.push(workflow_id);
        self.failed_task_indexes.push(task_index);
        self.failed_results.push(result);
        Ok(())
    }

    fn push_node(
        &mut self,
        workflow_id: Uuid,
        node: &AnyNode,
        retention: &RetentionConfig,
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
                queue,
                merged_task_options.as_deref(),
                retention,
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
        let workflow_ctx_from = node
            .workflow_ctx_from
            .as_ref()
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

/// The bound start statement, with `sent_at` as the start call time.
fn start_statement<'a>(
    arguments: &'a StartArguments,
    sent_at: &'a DateTime<Utc>,
) -> BoundStatement<'a> {
    let binder = arguments.bind_workflows(START.bind()).value(sent_at);
    arguments
        .bind_failed_records(arguments.bind_nodes(binder))
        .finish()
}

/// Starts the tree as producer work ([`CheckoutClass::Batch`]), with
/// `sent_at` as the start call time of every workflow and task row of the
/// tree.
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
    build: ChildBuild<'_>,
) -> Result<StartedTree, WorkflowError> {
    let statement = start_statement(arguments, &sent_at);
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

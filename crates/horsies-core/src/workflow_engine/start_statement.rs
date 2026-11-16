//! The workflow start on the core statement layer: the whole tree in one call
//! of `horsies_workflow_start` (`0005_workflow_functions.sql`).
//!
//! The core builds the tree in memory: the top-level workflow, a pre-built
//! child workflow for every sub-workflow root (recursively, to the depth
//! limit), and every node row with its typed facts. The function inserts the
//! rows, mints the task rows of the READY task nodes, and links each
//! sub-workflow root to its pre-built child, in one statement in autocommit.

use std::borrow::Cow;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::PostgresBroker;
use crate::core::config::retention::RetentionConfig;
use crate::core::{AnyNode, OnError, WorkflowSpec, WorkflowSpecRegistry};
use crate::db::{CheckoutClass, ParameterType, TypedStatement};
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::merge_good_until_into_options;
use crate::workflow_engine::start::{
    build_child_spec, child_workflow_depth, node_task_facts, require_subworkflow_node_keys,
    stored_queue_and_priority,
};
use crate::workflow_engine::steps::{run_alone, step_rows, StepContractError, StepDisposition};

/// `horsies_workflow_start`: the workflow rows of the tree (top-level first),
/// the start call time, the node rows, and the failed-node records (always
/// empty here: a root child that cannot be built refuses the start).
pub(crate) static START: TypedStatement = TypedStatement::new(
    "horsies_workflow_start",
    "SELECT row_kind, disposition, workflow_id, advance_child_workflow_id, detail, task_index
     FROM horsies_workflow_start(
         $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
         $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
         $27, $28, $29, $30, $31, $32,
         $33, $34, $35
     )",
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

/// The parallel arrays of `horsies_workflow_start`, one element per workflow
/// row or node row of the tree. Only [`start_arguments`] builds it.
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

/// Builds the arguments of a start: the top-level workflow `workflow_id` of
/// `spec`, and a pre-built child for every sub-workflow node without
/// dependencies, recursively. Parents come before their children; the
/// top-level row is first. Does no I/O; it runs the registered child builders
/// (`spec_builder`), which must have no side effects.
///
/// Errors: [`WorkflowError::Validation`] when a child cannot be built (no
/// registration under its definition key, a builder that fails, a child past
/// the depth limit, a sub-workflow node of the child without a definition
/// key), a node without a resolved queue or priority, a node index above
/// `i32::MAX`, or task options that the enqueue rules refuse;
/// [`WorkflowError::Serialization`] for a value that does not encode. Python
/// fails only the sub-workflow node when its child cannot be built; the core
/// refuses the start as before, until the failed-node records run in the
/// transaction form with `horsies_workflow_child_start` (#751).
pub(crate) fn start_arguments(
    spec: &WorkflowSpec,
    workflow_id: Uuid,
    registry: &WorkflowSpecRegistry,
    retention: &RetentionConfig,
) -> Result<StartArguments, WorkflowError> {
    let mut arguments = StartArguments::default();
    // Each workflow is pushed once, and a child only below the depth limit,
    // so the work list ends.
    let mut pending = vec![TreeWorkflow {
        spec: Cow::Borrowed(spec),
        workflow_id,
        parent: None,
        depth: 0,
    }];
    while let Some(workflow) = pending.pop() {
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
            pending.push(root_child(&workflow, node, registry)?);
        }
    }
    Ok(arguments)
}

/// The pre-built child of the sub-workflow root `node` of `parent`.
fn root_child<'a>(
    parent: &TreeWorkflow<'_>,
    node: &AnyNode,
    registry: &WorkflowSpecRegistry,
) -> Result<TreeWorkflow<'a>, WorkflowError> {
    let task_index = node_index(node.index)?;
    let depth = child_workflow_depth(parent.depth, parent.workflow_id, task_index)
        .map_err(|error| WorkflowError::Validation(error.to_string()))?;
    let spec_name = node
        .task_name
        .strip_prefix("__sub_workflow:")
        .unwrap_or(&node.task_name);
    let child_spec = build_child_spec(
        spec_name,
        node.sub_definition_key.as_deref(),
        node,
        registry,
    )?;
    require_subworkflow_node_keys(&child_spec)?;
    Ok(TreeWorkflow {
        spec: Cow::Owned(child_spec),
        workflow_id: Uuid::new_v4(),
        parent: Some((parent.workflow_id, task_index)),
        depth,
    })
}

/// A node index as the `integer` column stores it.
fn node_index(index: usize) -> Result<i32, WorkflowError> {
    i32::try_from(index).map_err(|_| {
        WorkflowError::Validation(format!("node index {index} is above the integer range"))
    })
}

impl StartArguments {
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

    fn push_node(
        &mut self,
        workflow_id: Uuid,
        node: &AnyNode,
        retention: &RetentionConfig,
    ) -> Result<(), WorkflowError> {
        let (queue, priority) = stored_queue_and_priority(node)?;
        let merged_task_options =
            merge_good_until_into_options(node.good_until, node.task_options_json.as_deref());
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

/// Starts the tree in one statement in autocommit, as producer work
/// ([`CheckoutClass::Batch`]), with `sent_at` as the start call time of every
/// workflow and task row of the tree.
///
/// Errors: [`WorkflowError::Broker`] when the statement failed (nothing
/// committed, or `OutcomeUnknown`: a resend with the same id meets `EXISTS`);
/// [`WorkflowError::StepContract`] for a row outside the contract. Owed child
/// starts are outside it: every sub-workflow root has its pre-built child and
/// no failed-node record is passed, so the function owes none.
pub(crate) async fn start_tree(
    broker: &PostgresBroker,
    arguments: &StartArguments,
    sent_at: DateTime<Utc>,
) -> Result<StartOutcome, WorkflowError> {
    let no_failed_workflow_ids: Vec<Uuid> = Vec::new();
    let no_failed_task_indexes: Vec<i32> = Vec::new();
    let no_failed_results: Vec<String> = Vec::new();
    let statement = START
        .bind()
        .value(&arguments.workflow_ids)
        .value(&arguments.names)
        .value(&arguments.definition_keys)
        .value(&arguments.on_errors)
        .value(&arguments.output_task_indexes)
        .value(&arguments.success_policies)
        .value(&arguments.parent_workflow_ids)
        .value(&arguments.parent_task_indexes)
        .value(&arguments.depths)
        .value(&sent_at)
        .value(&arguments.node_workflow_ids)
        .value(&arguments.node_task_indexes)
        .value(&arguments.node_ids)
        .value(&arguments.node_task_names)
        .value(&arguments.node_inputs)
        .value(&arguments.node_queue_names)
        .value(&arguments.node_priorities)
        .value(&arguments.node_dependencies)
        .value(&arguments.node_args_from)
        .value(&arguments.node_workflow_ctx_from)
        .value(&arguments.node_allow_failed_deps)
        .value(&arguments.node_join_types)
        .value(&arguments.node_min_successes)
        .value(&arguments.node_task_options)
        .value(&arguments.node_max_retries)
        .value(&arguments.node_executions)
        .value(&arguments.node_good_untils)
        .value(&arguments.node_retention_class_keys)
        .value(&arguments.node_is_subworkflow)
        .value(&arguments.node_sub_workflow_names)
        .value(&arguments.node_sub_definition_keys)
        .value(&arguments.node_sub_workflow_params)
        .value(&no_failed_workflow_ids)
        .value(&no_failed_task_indexes)
        .value(&no_failed_results)
        .finish();
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
    Ok(match step.outcome.disposition {
        StartDisposition::Started => StartOutcome::Started,
        StartDisposition::Exists => StartOutcome::Exists {
            stored_name: step.outcome.detail,
        },
    })
}

#[cfg(test)]
mod tests;

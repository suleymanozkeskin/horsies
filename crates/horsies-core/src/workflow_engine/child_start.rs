//! The owed child start of a sub-workflow node on the core statement layer:
//! `horsies_workflow_child_start` (`0005_workflow_functions.sql`), in the
//! transaction of the step that owes it.
//!
//! A step function returns one `CHILD_START` row (with its `SOURCE` rows) per
//! promoted sub-workflow node that has no child yet. For each, the core runs
//! the host build of the child (Python's `enqueue_subworkflow_task` before
//! its inserts, `core/workflows/engine.py:1019-1212`) and calls the function
//! in the same transaction:
//!
//! - `TREE`: the child built; the child tree goes in the arrays of
//!   `horsies_workflow_start`, with its own pre-built children and
//!   failed-node records.
//! - `FAILURE`: the child cannot start (depth limit, no registration under
//!   its key, an `args_from` key that names a static member, a failing
//!   builder); the node fails with its error envelope and its workflow's
//!   `on_error` applies.
//!
//! A child start can owe further child starts; they run in the same
//! transaction, at most [`MAX_CHILD_STARTS_PER_STEP`] per step.

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::core_rows::{query_in, StatementLog};
use crate::db::{
    BoundStatement, ParameterBinder, ParameterType, Row, StatementFailure, Transaction,
    TypedStatement,
};
use crate::workflow_engine::args_merge::{
    merge_args_from_sync, ArgsFromMerge, DepResult, MergedNodeKind,
};
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::start_statement::{
    build_child, child_tree_arguments, node_failure_result, ChildBuildOutcome, ChildOfNode,
    NodeInput, StartArguments,
};
use crate::workflow_engine::steps::{
    step_row_select, step_rows, ChildBuild, OwedChildStart, OwedParentAdvances, StepContractError,
    StepDisposition, StepOutcome,
};

/// `horsies_workflow_child_start`: the parent workflow and node, the kind
/// (`TREE` or `FAILURE`), the failure envelope, the child start call time,
/// then the tree arrays of `horsies_workflow_start` (workflow rows, node
/// rows, failed-node records). A `FAILURE` call passes NULL for the call time
/// and every array.
pub(crate) static CHILD_START: TypedStatement = TypedStatement::new(
    "horsies_workflow_child_start",
    step_row_select!(
        "horsies_workflow_child_start($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, \
         $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, \
         $31, $32, $33, $34, $35, $36, $37, $38, $39)"
    ),
    &[
        // The parent node, the kind, the failure envelope, the call time.
        ParameterType::Uuid,
        ParameterType::Int4,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Timestamptz,
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

/// The most child starts that one step transaction runs: the child starts
/// that the step owes and those that each child start owes in turn. A step
/// that owes more fails closed ([`StepContractError::TooManyChildStarts`]):
/// its transaction rolls back and nothing of it commits.
pub(crate) const MAX_CHILD_STARTS_PER_STEP: usize = 10_000;

/// The kind text of a child start whose child built.
static TREE_KIND: &str = "TREE";
/// The kind text of a child start whose child cannot start.
static FAILURE_KIND: &str = "FAILURE";
/// The failure envelope of a `TREE` call.
static NO_FAILURE_RESULT: Option<String> = None;
/// The call time of a `FAILURE` call.
static NO_SENT_AT: Option<DateTime<Utc>> = None;
/// A NULL array of each element type, for a `FAILURE` call.
static NO_UUIDS: Option<Vec<Uuid>> = None;
static NO_TEXTS: Option<Vec<String>> = None;
static NO_INTEGERS: Option<Vec<i32>> = None;
static NO_BOOLS: Option<Vec<bool>> = None;
static NO_TIMES: Option<Vec<DateTime<Utc>>> = None;

/// The disposition of `horsies_workflow_child_start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChildStartDisposition {
    /// The child rows, the node rows and the link are written, and the
    /// child's start progression ran (`TREE`).
    Started,
    /// The node is FAILED with the envelope, and its progression ran
    /// (`FAILURE`).
    NodeFailed,
    /// The node is not an ENQUEUED sub-workflow node with no child (`detail`:
    /// its status): nothing written.
    Stale,
    /// The parent workflow is not RUNNING (`detail`: its status): the node is
    /// back to READY with no start time, so a resume or recovery case 1.5
    /// starts it later.
    WorkflowNotRunning,
}

impl StepDisposition for ChildStartDisposition {
    fn from_text(text: &str) -> Option<Self> {
        match text {
            "STARTED" => Some(Self::Started),
            "NODE_FAILED" => Some(Self::NodeFailed),
            "STALE" => Some(Self::Stale),
            "WORKFLOW_NOT_RUNNING" => Some(Self::WorkflowNotRunning),
            _ => None,
        }
    }
}

/// What the core passes for one owed child start.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ChildStartKind {
    /// The child built: its tree (`TREE`).
    Tree(StartArguments),
    /// The child cannot start: the node's error in its result envelope
    /// (`FAILURE`).
    Failure { failure_result: String },
}

/// Why the owed child starts of a transaction did not all run.
#[derive(Debug)]
pub(crate) enum ChildStartsFailure {
    /// A child start statement failed. The transaction holds a failed
    /// statement: the caller drops it, which discards the connection, and
    /// nothing commits.
    Statement(StatementFailure),
    /// The host build of a child or the rows of a child start were refused
    /// before any statement failed. The caller rolls the transaction back.
    Refused(WorkflowError),
}

/// Runs the owed child starts of a step in its transaction, in the order
/// the rows named them, then the child starts that each owes in turn.
///
/// Returns the parent advances that the child starts owe after the commit
/// (a child start whose node failure or child ended its parent workflow).
/// Does not commit.
///
/// Errors: [`ChildStartsFailure`]. After an error, the child starts before
/// it are written in the open transaction; nothing is committed.
pub(crate) async fn run_child_starts(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
    owed: Vec<OwedChildStart>,
    build: ChildBuild<'_>,
) -> Result<OwedParentAdvances, ChildStartsFailure> {
    let mut pending: VecDeque<OwedChildStart> = VecDeque::from(owed);
    let mut advances = OwedParentAdvances::default();
    for _start in 0..MAX_CHILD_STARTS_PER_STEP {
        let Some(owed_start) = pending.pop_front() else {
            return Ok(advances);
        };
        let kind = child_start_kind(&owed_start, build).map_err(ChildStartsFailure::Refused)?;
        let rows = start_one_child(transaction, log, &owed_start, &kind)
            .await
            .map_err(ChildStartsFailure::Statement)?;
        let step = step_rows::<ChildStartDisposition>(&rows, &CHILD_START)
            .map_err(|contract| ChildStartsFailure::Refused(contract.into()))?;
        log_child_start(&owed_start, &kind, &step.outcome);
        advances.add(step.outcome.parent_advance);
        pending.extend(step.owed_child_starts);
    }
    match pending.is_empty() {
        true => Ok(advances),
        false => Err(ChildStartsFailure::Refused(
            StepContractError::TooManyChildStarts {
                bound: MAX_CHILD_STARTS_PER_STEP,
            }
            .into(),
        )),
    }
}

/// The host build of one owed child start: what the core passes for it.
///
/// The child's input is the node's stored input with one member per
/// `args_from` key, each the stored result of the named dependency (the
/// `SOURCE` rows; a dependency that is not terminal gives
/// `UPSTREAM_SKIPPED`, as the engine code does). The built child gets a new
/// id (`Uuid::new_v4()`) and its tree as [`child_tree_arguments`] builds it.
/// Does no I/O; it runs the registered builder.
///
/// Errors: [`WorkflowError::Serialization`] for an `args_from` text that does
/// not parse or a value that does not encode; [`WorkflowError::Validation`]
/// for a stored input that the `args_from` merge refuses, and for a child
/// spec that the tree arguments refuse (as [`child_tree_arguments`]).
pub(crate) fn child_start_kind(
    owed: &OwedChildStart,
    build: ChildBuild<'_>,
) -> Result<ChildStartKind, WorkflowError> {
    let spec_name = owed.sub_workflow_name.as_deref().unwrap_or("?");
    let built = build_child(
        ChildOfNode {
            parent_workflow_id: owed.workflow_id,
            task_index: owed.task_index,
            parent_depth: owed.parent_depth,
            spec_name,
            definition_key: owed.sub_definition_key.as_deref(),
        },
        || merged_child_input(owed),
        owed.sub_workflow_params,
        build.registry,
    )?;
    match built {
        ChildBuildOutcome::Built { spec, depth } => Ok(ChildStartKind::Tree(child_tree_arguments(
            &spec,
            Uuid::new_v4(),
            owed.workflow_id,
            owed.task_index,
            depth,
            build.registry,
            build.retention,
        )?)),
        ChildBuildOutcome::NodeFails(error) => {
            tracing::error!(
                workflow_id = %owed.workflow_id,
                task_index = owed.task_index,
                definition_key = ?owed.sub_definition_key,
                error = ?error.message,
                "sub-workflow child not started; its node fails",
            );
            Ok(ChildStartKind::Failure {
                failure_result: node_failure_result(error)?,
            })
        }
    }
}

/// The node input of an owed child start with its `args_from` members
/// merged; an `args_from` key that names a static member fails the node
/// (Python `core/workflows/engine.py:1111-1123`).
fn merged_child_input(owed: &OwedChildStart) -> Result<NodeInput, WorkflowError> {
    let args_from = owed
        .args_from
        .as_deref()
        .map(serde_json::from_str::<serde_json::Value>)
        .transpose()?;
    let dependency_results: HashMap<i32, DepResult> = owed
        .sources
        .iter()
        .map(|source| {
            (
                source.task_index,
                DepResult {
                    status: source.status.clone(),
                    result: source.result.clone(),
                },
            )
        })
        .collect();
    Ok(
        match merge_args_from_sync(&owed.node_input, &args_from, &dependency_results)? {
            ArgsFromMerge::Merged(input) => NodeInput::Input(input),
            ArgsFromMerge::StaticMemberConflict(conflict) => {
                NodeInput::NodeFails(conflict.node_error(
                    owed.workflow_id,
                    owed.task_index,
                    MergedNodeKind::SubWorkflow,
                ))
            }
        },
    )
}

/// Runs `horsies_workflow_child_start` for one owed child start in the
/// transaction and reads its rows. `sent_at` of a `TREE` call is the call
/// time.
async fn start_one_child(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
    owed: &OwedChildStart,
    kind: &ChildStartKind,
) -> Result<Vec<Row>, StatementFailure> {
    let sent_at = Utc::now();
    let statement = match kind {
        ChildStartKind::Tree(arguments) => tree_statement(owed, arguments, &sent_at),
        ChildStartKind::Failure { failure_result } => failure_statement(owed, failure_result),
    };
    query_in(transaction, log, &statement).await
}

/// The bound `TREE` call.
fn tree_statement<'a>(
    owed: &'a OwedChildStart,
    arguments: &'a StartArguments,
    sent_at: &'a DateTime<Utc>,
) -> BoundStatement<'a> {
    let binder = CHILD_START
        .bind()
        .value(&owed.workflow_id)
        .value(&owed.task_index)
        .value(&TREE_KIND)
        .value(&NO_FAILURE_RESULT)
        .value(sent_at);
    arguments
        .bind_failed_records(arguments.bind_nodes(arguments.bind_workflows(binder)))
        .finish()
}

/// The bound `FAILURE` call: the envelope, and NULL for the call time and
/// every tree array.
fn failure_statement<'a>(
    owed: &'a OwedChildStart,
    failure_result: &'a String,
) -> BoundStatement<'a> {
    let binder = CHILD_START
        .bind()
        .value(&owed.workflow_id)
        .value(&owed.task_index)
        .value(&FAILURE_KIND)
        .value(failure_result)
        .value(&NO_SENT_AT);
    bind_no_tree(binder).finish()
}

/// Binds NULL for the 34 tree arrays, in the order of
/// [`StartArguments::bind_workflows`], [`StartArguments::bind_nodes`] and
/// [`StartArguments::bind_failed_records`].
fn bind_no_tree(binder: ParameterBinder<'_>) -> ParameterBinder<'_> {
    binder
        // Workflow rows.
        .value(&NO_UUIDS)
        .value(&NO_TEXTS)
        .value(&NO_TEXTS)
        .value(&NO_TEXTS)
        .value(&NO_INTEGERS)
        .value(&NO_TEXTS)
        .value(&NO_UUIDS)
        .value(&NO_INTEGERS)
        .value(&NO_INTEGERS)
        // Node rows.
        .value(&NO_UUIDS)
        .value(&NO_INTEGERS)
        .value(&NO_TEXTS)
        .value(&NO_TEXTS)
        .value(&NO_TEXTS)
        .value(&NO_TEXTS)
        .value(&NO_INTEGERS)
        .value(&NO_TEXTS)
        .value(&NO_TEXTS)
        .value(&NO_TEXTS)
        .value(&NO_BOOLS)
        .value(&NO_TEXTS)
        .value(&NO_INTEGERS)
        .value(&NO_TEXTS)
        .value(&NO_INTEGERS)
        .value(&NO_TEXTS)
        .value(&NO_TIMES)
        .value(&NO_TEXTS)
        .value(&NO_BOOLS)
        .value(&NO_TEXTS)
        .value(&NO_TEXTS)
        .value(&NO_TEXTS)
        // Failed-node records.
        .value(&NO_UUIDS)
        .value(&NO_INTEGERS)
        .value(&NO_TEXTS)
}

/// Logs what one child start did.
fn log_child_start(
    owed: &OwedChildStart,
    kind: &ChildStartKind,
    outcome: &StepOutcome<ChildStartDisposition>,
) {
    let child_workflow_id = match kind {
        ChildStartKind::Tree(arguments) => arguments.top_workflow_id(),
        ChildStartKind::Failure { .. } => None,
    };
    match outcome.disposition {
        ChildStartDisposition::Started => tracing::debug!(
            workflow_id = %owed.workflow_id,
            task_index = owed.task_index,
            child_workflow_id = ?child_workflow_id,
            "sub-workflow child started",
        ),
        ChildStartDisposition::NodeFailed => tracing::debug!(
            workflow_id = %owed.workflow_id,
            task_index = owed.task_index,
            "sub-workflow node failed at its child start",
        ),
        ChildStartDisposition::Stale => tracing::debug!(
            workflow_id = %owed.workflow_id,
            task_index = owed.task_index,
            node_status = ?outcome.detail,
            "child start found the node moved on; nothing written",
        ),
        ChildStartDisposition::WorkflowNotRunning => tracing::info!(
            workflow_id = %owed.workflow_id,
            task_index = owed.task_index,
            workflow_status = ?outcome.detail,
            "child start found the parent workflow not RUNNING; the node is READY again",
        ),
    }
}

#[cfg(test)]
mod tests;

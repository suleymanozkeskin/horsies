use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use std::sync::Arc;

use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::history::enqueue::{prepare_workflow_task_facts, WorkflowTaskFacts};
use crate::core::workflow::meta::WorkflowMeta;
use crate::core::workflow::node::SubWorkflowParams;
use crate::core::workflow::WORKFLOW_META_KWARG;
use crate::core::{
    AnyNode, ErrorCategory, HorsiesError, OnError, OperationalErrorCode, SpecBuildError, TaskError,
    WorkflowSpec, WorkflowSpecRegistry, WorkflowStartError, WorkflowStartErrorCode,
    WorkflowStartResult,
};

use crate::db::CheckoutClass;
use crate::workflow_engine::args_merge::input_object;
use crate::workflow_engine::bound_handle::WorkflowHandle;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::start_statement::{start_arguments, start_tree, StartOutcome};
use crate::workflow_engine::steps::{advance_parents, ChildBuild};
use crate::workflow_engine::{node_good_until, node_max_retries};

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

/// `sent_at` is the time the engine built the child workflow.
const INSERT_CHILD_WORKFLOW_SQL: &str = "\
INSERT INTO horsies_workflows (
    id, name, status, on_error, output_task_index, success_policy,
    definition_key,
    parent_workflow_id, parent_task_index, depth, root_workflow_id,
    sent_at, created_at, started_at, updated_at
)
VALUES ($1::uuid, $2, 'RUNNING', $3, $4, $5, $6, $7::uuid, $8, $9, $10::uuid,
        $11, NOW(), NOW(), NOW())";

// An engine-minted task row carries no command fingerprint, no idempotency
// key and no retained input (horsies_tasks_command_fingerprint_by_origin).
// `sent_at` ($11) is the start call time of the node's workflow.
const ENQUEUE_ROOT_TASK_SQL: &str = "\
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, input, task_options, execution,
    retention_class_key, sent_at, created_at,
    command_fingerprint_version, command_fingerprint, idempotency_key_digest,
    status, enqueued_at, retry_count, max_retries, good_until,
    is_workflow_task, updated_at,
    retain_rerun_input, rerun_of_task_id, rerun_root_task_id
)
VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $11, NOW(), NULL, NULL, NULL,
        'PENDING', NOW(), 0, $9, $10, TRUE, NOW(), FALSE, NULL, NULL)";

/// A node's `started_at` is its enqueue time (Python `ENQUEUE_WORKFLOW_TASK_SQL`,
/// `core/workflows/sql.py:114-116`).
const LINK_WORKFLOW_TASK_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET task_id = $1::uuid, status = 'ENQUEUED', started_at = NOW()
WHERE workflow_id = $2::uuid AND task_index = $3";

/// A linked parent node is RUNNING while its child runs, on every path (Python `LINK_SUB_WORKFLOW_SQL`,
/// `core/workflows/sql.py:271-275`).
const LINK_ROOT_SUBWORKFLOW_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET sub_workflow_id = $1::uuid, status = 'RUNNING', started_at = NOW()
WHERE workflow_id = $2::uuid AND task_index = $3
  AND status = 'READY'";

/// Get parent depth and root workflow ID (used by root sub-workflow launch at start).
const GET_WORKFLOW_DEPTH_SQL: &str = "\
SELECT depth, root_workflow_id, name
FROM horsies_workflows
WHERE id = $1::uuid";

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

/// What the insert of a top-level workflow row found.
#[derive(Debug, Clone, PartialEq, Eq)]
enum WorkflowRowInsert {
    /// The row is new; the start goes on in the same transaction.
    Inserted,
    /// A workflow with this id and this name exists. The start writes nothing and returns its handle.
    Existing,
    /// A workflow with this id exists under another name. The start writes nothing and is refused.
    IdConflict { existing_name: String },
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct DepthRow {
    depth: Option<i32>,
    root_workflow_id: Option<Uuid>,
    name: String,
}

/// The typed facts that a task node row stores, parsed once at workflow start
/// (`horsies_workflow_tasks_task_facts_by_kind`). The task row minted from the
/// node carries the same values.
pub(crate) struct NodeTaskFacts {
    pub(crate) max_retries: i32,
    pub(crate) good_until: Option<DateTime<Utc>>,
    pub(crate) facts: WorkflowTaskFacts,
}

/// Parse the typed facts of a task node from its merged task options and its
/// queue's retention class.
pub(crate) fn node_task_facts(
    node: &AnyNode,
    queue: &str,
    merged_task_options: Option<&str>,
    retention: &RetentionConfig,
) -> Result<NodeTaskFacts, WorkflowError> {
    let facts = prepare_workflow_task_facts(
        &node.input_json,
        merged_task_options,
        retention.resolve_queue_class(queue).as_deref(),
    )
    .map_err(|error| WorkflowError::Validation(error.to_string()))?;
    Ok(NodeTaskFacts {
        max_retries: node_max_retries(node.index, &node.task_name, merged_task_options)?,
        good_until: node_good_until(node.index, &node.task_name, merged_task_options)?,
        facts,
    })
}

/// A workflow_task row prepared in memory for batched insertion.
///
/// Holds the new identifiers and the per-node values derived once during the
/// prepare phase, so the bulk inserts and the per-row slow path read them without
/// recomputing. Parity with horsies PR #126.
struct PreparedNode<'a> {
    node: &'a AnyNode,
    wt_id: Uuid,
    queue: String,
    priority: i32,
    args_from_json: Option<serde_json::Value>,
    join_str: String,
    merged_task_options: Option<String>,
    sub_workflow_name: Option<String>,
    status: &'static str,
    /// `Some` only for fast-path roots — the `horsies_tasks` id, also written to
    /// the workflow_task's `task_id` so the two link without a follow-up UPDATE.
    task_id: Option<Uuid>,
    /// `Some` exactly for task nodes; a sub-workflow node has no task facts.
    task_facts: Option<NodeTaskFacts>,
    is_root: bool,
    fast_path: bool,
    /// Whether a sub-workflow node is below this node in its workflow.
    downstream_subworkflow: DownstreamSubworkflow,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Start a workflow. The returned handle reuses the broker's shared listener
/// for result waits (P2).
pub async fn start_workflow<T>(
    broker: &Arc<PostgresBroker>,
    spec: &WorkflowSpec,
    workflow_id: Option<Uuid>,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> WorkflowStartResult<WorkflowHandle<T>> {
    let wf_id = workflow_id.unwrap_or_else(Uuid::new_v4);
    start_workflow_attempt(
        broker,
        spec,
        wf_id,
        chrono::Utc::now(),
        registry,
        payload,
        retention,
    )
    .await
}

/// One start attempt with a fixed id and start time.
///
/// - Outcomes: the handle of the new workflow; the handle of an existing workflow with this id and name
///   (nothing written); `WORKFLOW_ID_CONFLICT` when the id belongs to another workflow name (nothing written);
///   `VALIDATION_FAILED` for a spec with no definition key (nothing written, Python
///   `WorkflowSpec._require_definition_key`); `VALIDATION_FAILED` or `ENQUEUE_FAILED` from the transaction,
///   which rolled back.
/// - `sent_at` is the caller's start time; every attempt of one start uses the same value.
async fn start_workflow_attempt<T>(
    broker: &Arc<PostgresBroker>,
    spec: &WorkflowSpec,
    wf_id: Uuid,
    sent_at: chrono::DateTime<chrono::Utc>,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> WorkflowStartResult<WorkflowHandle<T>> {
    let refused =
        |code: WorkflowStartErrorCode, message: String, retryable: bool| WorkflowStartError {
            code,
            message,
            retryable,
            workflow_name: spec.name.clone(),
            workflow_id: Some(wf_id),
        };
    if let Err(message) = require_definition_key(spec) {
        return Err(refused(
            WorkflowStartErrorCode::ValidationFailed,
            message,
            false,
        ));
    }
    match start_workflow_inner(broker, spec, wf_id, sent_at, registry, retention).await {
        Ok(WorkflowRowInsert::Inserted | WorkflowRowInsert::Existing) => Ok(WorkflowHandle::new(
            wf_id,
            Arc::clone(broker),
            Arc::new(registry.clone()),
            payload.clone(),
            retention.clone(),
        )),
        Ok(WorkflowRowInsert::IdConflict { existing_name }) => Err(refused(
            WorkflowStartErrorCode::WorkflowIdConflict,
            format!(
                "workflow_id '{wf_id}' already exists for workflow name '{existing_name}', \
                 cannot start workflow name '{}' with the same id",
                spec.name,
            ),
            false,
        )),
        Err(error) => Err(refused(
            classify_workflow_error(&error, &spec.name, wf_id),
            error.to_string(),
            is_retryable_workflow_error(&error),
        )),
    }
}

/// Refuses a top-level spec with no definition key, and a sub-workflow node with no definition key of its
/// child. Python: `WorkflowSpec._require_definition_key` (`core/models/workflow/spec.py:1731-1743`) at start
/// (`core/workflows/lifecycle.py:315`) and for each sub-workflow node (`lifecycle.py:546`).
fn require_definition_key(spec: &WorkflowSpec) -> Result<(), String> {
    let has_key = matches!(
        spec.definition_key.as_deref().map(str::trim),
        Some(key) if !key.is_empty()
    );
    match has_key {
        true => require_subworkflow_node_keys(spec).map_err(|error| error.to_string()),
        false => Err(format!(
            "WorkflowSpec is missing definition_key: workflow '{}' has no stable \
             definition_key; set a definition key such as 'your.workflow.v1'",
            spec.name,
        )),
    }
}

/// The greatest depth of a child workflow. A top-level workflow has depth 0; a child has the depth of its
/// parent plus 1. Python: `MAX_SUBWORKFLOW_DEPTH` (`core/models/workflow/limits.py`); the baseline CHECK
/// `horsies_workflows_depth_range` holds the same bound.
pub(crate) const MAX_SUBWORKFLOW_DEPTH: i32 = 32;

/// The depth of a new child of the workflow `parent_workflow_id` (named `parent_workflow_name`) at
/// `parent_depth`.
///
/// `Err` is the error that the sub-workflow node at `task_index` stores when the child would pass
/// [`MAX_SUBWORKFLOW_DEPTH`]: the child is not started, the node fails, and the parent's `on_error` applies
/// (Python `core/workflows/engine.py:1017-1033`). The message names the workflow by its name; `data` names
/// it by its id, with the node, as Python's `_fail_enqueued_task` does (`core/workflows/engine.py:507-512`).
pub(crate) fn child_workflow_depth(
    parent_depth: i32,
    parent_workflow_id: Uuid,
    parent_workflow_name: &str,
    task_index: i32,
) -> Result<i32, TaskError> {
    let child_depth = parent_depth + 1;
    match child_depth > MAX_SUBWORKFLOW_DEPTH {
        false => Ok(child_depth),
        true => Err(TaskError {
            data: Some(serde_json::json!({
                "workflow_id": parent_workflow_id.to_string(),
                "task_index": task_index,
            })),
            ..TaskError::builtin(
                OperationalErrorCode::SubworkflowDepthLimitReached,
                format!(
                    "Subworkflow not started: the child workflow would have depth {child_depth}, \
                     and the limit is {MAX_SUBWORKFLOW_DEPTH} (workflow {parent_workflow_name}:{task_index})"
                ),
            )
        }),
    }
}

/// Refuses a sub-workflow node with no definition key: a child is resolved only by its key.
pub(crate) fn require_subworkflow_node_keys(spec: &WorkflowSpec) -> Result<(), WorkflowError> {
    let unkeyed = spec.tasks.iter().find(|node| {
        node.is_subworkflow
            && !matches!(
                node.sub_definition_key.as_deref().map(str::trim),
                Some(key) if !key.is_empty()
            )
    });
    match unkeyed {
        None => Ok(()),
        Some(node) => Err(WorkflowError::Validation(format!(
            "sub-workflow node '{}' of workflow '{}' has no definition_key of its child workflow; \
             a child workflow is resolved only by its definition_key",
            node.node_id.as_deref().unwrap_or("?"),
            spec.name,
        ))),
    }
}

/// Start a workflow with retry and a separate session-capable pool for result
/// LISTEN/NOTIFY handles.
pub async fn start_workflow_with_retry<T>(
    broker: &Arc<PostgresBroker>,
    spec: &WorkflowSpec,
    workflow_id: Option<Uuid>,
    registry: &WorkflowSpecRegistry,
    resend_on_transient_err: bool,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> WorkflowStartResult<WorkflowHandle<T>> {
    if !resend_on_transient_err {
        return start_workflow(broker, spec, workflow_id, registry, payload, retention).await;
    }

    // 1 initial attempt + START_RETRY_COUNT retries = 4 total attempts.
    // Matches Python's `1 + _START_RETRY_COUNT`.
    const START_RETRY_COUNT: u32 = 3;
    const START_RETRY_INITIAL_MS: u64 = 200;
    const START_RETRY_MAX_MS: u64 = 2000;

    let max_attempts = 1 + START_RETRY_COUNT;
    let wf_id = workflow_id.unwrap_or_else(Uuid::new_v4);
    // One start time for every attempt, as Python computes it once before its retry loop.
    let sent_at = chrono::Utc::now();
    let mut last_err: Option<WorkflowStartError> = None;

    for attempt in 0..max_attempts {
        if attempt > 0 {
            if let Some(ref err) = last_err {
                if !err.retryable {
                    return Err(last_err
                        .take()
                        .expect("retry loop should have recorded the previous start error"));
                }
            }
            let delay_ms = (START_RETRY_INITIAL_MS * 2u64.pow(attempt - 1)).min(START_RETRY_MAX_MS);
            tracing::warn!(
                workflow_name = %spec.name,
                workflow_id = %wf_id,
                attempt = attempt + 1,
                delay_ms,
                "workflow start failed (retryable), retrying",
            );
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }

        match start_workflow_attempt(broker, spec, wf_id, sent_at, registry, payload, retention)
            .await
        {
            Ok(handle) => return Ok(handle),
            Err(e) => {
                if e.retryable && attempt < max_attempts - 1 {
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }

    Err(last_err.expect("loop ran at least once"))
}

/// Writes the workflow tree through `horsies_workflow_start`.
///
/// The tree is built in memory first ([`start_arguments`]): the node rows of the workflow, a pre-built child
/// for every sub-workflow root (recursively), and a failed-node record for each sub-workflow root whose child
/// cannot start (past the depth limit, no registration under its key, a failing builder). A failed root fails
/// only its node and its workflow's `on_error` applies, as Python does (`core/workflows/engine.py:1019-1212`).
///
/// Then the broker handle checks its client family (`ensure_client_family_for_write`). A database of the
/// other family, or with no family row, gives `WorkflowError::Broker` with
/// `BrokerError::ClientFamilyMismatch` (`ENQUEUE_FAILED`, not retryable) and nothing is written.
///
/// The function inserts the top-level row with `ON CONFLICT (id) DO NOTHING`, so a concurrent start with the
/// same id waits for the first one and then sees its row: `EXISTS` with the stored name, and nothing is
/// written. Otherwise it inserts the child rows and every node row, fails the recorded nodes and runs their
/// progression, links each sub-workflow root to its pre-built child, and mints the task rows of the READY task
/// nodes. Without failed-node records that is one statement in autocommit; with them, a transaction that also
/// runs the child starts the progression owes ([`start_tree`]).
///
/// After the commit, the parent advances that those child starts owe run, one transaction per level. A
/// failed advance is logged and left to workflow recovery (case 1.6): the start committed, so the start
/// returns its outcome.
async fn start_workflow_inner(
    broker: &Arc<PostgresBroker>,
    spec: &WorkflowSpec,
    wf_id: Uuid,
    sent_at: chrono::DateTime<chrono::Utc>,
    registry: &WorkflowSpecRegistry,
    retention: &RetentionConfig,
) -> Result<WorkflowRowInsert, WorkflowError> {
    let arguments = start_arguments(spec, wf_id, registry, retention)?;

    // The handle writes only into a database of its own client family: checked once per handle,
    // before the first write. A mismatch is not retryable and writes nothing.
    broker.ensure_client_family_for_write().await?;

    let build = ChildBuild {
        registry,
        retention,
    };
    let started = start_tree(broker, &arguments, sent_at, build).await?;
    let outcome = match started.outcome {
        StartOutcome::Started => WorkflowRowInsert::Inserted,
        StartOutcome::Exists { stored_name } => existing_workflow(stored_name, &spec.name),
    };
    match &outcome {
        WorkflowRowInsert::Inserted => {
            tracing::debug!(workflow_id = %wf_id, name = %spec.name, "workflow created");
        }
        WorkflowRowInsert::Existing | WorkflowRowInsert::IdConflict { .. } => tracing::warn!(
            workflow_id = %wf_id,
            outcome = ?outcome,
            "workflow id already exists; nothing was written",
        ),
    }
    if let Err(failure) =
        advance_parents(broker, CheckoutClass::Batch, started.parent_advances, build).await
    {
        tracing::error!(
            workflow_id = %wf_id,
            child_workflow_id = %failure.failed_child_workflow_id,
            error = %failure.error,
            "parent advance after the workflow start failed; workflow recovery advances it",
        );
    }
    Ok(outcome)
}

/// What an existing top-level id means for a start of `name`: the same name is the existing workflow; another
/// name is an id conflict. A row that conflicted and then disappeared before the name read (a retention
/// delete) is `Existing`, as in Python (`core/workflows/lifecycle.py:420-458`). Pure.
fn existing_workflow(stored_name: Option<String>, name: &str) -> WorkflowRowInsert {
    match stored_name {
        Some(existing_name) if existing_name != name => {
            WorkflowRowInsert::IdConflict { existing_name }
        }
        Some(_) | None => WorkflowRowInsert::Existing,
    }
}

/// Maps a start error to its catalog code.
///
/// The start path returns only database, broker, serialization and validation errors. Any other variant is a
/// defect, so it panics with the workflow and the error; it is not a catalog code.
fn classify_workflow_error(
    error: &WorkflowError,
    workflow_name: &str,
    workflow_id: Uuid,
) -> WorkflowStartErrorCode {
    match error {
        WorkflowError::Serialization(_) | WorkflowError::Validation(_) => {
            WorkflowStartErrorCode::ValidationFailed
        }
        WorkflowError::Database(_) | WorkflowError::Broker(_) | WorkflowError::StepContract(_) => {
            WorkflowStartErrorCode::EnqueueFailed
        }
        WorkflowError::WorkflowNotFound { .. }
        | WorkflowError::WorkflowTimeout { .. }
        | WorkflowError::WorkflowError(_)
        | WorkflowError::InvalidStatus(_) => panic!(
            "start of workflow '{workflow_name}' ({workflow_id}) returned an error outside the start catalog: {error}"
        ),
    }
}

fn is_retryable_workflow_error(e: &WorkflowError) -> bool {
    match e {
        WorkflowError::Database(sqlx_err) => crate::broker::is_retryable_sqlx_error(sqlx_err),
        WorkflowError::Broker(broker_err) => broker_err.is_retryable(),
        _ => false,
    }
}

/// Retry a failed workflow start using the broker.
pub async fn retry_start<T>(
    broker: &Arc<PostgresBroker>,
    spec: &WorkflowSpec,
    error: &WorkflowStartError,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> WorkflowStartResult<WorkflowHandle<T>> {
    // Reuse the existing validation from horsies-core.
    let workflow_id = crate::core::workflow::start_types::validate_start_retry(error, &spec.name)?;
    start_workflow(
        broker,
        spec,
        Some(workflow_id),
        registry,
        payload,
        retention,
    )
    .await
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// The queue that the row of a sub-workflow node stores. Python: `core/workflows/lifecycle.py:534`.
const SUBWORKFLOW_NODE_QUEUE: &str = "default";

/// The priority that the row of a sub-workflow node stores. Python: `core/workflows/lifecycle.py:535`.
const SUBWORKFLOW_NODE_PRIORITY: i32 = 100;

/// Whether a sub-workflow node is reachable below a node along the dependency edges of its workflow.
///
/// The start computes it from the static graph, and the node row stores it
/// (`horsies_workflow_tasks.downstream_subworkflow`). Every node that the phase 2 of a node can
/// promote is below it, so the phase 2 of a `NotReachable` node never owes a child start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DownstreamSubworkflow {
    /// No sub-workflow node is below the node. Stored as `NONE`.
    NotReachable,
    /// At least one sub-workflow node is below the node. Stored as `REACHABLE`.
    Reachable,
}

impl DownstreamSubworkflow {
    /// The value that the node row stores.
    pub(crate) fn as_stored(self) -> &'static str {
        match self {
            Self::NotReachable => "NONE",
            Self::Reachable => "REACHABLE",
        }
    }
}

/// The `downstream_subworkflow` fact of each node of one workflow, in the order of `nodes`.
///
/// The walk starts at the dependencies of each sub-workflow node and goes up through the
/// dependencies. It visits each index once, so it takes at most one step per node and edge. A
/// sub-workflow node is `Reachable` only when another sub-workflow node is below it. Does no I/O.
///
/// Python computes the same at start (`with_downstream_subworkflow`, `python/horsies/core/workflows/sql.py`),
/// and so does the SQL start (`horsies_workflow_start_nodes`).
pub(crate) fn downstream_subworkflow_facts(nodes: &[AnyNode]) -> Vec<DownstreamSubworkflow> {
    let dependencies: HashMap<usize, &[usize]> = nodes
        .iter()
        .map(|node| (node.index, node.dependencies.as_slice()))
        .collect();
    let mut pending: Vec<usize> = nodes
        .iter()
        .filter(|node| node.is_subworkflow)
        .flat_map(|node| node.dependencies.iter().copied())
        .collect();
    let mut reachable: HashSet<usize> = HashSet::with_capacity(nodes.len());
    while let Some(index) = pending.pop() {
        if !reachable.insert(index) {
            continue;
        }
        if let Some(upstream) = dependencies.get(&index) {
            pending.extend_from_slice(upstream);
        }
    }
    nodes
        .iter()
        .map(|node| match reachable.contains(&node.index) {
            true => DownstreamSubworkflow::Reachable,
            false => DownstreamSubworkflow::NotReachable,
        })
        .collect()
}

/// The queue and priority that a node row stores.
///
/// A sub-workflow node is never claimed as a task, so it has no queue and no priority; its row stores the
/// constants of Python. A task node must have both resolved.
pub(crate) fn stored_queue_and_priority(node: &AnyNode) -> Result<(&str, i32), WorkflowError> {
    match node.is_subworkflow {
        true => Ok((SUBWORKFLOW_NODE_QUEUE, SUBWORKFLOW_NODE_PRIORITY)),
        false => {
            let queue = node.queue.as_deref().ok_or_else(|| {
                WorkflowError::Validation(format!(
                    "node '{}' (task '{}') has no resolved queue; \
                     this indicates a missing resolution step before workflow start",
                    node.node_id.as_deref().unwrap_or("?"),
                    node.task_name,
                ))
            })?;
            Ok((queue, resolve_insert_priority(node)?))
        }
    }
}

/// The `workflow_ctx_from` that a node row stores: the node ids of the node's context sources, or
/// `None` (SQL NULL) when the node takes no context.
///
/// A node defined with an empty list takes no context, and its row stores NULL, as for no list
/// (Python `_graph_columns`, `python/horsies/core/workflows/child_build.py:479-485`). The task input of
/// the baseline mint adds the context member for any stored array, an empty one included
/// (`horsies_workflow_task_input`). Pure.
pub(crate) fn stored_workflow_ctx_from(node: &AnyNode) -> Option<&[String]> {
    match node.workflow_ctx_from.as_deref() {
        None | Some([]) => None,
        Some(node_ids) => Some(node_ids),
    }
}

/// The task input of the root task node `task_index` (`task_name`) of the workflow `workflow_id`
/// that the child start of the engine code mints: the node's input with the workflow meta member
/// ([`WORKFLOW_META_KWARG`]: the workflow id, the node index and the task name). Python writes the
/// member into the input of every task it mints for a node
/// (`python/horsies/core/workflows/engine.py:946-950`, `:1447-1453` for a child root), and so does the
/// baseline mint (`horsies_workflow_task_input`). The worker removes it before the input decode.
///
/// Errors: the refusals of [`input_object`]: an input that does not read, that is not a JSON
/// object, or that holds a number that the re-encode would round. Pure.
pub(crate) fn input_with_workflow_meta(
    input: &str,
    workflow_id: Uuid,
    task_index: i32,
    task_name: &str,
) -> Result<String, WorkflowError> {
    let mut object = input_object(input)?;
    let meta = WorkflowMeta::new(workflow_id, task_index, task_name.to_owned());
    object.insert(WORKFLOW_META_KWARG.to_owned(), serde_json::to_value(meta)?);
    Ok(serde_json::to_string(&object)?)
}

/// Resolve the claimable priority for a task node about to be persisted.
///
/// A real task node must already have a resolved priority by insert time —
/// `resolve_node_queue_priority` runs at workflow registration, child-spec
/// materialization, and `check()`, filling it from the queue config. A `None`
/// here means a resolution step was skipped, so it is surfaced (fail-closed)
/// rather than silently persisting a wrong literal — mirroring the queue check
/// in `stored_queue_and_priority` and Python's `WORKFLOW_UNRESOLVED_PRIORITY`.
fn resolve_insert_priority(node: &AnyNode) -> Result<i32, WorkflowError> {
    node.priority.ok_or_else(|| {
        WorkflowError::Validation(format!(
            "node '{}' (task '{}') has no resolved priority; \
             this indicates a missing resolution step before workflow start",
            node.node_id.as_deref().unwrap_or("?"),
            node.task_name,
        ))
    })
}

/// Why the child start of the engine code ([`start_child_workflow_in_tx`]) did not write the child.
///
/// Python fails the parent node while it prepares the child (`enqueue_subworkflow_task`,
/// `python/horsies/core/workflows/engine.py:1045-1260`, `:1370-1425`): the depth limit, the definition
/// load, the `args_from` merge, the build, and the queue of a child node. The callers here check
/// those before the start (the build resolves the queues). Once the child
/// spec is built, a child that the start cannot write raises, and the caller's transaction rolls
/// back: task options of the task function that do not parse (`_child_task_options_json` and
/// `start_task_node_facts`, `:425-475`, `:1399-1401`, `:1430-1437`), a value that does not
/// serialize, a database error. The child start through `horsies_workflow_child_start` treats a
/// child tree that it cannot write the same way (`child_start::run_child_starts`).
#[derive(Debug)]
pub(crate) enum ChildStartStop {
    /// A sub-workflow root of the child cannot start: it would pass the depth limit, its definition
    /// is not registered, or its build failed. This start writes no failed-node record for it, so the
    /// parent node fails instead. The error is split as
    /// [`crate::workflow_engine::engine::PreparationStop`] splits a `WorkflowError`. (Python fails
    /// the root node inside the child.)
    RootSubworkflowNotStarted(WorkflowError),
    /// Every other failure: an error of the caller's step.
    Failed(WorkflowError),
}

/// A `WorkflowError` of the child start is an error of the caller's step.
impl From<WorkflowError> for ChildStartStop {
    fn from(error: WorkflowError) -> Self {
        Self::Failed(error)
    }
}

impl From<sqlx::Error> for ChildStartStop {
    fn from(error: sqlx::Error) -> Self {
        Self::Failed(WorkflowError::Database(error))
    }
}

impl From<serde_json::Error> for ChildStartStop {
    fn from(error: serde_json::Error) -> Self {
        Self::Failed(WorkflowError::Serialization(error))
    }
}

/// Insert all workflow_task rows. Root tasks (no deps) are marked READY
/// and immediately enqueued into horsies_tasks. Root sub-workflow nodes
/// are detected and launched as child workflows.
///
/// `workflow_sent_at` is the start call time of the workflow (its stored `sent_at`); every root task row
/// stores it as its own `sent_at`.
///
/// Errors: [`ChildStartStop`]. Task options that do not parse or merge with the node's deadline are
/// [`ChildStartStop::Failed`], as every failure is except a sub-workflow root that cannot start.
///
/// Returns a boxed future because this function is mutually recursive with
/// `launch_root_subworkflow` → `start_child_workflow_in_tx`.
fn insert_workflow_tasks<'a>(
    tx: &'a mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: Uuid,
    workflow_sent_at: DateTime<Utc>,
    tasks: &'a [AnyNode],
    registry: &'a WorkflowSpecRegistry,
    retention: &'a RetentionConfig,
) -> Pin<Box<dyn Future<Output = Result<(), ChildStartStop>> + Send + 'a>> {
    Box::pin(async move {
        if tasks.is_empty() {
            return Ok(());
        }

        // ── Phase 1: prepare every row in memory ──
        let downstream_facts = downstream_subworkflow_facts(tasks);
        let mut prepared: Vec<PreparedNode> = Vec::with_capacity(tasks.len());
        for (task, downstream_subworkflow) in tasks.iter().zip(downstream_facts) {
            let is_root = task.dependencies.is_empty();
            let (queue, priority) = stored_queue_and_priority(task)?;

            // Merge good_until into task_options_json (mirrors Python's lifecycle.py).
            let merged_task_options = crate::workflow_engine::merge_good_until_into_options(
                task.index,
                &task.task_name,
                task.good_until,
                task.task_options_json.as_deref(),
            )?;

            let args_from_json = if task.args_from.is_empty() {
                None
            } else {
                Some(serde_json::to_value(&task.args_from)?)
            };

            // Extract sub_workflow_name from task_name for sub-workflow nodes.
            let sub_workflow_name: Option<String> = if task.is_subworkflow {
                Some(
                    task.task_name
                        .strip_prefix("__sub_workflow:")
                        .unwrap_or(&task.task_name)
                        .to_owned(),
                )
            } else {
                None
            };

            // Fast-path roots: plain TaskNodes with no inbound args/ctx. They are
            // inserted directly as ENQUEUED with a task_id (no follow-up LINK) and
            // their horsies_tasks rows are bulk-inserted. Skipping the per-row CAS
            // is sound: the node rows are created in this same uncommitted tx, so no
            // concurrent transaction can observe or race them.
            let has_ctx_from = stored_workflow_ctx_from(task).is_some();
            let fast_path =
                is_root && !task.is_subworkflow && task.args_from.is_empty() && !has_ctx_from;
            let task_id = if fast_path {
                Some(
                    crate::core::history::identity::uuid7::mint_task_id().map_err(|error| {
                        WorkflowError::Validation(format!("task identity mint failed: {error}"))
                    })?,
                )
            } else {
                None
            };
            let task_facts = if task.is_subworkflow {
                None
            } else {
                Some(node_task_facts(
                    task,
                    queue,
                    merged_task_options.as_deref(),
                    retention,
                )?)
            };
            let status = if fast_path {
                "ENQUEUED"
            } else if is_root {
                "READY"
            } else {
                "PENDING"
            };

            prepared.push(PreparedNode {
                node: task,
                wt_id: Uuid::new_v4(),
                queue: queue.to_owned(),
                priority,
                args_from_json,
                join_str: task.join.to_string(),
                merged_task_options,
                sub_workflow_name,
                status,
                task_id,
                task_facts,
                is_root,
                fast_path,
                downstream_subworkflow,
            });
        }

        // ── Phase 2: bulk-insert all workflow_task rows ──
        let mut qb: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO horsies_workflow_tasks (\
             id, workflow_id, task_index, node_id, task_name, \
             input, queue_name, priority, \
             dependencies, args_from, workflow_ctx_from, \
             allow_failed_deps, join_type, min_success, \
             task_options, max_retries, execution, good_until, \
             retention_class_key, status, is_subworkflow, sub_workflow_params, \
             downstream_subworkflow, sub_workflow_name, sub_definition_key, \
             task_id, started_at, created_at) ",
        );
        qb.push_values(prepared.iter(), |mut b, p| {
            let deps: Vec<i32> = p.node.dependencies.iter().map(|&d| d as i32).collect();
            b.push_bind(p.wt_id)
                .push_unseparated("::uuid")
                .push_bind(workflow_id)
                .push_unseparated("::uuid")
                .push_bind(p.node.index as i32)
                .push_bind(p.node.node_id.clone())
                .push_bind(p.node.task_name.clone())
                .push_bind(p.node.input_json.clone())
                .push_bind(p.queue.clone())
                .push_bind(p.priority)
                .push_bind(deps)
                .push_bind(p.args_from_json.clone())
                .push_bind(stored_workflow_ctx_from(p.node).map(|node_ids| node_ids.to_vec()))
                .push_bind(p.node.allow_failed_deps)
                .push_bind(p.join_str.clone())
                .push_bind(p.node.min_success)
                .push_bind(p.merged_task_options.clone())
                .push_bind(p.task_facts.as_ref().map(|facts| facts.max_retries))
                .push_bind(
                    p.task_facts
                        .as_ref()
                        .map(|facts| facts.facts.execution.as_str()),
                )
                .push_bind(p.task_facts.as_ref().and_then(|facts| facts.good_until))
                .push_bind(
                    p.task_facts
                        .as_ref()
                        .map(|facts| facts.facts.retention_class_key.clone()),
                )
                .push_bind(p.status)
                .push_bind(p.node.is_subworkflow)
                .push_bind(match p.node.is_subworkflow {
                    // A task node has no child: its row stores NULL.
                    true => Some(p.node.sub_workflow_params.as_stored()),
                    false => None,
                })
                .push_bind(p.downstream_subworkflow.as_stored())
                .push_bind(p.sub_workflow_name.clone())
                .push_bind(p.node.sub_definition_key.clone())
                .push_bind(p.task_id.clone())
                .push_unseparated("::uuid");
            // started_at: a fast root goes in ENQUEUED, so it holds its enqueue time; every other
            // node has none until its enqueue (Python `INSERT_WORKFLOW_TASKS_BULK_SQL`,
            // `core/workflows/sql.py:46-58`).
            match p.fast_path {
                true => b.push("NOW()"),
                false => b.push("NULL"),
            };
            b.push("NOW()"); // created_at
        });
        qb.build().execute(&mut **tx).await?;

        // ── Phase 3: bulk-insert fast-path roots' horsies_tasks rows ──
        // Engine-minted rows carry no fingerprint, key or retained input.
        if prepared.iter().any(|p| p.fast_path) {
            // The task input of each fast root: its node input with the workflow meta member
            // (Python `python/horsies/core/workflows/engine.py:1447-1453`). The node row keeps the
            // node input.
            let fast_roots = prepared
                .iter()
                .filter(|p| p.fast_path)
                .map(|p| {
                    input_with_workflow_meta(
                        &p.node.input_json,
                        workflow_id,
                        p.node.index as i32,
                        &p.node.task_name,
                    )
                    .map(|task_input| (p, task_input))
                })
                .collect::<Result<Vec<_>, WorkflowError>>()?;
            let mut tq: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
                "INSERT INTO horsies_tasks (\
                 id, task_name, queue_name, priority, input, task_options, execution, \
                 retention_class_key, sent_at, created_at, \
                 command_fingerprint_version, command_fingerprint, idempotency_key_digest, \
                 status, enqueued_at, retry_count, max_retries, good_until, \
                 is_workflow_task, updated_at, \
                 retain_rerun_input, rerun_of_task_id, rerun_root_task_id) ",
            );
            tq.push_values(fast_roots.iter(), |mut b, (p, task_input)| {
                let task_id = p
                    .task_id
                    .clone()
                    .expect("fast-path root always has a task_id");
                let task_facts = p
                    .task_facts
                    .as_ref()
                    .expect("fast-path root is a task node and always has task facts");
                b.push_bind(task_id)
                    .push_unseparated("::uuid")
                    .push_bind(p.node.task_name.clone())
                    .push_bind(p.queue.clone())
                    .push_bind(p.priority)
                    .push_bind(task_input.clone())
                    .push_bind(p.merged_task_options.clone())
                    .push_bind(task_facts.facts.execution.as_str())
                    .push_bind(task_facts.facts.retention_class_key.clone())
                    // sent_at: the start call time of the workflow.
                    .push_bind(workflow_sent_at);
                // created_at, the fingerprint pair and the key digest, status,
                // enqueued_at, retry_count as literals — NOW() is
                // transaction-stable, so claim ordering matches the per-row shape.
                b.push("NOW()")
                    .push("NULL")
                    .push("NULL")
                    .push("NULL")
                    .push("'PENDING'")
                    .push("NOW()")
                    .push("0");
                b.push_bind(task_facts.max_retries)
                    .push_bind(task_facts.good_until);
                b.push("TRUE")
                    .push("NOW()")
                    .push("FALSE")
                    .push("NULL")
                    .push("NULL");
            });
            tq.build().execute(&mut **tx).await?;
        }

        // ── Phase 4: per-row slow path (subworkflow roots, args_from/ctx_from roots) ──
        // Node rows already exist (Phase 2, status READY); these promote them.
        for p in &prepared {
            if !p.is_root || p.fast_path {
                continue;
            }
            if p.node.is_subworkflow {
                launch_root_subworkflow(tx, workflow_id, p.node, registry, retention).await?;
            } else {
                let task_id = enqueue_root_task(
                    &mut **tx,
                    workflow_id,
                    p.node,
                    &p.queue,
                    p.priority,
                    p.merged_task_options.as_deref(),
                    retention,
                    workflow_sent_at,
                )
                .await?;
                sqlx::query(LINK_WORKFLOW_TASK_SQL)
                    .bind(&task_id)
                    .bind(workflow_id)
                    .bind(p.node.index as i32)
                    .execute(&mut **tx)
                    .await?;

                tracing::debug!(
                    workflow_id = %workflow_id,
                    task_index = p.node.index,
                    task_id = %task_id,
                    "root task enqueued (slow path)",
                );
            }
        }

        Ok(())
    }) // end Box::pin
}

/// Create a horsies_tasks row for a root workflow task of the workflow `workflow_id`.
/// Returns the task_id. `workflow_sent_at` is the start call time of the node's workflow, which the row
/// stores as its `sent_at`. The row's input is the node input with the workflow meta member
/// ([`input_with_workflow_meta`]).
async fn enqueue_root_task(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    workflow_id: Uuid,
    node: &AnyNode,
    queue: &str,
    priority: i32,
    merged_task_options: Option<&str>,
    retention: &RetentionConfig,
    workflow_sent_at: DateTime<Utc>,
) -> Result<Uuid, WorkflowError> {
    let task_id = crate::core::history::identity::uuid7::mint_task_id().map_err(|error| {
        WorkflowError::Validation(format!("task identity mint failed: {error}"))
    })?;
    let task_facts = node_task_facts(node, queue, merged_task_options, retention)?;
    let task_input = input_with_workflow_meta(
        &node.input_json,
        workflow_id,
        node.index as i32,
        &node.task_name,
    )?;

    sqlx::query(ENQUEUE_ROOT_TASK_SQL)
        .bind(task_id)
        .bind(&node.task_name)
        .bind(queue)
        .bind(priority)
        .bind(task_input.as_str())
        .bind(merged_task_options)
        .bind(task_facts.facts.execution.as_str())
        .bind(&task_facts.facts.retention_class_key)
        .bind(task_facts.max_retries)
        .bind(task_facts.good_until)
        .bind(workflow_sent_at)
        .execute(executor)
        .await?;

    Ok(task_id)
}

/// Launch a child workflow for a root sub-workflow node at start time.
///
/// Resolves the spec from the registry and starts the child workflow,
/// linking the parent workflow_task to it.
///
/// Errors: a child past the depth limit, or a child that cannot be built, is
/// [`ChildStartStop::RootSubworkflowNotStarted`]; the errors of its own start
/// are passed on unchanged.
async fn launch_root_subworkflow(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: Uuid,
    task: &AnyNode,
    registry: &WorkflowSpecRegistry,
    retention: &RetentionConfig,
) -> Result<(), ChildStartStop> {
    let spec_name = task
        .task_name
        .strip_prefix("__sub_workflow:")
        .unwrap_or(&task.task_name);

    // Get parent depth and root_workflow_id.
    let depth_row: DepthRow = sqlx::query_as(GET_WORKFLOW_DEPTH_SQL)
        .bind(workflow_id)
        .fetch_one(&mut **tx)
        .await?;

    let parent_depth = depth_row.depth.unwrap_or(0);
    let root_wf_id = depth_row.root_workflow_id.unwrap_or(workflow_id);
    // A root sub-workflow node of a start fails the whole start: the node-level failure of a root child
    // comes with the start redesign.
    let child_depth = child_workflow_depth(
        parent_depth,
        workflow_id,
        &depth_row.name,
        task.index as i32,
    )
    .map_err(|error| {
        ChildStartStop::RootSubworkflowNotStarted(WorkflowError::Validation(error.to_string()))
    })?;

    // Build the child spec (supports dynamic parameterization if a builder is registered).
    let child_spec = build_child_spec(
        spec_name,
        task.sub_definition_key.as_deref(),
        task,
        registry,
    )
    .map_err(ChildStartStop::RootSubworkflowNotStarted)?;

    // Start the child workflow within the same transaction.
    let child_id = start_child_workflow_in_tx(
        tx,
        &child_spec,
        workflow_id,
        task.index as i32,
        child_depth,
        root_wf_id,
        registry,
        retention,
    )
    .await?;

    // Link parent workflow_task to child workflow.
    sqlx::query(LINK_ROOT_SUBWORKFLOW_SQL)
        .bind(&child_id)
        .bind(workflow_id)
        .bind(task.index as i32)
        .execute(&mut **tx)
        .await?;

    tracing::debug!(
        workflow_id = %workflow_id,
        task_index = task.index,
        child_workflow_id = %child_id,
        spec_name,
        "root sub-workflow launched at start",
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Child workflow start
// ---------------------------------------------------------------------------

/// Start a child workflow within an existing transaction.
///
/// Used by `launch_root_subworkflow`, `enqueue_subworkflow_task` and recovery
/// case 1.5 to keep child creation + parent link inside a single transaction.
///
/// Errors: [`ChildStartStop`]. Only a sub-workflow root of the child that
/// cannot start refuses the parent node; every other failure (task options of
/// the task function that do not parse or do not merge with the node's
/// deadline, a database error) is an error of the caller's step. Nothing is
/// committed here: the caller rolls its transaction back.
pub(crate) async fn start_child_workflow_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    spec: &WorkflowSpec,
    parent_workflow_id: Uuid,
    parent_task_index: i32,
    depth: i32,
    root_workflow_id: Uuid,
    registry: &WorkflowSpecRegistry,
    retention: &RetentionConfig,
) -> Result<Uuid, ChildStartStop> {
    let child_id = Uuid::new_v4();
    let sent_at = chrono::Utc::now();
    require_subworkflow_node_keys(spec)?;

    let on_error_str = match spec.on_error {
        OnError::Fail => "fail",
        OnError::Pause => "pause",
    };

    let success_policy_json = spec
        .success_policy
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;

    sqlx::query(INSERT_CHILD_WORKFLOW_SQL)
        .bind(&child_id)
        .bind(&spec.name)
        .bind(on_error_str)
        .bind(spec.output_index.map(|i| i as i32))
        .bind(&success_policy_json)
        .bind(&spec.definition_key)
        .bind(parent_workflow_id)
        .bind(parent_task_index)
        .bind(depth)
        .bind(root_workflow_id)
        .bind(sent_at)
        .execute(&mut **tx)
        .await?;

    tracing::debug!(
        child_workflow_id = %child_id,
        parent_workflow_id = %parent_workflow_id,
        parent_task_index,
        depth,
        name = %spec.name,
        "child workflow created (in transaction)",
    );

    // Insert all workflow_task rows and enqueue roots within same tx.
    insert_workflow_tasks(tx, child_id, sent_at, &spec.tasks, registry, retention).await?;

    Ok(child_id)
}

/// Build a child workflow spec, using the spec_builder callback if available,
/// otherwise falling back to the static registered spec.
///
/// This enables dynamic sub-workflow parameterization (Gap 14-7): the
/// parent node's input is passed to the builder so the child DAG can vary
/// based on upstream results.
///
/// For dynamically built specs, resolves task retry options from the
/// registry's `task_options_map` so workflow tasks inherit their
/// registered retry configuration.
pub(crate) fn build_child_spec(
    spec_name: &str,
    definition_key: Option<&str>,
    parent_task: &AnyNode,
    registry: &WorkflowSpecRegistry,
) -> Result<WorkflowSpec, WorkflowError> {
    let resolved = definition_key
        .and_then(|key| registry.resolve_child_registration(key))
        .ok_or_else(|| {
            WorkflowError::Validation(format!(
                "sub-workflow spec not found (definition_key={:?}, name='{}')",
                definition_key, spec_name,
            ))
        })?;

    // A start node has no dependencies, so it has no `args_from`.
    let input =
        crate::workflow_engine::args_merge::child_build_input(parent_task.input_json.clone(), None)
            .map_err(|refused| WorkflowError::from(ChildSpecError::of_stored_input(refused)))?;
    Ok(materialize_child_spec(
        resolved,
        parent_task.sub_workflow_params,
        &input.text,
        registry,
    )?)
}

/// The params choice that a sub-workflow node row stores
/// (`horsies_workflow_tasks.sub_workflow_params`), as the client set it when
/// it defined the node.
///
/// Every sub-workflow node row stores `NOT_GIVEN` or `GIVEN`. `NULL` or any
/// other value on a sub-workflow node row is refused with
/// `WorkflowError::Validation`; the row is not read any other way. Does no
/// I/O.
pub(crate) fn stored_sub_workflow_params(
    stored: Option<&str>,
) -> Result<SubWorkflowParams, WorkflowError> {
    match stored.map(SubWorkflowParams::from_stored) {
        Some(Some(params)) => Ok(params),
        Some(None) | None => Err(WorkflowError::Validation(format!(
            "sub-workflow node row stores no known params choice \
             (sub_workflow_params = {:?}; expected NOT_GIVEN or GIVEN)",
            stored,
        ))),
    }
}

/// What names a failed child build in the node error. A builder failure takes the Python exception
/// class, Python's `type(exc).__name__` (`core/workflows/engine.py:1196-1211`): a [`HorsiesError`]
/// takes the class of its error category, the subclass of Python's `HorsiesError` that the category
/// stands for ([`ErrorCategory`]); an error with no code takes the base class. A stored node input
/// that the core refuses ([`Self::StoredInputRefused`]) names no class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChildSpecErrorType {
    /// A [`HorsiesError`] with no error code.
    HorsiesError,
    /// A [`HorsiesError`] whose code is of [`ErrorCategory::Workflow`].
    WorkflowValidationError,
    /// A [`HorsiesError`] whose code is of [`ErrorCategory::Task`].
    TaskDefinitionError,
    /// A [`HorsiesError`] whose code is of [`ErrorCategory::Config`].
    ConfigurationError,
    /// A [`HorsiesError`] whose code is of [`ErrorCategory::Registry`].
    RegistryError,
    /// A node that gives params to a child registered without a builder. Python raises
    /// `RuntimeError` for the same node (`core/workflows/engine.py:1185-1189`).
    RuntimeError,
    /// A stored node input from which the core cannot make the builder input
    /// ([`child_build_input`](crate::workflow_engine::args_merge::child_build_input)): the text
    /// does not read, or an `args_from` entry is malformed. Python has no such case: its builder
    /// gets typed values, and the stored input is corrupt data, not a user error.
    StoredInputRefused,
}

impl ChildSpecErrorType {
    /// The class of `error`. Pure.
    pub(crate) fn of(error: &HorsiesError) -> Self {
        match error.category() {
            None => Self::HorsiesError,
            Some(ErrorCategory::Workflow) => Self::WorkflowValidationError,
            Some(ErrorCategory::Task) => Self::TaskDefinitionError,
            Some(ErrorCategory::Config) => Self::ConfigurationError,
            Some(ErrorCategory::Registry) => Self::RegistryError,
        }
    }

    /// The label before the text in the node error: the class name, as Python's
    /// `type(exc).__name__` gives it, or `stored node input refused`, which is no class.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::HorsiesError => "HorsiesError",
            Self::WorkflowValidationError => "WorkflowValidationError",
            Self::TaskDefinitionError => "TaskDefinitionError",
            Self::ConfigurationError => "ConfigurationError",
            Self::RegistryError => "RegistryError",
            Self::RuntimeError => "RuntimeError",
            Self::StoredInputRefused => "stored node input refused",
        }
    }
}

/// Why a registered child gave no spec: the label and the text. For a builder failure they are
/// Python's `type(exc).__name__` and `str(exc)` (`core/workflows/engine.py:1196-1211`); for a
/// refused stored input, `stored node input refused` and the refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChildSpecError {
    pub(crate) error_type: ChildSpecErrorType,
    /// The full text; the node error keeps its first 200 characters.
    pub(crate) text: String,
}

impl ChildSpecError {
    /// The error of a builder, or of the check of the spec it built: the class of its category and its
    /// text (`Display`). Pure.
    fn of_builder(error: &HorsiesError) -> Self {
        Self {
            error_type: ChildSpecErrorType::of(error),
            text: error.to_string(),
        }
    }

    /// A stored node input from which the builder input cannot be made. Pure.
    pub(crate) fn of_stored_input(
        refused: crate::workflow_engine::args_merge::StoredInputRefused,
    ) -> Self {
        Self {
            error_type: ChildSpecErrorType::StoredInputRefused,
            text: refused.refusal,
        }
    }
}

/// A child build that fails on a path that refuses the step instead of failing the node: the label and
/// the text in one validation message.
impl From<ChildSpecError> for WorkflowError {
    fn from(error: ChildSpecError) -> Self {
        WorkflowError::Validation(format!("{}: {}", error.error_type.as_str(), error.text))
    }
}

/// Why a registered child gave no spec for a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChildSpecFailure {
    /// The value of the node input member `key` does not decode to the builder parameter of that name;
    /// the builder did not run. `cause` is the decode's text. Python fails the node with
    /// `WORKER_SERIALIZATION_ERROR` (`core/workflows/engine.py:1063-1080`, `:1134-1157`).
    MemberRefused { key: String, cause: String },
    /// The build failed: the builder, the check of its spec, or params given to a child without a
    /// builder. Python fails the node with `WORKFLOW_ENQUEUE_FAILED` (`:1196-1211`).
    BuildFailed(ChildSpecError),
}

/// A child build that fails on a path that refuses the step instead of failing the node.
impl From<ChildSpecFailure> for WorkflowError {
    fn from(failure: ChildSpecFailure) -> Self {
        match failure {
            ChildSpecFailure::MemberRefused { key, cause } => WorkflowError::Validation(format!(
                "the child's params member '{key}' does not decode: {cause}"
            )),
            ChildSpecFailure::BuildFailed(error) => error.into(),
        }
    }
}

/// Build the child spec from the parent node's encoded input (`input_json`,
/// as the client encoded it). A child registered without a builder refuses a
/// node that gives params (`SubWorkflowParams::Given`); the refusal text names
/// the client's builder hook ([`WorkflowSpecRegistry::builder_hook`]: Python
/// `build_with()`, Rust `spec_builder`; Python's text,
/// `core/workflows/engine.py:1185-1189`).
///
/// `input_json` is the text of a [`ChildBuildInput`](crate::workflow_engine::args_merge::ChildBuildInput)
/// ([`child_build_input`](crate::workflow_engine::args_merge::child_build_input)): each `args_from`
/// wrap is already replaced by its `inner` result envelope. This function does
/// not unwrap.
///
/// Errors: [`ChildSpecFailure::MemberRefused`] when the builder's decode
/// refuses the value of one input member; [`ChildSpecFailure::BuildFailed`]
/// when the builder fails, when the built spec does not pass the registry's
/// check, or for params given to a child without a builder
/// ([`ChildSpecErrorType::RuntimeError`]). Does no I/O; it runs the
/// registered builder.
pub(crate) fn materialize_child_spec(
    resolved: crate::core::registry::workflow::ResolvedChildWorkflow<'_>,
    params: SubWorkflowParams,
    input_json: &str,
    registry: &WorkflowSpecRegistry,
) -> Result<WorkflowSpec, ChildSpecFailure> {
    match resolved {
        crate::core::registry::workflow::ResolvedChildWorkflow::Dynamic(registered) => {
            built_child_spec(&registered.spec_builder, input_json, registry)
        }
        crate::core::registry::workflow::ResolvedChildWorkflow::Static(registered) => {
            match (&registered.spec_builder, params) {
                (Some(builder), _) => built_child_spec(builder, input_json, registry),
                (None, SubWorkflowParams::Given) => {
                    Err(ChildSpecFailure::BuildFailed(ChildSpecError {
                        error_type: ChildSpecErrorType::RuntimeError,
                        text: format!(
                            "subworkflow '{}' received runtime parameters but has no {} that accepts them",
                            registered.spec.name,
                            registry.builder_hook().as_str(),
                        ),
                    }))
                }
                (None, SubWorkflowParams::NotGiven) => Ok(registered.spec.clone()),
            }
        }
    }
}

/// Runs a registered builder on `input_json`, then resolves the task options
/// of the built spec and checks it against the registry.
///
/// Errors: [`ChildSpecFailure::MemberRefused`] for the builder's member
/// refusal; [`ChildSpecFailure::BuildFailed`] with the builder's error or the
/// check's error.
fn built_child_spec(
    builder: &crate::core::SpecBuilderFn,
    input_json: &str,
    registry: &WorkflowSpecRegistry,
) -> Result<WorkflowSpec, ChildSpecFailure> {
    let mut spec = builder(input_json).map_err(|error| match error {
        SpecBuildError::MemberRefused { key, cause } => {
            ChildSpecFailure::MemberRefused { key, cause }
        }
        SpecBuildError::Failed(error) => {
            ChildSpecFailure::BuildFailed(ChildSpecError::of_builder(&error))
        }
    })?;
    registry.resolve_spec_task_options(&mut spec);
    registry
        .resolve_and_validate_spec(&mut spec)
        .map_err(|error| ChildSpecFailure::BuildFailed(ChildSpecError::of_builder(&error)))?;
    Ok(spec)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::core::registry::workflow::{RegisteredWorkflowDefinition, RegisteredWorkflowSpec};
    use crate::core::workflow::spec::WorkflowSpecBuilder;
    use crate::core::workflow::sub_workflow::SubWorkflowNode;
    use crate::TaskNode;
    use serial_test::serial;

    fn child_registered_static() -> RegisteredWorkflowSpec {
        let mut builder = WorkflowSpecBuilder::new("child_static");
        builder.definition_key("tests.child_static.v1");
        let node = builder.task(TaskNode::<String>::raw("hello_task", "null").queue("default"));
        builder.output(node);
        RegisteredWorkflowSpec {
            spec: builder.build().unwrap(),
            spec_builder: None,
        }
    }

    /// A node with no input stores `{}`, as Python does: the start and the resume refuse a node input that
    /// is not a JSON object text (`horsies_workflow_task_input`, `0005_workflow_functions.sql:341-343`).
    fn node_with(is_subworkflow: bool, priority: Option<i32>) -> AnyNode {
        AnyNode {
            task_name: "t".to_owned(),
            input_json: "{}".to_owned(),
            dependencies: vec![],
            args_from: std::collections::HashMap::new(),
            workflow_ctx_from: None,
            workflow_ctx_from_refs: None,
            queue: Some("default".to_owned()),
            priority,
            allow_failed_deps: false,
            join: crate::core::workflow::node::JoinType::All,
            min_success: None,
            good_until: None,
            index: 0,
            node_id: Some("n".to_owned()),
            task_options_json: None,
            is_subworkflow,
            sub_definition_key: None,
            sub_workflow_params: crate::core::workflow::node::SubWorkflowParams::NotGiven,
        }
    }

    #[test]
    fn resolve_insert_priority_returns_resolved_task_priority() {
        assert_eq!(
            resolve_insert_priority(&node_with(false, Some(30))).unwrap(),
            30
        );
    }

    #[test]
    fn resolve_insert_priority_fails_closed_for_unresolved_task() {
        let err = resolve_insert_priority(&node_with(false, None)).unwrap_err();
        assert!(
            matches!(err, WorkflowError::Validation(ref m) if m.contains("no resolved priority")),
            "expected fail-closed unresolved-priority error, got: {:?}",
            err
        );
    }

    #[test]
    fn a_subworkflow_node_row_stores_the_python_constants() {
        let mut node = node_with(true, Some(50));
        node.queue = None;
        assert_eq!(stored_queue_and_priority(&node).unwrap(), ("default", 100));
        node.queue = Some("bulk".to_owned());
        assert_eq!(stored_queue_and_priority(&node).unwrap(), ("default", 100));
    }

    #[test]
    fn a_task_node_row_stores_its_resolved_queue_and_priority() {
        assert_eq!(
            stored_queue_and_priority(&node_with(false, Some(30))).unwrap(),
            ("default", 30)
        );
        let mut unresolved = node_with(false, Some(30));
        unresolved.queue = None;
        let err = stored_queue_and_priority(&unresolved).unwrap_err();
        assert!(
            matches!(err, WorkflowError::Validation(ref m) if m.contains("no resolved queue")),
            "expected fail-closed unresolved-queue error, got: {:?}",
            err
        );
    }

    /// A node defined with no list or with an empty list takes no context, and its row stores NULL
    /// (Python `_graph_columns`, `python/horsies/core/workflows/child_build.py:479-485`).
    #[test]
    fn an_empty_context_list_is_no_context() {
        let mut node = node_with(false, Some(10));
        assert_eq!(stored_workflow_ctx_from(&node), None);
        node.workflow_ctx_from = Some(Vec::new());
        assert_eq!(stored_workflow_ctx_from(&node), None);
        node.workflow_ctx_from = Some(vec!["a".to_owned(), "b".to_owned()]);
        assert_eq!(
            stored_workflow_ctx_from(&node),
            Some(["a".to_owned(), "b".to_owned()].as_slice())
        );
    }

    /// The task input of a root carries the workflow meta member next to the node's own members
    /// (Python `python/horsies/core/workflows/engine.py:1447-1453`). An input that is not a JSON object
    /// takes no member and is refused.
    #[test]
    fn a_root_task_input_carries_the_workflow_meta() {
        let workflow_id = Uuid::new_v4();
        let input =
            input_with_workflow_meta(r#"{"value": 1}"#, workflow_id, 3, "meta_task").unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&input).unwrap();
        assert_eq!(parsed["value"], serde_json::json!(1));
        assert_eq!(
            parsed[WORKFLOW_META_KWARG],
            serde_json::json!({
                "workflow_id": workflow_id.to_string(),
                "task_index": 3,
                "task_name": "meta_task",
            })
        );
        assert_eq!(parsed.as_object().map(|object| object.len()), Some(2));
        for refused in ["null", "[1]", "\"text\""] {
            assert!(
                matches!(
                    input_with_workflow_meta(refused, workflow_id, 0, "meta_task"),
                    Err(WorkflowError::Validation(_))
                ),
                "{refused}"
            );
        }
    }

    /// A node of the given kind at `index` with the given dependencies.
    fn graph_node(index: usize, is_subworkflow: bool, dependencies: Vec<usize>) -> AnyNode {
        let mut node = node_with(is_subworkflow, Some(10));
        node.index = index;
        node.node_id = Some(format!("n{index}"));
        node.dependencies = dependencies;
        node.sub_definition_key = match is_subworkflow {
            true => Some("tests.downstream.child.v1".to_owned()),
            false => None,
        };
        node
    }

    #[test]
    fn a_node_above_a_subworkflow_node_is_reachable() {
        use DownstreamSubworkflow::{NotReachable, Reachable};
        // 0 -> 1 -> 2 (sub-workflow) -> 4 (sub-workflow); 0 -> 3.
        let nodes = vec![
            graph_node(0, false, vec![]),
            graph_node(1, false, vec![0]),
            graph_node(2, true, vec![1]),
            graph_node(3, false, vec![0]),
            graph_node(4, true, vec![2]),
        ];
        assert_eq!(
            downstream_subworkflow_facts(&nodes),
            vec![Reachable, Reachable, Reachable, NotReachable, NotReachable]
        );
        // Diamond: both paths lead to the one sub-workflow node, each node is visited once.
        let diamond = vec![
            graph_node(0, false, vec![]),
            graph_node(1, false, vec![0]),
            graph_node(2, false, vec![0]),
            graph_node(3, true, vec![1, 2]),
        ];
        assert_eq!(
            downstream_subworkflow_facts(&diamond),
            vec![Reachable, Reachable, Reachable, NotReachable]
        );
    }

    #[test]
    fn a_workflow_without_subworkflow_nodes_has_no_reachable_node() {
        let nodes = vec![graph_node(0, false, vec![]), graph_node(1, false, vec![0])];
        assert_eq!(
            downstream_subworkflow_facts(&nodes),
            vec![DownstreamSubworkflow::NotReachable; 2]
        );
        assert_eq!(DownstreamSubworkflow::NotReachable.as_stored(), "NONE");
        assert_eq!(DownstreamSubworkflow::Reachable.as_stored(), "REACHABLE");
    }

    /// The start stores each node's `downstream_subworkflow` from the static graph, and
    /// `sub_workflow_params` on the sub-workflow node only.
    #[tokio::test]
    #[serial]
    async fn the_start_stores_the_downstream_subworkflow_fact_of_each_node() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let spec = keyed_spec(
            Some("tests.downstream.start.v1"),
            vec![
                graph_node(0, false, vec![]),
                graph_node(1, true, vec![0]),
                graph_node(2, false, vec![0]),
            ],
        );
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let workflow_id = Uuid::new_v4();

        let _: WorkflowHandle<serde_json::Value> = start_workflow(
            &broker,
            &spec,
            Some(workflow_id),
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("start workflow");

        let nodes: Vec<(i32, String, Option<String>)> = sqlx::query_as(
            "SELECT task_index, downstream_subworkflow, sub_workflow_params
             FROM horsies_workflow_tasks WHERE workflow_id = $1 ORDER BY task_index",
        )
        .bind(workflow_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            nodes,
            vec![
                (0, "REACHABLE".to_owned(), None),
                (1, "NONE".to_owned(), Some("NOT_GIVEN".to_owned())),
                (2, "NONE".to_owned(), None),
            ]
        );

        sqlx::query(
            "DELETE FROM horsies_tasks WHERE id IN (
                SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1
             )",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// Python `core/workflows/engine.py:1017-1033`: the message names the parent by its name, `data` by
    /// its id.
    #[test]
    fn child_depth_is_bounded_at_32() {
        let parent = Uuid::new_v4();
        assert_eq!(
            child_workflow_depth(0, parent, "deep_parent", 1).unwrap(),
            1
        );
        assert_eq!(
            child_workflow_depth(31, parent, "deep_parent", 1).unwrap(),
            32
        );
        let error = child_workflow_depth(32, parent, "deep_parent", 4).unwrap_err();
        assert_eq!(
            error.error_code.to_string(),
            "SUBWORKFLOW_DEPTH_LIMIT_REACHED"
        );
        assert_eq!(
            error.data,
            Some(serde_json::json!({"workflow_id": parent.to_string(), "task_index": 4}))
        );
        assert_eq!(
            error.message.as_deref(),
            Some(
                "Subworkflow not started: the child workflow would have depth 33, \
                 and the limit is 32 (workflow deep_parent:4)"
            )
        );
    }

    /// Only a sub-workflow root of the child that cannot start refuses the parent node. Every other
    /// failure of the engine code's child start is an error of the step, as Python raises it: an
    /// options merge that fails, for example (`_child_task_options_json`,
    /// `python/horsies/core/workflows/engine.py:425-475`). A `?` on a `WorkflowError` inside the start takes
    /// the second form.
    #[test]
    fn only_a_root_sub_workflow_that_cannot_start_refuses_the_parent_node() {
        use crate::workflow_engine::engine::{NodeRefusal, PreparationStop};

        let refused = PreparationStop::from(ChildStartStop::RootSubworkflowNotStarted(
            WorkflowError::Validation("depth limit".to_owned()),
        ));
        assert!(matches!(
            refused,
            PreparationStop::Refused(NodeRefusal::NotPrepared(ref message))
                if message == "depth limit"
        ));

        let raised = PreparationStop::from(ChildStartStop::Failed(WorkflowError::Validation(
            "options merge refused".to_owned(),
        )));
        assert!(matches!(
            raised,
            PreparationStop::Failed(WorkflowError::Validation(ref message))
                if message == "options merge refused"
        ));

        assert!(matches!(
            ChildStartStop::from(WorkflowError::Validation("unresolved queue".to_owned())),
            ChildStartStop::Failed(WorkflowError::Validation(_))
        ));
    }

    /// A builder error takes the Python class of its error category; an error with no code takes
    /// `HorsiesError` (Python `type(exc).__name__`, `core/workflows/engine.py:1196-1211`).
    #[test]
    fn a_builder_error_takes_the_python_class_of_its_category() {
        use crate::core::ErrorCode;

        for (error, class) in [
            (HorsiesError::new("no code"), "HorsiesError"),
            (
                HorsiesError::new("cycle").with_code(ErrorCode::WorkflowCycleDetected),
                "WorkflowValidationError",
            ),
            (
                HorsiesError::new("options").with_code(ErrorCode::TaskInvalidOptions),
                "TaskDefinitionError",
            ),
            (
                HorsiesError::new("queue mode").with_code(ErrorCode::ConfigInvalidQueueMode),
                "ConfigurationError",
            ),
            (
                HorsiesError::new("not registered").with_code(ErrorCode::TaskNotRegistered),
                "RegistryError",
            ),
        ] {
            let failure = ChildSpecError::of_builder(&error);
            assert_eq!(failure.error_type.as_str(), class, "{error}");
            assert_eq!(failure.text, error.to_string());
        }
    }

    /// A failing builder is a `ChildSpecError` with the class and text of its error; a path that refuses
    /// the step reads it as one validation message.
    #[test]
    fn a_failing_builder_is_a_child_spec_error() {
        let builder: crate::core::SpecBuilderFn =
            Arc::new(|_input: &str| -> Result<WorkflowSpec, SpecBuildError> {
                Err(SpecBuildError::Failed(
                    HorsiesError::new("the builder refuses")
                        .with_code(crate::core::ErrorCode::WorkflowCycleDetected),
                ))
            });
        let mut registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let mut registered = child_registered_static();
        registered.spec_builder = Some(builder);
        registry.register(registered).unwrap();
        let resolved = registry
            .resolve_child_registration("tests.child_static.v1")
            .expect("the child is registered");

        let ChildSpecFailure::BuildFailed(failure) =
            materialize_child_spec(resolved, SubWorkflowParams::Given, "{}", &registry)
                .unwrap_err()
        else {
            panic!("a failing builder is a build failure");
        };

        assert_eq!(
            failure.error_type,
            ChildSpecErrorType::WorkflowValidationError
        );
        let expected_text = HorsiesError::new("the builder refuses")
            .with_code(crate::core::ErrorCode::WorkflowCycleDetected)
            .to_string();
        assert_eq!(failure.text, expected_text);
        match WorkflowError::from(failure) {
            WorkflowError::Validation(message) => {
                assert_eq!(message, format!("WorkflowValidationError: {expected_text}"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    /// A builder whose decode refuses one input member is a member refusal, not a build failure; a
    /// path that refuses the step reads it as one validation message.
    #[test]
    fn a_builder_s_member_refusal_is_not_a_build_failure() {
        let builder: crate::core::SpecBuilderFn =
            Arc::new(|_input: &str| -> Result<WorkflowSpec, SpecBuildError> {
                Err(SpecBuildError::MemberRefused {
                    key: "count".to_owned(),
                    cause: "count: invalid type".to_owned(),
                })
            });
        let mut registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let mut registered = child_registered_static();
        registered.spec_builder = Some(builder);
        registry.register(registered).unwrap();
        let resolved = registry
            .resolve_child_registration("tests.child_static.v1")
            .expect("the child is registered");

        let failure = materialize_child_spec(resolved, SubWorkflowParams::Given, "{}", &registry)
            .unwrap_err();

        assert_eq!(
            failure,
            ChildSpecFailure::MemberRefused {
                key: "count".to_owned(),
                cause: "count: invalid type".to_owned(),
            }
        );
        match WorkflowError::from(failure) {
            WorkflowError::Validation(message) => assert_eq!(
                message,
                "the child's params member 'count' does not decode: count: invalid type"
            ),
            other => panic!("unexpected error: {other}"),
        }
    }

    /// Params given to a child registered without a builder: the `RuntimeError`
    /// text names the builder hook of the client that built the registry.
    /// Python's text names `build_with()` (`core/workflows/engine.py:1185-1189`).
    #[test]
    fn the_params_refusal_names_the_builder_hook_of_the_client() {
        for hook in [
            crate::core::registry::BuilderHookName::SPEC_BUILDER,
            crate::core::registry::BuilderHookName::new("build_with()"),
        ] {
            let mut registry = WorkflowSpecRegistry::new(hook);
            registry.register(child_registered_static()).unwrap();
            let resolved = registry
                .resolve_child_registration("tests.child_static.v1")
                .expect("the child is registered");

            let failure =
                materialize_child_spec(resolved, SubWorkflowParams::Given, "{}", &registry)
                    .unwrap_err();

            assert_eq!(
                failure,
                ChildSpecFailure::BuildFailed(ChildSpecError {
                    error_type: ChildSpecErrorType::RuntimeError,
                    text: format!(
                        "subworkflow 'child_static' received runtime parameters but has no {} \
                         that accepts them",
                        hook.as_str(),
                    ),
                })
            );
        }
    }

    /// A merged input with an `args_from` entry that has no `inner` is a refused stored input: the
    /// builder input is not made, and the node fails with `WORKFLOW_ENQUEUE_FAILED` and the refusal.
    #[test]
    fn a_stored_input_with_a_malformed_entry_is_a_refused_stored_input() {
        let input = r#"{"value": {"__h_taskresult_envelope__": true, "source_task_name": "t"}}"#;
        let args_from = serde_json::json!({"value": 0});

        let refused = crate::workflow_engine::args_merge::child_build_input(
            input.to_owned(),
            Some(&args_from),
        )
        .expect_err("a wrap without inner is refused");
        let failure = ChildSpecError::of_stored_input(refused);

        assert_eq!(failure.error_type, ChildSpecErrorType::StoredInputRefused);
        assert_eq!(
            failure.text,
            "args_from envelope for kwarg 'value' has no inner result"
        );
        let workflow_id = Uuid::new_v4();
        let error = crate::workflow_engine::start_statement::build_failed_error(
            workflow_id,
            "parent_workflow",
            3,
            &failure,
        );
        assert_eq!(error.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
        assert_eq!(
            error.message.as_deref(),
            Some(
                "Subworkflow build failed: stored node input refused: args_from envelope for \
                 kwarg 'value' has no inner result (workflow parent_workflow:3)"
            )
        );
        assert_eq!(
            error.data,
            Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 3}))
        );
    }

    /// A start through a handle of the other client family is refused before its first write:
    /// `ENQUEUE_FAILED`, not retryable, and no workflow row.
    #[tokio::test]
    #[serial]
    async fn a_start_of_another_client_family_is_refused_and_writes_nothing() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let broker = Arc::new(
            PostgresBroker::from_pool(pool.clone())
                .with_client_family(crate::broker::client_family::ClientFamily::Python),
        );
        let workflow_id = Uuid::new_v4();
        let spec = keyed_spec(
            Some("tests.family.start.v1"),
            vec![node_with(false, Some(17))],
        );

        let refused = start_workflow::<serde_json::Value>(
            &broker,
            &spec,
            Some(workflow_id),
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect_err("a start of another client family");

        assert_eq!(refused.code, WorkflowStartErrorCode::EnqueueFailed);
        assert!(!refused.retryable);
        assert!(
            refused.message.contains("its own family"),
            "{}",
            refused.message
        );
        let written: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(written, 0);
    }

    fn keyed_spec(definition_key: Option<&str>, tasks: Vec<AnyNode>) -> WorkflowSpec {
        WorkflowSpec {
            name: "keyed".to_owned(),
            definition_key: definition_key.map(str::to_owned),
            tasks,
            on_error: OnError::Fail,
            output_index: None,
            success_policy: None,
        }
    }

    #[test]
    fn a_spec_with_no_definition_key_is_refused() {
        let task = node_with(false, Some(10));
        for key in [None, Some(""), Some("   ")] {
            let message = require_definition_key(&keyed_spec(key, vec![task.clone()])).unwrap_err();
            assert!(message.contains("missing definition_key"), "{message}");
        }
        assert!(require_definition_key(&keyed_spec(Some("tests.keyed.v1"), vec![task])).is_ok());
    }

    #[test]
    fn a_subworkflow_node_with_no_definition_key_is_refused() {
        let unkeyed = node_with(true, None);
        let message =
            require_definition_key(&keyed_spec(Some("tests.keyed.v1"), vec![unkeyed.clone()]))
                .unwrap_err();
        assert!(
            message.contains("has no definition_key of its child workflow"),
            "{message}"
        );
        let mut keyed = unkeyed;
        keyed.sub_definition_key = Some("tests.child.v1".to_owned());
        assert!(require_definition_key(&keyed_spec(Some("tests.keyed.v1"), vec![keyed])).is_ok());
    }

    /// Python refuses a node whose stored retry policy does not validate, and the start's transaction rolls
    /// back (`python/horsies/core/workflows/lifecycle.py:152-203`, `root_retry_and_deadline`; the reader is
    /// `stored_max_retries`, `python/horsies/core/codec/task_options.py:80-103`). Each options text here is a JSON
    /// object, so `prepare_workflow_task_facts` accepts it; only the retry limit refuses it. A reader that
    /// guesses a limit (3 for a policy without an integer `max_retries`) lets the start write its rows, and
    /// passes 21 on to the baseline CHECK (`max_retries BETWEEN 0 AND 20`) as a database error.
    #[tokio::test]
    #[serial]
    async fn a_node_whose_retry_policy_does_not_fit_is_refused_before_any_row() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let task_name = "start_refused_retry_policy_node";
        let exponential = |max_retries: &str| {
            format!(
                r#"{{"retry_policy": {{"max_retries": {max_retries}, "intervals": [60],
                    "backoff_strategy": "exponential", "jitter": true, "auto_retry_for": ["X"]}}}}"#
            )
        };
        let cases = [
            (
                "no max_retries",
                r#"{"retry_policy": {"backoff_strategy": "exponential"}}"#.to_owned(),
            ),
            ("max_retries is text", exponential("\"5\"")),
            ("max_retries above 20", exponential("21")),
            ("policy is a number", r#"{"retry_policy": 5}"#.to_owned()),
        ];
        for (case, options) in cases {
            let mut root = node_with(false, Some(17));
            root.task_name = task_name.to_owned();
            root.task_options_json = Some(options);
            let spec = keyed_spec(Some("tests.start.refused_retry_policy.v1"), vec![root]);
            let workflow_id = Uuid::new_v4();

            let started = start_workflow::<serde_json::Value>(
                &broker,
                &spec,
                Some(workflow_id),
                &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
                &PayloadPolicy::default(),
                &RetentionConfig::default(),
            )
            .await;

            let refused = match started {
                Ok(_) => panic!("{case}: the start must be refused"),
                Err(refused) => refused,
            };
            assert_eq!(
                refused.code,
                WorkflowStartErrorCode::ValidationFailed,
                "{case}"
            );
            assert!(!refused.retryable, "{case}");
            assert!(
                refused.message.contains(&format!(
                    "task_options of workflow node 0 ({task_name}) refused"
                )),
                "{case}: {}",
                refused.message
            );
            let rows: (i64, i64, i64) = sqlx::query_as(
                "SELECT (SELECT count(*) FROM horsies_workflows WHERE id = $1),
                        (SELECT count(*) FROM horsies_workflow_tasks WHERE workflow_id = $1),
                        (SELECT count(*) FROM horsies_tasks WHERE task_name = $2)",
            )
            .bind(workflow_id)
            .bind(task_name)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(rows, (0, 0, 0), "{case}");
        }
    }

    /// Python raises when a node's options are not a JSON object and the start rolls back
    /// (`python/horsies/core/workflows/lifecycle.py:576-604`); a deadline the database cannot read fails the
    /// insert. The core refuses both before any row: options that do not read are not replaced when a
    /// deadline is merged in, and a stored `good_until` that is not an RFC 3339 text is not read as no
    /// deadline.
    #[tokio::test]
    #[serial]
    async fn a_node_whose_options_or_deadline_do_not_read_is_refused_before_any_row() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let task_name = "start_refused_deadline_node";
        let deadline = chrono::Utc::now() + chrono::Duration::hours(1);
        let cases = [
            (
                "options are a list, with a node deadline",
                Some(deadline),
                "[1]",
            ),
            (
                "good_until is not a time",
                None,
                r#"{"good_until": "not-a-date"}"#,
            ),
            ("good_until is a number", None, r#"{"good_until": 12345}"#),
        ];
        for (case, good_until, options) in cases {
            let mut root = node_with(false, Some(17));
            root.task_name = task_name.to_owned();
            root.good_until = good_until;
            root.task_options_json = Some(options.to_owned());
            let spec = keyed_spec(Some("tests.start.refused_deadline.v1"), vec![root]);
            let workflow_id = Uuid::new_v4();

            let started = start_workflow::<serde_json::Value>(
                &broker,
                &spec,
                Some(workflow_id),
                &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
                &PayloadPolicy::default(),
                &RetentionConfig::default(),
            )
            .await;

            let refused = match started {
                Ok(_) => panic!("{case}: the start must be refused"),
                Err(refused) => refused,
            };
            assert_eq!(
                refused.code,
                WorkflowStartErrorCode::ValidationFailed,
                "{case}"
            );
            assert!(!refused.retryable, "{case}");
            assert!(
                refused.message.contains(&format!(
                    "task_options of workflow node 0 ({task_name}) refused"
                )),
                "{case}: {}",
                refused.message
            );
            let rows: (i64, i64, i64) = sqlx::query_as(
                "SELECT (SELECT count(*) FROM horsies_workflows WHERE id = $1),
                        (SELECT count(*) FROM horsies_workflow_tasks WHERE workflow_id = $1),
                        (SELECT count(*) FROM horsies_tasks WHERE task_name = $2)",
            )
            .bind(workflow_id)
            .bind(task_name)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(rows, (0, 0, 0), "{case}");
        }
    }

    /// A node whose retry policy fits stores its `max_retries` on the node row and on its task row, from
    /// the same reader as a task row (Python `stored_max_retries`).
    #[tokio::test]
    #[serial]
    async fn a_node_whose_retry_policy_fits_stores_its_limit_on_both_rows() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let mut root = node_with(false, Some(17));
        root.task_name = "start_fitting_retry_policy_node".to_owned();
        root.task_options_json = Some(
            r#"{"retry_policy": {"max_retries": 4, "intervals": [60],
                "backoff_strategy": "exponential", "jitter": true, "auto_retry_for": ["X"]}}"#
                .to_owned(),
        );
        let spec = keyed_spec(Some("tests.start.fitting_retry_policy.v1"), vec![root]);
        let workflow_id = Uuid::new_v4();

        let _: WorkflowHandle<serde_json::Value> = start_workflow(
            &broker,
            &spec,
            Some(workflow_id),
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("start a workflow whose node policy fits");

        let limits: (i32, i32) = sqlx::query_as(
            "SELECT wt.max_retries, t.max_retries
             FROM horsies_workflow_tasks wt
             JOIN horsies_tasks t ON t.id = wt.task_id
             WHERE wt.workflow_id = $1 AND wt.task_index = 0",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query(
            "DELETE FROM horsies_tasks WHERE id IN (
                SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1
             )",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
        assert_eq!(limits, (4, 4));
    }

    /// A root node that the start enqueues holds its enqueue time as `started_at`: a root that names no
    /// context source and a root that names one. The start statement writes both node rows and their task
    /// rows, so the time equals `enqueued_at` of the node's task row.
    #[tokio::test]
    #[serial]
    async fn an_enqueued_root_node_holds_its_enqueue_time() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let mut fast = node_with(false, Some(17));
        fast.task_name = "enqueue_time_fast_root".to_owned();
        fast.node_id = Some("fast".to_owned());
        let mut slow = node_with(false, Some(17));
        slow.task_name = "enqueue_time_slow_root".to_owned();
        slow.node_id = Some("slow".to_owned());
        slow.index = 1;
        // A root that names a context source takes the per-row slow path.
        slow.workflow_ctx_from = Some(vec!["fast".to_owned()]);
        let spec = keyed_spec(Some("tests.enqueue_time.root.v1"), vec![fast, slow]);
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let workflow_id = Uuid::new_v4();

        let _: WorkflowHandle<serde_json::Value> = start_workflow(
            &broker,
            &spec,
            Some(workflow_id),
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("start workflow with a fast and a slow root");

        let nodes: Vec<(i32, String, bool)> = sqlx::query_as(
            "SELECT wt.task_index, wt.status,
                    wt.started_at IS NOT NULL AND wt.started_at = t.enqueued_at
             FROM horsies_workflow_tasks wt
             JOIN horsies_tasks t ON t.id = wt.task_id
             WHERE wt.workflow_id = $1
             ORDER BY wt.task_index",
        )
        .bind(workflow_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            nodes,
            vec![
                (0, "ENQUEUED".to_owned(), true),
                (1, "ENQUEUED".to_owned(), true),
            ]
        );

        sqlx::query(
            "DELETE FROM horsies_tasks WHERE id IN (
                SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1
             )",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[test]
    fn an_existing_id_is_the_workflow_by_its_name_or_a_conflict() {
        assert_eq!(
            existing_workflow(Some("first_name".to_owned()), "first_name"),
            WorkflowRowInsert::Existing
        );
        assert_eq!(
            existing_workflow(Some("first_name".to_owned()), "other_name"),
            WorkflowRowInsert::IdConflict {
                existing_name: "first_name".to_owned()
            }
        );
        // The row was gone by the name read.
        assert_eq!(
            existing_workflow(None, "other_name"),
            WorkflowRowInsert::Existing
        );
    }

    #[tokio::test]
    #[serial]
    async fn an_id_of_another_workflow_name_is_a_conflict_and_writes_nothing() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let retention = RetentionConfig::default();
        let workflow_id = Uuid::new_v4();
        let sent_at = chrono::Utc::now() - chrono::Duration::minutes(5);
        let mut first = keyed_spec(
            Some("tests.start.conflict.v1"),
            vec![node_with(false, Some(100))],
        );
        first.name = "first_name".to_owned();
        let mut other = first.clone();
        other.name = "other_name".to_owned();

        for (spec, expected) in [
            (&first, WorkflowRowInsert::Inserted),
            (&first, WorkflowRowInsert::Existing),
            (
                &other,
                WorkflowRowInsert::IdConflict {
                    existing_name: "first_name".to_owned(),
                },
            ),
        ] {
            assert_eq!(
                start_workflow_inner(&broker, spec, workflow_id, sent_at, &registry, &retention)
                    .await
                    .expect("start"),
                expected
            );
        }

        let (name, stored_sent_at): (String, chrono::DateTime<chrono::Utc>) =
            sqlx::query_as("SELECT name, sent_at FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(name, "first_name");
        // The caller's start time is stored (microsecond precision), not the database clock.
        assert_eq!(
            stored_sent_at.timestamp_micros(),
            sent_at.timestamp_micros()
        );
        let node_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM horsies_workflow_tasks WHERE workflow_id = $1",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(node_rows, 1, "the repeated starts wrote no node row");
    }

    #[tokio::test]
    #[serial]
    async fn p6_workflow_enqueue_start_paths_persist_mapped_class_and_never_eligible_facts() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let workflow_name = "p6_workflow_facts";
        let task_name = "p6_workflow_node_facts";
        sqlx::query("DELETE FROM horsies_workflows WHERE name = $1")
            .bind(workflow_name)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(&pool)
            .await
            .unwrap();

        let mut root = node_with(false, Some(17));
        root.task_name = task_name.to_owned();
        root.queue = Some("bulk".to_owned());
        root.input_json = r#"{"value": 1}"#.to_owned();
        let spec = WorkflowSpec {
            name: workflow_name.to_owned(),
            definition_key: Some("p6.workflow.facts.v1".to_owned()),
            tasks: vec![root.clone()],
            on_error: OnError::Fail,
            output_index: Some(0),
            success_policy: None,
        };
        let mut retention = RetentionConfig::default();
        retention
            .queue_retention
            .insert("bulk".to_owned(), Some(chrono::Duration::days(7)));
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let workflow_id = Uuid::new_v4();

        let _: WorkflowHandle<serde_json::Value> = start_workflow(
            &broker,
            &spec,
            Some(workflow_id),
            &registry,
            &PayloadPolicy::default(),
            &retention,
        )
        .await
        .expect("start workflow with fast-path root");
        let slow_id = enqueue_root_task(
            &pool,
            workflow_id,
            &root,
            "bulk",
            17,
            None,
            &retention,
            chrono::Utc::now(),
        )
        .await
        .expect("enqueue slow-path workflow root");

        let rows: Vec<(String, String, Option<i32>, Option<i16>, bool)> = sqlx::query_as(
            "SELECT id::text, retention_class_key,
                    octet_length(command_fingerprint), command_fingerprint_version,
                    retain_rerun_input
             FROM horsies_tasks WHERE task_name = $1 ORDER BY id",
        )
        .bind(task_name)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| row.0 == slow_id.to_string()));
        for row in rows {
            assert_eq!(row.1, "standard_7d");
            assert!(row.2.is_none());
            assert!(row.3.is_none());
            assert!(!row.4);
            assert_eq!(Uuid::parse_str(&row.0).unwrap().get_version_num(), 7);
        }
    }

    /// A root task row stores the start call time of its workflow as `sent_at`, on the fast path (bulk
    /// insert) and on the slow path (`enqueue_root_task`), not the time the engine built the row.
    #[tokio::test]
    #[serial]
    async fn a_root_task_row_stores_the_start_call_time_of_its_workflow() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let task_name = "sent_at_root_task";
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(&pool)
            .await
            .unwrap();
        let mut root = node_with(false, Some(17));
        root.task_name = task_name.to_owned();
        let spec = keyed_spec(Some("tests.sent_at.root.v1"), vec![root.clone()]);
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let workflow_id = Uuid::new_v4();
        let started = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("fixed start call time")
            .with_timezone(&chrono::Utc);

        let _: WorkflowHandle<serde_json::Value> = start_workflow_attempt(
            &broker,
            &spec,
            workflow_id,
            started,
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("start workflow with a fast-path root");
        let slow_id = enqueue_root_task(
            &pool,
            workflow_id,
            &root,
            "default",
            17,
            None,
            &RetentionConfig::default(),
            started,
        )
        .await
        .expect("enqueue a slow-path workflow root");

        let stored: Vec<(Uuid, chrono::DateTime<chrono::Utc>)> =
            sqlx::query_as("SELECT id, sent_at FROM horsies_tasks WHERE task_name = $1")
                .bind(task_name)
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(stored.len(), 2, "{stored:?}");
        assert!(stored.iter().any(|(id, _)| *id == slow_id));
        for (id, sent_at) in &stored {
            assert_eq!(*sent_at, started, "task {id}");
        }

        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// The child start of the engine code (recovery case 1.5, the engine promotion of a
    /// sub-workflow node) writes the workflow meta into the input of each root task of the child,
    /// on the fast path (bulk insert) and on the slow path (a root that names a context source), as
    /// Python does (`python/horsies/core/workflows/engine.py:1447-1453`) and as the baseline mint does
    /// (`horsies_workflow_task_input`). The node rows keep the node input, and a node defined with
    /// an empty context list stores NULL. The test's transaction rolls back.
    #[tokio::test]
    #[serial]
    async fn the_engine_child_start_writes_the_workflow_meta_into_each_root_task() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let parent_id = Uuid::new_v4();
        let mut transaction = pool.begin().await.unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, definition_key, depth,
                 root_workflow_id, sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'engine_child_meta_parent', 'RUNNING', 'fail',
                       'tests.engine_child_meta.parent.v1', 0, $1, NOW(), NOW(), NOW(), NOW())",
        )
        .bind(parent_id)
        .execute(&mut *transaction)
        .await
        .unwrap();
        let mut fast = node_with(false, Some(17));
        fast.task_name = "engine_child_meta_fast".to_owned();
        fast.node_id = Some("fast".to_owned());
        fast.input_json = r#"{"value": 1}"#.to_owned();
        fast.workflow_ctx_from = Some(Vec::new());
        let mut slow = node_with(false, Some(17));
        slow.task_name = "engine_child_meta_slow".to_owned();
        slow.node_id = Some("slow".to_owned());
        slow.index = 1;
        // A root that names a context source takes the per-row slow path.
        slow.workflow_ctx_from = Some(vec!["fast".to_owned()]);
        let child = keyed_spec(Some("tests.engine_child_meta.child.v1"), vec![fast, slow]);

        let child_id = start_child_workflow_in_tx(
            &mut transaction,
            &child,
            parent_id,
            0,
            1,
            parent_id,
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &RetentionConfig::default(),
        )
        .await
        .expect("the child starts");

        let rows: Vec<(i32, String, String, Option<Vec<String>>, String)> = sqlx::query_as(
            "SELECT wt.task_index, wt.task_name, wt.input, wt.workflow_ctx_from::text[], t.input
             FROM horsies_workflow_tasks wt
             JOIN horsies_tasks t ON t.id = wt.task_id
             WHERE wt.workflow_id = $1
             ORDER BY wt.task_index",
        )
        .bind(child_id)
        .fetch_all(&mut *transaction)
        .await
        .unwrap();
        assert_eq!(rows.len(), 2, "{rows:?}");
        for (task_index, task_name, node_input, _, task_input) in &rows {
            let task_input: serde_json::Value = serde_json::from_str(task_input).unwrap();
            assert_eq!(
                task_input[WORKFLOW_META_KWARG],
                serde_json::json!({
                    "workflow_id": child_id.to_string(),
                    "task_index": task_index,
                    "task_name": task_name,
                }),
                "{task_name}"
            );
            let node_input: serde_json::Value = serde_json::from_str(node_input).unwrap();
            assert!(
                node_input.get(WORKFLOW_META_KWARG).is_none(),
                "{task_name}: {node_input}"
            );
        }
        let (_, _, _, fast_context, fast_task_input) = &rows[0];
        assert_eq!(*fast_context, None);
        let fast_task_input: serde_json::Value = serde_json::from_str(fast_task_input).unwrap();
        assert_eq!(fast_task_input["value"], serde_json::json!(1));
        assert_eq!(rows[1].3, Some(vec!["fast".to_owned()]));

        transaction.rollback().await.unwrap();
    }

    #[test]
    fn build_child_spec_passes_explicit_input_to_dynamic_definition() {
        let registered = RegisteredWorkflowDefinition {
            name: "child_dynamic".to_owned(),
            definition_key: "tests.child_dynamic.v1".to_owned(),
            declared_children: vec![],
            spec_builder: Arc::new(|input_json| {
                let params = serde_json::from_str::<serde_json::Value>(input_json)
                    .map_err(|e| SpecBuildError::Failed(crate::HorsiesError::new(e.to_string())))?;
                let region = params["region"].as_str().unwrap().to_owned();

                let mut builder = WorkflowSpecBuilder::new("child_dynamic");
                let node = builder.task(
                    TaskNode::<String>::raw("hello_task", "null")
                        .queue("default")
                        .input_json(serde_json::to_string(&region).unwrap()),
                );
                builder.output(node);
                builder.build().map_err(SpecBuildError::Failed)
            }),
        };

        let parent = SubWorkflowNode::<serde_json::Value, String>::typed(
            "child_dynamic",
            "null",
            crate::core::workflow::node::SubWorkflowParams::NotGiven,
        )
        .definition_key("tests.child_dynamic.v1")
        .input_json(r#"{"region":"eu"}"#)
        .into_any_node(0);

        let mut registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        registry.register_definition(registered).unwrap();
        let child = build_child_spec(
            "child_dynamic",
            parent.sub_definition_key.as_deref(),
            &parent,
            &registry,
        )
        .unwrap();
        assert_eq!(child.tasks[0].input_json, "\"eu\"");
    }

    #[test]
    fn build_child_spec_follows_the_params_choice() {
        // A child without a spec_builder takes a `NotGiven` node with any
        // text and refuses a `Given` node, even when its text is `null`.
        for (input, params, accepted) in [
            ("null", SubWorkflowParams::NotGiven, true),
            ("{}", SubWorkflowParams::NotGiven, true),
            ("null", SubWorkflowParams::Given, false),
        ] {
            let parent = SubWorkflowNode::<(), String>::typed("child_static", input, params)
                .definition_key("tests.child_static.v1")
                .into_any_node(0);

            let mut registry =
                WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
            registry.register(child_registered_static()).unwrap();
            let child = build_child_spec(
                "child_static",
                parent.sub_definition_key.as_deref(),
                &parent,
                &registry,
            );
            match (child, accepted) {
                (Ok(spec), true) => assert_eq!(spec.name, "child_static"),
                (Err(WorkflowError::Validation(message)), false) => assert_eq!(
                    message,
                    "RuntimeError: subworkflow 'child_static' received runtime parameters \
                     but has no spec_builder that accepts them"
                ),
                (other, _) => panic!("{input} {params:?}: unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn a_sub_workflow_row_is_read_by_its_stored_choice_only() {
        assert_eq!(
            stored_sub_workflow_params(Some("NOT_GIVEN")).unwrap(),
            SubWorkflowParams::NotGiven
        );
        assert_eq!(
            stored_sub_workflow_params(Some("GIVEN")).unwrap(),
            SubWorkflowParams::Given
        );
        for refused in [None, Some(""), Some("given")] {
            assert!(
                matches!(
                    stored_sub_workflow_params(refused),
                    Err(WorkflowError::Validation(_))
                ),
                "{refused:?}"
            );
        }
    }

    #[test]
    fn build_child_spec_rejects_explicit_input_without_builder() {
        let registered = child_registered_static();
        let parent = SubWorkflowNode::<String, String>::typed(
            "child_static",
            "null",
            crate::core::workflow::node::SubWorkflowParams::NotGiven,
        )
        .definition_key("tests.child_static.v1")
        .set_input("eu".to_owned())
        .unwrap()
        .into_any_node(0);

        let mut registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        registry.register(registered).unwrap();
        let err = build_child_spec(
            "child_static",
            parent.sub_definition_key.as_deref(),
            &parent,
            &registry,
        )
        .unwrap_err();
        match err {
            WorkflowError::Validation(message) => assert_eq!(
                message,
                "RuntimeError: subworkflow 'child_static' received runtime parameters \
                 but has no spec_builder that accepts them"
            ),
            other => panic!("unexpected error: {}", other),
        }
    }
}

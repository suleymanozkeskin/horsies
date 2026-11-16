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
use crate::core::task::retry_utils::parse_max_retries;
use crate::core::workflow::node::SubWorkflowParams;
use crate::core::{
    AnyNode, OnError, OperationalErrorCode, TaskError, WorkflowSpec, WorkflowSpecRegistry,
    WorkflowStartError, WorkflowStartErrorCode, WorkflowStartResult,
};

use crate::db::CheckoutClass;
use crate::workflow_engine::bound_handle::WorkflowHandle;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::parse_good_until_from_options;
use crate::workflow_engine::start_statement::{start_arguments, start_tree, StartOutcome};
use crate::workflow_engine::steps::{advance_parents, ChildBuild};

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
const ENQUEUE_ROOT_TASK_SQL: &str = "\
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, input, task_options, execution,
    retention_class_key, sent_at, created_at,
    command_fingerprint_version, command_fingerprint, idempotency_key_digest,
    status, enqueued_at, retry_count, max_retries, good_until,
    is_workflow_task, updated_at,
    retain_rerun_input, rerun_of_task_id, rerun_root_task_id
)
VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW(), NULL, NULL, NULL,
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
SELECT depth, root_workflow_id
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
        max_retries: parse_max_retries(merged_task_options),
        good_until: parse_good_until_from_options(merged_task_options),
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

/// The depth of a new child of the workflow `parent_workflow_id` at `parent_depth`.
///
/// `Err` is the error that the sub-workflow node at `task_index` stores when the child would pass
/// [`MAX_SUBWORKFLOW_DEPTH`]: the child is not started, the node fails, and the parent's `on_error` applies
/// (Python `core/workflows/engine.py:1052-1066`).
pub(crate) fn child_workflow_depth(
    parent_depth: i32,
    parent_workflow_id: Uuid,
    task_index: i32,
) -> Result<i32, TaskError> {
    let child_depth = parent_depth + 1;
    match child_depth > MAX_SUBWORKFLOW_DEPTH {
        false => Ok(child_depth),
        true => Err(TaskError::builtin(
            OperationalErrorCode::SubworkflowDepthLimitReached,
            format!(
                "Subworkflow not started: the child workflow would have depth {child_depth}, \
                 and the limit is {MAX_SUBWORKFLOW_DEPTH} (workflow {parent_workflow_id}:{task_index})"
            ),
        )),
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
/// Python computes the same at start (`with_downstream_subworkflow`, `horsies/core/workflows/sql.py`),
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

/// Insert all workflow_task rows. Root tasks (no deps) are marked READY
/// and immediately enqueued into horsies_tasks. Root sub-workflow nodes
/// are detected and launched as child workflows.
///
/// Returns a boxed future because this function is mutually recursive with
/// `launch_root_subworkflow` → `start_child_workflow_in_tx`.
fn insert_workflow_tasks<'a>(
    tx: &'a mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: Uuid,
    tasks: &'a [AnyNode],
    registry: &'a WorkflowSpecRegistry,
    retention: &'a RetentionConfig,
) -> Pin<Box<dyn Future<Output = Result<(), WorkflowError>> + Send + 'a>> {
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
                task.good_until,
                task.task_options_json.as_deref(),
            );

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
            let has_ctx_from = task
                .workflow_ctx_from
                .as_ref()
                .is_some_and(|v| !v.is_empty());
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
                .push_bind(p.node.workflow_ctx_from.clone())
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
            let mut tq: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
                "INSERT INTO horsies_tasks (\
                 id, task_name, queue_name, priority, input, task_options, execution, \
                 retention_class_key, sent_at, created_at, \
                 command_fingerprint_version, command_fingerprint, idempotency_key_digest, \
                 status, enqueued_at, retry_count, max_retries, good_until, \
                 is_workflow_task, updated_at, \
                 retain_rerun_input, rerun_of_task_id, rerun_root_task_id) ",
            );
            tq.push_values(prepared.iter().filter(|p| p.fast_path), |mut b, p| {
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
                    .push_bind(p.node.input_json.clone())
                    .push_bind(p.merged_task_options.clone())
                    .push_bind(task_facts.facts.execution.as_str())
                    .push_bind(task_facts.facts.retention_class_key.clone());
                // sent_at, created_at, the fingerprint pair and the key digest,
                // status, enqueued_at, retry_count as literals — NOW() is
                // transaction-stable, so claim ordering matches the per-row shape.
                b.push("NOW()")
                    .push("NOW()")
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
                    p.node,
                    &p.queue,
                    p.priority,
                    p.merged_task_options.as_deref(),
                    retention,
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

/// Create a horsies_tasks row for a root workflow task.
/// Returns the task_id.
async fn enqueue_root_task(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    node: &AnyNode,
    queue: &str,
    priority: i32,
    merged_task_options: Option<&str>,
    retention: &RetentionConfig,
) -> Result<Uuid, WorkflowError> {
    let task_id = crate::core::history::identity::uuid7::mint_task_id().map_err(|error| {
        WorkflowError::Validation(format!("task identity mint failed: {error}"))
    })?;
    let task_facts = node_task_facts(node, queue, merged_task_options, retention)?;

    sqlx::query(ENQUEUE_ROOT_TASK_SQL)
        .bind(task_id)
        .bind(&node.task_name)
        .bind(queue)
        .bind(priority)
        .bind(node.input_json.as_str())
        .bind(merged_task_options)
        .bind(task_facts.facts.execution.as_str())
        .bind(&task_facts.facts.retention_class_key)
        .bind(task_facts.max_retries)
        .bind(task_facts.good_until)
        .execute(executor)
        .await?;

    Ok(task_id)
}

/// Launch a child workflow for a root sub-workflow node at start time.
///
/// Resolves the spec from the registry and starts the child workflow,
/// linking the parent workflow_task to it.
async fn launch_root_subworkflow(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: Uuid,
    task: &AnyNode,
    registry: &WorkflowSpecRegistry,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
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
    let child_depth = child_workflow_depth(parent_depth, workflow_id, task.index as i32)
        .map_err(|error| WorkflowError::Validation(error.to_string()))?;

    // Build the child spec (supports dynamic parameterization if a builder is registered).
    let child_spec = build_child_spec(
        spec_name,
        task.sub_definition_key.as_deref(),
        task,
        registry,
    )?;

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
/// Used by `launch_root_subworkflow` and `enqueue_subworkflow_task` to keep
/// child creation + parent link inside a single transaction.
pub async fn start_child_workflow_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    spec: &WorkflowSpec,
    parent_workflow_id: Uuid,
    parent_task_index: i32,
    depth: i32,
    root_workflow_id: Uuid,
    registry: &WorkflowSpecRegistry,
    retention: &RetentionConfig,
) -> Result<Uuid, WorkflowError> {
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
    insert_workflow_tasks(tx, child_id, &spec.tasks, registry, retention).await?;

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

    materialize_child_spec(
        resolved,
        parent_task.sub_workflow_params,
        &parent_task.input_json,
        registry,
    )
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

/// Build the child spec from the parent node's encoded input (`input_json`,
/// as the client encoded it). A child registered without a `spec_builder`
/// refuses a node that gives params (`SubWorkflowParams::Given`).
pub(crate) fn materialize_child_spec(
    resolved: crate::core::registry::workflow::ResolvedChildWorkflow<'_>,
    params: SubWorkflowParams,
    input_json: &str,
    registry: &WorkflowSpecRegistry,
) -> Result<WorkflowSpec, WorkflowError> {
    match resolved {
        crate::core::registry::workflow::ResolvedChildWorkflow::Dynamic(registered) => {
            let mut spec = (registered.spec_builder)(input_json).map_err(|e| {
                WorkflowError::Validation(format!(
                    "spec_builder for '{}' failed: {}",
                    registered.name, e,
                ))
            })?;
            registry.resolve_spec_task_options(&mut spec);
            registry
                .resolve_and_validate_spec(&mut spec)
                .map_err(|e| WorkflowError::Validation(e.to_string()))?;
            Ok(spec)
        }
        crate::core::registry::workflow::ResolvedChildWorkflow::Static(registered) => {
            match (&registered.spec_builder, params) {
                (Some(builder), _) => {
                    let mut spec = builder(input_json).map_err(|e| {
                        WorkflowError::Validation(format!(
                            "spec_builder for '{}' failed: {}",
                            registered.spec.name, e,
                        ))
                    })?;
                    registry.resolve_spec_task_options(&mut spec);
                    registry
                        .resolve_and_validate_spec(&mut spec)
                        .map_err(|e| WorkflowError::Validation(e.to_string()))?;
                    Ok(spec)
                }
                (None, SubWorkflowParams::Given) => Err(WorkflowError::Validation(format!(
                    "child workflow '{}' received params from its parent, but the registered child has no spec_builder",
                    registered.spec.name,
                ))),
                (None, SubWorkflowParams::NotGiven) => Ok(registered.spec.clone()),
            }
        }
    }
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

    fn node_with(is_subworkflow: bool, priority: Option<i32>) -> AnyNode {
        AnyNode {
            task_name: "t".to_owned(),
            input_json: "null".to_owned(),
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
            &WorkflowSpecRegistry::new(),
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

    #[test]
    fn child_depth_is_bounded_at_32() {
        let parent = Uuid::new_v4();
        assert_eq!(child_workflow_depth(0, parent, 1).unwrap(), 1);
        assert_eq!(child_workflow_depth(31, parent, 1).unwrap(), 32);
        let error = child_workflow_depth(32, parent, 4).unwrap_err();
        assert_eq!(
            error.error_code.to_string(),
            "SUBWORKFLOW_DEPTH_LIMIT_REACHED"
        );
        let message = error.message.unwrap();
        assert!(message.contains("depth 33"));
        assert!(message.contains(&format!("{parent}:4")));
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
            &WorkflowSpecRegistry::new(),
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

    /// A root node that the start enqueues holds its enqueue time as `started_at`: a root with no
    /// context source and a root that names one. `horsies_workflow_start` writes the node and mints its
    /// task row in one statement, so the time equals `enqueued_at` of the node's task row.
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
        slow.input_json = "{}".to_owned();
        let spec = keyed_spec(Some("tests.enqueue_time.root.v1"), vec![fast, slow]);
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let workflow_id = Uuid::new_v4();

        let _: WorkflowHandle<serde_json::Value> = start_workflow(
            &broker,
            &spec,
            Some(workflow_id),
            &WorkflowSpecRegistry::new(),
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
        let registry = WorkflowSpecRegistry::new();
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
        root.input_json = "[1]".to_owned();
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
        let registry = WorkflowSpecRegistry::new();
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
        let slow_id = enqueue_root_task(&pool, &root, "bulk", 17, None, &retention)
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

    #[test]
    fn build_child_spec_passes_explicit_input_to_dynamic_definition() {
        let registered = RegisteredWorkflowDefinition {
            name: "child_dynamic".to_owned(),
            definition_key: "tests.child_dynamic.v1".to_owned(),
            declared_children: vec![],
            spec_builder: Arc::new(|input_json| {
                let params = serde_json::from_str::<serde_json::Value>(input_json)
                    .map_err(|e| crate::HorsiesError::new(e.to_string()))?;
                let region = params["region"].as_str().unwrap().to_owned();

                let mut builder = WorkflowSpecBuilder::new("child_dynamic");
                let node = builder.task(
                    TaskNode::<String>::raw("hello_task", "null")
                        .queue("default")
                        .input_json(serde_json::to_string(&region).unwrap()),
                );
                builder.output(node);
                builder.build()
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

        let mut registry = WorkflowSpecRegistry::new();
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

            let mut registry = WorkflowSpecRegistry::new();
            registry.register(child_registered_static()).unwrap();
            let child = build_child_spec(
                "child_static",
                parent.sub_definition_key.as_deref(),
                &parent,
                &registry,
            );
            match (child, accepted) {
                (Ok(spec), true) => assert_eq!(spec.name, "child_static"),
                (Err(WorkflowError::Validation(message)), false) => {
                    assert!(message.contains("has no spec_builder"), "{message}")
                }
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

        let mut registry = WorkflowSpecRegistry::new();
        registry.register(registered).unwrap();
        let err = build_child_spec(
            "child_static",
            parent.sub_definition_key.as_deref(),
            &parent,
            &registry,
        )
        .unwrap_err();
        match err {
            WorkflowError::Validation(message) => {
                assert!(message.contains("received params from its parent"));
                assert!(message.contains("has no spec_builder"));
            }
            other => panic!("unexpected error: {}", other),
        }
    }
}

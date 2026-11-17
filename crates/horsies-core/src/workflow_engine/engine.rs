use std::collections::HashMap;

use sqlx::PgPool;
use uuid::Uuid;

use crate::core::config::payload::{enforce_payload_policy, PayloadKind, PayloadPolicy};
use crate::core::config::retention::RetentionConfig;
use crate::core::history::enqueue::{prepare_workflow_task_facts, WorkflowTaskFacts};
use crate::core::workflow::context::WORKFLOW_CTX_KWARG;
use crate::core::{
    JoinType, OperationalErrorCode, OutcomeCode, SubWorkflowFailure, SubWorkflowSummary,
    SuccessPolicy, TaskError, TaskErrorKind, TaskResult, WorkflowSpecRegistry, WorkflowTaskStatus,
};

use crate::workflow_engine::args_merge::child_build_input;
use crate::workflow_engine::start::{
    child_workflow_depth, materialize_child_spec, start_child_workflow_in_tx,
    stored_sub_workflow_params, ChildSpecError, ChildSpecFailure, ChildStartStop,
};
use crate::workflow_engine::start_statement::{
    child_spec_failure_text, params_refused_error, RefusedMember,
};

use crate::broker::PostgresBroker;
use crate::db::CheckoutClass;
use crate::workflow_engine::error::WorkflowError;
pub use crate::workflow_engine::steps::SourceFailure;
use crate::workflow_engine::steps::{
    advance_parents, node_finish_in_transaction, ChildBuild, NodeFinishDisposition,
};

#[cfg(test)]
fn test_uuid(value: &str) -> Uuid {
    Uuid::parse_str(value).expect("test identity must be UUID")
}

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

// Lock order: every transaction that locks both `horsies_workflows` and
// `horsies_workflow_tasks` rows locks the workflow row first, as phase 2
// (`horsies_phase2_consume`) does, or it can deadlock against phase 2. The
// cancel transaction (`LOCK_WORKFLOW_ROW_FOR_CANCEL_SQL`) is held to the same
// order.

/// The node error lives in `result` only; the node `error` column has no writer (Python
/// `MARK_WORKFLOW_TASK_FAILED_SQL`, `core/workflows/sql.py:253-259`).
///
/// Every caller fails a node at its enqueue step, so a node with no `started_at` takes its enqueue time:
/// Python marks the node ENQUEUED (with `started_at`) before it fails it (`core/workflows/sql.py:114-116,
/// 238-240, 449-470`; `core/workflows/engine.py:1030-1035`).
const UPDATE_WORKFLOW_TASK_FAILED_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = 'FAILED', result = $1, completed_at = NOW(),
    started_at = COALESCE(started_at, NOW())
WHERE workflow_id = $2 AND task_index = $3
  AND status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
RETURNING id";

/// Batched dependent evaluation: one row per PENDING dependent of ANY source
/// index (the just-terminal nodes of this cascade level), carrying the full node
/// config plus its dependency-status counts as jsonb. Replaces the per-dependent
/// FIND + DEP_STATUS_COUNTS pair so a fan-out promotion is O(1) round trips
/// instead of O(F). The `w.status = 'RUNNING'` join folds in the workflow guard:
/// zero rows when paused/cancelled, same outcome as the per-node path.
const GET_DEPENDENTS_EVAL_SQL: &str = "\
SELECT d.task_index, d.dependencies, d.args_from, d.workflow_ctx_from,
       d.allow_failed_deps, d.join_type, d.min_success, d.task_name,
       d.input, d.queue_name, d.priority,
       d.node_id, d.task_options, d.status,
       d.is_subworkflow, d.sub_workflow_name, d.sub_definition_key,
       d.sub_workflow_params,
       COALESCE((
           SELECT jsonb_object_agg(s.status, s.cnt)
           FROM (SELECT src.status, COUNT(*) AS cnt
                 FROM horsies_workflow_tasks src
                 WHERE src.workflow_id = d.workflow_id
                   AND src.task_index = ANY(d.dependencies)
                 GROUP BY src.status) s
       ), '{}'::jsonb) AS dep_status_counts
FROM horsies_workflow_tasks d
JOIN horsies_workflows w ON w.id = d.workflow_id
WHERE d.workflow_id = $1::uuid
  AND d.dependencies && $2::INTEGER[]
  AND d.status = 'PENDING'
  AND w.status = 'RUNNING'
ORDER BY d.task_index";

const GET_PENDING_WORKFLOW_TASK_SQL: &str = "\
SELECT task_index, dependencies, args_from, workflow_ctx_from,
       allow_failed_deps, join_type, min_success, task_name,
       input, queue_name, priority, node_id,
       task_options, status, is_subworkflow, sub_workflow_name,
       sub_definition_key, sub_workflow_params
FROM horsies_workflow_tasks
WHERE workflow_id = $1 AND task_index = $2 AND status = 'PENDING'";

/// Batched PENDING -> SKIPPED CAS (per-row predicate preserved; losers drop via
/// RETURNING).
const SKIP_PENDING_TASKS_BATCH_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = 'SKIPPED', completed_at = NOW()
WHERE workflow_id = $1::uuid AND task_index = ANY($2) AND status = 'PENDING'
RETURNING task_index";

/// Batched PENDING -> READY CAS (RETURNING the won set).
const MARK_TASKS_READY_BATCH_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = 'READY'
WHERE workflow_id = $1::uuid AND task_index = ANY($2) AND status = 'PENDING'
RETURNING task_index";

/// Batched READY -> ENQUEUED CAS, guarded on the workflow still RUNNING (the same
/// re-check the single-node LINK does). RETURNING the won set drives the bulk
/// INSERT + LINK; a paused workflow returns zero rows so no task row is inserted.
/// A node's `started_at` is its enqueue time (Python `PROMOTE_DEPENDENTS_BATCH_SQL`,
/// `core/workflows/sql.py:449-470`).
const ENQUEUE_WORKFLOW_TASKS_BATCH_SQL: &str = "\
UPDATE horsies_workflow_tasks wt
SET status = 'ENQUEUED', started_at = NOW()
FROM horsies_workflows w
WHERE wt.workflow_id = $1::uuid AND wt.task_index = ANY($2)
  AND wt.status = 'READY'
  AND w.id = wt.workflow_id
  AND w.status = 'RUNNING'
RETURNING wt.task_index";

/// Batched link: set task_id on the CAS-won (already ENQUEUED) nodes via parallel
/// arrays. Status is already ENQUEUED from the CAS above.
const LINK_ENQUEUED_TASKS_BATCH_SQL: &str = "\
UPDATE horsies_workflow_tasks wt
SET task_id = data.tid
FROM (SELECT UNNEST($1::uuid[]) AS tid, UNNEST($2::int[]) AS idx) data
WHERE wt.workflow_id = $3::uuid AND wt.task_index = data.idx
  AND wt.status = 'ENQUEUED'";

const DEP_STATUS_COUNTS_SQL: &str = "\
SELECT status, COUNT(*)::int as cnt
FROM horsies_workflow_tasks
WHERE workflow_id = $1::uuid AND task_index = ANY($2)
GROUP BY status";

const UPDATE_WORKFLOW_TASK_READY_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = 'READY'
WHERE workflow_id = $1 AND task_index = $2 AND status = 'PENDING'
RETURNING id";

const UPDATE_WORKFLOW_TASK_SKIPPED_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = 'SKIPPED', completed_at = NOW()
WHERE workflow_id = $1 AND task_index = $2 AND status IN ('PENDING', 'READY')";

// An engine-minted task row carries no command fingerprint, no idempotency
// key and no retained input (horsies_tasks_command_fingerprint_by_origin).
// `sent_at` is the start call time of the node's workflow ($10). `good_until`
// is the `good_until` member of the node's stored task options ($6), cast as
// `horsies_workflow_node_tasks_insert` casts it: a text that the timestamptz
// input cannot read raises (22007 or 22008) and nothing is inserted. Python
// passes `str()` of the member to its insert (`horsies/core/workflows/engine.py:831-835`).
const ENQUEUE_TASK_SQL: &str = "\
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, input, task_options, execution,
    retention_class_key, sent_at, created_at,
    command_fingerprint_version, command_fingerprint, idempotency_key_digest,
    status, enqueued_at, retry_count, max_retries, good_until,
    is_workflow_task, updated_at,
    retain_rerun_input, rerun_of_task_id, rerun_root_task_id
)
VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8,
        (SELECT w.sent_at FROM horsies_workflows w WHERE w.id = $10::uuid), NOW(), NULL, NULL, NULL,
        'PENDING', NOW(), 0, $9, (nullif($6, '')::jsonb ->> 'good_until')::timestamptz,
        TRUE, NOW(), FALSE, NULL, NULL)";

/// A node's `started_at` is its enqueue time (Python `ENQUEUE_WORKFLOW_TASK_SQL`,
/// `core/workflows/sql.py:114-116`).
const LINK_ENQUEUED_TASK_SQL: &str = "\
UPDATE horsies_workflow_tasks wt
SET task_id = $1::uuid, status = 'ENQUEUED', started_at = NOW()
FROM horsies_workflows w
WHERE wt.workflow_id = $2::uuid AND wt.task_index = $3
  AND wt.status = 'READY'
  AND w.id = wt.workflow_id
  AND w.status = 'RUNNING'";

/// Every dependency row, with its task name and definition key for the `args_from` wrap; a dependency that is
/// not terminal is classified by its status (`args_merge::dependency_result`).
const GET_DEP_RESULTS_SQL: &str = "\
SELECT task_index, node_id, task_name, sub_definition_key, status, result
FROM horsies_workflow_tasks
WHERE workflow_id = $1 AND task_index = ANY($2)";

const COUNT_CTX_TERMINAL_SQL: &str = "\
SELECT COUNT(*)::int as cnt
FROM horsies_workflow_tasks
WHERE workflow_id = $1
  AND node_id = ANY($2)
  AND status IN ('COMPLETED', 'FAILED', 'SKIPPED')";

const GET_CTX_RESULTS_BY_NODE_ID_SQL: &str = "\
SELECT node_id, task_index, task_name, sub_definition_key, status, result, sub_workflow_summary
FROM horsies_workflow_tasks
WHERE workflow_id = $1
  AND node_id = ANY($2)
  AND status IN ('COMPLETED', 'FAILED', 'SKIPPED')";

const COUNT_NON_TERMINAL_SQL: &str = "\
SELECT COUNT(*)::int as cnt
FROM horsies_workflow_tasks
WHERE workflow_id = $1
  AND status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')";

const ALL_TASK_STATUSES_SQL: &str = "\
SELECT task_index, status
FROM horsies_workflow_tasks
WHERE workflow_id = $1
ORDER BY task_index";

const GET_WORKFLOW_META_SQL: &str = "\
SELECT status, output_task_index, success_policy, on_error, error IS NOT NULL AS has_stored_error
FROM horsies_workflows
WHERE id = $1";

const GET_OUTPUT_NODE_SQL: &str = "\
SELECT node_id, result, status, task_index, task_name, sub_definition_key
FROM horsies_workflow_tasks
WHERE workflow_id = $1 AND task_index = $2";

/// Payload-free edge read for terminal-set resolution. The per-row correlated
/// `NOT EXISTS (other.dependencies @> ARRAY[wt.task_index])` ran one GIN probe
/// per row inside the completion lock; the terminal set is instead computed in
/// app code (`compute_terminal_indexes`, O(N+E)) from these edges, then the
/// terminal rows are fetched by index. Parity with horsies PR #117.
const ALL_EDGES_SQL: &str = "\
SELECT task_index, dependencies
FROM horsies_workflow_tasks
WHERE workflow_id = $1";

/// Fetch the terminal tasks' results by index (companion to `ALL_EDGES_SQL`).
const TERMINAL_RESULTS_BY_INDEX_SQL: &str = "\
SELECT node_id, result, status, task_index, task_name, sub_definition_key
FROM horsies_workflow_tasks
WHERE workflow_id = $1 AND task_index = ANY($2)";

const UPDATE_WORKFLOW_COMPLETED_SQL: &str = "\
UPDATE horsies_workflows
SET status = 'COMPLETED', result = $2, completed_at = NOW(), updated_at = NOW()
WHERE id = $1 AND status = 'RUNNING' AND completed_at IS NULL
RETURNING id";

const UPDATE_WORKFLOW_FAILED_SQL: &str = "\
UPDATE horsies_workflows
SET status = 'FAILED', result = $2, error = COALESCE($3, error), completed_at = NOW(), updated_at = NOW()
WHERE id = $1 AND status = 'RUNNING' AND completed_at IS NULL
RETURNING id";

const LOCK_WORKFLOW_RECOVERY_CLAIM_SQL: &str = "\
SELECT TRUE
FROM horsies_recovery_scan_cursors
WHERE scan_name = 'running_workflows'
  AND claim_token = $1
  AND claim_expires_at > statement_timestamp()
FOR SHARE";

const GET_PARENT_WORKFLOW_SQL: &str = "\
SELECT parent_workflow_id, parent_task_index
FROM horsies_workflows
WHERE id = $1";

const GET_WORKFLOW_DEPTH_SQL: &str = "\
SELECT depth, root_workflow_id, name
FROM horsies_workflows
WHERE id = $1";

// Guard on the parent workflow being RUNNING (mirrors LINK_ENQUEUED_TASK_SQL):
// pausing a workflow does not change its READY nodes, so without this join a
// child sub-workflow could be launched and linked under a PAUSED parent when the
// pause lands between the dependents evaluation and this launch (C9).
const UPDATE_SUBWORKFLOW_LINK_SQL: &str = "\
UPDATE horsies_workflow_tasks wt
SET sub_workflow_id = $1, status = 'RUNNING', started_at = NOW()
FROM horsies_workflows w
WHERE wt.workflow_id = $2 AND wt.task_index = $3
  AND wt.status = 'READY'
  AND w.id = wt.workflow_id
  AND w.status = 'RUNNING'";

/// Writes the parent node of a terminal child. There is no gate on the parent workflow's status: the node of
/// an ended parent records what its child did, and nothing is promoted, because promotion and completion read
/// only RUNNING workflows (Python `UPDATE_PARENT_NODE_RESULT_SQL`, `core/workflows/sql.py:596-603`). The node
/// error is in `result`; the node `error` column has no writer.
const UPDATE_PARENT_NODE_RESULT_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = $1, result = $2, sub_workflow_summary = $3, completed_at = NOW()
WHERE workflow_id = $4 AND task_index = $5
  AND status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
RETURNING id";

/// The stored facts of a terminal child workflow that its parent node needs.
const GET_CHILD_WORKFLOW_FACTS_SQL: &str = "\
SELECT output_task_index, error FROM horsies_workflows WHERE id = $1";

const COUNT_CHILD_TASK_STATUSES_SQL: &str = "\
SELECT status, COUNT(*)::int as cnt
FROM horsies_workflow_tasks
WHERE workflow_id = $1
GROUP BY status";

/// Store the recomputed failure error on a still-RUNNING workflow (on_error=fail).
/// Unconditional SET: the caller passes the deterministic first-failed-task
/// error (by index), which must replace any stale higher-index error. The final
/// error is reselected at completion via `get_workflow_failure_error`.
const SET_WORKFLOW_ERROR_SQL: &str = "\
UPDATE horsies_workflows
SET error = $2, updated_at = NOW()
WHERE id = $1 AND status = 'RUNNING'";

/// Pause workflow and store the triggering error (on_error=pause).
const PAUSE_WORKFLOW_WITH_ERROR_SQL: &str = "\
UPDATE horsies_workflows
SET status = 'PAUSED', error = $2, updated_at = NOW()
WHERE id = $1 AND status = 'RUNNING'
RETURNING id";

/// Lock workflow row for completion check (prevents concurrent finalization races).
const LOCK_WORKFLOW_FOR_COMPLETION_SQL: &str = "\
SELECT id FROM horsies_workflows WHERE id = $1 FOR UPDATE";

/// Check current workflow status (for PAUSED guard).
const GET_WORKFLOW_STATUS_SQL: &str = "\
SELECT status FROM horsies_workflows WHERE id = $1";

/// Get the first failed task's error for workflow failure error specificity.
/// No success_policy: first failed task by index.
const FIRST_FAILED_TASK_ERROR_SQL: &str = "\
SELECT result FROM horsies_workflow_tasks
WHERE workflow_id = $1 AND status = 'FAILED'
ORDER BY task_index ASC LIMIT 1";

/// Get the first failed required task's error when success_policy is set.
const FIRST_FAILED_REQUIRED_TASK_ERROR_SQL: &str = "\
SELECT result FROM horsies_workflow_tasks
WHERE workflow_id = $1 AND status = 'FAILED'
  AND task_index = ANY($2)
ORDER BY task_index ASC LIMIT 1";

// ---------------------------------------------------------------------------
// Row types for internal queries
// ---------------------------------------------------------------------------

#[derive(Debug, sqlx::FromRow)]
struct WorkflowStatusRow {
    status: String,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct DependentRow {
    task_index: i32,
    dependencies: Vec<i32>,
    args_from: Option<serde_json::Value>,
    workflow_ctx_from: Option<Vec<String>>,
    allow_failed_deps: bool,
    join_type: String,
    min_success: Option<i32>,
    task_name: String,
    /// The node's stored input: one JSON value, as the client encoded it.
    input: String,
    queue_name: String,
    priority: i32,
    node_id: Option<String>,
    task_options: Option<String>,
    status: String,
    is_subworkflow: bool,
    sub_workflow_name: Option<String>,
    sub_definition_key: Option<String>,
    /// The client's params choice on a sub-workflow node (`NOT_GIVEN` or
    /// `GIVEN`); `NULL` on a task node.
    sub_workflow_params: Option<String>,
}

/// One batched-eval row: a PENDING dependent's full node config plus its
/// dependency-status counts (jsonb `{status: count}`), from `GET_DEPENDENTS_EVAL_SQL`.
#[derive(Debug, sqlx::FromRow)]
struct DependentEvalRow {
    #[sqlx(flatten)]
    node: DependentRow,
    dep_status_counts: serde_json::Value,
}

#[derive(Debug, sqlx::FromRow)]
struct StatusCount {
    status: String,
    cnt: i32,
}

#[derive(Debug, sqlx::FromRow)]
struct DepResult {
    task_index: i32,
    node_id: Option<String>,
    task_name: String,
    sub_definition_key: Option<String>,
    status: String,
    result: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct WorkflowMeta {
    status: String,
    output_task_index: Option<i32>,
    success_policy: Option<serde_json::Value>,
    on_error: String,
    has_stored_error: bool,
}

#[derive(Debug, sqlx::FromRow)]
struct TaskResultOnly {
    result: Option<String>,
}

/// A terminal node read for the workflow result.
#[derive(Debug, sqlx::FromRow)]
struct NodeResult {
    node_id: Option<String>,
    result: Option<String>,
    status: String,
    task_index: i32,
    task_name: String,
    sub_definition_key: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct EdgeRow {
    task_index: i32,
    dependencies: Vec<i32>,
}

#[derive(Debug, sqlx::FromRow)]
struct CtxResultRow {
    node_id: Option<String>,
    task_index: i32,
    task_name: String,
    sub_definition_key: Option<String>,
    status: String,
    result: Option<String>,
    sub_workflow_summary: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct NonTerminalCount {
    cnt: i32,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct TaskStatusRow {
    task_index: i32,
    status: String,
}

#[derive(Debug, sqlx::FromRow)]
struct ChildWorkflowFacts {
    output_task_index: Option<i32>,
    error: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct IdRow {
    id: Uuid,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct ParentWorkflowRow {
    parent_workflow_id: Option<Uuid>,
    parent_task_index: Option<i32>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct DepthRow {
    depth: Option<i32>,
    root_workflow_id: Option<Uuid>,
    name: String,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// What [`on_workflow_task_complete`] did with the phase-2 record of a finished workflow task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowTaskCompletion {
    /// The node took the task's terminal status and result, and the record is deleted. The workflow is
    /// live: its failure policy, the promotion of the node's dependents, the owed child starts and the
    /// completion check ran.
    Applied,
    /// The node took the task's terminal status and result, and the record is deleted. The workflow had
    /// ended (COMPLETED, FAILED, CANCELLED or EXPIRED): nothing was promoted, and the workflow and the
    /// other nodes keep their statuses.
    AppliedWorkflowEnded,
    /// The node was terminal already (a replay): nothing written.
    AlreadyApplied,
    /// No phase-2 record, and the node is not terminal: nothing written.
    PendingAbsent,
}

/// Why [`on_workflow_task_complete`] did not finish.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowTaskCompletionError {
    /// The node finish did not commit: a failed checkout, `BEGIN`, step, child start or `COMMIT`, a row
    /// outside the contract of the step, or a refused host build of an owed child. Nothing committed,
    /// except after a `COMMIT` whose reply was lost (`OutcomeUnknown` in the broker error). The phase-2
    /// record stays for a retry or for phase-2 recovery. Retry only when `error` may clear (a retryable
    /// broker or database error).
    #[error("node finish of workflow task {task_id} failed: {error}")]
    NodeFinish { task_id: Uuid, error: WorkflowError },
    /// The stored result of the task did not pass its check (`check`): nothing was written, and the
    /// phase-2 record is kept. Phase-2 recovery counts it and quarantines it at its bound. Not retryable.
    #[error(
        "phase-2 record of workflow task {task_id} kept: the stored result did not pass its check \
         ({check:?})"
    )]
    RecordKept { task_id: Uuid, check: SourceFailure },
    /// The node finish committed. The advance of the parent node above `child_workflow_id`, a child
    /// workflow that the step ended, failed; the levels before it committed, and that level wrote nothing
    /// or its outcome is unknown. Workflow recovery (case 1.6: a terminal child under a live parent node)
    /// advances the rest.
    #[error(
        "workflow task {task_id} finished; the advance of the parent of child workflow \
         {child_workflow_id} failed: {error}"
    )]
    ParentAdvance {
        task_id: Uuid,
        child_workflow_id: Uuid,
        error: WorkflowError,
    },
}

/// Phase 2 of a finished workflow task, in the form the worker runs when the phase-1 node facts are not
/// at hand: `BEGIN`; `horsies_workflow_node_finish(task_id, FALSE, 'READ')`; one
/// `horsies_workflow_child_start` per owed child start; `COMMIT`. Then the parent advances that the step
/// owes (`advance_parents`, one transaction per level). Every statement runs on the main core pool of
/// `broker` with the worker reserve ([`CheckoutClass::Reserved`]), as the worker's phase 2.
///
/// Phase 1 (the terminalization) stored the task's result and its phase-2 record. The node takes its
/// status from the record kind (COMPLETED for a completed task, FAILED for the other kinds) and its result
/// from the stored history row, whatever the status of its workflow. An ended workflow promotes nothing
/// (Python `COMPLETE_WORKFLOW_TASK_SQL`, `core/workflows/sql.py:418-465`, run by
/// `on_workflow_task_complete`, `core/workflows/engine.py:1746-1843`). The step sends no capacity wake
/// (Python's default `NoQueueWake`). Does not retry.
///
/// Returns: [`WorkflowTaskCompletion`]. Errors: [`WorkflowTaskCompletionError`], with the state that
/// stays after each.
pub async fn on_workflow_task_complete(
    broker: &PostgresBroker,
    task_id: Uuid,
    registry: &WorkflowSpecRegistry,
    retention: &RetentionConfig,
) -> Result<WorkflowTaskCompletion, WorkflowTaskCompletionError> {
    let build = ChildBuild {
        registry,
        retention,
    };
    let committed = node_finish_in_transaction(broker, task_id, false, build)
        .await
        .map_err(|error| WorkflowTaskCompletionError::NodeFinish { task_id, error })?;
    let completion = match committed.disposition {
        NodeFinishDisposition::Applied => WorkflowTaskCompletion::Applied,
        NodeFinishDisposition::AppliedWorkflowEnded => WorkflowTaskCompletion::AppliedWorkflowEnded,
        NodeFinishDisposition::AlreadyApplied => WorkflowTaskCompletion::AlreadyApplied,
        NodeFinishDisposition::PendingAbsent => WorkflowTaskCompletion::PendingAbsent,
        NodeFinishDisposition::Retained(check) => {
            return Err(WorkflowTaskCompletionError::RecordKept { task_id, check });
        }
    };
    advance_parents(
        broker,
        CheckoutClass::Reserved,
        committed.parent_advances,
        build,
    )
    .await
    .map_err(|failure| WorkflowTaskCompletionError::ParentAdvance {
        task_id,
        child_workflow_id: failure.failed_child_workflow_id,
        error: failure.error,
    })?;
    Ok(completion)
}

/// Whether a promotion may go on after one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PromotionStep {
    Continue,
    /// A node failure paused the workflow (`on_error = pause`): no other node of the step is promoted. Nodes
    /// that the step did not reach stay PENDING, and resume evaluates them again (Python reverts the nodes of
    /// the level that it moved and did not build to READY, `horsies/core/workflows/engine.py:474-533`).
    WorkflowPaused,
}

/// A promoted node, prepared from its own stored data, or refused. Python: `_EnqueueBuildError`
/// (`horsies/core/workflows/engine.py:620-631`).
pub(crate) enum Preparation<T> {
    Built(T),
    Refused(NodeRefusal),
}

/// Why the promotion refuses a node. The node fails alone with the error of
/// [`NodeRefusal::task_node_error`] or [`NodeRefusal::subworkflow_node_error`].
pub(crate) enum NodeRefusal {
    /// The node's own data cannot be prepared: its stored options, input or `args_from` do not read, or its
    /// child cannot be built or started. The text of the refusal.
    NotPrepared(String),
    /// An `args_from` key names a member of the node's static input (Python
    /// `horsies/core/workflows/engine.py:834-839`, `:1151-1163`).
    StaticMemberConflict(ArgsFromConflict),
    /// A member of the child's params does not decode to its builder parameter; the builder did not run.
    /// The node error is Python's for that member, as [`params_refused_error`] builds it:
    /// `WORKER_SERIALIZATION_ERROR` with the static-member or the `args_from` text, and the workflow id and
    /// the node index in `data` (`horsies/core/workflows/engine.py:1063-1080`, `:1134-1157`).
    ParamsRefused(TaskError),
}

impl NodeRefusal {
    /// The error that a refused task node stores: `WORKER_SERIALIZATION_ERROR` with the text of the refusal
    /// (Python `horsies/core/workflows/engine.py:746-790`), or `WORKFLOW_ENQUEUE_FAILED` for an `args_from`
    /// conflict (`:834-839`).
    pub(crate) fn task_node_error(self, workflow_id: Uuid, task_index: i32) -> TaskError {
        match self {
            NodeRefusal::NotPrepared(message) => node_not_prepared_error(
                workflow_id,
                task_index,
                OperationalErrorCode::WorkerSerializationError,
                message,
            ),
            NodeRefusal::StaticMemberConflict(conflict) => {
                conflict.node_error(workflow_id, task_index, MergedNodeKind::Task)
            }
            NodeRefusal::ParamsRefused(error) => error,
        }
    }

    /// The error that a refused sub-workflow node stores, `WORKFLOW_ENQUEUE_FAILED` in both cases: the build
    /// failure with the name of the workflow that holds the node, `workflow_name` (Python
    /// `horsies/core/workflows/engine.py:1180-1251`), or the `args_from` conflict (`:1151-1163`).
    pub(crate) fn subworkflow_node_error(
        self,
        workflow_id: Uuid,
        task_index: i32,
        workflow_name: &str,
    ) -> TaskError {
        match self {
            NodeRefusal::NotPrepared(message) => node_not_prepared_error(
                workflow_id,
                task_index,
                OperationalErrorCode::WorkflowEnqueueFailed,
                format!(
                    "Subworkflow build failed: {message} (workflow {workflow_name}:{task_index})"
                ),
            ),
            NodeRefusal::StaticMemberConflict(conflict) => {
                conflict.node_error(workflow_id, task_index, MergedNodeKind::SubWorkflow)
            }
            NodeRefusal::ParamsRefused(error) => error,
        }
    }
}

/// Why the preparation of a promoted node stopped.
pub(crate) enum PreparationStop {
    /// The node's own data refuses it: the node fails alone.
    Refused(NodeRefusal),
    /// An error of the step (a failed read, for example): the step's transaction rolls back.
    Failed(WorkflowError),
}

/// A `Validation` or `Serialization` error of a preparation step is a refusal of the node's own data; every
/// other error is an error of the step.
impl From<WorkflowError> for PreparationStop {
    fn from(error: WorkflowError) -> Self {
        match error {
            WorkflowError::Validation(message) => {
                PreparationStop::Refused(NodeRefusal::NotPrepared(message))
            }
            WorkflowError::Serialization(error) => {
                PreparationStop::Refused(NodeRefusal::NotPrepared(error.to_string()))
            }
            error @ (WorkflowError::Database(_)
            | WorkflowError::Broker(_)
            | WorkflowError::WorkflowNotFound { .. }
            | WorkflowError::WorkflowTimeout { .. }
            | WorkflowError::WorkflowError(_)
            | WorkflowError::InvalidStatus(_)
            | WorkflowError::StepContract(_)) => PreparationStop::Failed(error),
        }
    }
}

/// A sub-workflow root of the child that cannot start is split as a `WorkflowError` is; every other failure
/// of the child start of the engine code is an error of the step ([`ChildStartStop`]).
impl From<ChildStartStop> for PreparationStop {
    fn from(stop: ChildStartStop) -> Self {
        match stop {
            ChildStartStop::RootSubworkflowNotStarted(error) => PreparationStop::from(error),
            ChildStartStop::Failed(error) => PreparationStop::Failed(error),
        }
    }
}

/// The node input after the `args_from` merge; an `args_from` key that names a member of the static input
/// refuses the node. Pure.
pub(crate) fn merged_input_or_refusal(merge: ArgsFromMerge) -> Result<String, PreparationStop> {
    match merge {
        ArgsFromMerge::Merged(input) => Ok(input),
        ArgsFromMerge::StaticMemberConflict(conflict) => Err(PreparationStop::Refused(
            NodeRefusal::StaticMemberConflict(conflict),
        )),
    }
}

/// Splits the outcome of a preparation: a refusal fails the node alone ([`Preparation::Refused`]); an error
/// of the step is returned, and the step's transaction rolls back. A `WorkflowError` is split as
/// [`PreparationStop`] splits it. Pure.
pub(crate) fn preparation<T, E: Into<PreparationStop>>(
    result: Result<T, E>,
) -> Result<Preparation<T>, WorkflowError> {
    let result: Result<T, PreparationStop> = result.map_err(Into::into);
    match result {
        Ok(built) => Ok(Preparation::Built(built)),
        Err(PreparationStop::Refused(refusal)) => Ok(Preparation::Refused(refusal)),
        Err(PreparationStop::Failed(error)) => Err(error),
    }
}

/// The child spec of the promoted sub-workflow node `task_index` of `workflow_id`: its input after the
/// `args_from` merge, made into the builder input by [`child_build_input`] with the node's stored
/// `args_from`, its stored params, then the build (Python checks the merge and its conflict,
/// `horsies/core/workflows/engine.py:1048-1163`, before the build, `:1165-1215`).
///
/// Every refusal fails the node alone: a stored input from which the builder input cannot be made and a
/// failed build are [`NodeRefusal::NotPrepared`] with the label and the text of the [`ChildSpecError`]; a
/// params member that does not decode is [`NodeRefusal::ParamsRefused`]. Pure.
pub(crate) fn prepare_child_spec(
    registered: crate::core::registry::workflow::ResolvedChildWorkflow<'_>,
    workflow_id: Uuid,
    task_index: i32,
    merge: Result<ArgsFromMerge, WorkflowError>,
    args_from: Option<&serde_json::Value>,
    sub_workflow_params: Option<&str>,
    registry: &WorkflowSpecRegistry,
) -> Result<crate::core::WorkflowSpec, PreparationStop> {
    let merged_input = merged_input_or_refusal(merge?)?;
    let input = child_build_input(merged_input, args_from).map_err(|refused| {
        PreparationStop::Refused(NodeRefusal::NotPrepared(child_spec_failure_text(
            &ChildSpecError::of_stored_input(refused),
        )))
    })?;
    let params = stored_sub_workflow_params(sub_workflow_params)?;
    materialize_child_spec(registered, params, &input.text, registry).map_err(|failure| {
        match failure {
            ChildSpecFailure::BuildFailed(error) => {
                PreparationStop::Refused(NodeRefusal::NotPrepared(child_spec_failure_text(&error)))
            }
            ChildSpecFailure::MemberRefused { key, cause } => {
                PreparationStop::Refused(NodeRefusal::ParamsRefused(params_refused_error(
                    workflow_id,
                    task_index,
                    RefusedMember {
                        key: &key,
                        cause: &cause,
                    },
                    &input,
                    registry.builder_hook(),
                )))
            }
        }
    })
}

/// The error that a node stores when the promotion cannot prepare it (Python `_fail_enqueued_task`,
/// `horsies/core/workflows/engine.py:474-533`): the code, the message, and the workflow id and the node
/// index in `data`.
pub(crate) fn node_not_prepared_error(
    workflow_id: Uuid,
    task_index: i32,
    code: OperationalErrorCode,
    message: String,
) -> TaskError {
    TaskError {
        data: Some(serde_json::json!({
            "workflow_id": workflow_id.to_string(),
            "task_index": task_index,
        })),
        ..TaskError::builtin(code, message)
    }
}

/// The values of a promoted task node's task row that come from the node's own stored data. The deadline
/// is not here: the insert casts the `good_until` member of the stored options (`ENQUEUE_TASK_SQL`).
pub(crate) struct PreparedTaskRow {
    /// The node's input with its `args_from` and workflow context members.
    pub(crate) input: String,
    pub(crate) max_retries: i32,
    pub(crate) facts: WorkflowTaskFacts,
}

/// The stored data of a READY task node that recovery case 1 enqueues.
pub(crate) struct ReadyNodeData<'a> {
    pub(crate) task_index: i32,
    pub(crate) task_name: &'a str,
    pub(crate) input: &'a str,
    pub(crate) args_from: &'a Option<serde_json::Value>,
    pub(crate) dependencies: &'a [i32],
    pub(crate) workflow_ctx_from: Option<&'a [String]>,
    pub(crate) task_options: Option<&'a str>,
    pub(crate) queue_name: &'a str,
}

/// Builds the task row values of a READY task node that recovery case 1 enqueues: the input (`args_from`
/// from the dependency rows, then the workflow context), the retry limit and the facts. A refusal of the
/// node's own data, or an `args_from` key that names a member of its static input, is
/// [`PreparationStop::Refused`]; a failed read is [`PreparationStop::Failed`]. The stored `good_until` is
/// not read here: the insert casts it, and a text that the database cannot read raises there. Reads on
/// `pool`; writes nothing.
pub(crate) async fn prepare_ready_task_row(
    pool: &PgPool,
    workflow_id: Uuid,
    node: ReadyNodeData<'_>,
    retention: &RetentionConfig,
) -> Result<PreparedTaskRow, PreparationStop> {
    let merged_input = merged_input_or_refusal(
        crate::workflow_engine::args_merge::merge_args_from_async(
            pool,
            workflow_id,
            node.input,
            node.args_from,
            node.dependencies,
        )
        .await?,
    )?;
    let input = inject_workflow_ctx_into_input(
        pool,
        workflow_id,
        node.task_index,
        node.task_name,
        node.workflow_ctx_from,
        merged_input,
    )
    .await?;
    // The retry limit is read first: its refusal of options that do not read names the node.
    let max_retries = crate::workflow_engine::node_max_retries(
        node.task_index,
        node.task_name,
        node.task_options,
    )?;
    let facts = prepare_workflow_task_facts(
        &input,
        node.task_options,
        retention.resolve_queue_class(node.queue_name).as_deref(),
    )
    .map_err(|error| WorkflowError::Validation(error.to_string()))?;
    Ok(PreparedTaskRow {
        input,
        max_retries,
        facts,
    })
}

/// The error of the sub-workflow node `task_index` of the workflow named `parent_workflow_name` whose
/// child definition is not registered under its definition key. A child is resolved only by its key
/// (Python `core/workflows/engine.py:1034-1045`). The message names the parent workflow and the node;
/// `data` holds the definition key (`null` when the node has none), as Python's `_fail_subworkflow_load`
/// does (`core/workflows/engine.py:577-581`).
pub(crate) fn subworkflow_load_failed_error(
    parent_workflow_name: &str,
    task_index: i32,
    definition_key: Option<&str>,
) -> TaskError {
    TaskError {
        data: Some(serde_json::json!({ "definition_key": definition_key })),
        ..TaskError::builtin(
            OperationalErrorCode::SubworkflowLoadFailed,
            format!(
                "Failed to load subworkflow definition for {parent_workflow_name}:{task_index}"
            ),
        )
    }
}

// ---------------------------------------------------------------------------
// Dependency resolution
// ---------------------------------------------------------------------------

/// Promote/skip the dependents of a completed task, one batched pipeline per
/// skip-cascade level.
///
/// Level 0's source is the completed task; each level's newly SKIPPED nodes
/// become the next level's sources (their dependents must be re-evaluated).
/// Statuses advance monotonically PENDING -> terminal, so the level count is
/// bounded by the longest skip chain. A dependent is re-evaluated at every level
/// one of its deps skipped (no cross-level dedupe — a quorum verdict may only be
/// satisfiable after a later skip).
///
/// Uses `Box::pin` because of the recursive call chain:
/// process_dependents -> promote_dependents_level -> try_make_ready_and_enqueue
/// (slow path) -> skip_task -> process_dependents.
pub(crate) fn process_dependents<'a>(
    pool: &'a PgPool,
    workflow_id: Uuid,
    completed_index: i32,
    registry: &'a WorkflowSpecRegistry,
    payload: &'a PayloadPolicy,
    retention: &'a RetentionConfig,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), WorkflowError>> + Send + 'a>> {
    Box::pin(async move {
        let mut sources = vec![completed_index];
        while !sources.is_empty() {
            sources =
                promote_dependents_level(pool, workflow_id, &sources, registry, payload, retention)
                    .await?;
        }
        Ok(())
    })
}

/// Read a status's count for a dependent's deps from the jsonb `{status: count}`
/// aggregate produced by `GET_DEPENDENTS_EVAL_SQL`.
fn dep_status_count(counts: &serde_json::Value, status: &str) -> i32 {
    counts
        .get(status)
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0) as i32
}

/// Evaluate and promote/skip every PENDING dependent of the source indexes in one
/// batched pipeline. Returns the indexes this level transitioned to SKIPPED (the
/// next level's sources).
///
/// Plain TaskNode dependents take the batched fast path (zero per-node
/// statements): one grouped eval, in-memory join classification, batched
/// SKIP/READY CAS, one grouped dependency-results read, payload build, one batched
/// READY->ENQUEUED CAS, one bulk task INSERT + one bulk LINK — all writes in a
/// single transaction so a crash rolls back atomically (no orphan task row, no
/// stranded ENQUEUED). SubWorkflowNode and `workflow_ctx_from` dependents take the
/// per-node `try_make_ready_and_enqueue` path unchanged. A plain dependent that the
/// build refuses ([`NodeRefusal`]) fails before the level's transaction
/// (`fail_workflow_node`); a failure that pauses the workflow ends the level before
/// it writes.
async fn promote_dependents_level(
    pool: &PgPool,
    workflow_id: Uuid,
    source_indexes: &[i32],
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<Vec<i32>, WorkflowError> {
    let eval_rows: Vec<DependentEvalRow> = sqlx::query_as(GET_DEPENDENTS_EVAL_SQL)
        .bind(workflow_id)
        .bind(source_indexes)
        .fetch_all(pool)
        .await?;
    if eval_rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut skip_pending: Vec<i32> = Vec::new();
    let mut ready_plain: Vec<DependentRow> = Vec::new();
    let mut slow_nodes: Vec<DependentRow> = Vec::new();

    for eval in eval_rows {
        let node = eval.node;
        let total_deps = node.dependencies.len() as i32;
        if total_deps == 0 {
            continue; // roots are never re-promoted here
        }

        let completed = dep_status_count(&eval.dep_status_counts, "COMPLETED");
        let failed = dep_status_count(&eval.dep_status_counts, "FAILED");
        let skipped = dep_status_count(&eval.dep_status_counts, "SKIPPED");
        let terminal = completed + failed + skipped;

        let join_type = parse_join_type(&node.join_type);
        let min_success = node.min_success.unwrap_or(1);

        // Same join evaluation as try_make_ready_and_enqueue.
        let (should_skip, should_ready) = match join_type {
            JoinType::All => {
                if terminal < total_deps {
                    (false, false) // not all deps done yet
                } else if failed + skipped > 0 && !node.allow_failed_deps {
                    (true, false) // cannot run — skip (PENDING -> SKIPPED directly)
                } else {
                    (false, true)
                }
            }
            JoinType::Any => {
                if completed > 0 {
                    (false, true)
                } else if terminal == total_deps {
                    (true, false) // all terminal, none completed — impossible
                } else {
                    (false, false)
                }
            }
            JoinType::Quorum => {
                if completed >= min_success {
                    (false, true)
                } else if completed + (total_deps - terminal) < min_success {
                    (true, false) // quorum unreachable
                } else {
                    (false, false)
                }
            }
        };

        if should_skip {
            skip_pending.push(node.task_index);
        } else if should_ready {
            let has_ctx = node
                .workflow_ctx_from
                .as_ref()
                .is_some_and(|ids| !ids.is_empty());
            if node.is_subworkflow || has_ctx {
                slow_nodes.push(node); // per-node path (child start / ctx gating)
            } else {
                ready_plain.push(node);
            }
        }
        // else: stay PENDING (also covers unknown join types via parse_join_type)
    }

    // Dependency results for the union of args_from deps of the ready plain nodes.
    // Deps of a ready node are all terminal (immutable), so reading before the
    // write transaction is race-free.
    let dep_union: Vec<i32> = {
        let mut set: std::collections::BTreeSet<i32> = std::collections::BTreeSet::new();
        for node in &ready_plain {
            if node.args_from.is_some() {
                set.extend(node.dependencies.iter().copied());
            }
        }
        set.into_iter().collect()
    };
    let dep_results = if dep_union.is_empty() {
        HashMap::new()
    } else {
        get_dependency_results(pool, workflow_id, &dep_union).await?
    };

    // Build the task row of each ready plain node before the level writes: these nodes take no
    // workflow context, so the build reads no row. A node that the build refuses ([`NodeRefusal`])
    // fails alone here (Python `_fail_enqueued_task`, `horsies/core/workflows/engine.py:474-533`):
    // its own data does not read, or its `args_from` names a member of its static input
    // (`:834-839`). A failure that pauses the workflow ends the level before it writes: no node
    // of the level is promoted and no sibling gets a task row; the nodes stay PENDING and resume
    // evaluates them again (Python reverts the level's nodes to READY and inserts no task row,
    // `:2365-2378`). A task-id mint failure or any other build error leaves the node without
    // insert values: it becomes READY below and recovery case 1 enqueues it.
    let mut built_by_index: HashMap<i32, EnqueueInsertParams> = HashMap::new();
    let mut refused: Vec<(i32, NodeRefusal)> = Vec::new();
    for node in &ready_plain {
        let task_id = match crate::core::history::identity::uuid7::mint_task_id() {
            Ok(task_id) => task_id,
            Err(error) => {
                tracing::error!(
                    workflow_id = %workflow_id,
                    task_index = node.task_index,
                    error = %error,
                    "task identity mint failed; node stays READY",
                );
                continue;
            }
        };
        match preparation(
            build_enqueued_task_params(
                pool,
                workflow_id,
                node,
                task_id,
                &dep_results,
                payload,
                retention,
            )
            .await,
        ) {
            Ok(Preparation::Built(params)) => {
                built_by_index.insert(node.task_index, params);
            }
            Ok(Preparation::Refused(refusal)) => refused.push((node.task_index, refusal)),
            Err(e) => {
                tracing::error!(
                    workflow_id = %workflow_id,
                    task_index = node.task_index,
                    error = %e,
                    "failed to build enqueued task params; node stays READY",
                );
            }
        }
    }
    let refused_indexes: std::collections::HashSet<i32> =
        refused.iter().map(|(task_index, _)| *task_index).collect();
    ready_plain.retain(|node| !refused_indexes.contains(&node.task_index));
    for (task_index, refusal) in refused {
        let step = fail_workflow_node(
            pool,
            workflow_id,
            task_index,
            refusal.task_node_error(workflow_id, task_index),
            registry,
            payload,
            retention,
        )
        .await?;
        match step {
            PromotionStep::Continue => {}
            PromotionStep::WorkflowPaused => return Ok(Vec::new()),
        }
    }

    let mut newly_skipped: Vec<i32> = Vec::new();
    let mut tx = pool.begin().await?;

    // 1. Batched PENDING -> SKIPPED.
    if !skip_pending.is_empty() {
        let rows: Vec<(i32,)> = sqlx::query_as(SKIP_PENDING_TASKS_BATCH_SQL)
            .bind(workflow_id)
            .bind(&skip_pending)
            .fetch_all(&mut *tx)
            .await?;
        newly_skipped.extend(rows.into_iter().map(|r| r.0));
    }

    // 2. Batched PENDING -> READY (CAS losers drop out of the won set).
    if !ready_plain.is_empty() {
        let ready_indexes: Vec<i32> = ready_plain.iter().map(|n| n.task_index).collect();
        let won_rows: Vec<(i32,)> = sqlx::query_as(MARK_TASKS_READY_BATCH_SQL)
            .bind(workflow_id)
            .bind(&ready_indexes)
            .fetch_all(&mut *tx)
            .await?;
        let ready_won: std::collections::HashSet<i32> = won_rows.into_iter().map(|r| r.0).collect();

        // 3. The INSERT payloads of the won nodes, built above. A node whose build
        // failed has none and stays READY.
        let mut built: Vec<EnqueueInsertParams> = Vec::new();
        let mut built_indexes: Vec<i32> = Vec::new();
        for node in &ready_plain {
            if !ready_won.contains(&node.task_index) {
                continue;
            }
            if let Some(params) = built_by_index.remove(&node.task_index) {
                built_indexes.push(node.task_index);
                built.push(params);
            }
        }

        // 4. Batched READY -> ENQUEUED CAS (guarded on workflow RUNNING).
        if !built_indexes.is_empty() {
            let enqueued_rows: Vec<(i32,)> = sqlx::query_as(ENQUEUE_WORKFLOW_TASKS_BATCH_SQL)
                .bind(workflow_id)
                .bind(&built_indexes)
                .fetch_all(&mut *tx)
                .await?;
            let enqueue_won: std::collections::HashSet<i32> =
                enqueued_rows.into_iter().map(|r| r.0).collect();

            // 5. Bulk INSERT task rows + bulk LINK for the CAS-won set.
            let winners: Vec<(i32, &EnqueueInsertParams)> = built_indexes
                .iter()
                .zip(built.iter())
                .filter(|(idx, _)| enqueue_won.contains(idx))
                .map(|(idx, p)| (*idx, p))
                .collect();

            if !winners.is_empty() {
                let mut qb: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
                    "INSERT INTO horsies_tasks (\
                     id, task_name, queue_name, priority, input, task_options, execution, \
                     retention_class_key, sent_at, created_at, \
                     command_fingerprint_version, command_fingerprint, idempotency_key_digest, \
                     status, enqueued_at, retry_count, max_retries, good_until, \
                     is_workflow_task, updated_at, \
                     retain_rerun_input, rerun_of_task_id, rerun_root_task_id) ",
                );
                qb.push_values(winners.iter(), |mut b, (_, p)| {
                    b.push_bind(p.task_id)
                        .push_bind(p.task_name.clone())
                        .push_bind(p.queue_name.clone())
                        .push_bind(p.priority)
                        .push_bind(p.input.clone())
                        .push_bind(p.task_options.clone())
                        .push_bind(p.facts.execution.as_str())
                        .push_bind(p.facts.retention_class_key.clone())
                        // sent_at: the start call time of the workflow.
                        .push("(SELECT w.sent_at FROM horsies_workflows w WHERE w.id = ")
                        .push_bind_unseparated(workflow_id)
                        .push_unseparated(")")
                        .push("NOW()")
                        .push("NULL")
                        .push("NULL")
                        .push("NULL")
                        .push("'PENDING'")
                        .push("NOW()")
                        .push("0")
                        .push_bind(p.max_retries)
                        // good_until: the member of the stored options, cast as in
                        // ENQUEUE_TASK_SQL. A text that does not read raises, and the level's
                        // transaction rolls back.
                        .push("(nullif(")
                        .push_bind_unseparated(p.task_options.clone())
                        .push_unseparated(", '')::jsonb ->> 'good_until')::timestamptz")
                        .push("TRUE")
                        .push("NOW()")
                        .push("FALSE")
                        .push("NULL")
                        .push("NULL");
                });
                qb.build().execute(&mut *tx).await?;

                let tids: Vec<Uuid> = winners.iter().map(|(_, p)| p.task_id).collect();
                let idxs: Vec<i32> = winners.iter().map(|(idx, _)| *idx).collect();
                sqlx::query(LINK_ENQUEUED_TASKS_BATCH_SQL)
                    .bind(&tids)
                    .bind(&idxs)
                    .bind(workflow_id)
                    .execute(&mut *tx)
                    .await?;

                tracing::debug!(
                    workflow_id = %workflow_id,
                    count = winners.len(),
                    "batch-enqueued workflow tasks",
                );
            }
        }
    }

    tx.commit().await?;

    // 6. Slow path: per-node enqueue for subworkflow / ctx_from dependents (own
    // transactions, unchanged). Errors are logged and the level continues.
    for node in &slow_nodes {
        if let Err(e) =
            try_make_ready_and_enqueue(pool, workflow_id, node, registry, payload, retention).await
        {
            tracing::error!(
                workflow_id = %workflow_id,
                task_index = node.task_index,
                error = %e,
                "failed to process dependent task",
            );
        }
    }

    Ok(newly_skipped)
}

/// Core resolver: evaluate whether a dependent task can transition to READY.
///
/// Counts dependency statuses, evaluates join condition, and either
/// enqueues the node, or marks it SKIPPED if the join condition can never be
/// satisfied.
///
/// The READY mark of an enqueued node commits in one transaction with its task row
/// ([`enqueue_workflow_task`]) or its child workflow ([`enqueue_subworkflow_task`]).
/// An enqueue that raises leaves the node PENDING, as Python's promotion, which runs
/// in one transaction, leaves it (recovery case 0 runs each candidate in a savepoint,
/// `horsies/core/workflows/recovery.py:93-94`, `:712-760`).
async fn try_make_ready_and_enqueue(
    pool: &PgPool,
    workflow_id: Uuid,
    task: &DependentRow,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let dep_indices = &task.dependencies;
    let dep_count = dep_indices.len() as i32;

    // Count statuses of all dependencies.
    let status_counts: Vec<StatusCount> = sqlx::query_as(DEP_STATUS_COUNTS_SQL)
        .bind(workflow_id)
        .bind(dep_indices)
        .fetch_all(pool)
        .await?;

    let mut completed = 0i32;
    let mut failed = 0i32;
    let mut skipped = 0i32;
    let mut terminal = 0i32;

    for sc in &status_counts {
        match sc.status.as_str() {
            "COMPLETED" => {
                completed += sc.cnt;
                terminal += sc.cnt;
            }
            "FAILED" => {
                failed += sc.cnt;
                terminal += sc.cnt;
            }
            "SKIPPED" => {
                skipped += sc.cnt;
                terminal += sc.cnt;
            }
            _ => {}
        }
    }

    let join_type = parse_join_type(&task.join_type);
    let min_success = task.min_success.unwrap_or(1);

    // Evaluate join condition.
    let should_run = match join_type {
        JoinType::All => {
            if terminal < dep_count {
                return Ok(()); // Not all deps done yet.
            }
            // All terminal. Run if no failures, or if allow_failed_deps.
            if failed + skipped > 0 && !task.allow_failed_deps {
                // Cannot run — skip this task.
                skip_task(
                    pool,
                    workflow_id,
                    task.task_index,
                    registry,
                    payload,
                    retention,
                )
                .await?;
                return Ok(());
            }
            true
        }
        JoinType::Any => {
            if completed > 0 {
                true // At least one succeeded.
            } else if terminal == dep_count {
                // All terminal but none completed — impossible to satisfy.
                skip_task(
                    pool,
                    workflow_id,
                    task.task_index,
                    registry,
                    payload,
                    retention,
                )
                .await?;
                return Ok(());
            } else {
                return Ok(()); // Still waiting.
            }
        }
        JoinType::Quorum => {
            if completed >= min_success {
                true // Quorum reached.
            } else {
                // Check if quorum is still possible.
                let remaining = dep_count - terminal;
                if completed + remaining < min_success {
                    // Impossible to reach quorum — skip.
                    skip_task(
                        pool,
                        workflow_id,
                        task.task_index,
                        registry,
                        payload,
                        retention,
                    )
                    .await?;
                    return Ok(());
                }
                return Ok(()); // Still possible, keep waiting.
            }
        }
    };

    if !should_run {
        return Ok(());
    }

    // Ensure workflow_ctx_from deps are terminal before enqueueing.
    if let Some(ctx_from_ids) = task.workflow_ctx_from.as_ref() {
        if !ctx_from_ids.is_empty() {
            let count: i32 = sqlx::query_scalar(COUNT_CTX_TERMINAL_SQL)
                .bind(workflow_id)
                .bind(ctx_from_ids)
                .fetch_one(pool)
                .await?;
            if count < ctx_from_ids.len() as i32 {
                return Ok(()); // Context deps not ready.
            }
        }
    }

    // The dependency rows for the `args_from` members, read on the pool before the
    // enqueue's transaction, which holds only the enqueue's writes.
    let dep_results = if task.args_from.is_some() {
        get_dependency_results(pool, workflow_id, dep_indices).await?
    } else {
        HashMap::new()
    };

    // The enqueue marks the node READY (the CAS that claims it) with its task row or
    // child workflow, in one transaction.
    match task.is_subworkflow {
        true => {
            enqueue_subworkflow_task(
                pool,
                workflow_id,
                task,
                registry,
                &dep_results,
                payload,
                retention,
            )
            .await
        }
        false => {
            enqueue_workflow_task(
                pool,
                workflow_id,
                task,
                &dep_results,
                registry,
                payload,
                retention,
            )
            .await
        }
    }
}

/// Re-evaluate the exact PENDING node selected by workflow recovery.
///
/// Returns true only when that node leaves PENDING. This does not use a
/// dependency as a proxy, so it cannot advance unrelated sibling nodes.
///
/// Errors: the error of the enqueue. A task insert that raises (a stored
/// `good_until` that the database cannot read) or a child start that cannot write
/// its child rolls back with the READY mark, so the node stays PENDING
/// ([`try_make_ready_and_enqueue`]); the recovery pass counts the error toward the
/// failure limit.
pub(crate) async fn recover_pending_workflow_task(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<bool, WorkflowError> {
    let task: Option<DependentRow> = sqlx::query_as(GET_PENDING_WORKFLOW_TASK_SQL)
        .bind(workflow_id)
        .bind(task_index)
        .fetch_optional(pool)
        .await?;
    let Some(task) = task else {
        return Ok(false);
    };
    try_make_ready_and_enqueue(pool, workflow_id, &task, registry, payload, retention).await?;
    let remains_pending: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM horsies_workflow_tasks
             WHERE workflow_id = $1 AND task_index = $2 AND status = 'PENDING'
         )",
    )
    .bind(workflow_id)
    .bind(task_index)
    .fetch_one(pool)
    .await?;
    Ok(!remains_pending)
}

/// Mark a task as SKIPPED and cascade to its dependents.
async fn skip_task(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    sqlx::query(UPDATE_WORKFLOW_TASK_SKIPPED_SQL)
        .bind(workflow_id)
        .bind(task_index)
        .execute(pool)
        .await?;

    tracing::debug!(workflow_id = %workflow_id, task_index, "workflow task skipped");

    // Cascade: process dependents of this skipped task.
    process_dependents(pool, workflow_id, task_index, registry, payload, retention).await?;

    Ok(())
}

// Mark a workflow task as FAILED due to a condition evaluation error.
//
// Mirrors the failure path in `on_workflow_task_complete`: marks the
// workflow_task as FAILED, invokes `handle_workflow_task_failure` for
// the on_error policy, then cascades through `process_dependents` and
// `check_workflow_completion`.
// ---------------------------------------------------------------------------
// Task enqueue
// ---------------------------------------------------------------------------

/// Computed `horsies_tasks` INSERT values for an enqueued workflow node.
///
/// Built by [`build_enqueued_task_params`] (args_from merge, ctx injection,
/// options parse, execution class), then bound by the single-node and batched
/// enqueue paths so the two cannot diverge.
struct EnqueueInsertParams {
    task_id: Uuid,
    task_name: String,
    queue_name: String,
    priority: i32,
    /// The node's input with its args_from and workflow context members.
    input: String,
    max_retries: i32,
    /// The node's stored task options. The insert casts their `good_until` member.
    task_options: Option<String>,
    facts: WorkflowTaskFacts,
}

/// Build the `horsies_tasks` INSERT params for a workflow node ready to enqueue.
///
/// Shared by `enqueue_workflow_task` (single-node) and the batched promotion
/// pipeline. Merges `args_from` dependency results into the node's input, injects
/// `workflow_ctx` when configured (the only DB read here), parses the retry limit
/// and the execution class from task_options, with the task id the caller minted.
/// A refusal of the node's own data, or an `args_from` key that names a member of
/// its static input, is [`PreparationStop::Refused`] (the node fails alone,
/// [`preparation`]); a failed read is [`PreparationStop::Failed`]. The stored
/// `good_until` is not read here: the insert casts it, and a text that the database
/// cannot read raises there (Python `horsies/core/workflows/engine.py:831-835`).
/// Writes nothing.
async fn build_enqueued_task_params(
    pool: &PgPool,
    workflow_id: Uuid,
    task: &DependentRow,
    task_id: Uuid,
    dep_results: &HashMap<i32, DepResultValue>,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<EnqueueInsertParams, PreparationStop> {
    // Merge args_from into the node's input.
    let merged_input =
        merged_input_or_refusal(merge_args_from(&task.input, &task.args_from, dep_results)?)?;

    // Inject workflow_ctx if configured (shared with the resume/recovery
    // re-enqueue paths via inject_workflow_ctx_into_input, so all three paths
    // produce identical context under WORKFLOW_CTX_KWARG).
    let merged_input = inject_workflow_ctx_into_input(
        pool,
        workflow_id,
        task.task_index,
        &task.task_name,
        task.workflow_ctx_from.as_deref(),
        merged_input,
    )
    .await?;

    // Warn-only payload guardrail at the args_from injection point, where an
    // upstream task's result envelope becomes part of this node's input — the
    // likeliest producer of oversized payloads. Rejecting a mid-workflow node
    // needs a designed node-failure path (a size limit must not strand a
    // running workflow), so reject semantics for workflow nodes are deferred —
    // same as Python. Parity with horsies PR #208.
    enforce_payload_policy(
        payload,
        &task.task_name,
        PayloadKind::Kwargs,
        merged_input.len(),
    );

    let max_retries = crate::workflow_engine::node_max_retries(
        task.task_index,
        &task.task_name,
        task.task_options.as_deref(),
    )?;
    let retention_class_key = retention.resolve_queue_class(&task.queue_name);
    let facts = prepare_workflow_task_facts(
        &merged_input,
        task.task_options.as_deref(),
        retention_class_key.as_deref(),
    )
    .map_err(|error| WorkflowError::Validation(error.to_string()))?;

    Ok(EnqueueInsertParams {
        task_id,
        task_name: task.task_name.clone(),
        queue_name: task.queue_name.clone(),
        priority: task.priority,
        input: merged_input,
        max_retries,
        task_options: task.task_options.clone(),
        facts,
    })
}

/// Inject the `workflow_ctx` payload into a node's input when `workflow_ctx_from`
/// is non-empty, returning the (possibly updated) input JSON.
///
/// Shared by the runtime promotion path (`build_enqueued_task_params`) and the
/// recovery re-enqueue path (`recovery::enqueue_ready_task`) so both inject
/// identical context under `WORKFLOW_CTX_KWARG`. A node with no `workflow_ctx_from` returns its input
/// unchanged; the DB read only happens when context is actually required.
pub(crate) async fn inject_workflow_ctx_into_input(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
    task_name: &str,
    ctx_from_ids: Option<&[String]>,
    merged_input: String,
) -> Result<String, WorkflowError> {
    let ctx_from_ids = match ctx_from_ids {
        Some(ids) if !ids.is_empty() => ids,
        _ => return Ok(merged_input),
    };

    let ctx_data =
        build_workflow_context_data(pool, workflow_id, task_index, task_name, ctx_from_ids).await?;
    let ctx_payload = ctx_data.to_payload()?;

    let mut object = input_object(&merged_input)?;
    object.insert(WORKFLOW_CTX_KWARG.to_owned(), ctx_payload);
    Ok(serde_json::to_string(&object)?)
}

/// Mark a PENDING workflow task READY, create its horsies_tasks row and link it, in one transaction.
///
/// Merges `args_from` results into the task's input. A node that the build refuses ([`NodeRefusal`])
/// fails alone instead ([`claim_and_fail_node`]). An insert that raises (a stored `good_until` that the
/// database cannot read) rolls back the READY mark too: the node stays PENDING and the error is returned.
/// A node that another promotion claimed first is left to it.
async fn enqueue_workflow_task(
    pool: &PgPool,
    workflow_id: Uuid,
    task: &DependentRow,
    dep_results: &HashMap<i32, DepResultValue>,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let task_id = crate::core::history::identity::uuid7::mint_task_id().map_err(|error| {
        WorkflowError::Validation(format!("task identity mint failed: {error}"))
    })?;
    let built = preparation(
        build_enqueued_task_params(
            pool,
            workflow_id,
            task,
            task_id,
            dep_results,
            payload,
            retention,
        )
        .await,
    )?;
    let params = match built {
        Preparation::Built(params) => params,
        Preparation::Refused(refusal) => {
            return claim_and_fail_node(
                pool,
                workflow_id,
                task.task_index,
                refusal.task_node_error(workflow_id, task.task_index),
                registry,
                payload,
                retention,
            )
            .await;
        }
    };

    // READY + INSERT + LINK in a single transaction: an insert that raises leaves the node PENDING,
    // and a LINK that matches no row (the workflow was paused or cancelled) leaves no orphaned
    // horsies_task row. Matches Python's atomic ENQUEUE_WORKFLOW_TASK_SQL pattern.
    let mut tx = pool.begin().await?;

    let claimed: Option<IdRow> = sqlx::query_as(UPDATE_WORKFLOW_TASK_READY_SQL)
        .bind(workflow_id)
        .bind(task.task_index)
        .fetch_optional(&mut *tx)
        .await?;
    if claimed.is_none() {
        // Already transitioned (race condition guard).
        tx.rollback().await?;
        return Ok(());
    }

    sqlx::query(ENQUEUE_TASK_SQL)
        .bind(params.task_id)
        .bind(&params.task_name)
        .bind(&params.queue_name)
        .bind(params.priority)
        .bind(&params.input)
        .bind(&params.task_options)
        .bind(params.facts.execution.as_str())
        .bind(&params.facts.retention_class_key)
        .bind(params.max_retries)
        .bind(workflow_id)
        .execute(&mut *tx)
        .await?;

    // Link workflow_task to the created horsies_tasks row.
    // Guards on wt.status = 'READY' AND w.status = 'RUNNING'.
    // If the workflow was paused/cancelled, this updates 0 rows and the
    // transaction rolls back (no orphaned horsies_task, and the node stays
    // PENDING; resume evaluates it again).
    let link_result = sqlx::query(LINK_ENQUEUED_TASK_SQL)
        .bind(&params.task_id)
        .bind(workflow_id)
        .bind(task.task_index)
        .execute(&mut *tx)
        .await?;

    if link_result.rows_affected() == 0 {
        // Workflow was paused/cancelled. Rollback.
        tx.rollback().await?;
        tracing::debug!(
            workflow_id = %workflow_id,
            task_index = task.task_index,
            "workflow task link failed (workflow no longer RUNNING), rolled back",
        );
        return Ok(());
    }

    tx.commit().await?;

    tracing::debug!(
        workflow_id = %workflow_id,
        task_index = task.task_index,
        task_id = %params.task_id,
        "workflow task enqueued",
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Sub-workflow enqueue and completion
// ---------------------------------------------------------------------------

/// Mark a promoted node FAILED and apply the failure: a node that the promotion refuses ([`NodeRefusal`]:
/// `WORKER_SERIALIZATION_ERROR` for a task node whose own data does not read, `WORKFLOW_ENQUEUE_FAILED` for a
/// sub-workflow node whose child cannot be built or started and for a node whose `args_from` names a member
/// of its static input), or a sub-workflow node whose child cannot start: its definition is not registered
/// under the node's key (`SUBWORKFLOW_LOAD_FAILED`), or the child would pass the depth limit
/// (`SUBWORKFLOW_DEPTH_LIMIT_REACHED`).
///
/// Mirrors Python's `_fail_enqueued_task` and `_fail_subworkflow_load`
/// (`horsies/core/workflows/engine.py:474-584`): writes FAILED via the terminal-state CAS guard
/// (`UPDATE_WORKFLOW_TASK_FAILED_SQL`), then runs the failure-propagation chain (handle failure policy →
/// process dependents → check completion). On a CAS-miss (row already terminal), skips propagation because
/// whichever path set the terminal state has already propagated the appropriate outcome.
///
/// Returns [`PromotionStep::WorkflowPaused`] when the failure paused the workflow (or found it paused), so a
/// caller that promotes more nodes stops.
pub(crate) async fn fail_workflow_node(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
    error: TaskError,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<PromotionStep, WorkflowError> {
    let error_code = Some(error.error_code.to_string());
    let error_json = serde_json::to_string(&error)?;
    let result_json = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(error))?;

    // Mark the workflow_task FAILED via the terminal-state CAS guard.
    let updated: Option<IdRow> = sqlx::query_as(UPDATE_WORKFLOW_TASK_FAILED_SQL)
        .bind(&result_json)
        .bind(workflow_id)
        .bind(task_index)
        .fetch_optional(pool)
        .await?;

    if updated.is_none() {
        // Already terminal — whichever path set it has already propagated.
        tracing::warn!(
            workflow_id = %workflow_id,
            task_index,
            "skipped failure mark for terminal workflow task",
        );
        return Ok(PromotionStep::Continue);
    }

    tracing::error!(
        workflow_id = %workflow_id,
        task_index,
        error_code = ?error_code,
        "workflow node could not be prepared or its child could not start; node marked FAILED",
    );

    // Run the failure-propagation chain (same shape as on_workflow_task_complete).
    let on_error: String =
        sqlx::query_scalar("SELECT on_error FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .fetch_one(pool)
            .await?;

    // This path marks FAILED via UPDATE_WORKFLOW_TASK_FAILED_SQL (no atomic
    // pause_wf), so the pause below still wins the RUNNING -> PAUSED transition:
    // wf_paused = false.
    let should_continue =
        handle_workflow_task_failure(pool, workflow_id, task_index, &on_error, &error_json, false)
            .await?;
    if !should_continue {
        // on_error=pause — workflow paused, no further processing.
        return Ok(PromotionStep::WorkflowPaused);
    }

    // PAUSED guard before propagating to dependents.
    let status_row: Option<WorkflowStatusRow> = sqlx::query_as(GET_WORKFLOW_STATUS_SQL)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;
    if status_row.as_ref().map(|row| row.status.as_str()) == Some("PAUSED") {
        return Ok(PromotionStep::WorkflowPaused);
    }

    process_dependents(pool, workflow_id, task_index, registry, payload, retention).await?;
    check_workflow_completion(pool, workflow_id, registry, payload, retention).await?;

    Ok(PromotionStep::Continue)
}

/// Fail a PENDING node that its promotion refused, or whose child cannot start: the READY mark
/// (`UPDATE_WORKFLOW_TASK_READY_SQL`) claims the node first, as on every promotion path, then
/// [`fail_workflow_node`] writes FAILED and applies the failure. A node that another promotion claimed
/// first, or that left PENDING, is left alone and nothing is written.
async fn claim_and_fail_node(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
    error: TaskError,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let claimed: Option<IdRow> = sqlx::query_as(UPDATE_WORKFLOW_TASK_READY_SQL)
        .bind(workflow_id)
        .bind(task_index)
        .fetch_optional(pool)
        .await?;
    match claimed {
        None => Ok(()),
        // The step's outcome (paused or not) has no further node to stop here.
        Some(_claimed) => fail_workflow_node(
            pool,
            workflow_id,
            task_index,
            error,
            registry,
            payload,
            retention,
        )
        .await
        .map(|_step| ()),
    }
}

/// Launch a child workflow for a PENDING sub-workflow node.
///
/// Looks up the child spec in the registry, builds it dynamically if a
/// `spec_builder` is registered (supporting runtime parameterization), then
/// marks the node READY, starts the child workflow and links the parent
/// workflow_task to it, in one transaction. A child that cannot be built, and a
/// sub-workflow root of the child that cannot start, fail the node alone
/// ([`claim_and_fail_node`]). Any other failure of the child start
/// ([`ChildStartStop::Failed`]: task options of the task function that do not
/// parse, a database error) rolls back the READY mark too: the node stays
/// PENDING and the error is returned (Python raises from
/// `enqueue_subworkflow_task` and its transaction rolls back,
/// `horsies/core/workflows/engine.py:1011-1025`).
async fn enqueue_subworkflow_task(
    pool: &PgPool,
    workflow_id: Uuid,
    task: &DependentRow,
    registry: &WorkflowSpecRegistry,
    dep_results: &HashMap<i32, DepResultValue>,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    // Resolve child workflow spec: try definition_key first, then name-based lookup.
    let spec_name = task
        .task_name
        .strip_prefix("__sub_workflow:")
        .or(task.sub_workflow_name.as_deref())
        .unwrap_or(&task.task_name);

    // Get parent depth and root_workflow_id. A child past the depth limit is not started: its node fails
    // (Python core/workflows/engine.py:1052-1066, checked before the definition load).
    let depth_row: DepthRow = sqlx::query_as(GET_WORKFLOW_DEPTH_SQL)
        .bind(workflow_id)
        .fetch_one(pool)
        .await?;
    let child_depth = match child_workflow_depth(
        depth_row.depth.unwrap_or(0),
        workflow_id,
        &depth_row.name,
        task.task_index,
    ) {
        Ok(child_depth) => child_depth,
        Err(error) => {
            return claim_and_fail_node(
                pool,
                workflow_id,
                task.task_index,
                error,
                registry,
                payload,
                retention,
            )
            .await;
        }
    };

    let Some(registered) = task
        .sub_definition_key
        .as_deref()
        .and_then(|key| registry.resolve_child_registration(key))
    else {
        // Child definition is not registered on this worker under the node's key. Mark the parent
        // workflow_task FAILED and propagate, rather than returning an error
        // that leaves the parent stuck and the workflow RUNNING
        // forever (parity with Python's _fail_subworkflow_load / PR #39).
        return claim_and_fail_node(
            pool,
            workflow_id,
            task.task_index,
            subworkflow_load_failed_error(
                &depth_row.name,
                task.task_index,
                task.sub_definition_key.as_deref(),
            ),
            registry,
            payload,
            retention,
        )
        .await;
    };

    // Build child spec: use spec_builder if available for dynamic parameterization,
    // otherwise use the static registered spec. A child that cannot be built or started,
    // or an `args_from` key that names a member of the static input, fails the node alone
    // with WORKFLOW_ENQUEUE_FAILED (Python `horsies/core/workflows/engine.py:1151-1215`).
    let built = preparation(prepare_child_spec(
        registered,
        workflow_id,
        task.task_index,
        merge_args_from(&task.input, &task.args_from, dep_results),
        task.args_from.as_ref(),
        task.sub_workflow_params.as_deref(),
        registry,
    ))?;
    let child_spec = match built {
        Preparation::Built(child_spec) => child_spec,
        Preparation::Refused(refusal) => {
            return claim_and_fail_node(
                pool,
                workflow_id,
                task.task_index,
                refusal.subworkflow_node_error(workflow_id, task.task_index, &depth_row.name),
                registry,
                payload,
                retention,
            )
            .await;
        }
    };

    let root_wf_id = depth_row.root_workflow_id.unwrap_or(workflow_id);

    // READY mark + child workflow + parent link in a single transaction.
    // Prevents orphaned child workflows if the parent is paused between
    // child creation and link (matching Python's atomic pattern), and a child
    // start that raises leaves the node PENDING.
    let mut tx = pool.begin().await?;

    let claimed: Option<IdRow> = sqlx::query_as(UPDATE_WORKFLOW_TASK_READY_SQL)
        .bind(workflow_id)
        .bind(task.task_index)
        .fetch_optional(&mut *tx)
        .await?;
    if claimed.is_none() {
        // Already transitioned (race condition guard).
        tx.rollback().await?;
        return Ok(());
    }

    let started = preparation(
        start_child_workflow_in_tx(
            &mut tx,
            &child_spec,
            workflow_id,
            task.task_index,
            child_depth,
            root_wf_id,
            registry,
            retention,
        )
        .await,
    )?;
    let child_id = match started {
        Preparation::Built(child_id) => child_id,
        Preparation::Refused(refusal) => {
            // No child row stays: the child's writes and the READY mark roll back
            // before the node fails.
            tx.rollback().await?;
            return claim_and_fail_node(
                pool,
                workflow_id,
                task.task_index,
                refusal.subworkflow_node_error(workflow_id, task.task_index, &depth_row.name),
                registry,
                payload,
                retention,
            )
            .await;
        }
    };

    // Link parent workflow_task to child workflow (inside same tx).
    // Guards on the node being READY AND the parent workflow being RUNNING — if
    // the parent was paused, this matches 0 rows and the whole transaction
    // (including the child workflow creation above) rolls back (C9).
    let link_result = sqlx::query(UPDATE_SUBWORKFLOW_LINK_SQL)
        .bind(&child_id)
        .bind(workflow_id)
        .bind(task.task_index)
        .execute(&mut *tx)
        .await?;

    if link_result.rows_affected() == 0 {
        tx.rollback().await?;
        tracing::debug!(
            workflow_id = %workflow_id,
            task_index = task.task_index,
            "sub-workflow link failed (workflow no longer RUNNING or task not READY), rolled back",
        );
        return Ok(());
    }

    tx.commit().await?;

    tracing::debug!(
        workflow_id = %workflow_id,
        task_index = task.task_index,
        child_workflow_id = %child_id,
        spec_name,
        "sub-workflow launched",
    );

    Ok(())
}

/// Characters of a child's stored error text that its summary keeps when the text is not a JSON object.
const CHILD_ERROR_SUMMARY_CHARS: usize = 200;

/// Task counts of a child workflow, for its summary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ChildTaskCounts {
    total: i32,
    completed: i32,
    failed: i32,
    skipped: i32,
}

impl ChildTaskCounts {
    fn from_rows(rows: &[StatusCount]) -> Self {
        rows.iter().fold(Self::default(), |counts, row| {
            let total = counts.total + row.cnt;
            match row.status.as_str() {
                "COMPLETED" => Self {
                    total,
                    completed: counts.completed + row.cnt,
                    ..counts
                },
                "FAILED" => Self {
                    total,
                    failed: counts.failed + row.cnt,
                    ..counts
                },
                "SKIPPED" => Self {
                    total,
                    skipped: counts.skipped + row.cnt,
                    ..counts
                },
                _ => Self { total, ..counts },
            }
        })
    }
}

/// The stored result of a child workflow, parsed. Python: `_ChildOutput`
/// (`core/workflows/engine.py:3110-3150`).
#[derive(Debug)]
enum ChildOutput {
    /// An ok value. `text` is the stored result, which the parent node copies byte for byte.
    Value {
        value: serde_json::Value,
        text: String,
    },
    /// An err value. `text` is the stored result.
    Error { error: TaskError, text: String },
    /// The child has no output node, so its stored result is for its own handle only.
    Outputless,
    /// The stored result is missing or does not parse.
    Unreadable { cause: String },
}

fn parse_child_output(stored_result: Option<&str>, has_output_node: bool) -> ChildOutput {
    match (stored_result, has_output_node) {
        (None | Some(""), _) => ChildOutput::Unreadable {
            cause: "the result is NULL or empty".to_owned(),
        },
        (Some(_), false) => ChildOutput::Outputless,
        (Some(text), true) => match serde_json::from_str::<TaskResult<serde_json::Value>>(text) {
            Ok(TaskResult::Ok(value)) => ChildOutput::Value {
                value,
                text: text.to_owned(),
            },
            Ok(TaskResult::Err(error)) => ChildOutput::Error {
                error,
                text: text.to_owned(),
            },
            Err(error) => ChildOutput::Unreadable {
                cause: error.to_string(),
            },
        },
    }
}

/// What a terminal child writes into its parent node. Python: `_ParentNodeOutcome`
/// (`core/workflows/engine.py:3152-3281`).
#[derive(Debug)]
enum ParentNodeOutcome {
    /// The node holds an ok.
    Completed { result_json: String },
    /// The node holds an err. `handler_error` goes to the failure handler, which stores it on a pause.
    Failed {
        result_json: String,
        handler_error: TaskError,
    },
}

impl ParentNodeOutcome {
    fn status(&self) -> &'static str {
        match self {
            Self::Completed { .. } => "COMPLETED",
            Self::Failed { .. } => "FAILED",
        }
    }

    fn result_json(&self) -> &str {
        match self {
            Self::Completed { result_json } | Self::Failed { result_json, .. } => result_json,
        }
    }
}

/// The summary and the parent node outcome of a terminal child.
///
/// - `child_status` is terminal; the callers check it first.
/// - A COMPLETED child gives a COMPLETED node only when its stored result is an ok value or the child has no
///   output node; an err output fails the node with that error; a stored result that is missing or does not
///   parse fails the node with `RESULT_DESERIALIZATION_ERROR` naming the child.
/// - A FAILED, CANCELLED or EXPIRED child fails the node with `SUBWORKFLOW_FAILED`.
fn terminal_child_outcome(
    child_workflow_id: Uuid,
    child_status: &str,
    child_result_json: Option<&str>,
    facts: &ChildWorkflowFacts,
    counts: &[StatusCount],
) -> Result<(SubWorkflowSummary, ParentNodeOutcome), serde_json::Error> {
    let child_output = parse_child_output(child_result_json, facts.output_task_index.is_some());
    let counts = ChildTaskCounts::from_rows(counts);
    let summary = SubWorkflowSummary {
        status: child_status.to_owned(),
        output: match &child_output {
            ChildOutput::Value { value, .. } => Some(value.clone()),
            ChildOutput::Error { .. }
            | ChildOutput::Outputless
            | ChildOutput::Unreadable { .. } => None,
        },
        total_tasks: counts.total,
        completed_tasks: counts.completed,
        failed_tasks: counts.failed,
        skipped_tasks: counts.skipped,
        error_summary: child_error_summary(facts.error.as_deref()),
    };
    let outcome = match child_status {
        "COMPLETED" => completed_child_node(child_workflow_id, child_output)?,
        _ => failed_child_node(child_workflow_id, child_status, &summary)?,
    };
    Ok((summary, outcome))
}

/// The parent node of a COMPLETED child. Python: `_completed_child_node`.
fn completed_child_node(
    child_workflow_id: Uuid,
    child_output: ChildOutput,
) -> Result<ParentNodeOutcome, serde_json::Error> {
    match child_output {
        ChildOutput::Value { text, .. } => Ok(ParentNodeOutcome::Completed { result_json: text }),
        ChildOutput::Outputless => Ok(ParentNodeOutcome::Completed {
            result_json: serde_json::to_string(&TaskResult::Ok(serde_json::Value::Null))?,
        }),
        ChildOutput::Error { error, text } => Ok(ParentNodeOutcome::Failed {
            result_json: text,
            handler_error: error,
        }),
        ChildOutput::Unreadable { cause } => {
            let error = TaskError {
                error_code: OperationalErrorCode::ResultDeserializationError.into(),
                message: Some(format!(
                    "Child workflow {child_workflow_id} is COMPLETED and its stored result does not parse: {cause}"
                )),
                exception: None,
                data: Some(serde_json::json!({ "workflow_id": child_workflow_id.to_string() })),
                kind: crate::core::TaskErrorKind::Plain,
            };
            Ok(ParentNodeOutcome::Failed {
                result_json: serde_json::to_string(&TaskResult::<serde_json::Value>::Err(
                    error.clone(),
                ))?,
                handler_error: error,
            })
        }
    }
}

/// The parent node of a FAILED, CANCELLED or EXPIRED child. Python: `_failed_child_node`: the node stores
/// "failed with status", the failure handler gets "failed".
fn failed_child_node(
    child_workflow_id: Uuid,
    child_status: &str,
    summary: &SubWorkflowSummary,
) -> Result<ParentNodeOutcome, serde_json::Error> {
    let node_error = subworkflow_failed_error(
        child_workflow_id,
        format!("Subworkflow {child_workflow_id} failed with status {child_status}"),
        summary,
    );
    Ok(ParentNodeOutcome::Failed {
        result_json: serde_json::to_string(&TaskResult::<serde_json::Value>::Err(node_error))?,
        handler_error: subworkflow_failed_error(
            child_workflow_id,
            format!("Subworkflow {child_workflow_id} failed"),
            summary,
        ),
    })
}

/// The `SUBWORKFLOW_FAILED` error of a parent node: Python's `SubWorkflowError`
/// (`core/workflows/engine.py:3238-3281`), with `sub_workflow_id` and `sub_workflow_summary` stored at the top
/// level of the error object and `data` null.
fn subworkflow_failed_error(
    child_workflow_id: Uuid,
    message: String,
    summary: &SubWorkflowSummary,
) -> TaskError {
    TaskError {
        exception: None,
        error_code: OutcomeCode::SubworkflowFailed.into(),
        data: None,
        message: Some(message),
        kind: TaskErrorKind::SubWorkflow(SubWorkflowFailure {
            sub_workflow_id: child_workflow_id,
            sub_workflow_summary: summary.clone(),
        }),
    }
}

/// The error summary of a child, from its stored workflow error, as Python takes it
/// (`core/workflows/engine.py:3355-3364`): the `message` of a JSON object; the first 200 characters of text that
/// is not JSON or is JSON `null`; none for other JSON and for no text.
fn child_error_summary(child_error: Option<&str>) -> Option<String> {
    let text = child_error.filter(|text| !text.is_empty())?;
    match crate::core::codec::from_json_str::<serde_json::Value>(text) {
        Ok(serde_json::Value::Object(fields)) => fields
            .get("message")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        Ok(serde_json::Value::Null) | Err(_) => {
            Some(text.chars().take(CHILD_ERROR_SUMMARY_CHARS).collect())
        }
        Ok(_) => None,
    }
}

/// Called when a child workflow reaches a terminal state.
///
/// Builds a SubWorkflowSummary, updates the parent workflow_task,
/// and triggers dependent processing on the parent.
pub async fn on_subworkflow_complete(
    pool: &PgPool,
    parent_workflow_id: Uuid,
    parent_task_index: i32,
    child_workflow_id: Uuid,
    child_status: &str,
    child_result_json: Option<&str>,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    // Fail closed on a non-terminal/unknown child status. A valid-but-non-terminal
    // status (e.g. RUNNING on a corrupt row) would otherwise be treated as a failure
    // and fail the parent node of a still-live child. Both call sites gate on
    // terminal children (engine emits COMPLETED/FAILED; recovery CASE1_6 filters
    // cw.status IN (COMPLETED,FAILED,CANCELLED)), so this only fires on corrupt rows.
    // `is_terminal_workflow_status` mirrors WORKFLOW_TERMINAL_STATES. Parity with
    // horsies PR #116.
    if !is_terminal_workflow_status(child_status) {
        tracing::error!(
            child_workflow_id = %child_workflow_id,
            parent_workflow_id = %parent_workflow_id,
            child_status,
            "subworkflow completion called with non-terminal/unknown status; \
             refusing to propagate to parent",
        );
        return Ok(());
    }

    // Count child task statuses for summary.
    let status_counts: Vec<StatusCount> = sqlx::query_as(COUNT_CHILD_TASK_STATUSES_SQL)
        .bind(child_workflow_id)
        .fetch_all(pool)
        .await?;
    let facts: Option<ChildWorkflowFacts> = sqlx::query_as(GET_CHILD_WORKFLOW_FACTS_SQL)
        .bind(child_workflow_id)
        .fetch_optional(pool)
        .await?;
    let Some(facts) = facts else {
        tracing::error!(
            child_workflow_id = %child_workflow_id,
            "child workflow not found; skipping sub-workflow progression",
        );
        return Ok(());
    };
    let (summary, outcome) = terminal_child_outcome(
        child_workflow_id,
        child_status,
        child_result_json,
        &facts,
        &status_counts,
    )?;
    let summary_json = serde_json::to_string(&summary)?;

    let updated = sqlx::query_as::<_, IdRow>(UPDATE_PARENT_NODE_RESULT_SQL)
        .bind(outcome.status())
        .bind(outcome.result_json())
        .bind(&summary_json)
        .bind(parent_workflow_id)
        .bind(parent_task_index)
        .fetch_optional(pool)
        .await?
        .is_some();
    if updated {
        tracing::debug!(
            parent_workflow_id = %parent_workflow_id,
            parent_task_index,
            child_workflow_id = %child_workflow_id,
            node_status = outcome.status(),
            "sub-workflow parent node written",
        );
    }

    if !updated {
        // Parent task already terminal — another worker handled this.
        tracing::debug!(
            parent_workflow_id = %parent_workflow_id,
            parent_task_index,
            child_workflow_id = %child_workflow_id,
            "parent workflow task already terminal, skipping",
        );
        return Ok(());
    }

    // Handle failure policy for sub-workflow failures (same as regular task failures).
    if let ParentNodeOutcome::Failed { handler_error, .. } = &outcome {
        let on_error: String =
            sqlx::query_scalar("SELECT on_error FROM horsies_workflows WHERE id = $1")
                .bind(parent_workflow_id)
                .fetch_one(pool)
                .await?;

        let error_json = serde_json::to_string(handler_error)?;

        // Sub-workflow failures mark the node via UPDATE_PARENT_NODE_RESULT_SQL
        // (no atomic pause_wf), so the pause still wins here: wf_paused = false.
        let should_continue = handle_workflow_task_failure(
            pool,
            parent_workflow_id,
            parent_task_index,
            &on_error,
            &error_json,
            false,
        )
        .await?;

        if !should_continue {
            // on_error=Pause — workflow paused, no further processing.
            return Ok(());
        }
    }

    // PAUSED guard: re-check workflow status before processing dependents.
    // Runs for both success and failure completions — a concurrent task failure
    // may have paused the workflow while this subworkflow was completing.
    let status_row: Option<WorkflowStatusRow> = sqlx::query_as(GET_WORKFLOW_STATUS_SQL)
        .bind(parent_workflow_id)
        .fetch_optional(pool)
        .await?;
    if let Some(row) = &status_row {
        if row.status == "PAUSED" {
            tracing::debug!(
                parent_workflow_id = %parent_workflow_id,
                parent_task_index,
                "parent workflow is PAUSED, skipping dependent processing",
            );
            return Ok(());
        }
    }

    // Process dependents on the parent workflow.
    process_dependents(
        pool,
        parent_workflow_id,
        parent_task_index,
        registry,
        payload,
        retention,
    )
    .await?;

    // Check if parent workflow is complete.
    check_workflow_completion(pool, parent_workflow_id, registry, payload, retention).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Failure handling
// ---------------------------------------------------------------------------

/// Handle a workflow task failure according to the on_error policy.
///
/// Returns `true` if the workflow should continue processing (OnError::Fail),
/// or `false` if the workflow was paused (OnError::Pause).
///
/// For `on_error=fail`, stores the triggering error on the workflow row
/// immediately (matching Python's behavior), using COALESCE to keep the
/// first error if multiple tasks fail concurrently.
///
/// For `on_error=pause`, stores the triggering error alongside the PAUSED
/// status transition.
async fn handle_workflow_task_failure(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
    on_error: &str,
    error_json: &str,
    // True when COMPLETE_WORKFLOW_TASK_SQL's `pause_wf` CTE already won the
    // RUNNING -> PAUSED transition atomically. The separate pause below then
    // no-ops, so this flag is what tells the pause arm it still owns the cascade.
    wf_paused: bool,
) -> Result<bool, WorkflowError> {
    // Lock the workflow row for the duration of failure handling so concurrent
    // failure handlers are serialized. This makes the stored running error the
    // deterministic first-failed-task error (by index) rather than a race on
    // whichever handler writes last. Mirrors Python PR #29.
    let mut tx = pool.begin().await?;
    let locked: Option<IdRow> = sqlx::query_as(LOCK_WORKFLOW_FOR_COMPLETION_SQL)
        .bind(workflow_id)
        .fetch_optional(&mut *tx)
        .await?;
    if locked.is_none() {
        // Workflow row is gone (e.g. cancelled/deleted). Nothing to handle;
        // continue dependency propagation.
        return Ok(true);
    }

    match on_error {
        "fail" => {
            // Store the first failed task's error (by index) for the still-RUNNING
            // workflow, recomputed under the lock so a later lower-index failure
            // replaces a higher-index one. The current task is already FAILED, so
            // this always resolves to a real error.
            // Python: `first_failure_payload or error_payload`.
            let first_error = get_workflow_failure_error(&mut tx, workflow_id, &None)
                .await?
                .unwrap_or_else(|| error_json.to_owned());
            sqlx::query(SET_WORKFLOW_ERROR_SQL)
                .bind(workflow_id)
                .bind(&first_error)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;

            tracing::debug!(
                workflow_id = %workflow_id,
                task_index,
                "on_error=fail, stored first failed error, continuing DAG resolution",
            );
            Ok(true)
        }
        "pause" => {
            // Pause the workflow immediately, storing the triggering error.
            let paused: Option<IdRow> = sqlx::query_as(PAUSE_WORKFLOW_WITH_ERROR_SQL)
                .bind(workflow_id)
                .bind(error_json)
                .fetch_optional(&mut *tx)
                .await?;

            tx.commit().await?;

            // This handler owns the cascade if it won the RUNNING -> PAUSED
            // transition — via this statement OR the atomic `pause_wf` CTE in
            // COMPLETE_WORKFLOW_TASK_SQL (`wf_paused`), which now typically wins
            // it first and leaves the statement above a no-op. Only a genuine
            // lost race (another actor paused/finalized the workflow before both)
            // skips the cascade.
            if paused.is_none() && !wf_paused {
                return Ok(false);
            }

            tracing::info!(
                workflow_id = %workflow_id,
                task_index,
                "on_error=pause, workflow paused with error stored",
            );

            // Cascade the implicit pause to running child workflows, matching
            // explicit pause behavior (mirrors Python PR #28).
            crate::workflow_engine::lifecycle::pause_tree_and_relocate(pool, workflow_id).await?;

            Ok(false)
        }
        _ => {
            tracing::warn!(
                workflow_id = %workflow_id,
                on_error,
                "unknown on_error policy, treating as fail",
            );
            // Store error even for unknown policy (same as fail).
            // Python: `first_failure_payload or error_payload`.
            let first_error = get_workflow_failure_error(&mut tx, workflow_id, &None)
                .await?
                .unwrap_or_else(|| error_json.to_owned());
            sqlx::query(SET_WORKFLOW_ERROR_SQL)
                .bind(workflow_id)
                .bind(&first_error)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(true)
        }
    }
}

// ---------------------------------------------------------------------------
// Completion check
// ---------------------------------------------------------------------------

/// Check if all workflow tasks are terminal. If so, evaluate success
/// and finalize the workflow. Uses `Box::pin` due to mutual recursion
/// with `on_subworkflow_complete`.
pub(crate) fn check_workflow_completion<'a>(
    pool: &'a PgPool,
    workflow_id: Uuid,
    registry: &'a WorkflowSpecRegistry,
    payload: &'a PayloadPolicy,
    retention: &'a RetentionConfig,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), WorkflowError>> + Send + 'a>> {
    Box::pin(async move {
        check_workflow_completion_inner(pool, workflow_id, registry, payload, retention, None)
            .await
            .map(|_| ())
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryClaimOutcome {
    Held,
    Lost,
}

pub(crate) fn check_workflow_completion_with_recovery_claim<'a>(
    pool: &'a PgPool,
    workflow_id: Uuid,
    registry: &'a WorkflowSpecRegistry,
    payload: &'a PayloadPolicy,
    retention: &'a RetentionConfig,
    claim_token: Uuid,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<RecoveryClaimOutcome, WorkflowError>> + Send + 'a>,
> {
    check_workflow_completion_inner(
        pool,
        workflow_id,
        registry,
        payload,
        retention,
        Some(claim_token),
    )
}

#[allow(clippy::explicit_auto_deref)]
fn check_workflow_completion_inner<'a>(
    pool: &'a PgPool,
    workflow_id: Uuid,
    registry: &'a WorkflowSpecRegistry,
    payload: &'a PayloadPolicy,
    retention: &'a RetentionConfig,
    recovery_claim_token: Option<Uuid>,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<RecoveryClaimOutcome, WorkflowError>> + Send + 'a>,
> {
    Box::pin(async move {
        // Normal completion uses an unlocked pre-check. Recovery first locks
        // its claim row and keeps that lock through the finalization transaction.
        // A stale non-zero normal pre-check is safe. It defers finalization to a
        // later completion check. The last task completion observes zero.
        let mut tx = match recovery_claim_token {
            Some(claim_token) => {
                let mut tx = pool.begin().await?;
                let claim_owned: bool = sqlx::query_scalar(LOCK_WORKFLOW_RECOVERY_CLAIM_SQL)
                    .bind(claim_token)
                    .fetch_optional(&mut *tx)
                    .await?
                    .unwrap_or(false);
                if !claim_owned {
                    tx.rollback().await?;
                    return Ok(RecoveryClaimOutcome::Lost);
                }
                let pre_count: NonTerminalCount = sqlx::query_as(COUNT_NON_TERMINAL_SQL)
                    .bind(workflow_id)
                    .fetch_one(&mut *tx)
                    .await?;
                if pre_count.cnt > 0 {
                    tx.commit().await?;
                    return Ok(RecoveryClaimOutcome::Held);
                }
                tx
            }
            None => {
                let pre_count: NonTerminalCount = sqlx::query_as(COUNT_NON_TERMINAL_SQL)
                    .bind(workflow_id)
                    .fetch_one(pool)
                    .await?;
                if pre_count.cnt > 0 {
                    return Ok(RecoveryClaimOutcome::Held);
                }
                pool.begin().await?
            }
        };

        // Looks complete. Lock and re-count to serialize concurrent finalizers
        // and guard against a task transition after the pre-check.

        // Lock workflow row — serializes concurrent completion checks.
        sqlx::query(LOCK_WORKFLOW_FOR_COMPLETION_SQL)
            .bind(workflow_id)
            .execute(&mut *tx)
            .await?;

        let count: NonTerminalCount = sqlx::query_as(COUNT_NON_TERMINAL_SQL)
            .bind(workflow_id)
            .fetch_one(&mut *tx)
            .await?;

        if count.cnt > 0 {
            tx.commit().await?;
            return Ok(RecoveryClaimOutcome::Held); // Still has non-terminal tasks.
        }

        // Only a RUNNING workflow may be finalized. PAUSED waits for manual
        // intervention; CANCELLED/COMPLETED/FAILED are terminal — a late task
        // completion must not resurrect them. (Rust sets completed_at on cancel,
        // so the completed_at guard on the mark SQL already blocks resurrection;
        // this short-circuit additionally avoids wasted finalize work on a
        // terminal workflow. Matches Python PR #101 ed61c3a2.)
        let meta: Option<WorkflowMeta> = sqlx::query_as(GET_WORKFLOW_META_SQL)
            .bind(workflow_id)
            .fetch_optional(&mut *tx)
            .await?;
        let meta = match meta {
            Some(meta) if meta.status == "RUNNING" => meta,
            _ => {
                tx.commit().await?;
                return Ok(RecoveryClaimOutcome::Held);
            }
        };

        // Count failures.
        let statuses: Vec<TaskStatusRow> = sqlx::query_as(ALL_TASK_STATUSES_SQL)
            .bind(workflow_id)
            .fetch_all(&mut *tx)
            .await?;

        let has_failure = statuses.iter().any(|s| s.status == "FAILED");

        let is_success = evaluate_workflow_success(
            &meta.success_policy,
            meta.has_stored_error,
            has_failure,
            &statuses,
        )?;

        let result_json = match is_success {
            true => {
                Some(get_workflow_final_result(&mut tx, workflow_id, meta.output_task_index).await?)
            }
            false => None,
        };

        // Attempt to finalize the workflow. Use RETURNING to detect if another
        // worker already finalized (prevents duplicate on_subworkflow_complete).
        let finalized = if is_success {
            let row: Option<IdRow> = sqlx::query_as(UPDATE_WORKFLOW_COMPLETED_SQL)
                .bind(workflow_id)
                .bind(&result_json)
                .fetch_optional(&mut *tx)
                .await?;

            if row.is_some() {
                tracing::info!(workflow_id = %workflow_id, "workflow completed successfully");
            }
            row.is_some()
        } else {
            // Compute error for the FAILED workflow deterministically from the
            // terminal task results (first failed task by index). Recovery uses
            // the same selection as normal completion. When no error can be
            // selected, the stored error stays (COALESCE in the statement), as
            // in Python.
            let error_json =
                get_workflow_failure_error(&mut *tx, workflow_id, &meta.success_policy).await?;

            let row: Option<IdRow> = sqlx::query_as(UPDATE_WORKFLOW_FAILED_SQL)
                .bind(workflow_id)
                .bind(None::<&str>) // result — not set on failure
                .bind(&error_json)
                .fetch_optional(&mut *tx)
                .await?;

            if row.is_some() {
                tracing::info!(workflow_id = %workflow_id, "workflow failed");
            }
            row.is_some()
        };

        // Check if this workflow has a parent (sub-workflow completion callback).
        let parent: ParentWorkflowRow = sqlx::query_as(GET_PARENT_WORKFLOW_SQL)
            .bind(workflow_id)
            .fetch_one(&mut *tx)
            .await?;

        // Commit transaction before recursive callback to avoid holding locks.
        tx.commit().await?;

        if !finalized {
            // Another worker already finalized this workflow — skip sub-workflow callback.
            tracing::debug!(workflow_id = %workflow_id, "workflow already finalized by another worker");
            return Ok(RecoveryClaimOutcome::Held);
        }

        if let (Some(ref parent_wf_id), Some(parent_idx)) =
            (&parent.parent_workflow_id, parent.parent_task_index)
        {
            let status_str = if is_success { "COMPLETED" } else { "FAILED" };
            on_subworkflow_complete(
                pool,
                *parent_wf_id,
                parent_idx,
                workflow_id,
                status_str,
                result_json.as_deref(),
                registry,
                payload,
                retention,
            )
            .await?;
        }

        Ok(RecoveryClaimOutcome::Held)
    })
}

/// The success policy that a workflow row stores, read at completion.
#[derive(Debug, PartialEq, Eq)]
enum StoredSuccessPolicy {
    /// SQL NULL or JSON null: a FAILED node or a stored workflow error fails the workflow.
    NoPolicy,
    /// The form that the workflow start stores.
    Policy(SuccessPolicy),
}

/// Read the stored `success_policy` column by one rule, the rule of Python `read_stored_success_policy`
/// (`core/workflows/stored_success_policy.py`) and of the baseline function `horsies_workflow_success_cases`
/// (`0005_workflow_functions.sql`): SQL NULL or JSON null is no policy; otherwise an object whose `cases` is an
/// array of objects, each with `required_indices`, an array of integers from 0; `optional_indices`, when
/// present and not null, the same kind of array. Other keys are ignored. Pure.
///
/// Errors: [`WorkflowError::Serialization`] for any other value. The step fails, and the recovery failure bound
/// then fails the workflow closed; no reader falls back to the default rule (decided rule).
fn read_stored_success_policy(
    stored: &Option<serde_json::Value>,
) -> Result<StoredSuccessPolicy, WorkflowError> {
    match stored {
        None | Some(serde_json::Value::Null) => Ok(StoredSuccessPolicy::NoPolicy),
        Some(value) => Ok(StoredSuccessPolicy::Policy(serde_json::from_value(
            value.clone(),
        )?)),
    }
}

/// The node indexes that some case of `policy` requires, as `task_index` values. An index above the `i32`
/// range names no node and is left out. Pure.
fn required_task_indexes(policy: &SuccessPolicy) -> Vec<i32> {
    policy
        .cases
        .iter()
        .flat_map(|case| case.required_indices.iter())
        .filter_map(|index| i32::try_from(*index).ok())
        .collect()
}

/// Evaluate whether the workflow should be considered successful.
///
/// Python: `evaluate_workflow_success` (`core/workflows/engine.py`). The stored policy is read with
/// [`read_stored_success_policy`].
/// - No policy: success only with no FAILED node and no stored workflow error.
/// - A policy: success when one case with at least one required node has every required node COMPLETED.
/// - A stored policy that does not parse is an error: the step fails, and the recovery failure bound then
///   fails the workflow closed.
fn evaluate_workflow_success(
    success_policy_json: &Option<serde_json::Value>,
    has_stored_error: bool,
    has_failure: bool,
    statuses: &[TaskStatusRow],
) -> Result<bool, WorkflowError> {
    match read_stored_success_policy(success_policy_json)? {
        StoredSuccessPolicy::NoPolicy => Ok(!has_stored_error && !has_failure),
        StoredSuccessPolicy::Policy(policy) => {
            let status_vec: Vec<WorkflowTaskStatus> = statuses
                .iter()
                .map(|s| parse_workflow_task_status(&s.status))
                .collect::<Result<_, _>>()?;

            Ok(policy.evaluate(&status_vec))
        }
    }
}

/// Get the workflow's final result JSON.
///
/// - An output node (`output_task_index`): the outcome of [`explicit_output_result`].
/// - No output node: [`outputless_result`] over the terminal nodes (nodes that no other node depends on).
async fn get_workflow_final_result(
    conn: &mut sqlx::PgConnection,
    workflow_id: Uuid,
    output_task_index: Option<i32>,
) -> Result<String, WorkflowError> {
    if let Some(idx) = output_task_index {
        let row: Option<NodeResult> = sqlx::query_as(GET_OUTPUT_NODE_SQL)
            .bind(workflow_id)
            .bind(idx)
            .fetch_optional(&mut *conn)
            .await?;
        return explicit_output_result(workflow_id, idx, row);
    }

    // Terminal output tasks = those no other task depends on. Read edges
    // payload-free, compute the terminal set in app code, then fetch exactly
    // those rows — avoiding a per-row GIN probe under the completion lock.
    let edges: Vec<EdgeRow> = sqlx::query_as(ALL_EDGES_SQL)
        .bind(workflow_id)
        .fetch_all(&mut *conn)
        .await?;

    let terminal_indexes = compute_terminal_indexes(&edges);

    let rows: Vec<NodeResult> = sqlx::query_as(TERMINAL_RESULTS_BY_INDEX_SQL)
        .bind(workflow_id)
        .bind(&terminal_indexes)
        .fetch_all(&mut *conn)
        .await?;

    Ok(serde_json::to_string(&outputless_result(
        workflow_id,
        &rows,
    )?)?)
}

/// The marker key of the result of a workflow with no output node (Python `__h_outputless_terminals__`,
/// `core/workflows/engine.py:3973-3983`).
pub(crate) const OUTPUTLESS_TERMINALS_KEY: &str = "__h_outputless_terminals__";

/// The stored result of a COMPLETED workflow with no output node, in Python's shape
/// (`core/workflows/engine.py:3935-3983`): the result envelope marker, the outputless marker, and an `ok`
/// slot with the entry, the task name and the definition key of each terminal node.
#[derive(Debug, serde::Serialize)]
struct OutputlessResult {
    #[serde(rename = "__h_task_result__")]
    result_marker: bool,
    #[serde(rename = "__h_outputless_terminals__")]
    outputless_marker: bool,
    ok: OutputlessTerminals,
    err: (),
}

#[derive(Debug, serde::Serialize)]
struct OutputlessTerminals {
    results_by_id: serde_json::Map<String, serde_json::Value>,
    task_name_by_id: std::collections::BTreeMap<String, String>,
    /// `null` for a task node.
    definition_key_by_id: std::collections::BTreeMap<String, Option<String>>,
}

/// The outputless result over the terminal node rows. Pure. A row with no node id has no entry.
fn outputless_result(
    workflow_id: Uuid,
    rows: &[NodeResult],
) -> Result<OutputlessResult, serde_json::Error> {
    let mut terminals = OutputlessTerminals {
        results_by_id: serde_json::Map::new(),
        task_name_by_id: std::collections::BTreeMap::new(),
        definition_key_by_id: std::collections::BTreeMap::new(),
    };
    for row in rows {
        let Some(node_id) = &row.node_id else {
            continue;
        };
        terminals
            .task_name_by_id
            .insert(node_id.clone(), row.task_name.clone());
        terminals
            .definition_key_by_id
            .insert(node_id.clone(), row.sub_definition_key.clone());
        terminals.results_by_id.insert(
            node_id.clone(),
            outputless_terminal_entry(workflow_id, node_id, row)?,
        );
    }
    Ok(OutputlessResult {
        result_marker: true,
        outputless_marker: true,
        ok: terminals,
        err: (),
    })
}

/// The entry of one terminal node of an outputless workflow (Python `_terminal_entry`,
/// `core/workflows/engine.py:3870-3907`):
/// - SKIPPED: an error envelope with `WORKFLOW_OUTPUT_SKIPPED`;
/// - a stored text that parses to an object with `__h_task_result__: true`: that object;
/// - any other row: `null`; the workflow handle reads it as `RESULT_DESERIALIZATION_ERROR`.
fn outputless_terminal_entry(
    workflow_id: Uuid,
    node_id: &str,
    row: &NodeResult,
) -> Result<serde_json::Value, serde_json::Error> {
    if row.status == "SKIPPED" {
        return serde_json::to_value(TaskResult::<serde_json::Value>::Err(output_error(
            OutcomeCode::WorkflowOutputSkipped.into(),
            format!(
                "Workflow {workflow_id} is COMPLETED and its terminal node {node_id:?} is SKIPPED"
            ),
            workflow_id,
            row,
        )));
    }
    let parsed = row
        .result
        .as_deref()
        .filter(|text| !text.is_empty())
        .and_then(|text| crate::core::codec::from_json_str::<serde_json::Value>(text).ok());
    match parsed {
        Some(value)
            if value.get(crate::core::task::result::TASK_RESULT_ENVELOPE_KEY)
                == Some(&serde_json::Value::Bool(true)) =>
        {
            Ok(value)
        }
        Some(_) | None => {
            tracing::warn!(
                workflow_id = %workflow_id,
                node_id,
                "terminal node result is not a result envelope; its outputless entry is null",
            );
            Ok(serde_json::Value::Null)
        }
    }
}

/// The result text of a COMPLETED workflow with an explicit output node. Python: `_explicit_output_result` and
/// `_stored_output_result` (`core/workflows/engine.py:3772-3907`).
///
/// - SKIPPED: an error envelope with `WORKFLOW_OUTPUT_SKIPPED`; the workflow succeeded and has no output.
/// - COMPLETED or FAILED with a stored result: that text, unchanged.
/// - COMPLETED or FAILED with no stored result: an error envelope with `RESULT_DESERIALIZATION_ERROR`.
/// - No node at the index, or a node that is not terminal: an error naming the workflow and the node. The
///   completion check counted no live node under the workflow lock, so either is a broken invariant.
fn explicit_output_result(
    workflow_id: Uuid,
    output_task_index: i32,
    row: Option<NodeResult>,
) -> Result<String, WorkflowError> {
    let Some(row) = row else {
        return Err(WorkflowError::Validation(format!(
            "workflow {workflow_id} has output_task_index {output_task_index} and no node row at that index"
        )));
    };
    let node_id = row.node_id.clone().unwrap_or_default();
    let error = match (row.status.as_str(), row.result.as_deref()) {
        ("SKIPPED", _) => output_error(
            OutcomeCode::WorkflowOutputSkipped.into(),
            format!(
                "Workflow {workflow_id} is COMPLETED and has no output: its output node {node_id:?} is SKIPPED"
            ),
            workflow_id,
            &row,
        ),
        ("COMPLETED" | "FAILED", Some(stored)) if !stored.is_empty() => {
            return Ok(stored.to_owned());
        }
        (status @ ("COMPLETED" | "FAILED"), _) => output_error(
            OperationalErrorCode::ResultDeserializationError.into(),
            format!(
                "Workflow {workflow_id} is COMPLETED and its output node {node_id:?} is {status} with no stored result"
            ),
            workflow_id,
            &row,
        ),
        (status, _) => {
            return Err(WorkflowError::Validation(format!(
                "workflow {workflow_id} output node {node_id:?} is {status} at workflow completion; \
                 the completion check counted no live node under the workflow lock"
            )));
        }
    };
    Ok(serde_json::to_string(
        &TaskResult::<serde_json::Value>::Err(error),
    )?)
}

/// The error of a COMPLETED workflow output that has no result: the explicit output node, or a terminal node
/// of an outputless workflow. `data` names the workflow and the node (Python `_output_error_envelope`).
fn output_error(
    error_code: crate::core::TaskErrorCode,
    message: String,
    workflow_id: Uuid,
    row: &NodeResult,
) -> TaskError {
    TaskError {
        error_code,
        message: Some(message),
        exception: None,
        data: Some(serde_json::json!({
            "workflow_id": workflow_id.to_string(),
            "output_node_id": row.node_id,
            "output_task_index": row.task_index,
            "output_task_name": row.task_name,
        })),
        kind: crate::core::TaskErrorKind::Plain,
    }
}

/// Get a specific error for a failed workflow.
///
/// Matches Python's `get_workflow_failure_error()` (`core/workflows/engine.py`). The stored policy is read
/// with [`read_stored_success_policy`]; one that does not parse is an error.
/// - Without success_policy: the first failed task's error (by index). With no such error, `None`: the
///   caller keeps the stored workflow error.
/// - With success_policy: the first failed required task's error, or WORKFLOW_SUCCESS_CASE_NOT_MET when no
///   required task failed.
async fn get_workflow_failure_error(
    conn: &mut sqlx::PgConnection,
    workflow_id: Uuid,
    success_policy_json: &Option<serde_json::Value>,
) -> Result<Option<String>, WorkflowError> {
    match read_stored_success_policy(success_policy_json)? {
        StoredSuccessPolicy::NoPolicy => {
            // Default: get first failed task's error by index.
            let row: Option<TaskResultOnly> = sqlx::query_as(FIRST_FAILED_TASK_ERROR_SQL)
                .bind(workflow_id)
                .fetch_optional(&mut *conn)
                .await?;

            if let Some(row) = row {
                if let Some(result_json) = &row.result {
                    // Extract the TaskError from the stored TaskResult.
                    if let Ok(TaskResult::<serde_json::Value>::Err(task_error)) =
                        serde_json::from_str::<TaskResult<serde_json::Value>>(result_json)
                    {
                        return Ok(Some(serde_json::to_string(&task_error)?));
                    }
                }
            }

            Ok(None)
        }
        StoredSuccessPolicy::Policy(policy) => {
            // With success_policy: find the first failed task that some case requires.
            let all_required = required_task_indexes(&policy);

            if !all_required.is_empty() {
                let row: Option<TaskResultOnly> =
                    sqlx::query_as(FIRST_FAILED_REQUIRED_TASK_ERROR_SQL)
                        .bind(workflow_id)
                        .bind(&all_required)
                        .fetch_optional(&mut *conn)
                        .await?;

                if let Some(row) = row {
                    if let Some(result_json) = &row.result {
                        if let Ok(TaskResult::<serde_json::Value>::Err(task_error)) =
                            serde_json::from_str::<TaskResult<serde_json::Value>>(result_json)
                        {
                            return Ok(Some(serde_json::to_string(&task_error)?));
                        }
                    }
                }
            }

            // No required task failed but no case satisfied (all SKIPPED?).
            Ok(Some(serde_json::to_string(&TaskError::builtin(
                OutcomeCode::WorkflowSuccessCaseNotMet,
                "No success case was satisfied",
            ))?))
        }
    }
}

// ---------------------------------------------------------------------------
// Dependency result fetching
// ---------------------------------------------------------------------------

/// Intermediate result for a dependency.
///
/// Re-exports as `args_merge::DepResult` for the shared merging logic.
pub type DepResultValue = crate::workflow_engine::args_merge::DepResult;

/// Fetch results for the given dependency indices.
async fn get_dependency_results(
    pool: &PgPool,
    workflow_id: Uuid,
    dep_indices: &[i32],
) -> Result<HashMap<i32, DepResultValue>, WorkflowError> {
    let rows: Vec<DepResult> = sqlx::query_as(GET_DEP_RESULTS_SQL)
        .bind(workflow_id)
        .bind(dep_indices)
        .fetch_all(pool)
        .await?;

    let mut map = HashMap::with_capacity(rows.len());
    for row in rows {
        map.insert(
            row.task_index,
            DepResultValue {
                node_id: row.node_id,
                task_name: row.task_name,
                sub_definition_key: row.sub_definition_key,
                status: row.status,
                result: row.result,
            },
        );
    }
    Ok(map)
}

use crate::workflow_engine::args_merge::input_object;
use crate::workflow_engine::args_merge::merge_args_from_sync as merge_args_from;
use crate::workflow_engine::args_merge::{ArgsFromConflict, ArgsFromMerge, MergedNodeKind};

/// The workflow context payload under `WORKFLOW_CTX_KWARG`, in Python's shape (`_build_workflow_context_data`,
/// `core/workflows/engine.py:1629-1721`): the result envelope, the task name and the definition key of each
/// context node, and the stored summary text of each sub-workflow node. One entry that does not read does not
/// stop the node: it holds a `RESULT_DESERIALIZATION_ERROR` that names the node (Python
/// `_decode_stored_task_result`, `:175-260`).
#[derive(Debug, serde::Serialize)]
struct WorkflowContextData {
    workflow_id: Uuid,
    task_index: i32,
    task_name: String,
    results_by_id: std::collections::BTreeMap<String, TaskResult<serde_json::Value>>,
    task_name_by_id: std::collections::BTreeMap<String, String>,
    /// `null` for a task node.
    definition_key_by_id: std::collections::BTreeMap<String, Option<String>>,
    /// The stored text, unparsed: the receiving task parses it (Python `core/worker/child_runner.py:1740-1757`).
    summaries_by_id: std::collections::BTreeMap<String, String>,
}

impl WorkflowContextData {
    /// The payload of a node from the rows of its terminal context nodes. Pure.
    fn from_rows(
        workflow_id: Uuid,
        task_index: i32,
        task_name: &str,
        rows: Vec<CtxResultRow>,
    ) -> Self {
        let mut context = Self {
            workflow_id,
            task_index,
            task_name: task_name.to_owned(),
            results_by_id: std::collections::BTreeMap::new(),
            task_name_by_id: std::collections::BTreeMap::new(),
            definition_key_by_id: std::collections::BTreeMap::new(),
            summaries_by_id: std::collections::BTreeMap::new(),
        };
        for row in rows {
            let Some(node_id) = row.node_id.clone() else {
                continue;
            };
            // Python: `get_dependency_results_with_names` (`core/workflows/engine.py`), with the decided entry
            // for a terminal node whose stored result is NULL or empty.
            let entry = match (row.status.as_str(), row.result.as_deref()) {
                ("SKIPPED", _) => TaskResult::Err(
                    crate::workflow_engine::args_merge::upstream_skipped_error(row.task_index),
                ),
                (_, Some(text)) if !text.is_empty() => stored_ctx_entry(workflow_id, &row, text),
                (status, _) => {
                    TaskResult::Err(crate::workflow_engine::args_merge::missing_result_error(
                        row.task_index,
                        Some(node_id.as_str()),
                        &row.task_name,
                        status,
                    ))
                }
            };
            context.results_by_id.insert(node_id.clone(), entry);
            context
                .task_name_by_id
                .insert(node_id.clone(), row.task_name.clone());
            context
                .definition_key_by_id
                .insert(node_id.clone(), row.sub_definition_key.clone());
            if let Some(summary) = row.sub_workflow_summary.filter(|text| !text.is_empty()) {
                context.summaries_by_id.insert(node_id, summary);
            }
        }
        context
    }

    fn to_payload(&self) -> Result<serde_json::Value, WorkflowError> {
        Ok(serde_json::to_value(self)?)
    }
}

/// The context entry of a node with a stored result (Python `_decode_stored_task_result`,
/// `core/workflows/engine.py:175-260`): the stored envelope, or a `RESULT_DESERIALIZATION_ERROR` naming the node
/// when the text is not strict JSON (`stage: json_parse`) or not a result envelope (`stage: decode`).
fn stored_ctx_entry(
    workflow_id: Uuid,
    row: &CtxResultRow,
    text: &str,
) -> TaskResult<serde_json::Value> {
    let unreadable = |stage: &str, message: String| -> TaskResult<serde_json::Value> {
        TaskResult::Err(TaskError {
            exception: None,
            error_code: OperationalErrorCode::ResultDeserializationError.into(),
            data: Some(serde_json::json!({
                "workflow_id": workflow_id.to_string(),
                "task_index": row.task_index,
                "status": row.status,
                "stage": stage,
            })),
            message: Some(message),
            kind: TaskErrorKind::Plain,
        })
    };
    let value: serde_json::Value = match crate::core::codec::from_json_str(text) {
        Ok(value) => value,
        Err(_) => {
            return unreadable(
                "json_parse",
                format!(
                    "Stored TaskResult JSON corrupt for task '{}'",
                    row.task_name
                ),
            )
        }
    };
    match serde_json::from_value::<TaskResult<serde_json::Value>>(value) {
        Ok(result) => result,
        Err(error) => unreadable(
            "decode",
            format!(
                "decode_task_result failed for task '{}': {error}",
                row.task_name
            ),
        ),
    }
}

async fn build_workflow_context_data(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
    task_name: &str,
    ctx_from_ids: &[String],
) -> Result<WorkflowContextData, WorkflowError> {
    if ctx_from_ids.is_empty() {
        return Ok(WorkflowContextData::from_rows(
            workflow_id,
            task_index,
            task_name,
            Vec::new(),
        ));
    }

    let rows: Vec<CtxResultRow> = sqlx::query_as(GET_CTX_RESULTS_BY_NODE_ID_SQL)
        .bind(workflow_id)
        .bind(ctx_from_ids)
        .fetch_all(pool)
        .await?;

    Ok(WorkflowContextData::from_rows(
        workflow_id,
        task_index,
        task_name,
        rows,
    ))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_join_type(s: &str) -> JoinType {
    match s {
        "all" => JoinType::All,
        "any" => JoinType::Any,
        "quorum" => JoinType::Quorum,
        unknown => {
            tracing::warn!(join_type = %unknown, "unknown join type, defaulting to All");
            JoinType::All
        }
    }
}

fn parse_workflow_task_status(s: &str) -> Result<WorkflowTaskStatus, WorkflowError> {
    match s {
        "PENDING" => Ok(WorkflowTaskStatus::Pending),
        "READY" => Ok(WorkflowTaskStatus::Ready),
        "ENQUEUED" => Ok(WorkflowTaskStatus::Enqueued),
        "RUNNING" => Ok(WorkflowTaskStatus::Running),
        "COMPLETED" => Ok(WorkflowTaskStatus::Completed),
        "FAILED" => Ok(WorkflowTaskStatus::Failed),
        "SKIPPED" => Ok(WorkflowTaskStatus::Skipped),
        unknown => Err(WorkflowError::InvalidStatus(unknown.to_owned())),
    }
}

fn is_terminal_workflow_status(s: &str) -> bool {
    matches!(s, "COMPLETED" | "FAILED" | "CANCELLED" | "EXPIRED")
}

/// Terminal task indexes = those no other task depends on:
/// `{all task_index} − ⋃(dependencies)`. Exactly equivalent to the old
/// `NOT EXISTS (other.dependencies @> ARRAY[wt.task_index])` query (a task in
/// its own `dependencies` is non-terminal under both forms). Parity with #117.
fn compute_terminal_indexes(edges: &[EdgeRow]) -> Vec<i32> {
    let depended_on: std::collections::HashSet<i32> = edges
        .iter()
        .flat_map(|e| e.dependencies.iter().copied())
        .collect();
    edges
        .iter()
        .map(|e| e.task_index)
        .filter(|idx| !depended_on.contains(idx))
        .collect()
}

#[cfg(test)]
mod guard_tests {
    use super::*;
    use crate::broker::PostgresBroker;
    use serial_test::serial;
    use uuid::Uuid;

    async fn insert_parent_workflow(pool: &PgPool, id: &str) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES (
                $1, 'guard_test_wf', 'RUNNING', 'fail', NULL,
                'test.guard.v1', 0, $1,
                NOW(), NOW(), NOW(), NOW()
            )",
        )
        .bind(test_uuid(id))
        .execute(pool)
        .await
        .expect("insert parent workflow");
    }

    async fn insert_subworkflow_task(pool: &PgPool, wf_id: &str, child_id: &str) {
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, sub_workflow_params,
                sub_workflow_id, created_at
            ) VALUES (
                'NONE', $1, $2, 0, 'node_0', 'subwf_node', 'null', 'default', 100, '{}', FALSE, 'all',
                'RUNNING', TRUE, 'NOT_GIVEN', $3, NOW()
            )",
        )
        .bind(Uuid::new_v4())
        .bind(test_uuid(wf_id))
        .bind(test_uuid(child_id))
        .execute(pool)
        .await
        .expect("insert subworkflow task");
    }

    async fn task_status(pool: &PgPool, wf_id: &str) -> String {
        sqlx::query_scalar(
            "SELECT status FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = 0",
        )
        .bind(test_uuid(wf_id))
        .fetch_one(pool)
        .await
        .expect("fetch wf_task status")
    }

    /// A non-terminal child status must not fail the parent node of a still-live
    /// child: the guard logs and returns without propagating. Parity with #116.
    #[tokio::test]
    #[serial]
    async fn non_terminal_child_status_does_not_fail_parent() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();

        let parent_id = Uuid::new_v4().to_string();
        let child_id = Uuid::new_v4().to_string();
        insert_parent_workflow(&pool, &parent_id).await;
        // The child workflow must exist (sub_workflow_id FK), even though the
        // guard returns before reading it.
        insert_parent_workflow(&pool, &child_id).await;
        insert_subworkflow_task(&pool, &parent_id, &child_id).await;

        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let result = on_subworkflow_complete(
            &pool,
            Uuid::parse_str(&parent_id).unwrap(),
            0,
            Uuid::parse_str(&child_id).unwrap(),
            "RUNNING",
            None,
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await;
        assert!(result.is_ok(), "guard must return Ok, got {:?}", result);
        assert_eq!(
            task_status(&pool, &parent_id).await,
            "RUNNING",
            "parent node must stay RUNNING (guard refused to propagate)",
        );

        cleanup(&pool, &parent_id, &child_id).await;
    }

    /// An unknown status string must also fail closed (no poison loop, no
    /// silent parent failure). Parity with #116.
    #[tokio::test]
    #[serial]
    async fn unknown_child_status_does_not_fail_parent() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();

        let parent_id = Uuid::new_v4().to_string();
        let child_id = Uuid::new_v4().to_string();
        insert_parent_workflow(&pool, &parent_id).await;
        insert_parent_workflow(&pool, &child_id).await;
        insert_subworkflow_task(&pool, &parent_id, &child_id).await;

        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let result = on_subworkflow_complete(
            &pool,
            Uuid::parse_str(&parent_id).unwrap(),
            0,
            Uuid::parse_str(&child_id).unwrap(),
            "GARBAGE",
            None,
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await;
        assert!(result.is_ok(), "guard must return Ok, got {:?}", result);
        assert_eq!(
            task_status(&pool, &parent_id).await,
            "RUNNING",
            "parent node must stay RUNNING (guard refused to propagate)",
        );

        cleanup(&pool, &parent_id, &child_id).await;
    }

    /// An outputless child (no output_index) must propagate `None` to the parent
    /// node, not its terminal-results map. Parity with horsies PR #141.
    #[tokio::test]
    #[serial]
    async fn outputless_subworkflow_propagates_none() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();

        let parent_id = Uuid::new_v4().to_string();
        let child_id = Uuid::new_v4().to_string();
        // insert_parent_workflow sets output_task_index NULL → outputless child.
        insert_parent_workflow(&pool, &parent_id).await;
        insert_parent_workflow(&pool, &child_id).await;
        insert_subworkflow_task(&pool, &parent_id, &child_id).await;

        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        // Child finalized with the terminal-results map (the outputless shape).
        let child_result = "{\"Ok\":{\"node_0\":{\"Ok\":42}}}";
        on_subworkflow_complete(
            &pool,
            Uuid::parse_str(&parent_id).unwrap(),
            0,
            Uuid::parse_str(&child_id).unwrap(),
            "COMPLETED",
            Some(child_result),
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("on_subworkflow_complete");

        let (result, summary_str): (Option<String>, Option<String>) = sqlx::query_as(
            "SELECT result, sub_workflow_summary FROM horsies_workflow_tasks \
             WHERE workflow_id = $1 AND task_index = 0",
        )
        .bind(test_uuid(&parent_id))
        .fetch_one(&pool)
        .await
        .expect("read parent node");

        let parsed: TaskResult<serde_json::Value> =
            serde_json::from_str(result.as_deref().expect("result")).expect("parse result");
        match parsed {
            TaskResult::Ok(v) => assert!(
                v.is_null(),
                "parent node result must be Ok(None), not the child terminal map; got {v}",
            ),
            TaskResult::Err(e) => panic!("expected Ok(null), got Err: {:?}", e),
        }

        let summary: SubWorkflowSummary =
            serde_json::from_str(&summary_str.expect("summary")).expect("parse summary");
        assert!(
            summary.output.is_none(),
            "summary.output must be None for an outputless child",
        );

        cleanup(&pool, &parent_id, &child_id).await;
    }

    async fn cleanup(pool: &PgPool, parent_id: &str, child_id: &str) {
        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(test_uuid(parent_id))
            .execute(pool)
            .await
            .expect("cleanup wf_tasks");
        sqlx::query("DELETE FROM horsies_workflows WHERE id = ANY($1)")
            .bind(vec![test_uuid(parent_id), test_uuid(child_id)])
            .execute(pool)
            .await
            .expect("cleanup workflows");
    }
}

#[cfg(test)]
mod completion_qw_tests {
    use super::*;
    use crate::broker::PostgresBroker;
    use serial_test::serial;
    use uuid::Uuid;

    fn edge(task_index: i32, dependencies: Vec<i32>) -> EdgeRow {
        EdgeRow {
            task_index,
            dependencies,
        }
    }

    fn sorted(mut v: Vec<i32>) -> Vec<i32> {
        v.sort_unstable();
        v
    }

    #[test]
    fn terminal_all_empty_deps_are_all_terminal() {
        let edges = vec![edge(0, vec![]), edge(1, vec![]), edge(2, vec![])];
        assert_eq!(sorted(compute_terminal_indexes(&edges)), vec![0, 1, 2]);
    }

    #[test]
    fn terminal_chain_only_last_is_terminal() {
        // 0 <- 1 <- 2 <- 3 (each depends on the previous).
        let edges = vec![
            edge(0, vec![]),
            edge(1, vec![0]),
            edge(2, vec![1]),
            edge(3, vec![2]),
        ];
        assert_eq!(sorted(compute_terminal_indexes(&edges)), vec![3]);
    }

    #[test]
    fn terminal_fan_in_join_is_terminal() {
        // 0 -> {1,2} -> 3 (3 is the join, nothing depends on it).
        let edges = vec![
            edge(0, vec![]),
            edge(1, vec![0]),
            edge(2, vec![0]),
            edge(3, vec![1, 2]),
        ];
        assert_eq!(sorted(compute_terminal_indexes(&edges)), vec![3]);
    }

    #[test]
    fn terminal_single_task_is_terminal() {
        assert_eq!(compute_terminal_indexes(&[edge(0, vec![])]), vec![0]);
    }

    /// Old correlated form, kept as the equivalence oracle.
    const OLD_TERMINAL_SQL: &str = "\
SELECT wt.task_index
FROM horsies_workflow_tasks wt
WHERE wt.workflow_id = $1
  AND NOT EXISTS (
      SELECT 1 FROM horsies_workflow_tasks other
      WHERE other.workflow_id = wt.workflow_id
        AND other.dependencies @> ARRAY[wt.task_index]
  )";

    async fn insert_workflow(pool: &PgPool, id: &str) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES (
                $1, 'qw_test_wf', 'RUNNING', 'fail', NULL,
                'test.qw.v1', 0, $1,
                NOW(), NOW(), NOW(), NOW()
            )",
        )
        .bind(test_uuid(id))
        .execute(pool)
        .await
        .expect("insert workflow");
    }

    async fn insert_task(pool: &PgPool, wf_id: &str, idx: i32, deps: Vec<i32>) {
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, created_at,
                max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, $3, $4, 'qw_task', 'null', 'default', 100, $5, FALSE, 'all',
                'COMPLETED', FALSE, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(test_uuid(wf_id))
        .bind(idx)
        .bind(format!("node_{idx}"))
        .bind(deps)
        .execute(pool)
        .await
        .expect("insert task");
    }

    /// The app-side terminal set equals the old correlated `NOT EXISTS` query
    /// for a representative DAG (fan-out → fan-in). Parity with horsies PR #117.
    #[tokio::test]
    #[serial]
    async fn terminal_set_matches_old_sql_oracle() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();

        let wf_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id).await;
        // 0 -> {1,2} -> 3 ; plus an independent leaf 4 (no deps, no dependents).
        insert_task(&pool, &wf_id, 0, vec![]).await;
        insert_task(&pool, &wf_id, 1, vec![0]).await;
        insert_task(&pool, &wf_id, 2, vec![0]).await;
        insert_task(&pool, &wf_id, 3, vec![1, 2]).await;
        insert_task(&pool, &wf_id, 4, vec![]).await;

        let oracle: Vec<i32> = sqlx::query_scalar(OLD_TERMINAL_SQL)
            .bind(test_uuid(&wf_id))
            .fetch_all(&pool)
            .await
            .expect("oracle query");

        let edges: Vec<EdgeRow> = sqlx::query_as(ALL_EDGES_SQL)
            .bind(test_uuid(&wf_id))
            .fetch_all(&pool)
            .await
            .expect("edge read");
        let computed = compute_terminal_indexes(&edges);

        assert_eq!(
            sorted(computed),
            sorted(oracle),
            "app-side terminal set must equal the old NOT EXISTS query",
        );

        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .expect("cleanup tasks");
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .expect("cleanup workflow");
    }

    /// C9: the sub-workflow link CAS must match 0 rows when the parent workflow
    /// is PAUSED, so a child cannot be linked/started under a paused parent.
    #[tokio::test]
    #[serial]
    async fn subworkflow_link_blocked_when_parent_paused() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let wf_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id).await;

        // A READY sub-workflow node.
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, sub_workflow_params, created_at
            ) VALUES (
                'NONE', $1, $2, 0, 'node_0', 'sub', 'null', 'default', 100, '{}', FALSE, 'all',
                'READY', TRUE, 'NOT_GIVEN', NOW()
            )",
        )
        .bind(Uuid::new_v4())
        .bind(test_uuid(&wf_id))
        .execute(&pool)
        .await
        .expect("insert subworkflow node");

        // A real child workflow (the link's sub_workflow_id has an FK to it).
        let child_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &child_id).await;

        // Parent PAUSED: the link must be a no-op (guard blocks it).
        sqlx::query("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = $1")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .unwrap();
        let blocked = sqlx::query(UPDATE_SUBWORKFLOW_LINK_SQL)
            .bind(test_uuid(&child_id))
            .bind(test_uuid(&wf_id))
            .bind(0_i32)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            blocked.rows_affected(),
            0,
            "sub-workflow link must not fire under a PAUSED parent",
        );

        // Parent RUNNING again: the link now applies.
        sqlx::query("UPDATE horsies_workflows SET status = 'RUNNING' WHERE id = $1")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .unwrap();
        let applied = sqlx::query(UPDATE_SUBWORKFLOW_LINK_SQL)
            .bind(test_uuid(&child_id))
            .bind(test_uuid(&wf_id))
            .bind(0_i32)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            applied.rows_affected(),
            1,
            "link must fire under a RUNNING parent"
        );

        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = ANY($1)")
            .bind(vec![test_uuid(&wf_id), test_uuid(&child_id)])
            .execute(&pool)
            .await
            .ok();
    }

    /// A child that ends after its parent ended still writes its parent node, as in Python
    /// (`UPDATE_PARENT_NODE_RESULT_SQL`, no parent status gate). Nothing is promoted: promotion and
    /// completion read only RUNNING workflows.
    #[tokio::test]
    #[serial]
    async fn a_parent_node_of_an_ended_parent_is_written() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let wf_id = Uuid::new_v4();

        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES ($1, 'ended_parent', 'CANCELLED', 'fail', NULL, $2, 0, $1,
                      NOW(), NOW(), NOW(), NOW())",
        )
        .bind(wf_id)
        .bind(format!("test.ended-parent.{wf_id}"))
        .execute(&pool)
        .await
        .expect("insert workflow");
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, sub_workflow_params, sub_workflow_name, sub_definition_key, created_at
            ) VALUES ('NONE', $1, $2, 0, 'node_0', 'child', '{}',
                      'default', 100, '{}', FALSE, 'all',
                      'RUNNING', TRUE, 'NOT_GIVEN', 'child', 'tests.child.v1', NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(wf_id)
        .execute(&pool)
        .await
        .expect("insert node");

        let result = serde_json::to_string(&TaskResult::Ok(serde_json::json!(7))).unwrap();
        let written = sqlx::query(UPDATE_PARENT_NODE_RESULT_SQL)
            .bind("COMPLETED")
            .bind(&result)
            .bind("{}")
            .bind(wf_id)
            .bind(0_i32)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(written.rows_affected(), 1);
        let (status, done): (String, bool) = sqlx::query_as(
            "SELECT status, completed_at IS NOT NULL FROM horsies_workflow_tasks
             WHERE workflow_id = $1",
        )
        .bind(wf_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!((status.as_str(), done), ("COMPLETED", true));

        // A terminal node is not written again.
        let again = sqlx::query(UPDATE_PARENT_NODE_RESULT_SQL)
            .bind("FAILED")
            .bind(&result)
            .bind("{}")
            .bind(wf_id)
            .bind(0_i32)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(again.rows_affected(), 0);
    }

    /// P3: the unlocked pre-check must still finalize an all-terminal workflow
    /// and leave one with a non-terminal task RUNNING (behavior preserved while
    /// dropping the lock-hold on the common early exit).
    #[tokio::test]
    #[serial]
    async fn completion_check_finalizes_all_terminal_and_defers_otherwise() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);

        async fn seed(pool: &sqlx::PgPool, wf: &str, node_status: &str) {
            sqlx::query(
                "INSERT INTO horsies_workflows (
                    id, name, status, on_error, output_task_index, definition_key, depth,
                    root_workflow_id, sent_at, created_at, started_at, updated_at
                ) VALUES ($1, 'p3_wf', 'RUNNING', 'fail', NULL, 'test.p3.v1', 0, $1,
                          NOW(), NOW(), NOW(), NOW())",
            )
            .bind(test_uuid(wf))
            .execute(pool)
            .await
            .expect("insert workflow");
            sqlx::query(
                "INSERT INTO horsies_workflow_tasks (
                    downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name,
                    priority, dependencies, allow_failed_deps, join_type, status,
                    is_subworkflow, created_at, max_retries, execution, retention_class_key
                ) VALUES (
                    'NONE', $1, $2, 0, 'node_0', 'p3_task', 'null', 'default', 100, '{}', FALSE, 'all',
                    $3, FALSE, NOW(), 0, 'ISOLATED', 'standard_30d'
                )",
            )
            .bind(Uuid::new_v4())
            .bind(test_uuid(wf))
            .bind(node_status)
            .execute(pool)
            .await
            .expect("insert node");
        }

        // All-terminal: finalizes to COMPLETED.
        let done = Uuid::new_v4().to_string();
        seed(&pool, &done, "COMPLETED").await;
        check_workflow_completion(
            &pool,
            Uuid::parse_str(&done).unwrap(),
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("completion check");
        let done_status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(test_uuid(&done))
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(done_status, "COMPLETED");

        // Non-terminal node: the pre-check defers, workflow stays RUNNING.
        let running = Uuid::new_v4().to_string();
        seed(&pool, &running, "RUNNING").await;
        check_workflow_completion(
            &pool,
            Uuid::parse_str(&running).unwrap(),
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("completion check");
        let running_status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(test_uuid(&running))
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(running_status, "RUNNING");

        for id in [&done, &running] {
            sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
                .bind(test_uuid(id))
                .execute(&pool)
                .await
                .ok();
            sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
                .bind(test_uuid(id))
                .execute(&pool)
                .await
                .ok();
        }
    }
}

#[cfg(test)]
mod promotion_batch_tests {
    //! Behavior pins for the batched skip-cascade promotion (parity with horsies
    //! PR #129): fan-out promotes every dependent in one level with task rows
    //! linked, and a skip chain cascades across levels to the same fixpoint as the
    //! per-node path.
    use super::*;
    use crate::broker::PostgresBroker;
    use serial_test::serial;
    use uuid::Uuid;

    async fn insert_workflow(pool: &PgPool, id: &str) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES (
                $1::uuid, 'promo_test_wf', 'RUNNING', 'fail', NULL,
                'test.promo.v1', 0, $1::uuid,
                NOW(), NOW(), NOW(), NOW()
            )",
        )
        .bind(test_uuid(id))
        .execute(pool)
        .await
        .expect("insert workflow");
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_node(
        pool: &PgPool,
        wf_id: &str,
        idx: i32,
        deps: Vec<i32>,
        status: &str,
        join_type: &str,
        allow_failed_deps: bool,
    ) {
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, created_at,
                max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1::uuid, $2::uuid, $3, $4, 'promo_task', 'null', 'default', 100, $5, $6, $7,
                $8, FALSE, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(test_uuid(wf_id))
        .bind(idx)
        .bind(format!("node_{idx}"))
        .bind(deps)
        .bind(allow_failed_deps)
        .bind(join_type)
        .bind(status)
        .execute(pool)
        .await
        .expect("insert node");
    }

    async fn node_status(pool: &PgPool, wf_id: &str, idx: i32) -> String {
        sqlx::query_scalar(
            "SELECT status FROM horsies_workflow_tasks WHERE workflow_id = $1::uuid AND task_index = $2",
        )
        .bind(test_uuid(wf_id))
        .bind(idx)
        .fetch_one(pool)
        .await
        .expect("node status")
    }

    async fn task_id_of(pool: &PgPool, wf_id: &str, idx: i32) -> Option<String> {
        sqlx::query_scalar(
            "SELECT task_id::text FROM horsies_workflow_tasks WHERE workflow_id = $1::uuid AND task_index = $2",
        )
        .bind(test_uuid(wf_id))
        .bind(idx)
        .fetch_one(pool)
        .await
        .expect("task_id")
    }

    async fn cleanup(pool: &PgPool, wf_id: &str) {
        sqlx::query("DELETE FROM horsies_tasks WHERE is_workflow_task AND id IN (SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1::uuid)")
            .bind(test_uuid(wf_id))
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1::uuid")
            .bind(test_uuid(wf_id))
            .execute(pool)
            .await
            .expect("cleanup tasks");
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1::uuid")
            .bind(test_uuid(wf_id))
            .execute(pool)
            .await
            .expect("cleanup workflow");
    }

    /// Fan-out: one completed root unblocks F dependents; all are promoted to
    /// ENQUEUED with a linked horsies_tasks row in a single level.
    #[tokio::test]
    #[serial]
    async fn p6_workflow_enqueue_engine_batch_persists_never_eligible_facts() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let wf_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id).await;
        insert_node(&pool, &wf_id, 0, vec![], "COMPLETED", "all", false).await;
        let fan = 6;
        for idx in 1..=fan {
            insert_node(&pool, &wf_id, idx, vec![0], "PENDING", "all", false).await;
        }

        process_dependents(
            &pool,
            Uuid::parse_str(&wf_id).unwrap(),
            0,
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("process dependents");

        for idx in 1..=fan {
            assert_eq!(
                node_status(&pool, &wf_id, idx).await,
                "ENQUEUED",
                "node {idx}"
            );
            assert!(
                task_id_of(&pool, &wf_id, idx).await.is_some(),
                "node {idx} must be linked to a task row",
            );
        }
        let task_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_tasks WHERE id IN \
             (SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1::uuid)",
        )
        .bind(test_uuid(&wf_id))
        .fetch_one(&pool)
        .await
        .expect("count");
        assert_eq!(task_count, fan as i64, "one task row per promoted node");
        let conformant_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_tasks
             WHERE id IN (
                 SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1::uuid
             )
               AND retention_class_key = 'standard_30d'
               AND command_fingerprint IS NULL
               AND command_fingerprint_version IS NULL
               AND rerun_of_task_id IS NULL
               AND rerun_root_task_id IS NULL
               AND idempotency_key_digest IS NULL
               AND retain_rerun_input = FALSE",
        )
        .bind(test_uuid(&wf_id))
        .fetch_one(&pool)
        .await
        .expect("count conformant promoted task facts");
        assert_eq!(conformant_count, fan as i64);

        cleanup(&pool, &wf_id).await;
    }

    /// Gives node `idx` the static input `{"x": 5}` and `args_from` `{"x": 0}`: the `args_from` key
    /// names a member of the static input.
    async fn set_conflicting_args_from(pool: &PgPool, wf_id: &str, idx: i32) {
        sqlx::query(
            r#"UPDATE horsies_workflow_tasks
               SET input = '{"x": 5}', args_from = '{"x": 0}'::jsonb
               WHERE workflow_id = $1::uuid AND task_index = $2"#,
        )
        .bind(test_uuid(wf_id))
        .bind(idx)
        .execute(pool)
        .await
        .expect("set a conflicting args_from");
    }

    /// The status and the stored error of a node.
    async fn failed_node(pool: &PgPool, wf_id: &str, idx: i32) -> (String, TaskError) {
        let (status, result): (String, Option<String>) = sqlx::query_as(
            "SELECT status, result FROM horsies_workflow_tasks
             WHERE workflow_id = $1::uuid AND task_index = $2",
        )
        .bind(test_uuid(wf_id))
        .bind(idx)
        .fetch_one(pool)
        .await
        .expect("node status and result");
        let result = result.unwrap_or_else(|| panic!("node {idx} has no stored result"));
        match serde_json::from_str::<TaskResult<serde_json::Value>>(&result)
            .expect("stored node result")
        {
            TaskResult::Err(error) => (status, error),
            TaskResult::Ok(value) => panic!("node {idx} holds an ok result: {value}"),
        }
    }

    /// The node failed with Python's conflict error (`core/workflows/engine.py:834-839`, `:1151-1163`).
    fn assert_conflict_error(error: &TaskError, wf_id: &str, idx: i32, message: &str) {
        assert_eq!(error.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
        assert_eq!(error.message.as_deref(), Some(message));
        assert_eq!(
            error.data,
            Some(serde_json::json!({"workflow_id": wf_id, "task_index": idx}))
        );
    }

    /// On the batched level, a plain dependent whose `args_from` names a member of its static
    /// input fails with the conflict error and gets no task row; its sibling is enqueued
    /// (`on_error=fail`).
    #[tokio::test]
    #[serial]
    async fn an_args_from_conflict_fails_the_node_on_the_batched_level() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let wf_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id).await;
        insert_node(&pool, &wf_id, 0, vec![], "COMPLETED", "all", false).await;
        insert_node(&pool, &wf_id, 1, vec![0], "PENDING", "all", false).await;
        insert_node(&pool, &wf_id, 2, vec![0], "PENDING", "all", false).await;
        set_conflicting_args_from(&pool, &wf_id, 1).await;

        process_dependents(
            &pool,
            test_uuid(&wf_id),
            0,
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("process dependents");

        let (status, error) = failed_node(&pool, &wf_id, 1).await;
        assert_eq!(status, "FAILED");
        assert_conflict_error(
            &error,
            &wf_id,
            1,
            "args_from key 'x' conflicts with static kwarg (task 1)",
        );
        assert!(task_id_of(&pool, &wf_id, 1).await.is_none());
        assert_eq!(node_status(&pool, &wf_id, 2).await, "ENQUEUED");
        assert!(task_id_of(&pool, &wf_id, 2).await.is_some());

        cleanup(&pool, &wf_id).await;
    }

    /// With `on_error=pause`, the conflict pauses the workflow before the level writes: the level
    /// promotes no node, so the sibling stays PENDING with no task row for resume. Python's level
    /// moves its siblings back to READY and inserts no task row.
    #[tokio::test]
    #[serial]
    async fn an_args_from_conflict_that_pauses_leaves_the_level_without_task_rows() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let wf_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id).await;
        sqlx::query("UPDATE horsies_workflows SET on_error = 'pause' WHERE id = $1::uuid")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .expect("on_error=pause");
        insert_node(&pool, &wf_id, 0, vec![], "COMPLETED", "all", false).await;
        insert_node(&pool, &wf_id, 1, vec![0], "PENDING", "all", false).await;
        insert_node(&pool, &wf_id, 2, vec![0], "PENDING", "all", false).await;
        set_conflicting_args_from(&pool, &wf_id, 1).await;

        process_dependents(
            &pool,
            test_uuid(&wf_id),
            0,
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("process dependents");

        let workflow_status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1::uuid")
                .bind(test_uuid(&wf_id))
                .fetch_one(&pool)
                .await
                .expect("workflow status");
        assert_eq!(workflow_status, "PAUSED");
        assert_eq!(failed_node(&pool, &wf_id, 1).await.0, "FAILED");
        assert_eq!(node_status(&pool, &wf_id, 2).await, "PENDING");
        assert!(task_id_of(&pool, &wf_id, 2).await.is_none());

        cleanup(&pool, &wf_id).await;
    }

    /// A node of the batched level whose stored options are not a JSON object fails alone with
    /// `WORKER_SERIALIZATION_ERROR` before the level writes (Python
    /// `horsies/core/workflows/engine.py:798-805`). With `on_error=pause` the failure pauses the
    /// workflow and ends the level: the sibling stays PENDING and gets no task row (Python
    /// `_enqueue_promoted`, `horsies/core/workflows/engine.py:2331-2378`, inserts no task row for
    /// the level).
    #[tokio::test]
    #[serial]
    async fn a_refused_node_that_pauses_the_workflow_ends_the_level_before_it_writes() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let wf_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id).await;
        sqlx::query("UPDATE horsies_workflows SET on_error = 'pause' WHERE id = $1::uuid")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .expect("on_error=pause");
        insert_node(&pool, &wf_id, 0, vec![], "COMPLETED", "all", false).await;
        insert_node(&pool, &wf_id, 1, vec![0], "PENDING", "all", false).await;
        insert_node(&pool, &wf_id, 2, vec![0], "PENDING", "all", false).await;
        sqlx::query(
            "UPDATE horsies_workflow_tasks SET task_options = '[]'
             WHERE workflow_id = $1::uuid AND task_index = 1",
        )
        .bind(test_uuid(&wf_id))
        .execute(&pool)
        .await
        .expect("store options that are not a JSON object");

        process_dependents(
            &pool,
            test_uuid(&wf_id),
            0,
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("process dependents");

        let workflow_status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1::uuid")
                .bind(test_uuid(&wf_id))
                .fetch_one(&pool)
                .await
                .expect("workflow status");
        assert_eq!(workflow_status, "PAUSED");
        let (status, error) = failed_node(&pool, &wf_id, 1).await;
        assert_eq!(status, "FAILED");
        assert_eq!(error.error_code.to_string(), "WORKER_SERIALIZATION_ERROR");
        assert_eq!(
            error.data,
            Some(serde_json::json!({"workflow_id": wf_id.as_str(), "task_index": 1}))
        );
        assert!(task_id_of(&pool, &wf_id, 1).await.is_none());
        assert_eq!(node_status(&pool, &wf_id, 2).await, "PENDING");
        assert!(task_id_of(&pool, &wf_id, 2).await.is_none());

        cleanup(&pool, &wf_id).await;
    }

    /// A node of the batched level whose stored `good_until` the database cannot read is not
    /// refused: the bulk insert casts the text and raises (22007), and the level's transaction
    /// rolls back, so no node of the level is promoted and no task row stays (Python passes
    /// `str()` of the member to its insert, `horsies/core/workflows/engine.py:831-835`, and the
    /// step rolls back).
    #[tokio::test]
    #[serial]
    async fn a_stored_deadline_that_does_not_read_raises_from_the_level_insert() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let wf_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id).await;
        insert_node(&pool, &wf_id, 0, vec![], "COMPLETED", "all", false).await;
        insert_node(&pool, &wf_id, 1, vec![0], "PENDING", "all", false).await;
        insert_node(&pool, &wf_id, 2, vec![0], "PENDING", "all", false).await;
        sqlx::query(
            r#"UPDATE horsies_workflow_tasks SET task_options = '{"good_until": "not-a-date"}'
               WHERE workflow_id = $1::uuid AND task_index = 1"#,
        )
        .bind(test_uuid(&wf_id))
        .execute(&pool)
        .await
        .expect("store a deadline that the database cannot read");

        let error = process_dependents(
            &pool,
            test_uuid(&wf_id),
            0,
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect_err("the level insert raises");

        match error {
            WorkflowError::Database(error) => assert_eq!(
                error
                    .as_database_error()
                    .and_then(|database_error| database_error.code())
                    .as_deref(),
                Some("22007"),
                "{error}"
            ),
            other => panic!("expected the database refusal of the deadline, got {other:?}"),
        }
        for task_index in [1, 2] {
            assert_eq!(node_status(&pool, &wf_id, task_index).await, "PENDING");
            assert!(task_id_of(&pool, &wf_id, task_index).await.is_none());
        }

        cleanup(&pool, &wf_id).await;
    }

    /// A sub-workflow node with an `args_from` conflict fails before its child starts: no child
    /// workflow row is written.
    #[tokio::test]
    #[serial]
    async fn an_args_from_conflict_fails_a_subworkflow_node_before_its_child_starts() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let child_key = "tests.args_from_conflict.child.v1";
        let mut registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        registry
            .register(crate::core::registry::workflow::RegisteredWorkflowSpec {
                spec: crate::core::WorkflowSpec {
                    name: "args_from_conflict_child".to_owned(),
                    definition_key: Some(child_key.to_owned()),
                    tasks: Vec::new(),
                    on_error: crate::core::OnError::Fail,
                    output_index: None,
                    success_policy: None,
                },
                spec_builder: None,
            })
            .expect("register the child definition");
        let wf_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id).await;
        insert_node(&pool, &wf_id, 0, vec![], "COMPLETED", "all", false).await;
        sqlx::query(
            r#"INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, args_from, allow_failed_deps, join_type, status, is_subworkflow,
                sub_workflow_params, sub_workflow_name, sub_definition_key, created_at
            ) VALUES (
                'NONE', $1, $2::uuid, 1, 'child', 'args_from_conflict_child', '{"x": 5}', 'default',
                100, '{0}', '{"x": 0}'::jsonb, FALSE, 'all', 'PENDING', TRUE, 'GIVEN',
                'args_from_conflict_child', $3, NOW()
            )"#,
        )
        .bind(Uuid::new_v4())
        .bind(test_uuid(&wf_id))
        .bind(child_key)
        .execute(&pool)
        .await
        .expect("insert the sub-workflow node");

        process_dependents(
            &pool,
            test_uuid(&wf_id),
            0,
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("process dependents");

        let (status, error) = failed_node(&pool, &wf_id, 1).await;
        assert_eq!(status, "FAILED");
        assert_conflict_error(
            &error,
            &wf_id,
            1,
            "args_from key 'x' conflicts with static kwarg (subworkflow task 1)",
        );
        let children: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_workflows WHERE parent_workflow_id = $1::uuid",
        )
        .bind(test_uuid(&wf_id))
        .fetch_one(&pool)
        .await
        .expect("count child workflows");
        assert_eq!(children, 0);

        cleanup(&pool, &wf_id).await;
    }

    /// Every promoted task row stores the start call time of its workflow (`horsies_workflows.sent_at`) as
    /// its `sent_at`: the batched level (node 1) and the per-node path (node 2, which reads the workflow
    /// context).
    #[tokio::test]
    #[serial]
    async fn a_promoted_task_row_stores_the_start_call_time_of_its_workflow() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let started = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("fixed start call time")
            .with_timezone(&chrono::Utc);
        let level_wf = Uuid::new_v4().to_string();
        insert_workflow(&pool, &level_wf).await;
        sqlx::query("UPDATE horsies_workflows SET sent_at = $2 WHERE id = $1::uuid")
            .bind(test_uuid(&level_wf))
            .bind(started)
            .execute(&pool)
            .await
            .expect("set the start call time");
        insert_node(&pool, &level_wf, 0, vec![], "COMPLETED", "all", false).await;
        insert_node(&pool, &level_wf, 1, vec![0], "PENDING", "all", false).await;
        insert_node(&pool, &level_wf, 2, vec![0], "PENDING", "all", false).await;
        // A context member merges into an object input.
        sqlx::query(
            "UPDATE horsies_workflow_tasks SET workflow_ctx_from = ARRAY['node_0'], input = '{}'
             WHERE workflow_id = $1::uuid AND task_index = 2",
        )
        .bind(test_uuid(&level_wf))
        .execute(&pool)
        .await
        .expect("node 2 reads the workflow context");

        process_dependents(
            &pool,
            test_uuid(&level_wf),
            0,
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("process dependents");

        let stored: Vec<(String, i32, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
            "SELECT wt.workflow_id::text, wt.task_index, t.sent_at
             FROM horsies_workflow_tasks wt
             JOIN horsies_tasks t ON t.id = wt.task_id
             WHERE wt.workflow_id = ANY($1::uuid[])
             ORDER BY 1, 2",
        )
        .bind(vec![test_uuid(&level_wf)])
        .fetch_all(&pool)
        .await
        .expect("read the promoted task rows");
        assert_eq!(stored.len(), 2, "two promoted task rows: {stored:?}");
        for (workflow_id, task_index, sent_at) in stored {
            assert_eq!(sent_at, started, "{workflow_id}:{task_index}");
        }

        cleanup(&pool, &level_wf).await;
    }

    /// Multi-level skip cascade: a failed root skips a chain of join='all'
    /// dependents across levels (1 -> 2 -> 3), driven by the level loop.
    #[tokio::test]
    #[serial]
    async fn skip_cascade_resolves_chain() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let wf_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id).await;
        insert_node(&pool, &wf_id, 0, vec![], "FAILED", "all", false).await;
        insert_node(&pool, &wf_id, 1, vec![0], "PENDING", "all", false).await;
        insert_node(&pool, &wf_id, 2, vec![1], "PENDING", "all", false).await;
        insert_node(&pool, &wf_id, 3, vec![2], "PENDING", "all", false).await;

        process_dependents(
            &pool,
            Uuid::parse_str(&wf_id).unwrap(),
            0,
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("process dependents");

        for idx in 1..=3 {
            assert_eq!(
                node_status(&pool, &wf_id, idx).await,
                "SKIPPED",
                "node {idx}"
            );
        }

        cleanup(&pool, &wf_id).await;
    }

    /// A promoted node holds its enqueue time as `started_at` on the batched level (node 1) and the
    /// per-node path (node 2, which reads the workflow context). The node write and the task insert share
    /// a transaction, so the time equals `enqueued_at` of the task row.
    #[tokio::test]
    #[serial]
    async fn a_promoted_node_holds_its_enqueue_time() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let level_wf = Uuid::new_v4().to_string();
        insert_workflow(&pool, &level_wf).await;
        insert_node(&pool, &level_wf, 0, vec![], "COMPLETED", "all", false).await;
        insert_node(&pool, &level_wf, 1, vec![0], "PENDING", "all", false).await;
        insert_node(&pool, &level_wf, 2, vec![0], "PENDING", "all", false).await;
        // A context member merges into an object input.
        sqlx::query(
            "UPDATE horsies_workflow_tasks SET workflow_ctx_from = ARRAY['node_0'], input = '{}'
             WHERE workflow_id = $1::uuid AND task_index = 2",
        )
        .bind(test_uuid(&level_wf))
        .execute(&pool)
        .await
        .expect("node 2 reads the workflow context");

        process_dependents(
            &pool,
            test_uuid(&level_wf),
            0,
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("process dependents");

        let nodes: Vec<(String, i32, String, bool)> = sqlx::query_as(
            "SELECT wt.workflow_id::text, wt.task_index, wt.status,
                    wt.started_at IS NOT NULL AND wt.started_at = t.enqueued_at
             FROM horsies_workflow_tasks wt
             JOIN horsies_tasks t ON t.id = wt.task_id
             WHERE wt.workflow_id = ANY($1::uuid[])
             ORDER BY 1, 2",
        )
        .bind(vec![test_uuid(&level_wf)])
        .fetch_all(&pool)
        .await
        .expect("read the promoted nodes");
        assert_eq!(nodes.len(), 2, "two promoted nodes: {nodes:?}");
        for (workflow_id, task_index, status, holds_enqueue_time) in nodes {
            assert_eq!(status, "ENQUEUED", "{workflow_id}:{task_index}");
            assert!(holds_enqueue_time, "{workflow_id}:{task_index}");
        }

        cleanup(&pool, &level_wf).await;
    }

    /// allow_failed_deps: a failed dep still promotes the dependent (not skipped).
    #[tokio::test]
    #[serial]
    async fn allow_failed_deps_promotes() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let wf_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id).await;
        insert_node(&pool, &wf_id, 0, vec![], "FAILED", "all", false).await;
        insert_node(&pool, &wf_id, 1, vec![0], "PENDING", "all", true).await;

        process_dependents(
            &pool,
            Uuid::parse_str(&wf_id).unwrap(),
            0,
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("process dependents");

        assert_eq!(node_status(&pool, &wf_id, 1).await, "ENQUEUED");
        assert!(task_id_of(&pool, &wf_id, 1).await.is_some());

        cleanup(&pool, &wf_id).await;
    }

    /// A paused workflow promotes nothing: the eval's RUNNING guard returns no
    /// dependents.
    #[tokio::test]
    #[serial]
    async fn paused_workflow_promotes_nothing() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let wf_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id).await;
        sqlx::query("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = $1")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .expect("pause");
        insert_node(&pool, &wf_id, 0, vec![], "COMPLETED", "all", false).await;
        insert_node(&pool, &wf_id, 1, vec![0], "PENDING", "all", false).await;

        process_dependents(
            &pool,
            Uuid::parse_str(&wf_id).unwrap(),
            0,
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("process dependents");

        assert_eq!(
            node_status(&pool, &wf_id, 1).await,
            "PENDING",
            "stays pending under pause"
        );

        cleanup(&pool, &wf_id).await;
    }
}

#[cfg(test)]
mod finalization_contract_tests {
    use super::*;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn finalization_preserves_explicit_and_default_outputs() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let mut listener = sqlx::postgres::PgListener::connect_with(&pool)
            .await
            .unwrap();
        listener.listen("workflow_done").await.unwrap();
        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let payload = PayloadPolicy::default();
        let retention = RetentionConfig::default();
        for success in [false, true] {
            for output in [None, Some(0_i32)] {
                for bytes in [64_usize, 65536] {
                    let id = Uuid::new_v4();
                    sqlx::query(
                        "INSERT INTO horsies_workflows (
                                id, name, status, on_error, output_task_index,
                                definition_key, root_workflow_id, depth, sent_at,
                                created_at, updated_at
                             ) VALUES ($1, 'finalization_contract', 'RUNNING', 'fail',
                                       $2, 'finalization_contract', $1, 0, NOW(),
                                       NOW(), NOW())",
                    )
                    .bind(id)
                    .bind(output)
                    .execute(&pool)
                    .await
                    .unwrap();
                    let result = serde_json::to_string(&TaskResult::Ok(serde_json::Value::String(
                        "x".repeat(bytes),
                    )))
                    .unwrap();
                    for index in 0..8_i32 {
                        let status = match (success, index) {
                            (false, 0) => "FAILED",
                            _ => "COMPLETED",
                        };
                        sqlx::query(
                                "INSERT INTO horsies_workflow_tasks (
                                    downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                                    status, result, queue_name, priority, dependencies,
                                    allow_failed_deps, join_type, is_subworkflow, created_at,
                                    max_retries, execution, retention_class_key
                                ) VALUES (
                                    'NONE', $1, $2, $3, $4, 'finalization_contract', 'null', $5, $6,
                                    'default', 100, '{}', FALSE, 'all', FALSE, NOW(), 0,
                                    'ISOLATED', 'standard_30d'
                                )",
                            )
                            .bind(Uuid::new_v4())
                            .bind(id)
                            .bind(index)
                            .bind(format!("node_{index}"))
                            .bind(status)
                            .bind(&result)
                            .execute(&pool)
                            .await
                            .unwrap();
                    }
                    check_workflow_completion(&pool, id, &registry, &payload, &retention)
                        .await
                        .unwrap();
                    let (status, stored): (String, Option<String>) = sqlx::query_as(
                        "SELECT status, result FROM horsies_workflows WHERE id = $1",
                    )
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                    assert_eq!(status, if success { "COMPLETED" } else { "FAILED" });
                    match (success, output) {
                        (false, _) => assert_eq!(stored, None),
                        (true, Some(_)) => assert_eq!(stored, Some(result.clone())),
                        (true, None) => {
                            // Python's outputless result (`core/workflows/engine.py:3973-3983`).
                            let entry: serde_json::Value = serde_json::from_str(&result).unwrap();
                            let per_node = |value: serde_json::Value| {
                                (0..8)
                                    .map(|index| (format!("node_{index}"), value.clone()))
                                    .collect::<serde_json::Map<_, _>>()
                            };
                            let stored: serde_json::Value =
                                serde_json::from_str(stored.as_deref().unwrap()).unwrap();
                            assert_eq!(
                                stored,
                                serde_json::json!({
                                    "__h_task_result__": true,
                                    "__h_outputless_terminals__": true,
                                    "ok": {
                                        "results_by_id": per_node(entry),
                                        "task_name_by_id": per_node(serde_json::json!(
                                            "finalization_contract"
                                        )),
                                        "definition_key_by_id": per_node(serde_json::Value::Null),
                                    },
                                    "err": null,
                                })
                            );
                        }
                    }
                    let notification =
                        tokio::time::timeout(std::time::Duration::from_secs(5), listener.recv())
                            .await
                            .unwrap()
                            .unwrap();
                    assert_eq!(notification.payload(), id.to_string());
                    assert!(tokio::time::timeout(
                        std::time::Duration::from_millis(25),
                        listener.recv(),
                    )
                    .await
                    .is_err());
                    sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
                        .bind(id)
                        .execute(&pool)
                        .await
                        .unwrap();
                }
            }
        }
    }
}

#[cfg(test)]
mod child_outcome_tests {
    use super::*;

    fn facts(output_task_index: Option<i32>, error: Option<&str>) -> ChildWorkflowFacts {
        ChildWorkflowFacts {
            output_task_index,
            error: error.map(str::to_owned),
        }
    }

    fn counts() -> Vec<StatusCount> {
        vec![
            StatusCount {
                status: "COMPLETED".to_owned(),
                cnt: 2,
            },
            StatusCount {
                status: "FAILED".to_owned(),
                cnt: 1,
            },
            StatusCount {
                status: "SKIPPED".to_owned(),
                cnt: 1,
            },
        ]
    }

    fn code(error: &TaskError) -> Option<String> {
        Some(error.error_code.to_string())
    }

    #[test]
    fn a_completed_child_with_an_ok_output_completes_the_node_with_its_text() {
        let child = Uuid::new_v4();
        let text = serde_json::to_string(&TaskResult::Ok(serde_json::json!({"n": 1}))).unwrap();
        let (summary, outcome) = terminal_child_outcome(
            child,
            "COMPLETED",
            Some(&text),
            &facts(Some(1), None),
            &counts(),
        )
        .unwrap();
        assert_eq!(outcome.status(), "COMPLETED");
        assert_eq!(outcome.result_json(), text);
        assert_eq!(summary.output, Some(serde_json::json!({"n": 1})));
        assert_eq!(
            (
                summary.total_tasks,
                summary.completed_tasks,
                summary.failed_tasks,
                summary.skipped_tasks
            ),
            (4, 2, 1, 1)
        );
    }

    #[test]
    fn a_completed_child_with_an_err_output_fails_the_node_with_that_error() {
        let child = Uuid::new_v4();
        let text = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(
            TaskError::new("OUTPUT_FAILED", "output node failed").unwrap(),
        ))
        .unwrap();
        let (summary, outcome) = terminal_child_outcome(
            child,
            "COMPLETED",
            Some(&text),
            &facts(Some(1), None),
            &counts(),
        )
        .unwrap();
        assert_eq!(outcome.result_json(), text);
        let ParentNodeOutcome::Failed { handler_error, .. } = outcome else {
            panic!("an err output must fail the node");
        };
        assert_eq!(code(&handler_error).as_deref(), Some("OUTPUT_FAILED"));
        assert_eq!(summary.output, None);
    }

    #[test]
    fn a_completed_child_with_an_unreadable_result_fails_the_node_naming_the_child() {
        let child = Uuid::new_v4();
        for stored in [None, Some(""), Some("not json")] {
            let (_, outcome) =
                terminal_child_outcome(child, "COMPLETED", stored, &facts(Some(1), None), &[])
                    .unwrap();
            let ParentNodeOutcome::Failed {
                result_json,
                handler_error,
            } = outcome
            else {
                panic!("an unreadable output must fail the node");
            };
            assert_eq!(
                code(&handler_error).as_deref(),
                Some("RESULT_DESERIALIZATION_ERROR")
            );
            assert!(handler_error
                .message
                .as_deref()
                .unwrap()
                .contains(&child.to_string()));
            let stored: TaskResult<serde_json::Value> = serde_json::from_str(&result_json).unwrap();
            assert!(stored.is_err());
        }
    }

    #[test]
    fn an_outputless_child_completes_the_node_with_an_ok_null() {
        let text = serde_json::to_string(&TaskResult::Ok(serde_json::json!({"a": 1}))).unwrap();
        let (summary, outcome) = terminal_child_outcome(
            Uuid::new_v4(),
            "COMPLETED",
            Some(&text),
            &facts(None, None),
            &[],
        )
        .unwrap();
        assert_eq!(outcome.status(), "COMPLETED");
        let stored: TaskResult<serde_json::Value> =
            serde_json::from_str(outcome.result_json()).unwrap();
        assert!(matches!(stored, TaskResult::Ok(serde_json::Value::Null)));
        assert_eq!(summary.output, None);
    }

    #[test]
    fn a_failed_child_fails_the_node_with_subworkflow_failed() {
        let child = Uuid::new_v4();
        let child_error =
            serde_json::to_string(&TaskError::new("BOOM", "task b exploded").unwrap()).unwrap();
        for status in ["FAILED", "CANCELLED", "EXPIRED"] {
            let (summary, outcome) = terminal_child_outcome(
                child,
                status,
                None,
                &facts(Some(0), Some(&child_error)),
                &counts(),
            )
            .unwrap();
            assert_eq!(summary.error_summary.as_deref(), Some("task b exploded"));
            let ParentNodeOutcome::Failed {
                result_json,
                handler_error,
            } = outcome
            else {
                panic!("a {status} child must fail the node");
            };
            let stored: TaskResult<serde_json::Value> = serde_json::from_str(&result_json).unwrap();
            let TaskResult::Err(node_error) = stored else {
                panic!("the node result must be an error");
            };
            assert_eq!(code(&node_error).as_deref(), Some("SUBWORKFLOW_FAILED"));
            assert_eq!(
                node_error.message.as_deref(),
                Some(format!("Subworkflow {child} failed with status {status}").as_str())
            );
            assert_eq!(
                handler_error.message.as_deref(),
                Some(format!("Subworkflow {child} failed").as_str())
            );
            let details = node_error.sub_workflow_details().unwrap();
            assert_eq!(details.sub_workflow_id, child);
            assert_eq!(details.sub_workflow_summary, summary);
            // Row 35: both fields at the top level of the stored error, `data` null (Python
            // `SubWorkflowError`, `core/workflows/engine.py:3238-3281`).
            let stored_value: serde_json::Value = serde_json::from_str(&result_json).unwrap();
            assert_eq!(
                stored_value["err"]["sub_workflow_id"],
                serde_json::json!(child.to_string())
            );
            assert_eq!(
                stored_value["err"]["sub_workflow_summary"]["status"],
                serde_json::json!(status)
            );
            assert_eq!(stored_value["err"]["data"], serde_json::Value::Null);
        }
    }

    #[test]
    fn the_error_summary_follows_python() {
        assert_eq!(
            child_error_summary(Some(r#"{"message": "first failure"}"#)).as_deref(),
            Some("first failure")
        );
        assert_eq!(child_error_summary(Some(r#"{"error_code": "X"}"#)), None);
        assert_eq!(child_error_summary(Some("42")), None);
        assert_eq!(child_error_summary(Some("null")).as_deref(), Some("null"));
        let long = "x".repeat(300);
        assert_eq!(
            child_error_summary(Some(&long)).map(|text| text.chars().count()),
            Some(200)
        );
        assert_eq!(child_error_summary(Some("")), None);
        assert_eq!(child_error_summary(None), None);
    }
}

#[cfg(test)]
mod completion_rule_tests {
    use super::*;
    use serial_test::serial;

    fn statuses(values: &[&str]) -> Vec<TaskStatusRow> {
        values
            .iter()
            .enumerate()
            .map(|(index, status)| TaskStatusRow {
                task_index: index as i32,
                status: (*status).to_owned(),
            })
            .collect()
    }

    fn output_row(status: &str, result: Option<&str>) -> NodeResult {
        NodeResult {
            node_id: Some("out".to_owned()),
            result: result.map(str::to_owned),
            status: status.to_owned(),
            task_index: 2,
            task_name: "produce".to_owned(),
            sub_definition_key: None,
        }
    }

    fn stored_error(text: &str) -> TaskError {
        let stored: TaskResult<serde_json::Value> = serde_json::from_str(text).unwrap();
        match stored {
            TaskResult::Err(error) => error,
            TaskResult::Ok(value) => panic!("expected an error envelope, got ok {value}"),
        }
    }

    /// A context node whose stored result is NULL or empty gets the entry of `horsies_workflow_source_value`
    /// (0005) and Python `_missing_result_entry`.
    #[test]
    fn a_context_node_with_no_stored_result_gets_the_missing_result_entry() {
        for status in ["COMPLETED", "FAILED"] {
            for result in [None, Some(String::new())] {
                let row = CtxResultRow {
                    node_id: Some("source".to_owned()),
                    task_index: 1,
                    task_name: "produce".to_owned(),
                    sub_definition_key: None,
                    status: status.to_owned(),
                    result: result.clone(),
                    sub_workflow_summary: None,
                };
                let context = WorkflowContextData::from_rows(Uuid::nil(), 2, "receive", vec![row]);
                let Some(TaskResult::Err(error)) = context.results_by_id.get("source") else {
                    panic!(
                        "a {status} context node with result {result:?} must give an error entry"
                    );
                };
                let expected = format!(
                    "Dependency node source (index 1, task produce) is {status} with no stored result"
                );
                assert_eq!(error.error_code.to_string(), "RESULT_DESERIALIZATION_ERROR");
                assert_eq!(error.message.as_deref(), Some(expected.as_str()));
                assert_eq!(error.data, Some(serde_json::json!({"dependency_index": 1})));
            }
        }
    }

    #[test]
    fn the_default_rule_needs_no_failed_node_and_no_stored_error() {
        let all_completed = statuses(&["COMPLETED", "COMPLETED"]);
        assert!(evaluate_workflow_success(&None, false, false, &all_completed).unwrap());
        // A stored workflow error fails the workflow even with no FAILED node.
        assert!(!evaluate_workflow_success(&None, true, false, &all_completed).unwrap());
        assert!(!evaluate_workflow_success(&None, false, true, &all_completed).unwrap());
    }

    /// JSON null is no policy, as SQL NULL is (Python `read_stored_success_policy`, the baseline function
    /// `horsies_workflow_success_cases`).
    #[test]
    fn a_json_null_policy_is_no_policy() {
        let null = Some(serde_json::Value::Null);
        let all_completed = statuses(&["COMPLETED", "COMPLETED"]);
        assert_eq!(
            read_stored_success_policy(&null).unwrap(),
            StoredSuccessPolicy::NoPolicy
        );
        assert!(evaluate_workflow_success(&null, false, false, &all_completed).unwrap());
        assert!(!evaluate_workflow_success(&null, false, true, &all_completed).unwrap());
        assert!(!evaluate_workflow_success(&null, true, false, &all_completed).unwrap());
    }

    /// The forms of Python's reader: each refused form fails the step, each accepted form reads as a policy.
    #[test]
    fn the_stored_policy_is_read_by_python_s_rule() {
        let refused = [
            serde_json::json!([1, 2, 3]),
            serde_json::json!("text"),
            serde_json::json!(7),
            serde_json::json!({}),
            serde_json::json!({"cases": "not a list"}),
            serde_json::json!({"cases": null}),
            serde_json::json!({"cases": [1]}),
            serde_json::json!({"cases": [{}]}),
            serde_json::json!({"cases": [{"required_indices": null}]}),
            serde_json::json!({"cases": [{"required_indices": [0]}, {"required_indices": [true]}]}),
            serde_json::json!({"cases": [{"required_indices": [0, -1]}]}),
            serde_json::json!({"cases": [{"required_indices": [1.0]}]}),
            serde_json::json!({"cases": [{"required_indices": ["0"]}]}),
            serde_json::json!({"cases": [], "optional_indices": "x"}),
        ];
        for value in refused {
            assert!(
                matches!(
                    read_stored_success_policy(&Some(value.clone())),
                    Err(WorkflowError::Serialization(_))
                ),
                "{value} must not read"
            );
        }
        let accepted = [
            serde_json::json!({"cases": [], "optional_indices": null}),
            serde_json::json!({"cases": [], "extra": 1}),
            serde_json::json!({"cases": [{"required_indices": [], "extra": "x"}]}),
            serde_json::json!({"cases": [{"required_indices": [0, 3_000_000_000_u64]}], "optional_indices": [1]}),
        ];
        for value in accepted {
            assert!(
                matches!(
                    read_stored_success_policy(&Some(value.clone())),
                    Ok(StoredSuccessPolicy::Policy(_))
                ),
                "{value} must read"
            );
        }
    }

    /// An index above the `i32` range names no node: the failure-error read leaves it out instead of
    /// wrapping it to another index.
    #[test]
    fn a_required_index_above_the_i32_range_names_no_node() {
        let above = i32::MAX as usize + 1;
        let policy = SuccessPolicy {
            cases: vec![
                crate::core::SuccessCase {
                    required_indices: vec![1, above],
                },
                crate::core::SuccessCase {
                    required_indices: vec![0],
                },
            ],
            optional_indices: None,
        };
        assert_eq!(required_task_indexes(&policy), vec![1, 0]);
    }

    #[test]
    fn a_stored_success_policy_that_does_not_parse_fails_the_step() {
        let error = evaluate_workflow_success(
            &Some(serde_json::json!({"cases": "not a list"})),
            false,
            false,
            &statuses(&["COMPLETED"]),
        )
        .unwrap_err();
        assert!(matches!(error, WorkflowError::Serialization(_)));
    }

    #[test]
    fn a_skipped_output_node_gives_workflow_output_skipped() {
        let workflow_id = Uuid::new_v4();
        let text =
            explicit_output_result(workflow_id, 2, Some(output_row("SKIPPED", None))).unwrap();
        let error = stored_error(&text);
        assert_eq!(error.error_code.to_string(), "WORKFLOW_OUTPUT_SKIPPED");
        assert_eq!(
            error.data,
            Some(serde_json::json!({
                "workflow_id": workflow_id.to_string(),
                "output_node_id": "out",
                "output_task_index": 2,
                "output_task_name": "produce",
            }))
        );
    }

    #[test]
    fn an_output_node_with_no_stored_result_gives_result_deserialization_error() {
        for status in ["COMPLETED", "FAILED"] {
            for result in [None, Some("")] {
                let text =
                    explicit_output_result(Uuid::new_v4(), 2, Some(output_row(status, result)))
                        .unwrap();
                assert_eq!(
                    stored_error(&text).error_code.to_string(),
                    "RESULT_DESERIALIZATION_ERROR"
                );
            }
        }
    }

    #[test]
    fn a_stored_output_result_is_copied_unchanged() {
        let stored = serde_json::to_string(&TaskResult::Ok(serde_json::json!([1, 2]))).unwrap();
        for status in ["COMPLETED", "FAILED"] {
            assert_eq!(
                explicit_output_result(Uuid::new_v4(), 2, Some(output_row(status, Some(&stored))))
                    .unwrap(),
                stored
            );
        }
    }

    #[test]
    fn a_missing_or_live_output_node_is_an_error() {
        assert!(explicit_output_result(Uuid::new_v4(), 2, None).is_err());
        assert!(
            explicit_output_result(Uuid::new_v4(), 2, Some(output_row("RUNNING", None))).is_err()
        );
    }

    #[tokio::test]
    #[serial]
    async fn completion_with_a_stored_error_and_no_failed_node_keeps_that_error() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        let first_error =
            serde_json::to_string(&TaskError::new("FIRST", "first failure").unwrap()).unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, error, definition_key, depth,
                 root_workflow_id, sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'stored_error', 'RUNNING', 'fail', $2, $3, 0, $1,
                       NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .bind(&first_error)
        .bind(format!("test.stored-error.{workflow_id}"))
        .execute(&pool)
        .await
        .unwrap();
        let ok = serde_json::to_string(&TaskResult::Ok(serde_json::json!(1))).unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                 downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                 queue_name, priority, dependencies, allow_failed_deps, join_type,
                 max_retries, execution, retention_class_key, status, result,
                 is_subworkflow, created_at, completed_at
             ) VALUES ('NONE', $1, $2, 0, 'only', 'only_task', '{}', 'default', 100, '{}',
                       FALSE, 'all', 0, 'ISOLATED', 'forever', 'COMPLETED', $3,
                       FALSE, NOW(), NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .bind(&ok)
        .execute(&pool)
        .await
        .unwrap();

        check_workflow_completion(
            &pool,
            workflow_id,
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();

        let (status, error): (String, Option<String>) =
            sqlx::query_as("SELECT status, error FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "FAILED");
        assert_eq!(error, Some(first_error));
    }
}

#[cfg(test)]
mod task_complete_tests;

#[cfg(test)]
mod promotion_branch_tests;

#[cfg(test)]
mod subworkflow_branch_tests;

use std::collections::HashMap;

use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::core::config::payload::{enforce_payload_policy, PayloadKind, PayloadPolicy};
use crate::core::config::retention::RetentionConfig;
use crate::core::history::enqueue::{
    prepare_enqueue_facts, EnqueueInputEligibility, PreparedEnqueueFacts,
};
use crate::core::task::retry_utils::parse_max_retries;
use crate::core::workflow::context::WORKFLOW_CTX_KWARG;
use crate::core::{
    JoinType, OperationalErrorCode, OutcomeCode, RetrievalCode, SubWorkflowSummary, SuccessPolicy,
    TaskError, TaskResult, WorkflowSpecRegistry, WorkflowTaskStatus,
};

use crate::workflow_engine::start::{materialize_child_spec, start_child_workflow_in_tx};

use crate::workflow_engine::error::WorkflowError;

#[cfg(test)]
fn test_uuid(value: &str) -> Uuid {
    Uuid::parse_str(value).expect("test identity must be UUID")
}

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

/// Single statement replacing the old locate -> CAS-update pair. One round
/// trip does both: `found` locates the workflow task by its backing task id
/// (JOIN to the workflow row, taking its FOR UPDATE lock for the duration of
/// this statement), `upd` CAS-updates the node to its terminal status, and the
/// outer SELECT returns the progression context (`wf_status`, `on_error`) plus
/// `cas_won`. The `upd` predicate also gates on the workflow not being terminal,
/// subsuming the old pre-update early-return (a late task completion must not
/// mutate a node of an already-terminal workflow). PAUSED is intentionally NOT
/// gated here — it flows through and is caught by the fresh PAUSED guard after
/// failure handling, matching the prior control flow. Zero rows = not a
/// workflow task, or its workflow row is missing; the caller treats both as a
/// no-op. The data-modifying CTEs always execute exactly once even though the
/// outer query only reads `found`.
///
/// Lock-order invariant (N6): this statement locks the workflow row (`found`'s
/// `FOR UPDATE OF w`) before the workflow_task row (`upd`'s UPDATE). Every
/// transaction that locks both `horsies_workflows` and `horsies_workflow_tasks`
/// must take them in that order — workflows before workflow_tasks — or it can
/// deadlock against this path. The cancel transaction
/// (`LOCK_WORKFLOW_ROW_FOR_CANCEL_SQL`) is held to the same order.
///
/// `pause_wf` folds the `on_error = 'pause'` parent pause into this same
/// statement, atomic with the node-FAILED write: when the node CAS wins with
/// status FAILED and the workflow's `on_error` is `pause`, it CASes the workflow
/// RUNNING -> PAUSED and stores the triggering error. Without this, a crash
/// between the node-FAILED commit and the separate pause in
/// `handle_workflow_task_failure` permanently lost the pause and the workflow
/// finalized FAILED (C16). `handle_workflow_task_failure` still runs for the
/// child-pause cascade and error bookkeeping; its own parent pause then no-ops
/// (already PAUSED).
#[cfg(test)]
const COMPLETE_WORKFLOW_TASK_SQL: &str = "\
WITH found AS (
    SELECT wt.workflow_id, wt.task_index, w.status AS wf_status, w.on_error
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.task_id = $1
    FOR UPDATE OF w
),
upd AS (
    UPDATE horsies_workflow_tasks wt
    SET status = $2, result = $3, error = COALESCE($4::text, wt.error), completed_at = NOW()
    FROM found
    WHERE wt.workflow_id = found.workflow_id AND wt.task_index = found.task_index
      AND wt.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
      AND found.wf_status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
    RETURNING wt.task_index
),
pause_wf AS (
    UPDATE horsies_workflows w
    SET status = 'PAUSED', error = COALESCE($4::text, w.error), updated_at = NOW()
    FROM found
    WHERE w.id = found.workflow_id
      AND found.on_error = 'pause'
      AND $2 = 'FAILED'
      AND w.status = 'RUNNING'
      AND EXISTS (SELECT 1 FROM upd)
    RETURNING w.id
)
SELECT found.workflow_id, found.task_index, found.wf_status, found.on_error,
       EXISTS (SELECT 1 FROM upd) AS cas_won,
       EXISTS (SELECT 1 FROM pause_wf) AS wf_paused
FROM found";

const UPDATE_WORKFLOW_TASK_FAILED_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = 'FAILED', result = $1, error = $2, completed_at = NOW()
WHERE workflow_id = $3 AND task_index = $4
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
       d.task_args, d.task_kwargs, d.queue_name, d.priority,
       d.node_id, d.task_options, d.status,
       d.is_subworkflow, d.sub_workflow_name, d.sub_definition_key,
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
       task_args, task_kwargs, queue_name, priority, node_id,
       task_options, status, is_subworkflow, sub_workflow_name,
       sub_definition_key
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

const ENQUEUE_TASK_SQL: &str = "\
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, args, kwargs,
    status, sent_at, enqueued_at, good_until, max_retries, task_options,
    enqueue_sha, is_workflow_task, created_at, updated_at,
    command_fingerprint_version, command_fingerprint, retention_class_key,
    input_digest, rerun_of_task_id, rerun_root_task_id, idempotency_key_digest,
    retain_rerun_input, prepared_rerun_input_disposition,
    prepared_rerun_input_version, prepared_rerun_input_codec,
    prepared_rerun_input_content_type, prepared_rerun_input_digest,
    prepared_rerun_input_inline, prepared_rerun_input_reference
)
VALUES ($1::uuid, $2, $3, $4, $5, $6, 'PENDING', NOW(), NOW(), $7, $8, $9, $10,
        TRUE, NOW(), NOW(), $11, $12, $13, $14, NULL, NULL, NULL, $15, $16,
        $17, $18, $19, $20, $21, NULL)";

const LINK_ENQUEUED_TASK_SQL: &str = "\
UPDATE horsies_workflow_tasks wt
SET task_id = $1::uuid, status = 'ENQUEUED', started_at = NOW()
FROM horsies_workflows w
WHERE wt.workflow_id = $2::uuid AND wt.task_index = $3
  AND wt.status = 'READY'
  AND w.id = wt.workflow_id
  AND w.status = 'RUNNING'";

const GET_DEP_RESULTS_SQL: &str = "\
SELECT task_index, status, result
FROM horsies_workflow_tasks
WHERE workflow_id = $1 AND task_index = ANY($2)
  AND status IN ('COMPLETED', 'FAILED', 'SKIPPED')";

const COUNT_CTX_TERMINAL_SQL: &str = "\
SELECT COUNT(*)::int as cnt
FROM horsies_workflow_tasks
WHERE workflow_id = $1
  AND node_id = ANY($2)
  AND status IN ('COMPLETED', 'FAILED', 'SKIPPED')";

const GET_CTX_RESULTS_BY_NODE_ID_SQL: &str = "\
SELECT node_id, status, result, sub_workflow_summary
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
SELECT output_task_index, success_policy, on_error
FROM horsies_workflows
WHERE id = $1";

const GET_TASK_RESULT_BY_INDEX_SQL: &str = "\
SELECT result
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
SELECT node_id, result
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

const GET_PARENT_WORKFLOW_SQL: &str = "\
SELECT parent_workflow_id, parent_task_index
FROM horsies_workflows
WHERE id = $1";

const GET_WORKFLOW_DEPTH_SQL: &str = "\
SELECT depth, root_workflow_id
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

// The `w.status NOT IN (terminal)` gate mirrors COMPLETE_WORKFLOW_TASK_SQL: a
// child completing after its parent was cancelled must not flip the parent's
// (terminal-workflow) node to COMPLETED/FAILED post-cancellation (C22).
const UPDATE_SUBWORKFLOW_COMPLETED_SQL: &str = "\
UPDATE horsies_workflow_tasks wt
SET status = 'COMPLETED', result = $1, sub_workflow_summary = $2, completed_at = NOW()
FROM horsies_workflows w
WHERE wt.workflow_id = $3 AND wt.task_index = $4
  AND wt.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
  AND w.id = wt.workflow_id
  AND w.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
RETURNING wt.id";

// Same terminal-workflow gate as UPDATE_SUBWORKFLOW_COMPLETED_SQL (C22).
const UPDATE_SUBWORKFLOW_FAILED_SQL: &str = "\
UPDATE horsies_workflow_tasks wt
SET status = 'FAILED', result = $1, error = $2, sub_workflow_summary = $3, completed_at = NOW()
FROM horsies_workflows w
WHERE wt.workflow_id = $4 AND wt.task_index = $5
  AND wt.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
  AND w.id = wt.workflow_id
  AND w.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
RETURNING wt.id";

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

const PHASE2_NODE_RESULT_SQL: &str = "\
SELECT result FROM horsies_workflow_tasks WHERE id = $1 AND workflow_id = $2";

// ---------------------------------------------------------------------------
// Row types for internal queries
// ---------------------------------------------------------------------------

#[cfg(test)]
#[derive(Debug, sqlx::FromRow)]
struct CompleteTaskRow {
    workflow_id: Uuid,
    task_index: i32,
    wf_status: String,
    on_error: String,
    cas_won: bool,
    /// True when this same statement CAS-paused the workflow (on_error=pause,
    /// node CAS won with FAILED) atomically with the node-FAILED write (C16).
    wf_paused: bool,
}

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
    task_args: Option<String>,
    task_kwargs: Option<String>,
    queue_name: String,
    priority: i32,
    node_id: Option<String>,
    task_options: Option<String>,
    status: String,
    is_subworkflow: bool,
    sub_workflow_name: Option<String>,
    sub_definition_key: Option<String>,
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
    status: String,
    result: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct WorkflowMeta {
    output_task_index: Option<i32>,
    success_policy: Option<serde_json::Value>,
    on_error: String,
}

#[derive(Debug, sqlx::FromRow)]
struct TaskResultOnly {
    result: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct NodeResult {
    node_id: Option<String>,
    result: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct EdgeRow {
    task_index: i32,
    dependencies: Vec<i32>,
}

#[derive(Debug, sqlx::FromRow)]
struct CtxResultRow {
    node_id: Option<String>,
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
struct ChildPolicyRow {
    success_policy: Option<serde_json::Value>,
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
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Compatibility entry point for completing a workflow task.
///
/// The v35 terminalization program already persisted the result, moved the
/// live row, and minted durable phase-2 evidence. This function therefore
/// delegates to the same consume-and-progress transaction used by the worker;
/// it never reconstructs progression from a terminal live row.
pub async fn on_workflow_task_complete(
    pool: &PgPool,
    task_id: Uuid,
    _result_json: &str,
    is_success: bool,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    crate::workflow_engine::phase2_recovery::finalize_phase2(
        pool,
        task_id,
        if is_success { "COMPLETED" } else { "FAILED" },
        registry,
        payload,
        retention,
    )
    .await?;
    Ok(())
}

/// Continue workflow progression after the database-owned phase-2 consumer
/// has atomically written a terminal node. The caller owns `transaction`; the
/// pending-row delete, failure policy, dependent promotion, and workflow
/// completion therefore commit or roll back together.
pub(crate) async fn apply_phase2_progression_in_tx(
    transaction: &mut Transaction<'_, Postgres>,
    workflow_id: Uuid,
    node_row_id: Option<Uuid>,
    task_index: i32,
    node_status: &str,
    terminal_status: Option<&str>,
    on_error: Option<&str>,
    workflow_status: Option<&str>,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    if node_status == "FAILED" {
        let error_json = if on_error == Some("pause") {
            match node_row_id {
                Some(node_row_id) => {
                    recovered_phase2_failure_error(
                        transaction.as_mut(),
                        workflow_id,
                        node_row_id,
                        terminal_status,
                    )
                    .await?
                }
                None => {
                    get_workflow_failure_error(
                        transaction.as_mut(),
                        workflow_id,
                        &None,
                        terminal_status,
                    )
                    .await?
                }
            }
        } else {
            get_workflow_failure_error(transaction.as_mut(), workflow_id, &None, terminal_status)
                .await?
        };
        match on_error {
            Some("pause") => {
                let paused: Option<IdRow> = sqlx::query_as(PAUSE_WORKFLOW_WITH_ERROR_SQL)
                    .bind(workflow_id)
                    .bind(&error_json)
                    .fetch_optional(transaction.as_mut())
                    .await?;
                if paused.is_some() {
                    crate::workflow_engine::lifecycle::pause_tree_and_relocate_in_tx(
                        transaction,
                        workflow_id,
                    )
                    .await?;
                }
                return Ok(());
            }
            _ => {
                sqlx::query(SET_WORKFLOW_ERROR_SQL)
                    .bind(workflow_id)
                    .bind(&error_json)
                    .execute(transaction.as_mut())
                    .await?;
            }
        }
    }
    if workflow_status == Some("PAUSED") {
        return Ok(());
    }

    promote_phase2_dependents_in_tx(
        transaction,
        workflow_id,
        task_index,
        registry,
        payload,
        retention,
    )
    .await?;
    complete_phase2_workflow_in_tx(transaction, workflow_id, registry, payload, retention).await
}

async fn promote_phase2_dependents_in_tx(
    transaction: &mut Transaction<'_, Postgres>,
    workflow_id: Uuid,
    completed_index: i32,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let mut source_indexes = vec![completed_index];
    while !source_indexes.is_empty() {
        let eval_rows: Vec<DependentEvalRow> = sqlx::query_as(GET_DEPENDENTS_EVAL_SQL)
            .bind(workflow_id)
            .bind(&source_indexes)
            .fetch_all(transaction.as_mut())
            .await?;
        let mut next_skipped = Vec::new();
        for eval in eval_rows {
            let node = eval.node;
            let total = node.dependencies.len() as i32;
            let completed = dep_status_count(&eval.dep_status_counts, "COMPLETED");
            let failed = dep_status_count(&eval.dep_status_counts, "FAILED");
            let skipped = dep_status_count(&eval.dep_status_counts, "SKIPPED");
            let terminal = completed + failed + skipped;
            let verdict = match parse_join_type(&node.join_type) {
                JoinType::All
                    if terminal == total && failed + skipped > 0 && !node.allow_failed_deps =>
                {
                    Some(false)
                }
                JoinType::All if terminal == total => Some(true),
                JoinType::Any if completed > 0 => Some(true),
                JoinType::Any if terminal == total => Some(false),
                JoinType::Quorum if completed >= node.min_success.unwrap_or(1) => Some(true),
                JoinType::Quorum
                    if completed + (total - terminal) < node.min_success.unwrap_or(1) =>
                {
                    Some(false)
                }
                _ => None,
            };
            match verdict {
                Some(false) => {
                    let changed = sqlx::query(UPDATE_WORKFLOW_TASK_SKIPPED_SQL)
                        .bind(workflow_id)
                        .bind(node.task_index)
                        .execute(transaction.as_mut())
                        .await?;
                    if changed.rows_affected() > 0 {
                        next_skipped.push(node.task_index);
                    }
                }
                Some(true) => {
                    enqueue_phase2_dependent_in_tx(
                        transaction,
                        workflow_id,
                        &node,
                        registry,
                        payload,
                        retention,
                    )
                    .await?;
                }
                None => {}
            }
        }
        source_indexes = next_skipped;
    }
    Ok(())
}

async fn enqueue_phase2_dependent_in_tx(
    transaction: &mut Transaction<'_, Postgres>,
    workflow_id: Uuid,
    node: &DependentRow,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    if let Some(ctx_ids) = node
        .workflow_ctx_from
        .as_ref()
        .filter(|ids| !ids.is_empty())
    {
        let terminal: i32 = sqlx::query_scalar(COUNT_CTX_TERMINAL_SQL)
            .bind(workflow_id)
            .bind(ctx_ids)
            .fetch_one(transaction.as_mut())
            .await?;
        if terminal != ctx_ids.len() as i32 {
            return Ok(());
        }
    }
    let won = sqlx::query(UPDATE_WORKFLOW_TASK_READY_SQL)
        .bind(workflow_id)
        .bind(node.task_index)
        .execute(transaction.as_mut())
        .await?;
    if won.rows_affected() == 0 {
        return Ok(());
    }
    let dep_rows: Vec<DepResult> = sqlx::query_as(GET_DEP_RESULTS_SQL)
        .bind(workflow_id)
        .bind(&node.dependencies)
        .fetch_all(transaction.as_mut())
        .await?;
    let dep_results: HashMap<i32, DepResultValue> = dep_rows
        .into_iter()
        .map(|row| {
            (
                row.task_index,
                DepResultValue {
                    status: row.status,
                    result: row.result,
                },
            )
        })
        .collect();
    let merged_kwargs =
        merge_args_from(node.task_kwargs.as_deref(), &node.args_from, &dep_results)?;
    if node.is_subworkflow {
        return enqueue_phase2_subworkflow_in_tx(
            transaction,
            workflow_id,
            node,
            registry,
            &dep_results,
            payload,
            retention,
        )
        .await;
    }
    let merged_kwargs =
        inject_phase2_workflow_context_in_tx(transaction, workflow_id, node, merged_kwargs).await?;
    if let Some(kwargs) = merged_kwargs.as_deref() {
        enforce_payload_policy(payload, &node.task_name, PayloadKind::Kwargs, kwargs.len());
    }
    let task_id = crate::core::history::identity::uuid7::mint_task_id()
        .map_err(|error| WorkflowError::Validation(error.to_string()))?;
    let good_until =
        crate::workflow_engine::parse_good_until_from_options(node.task_options.as_deref());
    let facts = prepare_enqueue_facts(
        &node.task_name,
        &node.queue_name,
        node.priority,
        node.task_args.as_deref(),
        merged_kwargs.as_deref(),
        good_until,
        None,
        node.task_options.as_deref(),
        retention.resolve_queue_class(&node.queue_name).as_deref(),
        false,
        None,
        EnqueueInputEligibility::NeverEligible,
    )
    .map_err(|error| WorkflowError::Validation(error.to_string()))?;
    sqlx::query(ENQUEUE_TASK_SQL)
        .bind(task_id)
        .bind(&node.task_name)
        .bind(&node.queue_name)
        .bind(node.priority)
        .bind(&node.task_args)
        .bind(&merged_kwargs)
        .bind(good_until)
        .bind(parse_max_retries(node.task_options.as_deref()))
        .bind(&node.task_options)
        .bind(format!("wf-{task_id}"))
        .bind(facts.command_fingerprint_version)
        .bind(facts.command_fingerprint.as_slice())
        .bind(&facts.retention_class_key)
        .bind(facts.input_digest.as_slice())
        .bind(facts.retain_rerun_input)
        .bind(facts.prepared_rerun_input_disposition.as_str())
        .bind(facts.prepared_rerun_input_version)
        .bind(facts.prepared_rerun_input_codec)
        .bind(facts.prepared_rerun_input_content_type)
        .bind(
            facts
                .prepared_rerun_input_digest
                .as_ref()
                .map(|digest| digest.as_slice()),
        )
        .bind(facts.prepared_rerun_input_inline.as_deref())
        .execute(transaction.as_mut())
        .await?;
    let linked = sqlx::query(LINK_ENQUEUED_TASK_SQL)
        .bind(task_id)
        .bind(workflow_id)
        .bind(node.task_index)
        .execute(transaction.as_mut())
        .await?;
    if linked.rows_affected() != 1 {
        return Err(WorkflowError::Validation(
            "phase-2 dependent link lost its READY/RUNNING guard".to_owned(),
        ));
    }
    Ok(())
}

async fn inject_phase2_workflow_context_in_tx(
    transaction: &mut Transaction<'_, Postgres>,
    workflow_id: Uuid,
    node: &DependentRow,
    merged_kwargs: Option<String>,
) -> Result<Option<String>, WorkflowError> {
    let Some(ctx_ids) = node
        .workflow_ctx_from
        .as_ref()
        .filter(|ids| !ids.is_empty())
    else {
        return Ok(merged_kwargs);
    };
    let rows: Vec<CtxResultRow> = sqlx::query_as(GET_CTX_RESULTS_BY_NODE_ID_SQL)
        .bind(workflow_id)
        .bind(ctx_ids)
        .fetch_all(transaction.as_mut())
        .await?;
    let (results_by_id, summaries_by_id) = collect_ctx_maps(rows)?;
    let context = WorkflowContextData {
        workflow_id,
        task_index: node.task_index,
        task_name: node.task_name.clone(),
        results_by_id,
        summaries_by_id,
    };
    let mut kwargs: serde_json::Map<String, serde_json::Value> = match merged_kwargs.as_deref() {
        Some(json) => serde_json::from_str(json)?,
        None => serde_json::Map::new(),
    };
    kwargs.insert(WORKFLOW_CTX_KWARG.to_owned(), context.to_payload()?);
    Ok(Some(serde_json::to_string(&kwargs)?))
}

async fn enqueue_phase2_subworkflow_in_tx(
    transaction: &mut Transaction<'_, Postgres>,
    workflow_id: Uuid,
    node: &DependentRow,
    registry: &WorkflowSpecRegistry,
    dep_results: &HashMap<i32, DepResultValue>,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let spec_name = node
        .task_name
        .strip_prefix("__sub_workflow:")
        .or(node.sub_workflow_name.as_deref())
        .unwrap_or(&node.task_name);
    let Some(registered) =
        registry.resolve_child_registration(spec_name, node.sub_definition_key.as_deref())
    else {
        let error = TaskError::builtin(
            OperationalErrorCode::SubworkflowLoadFailed,
            format!(
                "failed to load sub-workflow definition (definition_key={:?}, name={spec_name:?})",
                node.sub_definition_key,
            ),
        );
        let error_json = serde_json::to_string(&error)?;
        let result_json = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(error))?;
        let updated: Option<IdRow> = sqlx::query_as(UPDATE_WORKFLOW_TASK_FAILED_SQL)
            .bind(&result_json)
            .bind(&error_json)
            .bind(workflow_id)
            .bind(node.task_index)
            .fetch_optional(transaction.as_mut())
            .await?;
        if let Some(updated) = updated {
            let context: Option<(String, String)> =
                sqlx::query_as("SELECT on_error, status FROM horsies_workflows WHERE id = $1")
                    .bind(workflow_id)
                    .fetch_optional(transaction.as_mut())
                    .await?;
            if let Some((on_error, status)) = context {
                Box::pin(apply_phase2_progression_in_tx(
                    transaction,
                    workflow_id,
                    Some(updated.id),
                    node.task_index,
                    "FAILED",
                    None,
                    Some(&on_error),
                    Some(&status),
                    registry,
                    payload,
                    retention,
                ))
                .await?;
            }
        }
        return Ok(());
    };
    let merged_kwargs = merge_args_from(node.task_kwargs.as_deref(), &node.args_from, dep_results)?;
    let has_inputs =
        node.task_args.is_some() || merged_kwargs.is_some() || node.args_from.is_some();
    let child_spec = materialize_child_spec(
        registered,
        has_inputs,
        node.task_args.as_deref(),
        merged_kwargs.as_deref(),
        registry,
    )?;
    let depth: DepthRow = sqlx::query_as(GET_WORKFLOW_DEPTH_SQL)
        .bind(workflow_id)
        .fetch_one(transaction.as_mut())
        .await?;
    let root = depth.root_workflow_id.unwrap_or(workflow_id);
    let child_id = start_child_workflow_in_tx(
        transaction,
        &child_spec,
        workflow_id,
        node.task_index,
        depth.depth.unwrap_or(0) + 1,
        root,
        registry,
        retention,
    )
    .await?;
    let linked = sqlx::query(UPDATE_SUBWORKFLOW_LINK_SQL)
        .bind(&child_id)
        .bind(workflow_id)
        .bind(node.task_index)
        .execute(transaction.as_mut())
        .await?;
    if linked.rows_affected() != 1 {
        return Err(WorkflowError::Validation(
            "phase-2 sub-workflow link lost its READY/RUNNING guard".to_owned(),
        ));
    }
    Ok(())
}

async fn complete_phase2_workflow_in_tx(
    transaction: &mut Transaction<'_, Postgres>,
    workflow_id: Uuid,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let count: NonTerminalCount = sqlx::query_as(COUNT_NON_TERMINAL_SQL)
        .bind(workflow_id)
        .fetch_one(transaction.as_mut())
        .await?;
    if count.cnt != 0 {
        return Ok(());
    }
    let status: Option<WorkflowStatusRow> = sqlx::query_as(GET_WORKFLOW_STATUS_SQL)
        .bind(workflow_id)
        .fetch_optional(transaction.as_mut())
        .await?;
    if !matches!(
        status.as_ref().map(|row| row.status.as_str()),
        Some("RUNNING")
    ) {
        return Ok(());
    }
    let meta: WorkflowMeta = sqlx::query_as(GET_WORKFLOW_META_SQL)
        .bind(workflow_id)
        .fetch_one(transaction.as_mut())
        .await?;
    let statuses: Vec<TaskStatusRow> = sqlx::query_as(ALL_TASK_STATUSES_SQL)
        .bind(workflow_id)
        .fetch_all(transaction.as_mut())
        .await?;
    let success = evaluate_workflow_success(
        &meta.success_policy,
        statuses.iter().any(|status| status.status == "FAILED"),
        &statuses,
    )?;
    let result =
        get_workflow_final_result(transaction.as_mut(), workflow_id, meta.output_task_index)
            .await?;
    let finalized = if success {
        sqlx::query_as::<_, IdRow>(UPDATE_WORKFLOW_COMPLETED_SQL)
            .bind(workflow_id)
            .bind(&result)
            .fetch_optional(transaction.as_mut())
            .await?
    } else {
        let error = get_workflow_failure_error(
            transaction.as_mut(),
            workflow_id,
            &meta.success_policy,
            None,
        )
        .await?;
        sqlx::query_as::<_, IdRow>(UPDATE_WORKFLOW_FAILED_SQL)
            .bind(workflow_id)
            .bind(Option::<&str>::None)
            .bind(error)
            .fetch_optional(transaction.as_mut())
            .await?
    };
    if finalized.is_none() {
        return Ok(());
    }
    sqlx::query("SELECT pg_notify('workflow_done', $1)")
        .bind(workflow_id.to_string())
        .execute(transaction.as_mut())
        .await?;
    propagate_terminal_child_in_tx(
        transaction,
        workflow_id,
        if success { "COMPLETED" } else { "FAILED" },
        success.then_some(result.as_str()),
        registry,
        payload,
        retention,
    )
    .await?;
    Ok(())
}

/// Propagate a terminal child workflow into its parent node inside the caller's
/// transaction, then enter the same phase-2 progression body for the parent.
pub(crate) async fn propagate_terminal_child_in_tx(
    transaction: &mut Transaction<'_, Postgres>,
    child_workflow_id: Uuid,
    child_status: &str,
    child_result_json: Option<&str>,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    if !matches!(
        child_status,
        "COMPLETED" | "FAILED" | "CANCELLED" | "EXPIRED"
    ) {
        return Err(WorkflowError::InvalidStatus(child_status.to_owned()));
    }
    let parent: ParentWorkflowRow = sqlx::query_as(GET_PARENT_WORKFLOW_SQL)
        .bind(child_workflow_id)
        .fetch_one(transaction.as_mut())
        .await?;
    let (Some(parent_id), Some(parent_index)) =
        (parent.parent_workflow_id, parent.parent_task_index)
    else {
        return Ok(());
    };
    let parent_present: Option<IdRow> = sqlx::query_as(LOCK_WORKFLOW_FOR_COMPLETION_SQL)
        .bind(parent_id)
        .fetch_optional(transaction.as_mut())
        .await?;
    if parent_present.is_none() {
        return Ok(());
    }
    let counts: Vec<StatusCount> = sqlx::query_as(COUNT_CHILD_TASK_STATUSES_SQL)
        .bind(child_workflow_id)
        .fetch_all(transaction.as_mut())
        .await?;
    let mut total = 0;
    let mut completed = 0;
    let mut failed = 0;
    let mut skipped = 0;
    for count in counts {
        total += count.cnt;
        match count.status.as_str() {
            "COMPLETED" => completed += count.cnt,
            "FAILED" => failed += count.cnt,
            "SKIPPED" => skipped += count.cnt,
            _ => {}
        }
    }
    let output_task_index: Option<i32> =
        sqlx::query_scalar("SELECT output_task_index FROM horsies_workflows WHERE id = $1")
            .bind(child_workflow_id)
            .fetch_optional(transaction.as_mut())
            .await?
            .flatten();
    let success = child_status == "COMPLETED";
    let output = if success && output_task_index.is_some() {
        child_result_json.and_then(|json| {
            serde_json::from_str::<TaskResult<serde_json::Value>>(json)
                .ok()
                .and_then(|result| match result {
                    TaskResult::Ok(value) => Some(value),
                    TaskResult::Err(_) => None,
                })
        })
    } else {
        None
    };
    let success_case = if success {
        let policy: Option<serde_json::Value> =
            sqlx::query_scalar("SELECT success_policy FROM horsies_workflows WHERE id = $1")
                .bind(child_workflow_id)
                .fetch_optional(transaction.as_mut())
                .await?
                .flatten();
        match policy.and_then(|value| serde_json::from_value::<SuccessPolicy>(value).ok()) {
            Some(policy) => {
                let statuses: Vec<TaskStatusRow> = sqlx::query_as(ALL_TASK_STATUSES_SQL)
                    .bind(child_workflow_id)
                    .fetch_all(transaction.as_mut())
                    .await?;
                let decoded: Vec<WorkflowTaskStatus> = statuses
                    .iter()
                    .map(|status| parse_workflow_task_status(&status.status))
                    .collect::<Result<_, _>>()?;
                policy.evaluate_with_case_name(&decoded)
            }
            None => None,
        }
    } else {
        None
    };
    let summary = SubWorkflowSummary {
        status: child_status.to_owned(),
        is_success: success,
        success_case,
        output,
        total_tasks: total,
        completed_tasks: completed,
        failed_tasks: failed,
        skipped_tasks: skipped,
        error_summary: (!success).then(|| {
            format!(
                "child workflow {child_status}: {completed}/{total} completed, {failed} failed, {skipped} skipped"
            )
        }),
        child_workflow_id,
    };
    let summary_json = serde_json::to_string(&summary)?;
    let (node_status, result_json, error_json) = if success {
        let result = if output_task_index.is_none() {
            serde_json::to_string(&TaskResult::Ok(serde_json::Value::Null))?
        } else {
            child_result_json.map(ToOwned::to_owned).unwrap_or_else(|| {
                serde_json::to_string(&TaskResult::Ok(serde_json::Value::Null))
                    .expect("null result serializes")
            })
        };
        ("COMPLETED", result, None)
    } else {
        let error = subworkflow_failed_error(&summary);
        (
            "FAILED",
            serde_json::to_string(&TaskResult::<serde_json::Value>::Err(error.clone()))?,
            Some(serde_json::to_string(&error)?),
        )
    };
    let updated: Option<IdRow> = if success {
        sqlx::query_as(UPDATE_SUBWORKFLOW_COMPLETED_SQL)
            .bind(&result_json)
            .bind(&summary_json)
            .bind(parent_id)
            .bind(parent_index)
            .fetch_optional(transaction.as_mut())
            .await?
    } else {
        sqlx::query_as(UPDATE_SUBWORKFLOW_FAILED_SQL)
            .bind(&result_json)
            .bind(error_json.as_deref())
            .bind(&summary_json)
            .bind(parent_id)
            .bind(parent_index)
            .fetch_optional(transaction.as_mut())
            .await?
    };
    let Some(updated) = updated else {
        return Ok(());
    };
    let context: (String, String) =
        sqlx::query_as("SELECT on_error, status FROM horsies_workflows WHERE id = $1")
            .bind(parent_id)
            .fetch_one(transaction.as_mut())
            .await?;
    Box::pin(apply_phase2_progression_in_tx(
        transaction,
        parent_id,
        Some(updated.id),
        parent_index,
        node_status,
        None,
        Some(&context.0),
        Some(&context.1),
        registry,
        payload,
        retention,
    ))
    .await
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
/// per-node `try_make_ready_and_enqueue` path unchanged.
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

        // 3. Build INSERT payloads for the won nodes. A build failure leaves the
        // node READY (recovery/resume re-enqueue it) — same as the per-node path.
        let mut built: Vec<EnqueueInsertParams> = Vec::new();
        let mut built_indexes: Vec<i32> = Vec::new();
        for node in &ready_plain {
            if !ready_won.contains(&node.task_index) {
                continue;
            }
            match build_enqueued_task_params(
                pool,
                workflow_id,
                node,
                &dep_results,
                payload,
                retention,
            )
            .await
            {
                Ok(params) => {
                    built_indexes.push(node.task_index);
                    built.push(params);
                }
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
                     id, task_name, queue_name, priority, args, kwargs, \
                     status, sent_at, enqueued_at, good_until, max_retries, task_options, \
                     enqueue_sha, is_workflow_task, created_at, updated_at,
                     command_fingerprint_version, command_fingerprint,
                     retention_class_key, input_digest, rerun_of_task_id,
                     rerun_root_task_id, idempotency_key_digest, retain_rerun_input,
                     prepared_rerun_input_disposition, prepared_rerun_input_version,
                     prepared_rerun_input_codec, prepared_rerun_input_content_type,
                     prepared_rerun_input_digest, prepared_rerun_input_inline,
                     prepared_rerun_input_reference) ",
                );
                qb.push_values(winners.iter(), |mut b, (_, p)| {
                    b.push_bind(p.task_id)
                        .push_bind(p.task_name.clone())
                        .push_bind(p.queue_name.clone())
                        .push_bind(p.priority)
                        .push_bind(p.task_args.clone())
                        .push_bind(p.merged_kwargs.clone())
                        .push("'PENDING'")
                        .push("NOW()")
                        .push("NOW()")
                        .push_bind(p.good_until)
                        .push_bind(p.max_retries)
                        .push_bind(p.task_options.clone())
                        .push_bind(p.enqueue_sha.clone())
                        .push("TRUE")
                        .push("NOW()")
                        .push("NOW()")
                        .push_bind(p.facts.command_fingerprint_version)
                        .push_bind(p.facts.command_fingerprint.to_vec())
                        .push_bind(p.facts.retention_class_key.clone())
                        .push_bind(p.facts.input_digest.to_vec())
                        .push("NULL")
                        .push("NULL")
                        .push("NULL")
                        .push_bind(p.facts.retain_rerun_input)
                        .push_bind(p.facts.prepared_rerun_input_disposition.as_str())
                        .push_bind(p.facts.prepared_rerun_input_version)
                        .push_bind(p.facts.prepared_rerun_input_codec)
                        .push_bind(p.facts.prepared_rerun_input_content_type)
                        .push_bind(
                            p.facts
                                .prepared_rerun_input_digest
                                .map(|digest| digest.to_vec()),
                        )
                        .push_bind(p.facts.prepared_rerun_input_inline.clone())
                        .push_bind(p.facts.prepared_rerun_input_reference.clone());
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
/// marks the task READY + enqueues it, or marks it SKIPPED if the
/// join condition can never be satisfied.
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

    // Mark READY.
    let ready_result: Option<IdRow> = sqlx::query_as(UPDATE_WORKFLOW_TASK_READY_SQL)
        .bind(workflow_id)
        .bind(task.task_index)
        .fetch_optional(pool)
        .await?;

    if ready_result.is_none() {
        // Already transitioned (race condition guard).
        return Ok(());
    }

    let dep_results = if task.args_from.is_some() {
        get_dependency_results(pool, workflow_id, dep_indices).await?
    } else {
        HashMap::new()
    };

    if task.is_subworkflow {
        // Sub-workflow: launch child workflow instead of enqueuing a task.
        enqueue_subworkflow_task(
            pool,
            workflow_id,
            task,
            registry,
            &dep_results,
            payload,
            retention,
        )
        .await?;
    } else {
        // Regular task: enqueue into horsies_tasks.
        enqueue_workflow_task(pool, workflow_id, task, &dep_results, payload, retention).await?;
    }

    Ok(())
}

/// Re-evaluate the exact PENDING node selected by workflow recovery.
///
/// Returns true only when that node leaves PENDING. This does not use a
/// dependency as a proxy, so it cannot advance unrelated sibling nodes.
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
/// Built by [`build_enqueued_task_params`] (kwargs/args_from merge, ctx
/// injection, options parse, fingerprint), then bound by the single-node and
/// batched enqueue paths so the two cannot diverge.
struct EnqueueInsertParams {
    task_id: Uuid,
    task_name: String,
    queue_name: String,
    priority: i32,
    task_args: Option<String>,
    merged_kwargs: Option<String>,
    good_until: Option<chrono::DateTime<chrono::Utc>>,
    max_retries: i32,
    task_options: Option<String>,
    enqueue_sha: String,
    facts: PreparedEnqueueFacts,
}

/// Build the `horsies_tasks` INSERT params for a workflow node ready to enqueue.
///
/// Shared by `enqueue_workflow_task` (single-node) and the batched promotion
/// pipeline. Merges `args_from` dependency results into kwargs, injects
/// `workflow_ctx` when configured (the only DB read here), parses retry/good_until
/// from task_options, and generates the task id + enqueue fingerprint. Errors are
/// per-node and propagate to the caller exactly as the inline code did.
async fn build_enqueued_task_params(
    pool: &PgPool,
    workflow_id: Uuid,
    task: &DependentRow,
    dep_results: &HashMap<i32, DepResultValue>,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<EnqueueInsertParams, WorkflowError> {
    let task_id = crate::core::history::identity::uuid7::mint_task_id().map_err(|error| {
        WorkflowError::Validation(format!("task identity mint failed: {error}"))
    })?;

    // Merge args_from into kwargs.
    let merged_kwargs = merge_args_from(task.task_kwargs.as_deref(), &task.args_from, dep_results)?;

    // Inject workflow_ctx if configured (shared with the resume/recovery
    // re-enqueue paths via inject_workflow_ctx_into_kwargs, so all three paths
    // produce identical context under WORKFLOW_CTX_KWARG).
    let merged_kwargs = inject_workflow_ctx_into_kwargs(
        pool,
        workflow_id,
        task.task_index,
        &task.task_name,
        task.workflow_ctx_from.as_deref(),
        merged_kwargs,
    )
    .await?;

    // Warn-only payload guardrail at the args_from injection point, where an
    // upstream task's result envelope becomes this node's kwargs — the
    // likeliest producer of oversized payloads. Rejecting a mid-workflow node
    // needs a designed node-failure path (a size limit must not strand a
    // running workflow), so reject semantics for workflow nodes are deferred —
    // same as Python. Parity with horsies PR #208.
    if let Some(kwargs_json) = merged_kwargs.as_deref() {
        enforce_payload_policy(
            payload,
            &task.task_name,
            PayloadKind::Kwargs,
            kwargs_json.len(),
        );
    }

    let max_retries = parse_max_retries(task.task_options.as_deref());
    let enqueue_sha = format!("wf-{}", task_id);
    let good_until =
        crate::workflow_engine::parse_good_until_from_options(task.task_options.as_deref());
    let retention_class_key = retention.resolve_queue_class(&task.queue_name);
    let facts = prepare_enqueue_facts(
        &task.task_name,
        &task.queue_name,
        task.priority,
        task.task_args.as_deref(),
        merged_kwargs.as_deref(),
        good_until,
        None,
        task.task_options.as_deref(),
        retention_class_key.as_deref(),
        false,
        None,
        EnqueueInputEligibility::NeverEligible,
    )
    .map_err(|error| WorkflowError::Validation(error.to_string()))?;

    Ok(EnqueueInsertParams {
        task_id,
        task_name: task.task_name.clone(),
        queue_name: task.queue_name.clone(),
        priority: task.priority,
        task_args: task.task_args.clone(),
        merged_kwargs,
        good_until,
        max_retries,
        task_options: task.task_options.clone(),
        enqueue_sha,
        facts,
    })
}

/// Inject the `workflow_ctx` payload into a node's kwargs when `workflow_ctx_from`
/// is non-empty, returning the (possibly updated) kwargs JSON.
///
/// Shared by the runtime promotion path (`build_enqueued_task_params`) and the
/// resume/recovery re-enqueue paths (`lifecycle::reevaluate_and_enqueue`,
/// `recovery::enqueue_ready_task`) so all three inject identical context under
/// `WORKFLOW_CTX_KWARG`. A node with no `workflow_ctx_from` returns its kwargs
/// unchanged; the DB read only happens when context is actually required.
pub(crate) async fn inject_workflow_ctx_into_kwargs(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
    task_name: &str,
    ctx_from_ids: Option<&[String]>,
    merged_kwargs: Option<String>,
) -> Result<Option<String>, WorkflowError> {
    let ctx_from_ids = match ctx_from_ids {
        Some(ids) if !ids.is_empty() => ids,
        _ => return Ok(merged_kwargs),
    };

    let ctx_data =
        build_workflow_context_data(pool, workflow_id, task_index, task_name, ctx_from_ids).await?;
    let ctx_payload = ctx_data.to_payload()?;

    let mut kwargs_map: serde_json::Map<String, serde_json::Value> = match merged_kwargs.as_deref()
    {
        Some(json) => serde_json::from_str(json)?,
        None => serde_json::Map::new(),
    };
    kwargs_map.insert(WORKFLOW_CTX_KWARG.to_owned(), ctx_payload);
    Ok(Some(serde_json::to_string(&kwargs_map)?))
}

/// Create a horsies_tasks row for a workflow task and link it.
///
/// Merges `args_from` results into the task's kwargs/args.
async fn enqueue_workflow_task(
    pool: &PgPool,
    workflow_id: Uuid,
    task: &DependentRow,
    dep_results: &HashMap<i32, DepResultValue>,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let params =
        build_enqueued_task_params(pool, workflow_id, task, dep_results, payload, retention)
            .await?;

    // INSERT + LINK in a single transaction to prevent orphaned horsies_task rows
    // if the workflow is paused/cancelled between INSERT and LINK.
    // Matches Python's atomic ENQUEUE_WORKFLOW_TASK_SQL pattern.
    let mut tx = pool.begin().await?;

    sqlx::query(ENQUEUE_TASK_SQL)
        .bind(&params.task_id)
        .bind(&params.task_name)
        .bind(&params.queue_name)
        .bind(params.priority)
        .bind(&params.task_args)
        .bind(&params.merged_kwargs)
        .bind(params.good_until)
        .bind(params.max_retries)
        .bind(&params.task_options)
        .bind(&params.enqueue_sha)
        .bind(params.facts.command_fingerprint_version)
        .bind(params.facts.command_fingerprint.as_slice())
        .bind(&params.facts.retention_class_key)
        .bind(params.facts.input_digest.as_slice())
        .bind(params.facts.retain_rerun_input)
        .bind(params.facts.prepared_rerun_input_disposition.as_str())
        .bind(params.facts.prepared_rerun_input_version)
        .bind(params.facts.prepared_rerun_input_codec)
        .bind(params.facts.prepared_rerun_input_content_type)
        .bind(
            params
                .facts
                .prepared_rerun_input_digest
                .as_ref()
                .map(|digest| digest.as_slice()),
        )
        .bind(params.facts.prepared_rerun_input_inline.as_deref())
        .execute(&mut *tx)
        .await?;

    // Link workflow_task to the created horsies_tasks row.
    // Guards on wt.status = 'READY' AND w.status = 'RUNNING'.
    // If the workflow was paused/cancelled, this updates 0 rows and the
    // transaction rolls back (no orphaned horsies_task).
    let link_result = sqlx::query(LINK_ENQUEUED_TASK_SQL)
        .bind(&params.task_id)
        .bind(workflow_id)
        .bind(task.task_index)
        .execute(&mut *tx)
        .await?;

    if link_result.rows_affected() == 0 {
        // Workflow was paused/cancelled or task already enqueued. Rollback.
        tx.rollback().await?;
        tracing::debug!(
            workflow_id = %workflow_id,
            task_index = task.task_index,
            "workflow task link failed (workflow no longer RUNNING or task not READY), rolled back",
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

/// Launch a child workflow for a sub-workflow node.
///
/// Looks up the child spec in the registry, builds it dynamically if a
/// Mark a sub-workflow node FAILED when its child definition cannot be resolved.
///
/// Mirrors Python's `_fail_subworkflow_load`: writes FAILED via the terminal-state
/// CAS guard (`UPDATE_WORKFLOW_TASK_FAILED_SQL`), then runs the failure-propagation
/// chain (handle failure policy → process dependents → check completion). On a
/// CAS-miss (row already terminal), skips propagation because whichever path set
/// the terminal state has already propagated the appropriate outcome.
async fn fail_subworkflow_load(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
    spec_name: &str,
    sub_definition_key: Option<&str>,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let error = TaskError::builtin(
        OperationalErrorCode::SubworkflowLoadFailed,
        format!(
            "failed to load sub-workflow definition (definition_key={:?}, name='{}')",
            sub_definition_key, spec_name,
        ),
    );
    let error_json = serde_json::to_string(&error)?;
    let result_json = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(error))?;

    // Mark the parent workflow_task FAILED via the terminal-state CAS guard.
    let updated: Option<IdRow> = sqlx::query_as(UPDATE_WORKFLOW_TASK_FAILED_SQL)
        .bind(&result_json)
        .bind(&error_json)
        .bind(workflow_id)
        .bind(task_index)
        .fetch_optional(pool)
        .await?;

    if updated.is_none() {
        // Already terminal — whichever path set it has already propagated.
        tracing::warn!(
            workflow_id = %workflow_id,
            task_index,
            "skipped sub-workflow load failure mark for terminal workflow task",
        );
        return Ok(());
    }

    tracing::error!(
        workflow_id = %workflow_id,
        task_index,
        spec_name,
        "sub-workflow definition load failed, parent task marked FAILED",
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
        return Ok(());
    }

    // PAUSED guard before propagating to dependents.
    let status_row: Option<WorkflowStatusRow> = sqlx::query_as(GET_WORKFLOW_STATUS_SQL)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;
    if let Some(row) = &status_row {
        if row.status == "PAUSED" {
            return Ok(());
        }
    }

    process_dependents(pool, workflow_id, task_index, registry, payload, retention).await?;
    check_workflow_completion(pool, workflow_id, registry, payload, retention).await?;

    Ok(())
}

/// `spec_builder` is registered (supporting runtime parameterization),
/// starts the child workflow, and links the parent workflow_task to it.
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

    let Some(registered) =
        registry.resolve_child_registration(spec_name, task.sub_definition_key.as_deref())
    else {
        // Child definition is not registered on this worker. Mark the parent
        // workflow_task FAILED and propagate, rather than returning an error
        // that leaves the parent stuck in READY and the workflow RUNNING
        // forever (parity with Python's _fail_subworkflow_load / PR #39).
        return fail_subworkflow_load(
            pool,
            workflow_id,
            task.task_index,
            spec_name,
            task.sub_definition_key.as_deref(),
            registry,
            payload,
            retention,
        )
        .await;
    };

    // Build child spec: use spec_builder if available for dynamic parameterization,
    // otherwise use the static registered spec.
    let has_child_inputs =
        task.task_args.is_some() || task.task_kwargs.is_some() || task.args_from.is_some();

    let merged_kwargs = merge_args_from(task.task_kwargs.as_deref(), &task.args_from, dep_results)?;
    let child_spec = materialize_child_spec(
        registered,
        has_child_inputs,
        task.task_args.as_deref(),
        merged_kwargs.as_deref(),
        registry,
    )?;

    // Get parent depth and root_workflow_id.
    let depth_row: DepthRow = sqlx::query_as(GET_WORKFLOW_DEPTH_SQL)
        .bind(workflow_id)
        .fetch_one(pool)
        .await?;

    let parent_depth = depth_row.depth.unwrap_or(0);
    let root_wf_id = depth_row.root_workflow_id.unwrap_or(workflow_id);

    // Start child workflow + link parent in a single transaction.
    // Prevents orphaned child workflows if the parent is paused between
    // child creation and link (matching Python's atomic pattern).
    let mut tx = pool.begin().await?;

    let child_id = start_child_workflow_in_tx(
        &mut tx,
        &child_spec,
        workflow_id,
        task.task_index,
        parent_depth + 1,
        root_wf_id,
        registry,
        retention,
    )
    .await?;

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

/// Called when a child workflow reaches a terminal state.
///
/// Build the `SUBWORKFLOW_FAILED` error for a failed child workflow.
///
/// Carries the child's `sub_workflow_id` and full `sub_workflow_summary` in
/// `data` so [`TaskError::sub_workflow_details`] can reconstruct the typed
/// `SubWorkflowError` at any error surface (including `WorkflowHandle::get`).
/// `get_workflow_failure_error` re-serializes the whole error, so `data`
/// propagates to the workflow-level error column intact.
fn subworkflow_failed_error(summary: &SubWorkflowSummary) -> crate::core::TaskError {
    crate::core::TaskError {
        error_code: Some(crate::core::TaskErrorCode::User(
            "SUBWORKFLOW_FAILED".to_owned(),
        )),
        message: Some(
            summary
                .error_summary
                .as_deref()
                .unwrap_or("sub-workflow failed")
                .to_owned(),
        ),
        cause: None,
        data: Some(serde_json::json!({
            "sub_workflow_id": summary.child_workflow_id,
            "sub_workflow_summary": summary,
        })),
    }
}

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

    let is_success = child_status == "COMPLETED";

    // Count child task statuses for summary.
    let status_counts: Vec<StatusCount> = sqlx::query_as(COUNT_CHILD_TASK_STATUSES_SQL)
        .bind(child_workflow_id)
        .fetch_all(pool)
        .await?;

    let mut total = 0i32;
    let mut completed = 0i32;
    let mut failed = 0i32;
    let mut skipped = 0i32;

    for sc in &status_counts {
        total += sc.cnt;
        match sc.status.as_str() {
            "COMPLETED" => completed += sc.cnt,
            "FAILED" => failed += sc.cnt,
            "SKIPPED" => skipped += sc.cnt,
            _ => {}
        }
    }

    // An outputless child workflow (no output_index) finalizes with its
    // terminal-results map as the result. That map is the child's own
    // handle.get() shape, not a value the parent node should expose: a parent
    // node over an outputless child has no output. Detect it and propagate None
    // (summary.output = None; parent node result = TaskResult(ok=None)) instead of
    // lifting the nested map. Parity with horsies PR #141.
    let child_is_outputless = sqlx::query_scalar::<_, Option<i32>>(
        "SELECT output_task_index FROM horsies_workflows WHERE id = $1",
    )
    .bind(child_workflow_id)
    .fetch_optional(pool)
    .await?
    .flatten()
    .is_none();

    let output = if child_is_outputless {
        None
    } else {
        match child_result_json {
            Some(json) => match serde_json::from_str::<TaskResult<serde_json::Value>>(json) {
                Ok(TaskResult::Ok(value)) => Some(value),
                _ => None,
            },
            None => None,
        }
    };

    // Determine which success case was satisfied (if any).
    let success_case_name: Option<String> = if is_success {
        // Fetch the child workflow's success_policy to determine the case name.
        let child_policy: Option<ChildPolicyRow> =
            sqlx::query_as("SELECT success_policy FROM horsies_workflows WHERE id = $1")
                .bind(child_workflow_id)
                .fetch_optional(pool)
                .await?;

        match child_policy.and_then(|m| m.success_policy) {
            Some(policy_json) => match serde_json::from_value::<SuccessPolicy>(policy_json) {
                Ok(policy) => {
                    let child_statuses: Vec<TaskStatusRow> = sqlx::query_as(ALL_TASK_STATUSES_SQL)
                        .bind(child_workflow_id)
                        .fetch_all(pool)
                        .await?;
                    let status_vec: Vec<WorkflowTaskStatus> = child_statuses
                        .iter()
                        .map(|s| parse_workflow_task_status(&s.status))
                        .collect::<Result<_, _>>()?;
                    policy.evaluate_with_case_name(&status_vec)
                }
                Err(_) => None,
            },
            None => None,
        }
    } else {
        None
    };

    let summary = SubWorkflowSummary {
        status: child_status.to_owned(),
        is_success,
        success_case: success_case_name,
        output: output.clone(),
        total_tasks: total,
        completed_tasks: completed,
        failed_tasks: failed,
        skipped_tasks: skipped,
        error_summary: if !is_success {
            Some(format!(
                "child workflow {}: {}/{} completed, {} failed, {} skipped",
                child_status, completed, total, failed, skipped,
            ))
        } else {
            None
        },
        child_workflow_id,
    };

    let summary_json = serde_json::to_string(&summary)?;
    let result_json = if is_success {
        if child_is_outputless {
            // Outputless child: parent node result is TaskResult(ok=None), not the
            // child's terminal-results map. Parity with horsies PR #141.
            let wrapped = TaskResult::Ok(serde_json::Value::Null);
            serde_json::to_string(&wrapped)?
        } else {
            match child_result_json {
                Some(json) => json.to_owned(),
                None => {
                    let wrapped = TaskResult::Ok(serde_json::Value::Null);
                    serde_json::to_string(&wrapped)?
                }
            }
        }
    } else {
        let err = subworkflow_failed_error(&summary);
        let wrapped = TaskResult::<serde_json::Value>::Err(err);
        serde_json::to_string(&wrapped)?
    };

    let updated = if is_success {
        let row: Option<IdRow> = sqlx::query_as(UPDATE_SUBWORKFLOW_COMPLETED_SQL)
            .bind(&result_json)
            .bind(&summary_json)
            .bind(parent_workflow_id)
            .bind(parent_task_index)
            .fetch_optional(pool)
            .await?;

        if row.is_some() {
            tracing::debug!(
                parent_workflow_id = %parent_workflow_id,
                parent_task_index,
                child_workflow_id = %child_workflow_id,
                "sub-workflow completed successfully",
            );
        }
        row.is_some()
    } else {
        let error_json = serde_json::to_string(&subworkflow_failed_error(&summary))?;

        let row: Option<IdRow> = sqlx::query_as(UPDATE_SUBWORKFLOW_FAILED_SQL)
            .bind(&result_json)
            .bind(&error_json)
            .bind(&summary_json)
            .bind(parent_workflow_id)
            .bind(parent_task_index)
            .fetch_optional(pool)
            .await?;

        if row.is_some() {
            tracing::debug!(
                parent_workflow_id = %parent_workflow_id,
                parent_task_index,
                child_workflow_id = %child_workflow_id,
                "sub-workflow failed",
            );
        }
        row.is_some()
    };

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
    if !is_success {
        let on_error: String =
            sqlx::query_scalar("SELECT on_error FROM horsies_workflows WHERE id = $1")
                .bind(parent_workflow_id)
                .fetch_one(pool)
                .await?;

        let error_json = serde_json::to_string(&subworkflow_failed_error(&summary))?;

        // Sub-workflow failures mark the node via UPDATE_SUBWORKFLOW_FAILED_SQL
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
            let first_error = get_workflow_failure_error(&mut tx, workflow_id, &None, None).await?;
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
            let first_error = get_workflow_failure_error(&mut tx, workflow_id, &None, None).await?;
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
    check_workflow_completion_inner(pool, workflow_id, registry, payload, retention)
}

#[allow(clippy::explicit_auto_deref)]
fn check_workflow_completion_inner<'a>(
    pool: &'a PgPool,
    workflow_id: Uuid,
    registry: &'a WorkflowSpecRegistry,
    payload: &'a PayloadPolicy,
    retention: &'a RetentionConfig,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), WorkflowError>> + Send + 'a>> {
    Box::pin(async move {
        // Cheap unlocked pre-check: if any workflow task is still non-terminal,
        // the workflow cannot complete yet, so skip the lock entirely. This runs
        // after every task completion, so holding the workflow row lock across
        // the count round trip here serialized all same-workflow completions
        // (~2 RTT of lock-hold each) — a large fan-in spent seconds in pure
        // serialized lock-hold (P3). A stale non-zero count is safe: it merely
        // defers finalize to a later completion's check; the completion that
        // terminalizes the last task reads 0 below and proceeds. The final
        // completion always observes 0.
        let pre_count: NonTerminalCount = sqlx::query_as(COUNT_NON_TERMINAL_SQL)
            .bind(workflow_id)
            .fetch_one(pool)
            .await?;
        if pre_count.cnt > 0 {
            return Ok(());
        }

        // Looks complete — now take the lock and re-count authoritatively to
        // serialize concurrent finalizers and guard against a task transitioning
        // between the unlocked read above and here. Matches Python's
        // LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL pattern.
        let mut tx = pool.begin().await?;

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
            return Ok(()); // Still has non-terminal tasks.
        }

        // Only a RUNNING workflow may be finalized. PAUSED waits for manual
        // intervention; CANCELLED/COMPLETED/FAILED are terminal — a late task
        // completion must not resurrect them. (Rust sets completed_at on cancel,
        // so the completed_at guard on the mark SQL already blocks resurrection;
        // this short-circuit additionally avoids wasted finalize work on a
        // terminal workflow. Matches Python PR #101 ed61c3a2.)
        let status_row: Option<WorkflowStatusRow> = sqlx::query_as(GET_WORKFLOW_STATUS_SQL)
            .bind(workflow_id)
            .fetch_optional(&mut *tx)
            .await?;
        match status_row {
            Some(ref row) if row.status == "RUNNING" => {}
            _ => {
                tx.commit().await?;
                return Ok(());
            }
        }

        // All tasks terminal — evaluate success.
        let meta: WorkflowMeta = sqlx::query_as(GET_WORKFLOW_META_SQL)
            .bind(workflow_id)
            .fetch_one(&mut *tx)
            .await?;

        // Count failures.
        let statuses: Vec<TaskStatusRow> = sqlx::query_as(ALL_TASK_STATUSES_SQL)
            .bind(workflow_id)
            .fetch_all(&mut *tx)
            .await?;

        let has_failure = statuses.iter().any(|s| s.status == "FAILED");

        let is_success = evaluate_workflow_success(&meta.success_policy, has_failure, &statuses)?;

        // Get the final result.
        let result_json =
            get_workflow_final_result(&mut tx, workflow_id, meta.output_task_index).await?;

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
            // the same selection as normal completion — no stale error is
            // preserved. Matches Python PR #27.
            let error_json = Some(
                get_workflow_failure_error(&mut *tx, workflow_id, &meta.success_policy, None)
                    .await?,
            );

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
            return Ok(());
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
                if is_success { Some(&result_json) } else { None },
                registry,
                payload,
                retention,
            )
            .await?;
        }

        Ok(())
    })
}

/// Evaluate whether the workflow should be considered successful.
fn evaluate_workflow_success(
    success_policy_json: &Option<serde_json::Value>,
    has_failure: bool,
    statuses: &[TaskStatusRow],
) -> Result<bool, WorkflowError> {
    match success_policy_json {
        None => {
            // Default: no failures means success.
            Ok(!has_failure)
        }
        Some(policy_json) => {
            // Parse policy and evaluate.
            let policy: SuccessPolicy = serde_json::from_value(policy_json.clone())?;

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
/// If `output_task_index` is set, return that task's result.
/// Otherwise, return a `TaskResult` containing a JSON object of terminal task results
/// (tasks that are not dependencies of any other task) keyed by node_id.
async fn get_workflow_final_result(
    conn: &mut sqlx::PgConnection,
    workflow_id: Uuid,
    output_task_index: Option<i32>,
) -> Result<String, WorkflowError> {
    if let Some(idx) = output_task_index {
        let row: TaskResultOnly = sqlx::query_as(GET_TASK_RESULT_BY_INDEX_SQL)
            .bind(workflow_id)
            .bind(idx)
            .fetch_one(&mut *conn)
            .await?;

        let result_json = row.result.unwrap_or_else(|| {
            let wrapped = TaskResult::Ok(serde_json::Value::Null);
            serde_json::to_string(&wrapped).unwrap_or_else(|_| "{}".to_owned())
        });
        return Ok(result_json);
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

    let mut result_map = serde_json::Map::new();
    for row in rows {
        if let Some(node_id) = row.node_id {
            let value = match &row.result {
                Some(json) => serde_json::from_str(json).unwrap_or(serde_json::Value::Null),
                None => serde_json::Value::Null,
            };
            result_map.insert(node_id, value);
        }
    }
    let wrapped = TaskResult::Ok(serde_json::Value::Object(result_map));
    Ok(serde_json::to_string(&wrapped)?)
}

/// Get a specific error for a failed workflow.
///
/// Matches Python's `_get_workflow_failure_error()`:
/// - Without success_policy: returns the first failed task's error (by index).
/// - With success_policy: returns the first failed required task's error,
///   or a WORKFLOW_SUCCESS_CASE_NOT_MET sentinel if no required task failed.
async fn get_workflow_failure_error(
    conn: &mut sqlx::PgConnection,
    workflow_id: Uuid,
    success_policy_json: &Option<serde_json::Value>,
    recovered_terminal_status: Option<&str>,
) -> Result<String, WorkflowError> {
    match success_policy_json {
        None => {
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
                        return Ok(serde_json::to_string(&task_error)?);
                    }
                }
            }

            let fallback = match recovered_terminal_status {
                Some("CANCELLED") => TaskError::builtin(
                    OutcomeCode::TaskCancelled,
                    "Task was cancelled before producing a result",
                ),
                Some("EXPIRED") => TaskError::builtin(
                    OutcomeCode::TaskExpired,
                    "Task expired before execution started (good_until passed)",
                ),
                Some("COMPLETED") => TaskError::builtin(
                    RetrievalCode::ResultNotAvailable,
                    "Task completed but result is missing",
                ),
                Some(status) => TaskError::builtin(
                    OperationalErrorCode::WorkerCrashed,
                    format!(
                        "Worker crashed during task execution (task_status={status}, no result stored)"
                    ),
                ),
                None => TaskError::builtin(
                    OutcomeCode::WorkflowSuccessCaseNotMet,
                    "one or more tasks failed",
                ),
            };
            Ok(serde_json::to_string(&fallback)?)
        }
        Some(policy_json) => {
            // With success_policy: find first failed required task.
            // Collect all required_indices across all cases.
            let mut all_required: Vec<i32> = Vec::new();
            if let Some(cases) = policy_json.get("cases").and_then(|c| c.as_array()) {
                for case in cases {
                    if let Some(indices) = case.get("required_indices").and_then(|i| i.as_array()) {
                        for idx in indices {
                            if let Some(n) = idx.as_i64() {
                                all_required.push(n as i32);
                            }
                        }
                    }
                }
            }

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
                            return Ok(serde_json::to_string(&task_error)?);
                        }
                    }
                }
            }

            // No required task failed but no case satisfied (all SKIPPED?).
            Ok(serde_json::to_string(&TaskError::builtin(
                OutcomeCode::WorkflowSuccessCaseNotMet,
                "no success case was satisfied",
            ))?)
        }
    }
}

async fn recovered_phase2_failure_error(
    conn: &mut sqlx::PgConnection,
    workflow_id: Uuid,
    node_row_id: Uuid,
    terminal_status: Option<&str>,
) -> Result<String, WorkflowError> {
    let row: Option<TaskResultOnly> = sqlx::query_as(PHASE2_NODE_RESULT_SQL)
        .bind(node_row_id)
        .bind(workflow_id)
        .fetch_optional(&mut *conn)
        .await?;
    if let Some(Some(result_json)) = row.map(|row| row.result) {
        if let Ok(TaskResult::<serde_json::Value>::Err(error)) =
            serde_json::from_str::<TaskResult<serde_json::Value>>(&result_json)
        {
            return Ok(serde_json::to_string(&error)?);
        }
    }

    let status = terminal_status.unwrap_or("");
    let (error_code, message) = match status {
        "CANCELLED" => (
            OutcomeCode::TaskCancelled.into(),
            "Task was cancelled before producing a result".to_owned(),
        ),
        "EXPIRED" => (
            OutcomeCode::TaskExpired.into(),
            "Task expired before execution started (good_until passed)".to_owned(),
        ),
        "COMPLETED" => (
            RetrievalCode::ResultNotAvailable.into(),
            "Task completed but result is missing".to_owned(),
        ),
        _ => (
            OperationalErrorCode::WorkerCrashed.into(),
            format!(
                "Worker crashed during task execution (task_status={status}, no result stored)"
            ),
        ),
    };
    Ok(serde_json::to_string(&TaskError {
        error_code: Some(error_code),
        message: Some(message),
        cause: None,
        data: Some(serde_json::json!({"task_status": status})),
    })?)
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
                status: row.status,
                result: row.result,
            },
        );
    }
    Ok(map)
}

use crate::workflow_engine::args_merge::merge_args_from_sync as merge_args_from;

#[derive(Debug)]
struct WorkflowContextData {
    workflow_id: Uuid,
    task_index: i32,
    task_name: String,
    results_by_id: HashMap<String, TaskResult<serde_json::Value>>,
    summaries_by_id: HashMap<String, SubWorkflowSummary>,
}

impl WorkflowContextData {
    fn to_payload(&self) -> Result<serde_json::Value, WorkflowError> {
        let mut results_by_id: HashMap<String, String> =
            HashMap::with_capacity(self.results_by_id.len());
        for (node_id, result) in &self.results_by_id {
            results_by_id.insert(node_id.clone(), serde_json::to_string(result)?);
        }

        let mut summaries_by_id: HashMap<String, String> =
            HashMap::with_capacity(self.summaries_by_id.len());
        for (node_id, summary) in &self.summaries_by_id {
            summaries_by_id.insert(node_id.clone(), serde_json::to_string(summary)?);
        }

        Ok(serde_json::json!({
            "workflow_id": self.workflow_id.clone(),
            "task_index": self.task_index,
            "task_name": self.task_name.clone(),
            "results_by_id": results_by_id,
            "summaries_by_id": summaries_by_id,
        }))
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
        return Ok(WorkflowContextData {
            workflow_id,
            task_index,
            task_name: task_name.to_owned(),
            results_by_id: HashMap::new(),
            summaries_by_id: HashMap::new(),
        });
    }

    let rows: Vec<CtxResultRow> = sqlx::query_as(GET_CTX_RESULTS_BY_NODE_ID_SQL)
        .bind(workflow_id)
        .bind(ctx_from_ids)
        .fetch_all(pool)
        .await?;

    let (results_by_id, summaries_by_id) = collect_ctx_maps(rows)?;

    Ok(WorkflowContextData {
        workflow_id,
        task_index,
        task_name: task_name.to_owned(),
        results_by_id,
        summaries_by_id,
    })
}

#[allow(clippy::type_complexity)]
fn collect_ctx_maps(
    rows: Vec<CtxResultRow>,
) -> Result<
    (
        HashMap<String, TaskResult<serde_json::Value>>,
        HashMap<String, SubWorkflowSummary>,
    ),
    WorkflowError,
> {
    let mut results_by_id = HashMap::with_capacity(rows.len());
    let mut summaries_by_id = HashMap::with_capacity(rows.len());

    for row in rows {
        let Some(node_id) = row.node_id else { continue };
        let status = row.status.as_str();

        let wrapped: TaskResult<serde_json::Value> = if status == "SKIPPED" {
            TaskResult::Err(TaskError::builtin(
                OutcomeCode::UpstreamSkipped,
                format!("upstream node '{}' was skipped", node_id),
            ))
        } else if let Some(json) = row.result {
            serde_json::from_str(&json)?
        } else {
            TaskResult::Err(TaskError::builtin(
                RetrievalCode::ResultNotAvailable,
                format!("no stored result for node '{}'", node_id),
            ))
        };

        results_by_id.insert(node_id.clone(), wrapped);

        if let Some(summary_json) = row.sub_workflow_summary {
            let summary: SubWorkflowSummary = serde_json::from_str(&summary_json)?;
            summaries_by_id.insert(node_id, summary);
        }
    }

    Ok((results_by_id, summaries_by_id))
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
mod p7_exact_error_tests {
    use super::*;
    use crate::core::{OperationalErrorCode, TaskErrorCode};
    use serial_test::serial;

    async fn seed_workflow(
        pool: &PgPool,
        id: Uuid,
        status: &str,
        on_error: &str,
        parent: Option<(Uuid, i32)>,
    ) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, definition_key, depth,
                 root_workflow_id, parent_workflow_id, parent_task_index,
                 sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'p7_exact_error', $2, $3, $4, $5, $6, $7, $8,
                       NOW(), NOW(), NOW(), NOW())",
        )
        .bind(id)
        .bind(status)
        .bind(on_error)
        .bind(format!("test.p7.exact-error.{id}"))
        .bind(if parent.is_some() { 1_i32 } else { 0_i32 })
        .bind(parent.map(|(root, _)| root).unwrap_or(id))
        .bind(parent.map(|(root, _)| root))
        .bind(parent.map(|(_, index)| index))
        .execute(pool)
        .await
        .unwrap();
    }

    async fn seed_prior_failure(pool: &PgPool, workflow_id: Uuid) {
        let prior = TaskError::new("PRIOR_FAILURE", "prior node failed");
        let result = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(prior)).unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                 id, workflow_id, task_index, node_id, task_name, queue_name,
                 priority, dependencies, allow_failed_deps, join_type, status,
                 result, is_subworkflow, completed_at, created_at
             ) VALUES ($1, $2, 0, 'prior', 'prior', 'default', 100, '{}',
                       FALSE, 'all', 'FAILED', $3, FALSE, NOW(), NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .bind(result)
        .execute(pool)
        .await
        .unwrap();
    }

    fn unresolved_subworkflow_node() -> DependentRow {
        DependentRow {
            task_index: 1,
            dependencies: Vec::new(),
            args_from: None,
            workflow_ctx_from: None,
            allow_failed_deps: false,
            join_type: "all".to_owned(),
            min_success: None,
            task_name: "__sub_workflow:p7_missing_child".to_owned(),
            task_args: None,
            task_kwargs: None,
            queue_name: "default".to_owned(),
            priority: 100,
            node_id: Some("missing_child".to_owned()),
            task_options: None,
            status: "READY".to_owned(),
            is_subworkflow: true,
            sub_workflow_name: Some("p7_missing_child".to_owned()),
            sub_definition_key: Some("missing.definition".to_owned()),
        }
    }

    #[tokio::test]
    #[serial]
    async fn phase2_subworkflow_load_pause_stores_the_triggering_node_error() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id, "RUNNING", "pause", None).await;
        seed_prior_failure(&pool, workflow_id).await;
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                 id, workflow_id, task_index, node_id, task_name, queue_name,
                 priority, dependencies, allow_failed_deps, join_type, status,
                 is_subworkflow, sub_workflow_name, sub_definition_key, created_at
             ) VALUES ($1, $2, 1, 'missing_child', '__sub_workflow:p7_missing_child',
                       'default', 100, '{}', FALSE, 'all', 'READY', TRUE,
                       'p7_missing_child', 'missing.definition', NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .execute(&pool)
        .await
        .unwrap();

        let mut transaction = pool.begin().await.unwrap();
        enqueue_phase2_subworkflow_in_tx(
            &mut transaction,
            workflow_id,
            &unresolved_subworkflow_node(),
            &WorkflowSpecRegistry::new(),
            &HashMap::new(),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let (status, error_json): (String, String) =
            sqlx::query_as("SELECT status, error FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        let error: TaskError = serde_json::from_str(&error_json).unwrap();
        assert_eq!(status, "PAUSED");
        assert_eq!(
            error.error_code,
            Some(TaskErrorCode::from(
                OperationalErrorCode::SubworkflowLoadFailed,
            )),
        );
    }

    #[tokio::test]
    #[serial]
    async fn terminal_child_pause_stores_the_triggering_subworkflow_error() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let parent_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        seed_workflow(&pool, parent_id, "RUNNING", "pause", None).await;
        seed_prior_failure(&pool, parent_id).await;
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                 id, workflow_id, task_index, node_id, task_name, queue_name,
                 priority, dependencies, allow_failed_deps, join_type, status,
                 is_subworkflow, created_at
             ) VALUES ($1, $2, 1, 'child', '__sub_workflow:p7_child', 'default',
                       100, '{}', FALSE, 'all', 'RUNNING', TRUE, NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(parent_id)
        .execute(&pool)
        .await
        .unwrap();
        seed_workflow(&pool, child_id, "FAILED", "fail", Some((parent_id, 1))).await;
        sqlx::query(
            "UPDATE horsies_workflow_tasks SET sub_workflow_id = $3
             WHERE workflow_id = $1 AND task_index = $2",
        )
        .bind(parent_id)
        .bind(1_i32)
        .bind(child_id)
        .execute(&pool)
        .await
        .unwrap();

        let mut transaction = pool.begin().await.unwrap();
        propagate_terminal_child_in_tx(
            &mut transaction,
            child_id,
            "FAILED",
            None,
            &WorkflowSpecRegistry::new(),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let (status, error_json): (String, String) =
            sqlx::query_as("SELECT status, error FROM horsies_workflows WHERE id = $1")
                .bind(parent_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        let error: TaskError = serde_json::from_str(&error_json).unwrap();
        assert_eq!(status, "PAUSED");
        assert_eq!(
            error.error_code,
            Some(TaskErrorCode::User("SUBWORKFLOW_FAILED".to_owned())),
        );
        assert_eq!(
            error
                .data
                .as_ref()
                .and_then(|data| data.get("sub_workflow_id")),
            Some(&serde_json::json!(child_id)),
        );
    }

    #[tokio::test]
    #[serial]
    async fn lost_pause_cas_does_not_relocate_the_tree_again() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let parent_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        seed_workflow(&pool, parent_id, "PAUSED", "pause", None).await;
        seed_prior_failure(&pool, parent_id).await;
        seed_workflow(&pool, child_id, "RUNNING", "fail", Some((parent_id, 0))).await;
        let node_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = 0",
        )
        .bind(parent_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        let mut transaction = pool.begin().await.unwrap();
        apply_phase2_progression_in_tx(
            &mut transaction,
            parent_id,
            Some(node_id),
            0,
            "FAILED",
            Some("FAILED"),
            Some("pause"),
            Some("RUNNING"),
            &WorkflowSpecRegistry::new(),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let child_status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(child_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(child_status, "RUNNING");
    }
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
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, sub_workflow_id, created_at
            ) VALUES (
                $1, $2, 0, 'node_0', 'subwf_node', '[]', '{}',
                'default', 100, '{}', FALSE, 'all',
                'RUNNING', TRUE, $3, NOW()
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

        let registry = WorkflowSpecRegistry::new();
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

        let registry = WorkflowSpecRegistry::new();
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

        let registry = WorkflowSpecRegistry::new();
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
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, created_at
            ) VALUES (
                $1, $2, $3, $4, 'qw_task', '[]', '{}',
                'default', 100, $5, FALSE, 'all',
                'COMPLETED', FALSE, NOW()
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
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, created_at
            ) VALUES (
                $1, $2, 0, 'node_0', 'sub', '[]', '{}',
                'default', 100, '{}', FALSE, 'all',
                'READY', TRUE, NOW()
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

    /// C16: `COMPLETE_WORKFLOW_TASK_SQL` must pause an `on_error=pause` workflow
    /// atomically with the node-FAILED write, so a crash before the separate
    /// failure handler cannot lose the pause and finalize the workflow FAILED.
    #[tokio::test]
    #[serial]
    async fn node_failed_pauses_on_error_pause_workflow_atomically() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let wf_id = Uuid::new_v4().to_string();
        let task_id = Uuid::new_v4().to_string();

        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES ($1, 'c16_wf', 'RUNNING', 'pause', NULL, 'test.c16.v1', 0, $1,
                      NOW(), NOW(), NOW(), NOW())",
        )
        .bind(test_uuid(&wf_id))
        .execute(&pool)
        .await
        .expect("insert workflow");

        // A non-terminal node linked to the backing task id.
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, task_id, created_at
            ) VALUES ($1, $2, 0, 'node_0', 'c16_task', '[]', '{}',
                      'default', 100, '{}', FALSE, 'all',
                      'RUNNING', FALSE, $3, NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(test_uuid(&wf_id))
        .bind(test_uuid(&task_id))
        .execute(&pool)
        .await
        .expect("insert node");

        // The single statement that writes the node FAILED must also pause the wf.
        let row: CompleteTaskRow = sqlx::query_as(COMPLETE_WORKFLOW_TASK_SQL)
            .bind(test_uuid(&task_id)) // $1
            .bind("FAILED") // $2
            .bind(Option::<String>::None) // $3 result
            .bind(Some(r#"{"message":"boom"}"#)) // $4 error
            .fetch_one(&pool)
            .await
            .expect("complete workflow task");
        assert!(row.cas_won, "node CAS must win");
        assert!(row.wf_paused, "the same statement must pause the workflow");

        let (status, error): (String, Option<String>) =
            sqlx::query_as("SELECT status, error FROM horsies_workflows WHERE id = $1")
                .bind(test_uuid(&wf_id))
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            status, "PAUSED",
            "on_error=pause workflow must be PAUSED atomically"
        );
        assert!(
            error.as_deref().unwrap_or("").contains("boom"),
            "the triggering error must be stored on pause",
        );

        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .ok();
    }

    /// C22: a late sub-workflow completion must not mutate a node whose parent
    /// workflow is already terminal (e.g. cancelled). The CAS now gates on the
    /// parent workflow not being terminal.
    #[tokio::test]
    #[serial]
    async fn subworkflow_completion_blocked_when_parent_terminal() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let wf_id = Uuid::new_v4().to_string();

        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES ($1, 'c22_wf', 'CANCELLED', 'fail', NULL, 'test.c22.v1', 0, $1,
                      NOW(), NOW(), NOW(), NOW())",
        )
        .bind(test_uuid(&wf_id))
        .execute(&pool)
        .await
        .expect("insert workflow");

        // A non-terminal sub-workflow node under the cancelled parent.
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, created_at
            ) VALUES ($1, $2, 0, 'node_0', 'sub', '[]', '{}',
                      'default', 100, '{}', FALSE, 'all',
                      'RUNNING', TRUE, NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(test_uuid(&wf_id))
        .execute(&pool)
        .await
        .expect("insert node");

        // Parent CANCELLED: the sub-workflow completion CAS must be a no-op.
        let blocked = sqlx::query(UPDATE_SUBWORKFLOW_COMPLETED_SQL)
            .bind(Option::<String>::None) // $1 result
            .bind(Option::<String>::None) // $2 summary
            .bind(test_uuid(&wf_id)) // $3
            .bind(0_i32) // $4
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            blocked.rows_affected(),
            0,
            "sub-workflow completion must not touch a node of a terminal parent",
        );
        let node_status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflow_tasks WHERE workflow_id = $1")
                .bind(test_uuid(&wf_id))
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(node_status, "RUNNING", "node must be untouched");

        // Parent RUNNING: the CAS applies.
        sqlx::query("UPDATE horsies_workflows SET status = 'RUNNING' WHERE id = $1")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .unwrap();
        let applied = sqlx::query(UPDATE_SUBWORKFLOW_COMPLETED_SQL)
            .bind(Option::<String>::None)
            .bind(Option::<String>::None)
            .bind(test_uuid(&wf_id))
            .bind(0_i32)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            applied.rows_affected(),
            1,
            "CAS must apply under a RUNNING parent"
        );

        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .ok();
    }

    /// C16 (child cascade): an on_error=pause node failure must still pause the
    /// workflow's RUNNING child workflows. The atomic `pause_wf` CTE wins the
    /// parent RUNNING->PAUSED flip, so `handle_workflow_task_failure` must be told
    /// it owns the cascade (via wf_paused) rather than skip it as a lost race.
    #[tokio::test]
    #[serial]
    async fn on_error_pause_node_failure_cascades_pause_to_running_child() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let registry = WorkflowSpecRegistry::new();
        let parent = Uuid::new_v4().to_string();
        let child = Uuid::new_v4().to_string();
        let task_id = Uuid::new_v4().to_string();
        let node_row_id = Uuid::new_v4();

        // Parent RUNNING (on_error=pause) with a RUNNING child workflow.
        for (id, status, parent_of) in [
            (&parent, "RUNNING", None),
            (&child, "RUNNING", Some(&parent)),
        ] {
            sqlx::query(
                "INSERT INTO horsies_workflows (
                    id, name, status, on_error, output_task_index,
                    definition_key, depth, root_workflow_id, parent_workflow_id,
                    sent_at, created_at, started_at, updated_at
                ) VALUES ($1, 'c16c_wf', $2, 'pause', NULL, 'test.c16c.v1', 0, $1, $3,
                          NOW(), NOW(), NOW(), NOW())",
            )
            .bind(test_uuid(id))
            .bind(status)
            .bind(parent_of.map(|value| test_uuid(value)))
            .execute(&pool)
            .await
            .expect("insert workflow");
        }

        // A RUNNING node on the parent, linked to the backing task id.
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, task_id, created_at
            ) VALUES ($1, $2, 0, 'node_0', 'c16c_task', '[]', '{}',
                      'default', 100, '{}', FALSE, 'all',
                      'RUNNING', FALSE, $3, NOW())",
        )
        .bind(node_row_id)
        .bind(test_uuid(&parent))
        .bind(test_uuid(&task_id))
        .execute(&pool)
        .await
        .expect("insert node");

        let err = TaskError::builtin(OperationalErrorCode::TaskError, "boom");
        let error_json = serde_json::to_string(&err).unwrap();
        let result_json =
            serde_json::to_string(&TaskResult::<serde_json::Value>::Err(err)).unwrap();
        sqlx::query(
            "UPDATE horsies_workflow_tasks
             SET status = 'FAILED', result = $1, error = $2, completed_at = NOW()
             WHERE id = $3",
        )
        .bind(&result_json)
        .bind(&error_json)
        .bind(node_row_id)
        .execute(&pool)
        .await
        .expect("fail workflow node");

        let mut tx = pool.begin().await.expect("begin progression");
        apply_phase2_progression_in_tx(
            &mut tx,
            test_uuid(&parent),
            Some(node_row_id),
            0,
            "FAILED",
            Some("FAILED"),
            Some("pause"),
            Some("RUNNING"),
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("apply failed-node progression");
        tx.commit().await.expect("commit progression");

        let parent_status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(test_uuid(&parent))
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(parent_status, "PAUSED", "parent must pause on_error=pause");

        let child_status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(test_uuid(&child))
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            child_status, "PAUSED",
            "the running child must be paused by the cascade (C16 regression)",
        );

        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(test_uuid(&parent))
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = ANY($1)")
            .bind(vec![test_uuid(&child), test_uuid(&parent)])
            .execute(&pool)
            .await
            .ok();
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
        let registry = WorkflowSpecRegistry::new();

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
                    id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                    queue_name, priority, dependencies, allow_failed_deps, join_type,
                    status, is_subworkflow, created_at
                ) VALUES ($1, $2, 0, 'node_0', 'p3_task', '[]', '{}',
                          'default', 100, '{}', FALSE, 'all', $3, FALSE, NOW())",
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
mod complete_task_merge_tests {
    //! Behavior pins for the merged `COMPLETE_WORKFLOW_TASK_SQL` (parity with
    //! horsies PR #128). The merge collapses locate + CAS-update into one
    //! statement; these tests fix the CAS predicate, the workflow-terminal gate
    //! that subsumes the old pre-update early-return, idempotence, the
    //! failure-path error-column write, and the no-row case.
    use super::*;
    use crate::broker::PostgresBroker;
    use serial_test::serial;
    use uuid::Uuid;

    async fn insert_workflow(pool: &PgPool, id: &str, status: &str) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES (
                $1, 'merge_test_wf', $2, 'pause', NULL,
                'test.merge.v1', 0, $1,
                NOW(), NOW(), NOW(), NOW()
            )",
        )
        .bind(test_uuid(id))
        .bind(status)
        .execute(pool)
        .await
        .expect("insert workflow");
    }

    /// Insert a workflow_task in the given status with a backing `task_id`.
    async fn insert_task(pool: &PgPool, wf_id: &str, task_id: &str, status: &str) {
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, task_id, created_at
            ) VALUES (
                $1, $2, 0, 'node_0', 'merge_task', '[]', '{}',
                'default', 100, '{}', FALSE, 'all',
                $3, FALSE, $4, NOW()
            )",
        )
        .bind(Uuid::new_v4())
        .bind(test_uuid(wf_id))
        .bind(status)
        .bind(test_uuid(task_id))
        .execute(pool)
        .await
        .expect("insert task");
    }

    #[derive(sqlx::FromRow)]
    struct TaskRow {
        status: String,
        result: Option<String>,
        error: Option<String>,
    }

    async fn read_task(pool: &PgPool, wf_id: &str) -> TaskRow {
        sqlx::query_as(
            "SELECT status, result, error FROM horsies_workflow_tasks
             WHERE workflow_id = $1 AND task_index = 0",
        )
        .bind(test_uuid(wf_id))
        .fetch_one(pool)
        .await
        .expect("read task")
    }

    async fn run_complete(
        pool: &PgPool,
        task_id: &str,
        status: &str,
        result: &str,
        error: Option<&str>,
    ) -> Option<CompleteTaskRow> {
        sqlx::query_as(COMPLETE_WORKFLOW_TASK_SQL)
            .bind(test_uuid(task_id))
            .bind(status)
            .bind(result)
            .bind(error)
            .fetch_optional(pool)
            .await
            .expect("run COMPLETE_WORKFLOW_TASK_SQL")
    }

    async fn cleanup(pool: &PgPool, wf_id: &str) {
        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(test_uuid(wf_id))
            .execute(pool)
            .await
            .expect("cleanup tasks");
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(test_uuid(wf_id))
            .execute(pool)
            .await
            .expect("cleanup workflow");
    }

    /// Success CAS on a RUNNING workflow: wins, writes status+result, leaves the
    /// error column untouched (COALESCE(NULL, error)), and returns context.
    #[tokio::test]
    #[serial]
    async fn success_cas_wins_and_returns_context() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let wf_id = Uuid::new_v4().to_string();
        let task_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id, "RUNNING").await;
        insert_task(&pool, &wf_id, &task_id, "RUNNING").await;

        let row = run_complete(&pool, &task_id, "COMPLETED", "{\"Ok\":1}", None)
            .await
            .expect("row present");
        assert!(row.cas_won, "CAS must win on a fresh RUNNING node");
        assert_eq!(row.workflow_id.to_string(), wf_id);
        assert_eq!(row.task_index, 0);
        assert_eq!(row.wf_status, "RUNNING");
        assert_eq!(row.on_error, "pause");

        let t = read_task(&pool, &wf_id).await;
        assert_eq!(t.status, "COMPLETED");
        assert_eq!(t.result.as_deref(), Some("{\"Ok\":1}"));
        assert!(t.error.is_none(), "success must not write the error column");

        cleanup(&pool, &wf_id).await;
    }

    /// Second completion of an already-terminal node is a no-op: cas_won=false,
    /// stored result unchanged. Mirrors the old `updated.is_none()` skip.
    #[tokio::test]
    #[serial]
    async fn idempotent_on_already_terminal_node() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let wf_id = Uuid::new_v4().to_string();
        let task_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id, "RUNNING").await;
        insert_task(&pool, &wf_id, &task_id, "RUNNING").await;

        let first = run_complete(&pool, &task_id, "COMPLETED", "{\"Ok\":1}", None)
            .await
            .expect("first row");
        assert!(first.cas_won);

        let second = run_complete(&pool, &task_id, "COMPLETED", "{\"Ok\":2}", None)
            .await
            .expect("second row still located via found");
        assert!(
            !second.cas_won,
            "already-terminal node must not re-win the CAS"
        );

        let t = read_task(&pool, &wf_id).await;
        assert_eq!(
            t.result.as_deref(),
            Some("{\"Ok\":1}"),
            "stored result must be the first write, not overwritten",
        );

        cleanup(&pool, &wf_id).await;
    }

    /// Workflow already terminal: the gate makes cas_won=false and the node is
    /// NOT mutated — this subsumes the old pre-update early-return.
    #[tokio::test]
    #[serial]
    async fn terminal_workflow_blocks_node_mutation() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let wf_id = Uuid::new_v4().to_string();
        let task_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id, "COMPLETED").await;
        insert_task(&pool, &wf_id, &task_id, "PENDING").await;

        let row = run_complete(&pool, &task_id, "COMPLETED", "{\"Ok\":1}", None)
            .await
            .expect("found still locates the node");
        assert!(!row.cas_won, "terminal workflow must block the CAS");
        assert_eq!(row.wf_status, "COMPLETED");

        let t = read_task(&pool, &wf_id).await;
        assert_eq!(t.status, "PENDING", "node must be untouched");
        assert!(t.result.is_none());

        cleanup(&pool, &wf_id).await;
    }

    /// PAUSED is NOT terminal: the node CAS still wins (the fresh PAUSED guard in
    /// the caller is what stops propagation, not this gate).
    #[tokio::test]
    #[serial]
    async fn paused_workflow_does_not_block_node_cas() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let wf_id = Uuid::new_v4().to_string();
        let task_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id, "PAUSED").await;
        insert_task(&pool, &wf_id, &task_id, "RUNNING").await;

        let row = run_complete(&pool, &task_id, "COMPLETED", "{\"Ok\":1}", None)
            .await
            .expect("row present");
        assert!(row.cas_won, "PAUSED must not block the node CAS");
        assert_eq!(row.wf_status, "PAUSED");

        cleanup(&pool, &wf_id).await;
    }

    /// Failure path writes the error column via COALESCE($4, error).
    #[tokio::test]
    #[serial]
    async fn failure_writes_error_column() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let wf_id = Uuid::new_v4().to_string();
        let task_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &wf_id, "RUNNING").await;
        insert_task(&pool, &wf_id, &task_id, "RUNNING").await;

        let row = run_complete(
            &pool,
            &task_id,
            "FAILED",
            "{\"Err\":{}}",
            Some("{\"error_code\":\"task_error\"}"),
        )
        .await
        .expect("row present");
        assert!(row.cas_won);

        let t = read_task(&pool, &wf_id).await;
        assert_eq!(t.status, "FAILED");
        assert_eq!(t.error.as_deref(), Some("{\"error_code\":\"task_error\"}"));

        cleanup(&pool, &wf_id).await;
    }

    /// Unknown task_id (or missing workflow row) yields no row → caller no-ops.
    #[tokio::test]
    #[serial]
    async fn unknown_task_id_returns_no_row() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let row = run_complete(
            &pool,
            &Uuid::new_v4().to_string(),
            "COMPLETED",
            "{\"Ok\":1}",
            None,
        )
        .await;
        assert!(row.is_none(), "no workflow task → no row");
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
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, created_at
            ) VALUES (
                $1::uuid, $2::uuid, $3, $4, 'promo_task', '[]', '{}',
                'default', 100, $5, $6, $7,
                $8, FALSE, NOW()
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
        sqlx::query("DELETE FROM horsies_tasks WHERE enqueue_sha LIKE 'wf-%' AND id IN (SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1::uuid)")
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
        let registry = WorkflowSpecRegistry::new();
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
               AND prepared_rerun_input_disposition = 'NEVER_ELIGIBLE'
               AND octet_length(command_fingerprint) = 32
               AND octet_length(input_digest) = 32
               AND rerun_of_task_id IS NULL
               AND rerun_root_task_id IS NULL
               AND idempotency_key_digest IS NULL
               AND retain_rerun_input = FALSE
               AND prepared_rerun_input_inline IS NULL
               AND prepared_rerun_input_reference IS NULL",
        )
        .bind(test_uuid(&wf_id))
        .fetch_one(&pool)
        .await
        .expect("count conformant promoted task facts");
        assert_eq!(conformant_count, fan as i64);

        cleanup(&pool, &wf_id).await;
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
        let registry = WorkflowSpecRegistry::new();
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

    /// allow_failed_deps: a failed dep still promotes the dependent (not skipped).
    #[tokio::test]
    #[serial]
    async fn allow_failed_deps_promotes() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let registry = WorkflowSpecRegistry::new();
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
        let registry = WorkflowSpecRegistry::new();
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

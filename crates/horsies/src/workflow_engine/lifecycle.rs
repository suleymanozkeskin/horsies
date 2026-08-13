use std::collections::VecDeque;

use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::history::enqueue::{prepare_enqueue_facts, EnqueueInputEligibility};
use crate::core::task::retry_utils::parse_max_retries;
use crate::core::workflow::handle_types::{HandleErrorCode, HandleOperationError, HandleResult};
use crate::core::WorkflowSpecRegistry;
use sqlx::PgPool;

use crate::workflow_engine::engine;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::parse_good_until_from_options;
use crate::workflow_engine::start::{materialize_child_spec, start_child_workflow_in_tx};

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

const CANCEL_WORKFLOW_SQL: &str = "\
UPDATE horsies_workflows
SET status = 'CANCELLED', completed_at = NOW(), updated_at = NOW()
WHERE id = $1 AND status IN ('PENDING', 'RUNNING', 'PAUSED')
RETURNING id";

/// Lock the workflow row first, before any workflow_task rows, so the cancel
/// transaction acquires `{horsies_workflows, horsies_workflow_tasks}` in the same
/// order as `COMPLETE_WORKFLOW_TASK_SQL` (workflows before workflow_tasks). The
/// two paths previously ran in opposite orders and could deadlock under
/// contention — a task completing while its workflow is being cancelled — with
/// Postgres aborting one side (SQLSTATE 40P01). Lock-order invariant (N6): any
/// transaction locking both tables must take horsies_workflows first.
const LOCK_WORKFLOW_ROW_FOR_CANCEL_SQL: &str = "\
SELECT id FROM horsies_workflows WHERE id = $1 FOR UPDATE";

/// Lock the workflow's backing horsies_tasks rows before flipping the workflow
/// status, so a concurrent worker claim (which uses `FOR UPDATE SKIP LOCKED`)
/// cannot pick them up during the cancellation window. Excludes terminal rows.
const LOCK_WORKFLOW_BACKING_TASKS_FOR_CANCEL_SQL: &str = "\
SELECT t.id
FROM horsies_tasks t
JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
WHERE wt.workflow_id = $1
  AND wt.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
  AND t.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
FOR UPDATE OF t";

/// Lock the workflow's non-terminal workflow_task rows for the duration of the
/// cancellation transaction.
const LOCK_WORKFLOW_TASKS_FOR_CANCEL_SQL: &str = "\
SELECT id
FROM horsies_workflow_tasks
WHERE workflow_id = $1
  AND status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
FOR UPDATE";

// Step 1 of workflow cancellation — cancelling not-yet-started backing tasks
// of ENQUEUED workflow_tasks — is `horsies_cancel_nodes_of_cancelled_workflow`
// (broker/terminalization.rs): the workflow's CANCELLED status is verified
// in-statement, so the function runs after the status flip in the same
// transaction. A backing task may briefly be RUNNING while its node is still
// ENQUEUED; user code runs only after the node's own RUNNING handoff, so
// that state is still cancellable.

/// Step 2: Skip PENDING/READY workflow_tasks (not ENQUEUED or RUNNING).
const SKIP_PENDING_READY_TASKS_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = 'SKIPPED'
WHERE workflow_id = $1 AND status IN ('PENDING', 'READY')";

/// Step 3: Skip ENQUEUED workflow_tasks whose horsies_tasks were just cancelled.
const SKIP_CANCELLED_ENQUEUED_TASKS_SQL: &str = "\
UPDATE horsies_workflow_tasks wt
SET status = 'SKIPPED',
    completed_at = NOW()
FROM horsies_tasks t
WHERE wt.workflow_id = $1
  AND wt.task_id = t.id
  AND wt.status = 'ENQUEUED'
  AND t.status = 'CANCELLED'";

const PAUSE_WORKFLOW_SQL: &str = "\
UPDATE horsies_workflows
SET status = 'PAUSED', updated_at = NOW()
WHERE id = $1 AND status = 'RUNNING'
RETURNING id";

const RESUME_WORKFLOW_SQL: &str = "\
UPDATE horsies_workflows
SET status = 'RUNNING', updated_at = NOW()
WHERE id = $1 AND status = 'PAUSED'
RETURNING id";

/// Check if a workflow exists (regardless of status).
const CHECK_WORKFLOW_EXISTS_SQL: &str = "\
SELECT id FROM horsies_workflows WHERE id = $1";

/// Current status of a workflow (for the idempotent resume path).
const GET_WORKFLOW_STATUS_SQL: &str = "\
SELECT status FROM horsies_workflows WHERE id = $1";

const FIND_PENDING_TASKS_SQL: &str = "\
SELECT task_index, dependencies, task_name, task_args, task_kwargs,
       queue_name, priority, args_from, workflow_ctx_from,
       allow_failed_deps, join_type, min_success,
       node_id, task_options
FROM horsies_workflow_tasks
WHERE workflow_id = $1::uuid AND status = 'PENDING'";

const FIND_READY_TASKS_SQL: &str = "\
SELECT task_index, task_name, task_args, task_kwargs,
       queue_name, priority, task_options, node_id,
       args_from, workflow_ctx_from, dependencies, is_subworkflow,
       sub_workflow_name, sub_definition_key
FROM horsies_workflow_tasks
WHERE workflow_id = $1::uuid AND status = 'READY'";

/// Find terminal deps for a PENDING task (used for resume re-evaluation).
const FIND_TERMINAL_DEPS_SQL: &str = "\
SELECT task_index
FROM horsies_workflow_tasks
WHERE workflow_id = $1 AND task_index = ANY($2)
  AND status IN ('COMPLETED', 'FAILED', 'SKIPPED')";

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

/// Find RUNNING child workflows of a given parent.
const FIND_RUNNING_CHILDREN_SQL: &str = "\
SELECT id FROM horsies_workflows
WHERE parent_workflow_id = $1 AND status = 'RUNNING'";

/// Find non-terminal child workflows of a given parent (for cancel cascade).
const FIND_CANCELLABLE_CHILDREN_SQL: &str = "\
SELECT id FROM horsies_workflows
WHERE parent_workflow_id = $1 AND status IN ('PENDING', 'RUNNING', 'PAUSED')";

/// Pause a single child workflow (RUNNING -> PAUSED).
const PAUSE_CHILD_WORKFLOW_SQL: &str = "\
UPDATE horsies_workflows
SET status = 'PAUSED', updated_at = NOW()
WHERE id = $1 AND status = 'RUNNING'";

/// Reset the abandoned nodes of just-cancelled backing rows to READY, so
/// resume enqueues a fresh row for each. Runs in the same transaction as
/// `horsies_abandon_nodes_of_paused_workflows`, whose applied outcomes name
/// the rows this reset targets.
const RESET_ABANDONED_NODES_SQL: &str = "\
UPDATE horsies_workflow_tasks wt
SET status = 'READY', task_id = NULL, started_at = NULL
WHERE wt.task_id = ANY($1)
  AND wt.status IN ('ENQUEUED', 'RUNNING')";

/// Find PAUSED child workflows of a given parent.
const FIND_PAUSED_CHILDREN_SQL: &str = "\
SELECT id, depth, root_workflow_id FROM horsies_workflows
WHERE parent_workflow_id = $1 AND status = 'PAUSED'";

/// Resume a single child workflow (PAUSED -> RUNNING).
const RESUME_CHILD_WORKFLOW_SQL: &str = "\
UPDATE horsies_workflows
SET status = 'RUNNING', updated_at = NOW()
WHERE id = $1 AND status = 'PAUSED'";

/// Get parent depth and root workflow ID (used for sub-workflow launch on resume).
const GET_WORKFLOW_DEPTH_SQL: &str = "\
SELECT depth, root_workflow_id
FROM horsies_workflows
WHERE id = $1";

/// Link a sub-workflow task to its child workflow.
const LINK_SUBWORKFLOW_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET sub_workflow_id = $1, status = 'ENQUEUED', started_at = NOW()
WHERE workflow_id = $2 AND task_index = $3
  AND status = 'READY'";

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct IdRow {
    id: String,
}

#[derive(Debug, sqlx::FromRow)]
struct TerminalDepRow {
    task_index: i32,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct PendingTaskRow {
    task_index: i32,
    dependencies: Vec<i32>,
    task_name: String,
    task_args: Option<String>,
    task_kwargs: Option<String>,
    queue_name: String,
    priority: i32,
    args_from: Option<serde_json::Value>,
    workflow_ctx_from: Option<Vec<String>>,
    allow_failed_deps: bool,
    join_type: String,
    min_success: Option<i32>,
    node_id: Option<String>,
    task_options: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct ReadyTaskRow {
    task_index: i32,
    task_name: String,
    task_args: Option<String>,
    task_kwargs: Option<String>,
    queue_name: String,
    priority: i32,
    task_options: Option<String>,
    node_id: Option<String>,
    args_from: Option<serde_json::Value>,
    workflow_ctx_from: Option<Vec<String>>,
    dependencies: Vec<i32>,
    is_subworkflow: bool,
    sub_workflow_name: Option<String>,
    sub_definition_key: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct ChildWorkflowRow {
    id: String,
    depth: Option<i32>,
    root_workflow_id: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct DepthRow {
    depth: Option<i32>,
    root_workflow_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a `WorkflowError` into a `HandleOperationError` for standalone lifecycle functions.
#[allow(clippy::needless_pass_by_value)]
fn to_handle_error(e: WorkflowError, workflow_id: &str) -> HandleOperationError {
    let (code, retryable) = match &e {
        WorkflowError::WorkflowNotFound { .. } => (HandleErrorCode::WorkflowNotFound, false),
        WorkflowError::Database(_) | WorkflowError::Broker(_) => {
            (HandleErrorCode::DbOperationFailed, true)
        }
        _ => (HandleErrorCode::InternalFailed, false),
    };
    HandleOperationError {
        code,
        message: e.to_string(),
        retryable,
        workflow_id: workflow_id.to_owned(),
    }
}

/// Check if a workflow exists. Returns `Err(WorkflowNotFound)` if not.
async fn ensure_workflow_exists(pool: &PgPool, workflow_id: &str) -> Result<(), WorkflowError> {
    let exists: Option<IdRow> = sqlx::query_as(CHECK_WORKFLOW_EXISTS_SQL)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;
    if exists.is_none() {
        return Err(WorkflowError::WorkflowNotFound {
            workflow_id: workflow_id.to_owned(),
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Cancel a workflow.
///
/// Transitions RUNNING/PAUSED -> CANCELLED. Skips all non-terminal
/// workflow_tasks and cancels any enqueued horsies_tasks.
///
/// Returns `true` if the workflow was actually cancelled.
pub async fn cancel_workflow(pool: &PgPool, workflow_id: &str) -> HandleResult<bool> {
    cancel_workflow_inner(pool, workflow_id)
        .await
        .map_err(|e| to_handle_error(e, workflow_id))
}

/// Lock the workflow row + backing tasks + workflow_tasks, flip the workflow
/// status to CANCELLED, and run the backing-task cleanup — all in one
/// transaction so the locks are held from before the status flip through commit.
/// Worker claiming uses `FOR UPDATE SKIP LOCKED`, so these locks close the window
/// where a worker could claim a queued task during the uncommitted cancellation
/// (horsies #65).
///
/// Lock order (N6): the workflow row is locked first, before any
/// workflow_task rows, matching `COMPLETE_WORKFLOW_TASK_SQL` (workflows before
/// workflow_tasks). Opposite orders across the two paths deadlock under
/// contention.
///
/// Returns `true` if the workflow was non-terminal and got cancelled.
async fn cancel_one_workflow_in_tx(
    pool: &PgPool,
    workflow_id: &str,
) -> Result<bool, WorkflowError> {
    let mut tx = pool.begin().await?;

    // Workflow row first — see the lock-order invariant above and on
    // COMPLETE_WORKFLOW_TASK_SQL.
    sqlx::query(LOCK_WORKFLOW_ROW_FOR_CANCEL_SQL)
        .bind(workflow_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(LOCK_WORKFLOW_BACKING_TASKS_FOR_CANCEL_SQL)
        .bind(workflow_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(LOCK_WORKFLOW_TASKS_FOR_CANCEL_SQL)
        .bind(workflow_id)
        .execute(&mut *tx)
        .await?;
    // Re-lock backing tasks: an in-flight enqueue may have linked a new task
    // row while we waited on the workflow_task locks above.
    sqlx::query(LOCK_WORKFLOW_BACKING_TASKS_FOR_CANCEL_SQL)
        .bind(workflow_id)
        .execute(&mut *tx)
        .await?;

    let cancelled: Option<IdRow> = sqlx::query_as(CANCEL_WORKFLOW_SQL)
        .bind(workflow_id)
        .fetch_optional(&mut *tx)
        .await?;

    if cancelled.is_none() {
        tx.rollback().await?;
        return Ok(false);
    }

    // Cancel not-yet-started horsies_tasks (PENDING/CLAIMED/RUNNING) for
    // ENQUEUED wf_tasks, so workers cannot pick them up after we commit. The
    // function verifies the workflow's CANCELLED status in-statement, so it
    // must follow the flip above; the #176 lock order (workflow row first,
    // then task rows) is preserved because this transaction already holds
    // both.
    crate::broker::terminalization::terminalize_in_tx(
        &mut tx,
        &crate::core::lifecycle::TerminalizationCommand::CancelNodesOfCancelledWorkflow {
            workflow_ids: vec![workflow_id.to_owned()],
        },
    )
    .await
    .map_err(WorkflowError::Broker)?;
    // Skip PENDING/READY wf_tasks (not ENQUEUED or RUNNING).
    sqlx::query(SKIP_PENDING_READY_TASKS_SQL)
        .bind(workflow_id)
        .execute(&mut *tx)
        .await?;
    // Skip ENQUEUED wf_tasks whose horsies_tasks were just cancelled.
    sqlx::query(SKIP_CANCELLED_ENQUEUED_TASKS_SQL)
        .bind(workflow_id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(true)
}

async fn cancel_workflow_inner(pool: &PgPool, workflow_id: &str) -> Result<bool, WorkflowError> {
    if !cancel_one_workflow_in_tx(pool, workflow_id).await? {
        ensure_workflow_exists(pool, workflow_id).await?;
        return Ok(false);
    }

    // Cascade cancellation to non-terminal descendant workflows so RUNNING
    // children stop executing (parity with horsies PR #66).
    cascade_cancel_to_children(pool, workflow_id).await?;

    tracing::info!(workflow_id, "workflow cancelled");
    Ok(true)
}

/// Pause a workflow.
///
/// Transitions RUNNING -> PAUSED. No new tasks will be enqueued.
/// Already-running tasks will complete but their results won't
/// trigger new enqueues until resume.
///
/// Cascades pause to all running child workflows (iterative BFS),
/// matching Python's `_cascade_pause_to_children()` behavior.
///
/// Returns `true` if the workflow was actually paused.
pub async fn pause_workflow(pool: &PgPool, workflow_id: &str) -> HandleResult<bool> {
    pause_workflow_inner(pool, workflow_id)
        .await
        .map_err(|e| to_handle_error(e, workflow_id))
}

async fn pause_workflow_inner(pool: &PgPool, workflow_id: &str) -> Result<bool, WorkflowError> {
    let paused: Option<IdRow> = sqlx::query_as(PAUSE_WORKFLOW_SQL)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;

    if paused.is_none() {
        ensure_workflow_exists(pool, workflow_id).await?;
        return Ok(false);
    }

    tracing::info!(workflow_id, "workflow paused");

    // Cascade pause to running child workflows (iterative BFS).
    let mut paused_ids = vec![workflow_id.to_owned()];
    paused_ids.extend(cascade_pause_to_children(pool, workflow_id).await?);

    // Claimed-but-not-started backing task rows cannot safely remain claimable
    // after their workflow_task is reset to READY for resume. Abandon the
    // claimed rows through the workflow-scoped operation (which verifies the
    // workflows are PAUSED in-statement and reaches claims other workers
    // hold) and reset their nodes in the same transaction (C15).
    let mut tx = pool.begin().await?;
    let outcomes = crate::broker::terminalization::terminalize_in_tx(
        &mut tx,
        &crate::core::lifecycle::TerminalizationCommand::AbandonNodesOfPausedWorkflows {
            workflow_ids: paused_ids,
        },
    )
    .await
    .map_err(WorkflowError::Broker)?;
    let abandoned: Vec<String> = outcomes
        .iter()
        .map(|outcome| outcome.task_id().to_string())
        .collect();
    if !abandoned.is_empty() {
        sqlx::query(RESET_ABANDONED_NODES_SQL)
            .bind(&abandoned)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;

    Ok(true)
}

/// Resume a paused workflow.
///
/// Transitions PAUSED -> RUNNING. Re-evaluates all PENDING tasks
/// (checking if deps became terminal while paused), enqueues READY
/// tasks (including sub-workflow READY tasks), and cascades resume
/// to all paused child workflows.
///
/// Idempotent: if the workflow is already RUNNING (e.g. a crash committed the
/// parent PAUSED->RUNNING flip but died before cascading resume to children,
/// leaving them stranded PAUSED), the re-evaluation, child cascade, and
/// completion check still run — only the status flip is skipped (C8).
///
/// Returns:
/// - `true` when the workflow was flipped PAUSED -> RUNNING, or when an
///   already-RUNNING call actually recovered something (children resumed or
///   ready nodes re-enqueued).
/// - `Ok(false)` when there was nothing to do: the workflow is already RUNNING
///   and consistent, or it is in a terminal state.
/// - `WorkflowNotFound` when the workflow does not exist.
pub async fn resume_workflow(
    pool: &PgPool,
    workflow_id: &str,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> HandleResult<bool> {
    resume_workflow_inner(pool, workflow_id, registry, payload, retention)
        .await
        .map_err(|e| to_handle_error(e, workflow_id))
}

async fn resume_workflow_inner(
    pool: &PgPool,
    workflow_id: &str,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<bool, WorkflowError> {
    let resumed: Option<IdRow> = sqlx::query_as(RESUME_WORKFLOW_SQL)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;
    let status_flipped = resumed.is_some();

    if !status_flipped {
        // Not PAUSED. Distinguish not-found / terminal / already-RUNNING so a
        // crash between the parent flip and the child cascade can still be
        // recovered on retry (C8) instead of returning early.
        let status: Option<String> = sqlx::query_scalar(GET_WORKFLOW_STATUS_SQL)
            .bind(workflow_id)
            .fetch_optional(pool)
            .await?;
        match status {
            None => {
                return Err(WorkflowError::WorkflowNotFound {
                    workflow_id: workflow_id.to_owned(),
                });
            }
            // A terminal workflow cannot be resumed; nothing to do.
            Some(s) if matches!(s.as_str(), "COMPLETED" | "FAILED" | "CANCELLED") => {
                return Ok(false);
            }
            // Already RUNNING (or otherwise non-terminal): fall through to run the
            // idempotent re-evaluation + cascade + completion below.
            Some(_) => {
                tracing::info!(
                    workflow_id,
                    "resume called on an already-RUNNING workflow; running idempotent recovery",
                );
            }
        }
    } else {
        tracing::info!(workflow_id, "workflow resumed, re-evaluating pending tasks");
    }

    // Re-evaluate and enqueue tasks for this workflow.
    let nodes_enqueued =
        reevaluate_and_enqueue(pool, workflow_id, registry, payload, retention).await?;

    // Cascade resume to paused child workflows (iterative BFS).
    let children_resumed =
        cascade_resume_to_children(pool, workflow_id, registry, payload, retention).await?;

    // Check if all tasks are already terminal (e.g., all pending tasks were
    // skipped during re-evaluation because upstream deps failed). Without
    // this, the workflow would remain stuck in RUNNING forever.
    engine::check_workflow_completion(pool, workflow_id, registry, payload, retention).await?;

    // Recovery completion pass: a child workflow may have COMPLETED while the
    // parent was paused, leaving the parent's sub-workflow node stale. Run the
    // recovery sweep immediately so the parent picks it up now rather than
    // waiting for the periodic reaper (parity with horsies PR #66). Uncapped
    // (max_rows = None): a resumed tree must recover completely in one pass,
    // not be left partial until the next periodic cycle (parity with PR #103).
    // finalizing_grace_ms = 0: resume recovers immediately (the grace only applies
    // to the periodic reaper smoothing the Phase 1→Phase 2 finalize window).
    if let Err(e) = crate::workflow_engine::recovery::recover_stuck_workflows_with_cap(
        pool, registry, None, 0, payload, retention,
    )
    .await
    {
        tracing::warn!(workflow_id, error = %e, "resume recovery pass failed (non-fatal)");
    }

    // A genuine PAUSED->RUNNING flip is always a resume. An already-RUNNING
    // (idempotent) call counts as a resume only if it actually recovered
    // something — children resumed or ready nodes re-enqueued — so a no-op
    // retry returns Ok(false) (C8).
    Ok(status_flipped || children_resumed > 0 || nodes_enqueued > 0)
}

// ---------------------------------------------------------------------------
// Cascade pause to children (Gap 14-4)
// ---------------------------------------------------------------------------

/// Iteratively pause all running child workflows using BFS.
///
/// Avoids deep recursion for deeply nested workflows. Mirrors Python's
/// `_cascade_pause_to_children()`.
///
/// Called by both the explicit pause path and the implicit on_error=pause
/// failure path (`engine::handle_workflow_task_failure`).
pub(crate) async fn cascade_pause_to_children(
    pool: &PgPool,
    workflow_id: &str,
) -> Result<Vec<String>, WorkflowError> {
    let mut queue: VecDeque<String> = VecDeque::new();
    queue.push_back(workflow_id.to_owned());
    let mut paused_child_ids: Vec<String> = Vec::new();

    while let Some(current_id) = queue.pop_front() {
        // Find running child workflows.
        let children: Vec<IdRow> = sqlx::query_as(FIND_RUNNING_CHILDREN_SQL)
            .bind(&current_id)
            .fetch_all(pool)
            .await?;

        for child in children {
            // Pause child workflow.
            sqlx::query(PAUSE_CHILD_WORKFLOW_SQL)
                .bind(&child.id)
                .execute(pool)
                .await?;

            tracing::debug!(
                parent_workflow_id = %current_id,
                child_workflow_id = %child.id,
                "cascade: child workflow paused",
            );

            paused_child_ids.push(child.id.clone());
            // Add to queue to pause its children.
            queue.push_back(child.id);
        }
    }

    Ok(paused_child_ids)
}

// ---------------------------------------------------------------------------
// Cascade cancel to children (parity with horsies PR #66)
// ---------------------------------------------------------------------------

/// Iteratively cancel all non-terminal descendant workflows using BFS.
///
/// Each descendant gets the same lock-before-flip + cleanup treatment as a
/// top-level cancel (`cancel_one_workflow_in_tx`), so a concurrent worker claim
/// cannot slip a child's queued task through during the cascade (parity with
/// horsies PR #66/#67). The `workflow_done` NOTIFY is fired by the status-flip
/// DB trigger, so no manual notify is needed. Without this cascade, cancelling
/// a parent leaves RUNNING child workflows executing.
async fn cascade_cancel_to_children(pool: &PgPool, workflow_id: &str) -> Result<(), WorkflowError> {
    let mut queue: VecDeque<String> = VecDeque::new();
    queue.push_back(workflow_id.to_owned());

    while let Some(current_id) = queue.pop_front() {
        let children: Vec<IdRow> = sqlx::query_as(FIND_CANCELLABLE_CHILDREN_SQL)
            .bind(&current_id)
            .fetch_all(pool)
            .await?;

        for child in children {
            if cancel_one_workflow_in_tx(pool, &child.id).await? {
                tracing::debug!(
                    parent_workflow_id = %current_id,
                    child_workflow_id = %child.id,
                    "cascade: child workflow cancelled",
                );
            }

            // Recurse into this child regardless: its own children may be
            // non-terminal even if it was already terminal.
            queue.push_back(child.id);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Cascade resume to children (Gap 14-4)
// ---------------------------------------------------------------------------

/// Iteratively resume all paused child workflows using BFS.
///
/// For each resumed child, re-evaluates PENDING tasks and enqueues READY
/// tasks (including sub-workflow READY tasks). Mirrors Python's
/// `_cascade_resume_to_children()`.
async fn cascade_resume_to_children(
    pool: &PgPool,
    workflow_id: &str,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<usize, WorkflowError> {
    let mut queue: VecDeque<String> = VecDeque::new();
    queue.push_back(workflow_id.to_owned());
    let mut resumed_count = 0usize;

    while let Some(current_id) = queue.pop_front() {
        // Find paused child workflows.
        let children: Vec<ChildWorkflowRow> = sqlx::query_as(FIND_PAUSED_CHILDREN_SQL)
            .bind(&current_id)
            .fetch_all(pool)
            .await?;

        for child in children {
            // Resume child workflow.
            sqlx::query(RESUME_CHILD_WORKFLOW_SQL)
                .bind(&child.id)
                .execute(pool)
                .await?;
            resumed_count += 1;

            tracing::debug!(
                parent_workflow_id = %current_id,
                child_workflow_id = %child.id,
                "cascade: child workflow resumed",
            );

            // Re-evaluate and enqueue tasks for the resumed child.
            reevaluate_and_enqueue(pool, &child.id, registry, payload, retention).await?;

            // Check if child is already complete after re-evaluation.
            engine::check_workflow_completion(pool, &child.id, registry, payload, retention)
                .await?;

            // Add to queue to resume its children.
            queue.push_back(child.id);
        }
    }

    Ok(resumed_count)
}

// ---------------------------------------------------------------------------
// Re-evaluate and enqueue logic (shared by resume + cascade)
// ---------------------------------------------------------------------------

/// Re-evaluate PENDING tasks and enqueue READY tasks for a single workflow.
///
/// Uses the engine's `process_dependents` for PENDING tasks to get full
/// join-type evaluation (all/any/quorum), matching Python's resume behavior
/// which calls `_try_make_ready_and_enqueue` for each PENDING task.
///
/// For already-READY tasks (which were READY at pause time but not yet
/// enqueued), enqueues directly including sub-workflow READY tasks.
/// Returns the number of READY nodes successfully enqueued (regular tasks and
/// launched sub-workflows). In a consistent RUNNING workflow this is 0; a
/// non-zero count signals recoverable state the resume actually re-enqueued.
async fn reevaluate_and_enqueue(
    pool: &PgPool,
    workflow_id: &str,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<usize, WorkflowError> {
    let mut enqueued = 0usize;
    // 1. Re-evaluate PENDING tasks using full join evaluation.
    //
    // For each PENDING task, find a terminal dependency and trigger
    // process_dependents, which calls try_make_ready_and_enqueue with
    // full join type evaluation (all/any/quorum), condition checks, etc.
    let pending_tasks: Vec<PendingTaskRow> = sqlx::query_as(FIND_PENDING_TASKS_SQL)
        .bind(workflow_id)
        .fetch_all(pool)
        .await?;

    // Collect unique terminal dependency indices to avoid redundant calls.
    let mut triggered_dep_indices = std::collections::HashSet::new();

    for task in &pending_tasks {
        if task.dependencies.is_empty() {
            continue; // Shouldn't happen (roots are enqueued at start).
        }

        // Find terminal deps for this task.
        let terminal_deps: Vec<TerminalDepRow> = sqlx::query_as(FIND_TERMINAL_DEPS_SQL)
            .bind(workflow_id)
            .bind(&task.dependencies)
            .fetch_all(pool)
            .await?;

        for dep in &terminal_deps {
            if triggered_dep_indices.insert(dep.task_index) {
                // Trigger full join evaluation via process_dependents.
                if let Err(e) = engine::process_dependents(
                    pool,
                    workflow_id,
                    dep.task_index,
                    registry,
                    payload,
                    retention,
                )
                .await
                {
                    tracing::error!(
                        workflow_id,
                        dep_index = dep.task_index,
                        error = %e,
                        "failed to re-evaluate dependents on resume",
                    );
                }
            }
        }
    }

    // 2. Enqueue all READY tasks (both regular and sub-workflow).
    // These may be tasks that were READY at pause time, or tasks that
    // became READY via step 1 but weren't enqueued (e.g., sub-workflows).
    let ready_tasks: Vec<ReadyTaskRow> = sqlx::query_as(FIND_READY_TASKS_SQL)
        .bind(workflow_id)
        .fetch_all(pool)
        .await?;

    for task in &ready_tasks {
        if task.is_subworkflow {
            // Sub-workflow READY task: launch child workflow.
            match enqueue_subworkflow_on_resume(pool, workflow_id, task, registry, retention).await
            {
                Ok(()) => enqueued += 1,
                Err(e) => {
                    tracing::error!(
                        workflow_id,
                        task_index = task.task_index,
                        error = %e,
                        "failed to launch sub-workflow on resume",
                    );
                }
            }
        } else {
            // Regular READY task: enqueue into horsies_tasks.
            let task_id = crate::core::history::identity::uuid7::mint_task_id()
                .map(|task_id| task_id.to_string())
                .map_err(|error| {
                    WorkflowError::Validation(format!("task identity mint failed: {error}"))
                })?;
            let max_retries = parse_max_retries(task.task_options.as_deref());

            // Merge args_from if present.
            let merged_kwargs = merge_args_from_for_ready(
                pool,
                workflow_id,
                task.task_kwargs.as_deref(),
                &task.args_from,
                &task.dependencies,
            )
            .await?;

            // Inject workflow_ctx for ctx-capable nodes, matching the runtime
            // promotion path. Without this, a READY `workflow_ctx_from` task
            // (e.g. reset to READY at pause time) would be enqueued on resume
            // with no upstream context and fail or produce a wrong result.
            let merged_kwargs = engine::inject_workflow_ctx_into_kwargs(
                pool,
                workflow_id,
                task.task_index,
                &task.task_name,
                task.workflow_ctx_from.as_deref(),
                merged_kwargs,
            )
            .await?;

            let enqueue_sha = format!("wf-{}", task_id);
            let good_until = parse_good_until_from_options(task.task_options.as_deref());
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

            // INSERT + LINK in a single transaction so a row whose LINK matches
            // 0 rows (workflow no longer RUNNING / task no longer READY) is
            // rolled back rather than left as an orphaned, claimable PENDING
            // row. Mirrors `enqueue_workflow_task` in engine.rs.
            let mut tx = pool.begin().await?;

            sqlx::query(ENQUEUE_TASK_SQL)
                .bind(&task_id)
                .bind(&task.task_name)
                .bind(&task.queue_name)
                .bind(task.priority)
                .bind(&task.task_args)
                .bind(&merged_kwargs)
                .bind(good_until)
                .bind(max_retries)
                .bind(&task.task_options)
                .bind(&enqueue_sha)
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
                .execute(&mut *tx)
                .await?;

            let link_result = sqlx::query(LINK_ENQUEUED_TASK_SQL)
                .bind(&task_id)
                .bind(workflow_id)
                .bind(task.task_index)
                .execute(&mut *tx)
                .await?;

            if link_result.rows_affected() == 0 {
                tx.rollback().await?;
                tracing::debug!(
                    workflow_id,
                    task_index = task.task_index,
                    "resume ready-task link matched 0 rows (workflow not RUNNING or task not READY), rolled back",
                );
                continue;
            }

            tx.commit().await?;
            enqueued += 1;

            tracing::debug!(
                workflow_id,
                task_index = task.task_index,
                task_id = %task_id,
                "ready task enqueued after resume",
            );
        }
    }

    Ok(enqueued)
}

// ---------------------------------------------------------------------------
// Sub-workflow launch on resume (Gap 14-6)
// ---------------------------------------------------------------------------

/// Launch a child workflow for a sub-workflow READY task during resume.
///
/// Resolves the spec from the registry, supports dynamic parameterization
/// via `spec_builder`, starts the child workflow, and links the parent task.
async fn enqueue_subworkflow_on_resume(
    pool: &PgPool,
    workflow_id: &str,
    task: &ReadyTaskRow,
    registry: &WorkflowSpecRegistry,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let spec_name = task
        .task_name
        .strip_prefix("__sub_workflow:")
        .or(task.sub_workflow_name.as_deref())
        .unwrap_or(&task.task_name);

    // Resolve by definition_key first, then fall back to name.
    let registered = registry
        .resolve_child_registration(spec_name, task.sub_definition_key.as_deref())
        .ok_or_else(|| WorkflowError::WorkflowNotFound {
            workflow_id: format!(
                "sub-workflow spec not found (definition_key={:?}, name='{}')",
                task.sub_definition_key, spec_name,
            ),
        })?;

    // Build the child spec (supports dynamic parameterization via spec_builder).
    let has_child_inputs =
        task.task_args.is_some() || task.task_kwargs.is_some() || task.args_from.is_some();

    let merged_kwargs = merge_args_from_for_ready(
        pool,
        workflow_id,
        task.task_kwargs.as_deref(),
        &task.args_from,
        &task.dependencies,
    )
    .await?;
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
    let root_wf_id = depth_row.root_workflow_id.as_deref().unwrap_or(workflow_id);

    // Start child workflow + link parent in a single transaction.
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

    let link_result = sqlx::query(LINK_SUBWORKFLOW_SQL)
        .bind(&child_id)
        .bind(workflow_id)
        .bind(task.task_index)
        .execute(&mut *tx)
        .await?;

    if link_result.rows_affected() == 0 {
        tx.rollback().await?;
        tracing::debug!(
            workflow_id,
            task_index = task.task_index,
            "sub-workflow link failed on resume, rolled back",
        );
        return Ok(());
    }

    tx.commit().await?;

    tracing::debug!(
        workflow_id,
        task_index = task.task_index,
        child_workflow_id = %child_id,
        spec_name,
        "sub-workflow launched on resume",
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

use crate::workflow_engine::args_merge::merge_args_from_async as merge_args_from_for_ready;

#[cfg(test)]
mod resume_idempotency_tests {
    //! C8: a crash after the parent PAUSED->RUNNING flip but before the child
    //! cascade leaves children stranded PAUSED under a RUNNING parent. Retrying
    //! `resume_workflow` must recover them (idempotent), not return early.
    use super::*;
    use crate::broker::PostgresBroker;
    use serial_test::serial;
    use uuid::Uuid;

    fn test_db_url() -> String {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return url;
        }
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let root = std::path::Path::new(manifest_dir)
            .ancestors()
            .find(|p| p.join(".env").exists());
        let pw = root
            .and_then(|r| std::fs::read_to_string(r.join(".env")).ok())
            .and_then(|c| {
                c.lines()
                    .filter_map(|l| l.trim().split_once('='))
                    .find(|(k, _)| k.trim() == "DB_PASSWORD")
                    .map(|(_, v)| v.trim().to_owned())
            })
            .unwrap_or_else(|| "W0rklane".to_owned());
        format!("postgresql://postgres:{pw}@localhost:5432/horsies-rust-port")
    }

    async fn insert_workflow(pool: &PgPool, id: &str, status: &str, parent: Option<&str>) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id, parent_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES (
                $1::uuid, 'c8_wf', $2, 'fail', NULL,
                'test.c8.v1', 0, $3::uuid, $4::uuid,
                NOW(), NOW(), NOW(), NOW()
            )",
        )
        .bind(id)
        .bind(status)
        .bind(parent.unwrap_or(id))
        .bind(parent)
        .execute(pool)
        .await
        .expect("insert workflow");
    }

    // A RUNNING task keeps its workflow non-terminal (untouched by re-evaluation).
    async fn insert_running_task(pool: &PgPool, wf_id: &str) {
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, created_at
            ) VALUES (
                $1, $2, 0, 'node_0', 'c8_task', '[]', '{}',
                'default', 100, '{}', FALSE, 'all',
                'RUNNING', FALSE, NOW()
            )",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(wf_id)
        .execute(pool)
        .await
        .expect("insert task");
    }

    async fn status_of(pool: &PgPool, id: &str) -> String {
        sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
            .bind(id)
            .fetch_one(pool)
            .await
            .expect("status")
    }

    async fn cleanup(pool: &PgPool, ids: &[&str]) {
        for id in ids {
            sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1::uuid")
                .bind(id)
                .execute(pool)
                .await
                .ok();
        }
        // Delete children before parents (FK).
        for id in ids.iter().rev() {
            sqlx::query("DELETE FROM horsies_workflows WHERE id = $1::uuid")
                .bind(id)
                .execute(pool)
                .await
                .ok();
        }
    }

    #[tokio::test]
    #[serial]
    async fn resume_on_running_parent_recovers_stranded_paused_child() {
        let broker = PostgresBroker::connect(&test_db_url())
            .await
            .expect("connect");
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let registry = WorkflowSpecRegistry::new();

        let parent = Uuid::new_v4().to_string();
        let child = Uuid::new_v4().to_string();
        // Post-crash state: parent already RUNNING, child still PAUSED.
        insert_workflow(&pool, &parent, "RUNNING", None).await;
        insert_running_task(&pool, &parent).await;
        insert_workflow(&pool, &child, "PAUSED", Some(&parent)).await;
        insert_running_task(&pool, &child).await;

        let resumed = resume_workflow(
            &pool,
            &parent,
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("resume");
        assert!(
            resumed,
            "idempotent resume must report it recovered the child"
        );
        assert_eq!(
            status_of(&pool, &child).await,
            "RUNNING",
            "stranded PAUSED child must be resumed, not left PAUSED forever",
        );

        cleanup(&pool, &[&parent, &child]).await;
    }

    #[tokio::test]
    #[serial]
    async fn resume_on_consistent_running_workflow_is_noop_false() {
        let broker = PostgresBroker::connect(&test_db_url())
            .await
            .expect("connect");
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let registry = WorkflowSpecRegistry::new();

        let id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &id, "RUNNING", None).await;
        insert_running_task(&pool, &id).await;

        // No paused children, no ready nodes → genuinely nothing to do.
        let resumed = resume_workflow(
            &pool,
            &id,
            &registry,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("resume");
        assert!(
            !resumed,
            "a consistent RUNNING workflow resume must return Ok(false)"
        );

        cleanup(&pool, &[&id]).await;
    }

    #[tokio::test]
    #[serial]
    async fn p6_workflow_enqueue_lifecycle_resume_persists_never_eligible_facts() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let workflow_id = Uuid::new_v4().to_string();
        insert_workflow(&pool, &workflow_id, "RUNNING", None).await;
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, created_at
             ) VALUES (
                $1::uuid, $2::uuid, 0, 'p6_resume', 'p6_resume_task', '[1]', '{}',
                'bulk', 17, '{}', FALSE, 'all', 'READY', FALSE, NOW()
             )",
        )
        .bind(Uuid::new_v4())
        .bind(&workflow_id)
        .execute(&pool)
        .await
        .expect("insert ready resume node");
        let mut retention = RetentionConfig::default();
        retention
            .queue_retention
            .insert("bulk".to_owned(), Some(chrono::Duration::days(7)));

        let enqueued = reevaluate_and_enqueue(
            &pool,
            &workflow_id,
            &WorkflowSpecRegistry::new(),
            &PayloadPolicy::default(),
            &retention,
        )
        .await
        .expect("resume enqueue path");
        assert_eq!(enqueued, 1);
        let facts: (String, String, i32, i32, bool, Option<i32>) = sqlx::query_as(
            "SELECT t.retention_class_key, t.prepared_rerun_input_disposition,
                    octet_length(t.command_fingerprint), octet_length(t.input_digest),
                    t.retain_rerun_input, octet_length(t.prepared_rerun_input_inline)
             FROM horsies_tasks t
             JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
             WHERE wt.workflow_id = $1::uuid AND wt.task_index = 0",
        )
        .bind(&workflow_id)
        .fetch_one(&pool)
        .await
        .expect("read resume enqueue facts");
        assert_eq!(facts.0, "q_bulk_7d");
        assert_eq!(facts.1, "NEVER_ELIGIBLE");
        assert_eq!(facts.2, 32);
        assert_eq!(facts.3, 32);
        assert!(!facts.4);
        assert!(facts.5.is_none());

        sqlx::query(
            "DELETE FROM horsies_tasks WHERE id IN (
                SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1::uuid
             )",
        )
        .bind(&workflow_id)
        .execute(&pool)
        .await
        .ok();
        cleanup(&pool, &[&workflow_id]).await;
    }
}

#[cfg(test)]
mod cancel_lock_order_tests {
    //! N6: the cancel transaction must lock the workflow row before workflow_task
    //! rows, matching COMPLETE_WORKFLOW_TASK_SQL (workflows before workflow_tasks).
    //! Opposite orders across the two paths deadlock under contention (Postgres
    //! aborts one side, SQLSTATE 40P01). This test drives the real cancel tx
    //! against a held completion-order lock pair and asserts neither side
    //! deadlocks and both commit.
    use super::*;
    use crate::broker::PostgresBroker;
    use serial_test::serial;
    use uuid::Uuid;

    fn test_db_url() -> String {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return url;
        }
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let root = std::path::Path::new(manifest_dir)
            .ancestors()
            .find(|p| p.join(".env").exists());
        let pw = root
            .and_then(|r| std::fs::read_to_string(r.join(".env")).ok())
            .and_then(|c| {
                c.lines()
                    .filter_map(|l| l.trim().split_once('='))
                    .find(|(k, _)| k.trim() == "DB_PASSWORD")
                    .map(|(_, v)| v.trim().to_owned())
            })
            .unwrap_or_else(|| "W0rklane".to_owned());
        format!("postgresql://postgres:{pw}@localhost:5432/horsies-rust-port")
    }

    async fn insert_running_workflow(pool: &PgPool, id: &str) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id, parent_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES (
                $1, 'n6_wf', 'RUNNING', 'fail', NULL,
                'test.n6.v1', 0, $1, NULL,
                NOW(), NOW(), NOW(), NOW()
            )",
        )
        .bind(id)
        .execute(pool)
        .await
        .expect("insert workflow");
    }

    async fn insert_wf_task(pool: &PgPool, wf_id: &str) {
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, created_at
            ) VALUES (
                $1, $2, 0, 'node_0', 'n6_task', '[]', '{}',
                'default', 100, '{}', FALSE, 'all',
                'RUNNING', FALSE, NOW()
            )",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(wf_id)
        .execute(pool)
        .await
        .expect("insert workflow_task");
    }

    async fn cleanup(pool: &PgPool, id: &str) {
        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await
            .ok();
    }

    /// Drive the real cancel transaction while a separate connection holds locks
    /// in completion order (workflow row first, then workflow_task row). With the
    /// fixed cancel path (workflow row first), cancel waits on the workflow row
    /// holding no workflow_task lock, so there is no circular wait — both sides
    /// commit. Before the fix cancel held the workflow_task row while waiting on
    /// the workflow row, closing a cycle against the completion-order holder and
    /// deadlocking (Postgres aborts one side with 40P01).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn cancel_matches_completion_lock_order_no_deadlock() {
        let broker = PostgresBroker::connect(&test_db_url())
            .await
            .expect("connect");
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();

        let wf = Uuid::new_v4().to_string();
        insert_running_workflow(&pool, &wf).await;
        insert_wf_task(&pool, &wf).await;

        // Conn A simulates an in-flight completion holding its first lock: the
        // workflow row (COMPLETE_WORKFLOW_TASK_SQL's `FOR UPDATE OF w`).
        let mut conn_a = pool.begin().await.expect("begin conn A");
        sqlx::query("SELECT id FROM horsies_workflows WHERE id = $1 FOR UPDATE")
            .bind(&wf)
            .execute(&mut *conn_a)
            .await
            .expect("conn A locks the workflow row");

        // Run the real cancel transaction concurrently.
        let cancel_pool = pool.clone();
        let cancel_wf = wf.clone();
        let cancel_task =
            tokio::spawn(async move { cancel_one_workflow_in_tx(&cancel_pool, &cancel_wf).await });

        // Barrier: let the cancel task reach its first lock-wait. Its lock
        // acquisition is sub-millisecond; this only has to exceed that so the
        // interleaving is deterministic (pre-fix, cancel is now parked holding the
        // workflow_task row and waiting on the workflow row).
        tokio::time::sleep(std::time::Duration::from_millis(750)).await;

        // Conn A now takes its second lock — the workflow_task row (completion's
        // `upd`). Pre-fix this closes the deadlock cycle; post-fix it succeeds
        // immediately because cancel holds no workflow_task lock while it waits.
        let a_wf_task_lock = sqlx::query(
            "SELECT task_index FROM horsies_workflow_tasks
             WHERE workflow_id = $1 AND task_index = 0 FOR UPDATE",
        )
        .bind(&wf)
        .execute(&mut *conn_a)
        .await;

        // Release conn A so the (fixed) cancel path can acquire the workflow row.
        conn_a.commit().await.ok();

        let cancel_result = cancel_task.await.expect("cancel task joins");

        assert!(
            a_wf_task_lock.is_ok(),
            "completion-order lock pair must not deadlock against cancel (40P01): {:?}",
            a_wf_task_lock.err(),
        );
        let cancelled =
            cancel_result.expect("cancel tx must not deadlock against completion order (40P01)");
        assert!(cancelled, "a RUNNING workflow must be cancelled");
        assert_eq!(
            status_of(&pool, &wf).await,
            "CANCELLED",
            "cancel must commit the CANCELLED status",
        );

        cleanup(&pool, &wf).await;
    }

    async fn status_of(pool: &PgPool, id: &str) -> String {
        sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
            .bind(id)
            .fetch_one(pool)
            .await
            .expect("status")
    }
}

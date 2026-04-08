use std::collections::VecDeque;

use sqlx::PgPool;
use uuid::Uuid;

use crate::core::task::retry_utils::parse_max_retries;
use crate::core::workflow::handle_types::{HandleErrorCode, HandleOperationError, HandleResult};
use crate::core::WorkflowSpecRegistry;

use crate::workflow_engine::engine;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::parse_good_until_from_options;
use crate::workflow_engine::start::start_child_workflow_in_tx;

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

const CANCEL_WORKFLOW_SQL: &str = "\
UPDATE horsies_workflows
SET status = 'CANCELLED', completed_at = NOW(), updated_at = NOW()
WHERE id = $1 AND status IN ('PENDING', 'RUNNING', 'PAUSED')
RETURNING id";

/// Step 1: Sync ENQUEUED workflow_tasks whose horsies_tasks are already RUNNING.
/// Prevents skipping tasks that are actively executing.
const SYNC_RUNNING_ENQUEUED_ON_CANCEL_SQL: &str = "\
UPDATE horsies_workflow_tasks wt
SET status = 'RUNNING',
    started_at = COALESCE(wt.started_at, NOW())
FROM horsies_tasks t
WHERE wt.workflow_id = $1
  AND wt.task_id = t.id
  AND wt.status = 'ENQUEUED'
  AND t.status = 'RUNNING'";

/// Step 2: Cancel PENDING/CLAIMED horsies_tasks for ENQUEUED workflow_tasks.
const CANCEL_ENQUEUED_HORSIES_TASKS_SQL: &str = "\
UPDATE horsies_tasks t
SET status = 'CANCELLED',
    claimed = FALSE,
    claimed_at = NULL,
    claimed_by_worker_id = NULL,
    claim_expires_at = NULL,
    updated_at = NOW()
FROM horsies_workflow_tasks wt
WHERE wt.workflow_id = $1
  AND wt.task_id = t.id
  AND wt.status = 'ENQUEUED'
  AND t.status IN ('PENDING', 'CLAIMED')";

/// Step 3: Skip PENDING/READY workflow_tasks (not ENQUEUED or RUNNING).
const SKIP_PENDING_READY_TASKS_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = 'SKIPPED'
WHERE workflow_id = $1 AND status IN ('PENDING', 'READY')";

/// Step 4: Skip ENQUEUED workflow_tasks whose horsies_tasks were just cancelled.
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

const FIND_PENDING_TASKS_SQL: &str = "\
SELECT task_index, dependencies, task_name, task_args, task_kwargs,
       queue_name, priority, args_from, workflow_ctx_from,
       allow_failed_deps, join_type, min_success,
       node_id, task_options
FROM horsies_workflow_tasks
WHERE workflow_id = $1 AND status = 'PENDING'";

const FIND_READY_TASKS_SQL: &str = "\
SELECT task_index, task_name, task_args, task_kwargs,
       queue_name, priority, task_options, node_id,
       args_from, dependencies, is_subworkflow, sub_workflow_name, sub_definition_key
FROM horsies_workflow_tasks
WHERE workflow_id = $1 AND status = 'READY'";

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
    enqueue_sha, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', NOW(), NOW(), $7, $8, $9, $10, NOW(), NOW())";

const LINK_ENQUEUED_TASK_SQL: &str = "\
UPDATE horsies_workflow_tasks wt
SET task_id = $1, status = 'ENQUEUED', started_at = NOW()
FROM horsies_workflows w
WHERE wt.workflow_id = $2 AND wt.task_index = $3
  AND wt.status = 'READY'
  AND w.id = wt.workflow_id
  AND w.status = 'RUNNING'";

/// Find RUNNING child workflows of a given parent.
const FIND_RUNNING_CHILDREN_SQL: &str = "\
SELECT id FROM horsies_workflows
WHERE parent_workflow_id = $1 AND status = 'RUNNING'";

/// Pause a single child workflow (RUNNING -> PAUSED).
const PAUSE_CHILD_WORKFLOW_SQL: &str = "\
UPDATE horsies_workflows
SET status = 'PAUSED', updated_at = NOW()
WHERE id = $1 AND status = 'RUNNING'";

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

async fn cancel_workflow_inner(pool: &PgPool, workflow_id: &str) -> Result<bool, WorkflowError> {
    let cancelled: Option<IdRow> = sqlx::query_as(CANCEL_WORKFLOW_SQL)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;

    if cancelled.is_none() {
        ensure_workflow_exists(pool, workflow_id).await?;
        return Ok(false);
    }

    // Step 1: Sync ENQUEUED wf_tasks whose horsies_tasks are already RUNNING.
    // This prevents skipping actively-executing tasks in step 3.
    sqlx::query(SYNC_RUNNING_ENQUEUED_ON_CANCEL_SQL)
        .bind(workflow_id)
        .execute(pool)
        .await?;

    // Step 2: Cancel PENDING/CLAIMED horsies_tasks for ENQUEUED wf_tasks.
    sqlx::query(CANCEL_ENQUEUED_HORSIES_TASKS_SQL)
        .bind(workflow_id)
        .execute(pool)
        .await?;

    // Step 3: Skip PENDING/READY wf_tasks (not ENQUEUED or RUNNING).
    sqlx::query(SKIP_PENDING_READY_TASKS_SQL)
        .bind(workflow_id)
        .execute(pool)
        .await?;

    // Step 4: Skip ENQUEUED wf_tasks whose horsies_tasks were just cancelled.
    sqlx::query(SKIP_CANCELLED_ENQUEUED_TASKS_SQL)
        .bind(workflow_id)
        .execute(pool)
        .await?;

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
    cascade_pause_to_children(pool, workflow_id).await?;

    Ok(true)
}

/// Resume a paused workflow.
///
/// Transitions PAUSED -> RUNNING. Re-evaluates all PENDING tasks
/// (checking if deps became terminal while paused), enqueues READY
/// tasks (including sub-workflow READY tasks), and cascades resume
/// to all paused child workflows.
///
/// Returns `true` if the workflow was actually resumed.
pub async fn resume_workflow(
    pool: &PgPool,
    workflow_id: &str,
    registry: &WorkflowSpecRegistry,
) -> HandleResult<bool> {
    resume_workflow_inner(pool, workflow_id, registry)
        .await
        .map_err(|e| to_handle_error(e, workflow_id))
}

async fn resume_workflow_inner(
    pool: &PgPool,
    workflow_id: &str,
    registry: &WorkflowSpecRegistry,
) -> Result<bool, WorkflowError> {
    let resumed: Option<IdRow> = sqlx::query_as(RESUME_WORKFLOW_SQL)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;

    if resumed.is_none() {
        ensure_workflow_exists(pool, workflow_id).await?;
        return Ok(false);
    }

    tracing::info!(workflow_id, "workflow resumed, re-evaluating pending tasks");

    // Re-evaluate and enqueue tasks for this workflow.
    reevaluate_and_enqueue(pool, workflow_id, registry).await?;

    // Cascade resume to paused child workflows (iterative BFS).
    cascade_resume_to_children(pool, workflow_id, registry).await?;

    // Check if all tasks are already terminal (e.g., all pending tasks were
    // skipped during re-evaluation because upstream deps failed). Without
    // this, the workflow would remain stuck in RUNNING forever.
    engine::check_workflow_completion(pool, workflow_id, registry).await?;

    Ok(true)
}

// ---------------------------------------------------------------------------
// Cascade pause to children (Gap 14-4)
// ---------------------------------------------------------------------------

/// Iteratively pause all running child workflows using BFS.
///
/// Avoids deep recursion for deeply nested workflows. Mirrors Python's
/// `_cascade_pause_to_children()`.
async fn cascade_pause_to_children(pool: &PgPool, workflow_id: &str) -> Result<(), WorkflowError> {
    let mut queue: VecDeque<String> = VecDeque::new();
    queue.push_back(workflow_id.to_owned());

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

            // Add to queue to pause its children.
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
) -> Result<(), WorkflowError> {
    let mut queue: VecDeque<String> = VecDeque::new();
    queue.push_back(workflow_id.to_owned());

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

            tracing::debug!(
                parent_workflow_id = %current_id,
                child_workflow_id = %child.id,
                "cascade: child workflow resumed",
            );

            // Re-evaluate and enqueue tasks for the resumed child.
            reevaluate_and_enqueue(pool, &child.id, registry).await?;

            // Check if child is already complete after re-evaluation.
            engine::check_workflow_completion(pool, &child.id, registry).await?;

            // Add to queue to resume its children.
            queue.push_back(child.id);
        }
    }

    Ok(())
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
async fn reevaluate_and_enqueue(
    pool: &PgPool,
    workflow_id: &str,
    registry: &WorkflowSpecRegistry,
) -> Result<(), WorkflowError> {
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
                if let Err(e) =
                    engine::process_dependents(pool, workflow_id, dep.task_index, registry).await
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
            if let Err(e) = enqueue_subworkflow_on_resume(pool, workflow_id, task, registry).await {
                tracing::error!(
                    workflow_id,
                    task_index = task.task_index,
                    error = %e,
                    "failed to launch sub-workflow on resume",
                );
            }
        } else {
            // Regular READY task: enqueue into horsies_tasks.
            let task_id = Uuid::new_v4().to_string();
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

            let enqueue_sha = format!("wf-{}", task_id);

            sqlx::query(ENQUEUE_TASK_SQL)
                .bind(&task_id)
                .bind(&task.task_name)
                .bind(&task.queue_name)
                .bind(task.priority)
                .bind(&task.task_args)
                .bind(&merged_kwargs)
                .bind(parse_good_until_from_options(task.task_options.as_deref()))
                .bind(max_retries)
                .bind(&task.task_options)
                .bind(&enqueue_sha)
                .execute(pool)
                .await?;

            sqlx::query(LINK_ENQUEUED_TASK_SQL)
                .bind(&task_id)
                .bind(workflow_id)
                .bind(task.task_index)
                .execute(pool)
                .await?;

            tracing::debug!(
                workflow_id,
                task_index = task.task_index,
                task_id = %task_id,
                "ready task enqueued after resume",
            );
        }
    }

    Ok(())
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
) -> Result<(), WorkflowError> {
    let spec_name = task
        .task_name
        .strip_prefix("__sub_workflow:")
        .or(task.sub_workflow_name.as_deref())
        .unwrap_or(&task.task_name);

    // Resolve by definition_key first, then fall back to name.
    let registered = task
        .sub_definition_key
        .as_deref()
        .and_then(|key| registry.get_by_definition_key(key))
        .or_else(|| registry.get(spec_name))
        .ok_or_else(|| WorkflowError::WorkflowNotFound {
            workflow_id: format!(
                "sub-workflow spec not found (definition_key={:?}, name='{}')",
                task.sub_definition_key, spec_name,
            ),
        })?;

    // Build the child spec (supports dynamic parameterization via spec_builder).
    let child_spec = if let Some(ref builder) = registered.spec_builder {
        let args_json = task.task_args.as_deref();
        let kwargs_json = task.task_kwargs.as_deref();
        let mut spec =
            builder(args_json, kwargs_json).map_err(|e| WorkflowError::WorkflowNotFound {
                workflow_id: format!("spec_builder for '{}' failed: {}", spec_name, e,),
            })?;
        // Dynamic specs bypass register_workflow(), so resolve task options
        // and queue/priority here.
        registry.resolve_spec_task_options(&mut spec);
        registry
            .resolve_and_validate_spec(&mut spec)
            .map_err(|e| WorkflowError::Validation(e.to_string()))?;
        spec
    } else {
        registered.spec.clone()
    };

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

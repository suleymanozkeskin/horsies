use std::collections::HashMap;

use sqlx::PgPool;
use uuid::Uuid;

use crate::core::task::retry_utils::parse_max_retries;
use crate::core::workflow::context::WORKFLOW_CTX_KWARG;
use crate::core::{
    JoinType, OperationalErrorCode, OutcomeCode, RetrievalCode, SubWorkflowSummary, SuccessPolicy,
    TaskError, TaskResult, WorkflowSpecRegistry, WorkflowTaskStatus,
};

use crate::workflow_engine::start::{materialize_child_spec, start_child_workflow_in_tx};

use crate::workflow_engine::error::WorkflowError;

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

const FIND_WORKFLOW_TASK_BY_TASK_ID_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index, w.on_error, w.status as workflow_status
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.task_id = $1";

const UPDATE_WORKFLOW_TASK_COMPLETED_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = 'COMPLETED', result = $1, completed_at = NOW()
WHERE workflow_id = $2 AND task_index = $3
  AND status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
RETURNING id";

const UPDATE_WORKFLOW_TASK_FAILED_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = 'FAILED', result = $1, error = $2, completed_at = NOW()
WHERE workflow_id = $3 AND task_index = $4
  AND status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
RETURNING id";

const FIND_DEPENDENTS_SQL: &str = "\
SELECT task_index, dependencies, args_from, workflow_ctx_from,
       allow_failed_deps, join_type, min_success, task_name,
       task_args, task_kwargs, queue_name, priority,
       node_id, task_options, status,
       is_subworkflow, sub_workflow_name, sub_definition_key
FROM horsies_workflow_tasks
WHERE workflow_id = $1
  AND $2 = ANY(dependencies)
  AND status = 'PENDING'";

const DEP_STATUS_COUNTS_SQL: &str = "\
SELECT status, COUNT(*)::int as cnt
FROM horsies_workflow_tasks
WHERE workflow_id = $1 AND task_index = ANY($2)
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

const TERMINAL_OUTPUT_RESULTS_SQL: &str = "\
SELECT wt.node_id, wt.task_index, wt.result
FROM horsies_workflow_tasks wt
WHERE wt.workflow_id = $1
  AND NOT EXISTS (
      SELECT 1 FROM horsies_workflow_tasks other
      WHERE other.workflow_id = wt.workflow_id
        AND wt.task_index = ANY(other.dependencies)
  )";

const UPDATE_WORKFLOW_COMPLETED_SQL: &str = "\
UPDATE horsies_workflows
SET status = 'COMPLETED', result = $2, completed_at = NOW(), updated_at = NOW()
WHERE id = $1 AND completed_at IS NULL
RETURNING id";

const UPDATE_WORKFLOW_FAILED_SQL: &str = "\
UPDATE horsies_workflows
SET status = 'FAILED', result = $2, error = COALESCE($3, error), completed_at = NOW(), updated_at = NOW()
WHERE id = $1 AND completed_at IS NULL
RETURNING id";

const GET_PARENT_WORKFLOW_SQL: &str = "\
SELECT parent_workflow_id, parent_task_index
FROM horsies_workflows
WHERE id = $1";

const GET_WORKFLOW_DEPTH_SQL: &str = "\
SELECT depth, root_workflow_id
FROM horsies_workflows
WHERE id = $1";

const UPDATE_SUBWORKFLOW_LINK_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET sub_workflow_id = $1, status = 'RUNNING', started_at = NOW()
WHERE workflow_id = $2 AND task_index = $3
  AND status = 'READY'";

const UPDATE_SUBWORKFLOW_COMPLETED_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = 'COMPLETED', result = $1, sub_workflow_summary = $2, completed_at = NOW()
WHERE workflow_id = $3 AND task_index = $4
  AND status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
RETURNING id";

const UPDATE_SUBWORKFLOW_FAILED_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET status = 'FAILED', result = $1, error = $2, sub_workflow_summary = $3, completed_at = NOW()
WHERE workflow_id = $4 AND task_index = $5
  AND status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
RETURNING id";

const COUNT_CHILD_TASK_STATUSES_SQL: &str = "\
SELECT status, COUNT(*)::int as cnt
FROM horsies_workflow_tasks
WHERE workflow_id = $1
GROUP BY status";

/// Store error on workflow row immediately (on_error=fail).
/// Uses COALESCE to preserve any earlier error if already set.
const STORE_WORKFLOW_ERROR_EARLY_SQL: &str = "\
UPDATE horsies_workflows
SET error = COALESCE(error, $2), updated_at = NOW()
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
struct WorkflowTaskLookup {
    workflow_id: String,
    task_index: i32,
    on_error: String,
    workflow_status: String,
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
    id: String,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct ParentWorkflowRow {
    parent_workflow_id: Option<String>,
    parent_task_index: Option<i32>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct DepthRow {
    depth: Option<i32>,
    root_workflow_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Called by the worker after a workflow task finishes (completed or failed).
///
/// Finds the workflow_task, updates its status/result, processes dependents,
/// and checks if the workflow is complete.
pub async fn on_workflow_task_complete(
    pool: &PgPool,
    task_id: &str,
    result_json: &str,
    is_success: bool,
    registry: &WorkflowSpecRegistry,
) -> Result<(), WorkflowError> {
    // Find the workflow_task for this horsies_tasks row.
    let lookup: Option<WorkflowTaskLookup> = sqlx::query_as(FIND_WORKFLOW_TASK_BY_TASK_ID_SQL)
        .bind(task_id)
        .fetch_optional(pool)
        .await?;

    let Some(lookup) = lookup else {
        tracing::debug!(task_id, "task is not part of a workflow");
        return Ok(());
    };

    let workflow_id = &lookup.workflow_id;
    let task_index = lookup.task_index;

    // If workflow is already terminal, skip.
    if is_terminal_workflow_status(&lookup.workflow_status) {
        tracing::debug!(
            workflow_id,
            task_index,
            status = %lookup.workflow_status,
            "workflow already terminal, ignoring task completion",
        );
        return Ok(());
    }

    if is_success {
        // Mark workflow_task as COMPLETED.
        let updated: Option<IdRow> = sqlx::query_as(UPDATE_WORKFLOW_TASK_COMPLETED_SQL)
            .bind(result_json)
            .bind(workflow_id)
            .bind(task_index)
            .fetch_optional(pool)
            .await?;

        if updated.is_none() {
            // Already processed by another worker — skip further processing.
            tracing::debug!(
                workflow_id,
                task_index,
                "workflow task already terminal, skipping",
            );
            return Ok(());
        }

        tracing::debug!(workflow_id, task_index, "workflow task completed");
    } else {
        // Extract TaskError for error column (best-effort).
        let error_json = match serde_json::from_str::<TaskResult<serde_json::Value>>(result_json) {
            Ok(TaskResult::Err(task_error)) => {
                serde_json::to_string(&task_error).unwrap_or_else(|_| "{}".to_owned())
            }
            Ok(TaskResult::Ok(_)) => {
                let err = TaskError::builtin(
                    OperationalErrorCode::TaskError,
                    "workflow task marked failed but result was ok",
                );
                serde_json::to_string(&err).unwrap_or_else(|_| "{}".to_owned())
            }
            Err(e) => {
                let err = TaskError::builtin(
                    OperationalErrorCode::ResultDeserializationError,
                    format!("failed to parse task result: {}", e),
                );
                serde_json::to_string(&err).unwrap_or_else(|_| "{}".to_owned())
            }
        };

        // Mark workflow_task as FAILED.
        let updated: Option<IdRow> = sqlx::query_as(UPDATE_WORKFLOW_TASK_FAILED_SQL)
            .bind(result_json) // $1 = result (TaskResult JSON)
            .bind(&error_json) // $2 = error
            .bind(workflow_id) // $3 = workflow_id
            .bind(task_index) // $4 = task_index
            .fetch_optional(pool)
            .await?;

        if updated.is_none() {
            tracing::debug!(
                workflow_id,
                task_index,
                "workflow task already terminal, skipping",
            );
            return Ok(());
        }

        tracing::debug!(workflow_id, task_index, "workflow task failed");

        // Handle failure policy (pass error_json for immediate storage).
        let should_continue = handle_workflow_task_failure(
            pool,
            workflow_id,
            task_index,
            &lookup.on_error,
            &error_json,
        )
        .await?;

        if !should_continue {
            // OnError::Pause — workflow paused, no further processing.
            return Ok(());
        }
    }

    // PAUSED guard: re-check workflow status before processing dependents.
    // Another concurrent task failure may have paused the workflow (on_error=pause).
    // If so, do not propagate — pending tasks stay pending for resume.
    let status_row: Option<WorkflowStatusRow> = sqlx::query_as(GET_WORKFLOW_STATUS_SQL)
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;
    if let Some(row) = &status_row {
        if row.status == "PAUSED" {
            tracing::debug!(
                workflow_id,
                task_index,
                "workflow is PAUSED, skipping dependent processing",
            );
            return Ok(());
        }
    }

    // Process downstream dependents.
    process_dependents(pool, workflow_id, task_index, registry).await?;

    // Check if all tasks are terminal.
    check_workflow_completion(pool, workflow_id, registry).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Dependency resolution
// ---------------------------------------------------------------------------

/// Find all PENDING tasks that depend on the completed task index
/// and attempt to make them ready.
///
/// Uses `Box::pin` because of the recursive call chain:
/// process_dependents -> try_make_ready_and_enqueue -> skip_task -> process_dependents
pub(crate) fn process_dependents<'a>(
    pool: &'a PgPool,
    workflow_id: &'a str,
    completed_index: i32,
    registry: &'a WorkflowSpecRegistry,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), WorkflowError>> + Send + 'a>> {
    Box::pin(async move {
        let dependents: Vec<DependentRow> = sqlx::query_as(FIND_DEPENDENTS_SQL)
            .bind(workflow_id)
            .bind(completed_index)
            .fetch_all(pool)
            .await?;

        for dep in dependents {
            if let Err(e) = try_make_ready_and_enqueue(pool, workflow_id, &dep, registry).await {
                tracing::error!(
                    workflow_id,
                    task_index = dep.task_index,
                    error = %e,
                    "failed to process dependent task",
                );
            }
        }

        Ok(())
    })
}

/// Core resolver: evaluate whether a dependent task can transition to READY.
///
/// Counts dependency statuses, evaluates join condition, and either
/// marks the task READY + enqueues it, or marks it SKIPPED if the
/// join condition can never be satisfied.
async fn try_make_ready_and_enqueue(
    pool: &PgPool,
    workflow_id: &str,
    task: &DependentRow,
    registry: &WorkflowSpecRegistry,
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
                skip_task(pool, workflow_id, task.task_index, registry).await?;
                return Ok(());
            }
            true
        }
        JoinType::Any => {
            if completed > 0 {
                true // At least one succeeded.
            } else if terminal == dep_count {
                // All terminal but none completed — impossible to satisfy.
                skip_task(pool, workflow_id, task.task_index, registry).await?;
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
                    skip_task(pool, workflow_id, task.task_index, registry).await?;
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
        enqueue_subworkflow_task(pool, workflow_id, task, registry, &dep_results).await?;
    } else {
        // Regular task: enqueue into horsies_tasks.
        enqueue_workflow_task(pool, workflow_id, task, &dep_results).await?;
    }

    Ok(())
}

/// Mark a task as SKIPPED and cascade to its dependents.
async fn skip_task(
    pool: &PgPool,
    workflow_id: &str,
    task_index: i32,
    registry: &WorkflowSpecRegistry,
) -> Result<(), WorkflowError> {
    sqlx::query(UPDATE_WORKFLOW_TASK_SKIPPED_SQL)
        .bind(workflow_id)
        .bind(task_index)
        .execute(pool)
        .await?;

    tracing::debug!(workflow_id, task_index, "workflow task skipped");

    // Cascade: process dependents of this skipped task.
    process_dependents(pool, workflow_id, task_index, registry).await?;

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

/// Create a horsies_tasks row for a workflow task and link it.
///
/// Merges `args_from` results into the task's kwargs/args.
async fn enqueue_workflow_task(
    pool: &PgPool,
    workflow_id: &str,
    task: &DependentRow,
    dep_results: &HashMap<i32, DepResultValue>,
) -> Result<(), WorkflowError> {
    let task_id = Uuid::new_v4().to_string();

    // Merge args_from into kwargs.
    let mut merged_kwargs =
        merge_args_from(task.task_kwargs.as_deref(), &task.args_from, dep_results)?;

    // Inject workflow_ctx if configured.
    if let Some(ctx_from_ids) = task.workflow_ctx_from.as_ref() {
        if !ctx_from_ids.is_empty() {
            let ctx_data = build_workflow_context_data(
                pool,
                workflow_id,
                task.task_index,
                &task.task_name,
                ctx_from_ids,
            )
            .await?;
            let ctx_payload = ctx_data.to_payload()?;

            let mut kwargs_map: serde_json::Map<String, serde_json::Value> =
                match merged_kwargs.as_deref() {
                    Some(json) => serde_json::from_str(json)?,
                    None => serde_json::Map::new(),
                };
            kwargs_map.insert(WORKFLOW_CTX_KWARG.to_owned(), ctx_payload);
            merged_kwargs = Some(serde_json::to_string(&kwargs_map)?);
        }
    }

    let max_retries = parse_max_retries(task.task_options.as_deref());

    let enqueue_sha = format!("wf-{}", task_id);

    // INSERT + LINK in a single transaction to prevent orphaned horsies_task rows
    // if the workflow is paused/cancelled between INSERT and LINK.
    // Matches Python's atomic ENQUEUE_WORKFLOW_TASK_SQL pattern.
    let mut tx = pool.begin().await?;

    let good_until =
        crate::workflow_engine::parse_good_until_from_options(task.task_options.as_deref());

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
        .execute(&mut *tx)
        .await?;

    // Link workflow_task to the created horsies_tasks row.
    // Guards on wt.status = 'READY' AND w.status = 'RUNNING'.
    // If the workflow was paused/cancelled, this updates 0 rows and the
    // transaction rolls back (no orphaned horsies_task).
    let link_result = sqlx::query(LINK_ENQUEUED_TASK_SQL)
        .bind(&task_id)
        .bind(workflow_id)
        .bind(task.task_index)
        .execute(&mut *tx)
        .await?;

    if link_result.rows_affected() == 0 {
        // Workflow was paused/cancelled or task already enqueued. Rollback.
        tx.rollback().await?;
        tracing::debug!(
            workflow_id,
            task_index = task.task_index,
            "workflow task link failed (workflow no longer RUNNING or task not READY), rolled back",
        );
        return Ok(());
    }

    tx.commit().await?;

    tracing::debug!(
        workflow_id,
        task_index = task.task_index,
        task_id = %task_id,
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
/// `spec_builder` is registered (supporting runtime parameterization),
/// starts the child workflow, and links the parent workflow_task to it.
async fn enqueue_subworkflow_task(
    pool: &PgPool,
    workflow_id: &str,
    task: &DependentRow,
    registry: &WorkflowSpecRegistry,
    dep_results: &HashMap<i32, DepResultValue>,
) -> Result<(), WorkflowError> {
    // Resolve child workflow spec: try definition_key first, then name-based lookup.
    let spec_name = task
        .task_name
        .strip_prefix("__sub_workflow:")
        .or(task.sub_workflow_name.as_deref())
        .unwrap_or(&task.task_name);

    let registered = registry
        .resolve_child_registration(spec_name, task.sub_definition_key.as_deref())
        .ok_or_else(|| WorkflowError::WorkflowNotFound {
            workflow_id: format!(
                "sub-workflow spec not found (definition_key={:?}, name='{}')",
                task.sub_definition_key, spec_name,
            ),
        })?;

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
    let root_wf_id = depth_row.root_workflow_id.as_deref().unwrap_or(workflow_id);

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
    )
    .await?;

    // Link parent workflow_task to child workflow (inside same tx).
    // Guards on status = 'READY' — if workflow was paused, this is a no-op
    // and the whole transaction rolls back.
    let link_result = sqlx::query(UPDATE_SUBWORKFLOW_LINK_SQL)
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
            "sub-workflow link failed (workflow no longer RUNNING or task not READY), rolled back",
        );
        return Ok(());
    }

    tx.commit().await?;

    tracing::debug!(
        workflow_id,
        task_index = task.task_index,
        child_workflow_id = %child_id,
        spec_name,
        "sub-workflow launched",
    );

    Ok(())
}

/// Called when a child workflow reaches a terminal state.
///
/// Builds a SubWorkflowSummary, updates the parent workflow_task,
/// and triggers dependent processing on the parent.
pub async fn on_subworkflow_complete(
    pool: &PgPool,
    parent_workflow_id: &str,
    parent_task_index: i32,
    child_workflow_id: &str,
    child_status: &str,
    child_result_json: Option<&str>,
    registry: &WorkflowSpecRegistry,
) -> Result<(), WorkflowError> {
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

    let output = match child_result_json {
        Some(json) => match serde_json::from_str::<TaskResult<serde_json::Value>>(json) {
            Ok(TaskResult::Ok(value)) => Some(value),
            _ => None,
        },
        None => None,
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
                        .collect();
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
        child_workflow_id: child_workflow_id.to_owned(),
    };

    let summary_json = serde_json::to_string(&summary)?;
    let result_json = if is_success {
        match child_result_json {
            Some(json) => json.to_owned(),
            None => {
                let wrapped = TaskResult::Ok(serde_json::Value::Null);
                serde_json::to_string(&wrapped)?
            }
        }
    } else {
        let err = TaskError::new(
            "SUBWORKFLOW_FAILED",
            summary
                .error_summary
                .as_deref()
                .unwrap_or("sub-workflow failed"),
        );
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
                parent_workflow_id,
                parent_task_index,
                child_workflow_id,
                "sub-workflow completed successfully",
            );
        }
        row.is_some()
    } else {
        let error_json = serde_json::to_string(&TaskError::new(
            "SUBWORKFLOW_FAILED",
            summary
                .error_summary
                .as_deref()
                .unwrap_or("sub-workflow failed"),
        ))?;

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
                parent_workflow_id,
                parent_task_index,
                child_workflow_id,
                "sub-workflow failed",
            );
        }
        row.is_some()
    };

    if !updated {
        // Parent task already terminal — another worker handled this.
        tracing::debug!(
            parent_workflow_id,
            parent_task_index,
            child_workflow_id,
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

        let error_json = serde_json::to_string(&TaskError::new(
            "SUBWORKFLOW_FAILED",
            summary
                .error_summary
                .as_deref()
                .unwrap_or("sub-workflow failed"),
        ))?;

        let should_continue = handle_workflow_task_failure(
            pool,
            parent_workflow_id,
            parent_task_index,
            &on_error,
            &error_json,
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
                parent_workflow_id,
                parent_task_index,
                "parent workflow is PAUSED, skipping dependent processing",
            );
            return Ok(());
        }
    }

    // Process dependents on the parent workflow.
    process_dependents(pool, parent_workflow_id, parent_task_index, registry).await?;

    // Check if parent workflow is complete.
    check_workflow_completion(pool, parent_workflow_id, registry).await?;

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
    workflow_id: &str,
    task_index: i32,
    on_error: &str,
    error_json: &str,
) -> Result<bool, WorkflowError> {
    match on_error {
        "fail" => {
            // Store error on workflow row immediately, but keep RUNNING.
            // Uses COALESCE to preserve the first error if multiple tasks fail.
            sqlx::query(STORE_WORKFLOW_ERROR_EARLY_SQL)
                .bind(workflow_id)
                .bind(error_json)
                .execute(pool)
                .await?;

            tracing::debug!(
                workflow_id,
                task_index,
                "on_error=fail, stored error, continuing DAG resolution",
            );
            Ok(true)
        }
        "pause" => {
            // Pause the workflow immediately, storing the triggering error.
            let paused: Option<IdRow> = sqlx::query_as(PAUSE_WORKFLOW_WITH_ERROR_SQL)
                .bind(workflow_id)
                .bind(error_json)
                .fetch_optional(pool)
                .await?;

            if paused.is_none() {
                // Another worker already paused/finalized this workflow; the
                // RUNNING -> PAUSED transition was a no-op, so there is nothing
                // to cascade. Stop processing dependents.
                return Ok(false);
            }

            tracing::info!(
                workflow_id,
                task_index,
                "on_error=pause, workflow paused with error stored",
            );

            // Cascade the implicit pause to running child workflows, matching
            // explicit pause behavior (mirrors Python PR #28).
            crate::workflow_engine::lifecycle::cascade_pause_to_children(pool, workflow_id).await?;

            Ok(false)
        }
        _ => {
            tracing::warn!(
                workflow_id,
                on_error,
                "unknown on_error policy, treating as fail",
            );
            // Store error even for unknown policy (same as fail).
            sqlx::query(STORE_WORKFLOW_ERROR_EARLY_SQL)
                .bind(workflow_id)
                .bind(error_json)
                .execute(pool)
                .await?;
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
    workflow_id: &'a str,
    registry: &'a WorkflowSpecRegistry,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), WorkflowError>> + Send + 'a>> {
    check_workflow_completion_inner(pool, workflow_id, registry)
}

#[allow(clippy::explicit_auto_deref)]
fn check_workflow_completion_inner<'a>(
    pool: &'a PgPool,
    workflow_id: &'a str,
    registry: &'a WorkflowSpecRegistry,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), WorkflowError>> + Send + 'a>> {
    Box::pin(async move {
        // Run the entire completion check inside a transaction with FOR UPDATE
        // to prevent concurrent workers from reading inconsistent state.
        // Matches Python's LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL pattern.
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

        // PAUSED guard: don't finalize if workflow is paused (waiting for manual
        // intervention). Matches Python's check in _check_workflow_completion.
        let status_row: Option<WorkflowStatusRow> = sqlx::query_as(GET_WORKFLOW_STATUS_SQL)
            .bind(workflow_id)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some(ref row) = status_row {
            if row.status == "PAUSED" {
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

        let is_success = evaluate_workflow_success(&meta.success_policy, has_failure, &statuses);

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
                tracing::info!(workflow_id, "workflow completed successfully");
            }
            row.is_some()
        } else {
            // Compute error for the FAILED workflow deterministically from the
            // terminal task results (first failed task by index). Recovery uses
            // the same selection as normal completion — no stale error is
            // preserved. Matches Python PR #27.
            let error_json =
                Some(get_workflow_failure_error(&mut *tx, workflow_id, &meta.success_policy).await?);

            let row: Option<IdRow> = sqlx::query_as(UPDATE_WORKFLOW_FAILED_SQL)
                .bind(workflow_id)
                .bind(None::<&str>) // result — not set on failure
                .bind(&error_json)
                .fetch_optional(&mut *tx)
                .await?;

            if row.is_some() {
                tracing::info!(workflow_id, "workflow failed");
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
            tracing::debug!(workflow_id, "workflow already finalized by another worker");
            return Ok(());
        }

        if let (Some(ref parent_wf_id), Some(parent_idx)) =
            (&parent.parent_workflow_id, parent.parent_task_index)
        {
            let status_str = if is_success { "COMPLETED" } else { "FAILED" };
            on_subworkflow_complete(
                pool,
                parent_wf_id,
                parent_idx,
                workflow_id,
                status_str,
                if is_success { Some(&result_json) } else { None },
                registry,
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
) -> bool {
    match success_policy_json {
        None => {
            // Default: no failures means success.
            !has_failure
        }
        Some(policy_json) => {
            // Parse policy and evaluate.
            let policy: SuccessPolicy = match serde_json::from_value(policy_json.clone()) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!(error = %e, "failed to parse success_policy");
                    return !has_failure;
                }
            };

            let status_vec: Vec<WorkflowTaskStatus> = statuses
                .iter()
                .map(|s| parse_workflow_task_status(&s.status))
                .collect();

            policy.evaluate(&status_vec)
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
    workflow_id: &str,
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

    // Collect results for terminal output tasks (no dependents) keyed by node_id.
    let rows: Vec<NodeResult> = sqlx::query_as(TERMINAL_OUTPUT_RESULTS_SQL)
        .bind(workflow_id)
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
    workflow_id: &str,
    success_policy_json: &Option<serde_json::Value>,
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

            // Fallback: generic error.
            Ok(serde_json::to_string(&TaskError::builtin(
                OutcomeCode::WorkflowSuccessCaseNotMet,
                "one or more tasks failed",
            ))?)
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
    workflow_id: &str,
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
    workflow_id: String,
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
    workflow_id: &str,
    task_index: i32,
    task_name: &str,
    ctx_from_ids: &[String],
) -> Result<WorkflowContextData, WorkflowError> {
    if ctx_from_ids.is_empty() {
        return Ok(WorkflowContextData {
            workflow_id: workflow_id.to_owned(),
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
        workflow_id: workflow_id.to_owned(),
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

fn parse_workflow_task_status(s: &str) -> WorkflowTaskStatus {
    match s {
        "PENDING" => WorkflowTaskStatus::Pending,
        "READY" => WorkflowTaskStatus::Ready,
        "ENQUEUED" => WorkflowTaskStatus::Enqueued,
        "RUNNING" => WorkflowTaskStatus::Running,
        "COMPLETED" => WorkflowTaskStatus::Completed,
        "FAILED" => WorkflowTaskStatus::Failed,
        "SKIPPED" => WorkflowTaskStatus::Skipped,
        _ => WorkflowTaskStatus::Pending,
    }
}

fn is_terminal_workflow_status(s: &str) -> bool {
    matches!(s, "COMPLETED" | "FAILED" | "CANCELLED")
}

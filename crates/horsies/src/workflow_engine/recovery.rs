use sqlx::PgPool;
use uuid::Uuid;

use crate::core::task::retry_utils::parse_max_retries;
use crate::core::{TaskResult, WorkflowSpecRegistry};

use crate::workflow_engine::engine;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::parse_good_until_from_options;
use crate::workflow_engine::start::{materialize_child_spec, start_child_workflow_in_tx};

// ---------------------------------------------------------------------------
// Recovery report
// ---------------------------------------------------------------------------

/// Tracks counts of recovered items by case.
#[derive(Debug, Default)]
pub struct RecoveryReport {
    /// Case 0: PENDING wf_tasks with all deps terminal.
    pub case0_pending_reevaluated: u32,
    /// Case 1: READY wf_tasks with no task_id.
    pub case1_ready_enqueued: u32,
    /// Case 1.5: READY sub-workflow wf_tasks with no child started.
    pub case1_5_subworkflow_started: u32,
    /// Case 1.6: Non-terminal sub-workflow wf_tasks with terminal child.
    pub case1_6_subworkflow_completed: u32,
    /// Case 1.7: Non-terminal wf_tasks with terminal linked task.
    pub case1_7_task_completed: u32,
    /// Case 2+3: RUNNING workflows with all terminal wf_tasks.
    pub case2_3_workflow_completed: u32,
    /// Case 4: Orphaned RUNNING workflows with zero workflow_tasks.
    pub case4_orphaned_failed: u32,
    /// Non-fatal errors encountered.
    pub errors: u32,
}

impl RecoveryReport {
    /// Total recoveries performed (excluding errors).
    pub fn total(&self) -> u32 {
        self.case0_pending_reevaluated
            + self.case1_ready_enqueued
            + self.case1_5_subworkflow_started
            + self.case1_6_subworkflow_completed
            + self.case1_7_task_completed
            + self.case2_3_workflow_completed
            + self.case4_orphaned_failed
    }
}

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

/// Case 0: PENDING wf_tasks where ALL dependencies are terminal, workflow RUNNING.
const CASE0_STUCK_PENDING_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index, wt.dependencies
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'PENDING'
  AND w.status = 'RUNNING'
  AND array_length(wt.dependencies, 1) > 0
  AND NOT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks dep
    WHERE dep.workflow_id = wt.workflow_id
      AND dep.task_index = ANY(wt.dependencies)
      AND dep.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
  )";

/// Case 1: READY regular wf_tasks with no linked horsies_task.
const CASE1_READY_NO_TASK_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index, wt.task_name,
       wt.task_args, wt.task_kwargs, wt.queue_name, wt.priority,
       wt.task_options, wt.args_from, wt.dependencies
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'READY'
  AND wt.task_id IS NULL
  AND wt.is_subworkflow = FALSE
  AND w.status = 'RUNNING'";

/// Case 1.5: READY sub-workflow wf_tasks with no child workflow started.
const CASE1_5_READY_SUBWORKFLOW_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index, wt.task_name,
       wt.task_args, wt.task_kwargs, wt.args_from, wt.dependencies,
       wt.sub_workflow_name, wt.sub_definition_key
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'READY'
  AND wt.sub_workflow_id IS NULL
  AND wt.is_subworkflow = TRUE
  AND w.status = 'RUNNING'";

/// Case 1.6: Non-terminal sub-workflow wf_tasks where child workflow is terminal.
const CASE1_6_STALE_SUBWORKFLOW_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index, wt.sub_workflow_id,
       cw.status as child_status, cw.result as child_result
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
JOIN horsies_workflows cw ON cw.id = wt.sub_workflow_id
WHERE wt.is_subworkflow = TRUE
  AND wt.sub_workflow_id IS NOT NULL
  AND wt.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
  AND cw.status IN ('COMPLETED', 'FAILED', 'CANCELLED')
  AND w.status = 'RUNNING'";

/// Case 1.7: Non-terminal wf_tasks where linked horsies_task is terminal.
///
/// Includes CANCELLED in the terminal task status check (matching Python).
/// Filters to non-subworkflow rows to avoid matching subworkflow entries
/// that happen to have a task_id set.
const CASE1_7_STALE_LINKED_TASK_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index, wt.task_id,
       t.status as task_status, t.result as task_result
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
JOIN horsies_tasks t ON t.id = wt.task_id
WHERE wt.task_id IS NOT NULL
  AND wt.is_subworkflow = FALSE
  AND wt.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
  AND t.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
  AND w.status = 'RUNNING'";

/// Case 2+3: RUNNING workflows where all wf_tasks are terminal.
///
/// NOTE: We require at least one workflow_task to exist. Orphaned workflows
/// (RUNNING but with zero workflow_tasks) are skipped to avoid "no rows
/// returned" errors in check_workflow_completion.
const CASE2_3_STUCK_WORKFLOW_SQL: &str = "\
SELECT w.id as workflow_id
FROM horsies_workflows w
WHERE w.status = 'RUNNING'
  AND EXISTS (
    SELECT 1 FROM horsies_workflow_tasks wt
    WHERE wt.workflow_id = w.id
  )
  AND NOT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks wt
    WHERE wt.workflow_id = w.id
      AND wt.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
  )";

/// Case 4: Orphaned RUNNING workflows with zero workflow_tasks.
///
/// These are workflows that were created but whose task DAG was never inserted,
/// likely due to a crash during workflow start. We mark them as FAILED.
const CASE4_ORPHANED_WORKFLOW_SQL: &str = "\
SELECT w.id as workflow_id, w.name
FROM horsies_workflows w
WHERE w.status = 'RUNNING'
  AND NOT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks wt
    WHERE wt.workflow_id = w.id
  )";

/// Mark an orphaned workflow as FAILED.
const FAIL_ORPHANED_WORKFLOW_SQL: &str = "\
UPDATE horsies_workflows
SET status = 'FAILED',
    error = $2,
    completed_at = NOW(),
    updated_at = NOW()
WHERE id = $1
  AND status = 'RUNNING'";

/// Re-enqueue a READY task into horsies_tasks.
const ENQUEUE_TASK_SQL: &str = "\
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, args, kwargs,
    status, sent_at, enqueued_at, good_until, max_retries, task_options,
    enqueue_sha, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', NOW(), NOW(), $7, $8, $9, $10, NOW(), NOW())";

/// Link workflow_task to a newly created horsies_task.
const LINK_ENQUEUED_TASK_SQL: &str = "\
UPDATE horsies_workflow_tasks wt
SET task_id = $1, status = 'ENQUEUED', started_at = NOW()
FROM horsies_workflows w
WHERE wt.workflow_id = $2 AND wt.task_index = $3
  AND wt.status = 'READY'
  AND w.id = wt.workflow_id
  AND w.status = 'RUNNING'";

/// Link workflow_task to a newly started child workflow.
const LINK_SUBWORKFLOW_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET sub_workflow_id = $1, status = 'ENQUEUED', started_at = NOW()
WHERE workflow_id = $2 AND task_index = $3
  AND status = 'READY'";

/// Get parent depth and root workflow ID.
const GET_DEPTH_AND_ROOT_SQL: &str = "\
SELECT depth, root_workflow_id
FROM horsies_workflows
WHERE id = $1";

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

#[derive(Debug, sqlx::FromRow)]
struct StuckPendingRow {
    workflow_id: String,
    task_index: i32,
    dependencies: Vec<i32>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct ReadyTaskRow {
    workflow_id: String,
    task_index: i32,
    task_name: String,
    task_args: Option<String>,
    task_kwargs: Option<String>,
    queue_name: String,
    priority: i32,
    task_options: Option<String>,
    args_from: Option<serde_json::Value>,
    dependencies: Vec<i32>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct ReadySubworkflowRow {
    workflow_id: String,
    task_index: i32,
    task_name: String,
    task_args: Option<String>,
    task_kwargs: Option<String>,
    args_from: Option<serde_json::Value>,
    dependencies: Vec<i32>,
    sub_workflow_name: Option<String>,
    sub_definition_key: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct StaleSubworkflowRow {
    workflow_id: String,
    task_index: i32,
    sub_workflow_id: String,
    child_status: String,
    child_result: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct StaleLinkedTaskRow {
    workflow_id: String,
    task_index: i32,
    task_id: String,
    task_status: String,
    task_result: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct StuckWorkflowRow {
    workflow_id: String,
}

#[derive(Debug, sqlx::FromRow)]
struct OrphanedWorkflowRow {
    workflow_id: String,
    name: String,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct DepthRow {
    depth: Option<i32>,
    root_workflow_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Recover stuck workflows by detecting and fixing inconsistent states.
///
/// Scans for 7 classes of stuck workflow tasks and applies the
/// appropriate fix for each. Returns a report with counts per case.
pub async fn recover_stuck_workflows(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
) -> Result<RecoveryReport, WorkflowError> {
    let mut report = RecoveryReport::default();

    recover_case0(pool, registry, &mut report).await;
    recover_case1(pool, &mut report).await;
    recover_case1_5(pool, registry, &mut report).await;
    recover_case1_6(pool, registry, &mut report).await;
    recover_case1_7(pool, registry, &mut report).await;
    recover_case2_3(pool, registry, &mut report).await;
    recover_case4(pool, &mut report).await;

    if report.total() > 0 {
        tracing::info!(
            case0 = report.case0_pending_reevaluated,
            case1 = report.case1_ready_enqueued,
            case1_5 = report.case1_5_subworkflow_started,
            case1_6 = report.case1_6_subworkflow_completed,
            case1_7 = report.case1_7_task_completed,
            case2_3 = report.case2_3_workflow_completed,
            case4 = report.case4_orphaned_failed,
            errors = report.errors,
            "workflow recovery complete",
        );
    }

    Ok(report)
}

// ---------------------------------------------------------------------------
// Case implementations
// ---------------------------------------------------------------------------

/// Case 0: Re-evaluate PENDING tasks whose deps are all terminal.
async fn recover_case0(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    report: &mut RecoveryReport,
) {
    let rows = match sqlx::query_as::<_, StuckPendingRow>(CASE0_STUCK_PENDING_SQL)
        .fetch_all(pool)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "recovery case 0: query failed");
            report.errors += 1;
            return;
        }
    };

    for row in rows {
        let Some(&dep_index) = row.dependencies.first() else {
            continue;
        };

        match engine::process_dependents(pool, &row.workflow_id, dep_index, registry).await {
            Ok(()) => {
                report.case0_pending_reevaluated += 1;
                tracing::debug!(
                    workflow_id = %row.workflow_id,
                    task_index = row.task_index,
                    "recovery case 0: re-evaluated pending task",
                );
            }
            Err(e) => {
                tracing::error!(
                    workflow_id = %row.workflow_id,
                    task_index = row.task_index,
                    error = %e,
                    "recovery case 0: failed to re-evaluate pending task",
                );
                report.errors += 1;
            }
        }
    }
}

/// Case 1: Re-enqueue READY regular tasks with no horsies_tasks row.
async fn recover_case1(pool: &PgPool, report: &mut RecoveryReport) {
    let rows = match sqlx::query_as::<_, ReadyTaskRow>(CASE1_READY_NO_TASK_SQL)
        .fetch_all(pool)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "recovery case 1: query failed");
            report.errors += 1;
            return;
        }
    };

    for row in rows {
        match enqueue_ready_task(pool, &row).await {
            Ok(()) => {
                report.case1_ready_enqueued += 1;
                tracing::debug!(
                    workflow_id = %row.workflow_id,
                    task_index = row.task_index,
                    "recovery case 1: re-enqueued ready task",
                );
            }
            Err(e) => {
                tracing::error!(
                    workflow_id = %row.workflow_id,
                    task_index = row.task_index,
                    error = %e,
                    "recovery case 1: failed to re-enqueue task",
                );
                report.errors += 1;
            }
        }
    }
}

/// Case 1.5: Start child workflows for READY sub-workflow tasks.
async fn recover_case1_5(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    report: &mut RecoveryReport,
) {
    let rows = match sqlx::query_as::<_, ReadySubworkflowRow>(CASE1_5_READY_SUBWORKFLOW_SQL)
        .fetch_all(pool)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "recovery case 1.5: query failed");
            report.errors += 1;
            return;
        }
    };

    for row in rows {
        match start_stuck_subworkflow(pool, registry, &row).await {
            Ok(()) => {
                report.case1_5_subworkflow_started += 1;
                tracing::debug!(
                    workflow_id = %row.workflow_id,
                    task_index = row.task_index,
                    "recovery case 1.5: started sub-workflow",
                );
            }
            Err(e) => {
                tracing::error!(
                    workflow_id = %row.workflow_id,
                    task_index = row.task_index,
                    error = %e,
                    "recovery case 1.5: failed to start sub-workflow",
                );
                report.errors += 1;
            }
        }
    }
}

/// Case 1.6: Trigger sub-workflow completion callback.
async fn recover_case1_6(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    report: &mut RecoveryReport,
) {
    let rows = match sqlx::query_as::<_, StaleSubworkflowRow>(CASE1_6_STALE_SUBWORKFLOW_SQL)
        .fetch_all(pool)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "recovery case 1.6: query failed");
            report.errors += 1;
            return;
        }
    };

    for row in rows {
        match engine::on_subworkflow_complete(
            pool,
            &row.workflow_id,
            row.task_index,
            &row.sub_workflow_id,
            &row.child_status,
            row.child_result.as_deref(),
            registry,
        )
        .await
        {
            Ok(()) => {
                report.case1_6_subworkflow_completed += 1;
                tracing::debug!(
                    workflow_id = %row.workflow_id,
                    task_index = row.task_index,
                    "recovery case 1.6: triggered sub-workflow completion",
                );
            }
            Err(e) => {
                tracing::error!(
                    workflow_id = %row.workflow_id,
                    task_index = row.task_index,
                    child_workflow_id = %row.sub_workflow_id,
                    error = %e,
                    "recovery case 1.6: sub-workflow completion failed",
                );
                report.errors += 1;
            }
        }
    }
}

/// Case 1.7: Trigger workflow task completion for terminal linked tasks.
async fn recover_case1_7(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    report: &mut RecoveryReport,
) {
    let rows = match sqlx::query_as::<_, StaleLinkedTaskRow>(CASE1_7_STALE_LINKED_TASK_SQL)
        .fetch_all(pool)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "recovery case 1.7: query failed");
            report.errors += 1;
            return;
        }
    };

    for row in rows {
        let is_success = row.task_status == "COMPLETED";
        let result_json = row.task_result.clone().unwrap_or_else(|| {
            // No result stored: produce a status-specific synthetic error
            // matching Python's granular error handling in Case 1.7.
            let synthetic: TaskResult<serde_json::Value> = match row.task_status.as_str() {
                "CANCELLED" => {
                    let err = crate::core::TaskError {
                        error_code: Some(crate::core::OutcomeCode::TaskCancelled.into()),
                        message: Some("Task was cancelled before producing a result".to_owned()),
                        cause: None,
                        data: Some(serde_json::json!({
                            "task_id": row.task_id,
                            "task_status": row.task_status,
                            "recovery": "case_1_7",
                        })),
                    };
                    TaskResult::Err(err)
                }
                "COMPLETED" => {
                    let err = crate::core::TaskError {
                        error_code: Some(crate::core::RetrievalCode::ResultNotAvailable.into()),
                        message: Some("Task completed but result is missing".to_owned()),
                        cause: None,
                        data: Some(serde_json::json!({
                            "task_id": row.task_id,
                            "task_status": row.task_status,
                            "recovery": "case_1_7",
                        })),
                    };
                    TaskResult::Err(err)
                }
                _ => {
                    // FAILED or any other status: worker likely crashed
                    let err = crate::core::TaskError {
                        error_code: Some(crate::core::OperationalErrorCode::WorkerCrashed.into()),
                        message: Some(format!(
                            "Worker crashed during task execution \
                             (task_status={}, no result stored)",
                            row.task_status,
                        )),
                        cause: None,
                        data: Some(serde_json::json!({
                            "task_id": row.task_id,
                            "task_status": row.task_status,
                            "recovery": "case_1_7",
                        })),
                    };
                    TaskResult::Err(err)
                }
            };
            serde_json::to_string(&synthetic).unwrap_or_else(|_| "{}".to_owned())
        });

        match engine::on_workflow_task_complete(
            pool,
            &row.task_id,
            &result_json,
            is_success,
            registry,
        )
        .await
        {
            Ok(()) => {
                report.case1_7_task_completed += 1;
                tracing::debug!(
                    workflow_id = %row.workflow_id,
                    task_index = row.task_index,
                    "recovery case 1.7: triggered task completion callback",
                );
            }
            Err(e) => {
                tracing::error!(
                    workflow_id = %row.workflow_id,
                    task_id = %row.task_id,
                    error = %e,
                    "recovery case 1.7: workflow task completion failed",
                );
                report.errors += 1;
            }
        }
    }
}

/// Case 2+3: Check completion for RUNNING workflows with all terminal tasks.
async fn recover_case2_3(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    report: &mut RecoveryReport,
) {
    let rows = match sqlx::query_as::<_, StuckWorkflowRow>(CASE2_3_STUCK_WORKFLOW_SQL)
        .fetch_all(pool)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "recovery case 2+3: query failed");
            report.errors += 1;
            return;
        }
    };

    for row in rows {
        match engine::check_workflow_completion_preserving(pool, &row.workflow_id, registry).await {
            Ok(()) => {
                report.case2_3_workflow_completed += 1;
                tracing::debug!(
                    workflow_id = %row.workflow_id,
                    "recovery case 2+3: triggered workflow completion check",
                );
            }
            Err(e) => {
                tracing::error!(
                    workflow_id = %row.workflow_id,
                    error = %e,
                    "recovery case 2+3: workflow completion check failed",
                );
                report.errors += 1;
            }
        }
    }
}

/// Case 4: Fail orphaned RUNNING workflows with zero workflow_tasks.
async fn recover_case4(pool: &PgPool, report: &mut RecoveryReport) {
    let rows = match sqlx::query_as::<_, OrphanedWorkflowRow>(CASE4_ORPHANED_WORKFLOW_SQL)
        .fetch_all(pool)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "recovery case 4: query failed");
            report.errors += 1;
            return;
        }
    };

    for row in rows {
        let error_json = serde_json::json!({
            "error_code": "E400",
            "message": format!(
                "Orphaned workflow '{}': no workflow_tasks found. \
                 Workflow was likely created but task DAG insertion failed.",
                row.name,
            ),
            "recovery": "case_4",
        });
        let error_str = serde_json::to_string(&error_json).unwrap_or_else(|_| "{}".to_owned());

        match sqlx::query(FAIL_ORPHANED_WORKFLOW_SQL)
            .bind(&row.workflow_id)
            .bind(&error_str)
            .execute(pool)
            .await
        {
            Ok(_) => {
                report.case4_orphaned_failed += 1;
                tracing::warn!(
                    workflow_id = %row.workflow_id,
                    workflow_name = %row.name,
                    "recovery case 4: failed orphaned workflow (no tasks)",
                );
            }
            Err(e) => {
                tracing::error!(
                    workflow_id = %row.workflow_id,
                    error = %e,
                    "recovery case 4: failed to mark orphaned workflow as FAILED",
                );
                report.errors += 1;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Enqueue a READY task into horsies_tasks and link it.
async fn enqueue_ready_task(pool: &PgPool, row: &ReadyTaskRow) -> Result<(), WorkflowError> {
    let task_id = Uuid::new_v4().to_string();
    let max_retries = parse_max_retries(row.task_options.as_deref());

    let merged_kwargs = merge_args_from_for_ready(
        pool,
        &row.workflow_id,
        row.task_kwargs.as_deref(),
        &row.args_from,
        &row.dependencies,
    )
    .await?;

    let enqueue_sha = format!("wf-{}", task_id);

    sqlx::query(ENQUEUE_TASK_SQL)
        .bind(&task_id)
        .bind(&row.task_name)
        .bind(&row.queue_name)
        .bind(row.priority)
        .bind(&row.task_args)
        .bind(&merged_kwargs)
        .bind(parse_good_until_from_options(row.task_options.as_deref()))
        .bind(max_retries)
        .bind(&row.task_options)
        .bind(&enqueue_sha)
        .execute(pool)
        .await?;

    sqlx::query(LINK_ENQUEUED_TASK_SQL)
        .bind(&task_id)
        .bind(&row.workflow_id)
        .bind(row.task_index)
        .execute(pool)
        .await?;

    Ok(())
}

/// Start a child workflow for a stuck READY sub-workflow task.
async fn start_stuck_subworkflow(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    row: &ReadySubworkflowRow,
) -> Result<(), WorkflowError> {
    let spec_name = row
        .task_name
        .strip_prefix("__sub_workflow:")
        .or(row.sub_workflow_name.as_deref())
        .unwrap_or(&row.task_name);

    // Resolve by definition_key first, then fall back to name.
    let registered = registry
        .resolve_child_registration(spec_name, row.sub_definition_key.as_deref())
        .ok_or_else(|| WorkflowError::WorkflowNotFound {
            workflow_id: format!(
                "sub-workflow spec not found (definition_key={:?}, name='{}')",
                row.sub_definition_key, spec_name,
            ),
        })?;

    let merged_kwargs = merge_args_from_for_ready(
        pool,
        &row.workflow_id,
        row.task_kwargs.as_deref(),
        &row.args_from,
        &row.dependencies,
    )
    .await?;
    let has_child_inputs =
        row.task_args.is_some() || row.task_kwargs.is_some() || row.args_from.is_some();
    let child_spec = materialize_child_spec(
        registered,
        has_child_inputs,
        row.task_args.as_deref(),
        merged_kwargs.as_deref(),
        registry,
    )?;

    let depth_row: DepthRow = sqlx::query_as(GET_DEPTH_AND_ROOT_SQL)
        .bind(&row.workflow_id)
        .fetch_one(pool)
        .await?;

    let parent_depth = depth_row.depth.unwrap_or(0);
    let root_wf_id = depth_row
        .root_workflow_id
        .as_deref()
        .unwrap_or(&row.workflow_id);

    // Start child workflow + link in a single transaction.
    let mut tx = pool.begin().await?;

    let child_id = start_child_workflow_in_tx(
        &mut tx,
        &child_spec,
        &row.workflow_id,
        row.task_index,
        parent_depth + 1,
        root_wf_id,
        registry,
    )
    .await?;

    let link_result = sqlx::query(LINK_SUBWORKFLOW_SQL)
        .bind(&child_id)
        .bind(&row.workflow_id)
        .bind(row.task_index)
        .execute(&mut *tx)
        .await?;

    if link_result.rows_affected() == 0 {
        tx.rollback().await?;
        return Ok(());
    }

    tx.commit().await?;

    Ok(())
}

use crate::workflow_engine::args_merge::merge_args_from_async as merge_args_from_for_ready;

use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::history::enqueue::{prepare_enqueue_facts, EnqueueInputEligibility};
use crate::core::task::retry_utils::parse_max_retries;
use crate::core::WorkflowSpecRegistry;
use sqlx::PgPool;
use uuid::Uuid;

use crate::workflow_engine::engine;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::parse_good_until_from_options;
use crate::workflow_engine::start::{materialize_child_spec, start_child_workflow_in_tx};

// ---------------------------------------------------------------------------
// Recovery report
// ---------------------------------------------------------------------------

/// Tracks counts of recovered items by case.
#[derive(Debug, Default, serde::Serialize)]
pub struct RecoveryReport {
    /// Case 0: PENDING wf_tasks with all deps terminal.
    pub case0_pending_reevaluated: u32,
    /// Case 1: READY wf_tasks with no task_id.
    pub case1_ready_enqueued: u32,
    /// Case 1.5: READY sub-workflow wf_tasks with no child started.
    pub case1_5_subworkflow_started: u32,
    /// Case 1.6: Non-terminal sub-workflow wf_tasks with terminal child.
    pub case1_6_subworkflow_completed: u32,
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
            + self.case2_3_workflow_completed
            + self.case4_orphaned_failed
    }
}

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

/// Case 0: PENDING wf_tasks where ALL dependencies are terminal, workflow RUNNING.
const CASE0_STUCK_PENDING_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'PENDING'
  AND w.status = 'RUNNING'
  AND ($1::uuid[] IS NULL OR wt.workflow_id = ANY($1::uuid[]))
  AND NOT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks dep
    WHERE dep.workflow_id = wt.workflow_id
      AND wt.dependencies @> ARRAY[dep.task_index]
      AND dep.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
  )
LIMIT CAST($2 AS bigint)";

/// Case 1: READY regular wf_tasks with no linked horsies_task.
const CASE1_READY_NO_TASK_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index, wt.task_name,
       wt.task_args, wt.task_kwargs, wt.queue_name, wt.priority,
       wt.task_options, wt.args_from, wt.workflow_ctx_from, wt.dependencies
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'READY'
  AND wt.task_id IS NULL
  AND wt.is_subworkflow = FALSE
  AND w.status = 'RUNNING'
  AND ($1::uuid[] IS NULL OR wt.workflow_id = ANY($1::uuid[]))
LIMIT CAST($2 AS bigint)";

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
  AND w.status = 'RUNNING'
  AND ($1::uuid[] IS NULL OR wt.workflow_id = ANY($1::uuid[]))
LIMIT CAST($2 AS bigint)";

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
  AND cw.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
  AND w.status = 'RUNNING'
  AND ($1::uuid[] IS NULL OR cw.id = ANY($1::uuid[]))
LIMIT CAST($2 AS bigint)";

/// Case 2+3: RUNNING workflows where all wf_tasks are terminal.
///
/// NOTE: We require at least one workflow_task to exist. Orphaned workflows
/// (RUNNING but with zero workflow_tasks) are skipped to avoid "no rows
/// returned" errors in check_workflow_completion.
const CASE2_3_STUCK_WORKFLOW_SQL: &str = "\
SELECT w.id as workflow_id
FROM horsies_workflows w
WHERE w.status = 'RUNNING'
  AND ($1::uuid[] IS NULL OR w.id = ANY($1::uuid[]))
  AND EXISTS (
    SELECT 1 FROM horsies_workflow_tasks wt
    WHERE wt.workflow_id = w.id
  )
  AND NOT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks wt
    WHERE wt.workflow_id = w.id
      AND wt.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
  )
LIMIT CAST($2 AS bigint)";

/// Case 4: Orphaned RUNNING workflows with zero workflow_tasks.
///
/// These are workflows that were created but whose task DAG was never inserted,
/// likely due to a crash during workflow start. We mark them as FAILED.
const CASE4_ORPHANED_WORKFLOW_SQL: &str = "\
SELECT w.id as workflow_id, w.name
FROM horsies_workflows w
WHERE w.status = 'RUNNING'
  AND ($1::uuid[] IS NULL OR w.id = ANY($1::uuid[]))
  AND NOT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks wt
    WHERE wt.workflow_id = w.id
  )
LIMIT CAST($2 AS bigint)";

const GET_WORKFLOW_TREE_IDS_SQL: &str = "\
WITH RECURSIVE tree AS (
    SELECT id FROM horsies_workflows WHERE id = $1
    UNION ALL
    SELECT child.id
    FROM horsies_workflows child
    JOIN tree parent ON child.parent_workflow_id = parent.id
)
SELECT id FROM tree";

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

/// Link workflow_task to a newly created horsies_task.
const LINK_ENQUEUED_TASK_SQL: &str = "\
UPDATE horsies_workflow_tasks wt
SET task_id = $1::uuid, status = 'ENQUEUED', started_at = NOW()
FROM horsies_workflows w
WHERE wt.workflow_id = $2::uuid AND wt.task_index = $3
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
    workflow_id: Uuid,
    task_index: i32,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct ReadyTaskRow {
    workflow_id: Uuid,
    task_index: i32,
    task_name: String,
    task_args: Option<String>,
    task_kwargs: Option<String>,
    queue_name: String,
    priority: i32,
    task_options: Option<String>,
    args_from: Option<serde_json::Value>,
    workflow_ctx_from: Option<Vec<String>>,
    dependencies: Vec<i32>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct ReadySubworkflowRow {
    workflow_id: Uuid,
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
    workflow_id: Uuid,
    task_index: i32,
    sub_workflow_id: Uuid,
    child_status: String,
    child_result: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct StuckWorkflowRow {
    workflow_id: Uuid,
}

#[derive(Debug, sqlx::FromRow)]
struct OrphanedWorkflowRow {
    workflow_id: Uuid,
    name: String,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct DepthRow {
    depth: Option<i32>,
    root_workflow_id: Option<Uuid>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Recover stuck workflows by detecting and fixing inconsistent states.
///
/// Scans for 6 classes of stuck workflow tasks and applies the
/// appropriate fix for each. Returns a report with counts per case.
/// Rows a single global recovery pass processes per candidate query, so one
/// pass cannot hold its transaction across an unbounded backlog (every
/// recovered row does engine work — enqueues, completion callbacks). Parity
/// with horsies PR #103 (GLOBAL_SCAN_ROW_CAP).
pub(crate) const GLOBAL_SCAN_ROW_CAP: i64 = 200;

#[derive(Clone, Copy)]
enum RecoveryScope<'a> {
    Global,
    WorkflowTree(&'a [Uuid]),
}

impl RecoveryScope<'_> {
    fn workflow_ids(self) -> Option<Vec<Uuid>> {
        match self {
            Self::Global => None,
            Self::WorkflowTree(ids) => Some(ids.to_vec()),
        }
    }
}

/// Global recovery pass, capped at [`GLOBAL_SCAN_ROW_CAP`] rows per candidate
/// query. The remainder (if any) is recovered by the next periodic pass.
pub async fn recover_stuck_workflows(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    finalizing_grace_ms: u64,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<RecoveryReport, WorkflowError> {
    recover_stuck_workflows_with_cap(
        pool,
        registry,
        Some(GLOBAL_SCAN_ROW_CAP),
        finalizing_grace_ms,
        payload,
        retention,
    )
    .await
}

/// Recovery pass with an explicit per-candidate-query row cap.
///
/// `max_rows = None` binds `LIMIT NULL` (uncapped) — used by the resume path so
/// a resumed tree is recovered completely in one pass, not left partial until
/// the next periodic cycle.
///
/// `_finalizing_grace_ms` remains in the compatibility signature but no longer
/// controls a terminal-live-row scan. The v35 phase-2 outbox is the sole
/// workflow-task completion recovery authority.
pub(crate) async fn recover_stuck_workflows_with_cap(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    max_rows: Option<i64>,
    _finalizing_grace_ms: u64,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<RecoveryReport, WorkflowError> {
    recover_stuck_workflows_in_scope(
        pool,
        registry,
        RecoveryScope::Global,
        max_rows,
        _finalizing_grace_ms,
        payload,
        retention,
    )
    .await
}

/// Uncapped recovery restricted to one workflow tree (root plus descendants).
/// Resume uses this path so a caller-facing operation never scans or mutates
/// unrelated workflows.
pub(crate) async fn recover_stuck_workflow_tree(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    root_workflow_id: Uuid,
    finalizing_grace_ms: u64,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<RecoveryReport, WorkflowError> {
    let workflow_ids: Vec<Uuid> = sqlx::query_scalar(GET_WORKFLOW_TREE_IDS_SQL)
        .bind(root_workflow_id)
        .fetch_all(pool)
        .await?;
    recover_stuck_workflows_in_scope(
        pool,
        registry,
        RecoveryScope::WorkflowTree(&workflow_ids),
        None,
        finalizing_grace_ms,
        payload,
        retention,
    )
    .await
}

async fn recover_stuck_workflows_in_scope(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    scope: RecoveryScope<'_>,
    max_rows: Option<i64>,
    _finalizing_grace_ms: u64,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<RecoveryReport, WorkflowError> {
    let mut report = RecoveryReport::default();

    recover_case0(
        pool,
        registry,
        scope,
        max_rows,
        &mut report,
        payload,
        retention,
    )
    .await?;
    recover_case1(pool, scope, max_rows, &mut report, retention).await?;
    recover_case1_5(pool, registry, scope, max_rows, &mut report, retention).await?;
    recover_case1_6(
        pool,
        registry,
        scope,
        max_rows,
        &mut report,
        payload,
        retention,
    )
    .await?;
    recover_case2_3(
        pool,
        registry,
        scope,
        max_rows,
        &mut report,
        payload,
        retention,
    )
    .await?;
    recover_case4(pool, scope, max_rows, &mut report).await?;

    if report.total() > 0 {
        tracing::info!(
            case0 = report.case0_pending_reevaluated,
            case1 = report.case1_ready_enqueued,
            case1_5 = report.case1_5_subworkflow_started,
            case1_6 = report.case1_6_subworkflow_completed,
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
    scope: RecoveryScope<'_>,
    max_rows: Option<i64>,
    report: &mut RecoveryReport,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let rows = sqlx::query_as::<_, StuckPendingRow>(CASE0_STUCK_PENDING_SQL)
        .bind(scope.workflow_ids())
        .bind(max_rows)
        .fetch_all(pool)
        .await?;

    for row in rows {
        match engine::recover_pending_workflow_task(
            pool,
            row.workflow_id,
            row.task_index,
            registry,
            payload,
            retention,
        )
        .await
        {
            Ok(true) => {
                report.case0_pending_reevaluated += 1;
                tracing::debug!(
                    workflow_id = %row.workflow_id,
                    task_index = row.task_index,
                    "recovery case 0: re-evaluated pending task",
                );
            }
            Ok(false) => {}
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
    Ok(())
}

/// Case 1: Re-enqueue READY regular tasks with no horsies_tasks row.
async fn recover_case1(
    pool: &PgPool,
    scope: RecoveryScope<'_>,
    max_rows: Option<i64>,
    report: &mut RecoveryReport,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let rows = sqlx::query_as::<_, ReadyTaskRow>(CASE1_READY_NO_TASK_SQL)
        .bind(scope.workflow_ids())
        .bind(max_rows)
        .fetch_all(pool)
        .await?;

    for row in rows {
        match enqueue_ready_task(pool, &row, retention).await {
            Ok(true) => {
                report.case1_ready_enqueued += 1;
                tracing::debug!(
                    workflow_id = %row.workflow_id,
                    task_index = row.task_index,
                    "recovery case 1: re-enqueued ready task",
                );
            }
            Ok(false) => {
                // LINK matched 0 rows; the inserted row was rolled back, so this
                // node was not recovered and must not be counted.
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
    Ok(())
}

/// Case 1.5: Start child workflows for READY sub-workflow tasks.
async fn recover_case1_5(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    scope: RecoveryScope<'_>,
    max_rows: Option<i64>,
    report: &mut RecoveryReport,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let rows = sqlx::query_as::<_, ReadySubworkflowRow>(CASE1_5_READY_SUBWORKFLOW_SQL)
        .bind(scope.workflow_ids())
        .bind(max_rows)
        .fetch_all(pool)
        .await?;

    for row in rows {
        match start_stuck_subworkflow(pool, registry, &row, retention).await {
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
    Ok(())
}

/// Case 1.6: Trigger sub-workflow completion callback.
async fn recover_case1_6(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    scope: RecoveryScope<'_>,
    max_rows: Option<i64>,
    report: &mut RecoveryReport,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let rows = sqlx::query_as::<_, StaleSubworkflowRow>(CASE1_6_STALE_SUBWORKFLOW_SQL)
        .bind(scope.workflow_ids())
        .bind(max_rows)
        .fetch_all(pool)
        .await?;

    for row in rows {
        match engine::on_subworkflow_complete(
            pool,
            row.workflow_id,
            row.task_index,
            row.sub_workflow_id,
            &row.child_status,
            row.child_result.as_deref(),
            registry,
            payload,
            retention,
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
    Ok(())
}

/// Case 2+3: Check completion for RUNNING workflows with all terminal tasks.
async fn recover_case2_3(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    scope: RecoveryScope<'_>,
    max_rows: Option<i64>,
    report: &mut RecoveryReport,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let rows = sqlx::query_as::<_, StuckWorkflowRow>(CASE2_3_STUCK_WORKFLOW_SQL)
        .bind(scope.workflow_ids())
        .bind(max_rows)
        .fetch_all(pool)
        .await?;

    for row in rows {
        match engine::check_workflow_completion(pool, row.workflow_id, registry, payload, retention)
            .await
        {
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
    Ok(())
}

/// Case 4: Fail orphaned RUNNING workflows with zero workflow_tasks.
async fn recover_case4(
    pool: &PgPool,
    scope: RecoveryScope<'_>,
    max_rows: Option<i64>,
    report: &mut RecoveryReport,
) -> Result<(), WorkflowError> {
    let rows = sqlx::query_as::<_, OrphanedWorkflowRow>(CASE4_ORPHANED_WORKFLOW_SQL)
        .bind(scope.workflow_ids())
        .bind(max_rows)
        .fetch_all(pool)
        .await?;

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
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Enqueue a READY task into horsies_tasks and link it.
///
/// Returns `true` when the workflow_task was linked (recovered), `false` when
/// the LINK matched 0 rows and the inserted row was rolled back.
async fn enqueue_ready_task(
    pool: &PgPool,
    row: &ReadyTaskRow,
    retention: &RetentionConfig,
) -> Result<bool, WorkflowError> {
    let task_id = crate::core::history::identity::uuid7::mint_task_id().map_err(|error| {
        WorkflowError::Validation(format!("task identity mint failed: {error}"))
    })?;
    let max_retries = parse_max_retries(row.task_options.as_deref());

    let merged_kwargs = merge_args_from_for_ready(
        pool,
        row.workflow_id,
        row.task_kwargs.as_deref(),
        &row.args_from,
        &row.dependencies,
    )
    .await?;

    // Inject workflow_ctx for ctx-capable nodes, matching the runtime promotion
    // path. Without this, a READY `workflow_ctx_from` task recovered here would
    // run with no upstream context and fail or produce a wrong result.
    let merged_kwargs = engine::inject_workflow_ctx_into_kwargs(
        pool,
        row.workflow_id,
        row.task_index,
        &row.task_name,
        row.workflow_ctx_from.as_deref(),
        merged_kwargs,
    )
    .await?;

    let enqueue_sha = format!("wf-{}", task_id);
    let good_until = parse_good_until_from_options(row.task_options.as_deref());
    let retention_class_key = retention.resolve_queue_class(&row.queue_name);
    let facts = prepare_enqueue_facts(
        &row.task_name,
        &row.queue_name,
        row.priority,
        row.task_args.as_deref(),
        merged_kwargs.as_deref(),
        good_until,
        None,
        row.task_options.as_deref(),
        retention_class_key.as_deref(),
        false,
        None,
        EnqueueInputEligibility::NeverEligible,
    )
    .map_err(|error| WorkflowError::Validation(error.to_string()))?;

    // INSERT + LINK in a single transaction so a row whose LINK matches 0 rows
    // (workflow no longer RUNNING / task no longer READY, or a concurrent
    // recovery already linked it) is rolled back rather than left as an
    // orphaned, claimable PENDING row that would run a duplicate side effect.
    // Mirrors `enqueue_workflow_task` in engine.rs.
    let mut tx = pool.begin().await?;

    sqlx::query(ENQUEUE_TASK_SQL)
        .bind(&task_id)
        .bind(&row.task_name)
        .bind(&row.queue_name)
        .bind(row.priority)
        .bind(&row.task_args)
        .bind(&merged_kwargs)
        .bind(good_until)
        .bind(max_retries)
        .bind(&row.task_options)
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
        .bind(&row.workflow_id)
        .bind(row.task_index)
        .execute(&mut *tx)
        .await?;

    if link_result.rows_affected() == 0 {
        tx.rollback().await?;
        tracing::debug!(
            workflow_id = %row.workflow_id,
            task_index = row.task_index,
            "recovery case 1: ready-task link matched 0 rows (workflow not RUNNING or task not READY), rolled back",
        );
        return Ok(false);
    }

    tx.commit().await?;

    Ok(true)
}

/// Start a child workflow for a stuck READY sub-workflow task.
async fn start_stuck_subworkflow(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    row: &ReadySubworkflowRow,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let spec_name = row
        .task_name
        .strip_prefix("__sub_workflow:")
        .or(row.sub_workflow_name.as_deref())
        .unwrap_or(&row.task_name);

    // Resolve by definition_key first, then fall back to name.
    let registered = registry
        .resolve_child_registration(spec_name, row.sub_definition_key.as_deref())
        .ok_or_else(|| {
            WorkflowError::Validation(format!(
                "sub-workflow spec not found (definition_key={:?}, name='{}')",
                row.sub_definition_key, spec_name,
            ))
        })?;

    let merged_kwargs = merge_args_from_for_ready(
        pool,
        row.workflow_id,
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
    let root_wf_id = depth_row.root_workflow_id.unwrap_or(row.workflow_id);

    // Start child workflow + link in a single transaction.
    let mut tx = pool.begin().await?;

    let child_id = start_child_workflow_in_tx(
        &mut tx,
        &child_spec,
        row.workflow_id,
        row.task_index,
        parent_depth + 1,
        root_wf_id,
        registry,
        retention,
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

#[cfg(test)]
mod cap_tests {
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

    async fn insert_orphaned_workflow(pool: &PgPool, id: Uuid) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES (
                $1, 'cap_test_wf', 'RUNNING', 'fail', NULL,
                'test.cap.v1', 0, $1,
                NOW(), NOW(), NOW(), NOW()
            )",
        )
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn failed_count(pool: &PgPool, ids: &[Uuid]) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_workflows WHERE id = ANY($1) AND status = 'FAILED'",
        )
        .bind(ids)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn discovery_query_failure_is_typed() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://postgres@localhost/postgres")
            .unwrap();
        pool.close().await;
        let error = recover_stuck_workflows(
            &pool,
            &WorkflowSpecRegistry::new(),
            0,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect_err("candidate discovery failure must cross the containment seam");
        assert!(matches!(error, WorkflowError::Database(_)));
    }

    #[tokio::test]
    #[serial]
    async fn case0_advances_the_selected_root_without_touching_unready_siblings() {
        let broker = PostgresBroker::connect(&test_db_url()).await.unwrap();
        broker.ensure_schema_initialized().await.unwrap();
        let pool = broker.pool().clone();
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, definition_key, depth,
                 root_workflow_id, sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'p7_case0', 'RUNNING', 'fail', $2, 0, $1,
                       NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .bind(format!("test.p7.case0.{workflow_id}"))
        .execute(&pool)
        .await
        .unwrap();
        for (task_index, dependencies) in [(0_i32, vec![]), (1_i32, vec![2_i32])] {
            sqlx::query(
                "INSERT INTO horsies_workflow_tasks (
                     id, workflow_id, task_index, node_id, task_name, task_args,
                     task_kwargs, queue_name, priority, dependencies,
                     allow_failed_deps, join_type, status, is_subworkflow,
                     created_at
                 ) VALUES ($1, $2, $3, $4, $5, '[]', '{}', 'default', 100,
                           $6, FALSE, 'all', 'PENDING', FALSE, NOW())",
            )
            .bind(Uuid::new_v4())
            .bind(workflow_id)
            .bind(task_index)
            .bind(format!("node_{task_index}"))
            .bind(format!("p7_case0_{task_index}"))
            .bind(dependencies)
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                 id, workflow_id, task_index, node_id, task_name, queue_name,
                 priority, dependencies, allow_failed_deps, join_type, status,
                 is_subworkflow, created_at
             ) VALUES ($1, $2, 2, 'blocker', 'p7_case0_blocker', 'default',
                       100, '{}', FALSE, 'all', 'RUNNING', FALSE, NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .execute(&pool)
        .await
        .unwrap();

        let report = recover_stuck_workflow_tree(
            &pool,
            &WorkflowSpecRegistry::new(),
            workflow_id,
            0,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.case0_pending_reevaluated, 1);
        let statuses: Vec<(i32, String)> = sqlx::query_as(
            "SELECT task_index, status FROM horsies_workflow_tasks
             WHERE workflow_id = $1 ORDER BY task_index",
        )
        .bind(workflow_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(statuses[0].1, "ENQUEUED");
        assert_eq!(statuses[1].1, "PENDING");
        assert_eq!(statuses[2].1, "RUNNING");
    }

    /// The per-query cap bounds how many candidates one global pass processes;
    /// an uncapped (None) pass processes the rest. Parity with horsies PR #103.
    #[tokio::test]
    #[serial]
    async fn recovery_cap_limits_rows_then_uncapped_processes_rest() {
        let broker = PostgresBroker::connect(&test_db_url()).await.unwrap();
        let pool = broker.pool().clone();

        // Isolate: remove any pre-existing orphaned workflows.
        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();

        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        for id in &ids {
            insert_orphaned_workflow(&pool, *id).await;
        }

        let registry = WorkflowSpecRegistry::new();

        // Capped at 2: exactly 2 of the 3 orphans are failed this pass.
        let report = recover_stuck_workflows_with_cap(
            &pool,
            &registry,
            Some(2),
            0,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.case4_orphaned_failed, 2);
        assert_eq!(failed_count(&pool, &ids).await, 2);

        // Uncapped: the remaining orphan is failed.
        let report = recover_stuck_workflows_with_cap(
            &pool,
            &registry,
            None,
            0,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.case4_orphaned_failed, 1);
        assert_eq!(failed_count(&pool, &ids).await, 3);
    }

    #[tokio::test]
    #[serial]
    async fn p6_workflow_enqueue_recovery_persists_never_eligible_facts() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
             ) VALUES (
                $1::uuid, 'p6_recovery_wf', 'RUNNING', 'fail', NULL,
                'p6.recovery.v1', 0, $1::uuid, NOW(), NOW(), NOW(), NOW()
             )",
        )
        .bind(&workflow_id)
        .execute(&pool)
        .await
        .expect("insert recovery workflow");
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, created_at
             ) VALUES (
                $1::uuid, $2::uuid, 0, 'p6_recovery', 'p6_recovery_task', '[1]', '{}',
                'bulk', 17, '{}', FALSE, 'all', 'READY', FALSE, NOW()
             )",
        )
        .bind(Uuid::new_v4())
        .bind(&workflow_id)
        .execute(&pool)
        .await
        .expect("insert ready recovery node");
        let row = ReadyTaskRow {
            workflow_id,
            task_index: 0,
            task_name: "p6_recovery_task".to_owned(),
            task_args: Some("[1]".to_owned()),
            task_kwargs: Some("{}".to_owned()),
            queue_name: "bulk".to_owned(),
            priority: 17,
            task_options: None,
            args_from: None,
            workflow_ctx_from: None,
            dependencies: Vec::new(),
        };
        let mut retention = RetentionConfig::default();
        retention
            .queue_retention
            .insert("bulk".to_owned(), Some(chrono::Duration::days(7)));
        assert!(enqueue_ready_task(&pool, &row, &retention)
            .await
            .expect("recovery enqueue path"));
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
        .expect("read recovery enqueue facts");
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
        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1::uuid")
            .bind(&workflow_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1::uuid")
            .bind(&workflow_id)
            .execute(&pool)
            .await
            .ok();
    }
}

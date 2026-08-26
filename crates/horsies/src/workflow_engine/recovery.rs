use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::history::enqueue::{prepare_enqueue_facts, EnqueueInputEligibility};
use crate::core::task::retry_utils::parse_max_retries;
use crate::core::WorkflowSpecRegistry;
use sqlx::PgPool;
use std::time::Instant;
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
    /// Query and processing metrics for each recovery case.
    pub metrics: RecoveryMetrics,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct RecoveryMetrics {
    pub case0: RecoveryCaseMetrics,
    pub case1: RecoveryCaseMetrics,
    pub case1_5: RecoveryCaseMetrics,
    pub case1_6: RecoveryCaseMetrics,
    pub case2_3: RecoveryCaseMetrics,
    pub case4: RecoveryCaseMetrics,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct RecoveryCaseMetrics {
    pub rows_selected: u32,
    pub candidates_returned: u32,
    pub duration_ms: u64,
    pub refusals: u32,
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

#[derive(Debug)]
pub(crate) struct RecoveryPassFailure {
    pub(crate) report: RecoveryReport,
    pub(crate) error: WorkflowError,
}

impl RecoveryPassFailure {
    pub(crate) fn into_health_snapshot(self) -> serde_json::Value {
        let error = self.error.to_string();
        let mut snapshot = serde_json::to_value(self.report)
            .unwrap_or_else(|serialization_error| {
                serde_json::json!({"serialization_error": serialization_error.to_string()})
            });
        if let Some(fields) = snapshot.as_object_mut() {
            fields.insert("state".to_owned(), serde_json::json!("error"));
            fields.insert("error".to_owned(), serde_json::json!(error));
        }
        snapshot
    }
}

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

/// Case 0: PENDING wf_tasks where ALL dependencies are terminal, workflow RUNNING.
const GLOBAL_CASE0_STUCK_PENDING_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'PENDING'
  AND w.status = 'RUNNING'
  AND NOT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks dep
    WHERE dep.workflow_id = wt.workflow_id
      AND wt.dependencies @> ARRAY[dep.task_index]
      AND dep.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
  )
LIMIT CAST($1 AS bigint)";

const TREE_CASE0_STUCK_PENDING_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'PENDING'
  AND w.status = 'RUNNING'
  AND wt.workflow_id = ANY($1::uuid[])
  AND NOT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks dep
    WHERE dep.workflow_id = wt.workflow_id
      AND wt.dependencies @> ARRAY[dep.task_index]
      AND dep.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
  )
LIMIT CAST($2 AS bigint)";

/// Case 1: READY regular wf_tasks with no linked horsies_task.
const GLOBAL_CASE1_READY_NO_TASK_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index, wt.task_name,
       wt.task_args, wt.task_kwargs, wt.queue_name, wt.priority,
       wt.task_options, wt.args_from, wt.workflow_ctx_from, wt.dependencies
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'READY'
  AND wt.task_id IS NULL
  AND wt.is_subworkflow = FALSE
  AND w.status = 'RUNNING'
LIMIT CAST($1 AS bigint)";

const TREE_CASE1_READY_NO_TASK_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index, wt.task_name,
       wt.task_args, wt.task_kwargs, wt.queue_name, wt.priority,
       wt.task_options, wt.args_from, wt.workflow_ctx_from, wt.dependencies
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'READY'
  AND wt.task_id IS NULL
  AND wt.is_subworkflow = FALSE
  AND w.status = 'RUNNING'
  AND wt.workflow_id = ANY($1::uuid[])
LIMIT CAST($2 AS bigint)";

/// Case 1.5: READY sub-workflow wf_tasks with no child workflow started.
const GLOBAL_CASE1_5_READY_SUBWORKFLOW_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index, wt.task_name,
       wt.task_args, wt.task_kwargs, wt.args_from, wt.dependencies,
       wt.sub_workflow_name, wt.sub_definition_key
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'READY'
  AND wt.sub_workflow_id IS NULL
  AND wt.is_subworkflow = TRUE
  AND w.status = 'RUNNING'
LIMIT CAST($1 AS bigint)";

const TREE_CASE1_5_READY_SUBWORKFLOW_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index, wt.task_name,
       wt.task_args, wt.task_kwargs, wt.args_from, wt.dependencies,
       wt.sub_workflow_name, wt.sub_definition_key
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'READY'
  AND wt.sub_workflow_id IS NULL
  AND wt.is_subworkflow = TRUE
  AND w.status = 'RUNNING'
  AND wt.workflow_id = ANY($1::uuid[])
LIMIT CAST($2 AS bigint)";

/// Case 1.6: Non-terminal sub-workflow wf_tasks where child workflow is terminal.
const GLOBAL_CASE1_6_STALE_SUBWORKFLOW_SQL: &str = "\
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
LIMIT CAST($1 AS bigint)";

const TREE_CASE1_6_STALE_SUBWORKFLOW_SQL: &str = "\
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
  AND cw.id = ANY($1::uuid[])
LIMIT CAST($2 AS bigint)";

/// Case 2+3: RUNNING workflows where all wf_tasks are terminal.
///
/// NOTE: We require at least one workflow_task to exist. Orphaned workflows
/// (RUNNING but with zero workflow_tasks) are skipped to avoid "no rows
/// returned" errors in check_workflow_completion.
const TREE_CASE2_3_STUCK_WORKFLOW_SQL: &str = "\
SELECT w.id as workflow_id
FROM horsies_workflows w
WHERE w.status = 'RUNNING'
  AND w.id = ANY($1::uuid[])
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
const TREE_CASE4_ORPHANED_WORKFLOW_SQL: &str = "\
SELECT w.id as workflow_id, w.name
FROM horsies_workflows w
WHERE w.status = 'RUNNING'
  AND w.id = ANY($1::uuid[])
  AND NOT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks wt
    WHERE wt.workflow_id = w.id
  )
LIMIT CAST($2 AS bigint)";

const GLOBAL_WORKFLOW_AUDIT_SQL: &str = "\
WITH cursor_row AS MATERIALIZED (
    SELECT last_created_at, last_id,
           cycle_upper_created_at, cycle_upper_id
    FROM horsies_recovery_scan_cursors
    WHERE scan_name = 'running_workflows'
      AND (claim_token IS NULL OR claim_expires_at <= statement_timestamp())
    FOR UPDATE SKIP LOCKED
),
upper_bound AS MATERIALIZED (
    SELECT COALESCE(c.cycle_upper_created_at, latest.created_at) AS created_at,
           COALESCE(c.cycle_upper_id, latest.id) AS id
    FROM cursor_row c
    LEFT JOIN LATERAL (
        SELECT w.created_at, w.id
        FROM horsies_workflows w
        WHERE c.cycle_upper_id IS NULL
          AND w.status = 'RUNNING'
        ORDER BY w.created_at DESC, w.id DESC
        LIMIT 1
    ) latest ON TRUE
),
scanned AS MATERIALIZED (
    SELECT page.created_at, page.id, page.name
    FROM cursor_row c
    CROSS JOIN upper_bound u
    CROSS JOIN LATERAL (
        SELECT bounded.created_at, bounded.id, bounded.name
        FROM (
            (
                SELECT w.created_at, w.id, w.name
                FROM horsies_workflows w
                WHERE c.last_id IS NULL
                  AND w.status = 'RUNNING'
                  AND u.id IS NOT NULL
                  AND (w.created_at, w.id) <= (u.created_at, u.id)
                ORDER BY w.created_at, w.id
                LIMIT CAST($1 AS bigint)
            )
            UNION ALL
            (
                SELECT w.created_at, w.id, w.name
                FROM horsies_workflows w
                WHERE c.last_id IS NOT NULL
                  AND w.status = 'RUNNING'
                  AND u.id IS NOT NULL
                  AND (w.created_at, w.id) > (c.last_created_at, c.last_id)
                  AND (w.created_at, w.id) <= (u.created_at, u.id)
                ORDER BY w.created_at, w.id
                LIMIT CAST($1 AS bigint)
            )
        ) bounded
        ORDER BY bounded.created_at, bounded.id
        LIMIT CAST($1 AS bigint)
    ) page
),
classified AS MATERIALIZED (
    SELECT s.created_at, s.id, s.name,
           any_task.found IS NOT NULL AS has_tasks,
           nonterminal_task.found IS NULL AS all_tasks_terminal
    FROM scanned s
    LEFT JOIN LATERAL (
        SELECT TRUE AS found
        FROM horsies_workflow_tasks wt
        WHERE wt.workflow_id = s.id
        LIMIT 1
    ) any_task ON TRUE
    LEFT JOIN LATERAL (
        SELECT TRUE AS found
        FROM horsies_workflow_tasks wt
        WHERE wt.workflow_id = s.id
          AND wt.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
        LIMIT 1
    ) nonterminal_task ON TRUE
),
summary AS MATERIALIZED (
    SELECT count(*)::bigint AS scanned_count,
           COALESCE(
               array_agg(id ORDER BY created_at, id)
                   FILTER (WHERE has_tasks AND all_tasks_terminal),
               '{}'::uuid[]
           ) AS completion_ids,
           COALESCE(
               array_agg(id ORDER BY created_at, id)
                   FILTER (WHERE NOT has_tasks),
               '{}'::uuid[]
           ) AS orphan_ids,
           COALESCE(
               array_agg(name ORDER BY created_at, id)
                   FILTER (WHERE NOT has_tasks),
               '{}'::text[]
           ) AS orphan_names
    FROM classified
),
progress AS MATERIALIZED (
    SELECT s.scanned_count,
           last_row.created_at AS last_created_at,
           last_row.id AS last_id,
           s.scanned_count < CAST($1 AS bigint)
               OR (last_row.created_at, last_row.id) = (u.created_at, u.id)
               AS cycle_complete
    FROM summary s
    CROSS JOIN upper_bound u
    LEFT JOIN LATERAL (
        SELECT scanned.created_at, scanned.id
        FROM scanned
        ORDER BY scanned.created_at DESC, scanned.id DESC
        LIMIT 1
    ) last_row ON TRUE
),
advance AS (
    UPDATE horsies_recovery_scan_cursors c
    SET last_created_at = CASE WHEN progress.cycle_complete THEN NULL
                               ELSE progress.last_created_at END,
        last_id = CASE WHEN progress.cycle_complete THEN NULL
                       ELSE progress.last_id END,
        cycle_upper_created_at = CASE WHEN progress.cycle_complete THEN NULL
                                      ELSE upper_bound.created_at END,
        cycle_upper_id = CASE WHEN progress.cycle_complete THEN NULL
                              ELSE upper_bound.id END,
        claim_token = CASE
            WHEN cardinality(summary.completion_ids)
               + cardinality(summary.orphan_ids) > 0
            THEN $2::uuid ELSE NULL
        END,
        claim_expires_at = CASE
            WHEN cardinality(summary.completion_ids)
               + cardinality(summary.orphan_ids) > 0
            THEN statement_timestamp() + CAST($3 AS bigint) * interval '1 millisecond'
            ELSE NULL
        END,
        completed_cycles = completed_cycles
            + CASE WHEN progress.cycle_complete THEN 1 ELSE 0 END,
        last_scan_rows = summary.scanned_count::integer,
        last_candidate_rows =
            cardinality(summary.completion_ids) + cardinality(summary.orphan_ids),
        last_scan_at = statement_timestamp()
    FROM summary, progress, upper_bound
    WHERE c.scan_name = 'running_workflows'
      AND EXISTS (SELECT 1 FROM cursor_row)
    RETURNING c.claim_token
)
SELECT summary.scanned_count, summary.completion_ids,
       summary.orphan_ids, summary.orphan_names,
       (SELECT claim_token FROM advance) AS claim_token
FROM summary
WHERE EXISTS (SELECT 1 FROM advance)";

const RENEW_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL: &str = "\
UPDATE horsies_recovery_scan_cursors
SET claim_expires_at = statement_timestamp()
        + CAST($2 AS bigint) * interval '1 millisecond'
WHERE scan_name = 'running_workflows'
  AND claim_token = $1
  AND claim_expires_at > statement_timestamp()
RETURNING TRUE";

const RELEASE_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL: &str = "\
UPDATE horsies_recovery_scan_cursors
SET claim_token = NULL, claim_expires_at = NULL
WHERE scan_name = 'running_workflows' AND claim_token = $1";

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

const FAIL_ORPHANED_WORKFLOW_WITH_CLAIM_SQL: &str = "\
WITH claim AS MATERIALIZED (
    SELECT TRUE
    FROM horsies_recovery_scan_cursors
    WHERE scan_name = 'running_workflows'
      AND claim_token = $3
      AND claim_expires_at > statement_timestamp()
    FOR SHARE
)
UPDATE horsies_workflows
SET status = 'FAILED',
    error = $2,
    completed_at = NOW(),
    updated_at = NOW()
WHERE id = $1
  AND status = 'RUNNING'
  AND EXISTS (SELECT 1 FROM claim)";

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
SET task_id = $1::uuid, status = 'ENQUEUED'
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
struct GlobalWorkflowAuditRow {
    scanned_count: i64,
    completion_ids: Vec<Uuid>,
    orphan_ids: Vec<Uuid>,
    orphan_names: Vec<String>,
    claim_token: Option<Uuid>,
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
/// Maximum rows one global recovery query returns or one workflow audit page
/// examines. This bound limits both recovery work and empty-result scan work.
pub(crate) const GLOBAL_SCAN_ROW_CAP: i64 = 200;
/// A caller renews this durable page lease before each candidate action.
/// A stopped caller releases ownership through expiry within five minutes.
const GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS: i64 = 300_000;

#[derive(Clone, Copy)]
enum RecoveryScope<'a> {
    Global,
    WorkflowTree(&'a [Uuid]),
}

fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn saturated_row_count(count: usize) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}

fn record_discovery_error(
    metrics: &mut RecoveryCaseMetrics,
    report_errors: &mut u32,
    started: Instant,
) {
    metrics.duration_ms = elapsed_millis(started);
    metrics.errors += 1;
    *report_errors += 1;
}

/// Global recovery pass with [`GLOBAL_SCAN_ROW_CAP`] as its per-query or
/// per-audit-page row budget. Later cursor pages cover the remaining workflows.
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

pub(crate) async fn recover_stuck_workflows_observed(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    finalizing_grace_ms: u64,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<RecoveryReport, RecoveryPassFailure> {
    recover_stuck_workflows_in_scope(
        pool,
        registry,
        RecoveryScope::Global,
        Some(GLOBAL_SCAN_ROW_CAP),
        finalizing_grace_ms,
        payload,
        retention,
    )
    .await
}

/// Recovery pass with an explicit per-candidate-query row cap.
///
/// `max_rows = None` leaves candidate queries uncapped and makes the global
/// workflow audit cover one complete cursor cycle. Workflow-tree recovery uses
/// the same uncapped rule so resume completes the requested tree in one pass.
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
    .map_err(|failure| failure.error)
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
    .map_err(|failure| failure.error)
}

async fn recover_stuck_workflows_in_scope(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    scope: RecoveryScope<'_>,
    max_rows: Option<i64>,
    _finalizing_grace_ms: u64,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<RecoveryReport, RecoveryPassFailure> {
    let mut report = RecoveryReport::default();

    if let Err(error) = recover_case0(
        pool,
        registry,
        scope,
        max_rows,
        &mut report,
        payload,
        retention,
    )
    .await
    {
        return Err(RecoveryPassFailure { report, error });
    }
    if let Err(error) = recover_case1(pool, scope, max_rows, &mut report, retention).await {
        return Err(RecoveryPassFailure { report, error });
    }
    if let Err(error) =
        recover_case1_5(pool, registry, scope, max_rows, &mut report, retention).await
    {
        return Err(RecoveryPassFailure { report, error });
    }
    if let Err(error) = recover_case1_6(
        pool,
        registry,
        scope,
        max_rows,
        &mut report,
        payload,
        retention,
    )
    .await
    {
        return Err(RecoveryPassFailure { report, error });
    }
    match scope {
        RecoveryScope::Global => {
            if let Err(error) = recover_global_workflow_end_states(
                pool,
                registry,
                max_rows,
                &mut report,
                payload,
                retention,
            )
            .await
            {
                return Err(RecoveryPassFailure { report, error });
            }
        }
        RecoveryScope::WorkflowTree(ids) => {
            if let Err(error) = recover_tree_case2_3(
                pool,
                registry,
                ids,
                max_rows,
                &mut report,
                payload,
                retention,
            )
            .await
            {
                return Err(RecoveryPassFailure { report, error });
            }
            if let Err(error) = recover_tree_case4(pool, ids, max_rows, &mut report).await {
                return Err(RecoveryPassFailure { report, error });
            }
        }
    }

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
    let started = Instant::now();
    let rows_result = match scope {
        RecoveryScope::Global => {
            sqlx::query_as::<_, StuckPendingRow>(GLOBAL_CASE0_STUCK_PENDING_SQL)
                .bind(max_rows)
                .fetch_all(pool)
                .await
        }
        RecoveryScope::WorkflowTree(ids) => {
            sqlx::query_as::<_, StuckPendingRow>(TREE_CASE0_STUCK_PENDING_SQL)
                .bind(ids)
                .bind(max_rows)
                .fetch_all(pool)
                .await
        }
    };
    let rows = match rows_result {
        Ok(rows) => rows,
        Err(error) => {
            record_discovery_error(&mut report.metrics.case0, &mut report.errors, started);
            return Err(error.into());
        }
    };
    let mut metrics = RecoveryCaseMetrics {
        rows_selected: saturated_row_count(rows.len()),
        candidates_returned: saturated_row_count(rows.len()),
        ..RecoveryCaseMetrics::default()
    };

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
            Ok(false) => metrics.refusals += 1,
            Err(e) => {
                tracing::error!(
                    workflow_id = %row.workflow_id,
                    task_index = row.task_index,
                    error = %e,
                    "recovery case 0: failed to re-evaluate pending task",
                );
                report.errors += 1;
                metrics.errors += 1;
            }
        }
    }
    metrics.duration_ms = elapsed_millis(started);
    report.metrics.case0 = metrics;
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
    let started = Instant::now();
    let rows_result = match scope {
        RecoveryScope::Global => {
            sqlx::query_as::<_, ReadyTaskRow>(GLOBAL_CASE1_READY_NO_TASK_SQL)
                .bind(max_rows)
                .fetch_all(pool)
                .await
        }
        RecoveryScope::WorkflowTree(ids) => {
            sqlx::query_as::<_, ReadyTaskRow>(TREE_CASE1_READY_NO_TASK_SQL)
                .bind(ids)
                .bind(max_rows)
                .fetch_all(pool)
                .await
        }
    };
    let rows = match rows_result {
        Ok(rows) => rows,
        Err(error) => {
            record_discovery_error(&mut report.metrics.case1, &mut report.errors, started);
            return Err(error.into());
        }
    };
    let mut metrics = RecoveryCaseMetrics {
        rows_selected: saturated_row_count(rows.len()),
        candidates_returned: saturated_row_count(rows.len()),
        ..RecoveryCaseMetrics::default()
    };

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
                metrics.refusals += 1;
            }
            Err(e) => {
                tracing::error!(
                    workflow_id = %row.workflow_id,
                    task_index = row.task_index,
                    error = %e,
                    "recovery case 1: failed to re-enqueue task",
                );
                report.errors += 1;
                metrics.errors += 1;
            }
        }
    }
    metrics.duration_ms = elapsed_millis(started);
    report.metrics.case1 = metrics;
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
    let started = Instant::now();
    let rows_result = match scope {
        RecoveryScope::Global => {
            sqlx::query_as::<_, ReadySubworkflowRow>(GLOBAL_CASE1_5_READY_SUBWORKFLOW_SQL)
                .bind(max_rows)
                .fetch_all(pool)
                .await
        }
        RecoveryScope::WorkflowTree(ids) => {
            sqlx::query_as::<_, ReadySubworkflowRow>(TREE_CASE1_5_READY_SUBWORKFLOW_SQL)
                .bind(ids)
                .bind(max_rows)
                .fetch_all(pool)
                .await
        }
    };
    let rows = match rows_result {
        Ok(rows) => rows,
        Err(error) => {
            record_discovery_error(&mut report.metrics.case1_5, &mut report.errors, started);
            return Err(error.into());
        }
    };
    let mut metrics = RecoveryCaseMetrics {
        rows_selected: saturated_row_count(rows.len()),
        candidates_returned: saturated_row_count(rows.len()),
        ..RecoveryCaseMetrics::default()
    };

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
                metrics.errors += 1;
            }
        }
    }
    metrics.duration_ms = elapsed_millis(started);
    report.metrics.case1_5 = metrics;
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
    let started = Instant::now();
    let rows_result = match scope {
        RecoveryScope::Global => {
            sqlx::query_as::<_, StaleSubworkflowRow>(GLOBAL_CASE1_6_STALE_SUBWORKFLOW_SQL)
                .bind(max_rows)
                .fetch_all(pool)
                .await
        }
        RecoveryScope::WorkflowTree(ids) => {
            sqlx::query_as::<_, StaleSubworkflowRow>(TREE_CASE1_6_STALE_SUBWORKFLOW_SQL)
                .bind(ids)
                .bind(max_rows)
                .fetch_all(pool)
                .await
        }
    };
    let rows = match rows_result {
        Ok(rows) => rows,
        Err(error) => {
            record_discovery_error(&mut report.metrics.case1_6, &mut report.errors, started);
            return Err(error.into());
        }
    };
    let mut metrics = RecoveryCaseMetrics {
        rows_selected: saturated_row_count(rows.len()),
        candidates_returned: saturated_row_count(rows.len()),
        ..RecoveryCaseMetrics::default()
    };

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
                metrics.errors += 1;
            }
        }
    }
    metrics.duration_ms = elapsed_millis(started);
    report.metrics.case1_6 = metrics;
    Ok(())
}

/// Check one bounded global page for completed and orphaned workflows.
async fn recover_global_workflow_end_states(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    max_rows: Option<i64>,
    report: &mut RecoveryReport,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let started = Instant::now();
    let scan_limit = max_rows.unwrap_or(i64::MAX);
    let requested_claim_token = Uuid::new_v4();
    let audit_result = sqlx::query_as::<_, GlobalWorkflowAuditRow>(GLOBAL_WORKFLOW_AUDIT_SQL)
        .bind(scan_limit)
        .bind(requested_claim_token)
        .bind(GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS)
        .fetch_optional(pool)
        .await;
    let Some(audit) = (match audit_result {
        Ok(audit) => audit,
        Err(error) => {
            let duration_ms = elapsed_millis(started);
            report.metrics.case2_3.duration_ms = duration_ms;
            report.metrics.case2_3.errors += 1;
            report.metrics.case4.duration_ms = duration_ms;
            report.metrics.case4.errors += 1;
            report.errors += 1;
            return Err(error.into());
        }
    }) else {
        let duration_ms = elapsed_millis(started);
        report.metrics.case2_3.duration_ms = duration_ms;
        report.metrics.case2_3.refusals = 1;
        report.metrics.case4.duration_ms = duration_ms;
        report.metrics.case4.refusals = 1;
        return Ok(());
    };

    let GlobalWorkflowAuditRow {
        scanned_count,
        completion_ids,
        orphan_ids,
        orphan_names,
        claim_token,
    } = audit;
    let scanned_count = u32::try_from(scanned_count).unwrap_or(u32::MAX);
    report.metrics.case2_3.rows_selected = scanned_count;
    report.metrics.case2_3.candidates_returned =
        u32::try_from(completion_ids.len()).unwrap_or(u32::MAX);
    report.metrics.case4.rows_selected = scanned_count;
    report.metrics.case4.candidates_returned = u32::try_from(orphan_ids.len()).unwrap_or(u32::MAX);

    let mut owns_claim = claim_token.is_some();
    for workflow_id in completion_ids {
        if let Some(token) = claim_token {
            match renew_global_workflow_audit_claim(pool, token).await {
                Ok(true) => {}
                Ok(false) => {
                    owns_claim = false;
                    report.metrics.case2_3.refusals += 1;
                    break;
                }
                Err(error) => {
                    owns_claim = false;
                    tracing::error!(%error, "workflow audit claim renewal failed");
                    report.errors += 1;
                    report.metrics.case2_3.errors += 1;
                    break;
                }
            }
        } else {
            report.metrics.case2_3.refusals += 1;
            break;
        }
        match engine::check_workflow_completion_with_recovery_claim(
            pool,
            workflow_id,
            registry,
            payload,
            retention,
            claim_token.expect("candidate page has a claim token"),
        )
        .await
        {
            Ok(engine::RecoveryClaimOutcome::Held) => {
                report.case2_3_workflow_completed += 1;
                tracing::debug!(
                    workflow_id = %workflow_id,
                    "recovery case 2+3: triggered workflow completion check",
                );
            }
            Ok(engine::RecoveryClaimOutcome::Lost) => {
                owns_claim = false;
                report.metrics.case2_3.refusals += 1;
                break;
            }
            Err(e) => {
                tracing::error!(
                    workflow_id = %workflow_id,
                    error = %e,
                    "recovery case 2+3: workflow completion check failed",
                );
                report.errors += 1;
                report.metrics.case2_3.errors += 1;
            }
        }
    }

    if owns_claim {
        for (workflow_id, name) in orphan_ids.into_iter().zip(orphan_names) {
            if let Some(token) = claim_token {
                match renew_global_workflow_audit_claim(pool, token).await {
                    Ok(true) => {}
                    Ok(false) => {
                        report.metrics.case4.refusals += 1;
                        break;
                    }
                    Err(error) => {
                        tracing::error!(%error, "workflow audit claim renewal failed");
                        report.errors += 1;
                        report.metrics.case4.errors += 1;
                        break;
                    }
                }
            } else {
                report.metrics.case4.refusals += 1;
                break;
            }
            match fail_orphaned_workflow_with_claim(
                pool,
                workflow_id,
                &name,
                claim_token.expect("candidate page has a claim token"),
            )
            .await
            {
                Ok(true) => {
                    report.case4_orphaned_failed += 1;
                    tracing::warn!(
                        workflow_id = %workflow_id,
                        workflow_name = %name,
                        "recovery case 4: failed orphaned workflow (no tasks)",
                    );
                }
                Ok(false) => report.metrics.case4.refusals += 1,
                Err(e) => {
                    tracing::error!(
                        workflow_id = %workflow_id,
                        error = %e,
                        "recovery case 4: failed to mark orphaned workflow as FAILED",
                    );
                    report.errors += 1;
                    report.metrics.case4.errors += 1;
                }
            }
        }
    }
    if let Some(token) = claim_token {
        if let Err(error) = release_global_workflow_audit_claim(pool, token).await {
            tracing::error!(%error, "workflow audit claim release failed");
            report.errors += 1;
            report.metrics.case2_3.errors += 1;
            report.metrics.case4.errors += 1;
        }
    }
    let duration_ms = elapsed_millis(started);
    report.metrics.case2_3.duration_ms = duration_ms;
    report.metrics.case4.duration_ms = duration_ms;
    Ok(())
}

async fn renew_global_workflow_audit_claim(
    pool: &PgPool,
    claim_token: Uuid,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query_scalar(RENEW_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL)
        .bind(claim_token)
        .bind(GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS)
        .fetch_optional(pool)
        .await?
        .unwrap_or(false))
}

async fn release_global_workflow_audit_claim(
    pool: &PgPool,
    claim_token: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(RELEASE_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL)
        .bind(claim_token)
        .execute(pool)
        .await?;
    Ok(())
}

/// Check completion for workflows in one requested tree.
async fn recover_tree_case2_3(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    workflow_ids: &[Uuid],
    max_rows: Option<i64>,
    report: &mut RecoveryReport,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), WorkflowError> {
    let started = Instant::now();
    let rows_result = sqlx::query_as::<_, StuckWorkflowRow>(TREE_CASE2_3_STUCK_WORKFLOW_SQL)
        .bind(workflow_ids)
        .bind(max_rows)
        .fetch_all(pool)
        .await;
    let rows = match rows_result {
        Ok(rows) => rows,
        Err(error) => {
            record_discovery_error(&mut report.metrics.case2_3, &mut report.errors, started);
            return Err(error.into());
        }
    };
    report.metrics.case2_3.rows_selected = u32::try_from(rows.len()).unwrap_or(u32::MAX);
    report.metrics.case2_3.candidates_returned = report.metrics.case2_3.rows_selected;

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
                report.metrics.case2_3.errors += 1;
            }
        }
    }
    report.metrics.case2_3.duration_ms = elapsed_millis(started);
    Ok(())
}

/// Fail orphaned workflows in one requested tree.
async fn recover_tree_case4(
    pool: &PgPool,
    workflow_ids: &[Uuid],
    max_rows: Option<i64>,
    report: &mut RecoveryReport,
) -> Result<(), WorkflowError> {
    let started = Instant::now();
    let rows_result = sqlx::query_as::<_, OrphanedWorkflowRow>(TREE_CASE4_ORPHANED_WORKFLOW_SQL)
        .bind(workflow_ids)
        .bind(max_rows)
        .fetch_all(pool)
        .await;
    let rows = match rows_result {
        Ok(rows) => rows,
        Err(error) => {
            record_discovery_error(&mut report.metrics.case4, &mut report.errors, started);
            return Err(error.into());
        }
    };
    report.metrics.case4.rows_selected = u32::try_from(rows.len()).unwrap_or(u32::MAX);
    report.metrics.case4.candidates_returned = report.metrics.case4.rows_selected;

    for row in rows {
        match fail_orphaned_workflow(pool, row.workflow_id, &row.name).await {
            Ok(true) => {
                report.case4_orphaned_failed += 1;
                tracing::warn!(
                    workflow_id = %row.workflow_id,
                    workflow_name = %row.name,
                    "recovery case 4: failed orphaned workflow (no tasks)",
                );
            }
            Ok(false) => report.metrics.case4.refusals += 1,
            Err(e) => {
                tracing::error!(
                    workflow_id = %row.workflow_id,
                    error = %e,
                    "recovery case 4: failed to mark orphaned workflow as FAILED",
                );
                report.errors += 1;
                report.metrics.case4.errors += 1;
            }
        }
    }
    report.metrics.case4.duration_ms = elapsed_millis(started);
    Ok(())
}

async fn fail_orphaned_workflow(
    pool: &PgPool,
    workflow_id: Uuid,
    name: &str,
) -> Result<bool, sqlx::Error> {
    let error_str = orphaned_workflow_error(name);
    let result = sqlx::query(FAIL_ORPHANED_WORKFLOW_SQL)
        .bind(workflow_id)
        .bind(error_str)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() == 1)
}

async fn fail_orphaned_workflow_with_claim(
    pool: &PgPool,
    workflow_id: Uuid,
    name: &str,
    claim_token: Uuid,
) -> Result<bool, sqlx::Error> {
    let error_str = orphaned_workflow_error(name);
    let result = sqlx::query(FAIL_ORPHANED_WORKFLOW_WITH_CLAIM_SQL)
        .bind(workflow_id)
        .bind(error_str)
        .bind(claim_token)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() == 1)
}

fn orphaned_workflow_error(name: &str) -> String {
    let error_json = serde_json::json!({
        "error_code": "E400",
        "message": format!(
            "Orphaned workflow '{}': no workflow_tasks found. \
             Workflow was likely created but task DAG insertion failed.",
            name,
        ),
        "recovery": "case_4",
    });
    serde_json::to_string(&error_json).unwrap_or_else(|_| "{}".to_owned())
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

    fn plan_has_sequential_scan(plan: &serde_json::Value, relation: &str) -> bool {
        match plan {
            serde_json::Value::Array(values) => values
                .iter()
                .any(|value| plan_has_sequential_scan(value, relation)),
            serde_json::Value::Object(fields) => {
                let is_target_scan = fields.get("Node Type").and_then(serde_json::Value::as_str)
                    == Some("Seq Scan")
                    && fields
                        .get("Relation Name")
                        .and_then(serde_json::Value::as_str)
                        == Some(relation);
                is_target_scan
                    || fields
                        .values()
                        .any(|value| plan_has_sequential_scan(value, relation))
            }
            _ => false,
        }
    }

    fn root_shared_buffers(plan: &serde_json::Value) -> u64 {
        let root = &plan[0]["Plan"];
        ["Shared Hit Blocks", "Shared Read Blocks"]
            .into_iter()
            .map(|field| root[field].as_u64().unwrap_or(0))
            .sum()
    }

    fn relation_rows_examined(plan: &serde_json::Value, relation: &str) -> f64 {
        match plan {
            serde_json::Value::Array(values) => values
                .iter()
                .map(|value| relation_rows_examined(value, relation))
                .sum(),
            serde_json::Value::Object(fields) => {
                let current = if fields
                    .get("Relation Name")
                    .and_then(serde_json::Value::as_str)
                    == Some(relation)
                {
                    let rows = [
                        "Actual Rows",
                        "Rows Removed by Filter",
                        "Rows Removed by Index Recheck",
                    ]
                    .into_iter()
                    .map(|field| {
                        fields
                            .get(field)
                            .and_then(serde_json::Value::as_f64)
                            .unwrap_or(0.0)
                    })
                    .sum::<f64>();
                    let loops = fields
                        .get("Actual Loops")
                        .and_then(serde_json::Value::as_f64)
                        .unwrap_or(0.0);
                    rows * loops
                } else {
                    0.0
                };
                current
                    + fields
                        .values()
                        .map(|value| relation_rows_examined(value, relation))
                        .sum::<f64>()
            }
            _ => 0.0,
        }
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

    async fn insert_running_workflow_with_pending_task(
        pool: &PgPool,
        workflow_id: Uuid,
        created_at: chrono::DateTime<chrono::Utc>,
    ) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, output_task_index,
                 definition_key, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at
             ) VALUES (
                 $1, 'bounded_recovery_watermark', 'RUNNING', 'fail', NULL,
                 $2, 0, $1, $3, $3, $3, $3
             )",
        )
        .bind(workflow_id)
        .bind(format!("test.bounded-recovery.watermark.{workflow_id}"))
        .bind(created_at)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                 id, workflow_id, task_index, node_id, task_name,
                 queue_name, priority, dependencies, allow_failed_deps,
                 join_type, status, is_subworkflow, created_at
             ) VALUES (
                 $1, $2, 0, 'root', 'bounded_recovery_watermark_task',
                 'default', 100, '{}'::integer[], FALSE,
                 'all', 'PENDING', FALSE, $3
             )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .bind(created_at)
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
    async fn discovery_query_failure_keeps_case_metrics_in_the_health_snapshot() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://postgres@localhost/postgres")
            .unwrap();
        pool.close().await;
        let failure = recover_stuck_workflows_observed(
            &pool,
            &WorkflowSpecRegistry::new(),
            0,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect_err("candidate discovery failure must keep its partial report");
        assert!(matches!(&failure.error, WorkflowError::Database(_)));
        let snapshot = failure.into_health_snapshot();
        assert_eq!(snapshot["state"], "error");
        assert_eq!(snapshot["errors"], 1);
        assert_eq!(snapshot["metrics"]["case0"]["rows_selected"], 0);
        assert_eq!(snapshot["metrics"]["case0"]["candidates_returned"], 0);
        assert_eq!(snapshot["metrics"]["case0"]["errors"], 1);
        assert!(snapshot["metrics"]["case0"]["duration_ms"].is_u64());
    }

    #[tokio::test]
    async fn shared_workflow_audit_failure_marks_both_case_metrics() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://postgres@localhost/postgres")
            .unwrap();
        pool.close().await;
        let mut report = RecoveryReport::default();
        let error = recover_global_workflow_end_states(
            &pool,
            &WorkflowSpecRegistry::new(),
            Some(200),
            &mut report,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect_err("shared audit query must fail on a closed pool");
        assert!(matches!(error, WorkflowError::Database(_)));
        assert_eq!(report.errors, 1);
        assert_eq!(report.metrics.case2_3.errors, 1);
        assert_eq!(report.metrics.case4.errors, 1);
        assert_eq!(report.metrics.case2_3.rows_selected, 0);
        assert_eq!(report.metrics.case4.candidates_returned, 0);
    }

    #[tokio::test]
    #[serial]
    async fn case0_advances_the_selected_root_without_touching_unready_siblings() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
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
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
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
        assert_eq!(report.case4_orphaned_failed, 2, "{report:?}");
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
    async fn empty_global_workflow_audit_is_bounded_at_fifty_thousand_rows() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE horsies_recovery_scan_cursors
             SET last_created_at = NULL, last_id = NULL,
                 cycle_upper_created_at = NULL, cycle_upper_id = NULL,
                 claim_token = NULL, claim_expires_at = NULL,
                 completed_cycles = 0,
                 last_scan_rows = 0, last_candidate_rows = 0,
                 last_scan_at = NULL
             WHERE scan_name = 'running_workflows'",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "WITH generated AS (
                 SELECT g,
                        ('10000000-0000-7000-8000-' ||
                         lpad(to_hex(g), 12, '0'))::uuid AS workflow_id
                 FROM generate_series(1, 50000) AS g
             )
             INSERT INTO horsies_workflows (
                 id, name, status, on_error, output_task_index,
                 definition_key, depth, root_workflow_id,
                 sent_at, created_at, started_at, completed_at, updated_at
             )
             SELECT workflow_id, 'bounded_recovery_stable_' || g,
                    CASE WHEN g % 4 = 0 THEN 'RUNNING' ELSE 'COMPLETED' END,
                    'fail', NULL,
                    'test.bounded-recovery.stable.' || g, 0, workflow_id,
                    NOW(), NOW(), NOW(),
                    CASE WHEN g % 4 = 0 THEN NULL ELSE NOW() END,
                    NOW()
             FROM generated",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "WITH generated AS (
                 SELECT g,
                        ('10000000-0000-7000-8000-' ||
                         lpad(to_hex(g), 12, '0'))::uuid AS workflow_id,
                        ('20000000-0000-7000-8000-' ||
                         lpad(to_hex(g), 12, '0'))::uuid AS node_id
                 FROM generate_series(1, 50000) AS g
             )
             INSERT INTO horsies_workflow_tasks (
                 id, workflow_id, task_index, node_id, task_name,
                 queue_name, priority, dependencies, allow_failed_deps,
                 join_type, status, is_subworkflow, created_at
             )
             SELECT node_id, workflow_id, 0, 'root', 'bounded_recovery_stable_task',
                    'default', 100, '{}'::integer[], FALSE,
                    'all',
                    CASE WHEN g % 4 = 0 THEN 'PENDING' ELSE 'COMPLETED' END,
                    FALSE, NOW()
             FROM generated",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("ANALYZE horsies_workflows, horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();

        let mut report = RecoveryReport::default();
        recover_global_workflow_end_states(
            &pool,
            &WorkflowSpecRegistry::new(),
            Some(200),
            &mut report,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.metrics.case2_3.rows_selected, 200);
        assert_eq!(report.metrics.case2_3.candidates_returned, 0);
        assert_eq!(report.metrics.case4.rows_selected, 200);
        assert_eq!(report.metrics.case4.candidates_returned, 0);

        let mut explain_transaction = pool.begin().await.unwrap();
        let explain =
            format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {GLOBAL_WORKFLOW_AUDIT_SQL}");
        let plan: serde_json::Value = sqlx::query_scalar(&explain)
            .bind(200_i64)
            .bind(Uuid::new_v4())
            .bind(GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS)
            .fetch_one(explain_transaction.as_mut())
            .await
            .unwrap();
        let rendered = plan.to_string();
        assert!(
            rendered.contains("idx_horsies_workflows_running_recovery_scan"),
            "bounded workflow audit must use the running-workflow scan index: {plan}",
        );
        assert!(
            rendered.contains("idx_horsies_workflow_tasks_workflow")
                || rendered.contains("uq_horsies_workflow_task_index"),
            "bounded workflow audit must use workflow-task index probes: {plan}",
        );
        assert!(
            !plan_has_sequential_scan(&plan, "horsies_workflows")
                && !plan_has_sequential_scan(&plan, "horsies_workflow_tasks"),
            "bounded workflow audit must not scan complete workflow tables: {plan}",
        );
        assert!(
            root_shared_buffers(&plan) <= 10_000,
            "bounded workflow audit must stay within its physical buffer budget: {plan}",
        );
        assert!(
            relation_rows_examined(&plan, "horsies_workflows") <= 201.0,
            "bounded workflow audit must examine no more than one workflow page and its upper bound: {plan}",
        );
        assert!(
            relation_rows_examined(&plan, "horsies_workflow_tasks") <= 400.0,
            "bounded workflow audit must use at most two task probes per workflow: {plan}",
        );
        explain_transaction.rollback().await.unwrap();

        let selected_workflow = Uuid::parse_str("10000000-0000-7000-8000-000000000004").unwrap();
        for tree_sql in [
            TREE_CASE2_3_STUCK_WORKFLOW_SQL,
            TREE_CASE4_ORPHANED_WORKFLOW_SQL,
        ] {
            let explain = format!("EXPLAIN (FORMAT JSON) {tree_sql}");
            let tree_plan: serde_json::Value = sqlx::query_scalar(&explain)
                .bind(vec![selected_workflow])
                .bind(200_i64)
                .fetch_one(&pool)
                .await
                .unwrap();
            let rendered = tree_plan.to_string();
            assert!(
                rendered.contains("horsies_workflows_pkey")
                    || rendered.contains("idx_horsies_workflows_running_recovery_scan"),
                "workflow-tree recovery must use an exact workflow index: {tree_plan}",
            );
            assert!(
                rendered.contains("idx_horsies_workflow_tasks_workflow")
                    || rendered.contains("uq_horsies_workflow_task_index"),
                "workflow-tree recovery must use workflow-task index probes: {tree_plan}",
            );
            assert!(
                !plan_has_sequential_scan(&tree_plan, "horsies_workflows")
                    && !plan_has_sequential_scan(&tree_plan, "horsies_workflow_tasks"),
                "workflow-tree recovery must not scan complete workflow tables: {tree_plan}",
            );
        }

        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("ANALYZE horsies_workflows, horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn global_workflow_cursor_reaches_a_completion_after_a_stable_page() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE horsies_recovery_scan_cursors
             SET last_created_at = NULL, last_id = NULL,
                 cycle_upper_created_at = NULL, cycle_upper_id = NULL,
                 claim_token = NULL, claim_expires_at = NULL,
                 completed_cycles = 0,
                 last_scan_rows = 0, last_candidate_rows = 0,
                 last_scan_at = NULL
             WHERE scan_name = 'running_workflows'",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "WITH generated AS (
                 SELECT g,
                        ('60000000-0000-7000-8000-' ||
                         lpad(to_hex(g), 12, '0'))::uuid AS workflow_id
                 FROM generate_series(1, 201) AS g
             )
             INSERT INTO horsies_workflows (
                 id, name, status, on_error, output_task_index,
                 definition_key, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at
             )
             SELECT workflow_id, 'bounded_recovery_cursor_' || g, 'RUNNING', 'fail', NULL,
                    'test.bounded-recovery.cursor.' || g, 0, workflow_id,
                    NOW(), NOW(), NOW(), NOW()
             FROM generated",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "WITH generated AS (
                 SELECT g,
                        ('60000000-0000-7000-8000-' ||
                         lpad(to_hex(g), 12, '0'))::uuid AS workflow_id,
                        ('61000000-0000-7000-8000-' ||
                         lpad(to_hex(g), 12, '0'))::uuid AS node_id
                 FROM generate_series(1, 201) AS g
             )
             INSERT INTO horsies_workflow_tasks (
                 id, workflow_id, task_index, node_id, task_name,
                 queue_name, priority, dependencies, allow_failed_deps,
                 join_type, status, is_subworkflow, completed_at, created_at
             )
             SELECT node_id, workflow_id, 0, 'root', 'bounded_recovery_cursor_task',
                    'default', 100, '{}'::integer[], FALSE, 'all',
                    CASE WHEN g = 201 THEN 'COMPLETED' ELSE 'PENDING' END,
                    FALSE,
                    CASE WHEN g = 201 THEN NOW() ELSE NULL END,
                    NOW()
             FROM generated",
        )
        .execute(&pool)
        .await
        .unwrap();

        let registry = WorkflowSpecRegistry::new();
        let mut first = RecoveryReport::default();
        recover_global_workflow_end_states(
            &pool,
            &registry,
            Some(200),
            &mut first,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(first.metrics.case2_3.candidates_returned, 0);

        let mut second = RecoveryReport::default();
        recover_global_workflow_end_states(
            &pool,
            &registry,
            Some(200),
            &mut second,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(second.metrics.case2_3.candidates_returned, 1);
        let completed_id = Uuid::parse_str("60000000-0000-7000-8000-0000000000c9").unwrap();
        let status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(completed_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "COMPLETED");

        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn global_cycle_watermark_revisits_old_rows_while_new_rows_arrive() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE horsies_recovery_scan_cursors
             SET last_created_at = NULL, last_id = NULL,
                 cycle_upper_created_at = NULL, cycle_upper_id = NULL,
                 claim_token = NULL, claim_expires_at = NULL,
                 completed_cycles = 0, last_scan_rows = 0,
                 last_candidate_rows = 0, last_scan_at = NULL
             WHERE scan_name = 'running_workflows'",
        )
        .execute(&pool)
        .await
        .unwrap();

        let base = chrono::Utc::now() - chrono::Duration::hours(1);
        let stable_ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        for (offset, workflow_id) in stable_ids.into_iter().enumerate() {
            insert_running_workflow_with_pending_task(
                &pool,
                workflow_id,
                base + chrono::Duration::seconds(i64::try_from(offset).unwrap()),
            )
            .await;
        }

        let registry = WorkflowSpecRegistry::new();
        let mut first = RecoveryReport::default();
        recover_global_workflow_end_states(
            &pool,
            &registry,
            Some(2),
            &mut first,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(first.metrics.case2_3.rows_selected, 2);

        sqlx::query(
            "UPDATE horsies_workflow_tasks
             SET status = 'COMPLETED', completed_at = NOW()
             WHERE workflow_id = $1",
        )
        .bind(stable_ids[0])
        .execute(&pool)
        .await
        .unwrap();
        for offset in 0..2_i64 {
            insert_running_workflow_with_pending_task(
                &pool,
                Uuid::new_v4(),
                base + chrono::Duration::minutes(10) + chrono::Duration::seconds(offset),
            )
            .await;
        }

        let mut second = RecoveryReport::default();
        recover_global_workflow_end_states(
            &pool,
            &registry,
            Some(2),
            &mut second,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(second.metrics.case2_3.rows_selected, 1);
        assert_eq!(second.case2_3_workflow_completed, 0);

        for offset in 0..2_i64 {
            insert_running_workflow_with_pending_task(
                &pool,
                Uuid::new_v4(),
                base + chrono::Duration::minutes(20) + chrono::Duration::seconds(offset),
            )
            .await;
        }
        let mut third = RecoveryReport::default();
        recover_global_workflow_end_states(
            &pool,
            &registry,
            Some(2),
            &mut third,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(third.case2_3_workflow_completed, 1);
        let status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(stable_ids[0])
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "COMPLETED");

        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn global_workflow_claim_refuses_a_second_caller_until_the_owner_expires() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE horsies_recovery_scan_cursors
             SET last_created_at = NULL, last_id = NULL,
                 cycle_upper_created_at = NULL, cycle_upper_id = NULL,
                 claim_token = NULL, claim_expires_at = NULL,
                 completed_cycles = 0,
                 last_scan_rows = 0, last_candidate_rows = 0,
                 last_scan_at = NULL
             WHERE scan_name = 'running_workflows'",
        )
        .execute(&pool)
        .await
        .unwrap();

        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, output_task_index,
                 definition_key, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at
             ) VALUES (
                 $1, 'bounded_recovery_retry', 'RUNNING', 'fail', NULL,
                 $2, 0, $1, NOW(), NOW(), NOW(), NOW()
             )",
        )
        .bind(workflow_id)
        .bind(format!("test.bounded-recovery.retry.{workflow_id}"))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                 id, workflow_id, task_index, node_id, task_name,
                 queue_name, priority, dependencies, allow_failed_deps,
                 join_type, status, is_subworkflow, completed_at, created_at
             ) VALUES (
                 $1, $2, 0, 'root', 'bounded_recovery_retry_task',
                 'default', 100, '{}'::integer[], FALSE,
                 'all', 'COMPLETED', FALSE, NOW(), NOW()
             )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .execute(&pool)
        .await
        .unwrap();

        let abandoned = sqlx::query_as::<_, GlobalWorkflowAuditRow>(GLOBAL_WORKFLOW_AUDIT_SQL)
            .bind(200_i64)
            .bind(Uuid::new_v4())
            .bind(GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(abandoned.completion_ids, vec![workflow_id]);
        assert!(abandoned.claim_token.is_some());
        let status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "RUNNING");

        let mut report = RecoveryReport::default();
        recover_global_workflow_end_states(
            &pool,
            &WorkflowSpecRegistry::new(),
            Some(200),
            &mut report,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.metrics.case2_3.refusals, 1);
        assert_eq!(report.case2_3_workflow_completed, 0);

        sqlx::query(
            "UPDATE horsies_recovery_scan_cursors
             SET claim_expires_at = statement_timestamp() - interval '1 second'
             WHERE scan_name = 'running_workflows'",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut report = RecoveryReport::default();
        recover_global_workflow_end_states(
            &pool,
            &WorkflowSpecRegistry::new(),
            Some(200),
            &mut report,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.case2_3_workflow_completed, 1);
        let status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "COMPLETED");

        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn workflow_action_fences_the_claim_after_lease_expiry() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE horsies_recovery_scan_cursors
             SET last_created_at = NULL, last_id = NULL,
                 cycle_upper_created_at = NULL, cycle_upper_id = NULL,
                 claim_token = NULL, claim_expires_at = NULL,
                 completed_cycles = 0, last_scan_rows = 0,
                 last_candidate_rows = 0, last_scan_at = NULL
             WHERE scan_name = 'running_workflows'",
        )
        .execute(&pool)
        .await
        .unwrap();

        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, output_task_index,
                 definition_key, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at
             ) VALUES (
                 $1, 'bounded_recovery_fenced', 'RUNNING', 'fail', NULL,
                 $2, 0, $1, NOW(), NOW(), NOW(), NOW()
             )",
        )
        .bind(workflow_id)
        .bind(format!("test.bounded-recovery.fenced.{workflow_id}"))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                 id, workflow_id, task_index, node_id, task_name,
                 queue_name, priority, dependencies, allow_failed_deps,
                 join_type, status, is_subworkflow, completed_at, created_at
             ) VALUES (
                 $1, $2, 0, 'root', 'bounded_recovery_fenced_task',
                 'default', 100, '{}'::integer[], FALSE,
                 'all', 'COMPLETED', FALSE, NOW(), NOW()
             )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .execute(&pool)
        .await
        .unwrap();

        let audit = sqlx::query_as::<_, GlobalWorkflowAuditRow>(GLOBAL_WORKFLOW_AUDIT_SQL)
            .bind(200_i64)
            .bind(Uuid::new_v4())
            .bind(500_i64)
            .fetch_one(&pool)
            .await
            .unwrap();
        let claim_token = audit.claim_token.expect("candidate page claim");

        let mut workflow_lock = pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM horsies_workflows WHERE id = $1 FOR UPDATE")
            .bind(workflow_id)
            .execute(workflow_lock.as_mut())
            .await
            .unwrap();
        let action_pool = pool.clone();
        let action = tokio::spawn(async move {
            engine::check_workflow_completion_with_recovery_claim(
                &action_pool,
                workflow_id,
                &WorkflowSpecRegistry::new(),
                &PayloadPolicy::default(),
                &RetentionConfig::default(),
                claim_token,
            )
            .await
        });

        let mut action_is_waiting = false;
        for _ in 0..100 {
            action_is_waiting = sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                     WHERE datname = current_database()
                       AND pid <> pg_backend_pid()
                       AND wait_event_type = 'Lock'
                       AND query LIKE 'SELECT id FROM horsies_workflows%FOR UPDATE%'
                 )",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if action_is_waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            action_is_waiting,
            "candidate action must reach the workflow lock"
        );
        tokio::time::sleep(std::time::Duration::from_millis(550)).await;
        let lease_expired: bool = sqlx::query_scalar(
            "SELECT claim_expires_at <= statement_timestamp()
             FROM horsies_recovery_scan_cursors
             WHERE scan_name = 'running_workflows'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(lease_expired);

        let mut second = RecoveryReport::default();
        recover_global_workflow_end_states(
            &pool,
            &WorkflowSpecRegistry::new(),
            Some(200),
            &mut second,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(second.metrics.case2_3.refusals, 1);
        assert_eq!(second.case2_3_workflow_completed, 0);

        workflow_lock.rollback().await.unwrap();
        assert_eq!(
            action.await.unwrap().unwrap(),
            engine::RecoveryClaimOutcome::Held
        );
        let status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "COMPLETED");

        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn busy_global_workflow_cursor_refuses_without_waiting() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let mut holder = pool.begin().await.unwrap();
        sqlx::query(
            "SELECT last_id FROM horsies_recovery_scan_cursors
             WHERE scan_name = 'running_workflows' FOR UPDATE",
        )
        .execute(holder.as_mut())
        .await
        .unwrap();

        let mut report = RecoveryReport::default();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            recover_global_workflow_end_states(
                &pool,
                &WorkflowSpecRegistry::new(),
                Some(200),
                &mut report,
                &PayloadPolicy::default(),
                &RetentionConfig::default(),
            ),
        )
        .await
        .expect("busy cursor must not wait")
        .unwrap();
        assert_eq!(report.metrics.case2_3.refusals, 1);
        assert_eq!(report.metrics.case4.refusals, 1);
        holder.rollback().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn workflow_tree_recovery_does_not_mutate_an_unrelated_orphan() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let selected = Uuid::new_v4();
        let unrelated = Uuid::new_v4();
        insert_orphaned_workflow(&pool, selected).await;
        insert_orphaned_workflow(&pool, unrelated).await;

        let report = recover_stuck_workflow_tree(
            &pool,
            &WorkflowSpecRegistry::new(),
            selected,
            0,
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.case4_orphaned_failed, 1);
        let statuses: Vec<(Uuid, String)> = sqlx::query_as(
            "SELECT id, status FROM horsies_workflows
             WHERE id = ANY($1) ORDER BY id",
        )
        .bind(vec![selected, unrelated])
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(statuses
            .iter()
            .any(|row| row.0 == selected && row.1 == "FAILED"));
        assert!(statuses
            .iter()
            .any(|row| row.0 == unrelated && row.1 == "RUNNING"));
        sqlx::query("DELETE FROM horsies_workflows WHERE id = ANY($1)")
            .bind(vec![selected, unrelated])
            .execute(&pool)
            .await
            .unwrap();
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

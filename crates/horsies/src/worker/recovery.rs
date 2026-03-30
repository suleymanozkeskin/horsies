use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::core::config::recovery::RecoveryConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::task::retry_utils::check_retry_eligibility;
use crate::core::{OperationalErrorCode, TaskError, TaskResult};

use crate::worker::retry::calculate_retry_delay;

/// SQL: Phase 1 scan — find stale RUNNING task IDs (no row locks).
/// Releases immediately so workers can finalize between scan and per-task lock.
const FIND_STALE_RUNNING_IDS_SQL: &str = "\
SELECT t.id
FROM horsies_tasks t
LEFT JOIN LATERAL (
    SELECT sent_at AS last_heartbeat
    FROM horsies_heartbeats
    WHERE task_id = t.id AND role = 'runner'
    ORDER BY sent_at DESC
    LIMIT 1
) hb ON TRUE
WHERE t.status = 'RUNNING'
  AND t.started_at IS NOT NULL
  AND COALESCE(hb.last_heartbeat, t.started_at) < NOW() - $1 * INTERVAL '1 second'";

/// SQL: Phase 2 per-task — re-acquire row with full context for retry eligibility.
///
/// Returns no rows if:
/// - the task is no longer RUNNING (worker finalized it between phases), OR
/// - a fresh runner heartbeat arrived after Phase 1 (closes the scan race).
///
/// The `NOT EXISTS` subquery re-checks heartbeat freshness using the same
/// stale threshold ($2) as Phase 1, ensuring a heartbeat that lands between
/// Phase 1 and Phase 2 saves the task from being falsely crashed.
const SELECT_STALE_TASK_FOR_UPDATE_SQL: &str = "\
SELECT
    t.id, t.retry_count, t.worker_pid, t.worker_hostname,
    t.claimed_by_worker_id, t.started_at, t.worker_process_name,
    t.max_retries, t.task_options, t.good_until, t.queue_name,
    clock_timestamp() AS db_now
FROM horsies_tasks t
WHERE t.id = $1 AND t.status = 'RUNNING'
  AND NOT EXISTS (
    SELECT 1 FROM horsies_heartbeats
    WHERE task_id = t.id AND role = 'runner'
      AND sent_at > NOW() - $2 * INTERVAL '1 second'
  )
FOR UPDATE OF t";

use crate::broker::UPSERT_TASK_ATTEMPT_SQL;

/// SQL: Requeue a stale RUNNING task for retry (clears all claim fields).
const SCHEDULE_STALE_TASK_RETRY_SQL: &str = "\
UPDATE horsies_tasks
SET status = 'PENDING',
    retry_count = $2,
    next_retry_at = $3,
    enqueued_at = $3,
    error_code = NULL,
    claimed = FALSE,
    claimed_at = NULL,
    claimed_by_worker_id = NULL,
    claim_expires_at = NULL,
    updated_at = NOW()
WHERE id = $1
  AND status = 'RUNNING'
  AND (good_until IS NULL OR $3 < good_until)";

/// SQL: Mark a single stale task as FAILED with a structured result payload.
const FAIL_SINGLE_STALE_SQL: &str = "\
UPDATE horsies_tasks
SET status = 'FAILED',
    failed_at = NOW(),
    failed_reason = $2,
    result = $3,
    error_code = $4,
    updated_at = NOW()
WHERE id = $1
AND status = 'RUNNING'";

/// Row from Phase 1 scan — just the task ID.
#[derive(Debug, FromRow)]
struct StaleTaskId {
    id: String,
}

/// Row from Phase 2 per-task FOR UPDATE — full context for retry eligibility.
#[derive(Debug, FromRow)]
struct StaleTaskContext {
    retry_count: i32,
    worker_pid: Option<i32>,
    worker_hostname: Option<String>,
    claimed_by_worker_id: Option<String>,
    started_at: Option<DateTime<Utc>>,
    worker_process_name: Option<String>,
    max_retries: i32,
    task_options: Option<String>,
    good_until: Option<DateTime<Utc>>,
    queue_name: String,
    db_now: DateTime<Utc>,
}

/// SQL: Requeue stale CLAIMED tasks back to PENDING.
///
/// A task is stale if:
/// - it has a lease (`claim_expires_at`) and the lease has expired, OR
/// - it has no lease and `claimed_at` is older than the threshold.
///
/// We intentionally ignore claimer heartbeats for CLAIMED tasks without a lease.
/// Otherwise a worker can keep a task CLAIMED forever even if it never starts.
const REQUEUE_STALE_CLAIMED_SQL: &str = "\
WITH stale AS (
    SELECT t.id
    FROM horsies_tasks t
    WHERE t.status = 'CLAIMED'
      AND (
        (t.claim_expires_at IS NOT NULL AND t.claim_expires_at < NOW())
        OR (t.claim_expires_at IS NULL
            AND t.claimed_at < NOW() - $1 * INTERVAL '1 second')
      )
    FOR UPDATE OF t SKIP LOCKED
)
UPDATE horsies_tasks
SET status = 'PENDING',
    claimed = FALSE,
    claimed_at = NULL,
    claimed_by_worker_id = NULL,
    claim_expires_at = NULL,
    updated_at = NOW()
FROM stale
WHERE horsies_tasks.id = stale.id";

// ---------------------------------------------------------------------------
// Retention cleanup SQL
// ---------------------------------------------------------------------------

const DELETE_EXPIRED_HEARTBEATS_SQL: &str = "\
DELETE FROM horsies_heartbeats
WHERE sent_at < NOW() - CAST($1 || ' hours' AS INTERVAL)";

const DELETE_EXPIRED_WORKER_STATES_SQL: &str = "\
DELETE FROM horsies_worker_states
WHERE snapshot_at < NOW() - CAST($1 || ' hours' AS INTERVAL)";

const DELETE_EXPIRED_WORKFLOW_TASKS_SQL: &str = "\
DELETE FROM horsies_workflow_tasks wt
USING horsies_workflows w
WHERE wt.workflow_id = w.id
  AND w.status IN ('COMPLETED', 'FAILED', 'CANCELLED')
  AND COALESCE(w.completed_at, w.updated_at, w.created_at) < NOW() - CAST($1 || ' hours' AS INTERVAL)";

const DELETE_EXPIRED_WORKFLOWS_SQL: &str = "\
DELETE FROM horsies_workflows
WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')
  AND COALESCE(completed_at, updated_at, created_at) < NOW() - CAST($1 || ' hours' AS INTERVAL)";

const DELETE_EXPIRED_TASKS_SQL: &str = "\
DELETE FROM horsies_tasks t
WHERE t.status IN ('COMPLETED', 'FAILED', 'CANCELLED')
  AND COALESCE(t.completed_at, t.failed_at, t.updated_at, t.created_at) < NOW() - CAST($1 || ' hours' AS INTERVAL)
  AND NOT EXISTS (
      SELECT 1
      FROM horsies_workflow_tasks wt
      JOIN horsies_workflows w ON w.id = wt.workflow_id
      WHERE wt.task_id = t.id
        AND w.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
  )";

/// Retention cleanup interval (1 hour), matching Python's `_RETENTION_CLEANUP_INTERVAL_S`.
const RETENTION_CLEANUP_INTERVAL: Duration = Duration::from_secs(3600);

/// Spawn the reaper loop for stale task recovery.
///
/// Periodically checks for stale RUNNING and CLAIMED tasks, marking them
/// as FAILED or requeuing them respectively.
#[allow(clippy::needless_pass_by_value)]
pub fn spawn_reaper(
    pool: PgPool,
    config: RecoveryConfig,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let check_interval = Duration::from_millis(config.check_interval_ms);
        let mut next_retention_cleanup = tokio::time::Instant::now() + RETENTION_CLEANUP_INTERVAL;

        tracing::info!(
            auto_requeue_claimed = config.auto_requeue_stale_claimed,
            auto_fail_running = config.auto_fail_stale_running,
            check_interval_ms = config.check_interval_ms,
            "reaper started",
        );

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(check_interval) => {
                    if config.auto_fail_stale_running {
                        let threshold_secs =
                            config.running_stale_threshold_ms as f64 / 1000.0;
                        match mark_stale_running_as_failed(&pool, threshold_secs).await {
                            Ok(count) if count > 0 => {
                                tracing::warn!(count, "reaper marked stale RUNNING tasks as FAILED");
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "reaper: failed to mark stale running tasks");
                            }
                            _ => {}
                        }
                    }

                    // Expire unclaimed PENDING tasks whose good_until has passed.
                    match expire_pending_tasks(&pool).await {
                        Ok(count) if count > 0 => {
                            tracing::info!(count, "reaper expired unclaimed PENDING tasks");
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "reaper: failed to expire pending tasks");
                        }
                        _ => {}
                    }

                    if config.auto_requeue_stale_claimed {
                        let threshold_secs =
                            config.claimed_stale_threshold_ms as f64 / 1000.0;
                        match requeue_stale_claimed(&pool, threshold_secs).await {
                            Ok(count) if count > 0 => {
                                tracing::info!(count, "reaper requeued stale CLAIMED tasks");
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "reaper: failed to requeue stale claimed tasks");
                            }
                            _ => {}
                        }
                    }

                    // Retention cleanup (runs every RETENTION_CLEANUP_INTERVAL).
                    if tokio::time::Instant::now() >= next_retention_cleanup {
                        run_retention_cleanup(&pool, &config).await;
                        next_retention_cleanup = tokio::time::Instant::now() + RETENTION_CLEANUP_INTERVAL;
                    }
                }
            }
        }
    })
}

/// Recover stale RUNNING tasks: retry if eligible, otherwise mark FAILED.
///
/// Two-phase approach matching Python's `mark_stale_tasks_as_failed`:
/// - Phase 1 (scan): Find stale task IDs without holding row locks.
/// - Phase 2 (per-task): For each candidate, re-acquire with SELECT FOR UPDATE.
///   If the task is no longer RUNNING (worker finalized it), skip.
///   If retry-eligible, requeue to PENDING. Otherwise, mark FAILED with
///   a structured WORKER_CRASHED result.
///
/// Each task commits independently (partial progress is durable).
/// Returns the number of tasks processed (retried or failed).
pub async fn mark_stale_running_as_failed(
    pool: &PgPool,
    threshold_secs: f64,
) -> Result<u64, sqlx::Error> {
    // Phase 1: Scan for stale task IDs (no row locks).
    let stale_ids: Vec<StaleTaskId> = sqlx::query_as(FIND_STALE_RUNNING_IDS_SQL)
        .bind(threshold_secs)
        .fetch_all(pool)
        .await?;

    if stale_ids.is_empty() {
        return Ok(0);
    }

    let threshold_ms = (threshold_secs * 1000.0) as u64;
    let error_code_str = OperationalErrorCode::WorkerCrashed.to_string();
    let mut count: u64 = 0;

    // Phase 2: Process each task independently.
    for stale in &stale_ids {
        let result = process_single_stale_task(
            pool,
            &stale.id,
            threshold_secs,
            threshold_ms,
            &error_code_str,
        )
        .await;

        match result {
            Ok(true) => count += 1,
            Ok(false) => {
                // Task no longer RUNNING — worker finalized between scan and lock.
                tracing::debug!(task_id = %stale.id, "stale task already finalized, skipping");
            }
            Err(e) => {
                tracing::error!(task_id = %stale.id, error = %e, "failed to process stale task");
            }
        }
    }

    Ok(count)
}

/// Process a single stale task: retry or fail.
/// Returns `Ok(true)` if processed, `Ok(false)` if skipped (no longer RUNNING).
async fn process_single_stale_task(
    pool: &PgPool,
    task_id: &str,
    threshold_secs: f64,
    threshold_ms: u64,
    error_code_str: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Re-acquire row with full context. Returns None if the task is no longer
    // RUNNING or if a fresh heartbeat arrived after the Phase 1 scan.
    let ctx: Option<StaleTaskContext> = sqlx::query_as(SELECT_STALE_TASK_FOR_UPDATE_SQL)
        .bind(task_id)
        .bind(threshold_secs)
        .fetch_optional(&mut *tx)
        .await?;

    let Some(row) = ctx else {
        // Task already finalized by worker — skip.
        tx.rollback().await?;
        return Ok(false);
    };

    let detected_at = row.db_now;
    let attempt_num = row.retry_count + 1;
    let attempt_started = row.started_at.unwrap_or(detected_at);

    let failed_reason = format!(
        "Worker process crashed (no runner heartbeat for {}ms = {:.1}s)",
        threshold_ms, threshold_secs,
    );

    // Check retry eligibility using fresh DB timestamp.
    let eligible = check_retry_eligibility(
        row.retry_count,
        row.max_retries,
        row.task_options.as_deref(),
        error_code_str,
        row.good_until,
        detected_at,
    );

    if eligible {
        // Retry path: attempt requeue, fall through to fail if good_until blocks.
        let new_count = row.retry_count + 1;
        let delay = calculate_retry_delay(new_count as u32, row.task_options.as_deref());
        let next_retry_at = detected_at + chrono::Duration::milliseconds((delay * 1000.0) as i64);

        let schedule_result = sqlx::query(SCHEDULE_STALE_TASK_RETRY_SQL)
            .bind(task_id)
            .bind(new_count)
            .bind(next_retry_at)
            .execute(&mut *tx)
            .await?;

        if schedule_result.rows_affected() > 0 {
            // Retry scheduled — record attempt with will_retry=true.
            sqlx::query(UPSERT_TASK_ATTEMPT_SQL)
                .bind(task_id)
                .bind(attempt_num)
                .bind("WORKER_FAILURE")
                .bind(true) // will_retry
                .bind(attempt_started)
                .bind(detected_at)
                .bind(Some(error_code_str))
                .bind(Some(&failed_reason))
                .bind(Some(&failed_reason))
                .bind(row.claimed_by_worker_id.as_deref())
                .bind(row.worker_hostname.as_deref())
                .bind(row.worker_pid)
                .bind(row.worker_process_name.as_deref())
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            tracing::info!(
                task_id,
                retry_count = new_count,
                next_retry_at = %next_retry_at,
                "stale RUNNING task scheduled for retry",
            );

            // Best-effort NOTIFY to wake workers.
            let _ = sqlx::query(&format!(
                "SELECT pg_notify('task_queue_{}', $1)",
                row.queue_name,
            ))
            .bind(task_id)
            .execute(pool)
            .await;

            return Ok(true);
        }

        // good_until guard blocked the retry — fall through to fail path.
        tracing::info!(
            task_id,
            "stale task retry blocked by good_until, falling through to fail",
        );
    }

    {
        // Failure path: upsert attempt (will_retry=false), mark FAILED.
        let task_error = TaskError {
            error_code: Some(OperationalErrorCode::WorkerCrashed.into()),
            message: Some(failed_reason.clone()),
            cause: None,
            data: Some(serde_json::json!({
                "stale_threshold_ms": threshold_ms,
                "stale_threshold_seconds": threshold_secs,
                "worker_pid": row.worker_pid,
                "worker_hostname": row.worker_hostname,
                "worker_id": row.claimed_by_worker_id,
                "started_at": row.started_at.map(|dt| dt.to_rfc3339()),
                "detected_at": detected_at.to_rfc3339(),
            })),
        };

        let task_result: TaskResult<()> = TaskResult::Err(task_error);
        let result_json = serde_json::to_string(&task_result).unwrap_or_else(|e| {
            tracing::error!(task_id, error = %e, "failed to serialize stale task result");
            r#"{"__type":"err","value":{"message":"serialization failed"}}"#.to_owned()
        });

        sqlx::query(UPSERT_TASK_ATTEMPT_SQL)
            .bind(task_id)
            .bind(attempt_num)
            .bind("WORKER_FAILURE")
            .bind(false) // will_retry
            .bind(attempt_started)
            .bind(detected_at)
            .bind(Some(error_code_str))
            .bind(Some(&failed_reason))
            .bind(Some(&failed_reason))
            .bind(row.claimed_by_worker_id.as_deref())
            .bind(row.worker_hostname.as_deref())
            .bind(row.worker_pid)
            .bind(row.worker_process_name.as_deref())
            .execute(&mut *tx)
            .await?;

        sqlx::query(FAIL_SINGLE_STALE_SQL)
            .bind(task_id)
            .bind(&failed_reason)
            .bind(&result_json)
            .bind(error_code_str)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        tracing::info!(task_id, "stale RUNNING task marked FAILED");
    }

    Ok(true)
}

/// Expire unclaimed PENDING tasks whose `good_until` has passed.
///
/// Transitions matching tasks to EXPIRED with a TASK_EXPIRED result.
/// No attempt rows are written (the task was never executed).
pub async fn expire_pending_tasks(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let task_error = TaskError::builtin(
        crate::core::OutcomeCode::TaskExpired,
        "task expired before being claimed (good_until passed)",
    );
    let task_result = TaskResult::<()>::Err(task_error);
    let result_json = serde_json::to_string(&task_result)
        .unwrap_or_else(|_| r#"{"__type":"err","value":{"message":"expired"}}"#.to_owned());

    let result = sqlx::query(
        "UPDATE horsies_tasks \
         SET status = 'EXPIRED', \
             failed_at = NOW(), \
             result = $1, \
             error_code = 'TASK_EXPIRED', \
             updated_at = NOW() \
         WHERE status = 'PENDING' \
           AND good_until IS NOT NULL \
           AND good_until < NOW()",
    )
    .bind(&result_json)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

/// Requeue stale CLAIMED tasks back to PENDING. Returns the number of affected rows.
pub async fn requeue_stale_claimed(pool: &PgPool, threshold_secs: f64) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(REQUEUE_STALE_CLAIMED_SQL)
        .bind(threshold_secs)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Run retention cleanup: prune old heartbeats, worker states, and terminal records.
///
/// Matches Python's retention cleanup logic in the worker's reaper loop.
/// Each category is gated by its config (None = disabled).
/// Order matters: workflow_tasks before workflows (FK constraint).
/// Run retention cleanup: delete expired heartbeats, worker states, and terminal records.
///
/// Normally called by the reaper loop on a 1-hour interval.
pub async fn run_retention_cleanup(pool: &PgPool, config: &RecoveryConfig) {
    let mut deleted_heartbeats: u64 = 0;
    let mut deleted_worker_states: u64 = 0;
    let mut deleted_workflow_tasks: u64 = 0;
    let mut deleted_workflows: u64 = 0;
    let mut deleted_tasks: u64 = 0;

    let result: Result<(), sqlx::Error> = async {
        if let Some(hours) = config.heartbeat_retention_hours {
            let r = sqlx::query(DELETE_EXPIRED_HEARTBEATS_SQL)
                .bind(hours.to_string())
                .execute(pool)
                .await?;
            deleted_heartbeats = r.rows_affected();
        }

        if let Some(hours) = config.worker_state_retention_hours {
            let r = sqlx::query(DELETE_EXPIRED_WORKER_STATES_SQL)
                .bind(hours.to_string())
                .execute(pool)
                .await?;
            deleted_worker_states = r.rows_affected();
        }

        if let Some(hours) = config.terminal_record_retention_hours {
            let hours_str = hours.to_string();

            let r = sqlx::query(DELETE_EXPIRED_WORKFLOW_TASKS_SQL)
                .bind(&hours_str)
                .execute(pool)
                .await?;
            deleted_workflow_tasks = r.rows_affected();

            let r = sqlx::query(DELETE_EXPIRED_WORKFLOWS_SQL)
                .bind(&hours_str)
                .execute(pool)
                .await?;
            deleted_workflows = r.rows_affected();

            let r = sqlx::query(DELETE_EXPIRED_TASKS_SQL)
                .bind(&hours_str)
                .execute(pool)
                .await?;
            deleted_tasks = r.rows_affected();
        }

        Ok(())
    }
    .await;

    match result {
        Ok(()) => {
            let total = deleted_heartbeats
                + deleted_worker_states
                + deleted_workflow_tasks
                + deleted_workflows
                + deleted_tasks;
            if total > 0 {
                tracing::info!(
                    deleted_heartbeats,
                    deleted_worker_states,
                    deleted_workflow_tasks,
                    deleted_workflows,
                    deleted_tasks,
                    "retention cleanup completed",
                );
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "retention cleanup failed");
        }
    }
}

/// Spawn a periodic workflow recovery loop.
///
/// Runs alongside the task reaper to detect and fix stuck workflow tasks.
/// Uses the same check interval as the task reaper.
#[allow(clippy::needless_pass_by_value)]
pub fn spawn_workflow_recovery(
    pool: PgPool,
    registry: Arc<WorkflowSpecRegistry>,
    config: RecoveryConfig,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let check_interval = Duration::from_millis(config.check_interval_ms);

        tracing::info!(
            check_interval_ms = config.check_interval_ms,
            "workflow recovery loop started",
        );

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(check_interval) => {
                    match crate::workflow_engine::recover_stuck_workflows(&pool, &registry).await {
                        Ok(report) if report.total() > 0 => {
                            tracing::info!(
                                total = report.total(),
                                errors = report.errors,
                                "workflow recovery pass completed",
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "workflow recovery pass failed",
                            );
                        }
                        _ => {}
                    }
                }
            }
        }
    })
}

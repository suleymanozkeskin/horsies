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
  AND (
      t.finalizing_at IS NULL
      OR t.finalizing_at < NOW() - $2 * INTERVAL '1 second'
  )
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
  AND (
      t.finalizing_at IS NULL
      OR t.finalizing_at < NOW() - $3 * INTERVAL '1 second'
  )
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
    finalizing_at = NULL,
    finalizing_by_worker_id = NULL,
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
    finalizing_at = NULL,
    finalizing_by_worker_id = NULL,
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

// Retain a terminal+expired workflow's linkage until EVERY backing horsies_tasks
// row is terminal. Defense-in-depth (parity with horsies PR #143): the invariant
// "terminal workflow ⇒ all backing tasks terminal" holds today (cancel cancels all
// linked task rows; complete/fail require all workflow_tasks terminal, which trails
// their task rows), so this guard never fires now — but it ensures a future change
// can never strand a live task row by deleting its workflow_task linkage.
const DELETE_EXPIRED_WORKFLOW_TASKS_SQL: &str = "\
DELETE FROM horsies_workflow_tasks wt
USING horsies_workflows w
WHERE wt.workflow_id = w.id
  AND w.status IN ('COMPLETED', 'FAILED', 'CANCELLED')
  AND COALESCE(w.completed_at, w.updated_at, w.created_at) < NOW() - CAST($1 || ' hours' AS INTERVAL)
  AND NOT EXISTS (
      SELECT 1
      FROM horsies_workflow_tasks live
      JOIN horsies_tasks t ON t.id = live.task_id
      WHERE live.workflow_id = w.id
        AND t.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
  )";

const DELETE_EXPIRED_WORKFLOWS_SQL: &str = "\
DELETE FROM horsies_workflows
WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')
  AND COALESCE(completed_at, updated_at, created_at) < NOW() - CAST($1 || ' hours' AS INTERVAL)
  AND NOT EXISTS (
      SELECT 1
      FROM horsies_workflow_tasks live
      JOIN horsies_tasks t ON t.id = live.task_id
      WHERE live.workflow_id = horsies_workflows.id
        AND t.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
  )";

const DELETE_EXPIRED_TASKS_SQL: &str = "\
DELETE FROM horsies_tasks t
WHERE t.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
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
                    // Cluster-wide gate: only one worker runs a pass per interval.
                    // The passes are safe to run concurrently (SKIP LOCKED), but
                    // redundant across a cluster; the gate elides the duplicate work.
                    match acquire_reaper_gate(&pool).await {
                        ReaperGate::Skip => {
                            tracing::debug!("reaper pass skipped: another worker holds the gate");
                        }
                        ReaperGate::Ungated => {
                            run_reaper_pass(&pool, &config, &mut next_retention_cleanup).await;
                        }
                        ReaperGate::Held(tx) => {
                            run_reaper_pass(&pool, &config, &mut next_retention_cleanup).await;
                            release_reaper_gate(tx).await;
                        }
                    }
                }
            }
        }
    })
}

/// Outcome of trying to acquire the cluster-wide reaper gate.
enum ReaperGate {
    /// Gate held by an otherwise-idle transaction; commit after the pass to
    /// release the xact-scoped lock.
    Held(sqlx::Transaction<'static, sqlx::Postgres>),
    /// Another worker holds the gate this interval; skip the pass.
    Skip,
    /// Gating disabled (single-connection pool): run the pass ungated.
    Ungated,
}

/// Fixed advisory key for the cluster-wide reaper gate (distinct from the claim
/// key). Parity with horsies PR #101 7a3eb0d6.
fn advisory_key_reaper() -> i64 {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"horsies:reaper:v1");
    let digest = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(bytes)
}

/// Try to acquire the reaper gate as a transaction-scoped advisory lock held
/// by an otherwise-idle transaction for the duration of the pass.
///
/// Xact scoping keeps acquire and release on one server backend under
/// PgBouncer transaction pooling (a session-level lock would not survive
/// between round-trips there), and rollback-on-drop releases the lock on any
/// error path.
async fn acquire_reaper_gate(pool: &PgPool) -> ReaperGate {
    // The gate holds one connection while the pass body needs another; on a
    // single-connection pool that would deadlock, so run ungated. SKIP LOCKED
    // keeps an ungated pass correct (just possibly duplicated). Parity with
    // horsies PR #101 4a7344ec.
    if pool.options().get_max_connections() < 2 {
        return ReaperGate::Ungated;
    }
    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, "reaper gate connection unavailable; running ungated");
            return ReaperGate::Ungated;
        }
    };
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(advisory_key_reaper())
        .fetch_one(tx.as_mut())
        .await
        .unwrap_or(false);
    if acquired {
        ReaperGate::Held(tx)
    } else {
        ReaperGate::Skip
    }
}

/// Release the reaper gate by committing its holder transaction (the
/// xact-scoped lock frees on commit; on error it frees via rollback-on-drop).
async fn release_reaper_gate(tx: sqlx::Transaction<'static, sqlx::Postgres>) {
    if let Err(e) = tx.commit().await {
        tracing::warn!(error = %e, "reaper gate commit failed; lock frees when the connection closes");
    }
}

/// Run one reaper pass: stale-RUNNING recovery, PENDING expiry, stale-CLAIMED
/// requeue, and periodic retention cleanup.
async fn run_reaper_pass(
    pool: &PgPool,
    config: &RecoveryConfig,
    next_retention_cleanup: &mut tokio::time::Instant,
) {
    if config.auto_fail_stale_running {
        let threshold_secs = config.running_stale_threshold_ms as f64 / 1000.0;
        let finalizing_threshold_secs = config.finalizing_stale_threshold_ms as f64 / 1000.0;
        match mark_stale_running_as_failed(pool, threshold_secs, finalizing_threshold_secs).await {
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
    match expire_pending_tasks(pool).await {
        Ok(count) if count > 0 => {
            tracing::info!(count, "reaper expired unclaimed PENDING tasks");
        }
        Err(e) => {
            tracing::warn!(error = %e, "reaper: failed to expire pending tasks");
        }
        _ => {}
    }

    if config.auto_requeue_stale_claimed {
        let threshold_secs = config.claimed_stale_threshold_ms as f64 / 1000.0;
        match requeue_stale_claimed(pool, threshold_secs).await {
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
    if tokio::time::Instant::now() >= *next_retention_cleanup {
        run_retention_cleanup(pool, config).await;
        *next_retention_cleanup = tokio::time::Instant::now() + RETENTION_CLEANUP_INTERVAL;
    }
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
    finalizing_threshold_secs: f64,
) -> Result<u64, sqlx::Error> {
    // Phase 1: Scan for stale task IDs (no row locks). A task that is actively
    // finalizing (finalizing_at set within finalizing_threshold_secs) is skipped.
    let stale_ids: Vec<StaleTaskId> = sqlx::query_as(FIND_STALE_RUNNING_IDS_SQL)
        .bind(threshold_secs)
        .bind(finalizing_threshold_secs)
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
            finalizing_threshold_secs,
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
    finalizing_threshold_secs: f64,
    threshold_ms: u64,
    error_code_str: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Re-acquire row with full context. Returns None if the task is no longer
    // RUNNING, if a fresh heartbeat arrived after the Phase 1 scan, or if the
    // task is actively finalizing within finalizing_threshold_secs.
    let ctx: Option<StaleTaskContext> = sqlx::query_as(SELECT_STALE_TASK_FOR_UPDATE_SQL)
        .bind(task_id)
        .bind(threshold_secs)
        .bind(finalizing_threshold_secs)
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
/// Batch size per expiry statement.
const EXPIRE_BATCH_SIZE: i64 = 500;
/// Max batches per reaper pass, bounding work and trigger-NOTIFY volume.
const EXPIRE_MAX_BATCHES_PER_PASS: u32 = 200;

/// SQL: expire one batch of unclaimed PENDING tasks past good_until.
///
/// The candidate set is bounded by `LIMIT $2 FOR UPDATE SKIP LOCKED` so a mass
/// expiry is spread across several committed statements instead of one
/// transaction that row-locks every match and flushes two trigger NOTIFYs per
/// row in a single commit (which can overflow listener queues).
const EXPIRE_PENDING_BATCH_SQL: &str = "\
UPDATE horsies_tasks t
SET status = 'EXPIRED',
    failed_at = NOW(),
    result = $1,
    error_code = 'TASK_EXPIRED',
    updated_at = NOW()
FROM (
    SELECT id FROM horsies_tasks
    WHERE status = 'PENDING'
      AND good_until IS NOT NULL
      AND good_until <= NOW()
    LIMIT $2
    FOR UPDATE SKIP LOCKED
) s
WHERE t.id = s.id";

pub async fn expire_pending_tasks(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let task_error = TaskError::builtin(
        crate::core::OutcomeCode::TaskExpired,
        "task expired before being claimed (good_until passed)",
    );
    let task_result = TaskResult::<()>::Err(task_error);
    let result_json = serde_json::to_string(&task_result)
        .unwrap_or_else(|_| r#"{"__type":"err","value":{"message":"expired"}}"#.to_owned());

    let mut total: u64 = 0;
    for _ in 0..EXPIRE_MAX_BATCHES_PER_PASS {
        let result = sqlx::query(EXPIRE_PENDING_BATCH_SQL)
            .bind(&result_json)
            .bind(EXPIRE_BATCH_SIZE)
            .execute(pool)
            .await?;
        let affected = result.rows_affected();
        total += affected;
        if affected < EXPIRE_BATCH_SIZE as u64 {
            break;
        }
    }
    Ok(total)
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
                    match crate::workflow_engine::recover_stuck_workflows(
                        &pool, &registry, config.crashed_worker_recovery_grace_ms,
                    ).await {
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

#[cfg(test)]
mod tests {
    use super::*;
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
        if let Some(root) = root {
            if let Ok(contents) = std::fs::read_to_string(root.join(".env")) {
                for line in contents.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    if let Some((key, value)) = line.split_once('=') {
                        if key.trim() == "DB_PASSWORD" {
                            return format!(
                                "postgresql://postgres:{}@localhost:5432/horsies-rust-port",
                                value.trim(),
                            );
                        }
                    }
                }
            }
        }
        panic!("database URL not found: set DATABASE_URL or add DB_PASSWORD to .env");
    }

    async fn test_pool() -> PgPool {
        let pool = PgPool::connect(&test_db_url()).await.expect("connect");
        crate::broker::migrations::run_horsies_migrations(&pool)
            .await
            .expect("migrations");
        pool
    }

    /// Insert a RUNNING task whose runner heartbeat is already stale, with the
    /// given `finalizing_at` (NULL or a timestamp).
    async fn insert_stale_running_task(pool: &PgPool, task_id: &str, finalizing_at_sql: &str) {
        let sql = format!(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, started_at, created_at, updated_at, claimed,
                claimed_by_worker_id, retry_count, max_retries, enqueue_sha,
                finalizing_at
            ) VALUES (
                $1, 'reaper_test', 'default', 100, '[]', '{{}}', 'RUNNING',
                NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', NOW(), NOW(), TRUE,
                'worker-1', 0, 0,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                {finalizing_at_sql}
            )"
        );
        sqlx::query(&sql).bind(task_id).execute(pool).await.unwrap();
    }

    async fn task_status(pool: &PgPool, task_id: &str) -> String {
        sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// A task actively finalizing (recent finalizing_at) must NOT be reclaimed
    /// by the stale-RUNNING reaper even though its runner heartbeat has stopped.
    #[tokio::test]
    #[serial]
    async fn reaper_skips_actively_finalizing_task() {
        let pool = test_pool().await;
        let task_id = Uuid::new_v4().to_string();
        // finalizing_at = NOW(): within the finalizing threshold → skip.
        insert_stale_running_task(&pool, &task_id, "NOW()").await;

        // running stale threshold 1s (heartbeat is 1h old → stale), finalizing
        // threshold 300s (finalizing_at is fresh → protected). The reaper scan is
        // global (shared test DB), so assert on this task's status, not the count.
        mark_stale_running_as_failed(&pool, 1.0, 300.0)
            .await
            .unwrap();

        assert_eq!(
            task_status(&pool, &task_id).await,
            "RUNNING",
            "actively-finalizing task must be skipped by the reaper"
        );

        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(&task_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// A task whose finalizing stamp is older than the finalizing threshold is
    /// treated as genuinely stuck and IS reclaimed (marked FAILED).
    #[tokio::test]
    #[serial]
    async fn reaper_reclaims_task_finalizing_past_threshold() {
        let pool = test_pool().await;
        let task_id = Uuid::new_v4().to_string();
        // finalizing_at well in the past → past the finalizing threshold.
        insert_stale_running_task(&pool, &task_id, "NOW() - INTERVAL '1 hour'").await;

        mark_stale_running_as_failed(&pool, 1.0, 300.0)
            .await
            .unwrap();

        assert_eq!(
            task_status(&pool, &task_id).await,
            "FAILED",
            "task finalizing past the threshold must be reclaimed"
        );

        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(&task_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// Retention must NOT delete a terminal+expired workflow's linkage while a
    /// backing task row is still live; once that task is terminal, it sweeps.
    /// Parity with horsies PR #143 (defensive prevent lever).
    #[tokio::test]
    #[serial]
    async fn retention_retains_workflow_with_live_backing_task() {
        let pool = test_pool().await;
        let wf_id = Uuid::new_v4().to_string();
        let task_id = Uuid::new_v4().to_string();

        // Terminal + expired workflow.
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at, completed_at
            ) VALUES (
                $1, 'ret_wf', 'CANCELLED', 'fail', 'test.ret.v1', 0, $1,
                NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                NOW() - INTERVAL '2 hours'
            )",
        )
        .bind(&wf_id)
        .execute(&pool)
        .await
        .unwrap();

        // A still-live (RUNNING) backing task row + its workflow_task linkage.
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, started_at, created_at, updated_at, retry_count, max_retries, enqueue_sha
            ) VALUES (
                $1, 'ret_task', 'default', 100, '[]', '{}', 'RUNNING',
                NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours', 0, 0, $1
            )",
        )
        .bind(&task_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, task_id, created_at
            ) VALUES (
                $1, $2, 0, 'node_0', 'ret_task', '[]', '{}',
                'default', 100, '{}', FALSE, 'all',
                'RUNNING', FALSE, $3, NOW() - INTERVAL '2 hours'
            )",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&wf_id)
        .bind(&task_id)
        .execute(&pool)
        .await
        .unwrap();

        let wt_count = |pool: PgPool, wf: String| async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = $1",
            )
            .bind(&wf)
            .fetch_one(&pool)
            .await
            .unwrap()
        };

        // hours = 0 → everything terminal+expired qualifies by age; only the
        // live-backing-task guard should hold the linkage back.
        sqlx::query(DELETE_EXPIRED_WORKFLOW_TASKS_SQL)
            .bind("0")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            wt_count(pool.clone(), wf_id.clone()).await,
            1,
            "linkage must be retained while a backing task is live",
        );

        // Backing task becomes terminal → linkage now sweepable.
        sqlx::query("UPDATE horsies_tasks SET status = 'COMPLETED', completed_at = NOW() - INTERVAL '2 hours' WHERE id = $1")
            .bind(&task_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(DELETE_EXPIRED_WORKFLOW_TASKS_SQL)
            .bind("0")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            wt_count(pool.clone(), wf_id.clone()).await,
            0,
            "linkage must be deleted once the backing task is terminal",
        );

        sqlx::query(DELETE_EXPIRED_WORKFLOWS_SQL)
            .bind("0")
            .execute(&pool)
            .await
            .unwrap();
        let wf_remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM horsies_workflows WHERE id = $1")
                .bind(&wf_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(wf_remaining, 0, "workflow swept once all backing tasks terminal");

        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(&task_id)
            .execute(&pool)
            .await
            .unwrap();
    }
}

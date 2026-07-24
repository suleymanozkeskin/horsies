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
  AND COALESCE(hb.last_heartbeat, t.started_at) < NOW() - $1 * INTERVAL '1 second'
LIMIT $3";

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

// Retention deletes run in bounded batches (parity with horsies PR #172):
// each statement deletes at most $2 rows selected by an id-subselect, and the
// pass commits per batch (autocommit). An unbounded DELETE turns the first
// pass over a large eligible backlog (retention newly enabled, or the window
// lowered) into one multi-hour-lock transaction: WAL burst, task_attempts
// cascades, long row locks. FOR UPDATE SKIP LOCKED lets concurrent ungated
// passes drain disjoint batches instead of blocking.

const DELETE_EXPIRED_HEARTBEATS_SQL: &str = "\
DELETE FROM horsies_heartbeats
WHERE id IN (
    SELECT id FROM horsies_heartbeats
    WHERE sent_at < NOW() - CAST($1 || ' hours' AS INTERVAL)
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)";

const DELETE_EXPIRED_WORKER_STATES_SQL: &str = "\
DELETE FROM horsies_worker_states
WHERE id IN (
    SELECT id FROM horsies_worker_states
    WHERE snapshot_at < NOW() - CAST($1 || ' hours' AS INTERVAL)
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)";

// Retain a terminal+expired workflow's linkage until EVERY backing horsies_tasks
// row is terminal. Defense-in-depth (parity with horsies PR #143): the invariant
// "terminal workflow ⇒ all backing tasks terminal" holds today (cancel cancels all
// linked task rows; complete/fail require all workflow_tasks terminal, which trails
// their task rows), so this guard never fires now — but it ensures a future change
// can never strand a live task row by deleting its workflow_task linkage.
//
// The workflow status list + COALESCE expression here and in
// DELETE_EXPIRED_WORKFLOWS_SQL must stay structurally aligned with
// idx_horsies_workflows_retention and stx_horsies_workflows_retention
// (migration 0028): the partial index serves the scan only while the status
// literals imply its predicate, and the statistics object supplies the
// whole-table estimate only while the parsed expression matches.
const DELETE_EXPIRED_WORKFLOW_TASKS_SQL: &str = "\
DELETE FROM horsies_workflow_tasks
WHERE id IN (
    SELECT wt.id
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE w.status IN ('COMPLETED', 'FAILED', 'CANCELLED')
      AND COALESCE(w.completed_at, w.updated_at, w.created_at) < NOW() - CAST($1 || ' hours' AS INTERVAL)
      AND NOT EXISTS (
          SELECT 1
          FROM horsies_workflow_tasks live
          JOIN horsies_tasks t ON t.id = live.task_id
          WHERE live.workflow_id = w.id
            AND t.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
      )
    LIMIT $2
    FOR UPDATE OF wt SKIP LOCKED
)";

const DELETE_EXPIRED_WORKFLOWS_SQL: &str = "\
DELETE FROM horsies_workflows
WHERE id IN (
    SELECT w.id
    FROM horsies_workflows w
    WHERE w.status IN ('COMPLETED', 'FAILED', 'CANCELLED')
      AND COALESCE(w.completed_at, w.updated_at, w.created_at) < NOW() - CAST($1 || ' hours' AS INTERVAL)
      AND NOT EXISTS (
          SELECT 1
          FROM horsies_workflow_tasks live
          JOIN horsies_tasks t ON t.id = live.task_id
          WHERE live.workflow_id = w.id
            AND t.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
      )
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)";

// The status list + COALESCE expression must stay textually aligned with
// idx_horsies_tasks_retention (migrations/0025_retention_indexes.sql): the
// planner can only serve the eligibility predicate from the partial index
// while it can prove the status predicate implies the index predicate.
const DELETE_EXPIRED_TASKS_SQL: &str = "\
DELETE FROM horsies_tasks
WHERE id IN (
    SELECT t.id
    FROM horsies_tasks t
    WHERE t.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
      AND COALESCE(t.completed_at, t.failed_at, t.updated_at, t.created_at) < NOW() - CAST($1 || ' hours' AS INTERVAL)
      AND NOT EXISTS (
          SELECT 1
          FROM horsies_workflow_tasks wt
          JOIN horsies_workflows w ON w.id = wt.workflow_id
          WHERE wt.task_id = t.id
            AND w.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
      )
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)";

/// Retention cleanup interval (1 hour), matching Python's `_RETENTION_CLEANUP_INTERVAL_S`.
const RETENTION_CLEANUP_INTERVAL: Duration = Duration::from_secs(3600);

/// Rows per retention DELETE batch. Bounds per-transaction WAL, row locks,
/// and task_attempts cascade volume.
const RETENTION_DELETE_BATCH_SIZE: i64 = 5_000;

/// Max stale-RUNNING candidates a single reaper pass processes. Phase 2 handles
/// each in its own transaction under the cluster-wide reaper gate, so this bounds
/// how long one pass holds the gate; successive passes drain any larger backlog
/// (P8).
const STALE_RUNNING_SCAN_LIMIT: i64 = 1_000;

/// Wall-clock budget for one retention pass across all five statements. A
/// backlog that does not drain within the budget resumes on the next pass;
/// every statement still runs at least one batch per pass so a deep backlog
/// in an earlier table cannot starve the later ones indefinitely.
const RETENTION_PASS_TIME_BUDGET: Duration = Duration::from_secs(60);

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
                    match acquire_gate(&pool, advisory_key_reaper()).await {
                        GatePass::Skip => {
                            tracing::debug!("reaper pass skipped: another worker holds the gate");
                        }
                        GatePass::Ungated => {
                            run_reaper_pass(&pool, &config, &mut next_retention_cleanup).await;
                        }
                        GatePass::Held(tx) => {
                            run_reaper_pass(&pool, &config, &mut next_retention_cleanup).await;
                            release_gate(tx).await;
                        }
                    }
                }
            }
        }
    })
}

/// Outcome of trying to acquire a cluster-wide periodic-pass gate.
enum GatePass {
    /// Gate held by an otherwise-idle transaction; commit after the pass to
    /// release the xact-scoped lock.
    Held(sqlx::Transaction<'static, sqlx::Postgres>),
    /// Another worker holds the gate this interval; skip the pass.
    Skip,
    /// Gating disabled (single-connection pool): run the pass ungated.
    Ungated,
}

/// Derive a fixed 64-bit advisory key from a label (first 8 bytes of SHA-256).
fn advisory_key_from(label: &[u8]) -> i64 {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(label);
    let digest = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(bytes)
}

/// Fixed advisory key for the cluster-wide reaper gate (distinct from the claim
/// key). Parity with horsies PR #101 7a3eb0d6.
fn advisory_key_reaper() -> i64 {
    advisory_key_from(b"horsies:reaper:v1")
}

/// Fixed advisory key for the cluster-wide workflow-recovery gate. Distinct from
/// the reaper key so the two passes gate independently. Python runs workflow
/// recovery inside the reaper pass (one shared gate); the Rust port splits it
/// into its own loop, so it needs its own gate to keep the same "one worker per
/// interval, cluster-wide" behavior (parity with horsies PR #101).
fn advisory_key_workflow_recovery() -> i64 {
    advisory_key_from(b"horsies:workflow_recovery:v1")
}

/// Try to acquire a periodic-pass gate as a transaction-scoped advisory lock on
/// `key`, held by an otherwise-idle transaction for the duration of the pass.
///
/// Xact scoping keeps acquire and release on one server backend under
/// PgBouncer transaction pooling (a session-level lock would not survive
/// between round-trips there), and rollback-on-drop releases the lock on any
/// error path.
async fn acquire_gate(pool: &PgPool, key: i64) -> GatePass {
    // The gate holds one connection while the pass body needs another; on a
    // single-connection pool that would deadlock, so run ungated. SKIP LOCKED
    // keeps an ungated pass correct (just possibly duplicated). Parity with
    // horsies PR #101 4a7344ec.
    if pool.options().get_max_connections() < 2 {
        return GatePass::Ungated;
    }
    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, "periodic-pass gate connection unavailable; running ungated");
            return GatePass::Ungated;
        }
    };
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(key)
        .fetch_one(tx.as_mut())
        .await
        .unwrap_or(false);
    if acquired {
        GatePass::Held(tx)
    } else {
        GatePass::Skip
    }
}

/// Release a periodic-pass gate by committing its holder transaction (the
/// xact-scoped lock frees on commit; on error it frees via rollback-on-drop).
async fn release_gate(tx: sqlx::Transaction<'static, sqlx::Postgres>) {
    if let Err(e) = tx.commit().await {
        tracing::warn!(error = %e, "periodic-pass gate commit failed; lock frees when the connection closes");
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
        match mark_stale_running_as_failed(
            pool,
            threshold_secs,
            finalizing_threshold_secs,
            STALE_RUNNING_SCAN_LIMIT,
        )
        .await
        {
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
    scan_limit: i64,
) -> Result<u64, sqlx::Error> {
    // Phase 1: Scan for stale task IDs (no row locks). A task that is actively
    // finalizing (finalizing_at set within finalizing_threshold_secs) is skipped.
    // Bounded by `scan_limit`: Phase 2 processes candidates serially, one
    // transaction each, while the cluster-wide reaper gate is held — an unbounded
    // mass-stale event (a crashed fleet) would make one worker process the whole
    // backlog under the gate while others skip their passes. Successive passes
    // drain the remainder (P8).
    let stale_ids: Vec<StaleTaskId> = sqlx::query_as(FIND_STALE_RUNNING_IDS_SQL)
        .bind(threshold_secs)
        .bind(finalizing_threshold_secs)
        .bind(scan_limit)
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

/// Run one retention DELETE in bounded batches (autocommit per batch).
///
/// Always runs at least one batch; stops when a batch comes back short
/// (backlog drained) or the pass deadline is reached (backlog resumes next
/// pass). Bounded batches keep per-transaction WAL, row locks, and
/// task_attempts cascade volume flat regardless of backlog size.
async fn delete_expired_in_batches(
    pool: &PgPool,
    sql: &str,
    retention_hours: u32,
    batch_size: i64,
    deadline: tokio::time::Instant,
) -> Result<u64, sqlx::Error> {
    let hours = retention_hours.to_string();
    let mut total: u64 = 0;
    loop {
        let deleted = sqlx::query(sql)
            .bind(&hours)
            .bind(batch_size)
            .execute(pool)
            .await?
            .rows_affected();
        total += deleted;
        if deleted < batch_size as u64 {
            return Ok(total);
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::info!(
                total_deleted = total,
                "retention pass time budget reached; remaining backlog resumes next pass",
            );
            return Ok(total);
        }
    }
}

/// Run retention cleanup: prune old heartbeats, worker states, and terminal records.
///
/// Matches Python's retention cleanup logic in the worker's reaper loop.
/// Each category is gated by its config (None = disabled).
/// Order matters: workflow_tasks before workflows (FK constraint).
/// Deletes run in bounded batches under a shared pass time budget.
///
/// Normally called by the reaper loop on a 1-hour interval.
pub async fn run_retention_cleanup(pool: &PgPool, config: &RecoveryConfig) {
    let mut deleted_heartbeats: u64 = 0;
    let mut deleted_worker_states: u64 = 0;
    let mut deleted_workflow_tasks: u64 = 0;
    let mut deleted_workflows: u64 = 0;
    let mut deleted_tasks: u64 = 0;

    // Shared wall-clock budget across the five statements. A backlog that
    // outlives the budget resumes next pass.
    let deadline = tokio::time::Instant::now() + RETENTION_PASS_TIME_BUDGET;

    let result: Result<(), sqlx::Error> = async {
        if let Some(hours) = config.heartbeat_retention_hours {
            deleted_heartbeats = delete_expired_in_batches(
                pool,
                DELETE_EXPIRED_HEARTBEATS_SQL,
                hours,
                RETENTION_DELETE_BATCH_SIZE,
                deadline,
            )
            .await?;
        }

        if let Some(hours) = config.worker_state_retention_hours {
            deleted_worker_states = delete_expired_in_batches(
                pool,
                DELETE_EXPIRED_WORKER_STATES_SQL,
                hours,
                RETENTION_DELETE_BATCH_SIZE,
                deadline,
            )
            .await?;
        }

        if let Some(hours) = config.terminal_record_retention_hours {
            // Order preserved: workflow_tasks -> workflows -> tasks. The
            // all-backing-tasks-terminal guards inside each statement make
            // partial progress between tables safe.
            deleted_workflow_tasks = delete_expired_in_batches(
                pool,
                DELETE_EXPIRED_WORKFLOW_TASKS_SQL,
                hours,
                RETENTION_DELETE_BATCH_SIZE,
                deadline,
            )
            .await?;

            deleted_workflows = delete_expired_in_batches(
                pool,
                DELETE_EXPIRED_WORKFLOWS_SQL,
                hours,
                RETENTION_DELETE_BATCH_SIZE,
                deadline,
            )
            .await?;

            deleted_tasks = delete_expired_in_batches(
                pool,
                DELETE_EXPIRED_TASKS_SQL,
                hours,
                RETENTION_DELETE_BATCH_SIZE,
                deadline,
            )
            .await?;
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
                    // Cluster-wide gate: only one worker runs a recovery pass per
                    // interval. Passes are safe to run concurrently (each case query
                    // uses per-row CAS / SKIP LOCKED), but redundant across a cluster;
                    // the gate elides the duplicate work. Restores the "one worker per
                    // interval" behavior Python gets by running recovery inside the
                    // gated reaper pass.
                    match acquire_gate(&pool, advisory_key_workflow_recovery()).await {
                        GatePass::Skip => {
                            tracing::debug!(
                                "workflow recovery pass skipped: another worker holds the gate",
                            );
                        }
                        GatePass::Ungated => {
                            run_workflow_recovery_pass(&pool, &registry, &config).await;
                        }
                        GatePass::Held(tx) => {
                            run_workflow_recovery_pass(&pool, &registry, &config).await;
                            release_gate(tx).await;
                        }
                    }
                }
            }
        }
    })
}

/// Run one workflow-recovery pass and log its outcome.
async fn run_workflow_recovery_pass(
    pool: &PgPool,
    registry: &WorkflowSpecRegistry,
    config: &RecoveryConfig,
) {
    match crate::workflow_engine::recover_stuck_workflows(
        pool,
        registry,
        config.crashed_worker_recovery_grace_ms,
    )
    .await
    {
        Ok(report) if report.total() > 0 => {
            tracing::info!(
                total = report.total(),
                errors = report.errors,
                "workflow recovery pass completed",
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "workflow recovery pass failed");
        }
        _ => {}
    }
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

    /// N3: the workflow-recovery loop must gate cluster-wide (like the reaper),
    /// so only one worker runs a pass per interval. Before this fix the loop had
    /// no gate and every worker scanned each interval. Verify the gate mechanism
    /// is wired to a distinct key and skips when another holder owns it.
    #[tokio::test]
    #[serial]
    async fn workflow_recovery_gate_skips_when_another_holder_owns_it() {
        // Distinct keys so the two periodic passes gate independently.
        assert_ne!(
            advisory_key_workflow_recovery(),
            advisory_key_reaper(),
            "workflow-recovery gate must not share the reaper key",
        );

        let pool = test_pool().await;

        // Hold the workflow-recovery advisory lock on a separate connection.
        let mut holder = pool.begin().await.expect("begin holder tx");
        let held: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
            .bind(advisory_key_workflow_recovery())
            .fetch_one(holder.as_mut())
            .await
            .expect("holder acquires lock");
        assert!(held, "holder must acquire the gate lock");

        // A second acquirer must be told to skip the pass this interval.
        match acquire_gate(&pool, advisory_key_workflow_recovery()).await {
            GatePass::Skip => {}
            GatePass::Held(_) => panic!("expected Skip while the gate is held, got Held"),
            GatePass::Ungated => panic!("expected Skip, got Ungated (pool too small?)"),
        }

        // Release the holder; acquisition then succeeds and returns a held gate.
        holder.rollback().await.expect("release holder");
        match acquire_gate(&pool, advisory_key_workflow_recovery()).await {
            GatePass::Held(tx) => release_gate(tx).await,
            GatePass::Skip => panic!("expected Held after release, got Skip"),
            GatePass::Ungated => panic!("expected Held after release, got Ungated"),
        }
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

    /// P8: the Phase-1 scan is bounded by `scan_limit`, so one pass processes at
    /// most that many stale tasks and successive passes drain the rest.
    #[tokio::test]
    #[serial]
    async fn stale_running_scan_is_bounded_by_limit() {
        let pool = test_pool().await;
        // Clean this test's namespace so only our stale tasks are in play.
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = 'reaper_test'")
            .execute(&pool)
            .await
            .unwrap();

        let mut ids = Vec::new();
        for _ in 0..3 {
            let id = Uuid::new_v4().to_string();
            insert_stale_running_task(&pool, &id, "NULL").await;
            ids.push(id);
        }

        // scan_limit = 2 with 3 stale candidates → exactly 2 processed this pass.
        let count = mark_stale_running_as_failed(&pool, 1.0, 300.0, 2)
            .await
            .unwrap();
        assert_eq!(count, 2, "the bounded scan must process at most scan_limit tasks");

        for id in &ids {
            sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .unwrap();
        }
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
        mark_stale_running_as_failed(&pool, 1.0, 300.0, STALE_RUNNING_SCAN_LIMIT)
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

        mark_stale_running_as_failed(&pool, 1.0, 300.0, STALE_RUNNING_SCAN_LIMIT)
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
            .bind(RETENTION_DELETE_BATCH_SIZE)
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
            .bind(RETENTION_DELETE_BATCH_SIZE)
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
            .bind(RETENTION_DELETE_BATCH_SIZE)
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

    /// Seed `count` heartbeat rows with an old sent_at, clearing the table first.
    async fn seed_old_heartbeats(pool: &PgPool, count: i64) {
        sqlx::query("DELETE FROM horsies_heartbeats")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at)
             SELECT 'ret-batch-' || g, 'test-sender', 'runner', NOW() - INTERVAL '2 hours'
             FROM generate_series(1, $1) g",
        )
        .bind(count)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn heartbeat_count(pool: &PgPool) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM horsies_heartbeats")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// A backlog larger than the batch size drains fully across batches
    /// (rowcounts 2, 2, 1 at batch_size=2). Parity with horsies PR #172.
    #[tokio::test]
    #[serial]
    async fn retention_batches_drain_backlog() {
        let pool = test_pool().await;
        seed_old_heartbeats(&pool, 5).await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let deleted = delete_expired_in_batches(&pool, DELETE_EXPIRED_HEARTBEATS_SQL, 1, 2, deadline)
            .await
            .unwrap();

        assert_eq!(deleted, 5, "full backlog drains across batches");
        assert_eq!(heartbeat_count(&pool).await, 0);
    }

    /// An expired pass deadline still runs exactly one batch, and the
    /// remaining backlog is left for the next pass.
    #[tokio::test]
    #[serial]
    async fn retention_budget_runs_at_least_one_batch() {
        let pool = test_pool().await;
        seed_old_heartbeats(&pool, 5).await;

        // Deadline already reached before the first batch.
        let deadline = tokio::time::Instant::now();
        let deleted = delete_expired_in_batches(&pool, DELETE_EXPIRED_HEARTBEATS_SQL, 1, 2, deadline)
            .await
            .unwrap();

        assert_eq!(deleted, 2, "exactly one batch under an expired budget");
        assert_eq!(heartbeat_count(&pool).await, 3, "backlog resumes next pass");

        sqlx::query("DELETE FROM horsies_heartbeats")
            .execute(&pool)
            .await
            .unwrap();
    }

    /// The planner must serve the tasks retention eligibility predicate from
    /// idx_horsies_tasks_retention (migration 0025). Catches a drifted
    /// COALESCE expression or a regression to bound-array status params,
    /// either of which silently falls back to a full heap scan.
    #[tokio::test]
    #[serial]
    async fn retention_delete_uses_retention_index() {
        let pool = test_pool().await;

        // Realistic statistics: a population of old terminal rows.
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, created_at, updated_at, completed_at, retry_count, max_retries, enqueue_sha
            )
            SELECT
                'ret-idx-' || g, 'ret_explain_task', 'default', 100, '[]', '{}', 'COMPLETED',
                NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days',
                NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days', 0, 0, 'ret-idx-' || g
            FROM generate_series(1, 500) g",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("ANALYZE horsies_tasks")
            .execute(&pool)
            .await
            .unwrap();

        // A 500-row table fits in a few pages, so the planner still prefers a
        // seq scan; disable it (transaction-local) to force the index choice
        // a production-sized heap produces on its own. EXPLAIN plans the real
        // DELETE statement without executing it, so drift between the DELETE
        // text and the index definition fails this test.
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *tx)
            .await
            .unwrap();
        let plan_rows: Vec<(String,)> =
            sqlx::query_as(&format!("EXPLAIN {}", DELETE_EXPIRED_TASKS_SQL))
                .bind("240")
                .bind(RETENTION_DELETE_BATCH_SIZE)
                .fetch_all(&mut *tx)
                .await
                .unwrap();
        tx.rollback().await.unwrap();

        let plan = plan_rows
            .iter()
            .map(|(line,)| line.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains("idx_horsies_tasks_retention"),
            "eligibility predicate must be served by the retention index; plan:\n{plan}",
        );

        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = 'ret_explain_task'")
            .execute(&pool)
            .await
            .unwrap();
    }

    /// The workflow retention DELETEs must execute via
    /// idx_horsies_workflows_retention (migration 0028). EXPLAIN ANALYZE runs
    /// the exact production statements inside a rolled-back transaction, so
    /// the assertion covers the plan the executor ran: a drifted COALESCE, a
    /// status-literal regression, or a lost statistics object (whose default
    /// 1/3 estimate flips the planner back to a full-table walk) fails here.
    #[tokio::test]
    #[serial]
    async fn workflow_retention_deletes_use_retention_index() {
        let pool = test_pool().await;

        // Realistic statistics: a population of old terminal workflows.
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at, completed_at
            )
            SELECT
                'ret-wf-idx-' || g, 'ret_explain_wf', 'COMPLETED', 'fail', 'test.ret.v1', 0,
                'ret-wf-idx-' || g,
                NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days',
                NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days',
                NOW() - INTERVAL '30 days'
            FROM generate_series(1, 500) g",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("ANALYZE horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();

        // A 500-row table fits in a few pages, so the planner still prefers a
        // seq scan; disable it (transaction-local) to force the index choice a
        // production-sized heap produces on its own. ANALYZE executes the
        // DELETE — the rollback reverts it, so both statements see the same
        // seeded state.
        for (delete_sql, label) in [
            (DELETE_EXPIRED_WORKFLOWS_SQL, "workflows"),
            (DELETE_EXPIRED_WORKFLOW_TASKS_SQL, "workflow_tasks"),
        ] {
            let mut tx = pool.begin().await.unwrap();
            sqlx::query("SET LOCAL enable_seqscan = off")
                .execute(&mut *tx)
                .await
                .unwrap();
            let plan_rows: Vec<(String,)> =
                sqlx::query_as(&format!("EXPLAIN (ANALYZE, BUFFERS) {delete_sql}"))
                    .bind("240")
                    .bind(RETENTION_DELETE_BATCH_SIZE)
                    .fetch_all(&mut *tx)
                    .await
                    .unwrap();
            tx.rollback().await.unwrap();

            let plan = plan_rows
                .iter()
                .map(|(line,)| line.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                plan.contains("idx_horsies_workflows_retention"),
                "{label} retention delete must execute via the workflows retention index; plan:\n{plan}",
            );
        }

        sqlx::query("DELETE FROM horsies_workflows WHERE name = 'ret_explain_wf'")
            .execute(&pool)
            .await
            .unwrap();
    }

    /// P4: the heartbeat retention DELETE filters `sent_at < cutoff`; the planner
    /// must serve it from idx_horsies_heartbeats_sent_at (migration 0026). Before
    /// it, 0013's composite (task_id, role, sent_at DESC) could not serve a
    /// leading sent_at range and every hourly pass seq-scanned the heartbeat heap.
    #[tokio::test]
    #[serial]
    async fn heartbeat_retention_delete_uses_sent_at_index() {
        let pool = test_pool().await;

        // Realistic statistics: a population of old heartbeat rows.
        sqlx::query(
            "INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
             SELECT 'hb-ret-' || g, 'w1', 'runner', NOW() - INTERVAL '30 days', 'h1', 1
             FROM generate_series(1, 500) g",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("ANALYZE horsies_heartbeats")
            .execute(&pool)
            .await
            .unwrap();

        // A 500-row table fits in a few pages, so force the index choice a
        // production-sized heap makes on its own. EXPLAIN plans the real DELETE
        // without executing it.
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *tx)
            .await
            .unwrap();
        let plan_rows: Vec<(String,)> =
            sqlx::query_as(&format!("EXPLAIN {}", DELETE_EXPIRED_HEARTBEATS_SQL))
                .bind("1")
                .bind(RETENTION_DELETE_BATCH_SIZE)
                .fetch_all(&mut *tx)
                .await
                .unwrap();
        tx.rollback().await.unwrap();

        let plan = plan_rows
            .iter()
            .map(|(line,)| line.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains("idx_horsies_heartbeats_sent_at"),
            "sent_at range predicate must be served by the sent_at index; plan:\n{plan}",
        );

        sqlx::query("DELETE FROM horsies_heartbeats WHERE task_id LIKE 'hb-ret-%'")
            .execute(&pool)
            .await
            .unwrap();
    }
}

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::core::config::payload::PayloadPolicy;
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

// The terminal stale-failure statement is `horsies_fail_stale_task`
// (broker/terminalization.rs): the function re-captures heartbeat/finalizing
// state under its own lock and judges staleness authoritatively, so the
// Phase 1 scan and the locked re-check here are advisory — a heartbeat
// landing between scan and call refuses with STALENESS evidence instead of
// failing a live task.

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

// One workflow-batched statement deletes a workflow and its node rows together
// (parity with horsies PR #216; replaces the former workflow_tasks + workflows
// statement pair, which re-evaluated the live-task guard per batch as a
// per-candidate "NOT terminal" probe — an inequality no index serves — and
// re-waded through drained node-less workflows on every later batch).
//
// The live-task guard retains a terminal+expired workflow (and its linkage)
// until EVERY backing horsies_tasks row is terminal. Defense-in-depth (parity
// with horsies PR #143): the invariant "terminal workflow ⇒ all backing tasks
// terminal" holds today (cancel cancels all linked task rows; complete/fail
// require all workflow_tasks terminal, which trails their task rows), so the
// guard never fires now — but it ensures a future change can never strand a
// live task row by deleting its workflow_task linkage. The `live` CTE computes
// it ONCE per statement from the non-terminal side: 'CLAIMED', 'PENDING',
// 'RUNNING' is the complement of the terminal set (together they cover every
// task status; keep both lists in sync), and in-flight work is small by
// definition, so the probe rides ix_horsies_tasks_status.
//
// The workflow status list + COALESCE expression must stay structurally
// aligned with idx_horsies_workflows_retention and
// stx_horsies_workflows_retention (migration 0028): the partial index serves
// the scan only while the status literals imply its predicate, and the
// statistics object supplies the whole-table estimate only while the parsed
// expression matches.
//
// `budgeted` keeps candidates while their running node total fits $2 (the
// knob keeps its rows-per-statement meaning), always keeping the first
// candidate so a workflow larger than the whole budget drains alone instead
// of starving. Node rows are purged set-wise in `purged_nodes` (the
// task_attempts pattern); the workflow_id FK cascade remains the correctness
// net for non-retention deletes.
//
// The top-level DELETE's rowcount counts WORKFLOWS, which under the node
// budget is routinely smaller than $2 while backlog remains — the reaper
// therefore drives this statement with DrainedWhen::EmptyBatch rather than
// the short-batch heuristic the row-batched statements use.
const DELETE_EXPIRED_WORKFLOWS_SQL: &str = "\
WITH live AS MATERIALIZED (
    SELECT DISTINCT wt.workflow_id
    FROM horsies_tasks t
    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
    WHERE t.status IN ('CLAIMED', 'PENDING', 'RUNNING')
),
doomed AS (
    SELECT w.id,
           (SELECT count(*)
            FROM horsies_workflow_tasks wt
            WHERE wt.workflow_id = w.id) AS node_count
    FROM horsies_workflows w
    WHERE w.status IN ('COMPLETED', 'FAILED', 'CANCELLED')
      AND COALESCE(w.completed_at, w.updated_at, w.created_at) < NOW() - CAST($1 || ' hours' AS INTERVAL)
      AND NOT EXISTS (
          SELECT 1 FROM live WHERE live.workflow_id = w.id
      )
    LIMIT $2
    FOR UPDATE SKIP LOCKED
),
budgeted AS (
    SELECT id
    FROM (
        SELECT id,
               SUM(node_count) OVER (ORDER BY id) AS nodes_running,
               ROW_NUMBER() OVER (ORDER BY id) AS position
        FROM doomed
    ) ranked
    WHERE nodes_running <= $2 OR position = 1
),
purged_nodes AS (
    DELETE FROM horsies_workflow_tasks
    WHERE workflow_id IN (SELECT id FROM budgeted)
)
DELETE FROM horsies_workflows
WHERE id IN (SELECT id FROM budgeted)";

// The status list + COALESCE expression must stay textually aligned with
// idx_horsies_tasks_retention (migrations/0025_retention_indexes.sql): the
// planner can only serve the eligibility predicate from the partial index
// while it can prove the status predicate implies the index predicate.
//
// task_attempts are purged set-wise in the purged_attempts CTE rather than
// left to the FK ON DELETE CASCADE: RI triggers are row-level, so the cascade
// issues one child DELETE per doomed task inside this statement's transaction
// — costlier in aggregate than the parent delete itself. The CTE removes the
// whole child set in one indexed statement; the cascade trigger still fires
// per parent row but finds nothing, and remains the correctness net for
// non-retention deletes. rows_affected reports the top-level DELETE only, so
// the batching loop keeps counting parent rows.
//
// $3 carries the queues with a per-queue override window
// (queue_terminal_record_retention_hours); an empty array excludes nothing.
// The exclusion shields PLAIN tasks only: workflow-backing rows
// (is_workflow_task = TRUE) age under the global window even on override
// queues, because the per-queue statement filters them out — an unconditional
// exclusion would leave them unreachable by both statements and retained
// forever. The exclusion is a heap filter on the already-bounded candidate
// scan, so the 0025 index plan is unchanged.
const DELETE_EXPIRED_TASKS_SQL: &str = "\
WITH doomed AS (
    SELECT t.id
    FROM horsies_tasks t
    WHERE t.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
      AND COALESCE(t.completed_at, t.failed_at, t.updated_at, t.created_at) < NOW() - CAST($1 || ' hours' AS INTERVAL)
      AND NOT (t.queue_name = ANY(CAST($3 AS text[]))
               AND t.is_workflow_task = FALSE)
      AND NOT EXISTS (
          SELECT 1
          FROM horsies_workflow_tasks wt
          JOIN horsies_workflows w ON w.id = wt.workflow_id
          WHERE wt.task_id = t.id
            AND w.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
      )
    LIMIT $2
    FOR UPDATE OF t SKIP LOCKED
),
purged_attempts AS (
    DELETE FROM horsies_task_attempts
    WHERE task_id IN (SELECT id FROM doomed)
)
DELETE FROM horsies_tasks
WHERE id IN (SELECT id FROM doomed)";

// Per-queue override window (queue_terminal_record_retention_hours), one
// statement per override queue. Served by idx_horsies_tasks_queue_retention
// (migration 0029): the 0025 expression index cannot serve an override window
// efficiently because the override cutoff is far more recent than the global
// one, making every other queue's retained terminal rows heap-filter misses.
// Scoped to plain tasks (is_workflow_task = FALSE): workflow-backing rows age
// under the global window so a workflow and its task rows are retained as a
// unit. The NOT EXISTS guard is kept as defense in depth (plain tasks have no
// workflow_task linkage). Same purged_attempts mechanism as the global delete.
const DELETE_EXPIRED_TASKS_FOR_QUEUE_SQL: &str = "\
WITH doomed AS (
    SELECT t.id
    FROM horsies_tasks t
    WHERE t.queue_name = $3
      AND t.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
      AND COALESCE(t.completed_at, t.failed_at, t.updated_at, t.created_at) < NOW() - CAST($1 || ' hours' AS INTERVAL)
      AND t.is_workflow_task = FALSE
      AND NOT EXISTS (
          SELECT 1
          FROM horsies_workflow_tasks wt
          JOIN horsies_workflows w ON w.id = wt.workflow_id
          WHERE wt.task_id = t.id
            AND w.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
      )
    LIMIT $2
    FOR UPDATE OF t SKIP LOCKED
),
purged_attempts AS (
    DELETE FROM horsies_task_attempts
    WHERE task_id IN (SELECT id FROM doomed)
)
DELETE FROM horsies_tasks
WHERE id IN (SELECT id FROM doomed)";

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
        let mut next_retention_cleanup = tokio::time::Instant::now()
            + Duration::from_secs(config.retention_sweep_interval_s);
        let mut orphan_state = OrphanSweepState::default();

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
                            run_reaper_pass(&pool, &config, &mut next_retention_cleanup, &mut orphan_state).await;
                        }
                        GatePass::Held(tx) => {
                            run_reaper_pass(&pool, &config, &mut next_retention_cleanup, &mut orphan_state).await;
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
    orphan_state: &mut OrphanSweepState,
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

    // Cancel orphaned workflow tasks (no live workflow_task linkage). These
    // cannot reach RUNNING, so the requeue above skips them and they would
    // otherwise stay CLAIMED forever; cancelling frees claim budget and lets
    // retention sweep them.
    if config.auto_terminate_orphaned_workflow_tasks && !orphan_state.disabled {
        match terminate_orphaned_workflow_tasks(pool).await {
            Ok(count) => {
                orphan_state.permanent_failures = 0;
                if count > 0 {
                    tracing::warn!(
                        count,
                        "reaper cancelled orphaned workflow task(s) (no live \
                         workflow_task linkage)",
                    );
                }
            }
            Err(e) if e.is_retryable() => {
                orphan_state.permanent_failures = 0;
                tracing::warn!(
                    error = %e,
                    "reaper orphan sweep transient failure (will retry next cycle)",
                );
            }
            Err(e) => {
                orphan_state.permanent_failures += 1;
                if orphan_state.permanent_failures >= ORPHAN_SWEEP_MAX_PERMANENT_FAILURES {
                    orphan_state.disabled = true;
                    tracing::error!(
                        error = %e,
                        failures = orphan_state.permanent_failures,
                        "reaper orphan sweep disabled after consecutive permanent \
                         failures; requires deploy or manual intervention",
                    );
                } else {
                    tracing::error!(
                        error = %e,
                        failures = orphan_state.permanent_failures,
                        max = ORPHAN_SWEEP_MAX_PERMANENT_FAILURES,
                        "reaper orphan sweep permanent failure",
                    );
                }
            }
        }
    }

    // Retention cleanup (runs every retention_sweep_interval_s).
    if tokio::time::Instant::now() >= *next_retention_cleanup {
        run_retention_cleanup(pool, config).await;
        *next_retention_cleanup =
            tokio::time::Instant::now() + Duration::from_secs(config.retention_sweep_interval_s);
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
) -> Result<u64, crate::broker::BrokerError> {
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
) -> Result<bool, crate::broker::BrokerError> {
    let mut tx = pool.begin().await.map_err(crate::broker::BrokerError::Database)?;

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
        // Failure path: the operation re-judges staleness from its own
        // capture (authoritative); the attempt row is written only for a
        // transition that applied, in the same transaction.
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

        let command = crate::core::lifecycle::TerminalizationCommand::FailStaleTask {
            task_id: task_id.to_owned(),
            stale_after_ms: threshold_ms as i32,
            finalizing_stale_after_ms: (finalizing_threshold_secs * 1000.0) as i32,
            result_json,
            error_code: error_code_str.to_owned(),
            failed_reason: failed_reason.clone(),
        };
        let outcomes =
            crate::broker::terminalization::terminalize_in_tx(&mut tx, &command).await?;

        let applied = matches!(
            outcomes.first(),
            Some(crate::core::lifecycle::TerminalizationOutcome::Applied { .. })
        );
        if !applied {
            // The authoritative capture disagreed with the advisory scan
            // (e.g. a heartbeat landed in between): nothing was failed, so
            // no attempt row either. Evidence is logged at the adapter
            // boundary.
            tx.rollback().await.map_err(crate::broker::BrokerError::Database)?;
            return Ok(false);
        }

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
            .await
            .map_err(crate::broker::BrokerError::Database)?;

        tx.commit().await.map_err(crate::broker::BrokerError::Database)?;

        tracing::info!(task_id, "stale RUNNING task marked FAILED");
    }

    Ok(true)
}

/// Batch size per expiry statement.
const EXPIRE_BATCH_SIZE: i32 = 500;
/// Max batches per reaper pass, bounding work and trigger-NOTIFY volume.
const EXPIRE_MAX_BATCHES_PER_PASS: u32 = 200;

/// Orphan-sweep bounds: same batch/pass convention as pending expiry.
const ORPHAN_BATCH_SIZE: i32 = 500;
const ORPHAN_MAX_BATCHES_PER_PASS: u32 = 200;
/// Consecutive permanent failures before the orphan sweep disables itself.
const ORPHAN_SWEEP_MAX_PERMANENT_FAILURES: u32 = 3;

/// Per-reaper state for the orphan sweep's disable-after-permanent-failures
/// guard: a sweep that keeps failing non-retryably (a contract breach, not a
/// network blip) stops burning every cycle on it.
#[derive(Default)]
struct OrphanSweepState {
    permanent_failures: u32,
    disabled: bool,
}

/// Cancel orphaned workflow tasks in bounded batches.
///
/// A discovery batch reports one row per transition it made, and every row
/// of a discovery batch is APPLIED — anything else is a contract breach
/// surfaced as an error by the adapter. Early-stops on a short batch.
async fn terminate_orphaned_workflow_tasks(
    pool: &PgPool,
) -> Result<u64, crate::broker::BrokerError> {
    let command = crate::core::lifecycle::TerminalizationCommand::CancelOrphanedTasks {
        batch_size: crate::core::lifecycle::BatchSize::new(ORPHAN_BATCH_SIZE)
            .expect("ORPHAN_BATCH_SIZE is positive"),
    };
    let mut total: u64 = 0;
    for _ in 0..ORPHAN_MAX_BATCHES_PER_PASS {
        let cancelled = crate::broker::terminalization::terminalize(pool, &command).await?;
        let affected = cancelled.len() as u64;
        total += affected;
        if affected < ORPHAN_BATCH_SIZE as u64 {
            break;
        }
    }
    Ok(total)
}

/// Expire unclaimed PENDING tasks whose `good_until` has passed.
///
/// Runs `horsies_expire_pending_tasks` in bounded batches (earliest
/// deadlines first, SKIP LOCKED) so a mass expiry is spread across several
/// committed statements instead of one transaction that row-locks every
/// match and flushes two trigger NOTIFYs per row in a single commit (which
/// can overflow listener queues). No attempt rows are written (the task was
/// never executed). Returns the number of expired tasks.
pub async fn expire_pending_tasks(pool: &PgPool) -> Result<u64, crate::broker::BrokerError> {
    let task_error = TaskError::builtin(
        crate::core::OutcomeCode::TaskExpired,
        "task expired before being claimed (good_until passed)",
    );
    let task_result = TaskResult::<()>::Err(task_error);
    let result_json = serde_json::to_string(&task_result)
        .unwrap_or_else(|_| r#"{"__type":"err","value":{"message":"expired"}}"#.to_owned());
    let command = crate::core::lifecycle::TerminalizationCommand::ExpirePendingTasks {
        batch_size: crate::core::lifecycle::BatchSize::new(EXPIRE_BATCH_SIZE)
            .expect("EXPIRE_BATCH_SIZE is positive"),
        result_json,
        error_code: "TASK_EXPIRED".to_owned(),
    };

    let mut total: u64 = 0;
    for _ in 0..EXPIRE_MAX_BATCHES_PER_PASS {
        let expired = crate::broker::terminalization::terminalize(pool, &command).await?;
        let affected = expired.len() as u64;
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

/// The drained signal for one retention DELETE's batching loop.
#[derive(Clone, Copy)]
enum DrainedWhen {
    /// The statement's rowcount equals the rows the batch selects: a short
    /// batch means nothing eligible is left.
    ShortBatch,
    /// The workflow statement: its rowcount counts workflows while the node
    /// budget keeps batches routinely short of `batch_size` — only a
    /// zero-row batch means drained, at the cost of one empty statement per
    /// drained pass.
    EmptyBatch,
}

/// Statement-specific third bind for the tasks retention deletes.
#[derive(Clone, Copy)]
enum ExtraBind<'a> {
    /// Global tasks delete: queues shielded by a per-queue override window.
    ExcludedQueues(&'a [String]),
    /// Per-queue override delete: the override queue's name.
    QueueName(&'a str),
}

/// Run one retention DELETE in bounded batches (autocommit per batch).
///
/// Always runs at least one batch; stops when the backlog reads as drained
/// (per `drained_when`) or the pass deadline is reached (backlog resumes next
/// pass). Bounded batches keep per-transaction WAL and row locks flat
/// regardless of backlog size. `extra` supplies the statement-specific third
/// bind of the tasks deletes; the two-bind statements pass `None`.
async fn delete_expired_in_batches(
    pool: &PgPool,
    sql: &str,
    retention_hours: u32,
    batch_size: i64,
    deadline: tokio::time::Instant,
    drained_when: DrainedWhen,
    extra: Option<ExtraBind<'_>>,
) -> Result<u64, sqlx::Error> {
    let hours = retention_hours.to_string();
    let mut total: u64 = 0;
    loop {
        let query = sqlx::query(sql).bind(&hours).bind(batch_size);
        let query = match extra {
            None => query,
            Some(ExtraBind::ExcludedQueues(queues)) => query.bind(queues),
            Some(ExtraBind::QueueName(queue)) => query.bind(queue),
        };
        let deleted = query.execute(pool).await?.rows_affected();
        total += deleted;
        let drained = match drained_when {
            DrainedWhen::ShortBatch => deleted < batch_size as u64,
            DrainedWhen::EmptyBatch => deleted == 0,
        };
        if drained {
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
/// Each category is gated by its config (None = disabled). A workflow and its
/// node rows are deleted together by the workflow-batched statement.
/// Deletes run in bounded batches under a shared pass time budget.
///
/// Normally called by the reaper loop every `retention_sweep_interval_s`.
pub async fn run_retention_cleanup(pool: &PgPool, config: &RecoveryConfig) {
    let mut deleted_heartbeats: u64 = 0;
    let mut deleted_worker_states: u64 = 0;
    let mut deleted_workflows: u64 = 0;
    let mut deleted_tasks: u64 = 0;

    let batch_size = i64::from(config.retention_delete_batch_size);

    // Shared wall-clock budget across the statements. A backlog that
    // outlives the budget resumes next pass.
    let deadline = tokio::time::Instant::now() + RETENTION_PASS_TIME_BUDGET;

    // Queues with a per-queue override window: shielded from the global
    // tasks delete, then swept with their own windows below.
    let mut override_queues: Vec<String> = config
        .queue_terminal_record_retention_hours
        .keys()
        .cloned()
        .collect();
    override_queues.sort_unstable();

    let result: Result<(), sqlx::Error> = async {
        if let Some(hours) = config.heartbeat_retention_hours {
            deleted_heartbeats = delete_expired_in_batches(
                pool,
                DELETE_EXPIRED_HEARTBEATS_SQL,
                hours,
                batch_size,
                deadline,
                DrainedWhen::ShortBatch,
                None,
            )
            .await?;
        }

        if let Some(hours) = config.worker_state_retention_hours {
            deleted_worker_states = delete_expired_in_batches(
                pool,
                DELETE_EXPIRED_WORKER_STATES_SQL,
                hours,
                batch_size,
                deadline,
                DrainedWhen::ShortBatch,
                None,
            )
            .await?;
        }

        if let Some(hours) = config.terminal_record_retention_hours {
            // Workflows and their node rows go together in one
            // workflow-batched statement (node purge is a CTE inside it);
            // tasks follow. The all-backing-tasks-terminal guard makes
            // partial progress between tables safe. The statement's rowcount
            // counts workflows, which the node budget keeps below batch_size
            // while backlog remains — hence EmptyBatch.
            deleted_workflows = delete_expired_in_batches(
                pool,
                DELETE_EXPIRED_WORKFLOWS_SQL,
                hours,
                batch_size,
                deadline,
                DrainedWhen::EmptyBatch,
                None,
            )
            .await?;

            deleted_tasks = delete_expired_in_batches(
                pool,
                DELETE_EXPIRED_TASKS_SQL,
                hours,
                batch_size,
                deadline,
                DrainedWhen::ShortBatch,
                Some(ExtraBind::ExcludedQueues(&override_queues)),
            )
            .await?;
        }

        // Per-queue override windows govern plain (non-workflow) tasks on
        // their queues and apply even when the global terminal window is
        // disabled.
        for queue_name in &override_queues {
            let override_hours = config.queue_terminal_record_retention_hours[queue_name];
            deleted_tasks += delete_expired_in_batches(
                pool,
                DELETE_EXPIRED_TASKS_FOR_QUEUE_SQL,
                override_hours,
                batch_size,
                deadline,
                DrainedWhen::ShortBatch,
                Some(ExtraBind::QueueName(queue_name)),
            )
            .await?;
        }

        Ok(())
    }
    .await;

    match result {
        Ok(()) => {
            let total =
                deleted_heartbeats + deleted_worker_states + deleted_workflows + deleted_tasks;
            if total > 0 {
                tracing::info!(
                    deleted_heartbeats,
                    deleted_worker_states,
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
    payload: PayloadPolicy,
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
                            run_workflow_recovery_pass(&pool, &registry, &config, &payload).await;
                        }
                        GatePass::Held(tx) => {
                            run_workflow_recovery_pass(&pool, &registry, &config, &payload).await;
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
    payload: &PayloadPolicy,
) {
    match crate::workflow_engine::recover_stuck_workflows(
        pool,
        registry,
        config.crashed_worker_recovery_grace_ms,
        payload,
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

    /// Batch size for direct retention-statement tests (the default
    /// `RecoveryConfig::retention_delete_batch_size`).
    const TEST_RETENTION_BATCH: i64 = 500;

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

        // Drain pre-existing eligible candidates (other tests' leftovers) so
        // the rows_affected assertions below see only this test's workflow.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        delete_expired_in_batches(
            &pool,
            DELETE_EXPIRED_WORKFLOWS_SQL,
            0,
            TEST_RETENTION_BATCH,
            deadline,
            DrainedWhen::EmptyBatch,
            None,
        )
        .await
        .unwrap();

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

        let wf_count = |pool: PgPool, wf: String| async move {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM horsies_workflows WHERE id = $1")
                .bind(&wf)
                .fetch_one(&pool)
                .await
                .unwrap()
        };

        // hours = 0 → everything terminal+expired qualifies by age; only the
        // live-backing-task guard should hold the workflow (and linkage) back.
        let deleted = sqlx::query(DELETE_EXPIRED_WORKFLOWS_SQL)
            .bind("0")
            .bind(TEST_RETENTION_BATCH)
            .execute(&pool)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(deleted, 0, "no workflow deleted while a backing task is live");
        assert_eq!(
            wt_count(pool.clone(), wf_id.clone()).await,
            1,
            "linkage must be retained while a backing task is live",
        );
        assert_eq!(
            wf_count(pool.clone(), wf_id.clone()).await,
            1,
            "workflow must be retained while a backing task is live",
        );

        // Backing task becomes terminal → workflow and linkage sweep together
        // in one statement.
        sqlx::query("UPDATE horsies_tasks SET status = 'COMPLETED', completed_at = NOW() - INTERVAL '2 hours', terminal_at = NOW() - INTERVAL '2 hours' WHERE id = $1")
            .bind(&task_id)
            .execute(&pool)
            .await
            .unwrap();
        let deleted = sqlx::query(DELETE_EXPIRED_WORKFLOWS_SQL)
            .bind("0")
            .bind(TEST_RETENTION_BATCH)
            .execute(&pool)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(deleted, 1, "rowcount counts workflows");
        assert_eq!(
            wt_count(pool.clone(), wf_id.clone()).await,
            0,
            "linkage must leave with its workflow",
        );
        assert_eq!(
            wf_count(pool.clone(), wf_id.clone()).await,
            0,
            "workflow swept once all backing tasks terminal",
        );

        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(&task_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// The node budget bounds each statement's node deletions: 4 workflows ×
    /// 3 nodes against budget 6 → two workflows per statement; the empty-batch
    /// drain loop still removes the whole backlog in one
    /// `delete_expired_in_batches` call (a short-batch heuristic would stop
    /// after the first 2-row batch — the revert-proof). A workflow larger than
    /// the whole budget drains alone instead of starving.
    /// Parity with horsies PR #216.
    #[tokio::test]
    #[serial]
    async fn workflow_retention_budgets_nodes_and_drains_on_empty_batch() {
        let pool = test_pool().await;

        // Drain pre-existing eligible candidates (other tests' leftovers) so
        // the rows_affected assertions below see only this test's workflows.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        delete_expired_in_batches(
            &pool,
            DELETE_EXPIRED_WORKFLOWS_SQL,
            0,
            TEST_RETENTION_BATCH,
            deadline,
            DrainedWhen::EmptyBatch,
            None,
        )
        .await
        .unwrap();

        let seed_workflow = |pool: PgPool, nodes: i64| async move {
            let wf_id = Uuid::new_v4().to_string();
            sqlx::query(
                "INSERT INTO horsies_workflows (
                    id, name, status, on_error, definition_key, depth, root_workflow_id,
                    sent_at, created_at, started_at, updated_at, completed_at
                ) VALUES (
                    $1, 'ret_budget_wf', 'COMPLETED', 'fail', 'test.ret.v1', 0, $1,
                    NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                    NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                    NOW() - INTERVAL '2 hours'
                )",
            )
            .bind(&wf_id)
            .execute(&pool)
            .await
            .unwrap();
            for i in 0..nodes {
                sqlx::query(
                    "INSERT INTO horsies_workflow_tasks (
                        id, workflow_id, task_index, node_id, task_name, task_args,
                        task_kwargs, queue_name, priority, dependencies,
                        allow_failed_deps, join_type, status, is_subworkflow, created_at
                    ) VALUES (
                        $1, $2, $3, 'node_' || $3, 'ret_budget_task', '[]', '{}',
                        'default', 100, '{}', FALSE, 'all',
                        'COMPLETED', FALSE, NOW() - INTERVAL '2 hours'
                    )",
                )
                .bind(Uuid::new_v4().to_string())
                .bind(&wf_id)
                .bind(i as i32)
                .execute(&pool)
                .await
                .unwrap();
            }
            wf_id
        };

        let count_workflows = |pool: PgPool| async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM horsies_workflows WHERE name = 'ret_budget_wf'",
            )
            .fetch_one(&pool)
            .await
            .unwrap()
        };

        // 4 workflows × 3 nodes, budget 6 → 2 workflows per statement.
        for _ in 0..4 {
            seed_workflow(pool.clone(), 3).await;
        }
        let first = sqlx::query(DELETE_EXPIRED_WORKFLOWS_SQL)
            .bind("0")
            .bind(6_i64)
            .execute(&pool)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(first, 2, "node budget 6 admits two 3-node workflows");
        assert_eq!(count_workflows(pool.clone()).await, 2);

        // The empty-batch drain loop removes the rest in one call (2 + 0-row
        // statements); short-batch semantics would have returned after the
        // first 2-row batch above.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let total = delete_expired_in_batches(
            &pool,
            DELETE_EXPIRED_WORKFLOWS_SQL,
            0,
            6,
            deadline,
            DrainedWhen::EmptyBatch,
            None,
        )
        .await
        .unwrap();
        assert_eq!(total, 2, "drain loop continues past short batches to empty");
        assert_eq!(count_workflows(pool.clone()).await, 0);

        // Jumbo: 9 nodes against budget 4 → drains alone (position = 1 escape).
        let jumbo = seed_workflow(pool.clone(), 9).await;
        let deleted = sqlx::query(DELETE_EXPIRED_WORKFLOWS_SQL)
            .bind("0")
            .bind(4_i64)
            .execute(&pool)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(deleted, 1, "over-budget workflow must not starve");
        let nodes_left: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = $1",
        )
        .bind(&jumbo)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(nodes_left, 0, "jumbo's nodes leave with it");
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
        let deleted = delete_expired_in_batches(&pool, DELETE_EXPIRED_HEARTBEATS_SQL, 1, 2, deadline, DrainedWhen::ShortBatch, None)
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
        let deleted = delete_expired_in_batches(&pool, DELETE_EXPIRED_HEARTBEATS_SQL, 1, 2, deadline, DrainedWhen::ShortBatch, None)
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
    /// either of which silently falls back to a full heap scan. The plan must
    /// also carry the set-wise attempts purge as its own Delete node — the FK
    /// cascade form cannot produce one (row-level triggers never appear as
    /// plan nodes), so this assertion is the revert-proof for the
    /// purged_attempts CTE (parity with horsies PR #204).
    #[tokio::test]
    #[serial]
    async fn retention_delete_uses_retention_index() {
        let pool = test_pool().await;

        // Re-runnable after a failed run: drop any leftover seed rows.
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = 'ret_explain_task'")
            .execute(&pool)
            .await
            .unwrap();

        // Realistic statistics: 500 old terminal rows (eligible) + 2000
        // recent terminal rows (in-window). The recent population makes the
        // retention index's cutoff range decisively more selective than the
        // status index's terminal-ANY condition — with only eligible rows the
        // two cost the same and the planner's pick is arbitrary.
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, created_at, updated_at, completed_at, terminal_at,
                retry_count, max_retries, enqueue_sha
            )
            SELECT
                'ret-idx-' || g, 'ret_explain_task', 'default', 100, '[]', '{}', 'COMPLETED',
                NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days',
                NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days',
                NOW() - INTERVAL '30 days', 0, 0, 'ret-idx-' || g
            FROM generate_series(1, 500) g",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, created_at, updated_at, completed_at, terminal_at,
                retry_count, max_retries, enqueue_sha
            )
            SELECT
                'ret-idx-recent-' || g, 'ret_explain_task', 'default', 100, '[]', '{}', 'COMPLETED',
                NOW(), NOW(), NOW(), NOW(), NOW(), 0, 0, 'ret-idx-recent-' || g
            FROM generate_series(1, 2000) g",
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
                .bind(TEST_RETENTION_BATCH)
                .bind(Vec::<String>::new())
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
        assert!(
            plan.contains("Delete on horsies_task_attempts"),
            "attempts must be purged set-wise in the statement, not via FK cascade; plan:\n{plan}",
        );

        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = 'ret_explain_task'")
            .execute(&pool)
            .await
            .unwrap();
    }

    /// The purged_attempts CTE removes a doomed task's attempt history in the
    /// same statement, leaves survivors' history intact, and reports parent
    /// rows only in rows_affected (parity with horsies PR #204).
    #[tokio::test]
    #[serial]
    async fn retention_delete_purges_attempts_set_wise() {
        let pool = test_pool().await;

        // Drain pre-existing eligible candidates (other tests' leftovers) so
        // the rows_affected assertion below sees only this test's rows.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        delete_expired_in_batches(
            &pool,
            DELETE_EXPIRED_TASKS_SQL,
            0,
            TEST_RETENTION_BATCH,
            deadline,
            DrainedWhen::ShortBatch,
            Some(ExtraBind::ExcludedQueues(&[])),
        )
        .await
        .unwrap();

        let doomed_id = Uuid::new_v4().to_string();
        let survivor_id = Uuid::new_v4().to_string();

        // Doomed: terminal, aged out. Survivor: terminal but in-window.
        for (id, completed_at_expr) in [
            (&doomed_id, "NOW() - INTERVAL '2 hours'"),
            (&survivor_id, "NOW()"),
        ] {
            sqlx::query(&format!(
                "INSERT INTO horsies_tasks (
                    id, task_name, queue_name, priority, args, kwargs, status,
                    sent_at, created_at, updated_at, completed_at, terminal_at,
                    retry_count, max_retries, enqueue_sha
                ) VALUES (
                    $1, 'ret_attempts_task', 'default', 100, '[]', '{{}}', 'COMPLETED',
                    {expr}, {expr}, {expr}, {expr}, {expr}, 0, 0, $1
                )",
                expr = completed_at_expr,
            ))
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO horsies_task_attempts (
                    task_id, attempt, outcome, will_retry, started_at, finished_at
                ) VALUES ($1, 1, 'COMPLETED', FALSE, NOW(), NOW())",
            )
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
        }

        // hours = 1 → only the doomed task qualifies by age.
        let deleted = sqlx::query(DELETE_EXPIRED_TASKS_SQL)
            .bind("1")
            .bind(TEST_RETENTION_BATCH)
            .bind(Vec::<String>::new())
            .execute(&pool)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(deleted, 1, "rows_affected counts parent rows only");

        let attempts_for = |pool: PgPool, id: String| async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM horsies_task_attempts WHERE task_id = $1",
            )
            .bind(&id)
            .fetch_one(&pool)
            .await
            .unwrap()
        };
        assert_eq!(
            attempts_for(pool.clone(), doomed_id.clone()).await,
            0,
            "doomed task's attempt history must be purged",
        );
        assert_eq!(
            attempts_for(pool.clone(), survivor_id.clone()).await,
            1,
            "survivor's attempt history must be intact",
        );

        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(&survivor_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// Per-queue override windows (parity with horsies PR #207): the override
    /// delete removes only its queue's eligible PLAIN tasks (other queues,
    /// in-window rows, and workflow-backing rows on the same queue survive;
    /// the doomed row's attempts are purged); the global delete's
    /// excluded_queues shields override queues' plain tasks but NOT their
    /// workflow-backing rows, and an empty array excludes nothing.
    #[tokio::test]
    #[serial]
    async fn per_queue_retention_override_scopes_and_shields() {
        let pool = test_pool().await;

        // Drain pre-existing eligible candidates for deterministic counts.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        delete_expired_in_batches(
            &pool,
            DELETE_EXPIRED_TASKS_SQL,
            0,
            TEST_RETENTION_BATCH,
            deadline,
            DrainedWhen::ShortBatch,
            Some(ExtraBind::ExcludedQueues(&[])),
        )
        .await
        .unwrap();

        let seed_task = |pool: PgPool, queue: &'static str, aged: bool, wf: bool| async move {
            let id = Uuid::new_v4().to_string();
            let ts = if aged { "NOW() - INTERVAL '2 hours'" } else { "NOW()" };
            sqlx::query(&format!(
                "INSERT INTO horsies_tasks (
                    id, task_name, queue_name, priority, args, kwargs, status,
                    is_workflow_task, sent_at, created_at, updated_at,
                    completed_at, terminal_at, retry_count, max_retries, enqueue_sha
                ) VALUES (
                    $1, 'ret_override_task', $2, 100, '[]', '{{}}', 'COMPLETED',
                    $3, {ts}, {ts}, {ts}, {ts}, {ts}, 0, 0, $1
                )",
            ))
            .bind(&id)
            .bind(queue)
            .bind(wf)
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO horsies_task_attempts (
                    task_id, attempt, outcome, will_retry, started_at, finished_at
                ) VALUES ($1, 1, 'COMPLETED', FALSE, NOW(), NOW())",
            )
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();
            id
        };
        let exists = |pool: PgPool, id: String| async move {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM horsies_tasks WHERE id = $1")
                .bind(&id)
                .fetch_one(&pool)
                .await
                .unwrap()
                == 1
        };

        let a_old_plain = seed_task(pool.clone(), "ret-q-a", true, false).await;
        let a_recent_plain = seed_task(pool.clone(), "ret-q-a", false, false).await;
        let a_old_wf = seed_task(pool.clone(), "ret-q-a", true, true).await;
        let b_old_plain = seed_task(pool.clone(), "ret-q-b", true, false).await;

        // Override delete (1h window) on queue a: only a_old_plain leaves.
        let deleted = sqlx::query(DELETE_EXPIRED_TASKS_FOR_QUEUE_SQL)
            .bind("1")
            .bind(TEST_RETENTION_BATCH)
            .bind("ret-q-a")
            .execute(&pool)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(deleted, 1, "override deletes only its queue's eligible plain tasks");
        assert!(!exists(pool.clone(), a_old_plain.clone()).await);
        assert!(exists(pool.clone(), a_recent_plain.clone()).await, "in-window survives");
        assert!(exists(pool.clone(), a_old_wf.clone()).await, "workflow-backing survives");
        assert!(exists(pool.clone(), b_old_plain.clone()).await, "other queue survives");
        let orphan_attempts: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_task_attempts WHERE task_id = $1",
        )
        .bind(&a_old_plain)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(orphan_attempts, 0, "doomed row's attempts purged");

        // Global delete (0h window) with queue a excluded: shields queue a's
        // remaining PLAIN task but not its workflow-backing row; queue b's
        // plain task leaves.
        let excluded = vec!["ret-q-a".to_owned()];
        sqlx::query(DELETE_EXPIRED_TASKS_SQL)
            .bind("0")
            .bind(TEST_RETENTION_BATCH)
            .bind(&excluded)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            exists(pool.clone(), a_recent_plain.clone()).await,
            "exclusion shields the override queue's plain tasks from the global window",
        );
        assert!(
            !exists(pool.clone(), a_old_wf.clone()).await,
            "workflow-backing rows are NOT shielded (they age under the global window)",
        );
        assert!(!exists(pool.clone(), b_old_plain.clone()).await);

        // Empty exclusion excludes nothing.
        sqlx::query(DELETE_EXPIRED_TASKS_SQL)
            .bind("0")
            .bind(TEST_RETENTION_BATCH)
            .bind(Vec::<String>::new())
            .execute(&pool)
            .await
            .unwrap();
        assert!(!exists(pool.clone(), a_recent_plain.clone()).await);
    }

    /// The per-queue override delete must plan onto a retention partial index
    /// — the 0029 queue-leading composite or the 0025 expression index. At
    /// seeded scale the planner's pick between them is arbitrary (the LIMIT
    /// lets either terminate early under a correlation-blind estimate), so
    /// the EXPLAIN accepts both: a drifted COALESCE or status-literal
    /// regression falls off both partials and still fails. The 0029
    /// composite's own shape is pinned against the catalog, where planner
    /// arbitrariness cannot reach.
    #[tokio::test]
    #[serial]
    async fn per_queue_retention_delete_uses_queue_index() {
        let pool = test_pool().await;

        // Re-runnable after a failed run: drop any leftover seed rows.
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = 'ret_qidx_task'")
            .execute(&pool)
            .await
            .unwrap();

        // 500 old (eligible) + 2000 recent terminal rows on the override
        // queue, plus 2000 equally-old terminal rows on another queue
        // (retained under the longer global window). Both single-column
        // contenders scan 2500 rows and filter to 500 — the plain queue_name
        // index carries the recent same-queue rows, the 0025 expression index
        // carries the old other-queue rows — while the queue-leading
        // composite lands on exactly the 500.
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, created_at, updated_at, completed_at, terminal_at,
                retry_count, max_retries, enqueue_sha
            )
            SELECT
                'ret-qidx-' || g, 'ret_qidx_task', 'ret-q-idx', 100, '[]', '{}', 'COMPLETED',
                NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days',
                NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days',
                NOW() - INTERVAL '30 days', 0, 0, 'ret-qidx-' || g
            FROM generate_series(1, 500) g",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, created_at, updated_at, completed_at, terminal_at,
                retry_count, max_retries, enqueue_sha
            )
            SELECT
                'ret-qidx-other-' || g, 'ret_qidx_task', 'ret-q-other', 100, '[]', '{}', 'COMPLETED',
                NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days',
                NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days',
                NOW() - INTERVAL '30 days', 0, 0, 'ret-qidx-other-' || g
            FROM generate_series(1, 2000) g",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, created_at, updated_at, completed_at, terminal_at,
                retry_count, max_retries, enqueue_sha
            )
            SELECT
                'ret-qidx-recent-' || g, 'ret_qidx_task', 'ret-q-idx', 100, '[]', '{}', 'COMPLETED',
                NOW(), NOW(), NOW(), NOW(), NOW(), 0, 0, 'ret-qidx-recent-' || g
            FROM generate_series(1, 2000) g",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("ANALYZE horsies_tasks")
            .execute(&pool)
            .await
            .unwrap();

        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *tx)
            .await
            .unwrap();
        let plan_rows: Vec<(String,)> =
            sqlx::query_as(&format!("EXPLAIN {}", DELETE_EXPIRED_TASKS_FOR_QUEUE_SQL))
                .bind("1")
                .bind(TEST_RETENTION_BATCH)
                .bind("ret-q-idx")
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
            plan.contains("idx_horsies_tasks_queue_retention")
                || plan.contains("idx_horsies_tasks_retention"),
            "per-queue delete must be served by a retention partial index; plan:\n{plan}",
        );

        // Catalog pin of the 0029 composite: queue-leading column order, the
        // retention COALESCE, and the terminal-status partial predicate.
        let indexdef: String = sqlx::query_scalar(
            "SELECT pg_get_indexdef(oid) FROM pg_class \
             WHERE relname = 'idx_horsies_tasks_queue_retention'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            indexdef.contains(
                "(queue_name, COALESCE(completed_at, failed_at, updated_at, created_at))",
            ),
            "0029 composite must lead with queue_name; got: {indexdef}",
        );
        assert!(
            indexdef.contains("WHERE"),
            "0029 composite must be partial on terminal statuses; got: {indexdef}",
        );

        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = 'ret_qidx_task'")
            .execute(&pool)
            .await
            .unwrap();
    }

    /// The workflow retention DELETE must execute via
    /// idx_horsies_workflows_retention (migration 0028). EXPLAIN ANALYZE runs
    /// the exact production statement inside a rolled-back transaction, so
    /// the assertion covers the plan the executor ran: a drifted COALESCE, a
    /// status-literal regression, or a lost statistics object (whose default
    /// 1/3 estimate flips the planner back to a full-table walk) fails here.
    /// The plan must also carry the set-wise node purge as its own Delete
    /// node (parity with horsies PR #216).
    #[tokio::test]
    #[serial]
    async fn workflow_retention_deletes_use_retention_index() {
        let pool = test_pool().await;

        // Re-runnable after a failed run: drop any leftover seed rows.
        sqlx::query("DELETE FROM horsies_workflows WHERE name = 'ret_explain_wf'")
            .execute(&pool)
            .await
            .unwrap();

        // Realistic statistics: 500 old terminal workflows (eligible) + 2000
        // recent terminal workflows (in-window). The recent population makes
        // the retention index's cutoff range decisively more selective — with
        // only eligible rows the planner's index pick is arbitrary.
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
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at, completed_at
            )
            SELECT
                'ret-wf-idx-recent-' || g, 'ret_explain_wf', 'COMPLETED', 'fail', 'test.ret.v1', 0,
                'ret-wf-idx-recent-' || g,
                NOW(), NOW(), NOW(), NOW(), NOW()
            FROM generate_series(1, 2000) g",
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
        // DELETE — the rollback reverts it.
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *tx)
            .await
            .unwrap();
        let plan_rows: Vec<(String,)> = sqlx::query_as(&format!(
            "EXPLAIN (ANALYZE, BUFFERS) {DELETE_EXPIRED_WORKFLOWS_SQL}"
        ))
        .bind("240")
        .bind(TEST_RETENTION_BATCH)
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
            "workflow retention delete must execute via the workflows retention index; plan:\n{plan}",
        );
        assert!(
            plan.contains("Delete on horsies_workflow_tasks"),
            "node rows must be purged set-wise in the statement; plan:\n{plan}",
        );

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
                .bind(TEST_RETENTION_BATCH)
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

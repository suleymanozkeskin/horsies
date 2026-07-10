use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{ConnectOptions, Postgres, Transaction};
use std::str::FromStr;
use tokio::time::Instant;
use uuid::Uuid;

use crate::core::{
    OutcomeCode, PostgresConfig, ResolvedEnqueue, RetrievalCode, TaskError, TaskInfo, TaskOptions,
    TaskResult, TaskSendError, TaskSendErrorCode, TaskSendPayload, TaskSendResult,
};

use crate::broker::bound_handle::TaskHandle;
use crate::broker::error::{is_retryable_sqlx_error, BrokerError};
use crate::broker::health::{
    DatabasePing, WorkerPingRequest, WorkerPong, WorkerPongPayload, WorkerStateSnapshot,
    WORKER_PING_CHANNEL,
};
use crate::broker::result_types::{BrokerErrorCode, BrokerOperationError, BrokerResult};
use crate::broker::row::task::{
    ClaimedId, ClaimedTaskRow, ExpiredTaskRow, SetRunningRow, StaleTaskRow, TaskAttemptRow,
    TaskInfoRow, TaskResultRow, TaskRunningContextRow,
};
use crate::broker::shared_listener::SharedNotifyListener;

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

const ENQUEUE_SQL: &str = "\
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, args, kwargs,
    status, sent_at, enqueued_at, good_until, max_retries, task_options,
    enqueue_sha, is_workflow_task, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', $7, COALESCE($8, NOW()), $9, $10, $11, $12, FALSE, NOW(), NOW())
ON CONFLICT (id) DO NOTHING
RETURNING id";

// Eligibility split into two CTE arms — PENDING and expired-CLAIMED — each with
// its own ORDER BY + FOR UPDATE SKIP LOCKED + LIMIT, merged and re-limited. The
// selected set is provably identical to a single OR-predicate scan: any row in
// the global top-N is within the top-N of its own arm, so merging both arms'
// top-N and re-limiting yields the same N rows. The arms let the planner use a
// dedicated partial index per status instead of a filtered scan over both. The
// one behavioral delta (matching Python) is that up to $2 extra candidate rows
// of the losing arm stay row-locked for the short claim transaction.
const CLAIM_SQL: &str = "\
WITH pending AS (
    SELECT id, priority, enqueued_at
    FROM horsies_tasks
    WHERE queue_name = $1
      AND status = 'PENDING'
      AND enqueued_at <= now()
      AND (next_retry_at IS NULL OR next_retry_at <= now())
      AND (good_until IS NULL OR good_until > now())
    ORDER BY priority ASC, enqueued_at ASC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $2
),
expired AS (
    SELECT id, priority, enqueued_at
    FROM horsies_tasks
    WHERE queue_name = $1
      AND status = 'CLAIMED' AND claim_expires_at IS NOT NULL AND claim_expires_at < now()
      AND enqueued_at <= now()
      AND (next_retry_at IS NULL OR next_retry_at <= now())
      AND (good_until IS NULL OR good_until > now())
    ORDER BY priority ASC, enqueued_at ASC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $2
),
next AS (
    SELECT id
    FROM (
        SELECT id, priority, enqueued_at FROM pending
        UNION ALL
        SELECT id, priority, enqueued_at FROM expired
    ) candidates
    ORDER BY priority ASC, enqueued_at ASC, id ASC
    LIMIT $2
)
UPDATE horsies_tasks t
SET status = 'CLAIMED',
    claimed = TRUE,
    claimed_at = now(),
    claimed_by_worker_id = $3,
    claim_expires_at = $4,
    finalizing_at = NULL,
    finalizing_by_worker_id = NULL,
    updated_at = now()
FROM next
WHERE t.id = next.id
RETURNING t.id, t.task_name, t.args, t.kwargs, t.retry_count, t.max_retries, t.task_options, t.queue_name, t.good_until, t.is_workflow_task";

// One server-side call performs advisory-lock acquisition + cap counts + the
// windowed claim (see horsies_claim, migrations/0024_claim_function.sql;
// parity with horsies PR #160). The xact-scoped locks live only for the
// statement's own transaction — never across a client round trip. Replaces
// the per-pass lock loop + count statements + per-queue CLAIM_SQL loop.
const HORSIES_CLAIM_SQL: &str = "\
SELECT id, task_name, args, kwargs, retry_count, max_retries, task_options, \
       queue_name, good_until, is_workflow_task \
FROM horsies_claim($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";

// CLAIMED -> RUNNING transition with the first runner heartbeat fused in (parity
// with horsies PR #134): the heartbeat row is inserted in the same statement as the
// transition, so a task is never observable RUNNING without heartbeat coverage, and
// a transition that does not apply (expiry / PAUSED / CANCELLED / ownership change)
// inserts no orphan beat. The heartbeat thread no longer sends an immediate beat.
const SET_RUNNING_SQL: &str = "\
WITH upd AS (
    UPDATE horsies_tasks
    SET status = 'RUNNING',
        claimed = FALSE,
        claim_expires_at = NULL,
        started_at = NOW(),
        worker_pid = $2,
        worker_hostname = $3,
        worker_process_name = $4,
        updated_at = NOW()
    WHERE id = $1
      AND status = 'CLAIMED'
      AND claimed_by_worker_id = $5
      AND (claim_expires_at IS NULL OR claim_expires_at > now())
      AND (good_until IS NULL OR good_until > now())
      AND NOT EXISTS (
          SELECT 1
          FROM horsies_workflow_tasks wt
          JOIN horsies_workflows w ON w.id = wt.workflow_id
          WHERE wt.task_id = $1
            AND w.status IN ('PAUSED', 'CANCELLED')
      )
    RETURNING id, started_at
),
hb AS (
    INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
    SELECT id, $5, 'runner', NOW(), $3, $2 FROM upd
)
SELECT id, started_at FROM upd";

// The optional `started_at` fence ($4) scopes the CAS to a specific claim
// generation: `set_running` returns the attempt's `started_at`, and threading it
// here rejects a stale finalize from an attempt the reaper already requeued and
// the same worker re-claimed (a new `started_at`). `NULL` disables the fence,
// preserving the worker+status-only behavior for callers that do not pass it (C10).
const COMPLETE_SQL: &str = "\
UPDATE horsies_tasks
SET status = 'COMPLETED',
    completed_at = NOW(),
    result = $2,
    error_code = NULL,
    finalizing_at = NULL,
    finalizing_by_worker_id = NULL,
    updated_at = NOW()
WHERE id = $1
  AND status = 'RUNNING'
  AND claimed_by_worker_id = $3
  AND ($4::timestamptz IS NULL OR started_at = $4)
RETURNING id";

/// Fused ok-path finalization for plain (non-workflow) tasks: one statement that
/// locks the RUNNING row, upserts the COMPLETED attempt from the locked row's own
/// columns, CASes the task to COMPLETED, and fires the capacity wake. Empty result
/// = status/ownership changed (reaper reclaim) → nothing written. Parity with
/// horsies PR #134. Attempt fields are computed in SQL (`COALESCE(retry_count,0)+1`,
/// `COALESCE(started_at, clock_timestamp())`, `clock_timestamp()`) — identical to
/// the values the multi-statement path binds.
const FINALIZE_TASK_COMPLETED_SQL: &str = "\
WITH ctx AS (
    SELECT id, retry_count, started_at, claimed_by_worker_id,
           worker_hostname, worker_pid, worker_process_name,
           clock_timestamp() AS db_now
    FROM horsies_tasks
    WHERE id = $1
      AND status = 'RUNNING'
      AND claimed_by_worker_id = $2
      AND ($6::timestamptz IS NULL OR started_at = $6)
    FOR UPDATE
),
attempt AS (
    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry,
        started_at, finished_at,
        error_code, error_message, failed_reason,
        worker_id, worker_hostname, worker_pid, worker_process_name
    )
    SELECT ctx.id, COALESCE(ctx.retry_count, 0) + 1, 'COMPLETED', FALSE,
           COALESCE(ctx.started_at, ctx.db_now), ctx.db_now,
           NULL, NULL, NULL,
           ctx.claimed_by_worker_id, ctx.worker_hostname, ctx.worker_pid,
           ctx.worker_process_name
    FROM ctx
    ON CONFLICT (task_id, attempt) DO UPDATE SET
        outcome = EXCLUDED.outcome,
        will_retry = EXCLUDED.will_retry,
        started_at = EXCLUDED.started_at,
        finished_at = EXCLUDED.finished_at,
        error_code = EXCLUDED.error_code,
        error_message = EXCLUDED.error_message,
        failed_reason = EXCLUDED.failed_reason,
        worker_id = EXCLUDED.worker_id,
        worker_hostname = EXCLUDED.worker_hostname,
        worker_pid = EXCLUDED.worker_pid,
        worker_process_name = EXCLUDED.worker_process_name
),
upd AS (
    UPDATE horsies_tasks t
    SET status = 'COMPLETED',
        completed_at = NOW(),
        result = $3,
        error_code = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        updated_at = NOW()
    FROM ctx
    WHERE t.id = ctx.id
    RETURNING t.id AS id
)
SELECT upd.id, pg_notify($4, $5)
FROM upd";

/// Task failure: sets result and error_code (user/task error path).
///
/// The optional `started_at` fence ($5) scopes the CAS to a claim generation;
/// see `COMPLETE_SQL` (C10). `NULL` disables it.
const FAIL_TASK_SQL: &str = "\
UPDATE horsies_tasks
SET status = 'FAILED',
    failed_at = NOW(),
    result = $2,
    error_code = $3,
    finalizing_at = NULL,
    finalizing_by_worker_id = NULL,
    updated_at = NOW()
WHERE id = $1
  AND status = 'RUNNING'
  AND claimed_by_worker_id = $4
  AND ($5::timestamptz IS NULL OR started_at = $5)
RETURNING id";

/// Worker failure: sets failed_reason, clears error_code (worker crash path).
const FAIL_WORKER_SQL: &str = "\
UPDATE horsies_tasks
SET status = 'FAILED',
    failed_at = NOW(),
    failed_reason = $2,
    error_code = NULL,
    finalizing_at = NULL,
    finalizing_by_worker_id = NULL,
    updated_at = NOW()
WHERE id = $1
  AND status = 'RUNNING'
  AND claimed_by_worker_id = $3
RETURNING id";

const CANCEL_SQL: &str = "\
UPDATE horsies_tasks
SET status = 'CANCELLED',
    updated_at = NOW()
WHERE id = $1
  AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
RETURNING id";

// retry_count is derived from the row being CAS-updated (same idiom as
// FINALIZE_TASK_COMPLETED_SQL's ctx CTE), not passed by the caller: the
// ownership CAS makes the claim-time snapshot provably equal to the row, so
// self-incrementing removes the snapshot from the written value entirely.
// The optional `started_at` fence ($4) scopes the CAS to a claim generation;
// see `COMPLETE_SQL` (C10). `NULL` disables it.
const REQUEUE_SQL: &str = "\
UPDATE horsies_tasks
SET status = 'PENDING',
    retry_count = COALESCE(retry_count, 0) + 1,
    next_retry_at = $2,
    enqueued_at = $2,
    error_code = NULL,
    finalizing_at = NULL,
    finalizing_by_worker_id = NULL,
    updated_at = NOW()
WHERE id = $1
  AND status = 'RUNNING'
  AND claimed_by_worker_id = $3
  AND (good_until IS NULL OR $2 < good_until)
  AND ($4::timestamptz IS NULL OR started_at = $4)
RETURNING id";

const GET_RESULT_SQL: &str = "\
SELECT id, status, result, failed_reason
FROM horsies_tasks
WHERE id = $1";

const GET_TASK_INFO_SQL: &str = "\
SELECT id, task_name, status, queue_name, priority, retry_count, max_retries,
       next_retry_at, sent_at, enqueued_at, claimed_at, started_at, completed_at, failed_at,
       worker_hostname, worker_pid, worker_process_name, error_code, failed_reason, result
FROM horsies_tasks
WHERE id = $1";

/// Metadata query excluding result and failed_reason columns.
///
/// Used when `include_result=false` and `include_failed_reason=false` to
/// avoid fetching potentially large TEXT columns, matching Python's default
/// `info(include_result=False, include_failed_reason=False)` behaviour.
const GET_TASK_INFO_MINIMAL_SQL: &str = "\
SELECT id, task_name, status, queue_name, priority, retry_count, max_retries,
       next_retry_at, sent_at, enqueued_at, claimed_at, started_at, completed_at, failed_at,
       worker_hostname, worker_pid, worker_process_name, error_code,
       NULL::TEXT AS failed_reason, NULL::TEXT AS result
FROM horsies_tasks
WHERE id = $1";

/// Metadata query including result but excluding failed_reason.
const GET_TASK_INFO_RESULT_ONLY_SQL: &str = "\
SELECT id, task_name, status, queue_name, priority, retry_count, max_retries,
       next_retry_at, sent_at, enqueued_at, claimed_at, started_at, completed_at, failed_at,
       worker_hostname, worker_pid, worker_process_name, error_code,
       NULL::TEXT AS failed_reason, result
FROM horsies_tasks
WHERE id = $1";

/// Metadata query including failed_reason but excluding result.
const GET_TASK_INFO_REASON_ONLY_SQL: &str = "\
SELECT id, task_name, status, queue_name, priority, retry_count, max_retries,
       next_retry_at, sent_at, enqueued_at, claimed_at, started_at, completed_at, failed_at,
       worker_hostname, worker_pid, worker_process_name, error_code,
       failed_reason, NULL::TEXT AS result
FROM horsies_tasks
WHERE id = $1";

/// Count all RUNNING + active CLAIMED tasks across every worker in the cluster.
/// Excludes expired claims (reclaimable, must not consume cap budget).
const COUNT_GLOBAL_IN_FLIGHT_SQL: &str = "\
SELECT COUNT(*) FROM horsies_tasks \
WHERE status = 'RUNNING' \
   OR (status = 'CLAIMED' \
       AND (claim_expires_at IS NULL OR claim_expires_at > now()))";

/// Count only RUNNING tasks for a specific worker.
const COUNT_RUNNING_FOR_WORKER_SQL: &str = "\
SELECT COUNT(*) FROM horsies_tasks \
WHERE claimed_by_worker_id = $1 AND status = 'RUNNING'";

/// Cluster-wide RUNNING count per queue, for the given queues. Mirrors the
/// soft-mode per-queue cap accounting in `horsies_claim` (RUNNING only), so a
/// buffered-dispatch re-check against these counts matches the claim function's
/// view of a queue's `max_concurrency` cap (C17).
const COUNT_RUNNING_BY_QUEUE_SQL: &str = "\
SELECT queue_name, COUNT(*) FROM horsies_tasks \
WHERE queue_name = ANY($1) AND status = 'RUNNING' \
GROUP BY queue_name";

/// Load CLAIMED tasks owned by a specific worker (for prefetch buffer dispatch).
/// `$2` bounds the fetch to at most the available semaphore permits, since no
/// more than that can be dispatched this pass (P7).
const LOAD_BUFFERED_CLAIMED_SQL: &str = "\
SELECT id, task_name, args, kwargs, retry_count, max_retries, task_options, queue_name, good_until, is_workflow_task \
FROM horsies_tasks \
WHERE claimed_by_worker_id = $1 AND status = 'CLAIMED' \
ORDER BY priority ASC, enqueued_at ASC, id ASC \
LIMIT $2";

/// Get the workflow status for a task (if it belongs to a workflow).
const GET_WORKFLOW_STATUS_FOR_TASK_SQL: &str = "\
SELECT w.status FROM horsies_workflows w \
JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id \
WHERE wt.task_id = $1";

/// Find tasks from a batch that belong to PAUSED or CANCELLED workflows.
/// Returns (task_id, workflow_status) to allow split handling.
const FIND_NON_RUNNABLE_WORKFLOW_TASKS_SQL: &str = "\
SELECT t.id, w.status \
FROM horsies_tasks t \
JOIN horsies_workflow_tasks wt ON wt.task_id = t.id \
JOIN horsies_workflows w ON w.id = wt.workflow_id \
WHERE t.id = ANY($1) AND w.status IN ('PAUSED', 'CANCELLED')";

/// Cancel tasks belonging to CANCELLED workflows.
const CANCEL_CANCELLED_WORKFLOW_HORSIES_TASKS_SQL: &str = "\
UPDATE horsies_tasks \
SET status = 'CANCELLED', \
    claimed = FALSE, \
    claimed_at = NULL, \
    claimed_by_worker_id = NULL, \
    claim_expires_at = NULL, \
    updated_at = NOW() \
WHERE id = ANY($1) AND status IN ('CLAIMED', 'PENDING')";

/// Skip workflow_tasks for tasks belonging to CANCELLED workflows.
const SKIP_CANCELLED_WORKFLOW_TASKS_SQL: &str = "\
UPDATE horsies_workflow_tasks \
SET status = 'SKIPPED', \
    completed_at = NOW() \
WHERE task_id = ANY($1) AND status IN ('PENDING', 'READY', 'ENQUEUED')";

/// Cancel this worker's CLAIMED-but-not-started tasks in PAUSED workflows
/// (post-claim batch filter). The row is moved to terminal CANCELLED — not back
/// to PENDING — so it cannot be re-claimed and run as a duplicate after the
/// workflow resumes (resume enqueues a fresh row for the node, whose
/// workflow_task is reset to READY with task_id=NULL). Ownership-scoped so a
/// worker cannot touch another worker's claim; returns the affected ids so only
/// those workflow_task rows are reset.
const CANCEL_PAUSED_OWNED_TASKS_SQL: &str = "\
UPDATE horsies_tasks \
SET status = 'CANCELLED', \
    claimed = FALSE, \
    claimed_at = NULL, \
    claimed_by_worker_id = NULL, \
    claim_expires_at = NULL, \
    finalizing_at = NULL, \
    finalizing_by_worker_id = NULL, \
    error_code = 'TASK_CANCELLED', \
    failed_reason = 'Workflow paused before task start', \
    updated_at = NOW() \
WHERE id = ANY($1) \
  AND status = 'CLAIMED' \
  AND claimed_by_worker_id = $2 \
RETURNING id";

/// Cancel this worker's CLAIMED tasks in CANCELLED workflows (post-claim batch
/// filter). Ownership-scoped; returns the affected ids so only those
/// workflow_task rows are skipped.
const CANCEL_CANCELLED_OWNED_TASKS_SQL: &str = "\
UPDATE horsies_tasks \
SET status = 'CANCELLED', \
    claimed = FALSE, \
    claimed_at = NULL, \
    claimed_by_worker_id = NULL, \
    claim_expires_at = NULL, \
    updated_at = NOW() \
WHERE id = ANY($1) \
  AND status = 'CLAIMED' \
  AND claimed_by_worker_id = $2 \
RETURNING id";

/// Cancel claimed-but-not-started tasks of a PAUSED workflow (pre-start handler).
/// Moves CLAIMED rows to terminal CANCELLED so they cannot run as duplicates of
/// the node after resume; the node is reset to READY separately. Not
/// ownership-scoped: the caller reached this only after `set_running` declined to
/// start the task because its workflow is PAUSED.
const CANCEL_PAUSED_TASKS_SQL: &str = "\
UPDATE horsies_tasks \
SET status = 'CANCELLED', \
    claimed = FALSE, \
    claimed_at = NULL, \
    claimed_by_worker_id = NULL, \
    claim_expires_at = NULL, \
    finalizing_at = NULL, \
    finalizing_by_worker_id = NULL, \
    error_code = 'TASK_CANCELLED', \
    failed_reason = 'Workflow paused before task start', \
    updated_at = NOW() \
WHERE id = ANY($1) AND status = 'CLAIMED'";

/// Unclaim a single task, guarded by worker identity to prevent races.
const UNCLAIM_TASK_SQL: &str = "\
UPDATE horsies_tasks \
SET status = 'PENDING', \
    claimed = FALSE, \
    claimed_at = NULL, \
    claimed_by_worker_id = NULL, \
    claim_expires_at = NULL, \
    updated_at = NOW() \
WHERE id = $1 \
  AND status = 'CLAIMED' \
  AND claimed_by_worker_id = $2 \
RETURNING id";

/// Reset workflow_tasks for unclaimed tasks back to READY.
const RESET_WORKFLOW_TASKS_SQL: &str = "\
UPDATE horsies_workflow_tasks \
SET status = 'READY', task_id = NULL, started_at = NULL \
WHERE task_id = ANY($1)";

/// Count active CLAIMED tasks for a specific worker (excludes expired leases).
const COUNT_CLAIMED_FOR_WORKER_SQL: &str = "\
SELECT COUNT(*) FROM horsies_tasks \
WHERE claimed_by_worker_id = $1 AND status = 'CLAIMED' \
  AND (claim_expires_at IS NULL OR claim_expires_at > now())";

// ---------------------------------------------------------------------------
// Task attempt SQL
// ---------------------------------------------------------------------------

/// Lock the RUNNING task row and extract context for attempt recording.
const SELECT_RUNNING_TASK_CONTEXT_SQL: &str = "\
SELECT retry_count, started_at, claimed_by_worker_id,
       worker_hostname, worker_pid, worker_process_name
FROM horsies_tasks
WHERE id = $1 AND status = 'RUNNING'
FOR UPDATE";

/// Upsert a task attempt row (idempotent via ON CONFLICT). Public so recovery can reuse it.
pub const UPSERT_TASK_ATTEMPT_SQL: &str = "\
INSERT INTO horsies_task_attempts (
    task_id, attempt, outcome, will_retry,
    started_at, finished_at,
    error_code, error_message, failed_reason,
    worker_id, worker_hostname, worker_pid, worker_process_name
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (task_id, attempt) DO UPDATE SET
    outcome = EXCLUDED.outcome,
    will_retry = EXCLUDED.will_retry,
    started_at = EXCLUDED.started_at,
    finished_at = EXCLUDED.finished_at,
    error_code = EXCLUDED.error_code,
    error_message = EXCLUDED.error_message,
    failed_reason = EXCLUDED.failed_reason,
    worker_id = EXCLUDED.worker_id,
    worker_hostname = EXCLUDED.worker_hostname,
    worker_pid = EXCLUDED.worker_pid,
    worker_process_name = EXCLUDED.worker_process_name";

/// Query task attempts for a specific task (most recent first).
const SELECT_TASK_ATTEMPTS_SQL: &str = "\
SELECT task_id, attempt, outcome, will_retry,
       started_at, finished_at,
       error_code, error_message, failed_reason,
       worker_id, worker_hostname, worker_pid, worker_process_name
FROM horsies_task_attempts
WHERE task_id = $1
ORDER BY attempt DESC";

// ---------------------------------------------------------------------------
// Monitoring / observability SQL
// ---------------------------------------------------------------------------

/// Find RUNNING tasks whose most recent runner heartbeat (or `started_at`
/// when no heartbeat exists) is older than `$1` minutes.
///
/// Used for operational monitoring; does NOT mutate any rows.
const GET_STALE_TASKS_SQL: &str = "\
SELECT
    t.id,
    t.task_name,
    t.worker_hostname,
    t.worker_pid,
    t.worker_process_name,
    t.started_at,
    hb.last_heartbeat
FROM horsies_tasks t
LEFT JOIN LATERAL (
    SELECT sent_at AS last_heartbeat
    FROM horsies_heartbeats h
    WHERE h.task_id = t.id AND h.role = 'runner'
    ORDER BY sent_at DESC
    LIMIT 1
) hb ON TRUE
WHERE t.status = 'RUNNING'
  AND t.started_at IS NOT NULL
  AND COALESCE(hb.last_heartbeat, t.started_at) < NOW() - $1 * INTERVAL '1 minute'
ORDER BY hb.last_heartbeat NULLS FIRST";

/// Aggregate RUNNING task counts per worker for load visibility.
///
/// Groups by `(worker_hostname, worker_pid, worker_process_name)` and
/// includes the oldest task start time and most recent heartbeat.
/// Latest snapshot per worker (cluster-wide), including idle workers.
///
/// Recursive skip-scan: one `(worker_id, snapshot_at DESC)` index probe per
/// distinct worker (`idx_horsies_worker_states_worker_snapshot`, migration
/// 0018) — the seed term takes the first worker's newest snapshot, each
/// recursive step probes the next `worker_id` boundary. Postgres has no loose
/// index scan, so the previous `DISTINCT ON (worker_id)` form read every
/// retained snapshot in the timeseries to return one row per worker.
/// Parity with horsies PR #170.
const LIST_WORKER_STATES_SQL: &str = "\
WITH RECURSIVE latest AS (
    (
        SELECT
            worker_id, snapshot_at, hostname, pid, processes, max_claim_batch,
            max_claim_per_worker, cluster_wide_cap, queues, queue_priorities,
            queue_max_concurrency, recovery_config, tasks_running, tasks_claimed,
            memory_usage_mb, memory_percent, cpu_percent, worker_started_at
        FROM horsies_worker_states
        ORDER BY worker_id, snapshot_at DESC
        LIMIT 1
    )
    UNION ALL
    SELECT nxt.* FROM latest l
    CROSS JOIN LATERAL (
        SELECT
            w.worker_id, w.snapshot_at, w.hostname, w.pid, w.processes, w.max_claim_batch,
            w.max_claim_per_worker, w.cluster_wide_cap, w.queues, w.queue_priorities,
            w.queue_max_concurrency, w.recovery_config, w.tasks_running, w.tasks_claimed,
            w.memory_usage_mb, w.memory_percent, w.cpu_percent, w.worker_started_at
        FROM horsies_worker_states w
        WHERE w.worker_id > l.worker_id
        ORDER BY w.worker_id, w.snapshot_at DESC
        LIMIT 1
    ) nxt
)
SELECT * FROM latest";

/// Latest snapshot for a single worker.
const GET_WORKER_STATE_LATEST_SQL: &str = "\
SELECT
    worker_id, snapshot_at, hostname, pid, processes, max_claim_batch,
    max_claim_per_worker, cluster_wide_cap, queues, queue_priorities,
    queue_max_concurrency, recovery_config, tasks_running, tasks_claimed,
    memory_usage_mb, memory_percent, cpu_percent, worker_started_at
FROM horsies_worker_states
WHERE worker_id = $1
ORDER BY snapshot_at DESC
LIMIT 1";

/// History for a single worker, newest first. A `NULL` limit (`$2`) returns all
/// retained rows; callers pass an explicit cap to bound the fetch.
const GET_WORKER_STATE_HISTORY_SQL: &str = "\
SELECT
    worker_id, snapshot_at, hostname, pid, processes, max_claim_batch,
    max_claim_per_worker, cluster_wide_cap, queues, queue_priorities,
    queue_max_concurrency, recovery_config, tasks_running, tasks_claimed,
    memory_usage_mb, memory_percent, cpu_percent, worker_started_at
FROM horsies_worker_states
WHERE worker_id = $1
ORDER BY snapshot_at DESC
LIMIT $2";

/// Find PENDING tasks whose `good_until` deadline has passed.
///
/// These tasks will never be claimed because the claim SQL filters on
/// `good_until > now()`. Useful for detecting capacity issues.
const GET_EXPIRED_TASKS_SQL: &str = "\
SELECT
    id,
    task_name,
    queue_name,
    priority,
    sent_at,
    good_until
FROM horsies_tasks
WHERE status = 'PENDING'
  AND good_until IS NOT NULL
  AND good_until <= NOW()
ORDER BY good_until ASC";

/// Health check: `SELECT 1` to verify broker connectivity.
const HEALTH_CHECK_SQL: &str = "SELECT 1";

// Separate session/direct endpoints usually have much tighter connection
// limits than a transaction pool. Horsies needs only migrations plus a small
// number of long-lived LISTEN connections.
const SESSION_POOL_MAX_CONNECTIONS: u32 = 4;

fn pg_connect_options(
    database_url: &str,
    echo: bool,
    pgbouncer_transaction_mode: bool,
) -> Result<PgConnectOptions, BrokerError> {
    let mut connect_options = PgConnectOptions::from_str(database_url)
        .map_err(|e| BrokerError::ConnectionFailed(e.to_string()))?;

    if echo {
        connect_options = connect_options.log_statements(log::LevelFilter::Debug);
    } else {
        connect_options = connect_options.log_statements(log::LevelFilter::Off);
    }

    if pgbouncer_transaction_mode {
        connect_options = connect_options.statement_cache_capacity(0);
    }

    Ok(connect_options)
}

fn pg_pool_options(config: &PostgresConfig) -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(config.pool_size + config.max_overflow)
        .acquire_timeout(Duration::from_secs(config.pool_timeout as u64))
        .idle_timeout(Duration::from_secs(config.pool_recycle as u64))
        .test_before_acquire(config.pool_pre_ping)
}

fn pg_session_pool_options(config: &PostgresConfig) -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(SESSION_POOL_MAX_CONNECTIONS)
        .acquire_timeout(Duration::from_secs(config.pool_timeout as u64))
        .idle_timeout(Duration::from_secs(config.pool_recycle as u64))
        .test_before_acquire(config.pool_pre_ping)
}

fn listener_probe_failed(err: sqlx::Error) -> BrokerError {
    BrokerError::ConnectionFailed(format!(
        "Postgres LISTEN delivery probe failed; session_database_url must be direct/session-capable and able to preserve LISTEN/NOTIFY session state: {err}",
    ))
}

fn prepared_statement_tracking_failed(err: &sqlx::Error) -> bool {
    // String-sniff deliberately: PgBouncer prepared-statement tracking failures
    // do not have one stable SQLSTATE across "missing prepared statement",
    // "already exists", and protocol-level variants.
    err.to_string()
        .to_lowercase()
        .contains("prepared statement")
}

fn prepared_statement_tracking_error(err: sqlx::Error) -> BrokerError {
    BrokerError::ConnectionFailed(format!(
        "PgBouncer transaction mode requires protocol prepared-statement tracking \
         (PgBouncer max_prepared_statements > 0). Configure PgBouncer prepared \
         statement support, or use a direct/session-capable database_url for Rust \
         SQLx clients: {err}",
    ))
}

/// Inputs to one `horsies_claim` pass (parity with horsies PR #160): the
/// serviced queue set with its priority/cap config, the budget knobs, and the
/// advisory lock keys the pass must hold.
#[derive(Debug, Clone)]
pub struct ClaimPassParams {
    pub worker_id: String,
    /// Queues this pass claims from, in claim order.
    pub queues: Vec<String>,
    /// Queue priority per serviced queue (absent entries default to 100
    /// server-side; callers pass the resolved map for parity with Python).
    pub queue_priority: std::collections::HashMap<String, i32>,
    /// `max_concurrency` for the capped queues in `queues` only.
    pub queue_max_concurrency: std::collections::HashMap<String, u32>,
    /// `prefetch_buffer == 0`: budget counts RUNNING + active CLAIMED.
    pub hard_cap_mode: bool,
    pub processes: u32,
    pub prefetch_buffer: u32,
    pub max_claim_per_worker: u32,
    pub max_claim_batch: u32,
    pub cluster_wide_cap: Option<u32>,
    /// `None` claims with `claim_expires_at = NULL` (no lease expiry).
    pub claim_lease_ms: Option<u32>,
    /// Advisory keys for consistent cap accounting; acquired ascending.
    pub lock_keys: Vec<i64>,
}

// ---------------------------------------------------------------------------
// PostgresBroker
// ---------------------------------------------------------------------------

/// PostgreSQL-backed task broker.
///
/// All operations are async and use connection pooling via `sqlx::PgPool`.
pub struct PostgresBroker {
    pool: PgPool,
    session_pool: PgPool,
    pgbouncer_transaction_mode: bool,
    task_done_listener: tokio::sync::OnceCell<SharedNotifyListener>,
    workflow_done_listener: tokio::sync::OnceCell<SharedNotifyListener>,
    listener_delivery_checked: tokio::sync::OnceCell<()>,
    schema_initialized: tokio::sync::OnceCell<()>,
}

impl PostgresBroker {
    /// Construct a broker from an existing pool.
    ///
    /// Useful for tests and for applications that already manage their own
    /// `PgPool` lifecycle.
    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            session_pool: pool.clone(),
            pool,
            pgbouncer_transaction_mode: false,
            task_done_listener: tokio::sync::OnceCell::new(),
            workflow_done_listener: tokio::sync::OnceCell::new(),
            listener_delivery_checked: tokio::sync::OnceCell::new(),
            schema_initialized: tokio::sync::OnceCell::new(),
        }
    }

    /// Connect using a raw database URL.
    pub async fn connect(database_url: &str) -> Result<Self, BrokerError> {
        let pool = PgPool::connect(database_url)
            .await
            .map_err(BrokerError::Database)?;
        Ok(Self::from_pool(pool))
    }

    /// Connect using a `PostgresConfig` from horsies-core.
    pub async fn connect_with(config: &PostgresConfig) -> Result<Self, BrokerError> {
        config
            .validate()
            .map_err(|err| BrokerError::ConnectionFailed(err.to_string()))?;
        let connect_options = pg_connect_options(
            &config.database_url,
            config.echo,
            config.pgbouncer_transaction_mode,
        )?;
        let pool = pg_pool_options(config)
            .connect_with(connect_options)
            .await
            .map_err(BrokerError::Database)?;

        let session_pool = if config.effective_session_database_url() == config.database_url {
            pool.clone()
        } else {
            let session_options =
                pg_connect_options(config.effective_session_database_url(), config.echo, false)?;
            pg_session_pool_options(config)
                .connect_with(session_options)
                .await
                .map_err(BrokerError::Database)?
        };

        Ok(Self {
            pool,
            session_pool,
            pgbouncer_transaction_mode: config.pgbouncer_transaction_mode,
            task_done_listener: tokio::sync::OnceCell::new(),
            workflow_done_listener: tokio::sync::OnceCell::new(),
            listener_delivery_checked: tokio::sync::OnceCell::new(),
            schema_initialized: tokio::sync::OnceCell::new(),
        })
    }

    /// Run embedded SQL migrations.
    ///
    /// Bookkeeps in the horsies-owned `horsies_migrations` table so it never
    /// collides with an application's own `sqlx::migrate!()` runner.
    pub async fn migrate(&self) -> Result<(), BrokerError> {
        crate::broker::migrations::run_horsies_migrations(&self.session_pool).await
    }

    /// Ensure the embedded schema is initialized exactly once for this broker.
    ///
    /// The first successful call runs embedded SQL migrations. Later calls are
    /// a no-op. If initialization fails, the guard remains unset so a future
    /// caller can retry.
    pub async fn ensure_schema_initialized(&self) -> Result<(), BrokerError> {
        self.schema_initialized
            .get_or_try_init(|| async {
                self.migrate().await?;
                Ok::<(), BrokerError>(())
            })
            .await?;
        Ok(())
    }

    /// Get a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get the session-capable pool used for schema and LISTEN/NOTIFY.
    pub fn session_pool(&self) -> &PgPool {
        &self.session_pool
    }

    /// Whether this broker's runtime pool is configured for PgBouncer
    /// transaction pooling.
    pub fn pgbouncer_transaction_mode(&self) -> bool {
        self.pgbouncer_transaction_mode
    }

    /// Shared listener for the `task_done` NOTIFY channel.
    ///
    /// Lazily initialized on first call. All concurrent `get_result()`
    /// callers share a single `PgListener` connection instead of each
    /// creating their own.
    pub async fn task_done_listener(&self) -> Result<&SharedNotifyListener, BrokerError> {
        self.task_done_listener
            .get_or_try_init(|| SharedNotifyListener::new(&self.session_pool, "task_done"))
            .await
    }

    /// Shared listener for the `workflow_done` NOTIFY channel.
    ///
    /// Lazily initialized on first call. Exposed for use by
    /// `horsies-workflow::get_workflow_result()`.
    pub async fn workflow_done_listener(&self) -> Result<&SharedNotifyListener, BrokerError> {
        self.workflow_done_listener
            .get_or_try_init(|| SharedNotifyListener::new(&self.session_pool, "workflow_done"))
            .await
    }

    // -----------------------------------------------------------------------
    // Task lifecycle operations
    // -----------------------------------------------------------------------

    /// Enqueue a new task. Returns the task ID (UUID v4).
    ///
    /// Uses ON CONFLICT DO NOTHING with `enqueue_sha` verification for
    /// idempotent enqueue — matching Python's `enqueue_async()` behaviour.
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue(
        &self,
        task_name: &str,
        args: Option<&str>,
        kwargs: Option<&str>,
        queue_name: &str,
        priority: i32,
        sent_at: Option<DateTime<Utc>>,
        enqueued_at: Option<DateTime<Utc>>,
        good_until: Option<DateTime<Utc>>,
        task_options: Option<&str>,
        enqueue_sha: &str,
        predetermined_task_id: Option<&str>,
    ) -> Result<String, BrokerError> {
        let task_id = predetermined_task_id
            .map(|id| id.to_owned())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let max_retries = parse_max_retries(task_options);
        let effective_sent_at = sent_at.unwrap_or_else(Utc::now);

        let row: Option<(String,)> = sqlx::query_as(ENQUEUE_SQL)
            .bind(&task_id)
            .bind(task_name)
            .bind(queue_name)
            .bind(priority)
            .bind(args)
            .bind(kwargs)
            .bind(effective_sent_at)
            .bind(enqueued_at)
            .bind(good_until)
            .bind(max_retries)
            .bind(task_options)
            .bind(enqueue_sha)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id = %task_id, task_name, queue = queue_name, "task enqueued");
            return Ok(task_id);
        }

        // Conflict: task_id already exists. Verify payload identity via stored SHA.
        tracing::debug!(task_id = %task_id, task_name, "enqueue conflict — verifying enqueue_sha");

        let stored_sha: Option<(String,)> =
            sqlx::query_as("SELECT enqueue_sha FROM horsies_tasks WHERE id = $1")
                .bind(&task_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(BrokerError::Database)?;

        let outcome = classify_enqueue_conflict(
            stored_sha.as_ref().map(|(s,)| s.as_str()),
            enqueue_sha,
            &task_id,
            task_name,
        );
        if matches!(outcome, Err(BrokerError::EnqueueConflictUnverifiable { .. })) {
            tracing::warn!(
                task_id = %task_id,
                task_name,
                "enqueue conflict but row disappeared before verification — cannot verify payload identity",
            );
        }
        outcome
    }

    /// Send a task using pre-resolved enqueue parameters.
    ///
    /// This is the high-level enqueue API. Use [`Horsies::resolve_enqueue()`]
    /// to get a [`ResolvedEnqueue`], then pass it here along with serialized
    /// args.
    ///
    /// If `task_options` contains a `good_until` value, it is forwarded to the
    /// underlying `enqueue()` call so the broker can skip expired tasks.
    ///
    /// Returns a typed [`TaskHandle`] for result retrieval.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let resolved = app.resolve_enqueue("process_image", None, None)?;
    /// let args = serde_json::to_string(&my_args)?;
    /// let handle: TaskHandle<String> = broker.send_task(&resolved, Some(&args), None, None).await?;
    /// ```
    pub async fn send_task<T>(
        self: &Arc<Self>,
        resolved: &ResolvedEnqueue,
        args: Option<&str>,
        kwargs: Option<&str>,
        task_options: Option<&TaskOptions>,
    ) -> TaskSendResult<TaskHandle<T>> {
        let task_options_json = task_options
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| TaskSendError {
                code: TaskSendErrorCode::ValidationFailed,
                message: format!("task_options serialization failed: {}", e),
                retryable: false,
                task_id: None,
                payload: None,
            })?;

        let good_until = task_options.and_then(|opts| opts.good_until);
        let sent_at = Utc::now();
        let pre_task_id = Uuid::new_v4().to_string();

        let enqueue_sha = compute_enqueue_sha(
            &resolved.task_name,
            &resolved.queue_name,
            resolved.priority as i32,
            args,
            kwargs,
            sent_at,
            good_until,
            None, // enqueue_delay_seconds
            task_options_json.as_deref(),
        );

        let payload = TaskSendPayload {
            task_name: resolved.task_name.clone(),
            queue_name: resolved.queue_name.clone(),
            priority: resolved.priority as i32,
            args_json: args.map(|s| s.to_owned()),
            kwargs_json: kwargs.map(|s| s.to_owned()),
            sent_at,
            good_until,
            enqueue_delay_seconds: None,
            task_options: task_options_json.clone(),
            enqueue_sha: enqueue_sha.clone(),
        };

        let task_id = self
            .enqueue(
                &resolved.task_name,
                args,
                kwargs,
                &resolved.queue_name,
                resolved.priority as i32,
                Some(sent_at),
                None, // enqueued_at — DB default NOW()
                good_until,
                task_options_json.as_deref(),
                &enqueue_sha,
                Some(&pre_task_id),
            )
            .await
            .map_err(|e| TaskSendError {
                code: TaskSendErrorCode::EnqueueFailed,
                message: format!("{}", e),
                retryable: e.is_retryable(),
                task_id: Some(pre_task_id.clone()),
                payload: Some(payload.clone()),
            })?;

        Ok(TaskHandle::new(task_id, Arc::clone(self)))
    }

    /// Send a task with automatic retry on transient broker errors.
    ///
    /// When `resend_on_transient_err` is `true`, retries up to 3 times with
    /// exponential backoff (200ms, 400ms, 800ms) on retryable errors.
    /// Mirrors Python's `resend_on_transient_err` behavior.
    pub async fn send_task_with_retry<T>(
        self: &Arc<Self>,
        resolved: &ResolvedEnqueue,
        args: Option<&str>,
        kwargs: Option<&str>,
        task_options: Option<&TaskOptions>,
        resend_on_transient_err: bool,
    ) -> TaskSendResult<TaskHandle<T>> {
        if !resend_on_transient_err {
            return self.send_task(resolved, args, kwargs, task_options).await;
        }
        retry_send(|| self.send_task(resolved, args, kwargs, task_options)).await
    }

    /// Send a task with a delay (scheduled for future execution).
    ///
    /// Identical to [`send_task`](Self::send_task) but sets `enqueued_at` to
    /// `now() + delay`, so the task will not become eligible for claiming until
    /// that time.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// let resolved = app.resolve_enqueue("send_reminder", None, None)?;
    /// let handle: TaskHandle<()> = broker
    ///     .schedule_task(&resolved, None, None, Duration::from_secs(3600), None)
    ///     .await?;
    /// ```
    pub async fn schedule_task<T>(
        self: &Arc<Self>,
        resolved: &ResolvedEnqueue,
        args: Option<&str>,
        kwargs: Option<&str>,
        delay: Duration,
        task_options: Option<&TaskOptions>,
    ) -> TaskSendResult<TaskHandle<T>> {
        let task_options_json = task_options
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| TaskSendError {
                code: TaskSendErrorCode::ValidationFailed,
                message: format!("task_options serialization failed: {}", e),
                retryable: false,
                task_id: None,
                payload: None,
            })?;

        let good_until = task_options.and_then(|opts| opts.good_until);
        let sent_at = Utc::now();
        let delay_secs = delay.as_secs() as i64;
        let enqueued_at = sent_at
            + chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::zero());
        let pre_task_id = Uuid::new_v4().to_string();

        let enqueue_sha = compute_enqueue_sha(
            &resolved.task_name,
            &resolved.queue_name,
            resolved.priority as i32,
            args,
            kwargs,
            sent_at,
            good_until,
            Some(delay_secs),
            task_options_json.as_deref(),
        );

        let payload = TaskSendPayload {
            task_name: resolved.task_name.clone(),
            queue_name: resolved.queue_name.clone(),
            priority: resolved.priority as i32,
            args_json: args.map(|s| s.to_owned()),
            kwargs_json: kwargs.map(|s| s.to_owned()),
            sent_at,
            good_until,
            enqueue_delay_seconds: Some(delay_secs),
            task_options: task_options_json.clone(),
            enqueue_sha: enqueue_sha.clone(),
        };

        let task_id = self
            .enqueue(
                &resolved.task_name,
                args,
                kwargs,
                &resolved.queue_name,
                resolved.priority as i32,
                Some(sent_at),
                Some(enqueued_at),
                good_until,
                task_options_json.as_deref(),
                &enqueue_sha,
                Some(&pre_task_id),
            )
            .await
            .map_err(|e| TaskSendError {
                code: TaskSendErrorCode::EnqueueFailed,
                message: format!("{}", e),
                retryable: e.is_retryable(),
                task_id: Some(pre_task_id.clone()),
                payload: Some(payload.clone()),
            })?;

        Ok(TaskHandle::new(task_id, Arc::clone(self)))
    }

    /// Schedule a task with automatic retry on transient broker errors.
    ///
    /// When `resend_on_transient_err` is `true`, retries up to 3 times with
    /// exponential backoff (200ms, 400ms, 800ms) on retryable errors.
    pub async fn schedule_task_with_retry<T>(
        self: &Arc<Self>,
        resolved: &ResolvedEnqueue,
        args: Option<&str>,
        kwargs: Option<&str>,
        delay: Duration,
        task_options: Option<&TaskOptions>,
        resend_on_transient_err: bool,
    ) -> TaskSendResult<TaskHandle<T>> {
        if !resend_on_transient_err {
            return self
                .schedule_task(resolved, args, kwargs, delay, task_options)
                .await;
        }
        retry_send(|| self.schedule_task(resolved, args, kwargs, delay, task_options)).await
    }

    /// Replay a failed send from its stored payload.
    ///
    /// Only `TaskSendErrorCode::EnqueueFailed` errors are retryable.
    /// The payload contains all pre-serialized fields including the
    /// `enqueue_sha` for idempotent delivery.
    ///
    /// Mirrors Python's `TaskFunction.retry_send(error)`.
    pub async fn retry_send<T>(
        self: &Arc<Self>,
        payload: &TaskSendPayload,
        original_task_id: Option<&str>,
    ) -> Result<TaskHandle<T>, BrokerError> {
        let enqueued_at = payload
            .enqueue_delay_seconds
            .map(|secs| Utc::now() + chrono::Duration::seconds(secs));

        let task_id = self
            .enqueue(
                &payload.task_name,
                payload.args_json.as_deref(),
                payload.kwargs_json.as_deref(),
                &payload.queue_name,
                payload.priority,
                Some(payload.sent_at),
                enqueued_at,
                payload.good_until,
                payload.task_options.as_deref(),
                &payload.enqueue_sha,
                original_task_id,
            )
            .await?;

        Ok(TaskHandle::new(task_id, Arc::clone(self)))
    }

    /// Claim up to `limit` tasks from the given queue.
    ///
    /// Uses `SELECT FOR UPDATE SKIP LOCKED` to avoid contention.
    /// Already-claimed tasks with expired `claim_expires_at` are reclaimed.
    pub async fn claim(
        &self,
        queue_name: &str,
        limit: i32,
        worker_id: &str,
        claim_expires_at: Option<DateTime<Utc>>,
    ) -> Result<Vec<ClaimedTaskRow>, BrokerError> {
        let rows: Vec<ClaimedTaskRow> = sqlx::query_as(CLAIM_SQL)
            .bind(queue_name)
            .bind(limit)
            .bind(worker_id)
            .bind(claim_expires_at)
            .fetch_all(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        tracing::debug!(count = rows.len(), queue = queue_name, "tasks claimed");
        Ok(rows)
    }

    /// Run one collapsed claim pass via `horsies_claim` (parity with horsies
    /// PR #160).
    ///
    /// One server-side statement acquires the advisory locks (ascending key
    /// order), computes all cap accounting under that lock snapshot, and runs
    /// the two-arm windowed claim. The statement executes in its own implicit
    /// transaction, so the xact-scoped locks are released at statement end —
    /// never held across a client round trip.
    pub async fn claim_batch(
        &self,
        params: &ClaimPassParams,
    ) -> Result<Vec<ClaimedTaskRow>, BrokerError> {
        let rows: Vec<ClaimedTaskRow> = sqlx::query_as(HORSIES_CLAIM_SQL)
            .bind(&params.worker_id)
            .bind(sqlx::types::Json(&params.queues))
            .bind(sqlx::types::Json(&params.queue_priority))
            .bind(sqlx::types::Json(&params.queue_max_concurrency))
            .bind(params.hard_cap_mode)
            .bind(i32::try_from(params.processes).unwrap_or(i32::MAX))
            .bind(i32::try_from(params.prefetch_buffer).unwrap_or(i32::MAX))
            .bind(i32::try_from(params.max_claim_per_worker).unwrap_or(i32::MAX))
            .bind(i32::try_from(params.max_claim_batch).unwrap_or(i32::MAX))
            .bind(
                params
                    .cluster_wide_cap
                    .map(|cap| i32::try_from(cap).unwrap_or(i32::MAX)),
            )
            .bind(params.claim_lease_ms.map(i64::from))
            .bind(sqlx::types::Json(&params.lock_keys))
            .fetch_all(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        tracing::debug!(count = rows.len(), "claim pass claimed tasks");
        Ok(rows)
    }

    /// Transition a task from CLAIMED to RUNNING.
    ///
    /// Verifies ownership and checks that the parent workflow (if any)
    /// is not paused or cancelled. Returns `Some(started_at)` on success,
    /// `None` if the task was not in CLAIMED state (e.g. reaper already
    /// handled it, or workflow is PAUSED/CANCELLED).
    pub async fn set_running(
        &self,
        task_id: &str,
        worker_id: &str,
        pid: i32,
        hostname: &str,
        process_name: &str,
    ) -> Result<Option<DateTime<Utc>>, BrokerError> {
        let result: Option<SetRunningRow> = sqlx::query_as(SET_RUNNING_SQL)
            .bind(task_id)
            .bind(pid)
            .bind(hostname)
            .bind(process_name)
            .bind(worker_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        Ok(result.map(|r| r.started_at))
    }

    /// Expire a CLAIMED task whose `good_until` passed before user code started.
    ///
    /// Returns the persisted `TaskResult::Err` JSON when the transition was
    /// applied. Returns `None` if another actor already moved the task.
    pub async fn expire_claimed_task_before_start(
        &self,
        task_id: &str,
        worker_id: &str,
    ) -> Result<Option<String>, BrokerError> {
        let task_error = crate::core::TaskError::builtin(
            crate::core::OutcomeCode::TaskExpired,
            "task expired before execution started (good_until passed)",
        );
        let task_result = crate::core::TaskResult::<serde_json::Value>::Err(task_error);
        let result_json = serde_json::to_string(&task_result)
            .unwrap_or_else(|_| r#"{"__type":"err","value":{"message":"expired"}}"#.to_owned());
        let error_code = "TASK_EXPIRED";

        let updated: Option<crate::broker::row::task::ClaimedId> = sqlx::query_as(
            "UPDATE horsies_tasks \
             SET status = 'EXPIRED', \
                 claimed = FALSE, \
                 claim_expires_at = NULL, \
                 failed_at = NOW(), \
                 result = $1, \
                 error_code = $2, \
                 updated_at = NOW() \
             WHERE id = $3 \
               AND status = 'CLAIMED' \
               AND claimed_by_worker_id = $4 \
               AND good_until IS NOT NULL \
               AND good_until <= NOW() \
             RETURNING id",
        )
        .bind(&result_json)
        .bind(error_code)
        .bind(task_id)
        .bind(worker_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(BrokerError::Database)?;

        Ok(updated.map(|_| result_json))
    }

    /// Mark a task as completed (transactional variant).
    ///
    /// `started_at` fences the CAS to a specific claim generation (`Some` from the
    /// attempt's `set_running`); `None` disables the fence (C10).
    pub async fn complete_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        task_id: &str,
        result_json: Option<&str>,
        worker_id: &str,
        started_at: Option<DateTime<Utc>>,
    ) -> Result<bool, BrokerError> {
        let row: Option<ClaimedId> = sqlx::query_as(COMPLETE_SQL)
            .bind(task_id)
            .bind(result_json)
            .bind(worker_id)
            .bind(started_at)
            .fetch_optional(tx.as_mut())
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, "task completed");
        } else {
            tracing::warn!(
                task_id,
                "task completion skipped: task no longer RUNNING or not owned by this worker"
            );
        }
        Ok(row.is_some())
    }

    /// Fused ok-path finalization for a plain (non-workflow) task: lock the RUNNING
    /// row, upsert its COMPLETED attempt, CAS to COMPLETED, and fire the capacity
    /// wake — one statement, one autocommit transaction (parity with horsies PR
    /// #134). Returns `true` when applied; `false` when the row is no longer
    /// RUNNING/owned by this worker (reaper reclaim), in which case nothing was
    /// written and the capacity notify did not fire.
    pub async fn finalize_completed_fused(
        &self,
        task_id: &str,
        result_json: Option<&str>,
        worker_id: &str,
        notify_channel: &str,
        notify_payload: &str,
        started_at: Option<DateTime<Utc>>,
    ) -> Result<bool, BrokerError> {
        let row: Option<ClaimedId> = sqlx::query_as(FINALIZE_TASK_COMPLETED_SQL)
            .bind(task_id)
            .bind(worker_id)
            .bind(result_json)
            .bind(notify_channel)
            .bind(notify_payload)
            .bind(started_at)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)?;
        Ok(row.is_some())
    }

    /// Mark a task as completed with its serialized result.
    ///
    /// Returns `true` if the update was applied (task was RUNNING).
    /// Returns `false` if the task was no longer RUNNING (e.g., already
    /// marked FAILED by the stale-task reaper).
    pub async fn complete(
        &self,
        task_id: &str,
        result_json: Option<&str>,
        worker_id: &str,
    ) -> Result<bool, BrokerError> {
        // No claim-generation fence on the direct (non-execution) path.
        let row: Option<ClaimedId> = sqlx::query_as(COMPLETE_SQL)
            .bind(task_id)
            .bind(result_json)
            .bind(worker_id)
            .bind(None::<DateTime<Utc>>)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, "task completed");
        } else {
            tracing::warn!(
                task_id,
                "task completion skipped: task no longer RUNNING or not owned by this worker"
            );
        }
        Ok(row.is_some())
    }

    /// Mark a task as FAILED (transactional variant).
    ///
    /// `started_at` fences the CAS to a specific claim generation (`Some` from the
    /// attempt's `set_running`); `None` disables the fence (C10).
    pub async fn fail_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        task_id: &str,
        result_json: Option<&str>,
        error_code: Option<&str>,
        worker_id: &str,
        started_at: Option<DateTime<Utc>>,
    ) -> Result<bool, BrokerError> {
        let row: Option<ClaimedId> = sqlx::query_as(FAIL_TASK_SQL)
            .bind(task_id)
            .bind(result_json)
            .bind(error_code)
            .bind(worker_id)
            .bind(started_at)
            .fetch_optional(tx.as_mut())
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, "task failed");
        } else {
            tracing::warn!(
                task_id,
                "task failure skipped: task no longer RUNNING or not owned by this worker"
            );
        }
        Ok(row.is_some())
    }

    /// Mark a task as FAILED due to a task/user error.
    ///
    /// Sets `result` and `error_code`. Does NOT set `failed_reason`
    /// (that is for worker crashes via [`fail_worker`]).
    pub async fn fail(
        &self,
        task_id: &str,
        result_json: Option<&str>,
        error_code: Option<&str>,
        worker_id: &str,
    ) -> Result<bool, BrokerError> {
        // No claim-generation fence on the direct (non-execution) path.
        let row: Option<ClaimedId> = sqlx::query_as(FAIL_TASK_SQL)
            .bind(task_id)
            .bind(result_json)
            .bind(error_code)
            .bind(worker_id)
            .bind(None::<DateTime<Utc>>)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, "task failed");
        } else {
            tracing::warn!(
                task_id,
                "task failure skipped: task no longer RUNNING or not owned by this worker"
            );
        }
        Ok(row.is_some())
    }

    /// Mark a task as FAILED due to a worker crash.
    ///
    /// Sets `failed_reason` and clears `error_code`. Does NOT set `result`
    /// (that is for task errors via [`fail`]).
    pub async fn fail_worker(
        &self,
        task_id: &str,
        failed_reason: &str,
        worker_id: &str,
    ) -> Result<bool, BrokerError> {
        let row: Option<ClaimedId> = sqlx::query_as(FAIL_WORKER_SQL)
            .bind(task_id)
            .bind(failed_reason)
            .bind(worker_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, "task failed (worker crash)");
        } else {
            tracing::warn!(
                task_id,
                "worker failure skipped: task no longer RUNNING or not owned by this worker"
            );
        }
        Ok(row.is_some())
    }

    /// Cancel a task. Returns `true` if the task was actually cancelled
    /// (not already in a terminal state).
    pub async fn cancel(&self, task_id: &str) -> Result<bool, BrokerError> {
        let result: Option<ClaimedId> = sqlx::query_as(CANCEL_SQL)
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        if result.is_some() {
            tracing::debug!(task_id, "task cancelled");
        }
        Ok(result.is_some())
    }

    /// Requeue a task for retry (transactional variant).
    ///
    /// `retry_count` is incremented from the CAS-locked row itself, so the
    /// caller supplies no count. `started_at` fences the CAS to a specific claim
    /// generation (`Some` from the attempt's `set_running`); `None` disables the
    /// fence (C10).
    pub async fn requeue_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        task_id: &str,
        next_retry_at: Option<DateTime<Utc>>,
        worker_id: &str,
        started_at: Option<DateTime<Utc>>,
    ) -> Result<bool, BrokerError> {
        let row: Option<ClaimedId> = sqlx::query_as(REQUEUE_SQL)
            .bind(task_id)
            .bind(next_retry_at)
            .bind(worker_id)
            .bind(started_at)
            .fetch_optional(tx.as_mut())
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, "task requeued");
        } else {
            tracing::warn!(
                task_id,
                "task requeue skipped: task no longer RUNNING or not owned by this worker"
            );
        }
        Ok(row.is_some())
    }

    /// Requeue a task for retry with an optional delay.
    ///
    /// `retry_count` is incremented from the CAS-locked row itself.
    /// Returns `true` if the update was applied (task was RUNNING).
    /// Returns `false` if the task was no longer RUNNING (e.g., already
    /// marked FAILED by the stale-task reaper).
    pub async fn requeue(
        &self,
        task_id: &str,
        next_retry_at: Option<DateTime<Utc>>,
        worker_id: &str,
    ) -> Result<bool, BrokerError> {
        // No claim-generation fence on the direct (non-execution) path.
        let row: Option<ClaimedId> = sqlx::query_as(REQUEUE_SQL)
            .bind(task_id)
            .bind(next_retry_at)
            .bind(worker_id)
            .bind(None::<DateTime<Utc>>)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, "task requeued");
        } else {
            tracing::warn!(
                task_id,
                "task requeue skipped: task no longer RUNNING or not owned by this worker"
            );
        }
        Ok(row.is_some())
    }

    // -----------------------------------------------------------------------
    // Task attempt recording
    // -----------------------------------------------------------------------

    /// Lock the RUNNING task row and extract context for attempt recording.
    ///
    /// Returns None if the task is no longer RUNNING (reaper reclaimed it).
    pub async fn get_running_task_context(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        task_id: &str,
    ) -> Result<Option<TaskRunningContextRow>, BrokerError> {
        let row: Option<TaskRunningContextRow> = sqlx::query_as(SELECT_RUNNING_TASK_CONTEXT_SQL)
            .bind(task_id)
            .fetch_optional(tx.as_mut())
            .await
            .map_err(BrokerError::Database)?;
        Ok(row)
    }

    /// Record a task attempt (upsert, idempotent).
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_task_attempt(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        task_id: &str,
        attempt: i32,
        outcome: &str,
        will_retry: bool,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        error_code: Option<&str>,
        error_message: Option<&str>,
        failed_reason: Option<&str>,
        worker_id: Option<&str>,
        worker_hostname: Option<&str>,
        worker_pid: Option<i32>,
        worker_process_name: Option<&str>,
    ) -> Result<(), BrokerError> {
        sqlx::query(UPSERT_TASK_ATTEMPT_SQL)
            .bind(task_id)
            .bind(attempt)
            .bind(outcome)
            .bind(will_retry)
            .bind(started_at)
            .bind(finished_at)
            .bind(error_code)
            .bind(error_message)
            .bind(failed_reason)
            .bind(worker_id)
            .bind(worker_hostname)
            .bind(worker_pid)
            .bind(worker_process_name)
            .execute(tx.as_mut())
            .await
            .map_err(BrokerError::Database)?;
        Ok(())
    }

    /// Get task attempts for a specific task (most recent first).
    pub async fn get_task_attempts(
        &self,
        task_id: &str,
    ) -> Result<Vec<TaskAttemptRow>, BrokerError> {
        let rows: Vec<TaskAttemptRow> = sqlx::query_as(SELECT_TASK_ATTEMPTS_SQL)
            .bind(task_id)
            .fetch_all(&self.pool)
            .await
            .map_err(BrokerError::Database)?;
        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // Result retrieval
    // -----------------------------------------------------------------------

    /// Fetch a typed task result, optionally waiting for completion.
    ///
    /// 1. Quick-checks the database for a terminal status.
    /// 2. If not done, subscribes to the shared `task_done` listener and waits.
    /// 3. On timeout, returns `TaskResult::Err(TaskError(WaitTimeout))`.
    ///
    /// All concurrent callers share a single `PgListener` connection via
    /// [`SharedNotifyListener`], avoiding pool exhaustion under load.
    pub async fn get_result<T: DeserializeOwned>(
        &self,
        task_id: &str,
        timeout: Option<Duration>,
    ) -> Result<TaskResult<T>, BrokerError> {
        let start = Instant::now();

        // Quick check — task may already be done, or may not exist.
        if let Some(outcome) = self.poll_result(task_id).await? {
            return Ok(outcome);
        }

        // Subscribe to the shared task_done listener.
        let shared = self.task_done_listener().await?;
        let mut subscription = shared.subscribe(task_id);

        // Re-check after subscribing to avoid a race where the task completes
        // between our first check and the subscribe.
        if let Some(outcome) = self.poll_result(task_id).await? {
            return Ok(outcome);
        }

        let deadline = timeout.map(|t| Instant::now() + t);

        loop {
            // For a timed wait, stop once the deadline has passed (terminal /
            // not-found / WaitTimeout).
            if let Some(d) = deadline {
                if Instant::now() >= d {
                    return self
                        .final_poll_or_timeout(task_id, start.elapsed().as_millis() as u64)
                        .await;
                }
            }

            // Cap each wait at the re-poll interval so a lost NOTIFY (listener
            // reconnect) is recovered by a fresh poll rather than hanging — this
            // is what makes the no-timeout wait safe (C3). Never wait past the
            // deadline for a timed wait.
            let wait = match deadline {
                Some(d) => d
                    .saturating_duration_since(Instant::now())
                    .min(RESULT_WAIT_REPOLL),
                None => RESULT_WAIT_REPOLL,
            };

            match tokio::time::timeout(wait, subscription.recv()).await {
                // NOTIFY delivered, or the re-poll interval elapsed — re-check.
                Ok(Ok(())) | Err(_) => {
                    if let Some(outcome) = self.poll_result(task_id).await? {
                        return Ok(outcome);
                    }
                }
                // Listener error propagates.
                Ok(Err(e)) => return Err(e),
            }
        }
    }

    /// Fetch task metadata.
    ///
    /// By default, the `result` and `failed_reason` fields are excluded
    /// (returned as `None`), matching Python's
    /// `info(include_result=False, include_failed_reason=False)` default.
    /// Pass `true` for either flag to include the corresponding column.
    pub async fn get_task_info(
        &self,
        task_id: &str,
        include_result: bool,
        include_failed_reason: bool,
    ) -> BrokerResult<Option<TaskInfo>> {
        let sql = match (include_result, include_failed_reason) {
            (true, true) => GET_TASK_INFO_SQL,
            (true, false) => GET_TASK_INFO_RESULT_ONLY_SQL,
            (false, true) => GET_TASK_INFO_REASON_ONLY_SQL,
            (false, false) => GET_TASK_INFO_MINIMAL_SQL,
        };

        let row: Option<TaskInfoRow> = sqlx::query_as(sql)
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| BrokerOperationError {
                code: BrokerErrorCode::TaskInfoQueryFailed,
                message: format!("{}", e),
                retryable: is_retryable_sqlx_error(&e),
            })?;

        match row {
            Some(r) => Ok(Some(r.into_task_info().map_err(|e| {
                BrokerOperationError {
                    code: BrokerErrorCode::TaskInfoQueryFailed,
                    message: format!("{}", e),
                    retryable: false,
                }
            })?)),
            None => Ok(None),
        }
    }

    /// Count all RUNNING + CLAIMED tasks across every worker in the cluster.
    ///
    /// Used to enforce `cluster_wide_cap`: the total number of in-flight
    /// tasks globally must not exceed the cap.
    pub async fn count_global_in_flight(&self) -> Result<i64, BrokerError> {
        let count: (i64,) = sqlx::query_as(COUNT_GLOBAL_IN_FLIGHT_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(BrokerError::Database)?;
        Ok(count.0)
    }

    /// Count only RUNNING tasks for a specific worker.
    ///
    /// Used in soft-cap / prefetch mode where budget is based on running
    /// tasks only (claimed-but-not-yet-running tasks are intentionally
    /// excluded to allow prefetch buffering).
    pub async fn count_running_for_worker(&self, worker_id: &str) -> Result<i64, BrokerError> {
        let count: (i64,) = sqlx::query_as(COUNT_RUNNING_FOR_WORKER_SQL)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(BrokerError::Database)?;
        Ok(count.0)
    }

    /// Cluster-wide RUNNING count per queue, for the given queues.
    ///
    /// Backs the soft-cap buffered-dispatch cap re-check: queues with no RUNNING
    /// task are simply absent from the map (count 0). Returns an empty map when
    /// `queues` is empty (no query issued).
    pub async fn count_running_by_queue(
        &self,
        queues: &[String],
    ) -> Result<std::collections::HashMap<String, i64>, BrokerError> {
        if queues.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let rows: Vec<(String, i64)> = sqlx::query_as(COUNT_RUNNING_BY_QUEUE_SQL)
            .bind(queues)
            .fetch_all(&self.pool)
            .await
            .map_err(BrokerError::Database)?;
        Ok(rows.into_iter().collect())
    }

    /// Load CLAIMED tasks owned by a specific worker that are ready to dispatch.
    ///
    /// In prefetch/soft-cap mode, a worker may have CLAIMED tasks buffered
    /// in the database. When a semaphore permit becomes available, these
    /// buffered tasks can be dispatched without a new claim round-trip.
    pub async fn load_buffered_claimed(
        &self,
        worker_id: &str,
        limit: i64,
    ) -> Result<Vec<ClaimedTaskRow>, BrokerError> {
        let rows: Vec<ClaimedTaskRow> = sqlx::query_as(LOAD_BUFFERED_CLAIMED_SQL)
            .bind(worker_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(BrokerError::Database)?;
        Ok(rows)
    }

    /// Get the workflow status for a task if it belongs to a workflow.
    ///
    /// Returns `None` if the task does not belong to any workflow.
    pub async fn get_workflow_status_for_task(
        &self,
        task_id: &str,
    ) -> Result<Option<String>, BrokerError> {
        let row: Option<(String,)> = sqlx::query_as(GET_WORKFLOW_STATUS_FOR_TASK_SQL)
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)?;
        Ok(row.map(|r| r.0))
    }

    /// Handle a workflow stop discovered after the task was claimed but before it started.
    ///
    /// Mirrors Python's child-runner preflight behavior:
    /// - `PAUSED`: unclaim task → `PENDING`, reset `workflow_task` → `READY`
    /// - `CANCELLED`: cancel task → `CANCELLED`, mark `workflow_task` → `SKIPPED`
    pub async fn handle_workflow_stop_before_start(
        &self,
        task_id: &str,
        workflow_status: &str,
    ) -> Result<(), BrokerError> {
        let mut tx = self.pool.begin().await.map_err(BrokerError::Database)?;
        let task_ids = vec![task_id.to_owned()];

        match workflow_status {
            "PAUSED" => {
                // Cancel the claimed-but-not-started row (terminal) instead of
                // returning it to PENDING, then reset the node to READY so resume
                // enqueues a fresh row. Prevents the abandoned row from running as
                // a duplicate after resume (parity with horsies PR #96).
                sqlx::query(CANCEL_PAUSED_TASKS_SQL)
                    .bind(&task_ids)
                    .execute(tx.as_mut())
                    .await
                    .map_err(BrokerError::Database)?;

                sqlx::query(RESET_WORKFLOW_TASKS_SQL)
                    .bind(&task_ids)
                    .execute(tx.as_mut())
                    .await
                    .map_err(BrokerError::Database)?;
            }
            "CANCELLED" => {
                sqlx::query(CANCEL_CANCELLED_WORKFLOW_HORSIES_TASKS_SQL)
                    .bind(&task_ids)
                    .execute(tx.as_mut())
                    .await
                    .map_err(BrokerError::Database)?;

                sqlx::query(SKIP_CANCELLED_WORKFLOW_TASKS_SQL)
                    .bind(&task_ids)
                    .execute(tx.as_mut())
                    .await
                    .map_err(BrokerError::Database)?;
            }
            _ => return Ok(()),
        }

        tx.commit().await.map_err(BrokerError::Database)?;
        tracing::info!(
            task_id,
            workflow_status,
            "handled workflow stop before task start"
        );
        Ok(())
    }

    /// Filter out CLAIMED tasks belonging to non-runnable (PAUSED/CANCELLED) workflows.
    ///
    /// Post-claim guard that handles two cases:
    /// - PAUSED: unclaim task → PENDING, reset workflow_task → READY
    /// - CANCELLED: cancel task → CANCELLED, skip workflow_task → SKIPPED
    ///
    /// Returns the IDs of all filtered tasks (both paused and cancelled) so the
    /// caller can exclude them from dispatch. The cleanup mutations (unclaim /
    /// cancel + workflow_task reset / skip) are scoped to rows this worker
    /// actually owns, so one worker cannot clear or cancel another worker's
    /// claimed rows (mirrors Python's `_filter_non_runnable_workflow_tasks()` /
    /// PR #51).
    pub async fn filter_non_runnable_workflow_tasks(
        &self,
        task_ids: &[String],
        worker_id: &str,
    ) -> Result<Vec<String>, BrokerError> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Find which claimed tasks belong to PAUSED or CANCELLED workflows.
        let rows: Vec<(String, String)> = sqlx::query_as(FIND_NON_RUNNABLE_WORKFLOW_TASKS_SQL)
            .bind(task_ids)
            .fetch_all(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        let mut paused_ids: Vec<String> = Vec::new();
        let mut cancelled_ids: Vec<String> = Vec::new();
        for (task_id, wf_status) in &rows {
            match wf_status.as_str() {
                "PAUSED" => paused_ids.push(task_id.clone()),
                "CANCELLED" => cancelled_ids.push(task_id.clone()),
                _ => {}
            }
        }

        // PAUSED: cancel this worker's own claimed-but-not-started rows (terminal),
        // then reset the workflow_task rows we owned to READY so resume enqueues a
        // fresh row. The cancelled row is never re-claimable, so it can't run as a
        // duplicate of the node after resume.
        if !paused_ids.is_empty() {
            // One transaction: a crash between the task-cancel and the node-reset
            // would otherwise leave a terminal CANCELLED task linked to a live
            // node, which recovery Case 1.7 then completes as a node failure (C15).
            let mut tx = self.pool.begin().await.map_err(BrokerError::Database)?;
            let cancelled: Vec<(String,)> = sqlx::query_as(CANCEL_PAUSED_OWNED_TASKS_SQL)
                .bind(&paused_ids)
                .bind(worker_id)
                .fetch_all(&mut *tx)
                .await
                .map_err(BrokerError::Database)?;
            let owned: Vec<String> = cancelled.into_iter().map(|(id,)| id).collect();

            if !owned.is_empty() {
                sqlx::query(RESET_WORKFLOW_TASKS_SQL)
                    .bind(&owned)
                    .execute(&mut *tx)
                    .await
                    .map_err(BrokerError::Database)?;
            }
            tx.commit().await.map_err(BrokerError::Database)?;

            tracing::debug!(
                count = owned.len(),
                "cancelled own claimed-but-not-started tasks belonging to PAUSED workflows",
            );
        }

        // CANCELLED: hard-cancel this worker's own claimed rows and skip only the
        // workflow_task rows we owned.
        if !cancelled_ids.is_empty() {
            // Same atomicity requirement as the PAUSED branch: task-cancel and
            // node-skip must commit together (C15).
            let mut tx = self.pool.begin().await.map_err(BrokerError::Database)?;
            let cancelled: Vec<(String,)> = sqlx::query_as(CANCEL_CANCELLED_OWNED_TASKS_SQL)
                .bind(&cancelled_ids)
                .bind(worker_id)
                .fetch_all(&mut *tx)
                .await
                .map_err(BrokerError::Database)?;
            let owned: Vec<String> = cancelled.into_iter().map(|(id,)| id).collect();

            if !owned.is_empty() {
                sqlx::query(SKIP_CANCELLED_WORKFLOW_TASKS_SQL)
                    .bind(&owned)
                    .execute(&mut *tx)
                    .await
                    .map_err(BrokerError::Database)?;
            }
            tx.commit().await.map_err(BrokerError::Database)?;

            tracing::debug!(
                count = owned.len(),
                "cancelled own tasks belonging to CANCELLED workflows",
            );
        }

        let mut all_filtered: Vec<String> = paused_ids;
        all_filtered.extend(cancelled_ids);
        Ok(all_filtered)
    }

    /// Count CLAIMED tasks for a specific worker (not yet RUNNING).
    ///
    /// Used for `max_claim_per_worker` guard to prevent over-claiming.
    pub async fn count_claimed_for_worker(&self, worker_id: &str) -> Result<i64, BrokerError> {
        let count: (i64,) = sqlx::query_as(COUNT_CLAIMED_FOR_WORKER_SQL)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(BrokerError::Database)?;
        Ok(count.0)
    }

    /// Unclaim a single task, resetting it from CLAIMED back to PENDING.
    ///
    /// Guarded by `worker_id` so we only unclaim tasks we own — prevents
    /// races where the reaper already requeued the task and another worker
    /// re-claimed it.
    ///
    /// Returns `true` if the task was unclaimed, `false` if it was already
    /// handled (e.g. reaper moved it or another worker claimed it).
    pub async fn unclaim_task(&self, task_id: &str, worker_id: &str) -> Result<bool, BrokerError> {
        let row: Option<(String,)> = sqlx::query_as(UNCLAIM_TASK_SQL)
            .bind(task_id)
            .bind(worker_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, worker_id, "task unclaimed back to PENDING");
        }
        Ok(row.is_some())
    }

    /// Close the connection pool.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    // -----------------------------------------------------------------------
    // Monitoring / observability queries
    // -----------------------------------------------------------------------

    /// Find RUNNING tasks with stale heartbeats (potential worker crashes).
    ///
    /// Returns tasks whose most recent runner heartbeat (or `started_at` if
    /// no heartbeat was ever recorded) is older than `stale_threshold_minutes`.
    ///
    /// Expire unclaimed PENDING tasks whose `good_until` has passed.
    ///
    /// Transitions matching tasks to EXPIRED with a TASK_EXPIRED result.
    /// No attempt rows are written (the task was never executed).
    /// Returns the number of expired tasks.
    ///
    /// Called by the reaper loop, matching Python's `expire_pending_tasks()`.
    pub async fn expire_pending_tasks(&self) -> Result<u64, BrokerError> {
        let task_error = crate::core::TaskError::builtin(
            crate::core::OutcomeCode::TaskExpired,
            "task expired before being claimed (good_until passed)",
        );
        let task_result = crate::core::TaskResult::<()>::Err(task_error);
        let result_json = serde_json::to_string(&task_result)
            .unwrap_or_else(|_| r#"{"__type":"err","value":{"message":"expired"}}"#.to_owned());
        let error_code = "TASK_EXPIRED";

        let result = sqlx::query(
            "UPDATE horsies_tasks \
             SET status = 'EXPIRED', \
                 failed_at = NOW(), \
                 result = $1, \
                 error_code = $2, \
                 updated_at = NOW() \
             WHERE status = 'PENDING' \
               AND good_until IS NOT NULL \
               AND good_until <= NOW()",
        )
        .bind(&result_json)
        .bind(error_code)
        .execute(&self.pool)
        .await
        .map_err(BrokerError::Database)?;

        Ok(result.rows_affected())
    }

    /// Verify runtime-pool connectivity with a cheap `SELECT 1` query.
    ///
    /// Returns `Ok(())` if the database is reachable, `Err(BrokerError)` otherwise.
    /// Mirrors Python's `HEALTH_CHECK_SQL` used by `horsies check --live`.
    ///
    /// This intentionally does not probe LISTEN/NOTIFY. Workers call
    /// `ensure_listener_delivery_checked()` once during startup when configured
    /// for PgBouncer transaction pooling.
    pub async fn health_check(&self) -> Result<(), BrokerError> {
        let _: (i32,) = sqlx::query_as(HEALTH_CHECK_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|err| {
                if self.pgbouncer_transaction_mode && prepared_statement_tracking_failed(&err) {
                    prepared_statement_tracking_error(err)
                } else {
                    BrokerError::Database(err)
                }
            })?;
        Ok(())
    }

    /// Active database liveness probe: a timed `SELECT 1` round-trip.
    ///
    /// Returns a [`DatabasePing`] carrying the measured latency, or a
    /// `DB_PING_FAILED` broker error. Unlike [`health_check`](Self::health_check)
    /// (which callers use as a plain reachability gate), this surfaces latency
    /// for monitoring. Mirrors Python's `ping_database_async`.
    pub async fn ping_database(&self) -> BrokerResult<DatabasePing> {
        let start = std::time::Instant::now();
        let probe: Result<(i32,), sqlx::Error> =
            sqlx::query_as(HEALTH_CHECK_SQL).fetch_one(&self.pool).await;
        match probe {
            Ok(_) => Ok(DatabasePing {
                latency_ms: start.elapsed().as_secs_f64() * 1000.0,
            }),
            Err(e) => Err(BrokerOperationError {
                code: BrokerErrorCode::DbPingFailed,
                message: format!("database ping failed: {}", e),
                retryable: true,
            }),
        }
    }

    /// Active worker liveness probe: broadcast a ping and collect pongs.
    ///
    /// Subscribes to a unique reply channel, broadcasts a [`WorkerPingRequest`]
    /// on [`WORKER_PING_CHANNEL`], then collects distinct [`WorkerPong`] replies
    /// (de-duplicated by `worker_id`) until a stop condition:
    ///
    /// - `target_worker_id` set: returns as soon as that worker replies.
    /// - `min_responses` set: returns as soon as that many distinct workers
    ///   reply (fast fail-open liveness, e.g. `Some(1)` for a `/health` gate — a
    ///   healthy fleet answers in milliseconds; only a degraded fleet pays the
    ///   full `timeout`).
    /// - neither set: waits the full window and enumerates every responder.
    ///
    /// A pong proves the replying worker's event loop is responsive *and* that
    /// it can reach Postgres. Workers present in
    /// [`list_worker_states`](Self::list_worker_states) but absent here are
    /// non-responsive. Mirrors Python's `ping_workers_async`.
    ///
    /// The reply channel uses a dedicated `PgListener` (auto-unsubscribed on
    /// drop) rather than the broker's shared listener.
    pub async fn ping_workers(
        &self,
        target_worker_id: Option<&str>,
        timeout: std::time::Duration,
        min_responses: Option<usize>,
    ) -> BrokerResult<Vec<WorkerPong>> {
        if timeout.is_zero() {
            return Err(BrokerOperationError {
                code: BrokerErrorCode::WorkerPingFailed,
                message: "ping_workers timeout must be positive".to_owned(),
                retryable: false,
            });
        }
        if min_responses == Some(0) {
            return Err(BrokerOperationError {
                code: BrokerErrorCode::WorkerPingFailed,
                message: "ping_workers min_responses must be >= 1 when set".to_owned(),
                retryable: false,
            });
        }

        let correlation_id = Uuid::new_v4().simple().to_string();
        let reply_channel = format!("horsies_worker_pong_{}", correlation_id);

        let mut listener = sqlx::postgres::PgListener::connect_with(self.session_pool())
            .await
            .map_err(|e| worker_ping_error("reply listener connect", e))?;
        listener
            .listen(&reply_channel)
            .await
            .map_err(|e| worker_ping_error("reply subscribe", e))?;

        let request = WorkerPingRequest {
            correlation_id: correlation_id.clone(),
            reply_channel,
            target_worker_id: target_worker_id.map(str::to_owned),
        };
        let payload = serde_json::to_string(&request).map_err(|e| BrokerOperationError {
            code: BrokerErrorCode::WorkerPingFailed,
            message: format!("ping_workers payload encode failed: {}", e),
            retryable: false,
        })?;

        let start = std::time::Instant::now();
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(WORKER_PING_CHANNEL)
            .bind(&payload)
            .execute(self.pool())
            .await
            .map_err(|e| worker_ping_error("ping notify", e))?;

        let mut pongs: Vec<WorkerPong> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        loop {
            let Some(remaining) = timeout.checked_sub(start.elapsed()) else {
                break;
            };
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, listener.recv()).await {
                Err(_elapsed) => break, // window closed
                Ok(Err(e)) => return Err(worker_ping_error("reply recv", e)),
                Ok(Ok(notification)) => {
                    let Some(pong) =
                        decode_pong(notification.payload(), &correlation_id, start.elapsed())
                    else {
                        continue; // malformed or correlation-id mismatch
                    };
                    if !seen.insert(pong.worker_id.clone()) {
                        continue; // duplicate worker
                    }
                    let is_target = target_worker_id == Some(pong.worker_id.as_str());
                    pongs.push(pong);
                    if is_target {
                        break;
                    }
                    if min_responses.is_some_and(|n| pongs.len() >= n) {
                        break;
                    }
                }
            }
        }

        Ok(pongs)
    }

    /// Verify LISTEN/NOTIFY delivery once per broker in PgBouncer mode.
    ///
    /// Reconnect loops may call this repeatedly; successful probes are cached so
    /// transient listener reconnects do not open another direct connection.
    pub async fn ensure_listener_delivery_checked(&self) -> Result<(), BrokerError> {
        if !self.pgbouncer_transaction_mode {
            return Ok(());
        }
        self.listener_delivery_checked
            .get_or_try_init(|| async {
                self.check_listener_delivery().await?;
                Ok::<(), BrokerError>(())
            })
            .await?;
        Ok(())
    }

    /// Verify that the session pool can actually receive LISTEN/NOTIFY.
    ///
    /// PgBouncer transaction mode may accept `LISTEN` syntactically but drop
    /// the session state when the transaction ends. A bounded delivery probe
    /// catches that misconfiguration with a clear startup error.
    pub async fn check_listener_delivery(&self) -> Result<(), BrokerError> {
        let mut listener = sqlx::postgres::PgListener::connect_with(&self.session_pool)
            .await
            .map_err(listener_probe_failed)?;
        let channel = format!("horsies_probe_{}", Uuid::new_v4().simple());
        let payload = Uuid::new_v4().to_string();

        listener
            .listen(&channel)
            .await
            .map_err(listener_probe_failed)?;

        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(&channel)
            .bind(&payload)
            .execute(&self.pool)
            .await
            .map_err(|err| {
                if self.pgbouncer_transaction_mode && prepared_statement_tracking_failed(&err) {
                    prepared_statement_tracking_error(err)
                } else {
                    listener_probe_failed(err)
                }
            })?;

        let delivered = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let notification = listener.recv().await.map_err(listener_probe_failed)?;
                if notification.channel() == channel && notification.payload() == payload {
                    return Ok::<(), BrokerError>(());
                }
            }
        })
        .await;

        let _ = listener.unlisten(&channel).await;

        match delivered {
            Ok(result) => result,
            Err(_) => Err(BrokerError::ConnectionFailed(
                "Postgres LISTEN delivery probe timed out; session_database_url appears to be transaction-pooled or otherwise unable to preserve LISTEN/NOTIFY session state".to_owned(),
            )),
        }
    }

    /// This is a **read-only** query for operational dashboards and alerting.
    /// It does not modify any rows. To actually recover stale tasks, use the
    /// reaper (see `horsies_worker::recovery`).
    ///
    /// Mirrors `PostgresBroker.get_stale_tasks()` in the Python library.
    pub async fn get_stale_tasks(
        &self,
        stale_threshold_minutes: i32,
    ) -> Result<Vec<StaleTaskRow>, BrokerError> {
        let rows: Vec<StaleTaskRow> = sqlx::query_as(GET_STALE_TASKS_SQL)
            .bind(stale_threshold_minutes)
            .fetch_all(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        tracing::debug!(
            count = rows.len(),
            threshold_min = stale_threshold_minutes,
            "stale tasks query"
        );
        Ok(rows)
    }

    /// Latest state snapshot per worker (cluster-wide), including idle workers.
    ///
    /// Reads the `horsies_worker_states` timeseries, so every worker that has
    /// reported a snapshot appears regardless of current load — unlike the
    /// retired `get_worker_stats` (which counted RUNNING tasks only and missed
    /// idle workers). Mirrors Python's `list_worker_states_async`.
    pub async fn list_worker_states(&self) -> BrokerResult<Vec<WorkerStateSnapshot>> {
        sqlx::query_as(LIST_WORKER_STATES_SQL)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| monitoring_query_error("list_worker_states", e))
    }

    /// Latest state snapshot for one worker, or `None` if it has never reported.
    ///
    /// Mirrors Python's `get_worker_state_async`.
    pub async fn get_worker_state(
        &self,
        worker_id: &str,
    ) -> BrokerResult<Option<WorkerStateSnapshot>> {
        sqlx::query_as(GET_WORKER_STATE_LATEST_SQL)
            .bind(worker_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| monitoring_query_error("get_worker_state", e))
    }

    /// State-snapshot history for one worker, newest first.
    ///
    /// `limit` of `None` returns all retained rows; pass `Some(n)` to bound the
    /// fetch. Mirrors Python's `get_worker_state_history_async`.
    pub async fn get_worker_state_history(
        &self,
        worker_id: &str,
        limit: Option<i64>,
    ) -> BrokerResult<Vec<WorkerStateSnapshot>> {
        sqlx::query_as(GET_WORKER_STATE_HISTORY_SQL)
            .bind(worker_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| monitoring_query_error("get_worker_state_history", e))
    }

    /// Find PENDING tasks that have expired (past their `good_until` deadline).
    ///
    /// These tasks will never be picked up by workers because the claim SQL
    /// filters on `good_until > now()`. Surfacing them helps operators detect
    /// under-provisioned queues or misconfigured deadlines.
    ///
    /// Mirrors `PostgresBroker.get_expired_tasks()` in the Python library.
    pub async fn get_expired_tasks(&self) -> Result<Vec<ExpiredTaskRow>, BrokerError> {
        let rows: Vec<ExpiredTaskRow> = sqlx::query_as(GET_EXPIRED_TASKS_SQL)
            .fetch_all(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        tracing::debug!(count = rows.len(), "expired tasks query");
        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    async fn fetch_result_row(&self, task_id: &str) -> Result<Option<TaskResultRow>, BrokerError> {
        sqlx::query_as(GET_RESULT_SQL)
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)
    }

    /// Poll the result row once for the wait loop.
    ///
    /// Returns `Some(outcome)` when the wait should end: the parsed terminal
    /// result, or a typed `TaskNotFound` outcome for a row that does not exist
    /// (pruned by retention, or never present) so the caller never blocks
    /// forever (C3). Returns `None` when the task exists but is not yet terminal.
    async fn poll_result<T: DeserializeOwned>(
        &self,
        task_id: &str,
    ) -> Result<Option<TaskResult<T>>, BrokerError> {
        match self.fetch_result_row(task_id).await? {
            Some(row) if is_terminal_status(&row.status) => parse_task_result_row(row).map(Some),
            Some(_) => Ok(None),
            None => Ok(Some(TaskResult::Err(TaskError::builtin(
                RetrievalCode::TaskNotFound,
                format!("task {} not found", task_id),
            )))),
        }
    }

    async fn final_poll_or_timeout<T: DeserializeOwned>(
        &self,
        task_id: &str,
        elapsed_ms: u64,
    ) -> Result<TaskResult<T>, BrokerError> {
        match self.fetch_result_row(task_id).await? {
            Some(row) if is_terminal_status(&row.status) => parse_task_result_row(row),
            Some(_) => Ok(TaskResult::Err(TaskError::builtin(
                RetrievalCode::WaitTimeout,
                format!("task {} not terminal after {}ms", task_id, elapsed_ms),
            ))),
            None => Ok(TaskResult::Err(TaskError::builtin(
                RetrievalCode::TaskNotFound,
                format!("task {} not found", task_id),
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

fn is_terminal_status(status: &str) -> bool {
    matches!(status, "COMPLETED" | "FAILED" | "CANCELLED" | "EXPIRED")
}

/// Build a `MONITORING_QUERY_FAILED` broker error for a failed read query.
fn monitoring_query_error(operation: &str, err: sqlx::Error) -> BrokerOperationError {
    BrokerOperationError {
        code: BrokerErrorCode::MonitoringQueryFailed,
        message: format!("{} failed: {}", operation, err),
        retryable: true,
    }
}

/// Build a `WORKER_PING_FAILED` broker error for a failed ping-pong step.
fn worker_ping_error(operation: &str, err: sqlx::Error) -> BrokerOperationError {
    BrokerOperationError {
        code: BrokerErrorCode::WorkerPingFailed,
        message: format!("ping_workers {} failed: {}", operation, err),
        retryable: true,
    }
}

/// Decode a pong notification, discarding malformed or mismatched replies.
///
/// Returns `None` when the payload is unparseable or its `correlation_id` does
/// not match this ping round, so stray replies never pollute the result.
fn decode_pong(
    raw_payload: &str,
    correlation_id: &str,
    elapsed: std::time::Duration,
) -> Option<WorkerPong> {
    let payload: WorkerPongPayload = serde_json::from_str(raw_payload).ok()?;
    if payload.correlation_id != correlation_id {
        return None;
    }
    Some(WorkerPong {
        worker_id: payload.worker_id,
        hostname: payload.hostname,
        pid: payload.pid,
        round_trip_ms: elapsed.as_secs_f64() * 1000.0,
    })
}

/// Parse the result row into a `TaskResult<T>`.
///
/// Expects `result` to store a serialized `TaskResult<T>` for COMPLETED/FAILED.
/// Maps database statuses to `TaskResult` variants:
/// - `COMPLETED`/`FAILED` + valid JSON -> decoded `TaskResult<T>`
/// - `COMPLETED`/`FAILED` + null -> `TaskResult::Err(ResultNotAvailable)`
/// - `CANCELLED` -> `TaskResult::Err(TaskError(TaskCancelled))`
#[allow(clippy::needless_pass_by_value)]
fn parse_task_result_row<T: DeserializeOwned>(
    row: TaskResultRow,
) -> Result<TaskResult<T>, BrokerError> {
    match row.status.as_str() {
        "COMPLETED" | "FAILED" => match row.result {
            Some(ref result_json) => {
                let task_result: TaskResult<T> =
                    serde_json::from_str(result_json).map_err(BrokerError::Serialization)?;
                Ok(task_result)
            }
            None => Ok(TaskResult::Err(TaskError::builtin(
                RetrievalCode::ResultNotAvailable,
                format!("task {} completed but result is null", row.id),
            ))),
        },
        "EXPIRED" => match row.result {
            // Expiry writers persist a serialized `TaskResult::Err(TASK_EXPIRED)`;
            // decode it like COMPLETED/FAILED. A NULL result (older rows, or a
            // lost write) still yields the terminal expired outcome instead of a
            // spurious "non-terminal status EXPIRED" broker error (C2).
            Some(ref result_json) => {
                let task_result: TaskResult<T> =
                    serde_json::from_str(result_json).map_err(BrokerError::Serialization)?;
                Ok(task_result)
            }
            None => Ok(TaskResult::Err(TaskError::builtin(
                OutcomeCode::TaskExpired,
                format!("task {} expired", row.id),
            ))),
        },
        "CANCELLED" => Ok(TaskResult::Err(TaskError::builtin(
            OutcomeCode::TaskCancelled,
            format!("task {} was cancelled", row.id),
        ))),
        other => Err(BrokerError::InvalidStatus(format!(
            "task {} has non-terminal status {}",
            row.id, other,
        ))),
    }
}

use crate::core::task::retry_utils::parse_max_retries;

// ---------------------------------------------------------------------------
// Transient retry helper
// ---------------------------------------------------------------------------

/// Re-poll interval for a result wait, bounding each `subscription.recv()` so a
/// NOTIFY lost during a listener reconnect is recovered by a fresh terminal
/// -status poll instead of hanging the caller forever (C3). Shared by the task
/// (`get_result`) and workflow (`get_workflow_result`) wait loops.
pub(crate) const RESULT_WAIT_REPOLL: Duration = Duration::from_secs(30);

/// Retry count for `resend_on_transient_err` (1 initial + 3 retries = 4 total).
const SEND_RETRY_COUNT: u32 = 3;
/// Initial retry delay in ms.
const SEND_RETRY_INITIAL_MS: u64 = 200;
/// Maximum retry delay in ms.
const SEND_RETRY_MAX_MS: u64 = 2000;

/// Retry a send operation on transient errors with exponential backoff.
///
/// Matches Python's retry loop in `task_decorator.py`:
/// - Up to 3 retries (4 total attempts)
/// - Exponential backoff: 200ms, 400ms, 800ms (capped at 2000ms)
/// - Only retries when `TaskSendError.retryable` is `true`
async fn retry_send<T, F, Fut>(mut op: F) -> TaskSendResult<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = TaskSendResult<T>>,
{
    let max_attempts = 1 + SEND_RETRY_COUNT;
    let mut last_err: Option<TaskSendError> = None;

    for attempt in 0..max_attempts {
        if attempt > 0 {
            let delay_ms = std::cmp::min(
                SEND_RETRY_INITIAL_MS * 2u64.pow(attempt - 1),
                SEND_RETRY_MAX_MS,
            );
            tracing::debug!(attempt, delay_ms, "retrying send after transient error",);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }

        match op().await {
            Ok(val) => return Ok(val),
            Err(err) => {
                if err.retryable && attempt < max_attempts - 1 {
                    last_err = Some(err);
                    continue;
                }
                return Err(err);
            }
        }
    }

    // Exhausted all attempts — return the last error.
    Err(last_err.expect("retry loop should have set last_err"))
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    #[test]
    fn terminal_status_check() {
        assert!(is_terminal_status("COMPLETED"));
        assert!(is_terminal_status("FAILED"));
        assert!(is_terminal_status("CANCELLED"));
        assert!(!is_terminal_status("PENDING"));
        assert!(!is_terminal_status("CLAIMED"));
        assert!(!is_terminal_status("RUNNING"));
    }

    #[test]
    fn set_running_guards_good_until_before_start() {
        assert!(
            SET_RUNNING_SQL.contains("AND (good_until IS NULL OR good_until > now())"),
            "CLAIMED -> RUNNING must reject tasks whose deadline has passed",
        );
    }

    #[test]
    fn expired_task_queries_treat_equal_deadline_as_expired() {
        assert!(
            GET_EXPIRED_TASKS_SQL.contains("AND good_until <= NOW()"),
            "good_until is the last valid instant; equality is expired",
        );
    }

    #[test]
    fn parse_completed_result() {
        let wrapped = TaskResult::Ok(42);
        let result_json = serde_json::to_string(&wrapped).unwrap();
        let row = TaskResultRow {
            id: "abc".to_owned(),
            status: "COMPLETED".to_owned(),
            result: Some(result_json),
            failed_reason: None,
        };
        let task_result: TaskResult<i32> = parse_task_result_row(row).unwrap();
        assert!(task_result.is_ok());
        assert_eq!(task_result.unwrap(), 42);
    }

    #[test]
    fn parse_completed_null_result() {
        let row = TaskResultRow {
            id: "abc".to_owned(),
            status: "COMPLETED".to_owned(),
            result: None,
            failed_reason: None,
        };
        let task_result: TaskResult<i32> = parse_task_result_row(row).unwrap();
        assert!(task_result.is_err());
        let err = task_result.unwrap_err();
        assert!(err.message.unwrap().contains("result is null"));
    }

    #[test]
    fn parse_failed_with_task_error() {
        let err = TaskError::new("VALIDATION", "bad input");
        let wrapped: TaskResult<i32> = TaskResult::Err(err);
        let err_json = serde_json::to_string(&wrapped).unwrap();
        let row = TaskResultRow {
            id: "abc".to_owned(),
            status: "FAILED".to_owned(),
            result: Some(err_json),
            failed_reason: Some("bad input".to_owned()),
        };
        let task_result: TaskResult<i32> = parse_task_result_row(row).unwrap();
        assert!(task_result.is_err());
    }

    #[test]
    fn parse_failed_without_result() {
        let row = TaskResultRow {
            id: "abc".to_owned(),
            status: "FAILED".to_owned(),
            result: None,
            failed_reason: Some("something broke".to_owned()),
        };
        let task_result: TaskResult<i32> = parse_task_result_row(row).unwrap();
        assert!(task_result.is_err());
        let err = task_result.unwrap_err();
        assert!(err.message.unwrap().contains("result is null"));
    }

    #[test]
    fn parse_cancelled() {
        let row = TaskResultRow {
            id: "abc".to_owned(),
            status: "CANCELLED".to_owned(),
            result: None,
            failed_reason: None,
        };
        let task_result: TaskResult<i32> = parse_task_result_row(row).unwrap();
        assert!(task_result.is_err());
    }

    #[test]
    fn parse_non_terminal_status_is_error() {
        let row = TaskResultRow {
            id: "abc".to_owned(),
            status: "RUNNING".to_owned(),
            result: None,
            failed_reason: None,
        };
        let result: Result<TaskResult<i32>, BrokerError> = parse_task_result_row(row);
        assert!(matches!(result, Err(BrokerError::InvalidStatus(_))));
    }

    /// C2: an EXPIRED row with a stored `TaskResult::Err(TASK_EXPIRED)` must
    /// decode that stored outcome, not return a spurious `InvalidStatus` error.
    #[test]
    fn parse_expired_with_stored_result() {
        let wrapped: TaskResult<i32> =
            TaskResult::Err(TaskError::builtin(OutcomeCode::TaskExpired, "deadline passed"));
        let result_json = serde_json::to_string(&wrapped).unwrap();
        let row = TaskResultRow {
            id: "abc".to_owned(),
            status: "EXPIRED".to_owned(),
            result: Some(result_json),
            failed_reason: None,
        };
        let task_result: TaskResult<i32> = parse_task_result_row(row).unwrap();
        let err = task_result.unwrap_err();
        let expected = TaskError::builtin(OutcomeCode::TaskExpired, "").error_code;
        assert_eq!(err.error_code, expected);
    }

    /// C2: an EXPIRED row with a NULL result must still yield the terminal
    /// expired outcome (fallback), not an `InvalidStatus` broker error.
    #[test]
    fn parse_expired_null_result() {
        let row = TaskResultRow {
            id: "abc".to_owned(),
            status: "EXPIRED".to_owned(),
            result: None,
            failed_reason: None,
        };
        let task_result: TaskResult<i32> = parse_task_result_row(row).unwrap();
        let err = task_result.unwrap_err();
        let expected = TaskError::builtin(OutcomeCode::TaskExpired, "").error_code;
        assert_eq!(err.error_code, expected);
        assert!(err.message.unwrap().contains("expired"));
    }

    // -----------------------------------------------------------------------
    // Additional parse_task_result_row tests
    // -----------------------------------------------------------------------

    /// PENDING is not terminal; parse_task_result_row should return InvalidStatus.
    #[test]
    fn parse_pending_status_is_error() {
        let row = TaskResultRow {
            id: "xyz".to_owned(),
            status: "PENDING".to_owned(),
            result: None,
            failed_reason: None,
        };
        let result: Result<TaskResult<i32>, BrokerError> = parse_task_result_row(row);
        match result {
            Err(BrokerError::InvalidStatus(msg)) => {
                assert!(msg.contains("xyz"), "expected task id in message: {}", msg);
                assert!(
                    msg.contains("PENDING"),
                    "expected status in message: {}",
                    msg,
                );
            }
            other => panic!("expected InvalidStatus, got: {:?}", other),
        }
    }

    /// CLAIMED is not terminal; parse_task_result_row should return InvalidStatus.
    #[test]
    fn parse_claimed_status_is_error() {
        let row = TaskResultRow {
            id: "claimed-task".to_owned(),
            status: "CLAIMED".to_owned(),
            result: None,
            failed_reason: None,
        };
        let result: Result<TaskResult<i32>, BrokerError> = parse_task_result_row(row);
        assert!(matches!(result, Err(BrokerError::InvalidStatus(_))));
    }

    /// Cancelled tasks should produce TaskResult::Err with TaskCancelled code.
    #[test]
    fn parse_cancelled_produces_err() {
        let row = TaskResultRow {
            id: "cancel-me-123".to_owned(),
            status: "CANCELLED".to_owned(),
            result: None,
            failed_reason: None,
        };
        let task_result: TaskResult<i32> = parse_task_result_row(row).unwrap();
        assert!(task_result.is_err());
        let err = task_result.unwrap_err();
        assert!(err.message.as_deref().unwrap().contains("cancel-me-123"));
    }

    /// Cancelled with leftover result data should still return TaskCancelled
    /// (result column is ignored for CANCELLED status).
    #[test]
    fn parse_cancelled_ignores_result_column() {
        let row = TaskResultRow {
            id: "cancel-with-data".to_owned(),
            status: "CANCELLED".to_owned(),
            result: Some(r#"{"__type":"ok","value":42}"#.to_owned()),
            failed_reason: None,
        };
        let task_result: TaskResult<i32> = parse_task_result_row(row).unwrap();
        assert!(task_result.is_err());
        let err = task_result.unwrap_err();
        assert!(err.message.as_deref().unwrap().contains("cancel-with-data"));
    }

    /// Completed task with a string result type.
    #[test]
    fn parse_completed_string_result() {
        let wrapped = TaskResult::Ok("hello world".to_owned());
        let result_json = serde_json::to_string(&wrapped).unwrap();
        let row = TaskResultRow {
            id: "str-task".to_owned(),
            status: "COMPLETED".to_owned(),
            result: Some(result_json),
            failed_reason: None,
        };
        let task_result: TaskResult<String> = parse_task_result_row(row).unwrap();
        assert_eq!(task_result.unwrap(), "hello world");
    }

    /// Completed task with a complex struct result.
    #[test]
    fn parse_completed_struct_result() {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct Output {
            count: u64,
            label: String,
        }

        let value = Output {
            count: 99,
            label: "items".to_owned(),
        };
        let wrapped = TaskResult::Ok(value.clone());
        let result_json = serde_json::to_string(&wrapped).unwrap();
        let row = TaskResultRow {
            id: "struct-task".to_owned(),
            status: "COMPLETED".to_owned(),
            result: Some(result_json),
            failed_reason: None,
        };
        let task_result: TaskResult<Output> = parse_task_result_row(row).unwrap();
        assert_eq!(task_result.unwrap(), value);
    }

    /// Malformed JSON in result column should produce a Serialization error.
    #[test]
    fn parse_completed_with_malformed_json() {
        let row = TaskResultRow {
            id: "bad-json-task".to_owned(),
            status: "COMPLETED".to_owned(),
            result: Some("this is not json".to_owned()),
            failed_reason: None,
        };
        let result: Result<TaskResult<i32>, BrokerError> = parse_task_result_row(row);
        assert!(
            matches!(result, Err(BrokerError::Serialization(_))),
            "expected Serialization error for malformed JSON, got: {:?}",
            result,
        );
    }

    /// Failed task with malformed JSON should also produce Serialization error.
    #[test]
    fn parse_failed_with_malformed_json() {
        let row = TaskResultRow {
            id: "bad-json-fail".to_owned(),
            status: "FAILED".to_owned(),
            result: Some("{broken".to_owned()),
            failed_reason: Some("oops".to_owned()),
        };
        let result: Result<TaskResult<i32>, BrokerError> = parse_task_result_row(row);
        assert!(matches!(result, Err(BrokerError::Serialization(_))));
    }

    /// Completed with a unit-type result (common for fire-and-forget tasks).
    #[test]
    fn parse_completed_unit_result() {
        let wrapped: TaskResult<()> = TaskResult::Ok(());
        let result_json = serde_json::to_string(&wrapped).unwrap();
        let row = TaskResultRow {
            id: "unit-task".to_owned(),
            status: "COMPLETED".to_owned(),
            result: Some(result_json),
            failed_reason: None,
        };
        let task_result: TaskResult<()> = parse_task_result_row(row).unwrap();
        assert!(task_result.is_ok());
    }

    // -----------------------------------------------------------------------
    // is_terminal_status edge cases
    // -----------------------------------------------------------------------

    /// Empty string is not terminal.
    #[test]
    fn empty_string_not_terminal() {
        assert!(!is_terminal_status(""));
    }

    /// Case sensitivity: lowercase should not match.
    #[test]
    fn terminal_status_is_case_sensitive() {
        assert!(!is_terminal_status("completed"));
        assert!(!is_terminal_status("failed"));
        assert!(!is_terminal_status("cancelled"));
        assert!(!is_terminal_status("Completed"));
    }

    /// EXPIRED is terminal.
    #[test]
    fn expired_is_terminal() {
        assert!(is_terminal_status("EXPIRED"));
    }

    // -----------------------------------------------------------------------
    // send_task / schedule_task compile-time contract checks
    // -----------------------------------------------------------------------

    /// Verify that send_task returns TaskHandle<T> (compile-time check).
    #[test]
    fn send_task_return_type_contract() {
        // This function will never run but must compile, verifying
        // that send_task returns Result<TaskHandle<T>, BrokerError>.
        #[allow(unused, unreachable_code)]
        async fn _check(broker: &Arc<PostgresBroker>) {
            let resolved = ResolvedEnqueue {
                task_name: "my_task".to_owned(),
                queue_name: "default".to_owned(),
                priority: 0,
            };
            let _handle: TaskHandle<i32> =
                broker.send_task(&resolved, None, None, None).await.unwrap();
        }
    }

    /// Verify that schedule_task returns TaskHandle<T> and accepts Duration.
    #[test]
    fn schedule_task_return_type_contract() {
        #[allow(unused, unreachable_code)]
        async fn _check(broker: &Arc<PostgresBroker>) {
            let resolved = ResolvedEnqueue {
                task_name: "delayed_task".to_owned(),
                queue_name: "default".to_owned(),
                priority: 5,
            };
            let delay = Duration::from_secs(60);
            let _handle: TaskHandle<String> = broker
                .schedule_task(&resolved, None, None, delay, None)
                .await
                .unwrap();
        }
    }

    /// Verify that send_task accepts TaskOptions.
    #[test]
    fn send_task_accepts_task_options() {
        #[allow(unused, unreachable_code)]
        async fn _check(broker: &Arc<PostgresBroker>) {
            let resolved = ResolvedEnqueue {
                task_name: "opts_task".to_owned(),
                queue_name: "default".to_owned(),
                priority: 0,
            };
            let opts = TaskOptions {
                task_name: "opts_task".to_owned(),
                queue_name: None,
                good_until: None,
                auto_retry_for: None,
                retry_policy: None,
                timeout_ms: None,
            };
            let _handle: TaskHandle<()> = broker
                .send_task(&resolved, Some("{}"), None, Some(&opts))
                .await
                .unwrap();
        }
    }

    /// Verify that get_result returns Result<TaskResult<T>, BrokerError>.
    #[test]
    fn get_result_return_type_contract() {
        #[allow(unused, unreachable_code)]
        async fn _check(broker: &PostgresBroker) {
            let _result: Result<TaskResult<i32>, BrokerError> = broker
                .get_result("test", Some(Duration::from_secs(5)))
                .await;
        }
    }

    /// Verify that get_task_info returns BrokerResult<Option<TaskInfo>>
    /// and accepts include_result / include_failed_reason filter params.
    #[test]
    fn get_task_info_return_type_contract() {
        #[allow(unused, unreachable_code)]
        async fn _check(broker: &PostgresBroker) {
            let _result: BrokerResult<Option<TaskInfo>> =
                broker.get_task_info("some-task-id", false, false).await;
            let _result: BrokerResult<Option<TaskInfo>> =
                broker.get_task_info("some-task-id", true, false).await;
            let _result: BrokerResult<Option<TaskInfo>> =
                broker.get_task_info("some-task-id", true, true).await;
        }
    }

    /// Verify that ping_database returns BrokerResult<DatabasePing>.
    #[test]
    fn ping_database_return_type_contract() {
        #[allow(unused, unreachable_code)]
        async fn _check(broker: &PostgresBroker) {
            let _result: BrokerResult<DatabasePing> = broker.ping_database().await;
        }
    }

    // -----------------------------------------------------------------------
    // retry_send tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn retry_send_succeeds_on_first_attempt() {
        let result: TaskSendResult<String> = retry_send(|| async { Ok("done".to_owned()) }).await;
        assert_eq!(result.unwrap(), "done");
    }

    #[tokio::test]
    async fn retry_send_retries_on_retryable_error() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let attempts = AtomicU32::new(0);

        let result: TaskSendResult<String> = retry_send(|| {
            let a = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if a < 2 {
                    Err(TaskSendError {
                        code: TaskSendErrorCode::EnqueueFailed,
                        message: "transient".to_owned(),
                        retryable: true,
                        task_id: None,
                        payload: None,
                    })
                } else {
                    Ok("recovered".to_owned())
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), "recovered");
        assert_eq!(attempts.load(Ordering::SeqCst), 3); // 2 failures + 1 success
    }

    #[tokio::test]
    async fn retry_send_does_not_retry_non_retryable() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let attempts = AtomicU32::new(0);

        let result: TaskSendResult<String> = retry_send(|| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async {
                Err(TaskSendError {
                    code: TaskSendErrorCode::ValidationFailed,
                    message: "permanent".to_owned(),
                    retryable: false,
                    task_id: None,
                    payload: None,
                })
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1); // no retries
    }

    #[tokio::test]
    async fn retry_send_exhausts_attempts() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let attempts = AtomicU32::new(0);

        let result: TaskSendResult<String> = retry_send(|| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async {
                Err(TaskSendError {
                    code: TaskSendErrorCode::EnqueueFailed,
                    message: "always fails".to_owned(),
                    retryable: true,
                    task_id: None,
                    payload: None,
                })
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 4); // 1 initial + 3 retries
    }

    // -----------------------------------------------------------------------
    // PostgresBroker trait bounds
    // -----------------------------------------------------------------------

    /// PostgresBroker must be Send + Sync (used behind Arc in TaskHandle
    /// and shared across async tasks).
    #[test]
    fn postgres_broker_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<PostgresBroker>();
        assert_sync::<PostgresBroker>();
    }

    /// Arc<PostgresBroker> must be Send + Sync (used in TaskHandle).
    #[test]
    fn arc_postgres_broker_is_send_sync() {
        use std::sync::Arc;
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Arc<PostgresBroker>>();
        assert_sync::<Arc<PostgresBroker>>();
    }

    // Enqueue-conflict classification (parity with horsies PR #48).

    #[test]
    fn enqueue_conflict_matching_sha_is_idempotent_ok() {
        let out = classify_enqueue_conflict(Some("sha-1"), "sha-1", "task-1", "my_task");
        assert_eq!(out.unwrap(), "task-1");
    }

    #[test]
    fn enqueue_conflict_differing_sha_is_payload_mismatch() {
        let out = classify_enqueue_conflict(Some("other"), "sha-1", "task-1", "my_task");
        assert!(matches!(
            out,
            Err(BrokerError::PayloadMismatch { task_id }) if task_id == "task-1"
        ));
    }

    #[test]
    fn enqueue_conflict_missing_row_is_non_retryable_unverifiable() {
        // Row disappeared before verification: must NOT be treated as idempotent Ok.
        let out = classify_enqueue_conflict(None, "sha-1", "task-1", "my_task");
        match out {
            Err(err @ BrokerError::EnqueueConflictUnverifiable { .. }) => {
                assert!(!err.is_retryable(), "must be non-retryable");
                assert!(
                    err.to_string().contains("cannot verify payload identity"),
                    "message should explain the unverifiable conflict, got: {err}",
                );
            }
            other => panic!("expected EnqueueConflictUnverifiable, got: {other:?}"),
        }
    }
}

/// Canonical UTC datetime string for fingerprint hashing.
/// Matches Python's `_canon_dt()`: always UTC, microsecond precision.
fn canon_dt(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string()
}

/// Classify the outcome of an `INSERT ... ON CONFLICT DO NOTHING` enqueue
/// conflict by comparing the stored `enqueue_sha` against the expected one.
///
/// - `None` (row gone before verification): cannot prove payload identity, so
///   fail non-retryably rather than assume the original send succeeded
///   (parity with horsies PR #48).
/// - matching sha: idempotent success.
/// - differing sha: same task_id, different payload — a programming error.
fn classify_enqueue_conflict(
    stored_sha: Option<&str>,
    enqueue_sha: &str,
    task_id: &str,
    task_name: &str,
) -> Result<String, BrokerError> {
    match stored_sha {
        None => Err(BrokerError::EnqueueConflictUnverifiable {
            task_id: task_id.to_owned(),
            task_name: task_name.to_owned(),
        }),
        Some(existing) if existing == enqueue_sha => Ok(task_id.to_owned()),
        Some(_) => Err(BrokerError::PayloadMismatch {
            task_id: task_id.to_owned(),
        }),
    }
}

/// Compute a SHA-256 hex digest for idempotent enqueue verification.
///
/// Covers all enqueue-identity fields, matching Python's `enqueue_fingerprint()`.
/// Two sends with different priority, sent_at, or good_until produce different hashes.
pub fn compute_enqueue_sha(
    task_name: &str,
    queue_name: &str,
    priority: i32,
    args: Option<&str>,
    kwargs: Option<&str>,
    sent_at: DateTime<Utc>,
    good_until: Option<DateTime<Utc>>,
    enqueue_delay_seconds: Option<i64>,
    task_options: Option<&str>,
) -> String {
    let canonical = serde_json::to_string(&serde_json::json!([
        task_name,
        queue_name,
        priority,
        args,
        kwargs,
        canon_dt(sent_at),
        good_until.map(canon_dt),
        enqueue_delay_seconds,
        task_options,
    ]))
    .expect("JSON serialization of fingerprint fields cannot fail");
    let digest = Sha256::digest(canonical.as_bytes());
    format!("{:x}", digest)
}

#[cfg(test)]
mod horsies_claim_tests {
    //! Contract pins for the collapsed server-side claim (parity with horsies
    //! PR #160 tests `463e665`): selection order, per-queue/cluster caps,
    //! max_claim_batch, max_claim_per_worker guard, expired-CLAIMED reclaim,
    //! nullable lease.
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

    /// Connect and ensure migrations (incl. `horsies_claim`) are applied.
    async fn connect_migrated() -> PostgresBroker {
        let broker = PostgresBroker::connect(&test_db_url())
            .await
            .expect("connect");
        broker.migrate().await.expect("migrate");
        broker
    }

    /// Insert a task; `lease_offset_secs` (negative = expired) and `owner`
    /// only matter for CLAIMED/RUNNING rows.
    async fn seed(
        pool: &sqlx::PgPool,
        queue: &str,
        status: &str,
        priority: i32,
        owner: Option<&str>,
        lease_offset_secs: Option<i64>,
    ) -> String {
        let id = Uuid::new_v4().to_string();
        let expires = lease_offset_secs.map(|s| Utc::now() + chrono::Duration::seconds(s));
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs,
                status, sent_at, enqueued_at, max_retries,
                enqueue_sha, claimed_by_worker_id, claim_expires_at,
                is_workflow_task, created_at, updated_at
            ) VALUES (
                $1, 'pr160_task', $2, $3, '[]', '{}',
                $4, NOW(), NOW(), 0,
                $1, $5, $6,
                FALSE, NOW(), NOW()
            )",
        )
        .bind(&id)
        .bind(queue)
        .bind(priority)
        .bind(status)
        .bind(owner)
        .bind(expires)
        .execute(pool)
        .await
        .expect("seed task");
        id
    }

    fn base_params(worker_id: &str, queues: &[String]) -> ClaimPassParams {
        ClaimPassParams {
            worker_id: worker_id.to_owned(),
            queues: queues.to_vec(),
            queue_priority: queues.iter().map(|q| (q.clone(), 100)).collect(),
            queue_max_concurrency: std::collections::HashMap::new(),
            hard_cap_mode: true,
            processes: 10,
            prefetch_buffer: 0,
            max_claim_per_worker: 0,
            max_claim_batch: 0,
            cluster_wide_cap: None,
            claim_lease_ms: Some(60_000),
            lock_keys: Vec::new(),
        }
    }

    async fn cleanup(pool: &sqlx::PgPool, queues: &[String]) {
        sqlx::query("DELETE FROM horsies_tasks WHERE queue_name = ANY($1)")
            .bind(queues)
            .execute(pool)
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    #[serial]
    async fn budget_one_picks_higher_priority_queue_then_task_priority() {
        let broker = connect_migrated().await;
        let pool = broker.pool().clone();
        let suffix = Uuid::new_v4().simple().to_string();
        let wid = format!("w-{suffix}");
        let qa = format!("pr160_a_{suffix}");
        let qb = format!("pr160_b_{suffix}");
        let queues = vec![qa.clone(), qb.clone()];

        // qb carries the better task priority (0 vs 5), but qa has the better
        // QUEUE priority — queue rank must win the global ordering.
        seed(&pool, &qa, "PENDING", 5, None, None).await;
        let qb_id = seed(&pool, &qb, "PENDING", 0, None, None).await;

        let mut params = base_params(&wid, &queues);
        params.processes = 1; // budget of exactly 1
        params.queue_priority.insert(qa.clone(), 10);
        params.queue_priority.insert(qb.clone(), 20);

        let rows = broker.claim_batch(&params).await.expect("claim");
        assert_eq!(rows.len(), 1, "budget of 1 claims exactly one row");
        assert_eq!(rows[0].queue_name, qa, "queue priority outranks task priority");

        // Same budget, equal queue priorities: task priority decides.
        cleanup(&pool, &queues).await;
        seed(&pool, &qa, "PENDING", 5, None, None).await;
        let qb_id2 = seed(&pool, &qb, "PENDING", 0, None, None).await;
        let params_flat = {
            let mut p = base_params(&wid, &queues);
            p.processes = 1;
            p.worker_id = format!("w2-{suffix}");
            p
        };
        let rows = broker.claim_batch(&params_flat).await.expect("claim");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, qb_id2, "equal queue rank falls back to task priority");

        let _ = qb_id;
        cleanup(&pool, &queues).await;
    }

    #[tokio::test]
    #[serial]
    async fn per_queue_cap_subtracts_in_flight_and_never_over_claims() {
        let broker = connect_migrated().await;
        let pool = broker.pool().clone();
        let suffix = Uuid::new_v4().simple().to_string();
        let wid = format!("w-{suffix}");
        let qa = format!("pr160_cap_{suffix}");
        let queues = vec![qa.clone()];

        // Cap 2 with one RUNNING (other worker) -> exactly 1 claimable.
        seed(&pool, &qa, "RUNNING", 0, Some("other-worker"), None).await;
        for _ in 0..5 {
            seed(&pool, &qa, "PENDING", 0, None, None).await;
        }

        let mut params = base_params(&wid, &queues);
        params.queue_max_concurrency.insert(qa.clone(), 2);

        let rows = broker.claim_batch(&params).await.expect("claim");
        assert_eq!(rows.len(), 1, "cap 2 minus 1 in-flight = 1 claim");

        // A second pass sees cap exhausted (1 RUNNING + 1 CLAIMED) -> 0.
        let rows = broker.claim_batch(&params).await.expect("claim");
        assert!(rows.is_empty(), "cap exhausted, nothing claimed");

        cleanup(&pool, &queues).await;
    }

    #[tokio::test]
    #[serial]
    async fn cluster_cap_limits_total_claims() {
        let broker = connect_migrated().await;
        let pool = broker.pool().clone();
        let suffix = Uuid::new_v4().simple().to_string();
        let wid = format!("w-{suffix}");
        let qa = format!("pr160_cluster_{suffix}");
        let queues = vec![qa.clone()];

        for _ in 0..5 {
            seed(&pool, &qa, "PENDING", 0, None, None).await;
        }

        // The global in-flight count spans the whole table; anchor the cap on
        // the current value so leftover rows from other suites don't skew it.
        let global_now = broker.count_global_in_flight().await.expect("count") as u32;
        let mut params = base_params(&wid, &queues);
        params.cluster_wide_cap = Some(global_now + 2);

        let rows = broker.claim_batch(&params).await.expect("claim");
        assert_eq!(rows.len(), 2, "cluster cap leaves room for exactly 2");

        cleanup(&pool, &queues).await;
    }

    #[tokio::test]
    #[serial]
    async fn max_claim_batch_caps_each_queue() {
        let broker = connect_migrated().await;
        let pool = broker.pool().clone();
        let suffix = Uuid::new_v4().simple().to_string();
        let wid = format!("w-{suffix}");
        let qa = format!("pr160_batch_{suffix}");
        let queues = vec![qa.clone()];

        for _ in 0..5 {
            seed(&pool, &qa, "PENDING", 0, None, None).await;
        }

        let mut params = base_params(&wid, &queues);
        params.max_claim_batch = 2;

        let rows = broker.claim_batch(&params).await.expect("claim");
        assert_eq!(rows.len(), 2, "max_claim_batch bounds the per-queue window");

        cleanup(&pool, &queues).await;
    }

    #[tokio::test]
    #[serial]
    async fn expired_claimed_row_is_reclaimed() {
        let broker = connect_migrated().await;
        let pool = broker.pool().clone();
        let suffix = Uuid::new_v4().simple().to_string();
        let wid = format!("w-{suffix}");
        let qa = format!("pr160_exp_{suffix}");
        let queues = vec![qa.clone()];

        let expired_id =
            seed(&pool, &qa, "CLAIMED", 0, Some("dead-worker"), Some(-600)).await;
        // Active claim by another worker must NOT be reclaimed.
        seed(&pool, &qa, "CLAIMED", 0, Some("live-worker"), Some(600)).await;

        let params = base_params(&wid, &queues);
        let rows = broker.claim_batch(&params).await.expect("claim");
        assert_eq!(rows.len(), 1, "only the expired claim is eligible");
        assert_eq!(rows[0].id, expired_id);

        let owner: (Option<String>,) =
            sqlx::query_as("SELECT claimed_by_worker_id FROM horsies_tasks WHERE id = $1")
                .bind(&expired_id)
                .fetch_one(&pool)
                .await
                .expect("read owner");
        assert_eq!(owner.0.as_deref(), Some(wid.as_str()));

        cleanup(&pool, &queues).await;
    }

    #[tokio::test]
    #[serial]
    async fn null_lease_claims_without_expiry_and_lease_sets_it() {
        let broker = connect_migrated().await;
        let pool = broker.pool().clone();
        let suffix = Uuid::new_v4().simple().to_string();
        let wid = format!("w-{suffix}");
        let qa = format!("pr160_lease_{suffix}");
        let queues = vec![qa.clone()];

        let a = seed(&pool, &qa, "PENDING", 0, None, None).await;
        let b = seed(&pool, &qa, "PENDING", 0, None, None).await;

        let mut params = base_params(&wid, &queues);
        params.claim_lease_ms = None;
        params.max_claim_batch = 1;
        let rows = broker.claim_batch(&params).await.expect("claim");
        assert_eq!(rows.len(), 1);

        params.claim_lease_ms = Some(60_000);
        let rows2 = broker.claim_batch(&params).await.expect("claim");
        assert_eq!(rows2.len(), 1);

        let leases: Vec<(String, Option<DateTime<Utc>>)> = sqlx::query_as(
            "SELECT id, claim_expires_at FROM horsies_tasks WHERE id = ANY($1)",
        )
        .bind(vec![a.clone(), b.clone()])
        .fetch_all(&pool)
        .await
        .expect("read leases");
        let lease_of = |id: &str| {
            leases
                .iter()
                .find(|(row_id, _)| row_id == id)
                .map(|(_, lease)| *lease)
                .expect("row present")
        };
        assert!(
            lease_of(&rows[0].id).is_none(),
            "NULL p_lease_ms claims with claim_expires_at NULL",
        );
        assert!(
            lease_of(&rows2[0].id).is_some(),
            "explicit lease sets claim_expires_at",
        );

        cleanup(&pool, &queues).await;
    }

    #[tokio::test]
    #[serial]
    async fn max_claim_per_worker_guard_returns_empty() {
        let broker = connect_migrated().await;
        let pool = broker.pool().clone();
        let suffix = Uuid::new_v4().simple().to_string();
        let wid = format!("w-{suffix}");
        let qa = format!("pr160_guard_{suffix}");
        let queues = vec![qa.clone()];

        // Worker already holds 2 active claims; guard is 2 -> nothing claimed.
        seed(&pool, &qa, "CLAIMED", 0, Some(&wid), Some(600)).await;
        seed(&pool, &qa, "CLAIMED", 0, Some(&wid), Some(600)).await;
        seed(&pool, &qa, "PENDING", 0, None, None).await;

        let mut params = base_params(&wid, &queues);
        params.max_claim_per_worker = 2;

        let rows = broker.claim_batch(&params).await.expect("claim");
        assert!(rows.is_empty(), "max_claim_per_worker guard short-circuits");

        cleanup(&pool, &queues).await;
    }

    #[tokio::test]
    #[serial]
    async fn soft_cap_budget_subtracts_running_and_claimed() {
        let broker = connect_migrated().await;
        let pool = broker.pool().clone();
        let suffix = Uuid::new_v4().simple().to_string();
        let wid = format!("w-{suffix}");
        let qa = format!("pr160_soft_{suffix}");
        let queues = vec![qa.clone()];

        // budget = (processes 1 + prefetch 2) - RUNNING 1 - CLAIMED 1 = 1.
        seed(&pool, &qa, "RUNNING", 0, Some(&wid), None).await;
        seed(&pool, &qa, "CLAIMED", 0, Some(&wid), Some(600)).await;
        for _ in 0..3 {
            seed(&pool, &qa, "PENDING", 0, None, None).await;
        }

        let mut params = base_params(&wid, &queues);
        params.hard_cap_mode = false;
        params.processes = 1;
        params.prefetch_buffer = 2;

        let rows = broker.claim_batch(&params).await.expect("claim");
        assert_eq!(rows.len(), 1, "soft budget = concurrency + prefetch - running - claimed");

        cleanup(&pool, &queues).await;
    }
}

#[cfg(test)]
mod fused_finalize_tests {
    //! Behavior pins for the fused ok-path finalize (parity with horsies PR #134):
    //! one statement CASes the RUNNING row to COMPLETED, writes the COMPLETED
    //! attempt from the locked row, and (on a real row) fires the capacity notify.
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

    /// Seed a task in `status`, optionally claimed by `worker`, with `retry_count`.
    async fn seed(pool: &sqlx::PgPool, id: &str, status: &str, worker: Option<&str>, retry: i32) {
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs,
                status, sent_at, enqueued_at, started_at, max_retries, retry_count,
                enqueue_sha, claimed_by_worker_id, worker_hostname, worker_pid,
                worker_process_name, is_workflow_task, created_at, updated_at
            ) VALUES (
                $1, 'fused_task', 'default', 0, '[]', '{}',
                $2, NOW(), NOW(), NOW(), 3, $4,
                $1, $3, 'host1', 123,
                'worker-123', FALSE, NOW(), NOW()
            )",
        )
        .bind(id)
        .bind(status)
        .bind(worker)
        .bind(retry)
        .execute(pool)
        .await
        .expect("seed task");
    }

    async fn cleanup(pool: &sqlx::PgPool, id: &str) {
        sqlx::query("DELETE FROM horsies_task_attempts WHERE task_id = $1")
            .bind(id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    #[serial]
    async fn fused_finalize_completes_running_task() {
        let broker = PostgresBroker::connect(&test_db_url()).await.expect("connect");
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), 1).await;

        let applied = broker
            .finalize_completed_fused(&id, Some("{\"Ok\":7}"), "w1", "task_queue_default", "capacity:x", None)
            .await
            .expect("fused finalize");
        assert!(applied, "RUNNING owned task must finalize");

        let (status, result): (String, Option<String>) =
            sqlx::query_as("SELECT status, result FROM horsies_tasks WHERE id = $1")
                .bind(&id)
                .fetch_one(&pool)
                .await
                .expect("read task");
        assert_eq!(status, "COMPLETED");
        assert_eq!(result.as_deref(), Some("{\"Ok\":7}"));

        let (attempt, outcome, will_retry): (i32, String, bool) = sqlx::query_as(
            "SELECT attempt, outcome, will_retry FROM horsies_task_attempts WHERE task_id = $1",
        )
        .bind(&id)
        .fetch_one(&pool)
        .await
        .expect("read attempt");
        assert_eq!(attempt, 2, "attempt = retry_count(1) + 1");
        assert_eq!(outcome, "COMPLETED");
        assert!(!will_retry);

        cleanup(&pool, &id).await;
    }

    #[tokio::test]
    #[serial]
    async fn fused_finalize_noop_when_not_running() {
        let broker = PostgresBroker::connect(&test_db_url()).await.expect("connect");
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "COMPLETED", Some("w1"), 0).await;

        let applied = broker
            .finalize_completed_fused(&id, Some("{\"Ok\":1}"), "w1", "task_queue_default", "capacity:x", None)
            .await
            .expect("fused finalize");
        assert!(!applied, "non-RUNNING row must not be touched");

        let attempts: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM horsies_task_attempts WHERE task_id = $1")
                .bind(&id)
                .fetch_one(&pool)
                .await
                .expect("count attempts");
        assert_eq!(attempts, 0, "no attempt row written on no-op");

        cleanup(&pool, &id).await;
    }

    #[tokio::test]
    #[serial]
    async fn fused_finalize_noop_on_wrong_worker() {
        let broker = PostgresBroker::connect(&test_db_url()).await.expect("connect");
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), 0).await;

        let applied = broker
            .finalize_completed_fused(&id, Some("{\"Ok\":1}"), "w2", "task_queue_default", "capacity:x", None)
            .await
            .expect("fused finalize");
        assert!(!applied, "ownership mismatch must not finalize");

        let status: String = sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(&id)
            .fetch_one(&pool)
            .await
            .expect("status");
        assert_eq!(status, "RUNNING", "row untouched");

        cleanup(&pool, &id).await;
    }

    /// C10: the fused finalize must be fenced to a claim generation. A stale
    /// finalize carrying an earlier attempt's `started_at` must not complete a
    /// task the same worker re-claimed (new `started_at`) after a reaper requeue.
    #[tokio::test]
    #[serial]
    async fn fused_finalize_fenced_by_started_at() {
        let broker = PostgresBroker::connect(&test_db_url()).await.expect("connect");
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), 0).await;

        // The current claim generation is the row's actual started_at.
        let current: DateTime<Utc> =
            sqlx::query_scalar("SELECT started_at FROM horsies_tasks WHERE id = $1")
                .bind(&id)
                .fetch_one(&pool)
                .await
                .expect("started_at");
        let stale = current - chrono::Duration::minutes(1);

        // Stale generation: fenced out, row stays RUNNING.
        let applied = broker
            .finalize_completed_fused(
                &id,
                Some("{\"Ok\":1}"),
                "w1",
                "task_queue_default",
                "capacity:x",
                Some(stale),
            )
            .await
            .expect("fused finalize");
        assert!(!applied, "stale started_at must be fenced out");
        let status: String = sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(&id)
            .fetch_one(&pool)
            .await
            .expect("status");
        assert_eq!(status, "RUNNING", "row must stay RUNNING under a stale fence");

        // Current generation: finalizes.
        let applied = broker
            .finalize_completed_fused(
                &id,
                Some("{\"Ok\":2}"),
                "w1",
                "task_queue_default",
                "capacity:x",
                Some(current),
            )
            .await
            .expect("fused finalize");
        assert!(applied, "matching started_at must finalize");
        let status: String = sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(&id)
            .fetch_one(&pool)
            .await
            .expect("status");
        assert_eq!(status, "COMPLETED");

        cleanup(&pool, &id).await;
    }

    /// P7: `load_buffered_claimed` bounds its fetch to the requested limit, since
    /// no more than the available permits can be dispatched in one pass.
    #[tokio::test]
    #[serial]
    async fn load_buffered_claimed_respects_limit() {
        let broker = PostgresBroker::connect(&test_db_url()).await.expect("connect");
        let pool = broker.pool().clone();
        let mut ids = Vec::new();
        for _ in 0..5 {
            let id = Uuid::new_v4().to_string();
            seed(&pool, &id, "CLAIMED", Some("p7_worker"), 0).await;
            ids.push(id);
        }

        let rows = broker
            .load_buffered_claimed("p7_worker", 2)
            .await
            .expect("load buffered");
        assert_eq!(rows.len(), 2, "fetch must be bounded by the limit");

        for id in &ids {
            cleanup(&pool, id).await;
        }
    }

    /// C10: the retry requeue CAS is likewise fenced by `started_at`, so a stale
    /// attempt cannot requeue a task the worker is re-running under a new claim.
    #[tokio::test]
    #[serial]
    async fn requeue_in_tx_fenced_by_started_at() {
        let broker = PostgresBroker::connect(&test_db_url()).await.expect("connect");
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), 0).await;

        let current: DateTime<Utc> =
            sqlx::query_scalar("SELECT started_at FROM horsies_tasks WHERE id = $1")
                .bind(&id)
                .fetch_one(&pool)
                .await
                .expect("started_at");
        let stale = current - chrono::Duration::minutes(1);

        // Stale generation: fenced out.
        let mut tx = pool.begin().await.expect("begin");
        let applied = broker
            .requeue_in_tx(&mut tx, &id, Some(Utc::now()), "w1", Some(stale))
            .await
            .expect("requeue");
        tx.commit().await.expect("commit");
        assert!(!applied, "stale started_at must not requeue");

        // Current generation: applies.
        let mut tx = pool.begin().await.expect("begin");
        let applied = broker
            .requeue_in_tx(&mut tx, &id, Some(Utc::now()), "w1", Some(current))
            .await
            .expect("requeue");
        tx.commit().await.expect("commit");
        assert!(applied, "matching started_at must requeue");
        let status: String = sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(&id)
            .fetch_one(&pool)
            .await
            .expect("status");
        assert_eq!(status, "PENDING");

        cleanup(&pool, &id).await;
    }
}

#[cfg(test)]
mod set_running_heartbeat_tests {
    //! Pin that the CLAIMED -> RUNNING transition writes the first runner
    //! heartbeat atomically (parity with horsies PR #134): an applied transition
    //! leaves a 'runner' heartbeat row; a non-applied one writes none.
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

    async fn seed_claimed(pool: &sqlx::PgPool, id: &str, worker: &str) {
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs,
                status, sent_at, enqueued_at, claimed, claimed_at, claimed_by_worker_id,
                max_retries, enqueue_sha, is_workflow_task, created_at, updated_at
            ) VALUES (
                $1, 'hb_task', 'default', 0, '[]', '{}',
                'CLAIMED', NOW(), NOW(), TRUE, NOW(), $2,
                3, $1, FALSE, NOW(), NOW()
            )",
        )
        .bind(id)
        .bind(worker)
        .execute(pool)
        .await
        .expect("seed claimed");
    }

    async fn runner_beats(pool: &sqlx::PgPool, id: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_heartbeats WHERE task_id = $1 AND role = 'runner'",
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count beats")
    }

    async fn cleanup(pool: &sqlx::PgPool, id: &str) {
        sqlx::query("DELETE FROM horsies_heartbeats WHERE task_id = $1")
            .bind(id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    #[serial]
    async fn running_transition_writes_first_heartbeat() {
        let broker = PostgresBroker::connect(&test_db_url()).await.expect("connect");
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed_claimed(&pool, &id, "w1").await;

        let started = broker
            .set_running(&id, "w1", 321, "host1", "worker")
            .await
            .expect("set_running");
        assert!(started.is_some(), "transition must apply");
        assert_eq!(runner_beats(&pool, &id).await, 1, "exactly one fused first beat");

        cleanup(&pool, &id).await;
    }

    #[tokio::test]
    #[serial]
    async fn non_applied_transition_writes_no_heartbeat() {
        let broker = PostgresBroker::connect(&test_db_url()).await.expect("connect");
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed_claimed(&pool, &id, "w1").await;

        // Ownership mismatch → transition does not apply.
        let started = broker
            .set_running(&id, "w2", 321, "host1", "worker")
            .await
            .expect("set_running");
        assert!(started.is_none(), "transition must not apply for wrong worker");
        assert_eq!(runner_beats(&pool, &id).await, 0, "no orphan beat on non-applied transition");

        cleanup(&pool, &id).await;
    }

    /// C21: `horsies_heartbeats.id` is BIGINT (migration 0020). `HeartbeatRow.id`
    /// must be `i64` so `query_as` decodes it — including ids past `i32::MAX`.
    /// With the old `i32` field, sqlx errors decoding the INT8 column.
    #[tokio::test]
    #[serial]
    async fn heartbeat_row_decodes_bigint_id() {
        use crate::broker::row::heartbeat::HeartbeatRow;

        let broker = PostgresBroker::connect(&test_db_url()).await.expect("connect");
        let pool = broker.pool().clone();
        let big_id: i64 = 3_000_000_000; // > i32::MAX
        let task_id = Uuid::new_v4().to_string();

        sqlx::query(
            "INSERT INTO horsies_heartbeats (id, task_id, sender_id, role, sent_at, hostname, pid) \
             VALUES ($1, $2, 'w1', 'runner', NOW(), 'h1', 1)",
        )
        .bind(big_id)
        .bind(&task_id)
        .execute(&pool)
        .await
        .expect("insert heartbeat");

        let row: HeartbeatRow = sqlx::query_as(
            "SELECT id, task_id, sender_id, role, sent_at, hostname, pid \
             FROM horsies_heartbeats WHERE id = $1",
        )
        .bind(big_id)
        .fetch_one(&pool)
        .await
        .expect("HeartbeatRow must decode a BIGINT id");
        assert_eq!(row.id, big_id);

        sqlx::query("DELETE FROM horsies_heartbeats WHERE id = $1")
            .bind(big_id)
            .execute(&pool)
            .await
            .ok();
    }
}

#[cfg(test)]
mod get_result_wait_tests {
    //! C3: `get_result` must never block forever. A no-timeout wait on a task
    //! that does not exist (pruned by retention, or a bogus id) returns a typed
    //! `TaskNotFound` outcome from the initial poll instead of hanging.
    use super::*;
    use std::time::Duration;
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

    #[tokio::test]
    async fn get_result_no_timeout_returns_not_found_for_missing_task() {
        let broker = PostgresBroker::connect(&test_db_url()).await.expect("connect");
        let missing = format!("missing-{}", Uuid::new_v4());

        // Wrap in an outer timeout: before the fix, a no-timeout wait on a
        // missing task hangs here forever, so the expect() below fails fast.
        let res = tokio::time::timeout(
            Duration::from_secs(5),
            broker.get_result::<i32>(&missing, None),
        )
        .await
        .expect("get_result(None) must not hang for a missing task");

        let outcome = res.expect("no broker error");
        let err = outcome.unwrap_err();
        let expected = TaskError::builtin(RetrievalCode::TaskNotFound, "").error_code;
        assert_eq!(err.error_code, expected, "expected a TaskNotFound outcome");
    }
}

#[cfg(test)]
mod filter_non_runnable_tests {
    //! C15: `filter_non_runnable_workflow_tasks` must, for a PAUSED workflow,
    //! both cancel the worker's claimed task and reset its node to READY — in one
    //! transaction, so a crash cannot leave a terminal task linked to a live node.
    //! The atomicity itself is not crash-testable in a unit test; this pins the
    //! functional outcome (both mutations applied, id returned as filtered).
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

    #[tokio::test]
    #[serial]
    async fn paused_workflow_cancels_task_and_resets_node() {
        let broker = PostgresBroker::connect(&test_db_url()).await.expect("connect");
        let pool = broker.pool().clone();
        let wf_id = Uuid::new_v4().to_string();
        let task_id = Uuid::new_v4().to_string();

        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES ($1, 'c15_wf', 'PAUSED', 'fail', NULL, 'test.c15.v1', 0, $1,
                      NOW(), NOW(), NOW(), NOW())",
        )
        .bind(&wf_id)
        .execute(&pool)
        .await
        .expect("insert workflow");

        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, task_id, created_at
            ) VALUES ($1, $2, 0, 'node_0', 'c15_task', '[]', '{}',
                      'default', 100, '{}', FALSE, 'all',
                      'ENQUEUED', FALSE, $3, NOW())",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&wf_id)
        .bind(&task_id)
        .execute(&pool)
        .await
        .expect("insert workflow_task");

        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, claimed_at, created_at, updated_at, claimed,
                claimed_by_worker_id, retry_count, max_retries, is_workflow_task, enqueue_sha
            ) VALUES ($1, 'c15_task', 'default', 100, '[]', '{}', 'CLAIMED',
                      NOW(), NOW(), NOW(), NOW(), TRUE,
                      'w1', 0, 3, TRUE,
                      '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef')",
        )
        .bind(&task_id)
        .execute(&pool)
        .await
        .expect("insert task");

        let filtered = broker
            .filter_non_runnable_workflow_tasks(&[task_id.clone()], "w1")
            .await
            .expect("filter");
        assert_eq!(filtered, vec![task_id.clone()], "paused task must be filtered");

        let task_status: String =
            sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
                .bind(&task_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(task_status, "CANCELLED", "task must be cancelled");

        let (node_status, linked): (String, Option<String>) = sqlx::query_as(
            "SELECT status, task_id FROM horsies_workflow_tasks WHERE workflow_id = $1",
        )
        .bind(&wf_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(node_status, "READY", "node must be reset to READY");
        assert!(linked.is_none(), "node's task_id must be cleared on reset");

        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(&wf_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(&task_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(&wf_id)
            .execute(&pool)
            .await
            .ok();
    }
}

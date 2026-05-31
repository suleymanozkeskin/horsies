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
use crate::broker::result_types::{BrokerErrorCode, BrokerOperationError, BrokerResult};
use crate::broker::row::task::{
    ClaimedId, ClaimedTaskRow, ExpiredTaskRow, SetRunningRow, StaleTaskRow, TaskAttemptRow,
    TaskInfoRow, TaskResultRow, TaskRunningContextRow, WorkerStatsRow,
};
use crate::broker::shared_listener::SharedNotifyListener;

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

const ENQUEUE_SQL: &str = "\
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, args, kwargs,
    status, sent_at, enqueued_at, good_until, max_retries, task_options,
    enqueue_sha, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', $7, COALESCE($8, NOW()), $9, $10, $11, $12, NOW(), NOW())
ON CONFLICT (id) DO NOTHING
RETURNING id";

const CLAIM_SQL: &str = "\
WITH next AS (
    SELECT id
    FROM horsies_tasks
    WHERE queue_name = $1
      AND (
        status = 'PENDING'
        OR (status = 'CLAIMED' AND claim_expires_at IS NOT NULL AND claim_expires_at < now())
      )
      AND enqueued_at <= now()
      AND (next_retry_at IS NULL OR next_retry_at <= now())
      AND (good_until IS NULL OR good_until > now())
    ORDER BY priority ASC, enqueued_at ASC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $2
)
UPDATE horsies_tasks t
SET status = 'CLAIMED',
    claimed = TRUE,
    claimed_at = now(),
    claimed_by_worker_id = $3,
    claim_expires_at = $4,
    updated_at = now()
FROM next
WHERE t.id = next.id
RETURNING t.id, t.task_name, t.args, t.kwargs, t.retry_count, t.max_retries, t.task_options, t.queue_name, t.good_until";

const SET_RUNNING_SQL: &str = "\
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
RETURNING id, started_at";

const COMPLETE_SQL: &str = "\
UPDATE horsies_tasks
SET status = 'COMPLETED',
    completed_at = NOW(),
    result = $2,
    error_code = NULL,
    updated_at = NOW()
WHERE id = $1
  AND status = 'RUNNING'
RETURNING id";

/// Task failure: sets result and error_code (user/task error path).
const FAIL_TASK_SQL: &str = "\
UPDATE horsies_tasks
SET status = 'FAILED',
    failed_at = NOW(),
    result = $2,
    error_code = $3,
    updated_at = NOW()
WHERE id = $1
  AND status = 'RUNNING'
RETURNING id";

/// Worker failure: sets failed_reason, clears error_code (worker crash path).
const FAIL_WORKER_SQL: &str = "\
UPDATE horsies_tasks
SET status = 'FAILED',
    failed_at = NOW(),
    failed_reason = $2,
    error_code = NULL,
    updated_at = NOW()
WHERE id = $1
  AND status = 'RUNNING'
RETURNING id";

const CANCEL_SQL: &str = "\
UPDATE horsies_tasks
SET status = 'CANCELLED',
    updated_at = NOW()
WHERE id = $1
  AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
RETURNING id";

const REQUEUE_SQL: &str = "\
UPDATE horsies_tasks
SET status = 'PENDING',
    retry_count = $2,
    next_retry_at = $3,
    enqueued_at = $3,
    error_code = NULL,
    updated_at = NOW()
WHERE id = $1
  AND status = 'RUNNING'
  AND (good_until IS NULL OR $3 < good_until)
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

/// Transaction-scoped advisory lock to serialize claims across workers.
const ADVISORY_XACT_LOCK_SQL: &str = "\
SELECT pg_advisory_xact_lock($1)";

/// Count all RUNNING + active CLAIMED tasks across every worker in the cluster.
/// Excludes expired claims (reclaimable, must not consume cap budget).
const COUNT_GLOBAL_IN_FLIGHT_SQL: &str = "\
SELECT COUNT(*) FROM horsies_tasks \
WHERE status = 'RUNNING' \
   OR (status = 'CLAIMED' \
       AND (claim_expires_at IS NULL OR claim_expires_at > now()))";

/// Count RUNNING + active CLAIMED tasks for a specific worker.
const COUNT_IN_FLIGHT_FOR_WORKER_SQL: &str = "\
SELECT COUNT(*) FROM horsies_tasks \
WHERE claimed_by_worker_id = $1 \
  AND (status = 'RUNNING' \
       OR (status = 'CLAIMED' \
           AND (claim_expires_at IS NULL OR claim_expires_at > now())))";

/// Count only RUNNING tasks for a specific worker.
const COUNT_RUNNING_FOR_WORKER_SQL: &str = "\
SELECT COUNT(*) FROM horsies_tasks \
WHERE claimed_by_worker_id = $1 AND status = 'RUNNING'";

/// Count RUNNING + active CLAIMED tasks for a specific queue (hard cap mode).
const COUNT_IN_FLIGHT_FOR_QUEUE_SQL: &str = "\
SELECT COUNT(*) FROM horsies_tasks \
WHERE queue_name = $1 \
  AND (status = 'RUNNING' \
       OR (status = 'CLAIMED' \
           AND (claim_expires_at IS NULL OR claim_expires_at > now())))";

/// Count only RUNNING tasks for a specific queue (global).
const COUNT_RUNNING_FOR_QUEUE_SQL: &str = "\
SELECT COUNT(*) FROM horsies_tasks \
WHERE queue_name = $1 AND status = 'RUNNING'";

/// Load CLAIMED tasks owned by a specific worker (for prefetch buffer dispatch).
const LOAD_BUFFERED_CLAIMED_SQL: &str = "\
SELECT id, task_name, args, kwargs, retry_count, max_retries, task_options, queue_name, good_until \
FROM horsies_tasks \
WHERE claimed_by_worker_id = $1 AND status = 'CLAIMED' \
ORDER BY priority ASC, enqueued_at ASC, id ASC";

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

/// Unclaim this worker's CLAIMED tasks in PAUSED workflows (post-claim batch
/// filter). Ownership-scoped so a worker cannot reset another worker's claim;
/// returns the affected ids so only those workflow_task rows are reset.
const UNCLAIM_PAUSED_OWNED_TASKS_SQL: &str = "\
UPDATE horsies_tasks \
SET status = 'PENDING', \
    claimed = FALSE, \
    claimed_at = NULL, \
    claimed_by_worker_id = NULL, \
    claim_expires_at = NULL, \
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

/// Unclaim tasks: reset from CLAIMED back to PENDING.
const UNCLAIM_TASKS_SQL: &str = "\
UPDATE horsies_tasks \
SET status = 'PENDING', \
    claimed = FALSE, \
    claimed_at = NULL, \
    claimed_by_worker_id = NULL, \
    claim_expires_at = NULL, \
    updated_at = NOW() \
WHERE id = ANY($1)";

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
const GET_WORKER_STATS_SQL: &str = "\
SELECT
    t.worker_hostname,
    t.worker_pid,
    t.worker_process_name,
    COUNT(*)::BIGINT AS active_tasks,
    MIN(t.started_at) AS oldest_task_start,
    MAX(hb.last_heartbeat) AS latest_heartbeat
FROM horsies_tasks t
LEFT JOIN LATERAL (
    SELECT sent_at AS last_heartbeat
    FROM horsies_heartbeats h
    WHERE h.task_id = t.id AND h.role = 'runner'
    ORDER BY sent_at DESC
    LIMIT 1
) hb ON TRUE
WHERE t.status = 'RUNNING'
  AND t.worker_hostname IS NOT NULL
GROUP BY t.worker_hostname, t.worker_pid, t.worker_process_name
ORDER BY active_tasks DESC";

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

    /// Claim up to `limit` tasks from the given queue within an open transaction.
    ///
    /// Returns full task rows atomically — no separate load step needed.
    pub async fn claim_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
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
            .fetch_all(tx.as_mut())
            .await
            .map_err(BrokerError::Database)?;

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
    pub async fn complete_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        task_id: &str,
        result_json: Option<&str>,
    ) -> Result<bool, BrokerError> {
        let row: Option<ClaimedId> = sqlx::query_as(COMPLETE_SQL)
            .bind(task_id)
            .bind(result_json)
            .fetch_optional(tx.as_mut())
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, "task completed");
        } else {
            tracing::warn!(task_id, "task completion skipped: task no longer RUNNING");
        }
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
    ) -> Result<bool, BrokerError> {
        let row: Option<ClaimedId> = sqlx::query_as(COMPLETE_SQL)
            .bind(task_id)
            .bind(result_json)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, "task completed");
        } else {
            tracing::warn!(task_id, "task completion skipped: task no longer RUNNING");
        }
        Ok(row.is_some())
    }

    /// Mark a task as FAILED (transactional variant).
    pub async fn fail_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        task_id: &str,
        result_json: Option<&str>,
        error_code: Option<&str>,
    ) -> Result<bool, BrokerError> {
        let row: Option<ClaimedId> = sqlx::query_as(FAIL_TASK_SQL)
            .bind(task_id)
            .bind(result_json)
            .bind(error_code)
            .fetch_optional(tx.as_mut())
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, "task failed");
        } else {
            tracing::warn!(task_id, "task failure skipped: task no longer RUNNING");
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
    ) -> Result<bool, BrokerError> {
        let row: Option<ClaimedId> = sqlx::query_as(FAIL_TASK_SQL)
            .bind(task_id)
            .bind(result_json)
            .bind(error_code)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, "task failed");
        } else {
            tracing::warn!(task_id, "task failure skipped: task no longer RUNNING");
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
    ) -> Result<bool, BrokerError> {
        let row: Option<ClaimedId> = sqlx::query_as(FAIL_WORKER_SQL)
            .bind(task_id)
            .bind(failed_reason)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, "task failed (worker crash)");
        } else {
            tracing::warn!(task_id, "worker failure skipped: task no longer RUNNING");
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
    pub async fn requeue_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        task_id: &str,
        retry_count: i32,
        next_retry_at: Option<DateTime<Utc>>,
    ) -> Result<bool, BrokerError> {
        let row: Option<ClaimedId> = sqlx::query_as(REQUEUE_SQL)
            .bind(task_id)
            .bind(retry_count)
            .bind(next_retry_at)
            .fetch_optional(tx.as_mut())
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, retry_count, "task requeued");
        } else {
            tracing::warn!(task_id, "task requeue skipped: task no longer RUNNING");
        }
        Ok(row.is_some())
    }

    /// Requeue a task for retry with updated retry count and optional delay.
    ///
    /// Returns `true` if the update was applied (task was RUNNING).
    /// Returns `false` if the task was no longer RUNNING (e.g., already
    /// marked FAILED by the stale-task reaper).
    pub async fn requeue(
        &self,
        task_id: &str,
        retry_count: i32,
        next_retry_at: Option<DateTime<Utc>>,
    ) -> Result<bool, BrokerError> {
        let row: Option<ClaimedId> = sqlx::query_as(REQUEUE_SQL)
            .bind(task_id)
            .bind(retry_count)
            .bind(next_retry_at)
            .fetch_optional(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        if row.is_some() {
            tracing::debug!(task_id, retry_count, "task requeued");
        } else {
            tracing::warn!(task_id, "task requeue skipped: task no longer RUNNING");
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

        // Quick check — task may already be done.
        if let Some(row) = self.fetch_result_row(task_id).await? {
            if is_terminal_status(&row.status) {
                return parse_task_result_row(row);
            }
        }

        // Subscribe to the shared task_done listener.
        let shared = self.task_done_listener().await?;
        let mut subscription = shared.subscribe(task_id);

        // Re-check after subscribing to avoid race where task completes
        // between our first check and the subscribe.
        if let Some(row) = self.fetch_result_row(task_id).await? {
            if is_terminal_status(&row.status) {
                return parse_task_result_row(row);
            }
        }

        let deadline = timeout.map(|t| Instant::now() + t);

        loop {
            let remaining = match deadline {
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        return self
                            .final_poll_or_timeout(task_id, start.elapsed().as_millis() as u64)
                            .await;
                    }
                    Some(d - now)
                }
                None => None,
            };

            let wake = match remaining {
                Some(dur) => match tokio::time::timeout(dur, subscription.recv()).await {
                    Ok(result) => result,
                    Err(_) => {
                        return self
                            .final_poll_or_timeout(task_id, start.elapsed().as_millis() as u64)
                            .await;
                    }
                },
                None => subscription.recv().await,
            };

            match wake {
                Ok(()) => {
                    if let Some(row) = self.fetch_result_row(task_id).await? {
                        if is_terminal_status(&row.status) {
                            return parse_task_result_row(row);
                        }
                    }
                }
                Err(e) => return Err(e),
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

    /// Acquire the global claim advisory lock within an existing transaction.
    pub async fn advisory_xact_lock(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        key: i64,
    ) -> Result<(), BrokerError> {
        sqlx::query(ADVISORY_XACT_LOCK_SQL)
            .bind(key)
            .execute(tx.as_mut())
            .await
            .map_err(BrokerError::Database)?;
        Ok(())
    }

    /// Count all RUNNING + CLAIMED tasks across the cluster inside a transaction.
    pub async fn count_global_in_flight_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<i64, BrokerError> {
        let count: (i64,) = sqlx::query_as(COUNT_GLOBAL_IN_FLIGHT_SQL)
            .fetch_one(tx.as_mut())
            .await
            .map_err(BrokerError::Database)?;
        Ok(count.0)
    }

    /// Count RUNNING + CLAIMED tasks for a specific worker inside a transaction.
    pub async fn count_in_flight_for_worker_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        worker_id: &str,
    ) -> Result<i64, BrokerError> {
        let count: (i64,) = sqlx::query_as(COUNT_IN_FLIGHT_FOR_WORKER_SQL)
            .bind(worker_id)
            .fetch_one(tx.as_mut())
            .await
            .map_err(BrokerError::Database)?;
        Ok(count.0)
    }

    /// Count RUNNING tasks for a specific worker inside a transaction.
    pub async fn count_running_for_worker_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        worker_id: &str,
    ) -> Result<i64, BrokerError> {
        let count: (i64,) = sqlx::query_as(COUNT_RUNNING_FOR_WORKER_SQL)
            .bind(worker_id)
            .fetch_one(tx.as_mut())
            .await
            .map_err(BrokerError::Database)?;
        Ok(count.0)
    }

    /// Count RUNNING + CLAIMED tasks for a queue inside a transaction.
    pub async fn count_in_flight_for_queue_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        queue_name: &str,
    ) -> Result<i64, BrokerError> {
        let count: (i64,) = sqlx::query_as(COUNT_IN_FLIGHT_FOR_QUEUE_SQL)
            .bind(queue_name)
            .fetch_one(tx.as_mut())
            .await
            .map_err(BrokerError::Database)?;
        Ok(count.0)
    }

    /// Count RUNNING tasks for a queue inside a transaction.
    pub async fn count_running_for_queue_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        queue_name: &str,
    ) -> Result<i64, BrokerError> {
        let count: (i64,) = sqlx::query_as(COUNT_RUNNING_FOR_QUEUE_SQL)
            .bind(queue_name)
            .fetch_one(tx.as_mut())
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

    /// Load CLAIMED tasks owned by a specific worker that are ready to dispatch.
    ///
    /// In prefetch/soft-cap mode, a worker may have CLAIMED tasks buffered
    /// in the database. When a semaphore permit becomes available, these
    /// buffered tasks can be dispatched without a new claim round-trip.
    pub async fn load_buffered_claimed(
        &self,
        worker_id: &str,
    ) -> Result<Vec<ClaimedTaskRow>, BrokerError> {
        let rows: Vec<ClaimedTaskRow> = sqlx::query_as(LOAD_BUFFERED_CLAIMED_SQL)
            .bind(worker_id)
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
                sqlx::query(UNCLAIM_TASKS_SQL)
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

        // PAUSED: unclaim this worker's own claimed rows back to PENDING so they
        // can be picked up on resume. Reset only the workflow_task rows we owned.
        if !paused_ids.is_empty() {
            let unclaimed: Vec<(String,)> = sqlx::query_as(UNCLAIM_PAUSED_OWNED_TASKS_SQL)
                .bind(&paused_ids)
                .bind(worker_id)
                .fetch_all(&self.pool)
                .await
                .map_err(BrokerError::Database)?;
            let owned: Vec<String> = unclaimed.into_iter().map(|(id,)| id).collect();

            if !owned.is_empty() {
                sqlx::query(RESET_WORKFLOW_TASKS_SQL)
                    .bind(&owned)
                    .execute(&self.pool)
                    .await
                    .map_err(BrokerError::Database)?;
            }

            tracing::debug!(
                count = owned.len(),
                "unclaimed own tasks belonging to PAUSED workflows",
            );
        }

        // CANCELLED: hard-cancel this worker's own claimed rows and skip only the
        // workflow_task rows we owned.
        if !cancelled_ids.is_empty() {
            let cancelled: Vec<(String,)> = sqlx::query_as(CANCEL_CANCELLED_OWNED_TASKS_SQL)
                .bind(&cancelled_ids)
                .bind(worker_id)
                .fetch_all(&self.pool)
                .await
                .map_err(BrokerError::Database)?;
            let owned: Vec<String> = cancelled.into_iter().map(|(id,)| id).collect();

            if !owned.is_empty() {
                sqlx::query(SKIP_CANCELLED_WORKFLOW_TASKS_SQL)
                    .bind(&owned)
                    .execute(&self.pool)
                    .await
                    .map_err(BrokerError::Database)?;
            }

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

    /// Gather per-worker statistics for RUNNING tasks.
    ///
    /// Groups RUNNING tasks by `(worker_hostname, worker_pid, worker_process_name)`
    /// and returns the count of active tasks, the oldest task start time, and the
    /// most recent runner heartbeat for each worker.
    ///
    /// Useful for load-balancing dashboards and detecting overloaded workers.
    ///
    /// Mirrors `PostgresBroker.get_worker_stats()` in the Python library.
    pub async fn get_worker_stats(&self) -> Result<Vec<WorkerStatsRow>, BrokerError> {
        let rows: Vec<WorkerStatsRow> = sqlx::query_as(GET_WORKER_STATS_SQL)
            .fetch_all(&self.pool)
            .await
            .map_err(BrokerError::Database)?;

        tracing::debug!(count = rows.len(), "worker stats query");
        Ok(rows)
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

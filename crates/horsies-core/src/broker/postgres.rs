use std::time::Duration;

use chrono::{DateTime, Timelike, Utc};
use serde::de::DeserializeOwned;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::ConnectOptions;
use std::str::FromStr;
use tokio::time::Instant;
use uuid::Uuid;

use crate::core::history::archive::results::decode_result_envelope;
use crate::core::history::enqueue::{parse_optional_object, prepare_enqueue_facts, SendPart};
use crate::core::history::identity::keys::{
    reservation_window_micros, IDEMPOTENCY_WINDOW_DEFAULT_HOURS, IDEMPOTENCY_WINDOW_DEFAULT_MICROS,
};
use crate::core::history::reads::detail::{HistoryTaskDetail, TaskAbsenceReason, TaskDetailResult};
use crate::core::history::reads::result::{
    HistoryResultPayload, HistoryTaskResult, TaskResultRead,
};
use crate::core::history::rerun::operations::{
    rerun_first_statement, rerun_task_in_transaction, RerunEnqueuePolicy, RerunError, RerunOutcome,
    RerunTask,
};
use crate::core::{
    EnqueueRetention, OutcomeCode, PostgresConfig, RetrievalCode, TaskError, TaskInfo, TaskResult,
};

use crate::broker::core_rows::{
    column, expired_list_row, listed_worker_state_row, one_row, read_rows, scalar, stale_list_row,
    stored_worker_state, task_attempt_row, task_info_row, task_result_row, RowDecodeError,
    StatementLog,
};
use crate::broker::enqueue_task::{
    decode_enqueue_outcome, enqueue_result, EnqueueKey, EnqueueStart, EnqueueTaskOutcome, SendKind,
    ENQUEUE_TASK,
};
use crate::broker::error::{is_retryable_sqlx_error, BrokerError, EnqueueRefusal};
use crate::broker::health::{
    checked_history_limit, ping_min_responses_refused, ping_timeout_refused,
    read_listed_worker_states, read_worker_snapshot, read_worker_snapshots, DatabasePing,
    ListedWorkerState, WorkerPingRequest, WorkerPong, WorkerPongPayload, WorkerStateSnapshot,
    WORKER_PING_CHANNEL,
};
use crate::broker::history_reads::{read_history_detail, read_task_result, ResultReaderSeen};
use crate::broker::result_types::{
    BrokerErrorCode, BrokerOperationError, BrokerResult, RawResultRead, RawResultRecord,
    TaskInfoRead,
};
use crate::broker::row::task::{ExpiredTaskRow, StaleTaskRow, TaskAttemptRow, TaskResultRow};
use crate::broker::workflow_claim_settle::{settle_claimed_workflow_tasks, ClaimDisposition};

use crate::broker::client_family::{
    admit_family, recorded_family_from, ClientFamily, FamilyCheckError, FamilyCheckFailure,
    RecordedFamily,
};
use crate::broker::process_listener::{
    default_payload_capacity, fixed_channel, ListenError, ProcessListener, SessionCheckFailure,
    TASK_DONE_CHANNEL, WORKER_PONG_CHANNEL_PREFIX,
};
use crate::broker::task_lists::{
    bounded_list_from_rows, BoundedTaskList, ListShapeError, StaleMinutes, TASK_LIST_BOUND,
};
use crate::broker::wait_limit::WaitLimit;
use crate::db::{
    PayloadEvent, PayloadFilter, WakeEvent, WakeSubscription, DEFAULT_SOCKET_SETTINGS,
};

use crate::broker::pools::{
    CloseOutcome, CoreHandle, OpenPools, PoolProfile, SessionPool, WriteInFlight,
};
use crate::db::{
    BoundStatement, CheckoutClass, IsolationLevel, NotSentCause, ParameterType, Row,
    StatementBound, StatementFailure, TypedStatement, WORKER_RESERVED_CONNECTIONS,
};

/// Refuse a sent task input that breaks the rules of a user value (Python `.send()` refuses it as
/// `VALIDATION_FAILED`, `core/task_decorator.py:2378-2387`). An object input is Python's keyword input: no
/// member name is an engine key, and each member value has no engine key at any depth and at most
/// `MAX_VALUE_JSON_DEPTH` levels. Any other input except `null` is one value under the same rules. A text that
/// is not strict JSON is refused.
fn check_sent_input(input: &str) -> Result<(), BrokerError> {
    let value: serde_json::Value = crate::core::codec::from_json_str(input).map_err(|error| {
        BrokerError::EnqueueContract(EnqueueRefusal::unreadable_json(
            SendPart::Input,
            format!("task input refused: {error}"),
            &error,
        ))
    })?;
    let no_engine_members: std::collections::HashSet<String> = std::collections::HashSet::new();
    let checked = match &value {
        serde_json::Value::Object(members) => {
            crate::core::codec::user_value::check_task_input(&[], members, &no_engine_members)
        }
        serde_json::Value::Null => Ok(()),
        other => crate::core::codec::user_value::check_task_input(
            std::slice::from_ref(other),
            &serde_json::Map::new(),
            &no_engine_members,
        ),
    };
    checked.map_err(|refusal| {
        BrokerError::EnqueueContract(EnqueueRefusal::by_rule(
            SendPart::Input,
            format!("task input refused: {refusal}"),
        ))
    })
}

#[cfg(test)]
mod sent_input_tests {
    use super::check_sent_input;
    use crate::broker::BrokerError;

    /// Python `.send()` refuses an engine key in the input as `VALIDATION_FAILED`
    /// (`core/task_decorator.py:2378-2387`).
    #[test]
    fn a_sent_input_with_an_engine_key_is_refused() {
        for input in [
            r#"{"x": {"__h_task_result__": true}}"#,
            r#"{"__h_workflow_ctx__": {}}"#,
            r#"[{"a": [{"__builtin_task_code__": "X"}]}]"#,
            r#"{"x": NaN}"#,
        ] {
            assert!(
                matches!(
                    check_sent_input(input),
                    Err(BrokerError::EnqueueContract(_))
                ),
                "{input}"
            );
        }
        assert!(check_sent_input(r#"{"x": {"y": [1, 2]}}"#).is_ok());
        assert!(check_sent_input("[1]").is_ok());
        assert!(check_sent_input("null").is_ok());
    }
}

#[cfg(test)]
fn test_uuid(value: &str) -> Uuid {
    Uuid::parse_str(value).expect("test identity must be UUID")
}

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

/// What the pre-start expiry of a claimed task found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrestartExpiry {
    /// The task is EXPIRED: this call moved it, or an earlier call did.
    Expired,
    /// The deadline has not passed, or the row is not this worker's claim;
    /// nothing is written.
    NotExpired,
}

// Terminal transitions (COMPLETED/FAILED/CANCELLED/EXPIRED) run through the
// terminalization operations — broker/terminalization.rs and the functions
// installed by migration 0032.

// retry_count is derived from the row being CAS-updated (the same idiom as
// horsies_complete_task_fused's ctx CTE), not passed by the caller: the
// ownership CAS makes the claim-time snapshot provably equal to the row, so
// self-incrementing removes the snapshot from the written value entirely.
//
// The written row is the retry row of the baseline retry write
// (`crates/horsies-core/baseline/0004_lifecycle_functions.sql:1479-1496`):
// PENDING, retry_count + 1, enqueued_at = next_retry_at, and every claim and
// run field cleared. The retry time is `$2`, or the database clock when `$2`
// is NULL (no delay): `enqueued_at` is NOT NULL in the baseline
// (`0001_tables.sql:79`). A retry time at or after `good_until` is refused.
const REQUEUE_SQL: &str = "\
WITH retry AS (
    SELECT COALESCE($2::timestamptz, now()) AS due_at
)
UPDATE horsies_tasks t
SET status = 'PENDING',
    retry_count = t.retry_count + 1,
    next_retry_at = retry.due_at,
    enqueued_at = retry.due_at,
    claimed_at = NULL,
    claimed_by_worker_id = NULL,
    claim_expires_at = NULL,
    started_at = NULL,
    worker_pid = NULL,
    worker_hostname = NULL,
    worker_process_name = NULL,
    finalizing_at = NULL,
    finalizing_by_worker_id = NULL,
    updated_at = NOW()
FROM retry
WHERE t.id = $1
  AND t.status = 'RUNNING'
  AND t.claimed_by_worker_id = $3
  AND (t.good_until IS NULL OR retry.due_at < t.good_until)
RETURNING t.id";

/// [`REQUEUE_SQL`] on the statement layer: the task id, the retry time
/// (`NULL`: the database clock), the worker id.
static REQUEUE: TypedStatement = TypedStatement::new(
    "requeue",
    REQUEUE_SQL,
    &[
        ParameterType::Uuid,
        ParameterType::Timestamptz,
        ParameterType::Text,
    ],
);

// A live row holds no result and no failed reason: the baseline keeps them
// with the terminal record in history (`horsies_tasks` in
// `crates/horsies-core/baseline/0001_tables.sql`). The live reads below select
// `NULL` for them, as Python's live reads do (`GET_TASK_STATUS_NAME_SQL`,
// `python/horsies/core/brokers/postgres.py:942-948`; `get_task_info_async`,
// `:3659-3672`).

/// The live row of the result wait while the staged result function is not
/// published.
const GET_RESULT_SQL: &str = "\
SELECT id, status, NULL::text AS result, NULL::text AS failed_reason
FROM horsies_tasks
WHERE id = $1";

const GET_RAW_RESULT_PROBE_SQL: &str = "\
SELECT id, task_name, status
FROM horsies_tasks
WHERE id = $1";

static GET_RESULT: TypedStatement =
    TypedStatement::new("get_result", GET_RESULT_SQL, &[ParameterType::Uuid]);

static GET_RAW_RESULT_PROBE: TypedStatement = TypedStatement::new(
    "get_raw_result_probe",
    GET_RAW_RESULT_PROBE_SQL,
    &[ParameterType::Uuid],
);

/// The live row of a task info read. The terminal times, the error code, the
/// result and the failed reason are `NULL` whatever the include flags say:
/// the flags select fields of a history row only.
const GET_TASK_INFO_SQL: &str = "\
SELECT id, task_name, status, queue_name, priority, retry_count, max_retries,
       next_retry_at, sent_at, enqueued_at, claimed_at, started_at,
       NULL::timestamptz AS completed_at, NULL::timestamptz AS failed_at,
       worker_hostname, worker_pid, worker_process_name, NULL::text AS error_code,
       NULL::text AS failed_reason, NULL::text AS result, execution
FROM horsies_tasks
WHERE id = $1";

// ---------------------------------------------------------------------------
// Task attempt SQL
// ---------------------------------------------------------------------------

/// Query task attempts for a specific task (most recent first).
const SELECT_TASK_ATTEMPTS_SQL: &str = "\
SELECT task_id, attempt, outcome, will_retry,
       started_at, finished_at,
       error_code, error_message, failed_reason,
       worker_id, worker_hostname, worker_pid, worker_process_name
FROM horsies_task_attempts
WHERE task_id = $1
ORDER BY attempt DESC";

static SELECT_TASK_ATTEMPTS: TypedStatement = TypedStatement::new(
    "select_task_attempts",
    SELECT_TASK_ATTEMPTS_SQL,
    &[ParameterType::Uuid],
);

/// Whether the live table holds the task id `$1`.
static LIVE_TASK_EXISTS: TypedStatement = TypedStatement::new(
    "live_task_exists",
    "SELECT EXISTS (SELECT 1 FROM horsies_tasks WHERE id = $1)",
    &[ParameterType::Uuid],
);

static GET_TASK_INFO: TypedStatement =
    TypedStatement::new("get_task_info", GET_TASK_INFO_SQL, &[ParameterType::Uuid]);

// ---------------------------------------------------------------------------
// Monitoring / observability SQL
// ---------------------------------------------------------------------------

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
///
/// Each row also has `read_at`, the `statement_timestamp()` of this
/// statement: the database clock, one value for every row. The web worker
/// list measures ages from it (Python measures them with the web host clock,
/// `tests/dualrun/reference/web_routes/workers.py:108-119`).
const LIST_WORKER_STATES_SQL: &str = "\
WITH RECURSIVE latest AS (
    (
        SELECT
            worker_id, snapshot_at, hostname, pid, processes, max_claim_batch,
            max_claim_per_worker, cluster_wide_cap, queues, queue_priorities,
            queue_max_concurrency, recovery_config, tasks_running, tasks_claimed,
            memory_usage_mb, memory_percent, cpu_percent, children_memory_mb,
            worker_started_at, slots_down, class_capacity
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
            w.memory_usage_mb, w.memory_percent, w.cpu_percent, w.children_memory_mb,
            w.worker_started_at, w.slots_down, w.class_capacity
        FROM horsies_worker_states w
        WHERE w.worker_id > l.worker_id
        ORDER BY w.worker_id, w.snapshot_at DESC
        LIMIT 1
    ) nxt
)
SELECT latest.*, statement_timestamp() AS read_at FROM latest";

/// Latest snapshot for a single worker.
const GET_WORKER_STATE_LATEST_SQL: &str = "\
SELECT
    worker_id, snapshot_at, hostname, pid, processes, max_claim_batch,
    max_claim_per_worker, cluster_wide_cap, queues, queue_priorities,
    queue_max_concurrency, recovery_config, tasks_running, tasks_claimed,
    memory_usage_mb, memory_percent, cpu_percent, children_memory_mb,
    worker_started_at, slots_down, class_capacity
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
    memory_usage_mb, memory_percent, cpu_percent, children_memory_mb,
    worker_started_at, slots_down, class_capacity
FROM horsies_worker_states
WHERE worker_id = $1
ORDER BY snapshot_at DESC
LIMIT $2";

/// The stale list: `horsies_list_stale_tasks` (baseline `0007`), task rows
/// in list order, then its `LIST_END` row. `$1` is the threshold in minutes.
static LIST_STALE_TASKS: TypedStatement = TypedStatement::new(
    "list_stale_tasks",
    "SELECT row_kind, task_id, task_name, worker_hostname, worker_pid, worker_process_name,
            started_at, last_heartbeat, list_state, row_bound
     FROM horsies_list_stale_tasks($1)
     ORDER BY row_position NULLS LAST",
    &[ParameterType::Int4],
);

static LIST_WORKER_STATES: TypedStatement =
    TypedStatement::new("list_worker_states", LIST_WORKER_STATES_SQL, &[]);

static GET_WORKER_STATE_LATEST: TypedStatement = TypedStatement::new(
    "get_worker_state_latest",
    GET_WORKER_STATE_LATEST_SQL,
    &[ParameterType::Text],
);

static GET_WORKER_STATE_HISTORY: TypedStatement = TypedStatement::new(
    "get_worker_state_history",
    GET_WORKER_STATE_HISTORY_SQL,
    &[ParameterType::Text, ParameterType::Int8],
);

/// The expired list: `horsies_list_expired_tasks` (baseline `0007`), task
/// rows in list order, then its `LIST_END` row. `expired_for` is read as
/// whole milliseconds, as the list row carries it.
static LIST_EXPIRED_TASKS: TypedStatement = TypedStatement::new(
    "list_expired_tasks",
    "SELECT row_kind, task_id, task_name, queue_name, priority, sent_at, enqueued_at, good_until,
            (EXTRACT(EPOCH FROM expired_for) * 1000000)::bigint AS expired_for_us,
            list_state, row_bound
     FROM horsies_list_expired_tasks()
     ORDER BY row_position NULLS LAST",
    &[],
);

/// The worker ping broadcast: `$1` the ping channel, `$2` the request JSON.
static NOTIFY_WORKER_PING: TypedStatement = TypedStatement::new(
    "notify_worker_ping",
    "SELECT pg_notify($1, $2)",
    &[ParameterType::Text, ParameterType::Text],
);

/// The family row of the database, read before the first write of a handle.
static READ_CLIENT_FAMILY: TypedStatement = TypedStatement::new(
    "read_client_family",
    crate::broker::client_family::READ_FAMILY_SQL,
    &[],
);

/// Health check: `SELECT 1` to verify broker connectivity.
const HEALTH_CHECK_SQL: &str = "SELECT 1";

static HEALTH_CHECK: TypedStatement = TypedStatement::new("health_check", HEALTH_CHECK_SQL, &[]);

/// `SELECT 1` on the session pool in the worker boot check.
static SESSION_CHECK: TypedStatement = TypedStatement::new("session_check", HEALTH_CHECK_SQL, &[]);

/// The NOTIFY of the worker boot's session check, on the main core pool:
/// `$1` the check channel.
static SESSION_CHECK_NOTIFY: TypedStatement = TypedStatement::new(
    "session_check_notify",
    "SELECT pg_notify($1, 'ok')",
    &[ParameterType::Text],
);

/// A pooled connection that waited in the pool for longer than this is
/// pinged before use when `pool_pre_ping` is on (Python
/// `PRE_PING_AFTER_IDLE_S`, `python/horsies/core/defaults.py:28`). A connection used
/// again within this time, and a new connection, are used with no ping.
pub(crate) const PRE_PING_AFTER_IDLE: Duration = crate::db::DEFAULT_IDLE_PING_AFTER;

/// Connect options for every broker pool, including the runtime pool in
/// PgBouncer transaction mode.
///
/// The SQLx statement cache keeps its default capacity in every mode. SQLx
/// prepares each query under a statement name whether or not the cache is
/// enabled; with a capacity of zero it uses a new name for every execution and
/// never closes it, so a pooler that tracks prepared statements holds one entry
/// per execution until the client disconnects. With the cache, a connection
/// holds at most the cache capacity and closes the oldest name on eviction.
/// PgBouncer transaction mode therefore requires `max_prepared_statements > 0`.
fn pg_connect_options(database_url: &str, echo: bool) -> Result<PgConnectOptions, BrokerError> {
    let mut connect_options = PgConnectOptions::from_str(database_url).map_err(|error| {
        BrokerError::InvalidConnectionSettings(
            crate::core::config::PostgresConfigError::UnparseableUrl(error.to_string()),
        )
    })?;

    if echo {
        connect_options = connect_options.log_statements(log::LevelFilter::Debug);
    } else {
        connect_options = connect_options.log_statements(log::LevelFilter::Off);
    }

    Ok(connect_options)
}

/// Session setting for every Horsies connection that keeps its session: the
/// statement layer's rule ([`crate::db::KEPT_SESSION_SETUP_SQL`]), so the
/// sqlx pools and the core pools set the same session.
const DISABLE_JIT_SQL: &str = crate::db::KEPT_SESSION_SETUP_SQL;

/// Whether a pool's connections keep their own session settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionSettings {
    /// A direct or session-capable connection: a `SET` holds for the session.
    Kept,
    /// PgBouncer transaction mode: server sessions are shared, so a session
    /// `SET` would leak to other clients. JIT stays as the server configures it.
    NotKept,
}

fn session_settings(config: &PostgresConfig) -> SessionSettings {
    match config.pgbouncer_transaction_mode {
        true => SessionSettings::NotKept,
        false => SessionSettings::Kept,
    }
}

fn with_session_settings(options: PgPoolOptions, settings: SessionSettings) -> PgPoolOptions {
    match settings {
        SessionSettings::NotKept => options,
        SessionSettings::Kept => options.after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query(DISABLE_JIT_SQL).execute(connection).await?;
                Ok(())
            })
        }),
    }
}

/// With `pool_pre_ping` on, ping a pooled connection at checkout when it
/// waited in the pool for longer than [`PRE_PING_AFTER_IDLE`]. A ping that
/// fails closes that connection, and the checkout takes another one or opens
/// a new one (Python `register_idle_ping`, `python/horsies/core/utils/db.py:165-222`).
/// With it off, a dead pooled connection shows at its first statement as a
/// retryable error.
fn with_idle_ping(options: PgPoolOptions, pool_pre_ping: bool) -> PgPoolOptions {
    let options = options.test_before_acquire(false);
    match pool_pre_ping {
        false => options,
        true => options.before_acquire(|connection, metadata| {
            Box::pin(async move {
                if metadata.idle_for > PRE_PING_AFTER_IDLE {
                    sqlx::Connection::ping(connection).await?;
                }
                Ok(true)
            })
        }),
    }
}

/// The options shared by the runtime and the session pool, as Python builds
/// both engines from one engine config (`python/horsies/core/brokers/postgres.py:
/// 1343-1376`): `pool_size + max_overflow` connections, `pool_timeout` for a
/// checkout, `pool_recycle` as the age at which a connection is retired, and
/// the idle ping.
fn pooled_options(config: &PostgresConfig) -> PgPoolOptions {
    with_idle_ping(
        PgPoolOptions::new()
            .max_connections(config.pool_size + config.max_overflow)
            .acquire_timeout(Duration::from_secs(config.pool_timeout as u64))
            .max_lifetime(connection_lifetime(config.pool_recycle))
            .idle_timeout(None),
        config.pool_pre_ping,
    )
}

/// The age at which the pool retires a connection, or `None` to keep it.
fn connection_lifetime(recycle: crate::manifest::PoolRecycle) -> Option<Duration> {
    match recycle {
        crate::manifest::PoolRecycle::Never => None,
        crate::manifest::PoolRecycle::AfterSeconds { seconds } => {
            Some(Duration::from_secs(u64::from(seconds.get())))
        }
    }
}

fn pg_pool_options(config: &PostgresConfig) -> PgPoolOptions {
    with_session_settings(pooled_options(config), session_settings(config))
}

/// The session pool's URL is direct or session-capable (it carries LISTEN).
/// It is sized as the runtime pool, as Python's session engine.
fn pg_session_pool_options(config: &PostgresConfig) -> PgPoolOptions {
    with_session_settings(pooled_options(config), SessionSettings::Kept)
}

/// The runtime pool of a worker broker: the batch users get
/// `pool_size + max_overflow - WORKER_RESERVED_CONNECTIONS` connections, as
/// the worker profile pool's batch limit; the reserve has its own pool.
fn pg_worker_runtime_pool_options(config: &PostgresConfig) -> PgPoolOptions {
    let batch_limit = config
        .pool_size
        .saturating_add(config.max_overflow)
        .saturating_sub(WORKER_RESERVED_CONNECTIONS.get());
    pg_pool_options(config).max_connections(batch_limit)
}

/// The worker reserve on `database_url`: [`WORKER_RESERVED_CONNECTIONS`]
/// connections with the runtime pool's settings.
fn pg_reserve_pool_options(config: &PostgresConfig) -> PgPoolOptions {
    pg_pool_options(config).max_connections(WORKER_RESERVED_CONNECTIONS.get())
}

/// The sqlx pools of the worker reserve: connections that the users of
/// [`PostgresBroker::pool`] cannot take. The reaper takes its gate and runs
/// its pass on them, so a pass runs while the runtime pool is exhausted
/// (Python keeps the reaper's gate session and pass body in the parent
/// pool's reserve of 2).
struct ReservePools {
    /// [`WORKER_RESERVED_CONNECTIONS`] connections on `database_url`,
    /// opened at first use.
    runtime: PgPool,
    /// The reaper's partition maintenance: the session pool when the session
    /// URL differs, else `runtime`.
    maintenance: PgPool,
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

/// Schema behavior selected when a broker is constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaInitializationMode {
    /// Apply migrations and require the validated task-history fleet gate.
    MigrateAndValidate,
    /// Connect for monitoring reads without executing schema DDL or fleet
    /// gates. The broker keeps its listener, as Python's web tool, which runs
    /// with `run_schema_migrations=False`.
    ObserveOnly,
}

/// The stored values of one send command, checked: only the database can
/// refuse it now (Python `SendCommand`). Built by [`SendCommand::prepare`]
/// and [`PostgresBroker::prepare_send_command`].
///
/// The version-1 fingerprint leaves the task id and `sent_at` out, so one
/// value serves every row of the command: each slot of a schedule fire.
/// `stored_input` is the `input` text the row stores; `execution` and
/// `max_retries` are read from `task_options`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendCommand {
    pub task_name: String,
    pub queue_name: String,
    pub priority: i32,
    pub stored_input: String,
    pub task_options: Option<String>,
    pub execution: crate::core::task::execution::Execution,
    pub max_retries: i32,
    pub retention_class_key: String,
    pub retain_rerun_input: bool,
    pub fingerprint_version: i16,
    pub fingerprint: Vec<u8>,
}

impl SendCommand {
    /// The stored values of an unkeyed send with no deadline and no delay,
    /// with the rerun-input posture `retain_rerun_input`: the values that
    /// [`PostgresBroker::enqueue`] writes for the same arguments. Touches no
    /// database.
    ///
    /// Errors: `EnqueueContract`, the refusals of `enqueue` before its
    /// write: an input that the rules refuse, options that do not parse or
    /// whose retry policy does not fit, a value the fingerprint refuses.
    pub fn prepare(
        task_name: &str,
        queue_name: &str,
        priority: i32,
        input: &str,
        task_options: Option<&str>,
        retention: EnqueueRetention,
        retain_rerun_input: bool,
    ) -> Result<Self, BrokerError> {
        check_sent_input(input)?;
        let max_retries = enqueue_max_retries(task_name, task_options)?;
        let retention_class_key = retention.class_key();
        let facts = prepare_enqueue_facts(
            task_name,
            queue_name,
            priority,
            input,
            None,
            None,
            task_options,
            Some(&retention_class_key),
            retain_rerun_input,
            None,
        )
        .map_err(|error| BrokerError::EnqueueContract(EnqueueRefusal::from(error)))?;
        Ok(Self {
            task_name: task_name.to_owned(),
            queue_name: queue_name.to_owned(),
            priority,
            stored_input: input.to_owned(),
            task_options: task_options.map(str::to_owned),
            execution: facts.execution,
            max_retries,
            retention_class_key: facts.retention_class_key,
            retain_rerun_input: facts.retain_rerun_input,
            fingerprint_version: facts.command_fingerprint_version,
            fingerprint: facts.command_fingerprint.to_vec(),
        })
    }
}

/// PostgreSQL-backed task broker.
///
/// Two sets of pools on the same URLs:
/// - `core`: the statement layer's pools ([`crate::db::CorePool`], one per
///   role). The broker's own moved call sites run on them.
/// - `pool` and `session_pool`: sqlx pools. They are the adapter for the
///   call sites that have not moved to the statement layer yet: the broker's
///   lifecycle statements, the worker, the workflow engine, the scheduler,
///   the web layer and the listeners, which borrow them through
///   [`PostgresBroker::pool`] and [`PostgresBroker::session_pool`]. The
///   schema install runs on the core pools.
///
/// No pool connects when the broker is built: each opens its first
/// connection at its first checkout.
///
/// Every LISTEN of the broker goes through the process listener of its
/// session URL: one connection, shared with the other brokers of the process
/// on the same URL.
pub struct PostgresBroker {
    core: CoreHandle,
    statement_log: StatementLog,
    reserve: ReservePools,
    pool: PgPool,
    session_pool: PgPool,
    pgbouncer_transaction_mode: bool,
    retain_rerun_input_default: bool,
    idempotency_reservation_window_micros: i64,
    listener: ProcessListener,
    schema_initialized: tokio::sync::OnceCell<()>,
    schema_initialization_mode: SchemaInitializationMode,
    /// The client family of this handle. The constructors of this type serve
    /// the Rust client; the Python bridge sets `Python` with
    /// [`PostgresBroker::with_client_family`].
    client_family: ClientFamily,
    /// Set once the family check before the first write passed.
    client_family_admitted: tokio::sync::OnceCell<()>,
    /// Whether this broker saw the staged result function published. The
    /// result read keeps it ([`read_task_result`]).
    result_reader: ResultReaderSeen,
}

impl PostgresBroker {
    /// Construct a test broker on the database of a test's sqlx pool. The
    /// sqlx pool is the adapter pool; the core pools connect to the same
    /// database ([`crate::broker::pools::handle_for_test_pool`]).
    ///
    /// The broker has no session URL, so it has no listener: result waits
    /// poll, and an operation that needs a listener is refused with
    /// [`ListenError::NoSessionUrl`]. Use
    /// [`from_pool_and_session_url`](Self::from_pool_and_session_url) for a
    /// broker with a listener.
    #[cfg(test)]
    pub fn from_pool(pool: PgPool) -> Self {
        Self::from_pool_with_schema_initialization_mode(
            pool,
            SchemaInitializationMode::MigrateAndValidate,
            ProcessListener::without_session_url(),
        )
    }

    /// Construct a test broker on the database of a test's sqlx pool, with
    /// its listener on `session_url`: the process listener of that URL, which
    /// opens its connection at the first subscription with the default socket
    /// settings (`DEFAULT_SOCKET_SETTINGS`). The URL is parsed then.
    #[cfg(test)]
    pub fn from_pool_and_session_url(pool: PgPool, session_url: &str) -> Self {
        Self::from_pool_with_schema_initialization_mode(
            pool,
            SchemaInitializationMode::MigrateAndValidate,
            ProcessListener::on_session_url(session_url, DEFAULT_SOCKET_SETTINGS),
        )
    }

    /// Construct a test monitoring broker that never applies migrations or
    /// checks the fleet cutover gate. It has no session URL, so it has no
    /// listener.
    #[cfg(test)]
    pub fn from_pool_observe_only(pool: PgPool) -> Self {
        Self::from_pool_with_schema_initialization_mode(
            pool,
            SchemaInitializationMode::ObserveOnly,
            ProcessListener::without_session_url(),
        )
    }

    #[cfg(test)]
    fn from_pool_with_schema_initialization_mode(
        pool: PgPool,
        schema_initialization_mode: SchemaInitializationMode,
        listener: ProcessListener,
    ) -> Self {
        let reserve_runtime = PgPoolOptions::new()
            .max_connections(WORKER_RESERVED_CONNECTIONS.get())
            .connect_lazy_with((*pool.connect_options()).clone());
        Self {
            core: crate::broker::pools::handle_for_test_pool(&pool),
            statement_log: StatementLog::Off,
            reserve: ReservePools {
                maintenance: reserve_runtime.clone(),
                runtime: reserve_runtime,
            },
            session_pool: pool.clone(),
            pool,
            pgbouncer_transaction_mode: false,
            retain_rerun_input_default: false,
            idempotency_reservation_window_micros: IDEMPOTENCY_WINDOW_DEFAULT_MICROS,
            listener,
            schema_initialized: tokio::sync::OnceCell::new(),
            schema_initialization_mode,
            client_family: ClientFamily::Rust,
            client_family_admitted: tokio::sync::OnceCell::new(),
            result_reader: ResultReaderSeen::default(),
        }
    }

    /// Construct a test broker with an explicit keyed-enqueue reservation
    /// window. `None` selects the 24-hour contract default.
    #[cfg(test)]
    pub fn from_pool_with_idempotency_reservation_window(
        pool: PgPool,
        window: Option<chrono::Duration>,
    ) -> Result<Self, BrokerError> {
        Self::from_pool_with_enqueue_policy(pool, false, window)
    }

    /// Construct a test broker with both enqueue-time broker policies.
    #[cfg(test)]
    pub fn from_pool_with_enqueue_policy(
        pool: PgPool,
        retain_rerun_input_default: bool,
        window: Option<chrono::Duration>,
    ) -> Result<Self, BrokerError> {
        let micros = idempotency_reservation_window_micros(window)?;
        let mut broker = Self::from_pool(pool);
        broker.retain_rerun_input_default = retain_rerun_input_default;
        broker.idempotency_reservation_window_micros = micros;
        Ok(broker)
    }

    /// Connect using a raw database URL.
    ///
    /// A URL whose scheme is not `postgresql://` or `postgres://` is refused
    /// with [`BrokerError::InvalidConnectionSettings`] before any connection.
    pub async fn connect(database_url: &str) -> Result<Self, BrokerError> {
        Self::connect_with(&PostgresConfig::from_url(database_url)).await
    }

    /// Connect using a `PostgresConfig` from horsies-core.
    pub async fn connect_with(config: &PostgresConfig) -> Result<Self, BrokerError> {
        Self::connect_with_schema_initialization_mode(
            config,
            SchemaInitializationMode::MigrateAndValidate,
        )
        .await
    }

    /// Connect a monitoring broker without applying migrations or checking
    /// the task-history fleet gate.
    pub async fn connect_observe_only(config: &PostgresConfig) -> Result<Self, BrokerError> {
        Self::connect_with_schema_initialization_mode(config, SchemaInitializationMode::ObserveOnly)
            .await
    }

    /// Connect the broker of a worker's child: no migration and no fleet
    /// gate check, as [`Self::connect_observe_only`]; the main URL pool keeps
    /// `kept_open` connections open and, with `check`, pings every pooled
    /// connection at checkout ([`PoolProfile::Child`]).
    ///
    /// Must run inside a tokio runtime when `kept_open` keeps connections
    /// open: the pool starts the task that opens them.
    pub async fn connect_child(
        config: &PostgresConfig,
        kept_open: crate::db::KeptOpen,
        check: bool,
    ) -> Result<Self, BrokerError> {
        Self::connect_with_profile(
            config,
            SchemaInitializationMode::ObserveOnly,
            PoolProfile::Child { kept_open, check },
        )
        .await
    }

    /// Connect a worker's broker: the main URL pool is the worker profile
    /// pool, which keeps [`crate::db::WORKER_RESERVED_CONNECTIONS`] free from
    /// batch checkouts for the reaper and the per-task writes.
    pub async fn connect_worker_with(config: &PostgresConfig) -> Result<Self, BrokerError> {
        Self::connect_with_profile(
            config,
            SchemaInitializationMode::MigrateAndValidate,
            PoolProfile::Worker,
        )
        .await
    }

    async fn connect_with_schema_initialization_mode(
        config: &PostgresConfig,
        schema_initialization_mode: SchemaInitializationMode,
    ) -> Result<Self, BrokerError> {
        Self::connect_with_profile(config, schema_initialization_mode, PoolProfile::Producer).await
    }

    /// Checks the config and builds the core pools and the sqlx adapter
    /// pools. Opens no connection: each pool connects at its first checkout,
    /// so a broker whose operations all run on the core pools holds no
    /// adapter connection.
    async fn connect_with_profile(
        config: &PostgresConfig,
        schema_initialization_mode: SchemaInitializationMode,
        profile: PoolProfile,
    ) -> Result<Self, BrokerError> {
        config.validate()?;
        let core = CoreHandle::open(config, profile)?;
        let connect_options = pg_connect_options(&config.database_url, config.echo)?;
        let runtime_options = match profile {
            PoolProfile::Producer | PoolProfile::Child { .. } => pg_pool_options(config),
            PoolProfile::Worker => pg_worker_runtime_pool_options(config),
        };
        let reserve_runtime =
            pg_reserve_pool_options(config).connect_lazy_with(connect_options.clone());
        let pool = runtime_options.connect_lazy_with(connect_options);

        let session_pool = if config.effective_session_database_url() == config.database_url {
            pool.clone()
        } else {
            let session_options =
                pg_connect_options(config.effective_session_database_url(), config.echo)?;
            pg_session_pool_options(config).connect_lazy_with(session_options)
        };
        let reserve = ReservePools {
            maintenance: match config.effective_session_database_url() == config.database_url {
                true => reserve_runtime.clone(),
                false => session_pool.clone(),
            },
            runtime: reserve_runtime,
        };

        Ok(Self {
            core,
            statement_log: StatementLog::from_echo(config.echo),
            reserve,
            pool,
            session_pool,
            pgbouncer_transaction_mode: config.pgbouncer_transaction_mode,
            retain_rerun_input_default: config.retain_rerun_input_default,
            idempotency_reservation_window_micros: IDEMPOTENCY_WINDOW_DEFAULT_MICROS,
            listener: ProcessListener::on_session_url(
                config.effective_session_database_url(),
                config.socket_settings(),
            ),
            schema_initialized: tokio::sync::OnceCell::new(),
            schema_initialization_mode,
            client_family: ClientFamily::Rust,
            client_family_admitted: tokio::sync::OnceCell::new(),
            result_reader: ResultReaderSeen::default(),
        })
    }

    /// Connect with an explicit keyed-enqueue reservation window. Validation
    /// happens before any database connection is attempted.
    pub async fn connect_with_idempotency_reservation_window(
        config: &PostgresConfig,
        window: Option<chrono::Duration>,
    ) -> Result<Self, BrokerError> {
        let micros = idempotency_reservation_window_micros(window)?;
        let mut broker = Self::connect_with(config).await?;
        broker.idempotency_reservation_window_micros = micros;
        Ok(broker)
    }

    /// Connect with the schema behaviour and the keyed-enqueue reservation
    /// window of a client manifest, with the pools of `profile`: a producer,
    /// a worker process (the main URL pool keeps
    /// [`WORKER_RESERVED_CONNECTIONS`] free from batch checkouts), or a
    /// worker's child. The window is checked before any database connection
    /// is attempted.
    pub async fn connect_with_policies(
        config: &PostgresConfig,
        schema_initialization_mode: SchemaInitializationMode,
        window: chrono::Duration,
        profile: PoolProfile,
    ) -> Result<Self, BrokerError> {
        let micros = idempotency_reservation_window_micros(Some(window))?;
        let mut broker =
            Self::connect_with_profile(config, schema_initialization_mode, profile).await?;
        broker.idempotency_reservation_window_micros = micros;
        Ok(broker)
    }

    /// Install the schema: run the embedded migrations that the database
    /// lacks, and record or check this handle's client family
    /// ([`crate::broker::migrations`]). A broker built with
    /// [`SchemaInitializationMode::ObserveOnly`] runs nothing.
    ///
    /// Runs on the session-work core pool (the session pool, or the main
    /// pool when the URLs are equal). Bookkeeps in the horsies-owned
    /// `horsies_migrations` table, so it never collides with an
    /// application's own migration runner. A close waits for it as for a
    /// write.
    ///
    /// Errors: [`BrokerError::SchemaRefused`] and
    /// [`BrokerError::ClientFamilyMismatch`] (nothing was changed; not
    /// retryable); [`BrokerError::InstallLock`]; [`BrokerError::Statement`];
    /// [`BrokerError::RowDecode`]; [`BrokerError::PoolSettings`] when a
    /// closed handle cannot open its pools again.
    pub async fn migrate(&self) -> Result<(), BrokerError> {
        match self.schema_initialization_mode {
            SchemaInitializationMode::ObserveOnly => return Ok(()),
            SchemaInitializationMode::MigrateAndValidate => {}
        }
        let (open, _write) = self.core.pools_for_write()?;
        crate::broker::migrations::install_schema_on(
            open.pools().session_work(),
            self.client_family,
            crate::broker::migrations::InstallLockPlan::fleet(),
            self.statement_log,
        )
        .await
        .map_err(BrokerError::from)
    }

    /// The same handle for a client of `family`. The install records it, and
    /// every write checks it once.
    pub fn with_client_family(mut self, family: ClientFamily) -> Self {
        self.client_family = family;
        self
    }

    /// The same handle with no listener (Python's
    /// `PostgresBroker(assume_initialized=True)`): a result wait reads again
    /// every 200 ms (`TASK_RESULT_REPOLL_POLLING`), and an operation that
    /// needs a listener answers `LISTENER_DISABLED`. Opens and closes
    /// nothing: the listener of a connected broker opens its connection at
    /// the first subscription.
    pub fn without_listener(mut self) -> Self {
        self.listener = ProcessListener::without_session_url();
        self
    }

    /// Check once, before the first write of this handle, that the database
    /// records this handle's client family. Reads never call it. A handle
    /// whose install ran in this process has passed it already. A database of
    /// the other family, or with no family row, is
    /// `BrokerError::ClientFamilyMismatch`, not retryable; a failed read
    /// ([`FamilyCheckFailure`]) is `BrokerError::Statement`,
    /// `BrokerError::PoolSettings` or `BrokerError::RowDecode`, and is not
    /// kept, so the next write checks again. One statement, once.
    pub async fn ensure_client_family_for_write(&self) -> Result<(), BrokerError> {
        match self.check_client_family_for_write().await {
            Ok(()) => Ok(()),
            Err(FamilyCheckError::Mismatch(mismatch)) => Err(mismatch.into()),
            Err(FamilyCheckError::ReadFailed(failure)) => Err(failure.into()),
        }
    }

    /// The check of [`PostgresBroker::ensure_client_family_for_write`], with
    /// its closed error. A pass is kept for the life of the handle; a
    /// mismatch or a failed read is not.
    async fn check_client_family_for_write(&self) -> Result<(), FamilyCheckError> {
        self.client_family_admitted
            .get_or_try_init(|| async {
                let recorded = self
                    .read_client_family()
                    .await
                    .map_err(FamilyCheckError::ReadFailed)?;
                admit_family(recorded, self.client_family).map_err(FamilyCheckError::Mismatch)
            })
            .await?;
        Ok(())
    }

    /// Reads the family row on the main core pool. Checkout and bound as
    /// [`PostgresBroker::query_main`]. Writes nothing.
    async fn read_client_family(&self) -> Result<RecordedFamily, FamilyCheckFailure> {
        let open = self
            .core
            .pools()
            .map_err(FamilyCheckFailure::PoolsNotReopened)?;
        let statement = READ_CLIENT_FAMILY.bind().finish();
        self.log_statement(statement.statement());
        let rows = open
            .main()
            .query(
                CheckoutClass::Batch,
                &statement,
                StatementBound::ConnectionTimeouts,
            )
            .await
            .map_err(FamilyCheckFailure::Statement)?;
        let stored: Option<String> = match rows.first() {
            Some(row) => Some(
                column(row, &READ_CLIENT_FAMILY, "family")
                    .map_err(FamilyCheckFailure::FamilyColumnUndecodable)?,
            ),
            None => None,
        };
        Ok(recorded_family_from(stored))
    }

    /// Install the schema once for this broker ([`PostgresBroker::migrate`]).
    ///
    /// The first successful call installs or checks the schema; later calls
    /// are a no-op. A failure is not kept, so a later call runs the install
    /// again. The install ends only at this build's whole chain (a newer or
    /// unknown schema is refused), and it records or checks this handle's
    /// family, so writes of this handle need no second family read.
    pub async fn ensure_schema_initialized(&self) -> Result<(), BrokerError> {
        match self.schema_initialization_mode {
            SchemaInitializationMode::ObserveOnly => return Ok(()),
            SchemaInitializationMode::MigrateAndValidate => {}
        }
        self.schema_initialized
            .get_or_try_init(|| async {
                self.migrate().await?;
                let _already_admitted = self.client_family_admitted.set(());
                Ok::<(), BrokerError>(())
            })
            .await?;
        Ok(())
    }

    /// The schema behavior selected when this broker was constructed.
    pub const fn schema_initialization_mode(&self) -> SchemaInitializationMode {
        self.schema_initialization_mode
    }

    /// The sqlx pool on `database_url`: the adapter for call sites that
    /// have not moved to the statement layer. Moved call sites use the core
    /// pools.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The sqlx pool on the session URL, used for partition maintenance and
    /// LISTEN/NOTIFY: the adapter for call sites that have not moved to the
    /// statement layer.
    pub fn session_pool(&self) -> &PgPool {
        &self.session_pool
    }

    /// The sqlx pool of the worker reserve on `database_url`:
    /// [`WORKER_RESERVED_CONNECTIONS`] connections, opened at first use,
    /// that the users of [`PostgresBroker::pool`] cannot take. The reaper
    /// takes its gate and runs its pass on it.
    pub fn reserve_pool(&self) -> &PgPool {
        &self.reserve.runtime
    }

    /// The pool of the reaper's partition maintenance: the session pool when
    /// the session URL differs, else [`PostgresBroker::reserve_pool`].
    pub fn reserve_maintenance_pool(&self) -> &PgPool {
        &self.reserve.maintenance
    }

    /// The open core pools, for a lifecycle writer that runs on the main
    /// core pool ([`crate::broker::attempt_writes`]) or a workflow step
    /// (`workflow_engine::steps`), and the token of its write:
    /// [`PostgresBroker::close`] waits for the write while the token lives.
    /// In a worker the main core pool is the worker profile pool. An `Idle`
    /// handle opens its pools first.
    ///
    /// Errors: [`BrokerError::PoolSettings`] when the pools cannot be built.
    pub(crate) fn core_pools_for_write(
        &self,
    ) -> Result<(std::sync::Arc<OpenPools>, WriteInFlight), BrokerError> {
        Ok(self.core.pools_for_write()?)
    }

    /// The open core pools, for a read of a client handle that runs on the
    /// main core pool ([`crate::broker::lifecycle_operations`]). A close does
    /// not wait for it. An `Idle` handle opens its pools first.
    ///
    /// Errors: [`BrokerError::PoolSettings`] when the pools cannot be built.
    pub(crate) fn core_pools_for_read(&self) -> Result<std::sync::Arc<OpenPools>, BrokerError> {
        Ok(self.core.pools()?)
    }

    /// Whether this broker logs each core statement it runs, for the
    /// lifecycle writers that run on the main core pool.
    pub(crate) fn statement_log(&self) -> StatementLog {
        self.statement_log
    }

    /// Runs one statement on the main core pool and reads every row.
    ///
    /// The checkout passes the reserve gate ([`CheckoutClass::Batch`]): the
    /// moved call sites are producer and monitoring work, never the reaper or
    /// a per-task write. Bound: the connection's own timeouts, as the sqlx
    /// path.
    ///
    /// Errors: [`BrokerError::PoolSettings`] when a closed handle cannot open
    /// its pools again; [`BrokerError::Statement`].
    pub(crate) async fn query_main(
        &self,
        statement: &BoundStatement<'_>,
    ) -> Result<Vec<Row>, BrokerError> {
        let open = self.core.pools()?;
        self.log_statement(statement.statement());
        Ok(open
            .main()
            .query(
                CheckoutClass::Batch,
                statement,
                StatementBound::ConnectionTimeouts,
            )
            .await?)
    }

    /// Runs one statement on the main core pool and reads the count of
    /// affected rows. Checkout, bound and errors as
    /// [`PostgresBroker::query_main`].
    async fn execute_main(&self, statement: &BoundStatement<'_>) -> Result<u64, BrokerError> {
        let open = self.core.pools()?;
        self.log_statement(statement.statement());
        Ok(open
            .main()
            .execute(
                CheckoutClass::Batch,
                statement,
                StatementBound::ConnectionTimeouts,
            )
            .await?)
    }

    /// Runs one read on the main core pool and reads each row with `read`.
    ///
    /// Errors: [`BrokerError::Statement`] when the statement failed;
    /// [`BrokerError::RowDecode`] for the first row that does not read.
    async fn read_main<T>(
        &self,
        statement: &BoundStatement<'_>,
        read: fn(&Row, &TypedStatement) -> Result<T, RowDecodeError>,
    ) -> Result<Vec<T>, BrokerError> {
        let rows = self.query_main(statement).await?;
        Ok(read_rows(&rows, statement.statement(), read)?)
    }

    fn log_statement(&self, statement: &TypedStatement) {
        self.statement_log.log(statement);
    }

    /// Whether this broker's runtime pool is configured for PgBouncer
    /// transaction pooling.
    pub fn pgbouncer_transaction_mode(&self) -> bool {
        self.pgbouncer_transaction_mode
    }

    /// Rerun one retained terminal task in a broker-owned transaction on the
    /// main core pool ([`CheckoutClass::Batch`]): the source read, then one
    /// `horsies_enqueue_task` with the lineage `RERUN`, which claims the key
    /// of a keyed rerun (`core::history::rerun::operations`). A close waits
    /// for it as for a write.
    ///
    /// The handle checks its client family before the transaction: a
    /// database of another family, or with no family row, is
    /// `RerunError::ClientFamilyMismatch`; a family row that could not be
    /// read is `RerunError::FamilyCheckFailed`. In both cases nothing is
    /// written.
    ///
    /// Errors: as the rerun in its transaction, and
    /// [`RerunError::PoolSettings`] when a closed handle cannot open its
    /// pools again. After an error nothing committed, except after
    /// `OutcomeUnknown` at `COMMIT`.
    pub async fn rerun_task(
        &self,
        command: RerunTask,
        policy: RerunEnqueuePolicy,
    ) -> Result<RerunOutcome, RerunError> {
        self.check_client_family_for_write()
            .await
            .map_err(rerun_family_error)?;
        let (open, _write) = self.core.pools_for_write()?;
        let mut connection = open
            .main()
            .checkout(CheckoutClass::Batch)
            .await
            .map_err(|cause| StatementFailure::NotSent {
                statement: rerun_first_statement().name(),
                cause: NotSentCause::Checkout(cause),
            })?;
        let mut transaction = connection.begin(IsolationLevel::ServerDefault).await?;
        let outcome = rerun_task_in_transaction(
            &mut transaction,
            self.statement_log,
            &command,
            &policy,
            self.client_family,
        )
        .await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    /// The broker's listener: the process listener of the session URL, one
    /// LISTEN connection shared by every subscriber of every broker of the
    /// process on that URL.
    pub(crate) fn listener(&self) -> &ProcessListener {
        &self.listener
    }

    // -----------------------------------------------------------------------
    // Task lifecycle operations
    // -----------------------------------------------------------------------

    /// Enqueue a new task: one `horsies_enqueue_task` statement in
    /// autocommit on the main core pool, checked out as batch work, after
    /// the client family check (Python `PostgresBroker.enqueue_async`).
    /// Returns the id of the task that holds the send: the sent id (minted
    /// here when `predetermined_task_id` is `None`), or the task that holds
    /// the idempotency key with the same command.
    ///
    /// `input` is the encoded task input as one JSON value; it is stored as
    /// written. The input follows the rules of a user value
    /// (`check_sent_input`); a refusal is `EnqueueContract`, which the send
    /// maps to `VALIDATION_FAILED`. A task id that is taken answers by the
    /// stored version-1 command fingerprint and `sent_at` (the exact-id
    /// resend check). `send_kind` is [`SendKind::Repeat`] for a send retry:
    /// a task that finished then answers from history and does not run
    /// again; [`SendKind::First`] does not read history.
    ///
    /// Errors: `EnqueueContract` (refused before any statement: both
    /// `enqueued_at` and `enqueue_delay_seconds`, a future `sent_at` with
    /// neither, an input or options that the rules refuse);
    /// `IdempotencyKeyConflict`, `PayloadMismatch` and
    /// `EnqueueConflictUnverifiable` (nothing written, no reservation left);
    /// the client family check's errors; `PoolSettings`; `Statement` (nothing
    /// written, or `OutcomeUnknown`: a resend of the same send with
    /// [`SendKind::Repeat`] answers with the task id); `RowDecode`.
    ///
    /// Panics: when the function returns other than one row, or an outcome
    /// it does not define (Python raises `AssertionError`).
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue(
        &self,
        task_name: &str,
        input: &str,
        queue_name: &str,
        priority: i32,
        sent_at: Option<DateTime<Utc>>,
        enqueued_at: Option<DateTime<Utc>>,
        good_until: Option<DateTime<Utc>>,
        task_options: Option<&str>,
        predetermined_task_id: Option<Uuid>,
        enqueue_delay_seconds: Option<i64>,
        idempotency_key: Option<&str>,
        retention: EnqueueRetention,
        retain_rerun_input: Option<bool>,
        send_kind: SendKind,
    ) -> Result<Uuid, BrokerError> {
        let start = EnqueueStart::of(enqueued_at, enqueue_delay_seconds)?;
        if sent_at.is_some_and(|value| value > Utc::now() + chrono::Duration::seconds(5))
            && matches!(start, EnqueueStart::Now)
        {
            return Err(BrokerError::EnqueueContract(EnqueueRefusal::by_rule(
                SendPart::Input,
                "sent_at is in the future without enqueued_at or enqueue_delay_seconds; sent_at is a call-site timestamp".to_owned(),
            )));
        }
        check_sent_input(input)?;
        let task_id = match predetermined_task_id {
            Some(task_id) => task_id,
            // The task id is a fact of the send's input; a send with no id
            // mints it here.
            None => crate::core::history::identity::uuid7::mint_task_id().map_err(|error| {
                BrokerError::EnqueueContract(EnqueueRefusal::by_rule(
                    SendPart::Input,
                    format!("task identity mint failed: {error}"),
                ))
            })?,
        };
        // The options are read with the strict rule before the retry limit,
        // so a text that does not read names its reader's error (Python
        // `_parse_enqueue_task_options`).
        let _options = parse_optional_object(task_options)
            .map_err(|error| BrokerError::EnqueueContract(EnqueueRefusal::from(error)))?;
        let max_retries = enqueue_max_retries(task_name, task_options)?;
        let effective_sent_at = sent_at_at_stored_precision(sent_at.unwrap_or_else(Utc::now))?;
        let retain_rerun_input = retain_rerun_input.unwrap_or(self.retain_rerun_input_default);
        let retention_class_key = retention.class_key();
        let facts = prepare_enqueue_facts(
            task_name,
            queue_name,
            priority,
            input,
            good_until,
            enqueue_delay_seconds,
            task_options,
            Some(&retention_class_key),
            retain_rerun_input,
            idempotency_key,
        )
        .map_err(|error| BrokerError::EnqueueContract(EnqueueRefusal::from(error)))?;
        let key = EnqueueKey::of(
            facts.idempotency_key_digest,
            self.idempotency_reservation_window_micros,
        );
        self.ensure_client_family_for_write().await?;
        let (open, _write) = self.core.pools_for_write()?;

        let execution = facts.execution.as_str();
        let fingerprint: &[u8] = facts.command_fingerprint.as_slice();
        let (start_kind, start_at, start_delay_seconds) = start.columns();
        let (key_kind, key_scope_version, key_window_micros) = key.columns();
        let key_digest = key.digest();
        let send_kind_text = send_kind.as_sql();
        let statement = &ENQUEUE_TASK;
        let bound = statement
            .bind()
            .value(&task_id)
            .value(&task_name)
            .value(&queue_name)
            .value(&priority)
            .value(&input)
            .value(&task_options)
            .value(&execution)
            .value(&facts.retention_class_key)
            .value(&facts.retain_rerun_input)
            .value(&max_retries)
            .value(&good_until)
            .value(&effective_sent_at)
            .value(&start_kind)
            .value(&start_at)
            .value(&start_delay_seconds)
            .value(&facts.command_fingerprint_version)
            .value(&fingerprint)
            .value(&key_kind)
            .value(&key_digest)
            .value(&key_scope_version)
            .value(&key_window_micros)
            .value(&send_kind_text)
            .finish();
        self.log_statement(statement);
        let rows = open
            .main()
            .query(
                CheckoutClass::Batch,
                &bound,
                StatementBound::ConnectionTimeouts,
            )
            .await?;
        let [row] = rows.as_slice() else {
            panic!(
                "horsies_enqueue_task returned {} rows for task {task_id}; it returns one",
                rows.len()
            );
        };
        let outcome_text: String = column(row, statement, "outcome")?;
        let answering_task_id: Uuid = column(row, statement, "task_id")?;
        let outcome = match decode_enqueue_outcome(&outcome_text, answering_task_id) {
            Ok(outcome) => outcome,
            Err(contract) => panic!("{contract} for task {task_id}"),
        };
        match outcome {
            EnqueueTaskOutcome::Inserted => {
                tracing::debug!(task_id = %task_id, task_name, queue = queue_name, "task enqueued");
            }
            EnqueueTaskOutcome::ConflictUnverifiable => {
                tracing::warn!(
                    task_id = %task_id,
                    task_name,
                    "enqueue conflict but row disappeared before verification — cannot verify payload identity",
                );
            }
            EnqueueTaskOutcome::SameCommand
            | EnqueueTaskOutcome::KeyReplay { .. }
            | EnqueueTaskOutcome::KeyConflict { .. }
            | EnqueueTaskOutcome::PayloadMismatch => {}
        }
        enqueue_result(outcome, task_id, task_name)
    }

    /// The stored values of an unkeyed send with no deadline and no delay,
    /// without a write (Python `PostgresBroker.prepare_send_command`): the
    /// values [`PostgresBroker::enqueue`] writes for the same arguments, with
    /// the broker's default rerun-input posture ([`SendCommand::prepare`]).
    /// One command serves every row of the send, for example each slot of a
    /// schedule fire: the version-1 fingerprint leaves the task id and
    /// `sent_at` out. Touches no database.
    ///
    /// Errors: those of [`SendCommand::prepare`].
    pub fn prepare_send_command(
        &self,
        task_name: &str,
        queue_name: &str,
        priority: i32,
        input: &str,
        task_options: Option<&str>,
        retention: EnqueueRetention,
    ) -> Result<SendCommand, BrokerError> {
        SendCommand::prepare(
            task_name,
            queue_name,
            priority,
            input,
            task_options,
            retention,
            self.retain_rerun_input_default,
        )
    }

    /// Expire a CLAIMED task whose `good_until` passed before user code started.
    ///
    /// Runs through `horsies_expire_owned_claim`
    /// ([`crate::broker::expiry_and_orphans::expire_owned_claim`]), which
    /// fences on worker ownership but deliberately not on the claim
    /// generation: once the deadline has passed, expiry is the correct
    /// outcome for whichever generation holds the row. One statement on the
    /// main core pool, checked out as reserved work, within `deadline`, the
    /// deadline of the start attempt whose miss path it is
    /// ([`crate::broker::child_writes`]). An end and a replay of the expiry
    /// both mean the task is expired, as Python's
    /// `_expire_claimed_task_before_start` (`core/worker/child_runner.py`)
    /// reads them; any other outcome is logged with its evidence at the
    /// adapter boundary. The stored error is Python's: `TASK_EXPIRED` with
    /// its message and the task and worker ids as `data`.
    ///
    /// Errors: [`BrokerError::Statement`] (nothing sent when `deadline`
    /// passed first; nothing written, a cancel at the bound included; or
    /// `OutcomeUnknown`), or [`BrokerError::TerminalizationContract`] for a
    /// row outside the wire contract.
    pub async fn expire_claimed_task_before_start(
        &self,
        task_id: Uuid,
        worker_id: &str,
        deadline: crate::broker::child_writes::WriteDeadline,
    ) -> Result<PrestartExpiry, BrokerError> {
        let (open, _write) = self.core_pools_for_write()?;
        let end = crate::broker::expiry_and_orphans::expire_owned_claim(
            open.main(),
            self.statement_log,
            task_id,
            worker_id,
            deadline,
        )
        .await?;
        Ok(match end {
            crate::broker::expiry_and_orphans::TaskRowEnd::Ended { .. }
            | crate::broker::expiry_and_orphans::TaskRowEnd::Refused(
                crate::core::lifecycle::AttemptWriteRefusal::AlreadyApplied { .. },
            ) => PrestartExpiry::Expired,
            crate::broker::expiry_and_orphans::TaskRowEnd::Refused(
                crate::core::lifecycle::AttemptWriteRefusal::LostClaim { .. }
                | crate::core::lifecycle::AttemptWriteRefusal::SourceStateConflict { .. }
                | crate::core::lifecycle::AttemptWriteRefusal::TaskAbsent { .. },
            ) => PrestartExpiry::NotExpired,
        })
    }

    /// Requeue a task for retry with an optional delay.
    ///
    /// `retry_count` is incremented from the CAS-locked row itself.
    /// Returns `true` if the update was applied (task was RUNNING).
    /// Returns `false` if the task was no longer RUNNING (e.g., already
    /// marked FAILED by the stale-task reaper).
    ///
    /// One statement in autocommit on the main core pool, as a per-task
    /// write ([`CheckoutClass::Reserved`]); a close waits for it. The fence
    /// is the owner (`worker_id`), with no claim generation.
    ///
    /// Errors: [`BrokerError::PoolSettings`] when a closed handle cannot open
    /// its pools again; [`BrokerError::Statement`]. After `OutcomeUnknown`
    /// the row may be PENDING; the same requeue sent again answers `false`.
    pub async fn requeue(
        &self,
        task_id: Uuid,
        next_retry_at: Option<DateTime<Utc>>,
        worker_id: &str,
    ) -> Result<bool, BrokerError> {
        let (open, _write) = self.core_pools_for_write()?;
        let statement = &REQUEUE;
        self.log_statement(statement);
        let rows = open
            .main()
            .query(
                CheckoutClass::Reserved,
                &statement
                    .bind()
                    .value(&task_id)
                    .value(&next_retry_at)
                    .value(&worker_id)
                    .finish(),
                StatementBound::ConnectionTimeouts,
            )
            .await?;
        let requeued = !rows.is_empty();

        match requeued {
            true => tracing::debug!(task_id = %task_id, "task requeued"),
            false => tracing::warn!(
                task_id = %task_id,
                "task requeue skipped: task no longer RUNNING or not owned by this worker"
            ),
        }
        Ok(requeued)
    }

    /// Get task attempts for a specific task (most recent first).
    pub async fn get_task_attempts(
        &self,
        task_id: Uuid,
    ) -> Result<Vec<TaskAttemptRow>, BrokerError> {
        let rows = self
            .read_main(
                &SELECT_TASK_ATTEMPTS.bind().value(&task_id).finish(),
                task_attempt_row,
            )
            .await?;
        if !rows.is_empty() {
            return Ok(rows);
        }

        let live_rows = self
            .query_main(&LIVE_TASK_EXISTS.bind().value(&task_id).finish())
            .await?;
        let live: bool = scalar(&one_row(live_rows, &LIVE_TASK_EXISTS)?, &LIVE_TASK_EXISTS)?;
        if live {
            return Ok(rows);
        }

        let TaskDetailResult::History(detail) = self.fetch_history_detail(task_id).await? else {
            return Ok(Vec::new());
        };
        Ok(detail
            .attempts
            .into_iter()
            .rev()
            .map(|record| TaskAttemptRow {
                task_id,
                attempt: record.attempt(),
                outcome: record.outcome().to_owned(),
                will_retry: record.will_retry(),
                started_at: record.started_at(),
                finished_at: record.finished_at(),
                error_code: record.error_code().map(str::to_owned),
                error_message: record.error_message().map(str::to_owned),
                failed_reason: record.failed_reason().map(str::to_owned),
                worker_id: record.worker_id().map(str::to_owned),
                worker_hostname: record.worker_hostname().map(str::to_owned),
                worker_pid: record.worker_pid(),
                worker_process_name: record.worker_process_name().map(str::to_owned),
            })
            .collect())
    }

    // -----------------------------------------------------------------------
    // Result retrieval
    // -----------------------------------------------------------------------

    /// Fetch a typed task result, optionally waiting for completion.
    ///
    /// 1. Parses `timeout` into a [`WaitLimit`]. A limit above
    ///    [`MAX_WAIT_LIMIT_MS`](crate::broker::wait_limit::MAX_WAIT_LIMIT_MS)
    ///    returns `RetrievalCode::InvalidInput` before any read.
    /// 2. Quick-checks the database for a terminal status.
    /// 3. If not done, subscribes a wake for the task id on `task_done` on
    ///    the broker's listener, reads again, and waits. A listener gap reads
    ///    at once. A broker with no listener, or a subscription that is
    ///    refused, makes the wait poll.
    /// 4. On timeout, returns `TaskResult::Err(TaskError(WaitTimeout))`.
    ///
    /// Every concurrent wait shares the broker's one listener connection.
    pub async fn get_result<T: DeserializeOwned>(
        &self,
        task_id: Uuid,
        timeout: Option<Duration>,
    ) -> Result<TaskResult<T>, BrokerError> {
        let start = Instant::now();
        let limit = match WaitLimit::parse(timeout) {
            Ok(limit) => limit,
            Err(refused) => {
                return Ok(TaskResult::Err(TaskError::builtin(
                    RetrievalCode::InvalidInput,
                    refused.to_string(),
                )));
            }
        };

        // Quick check — task may already be done, or may not exist.
        if let Some(outcome) = Box::pin(self.poll_result::<T>(task_id)).await? {
            return Ok(outcome);
        }

        let mut wait_mode = self.result_wait_mode(task_id).await;

        // Re-check after subscribing to avoid a race where the task completes
        // between our first check and the subscribe.
        if let Some(outcome) = Box::pin(self.poll_result::<T>(task_id)).await? {
            return Ok(outcome);
        }

        let deadline = limit.deadline_from(start);

        loop {
            // For a timed wait, stop once the deadline has passed (terminal /
            // not-found / WaitTimeout).
            if let Some(d) = deadline {
                if Instant::now() >= d {
                    let elapsed_ms = start.elapsed().as_millis() as u64;
                    return Box::pin(self.final_poll_or_timeout::<T>(task_id, elapsed_ms)).await;
                }
            }

            // Cap each wait at the repoll interval of the mode, so a lost
            // NOTIFY costs at most one interval. Never wait past the deadline.
            let interval = wait_mode.repoll_interval();
            let wait = match deadline {
                Some(d) => d.saturating_duration_since(Instant::now()).min(interval),
                None => interval,
            };
            wait_mode.pause(wait).await;
            if let Some(outcome) = Box::pin(self.poll_result::<T>(task_id)).await? {
                return Ok(outcome);
            }
        }
    }

    /// Fetch the verified outer result envelope without task-specific decoding.
    ///
    /// A terminal history row is resolved through the staged detail reader.
    /// When `timeout` expires for a live non-terminal task, the latest status is
    /// returned with no result. A task absent from both live and retained
    /// history returns [`RawResultRead::Absent`] with the reason of the
    /// history read. A limit above
    /// [`MAX_WAIT_LIMIT_MS`](crate::broker::wait_limit::MAX_WAIT_LIMIT_MS) is
    /// `INVALID_INPUT` before any read. A listener that cannot start makes the
    /// wait poll.
    pub async fn get_raw_result_record(
        &self,
        task_id: Uuid,
        timeout: Option<Duration>,
    ) -> BrokerResult<RawResultRead> {
        let start = Instant::now();
        let limit =
            WaitLimit::parse(timeout).map_err(|refused| invalid_input(refused.to_string()))?;
        match Box::pin(self.raw_result_probe(task_id)).await? {
            RawResultProbe::Record(record) => return Ok(RawResultRead::Found(record)),
            RawResultProbe::Absent(reason) => return Ok(RawResultRead::Absent { reason }),
            RawResultProbe::Waiting(_) | RawResultProbe::Retry => {}
        }

        let mut wait_mode = self.result_wait_mode(task_id).await;

        match Box::pin(self.raw_result_probe(task_id)).await? {
            RawResultProbe::Record(record) => return Ok(RawResultRead::Found(record)),
            RawResultProbe::Absent(reason) => return Ok(RawResultRead::Absent { reason }),
            RawResultProbe::Waiting(_) | RawResultProbe::Retry => {}
        }

        let deadline = limit.deadline_from(start);
        loop {
            if deadline.is_some_and(|at| Instant::now() >= at) {
                return match Box::pin(self.raw_result_probe(task_id)).await? {
                    RawResultProbe::Record(record) | RawResultProbe::Waiting(record) => {
                        Ok(RawResultRead::Found(record))
                    }
                    RawResultProbe::Absent(reason) => Ok(RawResultRead::Absent { reason }),
                    RawResultProbe::Retry => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                };
            }
            let interval = wait_mode.repoll_interval();
            let wait = deadline
                .map(|at| at.saturating_duration_since(Instant::now()).min(interval))
                .unwrap_or(interval);
            wait_mode.pause(wait).await;
            match Box::pin(self.raw_result_probe(task_id)).await? {
                RawResultProbe::Record(record) => return Ok(RawResultRead::Found(record)),
                RawResultProbe::Absent(reason) => return Ok(RawResultRead::Absent { reason }),
                RawResultProbe::Waiting(_) | RawResultProbe::Retry => {}
            }
        }
    }

    /// Subscribe a result wait on the broker's listener: a wake for this task
    /// id on `task_done`. A broker with no listener, or a refused
    /// subscription, makes the wait poll instead, as Python
    /// (`python/horsies/core/brokers/postgres.py:3276-3281`).
    async fn result_wait_mode(&self, task_id: Uuid) -> ResultWaitMode {
        let filter = PayloadFilter::Payload(task_id.to_string());
        match self
            .listener
            .subscribe_wake(fixed_channel(TASK_DONE_CHANNEL), filter)
            .await
        {
            Ok(subscription) => ResultWaitMode::Subscribed(subscription),
            Err(error @ ListenError::NoSessionUrl) => {
                tracing::debug!(%task_id, error = %error, "the result wait polls");
                ResultWaitMode::Polling
            }
            Err(error @ (ListenError::PlanRefused(_) | ListenError::Subscribe(_))) => {
                tracing::warn!(
                    %task_id,
                    error = %error,
                    "the task_done subscription was refused; the result wait polls instead",
                );
                ResultWaitMode::Polling
            }
        }
    }

    /// Fetch task metadata.
    ///
    /// By default, the `result` and `failed_reason` fields are excluded
    /// (returned as `None`), matching Python's
    /// `info(include_result=False, include_failed_reason=False)` default.
    /// Pass `true` for either flag to include the field of a terminal task's
    /// history row. A live task has no result, failed reason, error code or
    /// terminal time: those fields are `None` whatever the flags say.
    pub async fn get_task_info(
        &self,
        task_id: Uuid,
        include_result: bool,
        include_failed_reason: bool,
    ) -> BrokerResult<TaskInfoRead> {
        self.get_task_info_with_attempts(task_id, include_result, include_failed_reason, false)
            .await
    }

    /// Fetch task metadata and optionally compose execution attempts from the
    /// live attempt table or the immutable history snapshot.
    ///
    /// A task that neither the live table nor the task history holds is
    /// [`TaskInfoRead::Absent`] with the reason of the history read. When
    /// the live read finds no row and the history read then finds the task
    /// live (a send of that id committed between the two statements), the
    /// read runs again, at most [`TASK_INFO_READ_PASSES`] times in all; a
    /// task that moves on every pass is `TASK_INFO_QUERY_FAILED`, retryable.
    pub async fn get_task_info_with_attempts(
        &self,
        task_id: Uuid,
        include_result: bool,
        include_failed_reason: bool,
        include_attempts: bool,
    ) -> BrokerResult<TaskInfoRead> {
        for _pass in 0..TASK_INFO_READ_PASSES {
            match Box::pin(self.task_info_pass(
                task_id,
                include_result,
                include_failed_reason,
                include_attempts,
            ))
            .await?
            {
                TaskInfoPass::Read(read) => return Ok(read),
                TaskInfoPass::BecameLive => {}
            }
        }
        Err(BrokerOperationError {
            code: BrokerErrorCode::TaskInfoQueryFailed,
            message: format!(
                "task {task_id} was absent from the live table and live in the history read \
                 on {TASK_INFO_READ_PASSES} passes in a row"
            ),
            retryable: true,
        })
    }

    /// One pass of [`Self::get_task_info_with_attempts`]: the live row, else
    /// the history record or the absence reason.
    async fn task_info_pass(
        &self,
        task_id: Uuid,
        include_result: bool,
        include_failed_reason: bool,
        include_attempts: bool,
    ) -> BrokerResult<TaskInfoPass> {
        let row = self
            .read_main(
                &GET_TASK_INFO.bind().value(&task_id).finish(),
                task_info_row,
            )
            .await
            .map_err(|error| BrokerOperationError {
                code: BrokerErrorCode::TaskInfoQueryFailed,
                message: error.to_string(),
                retryable: error.is_retryable(),
            })?
            .into_iter()
            .next();

        match row {
            Some(r) => {
                let mut info = r.into_task_info().map_err(task_info_history_error)?;
                if include_attempts {
                    info.attempts = Some(
                        self.get_task_attempts(task_id)
                            .await
                            .map_err(task_info_history_error)?
                            .into_iter()
                            .map(task_attempt_info_from_row)
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(task_info_history_error)?,
                    );
                }
                Ok(TaskInfoPass::Read(TaskInfoRead::Found(Box::new(info))))
            }
            None => {
                let detail = Box::pin(self.fetch_history_detail(task_id))
                    .await
                    .map_err(task_info_history_error)?;
                match detail {
                    TaskDetailResult::History(detail) => history_task_info(
                        detail,
                        include_result,
                        include_failed_reason,
                        include_attempts,
                    )
                    .map(|info| TaskInfoPass::Read(TaskInfoRead::Found(Box::new(info)))),
                    TaskDetailResult::Live { .. } => Ok(TaskInfoPass::BecameLive),
                    TaskDetailResult::Absent { reason, .. } => {
                        Ok(TaskInfoPass::Read(TaskInfoRead::Absent { reason }))
                    }
                }
            }
        }
    }

    /// The claimed tasks of a claim pass that must not be dispatched because
    /// their workflow is PAUSED or CANCELLED, after the settle wrote what
    /// each of them needs (Python `_filter_nonrunnable_workflow_tasks`,
    /// `core/worker/claiming.py:649-760`), through
    /// `horsies_workflow_claimed_tasks_settle`
    /// ([`crate::broker::workflow_claim_settle`]):
    /// - PAUSED: this worker's claim at its claim generation is abandoned,
    ///   and the node of an abandoned task goes back to READY with no task;
    /// - CANCELLED: the claim is cancelled, and the node of a cancelled task
    ///   is skipped.
    ///
    /// The workflow status is read under the workflow row lock, so the
    /// tasks of a workflow that a resume made RUNNING first are dispatched.
    /// A blocked task is never dispatched, whether its move applied or found
    /// the row moved on: that row belongs to its current owner. The moves are
    /// fenced to `worker_id` and each claim's generation, so one worker never
    /// ends another worker's claim, or a later claim of its own.
    ///
    /// Returns the blocked task ids, in input order. One statement per
    /// [`crate::broker::workflow_claim_settle::MAX_SETTLED_CLAIMS`] claims,
    /// each its own transaction, on the main core pool. No claim sends no
    /// statement.
    ///
    /// Errors: [`BrokerError::PoolSettings`] when a closed handle cannot open
    /// its pools again; [`BrokerError::Statement`];
    /// [`BrokerError::RowDecode`]; [`BrokerError::ClaimContract`] for rows
    /// that do not answer each claim once, in order. The statements before a
    /// failed one stay committed.
    pub async fn filter_non_runnable_workflow_tasks(
        &self,
        claims: &[(Uuid, DateTime<Utc>)],
        worker_id: &str,
    ) -> Result<Vec<Uuid>, BrokerError> {
        if claims.is_empty() {
            return Ok(Vec::new());
        }
        let (open, _write) = self.core_pools_for_write()?;
        let settled =
            settle_claimed_workflow_tasks(open.main(), self.statement_log, worker_id, claims)
                .await?;
        Ok(settled
            .into_iter()
            .filter_map(|(task_id, disposition)| match disposition {
                ClaimDisposition::Blocked => Some(task_id),
                ClaimDisposition::Dispatch => None,
            })
            .collect())
    }

    /// Close the broker's pools.
    ///
    /// - Core pools: no new operation takes them. Writes in flight run to
    ///   their end within [`crate::broker::pools::CLOSE_WRITE_BOUND`]; then
    ///   the pools close and the handle is `Idle`. The next operation opens
    ///   new core pools. A read in flight that checks out again fails with
    ///   `PoolClosed`.
    /// - The sqlx main pool closes as before and does not reopen; the call
    ///   sites that still borrow it through [`PostgresBroker::pool`] fail
    ///   after a close.
    ///
    /// Writes that did not end within the bound are logged with their count;
    /// their outcome is unknown.
    pub async fn close(&self) {
        match self.core.close().await {
            CloseOutcome::AlreadyIdle | CloseOutcome::Closed => {}
            CloseOutcome::ClosedWithWritesUnfinished { unfinished } => {
                tracing::warn!(
                    unfinished,
                    bound_ms = crate::broker::pools::CLOSE_WRITE_BOUND.as_millis() as u64,
                    "broker close: writes in flight did not end within the bound; their \
                     outcome is unknown",
                );
            }
        }
        self.pool.close().await;
    }

    // -----------------------------------------------------------------------
    // Monitoring / observability queries
    // -----------------------------------------------------------------------

    /// Verify runtime-pool connectivity with a cheap `SELECT 1` query.
    ///
    /// Returns `Ok(())` if the database is reachable, `Err(BrokerError)` otherwise.
    /// Mirrors Python's `HEALTH_CHECK_SQL` used by `horsies check --live`.
    ///
    /// This does not check LISTEN/NOTIFY. Workers run
    /// [`check_worker_session`](Self::check_worker_session) at every boot.
    pub async fn health_check(&self) -> Result<(), BrokerError> {
        let rows = self.query_main(&HEALTH_CHECK.bind().finish()).await?;
        let _row = one_row(rows, &HEALTH_CHECK)?;
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
        let probe = self
            .query_main(&HEALTH_CHECK.bind().finish())
            .await
            .and_then(|rows| one_row(rows, &HEALTH_CHECK).map_err(BrokerError::RowDecode));
        match probe {
            Ok(_row) => Ok(DatabasePing {
                latency_ms: start.elapsed().as_secs_f64() * 1000.0,
            }),
            Err(error) => Err(BrokerOperationError {
                code: BrokerErrorCode::DbPingFailed,
                message: format!("database ping failed: {error}"),
                retryable: error.is_retryable(),
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
    /// The replies arrive on a payload subscription of the reply channel on
    /// the broker's listener; the drop of the subscription UNLISTENs it. A
    /// broker with no listener is `LISTENER_DISABLED`, not retryable. A
    /// refused subscription is `WORKER_PING_FAILED` with the listener's
    /// retryable flag. Replies lost to a listener gap or a full reply queue
    /// are not waited for: the window goes on. The window starts when the
    /// ping NOTIFY has committed, as Python's. A reply that does not decode
    /// is logged and left out, as Python's (`_decode_pong`).
    pub async fn ping_workers(
        &self,
        target_worker_id: Option<&str>,
        timeout: std::time::Duration,
        min_responses: Option<usize>,
    ) -> BrokerResult<Vec<WorkerPong>> {
        if timeout.is_zero() {
            return Err(ping_timeout_refused(&0));
        }
        if min_responses == Some(0) {
            return Err(ping_min_responses_refused(&0));
        }

        let correlation_id = Uuid::new_v4().simple().to_string();
        let reply_channel = fixed_channel(&format!("{WORKER_PONG_CHANNEL_PREFIX}{correlation_id}"));

        let replies = self
            .listener
            .subscribe_payloads(reply_channel.clone(), default_payload_capacity())
            .await
            .map_err(|error| worker_ping_subscribe_error(&error))?;

        let request = WorkerPingRequest {
            correlation_id: correlation_id.clone(),
            reply_channel: reply_channel.as_str().to_owned(),
            target_worker_id: target_worker_id.map(str::to_owned),
        };
        let payload = serde_json::to_string(&request).map_err(|e| BrokerOperationError {
            code: BrokerErrorCode::WorkerPingFailed,
            message: format!("ping_workers payload encode failed: {}", e),
            retryable: false,
        })?;

        let _notified = self
            .execute_main(
                &NOTIFY_WORKER_PING
                    .bind()
                    .value(&WORKER_PING_CHANNEL)
                    .value(&payload)
                    .finish(),
            )
            .await
            .map_err(|error| worker_ping_statement_error("ping notify", error))?;
        // The window and each round trip start when the NOTIFY has committed
        // (Python's `sent_at`); a reply that arrived before waits in the
        // reply queue.
        let start = std::time::Instant::now();

        let mut pongs: Vec<WorkerPong> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        loop {
            let Some(remaining) = timeout.checked_sub(start.elapsed()) else {
                break;
            };
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, replies.recv()).await {
                Err(_elapsed) => break, // window closed
                // Replies sent while the listener reconnected, or dropped by
                // a full reply queue, are lost; the window goes on.
                Ok(PayloadEvent::Gap | PayloadEvent::Overflowed) => continue,
                Ok(PayloadEvent::Stopped) => {
                    return Err(BrokerOperationError {
                        code: BrokerErrorCode::WorkerPingFailed,
                        message: "ping_workers reply wait failed: the listener stopped".to_owned(),
                        retryable: true,
                    });
                }
                Ok(PayloadEvent::Payload(reply)) => {
                    let Some(pong) = decode_pong(&reply, &correlation_id, start.elapsed()) else {
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

    /// The session check of a worker boot, on the statement layer: both core
    /// pools keep a transaction, and the session URL keeps session state (a
    /// LISTEN on it receives a NOTIFY). Runs at every worker boot, with or
    /// without PgBouncer mode (Python `validate_worker_database_paths`,
    /// `python/horsies/core/brokers/postgres.py:1383-1412`).
    ///
    /// Steps:
    /// 1. `SELECT 1` on the session core pool when the session URL differs
    ///    from the main URL. Its first checkout runs the statement-pooling
    ///    check of that pool.
    /// 2. Subscribe a wake on `horsies_check_<32 hex>` on the broker's
    ///    listener, the connection that carries the claim wakes and the pings.
    /// 3. One autocommit `SELECT pg_notify($1, 'ok')` on the main core pool.
    ///    Its first checkout runs the statement-pooling check of the main
    ///    pool.
    /// 4. Wait at most `SESSION_CAPABILITY_TIMEOUT` (2 s) for the NOTIFY.
    ///
    /// Steps 2 to 4 are the LISTEN probe that the live check also runs
    /// ([`PostgresBroker::check_connectivity`]).
    ///
    /// The subscription is dropped at the end, and the listener UNLISTENs the
    /// channel. Nothing is written.
    ///
    /// Outcomes: `Ok` when the NOTIFY arrives. Not retryable: `NotCapable`
    /// (no NOTIFY in time), `PoolSettings`, and `SessionPool` or `Notify`
    /// with a permanent failure (a statement-pooled URL). `Subscribe`,
    /// `SessionPool`, `Notify` and `Interrupted` otherwise carry their retry
    /// rule (a database that cannot be reached is retryable).
    pub async fn check_worker_session(&self) -> Result<(), SessionCheckFailure> {
        let open = self
            .core
            .pools()
            .map_err(SessionCheckFailure::PoolSettings)?;
        self.check_session_pool(open.pools().session()).await?;
        self.probe_listen_delivery(&self.listener, &open)
            .await
            .map_err(SessionCheckFailure::from)
    }

    /// Step 1 of [`PostgresBroker::check_worker_session`]: `SELECT 1` on the
    /// session core pool when the session URL differs from the main URL;
    /// nothing when it does not (the main pool's check runs at the NOTIFY).
    async fn check_session_pool(&self, session: &SessionPool) -> Result<(), SessionCheckFailure> {
        match session {
            SessionPool::SameAsMain => Ok(()),
            SessionPool::Separate { pool, .. } => {
                self.log_statement(&SESSION_CHECK);
                let _selected = pool
                    .execute(
                        CheckoutClass::Batch,
                        &SESSION_CHECK.bind().finish(),
                        StatementBound::ConnectionTimeouts,
                    )
                    .await
                    .map_err(SessionCheckFailure::SessionPool)?;
                Ok(())
            }
        }
    }

    /// RUNNING tasks whose latest runner beat, or their start when they have
    /// no beat, is older than `stale_threshold_minutes`. Read-only: the
    /// reaper recovers stale tasks.
    ///
    /// A threshold outside 0 to one year is `INVALID_INPUT` with no database
    /// call. At most [`TASK_LIST_BOUND`] rows come back; more is
    /// [`BoundedTaskList::Truncated`]. A database failure is
    /// `MONITORING_QUERY_FAILED` with the driver classification.
    /// Mirrors `PostgresBroker.get_stale_tasks()` in the Python library.
    pub async fn get_stale_tasks(
        &self,
        stale_threshold_minutes: i32,
    ) -> BrokerResult<BoundedTaskList<StaleTaskRow>> {
        let threshold = StaleMinutes::parse(i64::from(stale_threshold_minutes))
            .map_err(|refused| refused.error())?;
        let minutes = threshold.minutes();
        let rows = self
            .read_main(
                &LIST_STALE_TASKS.bind().value(&minutes).finish(),
                stale_list_row,
            )
            .await
            .map_err(|error| monitoring_query_error("get_stale_tasks", error))?;
        bounded_list_from_rows(rows).map_err(|shape| {
            monitoring_query_error(
                "get_stale_tasks",
                list_shape_error(&LIST_STALE_TASKS, shape),
            )
        })
    }

    /// Latest state snapshot per worker (cluster-wide), including idle workers.
    ///
    /// Reads the `horsies_worker_states` timeseries, so every worker that has
    /// reported a snapshot appears regardless of current load — unlike the
    /// retired `get_worker_stats` (which counted RUNNING tasks only and missed
    /// idle workers). Mirrors Python's `list_worker_states_async`.
    ///
    /// A stored `slots_down`, `class_capacity` or map column of another shape
    /// fails the read: `MONITORING_QUERY_FAILED`, not retryable, naming the
    /// worker and the column (Python `postgres.py:2961-3034`).
    pub async fn list_worker_states(&self) -> BrokerResult<Vec<WorkerStateSnapshot>> {
        let listed = self.list_worker_states_with_read_time().await?;
        Ok(listed.into_iter().map(|state| state.snapshot).collect())
    }

    /// [`list_worker_states`](Self::list_worker_states), with the database
    /// clock of the list statement on each entry (`read_at`,
    /// `statement_timestamp()`). The web worker list measures the snapshot
    /// age, the staleness and the uptime from it. Same statement, same
    /// errors; reads only.
    pub(crate) async fn list_worker_states_with_read_time(
        &self,
    ) -> BrokerResult<Vec<ListedWorkerState>> {
        let rows = self
            .read_main(&LIST_WORKER_STATES.bind().finish(), listed_worker_state_row)
            .await
            .map_err(|error| monitoring_query_error("list_worker_states", error))?;
        read_listed_worker_states("list_worker_states", rows)
    }

    /// Latest state snapshot for one worker, or `None` if it has never reported.
    ///
    /// An unreadable stored row fails the read as in
    /// [`list_worker_states`](Self::list_worker_states). Mirrors Python's
    /// `get_worker_state_async`.
    pub async fn get_worker_state(
        &self,
        worker_id: &str,
    ) -> BrokerResult<Option<WorkerStateSnapshot>> {
        let rows = self
            .read_main(
                &GET_WORKER_STATE_LATEST.bind().value(&worker_id).finish(),
                stored_worker_state,
            )
            .await
            .map_err(|error| monitoring_query_error("get_worker_state", error))?;
        rows.into_iter()
            .next()
            .map(|stored| read_worker_snapshot("get_worker_state", stored))
            .transpose()
    }

    /// State-snapshot history for one worker, newest first.
    ///
    /// `limit` of `None` returns all retained rows; pass `Some(n)` to bound the
    /// fetch. A limit of 0 or less is `INVALID_INPUT`, with no database call.
    /// An unreadable stored row fails the read as in
    /// [`list_worker_states`](Self::list_worker_states). Mirrors Python's
    /// `get_worker_state_history_async`.
    pub async fn get_worker_state_history(
        &self,
        worker_id: &str,
        limit: Option<i64>,
    ) -> BrokerResult<Vec<WorkerStateSnapshot>> {
        let limit = checked_history_limit(limit)?;
        let rows = self
            .read_main(
                &GET_WORKER_STATE_HISTORY
                    .bind()
                    .value(&worker_id)
                    .value(&limit)
                    .finish(),
                stored_worker_state,
            )
            .await
            .map_err(|error| monitoring_query_error("get_worker_state_history", error))?;
        read_worker_snapshots("get_worker_state_history", rows)
    }

    /// Find PENDING tasks that have expired (past their `good_until` deadline).
    ///
    /// These tasks will never be picked up by workers because the claim SQL
    /// filters on `good_until > now()`. Surfacing them helps operators detect
    /// under-provisioned queues or misconfigured deadlines.
    ///
    /// At most [`TASK_LIST_BOUND`] rows come back, oldest deadline first;
    /// more is [`BoundedTaskList::Truncated`]. A database failure is
    /// `MONITORING_QUERY_FAILED` with the driver classification.
    /// Mirrors `PostgresBroker.get_expired_tasks()` in the Python library.
    pub async fn get_expired_tasks(&self) -> BrokerResult<BoundedTaskList<ExpiredTaskRow>> {
        let rows = self
            .read_main(&LIST_EXPIRED_TASKS.bind().finish(), expired_list_row)
            .await
            .map_err(|error| monitoring_query_error("get_expired_tasks", error))?;
        bounded_list_from_rows(rows).map_err(|shape| {
            monitoring_query_error(
                "get_expired_tasks",
                list_shape_error(&LIST_EXPIRED_TASKS, shape),
            )
        })
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// One statement through the staged result function. While that function
    /// is not published (an upgraded schema before its next coverage pass),
    /// falls back to the live read plus the staged detail read. A function
    /// dropped out of band after this broker saw it fails one call with the
    /// `42883` refusal that names it; the next call falls back (Python's
    /// rule, [`read_task_result`]).
    ///
    /// An absence from the result reader is confirmed by the detail reader: a
    /// result reader rendered before a leaf existed does not probe that leaf,
    /// and only the detail reader's absence is final.
    async fn fetch_result_row(&self, task_id: Uuid) -> Result<ResultRowProbe, BrokerError> {
        let open = self.core.pools()?;
        let read = read_task_result(
            open.main(),
            self.statement_log,
            &self.result_reader,
            task_id,
        )
        .await?;
        match read {
            TaskResultRead::Live(live) => Ok(ResultRowProbe::Row(TaskResultRow {
                id: task_id,
                status: live.status,
                result: None,
                failed_reason: None,
            })),
            TaskResultRead::History(history) => Ok(ResultRowProbe::Row(history_task_result_row(
                task_id, history,
            )?)),
            TaskResultRead::Absent => self.confirm_result_absence(task_id).await,
            TaskResultRead::NotPublished => self.fetch_result_row_unstaged(task_id).await,
        }
    }

    /// The detail reader's answer for a task id the result reader did not
    /// find: absent, live again, or the history row's result.
    async fn confirm_result_absence(&self, task_id: Uuid) -> Result<ResultRowProbe, BrokerError> {
        match self.fetch_history_detail(task_id).await? {
            TaskDetailResult::Absent { .. } => Ok(ResultRowProbe::Absent),
            TaskDetailResult::Live { .. } => Ok(ResultRowProbe::Live),
            TaskDetailResult::History(detail) => {
                Ok(ResultRowProbe::Row(history_result_row(detail)?))
            }
        }
    }

    async fn fetch_result_row_unstaged(
        &self,
        task_id: Uuid,
    ) -> Result<ResultRowProbe, BrokerError> {
        let live = self
            .read_main(&GET_RESULT.bind().value(&task_id).finish(), task_result_row)
            .await?;
        if let Some(row) = live.into_iter().next() {
            return Ok(ResultRowProbe::Row(row));
        }
        self.confirm_result_absence(task_id).await
    }

    /// One probe of the raw result wait. A live row holds no result, so the
    /// live read takes the id, name and status only, as Python's
    /// `_probe_live_result_row` (`python/horsies/core/brokers/postgres.py:2156-2191`):
    /// a terminal status is a record with no result, a live status waits, and
    /// a row that is not live is read from history.
    async fn raw_result_probe(&self, task_id: Uuid) -> BrokerResult<RawResultProbe> {
        let live = self
            .read_main(
                &GET_RAW_RESULT_PROBE.bind().value(&task_id).finish(),
                raw_result_probe_row,
            )
            .await
            .map_err(raw_result_operation_error)?;
        let Some(row) = live.into_iter().next() else {
            return self.history_raw_result_probe(task_id).await;
        };
        let status: crate::core::TaskStatus = row
            .status
            .parse()
            .map_err(|error| raw_result_operation_error(BrokerError::InvalidStatus(error)))?;
        let record = RawResultRecord {
            task_id: row.id,
            task_name: row.task_name,
            status,
            raw_result: None,
        };
        Ok(match status {
            crate::core::TaskStatus::Completed
            | crate::core::TaskStatus::Failed
            | crate::core::TaskStatus::Cancelled
            | crate::core::TaskStatus::Expired => RawResultProbe::Record(record),
            crate::core::TaskStatus::Pending
            | crate::core::TaskStatus::Claimed
            | crate::core::TaskStatus::Running => RawResultProbe::Waiting(record),
        })
    }

    async fn history_raw_result_probe(&self, task_id: Uuid) -> BrokerResult<RawResultProbe> {
        let detail = self
            .fetch_history_detail(task_id)
            .await
            .map_err(raw_result_operation_error)?;
        raw_result_from_history_detail(task_id, detail)
    }

    async fn fetch_history_detail(&self, task_id: Uuid) -> Result<TaskDetailResult, BrokerError> {
        let open = self.core.pools()?;
        read_history_detail(open.main(), self.statement_log, task_id).await
    }

    /// Poll the result row once for the wait loop.
    ///
    /// Returns `Some(outcome)` when the wait should end: the parsed terminal
    /// result, or a typed `TaskNotFound` outcome for a row that does not exist
    /// (pruned by retention, or never present) so the caller never blocks
    /// forever (C3). Returns `None` when the task exists but is not yet terminal.
    async fn poll_result<T: DeserializeOwned>(
        &self,
        task_id: Uuid,
    ) -> Result<Option<TaskResult<T>>, BrokerError> {
        match self.fetch_result_row(task_id).await? {
            ResultRowProbe::Row(row) if is_terminal_status(&row.status) => {
                parse_task_result_row(row).map(Some)
            }
            ResultRowProbe::Row(_) | ResultRowProbe::Live => Ok(None),
            ResultRowProbe::Absent => Ok(Some(TaskResult::Err(TaskError::builtin(
                RetrievalCode::TaskNotFound,
                format!("task {} not found", task_id),
            )))),
        }
    }

    async fn final_poll_or_timeout<T: DeserializeOwned>(
        &self,
        task_id: Uuid,
        elapsed_ms: u64,
    ) -> Result<TaskResult<T>, BrokerError> {
        match self.fetch_result_row(task_id).await? {
            ResultRowProbe::Row(row) if is_terminal_status(&row.status) => {
                parse_task_result_row(row)
            }
            ResultRowProbe::Row(_) | ResultRowProbe::Live => {
                Ok(TaskResult::Err(TaskError::builtin(
                    RetrievalCode::WaitTimeout,
                    format!("task {} not terminal after {}ms", task_id, elapsed_ms),
                )))
            }
            ResultRowProbe::Absent => Ok(TaskResult::Err(TaskError::builtin(
                RetrievalCode::TaskNotFound,
                format!("task {} not found", task_id),
            ))),
        }
    }
}

/// The keyed-send reservation window in microseconds, the unit of the
/// `interval` that the key claim stores. `None` selects the 24-hour default.
///
/// Errors: [`BrokerError::InvalidIdempotencyReservationWindow`] for a window
/// that is not positive, is over 30 days, or is not a whole number of
/// microseconds.
fn idempotency_reservation_window_micros(
    window: Option<chrono::Duration>,
) -> Result<i64, BrokerError> {
    let window =
        window.unwrap_or_else(|| chrono::Duration::hours(IDEMPOTENCY_WINDOW_DEFAULT_HOURS));
    reservation_window_micros(window)
        .map_err(|error| BrokerError::InvalidIdempotencyReservationWindow(error.to_string()))
}

/// The rerun error of a client-family check that did not pass.
fn rerun_family_error(error: FamilyCheckError) -> RerunError {
    match error {
        FamilyCheckError::Mismatch(mismatch) => RerunError::ClientFamilyMismatch {
            stored: mismatch.stored,
            own: mismatch.own,
        },
        FamilyCheckError::ReadFailed(failure) => RerunError::FamilyCheckFailed(failure),
    }
}

#[cfg(test)]
mod rerun_family_error_tests {
    use super::rerun_family_error;
    use crate::broker::client_family::{
        ClientFamily, FamilyCheckError, FamilyCheckFailure, FamilyMismatch, RecordedFamily,
    };
    use crate::core::history::rerun::RerunError;
    use crate::db::{NotSentCause, StatementFailure, Transience};

    #[test]
    fn a_family_mismatch_is_the_rerun_mismatch() {
        let error = rerun_family_error(FamilyCheckError::Mismatch(FamilyMismatch {
            stored: RecordedFamily::NotRecorded,
            own: ClientFamily::Rust,
        }));
        assert!(matches!(
            error,
            RerunError::ClientFamilyMismatch {
                stored: RecordedFamily::NotRecorded,
                own: ClientFamily::Rust,
            }
        ));
    }

    /// A failed family read is a rerun error, never a panic, and keeps the
    /// retry class of its statement.
    #[test]
    fn a_failed_family_read_is_a_rerun_error_with_its_retry_class() {
        let error = rerun_family_error(FamilyCheckError::ReadFailed(
            FamilyCheckFailure::Statement(StatementFailure::NotSent {
                statement: "read_client_family",
                cause: NotSentCause::ConnectionClosed,
            }),
        ));
        match error {
            RerunError::FamilyCheckFailed(failure) => {
                assert!(matches!(failure, FamilyCheckFailure::Statement(_)));
                assert_eq!(failure.transience(), Transience::Transient);
            }
            other => panic!("expected FamilyCheckFailed, got {other:?}"),
        }
    }
}

/// The task info code of a read error, by Python's rule
/// (`python/horsies/core/brokers/postgres.py:4889-4904`, `:4973-4977`): only a
/// stored result that fails its check is `INVALID_JSON_PAYLOAD`; a status or
/// an attempt snapshot that does not decode, and a database failure, are
/// `TASK_INFO_QUERY_FAILED`, retryable by the driver classification. A
/// history result is checked in [`history_task_info`]; on the live path the
/// only `Serialization` error is the stored result's.
fn task_info_history_error(error: BrokerError) -> BrokerOperationError {
    let code = match &error {
        BrokerError::Serialization(_) => BrokerErrorCode::InvalidJsonPayload,
        _ => BrokerErrorCode::TaskInfoQueryFailed,
    };
    BrokerOperationError {
        code,
        message: error.to_string(),
        retryable: error.is_retryable(),
    }
}

enum ResultRowProbe {
    Row(TaskResultRow),
    Live,
    Absent,
}

#[derive(Debug)]
struct RawResultProbeRow {
    id: Uuid,
    task_name: String,
    status: String,
}

fn raw_result_probe_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<RawResultProbeRow, RowDecodeError> {
    Ok(RawResultProbeRow {
        id: column(row, statement, "id")?,
        task_name: column(row, statement, "task_name")?,
        status: column(row, statement, "status")?,
    })
}

enum RawResultProbe {
    Record(RawResultRecord),
    Waiting(RawResultRecord),
    Retry,
    Absent(TaskAbsenceReason),
}

/// The passes of a task info read whose live read and history read race a
/// send of the same id.
const TASK_INFO_READ_PASSES: u32 = 3;

/// The end of one task info pass.
enum TaskInfoPass {
    Read(TaskInfoRead),
    /// The live read found no row, and the history read found the task
    /// live: a send of the id committed between the two statements.
    BecameLive,
}

fn raw_result_from_history_detail(
    task_id: Uuid,
    detail: TaskDetailResult,
) -> BrokerResult<RawResultProbe> {
    match detail {
        TaskDetailResult::Absent { reason, .. } => Ok(RawResultProbe::Absent(reason)),
        TaskDetailResult::Live { .. } => Ok(RawResultProbe::Retry),
        TaskDetailResult::History(detail) => {
            // Parsed at the history read (`HistoryTaskDetail.status`).
            let status: crate::core::TaskStatus = detail.status;
            if !status.is_terminal() {
                return Err(raw_result_operation_error(BrokerError::InvalidStatus(
                    format!("history task {task_id} has non-terminal status {status}"),
                )));
            }
            let raw_result = decode_history_result_value(&detail)
                .map_err(raw_result_operation_error)?
                .map(decode_raw_result_value)
                .transpose()?
                .flatten();
            Ok(RawResultProbe::Record(RawResultRecord {
                task_id: detail.task_id,
                task_name: detail.task_name,
                status,
                raw_result,
            }))
        }
    }
}

fn decode_raw_result_value(
    value: serde_json::Value,
) -> BrokerResult<Option<serde_json::Map<String, serde_json::Value>>> {
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Object(object) => Ok(Some(object)),
        other => Err(invalid_raw_result(format!(
            "result JSON must be an object, got {}",
            json_value_kind(&other),
        ))),
    }
}

fn json_value_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn invalid_raw_result(message: String) -> BrokerOperationError {
    BrokerOperationError {
        code: BrokerErrorCode::InvalidJsonPayload,
        message,
        retryable: false,
    }
}

fn raw_result_operation_error(error: BrokerError) -> BrokerOperationError {
    let code = match &error {
        BrokerError::Serialization(_)
        | BrokerError::HistoryReadContract(_)
        | BrokerError::InvalidStatus(_) => BrokerErrorCode::InvalidJsonPayload,
        _ => BrokerErrorCode::TaskInfoQueryFailed,
    };
    BrokerOperationError {
        code,
        message: error.to_string(),
        retryable: error.is_retryable(),
    }
}

fn decode_history_result_value(
    detail: &HistoryTaskDetail,
) -> Result<Option<serde_json::Value>, BrokerError> {
    let Some(payload) = detail.result_payload.as_deref() else {
        return Ok(None);
    };
    let digest = detail.result_digest.as_deref().ok_or_else(|| {
        BrokerError::HistoryReadContract(format!(
            "history result digest is absent for task {}",
            detail.task_id
        ))
    })?;
    decode_result_envelope(
        detail.result_envelope_version,
        &detail.result_codec,
        &detail.result_content_type,
        payload,
        digest,
    )
    .map(Some)
    .map_err(|error| BrokerError::HistoryReadContract(error.to_string()))
}

fn history_result_row(detail: HistoryTaskDetail) -> Result<TaskResultRow, BrokerError> {
    let result = decode_history_result_value(&detail)?
        .map(|value| serde_json::to_string(&value))
        .transpose()?;
    Ok(TaskResultRow {
        id: detail.task_id,
        status: detail.status.to_string(),
        result,
        failed_reason: detail.final_failed_reason,
    })
}

fn history_task_result_row(
    task_id: Uuid,
    history: HistoryTaskResult,
) -> Result<TaskResultRow, BrokerError> {
    let result = match &history.payload {
        HistoryResultPayload::Absent => None,
        HistoryResultPayload::Stored { payload, digest } => {
            let value = decode_result_envelope(
                history.envelope_version,
                &history.codec,
                &history.content_type,
                payload,
                digest,
            )
            .map_err(|error| BrokerError::HistoryReadContract(error.to_string()))?;
            Some(serde_json::to_string(&value)?)
        }
    };
    Ok(TaskResultRow {
        id: task_id,
        status: history.status,
        result,
        failed_reason: history.failed_reason,
    })
}

/// The task info of a history row. A stored result that fails its digest or
/// does not decode is `INVALID_JSON_PAYLOAD`; a status or an attempt snapshot
/// that does not decode is `TASK_INFO_QUERY_FAILED`, not retryable (Python
/// `python/horsies/core/brokers/postgres.py:4889-4904`, `:4973-4977`).
fn history_task_info(
    detail: HistoryTaskDetail,
    include_result: bool,
    include_failed_reason: bool,
    include_attempts: bool,
) -> BrokerResult<TaskInfo> {
    let status = detail.status;
    let result = match include_result {
        false => None,
        true => decode_history_result_value(&detail)
            .and_then(|value| {
                value
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(BrokerError::Serialization)
            })
            .map_err(|error| {
                invalid_raw_result(format!(
                    "History result envelope failed validation for task {}: {error}",
                    detail.task_id
                ))
            })?,
    };
    let terminal_at = detail.terminal_at;
    // The snapshot holds the attempts oldest first; a task info lists them
    // newest first, as the live read (`ORDER BY attempt DESC`) and Python's
    // history read (`reversed(detail.attempts)`).
    let attempts = include_attempts
        .then(|| {
            detail
                .attempts
                .iter()
                .rev()
                .map(|record| task_attempt_info_from_record(detail.task_id, record))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()
        .map_err(task_info_history_error)?;
    Ok(TaskInfo {
        task_id: detail.task_id,
        task_name: detail.task_name,
        status,
        queue_name: detail.queue_name,
        priority: detail.priority,
        retry_count: detail.retry_count as u32,
        max_retries: detail.max_retries as u32,
        next_retry_at: None,
        sent_at: detail.sent_at,
        enqueued_at: detail.enqueued_at,
        claimed_at: detail.claimed_at,
        started_at: detail.started_at,
        completed_at: (status == crate::core::TaskStatus::Completed).then_some(terminal_at),
        failed_at: (status != crate::core::TaskStatus::Completed).then_some(terminal_at),
        worker_hostname: detail.last_worker_hostname,
        worker_pid: detail
            .last_worker_pid
            .and_then(|pid| u32::try_from(pid).ok()),
        worker_process_name: None,
        error_code: detail.error_code,
        failed_reason: include_failed_reason
            .then_some(detail.final_failed_reason)
            .flatten(),
        result,
        attempts,
        // The history table has no `execution` column (Python reports `None`).
        execution: crate::core::ExecutionClass::NotKeptInHistory,
    })
}

fn task_attempt_info_from_row(
    row: TaskAttemptRow,
) -> Result<crate::core::TaskAttemptInfo, BrokerError> {
    Ok(crate::core::TaskAttemptInfo {
        task_id: row.task_id,
        attempt: row.attempt,
        outcome: parse_task_attempt_outcome(&row.outcome)?,
        will_retry: row.will_retry,
        started_at: row.started_at,
        finished_at: row.finished_at,
        error_code: row.error_code,
        error_message: row.error_message,
        failed_reason: row.failed_reason,
        worker_id: row.worker_id,
        worker_hostname: row.worker_hostname,
        worker_pid: row.worker_pid,
        worker_process_name: row.worker_process_name,
    })
}

fn task_attempt_info_from_record(
    task_id: Uuid,
    record: &crate::core::history::archive::attempts::AttemptRecord,
) -> Result<crate::core::TaskAttemptInfo, BrokerError> {
    Ok(crate::core::TaskAttemptInfo {
        task_id,
        attempt: record.attempt(),
        outcome: parse_task_attempt_outcome(record.outcome())?,
        will_retry: record.will_retry(),
        started_at: record.started_at(),
        finished_at: record.finished_at(),
        error_code: record.error_code().map(str::to_owned),
        error_message: record.error_message().map(str::to_owned),
        failed_reason: record.failed_reason().map(str::to_owned),
        worker_id: record.worker_id().map(str::to_owned),
        worker_hostname: record.worker_hostname().map(str::to_owned),
        worker_pid: record.worker_pid(),
        worker_process_name: record.worker_process_name().map(str::to_owned),
    })
}

fn parse_task_attempt_outcome(value: &str) -> Result<crate::core::TaskAttemptOutcome, BrokerError> {
    match value {
        "COMPLETED" => Ok(crate::core::TaskAttemptOutcome::Completed),
        "FAILED" => Ok(crate::core::TaskAttemptOutcome::Failed),
        "WORKER_FAILURE" => Ok(crate::core::TaskAttemptOutcome::WorkerFailure),
        other => Err(BrokerError::HistoryReadContract(format!(
            "unknown task-attempt outcome {other:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

fn is_terminal_status(status: &str) -> bool {
    matches!(status, "COMPLETED" | "FAILED" | "CANCELLED" | "EXPIRED")
}

/// A list function's rows that break its contract, as a row decode error.
fn list_shape_error(statement: &TypedStatement, shape: ListShapeError) -> BrokerError {
    BrokerError::RowDecode(RowDecodeError::Shape {
        statement: statement.name(),
        reason: shape.to_string(),
    })
}

/// Build a `MONITORING_QUERY_FAILED` broker error for a failed read query.
/// `retryable` is the classification of the cause, as Python's
/// `_operation_failed`.
fn monitoring_query_error(operation: &str, error: BrokerError) -> BrokerOperationError {
    BrokerOperationError {
        code: BrokerErrorCode::MonitoringQueryFailed,
        message: format!("{operation} failed: {error}"),
        retryable: error.is_retryable(),
    }
}

/// The error of a refused reply subscription of `ping_workers`, as Python
/// (`python/horsies/core/brokers/postgres.py:2821-2865`): a broker with no listener
/// is `LISTENER_DISABLED`, not retryable; any other refusal is
/// `WORKER_PING_FAILED` with the listener's retryable flag.
fn worker_ping_subscribe_error(error: &ListenError) -> BrokerOperationError {
    match error {
        ListenError::NoSessionUrl => BrokerOperationError {
            code: BrokerErrorCode::ListenerDisabled,
            message: format!("ping_workers needs a listener: {error}"),
            retryable: false,
        },
        ListenError::PlanRefused(_) | ListenError::Subscribe(_) => BrokerOperationError {
            code: BrokerErrorCode::WorkerPingFailed,
            message: format!("ping_workers reply subscribe failed: {error}"),
            retryable: error.is_retryable(),
        },
    }
}

/// Build a `WORKER_PING_FAILED` broker error for a failed core statement of
/// a ping. `retryable` is the classification of the cause.
fn worker_ping_statement_error(operation: &str, error: BrokerError) -> BrokerOperationError {
    BrokerOperationError {
        code: BrokerErrorCode::WorkerPingFailed,
        message: format!("ping_workers {operation} failed: {error}"),
        retryable: error.is_retryable(),
    }
}

/// Build an `INVALID_INPUT` broker error: an argument was refused before
/// anything ran, and the same argument is refused again.
fn invalid_input(message: String) -> BrokerOperationError {
    BrokerOperationError {
        code: BrokerErrorCode::InvalidInput,
        message,
        retryable: false,
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
    // Python's `_decode_pong`: text that is not JSON and an object that is
    // not a pong are logged; a value that is not an object and a reply of
    // another round are left out without a record.
    let body: serde_json::Value = match serde_json::from_str(raw_payload) {
        Ok(body) => body,
        Err(error) => {
            tracing::warn!("Discarding unparseable pong payload: {error}");
            return None;
        }
    };
    if !body.is_object() {
        return None;
    }
    let payload: WorkerPongPayload = match serde_json::from_value(body) {
        Ok(payload) => payload,
        Err(error) => {
            tracing::warn!("Discarding invalid pong payload: {error}");
            return None;
        }
    };
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
/// Expects `result` to store a serialized `TaskResult<T>` for COMPLETED,
/// FAILED and EXPIRED. Maps database statuses to `TaskResult` variants
/// (Python `TaskHandle._record_to_task_result`):
/// - `COMPLETED`/`FAILED`/`EXPIRED` + valid JSON -> decoded `TaskResult<T>`
/// - `COMPLETED`/`FAILED`/`EXPIRED` + null -> `TaskResult::Err(ResultNotAvailable)`
///   with `data = {"task_id": "<id>", "status": "<status>"}`
/// - `CANCELLED` -> `TaskResult::Err(TaskError(TaskCancelled))` with
///   `data = {"task_id": "<id>"}`
#[allow(clippy::needless_pass_by_value)]
fn parse_task_result_row<T: DeserializeOwned>(
    row: TaskResultRow,
) -> Result<TaskResult<T>, BrokerError> {
    match row.status.as_str() {
        // Expiry writers persist a serialized `TaskResult::Err(TASK_EXPIRED)`,
        // so an EXPIRED row decodes like COMPLETED and FAILED.
        "COMPLETED" | "FAILED" | "EXPIRED" => match row.result {
            Some(ref result_json) => {
                let task_result: TaskResult<T> =
                    serde_json::from_str(result_json).map_err(BrokerError::Serialization)?;
                Ok(task_result)
            }
            // Python `TaskHandle._record_to_task_result`: a terminal row
            // that stored no result is RESULT_NOT_AVAILABLE, with the id and
            // the status in `data`.
            None => Ok(TaskResult::Err(TaskError {
                data: Some(serde_json::json!({
                    "task_id": row.id.to_string(),
                    "status": row.status,
                })),
                ..TaskError::builtin(
                    RetrievalCode::ResultNotAvailable,
                    format!(
                        "Task {} reached terminal status {} but stored no result payload",
                        row.id, row.status
                    ),
                )
            })),
        },
        // Python `TaskHandle._record_to_task_result`: the code, the message
        // and the task id in `data`.
        "CANCELLED" => Ok(TaskResult::Err(TaskError {
            data: Some(serde_json::json!({ "task_id": row.id.to_string() })),
            ..TaskError::builtin(
                OutcomeCode::TaskCancelled,
                format!("Task {} was cancelled before completion", row.id),
            )
        })),
        other => Err(BrokerError::InvalidStatus(format!(
            "task {} has non-terminal status {}",
            row.id, other,
        ))),
    }
}

use crate::worker::retry::stored_max_retries;

/// The `max_retries` that enqueue stores, by Python's rule
/// (`python/horsies/core/codec/task_options.py:80-103`, `stored_max_retries`): no
/// options and no `retry_policy` store 0; a policy stores its `max_retries`.
/// The options are read with the reader of the dispatch
/// ([`stored_max_retries`]), so a text that the worker would refuse is refused
/// here, before any statement: text that does not parse, that is not a JSON
/// object, or whose `retry_policy` does not fit Python's `RetryPolicy` (a
/// limit outside 1 to 20, a wrong field, no code) is `EnqueueContract`, which
/// a send reports as `VALIDATION_FAILED`. Other keys are not judged here.
fn enqueue_max_retries(task_name: &str, task_options: Option<&str>) -> Result<i32, BrokerError> {
    stored_max_retries(task_options).map_err(|refusal| {
        BrokerError::EnqueueContract(EnqueueRefusal::by_rule(
            SendPart::TaskOptions,
            format!("task_options refused for {task_name}: {refusal}"),
        ))
    })
}

// ---------------------------------------------------------------------------
// Result wait
// ---------------------------------------------------------------------------

/// Re-poll interval of the workflow result wait (`get_workflow_result`),
/// bounding each `subscription.recv()` so a NOTIFY lost during a listener
/// reconnect is recovered by a fresh terminal-status poll instead of hanging
/// the caller forever.
pub(crate) const RESULT_WAIT_REPOLL: Duration = Duration::from_secs(30);

/// Repoll interval of a task result wait whose listener works: a lost
/// notification costs at most this long (Python's 5 s,
/// `python/horsies/core/brokers/postgres.py:3285`).
pub(crate) const TASK_RESULT_REPOLL_SUBSCRIBED: Duration = Duration::from_secs(5);

/// Repoll interval of a task result wait with no listener (Python's 0.2 s,
/// `python/horsies/core/brokers/postgres.py:3285`).
pub(crate) const TASK_RESULT_REPOLL_POLLING: Duration = Duration::from_millis(200);

/// How a task result wait learns that the task may have ended.
enum ResultWaitMode {
    /// A wake for the task id on `task_done`. The listener also sends a gap to
    /// every subscription after it reconnects, because notifications sent
    /// while it was down are lost; the wait then reads at once.
    Subscribed(WakeSubscription),
    /// No listener: the wait reads again at [`TASK_RESULT_REPOLL_POLLING`].
    Polling,
}

impl ResultWaitMode {
    /// The longest pause between two reads in this mode.
    fn repoll_interval(&self) -> Duration {
        match self {
            Self::Subscribed(_) => TASK_RESULT_REPOLL_SUBSCRIBED,
            Self::Polling => TASK_RESULT_REPOLL_POLLING,
        }
    }

    /// Pause until a wake for the task, a listener gap, or until `wait`
    /// passes. A listener that has stopped turns the wait into polling, so a
    /// stopped subscription never makes the loop spin.
    async fn pause(&mut self, wait: Duration) {
        match self {
            Self::Subscribed(subscription) => {
                match tokio::time::timeout(wait, subscription.recv()).await {
                    Ok(WakeEvent::Notified | WakeEvent::Gap) | Err(_) => {}
                    Ok(WakeEvent::Stopped) => {
                        tracing::warn!("the listener stopped; the result wait polls instead");
                        *self = Self::Polling;
                    }
                }
            }
            Self::Polling => tokio::time::sleep(wait).await,
        }
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_broker_without_a_listener_waits_for_a_result_by_polling() {
        // No server listens on port 1; the session URL would give a
        // listener, and `without_listener` takes it away.
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies")
            .expect("a lazy pool opens no connection");
        let broker = PostgresBroker::from_pool_and_session_url(
            pool,
            "postgresql://horsies@127.0.0.1:1/horsies",
        )
        .without_listener();
        let mode = broker.result_wait_mode(Uuid::from_u128(7)).await;
        assert!(matches!(mode, ResultWaitMode::Polling));
        assert_eq!(mode.repoll_interval(), TASK_RESULT_REPOLL_POLLING);
    }

    #[test]
    fn idempotency_reservation_window_defaults_and_bounds_are_exact() {
        assert_eq!(
            idempotency_reservation_window_micros(None).unwrap(),
            86_400_000_000
        );
        assert_eq!(
            idempotency_reservation_window_micros(Some(chrono::Duration::days(30))).unwrap(),
            2_592_000_000_000,
        );
        assert_eq!(
            idempotency_reservation_window_micros(Some(chrono::Duration::milliseconds(1_500)))
                .unwrap(),
            1_500_000,
        );
        assert_eq!(
            idempotency_reservation_window_micros(Some(chrono::Duration::milliseconds(500)))
                .unwrap(),
            500_000,
        );
        for invalid in [
            chrono::Duration::zero(),
            chrono::Duration::seconds(-1),
            chrono::Duration::nanoseconds(1_500),
        ] {
            assert!(matches!(
                idempotency_reservation_window_micros(Some(invalid)),
                Err(BrokerError::InvalidIdempotencyReservationWindow(_)),
            ));
        }
        assert!(matches!(
            idempotency_reservation_window_micros(Some(
                chrono::Duration::days(30) + chrono::Duration::microseconds(1),
            )),
            Err(BrokerError::InvalidIdempotencyReservationWindow(_)),
        ));
    }

    #[test]
    fn terminal_status_check() {
        assert!(is_terminal_status("COMPLETED"));
        assert!(is_terminal_status("FAILED"));
        assert!(is_terminal_status("CANCELLED"));
        assert!(!is_terminal_status("PENDING"));
        assert!(!is_terminal_status("CLAIMED"));
        assert!(!is_terminal_status("RUNNING"));
    }

    /// The list rules (equal deadline is expired; the stale verdict) live in
    /// the baseline functions; the broker calls them.
    #[test]
    fn the_task_lists_call_the_baseline_list_functions() {
        assert!(LIST_EXPIRED_TASKS
            .sql()
            .contains("FROM horsies_list_expired_tasks()"));
        assert!(LIST_STALE_TASKS
            .sql()
            .contains("FROM horsies_list_stale_tasks($1)"));
    }

    #[test]
    fn parse_completed_result() {
        let wrapped = TaskResult::Ok(42);
        let result_json = serde_json::to_string(&wrapped).unwrap();
        let row = TaskResultRow {
            id: Uuid::nil(),
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
            id: Uuid::nil(),
            status: "COMPLETED".to_owned(),
            result: None,
            failed_reason: None,
        };
        let task_result: TaskResult<i32> = parse_task_result_row(row).unwrap();
        assert!(task_result.is_err());
        let err = task_result.unwrap_err();
        assert!(err.message.unwrap().contains("stored no result payload"));
    }

    #[test]
    fn parse_failed_with_task_error() {
        let err = TaskError::new("VALIDATION", "bad input").unwrap();
        let wrapped: TaskResult<i32> = TaskResult::Err(err);
        let err_json = serde_json::to_string(&wrapped).unwrap();
        let row = TaskResultRow {
            id: Uuid::nil(),
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
            id: Uuid::nil(),
            status: "FAILED".to_owned(),
            result: None,
            failed_reason: Some("something broke".to_owned()),
        };
        let task_result: TaskResult<i32> = parse_task_result_row(row).unwrap();
        assert!(task_result.is_err());
        let err = task_result.unwrap_err();
        assert!(err.message.unwrap().contains("stored no result payload"));
    }

    #[test]
    fn parse_cancelled() {
        let row = TaskResultRow {
            id: Uuid::nil(),
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
            id: Uuid::nil(),
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
        let wrapped: TaskResult<i32> = TaskResult::Err(TaskError::builtin(
            OutcomeCode::TaskExpired,
            "deadline passed",
        ));
        let result_json = serde_json::to_string(&wrapped).unwrap();
        let row = TaskResultRow {
            id: Uuid::nil(),
            status: "EXPIRED".to_owned(),
            result: Some(result_json),
            failed_reason: None,
        };
        let task_result: TaskResult<i32> = parse_task_result_row(row).unwrap();
        let err = task_result.unwrap_err();
        let expected = TaskError::builtin(OutcomeCode::TaskExpired, "").error_code;
        assert_eq!(err.error_code, expected);
    }

    /// A terminal row that stored no result reads as `RESULT_NOT_AVAILABLE`
    /// with Python's message and the id and status in `data`, for each of
    /// COMPLETED, FAILED and EXPIRED (Python
    /// `TaskHandle._record_to_task_result`; `test_app_get_result.py::
    /// TestHorsiesGetResultEnvelopeGuards::
    /// test_terminal_status_with_none_payload_is_result_not_available`). An
    /// EXPIRED row with no stored result does not read as `TASK_EXPIRED`,
    /// and it is no `InvalidStatus` broker error.
    #[test]
    fn a_terminal_row_without_a_result_reads_as_result_not_available() {
        let task_id = Uuid::new_v4();
        for status in ["COMPLETED", "FAILED", "EXPIRED"] {
            let row = TaskResultRow {
                id: task_id,
                status: status.to_owned(),
                result: None,
                failed_reason: None,
            };

            let task_result: TaskResult<serde_json::Value> =
                parse_task_result_row(row).expect("a terminal row reads");

            let TaskResult::Err(error) = task_result else {
                panic!("{status}: a row with no result must read as an error");
            };
            assert_eq!(
                error.error_code,
                TaskError::builtin(RetrievalCode::ResultNotAvailable, "").error_code,
                "{status}"
            );
            assert_eq!(
                error.message.as_deref(),
                Some(
                    format!(
                        "Task {task_id} reached terminal status {status} but stored no result payload"
                    )
                    .as_str()
                ),
                "{status}"
            );
            assert_eq!(
                error.data,
                Some(serde_json::json!({ "task_id": task_id.to_string(), "status": status })),
                "{status}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Additional parse_task_result_row tests
    // -----------------------------------------------------------------------

    /// PENDING is not terminal; parse_task_result_row should return InvalidStatus.
    #[test]
    fn parse_pending_status_is_error() {
        let row = TaskResultRow {
            id: Uuid::nil(),
            status: "PENDING".to_owned(),
            result: None,
            failed_reason: None,
        };
        let result: Result<TaskResult<i32>, BrokerError> = parse_task_result_row(row);
        match result {
            Err(BrokerError::InvalidStatus(msg)) => {
                assert!(
                    msg.contains(&Uuid::nil().to_string()),
                    "expected task id in message: {}",
                    msg
                );
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
            id: Uuid::nil(),
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
            id: Uuid::nil(),
            status: "CANCELLED".to_owned(),
            result: None,
            failed_reason: None,
        };
        let task_result: TaskResult<i32> = parse_task_result_row(row).unwrap();
        assert!(task_result.is_err());
        let err = task_result.unwrap_err();
        assert!(err
            .message
            .as_deref()
            .unwrap()
            .contains(&Uuid::nil().to_string()));
    }

    /// Cancelled with leftover result data should still return TaskCancelled
    /// (result column is ignored for CANCELLED status).
    #[test]
    fn parse_cancelled_ignores_result_column() {
        let row = TaskResultRow {
            id: Uuid::nil(),
            status: "CANCELLED".to_owned(),
            result: Some(r#"{"__h_task_result__":true,"ok":42,"err":null}"#.to_owned()),
            failed_reason: None,
        };
        let task_result: TaskResult<i32> = parse_task_result_row(row).unwrap();
        assert!(task_result.is_err());
        let err = task_result.unwrap_err();
        assert!(err
            .message
            .as_deref()
            .unwrap()
            .contains(&Uuid::nil().to_string()));
    }

    /// A CANCELLED task reads as `TASK_CANCELLED` with Python's message and
    /// its task id in `data` (Python `TaskHandle._record_to_task_result`;
    /// `tests/e2e/test_layer1_tasks.py::test_task_cancelled`:
    /// `result.err.data['task_id'] == task_id`).
    #[test]
    fn parse_cancelled_carries_the_task_id_in_data() {
        let task_id = Uuid::new_v4();
        let row = TaskResultRow {
            id: task_id,
            status: "CANCELLED".to_owned(),
            result: None,
            failed_reason: None,
        };

        let task_result: TaskResult<serde_json::Value> =
            parse_task_result_row(row).expect("a CANCELLED row reads");

        let TaskResult::Err(error) = task_result else {
            panic!("a CANCELLED task must read as an error");
        };
        assert_eq!(
            error.error_code,
            TaskError::builtin(OutcomeCode::TaskCancelled, "").error_code
        );
        assert_eq!(
            error.message.as_deref(),
            Some(format!("Task {task_id} was cancelled before completion").as_str())
        );
        assert_eq!(
            error.data,
            Some(serde_json::json!({ "task_id": task_id.to_string() }))
        );
        assert!(error.exception.is_none());
    }

    /// Completed task with a string result type.
    #[test]
    fn parse_completed_string_result() {
        let wrapped = TaskResult::Ok("hello world".to_owned());
        let result_json = serde_json::to_string(&wrapped).unwrap();
        let row = TaskResultRow {
            id: Uuid::nil(),
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
            id: Uuid::nil(),
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
            id: Uuid::nil(),
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
            id: Uuid::nil(),
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
            id: Uuid::nil(),
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

    /// Verify that get_result returns Result<TaskResult<T>, BrokerError>.
    #[test]
    fn get_result_return_type_contract() {
        #[allow(unused, unreachable_code)]
        async fn _check(broker: &PostgresBroker) {
            let _result: Result<TaskResult<i32>, BrokerError> = broker
                .get_result(Uuid::nil(), Some(Duration::from_secs(5)))
                .await;
        }
    }

    /// Verify that get_task_info returns BrokerResult<TaskInfoRead>
    /// and accepts include_result / include_failed_reason filter params.
    #[test]
    fn get_task_info_return_type_contract() {
        #[allow(unused, unreachable_code)]
        async fn _check(broker: &PostgresBroker) {
            let _result: BrokerResult<TaskInfoRead> =
                broker.get_task_info(Uuid::nil(), false, false).await;
            let _result: BrokerResult<TaskInfoRead> =
                broker.get_task_info(Uuid::nil(), true, false).await;
            let _result: BrokerResult<TaskInfoRead> =
                broker.get_task_info(Uuid::nil(), true, true).await;
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

    #[test]
    fn sent_at_is_cut_to_whole_microseconds() {
        let sent_at = DateTime::parse_from_rfc3339("2026-11-16T12:00:00.123456789Z")
            .unwrap()
            .with_timezone(&Utc);
        let stored = sent_at_at_stored_precision(sent_at).unwrap();
        assert_eq!(stored.nanosecond(), 123_456_000);
        assert_eq!(sent_at_at_stored_precision(stored).unwrap(), stored);
    }
}

/// `sent_at` at the precision that PostgreSQL stores (whole microseconds), so
/// the stored value equals the value that a resend of the same call carries.
fn sent_at_at_stored_precision(sent_at: DateTime<Utc>) -> Result<DateTime<Utc>, BrokerError> {
    let nanos = sent_at.nanosecond();
    sent_at
        .with_nanosecond(nanos - nanos % 1_000)
        .ok_or_else(|| {
            BrokerError::EnqueueContract(EnqueueRefusal::by_rule(
                SendPart::Input,
                format!("sent_at {sent_at} has no whole-microsecond form"),
            ))
        })
}

#[cfg(test)]
mod fused_finalize_tests {
    //! Behavior pins for the fused ok-path finalize (parity with horsies PR
    //! #134, re-anchored on `horsies_complete_task_fused`): one statement
    //! locks the RUNNING row, writes the COMPLETED attempt from the locked
    //! row, transitions, and fires the capacity notify.
    use super::*;
    use crate::broker::terminalization::on_test_pool::terminalize;
    use crate::core::lifecycle::{OwnedClaim, TerminalizationCommand, TerminalizationOutcome};
    use serial_test::serial;
    use uuid::Uuid;

    async fn fused(
        pool: &sqlx::PgPool,
        task_id: &str,
        worker: &str,
        claimed_at: DateTime<Utc>,
        result_json: &str,
    ) -> Vec<TerminalizationOutcome> {
        terminalize(
            pool,
            &TerminalizationCommand::CompleteTaskFused {
                task_id: Uuid::parse_str(task_id).expect("test identity must be UUID"),
                fence: OwnedClaim {
                    worker_id: worker.to_owned(),
                    claimed_at,
                },
                result_json: result_json.to_owned(),
            },
        )
        .await
        .expect("terminalize")
    }

    fn applied(outcomes: &[TerminalizationOutcome]) -> bool {
        matches!(outcomes, [TerminalizationOutcome::Applied { .. }])
    }

    /// The claim generation the seeded row carries.
    async fn generation(pool: &sqlx::PgPool, id: &str) -> DateTime<Utc> {
        sqlx::query_scalar("SELECT claimed_at FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(id))
            .fetch_one(pool)
            .await
            .expect("seeded claim generation")
    }

    /// Seed a task in `status`, optionally claimed by `worker`, with `retry_count`.
    async fn seed(pool: &sqlx::PgPool, id: &str, status: &str, worker: Option<&str>, retry: i32) {
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at,
                command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                started_at, finalizing_at, finalizing_by_worker_id,
                worker_pid, worker_hostname, worker_process_name,
                retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
            ) VALUES (
                $1, 'fused_task', 'default', 100, 'null', NULL, 'ISOLATED',
                'standard_30d', NOW(), NOW(),
                1, decode(repeat('00', 32), 'hex'), NULL,
                $2, NOW(), CASE WHEN $3 IS NOT NULL THEN NOW() END, $3, CASE WHEN $2 = 'CLAIMED' THEN NOW() + interval '1 minute' END,
                NOW(), NULL, NULL,
                123, 'host1', 'worker-123',
                $4, 3, NULL, NULL, FALSE,
                NOW(), FALSE, NULL, NULL
            )",
        )
        .bind(test_uuid(id))
        .bind(status)
        .bind(worker)
        .bind(retry)
        .execute(pool)
        .await
        .expect("seed task");
    }

    async fn cleanup(pool: &sqlx::PgPool, id: &str) {
        sqlx::query("DELETE FROM horsies_task_attempts WHERE task_id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .expect("cleanup");
        sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[serial]
    async fn fused_finalize_completes_running_task() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), 1).await;

        let generation = generation(&pool, &id).await;
        let outcomes = fused(&pool, &id, "w1", generation, "{\"Ok\":7}").await;
        assert!(applied(&outcomes), "RUNNING owned task must finalize");

        let (status, result, attempt_snapshot): (String, Option<String>, Vec<u8>) = sqlx::query_as(
            "SELECT status, convert_from(result_payload, 'UTF8'), attempt_snapshot
                 FROM horsies_task_history WHERE task_id = $1",
        )
        .bind(test_uuid(&id))
        .fetch_one(&pool)
        .await
        .expect("read archived task");
        assert_eq!(status, "COMPLETED");
        assert_eq!(result.as_deref(), Some("{\"Ok\":7}"));

        let attempt: serde_json::Value = serde_json::from_slice(&attempt_snapshot).unwrap();
        assert_eq!(attempt[0][0], 2, "attempt = retry_count(1) + 1");
        assert_eq!(attempt[0][1], "COMPLETED");
        assert_eq!(attempt[0][2], false);

        cleanup(&pool, &id).await;
    }

    #[tokio::test]
    #[serial]
    async fn fused_finalize_noop_when_not_running() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "PENDING", Some("w1"), 0).await;

        let generation = generation(&pool, &id).await;
        let outcomes = fused(&pool, &id, "w1", generation, "{\"Ok\":1}").await;
        assert!(!applied(&outcomes), "non-RUNNING row must not be touched");

        let attempts: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM horsies_task_attempts WHERE task_id = $1")
                .bind(test_uuid(&id))
                .fetch_one(&pool)
                .await
                .expect("count attempts");
        assert_eq!(attempts, 0, "no attempt row written on no-op");

        cleanup(&pool, &id).await;
    }

    #[tokio::test]
    #[serial]
    async fn fused_finalize_noop_on_wrong_worker() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), 0).await;

        let generation = generation(&pool, &id).await;
        let outcomes = fused(&pool, &id, "w2", generation, "{\"Ok\":1}").await;
        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::LostClaim { .. }]
            ),
            "ownership mismatch must not finalize"
        );

        let status: String = sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(&id))
            .fetch_one(&pool)
            .await
            .expect("status");
        assert_eq!(status, "RUNNING", "row untouched");

        cleanup(&pool, &id).await;
    }

    /// C10: the fused finalize must be fenced to a claim generation. A stale
    /// finalize carrying an earlier attempt's `claimed_at` must not complete
    /// a task the same worker re-claimed (new `claimed_at`) after a reaper
    /// requeue.
    #[tokio::test]
    #[serial]
    async fn fused_finalize_fenced_by_claimed_at() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), 0).await;

        // The current claim generation is the row's actual claimed_at.
        let current: DateTime<Utc> =
            sqlx::query_scalar("SELECT claimed_at FROM horsies_tasks WHERE id = $1")
                .bind(test_uuid(&id))
                .fetch_one(&pool)
                .await
                .expect("claimed_at");
        let stale = current - chrono::Duration::minutes(1);

        // Stale generation: fenced out, row stays RUNNING.
        let outcomes = fused(&pool, &id, "w1", stale, "{\"Ok\":1}").await;
        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::LostClaim { .. }]
            ),
            "stale claimed_at must be fenced out"
        );
        let status: String = sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(&id))
            .fetch_one(&pool)
            .await
            .expect("status");
        assert_eq!(
            status, "RUNNING",
            "row must stay RUNNING under a stale fence"
        );

        // Current generation: finalizes.
        let outcomes = fused(&pool, &id, "w1", current, "{\"Ok\":2}").await;
        assert!(applied(&outcomes), "matching claimed_at must finalize");
        let status: String =
            sqlx::query_scalar("SELECT status FROM horsies_task_history WHERE task_id = $1")
                .bind(test_uuid(&id))
                .fetch_one(&pool)
                .await
                .expect("status");
        assert_eq!(status, "COMPLETED");

        cleanup(&pool, &id).await;
    }
}

#[cfg(test)]
mod prestart_expiry_tests {
    //! The expiry of a claimed row whose deadline passed before its start
    //! (`expire_claimed_task_before_start`, `horsies_expire_owned_claim`).
    use super::*;
    use serial_test::serial;
    use uuid::Uuid;

    async fn seed_claimed(pool: &sqlx::PgPool, id: &str, worker: &str) {
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at,
                command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                started_at, finalizing_at, finalizing_by_worker_id,
                worker_pid, worker_hostname, worker_process_name,
                retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
            ) VALUES (
                $1, 'hb_task', 'default', 100, 'null', NULL, 'ISOLATED',
                'standard_30d', NOW(), NOW(),
                1, decode(repeat('00', 32), 'hex'), NULL,
                'CLAIMED', NOW(), NOW(), $2, NOW() + interval '1 minute',
                NULL, NULL, NULL,
                NULL, NULL, NULL,
                0, 3, NULL, NULL, FALSE,
                NOW(), FALSE, NULL, NULL
            )",
        )
        .bind(test_uuid(id))
        .bind(worker)
        .execute(pool)
        .await
        .expect("seed claimed");
    }

    async fn cleanup(pool: &sqlx::PgPool, id: &str) {
        sqlx::query("DELETE FROM horsies_heartbeats WHERE task_id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .expect("cleanup");
    }

    /// The expiry of a claimed row before its start carries no claim
    /// generation: once the deadline has passed, expiry is the correct outcome
    /// for whichever generation of this worker holds the row, so a stale
    /// dispatch and a live one commit the same event. Before the deadline, the
    /// deadline guard (not a generation fence) refuses. The start of a stale
    /// generation is
    /// `lifecycle_tests::claim_start_release::start::a_start_that_does_not_hold_writes_nothing_and_names_its_reason`.
    #[tokio::test]
    #[serial]
    async fn prestart_expiry_waits_for_the_deadline_and_then_expires_any_generation() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed_claimed(&pool, &id, "w1").await;

        let not_due = broker
            .expire_claimed_task_before_start(
                Uuid::parse_str(&id).unwrap(),
                "w1",
                crate::broker::child_writes::WriteDeadline::start_attempt(),
            )
            .await
            .expect("expire");
        assert_eq!(
            not_due,
            PrestartExpiry::NotExpired,
            "unexpired deadline must refuse"
        );
        sqlx::query(
            "UPDATE horsies_tasks SET good_until = NOW() - INTERVAL '1 second' WHERE id = $1",
        )
        .bind(test_uuid(&id))
        .execute(&pool)
        .await
        .expect("age good_until");
        let expired = broker
            .expire_claimed_task_before_start(
                Uuid::parse_str(&id).unwrap(),
                "w1",
                crate::broker::child_writes::WriteDeadline::start_attempt(),
            )
            .await
            .expect("expire");
        assert_eq!(
            expired,
            PrestartExpiry::Expired,
            "expired deadline must expire for any generation of this worker's claim"
        );
        // Python counts a replay as expired too.
        let replayed = broker
            .expire_claimed_task_before_start(
                Uuid::parse_str(&id).unwrap(),
                "w1",
                crate::broker::child_writes::WriteDeadline::start_attempt(),
            )
            .await
            .expect("expire replay");
        assert_eq!(replayed, PrestartExpiry::Expired);

        cleanup(&pool, &id).await;
    }
}

#[cfg(test)]
mod get_result_wait_tests {
    //! C3: `get_result` must never block forever. A no-timeout wait on a task
    //! that does not exist (pruned by retention, or a bogus id) returns a typed
    //! `TaskNotFound` outcome from the initial poll instead of hanging.
    use super::*;
    use serial_test::serial;
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn staged_detail_live_race_retries_instead_of_reporting_absence() {
        let task_id = Uuid::new_v4();
        let probe =
            raw_result_from_history_detail(task_id, TaskDetailResult::Live { task_id }).unwrap();
        assert!(matches!(probe, RawResultProbe::Retry));
    }

    /// The absence reason of the history read reaches the raw result read:
    /// a task that retention can have removed differs from an id that no
    /// retained history held.
    #[test]
    fn an_absent_history_detail_keeps_its_reason() {
        let task_id = Uuid::new_v4();
        for reason in [
            TaskAbsenceReason::PredatesRetainedHistory,
            TaskAbsenceReason::WithinRetainedHistory,
            TaskAbsenceReason::NotClassified,
        ] {
            let probe = raw_result_from_history_detail(
                task_id,
                TaskDetailResult::Absent { task_id, reason },
            )
            .unwrap();
            assert!(
                matches!(probe, RawResultProbe::Absent(kept) if kept == reason),
                "{reason:?}"
            );
        }
    }

    #[tokio::test]
    #[serial]
    async fn get_result_no_timeout_returns_not_found_for_missing_task() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let broker = PostgresBroker::from_pool(pool);
        let missing = Uuid::new_v4();

        // Wrap in an outer timeout: before the fix, a no-timeout wait on a
        // missing task hangs here forever, so the expect() below fails fast.
        let res = tokio::time::timeout(
            Duration::from_secs(5),
            broker.get_result::<i32>(missing, None),
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

    /// The bounded lookup of the interim function, which Python's claim
    /// filter calls (`core/worker/sql.py:234-239`).
    const FIND_NON_RUNNABLE_WORKFLOW_TASKS_SQL: &str =
        "SELECT id, status FROM horsies_find_non_runnable_workflow_tasks($1)";

    #[tokio::test]
    #[serial]
    async fn bounded_lookup_preserves_rows_for_duplicates_missing_links_and_status_changes() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL plan_cache_mode = force_custom_plan")
            .execute(&mut *tx)
            .await
            .unwrap();
        let mut ids = Vec::new();
        for status in ["PAUSED", "CANCELLED", "RUNNING"] {
            let workflow_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO horsies_workflows(id,name,status,on_error,depth,root_workflow_id,sent_at,created_at,updated_at) VALUES($1,'lookup_test',$2,'fail',0,$1,NOW(),NOW(),NOW())",
            )
            .bind(workflow_id)
            .bind(status)
            .execute(&mut *tx)
            .await
            .unwrap();
            for index in 0..2 {
                let task_id = Uuid::new_v4();
                ids.push(task_id);
                sqlx::query("INSERT INTO horsies_workflow_tasks(downstream_subworkflow, id,workflow_id,task_index,node_id,task_name,input,queue_name,priority,dependencies,allow_failed_deps,join_type,status,is_subworkflow,max_retries,execution,retention_class_key,task_id,created_at) VALUES('NONE', $1,$2,$3,$4,'lookup_test','null','default',100,'{}',FALSE,'all','ENQUEUED',FALSE,0,'ISOLATED','forever',$5,NOW())")
                    .bind(Uuid::new_v4()).bind(workflow_id).bind(index)
                    .bind(format!("node_{index}")).bind(task_id)
                    .execute(&mut *tx).await.unwrap();
                if index == 0 {
                    sqlx::query("INSERT INTO horsies_tasks (
                            id, task_name, queue_name, priority, input, task_options, execution,
                            retention_class_key, sent_at, created_at,
                            command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                            status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                            started_at, finalizing_at, finalizing_by_worker_id,
                            worker_pid, worker_hostname, worker_process_name,
                            retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                            updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
                        ) VALUES (
                            $1, 'lookup_test', 'default', 100, 'null', NULL, 'ISOLATED',
                            'forever', NOW(), NOW(),
                            1, decode(repeat('00', 32), 'hex'), NULL,
                            'PENDING', NOW(), NULL, NULL, NULL,
                            NULL, NULL, NULL,
                            NULL, NULL, NULL,
                            0, 3, NULL, NULL, FALSE,
                            NOW(), FALSE, NULL, NULL
                        )")
                        .bind(task_id).execute(&mut *tx).await.unwrap();
                }
            }
        }
        let original = "SELECT t.id,w.status FROM horsies_tasks t JOIN horsies_workflow_tasks wt ON wt.task_id=t.id JOIN horsies_workflows w ON w.id=wt.workflow_id WHERE t.id=ANY($1) AND w.status IN ('PAUSED','CANCELLED')";
        for input in [
            Vec::<Option<Uuid>>::new(),
            vec![None],
            vec![Some(ids[0]), Some(ids[0]), None, Some(Uuid::new_v4())],
            ids.iter().copied().map(Some).collect(),
        ] {
            let mut expected: Vec<(Uuid, String)> = sqlx::query_as(original)
                .bind(&input)
                .fetch_all(&mut *tx)
                .await
                .unwrap();
            let mut actual: Vec<(Uuid, String)> =
                sqlx::query_as(FIND_NON_RUNNABLE_WORKFLOW_TASKS_SQL)
                    .bind(&input)
                    .fetch_all(&mut *tx)
                    .await
                    .unwrap();
            expected.sort();
            actual.sort();
            assert_eq!(actual, expected);
        }
        let absent: Vec<(Uuid, String)> = sqlx::query_as(FIND_NON_RUNNABLE_WORKFLOW_TASKS_SQL)
            .bind(Option::<Vec<Uuid>>::None)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        assert!(absent.is_empty());
        sqlx::query("UPDATE horsies_workflows SET status='RUNNING' WHERE name='lookup_test'")
            .execute(&mut *tx)
            .await
            .unwrap();
        let changed: Vec<(Uuid, String)> = sqlx::query_as(FIND_NON_RUNNABLE_WORKFLOW_TASKS_SQL)
            .bind(&ids)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        assert!(changed.is_empty());
        let mode: String = sqlx::query_scalar("SHOW plan_cache_mode")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(mode, "force_custom_plan");
        tx.rollback().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn paused_workflow_cancels_task_and_resets_node() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let wf_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();

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
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, max_retries, execution, retention_class_key,
                task_id, created_at
            ) VALUES ('NONE', $1, $2, 0, 'node_0', 'c15_task', 'null',
                      'default', 100, '{}', FALSE, 'all',
                      'ENQUEUED', FALSE, 3, 'ISOLATED', 'standard_30d',
                      $3, NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(&wf_id)
        .bind(&task_id)
        .execute(&pool)
        .await
        .expect("insert workflow_task");

        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at,
                command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                started_at, finalizing_at, finalizing_by_worker_id,
                worker_pid, worker_hostname, worker_process_name,
                retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
            ) VALUES (
                $1, 'c15_task', 'default', 100, 'null', NULL, 'ISOLATED',
                'standard_30d', NOW(), NOW(),
                NULL, NULL, NULL,
                'CLAIMED', NOW(), NOW(), 'w1', NOW() + interval '1 minute',
                NULL, NULL, NULL,
                NULL, NULL, NULL,
                0, 3, NULL, NULL, TRUE,
                NOW(), FALSE, NULL, NULL
            )",
        )
        .bind(&task_id)
        .execute(&pool)
        .await
        .expect("insert task");
        let generation: DateTime<Utc> =
            sqlx::query_scalar("SELECT claimed_at FROM horsies_tasks WHERE id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .expect("seeded claim generation");

        let filtered = broker
            .filter_non_runnable_workflow_tasks(&[(task_id, generation)], "w1")
            .await
            .expect("filter");
        assert_eq!(
            filtered,
            vec![task_id.clone()],
            "paused task must be filtered"
        );

        let task_status: String =
            sqlx::query_scalar("SELECT status FROM horsies_task_history WHERE task_id = $1")
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
        sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
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

#[cfg(test)]
mod terminal_at_stamp_tests {
    //! Every terminal transition moves the row out of `horsies_tasks` into
    //! `horsies_task_history` and stamps `terminal_at` from the database
    //! clock in the same statement; a live transition keeps the row live and
    //! writes no history row. The baseline live table has no `terminal_at`
    //! column, and the history column is NOT NULL
    //! (`crates/horsies-core/baseline/0001_tables.sql`). One pin per status
    //! family; the writer-inventory scan (tests/writer_inventory.rs) covers
    //! the rest structurally.
    use super::*;
    use serial_test::serial;
    use uuid::Uuid;

    async fn connect() -> PostgresBroker {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        broker
    }

    /// Seed a task in `status`, optionally claimed by `worker`, with
    /// `good_until` offset seconds from now (negative = already passed).
    async fn seed(
        pool: &sqlx::PgPool,
        id: &str,
        status: &str,
        worker: Option<&str>,
        good_until_offset_s: Option<i64>,
    ) {
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at,
                command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                started_at, finalizing_at, finalizing_by_worker_id,
                worker_pid, worker_hostname, worker_process_name,
                retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
            ) VALUES (
                $1, 'terminal_at_task', 'default', 100, 'null', NULL, 'ISOLATED',
                'standard_30d', NOW(), NOW(),
                1, decode(repeat('00', 32), 'hex'), NULL,
                $2, NOW(), CASE WHEN $3 IS NOT NULL THEN NOW() END, $3, CASE WHEN $2 = 'CLAIMED' THEN NOW() + interval '1 minute' END,
                NOW(), NULL, NULL,
                CASE WHEN $2 = 'RUNNING' THEN 4242 END, CASE WHEN $2 = 'RUNNING' THEN 'test-host' END, CASE WHEN $2 = 'RUNNING' THEN 'test-process' END,
                0, 3, NULL, NOW() + $4 * INTERVAL '1 second', FALSE,
                NOW(), FALSE, NULL, NULL
            )",
        )
        .bind(test_uuid(id))
        .bind(status)
        .bind(worker)
        .bind(good_until_offset_s)
        .execute(pool)
        .await
        .expect("seed task");
    }

    /// The database clock, read in a statement of its own.
    async fn database_now(pool: &sqlx::PgPool) -> DateTime<Utc> {
        sqlx::query_scalar("SELECT now()")
            .fetch_one(pool)
            .await
            .expect("read the database clock")
    }

    /// The status of the live row, or `None` when `horsies_tasks` does not
    /// hold the task.
    async fn live_status(pool: &sqlx::PgPool, id: &str) -> Option<String> {
        sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(id))
            .fetch_optional(pool)
            .await
            .expect("read the live row")
    }

    /// The status and `terminal_at` of the history row, or `None` when
    /// history does not hold the task.
    async fn history_row(pool: &sqlx::PgPool, id: &str) -> Option<(String, DateTime<Utc>)> {
        sqlx::query_as("SELECT status, terminal_at FROM horsies_task_history WHERE task_id = $1")
            .bind(test_uuid(id))
            .fetch_optional(pool)
            .await
            .expect("read the history row")
    }

    /// The task left the live table, and its history row has `status` and a
    /// `terminal_at` of the transition: at or after `before` (the database
    /// clock before the transition) and at or before the database clock
    /// after it.
    async fn assert_moved(pool: &sqlx::PgPool, id: &str, status: &str, before: DateTime<Utc>) {
        assert_eq!(
            live_status(pool, id).await,
            None,
            "the {status} transition must remove the live row"
        );
        let after = database_now(pool).await;
        let Some((history_status, terminal_at)) = history_row(pool, id).await else {
            panic!("the {status} transition must write a history row");
        };
        assert_eq!(history_status, status);
        assert!(
            before <= terminal_at && terminal_at <= after,
            "terminal_at {terminal_at} must be the transition time, from {before} to {after}"
        );
    }

    async fn cleanup(pool: &sqlx::PgPool, id: &str) {
        sqlx::query("DELETE FROM horsies_task_attempts WHERE task_id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .expect("cleanup");
        sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[serial]
    async fn completed_row_gets_terminal_at() {
        let broker = connect().await;
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), None).await;
        // The fused completion fences on the claim generation: stamp one.
        let generation: DateTime<Utc> = sqlx::query_scalar(
            "UPDATE horsies_tasks SET claimed_at = NOW() WHERE id = $1 RETURNING claimed_at",
        )
        .bind(test_uuid(&id))
        .fetch_one(&pool)
        .await
        .expect("stamp claim generation");
        let before = database_now(&pool).await;

        let outcomes = crate::broker::terminalization::on_test_pool::terminalize(
            &pool,
            &crate::core::lifecycle::TerminalizationCommand::CompleteTaskFused {
                task_id: Uuid::parse_str(&id).expect("test identity must be UUID"),
                fence: crate::core::lifecycle::OwnedClaim {
                    worker_id: "w1".to_owned(),
                    claimed_at: generation,
                },
                result_json: "{\"Ok\":1}".to_owned(),
            },
        )
        .await
        .expect("finalize");
        assert!(matches!(
            outcomes.as_slice(),
            [crate::core::lifecycle::TerminalizationOutcome::Applied { .. }]
        ));

        assert_moved(&pool, &id, "COMPLETED", before).await;

        cleanup(&pool, &id).await;
    }

    #[tokio::test]
    #[serial]
    async fn failed_row_gets_terminal_at() {
        let broker = connect().await;
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), None).await;
        let before = database_now(&pool).await;

        let outcomes = crate::test_support::terminal_writes::fail_running_task(
            &pool,
            Uuid::parse_str(&id).expect("test identity must be UUID"),
            "w1",
            &crate::test_support::terminal_writes::TerminalFailure::of_task(
                "{\"Err\":{}}",
                Some("TASK_EXCEPTION"),
                None,
            ),
        )
        .await
        .expect("fail");
        assert!(matches!(
            outcomes.as_slice(),
            [crate::core::lifecycle::TerminalizationOutcome::Applied { .. }]
        ));

        assert_moved(&pool, &id, "FAILED", before).await;

        cleanup(&pool, &id).await;
    }

    #[tokio::test]
    #[serial]
    async fn cancelled_row_gets_terminal_at() {
        let broker = connect().await;
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "PENDING", None, None).await;
        let before = database_now(&pool).await;

        let cancelled =
            crate::monitoring::cancel_task(&broker, Uuid::parse_str(&id).unwrap(), false)
                .await
                .expect("cancel");
        assert_eq!(
            cancelled.was_status,
            crate::core::types::status::TaskStatus::Pending
        );

        assert_moved(&pool, &id, "CANCELLED", before).await;

        cleanup(&pool, &id).await;
    }

    #[tokio::test]
    #[serial]
    async fn expired_row_gets_terminal_at() {
        let broker = connect().await;
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "CLAIMED", Some("w1"), Some(-60)).await;
        let before = database_now(&pool).await;

        let expired = broker
            .expire_claimed_task_before_start(
                Uuid::parse_str(&id).unwrap(),
                "w1",
                crate::broker::child_writes::WriteDeadline::start_attempt(),
            )
            .await
            .expect("expire");
        assert_eq!(expired, PrestartExpiry::Expired);

        assert_moved(&pool, &id, "EXPIRED", before).await;

        cleanup(&pool, &id).await;
    }

    /// A requeue is a live transition: the row stays live as PENDING, writes
    /// no history row, counts the retry, and clears every claim and run
    /// field, as the baseline retry write
    /// (`crates/horsies-core/baseline/0004_lifecycle_functions.sql:1479-1496`).
    #[tokio::test]
    #[serial]
    async fn requeue_keeps_the_row_live_and_writes_no_history_row() {
        let broker = connect().await;
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), None).await;

        let applied = broker
            .requeue(Uuid::parse_str(&id).unwrap(), Some(Utc::now()), "w1")
            .await
            .expect("requeue");
        assert!(applied);

        assert_eq!(live_status(&pool, &id).await.as_deref(), Some("PENDING"));
        assert_eq!(
            history_row(&pool, &id).await,
            None,
            "a live transition must write no history row"
        );
        let (retry_count, cleared): (i32, serde_json::Value) = sqlx::query_as(
            "SELECT retry_count,
                    jsonb_build_object(
                        'claimed_at', claimed_at,
                        'claimed_by_worker_id', claimed_by_worker_id,
                        'claim_expires_at', claim_expires_at,
                        'started_at', started_at,
                        'worker_pid', worker_pid,
                        'worker_hostname', worker_hostname,
                        'worker_process_name', worker_process_name,
                        'finalizing_at', finalizing_at,
                        'finalizing_by_worker_id', finalizing_by_worker_id
                    )
             FROM horsies_tasks WHERE id = $1",
        )
        .bind(test_uuid(&id))
        .fetch_one(&pool)
        .await
        .expect("requeued row");
        assert_eq!(retry_count, 1);
        let fields = cleared.as_object().expect("field object");
        for (field, value) in fields {
            assert!(value.is_null(), "requeue must clear {field}, found {value}");
        }
        assert_eq!(fields.len(), 9);

        cleanup(&pool, &id).await;
    }

    /// A requeue with no retry time is due at once: `next_retry_at` and the
    /// NOT NULL `enqueued_at` take the database clock of the requeue.
    #[tokio::test]
    #[serial]
    async fn requeue_with_no_retry_time_is_due_at_the_database_clock() {
        let broker = connect().await;
        let pool = broker.pool().clone();
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), None).await;
        let before = database_now(&pool).await;

        let applied = broker
            .requeue(Uuid::parse_str(&id).unwrap(), None, "w1")
            .await
            .expect("requeue");
        assert!(applied);

        let after = database_now(&pool).await;
        let (enqueued_at, next_retry_at): (DateTime<Utc>, Option<DateTime<Utc>>) =
            sqlx::query_as("SELECT enqueued_at, next_retry_at FROM horsies_tasks WHERE id = $1")
                .bind(test_uuid(&id))
                .fetch_one(&pool)
                .await
                .expect("requeued row");
        assert_eq!(next_retry_at, Some(enqueued_at));
        assert!(
            before <= enqueued_at && enqueued_at <= after,
            "enqueued_at {enqueued_at} must be the requeue time, from {before} to {after}"
        );

        cleanup(&pool, &id).await;
    }
}

#[cfg(test)]
mod session_settings_tests {
    use super::*;
    use serial_test::serial;

    fn config(database_url: String, pgbouncer_transaction_mode: bool) -> PostgresConfig {
        PostgresConfig {
            database_url,
            session_database_url: None,
            pgbouncer_transaction_mode,
            pool_pre_ping: true,
            worker_pool_pre_ping: false,
            pool_size: 2,
            max_overflow: 0,
            retain_rerun_input_default: false,
            pool_timeout: 30,
            pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
            echo: false,
            tcp_keepalives: Default::default(),
            tcp_user_timeout: Default::default(),
        }
    }

    async fn jit(pool: &PgPool) -> String {
        sqlx::query_scalar("SELECT current_setting('jit')")
            .fetch_one(pool)
            .await
            .expect("read jit setting")
    }

    #[test]
    fn transaction_mode_pools_do_not_keep_session_settings() {
        let url = "postgresql://postgres@localhost/horsies".to_owned();
        assert_eq!(
            session_settings(&config(url.clone(), false)),
            SessionSettings::Kept
        );
        assert_eq!(
            session_settings(&config(url, true)),
            SessionSettings::NotKept
        );
    }

    #[tokio::test]
    #[serial]
    async fn broker_pools_that_keep_session_settings_run_without_jit() {
        let url = crate::broker::terminalization_matrix::migrated_database_url().await;
        let separator = if url.contains('?') { '&' } else { '?' };
        let mut config = config(url.clone(), false);
        config.session_database_url = Some(format!("{url}{separator}application_name=jit_session"));
        let broker = PostgresBroker::connect_with(&config)
            .await
            .expect("connect broker");
        assert_eq!(jit(broker.pool()).await, "off");
        assert_eq!(jit(&broker.session_pool).await, "off");
        broker.pool().close().await;
        broker.session_pool.close().await;
    }

    #[tokio::test]
    #[serial]
    async fn pools_that_do_not_keep_session_settings_keep_the_server_jit() {
        let url = crate::broker::terminalization_matrix::migrated_database_url().await;
        let options = PgConnectOptions::from_str(&url).expect("parse test URL");
        let plain = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone())
            .await
            .expect("connect plain pool");
        let not_kept = with_session_settings(
            PgPoolOptions::new().max_connections(1),
            SessionSettings::NotKept,
        )
        .connect_with(options)
        .await
        .expect("connect not-kept pool");
        assert_eq!(jit(&not_kept).await, jit(&plain).await);
        plain.close().await;
        not_kept.close().await;
    }
}

#[cfg(test)]
mod broker_operation_error_tests {
    //! Python's catalog for broker operations: a refused argument is
    //! `INVALID_INPUT` with no database call, refused connection settings are
    //! not retryable, and a database failure carries the driver
    //! classification of its cause (`core/brokers/postgres.py`,
    //! `_operation_failed`).
    use super::*;
    use crate::db::StatementFailure;

    /// No server listens on port 1, so a call that reached the pool would
    /// fail with a pool timeout instead of the refusal under test.
    const UNREACHABLE_URL: &str = "postgresql://horsies@127.0.0.1:1/horsies";

    fn unreachable_broker() -> PostgresBroker {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy(UNREACHABLE_URL)
            .expect("a lazy pool opens no connection");
        PostgresBroker::from_pool(pool)
    }

    #[tokio::test]
    async fn connect_refuses_a_driver_suffix_scheme_before_any_connection() {
        let refused = PostgresBroker::connect("postgresql+psycopg://horsies@127.0.0.1:1/horsies")
            .await
            .err()
            .expect("the scheme is refused");

        assert!(matches!(
            &refused,
            BrokerError::InvalidConnectionSettings(
                crate::core::config::PostgresConfigError::InvalidUrlScheme(scheme)
            ) if scheme == "postgresql+psycopg"
        ));
        assert!(!refused.is_retryable());
    }

    #[tokio::test]
    async fn connect_with_refuses_a_config_rule_as_not_retryable() {
        let config = PostgresConfig::from_url("mysql://horsies@127.0.0.1:1/horsies");

        let refused = PostgresBroker::connect_with(&config)
            .await
            .err()
            .expect("the scheme is refused");

        assert!(matches!(
            refused,
            BrokerError::InvalidConnectionSettings(
                crate::core::config::PostgresConfigError::InvalidUrlScheme(_)
            )
        ));
        assert!(!refused.is_retryable());
    }

    #[test]
    fn an_unparseable_url_is_refused_as_not_retryable() {
        let refused = pg_connect_options("postgresql://horsies@127.0.0.1:notaport/horsies", false)
            .err()
            .expect("the port does not parse");

        assert!(matches!(
            refused,
            BrokerError::InvalidConnectionSettings(
                crate::core::config::PostgresConfigError::UnparseableUrl(_)
            )
        ));
        assert!(!refused.is_retryable());
    }

    #[tokio::test]
    async fn ping_workers_refuses_a_zero_timeout_as_invalid_input() {
        let broker = unreachable_broker();

        let Err(refused) = broker.ping_workers(None, Duration::ZERO, None).await else {
            panic!("a zero timeout must be refused");
        };

        assert_eq!(refused.code, BrokerErrorCode::InvalidInput);
        assert!(!refused.retryable);
    }

    #[tokio::test]
    async fn ping_workers_refuses_zero_min_responses_as_invalid_input() {
        let broker = unreachable_broker();

        let Err(refused) = broker
            .ping_workers(None, Duration::from_secs(1), Some(0))
            .await
        else {
            panic!("min_responses of 0 must be refused");
        };

        assert_eq!(refused.code, BrokerErrorCode::InvalidInput);
        assert!(!refused.retryable);
    }

    /// A broker with no listener (built from a pool with no session URL):
    /// `LISTENER_DISABLED`, not retryable, before any database call, as
    /// Python's broker with no listener.
    #[tokio::test]
    async fn ping_workers_on_a_broker_without_a_listener_is_listener_disabled() {
        let observe_only = PostgresBroker::from_pool_observe_only(
            PgPoolOptions::new()
                .connect_lazy(UNREACHABLE_URL)
                .expect("a lazy pool opens no connection"),
        );
        for broker in [unreachable_broker(), observe_only] {
            let Err(refused) = broker
                .ping_workers(None, Duration::from_secs(1), None)
                .await
            else {
                panic!("a broker without a listener cannot ping");
            };

            assert_eq!(refused.code, BrokerErrorCode::ListenerDisabled);
            assert!(!refused.retryable);
        }
    }

    /// Python `test_broker_postgres.py::test_history_rejects_nonpositive_limit`.
    #[tokio::test]
    async fn worker_state_history_refuses_a_limit_of_zero_or_less() {
        let broker = unreachable_broker();

        for limit in [0, -1, i64::MIN] {
            let Err(refused) = broker.get_worker_state_history("w1", Some(limit)).await else {
                panic!("limit {limit} must be refused");
            };
            assert_eq!(refused.code, BrokerErrorCode::InvalidInput, "{limit}");
            assert!(!refused.retryable, "{limit}");
        }
    }

    /// `close` returns the core pools to `Idle`, and the next operation opens
    /// new pools (`HandleState::Idle`, `broker/pools.rs`), as Python's broker
    /// stays usable after `close_async`
    /// (`python/horsies/core/brokers/postgres.py:2403-2411`). A ping after a close
    /// meets the server: here a refused connection, which is retryable. A
    /// checkout on a closed pool (`PoolClosed`) stays not retryable
    /// (`a_monitoring_read_failure_carries_the_driver_classification`).
    #[tokio::test]
    async fn a_database_ping_after_close_opens_new_pools_and_a_refused_connection_is_retryable() {
        let broker = unreachable_broker();
        broker.close().await;

        let Err(failed) = broker.ping_database().await else {
            panic!("no server listens on the unreachable URL");
        };

        assert_eq!(failed.code, BrokerErrorCode::DbPingFailed);
        assert!(
            !failed.message.contains("pool is closed"),
            "the ping must run on new pools: {}",
            failed.message
        );
        assert!(failed.retryable, "{}", failed.message);
    }

    fn checkout_failed(cause: crate::db::CheckoutFailure) -> BrokerError {
        BrokerError::Statement(StatementFailure::NotSent {
            statement: "read",
            cause: crate::db::NotSentCause::Checkout(cause),
        })
    }

    fn checkout_timed_out() -> BrokerError {
        checkout_failed(crate::db::CheckoutFailure::TimedOut {
            role: crate::db::PoolRole::Main,
            waited: Duration::from_secs(30),
        })
    }

    fn pool_closed() -> BrokerError {
        checkout_failed(crate::db::CheckoutFailure::PoolClosed {
            role: crate::db::PoolRole::Main,
        })
    }

    #[test]
    fn a_monitoring_read_failure_carries_the_driver_classification() {
        assert!(monitoring_query_error("read", checkout_timed_out()).retryable);
        assert!(!monitoring_query_error("read", pool_closed()).retryable);
        assert!(
            !monitoring_query_error(
                "read",
                BrokerError::RowDecode(RowDecodeError::NoRow { statement: "read" })
            )
            .retryable
        );
        assert!(worker_ping_statement_error("notify", checkout_timed_out()).retryable);
        assert!(!worker_ping_statement_error("notify", pool_closed()).retryable);
    }
}

#[cfg(test)]
mod connect_tests {
    //! A connect checks the config and opens no connection.
    use super::*;

    /// No server listens on port 1.
    const UNREACHABLE_URL: &str = "postgresql://horsies@127.0.0.1:1/horsies";

    #[tokio::test]
    async fn a_connect_opens_no_connection() {
        let broker = PostgresBroker::connect(UNREACHABLE_URL)
            .await
            .expect("a connect opens no connection, so it needs no server");

        assert_eq!(broker.pool().size(), 0);
        assert_eq!(broker.session_pool().size(), 0);
        assert_eq!(broker.reserve_pool().size(), 0);
    }

    #[tokio::test]
    async fn a_worker_connect_with_a_split_session_url_opens_no_connection() {
        let mut config = PostgresConfig::from_url(UNREACHABLE_URL);
        config.session_database_url = Some("postgresql://horsies@127.0.0.1:2/horsies".to_owned());

        let broker = PostgresBroker::connect_worker_with(&config)
            .await
            .expect("a connect opens no connection, so it needs no server");

        assert_eq!(broker.pool().size(), 0);
        assert_eq!(broker.session_pool().size(), 0);
    }
}

#[cfg(test)]
mod result_wait_tests {
    //! The result wait: Python's limit bound (`python/horsies/core/utils/wait_timeout.py`)
    //! before any read, Python's repoll intervals, a read at once after a
    //! listener gap, and polling with no listener or when the listener stops
    //! (`python/horsies/core/brokers/postgres.py:3265-3285`).
    use super::*;
    use crate::broker::wait_limit::MAX_WAIT_LIMIT_MS;
    use serial_test::serial;

    /// No server listens on port 1, so a call that reached the pool would
    /// fail with a pool timeout instead of the refusal under test.
    fn unreachable_broker() -> PostgresBroker {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies")
            .expect("a lazy pool opens no connection");
        PostgresBroker::from_pool(pool)
    }

    fn refused_limits() -> [Duration; 2] {
        [Duration::from_millis(MAX_WAIT_LIMIT_MS + 1), Duration::MAX]
    }

    /// Python `test_wait_timeout.py::TestTaskHandle::test_get_refuses_before_any_read`.
    /// `Duration::MAX` used to panic on `Instant + Duration`.
    #[tokio::test]
    async fn get_result_refuses_a_limit_above_the_bound_before_any_read() {
        let broker = unreachable_broker();
        let expected = TaskError::builtin(RetrievalCode::InvalidInput, "").error_code;

        for limit in refused_limits() {
            let outcome = broker
                .get_result::<i32>(Uuid::new_v4(), Some(limit))
                .await
                .expect("a refused limit is an outcome, not a broker error");
            let error = outcome.unwrap_err();
            assert_eq!(error.error_code, expected, "{limit:?}");
        }
    }

    #[tokio::test]
    async fn get_raw_result_record_refuses_a_limit_above_the_bound_before_any_read() {
        let broker = unreachable_broker();

        for limit in refused_limits() {
            let Err(refused) = broker
                .get_raw_result_record(Uuid::new_v4(), Some(limit))
                .await
            else {
                panic!("limit {limit:?} must be refused");
            };
            assert_eq!(refused.code, BrokerErrorCode::InvalidInput, "{limit:?}");
            assert!(!refused.retryable);
        }
    }

    #[test]
    fn the_repoll_intervals_are_python_s() {
        assert_eq!(TASK_RESULT_REPOLL_SUBSCRIBED, Duration::from_secs(5));
        assert_eq!(TASK_RESULT_REPOLL_POLLING, Duration::from_millis(200));
        assert_eq!(
            ResultWaitMode::Polling.repoll_interval(),
            TASK_RESULT_REPOLL_POLLING
        );
    }

    /// A broker with no listener (built from a pool with no session URL)
    /// polls, as Python's broker with no listener. Nothing connects.
    #[tokio::test]
    async fn a_broker_without_a_listener_makes_the_wait_poll() {
        let observe_only = PostgresBroker::from_pool_observe_only(
            PgPoolOptions::new()
                .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies")
                .expect("a lazy pool opens no connection"),
        );
        for broker in [unreachable_broker(), observe_only] {
            let mode = broker.result_wait_mode(Uuid::new_v4()).await;

            assert!(matches!(mode, ResultWaitMode::Polling));
        }
    }

    /// The longest a wake, a gap or a stop may take to end a pause in these
    /// tests; well below the subscribed repoll interval.
    const WAKE_BOUND: Duration = Duration::from_secs(3);

    /// A broker on the test database whose listener connection carries
    /// `application_name`, so a test can find its backend.
    async fn listener_broker(application_name: &str) -> PostgresBroker {
        let url = crate::broker::terminalization_matrix::migrated_database_url().await;
        let separator = if url.contains('?') { '&' } else { '?' };
        PostgresBroker::from_pool_and_session_url(
            crate::broker::terminalization_matrix::migrated_pool().await,
            &format!("{url}{separator}application_name={application_name}"),
        )
    }

    #[tokio::test]
    #[serial]
    async fn a_pause_without_a_wake_keeps_the_subscription() {
        let broker = listener_broker("result_wait_keep").await;
        let mut mode = broker.result_wait_mode(Uuid::new_v4()).await;
        assert!(matches!(mode, ResultWaitMode::Subscribed(_)));
        assert_eq!(mode.repoll_interval(), TASK_RESULT_REPOLL_SUBSCRIBED);

        mode.pause(Duration::from_millis(10)).await;

        assert!(matches!(mode, ResultWaitMode::Subscribed(_)));
    }

    #[tokio::test]
    #[serial]
    async fn a_task_done_notification_for_the_task_ends_the_pause() {
        let broker = listener_broker("result_wait_notify").await;
        let task_id = Uuid::new_v4();
        let mut mode = broker.result_wait_mode(task_id).await;
        assert!(matches!(mode, ResultWaitMode::Subscribed(_)));

        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(TASK_DONE_CHANNEL)
            .bind(task_id.to_string())
            .execute(broker.pool())
            .await
            .expect("notify task_done");
        let started = Instant::now();
        mode.pause(TASK_RESULT_REPOLL_SUBSCRIBED).await;

        assert!(started.elapsed() < WAKE_BOUND, "{:?}", started.elapsed());
    }

    /// The listener loses its connection: it connects again, LISTENs again
    /// and sends a gap, and the wait reads at once instead of at its repoll.
    #[tokio::test]
    #[serial]
    async fn a_listener_gap_ends_the_pause_at_once() {
        let application_name = format!("result_wait_gap_{}", Uuid::new_v4().simple());
        let broker = listener_broker(&application_name).await;
        let mut mode = broker.result_wait_mode(Uuid::new_v4()).await;
        assert!(matches!(mode, ResultWaitMode::Subscribed(_)));

        let terminated: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM (
                 SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                 WHERE application_name = $1
             ) AS killed",
        )
        .bind(&application_name)
        .fetch_one(broker.pool())
        .await
        .expect("terminate the listener backend");
        assert_eq!(terminated, 1, "exactly the listener backend is terminated");

        let started = Instant::now();
        mode.pause(TASK_RESULT_REPOLL_SUBSCRIBED).await;

        assert!(
            started.elapsed() < WAKE_BOUND,
            "the gap must end the pause before the repoll interval: {:?}",
            started.elapsed()
        );
        assert!(matches!(mode, ResultWaitMode::Subscribed(_)));
    }

    /// An observe-only broker keeps its listener, as Python's web tool
    /// (`run_schema_migrations=False`): its waits subscribe and it can ping.
    #[tokio::test]
    #[serial]
    async fn an_observe_only_broker_keeps_its_listener() {
        let config = PostgresConfig {
            database_url: crate::broker::terminalization_matrix::migrated_database_url().await,
            session_database_url: None,
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            worker_pool_pre_ping: false,
            pool_size: 2,
            max_overflow: 0,
            retain_rerun_input_default: false,
            pool_timeout: 30,
            pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
            echo: false,
            tcp_keepalives: Default::default(),
            tcp_user_timeout: Default::default(),
        };
        let broker = PostgresBroker::connect_observe_only(&config)
            .await
            .expect("connect an observe-only broker");

        let mode = broker.result_wait_mode(Uuid::new_v4()).await;
        assert!(matches!(mode, ResultWaitMode::Subscribed(_)));
        let pinged = broker
            .ping_workers(None, Duration::from_millis(200), None)
            .await;
        assert!(pinged.is_ok(), "{:?}", pinged.err());
    }

    /// Two brokers on one session URL in one process share one listener
    /// connection.
    #[tokio::test]
    #[serial]
    async fn two_brokers_on_one_session_url_open_one_listener_connection() {
        let application_name = format!("shared_listener_{}", Uuid::new_v4().simple());
        let first = listener_broker(&application_name).await;
        let second = listener_broker(&application_name).await;

        let first_wait = first.result_wait_mode(Uuid::new_v4()).await;
        let second_wait = second.result_wait_mode(Uuid::new_v4()).await;
        assert!(matches!(first_wait, ResultWaitMode::Subscribed(_)));
        assert!(matches!(second_wait, ResultWaitMode::Subscribed(_)));

        let connections: i64 =
            sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name = $1")
                .bind(&application_name)
                .fetch_one(first.pool())
                .await
                .expect("count the listener connections");
        assert_eq!(connections, 1);
    }

    /// Dropping the broker stops its listener. The wait turns into polling,
    /// so the loop does not spin on a stopped subscription.
    #[tokio::test]
    #[serial]
    async fn a_stopped_listener_turns_the_wait_into_polling() {
        let broker = listener_broker("result_wait_stop").await;
        let mut mode = broker.result_wait_mode(Uuid::new_v4()).await;
        assert!(matches!(mode, ResultWaitMode::Subscribed(_)));

        drop(broker);
        mode.pause(TASK_RESULT_REPOLL_SUBSCRIBED).await;

        assert!(matches!(mode, ResultWaitMode::Polling));
    }
}

#[cfg(test)]
mod stale_and_expired_list_tests {
    //! The stale and expired task lists: a threshold of whole minutes from 0
    //! to one year, a bound of `TASK_LIST_BOUND` rows, Python's verdict and
    //! order, and the expired record's `enqueued_at` and `expired_for`.
    use super::*;
    use serial_test::serial;

    #[tokio::test]
    async fn a_threshold_outside_one_year_is_refused_before_any_read() {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies")
            .expect("a lazy pool opens no connection");
        let broker = PostgresBroker::from_pool(pool);

        for minutes in [
            -1,
            crate::broker::task_lists::MAX_STALE_THRESHOLD_MINUTES + 1,
        ] {
            let Err(refused) = broker.get_stale_tasks(minutes).await else {
                panic!("threshold {minutes} must be refused");
            };
            assert_eq!(refused.code, BrokerErrorCode::InvalidInput, "{minutes}");
        }
    }

    async fn seed(
        pool: &PgPool,
        task_name: &str,
        status: &str,
        good_until_ago_min: Option<i32>,
    ) -> Uuid {
        let task_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_tasks (
                 id, task_name, queue_name, priority, input, task_options, execution,
                 retention_class_key, sent_at, created_at, command_fingerprint_version,
                 command_fingerprint, status, enqueued_at, started_at, good_until,
                 retry_count, max_retries, is_workflow_task, updated_at, retain_rerun_input
             ) VALUES (
                 $1, $2, 'default', 100, '{}', NULL, 'ISOLATED',
                 'forever', NOW(), NOW(), 1,
                 decode(repeat('0d', 32), 'hex'), $3, NOW() - INTERVAL '20 minutes',
                 CASE WHEN $3 = 'RUNNING' THEN NOW() - INTERVAL '10 minutes' END,
                 NOW() - $4 * INTERVAL '1 minute',
                 0, 0, FALSE, NOW(), FALSE
             )",
        )
        .bind(task_id)
        .bind(task_name)
        .bind(status)
        .bind(good_until_ago_min)
        .execute(pool)
        .await
        .expect("seed a baseline task");
        task_id
    }

    async fn runner_beat(pool: &PgPool, task_id: Uuid, minutes_ago: i32) {
        sqlx::query(
            "INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
             VALUES ($1, 'list-worker', 'runner', NOW() - $2 * INTERVAL '1 minute', 'h', 1)",
        )
        .bind(task_id)
        .bind(minutes_ago)
        .execute(pool)
        .await
        .expect("seed a runner beat");
    }

    async fn clean(pool: &PgPool, task_name: &str) {
        sqlx::query(
            "DELETE FROM horsies_heartbeats WHERE task_id IN (
                 SELECT id FROM horsies_tasks WHERE task_name = $1)",
        )
        .bind(task_name)
        .execute(pool)
        .await
        .expect("clean beats");
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(pool)
            .await
            .expect("clean tasks");
    }

    /// A row with no beat and a row whose latest beat is older than the
    /// threshold are stale, beatless first; a fresh beat keeps a row out.
    #[tokio::test]
    #[serial]
    async fn the_stale_list_follows_the_latest_beat() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let task_name = format!("stale_list_{}", Uuid::new_v4().simple());
        let broker = PostgresBroker::from_pool(pool.clone());
        let beatless = seed(&pool, &task_name, "RUNNING", None).await;
        let old_beat = seed(&pool, &task_name, "RUNNING", None).await;
        let fresh_beat = seed(&pool, &task_name, "RUNNING", None).await;
        runner_beat(&pool, old_beat, 5).await;
        runner_beat(&pool, fresh_beat, 0).await;

        let list = broker.get_stale_tasks(1).await.expect("stale list");

        let ours: Vec<&StaleTaskRow> = list
            .rows()
            .iter()
            .filter(|row| row.task_name == task_name)
            .collect();
        assert_eq!(
            ours.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![beatless, old_beat]
        );
        assert_eq!(ours[0].last_heartbeat, None);
        assert!(ours[1].last_heartbeat.is_some());
        clean(&pool, &task_name).await;
    }

    /// Python's expired record: `enqueued_at` and how long ago the deadline
    /// passed, on the database clock.
    #[tokio::test]
    #[serial]
    async fn the_expired_list_carries_enqueued_at_and_expired_for() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let task_name = format!("expired_list_{}", Uuid::new_v4().simple());
        let broker = PostgresBroker::from_pool(pool.clone());
        let expired = seed(&pool, &task_name, "PENDING", Some(2)).await;
        let _not_yet = seed(&pool, &task_name, "PENDING", Some(-5)).await;

        let list = broker.get_expired_tasks().await.expect("expired list");

        let ours: Vec<&ExpiredTaskRow> = list
            .rows()
            .iter()
            .filter(|row| row.task_name == task_name)
            .collect();
        assert_eq!(ours.len(), 1);
        assert_eq!(ours[0].id, expired);
        assert!(
            ours[0].expired_for_us >= 120_000_000,
            "{}",
            ours[0].expired_for_us
        );
        assert!(ours[0].enqueued_at < ours[0].good_until);
        clean(&pool, &task_name).await;
    }
}

#[cfg(test)]
mod pool_options_tests {
    //! Python's pool rules (`python/horsies/core/brokers/postgres.py:1343-1404`,
    //! `python/horsies/core/utils/db.py:165-222`): `pool_recycle` retires a
    //! connection by age, the session pool is sized as the runtime pool, and
    //! a connection idle for more than 1 s is pinged at checkout.
    use super::*;
    use serial_test::serial;

    fn config(pool_pre_ping: bool) -> PostgresConfig {
        let mut config = PostgresConfig::from_url("postgresql://horsies@127.0.0.1/horsies");
        config.pool_size = 3;
        config.max_overflow = 2;
        config.pool_timeout = 7;
        config.pool_recycle = crate::manifest::PoolRecycle::after_seconds(1_800);
        config.pool_pre_ping = pool_pre_ping;
        config
    }

    #[test]
    fn pool_recycle_is_the_age_at_which_a_connection_retires() {
        let options = pg_pool_options(&config(true));

        assert_eq!(options.get_max_lifetime(), Some(Duration::from_secs(1_800)));
        assert_eq!(options.get_idle_timeout(), None);
        assert_eq!(options.get_max_connections(), 5);
        assert_eq!(options.get_acquire_timeout(), Duration::from_secs(7));
    }

    #[test]
    fn a_pool_that_never_recycles_keeps_its_connections() {
        let mut never = config(true);
        never.pool_recycle = crate::manifest::PoolRecycle::Never;

        assert_eq!(pg_pool_options(&never).get_max_lifetime(), None);
        assert_eq!(pg_session_pool_options(&never).get_max_lifetime(), None);
    }

    #[test]
    fn the_session_pool_is_sized_as_the_runtime_pool() {
        let options = pg_session_pool_options(&config(true));

        assert_eq!(options.get_max_connections(), 5);
        assert_eq!(options.get_max_lifetime(), Some(Duration::from_secs(1_800)));
    }

    /// The ping at every checkout is gone in both settings; with
    /// `pool_pre_ping` on, the idle ping replaces it.
    #[test]
    fn no_pool_pings_at_every_checkout() {
        assert!(!pg_pool_options(&config(true)).get_test_before_acquire());
        assert!(!pg_pool_options(&config(false)).get_test_before_acquire());
        assert!(!pg_session_pool_options(&config(true)).get_test_before_acquire());
    }

    async fn pool_on_test_database(pool_pre_ping: bool) -> PgPool {
        let url = crate::broker::terminalization_matrix::migrated_database_url().await;
        let mut config = PostgresConfig::from_url(url.clone());
        config.pool_size = 1;
        config.max_overflow = 0;
        config.pool_pre_ping = pool_pre_ping;
        pg_pool_options(&config)
            .connect_with(pg_connect_options(&url, false).expect("parse test URL"))
            .await
            .expect("connect test pool")
    }

    async fn terminate(backend_pid: i32) {
        let url = crate::broker::terminalization_matrix::migrated_database_url().await;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect admin pool");
        let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
            .bind(backend_pid)
            .fetch_one(&admin)
            .await
            .expect("terminate the pooled backend");
        assert!(terminated);
        admin.close().await;
    }

    /// Python `test_idle_ping.py`: a pooled connection that died while it
    /// waited more than 1 s is found by the ping and replaced at checkout,
    /// so the statement runs on a new connection.
    #[tokio::test]
    #[serial]
    async fn a_dead_connection_idle_over_one_second_is_replaced_at_checkout() {
        let pool = pool_on_test_database(true).await;
        let first: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&pool)
            .await
            .expect("first statement");
        terminate(first).await;
        tokio::time::sleep(PRE_PING_AFTER_IDLE + Duration::from_millis(250)).await;

        let second: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&pool)
            .await
            .expect("the idle ping replaces the dead connection");

        assert_ne!(second, first);
        pool.close().await;
    }

    /// With `pool_pre_ping` off, nothing checks the connection at checkout:
    /// the first statement on the dead connection fails.
    #[tokio::test]
    #[serial]
    async fn without_the_idle_ping_the_first_statement_meets_the_dead_connection() {
        let pool = pool_on_test_database(false).await;
        let first: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&pool)
            .await
            .expect("first statement");
        terminate(first).await;
        tokio::time::sleep(PRE_PING_AFTER_IDLE + Duration::from_millis(250)).await;

        let failed = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
            .fetch_one(&pool)
            .await;

        assert!(failed.is_err(), "the dead connection serves the statement");
        pool.close().await;
    }
}

#[cfg(test)]
mod enqueue_task_options_tests {
    //! Python `test_enqueue_retry_limit.py` and `test_broker_postgres.py`
    //! (`test_malformed_task_options_raises`,
    //! `test_task_options_with_non_dict_retry_policy_is_refused`): enqueue
    //! stores a policy's limit, no policy stores 0, and options the worker
    //! would refuse are refused before any statement.
    use super::*;

    fn policy(max_retries: u32, extra: &str) -> String {
        let intervals = vec!["60"; max_retries as usize].join(", ");
        format!(
            r#"{{"retry_policy": {{"max_retries": {max_retries}, "intervals": [{intervals}],
                "backoff_strategy": "fixed", "jitter": false,
                "auto_retry_for": ["STORAGE_ERROR"]{extra}}},
              "good_until": null, "timeout_ms": null, "execution": "ISOLATED",
              "cancel_grace_ms": null}}"#
        )
    }

    #[test]
    fn no_options_and_no_policy_store_zero_retries() {
        for options in [
            None,
            Some(""),
            Some(r#"{"retry_policy": null, "execution": "ISOLATED"}"#),
            Some("{}"),
        ] {
            assert_eq!(
                enqueue_max_retries("t", options).expect("accepted"),
                0,
                "{options:?}"
            );
        }
    }

    #[test]
    fn a_valid_policy_stores_its_limit() {
        for limit in [1, 3, 20] {
            assert_eq!(
                enqueue_max_retries("t", Some(&policy(limit, ""))).expect("accepted"),
                limit as i32
            );
        }
    }

    #[test]
    fn options_the_worker_would_refuse_are_refused() {
        let over_limit = policy(21, "");
        let unknown_field = policy(2, r#", "extra": 1"#);
        for options in [
            "not json",
            "[]",
            r#"{"retry_policy": 5}"#,
            r#"{"retry_policy": {"max_retries": 2}}"#,
            over_limit.as_str(),
            unknown_field.as_str(),
        ] {
            assert!(
                matches!(
                    enqueue_max_retries("t", Some(options)),
                    Err(BrokerError::EnqueueContract(_))
                ),
                "{options}"
            );
        }
    }

    /// What the core writes for a task is what its enqueue accepts.
    #[test]
    fn the_core_written_options_are_accepted() {
        let options = crate::core::TaskOptions {
            task_name: "t".to_owned(),
            queue_name: None,
            good_until: None,
            auto_retry_for: Some(vec![crate::core::TaskErrorCode::User(
                crate::core::UserCode::parse("X").expect("X is a user code"),
            )]),
            retry_policy: Some(crate::core::RetryPolicy::exponential(30, 4, true).unwrap()),
            timeout_ms: Some(1_000),
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };
        let text = options.serialize_retry_options().expect("options are set");

        assert_eq!(enqueue_max_retries("t", Some(&text)).expect("accepted"), 4);
    }
}

#[cfg(test)]
mod client_family_tests {
    //! The client family at install and before the first write: a handle
    //! writes only into a database of its own family, and reads are not
    //! checked.
    use super::*;
    use serial_test::serial;

    async fn family_row(pool: &PgPool) -> Option<String> {
        sqlx::query_scalar("SELECT family FROM horsies_client_family")
            .fetch_optional(pool)
            .await
            .expect("read the family row")
    }

    /// The shared test database is installed for the Rust client.
    #[tokio::test]
    #[serial]
    async fn the_install_records_the_family_and_a_rust_handle_may_write() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        assert_eq!(family_row(&pool).await.as_deref(), Some("RUST"));

        let broker = PostgresBroker::from_pool(pool);
        broker
            .ensure_client_family_for_write()
            .await
            .expect("a Rust handle writes into a Rust database");
    }

    /// A handle of the other family is refused before it writes, and the
    /// refusal is not retryable. The check reads only.
    #[tokio::test]
    #[serial]
    async fn a_handle_of_the_other_family_is_refused_before_its_first_write() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let broker =
            PostgresBroker::from_pool(pool.clone()).with_client_family(ClientFamily::Python);

        let refused = broker.ensure_client_family_for_write().await.unwrap_err();

        assert!(matches!(
            refused,
            BrokerError::ClientFamilyMismatch {
                stored: crate::broker::client_family::RecordedFamily::Recorded(ClientFamily::Rust),
                own: ClientFamily::Python,
            }
        ));
        assert!(!refused.is_retryable());
        assert_eq!(family_row(&pool).await.as_deref(), Some("RUST"));
    }

    /// A handle with migrations off (observe only) runs no install, so its
    /// first send checks the family: on a database of the other family the
    /// send is refused before the insert, not retryable, and writes no row.
    #[tokio::test]
    #[serial]
    async fn a_send_of_a_handle_with_migrations_off_into_the_other_family_is_refused() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let broker = PostgresBroker::from_pool_observe_only(pool.clone())
            .with_client_family(ClientFamily::Python);
        let task_id = Uuid::new_v4();

        let refused = broker
            .enqueue(
                "family.send",
                "{}",
                "default",
                50,
                Some(Utc::now()),
                None,
                None,
                None,
                Some(task_id),
                None,
                None,
                EnqueueRetention::Default,
                None,
                SendKind::First,
            )
            .await
            .expect_err("a Python handle does not send into a Rust database");

        assert!(matches!(
            refused,
            BrokerError::ClientFamilyMismatch {
                own: ClientFamily::Python,
                ..
            }
        ));
        assert!(!refused.is_retryable());
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("count the rows of the refused send");
        assert_eq!(rows, 0);
    }
}

#[cfg(test)]
mod task_info_code_tests {
    //! Python's task info codes (`python/horsies/core/brokers/postgres.py:4889-4904`,
    //! `:4973-4977`): only a stored result that fails its check is
    //! `INVALID_JSON_PAYLOAD`.
    use super::*;
    use crate::db::StatementFailure;

    #[test]
    fn a_result_that_does_not_parse_is_invalid_json_payload() {
        let parse_error = serde_json::from_str::<serde_json::Value>("{").unwrap_err();

        let error = task_info_history_error(BrokerError::Serialization(parse_error));

        assert_eq!(error.code, BrokerErrorCode::InvalidJsonPayload);
        assert!(!error.retryable);
    }

    #[test]
    fn a_status_or_attempt_that_does_not_decode_is_task_info_query_failed() {
        for error in [
            BrokerError::InvalidStatus("BOGUS".to_owned()),
            BrokerError::HistoryReadContract("unknown task-attempt outcome".to_owned()),
        ] {
            let mapped = task_info_history_error(error);
            assert_eq!(mapped.code, BrokerErrorCode::TaskInfoQueryFailed);
            assert!(!mapped.retryable);
        }
    }

    #[test]
    fn a_database_failure_is_task_info_query_failed_with_the_driver_flag() {
        let mapped = task_info_history_error(BrokerError::Database(sqlx::Error::PoolTimedOut));

        assert_eq!(mapped.code, BrokerErrorCode::TaskInfoQueryFailed);
        assert!(mapped.retryable);
    }

    #[test]
    fn a_core_statement_failure_is_task_info_query_failed_with_its_transience() {
        let timed_out =
            task_info_history_error(BrokerError::Statement(StatementFailure::NotSent {
                statement: "select_task_attempts",
                cause: crate::db::NotSentCause::Checkout(crate::db::CheckoutFailure::TimedOut {
                    role: crate::db::PoolRole::Main,
                    waited: Duration::from_secs(30),
                }),
            }));
        assert_eq!(timed_out.code, BrokerErrorCode::TaskInfoQueryFailed);
        assert!(timed_out.retryable);

        let undecodable = task_info_history_error(BrokerError::RowDecode(RowDecodeError::NoRow {
            statement: "live_task_exists",
        }));
        assert_eq!(undecodable.code, BrokerErrorCode::TaskInfoQueryFailed);
        assert!(!undecodable.retryable);
    }

    /// A FAILED history row whose stored result is `payload`, with a
    /// matching digest and envelope version 1.
    fn history_detail_with_result(payload: &[u8]) -> HistoryTaskDetail {
        let at = chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 8, 7, 0, 0, 0).unwrap();
        HistoryTaskDetail {
            task_id: test_uuid("0192b3a4-0000-7000-8000-000000000001"),
            task_name: "stored_result".to_owned(),
            queue_name: "default".to_owned(),
            priority: 100,
            status: crate::core::TaskStatus::Failed,
            terminalization_kind: "FAIL_RUNNING".to_owned(),
            terminal_at: at,
            retention_class_key: "forever".to_owned(),
            enqueued_at: at,
            created_at: at,
            sent_at: Some(at),
            claimed_at: Some(at),
            started_at: Some(at),
            good_until: None,
            retry_count: 0,
            max_retries: 0,
            last_claimed_worker_id: Some("w-1".to_owned()),
            last_worker_hostname: Some("host-1".to_owned()),
            last_worker_pid: Some(100),
            result_envelope_version: crate::core::history::archive::versions::ARCHIVE_VERSION_1,
            result_codec: crate::core::history::archive::versions::JSON_UTF8_CODEC.to_owned(),
            result_content_type: crate::core::history::archive::versions::JSON_CONTENT_TYPE
                .to_owned(),
            result_payload: Some(payload.to_vec()),
            result_digest: Some(
                crate::core::history::archive::versions::archive_digest(payload).to_vec(),
            ),
            error_code: Some("BOOM".to_owned()),
            final_failed_reason: None,
            rerun_of_task_id: None,
            rerun_root_task_id: None,
            workflow_id: None,
            is_workflow_task: false,
            rerun_input_disposition: "NOT_REQUESTED".to_owned(),
            rerun_input_version: None,
            rerun_input_codec: None,
            rerun_input_content_type: None,
            rerun_input_digest: None,
            rerun_input_inline: None,
            attempts: Vec::new(),
        }
    }

    /// Asserts that the task info of a history row with `payload` as its
    /// stored result is `INVALID_JSON_PAYLOAD`, not retryable.
    fn assert_refused_as_invalid_json_payload(payload: &[u8]) {
        let shown = String::from_utf8_lossy(payload);
        let Err(refused) =
            history_task_info(history_detail_with_result(payload), true, false, false)
        else {
            panic!("a stored result of {shown:.40} must not read");
        };
        assert_eq!(
            refused.code,
            BrokerErrorCode::InvalidJsonPayload,
            "{shown:.40}"
        );
        assert!(!refused.retryable, "{shown:.40}");
    }

    fn history_attempt(
        attempt: i32,
        outcome: &str,
        will_retry: bool,
    ) -> crate::core::history::archive::attempts::AttemptRecord {
        let at = chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 8, 7, 0, 0, 0).unwrap();
        crate::core::history::archive::attempts::AttemptRecord::new(
            attempt,
            outcome,
            will_retry,
            at,
            at,
            None,
            None,
            None,
            Some("w-1".to_owned()),
            Some("host-1".to_owned()),
            Some(100),
            None,
        )
        .expect("a valid attempt record")
    }

    /// Python `test_broker_postgres.py::TestHistoryTaskInfoResultDecode::test_attempts_are_listed_newest_first`:
    /// the snapshot holds the attempts oldest first, and the task info lists
    /// them newest first, as the live read does.
    #[test]
    fn history_attempts_are_listed_newest_first() {
        let mut detail = history_detail_with_result(b"{}");
        detail.attempts = vec![
            history_attempt(1, "FAILED", true),
            history_attempt(2, "FAILED", false),
        ];

        let info = history_task_info(detail, false, false, true)
            .expect("a history row with two attempts reads");

        let attempts = info.attempts.expect("the read asked for the attempts");
        assert_eq!(
            attempts
                .iter()
                .map(|attempt| attempt.attempt)
                .collect::<Vec<i32>>(),
            vec![2, 1],
        );
    }

    /// Python `test_broker_postgres.py::TestHistoryTaskInfoResultDecode::test_stored_result_is_refused_as_invalid_json_payload`,
    /// the payloads that every build refuses: `NaN` and an empty payload.
    #[test]
    fn a_stored_result_outside_strict_json_is_invalid_json_payload() {
        for payload in [&b"{\"x\":NaN}"[..], &b""[..]] {
            assert_refused_as_invalid_json_payload(payload);
        }
    }

    /// Python `test_broker_postgres.py::TestHistoryTaskInfoResultDecode::test_stored_result_is_refused_as_invalid_json_payload`,
    /// the payloads past the strict read rule (`core::codec::check_json_bytes`):
    /// a float out of range and an integer of 5000 digits (refused in a
    /// default build by serde_json's range check, read as text under its
    /// `arbitrary_precision` feature), and a repeated key (read in every
    /// build).
    #[test]
    fn a_stored_result_past_the_strict_rule_is_invalid_json_payload_in_every_build() {
        let long_integer = format!("{{\"x\":{}}}", "1".repeat(5000));
        for payload in [
            &b"{\"x\":1e9999}"[..],
            long_integer.as_bytes(),
            &b"{\"x\":1,\"x\":2}"[..],
        ] {
            assert_refused_as_invalid_json_payload(payload);
        }
    }
}

mod connectivity;
pub use connectivity::{ConnectivityRefusal, ListenProbeFailure};

#[cfg(test)]
mod result_read_spec_tests;

#[cfg(test)]
mod ping_and_wait_tests;

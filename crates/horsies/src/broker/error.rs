/// Errors produced by broker operations.
///
/// Only infrastructure-level errors. Task-level outcomes (success, failure,
/// timeout, not-found, cancelled) are represented via `TaskResult<T>`.
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    /// Database connection or query error.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Migration error.
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    /// The migration chain is current, but the separately operated history
    /// cutover has not produced its validated completion attestation.
    #[error(
        "schema migrations are current but the offline task-history cutover is incomplete; \
         run the documented cutover stages through tighten and validation before starting \
         this fleet"
    )]
    IncompleteTaskHistoryCutover,

    /// The database migration ledger does not end at this binary's exact schema.
    #[error("schema version mismatch: binary requires {expected}, database reports {actual:?}")]
    SchemaVersionMismatch { expected: i64, actual: Option<i64> },

    /// JSON serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Unrecognized task status string from the database.
    #[error("invalid task status: {0}")]
    InvalidStatus(String),

    /// Connection configuration error (e.g. invalid URL for connect options).
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Enqueue conflict: same task_id but different payload (SHA mismatch).
    #[error("payload mismatch for task_id {task_id}: existing row has different enqueue_sha")]
    PayloadMismatch { task_id: Uuid },

    /// Enqueue observed a conflict, but the conflicting row disappeared before
    /// its `enqueue_sha` could be compared — payload identity cannot be proven.
    #[error(
        "task_id {task_id} conflict detected but row disappeared before verification \
         for {task_name}; cannot verify payload identity"
    )]
    EnqueueConflictUnverifiable { task_id: Uuid, task_name: String },

    /// The caller-provided idempotency key already owns a different command.
    #[error(
        "idempotency key for {task_name} is reserved by task {task_id} with a different command"
    )]
    IdempotencyKeyConflict { task_name: String, task_id: Uuid },

    /// Enqueue-time history facts could not be prepared or decoded.
    #[error("enqueue contract violation: {0}")]
    EnqueueContract(String),

    /// The broker's keyed-enqueue reservation window is outside `(0, 30d]`.
    #[error("invalid idempotency reservation window: {0}")]
    InvalidIdempotencyReservationWindow(String),

    /// The shared LISTEN/NOTIFY listener was closed or its background task stopped.
    #[error("shared listener closed")]
    ListenerClosed,

    /// A terminalization operation violated its wire contract: an undecodable
    /// outcome row, a wrong row count, or a broken ordinal set. Infrastructure
    /// failure, never a task outcome.
    #[error("terminalization contract violation: {0}")]
    TerminalizationContract(String),

    /// A staged task-history row or archive envelope violated its frozen
    /// read contract.
    #[error("task-history read contract violation: {0}")]
    HistoryReadContract(String),
}

impl BrokerError {
    /// Whether this error is PostgreSQL's nonblocking lock refusal.
    pub fn is_lock_not_available(&self) -> bool {
        match self {
            Self::Database(sqlx::Error::Database(error)) => {
                error.code().is_some_and(|code| code.as_ref() == "55P03")
            }
            _ => false,
        }
    }

    /// Whether this error is transient and the operation can be retried.
    ///
    /// Retryable: `Database` (when the underlying sqlx error is retryable),
    /// `ConnectionFailed`, `ListenerClosed`.
    ///
    /// Non-retryable: `Migration`, `IncompleteTaskHistoryCutover`,
    /// `Serialization`, `InvalidStatus`.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Database(e) => is_retryable_sqlx_error(e),
            Self::ConnectionFailed(_) | Self::ListenerClosed => true,
            Self::Migration(_)
            | Self::IncompleteTaskHistoryCutover
            | Self::SchemaVersionMismatch { .. }
            | Self::Serialization(_)
            | Self::InvalidStatus(_)
            | Self::PayloadMismatch { .. }
            | Self::EnqueueConflictUnverifiable { .. }
            | Self::IdempotencyKeyConflict { .. }
            | Self::EnqueueContract(_)
            | Self::InvalidIdempotencyReservationWindow(_)
            | Self::TerminalizationContract(_)
            | Self::HistoryReadContract(_) => false,
        }
    }
}

/// Check whether a raw `sqlx::Error` represents a transient failure.
///
/// Retryable conditions:
/// - I/O errors (network failures, connection drops)
/// - Pool timeout / pool closed
/// - Worker crashed
/// - Protocol / TLS errors
/// - PostgreSQL error codes: 08xxx (connection exceptions), 40P01
///   (deadlock), 55P03 (nonblocking lock refusal), and 57P01–57P03
///   (admin shutdown, crash shutdown, cannot connect now)
pub fn is_retryable_sqlx_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Io(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::Protocol(_)
        | sqlx::Error::Tls(_) => true,
        sqlx::Error::Database(db_err) => {
            if let Some(code) = db_err.code() {
                let code = code.as_ref();
                // 08xxx = connection exception class
                // 40P01 = deadlock_detected (one txn is rolled back; safe to retry)
                // 55P03 = lock_not_available (NOWAIT refusal; safe to retry)
                // 57P01 = admin_shutdown
                // 57P02 = crash_shutdown
                // 57P03 = cannot_connect_now
                code.starts_with("08")
                    || code == "40P01"
                    || code == "55P03"
                    || code == "57P01"
                    || code == "57P02"
                    || code == "57P03"
            } else {
                false
            }
        }
        _ => false,
    }
}

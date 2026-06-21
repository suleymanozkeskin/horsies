/// Errors produced by broker operations.
///
/// Only infrastructure-level errors. Task-level outcomes (success, failure,
/// timeout, not-found, cancelled) are represented via `TaskResult<T>`.
#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    /// Database connection or query error.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Migration error.
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

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
    PayloadMismatch { task_id: String },

    /// Enqueue observed a conflict, but the conflicting row disappeared before
    /// its `enqueue_sha` could be compared — payload identity cannot be proven.
    #[error(
        "task_id {task_id} conflict detected but row disappeared before verification \
         for {task_name}; cannot verify payload identity"
    )]
    EnqueueConflictUnverifiable { task_id: String, task_name: String },

    /// The shared LISTEN/NOTIFY listener was closed or its background task stopped.
    #[error("shared listener closed")]
    ListenerClosed,
}

impl BrokerError {
    /// Whether this error is transient and the operation can be retried.
    ///
    /// Retryable: `Database` (when the underlying sqlx error is retryable),
    /// `ConnectionFailed`, `ListenerClosed`.
    ///
    /// Non-retryable: `Migration`, `Serialization`, `InvalidStatus`.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Database(e) => is_retryable_sqlx_error(e),
            Self::ConnectionFailed(_) | Self::ListenerClosed => true,
            Self::Migration(_)
            | Self::Serialization(_)
            | Self::InvalidStatus(_)
            | Self::PayloadMismatch { .. }
            | Self::EnqueueConflictUnverifiable { .. } => false,
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
/// - PostgreSQL error codes: 08xxx (connection exceptions), 57P01–57P03
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
                // 57P01 = admin_shutdown
                // 57P02 = crash_shutdown
                // 57P03 = cannot_connect_now
                code.starts_with("08")
                    || code == "40P01"
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

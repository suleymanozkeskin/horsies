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

    /// Existing workflow nodes contain a status outside the typed node enum.
    #[error("migration refused: workflow nodes contain an unsupported status; repair these rows before retrying")]
    InvalidWorkflowNodeStatus,

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

    /// A connection or delivery check failed in a way that can clear.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// The connection settings were refused before any connection was
    /// opened: a URL scheme other than `postgresql://` or `postgres://`, a
    /// URL the driver cannot parse, or a config rule. The same settings are
    /// refused again, so it is not retryable.
    #[error("invalid connection settings: {0}")]
    InvalidConnectionSettings(#[from] crate::core::config::PostgresConfigError),

    /// Enqueue conflict: same task_id but another command or another send call
    /// (command fingerprint or `sent_at` differs).
    #[error(
        "payload mismatch for task_id {task_id}: existing row has a different command or sent_at"
    )]
    PayloadMismatch { task_id: Uuid },

    /// Enqueue observed a conflict, but the conflicting row disappeared before
    /// its fingerprint and `sent_at` could be compared — payload identity cannot
    /// be proven.
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
    /// Retryable: `Database` when the driver error is retryable (see
    /// [`is_retryable_sqlx_error`]), `ConnectionFailed`, `ListenerClosed`.
    /// Every other variant is a refusal or a broken contract, which the same
    /// call meets again.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Database(e) => is_retryable_sqlx_error(e),
            Self::ConnectionFailed(_) | Self::ListenerClosed => true,
            Self::Migration(_)
            | Self::InvalidWorkflowNodeStatus
            | Self::IncompleteTaskHistoryCutover
            | Self::SchemaVersionMismatch { .. }
            | Self::InvalidConnectionSettings(_)
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

/// SQLSTATE classes whose condition may clear, so a repeat of the operation
/// can succeed. The set is Python's rule (`core/utils/db.py`,
/// `is_retryable_connection_error`: psycopg's `OperationalError` family)
/// written as classes: 08 connection exception, 40 transaction rollback
/// (40001 serialization failure, 40P01 deadlock), 53 insufficient resources
/// (53300 too many connections), 55 object not in prerequisite state (55P03
/// lock not available), 57 operator intervention (57014 query cancelled by a
/// statement timeout, 57P01 to 57P03 shutdown).
pub const RETRYABLE_SQLSTATE_CLASSES: [&str; 5] = ["08", "40", "53", "55", "57"];

/// Whether a SQLSTATE belongs to a class of [`RETRYABLE_SQLSTATE_CLASSES`].
pub fn is_retryable_sqlstate(code: &str) -> bool {
    match code.get(..2) {
        Some(class) => RETRYABLE_SQLSTATE_CLASSES.contains(&class),
        None => false,
    }
}

/// Whether a raw `sqlx::Error` is a transient failure.
///
/// Retryable:
/// - a database error whose SQLSTATE is in [`RETRYABLE_SQLSTATE_CLASSES`];
/// - a lost or failed connection: I/O, TLS, protocol, a crashed pool
///   worker, a pool checkout that timed out.
///
/// Not retryable: `PoolClosed`. The handle was closed on purpose, and a
/// repeat on the same closed pool meets the same refusal. Every other driver
/// error (a decode error, a missing row, a configuration error) is not
/// retryable either.
pub fn is_retryable_sqlx_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Io(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::Protocol(_)
        | sqlx::Error::Tls(_) => true,
        sqlx::Error::Database(db_err) => match db_err.code() {
            Some(code) => is_retryable_sqlstate(code.as_ref()),
            None => false,
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;

    /// A database error that carries only a SQLSTATE, as the server sends it.
    #[derive(Debug)]
    struct ServerError {
        code: &'static str,
    }

    impl std::fmt::Display for ServerError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "server error {}", self.code)
        }
    }

    impl std::error::Error for ServerError {}

    impl sqlx::error::DatabaseError for ServerError {
        fn message(&self) -> &str {
            "server error"
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.code))
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn server_error(code: &'static str) -> sqlx::Error {
        sqlx::Error::Database(Box::new(ServerError { code }))
    }

    /// Python's rule: the `OperationalError` family of psycopg is retryable.
    /// It holds serialization failure, too many connections and a statement
    /// timeout, which the earlier Rust list left out.
    #[test]
    fn every_sqlstate_of_the_five_classes_is_retryable() {
        for code in [
            "08000", "08006", "08P01", "40001", "40P01", "53000", "53300", "55P03", "55006",
            "57014", "57P01", "57P02", "57P03",
        ] {
            assert!(
                is_retryable_sqlx_error(&server_error(code)),
                "{code} must be retryable"
            );
        }
    }

    #[test]
    fn a_sqlstate_outside_the_five_classes_is_not_retryable() {
        for code in [
            "23505", "23514", "22P02", "42P01", "42883", "P0001", "XX000", "58030", "54000",
        ] {
            assert!(
                !is_retryable_sqlx_error(&server_error(code)),
                "{code} must not be retryable"
            );
        }
    }

    #[test]
    fn a_sqlstate_shorter_than_its_class_is_not_retryable() {
        assert!(!is_retryable_sqlstate(""));
        assert!(!is_retryable_sqlstate("0"));
    }

    #[test]
    fn a_lost_connection_is_retryable() {
        let io = sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert!(is_retryable_sqlx_error(&io));
        assert!(is_retryable_sqlx_error(&sqlx::Error::PoolTimedOut));
        assert!(is_retryable_sqlx_error(&sqlx::Error::WorkerCrashed));
        assert!(is_retryable_sqlx_error(&sqlx::Error::Protocol(
            "unexpected message".to_owned()
        )));
    }

    /// A closed pool was closed on purpose; a repeat on it meets the same
    /// refusal.
    #[test]
    fn a_closed_pool_is_not_retryable() {
        assert!(!is_retryable_sqlx_error(&sqlx::Error::PoolClosed));
        assert!(!BrokerError::Database(sqlx::Error::PoolClosed).is_retryable());
    }

    #[test]
    fn refused_connection_settings_are_not_retryable() {
        let error = BrokerError::InvalidConnectionSettings(
            crate::core::config::PostgresConfigError::InvalidUrlScheme("mysql".to_owned()),
        );
        assert!(!error.is_retryable());
    }
}

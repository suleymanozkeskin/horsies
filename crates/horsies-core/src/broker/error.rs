/// Errors produced by broker operations.
///
/// Only infrastructure-level errors. Task-level outcomes (success, failure,
/// timeout, not-found, cancelled) are represented via `TaskResult<T>`.
use uuid::Uuid;

use crate::broker::core_rows::RowDecodeError;
use crate::broker::pools::PoolSettingsError;
use crate::core::codec::JsonReadError;
use crate::core::history::enqueue::{EnqueuePreparationError, SendPart};
use crate::db::{StatementFailure, Transience};

/// Why the core refused a send before any statement (Python's
/// `BrokerInvalidInput` and `BrokerInvalidTaskOptions`). Nothing was
/// written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct EnqueueRefusal {
    /// The part of the send that the rule refused.
    pub part: SendPart,
    /// What was refused and why.
    pub message: String,
    pub cause: EnqueueRefusalCause,
}

/// What a send refusal carries besides its message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueRefusalCause {
    /// The refused text does not read as JSON: the reader's error text
    /// (Python passes its `StrictJsonError` as the `exception`).
    JsonUnreadable(String),
    /// A rule refused a value that reads (Python passes `NoCause()`).
    RuleRefused,
}

impl EnqueueRefusal {
    /// A refusal of `part` by a rule, with no cause besides `message`.
    pub fn by_rule(part: SendPart, message: String) -> Self {
        Self {
            part,
            message,
            cause: EnqueueRefusalCause::RuleRefused,
        }
    }

    /// A refusal of `part` whose text does not read as strict JSON.
    pub fn unreadable_json(part: SendPart, message: String, error: &JsonReadError) -> Self {
        Self {
            part,
            message,
            cause: EnqueueRefusalCause::JsonUnreadable(error.to_string()),
        }
    }
}

impl From<EnqueuePreparationError> for EnqueueRefusal {
    /// The part and the cause of a preparation error: a JSON text that does
    /// not read keeps the reader's error; the options' shape and execution
    /// class are the task options; the fingerprint and the idempotency key
    /// are the input.
    fn from(error: EnqueuePreparationError) -> Self {
        let message = error.to_string();
        let (part, cause) = match &error {
            EnqueuePreparationError::StrictJson { field, source } => (
                *field,
                EnqueueRefusalCause::JsonUnreadable(source.to_string()),
            ),
            EnqueuePreparationError::Json { field, source } => (
                *field,
                EnqueueRefusalCause::JsonUnreadable(source.to_string()),
            ),
            EnqueuePreparationError::OptionsIntegerOutOfRange
            | EnqueuePreparationError::InvalidTaskOptions
            | EnqueuePreparationError::InvalidExecution(_) => {
                (SendPart::TaskOptions, EnqueueRefusalCause::RuleRefused)
            }
            EnqueuePreparationError::Fingerprint(_) | EnqueuePreparationError::Idempotency(_) => {
                (SendPart::Input, EnqueueRefusalCause::RuleRefused)
            }
        };
        Self {
            part,
            message,
            cause,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    /// Database connection or query error.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

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

    /// JSON serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Unrecognized task status string from the database.
    #[error("invalid task status: {0}")]
    InvalidStatus(String),

    /// A live task row's `execution` column holds a value outside
    /// `ISOLATED`, `SHARED_ASYNC` and `SHARED_THREAD`.
    #[error("invalid execution class: {0}")]
    InvalidExecutionClass(String),

    /// A connection or delivery check failed in a way that can clear.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// The connection settings were refused before any connection was
    /// opened: a URL scheme other than `postgresql://` or `postgres://`, a
    /// URL the driver cannot parse, or a config rule. The same settings are
    /// refused again, so it is not retryable.
    #[error("invalid connection settings: {0}")]
    InvalidConnectionSettings(#[from] crate::core::config::PostgresConfigError),

    /// The task id exists with a different command, or is bound to a
    /// different idempotency key. Nothing was written.
    #[error(
        "payload mismatch for task_id {task_id}: the existing task has a different command \
         or idempotency key"
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

    /// The core refused a send before any statement: its input, a fact of
    /// the call, or its task options break a rule. Nothing was written. Not
    /// retryable: the same send is refused again.
    #[error("enqueue contract violation: {0}")]
    EnqueueContract(EnqueueRefusal),

    /// The broker's keyed-enqueue reservation window is outside `(0, 30d]`.
    #[error("invalid idempotency reservation window: {0}")]
    InvalidIdempotencyReservationWindow(String),

    /// The database records another client family, or none. Nothing was
    /// written. Not retryable: the database stays of its family.
    #[error(
        "this database was installed by {stored}; this is the {} client, which writes only \
         into a database of its own family",
        own.as_str()
    )]
    ClientFamilyMismatch {
        stored: crate::broker::client_family::RecordedFamily,
        own: crate::broker::client_family::ClientFamily,
    },

    /// The schema install refused the database: it holds something this
    /// build does not install into (a 0.x schema, unknown horsies tables, a
    /// newer, foreign, failed or changed migration). Nothing was changed. Not
    /// retryable: the same database is refused again.
    #[error("schema install refused: {0}")]
    SchemaRefused(crate::broker::migrations::SchemaRefusal),

    /// The schema install lock could not be used.
    #[error("schema install lock: {0}")]
    InstallLock(InstallLockFailure),

    /// The broker's listener stopped while a wait used it.
    #[error("the broker's listener stopped")]
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

    /// A claim-pass argument is outside the range that the claim function
    /// takes. It is refused, never clamped.
    #[error("invalid claim argument: {0}")]
    InvalidClaimArgument(String),

    /// A row of the claim, the start, the release, the settle of claimed
    /// workflow tasks or the stale claim requeue breaks its function's row
    /// contract (`broker::claims`, `broker::workflow_claim_settle`,
    /// `broker::stale_recovery`). Not retryable: the same call meets it
    /// again.
    #[error("claim contract violation: {0}")]
    ClaimContract(String),

    /// A statement on a core pool failed with one of the statement layer's
    /// three outcomes. Retryable when its cause may clear
    /// ([`Transience::Transient`]); a closed pool and a statement-pooled URL
    /// are not.
    #[error("database error: {0}")]
    Statement(#[from] StatementFailure),

    /// The connection settings of a core pool were refused before any
    /// connection opened. Not retryable.
    #[error("connection settings refused: {0}")]
    PoolSettings(#[from] PoolSettingsError),

    /// A row of a core statement does not have the shape its reader
    /// requires. Not retryable.
    #[error("{0}")]
    RowDecode(#[from] RowDecodeError),
}

impl BrokerError {
    /// Whether this error is PostgreSQL's nonblocking lock refusal.
    pub fn is_lock_not_available(&self) -> bool {
        match self {
            Self::Database(sqlx::Error::Database(error)) => {
                error.code().is_some_and(|code| code.as_ref() == "55P03")
            }
            Self::Statement(StatementFailure::StatementRefused { cause, .. }) => {
                cause.server_error().code().code() == "55P03"
            }
            _ => false,
        }
    }

    /// Whether this error is transient and the operation can be retried.
    ///
    /// Retryable: `Database` when the driver error is retryable (see
    /// [`is_retryable_sqlx_error`]), `Statement` when its cause is transient
    /// (see [`is_retryable_statement_failure`]), `ConnectionFailed`,
    /// `ListenerClosed`. Every other variant is a refusal or a broken
    /// contract, which the same call meets again.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Database(e) => is_retryable_sqlx_error(e),
            Self::Statement(failure) => is_retryable_statement_failure(failure),
            Self::ConnectionFailed(_) | Self::ListenerClosed => true,
            Self::InstallLock(failure) => failure.is_retryable(),
            Self::SchemaRefused(_)
            | Self::InvalidWorkflowNodeStatus
            | Self::IncompleteTaskHistoryCutover
            | Self::InvalidConnectionSettings(_)
            | Self::ClientFamilyMismatch { .. }
            | Self::Serialization(_)
            | Self::InvalidStatus(_)
            | Self::InvalidExecutionClass(_)
            | Self::PayloadMismatch { .. }
            | Self::EnqueueConflictUnverifiable { .. }
            | Self::IdempotencyKeyConflict { .. }
            | Self::EnqueueContract(_)
            | Self::InvalidIdempotencyReservationWindow(_)
            | Self::TerminalizationContract(_)
            | Self::HistoryReadContract(_)
            | Self::InvalidClaimArgument(_)
            | Self::ClaimContract(_)
            | Self::PoolSettings(_)
            | Self::RowDecode(_) => false,
        }
    }
}

/// Whether a failed core statement can be retried: its cause may clear.
///
/// The rule is the statement layer's [`Transience`]: SQLSTATE classes 08,
/// 40, 53, 55 and 57 ([`crate::db::TRANSIENT_SQLSTATE_CLASSES`], the same
/// set as [`RETRYABLE_SQLSTATE_CLASSES`]) and lost connections are
/// transient; a closed pool and a statement-pooled URL are not. For
/// `OutcomeUnknown` the flag says nothing about whether a replay is safe.
pub fn is_retryable_statement_failure(failure: &StatementFailure) -> bool {
    match failure.transience() {
        Transience::Transient => true,
        Transience::Permanent => false,
    }
}

/// Why the schema install lock could not be used (Python
/// `python/horsies/core/brokers/postgres.py:1466-1525`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InstallLockFailure {
    /// Another installer held the lock for the whole wait bound. Nothing was
    /// installed by this call. Retryable: the other install ends.
    #[error("another installer held the lock for {} s", waited.as_secs())]
    WaitBoundReached { waited: std::time::Duration },
    /// The unlock did not release a lock that this connection held: the
    /// session lost the lock during the install. The install may or may not
    /// have completed. Not retryable.
    #[error("the lock was no longer held at the unlock; ownership was lost")]
    OwnershipLost,
}

impl InstallLockFailure {
    /// Whether a later install can succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::WaitBoundReached { .. } => true,
            Self::OwnershipLost => false,
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

    fn not_sent(cause: crate::db::NotSentCause) -> BrokerError {
        BrokerError::Statement(StatementFailure::NotSent {
            statement: "test_statement",
            cause,
        })
    }

    /// The statement layer and the sqlx rule read the same SQLSTATE classes.
    #[test]
    fn the_statement_layer_retries_the_same_sqlstate_classes() {
        assert_eq!(
            RETRYABLE_SQLSTATE_CLASSES,
            crate::db::TRANSIENT_SQLSTATE_CLASSES
        );
    }

    #[test]
    fn a_core_statement_that_may_clear_is_retryable() {
        use crate::db::{CheckoutFailure, NotSentCause, PoolRole};
        assert!(not_sent(NotSentCause::Checkout(CheckoutFailure::TimedOut {
            role: PoolRole::Main,
            waited: std::time::Duration::from_secs(30),
        }))
        .is_retryable());
        assert!(not_sent(NotSentCause::ConnectionClosed).is_retryable());
    }

    /// A closed pool and a statement-pooled URL meet the same refusal again.
    #[test]
    fn a_closed_core_pool_and_a_statement_pooled_url_are_not_retryable() {
        use crate::db::{CheckoutFailure, MaskedUrl, NotSentCause, PoolRole};
        assert!(
            !not_sent(NotSentCause::Checkout(CheckoutFailure::PoolClosed {
                role: PoolRole::Main,
            }))
            .is_retryable()
        );
        assert!(!not_sent(NotSentCause::Checkout(
            CheckoutFailure::StatementPooledUrl {
                masked_url: MaskedUrl::for_tests("postgresql://horsies:***@pgbouncer:6432/app"),
            }
        ))
        .is_retryable());
    }

    #[test]
    fn a_row_of_the_wrong_shape_is_not_retryable() {
        let error = BrokerError::RowDecode(RowDecodeError::NoRow {
            statement: "test_statement",
        });
        assert!(!error.is_retryable());
    }

    #[test]
    fn refused_connection_settings_are_not_retryable() {
        let error = BrokerError::InvalidConnectionSettings(
            crate::core::config::PostgresConfigError::InvalidUrlScheme("mysql".to_owned()),
        );
        assert!(!error.is_retryable());
    }
}

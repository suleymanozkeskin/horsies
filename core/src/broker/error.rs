/// Errors produced by broker operations.
///
/// Only infrastructure-level errors. Task-level outcomes (success, failure,
/// timeout, not-found, cancelled) are represented via `TaskResult<T>`.
use uuid::Uuid;

use crate::broker::core_rows::RowDecodeError;
use crate::broker::pools::PoolSettingsError;
use crate::core::codec::JsonReadError;
use crate::core::config::RetentionClassKeyRefused;
use crate::core::history::enqueue::{EnqueuePreparationError, SendPart};
use crate::core::history::errors::HistoryError;
use crate::core::history::identity::fingerprint::FingerprintError;
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
    /// A rule of the version-1 command refused a value: the rule's text
    /// (Python's `EnqueueCommandV1` raises it as a `ValueError`, which the
    /// broker passes as the `exception`).
    CommandRuleRefused(String),
    /// A rule of the scoped idempotency key refused the key or the task
    /// name: the rule's text (Python's `ScopedIdempotencyKey` raises it as a
    /// `ValueError`, which the broker passes as the `exception`).
    KeyRuleRefused(String),
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

    /// The refusal of an empty retention class key: the rule of the
    /// version-1 command, as the command refuses it. A client that takes the
    /// class key as text checks it with this before it reads the key as a
    /// class ([`crate::EnqueueRetention::from_class_key`]); a key that is not
    /// empty and that no class derives is not this refusal. Pure.
    pub fn empty_retention_class_key() -> Self {
        Self::from(EnqueuePreparationError::Fingerprint(
            FingerprintError::EmptyRetentionClass,
        ))
    }
}

impl From<EnqueuePreparationError> for EnqueueRefusal {
    /// The part and the cause of a preparation error: a JSON text that does
    /// not read keeps the reader's error; the options' shape and execution
    /// class are the task options; the fingerprint and the idempotency key
    /// are the input, and each keeps the text of the rule that refused it.
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
            EnqueuePreparationError::Fingerprint(rule) => (
                SendPart::Input,
                EnqueueRefusalCause::CommandRuleRefused(rule.to_string()),
            ),
            EnqueuePreparationError::Idempotency(rule) => (
                SendPart::Input,
                EnqueueRefusalCause::KeyRuleRefused(rule.to_string()),
            ),
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

    /// The retention class key of a send is not empty, and no retention
    /// class derives it ([`crate::EnqueueRetention::from_class_key`]). A
    /// client that takes the key as text refuses it before any statement:
    /// nothing was written. Not retryable: the same key is refused again.
    /// An empty key is [`EnqueueRefusal::empty_retention_class_key`].
    #[error(transparent)]
    RetentionClassKey(RetentionClassKeyRefused),

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

    /// The family row of the database holds a value that is not a client
    /// family: corrupt stored data. The install changed nothing, and no
    /// write ran. Not retryable: the same row is refused again until it is
    /// repaired.
    #[error("{0}")]
    UnknownStoredFamily(crate::broker::client_family::UnknownStoredFamily),

    /// The schema install refused the database: it holds something this
    /// build does not install into (a 0.x schema, unknown horsies tables, a
    /// newer, foreign, failed or changed migration). Nothing was changed. Not
    /// retryable: the same database is refused again.
    #[error("schema install refused: {0}")]
    SchemaRefused(crate::broker::migrations::SchemaRefusal),

    /// The schema install lock could not be used.
    #[error("schema install lock: {0}")]
    InstallLock(InstallLockFailure),

    /// The coverage pass of the schema install had no outcome: a probe, the
    /// coverage gate, the class registration or the reader publication
    /// failed, or a row broke its function's contract
    /// ([`PostgresBroker::ensure_schema_initialized`](crate::broker::PostgresBroker::ensure_schema_initialized)).
    /// The migrations committed, and the leaf transactions that committed
    /// before the failure stay. Retryable by the history error's own rule.
    #[error("the coverage pass of the schema install failed: {0}")]
    InstallCoverage(HistoryError),

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
    /// workflow tasks, the stale claim requeue or the node read of a task
    /// breaks its function's row contract (`broker::claims`,
    /// `broker::workflow_claim_settle`, `broker::stale_recovery`,
    /// `broker::task_workflow_node`). Not retryable: the same call meets it
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

    /// The client did not run the operation: the core's process runtime
    /// refused it. Nothing ran. Retryable when the OS refused the runtime's
    /// threads (a later use starts the runtime again); a fork of the
    /// process that started the runtime is not. A client's blocking form
    /// produces it before it calls the core.
    #[error("{0}")]
    ProcessRuntime(crate::process_runtime::ProcessRuntimeRefusal),
}

impl BrokerError {
    /// Whether this error is PostgreSQL's nonblocking lock refusal.
    pub fn is_lock_not_available(&self) -> bool {
        match self {
            Self::Statement(StatementFailure::StatementRefused { cause, .. }) => {
                cause.server_error().code().code() == "55P03"
            }
            _ => false,
        }
    }

    /// Whether this error is transient and the operation can be retried.
    ///
    /// Retryable: `Statement` when its cause is transient (see
    /// [`is_retryable_statement_failure`]), `ConnectionFailed`,
    /// `ListenerClosed`, and `ProcessRuntime`, `InstallLock` and
    /// `InstallCoverage` by their own rules. Every other variant is a refusal or a broken contract, which
    /// the same call meets again.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Statement(failure) => is_retryable_statement_failure(failure),
            Self::ConnectionFailed(_) | Self::ListenerClosed => true,
            Self::ProcessRuntime(refusal) => match refusal {
                crate::process_runtime::ProcessRuntimeRefusal::StartFailed { .. } => true,
                crate::process_runtime::ProcessRuntimeRefusal::ForkedAfterStart { .. } => false,
            },
            Self::InstallLock(failure) => failure.is_retryable(),
            Self::InstallCoverage(error) => error.is_retryable(),
            Self::SchemaRefused(_)
            | Self::InvalidWorkflowNodeStatus
            | Self::IncompleteTaskHistoryCutover
            | Self::InvalidConnectionSettings(_)
            | Self::ClientFamilyMismatch { .. }
            | Self::UnknownStoredFamily(_)
            | Self::Serialization(_)
            | Self::InvalidStatus(_)
            | Self::InvalidExecutionClass(_)
            | Self::PayloadMismatch { .. }
            | Self::EnqueueConflictUnverifiable { .. }
            | Self::IdempotencyKeyConflict { .. }
            | Self::EnqueueContract(_)
            | Self::RetentionClassKey(_)
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
/// 40, 53, 55 and 57 ([`crate::db::TRANSIENT_SQLSTATE_CLASSES`]) and lost
/// connections are
/// transient; a closed pool and a statement-pooled URL are not. For
/// `OutcomeUnknown` the flag says nothing about whether a replay is safe.
pub fn is_retryable_statement_failure(failure: &StatementFailure) -> bool {
    match failure.transience() {
        Transience::Transient => true,
        Transience::Permanent => false,
    }
}

/// Why the schema install lock could not be used (Python
/// `clients/python/horsies/core/brokers/postgres.py:1466-1525`).
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

#[cfg(test)]
mod tests {
    use tokio_postgres::error::DbError;

    use super::*;
    use crate::db::{CheckoutFailure, NotSentCause, PoolRole, RefusalCause};

    /// The server error of a statement that raises SQLSTATE `code`.
    async fn server_error(code: &str) -> DbError {
        crate::test_support::raised_server_error(code, "test refusal").await
    }

    fn refused(error: DbError) -> BrokerError {
        BrokerError::Statement(StatementFailure::StatementRefused {
            statement: "test_statement",
            cause: RefusalCause::Server(Box::new(error)),
        })
    }

    /// Python `test_broker_error_variants.py::TestProcessBoundaries::test_reaper_sqlstate_reads_the_driver_cause`
    /// and `::test_reaper_sqlstate_of_an_error_without_a_cause_is_none`: the
    /// reaper reads the SQLSTATE of the server error behind a refused
    /// statement; 55P03 is the lock refusal (the orphan audit reports it as
    /// `refused`, `worker::recovery`). Another SQLSTATE, and an error with no
    /// server cause, are not.
    #[tokio::test]
    async fn the_lock_refusal_is_read_from_the_server_cause() {
        assert!(refused(server_error("55P03").await).is_lock_not_available());
        assert!(!refused(server_error("40001").await).is_lock_not_available());
        assert!(
            !not_sent(NotSentCause::Checkout(CheckoutFailure::PoolClosed {
                role: PoolRole::Main,
            }))
            .is_lock_not_available()
        );
        assert!(!BrokerError::ListenerClosed.is_lock_not_available());
    }

    /// Python's rule: the `OperationalError` family of psycopg is retryable.
    /// It holds serialization failure, too many connections and a statement
    /// timeout. A statement that the server refuses with one of them is
    /// retryable.
    #[tokio::test]
    async fn every_sqlstate_of_the_five_classes_is_retryable() {
        for code in [
            "08000", "08006", "08P01", "40001", "40P01", "53000", "53300", "55P03", "55006",
            "57014", "57P01", "57P02", "57P03",
        ] {
            assert!(
                refused(server_error(code).await).is_retryable(),
                "{code} must be retryable"
            );
        }
    }

    #[tokio::test]
    async fn a_sqlstate_outside_the_five_classes_is_not_retryable() {
        for code in [
            "23505", "23514", "22P02", "42P01", "42883", "P0001", "XX000", "58030", "54000",
        ] {
            assert!(
                !refused(server_error(code).await).is_retryable(),
                "{code} must not be retryable"
            );
        }
    }

    fn not_sent(cause: NotSentCause) -> BrokerError {
        BrokerError::Statement(StatementFailure::NotSent {
            statement: "test_statement",
            cause,
        })
    }

    #[test]
    fn a_core_statement_that_may_clear_is_retryable() {
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
        use crate::db::MaskedUrl;
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

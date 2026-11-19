//! Failure outcomes of the statement layer and their SQLSTATE classification.
//!
//! A failed statement has exactly one of three outcomes:
//! - [`StatementFailure::NotSent`]: nothing reached the server.
//! - [`StatementFailure::StatementRefused`]: the server refused the statement
//!   with an ERROR. Nothing of the statement committed.
//! - [`StatementFailure::OutcomeUnknown`]: the statement went to the server and
//!   the reply was lost. The statement may have committed.
//!
//! Each failure also reports a [`Transience`]. `Transient` means the condition
//! may clear. It does not mean that a replay is safe: after `OutcomeUnknown`
//! the operation's own catalog decides the replay.

use std::error::Error as _;
use std::fmt;
use std::time::Duration;

use tokio_postgres::error::{DbError, Severity, SqlState};

use super::pool::PoolRole;
use super::url::MaskedUrl;
use crate::process_runtime::ProcessRuntimeRefusal;

/// SQLSTATE classes whose errors may clear without a change by the user:
/// connection exception (08), transaction rollback (40), insufficient
/// resources (53), object not in prerequisite state (55) and operator
/// intervention (57).
pub const TRANSIENT_SQLSTATE_CLASSES: [&str; 5] = ["08", "40", "53", "55", "57"];

/// The deepest `source()` chain walked to find a TLS certificate error.
const MAX_ERROR_CHAIN_DEPTH: usize = 8;

/// Whether the cause of a failure may clear on its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transience {
    /// The condition may clear; a later attempt may succeed.
    Transient,
    /// The condition stays until the configuration, the schema or the input
    /// changes.
    Permanent,
}

/// The transience of a server error, by its SQLSTATE class.
pub fn sqlstate_transience(code: &SqlState) -> Transience {
    sqlstate_code_transience(code.code())
}

/// The transience of a five-character SQLSTATE code, by its class (the first
/// two characters). A code shorter than two characters is permanent.
pub fn sqlstate_code_transience(code: &str) -> Transience {
    match code.get(..2) {
        Some(class) if TRANSIENT_SQLSTATE_CLASSES.contains(&class) => Transience::Transient,
        Some(_) | None => Transience::Permanent,
    }
}

/// What a server error ends: the statement only, or the whole session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorScope {
    /// ERROR and lower: the statement's transaction ended; the session lives.
    Statement,
    /// FATAL or PANIC: the server closes the session after this message.
    Session,
}

/// The scope of a server error, from its non-localized severity. A server
/// older than 9.6 sends no such field; then the localized severity text is
/// read.
pub fn error_scope(error: &DbError) -> ErrorScope {
    match error.parsed_severity() {
        Some(Severity::Fatal | Severity::Panic) => ErrorScope::Session,
        Some(
            Severity::Error
            | Severity::Warning
            | Severity::Notice
            | Severity::Debug
            | Severity::Info
            | Severity::Log,
        ) => ErrorScope::Statement,
        None => match error.severity() {
            "FATAL" | "PANIC" => ErrorScope::Session,
            _ => ErrorScope::Statement,
        },
    }
}

/// Display of a server error: severity, SQLSTATE and message.
pub struct DbErrorText<'a>(pub &'a DbError);

impl fmt::Display for DbErrorText<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} {}: {}",
            self.0.severity(),
            self.0.code().code(),
            self.0.message(),
        )
    }
}

/// Display of a driver error and its causes. tokio-postgres writes only the
/// kind of an error (`error connecting to server`, `connection closed`) and
/// keeps its cause, such as the refused socket, as the `source()`. Each cause
/// follows after `: `, at most [`MAX_ERROR_CHAIN_DEPTH`] of them; a longer
/// chain ends with a note that more causes follow.
pub struct DriverErrorText<'a>(pub &'a tokio_postgres::Error);

impl fmt::Display for DriverErrorText<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_with_causes(formatter, self.0)
    }
}

/// Writes `error`, then each cause of its `source()` chain after `: `, at
/// most [`MAX_ERROR_CHAIN_DEPTH`] causes; a longer chain ends with a note
/// that more causes follow.
fn write_with_causes(
    formatter: &mut fmt::Formatter<'_>,
    error: &(dyn std::error::Error + 'static),
) -> fmt::Result {
    write!(formatter, "{error}")?;
    let mut current = error.source();
    for _ in 0..MAX_ERROR_CHAIN_DEPTH {
        let Some(cause) = current else {
            return Ok(());
        };
        write!(formatter, ": {cause}")?;
        current = cause.source();
    }
    match current {
        Some(_) => write!(
            formatter,
            " (more causes after the first {MAX_ERROR_CHAIN_DEPTH} are not shown)"
        ),
        None => Ok(()),
    }
}

/// A connection attempt failed.
#[derive(Debug, thiserror::Error)]
#[error("connecting to {masked_url} failed: {cause}")]
pub struct ConnectError {
    /// The URL of the attempt, with the password masked.
    pub masked_url: MaskedUrl,
    /// Why the attempt failed.
    pub cause: ConnectFailure,
}

/// Why a connection attempt failed.
#[derive(Debug, thiserror::Error)]
pub enum ConnectFailure {
    /// The whole attempt (socket, TLS, startup and authentication) did not end
    /// within its bound.
    #[error("no connection within {bound:?}")]
    TimedOut { bound: Duration },
    /// The server answered the startup with an error (for example a failed
    /// authentication, or too many connections).
    #[error("the server refused the connection: {}", DbErrorText(.0))]
    ServerRefused(DbError),
    /// The socket, the TLS handshake or the protocol failed.
    #[error("the connection could not be made: {}", DriverErrorText(.0))]
    TransportFailed(tokio_postgres::Error),
    /// The connection opened, and its session setting statement failed. The
    /// connection is closed.
    #[error("the session setup `{statement}` failed: {}", DriverErrorText(.error))]
    SessionSetupFailed {
        statement: &'static str,
        error: tokio_postgres::Error,
    },
    /// The runtime of the process, which opens and drives every connection,
    /// cannot be used: nothing was connected.
    #[error("{0}")]
    RuntimeUnavailable(ProcessRuntimeRefusal),
}

impl ConnectError {
    /// Classifies a driver error of a connection attempt.
    pub(crate) fn from_driver(masked_url: MaskedUrl, error: tokio_postgres::Error) -> Self {
        let server_error = error.as_db_error().cloned();
        let cause = match server_error {
            Some(server_error) => ConnectFailure::ServerRefused(server_error),
            None => ConnectFailure::TransportFailed(error),
        };
        ConnectError { masked_url, cause }
    }

    /// Whether the failure may clear. A refused server certificate is
    /// permanent; other transport failures are transient; a server refusal
    /// and a failed session setup follow the SQLSTATE class (a setup failure
    /// with no server error is transient).
    pub fn transience(&self) -> Transience {
        match &self.cause {
            ConnectFailure::TimedOut { .. } => Transience::Transient,
            ConnectFailure::ServerRefused(error) => sqlstate_transience(error.code()),
            ConnectFailure::TransportFailed(error) => match tls_failure(error) {
                TlsFailure::CertificateRefused => Transience::Permanent,
                TlsFailure::OtherTls | TlsFailure::NotTls => Transience::Transient,
            },
            ConnectFailure::SessionSetupFailed { error, .. } => match error.as_db_error() {
                Some(server_error) => sqlstate_transience(server_error.code()),
                None => Transience::Transient,
            },
            ConnectFailure::RuntimeUnavailable(refusal) => match refusal {
                ProcessRuntimeRefusal::ForkedAfterStart { .. } => Transience::Permanent,
                ProcessRuntimeRefusal::StartFailed { .. } => Transience::Transient,
            },
        }
    }

    /// Whether `sslmode=prefer` tries again without TLS after this failure,
    /// as libpq does: after a failed TLS handshake, or after a server refusal
    /// of the session. A timeout and a socket failure are not tried again.
    pub(crate) fn plain_retry(&self) -> PlainRetry {
        match &self.cause {
            ConnectFailure::ServerRefused(_) => PlainRetry::Allowed,
            ConnectFailure::TransportFailed(error) => match tls_failure(error) {
                TlsFailure::CertificateRefused | TlsFailure::OtherTls => PlainRetry::Allowed,
                TlsFailure::NotTls => PlainRetry::NotAllowed,
            },
            ConnectFailure::TimedOut { .. }
            | ConnectFailure::SessionSetupFailed { .. }
            | ConnectFailure::RuntimeUnavailable(_) => PlainRetry::NotAllowed,
        }
    }
}

/// Whether a failed `sslmode=prefer` attempt is followed by a plain attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlainRetry {
    Allowed,
    NotAllowed,
}

/// The TLS part of a transport failure.
enum TlsFailure {
    /// rustls refused the server certificate.
    CertificateRefused,
    /// Another rustls error: the handshake failed.
    OtherTls,
    /// No rustls error in the chain.
    NotTls,
}

/// Walks the error's source chain (at most [`MAX_ERROR_CHAIN_DEPTH`] links)
/// for a rustls error. tokio-rustls wraps rustls errors in an `io::Error`,
/// whose `source()` skips the wrapped error, so each `io::Error` is opened
/// with `get_ref`.
fn tls_failure(error: &tokio_postgres::Error) -> TlsFailure {
    let mut current = error.source();
    for _ in 0..MAX_ERROR_CHAIN_DEPTH {
        let Some(link) = current else {
            return TlsFailure::NotTls;
        };
        let wrapped = link
            .downcast_ref::<std::io::Error>()
            .and_then(|io_error| io_error.get_ref())
            .and_then(|inner| inner.downcast_ref::<rustls::Error>());
        match link.downcast_ref::<rustls::Error>().or(wrapped) {
            Some(rustls::Error::InvalidCertificate(_)) => return TlsFailure::CertificateRefused,
            Some(_) => return TlsFailure::OtherTls,
            None => current = link.source(),
        }
    }
    TlsFailure::NotTls
}

/// A checkout from a core pool failed. Nothing was sent.
#[derive(Debug, thiserror::Error)]
pub enum CheckoutFailure {
    /// No connection became free within the checkout wait.
    #[error("the {role} pool had no free connection within {waited:?}")]
    TimedOut { role: PoolRole, waited: Duration },
    /// The pool had room, and opening a new connection failed.
    #[error("the {role} pool could not open a connection: {cause}")]
    ConnectFailed { role: PoolRole, cause: ConnectError },
    /// The pool was closed on purpose. Not retryable.
    #[error("the {role} pool is closed")]
    PoolClosed { role: PoolRole },
    /// The URL does not keep a transaction (PgBouncer statement pooling).
    /// Not retryable until the URL or the PgBouncer pool mode changes.
    #[error(
        "{masked_url} does not keep a transaction (PgBouncer statement pooling); use a direct \
         URL, or PgBouncer session or transaction pooling"
    )]
    StatementPooledUrl { masked_url: MaskedUrl },
    /// The statement-pooling check could not finish; the URL was not judged.
    #[error("the statement-pooling check on the {role} pool could not finish: {cause}")]
    PoolingCheckFailed {
        role: PoolRole,
        cause: PoolingCheckFailure,
    },
}

impl CheckoutFailure {
    /// Whether the failure may clear.
    pub fn transience(&self) -> Transience {
        match self {
            CheckoutFailure::TimedOut { .. } => Transience::Transient,
            CheckoutFailure::ConnectFailed { cause, .. } => cause.transience(),
            CheckoutFailure::PoolClosed { .. } => Transience::Permanent,
            CheckoutFailure::StatementPooledUrl { .. } => Transience::Permanent,
            CheckoutFailure::PoolingCheckFailed { .. } => Transience::Transient,
        }
    }
}

/// Why the statement-pooling check could not finish.
#[derive(Debug, thiserror::Error)]
pub enum PoolingCheckFailure {
    /// `SELECT 1` failed, so the check could not judge the URL.
    #[error("SELECT 1 failed: {}", DriverErrorText(.0))]
    SelectFailed(tokio_postgres::Error),
    /// The connection broke during the two-statement transaction.
    #[error("the connection broke during the check: {}", DriverErrorText(.0))]
    ConnectionLost(tokio_postgres::Error),
    /// The check did not end within its bound.
    #[error("the check did not end within {bound:?}")]
    TimedOut { bound: Duration },
}

/// A statement failed. The variant is the outcome; `statement` names the
/// statement.
#[derive(Debug, thiserror::Error)]
pub enum StatementFailure {
    /// Nothing reached the server. Nothing changed.
    #[error("statement `{statement}` was not sent: {cause}")]
    NotSent {
        statement: &'static str,
        cause: NotSentCause,
    },
    /// The server refused the statement with an ERROR. Nothing of the
    /// statement committed, and the connection stays usable.
    #[error("statement `{statement}` was refused: {cause}")]
    StatementRefused {
        statement: &'static str,
        cause: RefusalCause,
    },
    /// The statement went to the server and its reply was lost. It may have
    /// committed. The connection was discarded.
    #[error("statement `{statement}` has an unknown outcome: {cause}")]
    OutcomeUnknown {
        statement: &'static str,
        cause: UnknownOutcomeCause,
    },
}

impl StatementFailure {
    /// The name of the statement that failed.
    pub fn statement(&self) -> &'static str {
        match self {
            StatementFailure::NotSent { statement, .. }
            | StatementFailure::StatementRefused { statement, .. }
            | StatementFailure::OutcomeUnknown { statement, .. } => *statement,
        }
    }

    /// Whether the cause may clear. For `OutcomeUnknown` this says nothing
    /// about whether a replay is safe.
    pub fn transience(&self) -> Transience {
        match self {
            StatementFailure::NotSent { cause, .. } => cause.transience(),
            StatementFailure::StatementRefused { cause, .. } => cause.transience(),
            StatementFailure::OutcomeUnknown { .. } => Transience::Transient,
        }
    }
}

/// Why nothing reached the server.
#[derive(Debug, thiserror::Error)]
pub enum NotSentCause {
    /// The checkout failed.
    #[error("{0}")]
    Checkout(CheckoutFailure),
    /// The checked-out connection was already closed.
    #[error("the connection was closed before the statement was sent")]
    ConnectionClosed,
    /// The prepare round trip failed. The statement itself did not run.
    #[error("preparing the statement failed before it ran: {}", DriverErrorText(.0))]
    PrepareFailed(tokio_postgres::Error),
    /// An earlier failure discarded this connection (or dropped its open
    /// transaction); nothing more runs on it. Check out another connection.
    #[error("the connection was discarded after an earlier failure; check out another one")]
    ConnectionDiscarded,
    /// A statement of the transaction failed, so the transaction can only roll
    /// back. Nothing of it committed.
    #[error("the transaction is aborted: statement `{failed_statement}` failed in it")]
    TransactionAborted { failed_statement: &'static str },
}

impl NotSentCause {
    /// Whether the cause may clear.
    pub fn transience(&self) -> Transience {
        match self {
            NotSentCause::Checkout(failure) => failure.transience(),
            NotSentCause::ConnectionClosed => Transience::Transient,
            NotSentCause::PrepareFailed(error) => match error.as_db_error() {
                Some(server_error) => sqlstate_transience(server_error.code()),
                None => Transience::Transient,
            },
            NotSentCause::ConnectionDiscarded => Transience::Transient,
            NotSentCause::TransactionAborted { .. } => Transience::Permanent,
        }
    }
}

/// Why the server refused the statement.
#[derive(Debug, thiserror::Error)]
pub enum RefusalCause {
    /// The server answered with an ERROR.
    #[error("{}", DbErrorText(.0))]
    Server(DbError),
    /// The statement ran past its bound; the core sent a cancel request and
    /// the server answered with `query_canceled` (57014).
    #[error("cancelled after the bound of {bound:?}: {}", DbErrorText(.error))]
    CancelledAfterBound { bound: Duration, error: DbError },
}

impl RefusalCause {
    /// Whether the cause may clear, by the SQLSTATE class.
    pub fn transience(&self) -> Transience {
        match self {
            RefusalCause::Server(error) => sqlstate_transience(error.code()),
            RefusalCause::CancelledAfterBound { error, .. } => sqlstate_transience(error.code()),
        }
    }

    /// The server error behind the refusal.
    pub fn server_error(&self) -> &DbError {
        match self {
            RefusalCause::Server(error) | RefusalCause::CancelledAfterBound { error, .. } => error,
        }
    }
}

/// Why the outcome of a sent statement is unknown.
#[derive(Debug, thiserror::Error)]
pub enum UnknownOutcomeCause {
    /// The connection closed after the statement was sent.
    #[error("the connection was lost after the statement was sent: {}", DriverErrorText(.0))]
    ConnectionLost(tokio_postgres::Error),
    /// The server ended the session (FATAL or PANIC) after the statement was
    /// sent; it may have committed before the session ended.
    #[error("the server ended the session after the statement was sent: {}", DbErrorText(.0))]
    SessionTerminated(DbError),
    /// The driver failed with an error that is not a server error while the
    /// connection stayed open (a parameter value that does not encode, or a
    /// protocol message the driver did not expect).
    #[error("the driver failed after the statement was handed to it: {}", DriverErrorText(.0))]
    DriverFailed(tokio_postgres::Error),
    /// No reply came within the statement bound and the cancel grace after it.
    #[error(
        "no reply within the bound of {bound:?} and the cancel grace of {grace:?}; \
         cancel request: {cancel}"
    )]
    NoReplyWithinBound {
        bound: Duration,
        grace: Duration,
        cancel: CancelRequestOutcome,
    },
}

/// What happened to the cancel request sent after a statement bound.
#[derive(Debug)]
pub enum CancelRequestOutcome {
    /// The server accepted the cancel request. PostgreSQL does not report
    /// whether a statement was cancelled by it.
    Sent,
    /// The cancel connection failed.
    Failed(tokio_postgres::Error),
    /// The cancel connection did not end within its bound.
    TimedOut { bound: Duration },
}

impl fmt::Display for CancelRequestOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CancelRequestOutcome::Sent => formatter.write_str("sent"),
            CancelRequestOutcome::Failed(error) => {
                write!(formatter, "failed: {}", DriverErrorText(error))
            }
            CancelRequestOutcome::TimedOut { bound } => {
                write!(formatter, "no answer within {bound:?}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A link of an error chain of `depth` more links, for the chain tests.
    #[derive(Debug)]
    struct Link {
        depth: usize,
        next: Option<Box<Link>>,
    }

    fn chain(links: usize) -> Link {
        (0..links).rev().fold(
            Link {
                depth: links,
                next: None,
            },
            |next, depth| Link {
                depth,
                next: Some(Box::new(next)),
            },
        )
    }

    impl fmt::Display for Link {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "link {}", self.depth)
        }
    }

    impl std::error::Error for Link {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.next
                .as_deref()
                .map(|next| next as &(dyn std::error::Error + 'static))
        }
    }

    struct ChainText<'a>(&'a Link);

    impl fmt::Display for ChainText<'_> {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write_with_causes(formatter, self.0)
        }
    }

    #[tokio::test]
    async fn a_transport_failure_names_the_cause_of_the_driver_error() {
        let error = tokio_postgres::connect(
            "host=127.0.0.1 port=1 user=nobody connect_timeout=5",
            tokio_postgres::NoTls,
        )
        .await
        .err()
        .expect("nothing listens on port 1");
        let cause = error
            .source()
            .expect("a refused connect keeps its socket error as the source")
            .to_string();

        let text = ConnectFailure::TransportFailed(error).to_string();

        assert_eq!(
            text,
            format!("the connection could not be made: error connecting to server: {cause}")
        );
    }

    #[test]
    fn an_error_without_a_cause_is_its_own_text() {
        assert_eq!(ChainText(&chain(0)).to_string(), "link 0");
    }

    #[test]
    fn each_cause_within_the_bound_follows_its_error() {
        assert_eq!(
            ChainText(&chain(3)).to_string(),
            "link 0: link 1: link 2: link 3"
        );
    }

    #[test]
    fn a_chain_past_the_bound_ends_with_a_note() {
        let text = ChainText(&chain(MAX_ERROR_CHAIN_DEPTH + 2)).to_string();

        assert_eq!(
            text,
            format!(
                "link 0: link 1: link 2: link 3: link 4: link 5: link 6: link 7: link 8 \
                 (more causes after the first {MAX_ERROR_CHAIN_DEPTH} are not shown)"
            )
        );
    }

    #[test]
    fn transient_classes_are_the_five_named_classes() {
        for code in [
            "08006", "08P01", "40001", "40P01", "53300", "55P03", "57014", "57P01",
        ] {
            assert_eq!(
                sqlstate_code_transience(code),
                Transience::Transient,
                "{code}"
            );
        }
    }

    #[test]
    fn other_classes_are_permanent() {
        for code in [
            "23505", "42P01", "42883", "28P01", "3D000", "0A000", "22P02", "XX000",
        ] {
            assert_eq!(
                sqlstate_code_transience(code),
                Transience::Permanent,
                "{code}"
            );
        }
    }

    #[test]
    fn a_code_shorter_than_its_class_is_permanent() {
        assert_eq!(sqlstate_code_transience(""), Transience::Permanent);
        assert_eq!(sqlstate_code_transience("5"), Transience::Permanent);
    }

    #[test]
    fn typed_sqlstate_follows_its_class() {
        assert_eq!(
            sqlstate_transience(&SqlState::T_R_SERIALIZATION_FAILURE),
            Transience::Transient
        );
        assert_eq!(
            sqlstate_transience(&SqlState::QUERY_CANCELED),
            Transience::Transient
        );
        assert_eq!(
            sqlstate_transience(&SqlState::ADMIN_SHUTDOWN),
            Transience::Transient
        );
        assert_eq!(
            sqlstate_transience(&SqlState::UNIQUE_VIOLATION),
            Transience::Permanent
        );
        assert_eq!(
            sqlstate_transience(&SqlState::UNDEFINED_FUNCTION),
            Transience::Permanent
        );
    }

    #[test]
    fn pool_closed_is_permanent_and_a_wait_timeout_is_transient() {
        let closed = CheckoutFailure::PoolClosed {
            role: PoolRole::Main,
        };
        let waited = CheckoutFailure::TimedOut {
            role: PoolRole::Main,
            waited: Duration::from_secs(1),
        };
        assert_eq!(closed.transience(), Transience::Permanent);
        assert_eq!(waited.transience(), Transience::Transient);
    }

    #[test]
    fn a_statement_pooled_url_is_permanent() {
        let failure = CheckoutFailure::StatementPooledUrl {
            masked_url: MaskedUrl::for_tests("postgresql://u:***@h:6432/d"),
        };
        assert_eq!(failure.transience(), Transience::Permanent);
    }

    #[test]
    fn an_unknown_outcome_is_transient() {
        let failure = StatementFailure::OutcomeUnknown {
            statement: "probe",
            cause: UnknownOutcomeCause::NoReplyWithinBound {
                bound: Duration::from_secs(1),
                grace: Duration::from_secs(1),
                cancel: CancelRequestOutcome::TimedOut {
                    bound: Duration::from_secs(1),
                },
            },
        };
        assert_eq!(failure.transience(), Transience::Transient);
        assert_eq!(failure.statement(), "probe");
    }
}

//! The cause of a failed core operation, as a client names it to its user.
//!
//! A client turns the failure of a core operation into its own error value
//! (Python: the exception classes of `horsies._core`). [`FailureCause`] is
//! the closed set of causes:
//! - [`FailureCause::Server`]: the server answered a statement, or the start
//!   of a connection, with an error. It carries the SQLSTATE and the
//!   server's primary message.
//! - [`FailureCause::Connection`]: the operation got no answer from the
//!   server: a connect or a checkout that failed or timed out, a lost
//!   connection, or a reply that did not come within its bound.
//! - [`FailureCause::ContractViolation`]: a row, a stored value or a result
//!   broke the contract that the core reads it with. The same call meets it
//!   again.
//! - [`FailureCause::PreconditionFailed`]: a precondition of the operation
//!   does not hold: the settings, the database's client family or schema, an
//!   argument, a row that the call names, a key or a lock that another
//!   command holds, a usable transaction, or a client that can build a child
//!   spec now.
//!
//! Each error type of the core that a client turns into a failure record has
//! a `failure_cause` method with one exhaustive match, so a new variant has
//! to name its cause. Whether the cause may clear stays the error's own
//! `is_retryable` (or `transience`) rule.

use tokio_postgres::error::{DbError, SqlState};

use crate::broker::partition_maintenance::{CoveragePassError, PrunePassError};
use crate::broker::process_listener::ListenError;
use crate::broker::workflow_start::ClientStartError;
use crate::broker::{BrokerError, BrokerOperationSource};
use crate::core::history::errors::HistoryError;
use crate::db::{
    CheckoutFailure, ConnectError, ConnectFailure, ListenerStartFailure, NotSentCause,
    PoolingCheckFailure, StatementFailure, SubscribeError, UnknownOutcomeCause,
};
use crate::monitoring::{MonitoringQueryError, MonitoringQueryErrorSource};
use crate::worker::finalize_batch::BatchWriteError;
use crate::workflow_engine::child_spec::ChildSpecUnavailable;
use crate::workflow_engine::error::WorkflowError;

/// What a failed core operation met.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FailureCause {
    /// The server answered with an error.
    Server(ServerErrorFacts),
    /// No answer from the server.
    Connection,
    /// A row, a stored value or a result broke the core's read contract.
    ContractViolation,
    /// A precondition of the operation does not hold.
    PreconditionFailed,
}

/// The facts of a server error that a client shows: the SQLSTATE and the
/// server's primary message (no detail, hint or context).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerErrorFacts {
    sqlstate: SqlState,
    primary_message: String,
}

impl ServerErrorFacts {
    /// The facts of `sqlstate` and `primary_message`. Pure.
    pub fn new(sqlstate: SqlState, primary_message: String) -> Self {
        Self {
            sqlstate,
            primary_message,
        }
    }

    /// The facts of the five-character SQLSTATE `code` and
    /// `primary_message`, for a caller that holds the code as text. Pure.
    pub fn of_code(code: &str, primary_message: String) -> Self {
        Self::new(SqlState::from_code(code), primary_message)
    }

    /// The facts of a server error of the driver. Pure.
    pub fn of(error: &DbError) -> Self {
        Self::new(error.code().clone(), error.message().to_owned())
    }

    /// The five-character SQLSTATE.
    pub fn sqlstate(&self) -> &SqlState {
        &self.sqlstate
    }

    /// The server's primary message.
    pub fn primary_message(&self) -> &str {
        &self.primary_message
    }
}

impl FailureCause {
    /// A server error. Pure.
    pub fn of_server_error(error: &DbError) -> Self {
        Self::Server(ServerErrorFacts::of(error))
    }

    /// A driver error: a server error when the driver carries one, else no
    /// answer from the server. Pure.
    pub fn of_driver_error(error: &tokio_postgres::Error) -> Self {
        match error.as_db_error() {
            Some(server_error) => Self::of_server_error(server_error),
            None => Self::Connection,
        }
    }

    /// An error of the sqlx driver. Pure.
    ///
    /// - A database error with a SQLSTATE: [`FailureCause::Server`]. A
    ///   database error without one breaks the PostgreSQL protocol:
    ///   [`FailureCause::ContractViolation`].
    /// - A lost or failed connection, a pool wait that timed out, a closed
    ///   pool: [`FailureCause::Connection`].
    /// - A configuration error: [`FailureCause::PreconditionFailed`].
    /// - Every other driver error (`sqlx::Error` is non-exhaustive): a value
    ///   or a row that does not encode or decode, a missing row, column or
    ///   type, a migration error: [`FailureCause::ContractViolation`].
    pub fn of_sqlx_error(error: &sqlx::Error) -> Self {
        match error {
            sqlx::Error::Database(database_error) => match database_error.code() {
                Some(code) => Self::Server(ServerErrorFacts::new(
                    SqlState::from_code(code.as_ref()),
                    database_error.message().to_owned(),
                )),
                None => Self::ContractViolation,
            },
            sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::Protocol(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed => Self::Connection,
            sqlx::Error::Configuration(_) => Self::PreconditionFailed,
            _ => Self::ContractViolation,
        }
    }
}

impl ConnectError {
    /// The cause of a failed connection attempt. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match &self.cause {
            ConnectFailure::TimedOut { .. } | ConnectFailure::TransportFailed(_) => {
                FailureCause::Connection
            }
            ConnectFailure::ServerRefused(error) => FailureCause::of_server_error(error),
            ConnectFailure::SessionSetupFailed { error, .. } => {
                FailureCause::of_driver_error(error)
            }
            ConnectFailure::RuntimeUnavailable(_) => FailureCause::PreconditionFailed,
        }
    }
}

impl CheckoutFailure {
    /// The cause of a failed checkout. A closed pool and a statement-pooled
    /// URL are preconditions of the checkout. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            CheckoutFailure::TimedOut { .. } => FailureCause::Connection,
            CheckoutFailure::ConnectFailed { cause, .. } => cause.failure_cause(),
            CheckoutFailure::PoolClosed { .. } | CheckoutFailure::StatementPooledUrl { .. } => {
                FailureCause::PreconditionFailed
            }
            CheckoutFailure::PoolingCheckFailed { cause, .. } => cause.failure_cause(),
        }
    }
}

impl PoolingCheckFailure {
    /// The cause of a statement-pooling check that could not finish. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            PoolingCheckFailure::SelectFailed(error) => FailureCause::of_driver_error(error),
            PoolingCheckFailure::ConnectionLost(_) | PoolingCheckFailure::TimedOut { .. } => {
                FailureCause::Connection
            }
        }
    }
}

impl StatementFailure {
    /// The cause of a failed statement. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            StatementFailure::NotSent { cause, .. } => cause.failure_cause(),
            StatementFailure::StatementRefused { cause, .. } => {
                FailureCause::of_server_error(cause.server_error())
            }
            StatementFailure::OutcomeUnknown { cause, .. } => cause.failure_cause(),
        }
    }
}

impl NotSentCause {
    /// The cause of a statement that did not reach the server. An aborted
    /// transaction is a precondition of the statement. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            NotSentCause::Checkout(failure) => failure.failure_cause(),
            NotSentCause::ConnectionClosed | NotSentCause::ConnectionDiscarded => {
                FailureCause::Connection
            }
            NotSentCause::PrepareFailed(error) => FailureCause::of_driver_error(error),
            NotSentCause::TransactionAborted { .. } => FailureCause::PreconditionFailed,
        }
    }
}

impl UnknownOutcomeCause {
    /// The cause of a statement whose reply was lost. A session that the
    /// server ended carries the server's error. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            UnknownOutcomeCause::ConnectionLost(_)
            | UnknownOutcomeCause::DriverFailed(_)
            | UnknownOutcomeCause::NoReplyWithinBound { .. } => FailureCause::Connection,
            UnknownOutcomeCause::SessionTerminated(error) => FailureCause::of_server_error(error),
        }
    }
}

impl ListenerStartFailure {
    /// The cause of a listener that could not start. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            ListenerStartFailure::Connect(error) => error.failure_cause(),
            ListenerStartFailure::Listen(error) => FailureCause::of_driver_error(error),
            ListenerStartFailure::ListenTimedOut { .. } => FailureCause::Connection,
        }
    }
}

impl SubscribeError {
    /// The cause of a refused subscription. A full subscription table is a
    /// precondition of the subscription. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            SubscribeError::ListenerStopped | SubscribeError::TimedOut { .. } => {
                FailureCause::Connection
            }
            SubscribeError::SubscriptionLimit { .. } => FailureCause::PreconditionFailed,
            SubscribeError::StartFailed { cause } => cause.failure_cause(),
        }
    }
}

impl ListenError {
    /// The cause of a refused subscription on the broker's listener. A
    /// broker without a listener and refused session settings are
    /// preconditions. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            ListenError::NoSessionUrl | ListenError::PlanRefused(_) => {
                FailureCause::PreconditionFailed
            }
            ListenError::Subscribe(refused) => refused.failure_cause(),
        }
    }
}

impl BrokerError {
    /// The cause of a broker error. Pure.
    ///
    /// - A statement or a driver error: by its outcome and SQLSTATE.
    /// - A failed coverage pass of the schema install: by its history error.
    /// - A lost connection or a stopped listener: [`FailureCause::Connection`].
    /// - Stored data or rows that break their reader: [`FailureCause::ContractViolation`].
    /// - Refused settings, a database of another family or schema, a send
    ///   that another command holds, a claim argument out of range, a held
    ///   install lock, a process runtime that refused the call:
    ///   [`FailureCause::PreconditionFailed`].
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::Database(error) => FailureCause::of_sqlx_error(error),
            Self::Statement(failure) => failure.failure_cause(),
            Self::InstallCoverage(error) => error.failure_cause(),
            Self::ConnectionFailed(_) | Self::ListenerClosed => FailureCause::Connection,
            Self::Serialization(_)
            | Self::InvalidStatus(_)
            | Self::InvalidExecutionClass(_)
            | Self::TerminalizationContract(_)
            | Self::HistoryReadContract(_)
            | Self::ClaimContract(_)
            | Self::RowDecode(_) => FailureCause::ContractViolation,
            Self::InvalidWorkflowNodeStatus
            | Self::IncompleteTaskHistoryCutover
            | Self::InvalidConnectionSettings(_)
            | Self::PayloadMismatch { .. }
            | Self::EnqueueConflictUnverifiable { .. }
            | Self::IdempotencyKeyConflict { .. }
            | Self::EnqueueContract(_)
            | Self::InvalidIdempotencyReservationWindow(_)
            | Self::ClientFamilyMismatch { .. }
            | Self::SchemaRefused(_)
            | Self::InstallLock(_)
            | Self::InvalidClaimArgument(_)
            | Self::PoolSettings(_)
            | Self::ProcessRuntime(_) => FailureCause::PreconditionFailed,
        }
    }
}

impl BrokerOperationSource {
    /// The cause of what a failed broker operation met. Pure.
    ///
    /// - A broker error or a refused subscription: its own cause.
    /// - Stored data that a class read or a snapshot read cannot read:
    ///   [`FailureCause::ContractViolation`].
    /// - A task that moved between the two reads of a task info on every
    ///   pass, and an argument that a client could not read:
    ///   [`FailureCause::PreconditionFailed`].
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::Broker(error) => error.failure_cause(),
            Self::Listen(error) => error.failure_cause(),
            Self::ClassRead(_) | Self::Snapshot(_) => FailureCause::ContractViolation,
            Self::TaskInfoMoved { .. } | Self::ArgumentUnreadable(_) => {
                FailureCause::PreconditionFailed
            }
        }
    }
}

impl HistoryError {
    /// The cause of a history error. A leaf lock that is busy or not held
    /// and an absent history parent are preconditions. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::Database(error) => FailureCause::of_sqlx_error(error),
            Self::Statement(failure) => failure.failure_cause(),
            Self::RowDecode(_) | Self::ArchiveDecode(_) | Self::Contract(_) => {
                FailureCause::ContractViolation
            }
            Self::LeafLockNotHeld | Self::LeafLockBusy { .. } | Self::HistoryParentAbsent(_) => {
                FailureCause::PreconditionFailed
            }
        }
    }
}

impl ChildSpecUnavailable {
    /// The cause of a child spec that the client did not give: a
    /// precondition of the step (the client builds the spec now). Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::NotRun { .. } | Self::Interrupted { .. } | Self::Raised { .. } => {
                FailureCause::PreconditionFailed
            }
        }
    }
}

impl WorkflowError {
    /// The cause of a workflow engine error. A missing workflow and a
    /// refused workflow value are preconditions. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::Database(error) => FailureCause::of_sqlx_error(error),
            Self::Broker(error) => error.failure_cause(),
            Self::ChildSpecSource(unavailable) => unavailable.failure_cause(),
            Self::Serialization(_) | Self::InvalidStatus(_) | Self::StepContract(_) => {
                FailureCause::ContractViolation
            }
            Self::WorkflowNotFound { .. } | Self::Validation(_) => FailureCause::PreconditionFailed,
        }
    }
}

impl ClientStartError {
    /// The cause of a failed workflow start: the schema install's or the
    /// start's. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::SchemaInstall(error) => error.failure_cause(),
            Self::Start(error) => error.failure_cause(),
        }
    }
}

impl BatchWriteError {
    /// The cause of a batch statement with no outcome per row. Rows that
    /// break the statement's contract are a contract violation. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::NotSent { cause, .. } | Self::CommitUnknown { cause, .. } => cause.clone(),
            Self::Refused { server_error, .. } => FailureCause::Server(server_error.clone()),
            Self::ContractBroken { .. } => FailureCause::ContractViolation,
        }
    }
}

impl CoveragePassError {
    /// The cause of a coverage pass with no outcome. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::Broker(error) => error.failure_cause(),
            Self::History(error) => error.failure_cause(),
        }
    }
}

impl PrunePassError {
    /// The cause of a pruning pass with no report. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::Broker(error) => error.failure_cause(),
            Self::History(error) => error.failure_cause(),
        }
    }
}

impl MonitoringQueryError {
    /// The cause of a failed monitoring read: the cause of its source. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self.error_source() {
            MonitoringQueryErrorSource::Database(error) => FailureCause::of_sqlx_error(error),
            MonitoringQueryErrorSource::History(error) => error.failure_cause(),
            MonitoringQueryErrorSource::Broker(error) => error.failure_cause(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::db::CancelRequestOutcome;

    #[test]
    fn a_reply_lost_after_its_bound_is_a_connection_failure() {
        let failure = StatementFailure::OutcomeUnknown {
            statement: "probe",
            cause: UnknownOutcomeCause::NoReplyWithinBound {
                bound: Duration::from_secs(1),
                grace: Duration::from_secs(1),
                cancel: CancelRequestOutcome::Sent,
            },
        };
        assert_eq!(failure.failure_cause(), FailureCause::Connection);
    }

    #[test]
    fn an_aborted_transaction_is_a_failed_precondition() {
        let failure = StatementFailure::NotSent {
            statement: "probe",
            cause: NotSentCause::TransactionAborted {
                failed_statement: "earlier",
            },
        };
        assert_eq!(failure.failure_cause(), FailureCause::PreconditionFailed);
    }

    #[test]
    fn a_broken_row_contract_is_a_contract_violation() {
        let error = BrokerError::ClaimContract("ordinality 3 has no answer".to_owned());
        assert_eq!(error.failure_cause(), FailureCause::ContractViolation);
        assert!(!error.is_retryable());
    }

    #[test]
    fn a_lost_connection_and_a_stopped_listener_are_connection_failures() {
        assert_eq!(
            BrokerError::ConnectionFailed("reset".to_owned()).failure_cause(),
            FailureCause::Connection
        );
        assert_eq!(
            BrokerError::ListenerClosed.failure_cause(),
            FailureCause::Connection
        );
    }

    #[test]
    fn a_closed_sqlx_pool_is_a_connection_failure_and_a_missing_row_a_contract_violation() {
        assert_eq!(
            FailureCause::of_sqlx_error(&sqlx::Error::PoolClosed),
            FailureCause::Connection
        );
        assert_eq!(
            FailureCause::of_sqlx_error(&sqlx::Error::RowNotFound),
            FailureCause::ContractViolation
        );
    }

    #[test]
    fn server_facts_keep_the_sqlstate_and_the_primary_message() {
        let facts = ServerErrorFacts::new(
            SqlState::LOCK_NOT_AVAILABLE,
            "could not obtain lock".to_owned(),
        );
        assert_eq!(facts.sqlstate().code(), "55P03");
        assert_eq!(facts.primary_message(), "could not obtain lock");
    }

    #[test]
    fn a_child_spec_that_did_not_run_is_a_failed_precondition() {
        let error = WorkflowError::ChildSpecSource(ChildSpecUnavailable::NotRun {
            subject: "child spec build".to_owned(),
            cause: "callback queue full".to_owned(),
        });
        assert_eq!(error.failure_cause(), FailureCause::PreconditionFailed);
        assert!(error.is_retryable());
    }
}

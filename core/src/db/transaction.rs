//! Transactions on a checked-out connection.
//!
//! [`PooledConnection::begin`] sends `BEGIN` and returns a [`Transaction`]
//! that holds the connection until [`Transaction::commit`] or
//! [`Transaction::rollback`]. Statements in it have the usual three outcomes;
//! nothing of them is committed before `COMMIT` succeeds.
//!
//! `COMMIT` has the three outcomes too:
//! - `Ok`: committed.
//! - `StatementRefused`: the server refused the `COMMIT` (for example a
//!   deferred constraint or a serialization failure). Nothing committed; the
//!   connection stays usable.
//! - `NotSent`: `COMMIT` was not sent, because a statement of the transaction
//!   failed (the transaction is rolled back) or the connection was discarded.
//!   Nothing committed.
//! - `OutcomeUnknown`: the `COMMIT` reply was lost. The transaction may have
//!   committed.
//!
//! A transaction dropped while open discards its connection: no `ROLLBACK`
//! can be awaited in a drop, and an open transaction must not wait in the
//! pool with its locks. The server rolls it back when the socket closes. Call
//! [`Transaction::rollback`] to keep the connection.

use tokio_postgres::Row;

use super::failure::{NotSentCause, StatementFailure};
use super::pool::PooledConnection;
use super::statement::{BoundStatement, SqlScript, StatementBound};

/// The isolation level of a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IsolationLevel {
    /// `BEGIN` alone: the server's `default_transaction_isolation`.
    ServerDefault,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// The name of `COMMIT` in errors and logs: the
/// [`StatementFailure::statement`] of a failed [`Transaction::commit`].
pub const COMMIT_STATEMENT_NAME: &str = "commit";

/// What a statement failure of a transaction leaves of the transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailedTransactionOutcome {
    /// Nothing of the transaction committed.
    NotCommitted,
    /// The reply to `COMMIT` was lost: the transaction may have committed.
    Unknown,
}

impl FailedTransactionOutcome {
    /// The outcome of the transaction that `failure` ended: a failure of
    /// [`PooledConnection::begin`], of a statement of the [`Transaction`], or
    /// of [`Transaction::commit`]. Only a lost reply to `COMMIT` leaves it
    /// unknown: a lost reply to an earlier statement discards the connection
    /// with the transaction open, and the server rolls it back. A statement
    /// outside a transaction is not in this rule: a lost reply to it can have
    /// committed it. Pure.
    pub fn of(failure: &StatementFailure) -> Self {
        match failure {
            StatementFailure::OutcomeUnknown { statement, .. }
                if *statement == COMMIT_STATEMENT_NAME =>
            {
                Self::Unknown
            }
            StatementFailure::OutcomeUnknown { .. }
            | StatementFailure::NotSent { .. }
            | StatementFailure::StatementRefused { .. } => Self::NotCommitted,
        }
    }
}

/// A transaction-control command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransactionCommand {
    Begin(IsolationLevel),
    Commit,
    Rollback,
}

impl TransactionCommand {
    /// The name of the command in errors and logs.
    pub(crate) fn name(self) -> &'static str {
        match self {
            TransactionCommand::Begin(_) => "begin",
            TransactionCommand::Commit => COMMIT_STATEMENT_NAME,
            TransactionCommand::Rollback => "rollback",
        }
    }

    pub(crate) fn sql(self) -> &'static str {
        match self {
            TransactionCommand::Begin(IsolationLevel::ServerDefault) => "BEGIN",
            TransactionCommand::Begin(IsolationLevel::ReadCommitted) => {
                "BEGIN ISOLATION LEVEL READ COMMITTED"
            }
            TransactionCommand::Begin(IsolationLevel::RepeatableRead) => {
                "BEGIN ISOLATION LEVEL REPEATABLE READ"
            }
            TransactionCommand::Begin(IsolationLevel::Serializable) => {
                "BEGIN ISOLATION LEVEL SERIALIZABLE"
            }
            TransactionCommand::Commit => "COMMIT",
            TransactionCommand::Rollback => "ROLLBACK",
        }
    }
}

/// The stage of a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransactionState {
    /// Statements may run.
    Open,
    /// A statement failed; the server aborted the transaction. Only a
    /// rollback is possible.
    Aborted { failed_statement: &'static str },
    /// The connection was discarded; the server ends the transaction when
    /// the socket closes.
    Broken,
    /// Committed, rolled back, or refused at `COMMIT`.
    Ended,
}

/// An open transaction. It holds its connection until it ends.
pub struct Transaction<'c> {
    connection: &'c mut PooledConnection,
    state: TransactionState,
}

/// How [`Transaction::rollback`] ended.
#[derive(Debug)]
pub enum RollbackOutcome {
    /// The server rolled the transaction back; the connection stays usable.
    RolledBack,
    /// The connection was discarded; the server rolls the transaction back
    /// when the socket closes. Nothing of it committed.
    ConnectionDiscarded(DiscardReason),
}

/// Why a rollback ended with a discarded connection.
#[derive(Debug)]
pub enum DiscardReason {
    /// An earlier failure of the transaction discarded the connection.
    EarlierFailure,
    /// The `ROLLBACK` failed.
    RollbackFailed(StatementFailure),
}

impl PooledConnection {
    /// Sends `BEGIN` with the isolation level and opens a transaction.
    ///
    /// Errors: the three outcomes of the `BEGIN` statement. After an error
    /// no transaction is open; a discarded connection runs nothing more.
    pub async fn begin(
        &mut self,
        isolation: IsolationLevel,
    ) -> Result<Transaction<'_>, StatementFailure> {
        self.command(TransactionCommand::Begin(isolation)).await?;
        Ok(Transaction {
            connection: self,
            state: TransactionState::Open,
        })
    }
}

impl Transaction<'_> {
    /// Runs one statement in the transaction and reads every row.
    ///
    /// A failure aborts the transaction (`StatementRefused`) or breaks it
    /// (the connection was discarded); later statements are not sent.
    pub async fn query(
        &mut self,
        statement: &BoundStatement<'_>,
        bound: StatementBound,
    ) -> Result<Vec<Row>, StatementFailure> {
        let name = statement.statement().name();
        self.ensure_open(name)?;
        let result = self.connection.query(statement, bound).await;
        self.observe(name, &result);
        result
    }

    /// Runs one statement in the transaction and reads the count of affected
    /// rows. Failures as in [`Transaction::query`].
    pub async fn execute(
        &mut self,
        statement: &BoundStatement<'_>,
        bound: StatementBound,
    ) -> Result<u64, StatementFailure> {
        let name = statement.statement().name();
        self.ensure_open(name)?;
        let result = self.connection.execute(statement, bound).await;
        self.observe(name, &result);
        result
    }

    /// Runs a script in the transaction (simple query protocol, no
    /// parameters). Failures as in [`Transaction::query`]: a server ERROR in
    /// any statement of the script aborts the transaction.
    pub async fn run_script(
        &mut self,
        script: &SqlScript,
        bound: StatementBound,
    ) -> Result<(), StatementFailure> {
        let name = script.name();
        self.ensure_open(name)?;
        let result = self.connection.run_script(script, bound).await;
        self.observe(name, &result);
        result
    }

    /// Commits the transaction. The outcomes are in the module documentation.
    pub async fn commit(mut self) -> Result<(), StatementFailure> {
        let name = TransactionCommand::Commit.name();
        match self.state {
            TransactionState::Open => {
                let result = self.connection.command(TransactionCommand::Commit).await;
                self.state = TransactionState::Ended;
                result
            }
            TransactionState::Aborted { failed_statement } => {
                // A failed ROLLBACK discards the connection, and the server
                // ends the transaction when the socket closes; the answer to
                // the caller is the same either way.
                let _rollback = self.finish_with_rollback().await;
                Err(StatementFailure::NotSent {
                    statement: name,
                    cause: NotSentCause::TransactionAborted { failed_statement },
                })
            }
            TransactionState::Broken | TransactionState::Ended => {
                self.state = TransactionState::Ended;
                Err(StatementFailure::NotSent {
                    statement: name,
                    cause: NotSentCause::ConnectionDiscarded,
                })
            }
        }
    }

    /// Rolls the transaction back. Nothing of it commits in any outcome.
    pub async fn rollback(mut self) -> RollbackOutcome {
        match self.state {
            TransactionState::Open | TransactionState::Aborted { .. } => {
                self.finish_with_rollback().await
            }
            TransactionState::Broken | TransactionState::Ended => {
                self.state = TransactionState::Ended;
                RollbackOutcome::ConnectionDiscarded(DiscardReason::EarlierFailure)
            }
        }
    }

    async fn finish_with_rollback(&mut self) -> RollbackOutcome {
        let result = self.connection.command(TransactionCommand::Rollback).await;
        self.state = TransactionState::Ended;
        match result {
            Ok(()) => RollbackOutcome::RolledBack,
            Err(failure) => {
                self.connection.discard();
                RollbackOutcome::ConnectionDiscarded(DiscardReason::RollbackFailed(failure))
            }
        }
    }

    fn ensure_open(&self, statement: &'static str) -> Result<(), StatementFailure> {
        match self.state {
            TransactionState::Open => Ok(()),
            TransactionState::Aborted { failed_statement } => Err(StatementFailure::NotSent {
                statement,
                cause: NotSentCause::TransactionAborted { failed_statement },
            }),
            TransactionState::Broken | TransactionState::Ended => Err(StatementFailure::NotSent {
                statement,
                cause: NotSentCause::ConnectionDiscarded,
            }),
        }
    }

    /// A server ERROR aborts the transaction; any other failure discarded
    /// the connection.
    fn observe<T>(&mut self, statement: &'static str, result: &Result<T, StatementFailure>) {
        match result {
            Ok(_) => {}
            Err(StatementFailure::StatementRefused { .. }) => {
                self.state = TransactionState::Aborted {
                    failed_statement: statement,
                };
            }
            Err(StatementFailure::NotSent { .. } | StatementFailure::OutcomeUnknown { .. }) => {
                self.state = TransactionState::Broken;
            }
        }
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        match self.state {
            TransactionState::Open | TransactionState::Aborted { .. } => {
                self.connection.discard();
            }
            TransactionState::Broken | TransactionState::Ended => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::db::{CancelRequestOutcome, UnknownOutcomeCause};

    fn lost_reply(statement: &'static str) -> StatementFailure {
        StatementFailure::OutcomeUnknown {
            statement,
            cause: UnknownOutcomeCause::NoReplyWithinBound {
                bound: Duration::from_secs(30),
                grace: Duration::from_secs(1),
                cancel: CancelRequestOutcome::Sent,
            },
        }
    }

    #[test]
    fn each_command_has_its_sql() {
        assert_eq!(
            TransactionCommand::Begin(IsolationLevel::ServerDefault).sql(),
            "BEGIN"
        );
        assert_eq!(
            TransactionCommand::Begin(IsolationLevel::Serializable).sql(),
            "BEGIN ISOLATION LEVEL SERIALIZABLE"
        );
        assert_eq!(TransactionCommand::Commit.sql(), "COMMIT");
        assert_eq!(TransactionCommand::Rollback.sql(), "ROLLBACK");
        assert_eq!(TransactionCommand::Commit.name(), "commit");
    }

    #[test]
    fn the_commit_command_has_the_commit_statement_name() {
        assert_eq!(TransactionCommand::Commit.name(), COMMIT_STATEMENT_NAME);
    }

    #[test]
    fn a_lost_reply_to_commit_leaves_the_outcome_unknown() {
        assert_eq!(
            FailedTransactionOutcome::of(&lost_reply(TransactionCommand::Commit.name())),
            FailedTransactionOutcome::Unknown
        );
    }

    #[test]
    fn a_lost_reply_before_commit_committed_nothing() {
        for command in [
            TransactionCommand::Begin(IsolationLevel::ServerDefault),
            TransactionCommand::Rollback,
        ] {
            assert_eq!(
                FailedTransactionOutcome::of(&lost_reply(command.name())),
                FailedTransactionOutcome::NotCommitted,
                "{command:?}"
            );
        }
        assert_eq!(
            FailedTransactionOutcome::of(&lost_reply("rerun_enqueue")),
            FailedTransactionOutcome::NotCommitted
        );
    }

    #[test]
    fn a_commit_that_was_not_sent_committed_nothing() {
        let failure = StatementFailure::NotSent {
            statement: COMMIT_STATEMENT_NAME,
            cause: NotSentCause::TransactionAborted {
                failed_statement: "rerun_enqueue",
            },
        };
        assert_eq!(
            FailedTransactionOutcome::of(&failure),
            FailedTransactionOutcome::NotCommitted
        );
    }
}

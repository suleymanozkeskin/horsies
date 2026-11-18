//! A session lock: one dedicated connection that holds a session-level
//! advisory lock for as long as its session lives.
//!
//! - [`LockSession::open`] connects with a [`ConnectPlan`] and runs the lock
//!   statement: a `pg_try_advisory_lock` that takes one text argument and
//!   answers one boolean. A `true` answer means this session holds the lock
//!   ([`LockTake::Taken`]); a `false` answer means another session holds it
//!   ([`LockTake::HeldElsewhere`]). Nothing unlocks it: the server releases
//!   the lock when the session ends.
//! - [`LockSession::probe`] confirms that the session still lives
//!   (`SELECT 1`); [`LockSession::take`] runs the lock statement again.
//! - Each statement ends within [`SESSION_LOCK_STATEMENT_BOUND`], the
//!   listener's statement bound.
//! - [`LockSession::ended`] resolves when the connection ends.
//! - Dropping a [`LockSession`] closes its connection, so the server ends
//!   the session and releases the lock.

use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_postgres::Client;

use super::connect::{open_connection, ConnectPlan};
use super::failure::{sqlstate_transience, ConnectError, DriverErrorText, Transience};
use super::listener::{LISTENER_STATEMENT_BOUND, RECONNECT_BACKOFF_FIRST, RECONNECT_BACKOFF_LIMIT};

/// The bound of one statement of a lock session: the listener's statement
/// bound.
pub const SESSION_LOCK_STATEMENT_BOUND: Duration = LISTENER_STATEMENT_BOUND;

/// The first wait before a lock session is opened again after its
/// connection was lost: the listener's first reconnect wait.
pub const SESSION_LOCK_RECONNECT_FIRST: Duration = RECONNECT_BACKOFF_FIRST;

/// The longest wait between two openings of a lock session: the listener's
/// longest reconnect wait.
pub const SESSION_LOCK_RECONNECT_LIMIT: Duration = RECONNECT_BACKOFF_LIMIT;

/// The probe of a lock session.
const PROBE_SQL: &str = "SELECT 1";

/// What the lock statement answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockTake {
    /// This session holds the lock.
    Taken,
    /// Another session holds the lock; this session does not.
    HeldElsewhere,
}

/// How the connection of a lock session ended.
#[derive(Debug, thiserror::Error)]
pub enum SessionEnd {
    /// The server or the network closed the connection.
    #[error("the connection closed")]
    Closed,
    /// The connection failed.
    #[error("the connection failed: {}", DriverErrorText(.0))]
    Failed(tokio_postgres::Error),
}

/// A statement of a lock session failed. A session whose statement failed
/// may still hold its lock; the caller drops it to release the lock.
#[derive(Debug, thiserror::Error)]
pub enum SessionLockFailure {
    /// The connection could not be opened. No lock is held.
    #[error("{0}")]
    Connect(ConnectError),
    /// The server refused the statement, or the connection failed during it.
    #[error("the {statement} statement failed: {}", DriverErrorText(.error))]
    Statement {
        statement: &'static str,
        error: tokio_postgres::Error,
    },
    /// The statement did not end within its bound.
    #[error("the {statement} statement did not end within {bound:?}")]
    TimedOut {
        statement: &'static str,
        bound: Duration,
    },
    /// The lock statement answered no single boolean.
    #[error("the lock statement answered no boolean: {}", DriverErrorText(.0))]
    Answer(tokio_postgres::Error),
}

impl SessionLockFailure {
    /// Whether a later attempt may succeed. A server refusal follows its
    /// SQLSTATE class; a failed connection or a statement past its bound may
    /// clear; an answer that is not one boolean does not.
    pub fn transience(&self) -> Transience {
        match self {
            Self::Connect(error) => error.transience(),
            Self::Statement { error, .. } => match error.as_db_error() {
                Some(server_error) => sqlstate_transience(server_error.code()),
                None => Transience::Transient,
            },
            Self::TimedOut { .. } => Transience::Transient,
            Self::Answer(_) => Transience::Permanent,
        }
    }
}

/// Whether the end of the connection was reported.
enum EndWatch {
    Waiting(oneshot::Receiver<SessionEnd>),
    Reported,
}

/// One dedicated connection that holds, or waits for, one session advisory
/// lock.
pub struct LockSession {
    client: Client,
    driver: JoinHandle<()>,
    ended: EndWatch,
    lock_sql: &'static str,
    argument: String,
}

impl LockSession {
    /// Opens one connection with `plan` and runs `lock_sql` with `argument`
    /// as its one text parameter. `lock_sql` is a `pg_try_advisory_lock`
    /// statement that answers one boolean.
    ///
    /// The driver of the connection runs as a task of the current runtime.
    ///
    /// Errors: [`SessionLockFailure`]. The connection of a failed lock
    /// statement is closed; it may have taken the lock before it failed, and
    /// the server releases it when the session ends.
    pub async fn open(
        plan: &ConnectPlan,
        lock_sql: &'static str,
        argument: String,
    ) -> Result<(LockSession, LockTake), SessionLockFailure> {
        let (client, connection) = open_connection(plan)
            .await
            .map_err(SessionLockFailure::Connect)?;
        let (ended_sender, ended) = oneshot::channel();
        let driver = tokio::spawn(async move {
            let end = match connection.await {
                Ok(()) => SessionEnd::Closed,
                Err(error) => SessionEnd::Failed(error),
            };
            // The receiver is gone when the session was dropped.
            let _delivered = ended_sender.send(end);
        });
        let session = LockSession {
            client,
            driver,
            ended: EndWatch::Waiting(ended),
            lock_sql,
            argument,
        };
        let take = session.take().await?;
        Ok((session, take))
    }

    /// Runs the lock statement again.
    ///
    /// Errors: [`SessionLockFailure::Statement`],
    /// [`SessionLockFailure::TimedOut`], [`SessionLockFailure::Answer`].
    pub async fn take(&self) -> Result<LockTake, SessionLockFailure> {
        let statement = "lock";
        let answer = tokio::time::timeout(
            SESSION_LOCK_STATEMENT_BOUND,
            self.client.query_one(self.lock_sql, &[&self.argument]),
        )
        .await;
        let row = match answer {
            Err(_elapsed) => {
                return Err(SessionLockFailure::TimedOut {
                    statement,
                    bound: SESSION_LOCK_STATEMENT_BOUND,
                })
            }
            Ok(Err(error)) => return Err(SessionLockFailure::Statement { statement, error }),
            Ok(Ok(row)) => row,
        };
        match row.try_get::<_, bool>(0) {
            Ok(true) => Ok(LockTake::Taken),
            Ok(false) => Ok(LockTake::HeldElsewhere),
            Err(error) => Err(SessionLockFailure::Answer(error)),
        }
    }

    /// Confirms that the session lives: one `SELECT 1`.
    ///
    /// Errors: [`SessionLockFailure::Statement`],
    /// [`SessionLockFailure::TimedOut`].
    pub async fn probe(&self) -> Result<(), SessionLockFailure> {
        let statement = "probe";
        match tokio::time::timeout(
            SESSION_LOCK_STATEMENT_BOUND,
            self.client.batch_execute(PROBE_SQL),
        )
        .await
        {
            Err(_elapsed) => Err(SessionLockFailure::TimedOut {
                statement,
                bound: SESSION_LOCK_STATEMENT_BOUND,
            }),
            Ok(Err(error)) => Err(SessionLockFailure::Statement { statement, error }),
            Ok(Ok(())) => Ok(()),
        }
    }

    /// Resolves once, when the connection ends. A call after that never
    /// resolves: the connection does not end twice, and every later
    /// statement of the session fails.
    pub async fn ended(&mut self) -> SessionEnd {
        let receiver = match &mut self.ended {
            EndWatch::Waiting(receiver) => receiver,
            EndWatch::Reported => return std::future::pending().await,
        };
        let end = match receiver.await {
            Ok(end) => end,
            // The driver task ended without a report: it was aborted or it
            // panicked. Either way the connection is gone.
            Err(_dropped) => SessionEnd::Closed,
        };
        self.ended = EndWatch::Reported;
        end
    }
}

impl Drop for LockSession {
    fn drop(&mut self) {
        // Ending the driver closes the socket; the server ends the session
        // and releases its lock.
        self.driver.abort();
    }
}

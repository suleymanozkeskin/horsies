//! Rerun of a retained terminal task: in the app's own transaction, or in
//! the caller's transaction through `CallerTransaction` (Python
//! `rerun_task(connection, command, policy)`).
//!
//! A rerun reads the source task in one statement and, for an eligible
//! source with a retained input, writes one new task with the lineage
//! `RERUN` (`horsies_enqueue_task`), which claims the caller key of a keyed
//! rerun first. Every outcome but [`RerunOutcome::Enqueued`] writes nothing.
//!
//! - [`Horsies::rerun_task`] runs both statements in a transaction of the
//!   app's handle and commits it.
//! - [`rerun_task_in`] runs them in a transaction that the caller owns, so
//!   the rerun commits or rolls back with the caller's own writes. The core
//!   builds each statement and decides the outcome; the caller's driver runs
//!   the statements. Nothing commits inside. Adapters: `TokioPostgresCaller`
//!   (feature `tokio-postgres`) and `SqlxCaller` (feature `sqlx`).
//!
//! With the feature `otel`, both hand the core the trace carrier that the
//! global propagator injects for the caller's current OpenTelemetry context
//! (`horsies::otel`), as a send does, and the new task stores it: the
//! attempt spans of the rerun are children of the caller's span. A rerun
//! opens no span of its own. Without the feature, or with no valid span in
//! the current context, the new task stores no carrier. The carrier of the
//! source task is never copied.
//!
//! Both check the stored retry list of the source against the declared
//! custom codes of the app's tasks ([`Horsies::registered_task_codes`]): a
//! built-in code passes; a custom code passes when the app registers the
//! task and the task declares the code. Otherwise the rerun is refused and
//! writes nothing.

#[cfg(feature = "sqlx")]
mod sqlx_adapter;
#[cfg(feature = "tokio-postgres")]
mod tokio_postgres_adapter;

use chrono::{DateTime, Utc};
use horsies_core::{
    decode_rerun_write, prepare_rerun_write, rerun_read, BrokerError, ClientFamily,
    RerunPreparation,
};

use crate::config::declared_codes;
use crate::process_state::refuse_in_a_fork;
use crate::telemetry::rerun_trace;
use crate::Horsies;

pub use horsies_core::{
    CallerColumn, CallerRow, CallerStatement, CallerTransaction, CallerType, IdempotencyKeyError,
    NotEligibleReason, RecordedFamily, RegisteredTaskCodes, RerunEnqueuePolicy, RerunError,
    RerunOutcome, RerunRowError, RerunTask, StatementValue, TaskAbsenceReason, UnknownStoredFamily,
    Uuid7Error,
};
#[cfg(feature = "sqlx")]
pub use sqlx_adapter::SqlxCaller;
#[cfg(feature = "tokio-postgres")]
pub use tokio_postgres_adapter::TokioPostgresCaller;

/// Why a rerun in the caller's transaction failed. The rerun's write did
/// not run, or its outcome is the driver's error; the caller decides
/// whether to commit or roll back, and does not commit after an error.
#[derive(Debug, thiserror::Error)]
pub enum CallerRerunError<E> {
    /// A statement of the rerun failed in the caller's driver. Its retry
    /// rule is the driver's.
    #[error("a rerun statement failed in the caller's transaction: {0}")]
    Driver(#[source] E),
    /// The retention class of the new task has no class row: the new task
    /// would have no history partition. Nothing is written. A precondition
    /// of the call (Python `ValueError`).
    #[error(
        "retention class {key:?} is not registered: the new task would have no history \
         partition; register the class, then rerun"
    )]
    ClassNotRegistered { key: String },
    /// The database was installed by another client family, or records
    /// none. Nothing is written. Not retryable.
    #[error("rerun refused: this database was installed by {stored}; this is the Rust client")]
    ClientFamilyMismatch { stored: RecordedFamily },
    /// The family row of the database holds a value that is not a client
    /// family: corrupt stored data. Nothing is written. Not retryable: the
    /// same row is refused again until it is repaired.
    #[error("rerun refused: {0}")]
    UnknownStoredFamily(UnknownStoredFamily),
    /// The caller key of the command is not a valid idempotency key.
    /// Nothing is written.
    #[error("the caller key of the rerun is refused: {0}")]
    CallerKeyRefused(IdempotencyKeyError),
    /// The stored retry list of the source names the custom code `code`,
    /// and the task `task_name` of the app does not declare it. Nothing is
    /// written. Not retryable: the same call meets it again until the app
    /// declares the code.
    #[error(
        "rerun refused: the stored retry list of task '{task_name}' names the code '{code}', \
         which the task does not declare"
    )]
    RetryCodeNotDeclared { task_name: String, code: String },
    /// The stored retry list of the source names a custom code, and the app
    /// does not register the task `task_name`, so the code cannot be
    /// checked. Nothing is written. Not retryable: the same call meets it
    /// again until the app registers the task.
    #[error(
        "rerun refused: the app does not register task '{task_name}', so the custom codes of \
         its stored retry list cannot be checked"
    )]
    TaskNotRegistered { task_name: String },
    /// The task id source refused to mint the new task's id. Nothing is
    /// written.
    #[error("the task id of the rerun could not be minted: {0}")]
    IdMintRefused(Uuid7Error),
    /// A row did not fit its statement: a defect of the adapter, or a row
    /// outside the function's contract. Not retryable.
    #[error("{0}")]
    Row(RerunRowError),
}

/// Rerun `command` in `transaction`, a transaction that the caller owns
/// (Python `rerun_task(connection, command, policy,
/// registered_task_codes=...)`), with the new task sent at `sent_at`.
/// `codes` are the declared custom codes of the app's tasks
/// ([`Horsies::registered_task_codes`]); the stored retry list of the
/// source is checked against them before the write.
///
/// Statements, on `transaction`: the source read
/// (`horsies_rerun_source_read`); for an eligible source with a retained
/// input, one `horsies_enqueue_task` with the lineage `RERUN`. The read
/// keeps ACCESS SHARE on the source's history leaf until the transaction
/// ends. Nothing commits: the caller's commit keeps the new task and the
/// key claim, its rollback drops them.
///
/// Outcomes: Python's catalog ([`RerunOutcome`]); every outcome but
/// `Enqueued` writes nothing.
///
/// The new task stores the trace carrier of the caller's current context
/// (feature `otel`; see the module doc), in place of a carrier that
/// `command` holds.
///
/// Errors: [`CallerRerunError`]. After an error the transaction may hold
/// the key claim or the new row; the caller does not commit it.
pub async fn rerun_task_in<T>(
    transaction: &mut T,
    command: &RerunTask,
    policy: &RerunEnqueuePolicy,
    codes: &RegisteredTaskCodes,
    sent_at: DateTime<Utc>,
) -> Result<RerunOutcome, CallerRerunError<T::Error>>
where
    T: CallerTransaction,
{
    let command = traced(command.clone());
    rerun_command_in(transaction, &command, policy, codes, sent_at).await
}

/// `command` with the trace carrier of the caller's current context
/// ([`rerun_trace`]): none without the feature `otel`.
fn traced(command: RerunTask) -> RerunTask {
    command.with_trace(rerun_trace())
}

/// The statements of [`rerun_task_in`] for `command` as it is: its trace
/// is the one that the new task stores.
///
/// Errors: [`CallerRerunError`], as [`rerun_task_in`].
async fn rerun_command_in<T>(
    transaction: &mut T,
    command: &RerunTask,
    policy: &RerunEnqueuePolicy,
    codes: &RegisteredTaskCodes,
    sent_at: DateTime<Utc>,
) -> Result<RerunOutcome, CallerRerunError<T::Error>>
where
    T: CallerTransaction,
{
    let read = rerun_read(command, policy);
    let row = transaction
        .query_one(&read)
        .await
        .map_err(CallerRerunError::Driver)?;
    let prepared = prepare_rerun_write(
        &read,
        &row,
        command,
        policy,
        codes,
        ClientFamily::Rust,
        sent_at,
    )
    .map_err(CallerRerunError::Row)?;
    let write = match prepared {
        RerunPreparation::Write(write) => write,
        RerunPreparation::Refused(refusal) => return Ok(refusal.into()),
        RerunPreparation::ClassNotRegistered { class_key } => {
            return Err(CallerRerunError::ClassNotRegistered { key: class_key })
        }
        RerunPreparation::ClientFamilyMismatch { stored, own: _ } => {
            return Err(CallerRerunError::ClientFamilyMismatch { stored })
        }
        RerunPreparation::UnknownStoredFamily(unknown) => {
            return Err(CallerRerunError::UnknownStoredFamily(unknown))
        }
        RerunPreparation::CallerKeyRefused(refused) => {
            return Err(CallerRerunError::CallerKeyRefused(refused))
        }
        RerunPreparation::RetryCodeNotDeclared { task_name, code } => {
            return Err(CallerRerunError::RetryCodeNotDeclared { task_name, code })
        }
        RerunPreparation::TaskNotRegistered { task_name } => {
            return Err(CallerRerunError::TaskNotRegistered { task_name })
        }
        RerunPreparation::IdMintRefused(refused) => {
            return Err(CallerRerunError::IdMintRefused(refused))
        }
    };
    let row = transaction
        .query_one(write.statement())
        .await
        .map_err(CallerRerunError::Driver)?;
    let written = decode_rerun_write(&write, &row).map_err(CallerRerunError::Row)?;
    Ok(written.into())
}

/// Why a rerun in the app's own transaction failed.
#[derive(Debug, thiserror::Error)]
pub enum AppRerunError {
    /// The app's handle could not connect, or could not install the
    /// schema. Nothing ran. The retry rule is the cause's.
    #[error("the rerun could not connect: {0}")]
    Connect(BrokerError),
    /// The rerun failed ([`RerunError`]). Nothing committed, except after
    /// an outcome-unknown failure at `COMMIT`.
    #[error("{0}")]
    Rerun(RerunError),
}

impl Horsies {
    /// The declared custom codes of every task that the app registers, by
    /// task name: the input of [`rerun_task_in`]. [`Horsies::rerun_task`]
    /// takes them itself. Pure.
    ///
    /// # Panics
    ///
    /// When a registered task holds a code set that the registration
    /// refuses: a defect, because the registration checked every set.
    pub fn registered_task_codes(&self) -> RegisteredTaskCodes {
        RegisteredTaskCodes::new(
            self.tasks()
                .iter()
                .map(|(name, task)| (name.to_owned(), declared_codes(name, task)))
                .collect(),
        )
    }

    /// Rerun `command` in a transaction of the app's handle, and commit it
    /// (in a worker child, the child's producer handle). The handle checks
    /// the client family of the database before the transaction, and
    /// installs the schema once per handle, as a send does. The new task
    /// stores the trace carrier of the caller's current context (feature
    /// `otel`), as with [`rerun_task_in`]. The stored retry list of the
    /// source is checked against [`Horsies::registered_task_codes`].
    ///
    /// Outcomes: as [`rerun_task_in`].
    ///
    /// Errors: [`AppRerunError`].
    ///
    /// # Panics
    ///
    /// In a process that was forked without `exec` after the core's
    /// process runtime started. Nothing ran.
    pub async fn rerun_task(
        &self,
        command: RerunTask,
        policy: RerunEnqueuePolicy,
    ) -> Result<RerunOutcome, AppRerunError> {
        refuse_in_a_fork(format_args!("Horsies::rerun_task"));
        let command = traced(command);
        let broker = self
            .reads_handle()
            .initialized_broker()
            .await
            .map_err(AppRerunError::Connect)?;
        broker
            .rerun_task(command, policy, &self.registered_task_codes())
            .await
            .map_err(AppRerunError::Rerun)
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;

    use super::*;
    use uuid::Uuid;

    /// A transaction that answers each statement with the next row, and
    /// records the statements it ran.
    struct ScriptedTransaction {
        rows: Vec<CallerRow>,
        ran: Vec<&'static str>,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("scripted driver failure")]
    struct ScriptedFailure;

    impl CallerTransaction for ScriptedTransaction {
        type Error = ScriptedFailure;

        fn query_one(
            &mut self,
            statement: &CallerStatement,
        ) -> impl Future<Output = Result<CallerRow, ScriptedFailure>> + Send {
            self.ran.push(statement.name());
            let next = match self.rows.is_empty() {
                true => Err(ScriptedFailure),
                false => Ok(self.rows.remove(0)),
            };
            async move { next }
        }
    }

    fn source_read_row(
        statement: &CallerStatement,
        values: &[(&str, StatementValue)],
    ) -> CallerRow {
        CallerRow::new(
            statement
                .columns()
                .iter()
                .map(|column: &CallerColumn| {
                    values
                        .iter()
                        .find(|(name, _)| *name == column.name)
                        .map(|(_, value)| value.clone())
                        .unwrap_or(StatementValue::Null(column.value_type))
                })
                .collect(),
        )
    }

    fn command() -> RerunTask {
        RerunTask::new(Uuid::new_v4(), None, None)
    }

    #[tokio::test]
    async fn an_absent_source_answers_python_s_outcome_and_writes_nothing() {
        let command = command();
        let policy = RerunEnqueuePolicy::standard(true);
        let read = rerun_read(&command, &policy);
        let row = source_read_row(
            &read,
            &[
                ("class_registered", StatementValue::Bool(true)),
                ("client_family", StatementValue::Text("RUST".to_owned())),
                ("location", StatementValue::Text("ABSENT".to_owned())),
                (
                    "absence_reason",
                    StatementValue::Text("WITHIN_RETAINED_HISTORY".to_owned()),
                ),
            ],
        );
        let mut transaction = ScriptedTransaction {
            rows: vec![row],
            ran: Vec::new(),
        };
        let outcome = rerun_task_in(
            &mut transaction,
            &command,
            &policy,
            &RegisteredTaskCodes::NONE,
            Utc::now(),
        )
        .await
        .unwrap();
        assert!(
            matches!(outcome, RerunOutcome::SourceAbsent { task_id, .. } if task_id == command.source_task_id()),
            "{outcome:?}"
        );
        assert_eq!(transaction.ran, vec![read.name()]);
    }

    #[tokio::test]
    async fn a_database_of_the_python_client_is_refused_before_the_write() {
        let command = command();
        let policy = RerunEnqueuePolicy::standard(true);
        let read = rerun_read(&command, &policy);
        let row = source_read_row(
            &read,
            &[
                ("class_registered", StatementValue::Bool(true)),
                ("client_family", StatementValue::Text("PYTHON".to_owned())),
            ],
        );
        let mut transaction = ScriptedTransaction {
            rows: vec![row],
            ran: Vec::new(),
        };
        let refused = rerun_task_in(
            &mut transaction,
            &command,
            &policy,
            &RegisteredTaskCodes::NONE,
            Utc::now(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                refused,
                CallerRerunError::ClientFamilyMismatch {
                    stored: RecordedFamily::Recorded(ClientFamily::Python)
                }
            ),
            "{refused:?}"
        );
        assert_eq!(transaction.ran.len(), 1);
    }

    /// A family value that is not a client family ends the rerun with the
    /// named error and its stored value, before the write; no panic.
    #[tokio::test]
    async fn a_family_value_that_is_no_client_family_is_refused_before_the_write() {
        let command = command();
        let policy = RerunEnqueuePolicy::standard(true);
        let read = rerun_read(&command, &policy);
        let row = source_read_row(
            &read,
            &[
                ("class_registered", StatementValue::Bool(true)),
                ("client_family", StatementValue::Text("GO".to_owned())),
            ],
        );
        let mut transaction = ScriptedTransaction {
            rows: vec![row],
            ran: Vec::new(),
        };
        let refused = rerun_task_in(
            &mut transaction,
            &command,
            &policy,
            &RegisteredTaskCodes::NONE,
            Utc::now(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                &refused,
                CallerRerunError::UnknownStoredFamily(UnknownStoredFamily { stored })
                    if stored == "GO"
            ),
            "{refused:?}"
        );
        assert_eq!(transaction.ran.len(), 1);
    }

    #[tokio::test]
    async fn an_unregistered_class_is_refused_before_the_write() {
        let command = command();
        let policy =
            RerunEnqueuePolicy::new("missing_class", true, chrono::Duration::hours(24)).unwrap();
        let read = rerun_read(&command, &policy);
        let row = source_read_row(
            &read,
            &[
                ("class_registered", StatementValue::Bool(false)),
                ("client_family", StatementValue::Text("RUST".to_owned())),
            ],
        );
        let mut transaction = ScriptedTransaction {
            rows: vec![row],
            ran: Vec::new(),
        };
        let refused = rerun_task_in(
            &mut transaction,
            &command,
            &policy,
            &RegisteredTaskCodes::NONE,
            Utc::now(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&refused, CallerRerunError::ClassNotRegistered { key } if key == "missing_class"),
            "{refused:?}"
        );
    }

    #[tokio::test]
    async fn a_driver_failure_is_the_driver_error() {
        let mut transaction = ScriptedTransaction {
            rows: Vec::new(),
            ran: Vec::new(),
        };
        let refused = rerun_task_in(
            &mut transaction,
            &command(),
            &RerunEnqueuePolicy::standard(true),
            &RegisteredTaskCodes::NONE,
            Utc::now(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(refused, CallerRerunError::Driver(ScriptedFailure)),
            "{refused:?}"
        );
    }

    #[tokio::test]
    async fn a_row_that_does_not_fit_the_read_is_a_row_error() {
        let mut transaction = ScriptedTransaction {
            rows: vec![CallerRow::new(vec![StatementValue::Bool(true)])],
            ran: Vec::new(),
        };
        let refused = rerun_task_in(
            &mut transaction,
            &command(),
            &RerunEnqueuePolicy::standard(true),
            &RegisteredTaskCodes::NONE,
            Utc::now(),
        )
        .await
        .unwrap_err();
        assert!(matches!(refused, CallerRerunError::Row(_)), "{refused:?}");
    }
}

//! Running core statements (`crate::db`) and reading their rows: the
//! statement log, checkout and run helpers, the row helpers and the readers
//! of the broker's row types. Each reader takes the columns by name, with
//! the Rust type of the field.

use crate::broker::error::BrokerError;
use crate::broker::health::{ListedWorkerStateRow, StoredWorkerState};
use crate::broker::row::task::{ExpiredTaskRow, StaleTaskRow, TaskAttemptRow, TaskInfoRow};
use crate::broker::task_lists::{ListEnd, ListRow};
use crate::db::{
    BoundStatement, CheckoutClass, CorePool, DiscardReason, FromSql, NotSentCause, PartitionDdl,
    PooledConnection, RollbackOutcome, Row, StatementBound, StatementFailure, Transaction,
    TypedStatement,
};

/// Whether the broker logs each core statement it runs
/// (`PostgresConfig::echo`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatementLog {
    Off,
    /// One INFO event per statement on the target `horsies.sql`: the SQL
    /// text is the message, the statement name is the field `statement`.
    /// Bound values are never logged. INFO is the level of Python's `echo`
    /// (SQLAlchemy's `sqlalchemy.engine.Engine` records).
    Info,
}

impl StatementLog {
    pub(crate) fn from_echo(echo: bool) -> Self {
        match echo {
            true => Self::Info,
            false => Self::Off,
        }
    }

    /// Logs one statement before it runs.
    pub(crate) fn log(self, statement: &TypedStatement) {
        match self {
            Self::Off => {}
            Self::Info => {
                let sql = statement.sql();
                tracing::info!(target: "horsies.sql", statement = statement.name(), "{sql}");
            }
        }
    }

    /// Logs one partition DDL statement before it runs.
    pub(crate) fn log_ddl(self, ddl: &PartitionDdl) {
        match self {
            Self::Off => {}
            Self::Info => {
                let sql = ddl.sql();
                tracing::info!(target: "horsies.sql", statement = ddl.name(), "{sql}");
            }
        }
    }
}

/// Checks out a connection of `pool` for broker work whose first statement
/// is `first`. Broker work is never reserved work, so the checkout passes the
/// reserve gate ([`CheckoutClass::Batch`]).
///
/// Errors: the failed checkout as `first`'s `NotSent`.
pub(crate) async fn checkout_batch(
    pool: &CorePool,
    first: &TypedStatement,
) -> Result<PooledConnection, BrokerError> {
    pool.checkout(CheckoutClass::Batch).await.map_err(|cause| {
        BrokerError::Statement(StatementFailure::NotSent {
            statement: first.name(),
            cause: NotSentCause::Checkout(cause),
        })
    })
}

/// Runs one statement on a checked-out connection and reads every row,
/// bounded by the connection's own timeouts.
pub(crate) async fn query_on(
    connection: &mut PooledConnection,
    log: StatementLog,
    statement: &BoundStatement<'_>,
) -> Result<Vec<Row>, StatementFailure> {
    log.log(statement.statement());
    connection
        .query(statement, StatementBound::ConnectionTimeouts)
        .await
}

/// Runs one statement in a transaction and reads every row. Bound as
/// [`query_on`].
pub(crate) async fn query_in(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
    statement: &BoundStatement<'_>,
) -> Result<Vec<Row>, StatementFailure> {
    log.log(statement.statement());
    transaction
        .query(statement, StatementBound::ConnectionTimeouts)
        .await
}

/// Runs one statement in a transaction and reads the count of affected
/// rows. Bound as [`query_on`].
pub(crate) async fn execute_in(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
    statement: &BoundStatement<'_>,
) -> Result<u64, StatementFailure> {
    log.log(statement.statement());
    transaction
        .execute(statement, StatementBound::ConnectionTimeouts)
        .await
}

/// Rolls back a transaction whose statements all succeeded. Nothing of it
/// commits in any outcome.
///
/// Errors: the failed `ROLLBACK` as [`BrokerError::Statement`] (the
/// connection was discarded; the server rolls the transaction back when the
/// socket closes).
///
/// Panics when the transaction had a failed statement: every caller returns
/// at a failed statement, so its rollback never meets one.
pub(crate) async fn roll_back(transaction: Transaction<'_>) -> Result<(), BrokerError> {
    match transaction.rollback().await {
        RollbackOutcome::RolledBack => Ok(()),
        RollbackOutcome::ConnectionDiscarded(DiscardReason::RollbackFailed(failure)) => {
            Err(BrokerError::Statement(failure))
        }
        RollbackOutcome::ConnectionDiscarded(DiscardReason::EarlierFailure) => unreachable!(
            "a broker transaction was rolled back after a failed statement; every caller \
             returns at the failed statement"
        ),
    }
}

/// A row of a core statement that does not have the shape its reader
/// requires. The same statement meets it again, so it is not retryable.
#[derive(Debug, thiserror::Error)]
pub enum RowDecodeError {
    /// The statement returned no row where its reader requires one.
    #[error("statement `{statement}` returned no row")]
    NoRow { statement: &'static str },
    /// A column is missing, or its value does not decode into the reader's
    /// Rust type.
    #[error("statement `{statement}`: column `{column}` did not decode: {cause}")]
    Column {
        statement: &'static str,
        column: &'static str,
        cause: tokio_postgres::Error,
    },
    /// The rows break the result contract of the SQL function the statement
    /// calls (an unknown row kind, a missing end row).
    #[error("statement `{statement}` returned rows outside its contract: {reason}")]
    Shape {
        statement: &'static str,
        reason: String,
    },
}

/// Decodes one column of a row of `statement`.
pub(crate) fn column<'row, T: FromSql<'row>>(
    row: &'row Row,
    statement: &TypedStatement,
    column: &'static str,
) -> Result<T, RowDecodeError> {
    row.try_get(column).map_err(|cause| RowDecodeError::Column {
        statement: statement.name(),
        column,
        cause,
    })
}

/// The name a [`RowDecodeError::Column`] gives the first column of a
/// statement that returns one unnamed value.
const FIRST_COLUMN: &str = "(first column)";

/// Decodes the first column of a row of `statement`: the one value of a
/// statement such as `SELECT to_regprocedure($1) IS NOT NULL`.
pub(crate) fn scalar<'row, T: FromSql<'row>>(
    row: &'row Row,
    statement: &TypedStatement,
) -> Result<T, RowDecodeError> {
    row.try_get(0_usize)
        .map_err(|cause| RowDecodeError::Column {
            statement: statement.name(),
            column: FIRST_COLUMN,
            cause,
        })
}

/// The first row, when the reader takes zero or one row (the rest is not
/// read).
pub(crate) fn first_row(rows: Vec<Row>) -> Option<Row> {
    rows.into_iter().next()
}

/// The first row, when the reader requires one.
///
/// Errors: [`RowDecodeError::NoRow`] when the statement returned no row.
pub(crate) fn one_row(rows: Vec<Row>, statement: &TypedStatement) -> Result<Row, RowDecodeError> {
    first_row(rows).ok_or(RowDecodeError::NoRow {
        statement: statement.name(),
    })
}

/// Reads one row of a list function: `row_kind` `TASK` through `task`, or
/// the `LIST_END` row from `list_state` and `row_bound`.
fn list_row<T>(
    row: &Row,
    statement: &TypedStatement,
    task: fn(&Row, &TypedStatement) -> Result<T, RowDecodeError>,
) -> Result<ListRow<T>, RowDecodeError> {
    let shape = |reason: String| RowDecodeError::Shape {
        statement: statement.name(),
        reason,
    };
    let kind: String = column(row, statement, "row_kind")?;
    match kind.as_str() {
        "TASK" => Ok(ListRow::Task(task(row, statement)?)),
        "LIST_END" => {
            let state: String = column(row, statement, "list_state")?;
            match state.as_str() {
                "COMPLETE" => Ok(ListRow::End(ListEnd::Complete)),
                "TRUNCATED" => {
                    let bound: i32 = column(row, statement, "row_bound")?;
                    let bound = usize::try_from(bound)
                        .map_err(|_| shape(format!("row_bound {bound} is negative")))?;
                    Ok(ListRow::End(ListEnd::Truncated { bound }))
                }
                other => Err(shape(format!("unknown list_state {other:?}"))),
            }
        }
        other => Err(shape(format!("unknown row_kind {other:?}"))),
    }
}

/// Reads one row of `horsies_list_stale_tasks`.
pub(crate) fn stale_list_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<ListRow<StaleTaskRow>, RowDecodeError> {
    list_row(row, statement, stale_task_row)
}

/// Reads one row of `horsies_list_expired_tasks`.
pub(crate) fn expired_list_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<ListRow<ExpiredTaskRow>, RowDecodeError> {
    list_row(row, statement, expired_task_row)
}

/// Reads a [`StaleTaskRow`] from a `TASK` row of the stale list.
fn stale_task_row(row: &Row, statement: &TypedStatement) -> Result<StaleTaskRow, RowDecodeError> {
    Ok(StaleTaskRow {
        task_id: column(row, statement, "task_id")?,
        task_name: column(row, statement, "task_name")?,
        worker_hostname: column(row, statement, "worker_hostname")?,
        worker_pid: column(row, statement, "worker_pid")?,
        worker_process_name: column(row, statement, "worker_process_name")?,
        started_at: column(row, statement, "started_at")?,
        // The output column of `horsies_list_stale_tasks` keeps its schema
        // name `last_heartbeat`.
        last_heartbeat_at: column(row, statement, "last_heartbeat")?,
    })
}

/// Reads an [`ExpiredTaskRow`] from a `TASK` row of the expired list.
fn expired_task_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<ExpiredTaskRow, RowDecodeError> {
    // The statement gives the interval as whole microseconds, in the
    // column `expired_for_us`. A deadline that passed is never negative.
    let expired_for_micros: i64 = column(row, statement, "expired_for_us")?;
    let expired_for = u64::try_from(expired_for_micros)
        .map(std::time::Duration::from_micros)
        .map_err(|_| RowDecodeError::Shape {
            statement: statement.name(),
            reason: format!("expired_for_us {expired_for_micros} is negative"),
        })?;
    Ok(ExpiredTaskRow {
        task_id: column(row, statement, "task_id")?,
        task_name: column(row, statement, "task_name")?,
        queue_name: column(row, statement, "queue_name")?,
        priority: column(row, statement, "priority")?,
        sent_at: column(row, statement, "sent_at")?,
        enqueued_at: column(row, statement, "enqueued_at")?,
        good_until: column(row, statement, "good_until")?,
        expired_for,
    })
}

/// Reads a [`TaskInfoRow`].
pub(crate) fn task_info_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<TaskInfoRow, RowDecodeError> {
    Ok(TaskInfoRow {
        id: column(row, statement, "id")?,
        task_name: column(row, statement, "task_name")?,
        status: column(row, statement, "status")?,
        queue_name: column(row, statement, "queue_name")?,
        priority: column(row, statement, "priority")?,
        retry_count: column(row, statement, "retry_count")?,
        max_retries: column(row, statement, "max_retries")?,
        next_retry_at: column(row, statement, "next_retry_at")?,
        sent_at: column(row, statement, "sent_at")?,
        enqueued_at: column(row, statement, "enqueued_at")?,
        claimed_at: column(row, statement, "claimed_at")?,
        started_at: column(row, statement, "started_at")?,
        completed_at: column(row, statement, "completed_at")?,
        failed_at: column(row, statement, "failed_at")?,
        worker_hostname: column(row, statement, "worker_hostname")?,
        worker_pid: column(row, statement, "worker_pid")?,
        worker_process_name: column(row, statement, "worker_process_name")?,
        error_code: column(row, statement, "error_code")?,
        failed_reason: column(row, statement, "failed_reason")?,
        execution: column(row, statement, "execution")?,
    })
}

/// Reads a [`TaskAttemptRow`].
pub(crate) fn task_attempt_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<TaskAttemptRow, RowDecodeError> {
    Ok(TaskAttemptRow {
        task_id: column(row, statement, "task_id")?,
        attempt: column(row, statement, "attempt")?,
        outcome: column(row, statement, "outcome")?,
        will_retry: column(row, statement, "will_retry")?,
        started_at: column(row, statement, "started_at")?,
        finished_at: column(row, statement, "finished_at")?,
        error_code: column(row, statement, "error_code")?,
        error_message: column(row, statement, "error_message")?,
        failed_reason: column(row, statement, "failed_reason")?,
        worker_id: column(row, statement, "worker_id")?,
        worker_hostname: column(row, statement, "worker_hostname")?,
        worker_pid: column(row, statement, "worker_pid")?,
        worker_process_name: column(row, statement, "worker_process_name")?,
    })
}

/// Reads a [`StoredWorkerState`]: the stored row, before
/// [`StoredWorkerState::into_snapshot`] reads its JSON columns.
pub(crate) fn stored_worker_state(
    row: &Row,
    statement: &TypedStatement,
) -> Result<StoredWorkerState, RowDecodeError> {
    Ok(StoredWorkerState {
        worker_id: column(row, statement, "worker_id")?,
        snapshot_at: column(row, statement, "snapshot_at")?,
        hostname: column(row, statement, "hostname")?,
        pid: column(row, statement, "pid")?,
        processes: column(row, statement, "processes")?,
        max_claim_batch: column(row, statement, "max_claim_batch")?,
        max_claim_per_worker: column(row, statement, "max_claim_per_worker")?,
        cluster_wide_cap: column(row, statement, "cluster_wide_cap")?,
        queues: column(row, statement, "queues")?,
        queue_priorities: column(row, statement, "queue_priorities")?,
        queue_max_concurrency: column(row, statement, "queue_max_concurrency")?,
        recovery_config: column(row, statement, "recovery_config")?,
        tasks_running: column(row, statement, "tasks_running")?,
        tasks_claimed: column(row, statement, "tasks_claimed")?,
        memory_usage_mb: column(row, statement, "memory_usage_mb")?,
        memory_percent: column(row, statement, "memory_percent")?,
        cpu_percent: column(row, statement, "cpu_percent")?,
        children_memory_mb: column(row, statement, "children_memory_mb")?,
        worker_started_at: column(row, statement, "worker_started_at")?,
        slots_down: column(row, statement, "slots_down")?,
        class_capacity: column(row, statement, "class_capacity")?,
        profile_name: column(row, statement, "profile_name")?,
    })
}

/// Reads a [`ListedWorkerStateRow`]: a row of the worker list statement,
/// with its `read_at`.
pub(crate) fn listed_worker_state_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<ListedWorkerStateRow, RowDecodeError> {
    Ok(ListedWorkerStateRow {
        stored: stored_worker_state(row, statement)?,
        read_at: column(row, statement, "read_at")?,
    })
}

/// Reads every row with `read`; the first row that does not read is the
/// error.
pub(crate) fn read_rows<T>(
    rows: &[Row],
    statement: &TypedStatement,
    read: fn(&Row, &TypedStatement) -> Result<T, RowDecodeError>,
) -> Result<Vec<T>, RowDecodeError> {
    rows.iter().map(|row| read(row, statement)).collect()
}

#[cfg(test)]
mod tests;

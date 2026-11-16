//! Running core statements (`crate::db`) and reading their rows: the
//! statement log, the row helpers and the readers of the broker's row types.
//! Each reader takes the columns by name, with the Rust type the sqlx
//! `FromRow` of the same type reads.

use crate::broker::health::WorkerStateSnapshot;
use crate::broker::row::task::{ExpiredTaskRow, StaleTaskRow, TaskResultRow};
use crate::db::{FromSql, Row, TypedStatement};

/// Whether the broker logs each core statement it runs
/// (`PostgresConfig::echo`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatementLog {
    Off,
    /// Statement name and SQL text at debug level, target `horsies.sql`.
    Debug,
}

impl StatementLog {
    pub(crate) fn from_echo(echo: bool) -> Self {
        match echo {
            true => Self::Debug,
            false => Self::Off,
        }
    }

    /// Logs one statement before it runs.
    pub(crate) fn log(self, statement: &TypedStatement) {
        match self {
            Self::Off => {}
            Self::Debug => tracing::debug!(
                target: "horsies.sql",
                statement = statement.name(),
                sql = statement.sql(),
                "statement",
            ),
        }
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
/// statement such as `SELECT to_regprocedure($1) IS NOT NULL`, as sqlx's
/// `query_scalar`.
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
/// read, as sqlx's `fetch_optional`).
pub(crate) fn first_row(rows: Vec<Row>) -> Option<Row> {
    rows.into_iter().next()
}

/// The first row, when the reader requires one (as sqlx's `fetch_one`).
///
/// Errors: [`RowDecodeError::NoRow`] when the statement returned no row.
pub(crate) fn one_row(rows: Vec<Row>, statement: &TypedStatement) -> Result<Row, RowDecodeError> {
    first_row(rows).ok_or(RowDecodeError::NoRow {
        statement: statement.name(),
    })
}

/// Reads a [`StaleTaskRow`].
pub(crate) fn stale_task_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<StaleTaskRow, RowDecodeError> {
    Ok(StaleTaskRow {
        id: column(row, statement, "id")?,
        task_name: column(row, statement, "task_name")?,
        worker_hostname: column(row, statement, "worker_hostname")?,
        worker_pid: column(row, statement, "worker_pid")?,
        worker_process_name: column(row, statement, "worker_process_name")?,
        started_at: column(row, statement, "started_at")?,
        last_heartbeat: column(row, statement, "last_heartbeat")?,
    })
}

/// Reads an [`ExpiredTaskRow`].
pub(crate) fn expired_task_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<ExpiredTaskRow, RowDecodeError> {
    Ok(ExpiredTaskRow {
        id: column(row, statement, "id")?,
        task_name: column(row, statement, "task_name")?,
        queue_name: column(row, statement, "queue_name")?,
        priority: column(row, statement, "priority")?,
        sent_at: column(row, statement, "sent_at")?,
        enqueued_at: column(row, statement, "enqueued_at")?,
        good_until: column(row, statement, "good_until")?,
        expired_for_ms: column(row, statement, "expired_for_ms")?,
    })
}

/// Reads a [`TaskResultRow`].
pub(crate) fn task_result_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<TaskResultRow, RowDecodeError> {
    Ok(TaskResultRow {
        id: column(row, statement, "id")?,
        status: column(row, statement, "status")?,
        result: column(row, statement, "result")?,
        failed_reason: column(row, statement, "failed_reason")?,
    })
}

/// Reads a [`WorkerStateSnapshot`].
pub(crate) fn worker_state_snapshot(
    row: &Row,
    statement: &TypedStatement,
) -> Result<WorkerStateSnapshot, RowDecodeError> {
    Ok(WorkerStateSnapshot {
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
        worker_started_at: column(row, statement, "worker_started_at")?,
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

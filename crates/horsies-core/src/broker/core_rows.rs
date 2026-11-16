//! Reading the rows of core statements (`crate::db`): the row helpers and
//! the readers of the broker's row types. Each reader takes the columns by
//! name, with the Rust type the sqlx `FromRow` of the same type reads.

use crate::broker::health::{ListedWorkerStateRow, StoredWorkerState};
use crate::broker::row::task::{ExpiredTaskRow, StaleTaskRow};
use crate::db::{FromSql, Row, TypedStatement};

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

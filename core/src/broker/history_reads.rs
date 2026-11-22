//! The broker's reads of the staged history functions on the statement
//! layer: the result read and the detail read.
//!
//! The SQL text and the decoding are those of `core::history::reads`
//! (`task_result_sql`, `task_detail_sql`, `decode_result_row`,
//! `history_detail_from_wire`); only how the statements run differs. Each
//! read checks out one connection of the given pool with
//! `CheckoutClass::Batch` (broker reads are never reserved work) and runs
//! its statements on it one by one, in autocommit.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::LazyLock;

use uuid::Uuid;

use crate::broker::core_rows::{
    checkout_batch, column, first_row, one_row, query_on, scalar, RowDecodeError, StatementLog,
};
use crate::broker::error::BrokerError;
use crate::core::history::errors::HistoryError;
use crate::core::history::reads::detail::{
    history_detail_from_wire, task_detail_sql, HistoryDetailWireRow, TaskAbsenceReason,
    TaskDetailResult, TASK_ABSENCE_REASON_SQL,
};
use crate::core::history::reads::result::{
    decode_result_row, task_result_function_signature, task_result_sql, ResultWireRow,
    TaskResultRead, FUNCTION_PUBLISHED_SQL, UNDEFINED_FUNCTION,
};
use crate::db::{CorePool, ParameterType, PooledConnection, Row, StatementFailure, TypedStatement};

static TASK_RESULT_SQL: LazyLock<String> = LazyLock::new(task_result_sql);

static TASK_RESULT: LazyLock<TypedStatement> = LazyLock::new(|| {
    TypedStatement::new(
        "task_result_staged",
        TASK_RESULT_SQL.as_str(),
        &[ParameterType::Uuid],
    )
});

static TASK_DETAIL_SQL: LazyLock<String> = LazyLock::new(task_detail_sql);

static TASK_DETAIL: LazyLock<TypedStatement> = LazyLock::new(|| {
    TypedStatement::new(
        "task_detail_staged",
        TASK_DETAIL_SQL.as_str(),
        &[ParameterType::Uuid],
    )
});

static TASK_ABSENCE_REASON: TypedStatement = TypedStatement::new(
    "task_absence_reason",
    TASK_ABSENCE_REASON_SQL,
    &[ParameterType::Uuid],
);

static FUNCTION_PUBLISHED: TypedStatement = TypedStatement::new(
    "staged_function_published",
    FUNCTION_PUBLISHED_SQL,
    &[ParameterType::Text],
);

/// Whether a broker saw the staged result function published. One value per
/// broker; it starts not seen.
///
/// Only a publication probe that finds the function marks it seen. A `42883`
/// refusal of the result read clears it, so the next read probes again.
/// Python: `PostgresBroker._staged_result_reader_published`.
#[derive(Debug, Default)]
pub(crate) struct ResultReaderSeen(AtomicBool);

impl ResultReaderSeen {
    fn is_seen(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    fn mark_seen(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    fn forget(&self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// The result columns of one task through the staged result function.
///
/// Python's rule (`PostgresBroker._probe_result_row`):
/// - While `reader` is not seen, the call first probes the function with
///   `to_regprocedure` on the same connection. An absent function is
///   `NotPublished`, and the result read is not sent. A present function
///   marks `reader` seen.
/// - A `42883` refusal of the result read clears `reader` and returns the
///   refusal, which names the function. So a reader dropped out of band
///   after a call saw it fails one call; the next call probes and answers
///   `NotPublished` until the readers are published again.
///
/// Reads only. Changes only `reader`.
///
/// Errors: [`BrokerError::Statement`], [`BrokerError::RowDecode`], and
/// [`BrokerError::HistoryReadContract`] for a row that breaks the read
/// contract.
pub(crate) async fn read_task_result(
    pool: &CorePool,
    log: StatementLog,
    reader: &ResultReaderSeen,
    task_id: Uuid,
) -> Result<TaskResultRead, BrokerError> {
    let seen = reader.is_seen();
    let first_statement: &TypedStatement = match seen {
        true => &TASK_RESULT,
        false => &FUNCTION_PUBLISHED,
    };
    let mut connection = checkout_batch(pool, first_statement).await?;
    let connection = &mut connection;
    if !seen {
        match function_published(connection, log, &task_result_function_signature()).await? {
            true => reader.mark_seen(),
            false => return Ok(TaskResultRead::NotPublished),
        }
    }
    let read = query_on(
        connection,
        log,
        &TASK_RESULT.bind().value(&task_id).finish(),
    )
    .await;
    match read {
        Ok(rows) => match first_row(rows) {
            None => Ok(TaskResultRead::Absent),
            Some(row) => decode_result_row(task_id, result_wire_row(&row)?)
                .map_err(|error| history_contract(&error)),
        },
        Err(failure) if is_undefined_function(&failure) => {
            reader.forget();
            Err(BrokerError::Statement(failure))
        }
        Err(failure) => Err(BrokerError::Statement(failure)),
    }
}

/// The staged detail of one task: live, history, or absent with its reason.
///
/// Statements: the detail read and, for a task that neither the live table
/// nor the history holds, the absence reason. Reads only. The install and
/// every coverage pass publish the detail reader, so the read sends no
/// publication probe.
///
/// Errors: [`BrokerError::Statement`], [`BrokerError::RowDecode`], and
/// [`BrokerError::HistoryReadContract`] for a row that breaks the read
/// contract. A detail reader removed out of band is a
/// [`BrokerError::Statement`] refusal with SQLSTATE `42883` that names
/// `horsies_task_detail_staged`. The next coverage pass publishes the
/// reader again, and the call after it finds the reader.
pub(crate) async fn read_history_detail(
    pool: &CorePool,
    log: StatementLog,
    task_id: Uuid,
) -> Result<TaskDetailResult, BrokerError> {
    let mut connection = checkout_batch(pool, &TASK_DETAIL).await?;
    let connection = &mut connection;
    let rows = query_on(
        connection,
        log,
        &TASK_DETAIL.bind().value(&task_id).finish(),
    )
    .await?;
    let Some(row) = first_row(rows) else {
        let reason = absence_reason(connection, log, task_id).await?;
        return Ok(TaskDetailResult::Absent { task_id, reason });
    };
    let location: String = column(&row, &TASK_DETAIL, "location")?;
    match location.as_str() {
        "LIVE" => Ok(TaskDetailResult::Live { task_id }),
        "HISTORY" => history_detail_from_wire(history_detail_wire_row(&row)?)
            .map(|detail| TaskDetailResult::History(Box::new(detail)))
            .map_err(|error| history_contract(&error)),
        other => Err(history_contract(&HistoryError::contract(format!(
            "staged detail returned unknown location {other:?}"
        )))),
    }
}

/// Whether the function of a `regprocedure` text exists.
async fn function_published(
    connection: &mut PooledConnection,
    log: StatementLog,
    signature: &str,
) -> Result<bool, BrokerError> {
    let rows = query_on(
        connection,
        log,
        &FUNCTION_PUBLISHED.bind().value(&signature).finish(),
    )
    .await?;
    let row = one_row(rows, &FUNCTION_PUBLISHED)?;
    Ok(scalar(&row, &FUNCTION_PUBLISHED)?)
}

async fn absence_reason(
    connection: &mut PooledConnection,
    log: StatementLog,
    task_id: Uuid,
) -> Result<TaskAbsenceReason, BrokerError> {
    let rows = query_on(
        connection,
        log,
        &TASK_ABSENCE_REASON.bind().value(&task_id).finish(),
    )
    .await?;
    let row = one_row(rows, &TASK_ABSENCE_REASON)?;
    let reason: String = scalar(&row, &TASK_ABSENCE_REASON)?;
    TaskAbsenceReason::parse(&reason).map_err(|error| history_contract(&error))
}

/// A refusal with SQLSTATE `42883` (`undefined_function`).
fn is_undefined_function(failure: &StatementFailure) -> bool {
    match failure {
        StatementFailure::StatementRefused { cause, .. } => {
            cause.server_error().code().code() == UNDEFINED_FUNCTION
        }
        StatementFailure::NotSent { .. } | StatementFailure::OutcomeUnknown { .. } => false,
    }
}

/// A history read that breaks its contract (an unknown location, a missing
/// column of a live row, an attempt snapshot that does not decode).
fn history_contract(error: &HistoryError) -> BrokerError {
    BrokerError::HistoryReadContract(error.to_string())
}

fn result_wire_row(row: &Row) -> Result<ResultWireRow, RowDecodeError> {
    let statement: &TypedStatement = &TASK_RESULT;
    Ok(ResultWireRow {
        location: column(row, statement, "location")?,
        status: column(row, statement, "status")?,
        failed_reason: column(row, statement, "failed_reason")?,
        result_envelope_version: column(row, statement, "result_envelope_version")?,
        result_payload: column(row, statement, "result_payload")?,
        result_digest: column(row, statement, "result_digest")?,
        task_name: column(row, statement, "task_name")?,
    })
}

pub(crate) fn history_detail_wire_row(row: &Row) -> Result<HistoryDetailWireRow, RowDecodeError> {
    let statement: &TypedStatement = &TASK_DETAIL;
    Ok(HistoryDetailWireRow {
        task_id: column(row, statement, "task_id")?,
        task_name: column(row, statement, "task_name")?,
        queue_name: column(row, statement, "queue_name")?,
        priority: column(row, statement, "priority")?,
        status: column(row, statement, "status")?,
        terminalization_kind: column(row, statement, "terminalization_kind")?,
        terminal_at: column(row, statement, "terminal_at")?,
        retention_class_key: column(row, statement, "retention_class_key")?,
        enqueued_at: column(row, statement, "enqueued_at")?,
        created_at: column(row, statement, "created_at")?,
        sent_at: column(row, statement, "sent_at")?,
        claimed_at: column(row, statement, "claimed_at")?,
        started_at: column(row, statement, "started_at")?,
        good_until: column(row, statement, "good_until")?,
        retry_count: column(row, statement, "retry_count")?,
        max_retries: column(row, statement, "max_retries")?,
        last_claimed_worker_id: column(row, statement, "last_claimed_worker_id")?,
        last_worker_hostname: column(row, statement, "last_worker_hostname")?,
        last_worker_pid: column(row, statement, "last_worker_pid")?,
        result_envelope_version: column(row, statement, "result_envelope_version")?,
        result_payload: column(row, statement, "result_payload")?,
        result_digest: column(row, statement, "result_digest")?,
        error_code: column(row, statement, "error_code")?,
        final_failed_reason: column(row, statement, "final_failed_reason")?,
        rerun_of_task_id: column(row, statement, "rerun_of_task_id")?,
        rerun_root_task_id: column(row, statement, "rerun_root_task_id")?,
        workflow_id: column(row, statement, "workflow_id")?,
        is_workflow_task: column(row, statement, "is_workflow_task")?,
        rerun_input_disposition: column(row, statement, "rerun_input_disposition")?,
        rerun_input_version: column(row, statement, "rerun_input_version")?,
        rerun_input_digest: column(row, statement, "rerun_input_digest")?,
        rerun_input_inline: column(row, statement, "rerun_input_inline")?,
        attempt_archive_version: column(row, statement, "attempt_archive_version")?,
        attempt_snapshot: column(row, statement, "attempt_snapshot")?,
        attempt_snapshot_digest: column(row, statement, "attempt_snapshot_digest")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The statements run the SQL text of the history reads.
    #[test]
    fn the_statements_carry_the_history_read_sql() {
        assert_eq!(TASK_RESULT.sql(), task_result_sql());
        assert_eq!(TASK_DETAIL.sql(), task_detail_sql());
        assert_eq!(FUNCTION_PUBLISHED.sql(), FUNCTION_PUBLISHED_SQL);
    }

    /// The absence reason is the one SQL rule of the baseline, not a copy.
    #[test]
    fn the_absence_reason_calls_the_baseline_function() {
        assert_eq!(
            TASK_ABSENCE_REASON.sql(),
            "SELECT horsies_task_absence_reason($1)"
        );
    }
}

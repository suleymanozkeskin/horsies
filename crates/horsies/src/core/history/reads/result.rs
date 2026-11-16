//! One-statement terminal-result read over the staged result function.

use sqlx::{FromRow, PgConnection};
use uuid::Uuid;

use crate::core::history::errors::HistoryError;
use crate::core::history::names::TASK_RESULT_FUNCTION;

/// SQLSTATE `undefined_function`.
const UNDEFINED_FUNCTION: &str = "42883";

/// Result columns of a live task row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTaskResult {
    pub status: String,
    pub result: Option<String>,
    pub failed_reason: Option<String>,
}

/// Canonical result payload of a history row, still encoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryResultPayload {
    /// No canonical payload. A stored digest then belongs to the prior payload.
    Absent,
    Stored {
        payload: Vec<u8>,
        digest: Vec<u8>,
    },
}

/// Result columns of a history row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryTaskResult {
    pub status: String,
    pub failed_reason: Option<String>,
    pub envelope_version: i16,
    pub codec: String,
    pub content_type: String,
    pub payload: HistoryResultPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskResultRead {
    Live(LiveTaskResult),
    History(HistoryTaskResult),
    Absent,
    /// The staged result function is not installed in the current schema.
    NotPublished,
}

#[derive(Debug, Clone, FromRow)]
pub struct ResultWireRow {
    pub location: String,
    pub status: Option<String>,
    pub live_result: Option<String>,
    pub failed_reason: Option<String>,
    pub result_envelope_version: Option<i16>,
    pub result_codec: Option<String>,
    pub result_content_type: Option<String>,
    pub result_payload: Option<Vec<u8>>,
    pub result_digest: Option<Vec<u8>>,
}

/// Reads the result columns of one task in one statement. All probes of the
/// staged function use one snapshot. Reads only.
///
/// A `42883` failure is confirmed with `to_regprocedure`: an absent function
/// is `NotPublished`; a present function returns the original error. Call it
/// outside a transaction block. Inside one, the failed call aborts the
/// transaction and the confirmation returns that database error instead.
pub async fn read_task_result(
    connection: &mut PgConnection,
    task_id: Uuid,
) -> Result<TaskResultRead, HistoryError> {
    let sql = format!(
        "SELECT location, status, live_result, failed_reason,
                result_envelope_version, result_codec, result_content_type,
                result_payload, result_digest
         FROM {TASK_RESULT_FUNCTION}($1)"
    );
    let fetched = sqlx::query_as::<_, ResultWireRow>(&sql)
        .bind(task_id)
        .fetch_optional(&mut *connection)
        .await;
    match fetched {
        Ok(None) => Ok(TaskResultRead::Absent),
        Ok(Some(row)) => decode_result_row(task_id, row),
        Err(error) if is_undefined_function(&error) => {
            match staged_result_published(connection).await? {
                true => Err(HistoryError::Database(error)),
                false => Ok(TaskResultRead::NotPublished),
            }
        }
        Err(error) => Err(HistoryError::Database(error)),
    }
}

pub fn decode_result_row(
    task_id: Uuid,
    row: ResultWireRow,
) -> Result<TaskResultRead, HistoryError> {
    match row.location.as_str() {
        "LIVE" => Ok(TaskResultRead::Live(LiveTaskResult {
            status: required(row.status, "status", task_id)?,
            result: row.live_result,
            failed_reason: row.failed_reason,
        })),
        "HISTORY" => Ok(TaskResultRead::History(HistoryTaskResult {
            status: required(row.status, "status", task_id)?,
            failed_reason: row.failed_reason,
            envelope_version: required(
                row.result_envelope_version,
                "result_envelope_version",
                task_id,
            )?,
            codec: required(row.result_codec, "result_codec", task_id)?,
            content_type: required(row.result_content_type, "result_content_type", task_id)?,
            payload: decode_payload(row.result_payload, row.result_digest, task_id)?,
        })),
        other => Err(HistoryError::contract(format!(
            "staged result for task {task_id} returned unknown location {other:?}"
        ))),
    }
}

fn decode_payload(
    payload: Option<Vec<u8>>,
    digest: Option<Vec<u8>>,
    task_id: Uuid,
) -> Result<HistoryResultPayload, HistoryError> {
    match (payload, digest) {
        (None, _) => Ok(HistoryResultPayload::Absent),
        (Some(payload), Some(digest)) => Ok(HistoryResultPayload::Stored { payload, digest }),
        (Some(_), None) => Err(HistoryError::contract(format!(
            "history result digest is absent for task {task_id}"
        ))),
    }
}

fn required<T>(value: Option<T>, column: &str, task_id: Uuid) -> Result<T, HistoryError> {
    value.ok_or_else(|| {
        HistoryError::contract(format!("staged result for task {task_id} has no {column}"))
    })
}

fn is_undefined_function(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .as_deref()
        == Some(UNDEFINED_FUNCTION)
}

pub async fn staged_result_published(connection: &mut PgConnection) -> Result<bool, HistoryError> {
    Ok(sqlx::query_scalar("SELECT to_regprocedure($1) IS NOT NULL")
        .bind(format!("{TASK_RESULT_FUNCTION}(uuid)"))
        .fetch_one(connection)
        .await?)
}

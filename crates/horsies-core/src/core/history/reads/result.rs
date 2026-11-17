//! The staged result read: its SQL text and the decoding of its row. The
//! read runs on the statement layer (`broker::history_reads::read_task_result`).

use uuid::Uuid;

use crate::core::history::archive::versions::{JSON_CONTENT_TYPE, JSON_UTF8_CODEC};
use crate::core::history::errors::HistoryError;
use crate::core::history::names::TASK_RESULT_FUNCTION;

/// SQLSTATE `undefined_function`.
pub(crate) const UNDEFINED_FUNCTION: &str = "42883";

/// Whether the function named by `$1` (a `regprocedure` text) exists.
pub(crate) const FUNCTION_PUBLISHED_SQL: &str = "SELECT to_regprocedure($1) IS NOT NULL";

/// The staged result read: one row per task id that the live table or the
/// history holds, none otherwise. `$1` is the task id. The columns are those
/// of the RESULT reader that `horsies_render_history_reader` renders
/// (`0008_history_maintenance_functions.sql`).
pub(crate) fn task_result_sql() -> String {
    format!(
        "SELECT location, status, failed_reason,
                result_envelope_version, result_payload, result_digest, task_name
         FROM {TASK_RESULT_FUNCTION}($1)"
    )
}

/// The `regprocedure` text of the staged result function.
pub(crate) fn task_result_function_signature() -> String {
    format!("{TASK_RESULT_FUNCTION}(uuid)")
}

/// The task is in the live table. A live row stores no result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTaskResult {
    pub task_name: String,
    pub status: String,
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
    pub task_name: String,
    pub status: String,
    pub failed_reason: Option<String>,
    pub envelope_version: i16,
    /// Implied by `envelope_version`: the baseline stores the version only.
    pub codec: String,
    /// Implied by `envelope_version`.
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

#[derive(Debug, Clone)]
pub struct ResultWireRow {
    pub location: String,
    pub status: Option<String>,
    pub failed_reason: Option<String>,
    pub result_envelope_version: Option<i16>,
    pub result_payload: Option<Vec<u8>>,
    pub result_digest: Option<Vec<u8>>,
    pub task_name: Option<String>,
}

pub fn decode_result_row(
    task_id: Uuid,
    row: ResultWireRow,
) -> Result<TaskResultRead, HistoryError> {
    match row.location.as_str() {
        "LIVE" => Ok(TaskResultRead::Live(LiveTaskResult {
            task_name: required(row.task_name, "task_name", task_id)?,
            status: required(row.status, "status", task_id)?,
        })),
        "HISTORY" => Ok(TaskResultRead::History(HistoryTaskResult {
            task_name: required(row.task_name, "task_name", task_id)?,
            status: required(row.status, "status", task_id)?,
            failed_reason: row.failed_reason,
            envelope_version: required(
                row.result_envelope_version,
                "result_envelope_version",
                task_id,
            )?,
            // Version 1 implies these; the decode refuses any other version.
            codec: JSON_UTF8_CODEC.to_owned(),
            content_type: JSON_CONTENT_TYPE.to_owned(),
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

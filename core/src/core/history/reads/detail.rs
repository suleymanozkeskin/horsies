//! Full frozen history detail over the staged detail function.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::core::history::archive::attempts::{decode_attempt_snapshot, AttemptRecord};
use crate::core::history::archive::versions::{JSON_CONTENT_TYPE, JSON_UTF8_CODEC};
use crate::core::history::errors::HistoryError;
use crate::core::history::names::TASK_DETAIL_FUNCTION;
use crate::core::types::status::TaskStatus;
#[cfg(test)]
use crate::test_support::sql::{self, CallError, TestConnection};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryTaskDetail {
    pub task_id: Uuid,
    pub task_name: String,
    pub queue_name: String,
    pub priority: i32,
    /// Parsed at the read: a stored text that is not a task status is a
    /// contract error of the read.
    pub status: TaskStatus,
    pub terminalization_kind: String,
    pub terminal_at: DateTime<Utc>,
    pub retention_class_key: String,
    pub enqueued_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub sent_at: Option<DateTime<Utc>>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub good_until: Option<DateTime<Utc>>,
    pub retry_count: i32,
    pub max_retries: i32,
    pub last_claimed_worker_id: Option<String>,
    pub last_worker_hostname: Option<String>,
    pub last_worker_pid: Option<i32>,
    pub result_envelope_version: i16,
    /// Implied by `result_envelope_version`: the baseline stores the version only.
    pub result_codec: String,
    /// Implied by `result_envelope_version`.
    pub result_content_type: String,
    pub result_payload: Option<Vec<u8>>,
    pub result_digest: Option<Vec<u8>>,
    pub error_code: Option<String>,
    pub final_failed_reason: Option<String>,
    pub rerun_of_task_id: Option<Uuid>,
    pub rerun_root_task_id: Option<Uuid>,
    pub workflow_id: Option<Uuid>,
    pub is_workflow_task: bool,
    pub rerun_input_disposition: String,
    pub rerun_input_version: Option<i16>,
    /// Implied by `rerun_input_version`; `None` when no envelope is stored.
    pub rerun_input_codec: Option<String>,
    /// Implied by `rerun_input_version`; `None` when no envelope is stored.
    pub rerun_input_content_type: Option<String>,
    pub rerun_input_digest: Option<Vec<u8>>,
    pub rerun_input_inline: Option<Vec<u8>>,
    pub attempts: Vec<AttemptRecord>,
}

/// What a read knows about a task id that neither the live table nor the
/// task history holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, horsies_core_derive::CodeCatalog)]
#[code_catalog(module = "history", values = "equal_text")]
pub enum TaskAbsenceReason {
    /// The UUIDv7 birth, plus the clock bound, is before the latest upper
    /// bound of a period-class leaf that retention detached: retention can
    /// have removed the task.
    PredatesRetainedHistory,
    /// No detached leaf can have held the task: it did not exist, or its
    /// leaf was removed out of band.
    WithinRetainedHistory,
    /// The id is not a UUIDv7, so it carries no birth.
    NotClassified,
}

impl TaskAbsenceReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PredatesRetainedHistory => "PREDATES_RETAINED_HISTORY",
            Self::WithinRetainedHistory => "WITHIN_RETAINED_HISTORY",
            Self::NotClassified => "NOT_CLASSIFIED",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, HistoryError> {
        match value {
            "PREDATES_RETAINED_HISTORY" => Ok(Self::PredatesRetainedHistory),
            "WITHIN_RETAINED_HISTORY" => Ok(Self::WithinRetainedHistory),
            "NOT_CLASSIFIED" => Ok(Self::NotClassified),
            other => Err(HistoryError::contract(format!(
                "absence classification returned unknown reason {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskDetailResult {
    Live {
        task_id: Uuid,
    },
    History(Box<HistoryTaskDetail>),
    Absent {
        task_id: Uuid,
        reason: TaskAbsenceReason,
    },
}

/// The history columns of a staged detail row, as stored.
pub(crate) struct HistoryDetailWireRow {
    pub(crate) task_id: Uuid,
    pub(crate) task_name: String,
    pub(crate) queue_name: String,
    pub(crate) priority: i32,
    pub(crate) status: String,
    pub(crate) terminalization_kind: String,
    pub(crate) terminal_at: DateTime<Utc>,
    pub(crate) retention_class_key: String,
    pub(crate) enqueued_at: DateTime<Utc>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) sent_at: Option<DateTime<Utc>>,
    pub(crate) claimed_at: Option<DateTime<Utc>>,
    pub(crate) started_at: Option<DateTime<Utc>>,
    pub(crate) good_until: Option<DateTime<Utc>>,
    pub(crate) retry_count: i32,
    pub(crate) max_retries: i32,
    pub(crate) last_claimed_worker_id: Option<String>,
    pub(crate) last_worker_hostname: Option<String>,
    pub(crate) last_worker_pid: Option<i32>,
    pub(crate) result_envelope_version: i16,
    pub(crate) result_payload: Option<Vec<u8>>,
    pub(crate) result_digest: Option<Vec<u8>>,
    pub(crate) error_code: Option<String>,
    pub(crate) final_failed_reason: Option<String>,
    pub(crate) rerun_of_task_id: Option<Uuid>,
    pub(crate) rerun_root_task_id: Option<Uuid>,
    pub(crate) workflow_id: Option<Uuid>,
    pub(crate) is_workflow_task: bool,
    pub(crate) rerun_input_disposition: String,
    pub(crate) rerun_input_version: Option<i16>,
    pub(crate) rerun_input_digest: Option<Vec<u8>>,
    pub(crate) rerun_input_inline: Option<Vec<u8>>,
    pub(crate) attempt_archive_version: i16,
    pub(crate) attempt_snapshot: Vec<u8>,
    pub(crate) attempt_snapshot_digest: Vec<u8>,
}

/// The staged detail read: one row for a live or history task, none
/// otherwise. `$1` is the task id.
pub(crate) fn task_detail_sql() -> String {
    format!(
        "SELECT location, (detail.task_row).*
         FROM {TASK_DETAIL_FUNCTION}($1) AS detail"
    )
}

/// The absence reason of the task id `$1`, as one text column. The rule is
/// `horsies_task_absence_reason` (`0006_history_functions.sql`); the
/// monitoring task detail and the rerun read call the same function.
pub(crate) const TASK_ABSENCE_REASON_SQL: &str = "SELECT horsies_task_absence_reason($1)";

/// The detail read on a test session, with the statements of
/// [`crate::broker::history_reads::read_history_detail`]: the staged detail
/// read and, for a task that neither the live table nor the history holds,
/// the absence reason. The tests that read inside their own transaction call
/// it here; the core reads on its pools.
///
/// Errors: [`CallError`]; `Contract` for a row that breaks the read
/// contract or a history row that does not decode.
#[cfg(test)]
pub(crate) async fn read_task_detail(
    connection: &mut TestConnection,
    task_id: Uuid,
) -> Result<TaskDetailResult, CallError> {
    let detail_sql = task_detail_sql();
    let row = sql::query(&detail_sql)
        .bind(task_id)
        .fetch_optional(&mut *connection)
        .await
        .map_err(CallError::Database)?;
    let Some(row) = row else {
        let reason: String = sql::query_scalar(TASK_ABSENCE_REASON_SQL)
            .bind(task_id)
            .fetch_one(&mut *connection)
            .await
            .map_err(CallError::Database)?;
        return Ok(TaskDetailResult::Absent {
            task_id,
            reason: TaskAbsenceReason::parse(&reason).map_err(CallError::Contract)?,
        });
    };
    let location: String = sql::named(&row, "location").map_err(CallError::Database)?;
    match location.as_str() {
        "LIVE" => Ok(TaskDetailResult::Live { task_id }),
        "HISTORY" => crate::broker::history_reads::history_detail_wire_row(&row)
            .map_err(|error| CallError::Contract(HistoryError::from(error)))
            .and_then(|wire| history_detail_from_wire(wire).map_err(CallError::Contract))
            .map(|detail| TaskDetailResult::History(Box::new(detail))),
        other => Err(CallError::Contract(HistoryError::contract(format!(
            "staged detail returned unknown location {other:?}"
        )))),
    }
}

/// The stored status of a history row as a `TaskStatus`; any other text is a
/// contract error.
fn history_status(task_id: Uuid, value: &str) -> Result<TaskStatus, HistoryError> {
    value.parse::<TaskStatus>().map_err(|_| {
        HistoryError::contract(format!(
            "history row {task_id} has status {}, which is not a task status",
            crate::core::python_text::python_repr(value)
        ))
    })
}

/// The detail of a history row from its stored columns: reads the stored
/// status as a `TaskStatus`, decodes the attempt snapshot (digest first) and
/// adds the codec and content type that the stored versions imply. Pure.
pub(crate) fn history_detail_from_wire(
    row: HistoryDetailWireRow,
) -> Result<HistoryTaskDetail, HistoryError> {
    let status = history_status(row.task_id, &row.status)?;
    // Each retained attempt archive version (1, and 2 from migration 1001)
    // implies the json-utf8 codec and the application/json content type; the
    // decode refuses any other version.
    let attempts = decode_attempt_snapshot(
        row.attempt_archive_version,
        JSON_UTF8_CODEC,
        JSON_CONTENT_TYPE,
        &row.attempt_snapshot,
        &row.attempt_snapshot_digest,
    )?;
    let rerun_input_descriptor =
        |version: Option<i16>, value: &str| version.map(|_| value.to_owned());
    Ok(HistoryTaskDetail {
        task_id: row.task_id,
        task_name: row.task_name,
        queue_name: row.queue_name,
        priority: row.priority,
        status,
        terminalization_kind: row.terminalization_kind,
        terminal_at: row.terminal_at,
        retention_class_key: row.retention_class_key,
        enqueued_at: row.enqueued_at,
        created_at: row.created_at,
        sent_at: row.sent_at,
        claimed_at: row.claimed_at,
        started_at: row.started_at,
        good_until: row.good_until,
        retry_count: row.retry_count,
        max_retries: row.max_retries,
        last_claimed_worker_id: row.last_claimed_worker_id,
        last_worker_hostname: row.last_worker_hostname,
        last_worker_pid: row.last_worker_pid,
        result_envelope_version: row.result_envelope_version,
        result_codec: JSON_UTF8_CODEC.to_owned(),
        result_content_type: JSON_CONTENT_TYPE.to_owned(),
        result_payload: row.result_payload,
        result_digest: row.result_digest,
        error_code: row.error_code,
        final_failed_reason: row.final_failed_reason,
        rerun_of_task_id: row.rerun_of_task_id,
        rerun_root_task_id: row.rerun_root_task_id,
        workflow_id: row.workflow_id,
        is_workflow_task: row.is_workflow_task,
        rerun_input_disposition: row.rerun_input_disposition,
        rerun_input_version: row.rerun_input_version,
        rerun_input_codec: rerun_input_descriptor(row.rerun_input_version, JSON_UTF8_CODEC),
        rerun_input_content_type: rerun_input_descriptor(
            row.rerun_input_version,
            JSON_CONTENT_TYPE,
        ),
        rerun_input_digest: row.rerun_input_digest,
        rerun_input_inline: row.rerun_input_inline,
        attempts,
    })
}

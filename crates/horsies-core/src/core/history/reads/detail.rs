//! Full frozen history detail over the staged detail function.

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgConnection, Row};
use uuid::Uuid;

use crate::core::history::archive::attempts::{decode_attempt_snapshot, AttemptRecord};
use crate::core::history::archive::versions::{JSON_CONTENT_TYPE, JSON_UTF8_CODEC};
use crate::core::history::ddl::classes::PERIOD_CLASS_KIND;
use crate::core::history::errors::HistoryError;
use crate::core::history::names::{LEAF_CATALOG, RETENTION_CLASSES, TASK_DETAIL_FUNCTION};
use crate::core::history::reads::lookup_generation::CLOCK_BOUND_SECONDS;
use crate::core::types::status::TaskStatus;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, horsies_macros::CodeCatalog)]
#[code_catalog(module = "history", base = "str_enum")]
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

    fn parse(value: &str) -> Result<Self, HistoryError> {
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
    History(HistoryTaskDetail),
    Absent {
        task_id: Uuid,
        reason: TaskAbsenceReason,
    },
}

#[derive(FromRow)]
struct HistoryDetailWireRow {
    task_id: Uuid,
    task_name: String,
    queue_name: String,
    priority: i32,
    status: String,
    terminalization_kind: String,
    terminal_at: DateTime<Utc>,
    retention_class_key: String,
    enqueued_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    sent_at: Option<DateTime<Utc>>,
    claimed_at: Option<DateTime<Utc>>,
    started_at: Option<DateTime<Utc>>,
    good_until: Option<DateTime<Utc>>,
    retry_count: i32,
    max_retries: i32,
    last_claimed_worker_id: Option<String>,
    last_worker_hostname: Option<String>,
    last_worker_pid: Option<i32>,
    result_envelope_version: i16,
    result_payload: Option<Vec<u8>>,
    result_digest: Option<Vec<u8>>,
    error_code: Option<String>,
    final_failed_reason: Option<String>,
    rerun_of_task_id: Option<Uuid>,
    rerun_root_task_id: Option<Uuid>,
    workflow_id: Option<Uuid>,
    is_workflow_task: bool,
    rerun_input_disposition: String,
    rerun_input_version: Option<i16>,
    rerun_input_digest: Option<Vec<u8>>,
    rerun_input_inline: Option<Vec<u8>>,
    attempt_archive_version: i16,
    attempt_snapshot: Vec<u8>,
    attempt_snapshot_digest: Vec<u8>,
}

pub async fn read_task_detail(
    connection: &mut PgConnection,
    task_id: Uuid,
) -> Result<TaskDetailResult, HistoryError> {
    let sql = format!(
        "SELECT location, (detail.task_row).*
         FROM {TASK_DETAIL_FUNCTION}($1) AS detail"
    );
    let Some(row) = sqlx::query(&sql)
        .bind(task_id)
        .fetch_optional(&mut *connection)
        .await?
    else {
        return Ok(TaskDetailResult::Absent {
            task_id,
            reason: classify_absence(connection, task_id).await?,
        });
    };
    let location: String = row.try_get("location")?;
    match location.as_str() {
        "LIVE" => Ok(TaskDetailResult::Live { task_id }),
        "HISTORY" => decode_history_detail(&row).map(TaskDetailResult::History),
        other => Err(HistoryError::contract(format!(
            "staged detail returned unknown location {other:?}"
        ))),
    }
}

/// The stored status of a history row as a `TaskStatus`; any other text is a
/// contract error.
fn history_status(task_id: Uuid, value: &str) -> Result<TaskStatus, HistoryError> {
    value.parse::<TaskStatus>().map_err(|_| {
        HistoryError::contract(format!(
            "history row {task_id} has status {value:?}, which is not a task status"
        ))
    })
}

fn decode_history_detail(row: &sqlx::postgres::PgRow) -> Result<HistoryTaskDetail, HistoryError> {
    let row = HistoryDetailWireRow::from_row(row)?;
    let status = history_status(row.task_id, &row.status)?;
    // Archive format version 1 implies the json-utf8 codec and the
    // application/json content type; the decode refuses any other version.
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

/// The absence reason of the id bound to `task_id_parameter` (for example
/// `$1`), as one SQL expression. The record of what retention removed is the
/// detached period-class leaves: a v7 birth before the latest upper bound of
/// such a leaf, plus the clock bound, can have been in a removed leaf. A leaf
/// destroyed out of band is not detached in the catalog, so its tasks answer
/// `WITHIN_RETAINED_HISTORY`. The monitoring task detail and the rerun read
/// share this one rule.
pub(crate) fn task_absence_reason_sql(task_id_parameter: &str) -> String {
    format!(
        "CASE
             WHEN horsies_uuid7_birth({task_id_parameter}) IS NULL THEN 'NOT_CLASSIFIED'
             WHEN horsies_uuid7_birth({task_id_parameter}) < (
                 SELECT max(removed.upper_anchor)
                 FROM {LEAF_CATALOG} AS removed
                 JOIN {RETENTION_CLASSES} AS removed_class
                   ON removed_class.class_key = removed.class_key
                 WHERE removed_class.class_kind = '{PERIOD_CLASS_KIND}'
                   AND removed.detached_at IS NOT NULL
             ) + interval '{CLOCK_BOUND_SECONDS} seconds' THEN 'PREDATES_RETAINED_HISTORY'
             ELSE 'WITHIN_RETAINED_HISTORY'
         END"
    )
}

/// Reads only; one statement.
async fn classify_absence(
    connection: &mut PgConnection,
    task_id: Uuid,
) -> Result<TaskAbsenceReason, HistoryError> {
    let reason: String = sqlx::query_scalar(&format!("SELECT {}", task_absence_reason_sql("$1")))
        .bind(task_id)
        .fetch_one(connection)
        .await?;
    TaskAbsenceReason::parse(&reason)
}

pub async fn staged_detail_published(connection: &mut PgConnection) -> Result<bool, HistoryError> {
    Ok(sqlx::query_scalar("SELECT to_regprocedure($1) IS NOT NULL")
        .bind(format!("{TASK_DETAIL_FUNCTION}(uuid)"))
        .fetch_one(connection)
        .await?)
}

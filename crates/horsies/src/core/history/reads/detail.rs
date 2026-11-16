//! Full frozen history detail over the staged detail function.

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgConnection, Row};
use uuid::Uuid;

use crate::core::history::archive::attempts::{decode_attempt_snapshot, AttemptRecord};
use crate::core::history::errors::HistoryError;
use crate::core::history::identity::uuid7::uuid7_birth_at;
use crate::core::history::names::TASK_DETAIL_FUNCTION;
use crate::core::history::partitions::catalog::read_attached_birth_floor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryTaskDetail {
    pub task_id: Uuid,
    pub task_name: String,
    pub queue_name: String,
    pub priority: i32,
    pub status: String,
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
    pub result_codec: String,
    pub result_content_type: String,
    pub result_payload: Option<Vec<u8>>,
    pub prior_result_payload: Option<Vec<u8>>,
    pub result_digest: Option<Vec<u8>>,
    pub error_code: Option<String>,
    pub final_failed_reason: Option<String>,
    pub rerun_of_task_id: Option<Uuid>,
    pub rerun_root_task_id: Option<Uuid>,
    pub workflow_id: Option<Uuid>,
    pub is_workflow_task: bool,
    pub rerun_input_disposition: String,
    pub rerun_input_version: Option<i16>,
    pub rerun_input_codec: Option<String>,
    pub rerun_input_content_type: Option<String>,
    pub rerun_input_digest: Option<Vec<u8>>,
    pub rerun_input_inline: Option<Vec<u8>>,
    pub rerun_input_reference: Option<String>,
    pub attempts: Vec<AttemptRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskDetailResult {
    Live {
        task_id: Uuid,
    },
    History(HistoryTaskDetail),
    Absent {
        task_id: Uuid,
        predates_retained_floor: Option<bool>,
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
    result_codec: String,
    result_content_type: String,
    result_payload: Option<Vec<u8>>,
    prior_result_payload: Option<Vec<u8>>,
    result_digest: Option<Vec<u8>>,
    error_code: Option<String>,
    final_failed_reason: Option<String>,
    rerun_of_task_id: Option<Uuid>,
    rerun_root_task_id: Option<Uuid>,
    workflow_id: Option<Uuid>,
    is_workflow_task: bool,
    rerun_input_disposition: String,
    rerun_input_version: Option<i16>,
    rerun_input_codec: Option<String>,
    rerun_input_content_type: Option<String>,
    rerun_input_digest: Option<Vec<u8>>,
    rerun_input_inline: Option<Vec<u8>>,
    rerun_input_reference: Option<String>,
    attempt_archive_version: i16,
    attempt_snapshot_codec: String,
    attempt_snapshot_content_type: String,
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
            predates_retained_floor: classify_absence(connection, task_id).await?,
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

fn decode_history_detail(row: &sqlx::postgres::PgRow) -> Result<HistoryTaskDetail, HistoryError> {
    let row = HistoryDetailWireRow::from_row(row)?;
    let attempts = decode_attempt_snapshot(
        row.attempt_archive_version,
        &row.attempt_snapshot_codec,
        &row.attempt_snapshot_content_type,
        &row.attempt_snapshot,
        &row.attempt_snapshot_digest,
    )?;
    Ok(HistoryTaskDetail {
        task_id: row.task_id,
        task_name: row.task_name,
        queue_name: row.queue_name,
        priority: row.priority,
        status: row.status,
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
        result_codec: row.result_codec,
        result_content_type: row.result_content_type,
        result_payload: row.result_payload,
        prior_result_payload: row.prior_result_payload,
        result_digest: row.result_digest,
        error_code: row.error_code,
        final_failed_reason: row.final_failed_reason,
        rerun_of_task_id: row.rerun_of_task_id,
        rerun_root_task_id: row.rerun_root_task_id,
        workflow_id: row.workflow_id,
        is_workflow_task: row.is_workflow_task,
        rerun_input_disposition: row.rerun_input_disposition,
        rerun_input_version: row.rerun_input_version,
        rerun_input_codec: row.rerun_input_codec,
        rerun_input_content_type: row.rerun_input_content_type,
        rerun_input_digest: row.rerun_input_digest,
        rerun_input_inline: row.rerun_input_inline,
        rerun_input_reference: row.rerun_input_reference,
        attempts,
    })
}

async fn classify_absence(
    connection: &mut PgConnection,
    task_id: Uuid,
) -> Result<Option<bool>, HistoryError> {
    let Some(birth) = uuid7_birth_at(task_id) else {
        return Ok(None);
    };
    let Some(floor) = read_attached_birth_floor(connection).await? else {
        return Ok(None);
    };
    Ok(Some(birth < floor))
}

pub async fn staged_detail_published(connection: &mut PgConnection) -> Result<bool, HistoryError> {
    Ok(sqlx::query_scalar("SELECT to_regprocedure($1) IS NOT NULL")
        .bind(format!("{TASK_DETAIL_FUNCTION}(uuid)"))
        .fetch_one(connection)
        .await?)
}

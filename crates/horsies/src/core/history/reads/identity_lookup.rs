//! One-statement typed identity lookup over the staged reader.

use sqlx::{FromRow, PgConnection};
use uuid::Uuid;

use crate::core::history::errors::HistoryError;
use crate::core::history::names::TASK_LOOKUP_FUNCTION;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskIdentity {
    task_id: Uuid,
    fingerprint_version: i16,
    command_fingerprint: Vec<u8>,
}

impl TaskIdentity {
    pub fn task_id(&self) -> Uuid {
        self.task_id
    }

    pub fn fingerprint_version(&self) -> i16 {
        self.fingerprint_version
    }

    pub fn command_fingerprint(&self) -> &[u8] {
        &self.command_fingerprint
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskIdentityLookup {
    Live(TaskIdentity),
    History(TaskIdentity),
    Absent,
}

#[derive(Debug, Clone, FromRow)]
pub struct LookupWireRow {
    pub found: bool,
    pub location: Option<String>,
    pub task_id: Option<Uuid>,
    pub fingerprint_version: Option<i16>,
    pub command_fingerprint: Option<Vec<u8>>,
}

pub async fn lookup_task_identity(
    connection: &mut PgConnection,
    task_id: Uuid,
) -> Result<TaskIdentityLookup, HistoryError> {
    let sql = format!(
        "SELECT found, location, task_id, fingerprint_version,
                command_fingerprint
         FROM {TASK_LOOKUP_FUNCTION}($1)"
    );
    let row: LookupWireRow = sqlx::query_as(&sql)
        .bind(task_id)
        .fetch_one(connection)
        .await?;
    decode_lookup_row(row)
}

pub fn decode_lookup_row(row: LookupWireRow) -> Result<TaskIdentityLookup, HistoryError> {
    if !row.found {
        if row.location.is_some() || row.task_id.is_some() {
            return Err(HistoryError::contract(
                "absent lookup row carried location or identity values",
            ));
        }
        return Ok(TaskIdentityLookup::Absent);
    }
    let location = row
        .location
        .ok_or_else(|| HistoryError::contract("found lookup row did not decode"))?;
    let identity = TaskIdentity {
        task_id: row
            .task_id
            .ok_or_else(|| HistoryError::contract("found lookup row did not decode"))?,
        fingerprint_version: row
            .fingerprint_version
            .ok_or_else(|| HistoryError::contract("found lookup row did not decode"))?,
        command_fingerprint: row
            .command_fingerprint
            .ok_or_else(|| HistoryError::contract("found lookup row did not decode"))?,
    };
    match location.as_str() {
        "LIVE" => Ok(TaskIdentityLookup::Live(identity)),
        "HISTORY" => Ok(TaskIdentityLookup::History(identity)),
        other => Err(HistoryError::contract(format!(
            "lookup row carried unknown location {other:?}"
        ))),
    }
}

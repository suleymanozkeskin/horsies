//! Transactional archive-maintenance session primitives.

use sqlx::PgConnection;
use uuid::Uuid;

pub const ARCHIVE_ACCESS_GATE: &str = "horsies_archive_access_gate";
pub const ARCHIVE_MAINTENANCE_SESSIONS: &str = "horsies_archive_maintenance_sessions";
pub const ARCHIVE_AVAILABILITY_FUNCTION: &str = "horsies_assert_archive_available";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceSession {
    pub session_id: Uuid,
}

#[derive(Debug, thiserror::Error)]
pub enum MaintenanceSessionError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("archive maintenance is already active")]
    AlreadyActive,

    #[error("archive maintenance session is not active")]
    NotActive,

    #[error("archive maintenance state has more than one active session")]
    MultipleActive,
}

pub async fn lock_archive_gate_row(
    connection: &mut PgConnection,
) -> Result<(), MaintenanceSessionError> {
    let sql = format!(
        "SELECT singleton FROM {ARCHIVE_ACCESS_GATE}
         WHERE singleton IS TRUE FOR UPDATE"
    );
    let singleton: bool = sqlx::query_scalar(&sql).fetch_one(connection).await?;
    if !singleton {
        return Err(MaintenanceSessionError::NotActive);
    }
    Ok(())
}

pub async fn active_maintenance_session(
    connection: &mut PgConnection,
) -> Result<Option<Uuid>, MaintenanceSessionError> {
    let sql = format!(
        "SELECT session_id FROM {ARCHIVE_MAINTENANCE_SESSIONS}
         WHERE ended_at IS NULL"
    );
    let sessions: Vec<Uuid> = sqlx::query_scalar(&sql).fetch_all(connection).await?;
    match sessions.as_slice() {
        [] => Ok(None),
        [session_id] => Ok(Some(*session_id)),
        _ => Err(MaintenanceSessionError::MultipleActive),
    }
}

pub async fn begin_archive_maintenance(
    connection: &mut PgConnection,
    session_id: Uuid,
) -> Result<MaintenanceSession, MaintenanceSessionError> {
    lock_archive_gate_row(connection).await?;
    if active_maintenance_session(connection).await?.is_some() {
        return Err(MaintenanceSessionError::AlreadyActive);
    }
    let sql = format!(
        "INSERT INTO {ARCHIVE_MAINTENANCE_SESSIONS} (session_id, started_at)
         VALUES ($1, statement_timestamp())"
    );
    sqlx::query(&sql)
        .bind(session_id)
        .execute(connection)
        .await?;
    Ok(MaintenanceSession { session_id })
}

pub async fn finish_archive_maintenance(
    connection: &mut PgConnection,
    session_id: Uuid,
) -> Result<(), MaintenanceSessionError> {
    lock_archive_gate_row(connection).await?;
    let sql = format!(
        "UPDATE {ARCHIVE_MAINTENANCE_SESSIONS}
         SET ended_at = statement_timestamp()
         WHERE session_id = $1 AND ended_at IS NULL RETURNING session_id"
    );
    let ended: Option<Uuid> = sqlx::query_scalar(&sql)
        .bind(session_id)
        .fetch_optional(connection)
        .await?;
    if ended.is_none() {
        return Err(MaintenanceSessionError::NotActive);
    }
    Ok(())
}

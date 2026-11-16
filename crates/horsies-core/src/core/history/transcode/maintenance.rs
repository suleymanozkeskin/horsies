//! Real archive-gate sessions and the serialized transcode program lock.

use sqlx::PgConnection;
use uuid::Uuid;

use crate::core::history::maintenance::gate::{
    active_maintenance_session as read_active_session, begin_archive_maintenance,
    finish_archive_maintenance, lock_archive_gate_row, MaintenanceSession,
};

use super::jobs::TRANSCODE_JOBS;
use super::TranscodeError;

pub const PROGRAM_LOCK_SEED: i64 = 7412;
pub const PROGRAM_LOCK_NAME: &str = "horsies_archive_transcode_program";

pub async fn lock_transcode_program(connection: &mut PgConnection) -> Result<(), TranscodeError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, $2))")
        .bind(PROGRAM_LOCK_NAME)
        .bind(PROGRAM_LOCK_SEED)
        .execute(connection)
        .await?;
    Ok(())
}

pub async fn begin_transcode_maintenance(
    connection: &mut PgConnection,
    session_id: Uuid,
) -> Result<MaintenanceSession, TranscodeError> {
    lock_transcode_program(&mut *connection).await?;
    Ok(begin_archive_maintenance(connection, session_id).await?)
}

pub async fn finish_transcode_maintenance(
    connection: &mut PgConnection,
    session_id: Uuid,
) -> Result<(), TranscodeError> {
    lock_transcode_program(&mut *connection).await?;
    lock_archive_gate_row(&mut *connection).await?;
    let unfinished: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {TRANSCODE_JOBS}
         WHERE maintenance_session_id = $1 AND state <> 'COMPLETE'"
    ))
    .bind(session_id)
    .fetch_one(&mut *connection)
    .await?;
    if unfinished != 0 {
        return Err(TranscodeError::state(
            "archive maintenance has an unfinished replacement job",
        ));
    }
    Ok(finish_archive_maintenance(connection, session_id).await?)
}

pub async fn active_maintenance_session(
    connection: &mut PgConnection,
) -> Result<Option<Uuid>, TranscodeError> {
    Ok(read_active_session(connection).await?)
}

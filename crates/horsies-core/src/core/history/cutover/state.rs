//! Durable distinction between an applied journal and a validated cutover.

use sqlx::PgConnection;

use crate::core::history::errors::HistoryError;

pub const CUTOVER_STATE_TABLE: &str = "horsies_cutover_state";
pub const CUTOVER_NAME: &str = "task_history_v1_validated_v1";

pub async fn cutover_complete(connection: &mut PgConnection) -> Result<bool, HistoryError> {
    let table_exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(CUTOVER_STATE_TABLE)
        .fetch_one(&mut *connection)
        .await?;
    if !table_exists {
        return Ok(false);
    }
    Ok(sqlx::query_scalar(&format!(
        "SELECT EXISTS (SELECT 1 FROM {CUTOVER_STATE_TABLE} WHERE cutover_name = $1)"
    ))
    .bind(CUTOVER_NAME)
    .fetch_one(connection)
    .await?)
}

pub(crate) async fn mark_complete(connection: &mut PgConnection) -> Result<(), HistoryError> {
    sqlx::query(&format!(
        "INSERT INTO {CUTOVER_STATE_TABLE} (cutover_name) VALUES ($1) \
         ON CONFLICT (cutover_name) DO NOTHING"
    ))
    .bind(CUTOVER_NAME)
    .execute(connection)
    .await?;
    Ok(())
}

pub(crate) async fn clear_complete(connection: &mut PgConnection) -> Result<(), HistoryError> {
    sqlx::query(&format!(
        "DELETE FROM {CUTOVER_STATE_TABLE} WHERE cutover_name = $1"
    ))
    .bind(CUTOVER_NAME)
    .execute(connection)
    .await?;
    Ok(())
}

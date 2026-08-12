//! Database-keyed advisory locks for one leaf.

use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use crate::core::history::errors::HistoryError;
use crate::core::history::names::LEAF_LOCK_KEY_FUNCTION;

fn validate(class_key: &str) -> Result<(), HistoryError> {
    if class_key.is_empty() {
        Err(HistoryError::contract("class key must be non-empty"))
    } else {
        Ok(())
    }
}

pub async fn lock_leaf_for_transaction(
    connection: &mut PgConnection,
    class_key: &str,
    anchor: DateTime<Utc>,
) -> Result<(), HistoryError> {
    validate(class_key)?;
    let sql = format!("SELECT pg_advisory_xact_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    sqlx::query(&sql)
        .bind(class_key)
        .bind(anchor)
        .execute(connection)
        .await?;
    Ok(())
}

pub async fn lock_leaf_for_session(
    connection: &mut PgConnection,
    class_key: &str,
    anchor: DateTime<Utc>,
) -> Result<(), HistoryError> {
    validate(class_key)?;
    let sql = format!("SELECT pg_advisory_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    sqlx::query(&sql)
        .bind(class_key)
        .bind(anchor)
        .execute(connection)
        .await?;
    Ok(())
}

pub async fn unlock_leaf_for_session(
    connection: &mut PgConnection,
    class_key: &str,
    anchor: DateTime<Utc>,
) -> Result<(), HistoryError> {
    validate(class_key)?;
    let sql = format!("SELECT pg_advisory_unlock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let released: bool = sqlx::query_scalar(&sql)
        .bind(class_key)
        .bind(anchor)
        .fetch_one(connection)
        .await?;
    if !released {
        return Err(HistoryError::LeafLockNotHeld);
    }
    Ok(())
}

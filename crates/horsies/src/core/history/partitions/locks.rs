//! Database-keyed advisory locks for one leaf.

use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use crate::core::history::commands::is_safe_identifier;
use crate::core::history::errors::HistoryError;
use crate::core::history::names::LEAF_LOCK_KEY_FUNCTION;

fn validate(class_key: &str) -> Result<(), HistoryError> {
    if class_key.is_empty() {
        Err(HistoryError::contract("class key must be non-empty"))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafLockAttempt {
    Acquired,
    Busy,
}

pub async fn try_lock_leaf_for_transaction(
    connection: &mut PgConnection,
    class_key: &str,
    anchor: DateTime<Utc>,
) -> Result<LeafLockAttempt, HistoryError> {
    validate(class_key)?;
    let sql = format!("SELECT pg_try_advisory_xact_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let acquired: bool = sqlx::query_scalar(&sql)
        .bind(class_key)
        .bind(anchor)
        .fetch_one(connection)
        .await?;
    Ok(if acquired {
        LeafLockAttempt::Acquired
    } else {
        LeafLockAttempt::Busy
    })
}

pub async fn try_lock_leaf_for_session(
    connection: &mut PgConnection,
    class_key: &str,
    anchor: DateTime<Utc>,
) -> Result<LeafLockAttempt, HistoryError> {
    validate(class_key)?;
    let sql = format!("SELECT pg_try_advisory_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let acquired: bool = sqlx::query_scalar(&sql)
        .bind(class_key)
        .bind(anchor)
        .fetch_one(connection)
        .await?;
    Ok(if acquired {
        LeafLockAttempt::Acquired
    } else {
        LeafLockAttempt::Busy
    })
}

pub async fn try_lock_relation_exclusive_for_transaction(
    connection: &mut PgConnection,
    relation_name: &str,
) -> Result<LeafLockAttempt, HistoryError> {
    if !is_safe_identifier(relation_name) {
        return Err(HistoryError::contract(
            "relation lock name must be a safe identifier",
        ));
    }
    sqlx::query("SAVEPOINT horsies_leaf_relation_lock")
        .execute(&mut *connection)
        .await?;
    let statement = format!("LOCK TABLE {relation_name} IN ACCESS EXCLUSIVE MODE NOWAIT");
    match sqlx::query(&statement).execute(&mut *connection).await {
        Ok(_) => {
            sqlx::query("RELEASE SAVEPOINT horsies_leaf_relation_lock")
                .execute(connection)
                .await?;
            Ok(LeafLockAttempt::Acquired)
        }
        Err(error) if is_lock_not_available(&error) => {
            sqlx::query("ROLLBACK TO SAVEPOINT horsies_leaf_relation_lock")
                .execute(&mut *connection)
                .await?;
            sqlx::query("RELEASE SAVEPOINT horsies_leaf_relation_lock")
                .execute(connection)
                .await?;
            Ok(LeafLockAttempt::Busy)
        }
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn is_lock_not_available(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .as_deref()
        == Some("55P03")
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

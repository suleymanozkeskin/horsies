//! Database-keyed advisory locks for one leaf.

use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use crate::broker::core_rows::{one_row, query_on, scalar, StatementLog};
use crate::core::history::commands::is_safe_identifier;
use crate::core::history::errors::HistoryError;
use crate::core::history::names::LEAF_LOCK_KEY_FUNCTION;
use crate::db::{ParameterType, PooledConnection, TypedStatement};

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

/// `pg_advisory_unlock` of the session leaf lock: `$1` the class key, `$2`
/// the lower anchor of the leaf.
pub(crate) static UNLOCK_LEAF_SESSION_LOCK: TypedStatement = TypedStatement::new(
    "unlock_leaf_session_lock",
    "SELECT pg_advisory_unlock(horsies_task_history_leaf_lock_key($1, $2))",
    &[ParameterType::Text, ParameterType::Timestamptz],
);

/// Releases the session leaf lock of (`class_key`, `anchor`) on a core
/// connection: one statement in autocommit.
///
/// Errors: `HistoryError::Contract` for an empty class key (nothing sent);
/// `HistoryError::Statement` when the statement failed;
/// `HistoryError::RowDecode` for a reply of another shape;
/// `HistoryError::LeafLockNotHeld` when this session did not hold the lock.
pub(crate) async fn unlock_leaf_in_session(
    connection: &mut PooledConnection,
    log: StatementLog,
    class_key: &str,
    anchor: DateTime<Utc>,
) -> Result<(), HistoryError> {
    validate(class_key)?;
    let statement = &UNLOCK_LEAF_SESSION_LOCK;
    let rows = query_on(
        connection,
        log,
        &statement.bind().value(&class_key).value(&anchor).finish(),
    )
    .await?;
    let released: bool = scalar(&one_row(rows, statement)?, statement)?;
    match released {
        true => Ok(()),
        false => Err(HistoryError::LeafLockNotHeld),
    }
}

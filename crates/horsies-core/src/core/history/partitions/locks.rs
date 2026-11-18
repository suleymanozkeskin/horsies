//! Database-keyed advisory locks for one leaf.

use chrono::{DateTime, Utc};

use crate::broker::core_rows::{one_row, query_on, scalar, StatementLog};
use crate::core::history::errors::HistoryError;
use crate::db::{ParameterType, PooledConnection, TypedStatement};

fn validate(class_key: &str) -> Result<(), HistoryError> {
    if class_key.is_empty() {
        Err(HistoryError::contract("class key must be non-empty"))
    } else {
        Ok(())
    }
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

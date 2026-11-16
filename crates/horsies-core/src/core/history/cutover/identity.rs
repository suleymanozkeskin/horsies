//! Reversible attempt-identity normalization required by the move program.

use sqlx::PgConnection;

use crate::core::history::errors::HistoryError;
use crate::core::history::names::LIVE_ATTEMPTS;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptIdentityNormalization {
    AlreadyUuid,
    Converted,
    Refused { reasons: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptIdentityRestoration {
    AlreadyVarchar,
    Restored,
    Refused { reasons: Vec<String> },
}

pub const ATTEMPTS_TASK_FK: &str = "horsies_task_attempts_task_id_fkey";

async fn attempt_foreign_keys(
    connection: &mut PgConnection,
) -> Result<Vec<(String, bool)>, HistoryError> {
    Ok(sqlx::query_as(
        "SELECT con.conname,
                con.conname = $2
                AND con.confrelid = 'horsies_tasks'::regclass
                AND con.confdeltype = 'c'
                AND con.conkey = ARRAY[(
                    SELECT attnum FROM pg_attribute
                    WHERE attrelid = con.conrelid AND attname = 'task_id'
                )]::smallint[]
                AND con.confkey = ARRAY[(
                    SELECT attnum FROM pg_attribute
                    WHERE attrelid = con.confrelid AND attname = 'id'
                )]::smallint[] AS canonical
         FROM pg_constraint AS con
         WHERE con.conrelid = CAST($1 AS regclass) AND con.contype = 'f'
         ORDER BY con.conname",
    )
    .bind(LIVE_ATTEMPTS)
    .bind(ATTEMPTS_TASK_FK)
    .fetch_all(connection)
    .await?)
}

async fn attempts_identity_type(connection: &mut PgConnection) -> Result<String, HistoryError> {
    Ok(sqlx::query_scalar(
        "SELECT format_type(atttypid, atttypmod)
         FROM pg_attribute
         WHERE attrelid = CAST($1 AS regclass) AND attname = 'task_id'",
    )
    .bind(LIVE_ATTEMPTS)
    .fetch_one(connection)
    .await?)
}

pub async fn attempts_identity_is_uuid(
    connection: &mut PgConnection,
) -> Result<bool, HistoryError> {
    Ok(sqlx::query_scalar(
        "SELECT atttypid = 'uuid'::regtype
         FROM pg_attribute
         WHERE attrelid = CAST($1 AS regclass) AND attname = 'task_id'",
    )
    .bind(LIVE_ATTEMPTS)
    .fetch_one(connection)
    .await?)
}

pub async fn normalize_attempt_identity(
    connection: &mut PgConnection,
) -> Result<AttemptIdentityNormalization, HistoryError> {
    if attempts_identity_is_uuid(connection).await? {
        return Ok(AttemptIdentityNormalization::AlreadyUuid);
    }
    let foreign_keys = attempt_foreign_keys(&mut *connection).await?;
    match foreign_keys.as_slice() {
        [(name, true)] if name == ATTEMPTS_TASK_FK => {}
        rows => {
            return Ok(AttemptIdentityNormalization::Refused {
                reasons: vec![format!(
                    "attempt identity normalization requires exactly the canonical {ATTEMPTS_TASK_FK} foreign key; found {:?}",
                    rows.iter().map(|(name, canonical)| (name, canonical)).collect::<Vec<_>>()
                )],
            });
        }
    }
    sqlx::query(&format!(
        "ALTER TABLE {LIVE_ATTEMPTS} DROP CONSTRAINT {ATTEMPTS_TASK_FK}"
    ))
    .execute(&mut *connection)
    .await?;
    sqlx::query(&format!(
        "ALTER TABLE {LIVE_ATTEMPTS} ALTER COLUMN task_id TYPE uuid USING task_id::uuid"
    ))
    .execute(connection)
    .await?;
    Ok(AttemptIdentityNormalization::Converted)
}

pub async fn restore_attempt_identity(
    connection: &mut PgConnection,
) -> Result<AttemptIdentityRestoration, HistoryError> {
    let identity_type = attempts_identity_type(&mut *connection).await?;
    let foreign_keys = attempt_foreign_keys(&mut *connection).await?;
    match identity_type.as_str() {
        "character varying(36)" => match foreign_keys.as_slice() {
            [(name, true)] if name == ATTEMPTS_TASK_FK => {
                Ok(AttemptIdentityRestoration::AlreadyVarchar)
            }
            rows => Ok(AttemptIdentityRestoration::Refused {
                reasons: vec![format!(
                    "attempt identity rollback requires exactly the canonical {ATTEMPTS_TASK_FK} foreign key for an existing varchar identity; found {:?}",
                    rows.iter().map(|(name, canonical)| (name, canonical)).collect::<Vec<_>>()
                )],
            }),
        },
        "uuid" => {
            if !foreign_keys.is_empty() {
                return Ok(AttemptIdentityRestoration::Refused {
                    reasons: vec![format!(
                        "attempt identity rollback requires no foreign keys before the inverse cast; found {:?}",
                        foreign_keys
                            .iter()
                            .map(|(name, canonical)| (name, canonical))
                            .collect::<Vec<_>>()
                    )],
                });
            }
            sqlx::query(&format!(
                "ALTER TABLE {LIVE_ATTEMPTS} ALTER COLUMN task_id TYPE varchar(36) USING task_id::text"
            ))
            .execute(&mut *connection)
            .await?;
            sqlx::query(&format!(
                "ALTER TABLE {LIVE_ATTEMPTS} ADD CONSTRAINT {ATTEMPTS_TASK_FK} \
                 FOREIGN KEY (task_id) REFERENCES horsies_tasks(id) ON DELETE CASCADE"
            ))
            .execute(connection)
            .await?;
            Ok(AttemptIdentityRestoration::Restored)
        }
        actual => Ok(AttemptIdentityRestoration::Refused {
            reasons: vec![format!(
                "attempt identity rollback requires uuid or character varying(36), found {actual}"
            )],
        }),
    }
}

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres};

/// A row from `horsies_schedule_state`.
#[derive(Debug, sqlx::FromRow)]
pub struct ScheduleStateRow {
    pub schedule_name: String,
    #[allow(dead_code)] // populated by FromRow for completeness
    pub last_run_at: Option<DateTime<Utc>>,
    pub next_run_at: Option<DateTime<Utc>>,
    #[allow(dead_code)] // populated by FromRow for completeness
    pub last_task_id: Option<String>,
    pub run_count: i32,
    pub config_hash: Option<String>,
}

const UPSERT_STATE_SQL: &str = "\
INSERT INTO horsies_schedule_state (
    schedule_name, last_run_at, next_run_at, last_task_id, run_count, config_hash,
    updated_at
) VALUES ($1, $2, $3, $4, $5, $6, NOW())
ON CONFLICT (schedule_name) DO UPDATE SET
    last_run_at = $2,
    next_run_at = $3,
    last_task_id = $4,
    run_count = $5,
    config_hash = $6,
    updated_at = NOW()";

const GET_STATE_SQL: &str = "\
SELECT schedule_name, last_run_at, next_run_at, last_task_id, run_count, config_hash
FROM horsies_schedule_state
WHERE schedule_name = $1";

/// SQL for filtered due schedules query.
/// Note: The `$2` parameter is an array of schedule names (uses `= ANY($2)`).
/// This matches Python's `get_due_states(schedule_names, now)` which filters
/// to only the provided schedule names at the database level.
const GET_DUE_SCHEDULES_FILTERED_SQL: &str = "\
SELECT schedule_name, last_run_at, next_run_at, last_task_id, run_count, config_hash
FROM horsies_schedule_state
WHERE schedule_name = ANY($2)
  AND next_run_at IS NOT NULL
  AND next_run_at <= $1
ORDER BY next_run_at ASC";

#[allow(dead_code)] // backs delete_state, retained as a state primitive (see e26a0f55).
const DELETE_STATE_SQL: &str = "\
DELETE FROM horsies_schedule_state WHERE schedule_name = $1";

const GET_ALL_STATES_SQL: &str = "\
SELECT schedule_name, last_run_at, next_run_at, last_task_id, run_count, config_hash
FROM horsies_schedule_state
ORDER BY schedule_name ASC";

/// Transaction-scoped blocking advisory lock for the scheduler tick.
/// Automatically released when the transaction commits/rolls back.
/// Matches Python's `SCHEDULE_ADVISORY_LOCK_SQL`.
const SCHEDULER_XACT_LOCK_SQL: &str = "\
SELECT pg_advisory_xact_lock($1)";

const TRY_ACQUIRE_LOCK_SQL: &str = "\
SELECT pg_try_advisory_lock($1)";

const RELEASE_LOCK_SQL: &str = "\
SELECT pg_advisory_unlock($1)";

/// Advisory lock key for the scheduler (constant hash).
pub const SCHEDULER_LOCK_KEY: i64 = 0x0068_6F72_7369_6573; // "horsies" in hex, truncated

/// Upsert a schedule state row.
pub async fn upsert_state(
    pool: &PgPool,
    schedule_name: &str,
    last_run_at: Option<DateTime<Utc>>,
    next_run_at: Option<DateTime<Utc>>,
    last_task_id: Option<&str>,
    run_count: i32,
    config_hash: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(UPSERT_STATE_SQL)
        .bind(schedule_name)
        .bind(last_run_at)
        .bind(next_run_at)
        .bind(last_task_id)
        .bind(run_count)
        .bind(config_hash)
        .execute(pool)
        .await?;
    Ok(())
}

/// Get the state for a single schedule.
pub async fn get_state(
    pool: &PgPool,
    schedule_name: &str,
) -> Result<Option<ScheduleStateRow>, sqlx::Error> {
    sqlx::query_as(GET_STATE_SQL)
        .bind(schedule_name)
        .fetch_optional(pool)
        .await
}

/// Get due schedules filtered by a list of schedule names.
///
/// This mirrors Python's `get_due_states(schedule_names, now)` which filters
/// at the database level rather than in application code.
pub async fn get_due_schedules_filtered(
    pool: &PgPool,
    schedule_names: &[String],
    now: DateTime<Utc>,
) -> Result<Vec<ScheduleStateRow>, sqlx::Error> {
    if schedule_names.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_as(GET_DUE_SCHEDULES_FILTERED_SQL)
        .bind(now)
        .bind(schedule_names)
        .fetch_all(pool)
        .await
}

/// Delete the state entry for a schedule.
///
/// Retained as a state primitive but no longer called at startup: foreign
/// schedule-state rows are now kept, not pruned (parity with horsies PR #101
/// e26a0f55), so a rolling deploy / shared DB does not lose another scheduler's
/// rows.
#[allow(dead_code)]
pub async fn delete_state(pool: &PgPool, schedule_name: &str) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(DELETE_STATE_SQL)
        .bind(schedule_name)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Retrieve all schedule state rows, ordered by schedule name.
pub async fn get_all_states(pool: &PgPool) -> Result<Vec<ScheduleStateRow>, sqlx::Error> {
    sqlx::query_as(GET_ALL_STATES_SQL).fetch_all(pool).await
}

/// Acquire the scheduler advisory lock within a transaction (blocking).
///
/// Matches Python's `pg_advisory_xact_lock` pattern: the lock is held for
/// the duration of the transaction and automatically released on commit/rollback.
/// Multiple schedulers block (instead of exiting) until the lock is available.
pub async fn acquire_scheduler_xact_lock(
    tx: &mut sqlx::Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(SCHEDULER_XACT_LOCK_SQL)
        .bind(SCHEDULER_LOCK_KEY)
        .execute(tx.as_mut())
        .await?;
    Ok(())
}

/// Try to acquire a per-schedule advisory lock. Returns Some(conn) if acquired.
pub async fn try_acquire_schedule_lock(
    pool: &PgPool,
    schedule_name: &str,
) -> Result<Option<PoolConnection<Postgres>>, sqlx::Error> {
    let key = schedule_lock_key(schedule_name);
    let mut conn = pool.acquire().await?;
    let result: (bool,) = sqlx::query_as(TRY_ACQUIRE_LOCK_SQL)
        .bind(key)
        .fetch_one(&mut *conn)
        .await?;
    if result.0 {
        Ok(Some(conn))
    } else {
        Ok(None)
    }
}

/// Release a per-schedule advisory lock.
pub async fn release_schedule_lock(
    mut conn: PoolConnection<Postgres>,
    schedule_name: &str,
) -> Result<(), sqlx::Error> {
    let key = schedule_lock_key(schedule_name);
    sqlx::query(RELEASE_LOCK_SQL)
        .bind(key)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

fn schedule_lock_key(schedule_name: &str) -> i64 {
    let basis = format!("horsies-schedule:{}", schedule_name);
    let digest = Sha256::digest(basis.as_bytes());
    let bytes: [u8; 8] = digest[..8].try_into().unwrap_or([0u8; 8]);
    i64::from_be_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_constants_have_placeholders() {
        assert!(UPSERT_STATE_SQL.contains("$1"));
        assert!(GET_STATE_SQL.contains("$1"));
        assert!(GET_DUE_SCHEDULES_FILTERED_SQL.contains("$1"));
        assert!(GET_DUE_SCHEDULES_FILTERED_SQL.contains("$2"));
        assert!(DELETE_STATE_SQL.contains("$1"));
    }

    #[test]
    fn get_due_schedules_filtered_sql_uses_any() {
        assert!(GET_DUE_SCHEDULES_FILTERED_SQL.contains("= ANY($2)"));
        assert!(GET_DUE_SCHEDULES_FILTERED_SQL.contains("next_run_at <= $1"));
        assert!(GET_DUE_SCHEDULES_FILTERED_SQL.contains("ORDER BY next_run_at ASC"));
    }

    #[test]
    fn scheduler_lock_key_is_constant() {
        assert_ne!(SCHEDULER_LOCK_KEY, 0);
    }

    #[test]
    fn get_all_states_sql_is_valid() {
        assert!(GET_ALL_STATES_SQL.contains("schedule_name"));
        assert!(GET_ALL_STATES_SQL.contains("last_run_at"));
        assert!(GET_ALL_STATES_SQL.contains("next_run_at"));
        assert!(GET_ALL_STATES_SQL.contains("run_count"));
        assert!(GET_ALL_STATES_SQL.contains("config_hash"));
        assert!(GET_ALL_STATES_SQL.contains("ORDER BY schedule_name"));
    }

    #[test]
    fn delete_state_sql_targets_single_row() {
        assert!(DELETE_STATE_SQL.starts_with("DELETE FROM horsies_schedule_state"));
        assert!(DELETE_STATE_SQL.contains("WHERE schedule_name = $1"));
    }
}

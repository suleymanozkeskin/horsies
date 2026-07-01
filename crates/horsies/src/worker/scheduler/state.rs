use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};

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

/// Existing schedule-state row names (one PK-column SELECT, no locks). Backs the
/// per-tick self-heal diff.
const GET_EXISTING_NAMES_SQL: &str = "\
SELECT schedule_name FROM horsies_schedule_state";

/// Insert a schedule-state row only if absent. Unlike `UPSERT_STATE_SQL`
/// (`ON CONFLICT DO UPDATE`), a concurrent scheduler that lost the race leaves
/// the winner's row — including its `next_run_at` — untouched.
const INSERT_STATE_IF_ABSENT_SQL: &str = "\
INSERT INTO horsies_schedule_state (
    schedule_name, last_run_at, next_run_at, last_task_id, run_count, config_hash,
    updated_at
) VALUES ($1, $2, $3, $4, $5, $6, NOW())
ON CONFLICT (schedule_name) DO NOTHING";

/// Transaction-scoped per-schedule advisory lock. Auto-released on
/// commit/rollback, so no explicit unlock round-trip exists — under PgBouncer
/// transaction pooling the lock stays on one server backend for its whole
/// lifetime. Matches Python's `SCHEDULE_ADVISORY_LOCK_SQL` scoping (xact),
/// with try-lock skip semantics instead of blocking.
const TRY_ACQUIRE_LOCK_SQL: &str = "\
SELECT pg_try_advisory_xact_lock($1)";

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

/// Names of all existing schedule-state rows (for the per-tick self-heal diff).
pub async fn get_existing_names(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(GET_EXISTING_NAMES_SQL).fetch_all(pool).await
}

/// Insert a schedule-state row only if it does not already exist.
///
/// Returns `true` if a row was inserted, `false` if one already existed (the
/// `ON CONFLICT DO NOTHING` no-op — the existing row, including its
/// `next_run_at`, is preserved).
pub async fn insert_state_if_absent(
    pool: &PgPool,
    schedule_name: &str,
    last_run_at: Option<DateTime<Utc>>,
    next_run_at: Option<DateTime<Utc>>,
    last_task_id: Option<&str>,
    run_count: i32,
    config_hash: Option<&str>,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(INSERT_STATE_IF_ABSENT_SQL)
        .bind(schedule_name)
        .bind(last_run_at)
        .bind(next_run_at)
        .bind(last_task_id)
        .bind(run_count)
        .bind(config_hash)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Try to acquire a per-schedule advisory lock.
///
/// Returns `Some(tx)` if acquired: an otherwise-idle transaction whose only
/// job is to hold the xact-scoped lock while the caller processes the
/// schedule on other connections. Dropping or committing the transaction
/// releases the lock.
pub async fn try_acquire_schedule_lock(
    pool: &PgPool,
    schedule_name: &str,
) -> Result<Option<Transaction<'static, Postgres>>, sqlx::Error> {
    let key = schedule_lock_key(schedule_name);
    let mut tx = pool.begin().await?;
    let result: (bool,) = sqlx::query_as(TRY_ACQUIRE_LOCK_SQL)
        .bind(key)
        .fetch_one(tx.as_mut())
        .await?;
    if result.0 {
        Ok(Some(tx))
    } else {
        Ok(None)
    }
}

/// Release a per-schedule advisory lock by committing its holder transaction.
pub async fn release_schedule_lock(
    tx: Transaction<'static, Postgres>,
) -> Result<(), sqlx::Error> {
    tx.commit().await
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
    fn schedule_lock_sql_is_xact_scoped() {
        assert!(TRY_ACQUIRE_LOCK_SQL.contains("pg_try_advisory_xact_lock($1)"));
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

    #[test]
    fn insert_state_if_absent_sql_does_nothing_on_conflict() {
        assert!(INSERT_STATE_IF_ABSENT_SQL.starts_with("INSERT INTO horsies_schedule_state"));
        assert!(INSERT_STATE_IF_ABSENT_SQL.contains("ON CONFLICT (schedule_name) DO NOTHING"));
        assert!(!INSERT_STATE_IF_ABSENT_SQL.contains("DO UPDATE"));
    }

    fn test_db_url() -> String {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return url;
        }
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let root = std::path::Path::new(manifest_dir)
            .ancestors()
            .find(|p| p.join(".env").exists());
        let pw = root
            .and_then(|r| std::fs::read_to_string(r.join(".env")).ok())
            .and_then(|c| {
                c.lines()
                    .filter_map(|l| l.trim().split_once('='))
                    .find(|(k, _)| k.trim() == "DB_PASSWORD")
                    .map(|(_, v)| v.trim().to_owned())
            })
            .unwrap_or_else(|| "W0rklane".to_owned());
        format!("postgresql://postgres:{pw}@localhost:5432/horsies-rust-port")
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn get_existing_names_returns_inserted_rows() {
        let broker = crate::broker::PostgresBroker::connect(&test_db_url())
            .await
            .expect("connect");
        let pool = broker.pool().clone();
        let name = format!("qw_exist_{}", uuid::Uuid::new_v4());

        upsert_state(&pool, &name, None, None, None, 0, None)
            .await
            .expect("seed row");

        let names = get_existing_names(&pool).await.expect("get_existing_names");
        assert!(names.contains(&name));

        delete_state(&pool, &name).await.expect("cleanup");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn insert_state_if_absent_inserts_then_preserves() {
        let broker = crate::broker::PostgresBroker::connect(&test_db_url())
            .await
            .expect("connect");
        let pool = broker.pool().clone();
        let name = format!("qw_absent_{}", uuid::Uuid::new_v4());
        let first = Utc::now() + chrono::Duration::seconds(60);

        // Absent → inserts.
        let inserted = insert_state_if_absent(&pool, &name, None, Some(first), None, 0, None)
            .await
            .expect("insert");
        assert!(inserted, "first insert must create the row");

        // Present → no-op, existing next_run_at preserved (not overwritten).
        let second = Utc::now() + chrono::Duration::seconds(999);
        let inserted_again = insert_state_if_absent(&pool, &name, None, Some(second), None, 0, None)
            .await
            .expect("insert again");
        assert!(!inserted_again, "second insert must be a no-op");

        let row = get_state(&pool, &name).await.expect("get").expect("exists");
        let stored = row.next_run_at.expect("next_run_at set");
        assert_eq!(
            stored.timestamp(),
            first.timestamp(),
            "existing next_run_at must be preserved, not overwritten",
        );

        delete_state(&pool, &name).await.expect("cleanup");
    }
}

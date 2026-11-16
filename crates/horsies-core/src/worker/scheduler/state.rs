use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

/// A row from `horsies_schedule_state`.
///
/// The table's CHECK ties the run record together: `last_run_at` and
/// `last_task_id` are both set exactly when `run_count` is above 0.
#[derive(Debug, sqlx::FromRow)]
pub struct ScheduleStateRow {
    pub schedule_name: String,
    pub last_run_at: Option<DateTime<Utc>>,
    pub next_run_at: DateTime<Utc>,
    pub last_task_id: Option<Uuid>,
    pub run_count: i32,
    /// SHA-256 of the schedule's pattern and timezone (32 bytes).
    pub config_hash: Vec<u8>,
}

/// The column list of every state read (tests check the reads against it).
#[cfg(test)]
const STATE_COLUMNS: &str =
    "schedule_name, last_run_at, next_run_at, last_task_id, run_count, config_hash";

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

/// Insert the row of a schedule that has none: no run recorded yet. A
/// concurrent scheduler that lost the race leaves the winner's row, including
/// its `next_run_at`, untouched.
const INSERT_STATE_IF_ABSENT_SQL: &str = "\
INSERT INTO horsies_schedule_state (
    schedule_name, last_run_at, next_run_at, last_task_id, run_count, config_hash,
    updated_at
) VALUES ($1, NULL, $2, NULL, 0, $3, statement_timestamp())
ON CONFLICT (schedule_name) DO NOTHING";

/// Move a schedule whose pattern or timezone changed to its new first slot.
/// The run record (`run_count`, `last_run_at`, `last_task_id`) is kept
/// (Python `update_next_run` with a hash). A row that already holds this hash
/// is not written.
const UPDATE_SCHEDULE_CONFIG_SQL: &str = "\
UPDATE horsies_schedule_state
SET next_run_at = $2, config_hash = $3, updated_at = statement_timestamp()
WHERE schedule_name = $1 AND config_hash <> $3";

/// Record a fired slot and move to the next one (Python `update_after_run`).
const RECORD_RUN_SQL: &str = "\
UPDATE horsies_schedule_state
SET last_run_at = $2, next_run_at = $3, last_task_id = $4,
    run_count = run_count + 1, updated_at = statement_timestamp()
WHERE schedule_name = $1";

/// Move to the next slot without recording a run (Python `update_next_run`
/// without a hash): the scan cap of a deep backlog, or a slot skipped after a
/// refusal that repeats.
const ADVANCE_NEXT_RUN_SQL: &str = "\
UPDATE horsies_schedule_state
SET next_run_at = $2, updated_at = statement_timestamp()
WHERE schedule_name = $1";

/// Transaction-scoped per-schedule advisory lock. Auto-released on
/// commit/rollback, so no explicit unlock round-trip exists — under PgBouncer
/// transaction pooling the lock stays on one server backend for its whole
/// lifetime. Matches Python's `SCHEDULE_ADVISORY_LOCK_SQL` scoping (xact),
/// with try-lock skip semantics instead of blocking.
const TRY_ACQUIRE_LOCK_SQL: &str = "\
SELECT pg_try_advisory_xact_lock($1)";

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
    sqlx::query_scalar(GET_EXISTING_NAMES_SQL)
        .fetch_all(pool)
        .await
}

/// Insert the state row of a schedule that has none, with no run recorded.
///
/// Returns `true` if a row was inserted, `false` if one already existed (the
/// `ON CONFLICT DO NOTHING` no-op — the existing row, including its
/// `next_run_at`, is preserved).
pub async fn insert_state_if_absent(
    pool: &PgPool,
    schedule_name: &str,
    next_run_at: DateTime<Utc>,
    config_hash: &[u8],
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(INSERT_STATE_IF_ABSENT_SQL)
        .bind(schedule_name)
        .bind(next_run_at)
        .bind(config_hash)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Store a changed pattern or timezone: the new hash and the new first slot.
/// The run record is kept.
///
/// Returns `true` if the row was written, `false` if the row is missing or
/// already holds this hash.
pub async fn update_schedule_config(
    pool: &PgPool,
    schedule_name: &str,
    next_run_at: DateTime<Utc>,
    config_hash: &[u8],
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(UPDATE_SCHEDULE_CONFIG_SQL)
        .bind(schedule_name)
        .bind(next_run_at)
        .bind(config_hash)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Record a fired slot: `last_run_at`, `last_task_id`, `run_count + 1`, and
/// the next slot. It does not read the row first.
pub async fn record_run(
    pool: &PgPool,
    schedule_name: &str,
    executed_at: DateTime<Utc>,
    next_run_at: DateTime<Utc>,
    task_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(RECORD_RUN_SQL)
        .bind(schedule_name)
        .bind(executed_at)
        .bind(next_run_at)
        .bind(task_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Move to the next slot without recording a run.
pub async fn advance_next_run(
    pool: &PgPool,
    schedule_name: &str,
    next_run_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(ADVANCE_NEXT_RUN_SQL)
        .bind(schedule_name)
        .bind(next_run_at)
        .execute(pool)
        .await?;
    Ok(())
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
pub async fn release_schedule_lock(tx: Transaction<'static, Postgres>) -> Result<(), sqlx::Error> {
    tx.commit().await
}

fn schedule_lock_key(schedule_name: &str) -> i64 {
    let basis = format!("horsies-schedule:{}", schedule_name);
    let digest = Sha256::digest(basis.as_bytes());
    let bytes: [u8; 8] = digest[..8].try_into().unwrap_or([0u8; 8]);
    i64::from_be_bytes(bytes)
}

/// Replace a schedule's state row with a fresh one due at `next_run_at`
/// (tests only: seeds a known state).
#[cfg(test)]
pub(crate) async fn seed_state(
    pool: &PgPool,
    schedule_name: &str,
    next_run_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    delete_state(pool, schedule_name).await?;
    insert_state_if_absent(pool, schedule_name, next_run_at, &[0u8; 32]).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_constants_have_placeholders() {
        assert!(GET_STATE_SQL.contains("$1"));
        assert!(GET_DUE_SCHEDULES_FILTERED_SQL.contains("$1"));
        assert!(GET_DUE_SCHEDULES_FILTERED_SQL.contains("$2"));
        assert!(DELETE_STATE_SQL.contains("$1"));
        for sql in [
            GET_STATE_SQL,
            GET_DUE_SCHEDULES_FILTERED_SQL,
            GET_ALL_STATES_SQL,
        ] {
            assert!(sql.contains(STATE_COLUMNS), "{sql}");
        }
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

    /// Python `update_next_run` with a hash and `update_after_run`: a config
    /// change keeps the run record; a run increments `run_count` in SQL.
    #[test]
    fn config_change_keeps_the_run_record_and_a_run_increments_in_sql() {
        for kept in ["run_count", "last_run_at", "last_task_id"] {
            assert!(!UPDATE_SCHEDULE_CONFIG_SQL.contains(kept), "{kept}");
            assert!(!ADVANCE_NEXT_RUN_SQL.contains(kept), "{kept}");
        }
        assert!(UPDATE_SCHEDULE_CONFIG_SQL.contains("config_hash <> $3"));
        assert!(RECORD_RUN_SQL.contains("run_count = run_count + 1"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn get_existing_names_returns_inserted_rows() {
        let broker = crate::broker::PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::migrated_pool().await,
        );
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let name = format!("qw_exist_{}", uuid::Uuid::new_v4());

        seed_state(&pool, &name, Utc::now())
            .await
            .expect("seed row");

        let names = get_existing_names(&pool).await.expect("get_existing_names");
        assert!(names.contains(&name));

        delete_state(&pool, &name).await.expect("cleanup");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn insert_state_if_absent_inserts_then_preserves() {
        let broker = crate::broker::PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::migrated_pool().await,
        );
        broker.ensure_schema_initialized().await.expect("schema");
        let pool = broker.pool().clone();
        let name = format!("qw_absent_{}", uuid::Uuid::new_v4());
        let first = Utc::now() + chrono::Duration::seconds(60);

        // Absent → inserts.
        let inserted = insert_state_if_absent(&pool, &name, first, &[1u8; 32])
            .await
            .expect("insert");
        assert!(inserted, "first insert must create the row");

        // Present → no-op, existing next_run_at preserved (not overwritten).
        let second = Utc::now() + chrono::Duration::seconds(999);
        let inserted_again = insert_state_if_absent(&pool, &name, second, &[2u8; 32])
            .await
            .expect("insert again");
        assert!(!inserted_again, "second insert must be a no-op");

        let row = get_state(&pool, &name).await.expect("get").expect("exists");
        assert_eq!(
            row.next_run_at.timestamp(),
            first.timestamp(),
            "existing next_run_at must be preserved, not overwritten",
        );
        assert_eq!(row.config_hash, vec![1u8; 32]);
        assert_eq!(row.run_count, 0);

        delete_state(&pool, &name).await.expect("cleanup");
    }

    /// Python `test_scheduler_service.py::test_config_hash_changed_recalculates_next_run`:
    /// a changed hash moves `next_run_at` and keeps `run_count`,
    /// `last_run_at` and `last_task_id`.
    #[tokio::test]
    #[serial_test::serial]
    async fn config_change_moves_next_run_and_keeps_the_run_record() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let name = format!("qw_config_change_{}", uuid::Uuid::new_v4());
        let first_slot = Utc::now() - chrono::Duration::seconds(10);
        seed_state(&pool, &name, first_slot).await.expect("seed");
        let task_id = uuid::Uuid::new_v4();
        let executed_at = Utc::now();
        let after_run = executed_at + chrono::Duration::seconds(60);
        record_run(&pool, &name, executed_at, after_run, task_id)
            .await
            .expect("record run");

        let moved_to = executed_at + chrono::Duration::seconds(3_600);
        let written = update_schedule_config(&pool, &name, moved_to, &[7u8; 32])
            .await
            .expect("config change");
        assert!(written);
        let again = update_schedule_config(&pool, &name, after_run, &[7u8; 32])
            .await
            .expect("same hash");
        assert!(!again, "a row that holds the hash is not written");

        let row = get_state(&pool, &name).await.expect("get").expect("exists");
        assert_eq!(row.next_run_at.timestamp(), moved_to.timestamp());
        assert_eq!(row.config_hash, vec![7u8; 32]);
        assert_eq!(row.run_count, 1);
        assert_eq!(row.last_task_id, Some(task_id));
        assert_eq!(
            row.last_run_at.map(|at| at.timestamp()),
            Some(executed_at.timestamp())
        );

        delete_state(&pool, &name).await.expect("cleanup");
    }
}

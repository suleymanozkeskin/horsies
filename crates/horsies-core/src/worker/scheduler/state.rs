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

/// The due read: the rows of the names in `$1` whose `next_run_at` is at or
/// before the database time, soonest first, with that database time. The
/// one-row join returns the time also when no row is due (one row with a
/// NULL name). Python `ScheduleStateManager.read_due_schedules`.
const DUE_SCHEDULES_SQL: &str = "\
SELECT statement_timestamp() AS database_now, s.schedule_name, s.next_run_at
FROM (VALUES (1)) AS one
LEFT JOIN horsies_schedule_state AS s
    ON s.schedule_name = ANY($1)
   AND s.next_run_at <= statement_timestamp()
ORDER BY s.next_run_at";

#[allow(dead_code)] // backs delete_state, retained as a state primitive (see e26a0f55).
const DELETE_STATE_SQL: &str = "\
DELETE FROM horsies_schedule_state WHERE schedule_name = $1";

/// The start read: every row's name and digest, with the database time. The
/// one-row join returns the time also for an empty table. Python
/// `ScheduleStateManager.read_start_states`.
const START_STATES_SQL: &str = "\
SELECT statement_timestamp() AS database_now, s.schedule_name, s.config_hash
FROM (VALUES (1)) AS one
LEFT JOIN horsies_schedule_state AS s ON TRUE
ORDER BY s.schedule_name";

/// The start write: a new schedule gets its row with no run recorded; a row
/// whose digest differs gets the new `next_run_at` and digest and keeps
/// `run_count`, `last_run_at` and `last_task_id`. The WHERE clause leaves
/// alone a row that a concurrent scheduler already moved to this digest.
/// Python `ScheduleStateManager.store_start_states`.
const STORE_START_STATES_SQL: &str = "\
INSERT INTO horsies_schedule_state (
    schedule_name, last_run_at, next_run_at, last_task_id, run_count, config_hash,
    updated_at
)
SELECT u.schedule_name, NULL, u.next_run_at, NULL, 0, u.config_hash, statement_timestamp()
FROM unnest($1::text[], $2::timestamptz[], $3::bytea[])
    AS u(schedule_name, next_run_at, config_hash)
ON CONFLICT (schedule_name) DO UPDATE
SET next_run_at = EXCLUDED.next_run_at,
    config_hash = EXCLUDED.config_hash,
    updated_at = EXCLUDED.updated_at
WHERE horsies_schedule_state.config_hash <> EXCLUDED.config_hash
RETURNING schedule_name";

/// The existence heal: a row for each name that has none. A row that exists
/// is not written. Python `ScheduleStateManager.heal_missing_states`.
const HEAL_MISSING_STATES_SQL: &str = "\
INSERT INTO horsies_schedule_state (
    schedule_name, last_run_at, next_run_at, last_task_id, run_count, config_hash,
    updated_at
)
SELECT u.schedule_name, NULL, u.next_run_at, NULL, 0, u.config_hash, statement_timestamp()
FROM unnest($1::text[], $2::timestamptz[], $3::bytea[])
    AS u(schedule_name, next_run_at, config_hash)
ON CONFLICT (schedule_name) DO NOTHING
RETURNING schedule_name";

/// Insert the row of a schedule that has none: no run recorded yet. A
/// concurrent scheduler that lost the race leaves the winner's row, including
/// its `next_run_at`, untouched.
const INSERT_STATE_IF_ABSENT_SQL: &str = "\
INSERT INTO horsies_schedule_state (
    schedule_name, last_run_at, next_run_at, last_task_id, run_count, config_hash,
    updated_at
) VALUES ($1, NULL, $2, NULL, 0, $3, statement_timestamp())
ON CONFLICT (schedule_name) DO NOTHING";

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

/// One due row of the due read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueSchedule {
    pub schedule_name: String,
    pub next_run_at: DateTime<Utc>,
}

/// The due rows, soonest first, and the database time of the read. The tick
/// computes slots and the heal's first runs from `database_now`, never from
/// the host clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueScheduleRead {
    pub database_now: DateTime<Utc>,
    pub due: Vec<DueSchedule>,
}

/// The name and stored config digest of one state row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredConfigHash {
    pub schedule_name: String,
    pub config_hash: Vec<u8>,
}

/// Every state row's name and digest, and the database time of the read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartStateRead {
    pub database_now: DateTime<Utc>,
    pub rows: Vec<StoredConfigHash>,
}

/// One row for the start write or the existence heal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleStateWrite {
    pub schedule_name: String,
    pub next_run_at: DateTime<Utc>,
    pub config_hash: Vec<u8>,
}

/// The three parallel arrays that `unnest` reads.
fn write_arrays(writes: &[ScheduleStateWrite]) -> (Vec<String>, Vec<DateTime<Utc>>, Vec<Vec<u8>>) {
    (
        writes
            .iter()
            .map(|write| write.schedule_name.clone())
            .collect(),
        writes.iter().map(|write| write.next_run_at).collect(),
        writes
            .iter()
            .map(|write| write.config_hash.clone())
            .collect(),
    )
}

/// A row that names a column the statement never leaves NULL beside a set
/// name: a broken read, reported as a decode error.
fn partial_row(statement: &str, row: &str) -> sqlx::Error {
    sqlx::Error::Decode(format!("the {statement} returned a partial row: {row}").into())
}

/// Read the due rows of `schedule_names` and the database time, in one
/// statement (Python `read_due_schedules`). Rows of other names are never
/// returned. Writes nothing.
pub async fn read_due_schedules(
    pool: &PgPool,
    schedule_names: &[String],
) -> Result<DueScheduleRead, sqlx::Error> {
    let rows: Vec<(DateTime<Utc>, Option<String>, Option<DateTime<Utc>>)> =
        sqlx::query_as(DUE_SCHEDULES_SQL)
            .bind(schedule_names)
            .fetch_all(pool)
            .await?;
    let database_now = rows
        .first()
        .map(|row| row.0)
        .ok_or(sqlx::Error::RowNotFound)?;
    let mut due = Vec::with_capacity(rows.len());
    for (_, schedule_name, next_run_at) in rows {
        match (schedule_name, next_run_at) {
            (Some(schedule_name), Some(next_run_at)) => due.push(DueSchedule {
                schedule_name,
                next_run_at,
            }),
            (None, None) => {}
            (schedule_name, next_run_at) => {
                return Err(partial_row(
                    "due read",
                    &format!("{schedule_name:?}, {next_run_at:?}"),
                ))
            }
        }
    }
    Ok(DueScheduleRead { database_now, due })
}

/// Read every state row's name and digest and the database time, in one
/// statement (Python `read_start_states`). Writes nothing.
pub async fn read_start_states(pool: &PgPool) -> Result<StartStateRead, sqlx::Error> {
    let rows: Vec<(DateTime<Utc>, Option<String>, Option<Vec<u8>>)> =
        sqlx::query_as(START_STATES_SQL).fetch_all(pool).await?;
    let database_now = rows
        .first()
        .map(|row| row.0)
        .ok_or(sqlx::Error::RowNotFound)?;
    let mut stored = Vec::with_capacity(rows.len());
    for (_, schedule_name, config_hash) in rows {
        match (schedule_name, config_hash) {
            (Some(schedule_name), Some(config_hash)) => stored.push(StoredConfigHash {
                schedule_name,
                config_hash,
            }),
            (None, None) => {}
            (schedule_name, config_hash) => {
                return Err(partial_row(
                    "start read",
                    &format!("{schedule_name:?}, {config_hash:?}"),
                ))
            }
        }
    }
    Ok(StartStateRead {
        database_now,
        rows: stored,
    })
}

/// Write the rows of new and changed schedules in one statement (Python
/// `store_start_states`). A new name gets a row with no run recorded; a row
/// whose digest differs gets the new `next_run_at` and digest and keeps its
/// run record; a row that holds the digest is not written. `updated_at` is
/// the statement's database time. Returns the names of the rows written.
pub async fn store_start_states(
    pool: &PgPool,
    writes: &[ScheduleStateWrite],
) -> Result<Vec<String>, sqlx::Error> {
    if writes.is_empty() {
        return Ok(Vec::new());
    }
    let (names, next_runs, hashes) = write_arrays(writes);
    sqlx::query_scalar(STORE_START_STATES_SQL)
        .bind(names)
        .bind(next_runs)
        .bind(hashes)
        .fetch_all(pool)
        .await
}

/// Create the row of each name that has none, in one statement (Python
/// `heal_missing_states`). A row that exists is not written. `updated_at` is
/// the statement's database time. Returns the names of the rows created.
pub async fn heal_missing_states(
    pool: &PgPool,
    writes: &[ScheduleStateWrite],
) -> Result<Vec<String>, sqlx::Error> {
    if writes.is_empty() {
        return Ok(Vec::new());
    }
    let (names, next_runs, hashes) = write_arrays(writes);
    sqlx::query_scalar(HEAL_MISSING_STATES_SQL)
        .bind(names)
        .bind(next_runs)
        .bind(hashes)
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
        assert!(DELETE_STATE_SQL.contains("$1"));
        assert!(GET_STATE_SQL.contains(STATE_COLUMNS), "{GET_STATE_SQL}");
    }

    /// The due read, the start write and the heal take no time from the
    /// client: each time comes from `statement_timestamp()`.
    #[test]
    fn scheduler_times_come_from_the_database_clock() {
        assert!(DUE_SCHEDULES_SQL.contains("s.schedule_name = ANY($1)"));
        assert!(DUE_SCHEDULES_SQL.contains("s.next_run_at <= statement_timestamp()"));
        assert!(DUE_SCHEDULES_SQL.contains("statement_timestamp() AS database_now"));
        assert!(!DUE_SCHEDULES_SQL.contains("$2"));
        assert!(START_STATES_SQL.contains("statement_timestamp() AS database_now"));
        for sql in [STORE_START_STATES_SQL, HEAL_MISSING_STATES_SQL] {
            assert!(sql.contains("statement_timestamp()"), "{sql}");
            assert!(!sql.contains("$4"), "{sql}");
        }
        assert!(HEAL_MISSING_STATES_SQL.contains("ON CONFLICT (schedule_name) DO NOTHING"));
    }

    #[test]
    fn schedule_lock_sql_is_xact_scoped() {
        assert!(TRY_ACQUIRE_LOCK_SQL.contains("pg_try_advisory_xact_lock($1)"));
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

    /// Python `update_after_run` and `store_start_states`: a config change
    /// keeps the run record; a run increments `run_count` in SQL.
    #[test]
    fn config_change_keeps_the_run_record_and_a_run_increments_in_sql() {
        let update_part = STORE_START_STATES_SQL
            .split("DO UPDATE")
            .nth(1)
            .expect("the start write updates a changed row");
        for kept in ["run_count", "last_run_at", "last_task_id"] {
            assert!(!update_part.contains(kept), "{kept}");
            assert!(!ADVANCE_NEXT_RUN_SQL.contains(kept), "{kept}");
        }
        assert!(update_part
            .contains("WHERE horsies_schedule_state.config_hash <> EXCLUDED.config_hash"));
        assert!(RECORD_RUN_SQL.contains("run_count = run_count + 1"));
    }

    /// The database time, read in its own statement.
    async fn database_time(pool: &PgPool) -> DateTime<Utc> {
        sqlx::query_scalar("SELECT statement_timestamp()")
            .fetch_one(pool)
            .await
            .expect("database time")
    }

    /// Python `tests/integration/test_schedule_state_rows.py`,
    /// `test_the_due_read_returns_the_rows_at_or_before_the_database_time`:
    /// rows due at the database time, soonest first, only of the given
    /// names, and the database time of the read, also when nothing is due.
    #[tokio::test]
    #[serial_test::serial]
    async fn due_read_returns_the_database_time_and_the_rows_due_at_it() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let earlier = format!("qw_due_earlier_{suffix}");
        let now_due = format!("qw_due_now_{suffix}");
        let later = format!("qw_due_later_{suffix}");
        let other = format!("qw_due_other_{suffix}");
        let seeded_at = database_time(&pool).await;
        seed_state(&pool, &earlier, seeded_at - chrono::Duration::seconds(2))
            .await
            .expect("seed earlier");
        seed_state(&pool, &now_due, seeded_at - chrono::Duration::seconds(1))
            .await
            .expect("seed due");
        seed_state(&pool, &later, seeded_at + chrono::Duration::hours(1))
            .await
            .expect("seed later");
        seed_state(&pool, &other, seeded_at - chrono::Duration::seconds(1))
            .await
            .expect("seed other");

        let names = vec![now_due.clone(), earlier.clone(), later.clone()];
        let before = database_time(&pool).await;
        let read = read_due_schedules(&pool, &names).await.expect("due read");
        let after = database_time(&pool).await;
        assert!(before <= read.database_now && read.database_now <= after);
        let due: Vec<&str> = read
            .due
            .iter()
            .map(|row| row.schedule_name.as_str())
            .collect();
        assert_eq!(due, vec![earlier.as_str(), now_due.as_str()]);

        let nothing_due = read_due_schedules(&pool, std::slice::from_ref(&later))
            .await
            .expect("due read with nothing due");
        assert!(nothing_due.due.is_empty());
        assert!(after <= nothing_due.database_now);

        for name in [earlier, now_due, later, other] {
            delete_state(&pool, &name).await.expect("cleanup");
        }
    }

    /// Python `test_schedule_state_rows.py`,
    /// `test_start_stores_new_and_changed_rows_from_the_database_time`: the
    /// start read returns the rows and the database time; the start write
    /// creates a new row, moves a changed row and keeps its run record, and
    /// leaves a row with the same digest alone.
    #[tokio::test]
    #[serial_test::serial]
    async fn start_read_and_write_follow_the_stored_digests() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let new_name = format!("qw_start_new_{suffix}");
        let changed = format!("qw_start_changed_{suffix}");
        let unchanged = format!("qw_start_same_{suffix}");
        let seeded_at = database_time(&pool).await;
        seed_state(&pool, &changed, seeded_at - chrono::Duration::seconds(10))
            .await
            .expect("seed changed");
        let task_id = uuid::Uuid::new_v4();
        record_run(
            &pool,
            &changed,
            seeded_at,
            seeded_at + chrono::Duration::seconds(60),
            task_id,
        )
        .await
        .expect("record run");
        assert!(
            insert_state_if_absent(&pool, &unchanged, seeded_at, &[3u8; 32])
                .await
                .expect("seed unchanged")
        );

        let before = database_time(&pool).await;
        let read = read_start_states(&pool).await.expect("start read");
        assert!(before <= read.database_now);
        assert!(read
            .rows
            .iter()
            .any(|row| row.schedule_name == changed && row.config_hash == vec![0u8; 32]));

        let moved_to = read.database_now + chrono::Duration::seconds(3_600);
        let writes = vec![
            ScheduleStateWrite {
                schedule_name: new_name.clone(),
                next_run_at: moved_to,
                config_hash: vec![5u8; 32],
            },
            ScheduleStateWrite {
                schedule_name: changed.clone(),
                next_run_at: moved_to,
                config_hash: vec![7u8; 32],
            },
            ScheduleStateWrite {
                schedule_name: unchanged.clone(),
                next_run_at: moved_to,
                config_hash: vec![3u8; 32],
            },
        ];
        let mut written = store_start_states(&pool, &writes)
            .await
            .expect("start write");
        written.sort();
        let mut expected = vec![new_name.clone(), changed.clone()];
        expected.sort();
        assert_eq!(written, expected);

        let created = get_state(&pool, &new_name)
            .await
            .expect("get")
            .expect("row");
        assert_eq!(created.next_run_at, moved_to);
        assert_eq!(created.run_count, 0);
        assert_eq!(created.last_run_at, None);
        let moved = get_state(&pool, &changed).await.expect("get").expect("row");
        assert_eq!(moved.next_run_at, moved_to);
        assert_eq!(moved.config_hash, vec![7u8; 32]);
        assert_eq!(moved.run_count, 1);
        assert_eq!(moved.last_task_id, Some(task_id));
        let kept = get_state(&pool, &unchanged)
            .await
            .expect("get")
            .expect("row");
        assert_eq!(kept.next_run_at, seeded_at);

        for name in [new_name, changed, unchanged] {
            delete_state(&pool, &name).await.expect("cleanup");
        }
    }

    /// Python `test_schedule_state_rows.py`,
    /// `test_the_heal_creates_missing_rows_and_leaves_existing_rows`.
    #[tokio::test]
    #[serial_test::serial]
    async fn heal_creates_missing_rows_and_leaves_existing_rows() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let missing = format!("qw_heal_missing_{suffix}");
        let existing = format!("qw_heal_existing_{suffix}");
        let seeded_at = database_time(&pool).await;
        seed_state(&pool, &existing, seeded_at)
            .await
            .expect("seed existing");

        let next_run_at = seeded_at + chrono::Duration::seconds(60);
        let writes = vec![
            ScheduleStateWrite {
                schedule_name: existing.clone(),
                next_run_at,
                config_hash: vec![9u8; 32],
            },
            ScheduleStateWrite {
                schedule_name: missing.clone(),
                next_run_at,
                config_hash: vec![9u8; 32],
            },
        ];
        let healed = heal_missing_states(&pool, &writes).await.expect("heal");
        assert_eq!(healed, vec![missing.clone()]);

        let created = get_state(&pool, &missing).await.expect("get").expect("row");
        assert_eq!(created.next_run_at, next_run_at);
        assert_eq!(created.run_count, 0);
        let untouched = get_state(&pool, &existing)
            .await
            .expect("get")
            .expect("row");
        assert_eq!(untouched.next_run_at, seeded_at);
        assert_eq!(untouched.config_hash, vec![0u8; 32]);

        for name in [missing, existing] {
            delete_state(&pool, &name).await.expect("cleanup");
        }
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
}

use sqlx::PgPool;

use super::test_rows::{delete_state, record_run, seed_state, state_row};
use super::*;
use crate::broker::PostgresBroker;
use crate::core::EnqueueRetention;

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

/// Python `store_start_states`: a config change keeps the run record.
#[test]
fn a_config_change_keeps_the_run_record() {
    let update_part = STORE_START_STATES_SQL
        .split("DO UPDATE")
        .nth(1)
        .expect("the start write updates a changed row");
    for kept in ["run_count", "last_run_at", "last_task_id"] {
        assert!(!update_part.contains(kept), "{kept}");
    }
    assert!(
        update_part.contains("WHERE horsies_schedule_state.config_hash <> EXCLUDED.config_hash")
    );
}

/// The fire sends every argument of `horsies_fire_schedule`, in its order,
/// with one declared type per argument.
#[test]
fn the_fire_names_every_argument_of_the_function() {
    assert!(FIRE_SCHEDULE_SQL.contains("FROM horsies_fire_schedule("));
    assert!(FIRE_SCHEDULE_SQL.contains("$15::bytea"));
    assert!(!FIRE_SCHEDULE_SQL.contains("$16"));
    assert_eq!(FIRE_SCHEDULE.parameter_types().len(), 15);
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
    let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
    let core = crate::broker::pools::core_pool_for_test_pool(&pool);
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let earlier = format!("qw_due_earlier_{suffix}");
    let now_due = format!("qw_due_now_{suffix}");
    let later = format!("qw_due_later_{suffix}");
    let other = format!("qw_due_other_{suffix}");
    let seeded_at = database_time(&pool).await;
    seed_state(&pool, &earlier, seeded_at - chrono::Duration::seconds(2)).await;
    seed_state(&pool, &now_due, seeded_at - chrono::Duration::seconds(1)).await;
    seed_state(&pool, &later, seeded_at + chrono::Duration::hours(1)).await;
    seed_state(&pool, &other, seeded_at - chrono::Duration::seconds(1)).await;

    let names = vec![now_due.clone(), earlier.clone(), later.clone()];
    let before = database_time(&pool).await;
    let read = read_due_schedules(&core, StatementLog::Off, &names)
        .await
        .expect("due read");
    let after = database_time(&pool).await;
    assert!(before <= read.database_now && read.database_now <= after);
    let due: Vec<&str> = read
        .due
        .iter()
        .map(|row| row.schedule_name.as_str())
        .collect();
    assert_eq!(due, vec![earlier.as_str(), now_due.as_str()]);

    let nothing_due = read_due_schedules(&core, StatementLog::Off, std::slice::from_ref(&later))
        .await
        .expect("due read with nothing due");
    assert!(nothing_due.due.is_empty());
    assert!(after <= nothing_due.database_now);

    core.close();
    for name in [earlier, now_due, later, other] {
        delete_state(&pool, &name).await;
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
    let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
    let core = crate::broker::pools::core_pool_for_test_pool(&pool);
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let new_name = format!("qw_start_new_{suffix}");
    let changed = format!("qw_start_changed_{suffix}");
    let unchanged = format!("qw_start_same_{suffix}");
    let seeded_at = database_time(&pool).await;
    seed_state(&pool, &changed, seeded_at - chrono::Duration::seconds(10)).await;
    let task_id = uuid::Uuid::new_v4();
    record_run(
        &pool,
        &changed,
        seeded_at,
        seeded_at + chrono::Duration::seconds(60),
        task_id,
    )
    .await;
    seed_state(&pool, &unchanged, seeded_at).await;
    let _hash =
        sqlx::query("UPDATE horsies_schedule_state SET config_hash = $2 WHERE schedule_name = $1")
            .bind(&unchanged)
            .bind(vec![3_u8; 32])
            .execute(&pool)
            .await
            .expect("set the digest of the unchanged row");

    let before = database_time(&pool).await;
    let read = read_start_states(&core, StatementLog::Off)
        .await
        .expect("start read");
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
    let mut written = store_start_states(&core, StatementLog::Off, &writes)
        .await
        .expect("start write");
    written.sort();
    let mut expected = vec![new_name.clone(), changed.clone()];
    expected.sort();
    assert_eq!(written, expected);

    let created = state_row(&pool, &new_name).await.expect("row");
    assert_eq!(created.next_run_at, moved_to);
    assert_eq!(created.run_count, 0);
    assert_eq!(created.last_run_at, None);
    let moved = state_row(&pool, &changed).await.expect("row");
    assert_eq!(moved.next_run_at, moved_to);
    assert_eq!(moved.config_hash, vec![7u8; 32]);
    assert_eq!(moved.run_count, 1);
    assert_eq!(moved.last_task_id, Some(task_id));
    let kept = state_row(&pool, &unchanged).await.expect("row");
    assert_eq!(kept.next_run_at, seeded_at);

    core.close();
    for name in [new_name, changed, unchanged] {
        delete_state(&pool, &name).await;
    }
}

/// Python `test_schedule_state_rows.py`,
/// `test_the_heal_creates_missing_rows_and_leaves_existing_rows`. A second
/// heal of the same name creates nothing and keeps the first row's
/// `next_run_at` (the old per-row insert, `insert_state_if_absent`, is the
/// heal of one row).
#[tokio::test]
#[serial_test::serial]
async fn heal_creates_missing_rows_and_leaves_existing_rows() {
    let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
    let core = crate::broker::pools::core_pool_for_test_pool(&pool);
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let missing = format!("qw_heal_missing_{suffix}");
    let existing = format!("qw_heal_existing_{suffix}");
    let seeded_at = database_time(&pool).await;
    seed_state(&pool, &existing, seeded_at).await;

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
    let healed = heal_missing_states(&core, StatementLog::Off, &writes)
        .await
        .expect("heal");
    assert_eq!(healed, vec![missing.clone()]);

    let created = state_row(&pool, &missing).await.expect("row");
    assert_eq!(created.next_run_at, next_run_at);
    assert_eq!(created.run_count, 0);
    let untouched = state_row(&pool, &existing).await.expect("row");
    assert_eq!(untouched.next_run_at, seeded_at);
    assert_eq!(untouched.config_hash, vec![0u8; 32]);

    let again = vec![ScheduleStateWrite {
        schedule_name: missing.clone(),
        next_run_at: next_run_at + chrono::Duration::seconds(999),
        config_hash: vec![2u8; 32],
    }];
    let healed_again = heal_missing_states(&core, StatementLog::Off, &again)
        .await
        .expect("heal again");
    assert!(healed_again.is_empty());
    assert_eq!(
        state_row(&pool, &missing).await.expect("row").next_run_at,
        next_run_at
    );

    core.close();
    for name in [missing, existing] {
        delete_state(&pool, &name).await;
    }
}

/// The task rows of `task_name`.
async fn count_tasks(pool: &PgPool, task_name: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE task_name = $1")
        .bind(task_name)
        .fetch_one(pool)
        .await
        .expect("count the tasks")
}

/// The send command of a task `task_name` on the default queue with no
/// options and the input `{"value": 1}`.
fn command_of(pool: &PgPool, task_name: &str) -> SendCommand {
    PostgresBroker::from_pool(pool.clone(), crate::ClientFamily::Rust)
        .prepare_send_command(
            task_name,
            "default",
            100,
            r#"{"value":1}"#,
            None,
            EnqueueRetention::Default,
        )
        .expect("the command builds")
}

/// The fire answers: no row is `StateMissing`; a row at another
/// `next_run_at` is `StateMoved`; both write nothing. A due row is
/// `Advanced` with one `INSERTED` slot. A second fire of the same slot,
/// once the row is due at it again and the task is live, answers
/// `SAME_COMMAND` and records the run; no second task is written.
#[tokio::test]
#[serial_test::serial]
async fn the_fire_answers_each_state_of_the_row() {
    let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
    let core = crate::broker::pools::core_pool_for_test_pool(&pool);
    let name = format!("qw_fire_{}", uuid::Uuid::new_v4().simple());
    let command = command_of(&pool, &name);
    let slot: DateTime<Utc> = sqlx::query_scalar(
        "SELECT date_trunc('second', statement_timestamp()) - interval '1 second'",
    )
    .fetch_one(&pool)
    .await
    .expect("a due slot");
    let next = slot + chrono::Duration::seconds(60);
    let slots = [slot];
    let fire = ScheduleFire {
        schedule_name: &name,
        expected_next_run_at: slot,
        next_run_at: next,
        slot_ats: &slots,
        command: &command,
    };

    let missing = fire_schedule(&core, StatementLog::Off, &fire).await;
    assert!(
        matches!(missing, Ok(ScheduleFireAnswer::StateMissing)),
        "{missing:?}"
    );
    seed_state(&pool, &name, slot + chrono::Duration::seconds(1)).await;
    let moved = fire_schedule(&core, StatementLog::Off, &fire).await;
    assert!(
        matches!(moved, Ok(ScheduleFireAnswer::StateMoved)),
        "{moved:?}"
    );
    assert_eq!(count_tasks(&pool, &name).await, 0);

    seed_state(&pool, &name, slot).await;
    let first = fire_schedule(&core, StatementLog::Off, &fire)
        .await
        .expect("the first fire");
    let fired = match first {
        ScheduleFireAnswer::Advanced { slots } => slots,
        other => panic!("expected an advanced fire, got {other:?}"),
    };
    assert_eq!(fired.len(), 1);
    assert_eq!(
        (fired[0].slot_at, fired[0].outcome),
        (slot, SlotOutcome::Inserted)
    );

    seed_state(&pool, &name, slot).await;
    let repeated = fire_schedule(&core, StatementLog::Off, &fire)
        .await
        .expect("the repeated fire");
    let again = match repeated {
        ScheduleFireAnswer::Advanced { slots } => slots,
        other => panic!("expected an advanced fire, got {other:?}"),
    };
    assert_eq!(again[0].outcome, SlotOutcome::SameCommand);
    assert_eq!(again[0].task_id, fired[0].task_id);
    let row = state_row(&pool, &name).await.expect("row");
    assert_eq!(
        (row.next_run_at, row.run_count, row.last_task_id),
        (next, 1, Some(fired[0].task_id))
    );
    assert_eq!(count_tasks(&pool, &name).await, 1);

    core.close();
    let _deleted = sqlx::query("DELETE FROM horsies_tasks WHERE task_name = $1")
        .bind(&name)
        .execute(&pool)
        .await
        .expect("delete the task");
    delete_state(&pool, &name).await;
}

/// A slot after the database time breaks a precondition of the fire
/// function (SQLSTATE 22023): a defect of the caller, which panics.
/// Nothing is written.
#[tokio::test]
#[serial_test::serial]
#[should_panic(expected = "broke a precondition of horsies_fire_schedule")]
async fn a_slot_after_the_database_time_is_a_caller_defect() {
    let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
    let core = crate::broker::pools::core_pool_for_test_pool(&pool);
    let name = format!("qw_defect_{}", uuid::Uuid::new_v4().simple());
    let command = command_of(&pool, &name);
    let slot = database_time(&pool).await + chrono::Duration::hours(1);
    let slots = [slot];
    let fire = ScheduleFire {
        schedule_name: &name,
        expected_next_run_at: slot,
        next_run_at: slot + chrono::Duration::seconds(60),
        slot_ats: &slots,
        command: &command,
    };

    let _answer = fire_schedule(&core, StatementLog::Off, &fire).await;
}

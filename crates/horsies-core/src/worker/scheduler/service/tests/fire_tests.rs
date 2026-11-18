//! The tick and the fire on the baseline schema (Python
//! `tests/integration/test_schedule_state_rows.py`,
//! `tests/integration/test_scheduler_single_connection_pool.py`): one
//! `horsies_fire_schedule` statement per due schedule, on the core pool the
//! tick is given.

use sqlx::PgPool;

use super::*;
use crate::db::{
    parse_connection_url, CheckoutClass, ConnectPlan, ConnectionAge, ConnectionEnvironment,
    IdlePing, PoolPlan, PoolRole, PoolSizing, Reserve, StatementProtocol, DEFAULT_SOCKET_SETTINGS,
};
use crate::test_support::LogCapture;

const SCHEDULE_INTERVAL_SECONDS: u32 = 60;

/// A core pool of one connection on the test database of `pool`, with no
/// idle ping, no age limit and the given checkout wait.
fn one_connection_pool(pool: &PgPool, checkout_wait: Duration) -> CorePool {
    let url = crate::broker::pools::test_url_of(&pool.connect_options());
    let environment = ConnectionEnvironment::isolated();
    let settings =
        parse_connection_url(&url, &environment).expect("the statement layer accepts a test URL");
    let connect = ConnectPlan::load(&settings, &environment, &DEFAULT_SOCKET_SETTINGS)
        .expect("the connection plan of a test URL loads");
    CorePool::new(
        PoolPlan {
            role: PoolRole::Main,
            sizing: PoolSizing::new(1, Reserve::NoReserve)
                .expect("one connection with no reserve is a valid size"),
            checkout_wait,
            idle_ping: IdlePing::Off,
            max_age: ConnectionAge::Unlimited,
            protocol: StatementProtocol::PreparedPerConnection,
        },
        Arc::new(connect),
    )
}

/// A schedule of its own task name and a 60 s interval.
fn own_schedule(prefix: &str) -> TestSchedule {
    let name = format!("{prefix}_{}", Uuid::new_v4().simple());
    let mut schedule = interval_schedule_secs(&name, SCHEDULE_INTERVAL_SECONDS);
    schedule.task_name = name;
    schedule
}

/// The send commands of `schedules`, as the start builds them.
fn commands_for(schedules: &[TestSchedule]) -> HashMap<String, SendCommand> {
    active_plan(manifest_for(schedules)).commands
}

/// One tick on `core` with the existence heal not due.
async fn tick_on(
    core: &CorePool,
    schedules: &[TestSchedule],
    commands: &HashMap<String, SendCommand>,
) -> Result<(), BrokerError> {
    let mut cadence = ExistenceCheckCadence::new(1);
    cadence.record(true);
    check_and_enqueue(
        core,
        StatementLog::Off,
        &active_plan(manifest_for(schedules)).schedules,
        commands,
        &mut cadence,
    )
    .await
}

/// The database time cut to a whole second.
async fn whole_second_database_time(pool: &PgPool) -> DateTime<Utc> {
    sqlx::query_scalar("SELECT date_trunc('second', statement_timestamp())")
        .fetch_one(pool)
        .await
        .expect("read the database time")
}

/// The task id of a slot, from the database function that the fire uses.
async fn slot_task_id(pool: &PgPool, schedule_name: &str, slot: DateTime<Utc>) -> Uuid {
    sqlx::query_scalar("SELECT horsies_schedule_slot_task_id($1, $2)")
        .bind(schedule_name)
        .bind(slot)
        .fetch_one(pool)
        .await
        .expect("compute the slot task id")
}

/// Python `test_a_due_slot_fires_and_records_its_run_at_the_database_time`
/// and `test_a_fired_slot_stores_its_task_id_and_counts_one_run`: a tick
/// fires the due slot, stores its task with the slot time as `sent_at`, and
/// records one run at the database time of the fire.
#[tokio::test]
#[serial]
async fn a_due_slot_fires_and_records_its_run_at_the_database_time() {
    let pool = test_pool().await;
    let core = core_pool_of(&pool);
    let schedule = own_schedule("due_fires");
    let slot = whole_second_database_time(&pool).await - chrono::Duration::seconds(1);
    state::test_rows::seed_state(&pool, &schedule.name, slot).await;
    let schedules = vec![schedule.clone()];

    let before = database_time(&pool).await;
    tick_on(&core, &schedules, &commands_for(&schedules))
        .await
        .expect("the tick");
    let after = database_time(&pool).await;

    let row = state::test_rows::state_row(&pool, &schedule.name)
        .await
        .unwrap();
    let task_id = slot_task_id(&pool, &schedule.name, slot).await;
    assert_eq!(row.last_task_id, Some(task_id));
    assert_eq!(
        row.next_run_at,
        slot + chrono::Duration::seconds(i64::from(SCHEDULE_INTERVAL_SECONDS))
    );
    assert_eq!(row.run_count, 1);
    assert_eq!(
        row.config_hash,
        vec![0_u8; 32],
        "the fire keeps the stored digest"
    );
    let last_run_at = row.last_run_at.expect("a recorded run has its time");
    assert!(before <= last_run_at && last_run_at <= after);
    let sent_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT sent_at FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(sent_at, slot);
    core.close();
    cleanup(&pool, &schedule.task_name, &schedule.name).await;
}

/// A fire that finds no state row (the row was deleted after the due read)
/// creates the row with the first run after the database time of the due
/// read, and writes no task (Python `_heal_one`).
#[tokio::test]
#[serial]
async fn a_fire_that_finds_no_state_row_creates_it_and_writes_no_task() {
    let pool = test_pool().await;
    let schedule = own_schedule("state_missing");
    let database_now = whole_second_database_time(&pool).await;
    let first_due = database_now - chrono::Duration::seconds(1);

    fire_once(&pool, &schedule, first_due, database_now)
        .await
        .expect("the fire heals the row");

    let row = state::test_rows::state_row(&pool, &schedule.name)
        .await
        .expect("the heal created the row");
    assert!(row.next_run_at > database_now);
    assert!(
        row.next_run_at
            <= database_now + chrono::Duration::seconds(i64::from(SCHEDULE_INTERVAL_SECONDS))
    );
    assert_eq!(row.run_count, 0);
    assert_eq!(
        row.config_hash,
        compute_config_hash(&enabled(&schedule)).to_vec()
    );
    // Python `test_scheduler_service.py::TestCheckScheduleOneStatement::test_a_missing_state_row_is_healed_from_the_database_time`:
    // the first run after the database time of the due read, no run.
    let healed = enabled(&schedule);
    assert_eq!(
        row.next_run_at,
        next_run_at(&healed.pattern, database_now, &healed.timezone)
            .expect("an interval has a next run")
    );
    assert_eq!((row.last_run_at, row.last_task_id), (None, None));
    assert_eq!(task_count(&pool, &schedule.task_name).await, 0);
    cleanup(&pool, &schedule.task_name, &schedule.name).await;
}

/// Python `test_two_schedulers_that_fire_one_slot_write_one_task_and_one_run`:
/// four fires of one due slot run at once, each on its own pool. The fire
/// locks the state row, so the others find it moved and write nothing.
#[tokio::test]
#[serial]
async fn concurrent_fires_of_one_slot_write_one_task_and_one_run() {
    let pool = test_pool().await;
    let schedule = own_schedule("competition");
    let database_now = whole_second_database_time(&pool).await;
    let slot = database_now - chrono::Duration::seconds(1);
    state::test_rows::seed_state(&pool, &schedule.name, slot).await;

    let fires: Vec<_> = (0..4)
        .map(|_| {
            let pool = pool.clone();
            let schedule = schedule.clone();
            tokio::spawn(async move {
                fire_once(&pool, &schedule, slot, database_now)
                    .await
                    .expect("each fire answers");
            })
        })
        .collect();
    for fire in fires {
        fire.await.expect("each fire joins");
    }

    let row = state::test_rows::state_row(&pool, &schedule.name)
        .await
        .unwrap();
    assert_eq!(task_count(&pool, &schedule.task_name).await, 1);
    assert_eq!(
        (row.last_task_id, row.run_count),
        (Some(slot_task_id(&pool, &schedule.name, slot).await), 1)
    );
    assert_eq!(
        row.next_run_at,
        slot + chrono::Duration::seconds(i64::from(SCHEDULE_INTERVAL_SECONDS))
    );
    cleanup(&pool, &schedule.task_name, &schedule.name).await;
}

/// Python `test_a_fire_repeated_after_its_commit_writes_nothing_once_the_task_left_the_live_table`:
/// a fire repeated after its commit, once the slot's task left
/// `horsies_tasks`, finds the row moved and writes no second task; the next
/// tick does not show the slot either.
#[tokio::test]
#[serial]
async fn a_fire_repeated_after_its_commit_writes_nothing_once_the_task_left_the_live_table() {
    let pool = test_pool().await;
    let schedule = own_schedule("repeat_after_commit");
    let database_now = whole_second_database_time(&pool).await;
    let slot = database_now - chrono::Duration::seconds(1);
    state::test_rows::seed_state(&pool, &schedule.name, slot).await;
    let fire = || fire_once(&pool, &schedule, slot, database_now);

    fire().await.expect("the first fire");
    let task_id = slot_task_id(&pool, &schedule.name, slot).await;
    assert_eq!(task_count(&pool, &schedule.task_name).await, 1);
    let _deleted = sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .unwrap();
    let capture = LogCapture::new();
    let _default = capture.set_default();
    fire().await.expect("the repeated fire");
    let moved = capture.lines_starting(
        tracing::Level::DEBUG,
        "schedule was moved by another scheduler",
    );
    assert_eq!(moved.len(), 1, "{moved:?}");
    assert_eq!(moved[0].field("schedule"), Some(schedule.name.as_str()));
    let core = core_pool_of(&pool);
    let schedules = vec![schedule.clone()];
    tick_on(&core, &schedules, &commands_for(&schedules))
        .await
        .expect("the next tick");
    core.close();

    let row = state::test_rows::state_row(&pool, &schedule.name)
        .await
        .unwrap();
    assert_eq!(task_count(&pool, &schedule.task_name).await, 0);
    assert_eq!((row.last_task_id, row.run_count), (Some(task_id), 1));
    assert_eq!(
        row.next_run_at,
        slot + chrono::Duration::seconds(i64::from(SCHEDULE_INTERVAL_SECONDS))
    );
    cleanup(&pool, &schedule.task_name, &schedule.name).await;
}

/// Terminate the backend that waits for the state row lock that
/// `transaction` holds. Each poll first drops the activity snapshot: inside
/// one transaction, `pg_stat_activity` keeps the rows of its first read.
async fn terminate_the_waiting_fire(transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>) {
    for _ in 0..100 {
        let _cleared = sqlx::query("SELECT pg_stat_clear_snapshot()")
            .execute(&mut **transaction)
            .await
            .expect("drop the activity snapshot");
        let terminated: Vec<bool> = sqlx::query_scalar(
            "SELECT pg_terminate_backend(a.pid)
             FROM pg_stat_activity AS a
             WHERE a.datname = current_database()
               AND a.wait_event_type = 'Lock'
               AND a.pid <> pg_backend_pid()
               AND pg_backend_pid() = ANY(pg_blocking_pids(a.pid))",
        )
        .fetch_all(&mut **transaction)
        .await
        .expect("terminate the waiting fire");
        if !terminated.is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no fire statement waited for the state row lock");
}

/// Python `test_a_catch_up_whose_fire_fails_commits_nothing_and_the_next_tick_fires_each_slot_once`:
/// the fire of three slots loses its connection while it waits for the
/// state row: no slot and no state move commit. The next tick fires the
/// three slots once and records one run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn a_catch_up_whose_fire_fails_commits_nothing_and_the_next_tick_fires_each_slot_once() {
    let pool = test_pool().await;
    let mut schedule = own_schedule("catch_up_lost");
    schedule.catch_up_missed = true;
    let interval = chrono::Duration::seconds(i64::from(SCHEDULE_INTERVAL_SECONDS));
    let first =
        whole_second_database_time(&pool).await - interval * 2 - chrono::Duration::seconds(1);
    state::test_rows::seed_state(&pool, &schedule.name, first).await;
    let schedules = vec![schedule.clone()];
    let commands = commands_for(&schedules);
    let core = Arc::new(core_pool_of(&pool));

    let mut holder = pool.begin().await.expect("begin the holder");
    let _held =
        sqlx::query("SELECT 1 FROM horsies_schedule_state WHERE schedule_name = $1 FOR UPDATE")
            .bind(&schedule.name)
            .execute(&mut *holder)
            .await
            .expect("hold the state row");
    let capture = LogCapture::new();
    let _default = capture.set_default();
    let tick = {
        use tracing::instrument::WithSubscriber;

        let core = Arc::clone(&core);
        let schedules = schedules.clone();
        let commands = commands.clone();
        tokio::spawn(
            async move { tick_on(&core, &schedules, &commands).await }.with_current_subscriber(),
        )
    };
    terminate_the_waiting_fire(&mut holder).await;
    holder.rollback().await.expect("release the state row");
    tick.await
        .expect("the tick joins")
        .expect("a lost fire does not fail the tick");
    let transient = capture.lines_starting(
        tracing::Level::WARN,
        &format!("Schedule '{}' fire failed (transient)", schedule.name),
    );
    assert_eq!(transient.len(), 1, "{transient:?}");

    let row = state::test_rows::state_row(&pool, &schedule.name)
        .await
        .unwrap();
    assert_eq!((row.next_run_at, row.run_count), (first, 0));
    assert_eq!(task_count(&pool, &schedule.task_name).await, 0);

    tick_on(&core, &schedules, &commands)
        .await
        .expect("the next tick");
    core.close();

    let row = state::test_rows::state_row(&pool, &schedule.name)
        .await
        .unwrap();
    let last_slot = first + interval * 2;
    assert_eq!(task_count(&pool, &schedule.task_name).await, 3);
    assert_eq!(
        (row.last_task_id, row.run_count),
        (
            Some(slot_task_id(&pool, &schedule.name, last_slot).await),
            1
        )
    );
    assert_eq!(row.next_run_at, last_slot + interval);
    cleanup(&pool, &schedule.task_name, &schedule.name).await;
}

/// Python `test_scheduler_single_connection_pool.py::test_due_schedule_enqueues_with_a_one_connection_runtime_pool`:
/// a tick needs one connection: the due read and the fire run on a pool of
/// one connection, with no second connection for a lock.
#[tokio::test]
#[serial]
async fn a_tick_fires_a_due_slot_on_a_pool_of_one_connection() {
    let pool = test_pool().await;
    let core = one_connection_pool(&pool, Duration::from_secs(10));
    let schedule = own_schedule("one_connection");
    let slot = whole_second_database_time(&pool).await - chrono::Duration::seconds(1);
    state::test_rows::seed_state(&pool, &schedule.name, slot).await;
    let schedules = vec![schedule.clone()];

    tick_on(&core, &schedules, &commands_for(&schedules))
        .await
        .expect("the tick on one connection");

    let row = state::test_rows::state_row(&pool, &schedule.name)
        .await
        .unwrap();
    assert_eq!(row.run_count, 1);
    assert_eq!(task_count(&pool, &schedule.task_name).await, 1);
    assert_eq!(core.status().max_size, 1);
    core.close();
    cleanup(&pool, &schedule.task_name, &schedule.name).await;
}

/// A tick whose pool has no free connection fails at the due read and
/// writes nothing; the next tick fires the slot once.
#[tokio::test]
#[serial]
async fn a_tick_with_no_free_connection_fails_at_the_due_read_and_writes_nothing() {
    let pool = test_pool().await;
    let core = one_connection_pool(&pool, Duration::from_millis(200));
    let schedule = own_schedule("saturation");
    let slot = whole_second_database_time(&pool).await - chrono::Duration::seconds(1);
    state::test_rows::seed_state(&pool, &schedule.name, slot).await;
    let schedules = vec![schedule.clone()];
    let commands = commands_for(&schedules);

    let held = core
        .checkout(CheckoutClass::Batch)
        .await
        .expect("check out the one connection");
    let saturated = tick_on(&core, &schedules, &commands).await;
    drop(held);
    assert!(
        matches!(
            saturated,
            Err(BrokerError::Statement(
                crate::db::StatementFailure::NotSent { .. }
            ))
        ),
        "{saturated:?}"
    );
    assert_eq!(task_count(&pool, &schedule.task_name).await, 0);

    tick_on(&core, &schedules, &commands)
        .await
        .expect("the next tick");
    core.close();
    let row = state::test_rows::state_row(&pool, &schedule.name)
        .await
        .unwrap();
    assert_eq!(row.run_count, 1);
    assert_eq!(task_count(&pool, &schedule.task_name).await, 1);
    cleanup(&pool, &schedule.task_name, &schedule.name).await;
}

/// A fire whose state move fails commits nothing: no task and no run (the
/// slot rows and the state move are one statement). The next fire, after
/// the failure clears, fires the slot once.
#[tokio::test]
#[serial]
async fn a_fire_whose_state_move_fails_writes_no_task_and_the_next_tick_fires_once() {
    let pool = test_pool().await;
    let schedule = own_schedule("state_failure");
    let slot = whole_second_database_time(&pool).await - chrono::Duration::seconds(1);
    state::test_rows::seed_state(&pool, &schedule.name, slot).await;
    let schedules = vec![schedule.clone()];
    let commands = commands_for(&schedules);
    let core = core_pool_of(&pool);
    let _created = sqlx::raw_sql(
        "CREATE FUNCTION test_fail_schedule_state() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'injected state write failure'; END $$;
         CREATE TRIGGER test_fail_schedule_state BEFORE UPDATE ON horsies_schedule_state
             FOR EACH ROW EXECUTE FUNCTION test_fail_schedule_state();",
    )
    .execute(&pool)
    .await
    .expect("create the state failure trigger");

    let failed = tick_on(&core, &schedules, &commands).await;
    let _dropped = sqlx::raw_sql(
        "DROP TRIGGER test_fail_schedule_state ON horsies_schedule_state;
         DROP FUNCTION test_fail_schedule_state();",
    )
    .execute(&pool)
    .await
    .expect("drop the state failure trigger");
    failed.expect("a refused fire does not fail the tick");
    let row = state::test_rows::state_row(&pool, &schedule.name)
        .await
        .unwrap();
    assert_eq!((row.next_run_at, row.run_count), (slot, 0));
    assert_eq!(task_count(&pool, &schedule.task_name).await, 0);

    tick_on(&core, &schedules, &commands)
        .await
        .expect("the next tick");
    tick_on(&core, &schedules, &commands)
        .await
        .expect("a tick with nothing due");
    core.close();
    let row = state::test_rows::state_row(&pool, &schedule.name)
        .await
        .unwrap();
    assert_eq!(row.run_count, 1);
    assert_eq!(task_count(&pool, &schedule.task_name).await, 1);
    cleanup(&pool, &schedule.task_name, &schedule.name).await;
}

/// A running scheduler of a manifest starts, fires a slot that comes due on
/// the main core pool of its broker, and ends at its stop request with no
/// start error.
#[tokio::test]
#[serial]
async fn a_running_scheduler_fires_a_due_slot_and_stops() {
    let pool = test_pool().await;
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let schedule = own_schedule("spawned");
    let manifest = manifest_for(std::slice::from_ref(&schedule));
    let stop = SchedulerStop::new();
    let service = tokio::spawn(SchedulerService::from_manifest(broker, manifest, &stop).run());
    tokio::time::timeout(Duration::from_secs(5), async {
        while state::test_rows::state_row(&pool, &schedule.name)
            .await
            .is_none()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the start writes the state row");
    let _due = sqlx::query(
        "UPDATE horsies_schedule_state
         SET next_run_at = date_trunc('second', NOW()) - interval '1 second'
         WHERE schedule_name = $1",
    )
    .bind(&schedule.name)
    .execute(&pool)
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = state::test_rows::state_row(&pool, &schedule.name)
                .await
                .unwrap();
            if row.run_count == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a tick fires the due slot");
    assert_eq!(task_count(&pool, &schedule.task_name).await, 1);
    stop.request_stop();
    tokio::time::timeout(Duration::from_secs(5), service)
        .await
        .expect("the scheduler stops at its stop request")
        .expect("the scheduler task joins")
        .expect("the scheduler started and stopped without a start error");
    cleanup(&pool, &schedule.task_name, &schedule.name).await;
}

/// The bytes of the slot task id that the fire stores, from the database
/// function `horsies_schedule_slot_task_id`: its pinned vector, and vectors
/// computed with Python `hashlib` from the same layout (a whole second, a
/// slot with microseconds inside one millisecond, a name that is not ASCII,
/// and the Unix epoch). Each id has the UUIDv7 layout of its slot time.
#[tokio::test]
#[serial]
async fn the_slot_task_id_of_the_database_matches_its_vectors_and_layout() {
    let pool = test_pool().await;
    let slot = Utc.with_ymd_and_hms(2026, 11, 16, 10, 0, 0).unwrap();
    let vectors = [
        ("daily", slot, "01a1e3cc-9d00-7a2c-be43-740ce1fde7de"),
        (
            "daily",
            slot + chrono::Duration::microseconds(123_456),
            "01a1e3cc-9d7b-7ebb-a478-d6dd16388216",
        ),
        (
            "n\u{e4}chtlich-\u{5e8f}",
            slot,
            "01a1e3cc-9d00-7f4b-ba15-2858ace729b9",
        ),
        (
            "daily",
            DateTime::<Utc>::UNIX_EPOCH,
            "00000000-0000-7c74-b585-66f6ef335d1e",
        ),
    ];
    for (name, slot_time, expected) in vectors {
        let id = slot_task_id(&pool, name, slot_time).await;
        assert_eq!(
            id,
            Uuid::parse_str(expected).expect("vector is a UUID"),
            "{name} at {slot_time}"
        );
        assert_eq!(id.get_version_num(), 7);
        assert_eq!(id.get_variant(), uuid::Variant::RFC4122);
        let birth_ms = id.as_bytes()[..6]
            .iter()
            .fold(0_i64, |millis, byte| (millis << 8) | i64::from(*byte));
        assert_eq!(
            birth_ms,
            slot_time.timestamp_millis(),
            "{name} at {slot_time}"
        );
    }
    assert_ne!(
        slot_task_id(&pool, "daily", slot).await,
        slot_task_id(&pool, "weekly", slot).await
    );
    assert_ne!(
        slot_task_id(&pool, "daily", slot).await,
        slot_task_id(&pool, "daily", slot + chrono::Duration::seconds(60)).await
    );
    // Python `test_schedule_slot_task_id.py::test_another_name_or_another_microsecond_gives_another_id`
    // and `::test_one_instant_in_another_time_zone_gives_one_id`: one more
    // microsecond inside the same millisecond gives another id, and the
    // instant written in another zone gives the same id.
    assert_ne!(
        slot_task_id(&pool, "daily", slot).await,
        slot_task_id(&pool, "daily", slot + chrono::Duration::microseconds(1)).await
    );
    let other_zone: Uuid = sqlx::query_scalar(
        "SELECT horsies_schedule_slot_task_id('daily', '2026-11-16 11:00:00+01'::timestamptz)",
    )
    .fetch_one(&pool)
    .await
    .expect("compute the slot task id of the instant in another zone");
    assert_eq!(other_zone, slot_task_id(&pool, "daily", slot).await);
}

/// A slot time before the Unix epoch, or at or after 2^48 ms, has no slot
/// id: the database function refuses it with SQLSTATE 22023; the last
/// microsecond below the bound has one.
#[tokio::test]
#[serial]
async fn the_slot_task_id_refuses_a_slot_time_outside_the_uuid7_range() {
    let pool = test_pool().await;
    let end = DateTime::from_timestamp_millis(1_i64 << 48).expect("2^48 ms is a valid time");
    let before_epoch = DateTime::<Utc>::UNIX_EPOCH - chrono::Duration::microseconds(1);
    for slot_time in [before_epoch, end] {
        let refused = sqlx::query_scalar::<_, Uuid>("SELECT horsies_schedule_slot_task_id($1, $2)")
            .bind("daily")
            .bind(slot_time)
            .fetch_one(&pool)
            .await
            .expect_err("a slot time outside the range has no id");
        let code = refused
            .as_database_error()
            .and_then(|error| error.code())
            .map(|code| code.into_owned());
        assert_eq!(code.as_deref(), Some("22023"), "{slot_time}");
    }
    let last = slot_task_id(&pool, "daily", end - chrono::Duration::microseconds(1)).await;
    assert_eq!(last.as_bytes()[..6], [0xff; 6]);
}

/// Python `test_scheduler_service.py::TestCheckAndRunSchedules::test_iterates_due_states_and_checks_each`:
/// one tick fires every schedule of its due read: each due slot gets its
/// task and each row records one run.
#[tokio::test]
#[serial]
async fn one_tick_fires_every_due_schedule_of_its_due_read() {
    let pool = test_pool().await;
    let core = core_pool_of(&pool);
    let schedules = vec![own_schedule("tick_first"), own_schedule("tick_second")];
    let slot = whole_second_database_time(&pool).await - chrono::Duration::seconds(1);
    for schedule in &schedules {
        state::test_rows::seed_state(&pool, &schedule.name, slot).await;
    }

    tick_on(&core, &schedules, &commands_for(&schedules))
        .await
        .expect("the tick");
    core.close();

    for schedule in &schedules {
        let row = state::test_rows::state_row(&pool, &schedule.name)
            .await
            .unwrap();
        assert_eq!(
            (row.last_task_id, row.run_count),
            (Some(slot_task_id(&pool, &schedule.name, slot).await), 1),
            "{}",
            schedule.name
        );
        assert_eq!(
            row.next_run_at,
            slot + chrono::Duration::seconds(i64::from(SCHEDULE_INTERVAL_SECONDS))
        );
        assert_eq!(task_count(&pool, &schedule.task_name).await, 1);
        cleanup(&pool, &schedule.task_name, &schedule.name).await;
    }
}

/// Python `test_scheduler_service.py::TestCheckAndRunSchedules::test_check_schedule_error_does_not_block_others`:
/// the database refuses the fire of the first due schedule; the tick moves
/// that row past its slot with no run, and still fires the next schedule.
#[tokio::test]
#[serial]
async fn a_refused_fire_of_one_schedule_does_not_stop_the_fire_of_the_next() {
    let pool = test_pool().await;
    let core = core_pool_of(&pool);
    let refused = own_schedule("tick_refused");
    let fired = own_schedule("tick_fired");
    let schedules = vec![refused.clone(), fired.clone()];
    let slot = whole_second_database_time(&pool).await - chrono::Duration::seconds(1);
    // The due read returns the soonest row first: the refused one.
    let refused_slot = slot - chrono::Duration::seconds(1);
    state::test_rows::seed_state(&pool, &refused.name, refused_slot).await;
    state::test_rows::seed_state(&pool, &fired.name, slot).await;
    refuse_slot_rows(&pool, &refused.task_name).await;

    let ticked = tick_on(&core, &schedules, &commands_for(&schedules)).await;
    drop_slot_refusal(&pool).await;
    core.close();

    ticked.expect("a refused fire does not fail the tick");
    let interval = chrono::Duration::seconds(i64::from(SCHEDULE_INTERVAL_SECONDS));
    let refused_row = state::test_rows::state_row(&pool, &refused.name)
        .await
        .unwrap();
    assert_eq!(
        (
            refused_row.next_run_at,
            refused_row.run_count,
            refused_row.last_task_id
        ),
        (refused_slot + interval, 0, None)
    );
    assert_eq!(task_count(&pool, &refused.task_name).await, 0);
    let fired_row = state::test_rows::state_row(&pool, &fired.name)
        .await
        .unwrap();
    assert_eq!(
        (fired_row.next_run_at, fired_row.run_count),
        (slot + interval, 1)
    );
    assert_eq!(task_count(&pool, &fired.task_name).await, 1);
    for schedule in [refused, fired] {
        cleanup(&pool, &schedule.task_name, &schedule.name).await;
    }
}

/// Python `test_scheduler_service.py::TestCheckScheduleOutcomes::test_a_failed_move_past_a_refusal_is_logged_for_the_next_tick`:
/// the database refuses the fire, and the fire of no slots that moves the
/// row past the refused slot fails too. Both are logged; the row keeps its
/// slot and records no run, and no task is written.
#[tokio::test]
#[serial]
async fn a_skip_that_fails_after_a_refused_fire_is_logged_and_leaves_the_row() {
    let pool = test_pool().await;
    let schedule = own_schedule("skip_failure");
    let database_now = whole_second_database_time(&pool).await;
    let slot = database_now - chrono::Duration::seconds(1);
    state::test_rows::seed_state(&pool, &schedule.name, slot).await;
    let _created = sqlx::raw_sql(
        "CREATE FUNCTION test_fail_schedule_state() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'injected state write failure'; END $$;
         CREATE TRIGGER test_fail_schedule_state BEFORE UPDATE ON horsies_schedule_state
             FOR EACH ROW EXECUTE FUNCTION test_fail_schedule_state();",
    )
    .execute(&pool)
    .await
    .expect("create the state failure trigger");
    let capture = LogCapture::new();
    let _default = capture.set_default();

    let fired = fire_once(&pool, &schedule, slot, database_now).await;
    let _dropped = sqlx::raw_sql(
        "DROP TRIGGER test_fail_schedule_state ON horsies_schedule_state;
         DROP FUNCTION test_fail_schedule_state();",
    )
    .execute(&pool)
    .await
    .expect("drop the state failure trigger");

    fired.expect("a refused fire is handled by the skip");
    let refused = capture.lines_starting(
        tracing::Level::ERROR,
        &format!("Schedule '{}' fire refused (permanent)", schedule.name),
    );
    assert_eq!(refused.len(), 1, "{refused:?}");
    let skip_failed = capture.lines_starting(
        tracing::Level::ERROR,
        &format!(
            "Schedule '{}' could not move past the refused slot",
            schedule.name
        ),
    );
    assert_eq!(skip_failed.len(), 1, "{skip_failed:?}");
    let row = state::test_rows::state_row(&pool, &schedule.name)
        .await
        .unwrap();
    assert_eq!((row.next_run_at, row.run_count), (slot, 0));
    assert_eq!(task_count(&pool, &schedule.task_name).await, 0);
    cleanup(&pool, &schedule.task_name, &schedule.name).await;
}

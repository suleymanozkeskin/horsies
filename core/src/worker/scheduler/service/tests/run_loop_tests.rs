//! The run of a started scheduler and the state start (Python
//! `clients/python/tests/unit/test_scheduler_service.py`, `TestRunForever`,
//! `TestCheckAndRunSchedules`, `TestInitializeSchedules`): a stop asked for
//! before the run, a tick that fails, a tick with no enabled schedule, and
//! the start write's rows.

use crate::test_support::sql::{self, TestPool, TestPoolOptions};
use crate::test_support::LogCapture;

use super::*;

/// No server listens on port 1: a statement on this URL fails at its
/// connect.
const UNREACHABLE_URL: &str = "postgresql://horsies@127.0.0.1:1/unreachable";

/// A lazy pool on [`UNREACHABLE_URL`]. It opens no connection.
fn unreachable_pool() -> TestPool {
    TestPoolOptions::new()
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy(UNREACHABLE_URL)
}

/// A started scheduler of `plan` on a broker whose database cannot be
/// reached: each tick fails at its due read.
fn unreachable_scheduler(plan: ActivePlan, stop: &SchedulerStop) -> ActiveScheduler {
    ActiveScheduler {
        broker: Arc::new(PostgresBroker::from_test_url(
            unreachable_pool().url(),
            crate::ClientFamily::Rust,
        )),
        schedules: plan.schedules,
        commands: plan.commands,
        check_interval_seconds: plan.check_interval_seconds,
        existence_cadence: ExistenceCheckCadence::new(plan.check_interval_seconds),
        stop: stop.token.clone(),
    }
}

/// Python `TestCheckAndRunSchedules::test_no_enabled_schedules_returns_early`:
/// a tick of a scheduler whose schedules are all disabled sends no
/// statement. The pool cannot connect, so a statement would fail the tick.
#[tokio::test]
async fn a_tick_with_no_enabled_schedule_sends_no_statement() {
    let plan = active_plan(manifest_of_entries(vec![disabled_entry("paused_only")]));
    assert!(plan.schedules.is_empty());
    let core = crate::broker::pools::core_pool_for_test_url(unreachable_pool().url());
    let mut cadence = ExistenceCheckCadence::new(plan.check_interval_seconds);

    let ticked = check_and_enqueue(
        &core,
        StatementLog::Off,
        &plan.schedules,
        &plan.commands,
        &mut cadence,
    )
    .await;
    core.close();

    ticked.expect("a tick with no enabled schedule sends no statement, so it cannot fail");
}

/// Python `TestRunForever::test_schedule_error_does_not_crash_loop` and
/// `TestRunForever::test_timeout_continues_loop`: a tick that fails is
/// logged and the run ticks again one check interval later; the run ends
/// at its stop request.
#[tokio::test]
async fn a_failed_tick_is_logged_and_the_run_ticks_again() {
    let capture = LogCapture::new();
    let _default = capture.set_default();
    let plan = active_plan(manifest_for(&[interval_schedule_secs(
        "unreachable_run",
        60,
    )]));
    let stop = SchedulerStop::new();
    let run = tokio::spawn(unreachable_scheduler(plan, &stop).run());
    let failed_ticks = || capture.lines_starting(tracing::Level::ERROR, "Error in scheduler loop");

    tokio::time::timeout(Duration::from_secs(10), async {
        while failed_ticks().len() < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("two ticks fail, one check interval apart");

    assert!(!run.is_finished(), "a failed tick does not end the run");
    for failed in failed_ticks() {
        let error = failed
            .field("error")
            .expect("the failed tick names its error");
        assert!(error.starts_with("the due schedule read failed"), "{error}");
    }
    stop.request_stop();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("the run ends at its stop request")
        .expect("the run joins");
}

/// Python `TestRunForever::test_stop_signal_breaks_loop`: a stop asked for
/// before the run ends the run after its start. The start ran (it wrote the
/// state row of a new schedule) and no tick fired the row that was due.
#[tokio::test]
async fn a_stop_asked_for_before_the_run_ends_the_run_after_its_start() {
    let pool = test_pool().await;
    let mut due = interval_schedule_secs(&format!("stopped_due_{}", Uuid::new_v4().simple()), 60);
    due.task_name = due.name.clone();
    let mut new = interval_schedule_secs(&format!("stopped_new_{}", Uuid::new_v4().simple()), 60);
    new.task_name = new.name.clone();
    // The due row holds the digest of its config, so the start leaves it due.
    let slot = database_time(&pool).await - chrono::Duration::seconds(1);
    state::test_rows::seed_state(&pool, &due.name, slot).await;
    let _digest =
        sql::query("UPDATE horsies_schedule_state SET config_hash = $2 WHERE schedule_name = $1")
            .bind(&due.name)
            .bind(compute_config_hash(&enabled(&due)).to_vec())
            .execute(&pool)
            .await
            .expect("store the digest of the due schedule");
    let stop = SchedulerStop::new();
    stop.request_stop();
    let broker = Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    ));
    let service =
        SchedulerService::from_manifest(broker, manifest_for(&[due.clone(), new.clone()]), &stop);

    tokio::time::timeout(Duration::from_secs(5), service.run())
        .await
        .expect("a run asked to stop ends after its start")
        .expect("the start completes");

    assert!(
        state::test_rows::state_row(&pool, &new.name)
            .await
            .is_some(),
        "the start ran"
    );
    let row = state::test_rows::state_row(&pool, &due.name).await.unwrap();
    assert_eq!((row.next_run_at, row.run_count), (slot, 0));
    assert_eq!(task_count(&pool, &due.task_name).await, 0);
    for schedule in [due, new] {
        cleanup(&pool, &schedule.task_name, &schedule.name).await;
    }
}

/// Python `TestInitializeSchedules::test_foreign_schedule_state_rows_are_kept`:
/// the start keeps the state row of a schedule that is not in its config
/// (another scheduler may own it), logs it, and writes the row of its own
/// schedule.
#[tokio::test]
async fn the_start_keeps_and_logs_the_row_of_a_schedule_not_in_its_config() {
    let pool = test_pool().await;
    let core = core_pool_of(&pool);
    let foreign = format!("foreign_row_{}", Uuid::new_v4().simple());
    let kept = format!("start_kept_{}", Uuid::new_v4().simple());
    let foreign_next_run = database_time(&pool).await + chrono::Duration::hours(1);
    state::test_rows::seed_state(&pool, &foreign, foreign_next_run).await;
    let plan = active_plan(manifest_for(&[interval_schedule_secs(&kept, 60)]));
    let capture = LogCapture::new();
    let _default = capture.set_default();

    initialize_schedules(
        &core,
        StatementLog::Off,
        &plan.configured_names,
        &plan.schedules,
    )
    .await
    .expect("every pattern has a next slot");
    core.close();

    let foreign_row = state::test_rows::state_row(&pool, &foreign)
        .await
        .expect("the start deletes no row");
    assert_eq!(
        (
            foreign_row.next_run_at,
            foreign_row.run_count,
            foreign_row.config_hash
        ),
        (foreign_next_run, 0, vec![0_u8; 32])
    );
    assert!(state::test_rows::state_row(&pool, &kept).await.is_some());
    let logged = capture.lines_starting(
        tracing::Level::WARN,
        "schedule state present in DB but not in this config",
    );
    assert!(
        logged
            .iter()
            .any(|event| event.field("schedule") == Some(foreign.as_str())),
        "{logged:?}"
    );
    for name in [foreign, kept] {
        state::test_rows::delete_state(&pool, &name).await;
    }
}

/// Python `TestInitializeSchedules::test_store_failure_is_logged_and_start_goes_on`:
/// a database error of the start write is logged and does not refuse the
/// start; the row is left to the tick's existence heal.
#[tokio::test]
async fn a_failed_start_write_is_logged_and_the_start_goes_on() {
    let pool = test_pool().await;
    let core = core_pool_of(&pool);
    let name = format!("start_write_failure_{}", Uuid::new_v4().simple());
    let plan = active_plan(manifest_for(&[interval_schedule_secs(&name, 60)]));
    let _created = sql::raw_sql(&format!(
        "CREATE FUNCTION test_fail_start_write() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.schedule_name = '{name}' THEN
                 RAISE EXCEPTION 'injected start write failure';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER test_fail_start_write BEFORE INSERT ON horsies_schedule_state
             FOR EACH ROW EXECUTE FUNCTION test_fail_start_write();"
    ))
    .execute(&pool)
    .await
    .expect("create the start write failure trigger");
    let capture = LogCapture::new();
    let _default = capture.set_default();

    let started = initialize_schedules(
        &core,
        StatementLog::Off,
        &plan.configured_names,
        &plan.schedules,
    )
    .await;
    let _dropped = sql::raw_sql(
        "DROP TRIGGER test_fail_start_write ON horsies_schedule_state;
         DROP FUNCTION test_fail_start_write();",
    )
    .execute(&pool)
    .await
    .expect("drop the start write failure trigger");
    core.close();

    started.expect("a failed start write does not refuse the start");
    let logged = capture.lines_starting(
        tracing::Level::ERROR,
        "Failed to store schedule state rows at start",
    );
    assert_eq!(logged.len(), 1, "{logged:?}");
    assert!(state::test_rows::state_row(&pool, &name).await.is_none());
}

/// Python `TestInitializeSchedulesAdditionalPaths::test_start_read_failure_is_logged_and_writes_nothing`:
/// a start read that fails is logged and does not refuse the start; no
/// write follows it, and the tick's existence heal creates the rows.
#[tokio::test]
async fn a_failed_start_read_is_logged_and_writes_nothing() {
    let core = crate::broker::pools::core_pool_for_test_url(unreachable_pool().url());
    let plan = active_plan(manifest_for(&[interval_schedule_secs("unread_start", 60)]));
    let capture = LogCapture::new();
    let _default = capture.set_default();

    let started = initialize_schedules(
        &core,
        StatementLog::Off,
        &plan.configured_names,
        &plan.schedules,
    )
    .await;
    core.close();

    started.expect("a failed start read does not refuse the start");
    let read_failed = capture.lines_starting(
        tracing::Level::ERROR,
        "Failed to read schedule state rows at start",
    );
    assert_eq!(read_failed.len(), 1, "{read_failed:?}");
    assert!(capture
        .lines_starting(
            tracing::Level::ERROR,
            "Failed to store schedule state rows at start"
        )
        .is_empty());
}

//! Layer 3: the scheduler (Python `clients/python/tests/e2e/test_layer3_scheduler.py`).
//!
//! The scheduler is the test binary with `scheduler`: `horsies::cli::run`
//! runs the schedules of the scheduler instance. A worker of the same
//! instance runs what it sends.

use std::time::Duration;

use chrono::{DateTime, Timelike, Utc};
use horsies_test_support::tokio_postgres::Client;
use horsies_test_support::{poll, rows};
use horsies_test_worker::harness::Suite;
use horsies_test_worker::instance::{
    Instance, Profile, SCHEDULED_VALUE, SCHEDULE_CATCH_UP, SCHEDULE_CRON_EVERY_MINUTE,
    SCHEDULE_DISABLED, SCHEDULE_SIMPLE_INTERVAL, SCHEDULE_WITH_ARGS,
};
use serial_test::serial;

const BIN: &str = env!("CARGO_BIN_EXE_horsies-test-worker");

/// The longest wait for a schedule to fire and its task to run.
const FIRE_WAIT: Duration = Duration::from_secs(20);

/// One row of `horsies_schedule_state`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ScheduleState {
    next_run_at: DateTime<Utc>,
    last_run_at: Option<DateTime<Utc>>,
    run_count: i32,
    config_hash: Vec<u8>,
}

async fn schedule_state(client: &Client, name: &str) -> Option<ScheduleState> {
    client
        .query_opt(
            "SELECT next_run_at, last_run_at, run_count, config_hash \
             FROM horsies_schedule_state WHERE schedule_name = $1",
            &[&name],
        )
        .await
        .unwrap_or_else(|error| panic!("read the state of schedule {name}: {error}"))
        .map(|row| ScheduleState {
            next_run_at: row.get(0),
            last_run_at: row.get(1),
            run_count: row.get(2),
            config_hash: row.get(3),
        })
}

/// Wait until the state of `name` passes `reached`, and return it.
async fn wait_for_state(
    client: &Client,
    name: &str,
    limit: Duration,
    reached: impl Fn(&ScheduleState) -> bool,
) -> ScheduleState {
    poll::poll_until(
        limit,
        &format!("the state of schedule {name}"),
        async || {
            schedule_state(client, name)
                .await
                .filter(|state| reached(state))
        },
    )
    .await
}

async fn set_next_run_at(client: &Client, name: &str, at: DateTime<Utc>) {
    let changed = client
        .execute(
            "UPDATE horsies_schedule_state SET next_run_at = $2 WHERE schedule_name = $1",
            &[&name, &at],
        )
        .await
        .unwrap_or_else(|error| panic!("move the next run of {name}: {error}"));
    assert_eq!(changed, 1, "schedule {name} has a state row");
}

/// Python: `test_layer3_scheduler.py::test_schedule_persistence`.
#[tokio::test]
#[serial]
async fn test_schedule_persistence() {
    let suite = Suite::start(BIN, Instance::Scheduler).await;
    let client = suite.client().await;
    let _scheduler = suite.scheduler().await;
    let state = wait_for_state(&client, SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT, |_| true).await;
    assert!(state.next_run_at > Utc::now() - chrono::Duration::seconds(1));
    assert_eq!(state.run_count, 0);
}

/// Python: `test_layer3_scheduler.py::test_schedule_executes_after_due_time`.
#[tokio::test]
#[serial]
async fn test_schedule_executes_after_due_time() {
    let suite = Suite::start(BIN, Instance::Scheduler).await;
    let client = suite.client().await;
    let start = Utc::now();
    let _scheduler = suite.scheduler().await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let first = poll::poll_until(FIRE_WAIT, "a scheduled task", async || {
        rows::read_tasks_named(&client, "e2e_scheduled_simple")
            .await
            .into_iter()
            .next()
    })
    .await;
    assert!(
        first.sent_at >= start,
        "sent at {} before the start {start}",
        first.sent_at
    );
}

/// Python: `test_layer3_scheduler.py::test_schedule_does_not_execute_early`.
#[tokio::test]
#[serial]
async fn test_schedule_does_not_execute_early() {
    let suite = Suite::start(BIN, Instance::Scheduler).await;
    let client = suite.client().await;
    let _scheduler = suite.scheduler().await;
    let _state = wait_for_state(&client, SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT, |_| true).await;
    assert_eq!(
        rows::read_tasks_named(&client, "e2e_scheduled_simple").await,
        Vec::new(),
        "no task before the first next_run_at"
    );
}

/// Python: `test_layer3_scheduler.py::test_repeating_schedule`.
#[tokio::test]
#[serial]
async fn test_repeating_schedule() {
    let suite = Suite::start(BIN, Instance::Scheduler).await;
    let client = suite.client().await;
    let _scheduler = suite.scheduler().await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let state = wait_for_state(&client, SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT, |state| {
        state.run_count >= 2
    })
    .await;
    assert!(state.last_run_at.is_some());
    let sent = rows::read_tasks_named(&client, "e2e_scheduled_simple").await;
    assert!(sent.len() >= 2, "{} tasks sent", sent.len());
    for pair in sent.windows(2) {
        assert!(pair[1].sent_at > pair[0].sent_at, "sends follow each other");
    }
}

/// Python: `test_layer3_scheduler.py::test_disabled_schedule_does_not_fire`.
#[tokio::test]
#[serial]
async fn test_disabled_schedule_does_not_fire() {
    let suite = Suite::start(BIN, Instance::Scheduler).await;
    let client = suite.client().await;
    let _scheduler = suite.scheduler().await;
    let _state = wait_for_state(&client, SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT, |_| true).await;
    assert_eq!(schedule_state(&client, SCHEDULE_DISABLED).await, None);
}

/// Python: `test_layer3_scheduler.py::test_schedule_with_args_passes_arguments`.
#[tokio::test]
#[serial]
async fn test_schedule_with_args_passes_arguments() {
    let suite = Suite::start(BIN, Instance::Scheduler).await;
    let client = suite.client().await;
    let _scheduler = suite.scheduler().await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let _state = wait_for_state(&client, SCHEDULE_WITH_ARGS, FIRE_WAIT, |state| {
        state.run_count >= 1
    })
    .await;
    let completed = poll::poll_until(FIRE_WAIT, "a completed scheduled task", async || {
        rows::read_tasks_named(&client, "e2e_scheduled_with_args")
            .await
            .into_iter()
            .find(|row| row.status == "COMPLETED")
    })
    .await;
    let outcome = suite
        .app
        .get_result::<i64, std::convert::Infallible>(
            completed.task_id,
            horsies_test_worker::harness::within(Duration::from_secs(1)),
        )
        .await
        .expect("read the scheduled task");
    assert_eq!(
        outcome,
        horsies::TaskOutcome::Succeeded(SCHEDULED_VALUE * 2)
    );
}

/// Python: `test_layer3_scheduler.py::test_config_hash_change_recalculates_next_run`.
#[tokio::test]
#[serial]
async fn test_config_hash_change_recalculates_next_run() {
    let suite = Suite::start(BIN, Instance::Scheduler).await;
    let client = suite.client().await;
    let original = {
        let _scheduler = suite.scheduler().await;
        wait_for_state(&client, SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT, |_| true).await
    };
    let corrupted = vec![0_u8; 32];
    let changed = client
        .execute(
            "UPDATE horsies_schedule_state SET config_hash = $2 WHERE schedule_name = $1",
            &[&SCHEDULE_SIMPLE_INTERVAL, &corrupted],
        )
        .await
        .expect("corrupt the config hash");
    assert_eq!(changed, 1);
    let _scheduler = suite.scheduler().await;
    let restored = wait_for_state(&client, SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT, |state| {
        state.config_hash != corrupted
    })
    .await;
    assert_eq!(restored.config_hash, original.config_hash);
}

/// Python: `test_layer3_scheduler.py::test_catch_up_missed_enqueues_missed_runs`.
#[tokio::test]
#[serial]
async fn test_catch_up_missed_enqueues_missed_runs() {
    let suite = Suite::start(BIN, Instance::Scheduler).await;
    let client = suite.client().await;
    {
        let _scheduler = suite.scheduler().await;
        let _state = wait_for_state(&client, SCHEDULE_CATCH_UP, FIRE_WAIT, |_| true).await;
    }
    let past = Utc::now() - chrono::Duration::seconds(5);
    set_next_run_at(&client, SCHEDULE_CATCH_UP, past).await;
    let before = rows::read_tasks_named(&client, "e2e_catch_up_task")
        .await
        .len();
    let _scheduler = suite.scheduler().await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let _state = wait_for_state(&client, SCHEDULE_CATCH_UP, FIRE_WAIT, |state| {
        state.next_run_at > past
    })
    .await;
    let after = rows::read_tasks_named(&client, "e2e_catch_up_task")
        .await
        .len();
    assert!(
        after - before >= 2,
        "catch-up sent {} tasks for at least 5 missed 1 s slots",
        after - before
    );
}

/// Python: `test_layer3_scheduler.py::test_scheduler_restart_reuses_existing_state`.
#[tokio::test]
#[serial]
async fn test_scheduler_restart_reuses_existing_state() {
    let suite = Suite::start(BIN, Instance::Scheduler).await;
    let client = suite.client().await;
    let before = {
        let _scheduler = suite.scheduler().await;
        let _worker = suite.worker(Profile::Isolated1).await;
        wait_for_state(&client, SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT, |state| {
            state.run_count >= 1
        })
        .await
    };
    let _scheduler = suite.scheduler().await;
    let after = wait_for_state(&client, SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT, |_| true).await;
    assert!(
        after.run_count >= before.run_count,
        "the restart kept the run count: {} then {}",
        before.run_count,
        after.run_count
    );
    assert_eq!(after.config_hash, before.config_hash);
}

/// Python: `test_layer3_scheduler.py::test_cron_schedule_fires_on_due_minute`.
/// No worker: the scheduler writes the task row itself.
#[tokio::test]
#[serial]
async fn test_cron_schedule_fires_on_due_minute() {
    let suite = Suite::start(BIN, Instance::Scheduler).await;
    let client = suite.client().await;
    {
        let _scheduler = suite.scheduler().await;
        let state = wait_for_state(&client, SCHEDULE_CRON_EVERY_MINUTE, FIRE_WAIT, |_| true).await;
        assert_eq!(state.run_count, 0);
        assert_eq!(state.next_run_at.second(), 0, "a cron slot starts a minute");
    }
    let due_slot = Utc::now()
        .with_second(0)
        .and_then(|at| at.with_nanosecond(0))
        .expect("the start of the current minute");
    set_next_run_at(&client, SCHEDULE_CRON_EVERY_MINUTE, due_slot).await;
    let _scheduler = suite.scheduler().await;
    let fired = wait_for_state(&client, SCHEDULE_CRON_EVERY_MINUTE, FIRE_WAIT, |state| {
        state.run_count >= 1 && state.next_run_at > due_slot
    })
    .await;
    assert_eq!(fired.next_run_at.second(), 0);
    assert!(
        !rows::read_tasks_named(&client, "e2e_cron_task")
            .await
            .is_empty(),
        "the cron slot sent its task"
    );
}

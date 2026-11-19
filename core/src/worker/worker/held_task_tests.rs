//! Worker runs that hold one task in a scripted child.
//!
//! - The child is a bash script ([`child_holding_its_first_run`]): it
//!   boots, reads the run frame, writes its pid file and answers nothing,
//!   so the task stays held in the child.
//! - Once the pid file is there, the test writes the start of the attempt
//!   as a child does (`horsies_start_attempt`): the row is RUNNING under the
//!   worker's claim, and the runner heartbeats of the worker cover it. A
//!   start before the run frame reaches the child is an order that no child
//!   produces: a child that ends before it takes the run is replaced, and
//!   the run goes to the next child.
//! - Each test runs on a copy of the installed test template.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use super::Worker;
use crate::broker::PostgresBroker;
use crate::core::config::app::AppConfig;
use crate::core::config::recovery::fail_closed_threshold_floor_ms;
use crate::core::config::worker_profile::SelectedProfile;
use crate::test_support::sql::{self, TestPool, TestPoolOptions};
use crate::test_support::TestDatabase;
use crate::worker::children::ChildLaunch;
use crate::worker::claim_pass::RegisteredClasses;
use crate::worker::config::WorkerConfig;
use crate::worker::error::WorkerError;
use crate::worker::executor::protocol::{
    ChildConnection, ChildLogLevel, ChildProgram, DatabaseUrl,
};
use crate::worker::executor::slots::scripted::child_holding_its_first_run;
use crate::worker::run_end::{run_end_of, RunPhase, WorkerFailCause, WorkerRunEnd};

/// The longest wait for a claim, a beat or a write that a test expects.
const WAIT: Duration = Duration::from_secs(30);

/// How often a test reads the database while it waits.
const POLL: Duration = Duration::from_millis(100);

/// The longest wait for the end of a worker run.
const STOP_BOUND: Duration = Duration::from_secs(60);

/// The runner heartbeat interval of the heartbeat tests: the smallest that
/// the config takes.
const BEAT_INTERVAL_MS: u64 = 1_000;

/// The last lines of the worker log that a failed wait shows.
const FAILURE_LOG_LINES: usize = 200;

/// A drain that ends soon after the stop: the pool kills a child that
/// still holds its task.
const SHORT_DRAIN_SECS: u64 = 1;

/// A drain longer than [`STOP_BOUND`]: a run that ends within the bound
/// did not wait for it.
const LONG_DRAIN_SECS: u64 = 120;

/// The start of the attempt of a CLAIMED row, as its child writes it:
/// `horsies_start_attempt` under the worker's claim. No row while the task
/// is not CLAIMED.
const START_AS_THE_CHILD_SQL: &str = "\
SELECT s.outcome
FROM horsies_tasks t
CROSS JOIN LATERAL horsies_start_attempt(
    t.id, t.claimed_by_worker_id, t.claimed_at, 4242, 'scripted-host', 'scripted-child'
) AS s
WHERE t.id = $1 AND t.status = 'CLAIMED'";

/// A test database and a pool on it.
async fn database() -> (TestDatabase, TestPool) {
    let database = TestDatabase::create().await;
    let pool = TestPoolOptions::new()
        .max_connections(4)
        .connect(database.url())
        .await
        .expect("connect the test database");
    (database, pool)
}

/// A folder of its own for the pid file of the child.
fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("horsies-{name}-{}", Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).expect("create the test folder");
    dir
}

/// The app config of `url` with runner heartbeats every
/// [`BEAT_INTERVAL_MS`] and the smallest stale threshold that stale running
/// recovery takes: the worker fails closed after 3 s with no landed beat.
fn config_with_fast_beats(url: &str) -> AppConfig {
    let mut config = AppConfig::for_database_url(url);
    config.recovery.runner_heartbeat_interval_ms = BEAT_INTERVAL_MS;
    config.recovery.running_stale_threshold_ms = fail_closed_threshold_floor_ms(BEAT_INTERVAL_MS);
    config
}

/// A worker of `config` with one isolated child that holds its first run
/// and writes its pid to `pid_file`, and a drain of `drain_secs`.
async fn worker_holding_one_task(config: AppConfig, pid_file: &Path, drain_secs: u64) -> Worker {
    let broker = PostgresBroker::connect_worker_with(&config.broker, crate::ClientFamily::Rust)
        .await
        .expect("connect the broker of a worker");
    let database_url = DatabaseUrl::new(config.broker.database_url.clone());
    Worker::of_child_specs(
        Arc::new(broker),
        crate::test_support::child_specs::no_child_specs(),
        config,
        WorkerConfig::default(),
        SelectedProfile {
            name: "default".to_owned(),
            profile: serde_json::from_value(serde_json::json!({
                "isolated": {"kind": "isolated_children", "count": 1},
                "drain_timeout": {"secs": drain_secs, "nanos": 0}
            }))
            .expect("a valid test profile"),
        },
        ChildLaunch {
            command: child_holding_its_first_run(pid_file),
            preload: crate::worker::children::PreloadProgram::NoPreloadProgram,
            program: ChildProgram::Rust,
            log_level: ChildLogLevel::Info,
            database_url,
            connection: ChildConnection::default(),
        },
        RegisteredClasses::default(),
    )
    .expect("a valid test worker")
}

/// One PENDING task on the default queue, with no retries.
async fn insert_pending_task(pool: &TestPool) -> Uuid {
    let task_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, task_options, execution,
             retention_class_key, sent_at, created_at,
             command_fingerprint_version, command_fingerprint, idempotency_key_digest,
             status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
             started_at, finalizing_at, finalizing_by_worker_id,
             worker_pid, worker_hostname, worker_process_name,
             retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
             updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
         ) VALUES (
             $1, 'held_task_test', 'default', 100, '{}', NULL, 'ISOLATED',
             'forever', NOW(), NOW(),
             1, decode(repeat('00', 32), 'hex'), NULL,
             'PENDING', NOW(), NULL, NULL, NULL,
             NULL, NULL, NULL,
             NULL, NULL, NULL,
             0, 0, NULL, NULL, FALSE,
             NOW(), FALSE, NULL, NULL
         )",
    )
    .bind(task_id)
    .execute(pool)
    .await
    .expect("insert a pending task");
    task_id
}

/// Wait until the child holds the run of `task_id` (its pid file is
/// there), then write the start of the attempt as the child does
/// ([`START_AS_THE_CHILD_SQL`]). Returns the pid of the child.
async fn start_as_the_child(pool: &TestPool, task_id: Uuid, pid_file: &Path) -> i32 {
    tokio::time::timeout(WAIT, async {
        while !pid_file.exists() {
            tokio::time::sleep(POLL).await;
        }
    })
    .await
    .expect("the worker claims the task and sends the run to the child");
    let started: Option<String> = sql::query_scalar(START_AS_THE_CHILD_SQL)
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("write the start of the attempt");
    assert_eq!(started.as_deref(), Some("STARTED"));
    child_pid(pid_file)
}

/// The pid that the child wrote to `pid_file`.
fn child_pid(pid_file: &Path) -> i32 {
    std::fs::read_to_string(pid_file)
        .expect("the child wrote its pid")
        .trim()
        .parse()
        .expect("the pid file holds a pid")
}

/// The worker runs its runner heartbeats on the dispatches that its
/// children hold. The RUNNING task that the child holds gets a runner beat
/// of the worker; the stop ends the run with no error. While the child holds
/// the run, the worker writes no
/// end of it: the row stays RUNNING with no history row.
#[tokio::test]
#[serial_test::serial]
async fn a_task_held_in_a_child_gets_the_runner_beats_of_the_worker() {
    let (database, pool) = database().await;
    let dir = test_dir("held-task-beats");
    let pid_file = dir.join("child.pid");
    let worker = worker_holding_one_task(
        config_with_fast_beats(database.url()),
        &pid_file,
        SHORT_DRAIN_SECS,
    )
    .await;
    let worker_id = worker.worker_id().to_owned();
    let task_id = insert_pending_task(&pool).await;
    let cancel = worker.cancel_token();
    let run = tokio::spawn(async move { worker.run().await });

    let _pid = start_as_the_child(&pool, task_id, &pid_file).await;
    let beats = tokio::time::timeout(WAIT, async {
        loop {
            let beats: i64 = sql::query_scalar(
                "SELECT count(*) FROM horsies_heartbeats
                 WHERE task_id = $1 AND role = 'runner' AND sender_id = $2",
            )
            .bind(task_id)
            .bind(&worker_id)
            .fetch_one(&pool)
            .await
            .expect("read the runner beats of the worker");
            if beats >= 1 {
                break beats;
            }
            tokio::time::sleep(POLL).await;
        }
    })
    .await
    .expect("a runner beat of the worker lands for the held task");
    let held: (Option<String>, i64) = sql::query_as(
        "SELECT (SELECT status::text FROM horsies_tasks WHERE id = $1),
                (SELECT count(*) FROM horsies_task_history WHERE task_id = $1)",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .expect("read the held row");
    cancel.cancel();
    let ended = tokio::time::timeout(STOP_BOUND, run)
        .await
        .expect("the worker stops within the bound")
        .expect("the worker task joins");

    assert!(beats >= 1, "{beats}");
    assert_eq!(held, (Some("RUNNING".to_owned()), 0));
    assert!(ended.is_ok(), "the worker stopped with an error: {ended:?}");
    pool.close().await;
    database.drop().await;
    std::fs::remove_dir_all(&dir).expect("remove the test folder");
}

/// A trigger refuses every runner beat of the worker, so no beat lands for
/// the RUNNING task that the child holds. Past the fail-closed window the
/// watchdog kills the child and stops the worker: the run ends with
/// `RunnerHeartbeatLost`, not retryable, a failed run
/// (`RUNNER_HEARTBEAT_LOST`). The drain is longer than the bound of the
/// run, so a run that ends within the bound had its child killed by the
/// watchdog.
#[tokio::test]
#[serial_test::serial]
async fn runner_beats_that_cannot_land_stop_the_worker_with_runner_heartbeat_lost() {
    let (database, pool) = database().await;
    let refusal =
        "CREATE FUNCTION test_refuse_runner_beat() RETURNS trigger LANGUAGE plpgsql AS $body$
         BEGIN
             RAISE EXCEPTION 'a runner beat refused by the test';
         END
         $body$;
         CREATE TRIGGER test_refuse_runner_beat BEFORE INSERT ON horsies_heartbeats
             FOR EACH ROW WHEN (NEW.role = 'runner')
             EXECUTE FUNCTION test_refuse_runner_beat();";
    sql::raw_sql(refusal)
        .execute(&pool)
        .await
        .expect("install the refusal of the worker's runner beats");
    let dir = test_dir("held-task-fail-closed");
    let pid_file = dir.join("child.pid");
    let worker = worker_holding_one_task(
        config_with_fast_beats(database.url()),
        &pid_file,
        LONG_DRAIN_SECS,
    )
    .await;
    let task_id = insert_pending_task(&pool).await;
    let run = tokio::spawn(async move { worker.run().await });

    let pid = start_as_the_child(&pool, task_id, &pid_file).await;
    let ended = tokio::time::timeout(STOP_BOUND, run)
        .await
        .expect("the watchdog stops the worker within the bound")
        .expect("the worker task joins");

    let error = ended.expect_err("a worker whose runner beats failed closed ends with an error");
    match &error {
        WorkerError::RunnerHeartbeatLost { cause } => {
            assert!(
                cause.starts_with("no runner heartbeat landed for"),
                "{cause}"
            );
            assert!(cause.contains("; last error: "), "{cause}");
        }
        other => panic!("expected RunnerHeartbeatLost, got {other:?}"),
    }
    assert!(!error.is_retryable(), "{error}");
    assert!(
        matches!(
            run_end_of(Err(error), RunPhase::Serving),
            WorkerRunEnd::Failed {
                cause: WorkerFailCause::RunnerHeartbeatLost,
                ..
            }
        ),
        "the run end is RUNNER_HEARTBEAT_LOST"
    );
    // SAFETY: signal 0 sends nothing; it only checks that the process exists.
    let exists = unsafe { libc::kill(pid, 0) } == 0;
    assert!(
        !exists,
        "the child {pid} still runs after the worker stopped"
    );
    pool.close().await;
    database.drop().await;
    std::fs::remove_dir_all(&dir).expect("remove the test folder");
}

/// Python `test_worker_helpers.py::TestHandleFinalizeError::test_retryable_errors_spawn_retry_as_finalizer`:
/// the retries of an outcome write run in the write itself
/// (`OutcomeWriter::settle`), which the stop drains. The test kills the
/// child that holds the RUNNING task; the crash write of the attempt
/// (Python's future stage) meets a trigger that holds its first try for one
/// second and refuses it with SQLSTATE 40001 (`serialization_failure`,
/// class 40: may clear). A sequence counts the tries: `nextval` is not
/// rolled back with the refused statement. The test stops the worker during
/// that first try. The run returns only after the retry committed: the log
/// of committed attempt writes, which an AFTER trigger writes in the
/// transaction of the write, holds the task once, and the retry was logged.
#[tokio::test]
#[serial_test::serial]
async fn the_stop_waits_for_an_outcome_write_that_is_tried_again() {
    let (database, pool) = database().await;
    sql::raw_sql(
        "CREATE SEQUENCE test_attempt_write_tries;
         CREATE FUNCTION test_refuse_first_attempt_write() RETURNS trigger LANGUAGE plpgsql AS $body$
         BEGIN
             IF nextval('test_attempt_write_tries') = 1 THEN
                 PERFORM pg_sleep(1);
                 RAISE EXCEPTION 'a serialization failure for the test'
                     USING ERRCODE = 'serialization_failure';
             END IF;
             RETURN NEW;
         END
         $body$;
         CREATE TRIGGER test_refuse_first_attempt_write BEFORE INSERT ON horsies_task_attempts
             FOR EACH ROW EXECUTE FUNCTION test_refuse_first_attempt_write();
         CREATE TABLE test_committed_attempt_writes (task_id uuid NOT NULL);
         CREATE FUNCTION test_log_committed_attempt_write() RETURNS trigger LANGUAGE plpgsql AS $body$
         BEGIN
             INSERT INTO test_committed_attempt_writes VALUES (NEW.task_id);
             RETURN NULL;
         END
         $body$;
         CREATE TRIGGER test_log_committed_attempt_write AFTER INSERT ON horsies_task_attempts
             FOR EACH ROW EXECUTE FUNCTION test_log_committed_attempt_write();",
    )
    .execute(&pool)
    .await
    .expect("install the refusal of the first attempt write");
    let dir = test_dir("held-task-write-retry");
    let pid_file = dir.join("child.pid");
    let worker = worker_holding_one_task(
        AppConfig::for_database_url(database.url()),
        &pid_file,
        SHORT_DRAIN_SECS,
    )
    .await;
    let task_id = insert_pending_task(&pool).await;
    let capture = crate::test_support::LogCapture::new();
    let guard = capture.set_default();
    let cancel = worker.cancel_token();
    let run = tokio::spawn(async move { worker.run().await });

    let pid = start_as_the_child(&pool, task_id, &pid_file).await;
    // SAFETY: SIGKILL to the pid of the test's own child process.
    let killed = unsafe { libc::kill(pid, libc::SIGKILL) };
    assert_eq!(killed, 0, "kill the child {pid}");
    let tries = || {
        let pool = pool.clone();
        async move {
            sql::query_scalar::<i64>(
                "SELECT CASE WHEN is_called THEN last_value ELSE 0 END
                 FROM test_attempt_write_tries",
            )
            .fetch_one(&pool)
            .await
            .expect("read the tries of the attempt write")
        }
    };
    let first_try = tokio::time::timeout(WAIT, async {
        while tries().await < 1 {
            tokio::time::sleep(POLL).await;
        }
    })
    .await;
    match first_try {
        Ok(()) => {}
        Err(_elapsed) => {
            // The log of the worker names the end that the slot read.
            let events = capture.events();
            let worker_lines: Vec<String> = events
                .iter()
                .skip(events.len().saturating_sub(FAILURE_LOG_LINES))
                .map(|event| format!("{} {}: {:?}", event.level, event.message, event.fields))
                .collect();
            panic!(
                "the crash write of the attempt did not start its first try within {WAIT:?} \
                 after the kill of the child {pid}; worker log: {worker_lines:#?}"
            );
        }
    }
    cancel.cancel();
    let ended = tokio::time::timeout(STOP_BOUND, run)
        .await
        .expect("the worker stops within the bound")
        .expect("the worker task joins");
    drop(guard);

    let committed: i64 =
        sql::query_scalar("SELECT count(*) FROM test_committed_attempt_writes WHERE task_id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("read the committed attempt writes");
    let retry_lines: Vec<String> = capture
        .events()
        .into_iter()
        .filter(|event| {
            event.level == tracing::Level::WARN
                && event
                    .message
                    .starts_with("Finalize retry scheduled for task")
                && event.message.contains(&task_id.to_string())
        })
        .map(|event| event.message)
        .collect();
    assert!(ended.is_ok(), "the worker stopped with an error: {ended:?}");
    assert_eq!(tries().await, 2, "the first try and one retry");
    assert_eq!(committed, 1, "the retry committed before the run returned");
    assert_eq!(retry_lines.len(), 1, "{retry_lines:?}");
    pool.close().await;
    database.drop().await;
    std::fs::remove_dir_all(&dir).expect("remove the test folder");
}

//! The stage rules of `CoreScheduler` and its outcome records.

use std::time::Duration;

use chrono::Utc;
use horsies_core::manifest::parse_producer_manifest;
use horsies_core::PoolProfile;

use super::*;

/// A producer manifest whose database cannot be reached: an operation that
/// connects would fail, so a test that passes connects to nothing.
const PRODUCER_MANIFEST: &str = r#"{
    "manifest_version": 1,
    "database": {
        "database_url": "postgresql://user:secret@127.0.0.1:1/none",
        "session_database": {"kind": "SAME_AS_DATABASE_URL"},
        "pgbouncer_transaction_mode": false,
        "pool_pre_ping": true,
        "pool_size": 5,
        "max_overflow": 0,
        "pool_timeout": 30,
        "pool_recycle": {"kind": "AFTER_SECONDS", "seconds": 1800},
        "echo": false,
        "tcp_keepalives": {
            "kind": "ON",
            "idle_seconds": 30,
            "interval_seconds": 10,
            "probes": 3
        },
        "tcp_user_timeout": {"kind": "AFTER", "milliseconds": 30000}
    },
    "schema": "MIGRATE_AND_VALIDATE",
    "send": {
        "retain_rerun_input_default": false,
        "idempotency_reservation_window": 86400000000
    }
}"#;

const SCHEDULER_MANIFEST: &str = r#"{
    "manifest_version": 1,
    "enabled": true,
    "check_interval_seconds": 1,
    "schedules": []
}"#;

/// The longest wait for a run that is asked to stop.
const STOP_WAIT: Duration = Duration::from_secs(5);

fn connection() -> BrokerConnection {
    let manifest = parse_producer_manifest(PRODUCER_MANIFEST).expect("the manifest is complete");
    BrokerConnection::new(manifest, PoolProfile::Producer)
}

fn scheduler_manifest() -> Arc<SchedulerManifest> {
    Arc::new(parse_scheduler_manifest(SCHEDULER_MANIFEST).expect("the manifest is complete"))
}

fn held(stage: Stage) -> (Arc<Mutex<Stage>>, OwnedMutexGuard<Stage>) {
    let shared = Arc::new(Mutex::new(stage));
    let guard = Arc::clone(&shared)
        .try_lock_owned()
        .expect("nothing else holds the stage");
    (shared, guard)
}

#[test]
fn a_tick_needs_a_completed_start_and_a_run_that_has_not_ended() {
    assert!(tick_refusal(&Stage::NotStarted).is_some());
    assert!(tick_refusal(&Stage::RunEnded).is_some());
    assert!(tick_refusal(&Stage::Idle).is_none());
}

#[test]
fn a_refused_schedule_is_a_refusal_that_names_it() {
    for refused in [
        SchedulerStartError::UnknownTimezone {
            schedule: "nightly".to_owned(),
            timezone: "Not/A_Zone".to_owned(),
        },
        SchedulerStartError::CommandRefused {
            schedule: "nightly".to_owned(),
            cause: BrokerError::ClaimContract("refused options".to_owned()),
        },
        SchedulerStartError::NoNextSlot {
            schedule: "nightly".to_owned(),
            after: Utc::now(),
        },
    ] {
        match start_not_completed(&refused) {
            StartNotCompleted::Refused(record) => {
                assert_eq!(record.schedule, "nightly");
                assert_eq!(record.reason, refused.to_string());
            }
            StartNotCompleted::Failed(failed) => panic!("{refused} is a refusal, not {failed:?}"),
        }
    }
}

#[test]
fn a_failed_family_check_is_a_start_failure_with_its_retryable_flag() {
    for (cause, retryable) in [
        (BrokerError::ConnectionFailed("no route".to_owned()), true),
        (
            BrokerError::ClaimContract("a broken answer".to_owned()),
            false,
        ),
    ] {
        let error = SchedulerStartError::ClientFamily(cause);
        match start_not_completed(&error) {
            StartNotCompleted::Failed(failed) => {
                assert_eq!(failed.retryable, retryable);
                assert_eq!(failed.message, error.to_string());
            }
            StartNotCompleted::Refused(refused) => panic!("{error} is a failure, not {refused:?}"),
        }
    }
}

/// Python `test_scheduler_service.py::TestSchedulerStart::test_schema_init_failure_raises_runtime_error`:
/// a connect or a schema install that fails before the core start is a
/// start failure with the cause's text and its retryable flag, which the
/// facade raises as `RuntimeError`.
#[test]
fn a_failed_connect_or_install_is_a_start_failure_with_its_cause() {
    for (error, retryable) in [
        (BrokerError::ConnectionFailed("no route".to_owned()), true),
        (
            BrokerError::ClaimContract("a broken answer".to_owned()),
            false,
        ),
    ] {
        match start_failed(&error) {
            StartNotCompleted::Failed(failed) => {
                assert_eq!(failed.message, error.to_string());
                assert_eq!(failed.retryable, retryable);
            }
            StartNotCompleted::Refused(refused) => {
                panic!("{error} is a start failure, not {refused:?}")
            }
        }
    }
}

#[test]
fn a_failed_tick_keeps_the_retryable_flag_of_its_cause() {
    let failed = tick_failed(&SchedulerTickError::DueRead(BrokerError::ConnectionFailed(
        "no route".to_owned(),
    )));
    assert!(failed.retryable);
    assert!(
        failed.message.contains("due schedule read"),
        "{}",
        failed.message
    );
    let failed = tick_failed(&SchedulerTickError::PoolsUnavailable(
        BrokerError::ClaimContract("closed".to_owned()),
    ));
    assert!(!failed.retryable);
}

/// A start that completed before answers at once and connects to nothing.
#[tokio::test]
async fn a_start_after_a_completed_start_answers_at_once() {
    for stage in [Stage::Idle, Stage::RunEnded] {
        let (_shared, guard) = held(stage);
        let outcome = start_operation(
            connection(),
            scheduler_manifest(),
            SchedulerStop::new(),
            guard,
        )
        .await;
        assert!(matches!(outcome, StartOutcome::Started(_)), "{outcome:?}");
    }
}

/// An idle scheduler has nothing to fire: its run waits for the stop
/// request, as Python's loop with no schedule does, and then ends.
#[tokio::test]
async fn the_run_of_an_idle_scheduler_waits_for_the_stop_request() {
    let stop = SchedulerStop::new();
    let (shared, guard) = held(Stage::Idle);
    let run = tokio::spawn(run_operation(
        connection(),
        scheduler_manifest(),
        stop.clone(),
        guard,
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!run.is_finished(), "an idle run waits for the stop request");
    stop.request_stop();
    let outcome = tokio::time::timeout(STOP_WAIT, run)
        .await
        .expect("the run ends at the stop request")
        .expect("the run joins");
    assert!(matches!(outcome, RunOutcome::Stopped(_)), "{outcome:?}");
    assert!(matches!(*shared.lock().await, Stage::RunEnded));
}

/// A run of a scheduler whose run ended ends at once.
#[tokio::test]
async fn a_run_after_an_ended_run_ends_at_once() {
    let (_shared, guard) = held(Stage::RunEnded);
    let outcome = tokio::time::timeout(
        STOP_WAIT,
        run_operation(
            connection(),
            scheduler_manifest(),
            SchedulerStop::new(),
            guard,
        ),
    )
    .await
    .expect("the run ends at once");
    assert!(matches!(outcome, RunOutcome::Stopped(_)), "{outcome:?}");
}

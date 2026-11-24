//! Port of Python `clients/python/tests/integration/test_task_attempt_history.py`.
//!
//! `test_stale_cleanup_with_retry_policy_schedules_retry` is
//! `worker::recovery::...::a_stale_running_task_retries_when_its_policy_names_the_crash`
//! (#613) and is not repeated.

use chrono::Duration;

use super::support::*;
use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, FinishFailedAttempt, RetryRequest,
};
use crate::broker::claims::{start_attempt, StartAttempt, StartMiss, StartOutcome};
use crate::broker::core_rows::StatementLog;
use crate::broker::pools::core_pool_for_test_url;
use crate::broker::postgres::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::lifecycle::{FailedAttemptOutcome, OwnedClaim};
use crate::core::task::result::TaskResult;
use crate::core::task::Execution;
use crate::worker::execution::{persist_terminal_state, FinalizeOutcome};
use crate::worker::executor::slots::{ClaimedTask, RetryFacts, TaskKind};
use crate::worker::recovery::on_test_pool::mark_stale_running_as_failed;
use crate::worker::retry::read_stored_options;

/// Stale-scan bound of these tests.
const STALE_SCAN_LIMIT: i64 = 1_000;

/// Python `test_worker_failure_writes_worker_failure_attempt`, at the writer:
/// a worker-level failure writes one WORKER_FAILURE attempt that does not
/// retry. The decision that turns a lost child into this write (code
/// `BROKER_ERROR`, never retried) belongs to the executor's outcome writer.
#[tokio::test]
async fn worker_failure_writes_worker_failure_attempt() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;

    let outcome = write_failed_attempt(
        &pool,
        &FinishFailedAttempt {
            task_id,
            fence: owned_claim(),
            retry: RetryRequest::NoRetry,
            result_json: r#"{"err": "worker failure"}"#.to_owned(),
            error_code: Some("BROKER_ERROR".to_owned()),
            failed_reason: Some("Segfault in child".to_owned()),
            attempt: AttemptFailure {
                kind: AttemptFailureKind::WorkerFailure,
                error_message: Some("Segfault in child".to_owned()),
                failed_reason: Some("Segfault in child".to_owned()),
            },
            placement: crate::core::lifecycle::AttemptPlacement::NotObserved,
        },
    )
    .await
    .expect("failure write");

    assert!(
        matches!(outcome, FailedAttemptOutcome::FailureApplied { .. }),
        "{outcome:?}"
    );
    let records = archived_attempt_records(&pool, task_id).await;
    let [attempt] = records.as_slice() else {
        panic!("expected one attempt, got {records:?}");
    };
    assert_eq!(
        (
            attempt.attempt(),
            attempt.outcome(),
            attempt.will_retry(),
            attempt.error_code(),
            attempt.error_message(),
            attempt.failed_reason(),
            attempt.worker_id(),
        ),
        (
            1,
            "WORKER_FAILURE",
            false,
            Some("BROKER_ERROR"),
            Some("Segfault in child"),
            Some("Segfault in child"),
            Some(WORKER),
        )
    );
    assert_eq!(
        history_row(&pool, task_id)
            .await
            .expect("history row")
            .error_code
            .as_deref(),
        Some("BROKER_ERROR")
    );
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_stale_cleanup_writes_failed_attempt_worker_crashed`: the
/// stale recovery fails a silent runner with a FAILED attempt (not
/// WORKER_FAILURE) and the code WORKER_CRASHED.
#[tokio::test]
async fn stale_cleanup_writes_failed_attempt_worker_crashed() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;
    set_started(&pool, task_id, Since::SecondsAgo(120.0)).await;

    let processed = mark_stale_running_as_failed(&pool, 1.0, 300.0, STALE_SCAN_LIMIT)
        .await
        .expect("stale recovery");

    // The count holds every stale row of the database, not only this one.
    assert!(processed >= 1, "processed {processed}");
    let image = history_row(&pool, task_id).await.expect("history row");
    assert_eq!(image.status, "FAILED");
    assert_eq!(image.terminalization_kind, "FAIL_STALE");
    assert_eq!(image.error_code.as_deref(), Some("WORKER_CRASHED"));
    let records = archived_attempt_records(&pool, task_id).await;
    let [attempt] = records.as_slice() else {
        panic!("expected one attempt, got {records:?}");
    };
    assert_eq!(
        (
            attempt.outcome(),
            attempt.will_retry(),
            attempt.error_code()
        ),
        ("FAILED", false, Some("WORKER_CRASHED"))
    );
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_replay_does_not_create_duplicate_attempt`: the phase-1 write
/// of one success, run twice, leaves one attempt.
#[tokio::test]
async fn replay_does_not_create_duplicate_attempt() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let task = ClaimedTask {
        task_id,
        task_name: "lifecycle.test".to_owned(),
        queue_name: "default".to_owned(),
        claimed_at: generation(),
        execution: Execution::SharedAsync,
        kind: TaskKind::Plain,
        retry: RetryFacts {
            retry_count: 0,
            max_retries: 0,
        },
        options: read_stored_options(None),
        trace: crate::events::RunTrace::NoCarrier,
        trace_id: crate::telemetry::trace_carrier::StoredTraceId::NoTraceId,
    };
    let payload_policy = PayloadPolicy::default();

    for write in ["first", "replay"] {
        let outcome = persist_terminal_state(
            &broker,
            &task,
            TaskResult::Ok(br#""first""#.to_vec()),
            WORKER,
            &payload_policy,
        )
        .await
        .expect("phase 1");
        assert!(
            matches!(outcome, FinalizeOutcome::Finalized),
            "{write}: a plain success owes nothing more"
        );
        assert_eq!(archived_attempts(&pool, task_id).await.len(), 1, "{write}");
    }
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_claim_lost_writes_no_attempt`: a start
/// (`horsies_start_attempt`) that finds its claim lost is refused as
/// `NOT_HELD`, and the dispatch writes no attempt.
#[tokio::test]
async fn claim_lost_writes_no_attempt() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::claimed()).await;
    let core = core_pool_for_test_url(pool.url());

    let started = start_attempt(
        &core,
        StatementLog::Off,
        &StartAttempt {
            task_id,
            fence: OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at: generation() + Duration::seconds(1),
            },
            worker_pid: 4242,
            worker_hostname: "lifecycle-host".to_owned(),
            worker_process_name: "lifecycle-process".to_owned(),
        },
        crate::broker::child_writes::WriteDeadline::start_attempt(),
    )
    .await
    .expect("start");
    core.close();

    assert_eq!(started, StartOutcome::Missed(StartMiss::NotHeld));
    assert!(live_attempts(&pool, task_id).await.is_empty());
    assert_eq!(status_of(&pool, task_id).await, "CLAIMED");
    cleanup_tasks(&pool, &[task_id]).await;
}

//! Port of Python `clients/python/tests/integration/test_lifecycle_regressions.py`.
//!
//! The `test_future_failure_*` rows test the write after a child is lost.
//! The decision that turns a lost child into that write belongs to the
//! executor of the core; these ports pin the write (`horsies_finish_failed_attempt`
//! with WORKER_CRASHED or a WORKER_FAILURE attempt) and the retry rule
//! (`worker::retry::admit_retry`). `test_future_failure_terminal_phase2_error_schedules_finalize_retry`
//! needs the worker's private phase-2 retry and is deferred to #805.
//! `test_reaper_skips_recent_finalizing_task` and
//! `test_reaper_recovers_old_finalizing_task_and_clears_marker` are
//! `worker::recovery` tests.

use std::time::Duration as StdDuration;

use uuid::Uuid;

use super::support::*;
use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, FinishFailedAttempt, RetryPlacement, RetryRequest,
};
use crate::broker::postgres::PostgresBroker;
use crate::core::lifecycle::{
    AfterFailure, AttemptWriteRefusal, FailedAttemptOutcome, TerminalizationKind,
};
use crate::test_support::sql::{self, TestPool};
use crate::worker::recovery::on_test_pool::mark_stale_running_as_failed;
use crate::worker::retry::{admit_retry, read_stored_options, RetryAdmission};
use crate::workflow_engine::lifecycle::pause_workflow;

const QUEUE: &str = "lifecycle_regressions";
const CHANNEL: &str = "task_queue_lifecycle_regressions";
const CRASH_RESULT: &str = r#"{"err": "worker crashed"}"#;
const STALE_SCAN_LIMIT: i64 = 1_000;
/// Bound of a reaper pass after the rows it waits on are released.
const REAPER_WAIT: StdDuration = StdDuration::from_secs(5);
/// The task lock that every lifecycle function takes first.
const TASK_LOCK_SQL: &str = "SELECT pg_advisory_xact_lock(hashtextextended($1::text, 731))";
/// Bound of the polls for a backend that waits on a lock.
const LOCK_WAIT_POLLS: u32 = 400;
/// The wait between two polls for a lock wait.
const LOCK_WAIT_POLL: StdDuration = StdDuration::from_millis(25);

fn crash_policy(auto_retry_for: &str) -> String {
    format!(
        r#"{{"retry_policy": {{"max_retries": 3, "intervals": [5, 5, 5], "backoff_strategy": "fixed",
            "jitter": false, "max_delay_seconds": null, "auto_retry_for": ["{auto_retry_for}"]}}}}"#
    )
}

fn running(origin: Origin) -> TaskSeed {
    TaskSeed {
        origin,
        queue_name: QUEUE,
        max_retries: 3,
        ..TaskSeed::running()
    }
}

/// The write after a lost child of a RUNNING task: WORKER_CRASHED.
fn crash_write(task_id: Uuid, retry: RetryRequest) -> FinishFailedAttempt {
    FinishFailedAttempt {
        task_id,
        fence: owned_claim(),
        retry,
        result_json: CRASH_RESULT.to_owned(),
        error_code: Some("WORKER_CRASHED".to_owned()),
        failed_reason: Some("the child exited".to_owned()),
        attempt: AttemptFailure {
            kind: AttemptFailureKind::Failed,
            error_message: Some("the child exited".to_owned()),
            failed_reason: Some("the child exited".to_owned()),
        },
    }
}

/// A worker-level failure: the worker reports it, and it is never retried.
fn worker_failure(task_id: Uuid) -> FinishFailedAttempt {
    FinishFailedAttempt {
        error_code: Some("BROKER_ERROR".to_owned()),
        attempt: AttemptFailure {
            kind: AttemptFailureKind::WorkerFailure,
            error_message: Some("the worker lost the task".to_owned()),
            failed_reason: Some("the worker lost the task".to_owned()),
        },
        ..crash_write(task_id, RetryRequest::NoRetry)
    }
}

/// Python `test_future_failure_on_non_retryable_running_task_is_terminal_failed`.
#[tokio::test]
async fn future_failure_on_non_retryable_running_task_is_terminal_failed() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &running(Origin::Send)).await;
    assert_eq!(
        admit_retry(
            0,
            3,
            read_stored_options(None).retry(),
            Some("WORKER_CRASHED")
        ),
        RetryAdmission::NoPolicy
    );

    let outcome = write_failed_attempt(&pool, &crash_write(task_id, RetryRequest::NoRetry))
        .await
        .expect("crash write");

    assert!(
        matches!(outcome, FailedAttemptOutcome::FailureApplied { .. }),
        "{outcome:?}"
    );
    let image = history_row(&pool, task_id).await.expect("history row");
    assert_eq!(image.status, "FAILED");
    assert_eq!(image.error_code.as_deref(), Some("WORKER_CRASHED"));
    let records = archived_attempt_records(&pool, task_id).await;
    let [attempt] = records.as_slice() else {
        panic!("expected one attempt, got {records:?}");
    };
    assert_eq!(
        (attempt.outcome(), attempt.error_code()),
        ("FAILED", Some("WORKER_CRASHED"))
    );
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_future_failure_retry_replays_committed_terminal_phase`, at the
/// write: the replay of a committed crash write of a workflow task is
/// `AlreadyApplied`, adds no attempt, and leaves the phase-2 record that
/// phase 2 consumes. The worker's phase 2 of a replay is #700.
#[tokio::test]
async fn future_failure_retry_replays_committed_terminal_phase() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &running(Origin::WorkflowNode)).await;
    let workflow_id = link_workflow_task(&pool, task_id, "RUNNING", "RUNNING").await;
    let command = crash_write(task_id, RetryRequest::NoRetry);
    let first = write_failed_attempt(&pool, &command)
        .await
        .expect("crash write");
    assert!(
        matches!(
            first,
            FailedAttemptOutcome::FailureApplied {
                after: AfterFailure::PhaseTwoOwed(_),
                ..
            }
        ),
        "{first:?}"
    );

    let replay = write_failed_attempt(&pool, &command).await.expect("replay");

    assert!(
        matches!(
            replay,
            FailedAttemptOutcome::Refused(AttemptWriteRefusal::AlreadyApplied {
                kind: TerminalizationKind::FailRunning,
                ..
            })
        ),
        "{replay:?}"
    );
    assert_eq!(archived_attempts(&pool, task_id).await.len(), 1);
    assert!(phase2_pending(&pool, task_id).await);
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// Python `test_future_failure_replay_of_a_plain_task_sends_no_second_wake`.
#[tokio::test]
async fn future_failure_replay_of_a_plain_task_sends_no_second_wake() {
    let pool = pool().await;
    let mut wakes = Wakes::listen(&pool, CHANNEL).await;
    let task_id = seed_task(&pool, &running(Origin::Send)).await;
    let command = crash_write(task_id, RetryRequest::NoRetry);
    let first = write_failed_attempt(&pool, &command)
        .await
        .expect("crash write");
    assert!(
        matches!(
            first,
            FailedAttemptOutcome::FailureApplied {
                after: AfterFailure::CapacityWoken,
                ..
            }
        ),
        "{first:?}"
    );
    // The wake contract: channel `task_queue_<queue>`, payload `capacity`,
    // as the baseline write sends it (0004_lifecycle_functions.sql).
    assert_eq!(wakes.payloads(&pool).await, vec!["capacity".to_owned()]);

    let replay = write_failed_attempt(&pool, &command).await.expect("replay");

    assert!(
        matches!(
            replay,
            FailedAttemptOutcome::Refused(AttemptWriteRefusal::AlreadyApplied { .. })
        ),
        "{replay:?}"
    );
    assert!(wakes.payloads(&pool).await.is_empty());
    assert_eq!(archived_attempts(&pool, task_id).await.len(), 1);
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_future_failure_on_retryable_running_task_respects_retry_policy`:
/// WORKER_CRASHED retries only when the policy names it.
#[tokio::test]
async fn future_failure_on_retryable_running_task_respects_retry_policy() {
    let naming = read_stored_options(Some(&crash_policy("WORKER_CRASHED")));
    let other = read_stored_options(Some(&crash_policy("TASK_EXCEPTION")));
    assert!(matches!(
        admit_retry(0, 3, naming.retry(), Some("WORKER_CRASHED")),
        RetryAdmission::Admitted(_)
    ));
    assert_eq!(
        admit_retry(0, 3, other.retry(), Some("WORKER_CRASHED")),
        RetryAdmission::CodeNotListed
    );

    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            task_options: Some(crash_policy("WORKER_CRASHED")),
            ..running(Origin::Send)
        },
    )
    .await;
    let outcome = write_failed_attempt(
        &pool,
        &crash_write(
            task_id,
            RetryRequest::RetryAfter {
                expected_retry_count: 0,
                delay_seconds: 5.0,
                placement: RetryPlacement::SameClass,
            },
        ),
    )
    .await
    .expect("crash write");

    assert!(
        matches!(outcome, FailedAttemptOutcome::RetryScheduled { .. }),
        "{outcome:?}"
    );
    assert_eq!(status_of(&pool, task_id).await, "PENDING");
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_finalize_worker_failure_runs_phase2`: a worker-level failure
/// of a workflow task owes phase 2.
#[tokio::test]
async fn finalize_worker_failure_runs_phase2() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &running(Origin::WorkflowNode)).await;
    let workflow_id = link_workflow_task(&pool, task_id, "RUNNING", "RUNNING").await;

    let outcome = write_failed_attempt(&pool, &worker_failure(task_id))
        .await
        .expect("worker failure");

    assert!(
        matches!(
            outcome,
            FailedAttemptOutcome::FailureApplied {
                after: AfterFailure::PhaseTwoOwed(_),
                ..
            }
        ),
        "{outcome:?}"
    );
    assert!(phase2_pending(&pool, task_id).await);
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// Python `test_plain_worker_failure_wakes_the_queue_in_its_write`: one
/// capacity wake, sent by the write itself.
#[tokio::test]
async fn plain_worker_failure_wakes_the_queue_in_its_write() {
    let pool = pool().await;
    let mut wakes = Wakes::listen(&pool, CHANNEL).await;
    let task_id = seed_task(&pool, &running(Origin::Send)).await;

    let outcome = write_failed_attempt(&pool, &worker_failure(task_id))
        .await
        .expect("worker failure");

    assert!(
        matches!(
            outcome,
            FailedAttemptOutcome::FailureApplied {
                after: AfterFailure::CapacityWoken,
                ..
            }
        ),
        "{outcome:?}"
    );
    // The wake contract: channel `task_queue_<queue>`, payload `capacity`,
    // as the baseline write sends it (0004_lifecycle_functions.sql).
    assert_eq!(wakes.payloads(&pool).await, vec!["capacity".to_owned()]);
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_finalizer_row_lock_wins_reaper_race`: a finalizer that holds
/// the task lock and the row lock makes the stale recovery wait in
/// `horsies_finish_stale_attempt`. The finalizer completes the task under
/// its locks and commits; the recovery then finds the row moved and writes
/// nothing for it.
#[tokio::test]
async fn finalizer_row_lock_wins_reaper_race() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &running(Origin::WorkflowNode)).await;
    let workflow_id = link_workflow_task(&pool, task_id, "RUNNING", "RUNNING").await;
    set_started(&pool, task_id, Since::SecondsAgo(120.0)).await;

    let mut finalizer = pool.begin().await.expect("begin the finalizer");
    sql::query(TASK_LOCK_SQL)
        .bind(task_id)
        .execute(&mut *finalizer)
        .await
        .expect("hold the task lock");
    sql::query("SELECT id FROM horsies_tasks WHERE id = $1 FOR UPDATE")
        .bind(task_id)
        .execute(&mut *finalizer)
        .await
        .expect("hold the row lock");
    let reaper_pool = pool.clone();
    let reaper = tokio::spawn(async move {
        mark_stale_running_as_failed(&reaper_pool, 1.0, 300.0, STALE_SCAN_LIMIT).await
    });
    until_a_backend_waits_on_a_lock(&pool, "horsies_finish_stale_attempt").await;

    // The success takes the task lock again in the finalizer's session and
    // reads the row the finalizer holds.
    let outcome: String =
        sql::query_scalar("SELECT outcome FROM horsies_finish_workflow_success($1, $2, $3, $4)")
            .bind(task_id)
            .bind(WORKER)
            .bind(generation())
            .bind(r#"{"ok": 1}"#)
            .fetch_one(&mut *finalizer)
            .await
            .expect("workflow success");
    assert_eq!(outcome, "APPLIED");
    finalizer.commit().await.expect("commit the finalizer");

    let processed = tokio::time::timeout(REAPER_WAIT, reaper)
        .await
        .expect("the reaper ends after the finalizer commits")
        .expect("reaper join")
        .expect("stale recovery");
    // Other stale rows of the database may be processed; this one is not.
    let _other_rows: u64 = processed;
    assert_eq!(
        history_row(&pool, task_id)
            .await
            .expect("history row")
            .terminalization_kind,
        "COMPLETE_LOCKED"
    );
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// Return once a backend whose query names `function` waits on a lock.
async fn until_a_backend_waits_on_a_lock(pool: &TestPool, function: &str) {
    for _ in 0..LOCK_WAIT_POLLS {
        let waiting: bool = sql::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND wait_event_type = 'Lock'
                   AND query LIKE '%' || $1 || '%'
                   AND pid <> pg_backend_pid()
             )",
        )
        .bind(function)
        .fetch_one(pool)
        .await
        .expect("read the lock waits");
        match waiting {
            true => return,
            false => tokio::time::sleep(LOCK_WAIT_POLL).await,
        }
    }
    panic!("no backend waited on a lock in {function}");
}

/// Python `test_pause_resets_retry_window_claimed_workflow_task`: a pause
/// cancels a CLAIMED task in its retry window and resets its node, so the
/// workflow stays resumable.
#[tokio::test]
async fn pause_resets_retry_window_claimed_workflow_task() {
    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            retry_count: 1,
            ..TaskSeed::claimed()
        },
    )
    .await;
    sql::query(
        "UPDATE horsies_tasks SET next_retry_at = NOW() - interval '1 second' WHERE id = $1",
    )
    .bind(task_id)
    .execute(&pool)
    .await
    .expect("open the retry window");
    let workflow_id = link_workflow_task(&pool, task_id, "ENQUEUED", "RUNNING").await;

    let paused = pause_workflow(
        &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
        workflow_id,
    )
    .await
    .expect("pause");

    assert!(paused);
    let workflow_status: String =
        sql::query_scalar("SELECT status::text FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .fetch_one(&pool)
            .await
            .expect("read workflow status");
    assert_eq!(workflow_status, "PAUSED");
    assert_eq!(
        node_state(&pool, workflow_id).await,
        ("READY".to_owned(), None)
    );
    let image = history_row(&pool, task_id).await.expect("history row");
    assert_eq!(image.status, "CANCELLED");
    assert_eq!(image.error_code.as_deref(), Some("TASK_CANCELLED"));
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// Python `test_reaper_recovers_stale_runner_when_owner_worker_state_is_fresh`:
/// a fresh worker snapshot of the owner does not save a silent runner.
#[tokio::test]
async fn reaper_recovers_stale_runner_when_owner_worker_state_is_fresh() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &running(Origin::Send)).await;
    set_started(&pool, task_id, Since::SecondsAgo(120.0)).await;
    sql::query(
        "INSERT INTO horsies_worker_states (
            worker_id, snapshot_at, hostname, pid, processes, max_claim_batch,
            max_claim_per_worker, cluster_wide_cap, queues, queue_priorities,
            queue_max_concurrency, recovery_config, tasks_running, tasks_claimed,
            memory_usage_mb, memory_percent, cpu_percent, children_memory_mb,
            worker_started_at, slots_down, class_capacity, telemetry
        ) VALUES (
            $1, NOW(), 'lifecycle-host', 4242, 1, 1,
            1, NULL, ARRAY[$2]::varchar[], '{}'::jsonb,
            '{}'::jsonb, '{}'::jsonb, 1, 0,
            NULL, NULL, NULL, NULL,
            NOW() - interval '1 hour', '{}'::jsonb, '{}'::jsonb, jsonb_build_object('endpoint', jsonb_build_object('state', 'off'))
        )",
    )
    .bind(WORKER)
    .bind(QUEUE)
    .execute(&pool)
    .await
    .expect("seed a fresh worker snapshot");

    let processed = mark_stale_running_as_failed(&pool, 1.0, 300.0, STALE_SCAN_LIMIT)
        .await
        .expect("stale recovery");

    assert!(processed >= 1, "processed {processed}");
    let image = history_row(&pool, task_id).await.expect("history row");
    assert_eq!(image.status, "FAILED");
    assert_eq!(image.terminalization_kind, "FAIL_STALE");
    assert_eq!(image.error_code.as_deref(), Some("WORKER_CRASHED"));
    assert!(image.result_payload.is_some());
    sql::query("DELETE FROM horsies_worker_states WHERE worker_id = $1")
        .bind(WORKER)
        .execute(&pool)
        .await
        .expect("remove the worker snapshot");
    cleanup_tasks(&pool, &[task_id]).await;
}

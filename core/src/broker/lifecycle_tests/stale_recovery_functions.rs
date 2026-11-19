//! Port of the stale recovery cases of
//! `sql_tests/cases/baseline_lifecycle_functions.sql` ("Stale RUNNING
//! recovery: horsies_stale_running_scan, horsies_finish_stale_attempt" and
//! "horsies_requeue_stale_claims"), which
//! `sql_tests::baseline_lifecycle_functions::single_session_cases` runs: the
//! same cases, through the core's statements on a core pool
//! (`broker::stale_recovery`), and the reaper's steps that call them
//! (`worker::recovery`).
//!
//! The argument refusals of the SQL cases (a batch of 0 or above 10 000, a
//! negative threshold, a retry slot that does not fit its kind, a NULL error
//! code or result) cannot be built: `RecoveryBatch`, `ThresholdMs`,
//! `StaleRetry` and the `String` fields of `FinishStaleAttempt` make them
//! unrepresentable. The unit tests of `broker::stale_recovery` pin the
//! bounds.
//!
//! The round trips of each reaper statement are counted at the wire, through
//! the counting proxy on a core pool of one connection, after the statement
//! is prepared on the connection, with no checkout ping.

use chrono::Duration;
use serial_test::serial;
use uuid::Uuid;

use super::support::*;
use crate::broker::core_rows::StatementLog;
use crate::broker::expiry_and_orphans::expire_pending_pass;
use crate::broker::pools::{core_pool_for_test_url, unpinged_single_connection_core_pool_for_url};
use crate::broker::stale_recovery::{
    finish_stale_attempt, requeue_stale_claims, scan_stale_running, FinishStaleAttempt,
    RecoveryBatch, StaleEvidence, StaleRetry, StaleRunningThresholds, ThresholdMs,
};
use crate::core::lifecycle::outcomes::GuardEvidence;
use crate::core::lifecycle::{
    AfterStaleFailure, AttemptWriteRefusal, ObservedTaskState, PhaseTwoOwed, PhaseTwoStep,
    RetryRefusal, StaleAttemptOutcome,
};
use crate::core::types::status::TaskStatus;
use crate::db::CorePool;
use crate::test_support::sql::{self, TestPool};
use crate::worker::recovery::{mark_stale_running_as_failed, requeue_stale_claimed};

const QUEUE: &str = "q_stale";
const CHANNEL: &str = "task_queue_q_stale";

/// The largest scan, so a candidate of this test is inside it whatever stale
/// rows other tests left.
fn whole_scan() -> RecoveryBatch {
    RecoveryBatch::new(10_000).expect("10000 rows is a recovery batch")
}

/// 60 s without a runner beat; a finalizing marker protects for 600 s.
fn thresholds() -> StaleRunningThresholds {
    StaleRunningThresholds {
        stale_after: ThresholdMs::from_millis(60_000).expect("60 s fits"),
        finalizing_stale_after: ThresholdMs::from_millis(600_000).expect("600 s fits"),
    }
}

/// A RUNNING row of `WORKER` on `QUEUE`, started 10 minutes ago with no
/// beat, at retry count 1 of 3 (Python `lc_task(..., 'RUNNING')` and its
/// update).
async fn stale_task(pool: &TestPool) -> Uuid {
    stale_task_with(pool, None, 1, 3).await
}

async fn stale_task_with(
    pool: &TestPool,
    task_options: Option<String>,
    retry_count: i32,
    max_retries: i32,
) -> Uuid {
    let task_id = seed_task(
        pool,
        &TaskSeed {
            queue_name: QUEUE,
            task_options,
            retry_count,
            max_retries,
            ..TaskSeed::running()
        },
    )
    .await;
    set_started(pool, task_id, Since::SecondsAgo(600.0)).await;
    task_id
}

/// The stale recovery of `task_id` with `retry`.
fn finish(task_id: Uuid, retry: StaleRetry) -> FinishStaleAttempt {
    FinishStaleAttempt {
        task_id,
        evidence: StaleEvidence::BeatAge(thresholds()),
        retry,
        result_json: r#"{"err":"crashed"}"#.to_owned(),
        error_code: "WORKER_CRASHED".to_owned(),
        failed_reason: "Worker process crashed".to_owned(),
        attempt_message: "crash message".to_owned(),
        attempt_reason: "crash reason".to_owned(),
    }
}

async fn recover(pool: &CorePool, command: &FinishStaleAttempt) -> StaleAttemptOutcome {
    finish_stale_attempt(pool, StatementLog::Off, command)
        .await
        .expect("stale attempt")
}

/// The attempt rows of a live task: attempt, outcome, will_retry, error
/// code, message, reason, worker.
type AttemptRow = (
    i32,
    String,
    bool,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

async fn attempt_rows(pool: &TestPool, task_id: Uuid) -> Vec<AttemptRow> {
    sql::query_as(
        "SELECT attempt, outcome::text, will_retry, error_code, error_message, failed_reason,
                worker_id
         FROM horsies_task_attempts WHERE task_id = $1 ORDER BY attempt",
    )
    .bind(task_id)
    .fetch_all(pool)
    .await
    .expect("read attempt rows")
}

/// The policy that retries a crash once after 5 s, with no jitter.
fn crash_retry_options() -> String {
    serde_json::json!({
        "retry_policy": {
            "max_retries": 1,
            "intervals": [5],
            "backoff_strategy": "fixed",
            "jitter": false,
            "max_delay_seconds": null,
            "auto_retry_for": ["WORKER_CRASHED"]
        }
    })
    .to_string()
}

mod scan {
    use super::*;

    /// Case `stale scan: the silent rows, not the beaten or finalizing ones`.
    #[tokio::test]
    #[serial]
    async fn the_silent_rows_not_the_beaten_or_finalizing_ones() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let silent = stale_task(&pool).await;
        let beaten = stale_task(&pool).await;
        let finalizing = stale_task(&pool).await;
        let old_beat = stale_task(&pool).await;
        beat(&pool, beaten, BeatRole::Runner, Duration::zero()).await;
        set_finalizing(&pool, finalizing, Since::SecondsAgo(0.0)).await;
        beat(&pool, old_beat, BeatRole::Runner, Duration::seconds(540)).await;

        let candidates = scan_stale_running(&core, StatementLog::Off, thresholds(), whole_scan())
            .await
            .expect("stale scan");

        let ours: Vec<Uuid> = [silent, beaten, finalizing, old_beat]
            .into_iter()
            .filter(|task_id| {
                candidates
                    .iter()
                    .any(|candidate| candidate.task_id == *task_id)
            })
            .collect();
        assert_eq!(ours, vec![silent, old_beat]);
        core.close();
        cleanup_tasks(&pool, &[silent, beaten, finalizing, old_beat]).await;
    }

    /// Case `stale scan: the facts of a candidate`.
    #[tokio::test]
    #[serial]
    async fn the_facts_of_a_candidate() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let task_id = stale_task(&pool).await;
        let started_at: chrono::DateTime<chrono::Utc> =
            sql::query_scalar("SELECT started_at FROM horsies_tasks WHERE id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .expect("read started_at");

        let candidates = scan_stale_running(&core, StatementLog::Off, thresholds(), whole_scan())
            .await
            .expect("stale scan");

        let candidate = candidates
            .iter()
            .find(|candidate| candidate.task_id == task_id)
            .expect("the stale row is a candidate");
        assert_eq!(
            (
                candidate.retry_count,
                candidate.max_retries,
                candidate.task_options.as_deref(),
                candidate.good_until,
                candidate.claimed_by_worker_id.as_deref(),
                candidate.worker_pid,
                candidate.worker_hostname.as_deref(),
                candidate.worker_process_name.as_deref(),
                candidate.started_at,
            ),
            (
                1,
                3,
                None,
                None,
                Some(WORKER),
                Some(4242),
                Some("lifecycle-host"),
                Some("lifecycle-process"),
                started_at,
            )
        );
        assert!(candidate.db_now > candidate.started_at, "{candidate:?}");
        core.close();
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Case `stale scan: the batch bound`.
    #[tokio::test]
    #[serial]
    async fn the_batch_bound() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let task_ids = [
            stale_task(&pool).await,
            stale_task(&pool).await,
            stale_task(&pool).await,
        ];

        let candidates = scan_stale_running(
            &core,
            StatementLog::Off,
            thresholds(),
            RecoveryBatch::new(2).expect("2 rows is a recovery batch"),
        )
        .await
        .expect("stale scan");

        assert_eq!(candidates.len(), 2);
        core.close();
        cleanup_tasks(&pool, &task_ids).await;
    }
}

mod finish {
    use super::*;

    /// Case `stale attempt: no retry fails the task` and `stale attempt: the
    /// history row`.
    #[tokio::test]
    #[serial]
    async fn no_retry_fails_the_task() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let task_id = stale_task(&pool).await;
        // A row at retry count 1 holds its attempt 1: the archive numbers
        // attempts from 1 with no gap, and its decoder refuses a gap
        // (`non_contiguous_attempts`, `core::history::archive::attempts`).
        record_failed_first_attempt(&pool, task_id, "attempt one crashed").await;

        let outcome = recover(&core, &finish(task_id, StaleRetry::NoRetry)).await;

        match outcome {
            StaleAttemptOutcome::FailureApplied {
                retry: RetryRefusal::NotRequested,
                after: AfterStaleFailure::NothingOwed,
                observed,
                ..
            } => assert_eq!(
                observed,
                ObservedTaskState {
                    status: Some(TaskStatus::Running),
                    worker_id: Some(WORKER.to_owned()),
                    claimed_at: Some(generation()),
                }
            ),
            other => panic!("expected the stale failure, got {other:?}"),
        }
        let image = history_row(&pool, task_id).await.expect("history row");
        assert_eq!(
            (
                image.status.as_str(),
                image.terminalization_kind.as_str(),
                image.error_code.as_deref(),
                image.final_failed_reason.as_deref(),
            ),
            (
                "FAILED",
                "FAIL_STALE",
                Some("WORKER_CRASHED"),
                Some("Worker process crashed"),
            )
        );
        assert_eq!(
            archived_attempts(&pool, task_id).await,
            vec![
                (
                    1,
                    "FAILED".to_owned(),
                    Some("attempt one crashed".to_owned())
                ),
                (2, "FAILED".to_owned(), Some("crash reason".to_owned())),
            ]
        );
        core.close();
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Case `stale attempt: a replay is already applied`.
    #[tokio::test]
    #[serial]
    async fn a_replay_is_already_applied() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let task_id = stale_task(&pool).await;
        let command = finish(task_id, StaleRetry::NoRetry);
        let _first: StaleAttemptOutcome = recover(&core, &command).await;

        let replay = recover(&core, &command).await;

        assert!(
            matches!(
                replay,
                StaleAttemptOutcome::Refused(AttemptWriteRefusal::AlreadyApplied { .. })
            ),
            "{replay:?}"
        );
        core.close();
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Case `stale attempt: a crash retry by policy` and `stale attempt: the
    /// retried row and its FAILED attempt`.
    #[tokio::test]
    #[serial]
    async fn a_crash_retry_by_policy() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let task_id = stale_task(&pool).await;
        let before: chrono::DateTime<chrono::Utc> = sql::query_scalar("SELECT NOW()")
            .fetch_one(&pool)
            .await
            .expect("database clock");

        let outcome = recover(
            &core,
            &finish(
                task_id,
                StaleRetry::RetryAfter {
                    expected_retry_count: 1,
                    delay_seconds: 5.0,
                },
            ),
        )
        .await;

        let next_retry_at = match outcome {
            StaleAttemptOutcome::RetryScheduled {
                retry_count: 2,
                next_retry_at,
                queue_name,
                ..
            } if queue_name == QUEUE => next_retry_at,
            other => panic!("expected the retry, got {other:?}"),
        };
        assert!(next_retry_at > before + chrono::Duration::seconds(4));
        let row = live_row(&pool, task_id).await.expect("the retried row");
        assert_eq!(row["status"], "PENDING");
        assert_eq!(row["execution"], "ISOLATED");
        assert_eq!(row["retry_count"], 2);
        assert_eq!(row["enqueued_at"], row["next_retry_at"]);
        for cleared in [
            "claimed_at",
            "claimed_by_worker_id",
            "claim_expires_at",
            "started_at",
            "worker_pid",
            "worker_hostname",
            "worker_process_name",
            "finalizing_at",
            "finalizing_by_worker_id",
        ] {
            assert!(row[cleared].is_null(), "a stale retry clears {cleared}");
        }
        assert_eq!(
            attempt_rows(&pool, task_id).await,
            vec![(
                2,
                "FAILED".to_owned(),
                true,
                Some("WORKER_CRASHED".to_owned()),
                Some("crash message".to_owned()),
                Some("crash reason".to_owned()),
                Some(WORKER.to_owned()),
            )]
        );
        core.close();
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Case `stale attempt: a fresh beat is not stale`.
    #[tokio::test]
    #[serial]
    async fn a_fresh_beat_is_not_stale() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let task_id = stale_task(&pool).await;
        beat(&pool, task_id, BeatRole::Runner, Duration::zero()).await;

        let outcome = recover(&core, &finish(task_id, StaleRetry::NoRetry)).await;

        match outcome {
            StaleAttemptOutcome::Refused(AttemptWriteRefusal::SourceStateConflict {
                evidence: GuardEvidence::Staleness(staleness),
                ..
            }) => {
                assert_eq!(staleness.stale_after_ms, 60_000);
                assert!(staleness.last_heartbeat_at.is_some(), "{staleness:?}");
            }
            other => panic!("expected the staleness refusal, got {other:?}"),
        }
        assert_refusal_wrote_nothing(&pool, task_id).await;
        core.close();
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Case `stale attempt: a recent finalizing marker is not stale`.
    #[tokio::test]
    #[serial]
    async fn a_recent_finalizing_marker_is_not_stale() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let task_id = stale_task(&pool).await;
        set_finalizing(&pool, task_id, Since::SecondsAgo(0.0)).await;

        let outcome = recover(&core, &finish(task_id, StaleRetry::NoRetry)).await;

        assert!(
            matches!(
                outcome,
                StaleAttemptOutcome::Refused(AttemptWriteRefusal::SourceStateConflict {
                    evidence: GuardEvidence::Staleness(_),
                    ..
                })
            ),
            "{outcome:?}"
        );
        assert_refusal_wrote_nothing(&pool, task_id).await;
        core.close();
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Case `stale attempt: a retry from another retry count is refused`.
    #[tokio::test]
    #[serial]
    async fn a_retry_from_another_retry_count_is_refused() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let task_id = stale_task(&pool).await;

        let outcome = recover(
            &core,
            &finish(
                task_id,
                StaleRetry::RetryAfter {
                    expected_retry_count: 0,
                    delay_seconds: 5.0,
                },
            ),
        )
        .await;

        match outcome {
            StaleAttemptOutcome::Refused(AttemptWriteRefusal::SourceStateConflict {
                evidence: GuardEvidence::RetryCount(counts),
                ..
            }) => assert_eq!((counts.expected_retry_count, counts.retry_count), (0, 1)),
            other => panic!("expected the retry-count refusal, got {other:?}"),
        }
        assert_refusal_wrote_nothing(&pool, task_id).await;
        core.close();
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Case `stale attempt: a crash retry at or after the deadline fails the
    /// task`.
    #[tokio::test]
    #[serial]
    async fn a_crash_retry_at_or_after_the_deadline_fails_the_task() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let task_id = stale_task(&pool).await;
        set_deadline(&pool, task_id, Deadline::AheadSeconds(2.0)).await;

        let outcome = recover(
            &core,
            &finish(
                task_id,
                StaleRetry::RetryAfter {
                    expected_retry_count: 1,
                    delay_seconds: 5.0,
                },
            ),
        )
        .await;

        assert!(
            matches!(
                outcome,
                StaleAttemptOutcome::FailureApplied {
                    retry: RetryRefusal::PastDeadline { .. },
                    ..
                }
            ),
            "{outcome:?}"
        );
        assert_eq!(live_row(&pool, task_id).await, None);
        core.close();
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Case `stale attempt: a row that is not RUNNING`.
    #[tokio::test]
    #[serial]
    async fn a_row_that_is_not_running() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let task_id = seed_task(&pool, &TaskSeed::claimed()).await;

        let outcome = recover(&core, &finish(task_id, StaleRetry::NoRetry)).await;

        match outcome {
            StaleAttemptOutcome::Refused(AttemptWriteRefusal::SourceStateConflict {
                observed,
                ..
            }) => assert_eq!(observed.status, Some(TaskStatus::Claimed)),
            other => panic!("expected the source-state refusal, got {other:?}"),
        }
        core.close();
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Case `stale attempt: an absent task`.
    #[tokio::test]
    #[serial]
    async fn an_absent_task() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let task_id = Uuid::new_v4();

        let outcome = recover(&core, &finish(task_id, StaleRetry::NoRetry)).await;

        assert_eq!(
            outcome,
            StaleAttemptOutcome::Refused(AttemptWriteRefusal::TaskAbsent { task_id })
        );
        core.close();
    }

    /// Case `stale attempt: a workflow task owes phase 2 and reports its
    /// dependents`.
    #[tokio::test]
    #[serial]
    async fn a_workflow_task_owes_phase_two() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let task_id = seed_task(
            &pool,
            &TaskSeed {
                origin: Origin::WorkflowNode,
                queue_name: QUEUE,
                ..TaskSeed::running()
            },
        )
        .await;
        set_started(&pool, task_id, Since::SecondsAgo(600.0)).await;
        let workflow_id = link_workflow_task(&pool, task_id, "RUNNING", "RUNNING").await;

        let outcome = recover(&core, &finish(task_id, StaleRetry::NoRetry)).await;

        match outcome {
            StaleAttemptOutcome::FailureApplied { after, .. } => assert_eq!(
                after,
                AfterStaleFailure::PhaseTwoOwed(PhaseTwoOwed {
                    task_name: "lifecycle.test".to_owned(),
                    queue_name: QUEUE.to_owned(),
                    step: PhaseTwoStep::NoDependents,
                })
            ),
            other => panic!("expected the stale failure, got {other:?}"),
        }
        assert!(phase2_pending(&pool, task_id).await);
        core.close();
        cleanup_tasks(&pool, &[task_id]).await;
        cleanup_workflows(&pool, &[workflow_id]).await;
    }

    /// Case `stale attempt: refusals write nothing`, for one row.
    async fn assert_refusal_wrote_nothing(pool: &TestPool, task_id: Uuid) {
        assert_eq!(status_of(pool, task_id).await, "RUNNING");
        assert!(attempt_rows(pool, task_id).await.is_empty());
    }
}

mod reaper_pass {
    use super::*;

    /// The reaper's pass on the two statements: a row whose policy names the
    /// crash is retried and its queue is woken with `retry:<task id>`
    /// (Python `core/brokers/postgres.py:3307-3323`); a row with no policy
    /// fails, with no wake (as Python).
    #[tokio::test]
    #[serial]
    async fn a_pass_retries_by_policy_fails_the_rest_and_wakes_the_retried_queue() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let mut wakes = Wakes::listen(&pool, CHANNEL).await;
        let retried = stale_task_with(&pool, Some(crash_retry_options()), 0, 1).await;
        let failed = stale_task_with(&pool, None, 0, 0).await;

        let recovered =
            mark_stale_running_as_failed(&core, StatementLog::Off, thresholds(), whole_scan())
                .await
                .expect("stale pass");

        // The shared test database can hold stale rows of other tests.
        assert!(recovered >= 2, "recovered {recovered}");
        let row = live_row(&pool, retried).await.expect("the retried row");
        assert_eq!(row["status"], "PENDING");
        assert_eq!(row["retry_count"], 1);
        let image = history_row(&pool, failed).await.expect("history row");
        assert_eq!(
            (image.status.as_str(), image.error_code.as_deref()),
            ("FAILED", Some("WORKER_CRASHED"))
        );
        assert_eq!(
            image.final_failed_reason.as_deref(),
            Some("Worker process crashed (no runner heartbeat for 60000ms = 60.0s)")
        );
        assert_eq!(
            wakes.payloads(&pool).await,
            vec![format!("retry:{retried}")]
        );
        core.close();
        cleanup_tasks(&pool, &[retried, failed]).await;
    }
}

mod requeue {
    use super::*;

    /// 5 minutes without a claimer beat (Python's 300000 ms).
    fn claim_threshold() -> ThresholdMs {
        ThresholdMs::from_millis(300_000).expect("5 minutes fits")
    }

    /// A CLAIMED row of `WORKER`, claimed at `generation()`, months before
    /// the database clock, with no beat.
    async fn stale_claim(pool: &TestPool, origin: Origin) -> Uuid {
        seed_task(
            pool,
            &TaskSeed {
                origin,
                ..TaskSeed::claimed()
            },
        )
        .await
    }

    /// Case `requeue: the batch bound`.
    #[tokio::test]
    #[serial]
    async fn the_batch_bound() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let task_ids = [
            stale_claim(&pool, Origin::Send).await,
            stale_claim(&pool, Origin::Send).await,
            stale_claim(&pool, Origin::Send).await,
        ];

        let requeued = requeue_stale_claims(
            &core,
            StatementLog::Off,
            claim_threshold(),
            RecoveryBatch::new(2).expect("2 rows is a recovery batch"),
        )
        .await
        .expect("requeue");

        assert_eq!(requeued, 2);
        core.close();
        cleanup_tasks(&pool, &task_ids).await;
    }

    /// Cases `requeue: silent claims go back; a beaten claim, a fresh claim
    /// and an orphan stay` and `requeue: a requeued row has every claim and
    /// run field cleared`, through the reaper's step, which repeats the
    /// batch until one is short.
    #[tokio::test]
    #[serial]
    async fn silent_claims_go_back_a_beaten_claim_a_fresh_claim_and_an_orphan_stay() {
        let pool = pool().await;
        let core = core_pool_for_test_url(pool.url());
        let silent = stale_claim(&pool, Origin::Send).await;
        let beaten = stale_claim(&pool, Origin::Send).await;
        let fresh = stale_claim(&pool, Origin::Send).await;
        let orphan = stale_claim(&pool, Origin::WorkflowNode).await;
        let linked = stale_claim(&pool, Origin::WorkflowNode).await;
        let workflow_id = link_workflow_task(&pool, linked, "ENQUEUED", "RUNNING").await;
        set_finalizing(&pool, silent, Since::SecondsAgo(0.0)).await;
        beat(&pool, beaten, BeatRole::Claimer, Duration::zero()).await;
        sql::query("UPDATE horsies_tasks SET claimed_at = NOW() WHERE id = $1")
            .bind(fresh)
            .execute(&pool)
            .await
            .expect("claim the row again now");
        beat(&pool, fresh, BeatRole::Claimer, Duration::seconds(1_200)).await;

        let requeued = requeue_stale_claimed(&core, StatementLog::Off, claim_threshold())
            .await
            .expect("stale claim requeue");

        // The shared test database can hold stale claims of other tests.
        assert!(requeued >= 2, "requeued {requeued}");
        let mut statuses = Vec::new();
        for task_id in [silent, beaten, fresh, orphan, linked] {
            statuses.push(status_of(&pool, task_id).await);
        }
        assert_eq!(
            statuses,
            ["PENDING", "CLAIMED", "CLAIMED", "CLAIMED", "PENDING"]
        );
        let row = live_row(&pool, silent).await.expect("the requeued row");
        for cleared in [
            "claimed_at",
            "claimed_by_worker_id",
            "claim_expires_at",
            "started_at",
            "worker_pid",
            "worker_hostname",
            "worker_process_name",
            "finalizing_at",
            "finalizing_by_worker_id",
        ] {
            assert!(row[cleared].is_null(), "a requeue clears {cleared}");
        }
        core.close();
        cleanup_tasks(&pool, &[silent, beaten, fresh, orphan, linked]).await;
        cleanup_workflows(&pool, &[workflow_id]).await;
    }
}

mod round_trips {
    use super::*;

    /// A core pool of one connection through a counting proxy.
    struct CountedPool {
        proxy: WireProxy,
        pool: CorePool,
    }

    async fn counted_pool(pool: &TestPool) -> CountedPool {
        let upstream = pool.url();
        let proxy = WireProxy::start(upstream).await;
        let core = unpinged_single_connection_core_pool_for_url(&proxy.url(upstream));
        CountedPool { proxy, pool: core }
    }

    /// The stale RUNNING scan is one statement.
    #[tokio::test]
    #[serial]
    async fn the_stale_scan_is_one_statement() {
        let pool = pool().await;
        let counted = counted_pool(&pool).await;
        // Open the connection, run the pool's first-use check and prepare
        // the statement.
        let _warm_up =
            scan_stale_running(&counted.pool, StatementLog::Off, thresholds(), whole_scan())
                .await
                .expect("warm up");

        counted.proxy.reset_round_trips();
        let _candidates =
            scan_stale_running(&counted.pool, StatementLog::Off, thresholds(), whole_scan())
                .await
                .expect("stale scan");

        assert_eq!(counted.proxy.round_trips(), 1);
        counted.pool.close();
    }

    /// The stale failure and the stale retry are one statement each.
    #[tokio::test]
    #[serial]
    async fn a_stale_failure_and_a_stale_retry_are_one_statement_each() {
        let pool = pool().await;
        let counted = counted_pool(&pool).await;
        let _warm_up: StaleAttemptOutcome =
            recover(&counted.pool, &finish(Uuid::new_v4(), StaleRetry::NoRetry)).await;
        for retry in [
            StaleRetry::NoRetry,
            StaleRetry::RetryAfter {
                expected_retry_count: 1,
                delay_seconds: 600.0,
            },
        ] {
            let task_id = stale_task(&pool).await;

            counted.proxy.reset_round_trips();
            let outcome = recover(&counted.pool, &finish(task_id, retry)).await;

            assert_eq!(counted.proxy.round_trips(), 1, "{retry:?}");
            assert!(
                matches!(
                    (retry, &outcome),
                    (
                        StaleRetry::NoRetry,
                        StaleAttemptOutcome::FailureApplied { .. }
                    ) | (
                        StaleRetry::RetryAfter { .. },
                        StaleAttemptOutcome::RetryScheduled { .. }
                    )
                ),
                "{retry:?}: {outcome:?}"
            );
            cleanup_tasks(&pool, &[task_id]).await;
        }
        counted.pool.close();
    }

    /// A batch of the stale claim requeue is one statement.
    #[tokio::test]
    #[serial]
    async fn a_stale_claim_requeue_batch_is_one_statement() {
        let pool = pool().await;
        let counted = counted_pool(&pool).await;
        let threshold = ThresholdMs::from_millis(300_000).expect("5 minutes fits");
        let batch = RecoveryBatch::new(500).expect("500 rows is a recovery batch");
        let _warm_up = requeue_stale_claims(&counted.pool, StatementLog::Off, threshold, batch)
            .await
            .expect("warm up");
        let task_id = seed_task(&pool, &TaskSeed::claimed()).await;

        counted.proxy.reset_round_trips();
        let requeued = requeue_stale_claims(&counted.pool, StatementLog::Off, threshold, batch)
            .await
            .expect("requeue");

        assert_eq!(counted.proxy.round_trips(), 1);
        assert_eq!(requeued, 1);
        assert_eq!(status_of(&pool, task_id).await, "PENDING");
        counted.pool.close();
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// A batch of the reaper's pending expiry is one transaction of three
    /// round trips: `BEGIN`, the batch statement and `COMMIT`. The commit
    /// waits for the check of the batch's outcomes.
    #[tokio::test]
    #[serial]
    async fn a_pending_expiry_batch_is_one_transaction_of_three_round_trips() {
        let pool = pool().await;
        drain_pending_expiry(&pool).await;
        let counted = counted_pool(&pool).await;
        let _warm_up = expire_pending_pass(&counted.pool, StatementLog::Off)
            .await
            .expect("warm up");
        let task_id = seed_task(&pool, &TaskSeed::pending()).await;
        set_deadline(&pool, task_id, Deadline::PassedSecondsAgo(1.0)).await;

        counted.proxy.reset_round_trips();
        let expired = expire_pending_pass(&counted.pool, StatementLog::Off)
            .await
            .expect("pending expiry");

        assert_eq!(counted.proxy.round_trips(), 3);
        assert_eq!(expired.expired(), 1);
        assert_eq!(status_of(&pool, task_id).await, "EXPIRED");
        counted.pool.close();
        cleanup_tasks(&pool, &[task_id]).await;
    }
}

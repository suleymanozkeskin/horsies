//! Port of Python `clients/python/tests/integration/test_failed_attempt_finish.py`
//! (`horsies_finish_failed_attempt`).
//!
//! Rows that a core test already checks in full are not repeated:
//! `TestRetry::test_a_retry_writes_pending_and_an_attempt_that_will_retry`,
//! `TestRetry::test_a_same_class_retry_keeps_the_class` and
//! `TestNoWrite::test_a_changed_retry_count_is_part_of_the_fence` are in
//! `attempt_writes::tests` (#599); `TestNoWrite::test_a_claimed_row_is_a_conflict_and_writes_nothing`,
//! `::test_a_foreign_terminalization_is_a_conflict` and
//! `::test_an_unknown_task_is_absent` are `terminalization_functions::fail_locked`
//! (`the_source_status_is_wrong`, `a_stale_failure_is_a_conflict_not_a_replay`,
//! `a_task_that_does_not_exist`).

use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use super::support::*;
use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, FinishFailedAttempt, RetryPlacement, RetryRequest,
};
use crate::core::lifecycle::{
    AfterFailure, AttemptWriteRefusal, FailedAttemptOutcome, OwnedClaim, PhaseTwoOwed,
    PhaseTwoStep, RetryRefusal, TerminalizationKind,
};
use crate::core::types::status::TaskStatus;
use crate::test_support::sql::{self, TestPool};

const QUEUE: &str = "lifecycle_failed_attempt";
const CHANNEL: &str = "task_queue_lifecycle_failed_attempt";
const ERROR_RESULT: &str = r#"{"err": "boom"}"#;

fn running() -> TaskSeed {
    TaskSeed {
        queue_name: QUEUE,
        max_retries: 3,
        ..TaskSeed::running()
    }
}

fn failure(task_id: Uuid, retry: RetryRequest) -> FinishFailedAttempt {
    FinishFailedAttempt {
        task_id,
        fence: owned_claim(),
        retry,
        result_json: ERROR_RESULT.to_owned(),
        error_code: Some("BOOM".to_owned()),
        failed_reason: Some("final reason".to_owned()),
        attempt: AttemptFailure {
            kind: AttemptFailureKind::Failed,
            error_message: Some("boom message".to_owned()),
            failed_reason: Some("attempt reason".to_owned()),
        },
        placement: crate::core::lifecycle::AttemptPlacement::NotObserved,
    }
}

fn retry_after(delay_seconds: f64) -> RetryRequest {
    RetryRequest::RetryAfter {
        expected_retry_count: 0,
        delay_seconds,
        placement: RetryPlacement::SameClass,
    }
}

async fn good_until_of(pool: &TestPool, task_id: Uuid) -> DateTime<Utc> {
    sql::query_scalar("SELECT good_until FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read good_until")
}

mod terminal_failure {
    use super::*;

    /// Python `TestTerminalFailure::test_a_plain_failure_moves_the_row_with_its_attempt_and_wakes_the_queue`.
    #[tokio::test]
    async fn a_plain_failure_moves_the_row_with_its_attempt_and_wakes_the_queue() {
        let pool = pool().await;
        let mut wakes = Wakes::listen(&pool, CHANNEL).await;
        let task_id = seed_task(&pool, &running()).await;

        let outcome = write_failed_attempt(&pool, &failure(task_id, RetryRequest::NoRetry))
            .await
            .expect("failure write");

        let FailedAttemptOutcome::FailureApplied {
            task_id: applied,
            retry: RetryRefusal::NotRequested,
            after: AfterFailure::CapacityWoken,
            observed,
            ..
        } = outcome
        else {
            panic!("expected a plain failure, got {outcome:?}");
        };
        assert_eq!(applied, task_id);
        assert_eq!(observed.status, Some(TaskStatus::Running));
        assert_eq!(observed.worker_id.as_deref(), Some(WORKER));
        assert_eq!(observed.claimed_at, Some(generation()));
        assert_eq!(live_row(&pool, task_id).await, None);
        let image = history_row(&pool, task_id).await.expect("history row");
        assert_eq!(
            (
                image.status.as_str(),
                image.terminalization_kind.as_str(),
                image.error_code.as_deref(),
                image.final_failed_reason.as_deref(),
            ),
            ("FAILED", "FAIL_RUNNING", Some("BOOM"), Some("final reason"))
        );
        assert_eq!(
            image.result_payload.as_deref(),
            Some(ERROR_RESULT.as_bytes())
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
            ),
            (
                1,
                "FAILED",
                false,
                Some("BOOM"),
                Some("boom message"),
                Some("attempt reason"),
            )
        );
        assert_eq!(
            (
                attempt.worker_id(),
                attempt.worker_hostname(),
                attempt.worker_pid(),
                attempt.worker_process_name(),
            ),
            (
                Some(WORKER),
                Some("lifecycle-host"),
                Some(4242),
                Some("lifecycle-process"),
            )
        );
        assert_eq!(wakes.payloads(&pool).await, vec!["capacity".to_owned()]);
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestTerminalFailure::test_a_workflow_task_failure_owes_phase_two_and_sends_no_wake`.
    #[tokio::test]
    async fn a_workflow_task_failure_owes_phase_two_and_sends_no_wake() {
        let pool = pool().await;
        let mut wakes = Wakes::listen(&pool, CHANNEL).await;
        let task_id = seed_task(
            &pool,
            &TaskSeed {
                origin: Origin::WorkflowNode,
                ..running()
            },
        )
        .await;
        let workflow_id = link_workflow_task(&pool, task_id, "RUNNING", "RUNNING").await;

        let outcome = write_failed_attempt(&pool, &failure(task_id, RetryRequest::NoRetry))
            .await
            .expect("failure write");

        let FailedAttemptOutcome::FailureApplied {
            after:
                AfterFailure::PhaseTwoOwed(PhaseTwoOwed {
                    task_name,
                    queue_name,
                    step,
                }),
            ..
        } = outcome
        else {
            panic!("expected phase 2 to be owed, got {outcome:?}");
        };
        assert_eq!(
            (task_name.as_str(), queue_name.as_str()),
            ("lifecycle.test", QUEUE)
        );
        assert_eq!(step, PhaseTwoStep::NoDependents);
        assert_eq!(
            history_row(&pool, task_id)
                .await
                .expect("history row")
                .status,
            "FAILED"
        );
        assert!(phase2_pending(&pool, task_id).await);
        assert!(wakes.payloads(&pool).await.is_empty());
        cleanup_tasks(&pool, &[task_id]).await;
        cleanup_workflows(&pool, &[workflow_id]).await;
    }

    /// Python `TestTerminalFailure::test_a_worker_failure_records_its_attempt_outcome`.
    #[tokio::test]
    async fn a_worker_failure_records_its_attempt_outcome() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &running()).await;
        let worker_failure = FinishFailedAttempt {
            error_code: Some("DATABASE_ERROR".to_owned()),
            failed_reason: Some("worker lost it".to_owned()),
            attempt: AttemptFailure {
                kind: AttemptFailureKind::WorkerFailure,
                error_message: Some("worker lost it".to_owned()),
                failed_reason: Some("worker lost it".to_owned()),
            },
            ..failure(task_id, RetryRequest::NoRetry)
        };

        let outcome = write_failed_attempt(&pool, &worker_failure)
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
                attempt.outcome(),
                attempt.error_code(),
                attempt.failed_reason()
            ),
            (
                "WORKER_FAILURE",
                Some("DATABASE_ERROR"),
                Some("worker lost it")
            )
        );
        cleanup_tasks(&pool, &[task_id]).await;
    }
}

mod retry {
    use super::*;

    /// Python `TestRetry::test_an_isolated_retry_moves_the_row_to_the_isolated_class`.
    #[tokio::test]
    async fn an_isolated_retry_moves_the_row_to_the_isolated_class() {
        let pool = pool().await;
        let task_id = seed_task(
            &pool,
            &TaskSeed {
                execution: "SHARED_ASYNC",
                ..running()
            },
        )
        .await;

        let outcome = write_failed_attempt(
            &pool,
            &failure(
                task_id,
                RetryRequest::RetryAfter {
                    expected_retry_count: 0,
                    delay_seconds: 30.0,
                    placement: RetryPlacement::Isolated,
                },
            ),
        )
        .await
        .expect("retry write");

        assert!(
            matches!(outcome, FailedAttemptOutcome::RetryScheduled { .. }),
            "{outcome:?}"
        );
        assert_eq!(
            live_row(&pool, task_id).await.expect("live row")["execution"],
            "ISOLATED"
        );
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// A retry asked for a task whose deadline does not admit it: the
    /// failure is terminal, its attempt does not retry, and the queue wakes.
    async fn a_retry_past_its_deadline_is_the_terminal_failure(deadline: Deadline) {
        let pool = pool().await;
        let mut wakes = Wakes::listen(&pool, CHANNEL).await;
        let task_id = seed_task(&pool, &running()).await;
        set_deadline(&pool, task_id, deadline).await;
        let seeded_good_until = good_until_of(&pool, task_id).await;

        let outcome = write_failed_attempt(&pool, &failure(task_id, retry_after(60.0)))
            .await
            .expect("failure write");

        let FailedAttemptOutcome::FailureApplied {
            retry:
                RetryRefusal::PastDeadline {
                    next_retry_at,
                    good_until,
                },
            after: AfterFailure::CapacityWoken,
            ..
        } = outcome
        else {
            panic!("expected a terminal failure past the deadline, got {outcome:?}");
        };
        assert_eq!(good_until, seeded_good_until);
        assert!(next_retry_at >= good_until);
        assert_eq!(
            history_row(&pool, task_id)
                .await
                .expect("history row")
                .status,
            "FAILED"
        );
        let records = archived_attempt_records(&pool, task_id).await;
        let [attempt] = records.as_slice() else {
            panic!("expected one attempt, got {records:?}");
        };
        assert!(!attempt.will_retry());
        assert_eq!(wakes.payloads(&pool).await, vec!["capacity".to_owned()]);
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestRetry::test_a_retry_at_or_after_good_until_is_the_terminal_failure`,
    /// case `next_attempt_after_good_until`.
    #[tokio::test]
    async fn a_retry_at_or_after_good_until_is_the_terminal_failure_next_attempt_after_good_until()
    {
        a_retry_past_its_deadline_is_the_terminal_failure(Deadline::AheadSeconds(10.0)).await;
    }

    /// Python `TestRetry::test_a_retry_at_or_after_good_until_is_the_terminal_failure`,
    /// case `good_until_already_past`.
    #[tokio::test]
    async fn a_retry_at_or_after_good_until_is_the_terminal_failure_good_until_already_past() {
        a_retry_past_its_deadline_is_the_terminal_failure(Deadline::PassedSecondsAgo(10.0)).await;
    }

    /// Python `TestRetry::test_a_retry_before_good_until_is_scheduled`.
    #[tokio::test]
    async fn a_retry_before_good_until_is_scheduled() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &running()).await;
        set_deadline(&pool, task_id, Deadline::AheadSeconds(3_600.0)).await;

        let outcome = write_failed_attempt(&pool, &failure(task_id, retry_after(60.0)))
            .await
            .expect("retry write");

        assert!(
            matches!(outcome, FailedAttemptOutcome::RetryScheduled { .. }),
            "{outcome:?}"
        );
        assert_eq!(
            live_row(&pool, task_id).await.expect("live row")["status"],
            "PENDING"
        );
        cleanup_tasks(&pool, &[task_id]).await;
    }
}

mod no_write {
    use super::*;

    /// Python `TestNoWrite::test_a_replay_of_a_failure_is_already_applied_and_writes_nothing`.
    #[tokio::test]
    async fn a_replay_of_a_failure_is_already_applied_and_writes_nothing() {
        let pool = pool().await;
        let mut wakes = Wakes::listen(&pool, CHANNEL).await;
        let task_id = seed_task(&pool, &running()).await;
        let command = failure(task_id, RetryRequest::NoRetry);
        let first = write_failed_attempt(&pool, &command)
            .await
            .expect("failure write");
        let FailedAttemptOutcome::FailureApplied {
            terminal_at: applied_at,
            ..
        } = first
        else {
            panic!("expected a failure, got {first:?}");
        };
        assert_eq!(wakes.payloads(&pool).await, vec!["capacity".to_owned()]);

        let replay = write_failed_attempt(&pool, &command).await.expect("replay");

        let FailedAttemptOutcome::Refused(AttemptWriteRefusal::AlreadyApplied {
            terminal_at,
            kind,
            ..
        }) = replay
        else {
            panic!("expected a replay, got {replay:?}");
        };
        assert_eq!(terminal_at, applied_at);
        assert_eq!(kind, TerminalizationKind::FailRunning);
        assert_eq!(archived_attempts(&pool, task_id).await.len(), 1);
        assert!(wakes.payloads(&pool).await.is_empty());
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestNoWrite::test_a_replay_of_a_scheduled_retry_is_a_lost_claim`:
    /// the retry ended the claim generation, so a replay of the retry is a
    /// lost claim with no second write.
    #[tokio::test]
    async fn a_replay_of_a_scheduled_retry_is_a_lost_claim() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &running()).await;
        let command = failure(task_id, retry_after(30.0));
        let first = write_failed_attempt(&pool, &command)
            .await
            .expect("retry write");
        assert!(
            matches!(first, FailedAttemptOutcome::RetryScheduled { .. }),
            "{first:?}"
        );
        let before = live_row(&pool, task_id).await;

        let replay = write_failed_attempt(&pool, &command).await.expect("replay");

        let FailedAttemptOutcome::Refused(AttemptWriteRefusal::LostClaim { observed, .. }) = replay
        else {
            panic!("expected a lost claim, got {replay:?}");
        };
        assert_eq!(observed.status, Some(TaskStatus::Pending));
        assert_eq!(live_row(&pool, task_id).await, before);
        assert_eq!(live_attempts(&pool, task_id).await.len(), 1);
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestNoWrite::test_a_claim_this_call_does_not_hold_is_a_lost_claim`,
    /// cases `stale_generation` and `another_worker`.
    #[tokio::test]
    async fn a_claim_this_call_does_not_hold_is_a_lost_claim() {
        let pool = pool().await;
        for (worker, shift) in [
            (WORKER, Duration::seconds(1)),
            (OTHER_WORKER, Duration::zero()),
        ] {
            let task_id = seed_task(&pool, &running()).await;
            let before = live_row(&pool, task_id).await;
            let command = FinishFailedAttempt {
                fence: OwnedClaim {
                    worker_id: worker.to_owned(),
                    claimed_at: generation() + shift,
                },
                ..failure(task_id, retry_after(30.0))
            };

            let outcome = write_failed_attempt(&pool, &command)
                .await
                .expect("retry write");

            let FailedAttemptOutcome::Refused(AttemptWriteRefusal::LostClaim { observed, .. }) =
                outcome
            else {
                panic!("{worker}: expected a lost claim, got {outcome:?}");
            };
            assert_eq!(observed.status, Some(TaskStatus::Running));
            assert_eq!(observed.worker_id.as_deref(), Some(WORKER));
            assert_eq!(observed.claimed_at, Some(generation()));
            assert_eq!(live_row(&pool, task_id).await, before);
            assert!(live_attempts(&pool, task_id).await.is_empty());
            cleanup_tasks(&pool, &[task_id]).await;
        }
    }
}

/// The claimed-row rule `RELEASE` (#713): the write of a child that ended
/// without a report returns a row that no child started to the queue, in the
/// same statement, with no attempt row.
mod claimed_row_release {
    use super::*;
    use crate::core::lifecycle::{ReleasedClaim, UnreportedAttemptOutcome};

    fn claimed() -> TaskSeed {
        TaskSeed {
            queue_name: QUEUE,
            max_retries: 3,
            ..TaskSeed::claimed()
        }
    }

    /// A CLAIMED row of this worker at this claim generation goes back to
    /// PENDING with its claim fields cleared and no attempt row.
    #[tokio::test]
    async fn a_claimed_row_of_this_generation_is_released() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &claimed()).await;

        let outcome = write_unreported_attempt(&pool, &failure(task_id, retry_after(60.0)))
            .await
            .expect("release");

        assert_eq!(
            outcome,
            UnreportedAttemptOutcome::Released(ReleasedClaim {
                task_id,
                queue_name: QUEUE.to_owned(),
                observed: crate::core::lifecycle::ObservedTaskState {
                    status: Some(TaskStatus::Claimed),
                    worker_id: Some(WORKER.to_owned()),
                    claimed_at: Some(generation()),
                },
            })
        );
        let row = live_row(&pool, task_id).await.expect("live row");
        assert_eq!(row["status"], "PENDING");
        assert_eq!(row["claimed_by_worker_id"], serde_json::Value::Null);
        assert_eq!(row["claimed_at"], serde_json::Value::Null);
        assert_eq!(row["claim_expires_at"], serde_json::Value::Null);
        assert_eq!(row["retry_count"], 0);
        assert!(live_attempts(&pool, task_id).await.is_empty());
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// A CLAIMED row of an earlier claim generation of this worker is not
    /// released and not written.
    #[tokio::test]
    async fn a_claimed_row_of_another_generation_is_not_released() {
        let pool = pool().await;
        let task_id = seed_task(
            &pool,
            &TaskSeed {
                holder: Holder::Claim {
                    worker_id: WORKER.to_owned(),
                    claimed_at: generation() + Duration::seconds(1),
                },
                ..claimed()
            },
        )
        .await;
        let before = live_row(&pool, task_id).await;

        let outcome = write_unreported_attempt(&pool, &failure(task_id, RetryRequest::NoRetry))
            .await
            .expect("write");

        assert!(
            matches!(
                outcome,
                UnreportedAttemptOutcome::Failed(FailedAttemptOutcome::Refused(_))
            ),
            "{outcome:?}"
        );
        assert_eq!(live_row(&pool, task_id).await, before);
        assert!(live_attempts(&pool, task_id).await.is_empty());
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// A row the child started is RUNNING: the rule `RELEASE` writes the
    /// failure as the rule `REFUSE` does.
    #[tokio::test]
    async fn a_running_row_is_written_as_a_failure() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &running()).await;

        let outcome = write_unreported_attempt(&pool, &failure(task_id, RetryRequest::NoRetry))
            .await
            .expect("write");

        assert!(
            matches!(
                outcome,
                UnreportedAttemptOutcome::Failed(FailedAttemptOutcome::FailureApplied {
                    after: AfterFailure::CapacityWoken,
                    ..
                })
            ),
            "{outcome:?}"
        );
        assert_eq!(live_row(&pool, task_id).await, None);
        assert_eq!(status_of(&pool, task_id).await, "FAILED");
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// The rule `REFUSE` leaves a CLAIMED row of this claim generation as it
    /// is: the child reported, so the row must be RUNNING.
    #[tokio::test]
    async fn the_refuse_rule_leaves_a_claimed_row_in_place() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &claimed()).await;
        let before = live_row(&pool, task_id).await;

        let outcome = write_failed_attempt(&pool, &failure(task_id, RetryRequest::NoRetry))
            .await
            .expect("write");

        assert!(
            matches!(outcome, FailedAttemptOutcome::Refused(_)),
            "{outcome:?}"
        );
        assert_eq!(live_row(&pool, task_id).await, before);
        assert!(live_attempts(&pool, task_id).await.is_empty());
        cleanup_tasks(&pool, &[task_id]).await;
    }
}

mod arguments {
    use super::*;

    /// One wrong call of `horsies_finish_failed_attempt`.
    struct WrongCall {
        name: &'static str,
        retry_count: Option<i32>,
        delay_seconds: Option<f64>,
        placement: Option<&'static str>,
        result: Option<&'static str>,
        attempt_outcome: &'static str,
        sqlstate: &'static str,
    }

    const WRONG_CALLS: [WrongCall; 8] = [
        WrongCall {
            name: "retry_without_delay",
            retry_count: Some(0),
            delay_seconds: None,
            placement: Some("SAME_CLASS"),
            result: Some(ERROR_RESULT),
            attempt_outcome: "FAILED",
            sqlstate: "22023",
        },
        WrongCall {
            name: "delay_without_count",
            retry_count: None,
            delay_seconds: Some(1.0),
            placement: None,
            result: Some(ERROR_RESULT),
            attempt_outcome: "FAILED",
            sqlstate: "22023",
        },
        WrongCall {
            name: "unknown_placement",
            retry_count: Some(0),
            delay_seconds: Some(1.0),
            placement: Some("elsewhere"),
            result: Some(ERROR_RESULT),
            attempt_outcome: "FAILED",
            sqlstate: "22023",
        },
        WrongCall {
            name: "negative_delay",
            retry_count: Some(0),
            delay_seconds: Some(-1.0),
            placement: Some("SAME_CLASS"),
            result: Some(ERROR_RESULT),
            attempt_outcome: "FAILED",
            sqlstate: "22023",
        },
        WrongCall {
            name: "nan_delay",
            retry_count: Some(0),
            delay_seconds: Some(f64::NAN),
            placement: Some("SAME_CLASS"),
            result: Some(ERROR_RESULT),
            attempt_outcome: "FAILED",
            sqlstate: "22023",
        },
        WrongCall {
            name: "infinite_delay",
            retry_count: Some(0),
            delay_seconds: Some(f64::INFINITY),
            placement: Some("SAME_CLASS"),
            result: Some(ERROR_RESULT),
            attempt_outcome: "FAILED",
            sqlstate: "22023",
        },
        WrongCall {
            name: "completed_attempt",
            retry_count: None,
            delay_seconds: None,
            placement: None,
            result: Some(ERROR_RESULT),
            attempt_outcome: "COMPLETED",
            sqlstate: "22023",
        },
        WrongCall {
            name: "no_result",
            retry_count: None,
            delay_seconds: None,
            placement: None,
            result: None,
            attempt_outcome: "FAILED",
            sqlstate: "23502",
        },
    ];

    /// Python `TestArguments::test_a_wrong_argument_raises_and_writes_nothing`:
    /// a wrong argument is refused before any lock or write.
    #[tokio::test]
    async fn a_wrong_argument_raises_and_writes_nothing() {
        let pool = pool().await;
        for call in &WRONG_CALLS {
            let task_id = seed_task(&pool, &running()).await;
            let before = live_row(&pool, task_id).await;

            // A retry request names its three values; any other call is a
            // call with no retry (as Python).
            let retry_kind = match (call.retry_count, call.delay_seconds, call.placement) {
                (None, None, None) => "NONE",
                _ => "RETRY",
            };
            let refused = sql::query(
                "SELECT * FROM horsies_finish_failed_attempt(
                    $1, $2, $3, $4, $5::integer, $6::double precision, $7,
                    $8, 'BOOM', NULL, $9, 'message', NULL, 'REFUSE', TRUE, 'NOT_OBSERVED', NULL
                )",
            )
            .bind(task_id)
            .bind(WORKER)
            .bind(generation())
            .bind(retry_kind)
            .bind(call.retry_count)
            .bind(call.delay_seconds)
            .bind(call.placement)
            .bind(call.result)
            .bind(call.attempt_outcome)
            .fetch_all(&pool)
            .await
            .map(|rows| rows.len());

            assert_refused(refused, call.sqlstate, Constraint::Unnamed);
            assert_eq!(live_row(&pool, task_id).await, before, "{}", call.name);
            assert!(
                live_attempts(&pool, task_id).await.is_empty(),
                "{}",
                call.name
            );
            cleanup_tasks(&pool, &[task_id]).await;
        }
    }
}

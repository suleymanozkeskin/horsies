//! Port of Python `tests/integration/test_terminalization_functions.py`.
//!
//! The rows that a matrix test of `broker/terminalization_matrix.rs` already
//! checks in full are not repeated here. The locked writers
//! `horsies_complete_locked_task` and `horsies_fail_locked_task` leave the
//! core (differences ledger, row 5); their rows run against the
//! one-statement writers that keep their kinds: `horsies_finish_workflow_success`
//! writes `COMPLETE_LOCKED`, `horsies_finish_failed_attempt` writes
//! `FAIL_RUNNING`.

use chrono::Duration;
use serial_test::serial;
use uuid::Uuid;

use super::support::*;
use crate::broker::attempt_writes::{
    finish_failed_attempt, finish_workflow_success, AttemptFailure, AttemptFailureKind,
    FinishFailedAttempt, FinishWorkflowSuccess, RetryRequest,
};
use crate::broker::terminalization::{terminalize, terminalize_in_tx};
use crate::core::lifecycle::outcomes::ObservedClaim;
use crate::core::lifecycle::{
    AttemptWriteRefusal, FailedAttemptOutcome, GuardEvidence, TerminalizationCommand,
    TerminalizationKind, TerminalizationOutcome, WorkerOwned, WorkflowSuccessOutcome,
};
use crate::core::types::status::TaskStatus;

const STALE_AFTER_MS: i32 = 60_000;
const FINALIZING_STALE_AFTER_MS: i32 = 30_000;
const STALE_AFTER_SECONDS: f64 = 60.0;
const FINALIZING_STALE_AFTER_SECONDS: f64 = 30.0;

/// The claim evidence of a row that `WORKER` holds at `generation()`.
fn this_workers_claim() -> GuardEvidence {
    GuardEvidence::Claim(ObservedClaim {
        worker_id: Some(WORKER.to_owned()),
        claimed_at: Some(generation()),
    })
}

fn fused_completion(task_id: Uuid) -> TerminalizationCommand {
    TerminalizationCommand::CompleteTaskFused {
        task_id,
        fence: owned_claim(),
        result_json: r#"{"ok": 2}"#.to_owned(),
        notify_channel: "task_queue_default".to_owned(),
        notify_payload: format!("capacity:{task_id}"),
    }
}

fn workflow_success(task_id: Uuid) -> FinishWorkflowSuccess {
    FinishWorkflowSuccess {
        task_id,
        fence: owned_claim(),
        result_json: r#"{"ok": 1}"#.to_owned(),
    }
}

/// A terminal failure with no retry; `failed_reason` is the final reason of
/// the task and the reason of this attempt.
fn running_failure(task_id: Uuid, failed_reason: Option<&str>) -> FinishFailedAttempt {
    FinishFailedAttempt {
        task_id,
        fence: owned_claim(),
        retry: RetryRequest::NoRetry,
        result_json: r#"{"err": 1}"#.to_owned(),
        error_code: Some("TASK_EXCEPTION".to_owned()),
        failed_reason: failed_reason.map(str::to_owned),
        attempt: AttemptFailure {
            kind: AttemptFailureKind::Failed,
            error_message: Some("attempt raised".to_owned()),
            failed_reason: failed_reason.map(str::to_owned),
        },
    }
}

fn fail_stale(task_id: Uuid) -> TerminalizationCommand {
    TerminalizationCommand::FailStaleTask {
        task_id,
        stale_after_ms: STALE_AFTER_MS,
        finalizing_stale_after_ms: FINALIZING_STALE_AFTER_MS,
        result_json: r#"{"err": 2}"#.to_owned(),
        error_code: "WORKER_CRASHED".to_owned(),
        failed_reason: "Worker crashed".to_owned(),
    }
}

fn expire_claim(task_id: Uuid) -> TerminalizationCommand {
    TerminalizationCommand::ExpireOwnedClaim {
        task_id,
        fence: WorkerOwned {
            worker_id: WORKER.to_owned(),
        },
        result_json: r#"{"err": 3}"#.to_owned(),
        error_code: "TASK_EXPIRED".to_owned(),
    }
}

fn attempt_one_then(next: (i32, &str, Option<&str>)) -> Vec<AttemptSummary> {
    vec![
        (
            1,
            "FAILED".to_owned(),
            Some("attempt one crashed".to_owned()),
        ),
        (next.0, next.1.to_owned(), next.2.map(str::to_owned)),
    ]
}

mod terminal_at_invariant {
    use super::*;

    /// Python `TestTerminalAtInvariant::test_status_and_terminal_at_must_agree`
    /// on the baseline schema. The live table holds only live statuses (CHECK
    /// `horsies_tasks_status_live`; Python's baseline version of this test is
    /// `TestLiveStatusInvariant::test_the_live_table_refuses_a_terminal_status`),
    /// and a history row cannot lack its `terminal_at`: `terminal_at` is the
    /// range key of the history leaves, so partition routing refuses the row
    /// (23514) before the NOT NULL check runs.
    #[tokio::test]
    #[serial]
    async fn status_and_terminal_at_must_agree() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::pending()).await;

        let terminal_in_live_table =
            sqlx::query("UPDATE horsies_tasks SET status = 'CANCELLED' WHERE id = $1")
                .bind(task_id)
                .execute(&pool)
                .await
                .map(|done| done.rows_affected());
        assert_refused(
            terminal_in_live_table,
            "23514",
            Constraint::Named("horsies_tasks_status_live"),
        );
        assert_eq!(
            live_row(&pool, task_id).await.expect("live row")["status"],
            "PENDING"
        );

        let mut connection = pool.acquire().await.expect("acquire");
        let history_without_terminal_at = copy_live_row_to_history(
            &mut *connection,
            task_id,
            "CANCELLED",
            Some("CANCEL_ADMIN"),
            TerminalStamp::Missing,
        )
        .await;
        assert_refused(history_without_terminal_at, "23514", Constraint::Unnamed);
        assert_eq!(history_row(&pool, task_id).await, None);

        cleanup_tasks(&pool, &[task_id]).await;
    }
}

mod applied_transitions {
    use super::*;

    /// Python `TestAppliedTransitions::test_completion_clears_an_earlier_attempts_failure_summary`,
    /// fused arm. Attempt one failed and requeued; attempt two completes. The
    /// terminal row carries no failure summary, and attempt one keeps its
    /// reason.
    #[tokio::test]
    #[serial]
    async fn completion_clears_an_earlier_attempts_failure_summary_fused() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        record_failed_first_attempt(&pool, task_id, "attempt one crashed").await;

        let outcomes = terminalize(&pool, &fused_completion(task_id))
            .await
            .expect("fused completion");

        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::Applied {
                    kind: TerminalizationKind::CompleteFused,
                    ..
                }]
            ),
            "{outcomes:?}"
        );
        let image = history_row(&pool, task_id).await.expect("history row");
        assert_eq!(image.status, "COMPLETED");
        assert_eq!(image.error_code, None);
        assert_eq!(image.final_failed_reason, None);
        assert_eq!(
            archived_attempts(&pool, task_id).await,
            attempt_one_then((2, "COMPLETED", None))
        );

        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestAppliedTransitions::test_completion_clears_an_earlier_attempts_failure_summary`,
    /// locked arm, on `horsies_finish_workflow_success` (kind
    /// `COMPLETE_LOCKED`). The one-statement writer records the completion as
    /// attempt two; Python's locked writer wrote no attempt of its own.
    #[tokio::test]
    #[serial]
    async fn completion_clears_an_earlier_attempts_failure_summary_workflow_success() {
        let pool = pool().await;
        let workflow_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id, "RUNNING").await;
        let task_id = seed_task(
            &pool,
            &TaskSeed {
                origin: Origin::WorkflowNode,
                ..TaskSeed::running()
            },
        )
        .await;
        seed_node(&pool, workflow_id, Some(task_id), "RUNNING").await;
        record_failed_first_attempt(&pool, task_id, "attempt one crashed").await;

        let outcome = finish_workflow_success(&pool, &workflow_success(task_id))
            .await
            .expect("workflow success");

        assert!(
            matches!(outcome, WorkflowSuccessOutcome::SuccessApplied { .. }),
            "{outcome:?}"
        );
        let image = history_row(&pool, task_id).await.expect("history row");
        assert_eq!(image.status, "COMPLETED");
        assert_eq!(image.terminalization_kind, "COMPLETE_LOCKED");
        assert_eq!(image.error_code, None);
        assert_eq!(image.final_failed_reason, None);
        assert_eq!(
            archived_attempts(&pool, task_id).await,
            attempt_one_then((2, "COMPLETED", None))
        );

        cleanup_tasks(&pool, &[task_id]).await;
        cleanup_workflows(&pool, &[workflow_id]).await;
    }
}

mod refusals {
    use super::*;

    /// Python `TestRefusals::test_a_requeued_row_reports_a_lost_claim`. A
    /// PENDING row with its claim cleared is the requeue case: both completion
    /// writers report a lost claim, not a state conflict, and write nothing.
    #[tokio::test]
    #[serial]
    async fn a_requeued_row_reports_a_lost_claim() {
        let pool = pool().await;
        let plain = seed_task(&pool, &TaskSeed::pending()).await;
        let workflow_id = Uuid::new_v4();
        seed_workflow(&pool, workflow_id, "RUNNING").await;
        let workflow_task = seed_task(
            &pool,
            &TaskSeed {
                origin: Origin::WorkflowNode,
                ..TaskSeed::pending()
            },
        )
        .await;
        seed_node(&pool, workflow_id, Some(workflow_task), "ENQUEUED").await;
        let plain_before = live_row(&pool, plain).await;
        let workflow_before = live_row(&pool, workflow_task).await;

        let outcomes = terminalize(&pool, &fused_completion(plain))
            .await
            .expect("fused completion");
        let [TerminalizationOutcome::LostClaim { observed, .. }] = outcomes.as_slice() else {
            panic!("expected a lost claim, got {outcomes:?}");
        };
        assert_eq!(observed.status, Some(TaskStatus::Pending));

        let outcome = finish_workflow_success(&pool, &workflow_success(workflow_task))
            .await
            .expect("workflow success");
        let WorkflowSuccessOutcome::Refused(AttemptWriteRefusal::LostClaim { observed, .. }) =
            outcome
        else {
            panic!("expected a lost claim, got {outcome:?}");
        };
        assert_eq!(observed.status, Some(TaskStatus::Pending));

        assert_eq!(live_row(&pool, plain).await, plain_before);
        assert_eq!(live_row(&pool, workflow_task).await, workflow_before);
        cleanup_tasks(&pool, &[plain, workflow_task]).await;
        cleanup_workflows(&pool, &[workflow_id]).await;
    }
}

mod kind_vocabulary {
    use super::*;

    /// Python `TestKindVocabularyIsEnforced::test_the_database_rejects_a_kind_outside_the_vocabulary`,
    /// on the history table: a typo cannot become history.
    #[tokio::test]
    #[serial]
    #[ignore = "waits on #695"]
    async fn the_database_rejects_a_kind_outside_the_vocabulary() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;

        let mut connection = pool.acquire().await.expect("acquire");
        let refused = copy_live_row_to_history(
            &mut *connection,
            task_id,
            "COMPLETED",
            Some("COMPLETE_ISH"),
            TerminalStamp::Now,
        )
        .await;
        assert_refused(refused, "23514", Constraint::Unnamed);
        assert_eq!(history_row(&pool, task_id).await, None);

        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestKindVocabularyIsEnforced::test_a_null_kind_is_accepted`,
    /// with the core's rule: the NULL arm existed for rows written before the
    /// move functions, and the baseline removes it (differences ledger, row
    /// 4). A history row without a kind is refused.
    #[tokio::test]
    #[serial]
    async fn a_null_kind_is_refused() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;

        let mut connection = pool.acquire().await.expect("acquire");
        let refused = copy_live_row_to_history(
            &mut *connection,
            task_id,
            "COMPLETED",
            None,
            TerminalStamp::Now,
        )
        .await;
        assert_refused(refused, "23502", Constraint::Unnamed);
        assert_eq!(history_row(&pool, task_id).await, None);

        cleanup_tasks(&pool, &[task_id]).await;
    }
}

mod fail_locked {
    //! Python `TestFailLocked`, on `horsies_finish_failed_attempt` (kind
    //! `FAIL_RUNNING`). `test_replay_is_already_applied` is ported by
    //! `attempt_writes::tests::a_plain_failure_moves_the_row_with_its_attempt`.
    use super::*;

    /// Python `TestFailLocked::test_a_null_reason_clears_an_earlier_attempts_reason`.
    #[tokio::test]
    #[serial]
    async fn a_null_reason_clears_an_earlier_attempts_reason() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        record_failed_first_attempt(&pool, task_id, "attempt one crashed").await;

        let outcome = finish_failed_attempt(&pool, &running_failure(task_id, None))
            .await
            .expect("finish failed attempt");

        assert!(
            matches!(outcome, FailedAttemptOutcome::FailureApplied { .. }),
            "{outcome:?}"
        );
        let image = history_row(&pool, task_id).await.expect("history row");
        assert_eq!(image.terminalization_kind, "FAIL_RUNNING");
        assert_eq!(image.error_code.as_deref(), Some("TASK_EXCEPTION"));
        assert_eq!(image.final_failed_reason, None);
        assert_eq!(
            archived_attempts(&pool, task_id).await,
            attempt_one_then((2, "FAILED", None))
        );

        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestFailLocked::test_a_supplied_reason_overwrites`.
    #[tokio::test]
    #[serial]
    async fn a_supplied_reason_overwrites() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        record_failed_first_attempt(&pool, task_id, "attempt one crashed").await;

        let outcome = finish_failed_attempt(
            &pool,
            &running_failure(task_id, Some("attempt two crashed")),
        )
        .await
        .expect("finish failed attempt");

        assert!(
            matches!(outcome, FailedAttemptOutcome::FailureApplied { .. }),
            "{outcome:?}"
        );
        let image = history_row(&pool, task_id).await.expect("history row");
        assert_eq!(
            image.final_failed_reason.as_deref(),
            Some("attempt two crashed")
        );
        assert_eq!(
            archived_attempts(&pool, task_id).await,
            attempt_one_then((2, "FAILED", Some("attempt two crashed")))
        );

        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestFailLocked::test_a_stale_failure_is_a_conflict_not_a_replay`.
    /// Both write FAILED; they are not one class, so the caller is told to
    /// look instead of being told its work is done.
    #[tokio::test]
    #[serial]
    async fn a_stale_failure_is_a_conflict_not_a_replay() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        force_terminal(&pool, task_id, "FAILED", "FAIL_STALE").await;

        let outcome = finish_failed_attempt(&pool, &running_failure(task_id, None))
            .await
            .expect("finish failed attempt");

        let FailedAttemptOutcome::Refused(AttemptWriteRefusal::SourceStateConflict {
            evidence: GuardEvidence::ForeignTerminalization(foreign),
            ..
        }) = outcome
        else {
            panic!("expected a foreign terminalization, got {outcome:?}");
        };
        assert_eq!(foreign.committed_kind, TerminalizationKind::FailStale);

        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestFailLocked::test_the_source_status_is_wrong`: owned by this
    /// worker at this generation, but not yet running.
    #[tokio::test]
    #[serial]
    async fn the_source_status_is_wrong() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::claimed()).await;
        let before = live_row(&pool, task_id).await;

        let outcome = finish_failed_attempt(&pool, &running_failure(task_id, None))
            .await
            .expect("finish failed attempt");

        let FailedAttemptOutcome::Refused(AttemptWriteRefusal::SourceStateConflict {
            observed,
            evidence,
            ..
        }) = outcome
        else {
            panic!("expected a source state conflict, got {outcome:?}");
        };
        assert_eq!(observed.status, Some(TaskStatus::Claimed));
        assert_eq!(evidence, this_workers_claim());
        assert_eq!(live_row(&pool, task_id).await, before);
        assert!(live_attempts(&pool, task_id).await.is_empty());

        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestFailLocked::test_a_task_that_does_not_exist`.
    #[tokio::test]
    #[serial]
    async fn a_task_that_does_not_exist() {
        let pool = pool().await;
        let task_id = Uuid::new_v4();

        let outcome = finish_failed_attempt(&pool, &running_failure(task_id, None))
            .await
            .expect("finish failed attempt");

        assert!(
            matches!(
                outcome,
                FailedAttemptOutcome::Refused(AttemptWriteRefusal::TaskAbsent { .. })
            ),
            "{outcome:?}"
        );
    }
}

mod fail_stale {
    use super::*;

    /// Python `TestFailStale::test_fractional_second_threshold_is_not_truncated`:
    /// the millisecond contract reaches the guard unchanged. The beat and the
    /// call share one transaction, so both read one `NOW()`.
    #[tokio::test]
    #[serial]
    async fn fractional_second_threshold_is_not_truncated() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        ensure_recent_heartbeat_leaves(&pool).await;

        let mut transaction = pool.begin().await.expect("begin");
        sqlx::query(
            "UPDATE horsies_tasks SET started_at = NOW() - interval '10 seconds' WHERE id = $1",
        )
        .bind(task_id)
        .execute(&mut *transaction)
        .await
        .expect("age the start");
        insert_beat(&mut *transaction, task_id, BeatRole::Runner, 1.25).await;
        let outcomes = terminalize_in_tx(
            &mut transaction,
            &TerminalizationCommand::FailStaleTask {
                task_id,
                stale_after_ms: 1_500,
                finalizing_stale_after_ms: 2_750,
                result_json: r#"{"err": 2}"#.to_owned(),
                error_code: "WORKER_CRASHED".to_owned(),
                failed_reason: "Worker crashed".to_owned(),
            },
        )
        .await
        .expect("fail stale");
        transaction.commit().await.expect("commit");

        let [TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::Staleness(staleness),
            ..
        }] = outcomes.as_slice()
        else {
            panic!("expected a staleness refusal, got {outcomes:?}");
        };
        assert_eq!(staleness.stale_after_ms, 1_500);
        assert_eq!(staleness.finalizing_stale_after_ms, 2_750);
        assert_eq!(
            staleness.last_heartbeat_at,
            Some(staleness.evaluated_at - Duration::milliseconds(1_250))
        );

        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestFailStale::test_a_heartbeat_from_another_role_does_not_count`.
    /// Only the runner's beats prove the runner alive; the baseline knows two
    /// roles, so the other role is the claimer.
    #[tokio::test]
    #[serial]
    async fn a_heartbeat_from_another_role_does_not_count() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        set_started(&pool, task_id, Since::SecondsAgo(STALE_AFTER_SECONDS * 2.0)).await;
        beat(&pool, task_id, BeatRole::Claimer, 1.0).await;

        let outcomes = terminalize(&pool, &fail_stale(task_id))
            .await
            .expect("fail stale");

        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::Applied {
                    kind: TerminalizationKind::FailStale,
                    ..
                }]
            ),
            "{outcomes:?}"
        );
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestFailStale::test_a_row_that_never_started_refuses`: with no
    /// `started_at` the guard has no instant to measure silence from.
    #[tokio::test]
    #[serial]
    async fn a_row_that_never_started_refuses() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        set_started(&pool, task_id, Since::Never).await;

        let outcomes = terminalize(&pool, &fail_stale(task_id))
            .await
            .expect("fail stale");

        let [TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::Staleness(staleness),
            ..
        }] = outcomes.as_slice()
        else {
            panic!("expected a staleness refusal, got {outcomes:?}");
        };
        assert_eq!(staleness.started_at, None);
        assert_eq!(staleness.last_heartbeat_at, None);
        assert_eq!(
            live_row(&pool, task_id).await.expect("live row")["status"],
            "RUNNING"
        );
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestFailStale::test_an_active_finalizer_defers`. The runner is
    /// silent past its threshold, so only the finalizer arm can refuse, and
    /// its instant sits inside the window it was judged against.
    #[tokio::test]
    #[serial]
    async fn an_active_finalizer_defers() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        set_started(&pool, task_id, Since::SecondsAgo(STALE_AFTER_SECONDS * 2.0)).await;
        set_finalizing(&pool, task_id, Since::SecondsAgo(1.0)).await;

        let outcomes = terminalize(&pool, &fail_stale(task_id))
            .await
            .expect("fail stale");

        let [TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::Staleness(staleness),
            ..
        }] = outcomes.as_slice()
        else {
            panic!("expected a staleness refusal, got {outcomes:?}");
        };
        let evaluated_at = staleness.evaluated_at;
        let finalizing_at = staleness.finalizing_at.expect("finalizing_at evidence");
        assert!(
            finalizing_at
                >= evaluated_at - Duration::milliseconds(i64::from(FINALIZING_STALE_AFTER_MS))
        );
        let started_at = staleness.started_at.expect("started_at evidence");
        assert!(started_at < evaluated_at - Duration::milliseconds(i64::from(STALE_AFTER_MS)));
        assert_eq!(staleness.last_heartbeat_at, None);

        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestFailStale::test_replay_is_already_applied`.
    #[tokio::test]
    #[serial]
    async fn replay_is_already_applied() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        set_started(&pool, task_id, Since::SecondsAgo(STALE_AFTER_SECONDS * 2.0)).await;
        let first = terminalize(&pool, &fail_stale(task_id))
            .await
            .expect("fail stale");
        assert!(
            matches!(first.as_slice(), [TerminalizationOutcome::Applied { .. }]),
            "{first:?}"
        );

        let replay = terminalize(&pool, &fail_stale(task_id))
            .await
            .expect("replay");

        assert!(
            matches!(
                replay.as_slice(),
                [TerminalizationOutcome::AlreadyApplied {
                    kind: TerminalizationKind::FailStale,
                    ..
                }]
            ),
            "{replay:?}"
        );
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestFailStale::test_a_locked_failure_is_a_conflict_not_a_replay`.
    #[tokio::test]
    #[serial]
    async fn a_locked_failure_is_a_conflict_not_a_replay() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        force_terminal(&pool, task_id, "FAILED", "FAIL_RUNNING").await;

        let outcomes = terminalize(&pool, &fail_stale(task_id))
            .await
            .expect("fail stale");

        let [TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::ForeignTerminalization(foreign),
            ..
        }] = outcomes.as_slice()
        else {
            panic!("expected a foreign terminalization, got {outcomes:?}");
        };
        assert_eq!(foreign.committed_kind, TerminalizationKind::FailRunning);
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestFailStale::test_the_source_status_is_wrong`: not RUNNING,
    /// so no staleness was judged and the evidence is the claim.
    #[tokio::test]
    #[serial]
    async fn the_source_status_is_wrong() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::claimed()).await;

        let outcomes = terminalize(&pool, &fail_stale(task_id))
            .await
            .expect("fail stale");

        let [TerminalizationOutcome::SourceStateConflict {
            observed, evidence, ..
        }] = outcomes.as_slice()
        else {
            panic!("expected a source state conflict, got {outcomes:?}");
        };
        assert_eq!(observed.status, Some(TaskStatus::Claimed));
        assert_eq!(evidence, &this_workers_claim());
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestFailStale::test_a_task_that_does_not_exist`.
    #[tokio::test]
    #[serial]
    async fn a_task_that_does_not_exist() {
        let pool = pool().await;
        let outcomes = terminalize(&pool, &fail_stale(Uuid::new_v4()))
            .await
            .expect("fail stale");
        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::TaskAbsent { .. }]
            ),
            "{outcomes:?}"
        );
    }
}

mod expire_owned_claim {
    use super::*;

    /// Python `TestExpireOwnedClaim::test_expiry_clears_an_earlier_attempts_reason`:
    /// expiry has no failure reason of its own, and attempt one keeps its
    /// reason.
    #[tokio::test]
    #[serial]
    async fn expiry_clears_an_earlier_attempts_reason() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::claimed()).await;
        set_deadline(&pool, task_id, Deadline::PassedSecondsAgo(60.0)).await;
        record_failed_first_attempt(&pool, task_id, "attempt one crashed").await;

        let outcomes = terminalize(&pool, &expire_claim(task_id))
            .await
            .expect("expire claim");

        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::Applied {
                    kind: TerminalizationKind::ExpireClaimed,
                    ..
                }]
            ),
            "{outcomes:?}"
        );
        let image = history_row(&pool, task_id).await.expect("history row");
        assert_eq!(image.error_code.as_deref(), Some("TASK_EXPIRED"));
        assert_eq!(image.final_failed_reason, None);
        assert_eq!(
            archived_attempts(&pool, task_id).await,
            vec![(
                1,
                "FAILED".to_owned(),
                Some("attempt one crashed".to_owned())
            )]
        );
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestExpireOwnedClaim::test_deadline_change_while_waiting_on_the_row_is_rechecked`.
    /// A concurrent transaction holds the row with a past deadline; the
    /// expiry waits for it and applies from the locked post-commit image.
    #[tokio::test]
    #[serial]
    async fn deadline_change_while_waiting_on_the_row_is_rechecked() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::claimed()).await;
        set_deadline(&pool, task_id, Deadline::AheadSeconds(3_600.0)).await;

        let mut mutator = pool.begin().await.expect("begin mutator");
        let mutator_pid = backend_pid(&mut *mutator).await;
        sqlx::query(
            "UPDATE horsies_tasks SET good_until = NOW() - interval '1 hour' WHERE id = $1",
        )
        .bind(task_id)
        .execute(&mut *mutator)
        .await
        .expect("move the deadline under the row lock");

        let mut applier = pool.begin().await.expect("begin applier");
        let applier_pid = backend_pid(&mut *applier).await;
        let command = expire_claim(task_id);
        let application = tokio::spawn(async move {
            let outcomes = terminalize_in_tx(&mut applier, &command).await;
            applier.commit().await.expect("commit applier");
            outcomes
        });

        let waited = waits_on(&pool, applier_pid, mutator_pid).await;
        match waited {
            true => mutator.commit().await.expect("commit mutator"),
            false => mutator.rollback().await.expect("roll back mutator"),
        }
        let outcomes = tokio::time::timeout(std::time::Duration::from_secs(5), application)
            .await
            .expect("the expiry ends after the mutator")
            .expect("join the expiry")
            .expect("expire claim");
        assert!(waited, "the expiry never waited on the row lock");
        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::Applied {
                    kind: TerminalizationKind::ExpireClaimed,
                    ..
                }]
            ),
            "{outcomes:?}"
        );
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestExpireOwnedClaim::test_a_row_with_no_deadline_refuses`.
    #[tokio::test]
    #[serial]
    async fn a_row_with_no_deadline_refuses() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::claimed()).await;
        set_deadline(&pool, task_id, Deadline::Absent).await;

        let outcomes = terminalize(&pool, &expire_claim(task_id))
            .await
            .expect("expire claim");

        let [TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::Deadline(deadline),
            ..
        }] = outcomes.as_slice()
        else {
            panic!("expected a deadline refusal, got {outcomes:?}");
        };
        assert_eq!(deadline.good_until, None);
        assert_eq!(
            live_row(&pool, task_id).await.expect("live row")["status"],
            "CLAIMED"
        );
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestExpireOwnedClaim::test_the_pending_expiry_counts_as_the_same_work`.
    #[tokio::test]
    #[serial]
    async fn the_pending_expiry_counts_as_the_same_work() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::claimed()).await;
        force_terminal(&pool, task_id, "EXPIRED", "EXPIRE_PENDING").await;

        let outcomes = terminalize(&pool, &expire_claim(task_id))
            .await
            .expect("expire claim");

        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::AlreadyApplied {
                    kind: TerminalizationKind::ExpirePending,
                    ..
                }]
            ),
            "{outcomes:?}"
        );
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestExpireOwnedClaim::test_a_foreign_kind_is_a_conflict_not_a_replay`.
    #[tokio::test]
    #[serial]
    async fn a_foreign_kind_is_a_conflict_not_a_replay() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::claimed()).await;
        force_terminal(&pool, task_id, "FAILED", "FAIL_STALE").await;

        let outcomes = terminalize(&pool, &expire_claim(task_id))
            .await
            .expect("expire claim");

        let [TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::ForeignTerminalization(foreign),
            ..
        }] = outcomes.as_slice()
        else {
            panic!("expected a foreign terminalization, got {outcomes:?}");
        };
        assert_eq!(foreign.committed_kind, TerminalizationKind::FailStale);
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestExpireOwnedClaim::test_the_source_status_is_wrong`: RUNNING
    /// is past this operation's window even with the deadline gone.
    #[tokio::test]
    #[serial]
    async fn the_source_status_is_wrong() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        set_deadline(&pool, task_id, Deadline::PassedSecondsAgo(60.0)).await;

        let outcomes = terminalize(&pool, &expire_claim(task_id))
            .await
            .expect("expire claim");

        let [TerminalizationOutcome::SourceStateConflict {
            observed, evidence, ..
        }] = outcomes.as_slice()
        else {
            panic!("expected a source state conflict, got {outcomes:?}");
        };
        assert_eq!(observed.status, Some(TaskStatus::Running));
        assert_eq!(evidence, &this_workers_claim());
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestExpireOwnedClaim::test_a_task_that_does_not_exist`.
    #[tokio::test]
    #[serial]
    async fn a_task_that_does_not_exist() {
        let pool = pool().await;
        let outcomes = terminalize(&pool, &expire_claim(Uuid::new_v4()))
            .await
            .expect("expire claim");
        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::TaskAbsent { .. }]
            ),
            "{outcomes:?}"
        );
    }
}

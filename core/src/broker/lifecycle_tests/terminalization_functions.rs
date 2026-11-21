//! The terminalization functions of 0004.
//!
//! The rows that a matrix test of `broker/terminalization_matrix.rs` already
//! checks in full are not repeated here. The rows run against the 0004
//! writers that keep their kinds:
//! `horsies_finish_workflow_success` writes `COMPLETE_LOCKED`,
//! `horsies_finish_failed_attempt` writes `FAIL_RUNNING`,
//! `horsies_finish_stale_attempt` (with no retry) writes `FAIL_STALE` and
//! `horsies_cancel_task` writes `CANCEL_ADMIN`.

use chrono::Duration;
use uuid::Uuid;

use super::support::*;
use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, FinishFailedAttempt, FinishWorkflowSuccess, RetryRequest,
};
use crate::broker::terminalization::on_test_pool::terminalize;
use crate::broker::terminalization::on_test_pool::terminalize_in_transaction;
use crate::core::lifecycle::outcomes::{ObservedClaim, ObservedWorkflowLink};
use crate::core::lifecycle::{
    AttemptWriteRefusal, BatchSize, FailedAttemptOutcome, GuardEvidence, OwnedClaim,
    OwnedClaimBatch, TerminalizationCommand, TerminalizationKind, TerminalizationOutcome,
    WorkerOwned, WorkflowSuccessOutcome,
};
use crate::core::types::status::TaskStatus;
use crate::test_support::sql;
use crate::test_support::terminal_writes::{self, StaleFailure};

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

/// The stale RUNNING failure with no retry (`horsies_finish_stale_attempt`).
fn stale_failure() -> StaleFailure {
    StaleFailure {
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
    async fn status_and_terminal_at_must_agree() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::pending()).await;

        let terminal_in_live_table =
            sql::query("UPDATE horsies_tasks SET status = 'CANCELLED' WHERE id = $1")
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

        let outcome = write_workflow_success(&pool, &workflow_success(task_id))
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

        let outcome = write_workflow_success(&pool, &workflow_success(workflow_task))
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

    /// Two kinds share a class of `horsies_terminalization_family` exactly
    /// when they share a class of `equivalence_class_of`. A terminal write
    /// that missed answers `AlreadyApplied` by the database class; the core
    /// judges a replay by its own.
    #[tokio::test]
    async fn the_miss_classes_of_the_database_are_the_equivalence_classes() {
        let pool = pool().await;
        let mut families: Vec<(TerminalizationKind, String)> = Vec::new();
        for kind in TerminalizationKind::ALL {
            let family: Option<String> =
                sql::query_scalar("SELECT horsies_terminalization_family($1)")
                    .bind(kind.as_str())
                    .fetch_one(&pool)
                    .await
                    .expect("read the database class");
            let Some(family) = family else {
                panic!("{} has no database class", kind.as_str());
            };
            families.push((kind, family));
        }
        for (kind, family) in &families {
            let mut same: Vec<&str> = families
                .iter()
                .filter(|(_, other)| other == family)
                .map(|(other, _)| other.as_str())
                .collect();
            same.sort_unstable();
            let mut expected: Vec<&str> = crate::core::lifecycle::equivalence_class_of(*kind)
                .iter()
                .map(|member| member.as_str())
                .collect();
            expected.sort_unstable();
            assert_eq!(same, expected, "{}", kind.as_str());
        }
    }
}

mod fail_locked {
    //! Python `TestFailLocked`, on `horsies_finish_failed_attempt` (kind
    //! `FAIL_RUNNING`). `test_replay_is_already_applied` is ported by
    //! `attempt_writes::tests::a_plain_failure_moves_the_row_with_its_attempt`.
    use super::*;

    /// Python `TestFailLocked::test_a_null_reason_clears_an_earlier_attempts_reason`.
    #[tokio::test]
    async fn a_null_reason_clears_an_earlier_attempts_reason() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        record_failed_first_attempt(&pool, task_id, "attempt one crashed").await;

        let outcome = write_failed_attempt(&pool, &running_failure(task_id, None))
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
    async fn a_supplied_reason_overwrites() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        record_failed_first_attempt(&pool, task_id, "attempt one crashed").await;

        let outcome = write_failed_attempt(
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
    async fn a_stale_failure_is_a_conflict_not_a_replay() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        force_terminal(&pool, task_id, "FAILED", "FAIL_STALE").await;

        let outcome = write_failed_attempt(&pool, &running_failure(task_id, None))
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
    async fn the_source_status_is_wrong() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::claimed()).await;
        let before = live_row(&pool, task_id).await;

        let outcome = write_failed_attempt(&pool, &running_failure(task_id, None))
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
    async fn a_task_that_does_not_exist() {
        let pool = pool().await;
        let task_id = Uuid::new_v4();

        let outcome = write_failed_attempt(&pool, &running_failure(task_id, None))
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
    //! Python `TestFailStale`, on `horsies_finish_stale_attempt` (kind
    //! `FAIL_STALE`), with no retry.
    use super::*;

    /// Python `TestFailStale::test_fractional_second_threshold_is_not_truncated`:
    /// the millisecond contract reaches the guard unchanged. The beat and the
    /// call share one transaction, so both read one `NOW()`.
    #[tokio::test]
    async fn fractional_second_threshold_is_not_truncated() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        ensure_recent_heartbeat_leaves(&pool).await;

        let mut transaction = pool.begin().await.expect("begin");
        sql::query(
            "UPDATE horsies_tasks SET started_at = NOW() - interval '10 seconds' WHERE id = $1",
        )
        .bind(task_id)
        .execute(&mut *transaction)
        .await
        .expect("age the start");
        insert_beat(
            &mut *transaction,
            task_id,
            BeatRole::Runner,
            Duration::milliseconds(1_250),
        )
        .await;
        let outcomes = terminal_writes::fail_stale_task(
            &mut *transaction,
            task_id,
            &StaleFailure {
                stale_after_ms: 1_500,
                finalizing_stale_after_ms: 2_750,
                ..stale_failure()
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
    async fn a_heartbeat_from_another_role_does_not_count() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        set_started(&pool, task_id, Since::SecondsAgo(STALE_AFTER_SECONDS * 2.0)).await;
        beat(&pool, task_id, BeatRole::Claimer, Duration::seconds(1)).await;

        let outcomes = terminal_writes::fail_stale_task(&pool, task_id, &stale_failure())
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
    async fn a_row_that_never_started_refuses() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        set_started(&pool, task_id, Since::Never).await;

        let outcomes = terminal_writes::fail_stale_task(&pool, task_id, &stale_failure())
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
    async fn an_active_finalizer_defers() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        set_started(&pool, task_id, Since::SecondsAgo(STALE_AFTER_SECONDS * 2.0)).await;
        set_finalizing(&pool, task_id, Since::SecondsAgo(1.0)).await;

        let outcomes = terminal_writes::fail_stale_task(&pool, task_id, &stale_failure())
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
    async fn replay_is_already_applied() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        set_started(&pool, task_id, Since::SecondsAgo(STALE_AFTER_SECONDS * 2.0)).await;
        let first = terminal_writes::fail_stale_task(&pool, task_id, &stale_failure())
            .await
            .expect("fail stale");
        assert!(
            matches!(first.as_slice(), [TerminalizationOutcome::Applied { .. }]),
            "{first:?}"
        );

        let replay = terminal_writes::fail_stale_task(&pool, task_id, &stale_failure())
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
    async fn a_locked_failure_is_a_conflict_not_a_replay() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::running()).await;
        force_terminal(&pool, task_id, "FAILED", "FAIL_RUNNING").await;

        let outcomes = terminal_writes::fail_stale_task(&pool, task_id, &stale_failure())
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
    async fn the_source_status_is_wrong() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::claimed()).await;

        let outcomes = terminal_writes::fail_stale_task(&pool, task_id, &stale_failure())
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
    async fn a_task_that_does_not_exist() {
        let pool = pool().await;
        let outcomes = terminal_writes::fail_stale_task(&pool, Uuid::new_v4(), &stale_failure())
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
    async fn deadline_change_while_waiting_on_the_row_is_rechecked() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::claimed()).await;
        set_deadline(&pool, task_id, Deadline::AheadSeconds(3_600.0)).await;

        let mut mutator = pool.begin().await.expect("begin mutator");
        let mutator_pid = backend_pid(&mut *mutator).await;
        sql::query("UPDATE horsies_tasks SET good_until = NOW() - interval '1 hour' WHERE id = $1")
            .bind(task_id)
            .execute(&mut *mutator)
            .await
            .expect("move the deadline under the row lock");

        let mut applier = pool.begin().await.expect("begin applier");
        let applier_pid = backend_pid(&mut *applier).await;
        let command = expire_claim(task_id);
        let application = tokio::spawn(async move {
            let outcomes = terminalize_in_transaction(&mut applier, &command).await;
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

    /// Python `TestExpireOwnedClaim::test_another_workers_claim_is_lost`: the
    /// deadline has passed, but another worker holds the claim, so the
    /// expiry is a lost claim that names that worker, and the row stays
    /// CLAIMED.
    #[tokio::test]
    async fn another_workers_claim_is_lost() {
        let pool = pool().await;
        let task_id = seed_task(
            &pool,
            &TaskSeed {
                holder: Holder::other_worker(),
                ..TaskSeed::claimed()
            },
        )
        .await;
        set_deadline(&pool, task_id, Deadline::PassedSecondsAgo(60.0)).await;

        let outcomes = terminalize(&pool, &expire_claim(task_id))
            .await
            .expect("expire claim");

        let [TerminalizationOutcome::LostClaim { observed, .. }] = outcomes.as_slice() else {
            panic!("expected a lost claim, got {outcomes:?}");
        };
        assert_eq!(observed.worker_id.as_deref(), Some(OTHER_WORKER));
        assert_eq!(
            live_row(&pool, task_id).await.expect("live row")["status"],
            "CLAIMED"
        );
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestExpireOwnedClaim::test_the_pending_expiry_counts_as_the_same_work`.
    #[tokio::test]
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

fn expire_pending(batch_size: i32) -> TerminalizationCommand {
    TerminalizationCommand::ExpirePendingTasks {
        batch_size: BatchSize::new(batch_size).expect("positive batch size"),
        result_json: r#"{"err": 4}"#.to_owned(),
        error_code: "TASK_EXPIRED".to_owned(),
    }
}

fn cancel_owned_orphan(task_id: Uuid, fence: OwnedClaim) -> TerminalizationCommand {
    TerminalizationCommand::CancelOwnedOrphan { task_id, fence }
}

fn abandon_owned(task_id: Uuid, fence: OwnedClaim) -> TerminalizationCommand {
    TerminalizationCommand::AbandonOwnedNode { task_id, fence }
}

fn cancel_owned_node(task_id: Uuid, fence: OwnedClaim) -> TerminalizationCommand {
    TerminalizationCommand::CancelOwnedNode {
        task_id,
        fence,
        accepts_requeued_pending: true,
    }
}

/// The fence of `WORKER` one second before `generation()`.
fn stale_claim() -> OwnedClaim {
    OwnedClaim {
        worker_id: WORKER.to_owned(),
        claimed_at: generation() - Duration::seconds(1),
    }
}

/// A claimed workflow task of `WORKER` at `generation()`.
fn claimed_workflow_task() -> TaskSeed {
    TaskSeed {
        origin: Origin::WorkflowNode,
        ..TaskSeed::claimed()
    }
}

/// The task ids of `outcomes` that are in `task_ids`, in outcome order.
fn outcome_ids_among(outcomes: &[TerminalizationOutcome], task_ids: &[Uuid]) -> Vec<Uuid> {
    outcomes
        .iter()
        .map(TerminalizationOutcome::task_id)
        .filter(|task_id| task_ids.contains(task_id))
        .collect()
}

mod expire_pending_tasks {
    use super::*;

    /// Python `TestExpirePendingTasks::test_the_sweep_clears_an_earlier_attempts_reason`:
    /// a requeued PENDING row expires without its old reason, and attempt one
    /// keeps its reason.
    #[tokio::test]
    async fn the_sweep_clears_an_earlier_attempts_reason() {
        let pool = pool().await;
        drain_pending_expiry(&pool).await;
        let task_id = seed_task(&pool, &TaskSeed::pending()).await;
        set_deadline(&pool, task_id, Deadline::PassedSecondsAgo(60.0)).await;
        record_failed_first_attempt(&pool, task_id, "attempt one crashed").await;

        let outcomes = terminalize(&pool, &expire_pending(10))
            .await
            .expect("expire pending");

        assert_eq!(
            outcomes
                .iter()
                .map(TerminalizationOutcome::task_id)
                .collect::<Vec<_>>(),
            vec![task_id]
        );
        let image = history_row(&pool, task_id).await.expect("history row");
        assert_eq!(image.status, "EXPIRED");
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

    /// Python `TestExpirePendingTasks::test_a_row_a_concurrent_claim_holds_is_stepped_around`:
    /// SKIP LOCKED, so neither side waits.
    #[tokio::test]
    async fn a_row_a_concurrent_claim_holds_is_stepped_around() {
        let pool = pool().await;
        drain_pending_expiry(&pool).await;
        let held = seed_task(&pool, &TaskSeed::pending()).await;
        set_deadline(&pool, held, Deadline::PassedSecondsAgo(60.0)).await;
        let free = seed_task(&pool, &TaskSeed::pending()).await;
        set_deadline(&pool, free, Deadline::PassedSecondsAgo(60.0)).await;

        let mut holder = pool.begin().await.expect("begin holder");
        sql::query("SELECT id FROM horsies_tasks WHERE id = $1 FOR UPDATE")
            .bind(held)
            .execute(&mut *holder)
            .await
            .expect("hold the row lock");
        let outcomes = terminalize(&pool, &expire_pending(10))
            .await
            .expect("expire pending");
        holder.rollback().await.expect("release the row lock");

        assert_eq!(
            outcomes
                .iter()
                .map(TerminalizationOutcome::task_id)
                .collect::<Vec<_>>(),
            vec![free]
        );
        assert_eq!(
            live_row(&pool, held).await.expect("held row")["status"],
            "PENDING"
        );
        cleanup_tasks(&pool, &[held, free]).await;
    }
}

mod cancel_task {
    //! Python `TestCancelTask`, on `horsies_cancel_task` (kind
    //! `CANCEL_ADMIN`), with no RUNNING opt-in.
    use super::*;

    /// Python `TestCancelTask::test_replay_is_already_applied`.
    #[tokio::test]
    async fn replay_is_already_applied() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &TaskSeed::claimed()).await;
        let first = terminal_writes::cancel_live_task(&pool, task_id, false)
            .await
            .expect("cancel");
        assert!(
            matches!(first.as_slice(), [TerminalizationOutcome::Applied { .. }]),
            "{first:?}"
        );

        let replay = terminal_writes::cancel_live_task(&pool, task_id, false)
            .await
            .expect("replay");

        assert!(
            matches!(
                replay.as_slice(),
                [TerminalizationOutcome::AlreadyApplied {
                    kind: TerminalizationKind::CancelAdmin,
                    ..
                }]
            ),
            "{replay:?}"
        );
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestCancelTask::test_absence_is_an_outcome`.
    #[tokio::test]
    async fn absence_is_an_outcome() {
        let pool = pool().await;
        let outcomes = terminal_writes::cancel_live_task(&pool, Uuid::new_v4(), false)
            .await
            .expect("cancel");
        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::TaskAbsent { .. }]
            ),
            "{outcomes:?}"
        );
    }
}

mod cancel_owned_orphan {
    use super::*;

    /// Python `TestCancelOwnedOrphan::test_a_terminal_link_still_leaves_the_backing_task_orphaned`:
    /// a node that is already COMPLETED is not a runnable link.
    #[tokio::test]
    async fn a_terminal_link_still_leaves_the_backing_task_orphaned() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &claimed_workflow_task()).await;
        let workflow_id = link_workflow_task(&pool, task_id, "COMPLETED", "RUNNING").await;

        let outcomes = terminalize(&pool, &cancel_owned_orphan(task_id, owned_claim()))
            .await
            .expect("cancel orphan");

        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::Applied {
                    kind: TerminalizationKind::CancelOrphan,
                    ..
                }]
            ),
            "{outcomes:?}"
        );
        let image = history_row(&pool, task_id).await.expect("history row");
        assert_eq!(image.status, "CANCELLED");
        assert_eq!(image.workflow_id, Some(workflow_id));
        cleanup_tasks(&pool, &[task_id]).await;
        cleanup_workflows(&pool, &[workflow_id]).await;
    }

    /// Python `TestCancelOwnedOrphan::test_a_stale_generation_is_a_lost_claim_even_with_a_live_link`:
    /// the fence is judged before the link guard.
    #[tokio::test]
    async fn a_stale_generation_is_a_lost_claim_even_with_a_live_link() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &claimed_workflow_task()).await;
        let workflow_id = link_workflow_task(&pool, task_id, "ENQUEUED", "RUNNING").await;

        let outcomes = terminalize(
            &pool,
            &cancel_owned_orphan(
                task_id,
                OwnedClaim {
                    worker_id: WORKER.to_owned(),
                    claimed_at: generation() - Duration::minutes(1),
                },
            ),
        )
        .await
        .expect("cancel orphan");

        let [TerminalizationOutcome::LostClaim { observed, .. }] = outcomes.as_slice() else {
            panic!("expected a lost claim, got {outcomes:?}");
        };
        assert_eq!(observed.claimed_at, Some(generation()));
        assert_eq!(
            live_row(&pool, task_id).await.expect("live row")["status"],
            "CLAIMED"
        );
        cleanup_tasks(&pool, &[task_id]).await;
        cleanup_workflows(&pool, &[workflow_id]).await;
    }

    /// Python `TestCancelOwnedOrphan::test_the_sweep_kind_counts_as_the_same_work`.
    #[tokio::test]
    async fn the_sweep_kind_counts_as_the_same_work() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &claimed_workflow_task()).await;
        force_terminal(&pool, task_id, "CANCELLED", "CANCEL_ORPHAN_SWEEP").await;

        let outcomes = terminalize(&pool, &cancel_owned_orphan(task_id, owned_claim()))
            .await
            .expect("cancel orphan");

        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::AlreadyApplied {
                    kind: TerminalizationKind::CancelOrphanSweep,
                    ..
                }]
            ),
            "{outcomes:?}"
        );
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestCancelOwnedOrphan::test_an_administrative_cancellation_is_a_conflict`.
    #[tokio::test]
    async fn an_administrative_cancellation_is_a_conflict() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &claimed_workflow_task()).await;
        force_terminal(&pool, task_id, "CANCELLED", "CANCEL_ADMIN").await;

        let outcomes = terminalize(&pool, &cancel_owned_orphan(task_id, owned_claim()))
            .await
            .expect("cancel orphan");

        let [TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::ForeignTerminalization(foreign),
            ..
        }] = outcomes.as_slice()
        else {
            panic!("expected a foreign terminalization, got {outcomes:?}");
        };
        assert_eq!(foreign.committed_kind, TerminalizationKind::CancelAdmin);
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestCancelOwnedOrphan::test_absence_is_an_outcome`.
    #[tokio::test]
    async fn absence_is_an_outcome() {
        let pool = pool().await;
        let outcomes = terminalize(&pool, &cancel_owned_orphan(Uuid::new_v4(), owned_claim()))
            .await
            .expect("cancel orphan");
        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::TaskAbsent { .. }]
            ),
            "{outcomes:?}"
        );
    }
}

mod cancel_orphaned_tasks {
    use super::*;

    /// Python `TestCancelOrphanedTasks::test_transitions_only_orphans_in_the_effective_source_set`:
    /// the sweep cancels unlinked workflow tasks that are CLAIMED or PENDING;
    /// a linked task, a plain task and a RUNNING task stay.
    #[tokio::test]
    async fn transitions_only_orphans_in_the_effective_source_set() {
        let pool = pool().await;
        drain_orphan_sweep(&pool).await;
        let claimed = seed_task(&pool, &claimed_workflow_task()).await;
        let pending = seed_task(
            &pool,
            &TaskSeed {
                origin: Origin::WorkflowNode,
                ..TaskSeed::pending()
            },
        )
        .await;
        let linked = seed_task(&pool, &claimed_workflow_task()).await;
        let workflow_id = link_workflow_task(&pool, linked, "READY", "RUNNING").await;
        let plain = seed_task(&pool, &TaskSeed::claimed()).await;
        let running = seed_task(
            &pool,
            &TaskSeed {
                origin: Origin::WorkflowNode,
                ..TaskSeed::running()
            },
        )
        .await;
        let mine = [claimed, pending, linked, plain, running];

        let outcomes = sweep_orphans_one_cycle(&pool).await;

        let mut swept = outcome_ids_among(&outcomes, &mine);
        swept.sort_unstable();
        let mut expected = vec![claimed, pending];
        expected.sort_unstable();
        assert_eq!(swept, expected);
        for outcome in outcomes.iter().filter(|o| mine.contains(&o.task_id())) {
            assert!(
                matches!(
                    outcome,
                    TerminalizationOutcome::Applied {
                        kind: TerminalizationKind::CancelOrphanSweep,
                        ordinality: None,
                        ..
                    }
                ),
                "{outcome:?}"
            );
        }
        for (task_id, status) in [
            (linked, "CLAIMED"),
            (plain, "CLAIMED"),
            (running, "RUNNING"),
        ] {
            assert_eq!(
                live_row(&pool, task_id).await.expect("live row")["status"],
                status
            );
        }
        for task_id in [claimed, pending] {
            assert_eq!(
                history_row(&pool, task_id)
                    .await
                    .expect("history row")
                    .status,
                "CANCELLED"
            );
        }
        cleanup_tasks(&pool, &mine).await;
        cleanup_workflows(&pool, &[workflow_id]).await;
    }

    /// Python `TestCancelOrphanedTasks::test_a_locked_orphan_is_stepped_around`.
    #[tokio::test]
    async fn a_locked_orphan_is_stepped_around() {
        let pool = pool().await;
        drain_orphan_sweep(&pool).await;
        let held = seed_task(&pool, &claimed_workflow_task()).await;
        let free = seed_task(&pool, &claimed_workflow_task()).await;

        let mut holder = pool.begin().await.expect("begin holder");
        sql::query("SELECT id FROM horsies_tasks WHERE id = $1 FOR UPDATE")
            .bind(held)
            .execute(&mut *holder)
            .await
            .expect("hold the row lock");
        let outcomes = sweep_orphans_one_cycle(&pool).await;
        holder.rollback().await.expect("release the row lock");

        assert_eq!(outcome_ids_among(&outcomes, &[held, free]), vec![free]);
        assert_eq!(
            live_row(&pool, held).await.expect("held row")["status"],
            "CLAIMED"
        );
        cleanup_tasks(&pool, &[held, free]).await;
    }
}

mod abandon_owned_node {
    use super::*;

    /// Python `TestAbandonOwnedNode::test_a_null_generation_keeps_the_worker_fence`,
    /// with the core's rule: the stored function refuses a NULL generation
    /// before any lock or write (differences ledger, row 3).
    #[tokio::test]
    async fn a_null_generation_is_refused() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &claimed_workflow_task()).await;
        let before = live_row(&pool, task_id).await;

        let refused = sql::query("SELECT * FROM horsies_abandon_owned_node($1, $2, NULL)")
            .bind(task_id)
            .bind(WORKER)
            .fetch_all(&pool)
            .await
            .map(|rows| rows.len());

        assert_refused(refused, "22023", Constraint::Unnamed);
        assert_eq!(live_row(&pool, task_id).await, before);
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestAbandonOwnedNode::test_pause_kinds_are_equivalent_but_workflow_cancel_is_not`.
    #[tokio::test]
    async fn pause_kinds_are_equivalent_but_workflow_cancel_is_not() {
        let pool = pool().await;
        let same = seed_task(&pool, &claimed_workflow_task()).await;
        force_terminal(&pool, same, "CANCELLED", "PAUSE_ABANDON_WORKFLOW").await;
        let foreign = seed_task(&pool, &claimed_workflow_task()).await;
        force_terminal(&pool, foreign, "CANCELLED", "WORKFLOW_CANCEL_CLAIM").await;

        let same_outcomes = terminalize(&pool, &abandon_owned(same, owned_claim()))
            .await
            .expect("abandon");
        let foreign_outcomes = terminalize(&pool, &abandon_owned(foreign, owned_claim()))
            .await
            .expect("abandon");

        assert!(
            matches!(
                same_outcomes.as_slice(),
                [TerminalizationOutcome::AlreadyApplied {
                    kind: TerminalizationKind::PauseAbandonWorkflow,
                    ..
                }]
            ),
            "{same_outcomes:?}"
        );
        let [TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::ForeignTerminalization(foreign_kind),
            ..
        }] = foreign_outcomes.as_slice()
        else {
            panic!("expected a foreign terminalization, got {foreign_outcomes:?}");
        };
        assert_eq!(
            foreign_kind.committed_kind,
            TerminalizationKind::WorkflowCancelClaim
        );
        cleanup_tasks(&pool, &[same, foreign]).await;
    }

    /// Python `TestAbandonOwnedNode::test_absence_is_an_outcome`.
    #[tokio::test]
    async fn absence_is_an_outcome() {
        let pool = pool().await;
        let outcomes = terminalize(&pool, &abandon_owned(Uuid::new_v4(), owned_claim()))
            .await
            .expect("abandon");
        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::TaskAbsent { .. }]
            ),
            "{outcomes:?}"
        );
    }
}

mod cancel_owned_node {
    use super::*;

    /// Python `TestCancelOwnedNode::test_the_terminal_record_carries_its_own_summary`.
    /// A worker-side node cancel has no error summary of its own, so the
    /// summary of an earlier attempt does not reach the history row.
    #[tokio::test]
    async fn the_terminal_record_carries_its_own_summary() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &claimed_workflow_task()).await;
        record_failed_first_attempt(&pool, task_id, "old reason").await;

        let outcomes = terminalize(&pool, &cancel_owned_node(task_id, owned_claim()))
            .await
            .expect("cancel node");

        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::Applied {
                    kind: TerminalizationKind::WorkflowCancelClaim,
                    ..
                }]
            ),
            "{outcomes:?}"
        );
        let image = history_row(&pool, task_id).await.expect("history row");
        assert_eq!(image.status, "CANCELLED");
        assert_eq!(image.error_code, None);
        assert_eq!(image.final_failed_reason, None);
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestCancelOwnedNode::test_a_stale_claim_is_lost`.
    #[tokio::test]
    async fn a_stale_claim_is_lost() {
        let pool = pool().await;
        let task_id = seed_task(&pool, &claimed_workflow_task()).await;

        let outcomes = terminalize(&pool, &cancel_owned_node(task_id, stale_claim()))
            .await
            .expect("cancel node");

        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::LostClaim { .. }]
            ),
            "{outcomes:?}"
        );
        assert_eq!(
            live_row(&pool, task_id).await.expect("live row")["status"],
            "CLAIMED"
        );
        cleanup_tasks(&pool, &[task_id]).await;
    }

    /// Python `TestCancelOwnedNode::test_replay_foreign_kind_wrong_source_and_absence_are_distinct`.
    #[tokio::test]
    async fn replay_foreign_kind_wrong_source_and_absence_are_distinct() {
        let pool = pool().await;
        let replay_id = seed_task(&pool, &claimed_workflow_task()).await;
        force_terminal(&pool, replay_id, "CANCELLED", "WORKFLOW_CANCEL_WORKFLOW").await;
        let foreign_id = seed_task(&pool, &claimed_workflow_task()).await;
        force_terminal(&pool, foreign_id, "CANCELLED", "PAUSE_ABANDON_CLAIM").await;
        let running_id = seed_task(
            &pool,
            &TaskSeed {
                origin: Origin::WorkflowNode,
                ..TaskSeed::running()
            },
        )
        .await;

        let replay = terminalize(&pool, &cancel_owned_node(replay_id, owned_claim()))
            .await
            .expect("replay");
        let foreign = terminalize(&pool, &cancel_owned_node(foreign_id, owned_claim()))
            .await
            .expect("foreign");
        let wrong_source = terminalize(&pool, &cancel_owned_node(running_id, owned_claim()))
            .await
            .expect("wrong source");
        let absent = terminalize(&pool, &cancel_owned_node(Uuid::new_v4(), owned_claim()))
            .await
            .expect("absent");

        assert!(
            matches!(
                replay.as_slice(),
                [TerminalizationOutcome::AlreadyApplied {
                    kind: TerminalizationKind::WorkflowCancelWorkflow,
                    ..
                }]
            ),
            "{replay:?}"
        );
        assert!(
            matches!(
                foreign.as_slice(),
                [TerminalizationOutcome::SourceStateConflict {
                    evidence: GuardEvidence::ForeignTerminalization(_),
                    ..
                }]
            ),
            "{foreign:?}"
        );
        let [TerminalizationOutcome::SourceStateConflict { observed, .. }] =
            wrong_source.as_slice()
        else {
            panic!("expected a source state conflict, got {wrong_source:?}");
        };
        assert_eq!(observed.status, Some(TaskStatus::Running));
        assert!(
            matches!(
                absent.as_slice(),
                [TerminalizationOutcome::TaskAbsent { .. }]
            ),
            "{absent:?}"
        );
        cleanup_tasks(&pool, &[replay_id, foreign_id, running_id]).await;
    }
}

mod id_keyed_workflow_batches {
    use super::*;

    /// The id-keyed batch under test.
    #[derive(Debug, Clone, Copy)]
    enum Batch {
        Abandon,
        Cancel,
    }

    impl Batch {
        fn command(self, fence: OwnedClaimBatch) -> TerminalizationCommand {
            match self {
                Self::Abandon => TerminalizationCommand::AbandonOwnedNodes { fence },
                Self::Cancel => TerminalizationCommand::CancelOwnedNodes { fence },
            }
        }

        /// The kind that is the same work, the foreign kind, and the kind
        /// that this batch writes.
        fn kinds(self) -> (&'static str, &'static str, TerminalizationKind) {
            match self {
                Self::Abandon => (
                    "PAUSE_ABANDON_WORKFLOW",
                    "WORKFLOW_CANCEL_WORKFLOW",
                    TerminalizationKind::PauseAbandonClaimBatch,
                ),
                Self::Cancel => (
                    "WORKFLOW_CANCEL_WORKFLOW",
                    "PAUSE_ABANDON_WORKFLOW",
                    TerminalizationKind::WorkflowCancelClaimBatch,
                ),
            }
        }

        /// The error code and reason that the batch writes on its history
        /// row: pause-abandon states its cause; a workflow cancel has none.
        fn summary(self) -> (Option<&'static str>, Option<&'static str>) {
            match self {
                Self::Abandon => (
                    Some("TASK_CANCELLED"),
                    Some("Workflow paused before task start"),
                ),
                Self::Cancel => (None, None),
            }
        }
    }

    /// Python `TestIdKeyedWorkflowBatches::test_one_ordered_outcome_per_input_across_every_outcome`.
    /// The Python case of a kindless history row is not ported: the core
    /// has no such row (differences ledger, row 4).
    async fn one_ordered_outcome_per_input(batch: Batch) {
        let pool = pool().await;
        let (same_kind, foreign_kind, applied_kind) = batch.kinds();
        let applied_id = seed_task(&pool, &claimed_workflow_task()).await;
        record_failed_first_attempt(&pool, applied_id, "old reason").await;
        let lost_id = seed_task(&pool, &claimed_workflow_task()).await;
        let conflict_id = seed_task(
            &pool,
            &TaskSeed {
                origin: Origin::WorkflowNode,
                ..TaskSeed::running()
            },
        )
        .await;
        let same_id = seed_task(&pool, &claimed_workflow_task()).await;
        force_terminal(&pool, same_id, "CANCELLED", same_kind).await;
        let foreign_id = seed_task(&pool, &claimed_workflow_task()).await;
        force_terminal(&pool, foreign_id, "CANCELLED", foreign_kind).await;
        let absent_id = Uuid::new_v4();
        let fence = OwnedClaimBatch::new(
            WORKER.to_owned(),
            vec![
                (applied_id, generation()),
                (lost_id, generation() - Duration::seconds(1)),
                (conflict_id, generation()),
                (same_id, generation()),
                (foreign_id, generation()),
                (absent_id, generation()),
            ],
        )
        .expect("distinct ids");

        let outcomes = terminalize(&pool, &batch.command(fence))
            .await
            .expect("batch");

        assert_eq!(
            outcomes
                .iter()
                .map(TerminalizationOutcome::task_id)
                .collect::<Vec<_>>(),
            vec![
                applied_id,
                lost_id,
                conflict_id,
                same_id,
                foreign_id,
                absent_id
            ]
        );
        assert_eq!(
            outcomes
                .iter()
                .map(TerminalizationOutcome::ordinality)
                .collect::<Vec<_>>(),
            (1..=6).map(Some).collect::<Vec<_>>()
        );
        let [applied, lost, conflict, same, foreign, absent] = outcomes.as_slice() else {
            panic!("expected six outcomes, got {outcomes:?}");
        };
        assert!(
            matches!(applied, TerminalizationOutcome::Applied { kind, .. } if *kind == applied_kind),
            "{applied:?}"
        );
        assert!(
            matches!(lost, TerminalizationOutcome::LostClaim { .. }),
            "{lost:?}"
        );
        assert!(
            matches!(conflict, TerminalizationOutcome::SourceStateConflict { .. }),
            "{conflict:?}"
        );
        assert!(
            matches!(same, TerminalizationOutcome::AlreadyApplied { .. }),
            "{same:?}"
        );
        assert!(
            matches!(
                foreign,
                TerminalizationOutcome::SourceStateConflict {
                    evidence: GuardEvidence::ForeignTerminalization(_),
                    ..
                }
            ),
            "{foreign:?}"
        );
        assert!(
            matches!(absent, TerminalizationOutcome::TaskAbsent { .. }),
            "{absent:?}"
        );

        let image = history_row(&pool, applied_id).await.expect("history row");
        let (error_code, failed_reason) = batch.summary();
        assert_eq!(image.error_code.as_deref(), error_code);
        assert_eq!(image.final_failed_reason.as_deref(), failed_reason);

        cleanup_tasks(
            &pool,
            &[applied_id, lost_id, conflict_id, same_id, foreign_id],
        )
        .await;
    }

    /// `test_one_ordered_outcome_per_input_across_every_outcome`, abandon batch.
    #[tokio::test]
    async fn one_ordered_outcome_per_input_across_every_outcome_abandon() {
        one_ordered_outcome_per_input(Batch::Abandon).await;
    }

    /// `test_one_ordered_outcome_per_input_across_every_outcome`, cancel batch.
    #[tokio::test]
    async fn one_ordered_outcome_per_input_across_every_outcome_cancel() {
        one_ordered_outcome_per_input(Batch::Cancel).await;
    }

    /// Python `TestIdKeyedWorkflowBatches::test_a_null_generation_keeps_the_per_task_worker_fence`,
    /// with the core's rule: a NULL element of the generation array is
    /// refused before any lock or write (differences ledger, row 3).
    #[tokio::test]
    async fn a_null_generation_is_refused() {
        let pool = pool().await;
        for function in ["horsies_abandon_owned_nodes", "horsies_cancel_owned_nodes"] {
            let task_id = seed_task(&pool, &claimed_workflow_task()).await;
            let before = live_row(&pool, task_id).await;

            let refused = sql::query(&format!(
                "SELECT * FROM {function}(ARRAY[$1]::uuid[], ARRAY[NULL]::timestamptz[], $2)"
            ))
            .bind(task_id)
            .bind(WORKER)
            .fetch_all(&pool)
            .await
            .map(|rows| rows.len());

            assert_refused(refused, "22023", Constraint::Unnamed);
            assert_eq!(live_row(&pool, task_id).await, before, "{function}");
            cleanup_tasks(&pool, &[task_id]).await;
        }
    }

    /// Python `TestIdKeyedWorkflowBatches::test_empty_input_is_valid`.
    #[tokio::test]
    async fn empty_input_is_valid() {
        let pool = pool().await;
        for batch in [Batch::Abandon, Batch::Cancel] {
            let fence = OwnedClaimBatch::new(WORKER.to_owned(), Vec::new()).expect("empty batch");
            let outcomes = terminalize(&pool, &batch.command(fence))
                .await
                .expect("batch");
            assert!(outcomes.is_empty(), "{batch:?}: {outcomes:?}");
        }
    }
}

mod workflow_scoped_batches {
    use super::*;

    /// Python `TestWorkflowScopedBatches::test_pause_applies_only_under_both_live_guards`:
    /// a CLAIMED row with an ENQUEUED or RUNNING node under a PAUSED
    /// workflow. The history row keeps the claim time as provenance; the
    /// live claim ends with the live row.
    #[tokio::test]
    async fn pause_applies_only_under_both_live_guards() {
        let pool = pool().await;
        let eligible = seed_task(&pool, &claimed_workflow_task()).await;
        let eligible_workflow = link_workflow_task(&pool, eligible, "RUNNING", "PAUSED").await;
        let eligible_enqueued = seed_task(&pool, &claimed_workflow_task()).await;
        let eligible_enqueued_workflow =
            link_workflow_task(&pool, eligible_enqueued, "ENQUEUED", "PAUSED").await;
        let wrong_workflow = seed_task(&pool, &claimed_workflow_task()).await;
        let wrong_workflow_id =
            link_workflow_task(&pool, wrong_workflow, "RUNNING", "RUNNING").await;
        let wrong_link = seed_task(&pool, &claimed_workflow_task()).await;
        let wrong_link_workflow = link_workflow_task(&pool, wrong_link, "READY", "PAUSED").await;
        let wrong_task_source = seed_task(
            &pool,
            &TaskSeed {
                origin: Origin::WorkflowNode,
                ..TaskSeed::pending()
            },
        )
        .await;
        let wrong_task_workflow =
            link_workflow_task(&pool, wrong_task_source, "ENQUEUED", "PAUSED").await;
        let workflows = vec![
            eligible_workflow,
            eligible_enqueued_workflow,
            wrong_workflow_id,
            wrong_link_workflow,
            wrong_task_workflow,
        ];

        let outcomes = terminalize(
            &pool,
            &TerminalizationCommand::AbandonNodesOfPausedWorkflows {
                workflow_ids: workflows.clone(),
            },
        )
        .await
        .expect("pause sweep");

        let mut swept: Vec<Uuid> = outcomes
            .iter()
            .map(TerminalizationOutcome::task_id)
            .collect();
        swept.sort_unstable();
        let mut expected = vec![eligible, eligible_enqueued];
        expected.sort_unstable();
        assert_eq!(swept, expected);
        let eligible_outcome = outcomes
            .iter()
            .find(|outcome| outcome.task_id() == eligible)
            .expect("outcome of the eligible task");
        assert!(
            matches!(
                eligible_outcome,
                TerminalizationOutcome::Applied {
                    kind: TerminalizationKind::PauseAbandonWorkflow,
                    ordinality: None,
                    ..
                }
            ),
            "{eligible_outcome:?}"
        );
        let image = history_row(&pool, eligible).await.expect("history row");
        assert_eq!(image.status, "CANCELLED");
        assert_eq!(image.terminalization_kind, "PAUSE_ABANDON_WORKFLOW");
        assert_eq!(image.error_code.as_deref(), Some("TASK_CANCELLED"));
        assert_eq!(
            image.final_failed_reason.as_deref(),
            Some("Workflow paused before task start")
        );
        assert_eq!(image.claimed_at, Some(generation()));
        assert_eq!(live_row(&pool, eligible).await, None);
        for (task_id, status) in [
            (wrong_workflow, "CLAIMED"),
            (wrong_link, "CLAIMED"),
            (wrong_task_source, "PENDING"),
        ] {
            assert_eq!(
                live_row(&pool, task_id).await.expect("live row")["status"],
                status
            );
        }

        cleanup_tasks(
            &pool,
            &[
                eligible,
                eligible_enqueued,
                wrong_workflow,
                wrong_link,
                wrong_task_source,
            ],
        )
        .await;
        cleanup_workflows(&pool, &workflows).await;
    }

    /// Python `TestWorkflowScopedBatches::test_cancel_accepts_all_three_legacy_task_source_states`:
    /// PENDING, CLAIMED and RUNNING rows under a CANCELLED workflow. The
    /// sweep carries no error summary of its own.
    #[tokio::test]
    async fn cancel_accepts_all_three_legacy_task_source_states() {
        let pool = pool().await;
        let mut task_ids = Vec::new();
        let mut workflow_ids = Vec::new();
        for base in [
            TaskSeed::pending(),
            TaskSeed::claimed(),
            TaskSeed::running(),
        ] {
            let task_id = seed_task(
                &pool,
                &TaskSeed {
                    origin: Origin::WorkflowNode,
                    ..base
                },
            )
            .await;
            workflow_ids.push(link_workflow_task(&pool, task_id, "ENQUEUED", "CANCELLED").await);
            task_ids.push(task_id);
        }
        record_failed_first_attempt(&pool, task_ids[0], "old reason").await;

        let outcomes = terminalize(
            &pool,
            &TerminalizationCommand::CancelNodesOfCancelledWorkflow {
                workflow_ids: workflow_ids.clone(),
            },
        )
        .await
        .expect("cancel sweep");

        let mut swept: Vec<Uuid> = outcomes
            .iter()
            .map(TerminalizationOutcome::task_id)
            .collect();
        swept.sort_unstable();
        let mut expected = task_ids.clone();
        expected.sort_unstable();
        assert_eq!(swept, expected);
        for outcome in &outcomes {
            assert!(
                matches!(
                    outcome,
                    TerminalizationOutcome::Applied {
                        kind: TerminalizationKind::WorkflowCancelWorkflow,
                        ordinality: None,
                        ..
                    }
                ),
                "{outcome:?}"
            );
        }
        let image = history_row(&pool, task_ids[0]).await.expect("history row");
        assert_eq!(image.status, "CANCELLED");
        assert_eq!(image.error_code, None);
        assert_eq!(image.final_failed_reason, None);

        cleanup_tasks(&pool, &task_ids).await;
        cleanup_workflows(&pool, &workflow_ids).await;
    }

    /// Python `TestWorkflowScopedBatches::test_cancel_refuses_a_live_workflow_or_non_enqueued_link`.
    #[tokio::test]
    async fn cancel_refuses_a_live_workflow_or_non_enqueued_link() {
        let pool = pool().await;
        let live_workflow_task = seed_task(&pool, &claimed_workflow_task()).await;
        let live_workflow_id =
            link_workflow_task(&pool, live_workflow_task, "ENQUEUED", "RUNNING").await;
        let ready_link_task = seed_task(&pool, &claimed_workflow_task()).await;
        let ready_link_workflow =
            link_workflow_task(&pool, ready_link_task, "READY", "CANCELLED").await;

        let outcomes = terminalize(
            &pool,
            &TerminalizationCommand::CancelNodesOfCancelledWorkflow {
                workflow_ids: vec![live_workflow_id, ready_link_workflow],
            },
        )
        .await
        .expect("cancel sweep");

        assert!(outcomes.is_empty(), "{outcomes:?}");
        for task_id in [live_workflow_task, ready_link_task] {
            assert_eq!(
                live_row(&pool, task_id).await.expect("live row")["status"],
                "CLAIMED"
            );
        }
        cleanup_tasks(&pool, &[live_workflow_task, ready_link_task]).await;
        cleanup_workflows(&pool, &[live_workflow_id, ready_link_workflow]).await;
    }

    /// No command names the workflow status that its guard needs; the variant
    /// means it. The pause sweep acts only under a PAUSED workflow and the
    /// cancel sweep only under a CANCELLED one, so neither acts for the other.
    #[tokio::test]
    async fn each_workflow_sweep_acts_only_under_the_status_of_its_variant() {
        let pool = pool().await;
        let paused_task = seed_task(&pool, &claimed_workflow_task()).await;
        let paused = link_workflow_task(&pool, paused_task, "ENQUEUED", "PAUSED").await;
        let cancelled_task = seed_task(&pool, &claimed_workflow_task()).await;
        let cancelled = link_workflow_task(&pool, cancelled_task, "ENQUEUED", "CANCELLED").await;

        let pause_of_cancelled = terminalize(&pool, &pause_sweep(cancelled))
            .await
            .expect("pause sweep of a cancelled workflow");
        let cancel_of_paused = terminalize(&pool, &cancel_sweep(paused))
            .await
            .expect("cancel sweep of a paused workflow");

        assert!(pause_of_cancelled.is_empty(), "{pause_of_cancelled:?}");
        assert!(cancel_of_paused.is_empty(), "{cancel_of_paused:?}");
        for task_id in [paused_task, cancelled_task] {
            assert_eq!(
                live_row(&pool, task_id).await.expect("live row")["status"],
                "CLAIMED"
            );
        }
        let pause_of_paused = terminalize(&pool, &pause_sweep(paused))
            .await
            .expect("pause sweep of a paused workflow");
        let cancel_of_cancelled = terminalize(&pool, &cancel_sweep(cancelled))
            .await
            .expect("cancel sweep of a cancelled workflow");
        let both = [paused_task, cancelled_task];
        assert_eq!(
            outcome_ids_among(&pause_of_paused, &both),
            vec![paused_task]
        );
        assert_eq!(
            outcome_ids_among(&cancel_of_cancelled, &both),
            vec![cancelled_task]
        );
        cleanup_tasks(&pool, &both).await;
        cleanup_workflows(&pool, &[paused, cancelled]).await;
    }

    /// The pause sweep of one workflow.
    fn pause_sweep(workflow_id: Uuid) -> TerminalizationCommand {
        TerminalizationCommand::AbandonNodesOfPausedWorkflows {
            workflow_ids: vec![workflow_id],
        }
    }

    /// The cancel sweep of one workflow.
    fn cancel_sweep(workflow_id: Uuid) -> TerminalizationCommand {
        TerminalizationCommand::CancelNodesOfCancelledWorkflow {
            workflow_ids: vec![workflow_id],
        }
    }
}

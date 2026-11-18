//! Port of Python `tests/integration/test_task_history_terminalization_prototypes.py`.
//!
//! The Python file runs the move program against a prototype schema; these
//! ports run its rows against the baseline functions of the core. Rows not
//! ported here:
//! - `test_owned_orphan_classifies_stale_generation_before_link_guard` is
//!   `terminalization_functions::cancel_owned_orphan::a_stale_generation_is_a_lost_claim_even_with_a_live_link`;
//!   `test_live_and_history_duplicate_is_rejected` is
//!   `task_history_terminalization_move::duplicate_identity_fails_closed`.
//! - `test_workflow_cancel_preserves_coherent_prior_attempt_summary` pins a
//!   prototype rule (the cancel record keeps the prior attempt's summary)
//!   that production replaced: Python's
//!   `test_terminalization_functions.py::TestCancelOwnedNode::test_the_terminal_record_carries_its_own_summary`
//!   pins the opposite, and the core follows it
//!   (`terminalization_functions::cancel_owned_node::the_terminal_record_carries_its_own_summary`).
//!   The summary half of `test_pairwise_cancel_batch_preserves_summary_and_skips_node`
//!   is left out for the same reason.
//! - `test_archive_maintenance_also_refuses_history_replay` and
//!   `test_maintenance_start_waits_for_terminalization_transaction` test the
//!   archive maintenance gate, which the baseline leaves out (schema choice
//!   5, `removed-for-the-clean-slate.md`).

use chrono::Duration;
use serial_test::serial;
use uuid::Uuid;

use super::support::*;
use crate::broker::attempt_writes::FinishWorkflowSuccess;
use crate::broker::core_rows::StatementLog;
use crate::broker::pools::core_pool_for_test_url;
use crate::broker::postgres::PostgresBroker;
use crate::broker::terminalization::on_test_pool::terminalize;
use crate::broker::terminalization::on_test_pool::{
    terminalize_in_transaction, InTransactionError,
};
use crate::broker::workflow_stop_before_start::{stop_before_start, OrphanSelfHeal};
use crate::core::history::archive::attempts::{encode_attempt_snapshot, AttemptRecord};
use crate::core::history::errors::HISTORY_LEAF_MISSING_SQLSTATE;
use crate::core::lifecycle::{
    BatchSize, OwnedClaim, TerminalizationCommand, TerminalizationKind, TerminalizationOutcome,
    WorkflowSuccessOutcome,
};
use crate::test_support::sql;

const RESULT: &str = r#"{"ok": "done"}"#;

fn fused(task_id: Uuid) -> TerminalizationCommand {
    TerminalizationCommand::CompleteTaskFused {
        task_id,
        fence: owned_claim(),
        result_json: RESULT.to_owned(),
    }
}

fn expire_pending() -> TerminalizationCommand {
    TerminalizationCommand::ExpirePendingTasks {
        batch_size: BatchSize::new(10).expect("positive batch size"),
        result_json: r#"{"err": "expired"}"#.to_owned(),
        error_code: "TASK_EXPIRED".to_owned(),
    }
}

fn running_workflow_task() -> TaskSeed {
    TaskSeed {
        origin: Origin::WorkflowNode,
        ..TaskSeed::running()
    }
}

/// The database error of a command run in a test transaction
/// (`terminalize_in_transaction`).
fn database_error(result: Result<Vec<TerminalizationOutcome>, InTransactionError>) -> sql::Error {
    match result {
        Err(InTransactionError::Database(error)) => error,
        other => panic!("expected a database error, got {other:?}"),
    }
}

/// Python `test_sql_attempt_encoder_matches_archive_codec_bytes`: the SQL
/// attempt encoder of the move gives the bytes of the archive codec, also
/// for quotes, a backslash and non-ASCII text.
#[tokio::test]
#[serial]
async fn sql_attempt_encoder_matches_archive_codec_bytes() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;
    let special = "retry, quoted \"value\" with \\ and \u{e9}";
    let started_at = generation();
    let finished_at = generation() + Duration::seconds(1);
    sql::query(
        "INSERT INTO horsies_task_attempts (
            task_id, attempt, outcome, will_retry, started_at, finished_at,
            error_code, error_message, failed_reason, worker_id,
            worker_hostname, worker_pid, worker_process_name
        ) VALUES ($1, 1, 'FAILED', FALSE, $2, $3, 'TASK_EXCEPTION', $4,
                  'final attempt failed', $5, $4, 4242, $4)",
    )
    .bind(task_id)
    .bind(started_at)
    .bind(finished_at)
    .bind(special)
    .bind(WORKER)
    .execute(&pool)
    .await
    .expect("seed the attempt");

    let encoded: Vec<u8> = sql::query_scalar("SELECT horsies_encode_task_attempts($1)")
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .expect("encode in SQL");

    let expected = encode_attempt_snapshot(&[AttemptRecord::new(
        1,
        "FAILED",
        false,
        started_at,
        finished_at,
        Some("TASK_EXCEPTION".to_owned()),
        Some(special.to_owned()),
        Some("final attempt failed".to_owned()),
        Some(WORKER.to_owned()),
        Some(special.to_owned()),
        Some(4242),
        Some(special.to_owned()),
    )
    .expect("attempt record")])
    .expect("encode in Rust");
    assert_eq!(encoded, expected.payload);
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_terminalization_copies_request_and_last_worker_attribution`:
/// the history row carries the request facts and the last worker's
/// attribution.
#[tokio::test]
#[serial]
async fn terminalization_copies_request_and_last_worker_attribution() {
    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            max_retries: 3,
            ..TaskSeed::running()
        },
    )
    .await;
    set_deadline(&pool, task_id, Deadline::AheadSeconds(3_600.0)).await;
    let good_until: chrono::DateTime<chrono::Utc> =
        sql::query_scalar("SELECT good_until FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("read good_until");

    let outcomes = terminalize(&pool, &fused(task_id))
        .await
        .expect("completion");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{outcomes:?}"
    );

    #[allow(clippy::type_complexity)]
    let row: (
        Option<i16>,
        Option<Vec<u8>>,
        Option<chrono::DateTime<chrono::Utc>>,
        i32,
        Option<String>,
        Option<String>,
        Option<i32>,
        Option<String>,
    ) = sql::query_as(
        "SELECT command_fingerprint_version, command_fingerprint, good_until, max_retries,
                last_claimed_worker_id::text, last_worker_hostname::text, last_worker_pid,
                last_worker_process_name::text
         FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .expect("read the history row");
    assert_eq!(
        row,
        (
            Some(1),
            Some(vec![7_u8; 32]),
            Some(good_until),
            3,
            Some(WORKER.to_owned()),
            Some("lifecycle-host".to_owned()),
            Some(4242),
            Some("lifecycle-process".to_owned()),
        )
    );
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_pending_expiry_missing_workflow_link_aborts_whole_batch`: a
/// due workflow task with no node refuses the whole batch, so a due plain
/// task of the same batch stays live too.
#[tokio::test]
#[serial]
async fn pending_expiry_missing_workflow_link_aborts_whole_batch() {
    let pool = pool().await;
    drain_pending_expiry(&pool).await;
    let unlinked = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            ..TaskSeed::pending()
        },
    )
    .await;
    set_deadline(&pool, unlinked, Deadline::PassedSecondsAgo(1.0)).await;
    let plain = seed_task(&pool, &TaskSeed::pending()).await;
    set_deadline(&pool, plain, Deadline::PassedSecondsAgo(2.0)).await;

    let refused = server_refusal(terminalize(&pool, &expire_pending()).await);

    assert_server_refusal(&refused, "23503", "lacks exactly one node row");
    for task_id in [unlinked, plain] {
        assert_eq!(status_of(&pool, task_id).await, "PENDING");
        assert_eq!(history_row(&pool, task_id).await, None);
    }
    cleanup_tasks(&pool, &[unlinked, plain]).await;
}

/// Python `test_pending_expiry_emits_one_transactional_raw_id_per_task`: a
/// batch expiry sends one `task_done` per task, with the raw task id.
#[tokio::test]
#[serial]
async fn pending_expiry_emits_one_transactional_raw_id_per_task() {
    let pool = pool().await;
    drain_pending_expiry(&pool).await;
    let mut wakes = Wakes::listen(&pool, "task_done").await;
    let mut task_ids = Vec::new();
    for seconds_ago in [2.0, 1.0] {
        let task_id = seed_task(&pool, &TaskSeed::pending()).await;
        set_deadline(&pool, task_id, Deadline::PassedSecondsAgo(seconds_ago)).await;
        task_ids.push(task_id);
    }

    let outcomes = terminalize(&pool, &expire_pending())
        .await
        .expect("expire pending");

    assert_eq!(outcomes.len(), 2, "{outcomes:?}");
    let payloads = wakes.payloads(&pool).await;
    for task_id in &task_ids {
        let raw_id = task_id.to_string();
        assert_eq!(
            payloads
                .iter()
                .filter(|payload| **payload == raw_id)
                .count(),
            1,
            "{payloads:?}"
        );
    }
    cleanup_tasks(&pool, &task_ids).await;
}

/// Python `test_orphan_sweep_is_bounded_and_replays_through_single_variant`:
/// a task that the sweep cancelled replays through the single orphan cancel
/// as `AlreadyApplied` with the sweep's kind.
#[tokio::test]
#[serial]
async fn orphan_sweep_is_bounded_and_replays_through_single_variant() {
    let pool = pool().await;
    drain_orphan_sweep(&pool).await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            ..TaskSeed::claimed()
        },
    )
    .await;

    let swept = sweep_orphans_one_cycle(&pool).await;
    assert!(
        swept.iter().any(|outcome| matches!(
            outcome,
            TerminalizationOutcome::Applied {
                task_id: swept_id,
                kind: TerminalizationKind::CancelOrphanSweep,
                ..
            } if *swept_id == task_id
        )),
        "{swept:?}"
    );

    let replay = terminalize(
        &pool,
        &TerminalizationCommand::CancelOwnedOrphan {
            task_id,
            fence: owned_claim(),
        },
    )
    .await
    .expect("single orphan cancel");

    assert!(
        matches!(
            replay.as_slice(),
            [TerminalizationOutcome::AlreadyApplied {
                kind: TerminalizationKind::CancelOrphanSweep,
                ..
            }]
        ),
        "{replay:?}"
    );
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_coupled_node_failure_rolls_back_history_and_live_delete`, the
/// node half: the pre-start abandon of a task whose node is already
/// COMPLETED leaves the node COMPLETED with its task link. The task half
/// differs between the prototype (the move rolls back) and Python's
/// production code (the abandon applies), so it is not asserted.
#[tokio::test]
#[serial]
async fn coupled_node_failure_rolls_back_history_and_live_delete() {
    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            ..TaskSeed::claimed()
        },
    )
    .await;
    let workflow_id = link_workflow_task(&pool, task_id, "COMPLETED", "PAUSED").await;

    let _stopped = stop_before_start(
        &core_pool_for_test_url(pool.url()),
        StatementLog::Off,
        task_id,
        &OwnedClaim {
            worker_id: WORKER.to_owned(),
            claimed_at: generation(),
        },
        OrphanSelfHeal::Off,
        crate::broker::child_writes::WriteDeadline::start_attempt(),
    )
    .await
    .expect("stop before start");

    assert_eq!(
        node_state(&pool, workflow_id).await,
        ("COMPLETED".to_owned(), Some(task_id))
    );
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// The node half of `coupled_node_failure_rolls_back_history_and_live_delete`
/// on the post-claim path: the post-claim abandon of a task of a PAUSED
/// workflow whose node is already COMPLETED leaves the node COMPLETED with
/// its task link (#785; Python `RESET_PAUSED_WORKFLOW_TASKS_SQL`,
/// `python/horsies/core/worker/sql.py:291-296`).
#[tokio::test]
#[serial]
async fn post_claim_abandon_keeps_a_finished_node() {
    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            ..TaskSeed::claimed()
        },
    )
    .await;
    let workflow_id = link_workflow_task(&pool, task_id, "COMPLETED", "PAUSED").await;

    PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust)
        .filter_non_runnable_workflow_tasks(&[(task_id, generation())], WORKER)
        .await
        .expect("post-claim workflow check");

    assert_eq!(
        node_state(&pool, workflow_id).await,
        ("COMPLETED".to_owned(), Some(task_id))
    );
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// Python `test_pairwise_cancel_batch_preserves_summary_and_skips_node`, the
/// node half: the post-claim cancel of a task of a CANCELLED workflow skips
/// its node and keeps the node's task link.
#[tokio::test]
#[serial]
async fn pairwise_cancel_batch_skips_the_node() {
    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            ..TaskSeed::claimed()
        },
    )
    .await;
    let workflow_id = link_workflow_task(&pool, task_id, "ENQUEUED", "CANCELLED").await;

    PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust)
        .filter_non_runnable_workflow_tasks(&[(task_id, generation())], WORKER)
        .await
        .expect("post-claim workflow check");

    let image = history_row(&pool, task_id).await.expect("history row");
    assert_eq!(image.status, "CANCELLED");
    assert_eq!(image.terminalization_kind, "WORKFLOW_CANCEL_CLAIM_BATCH");
    assert_eq!(
        node_state(&pool, workflow_id).await,
        ("SKIPPED".to_owned(), Some(task_id))
    );
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// A step of the move that a fault can stop.
#[derive(Debug, Clone, Copy)]
enum Boundary {
    AttemptCapture,
    HistoryInsert,
    PendingInsert,
    AttemptDelete,
    LiveDelete,
}

impl Boundary {
    /// The statement that makes this step raise.
    fn fault(self) -> &'static str {
        match self {
            Self::AttemptCapture => {
                "CREATE OR REPLACE FUNCTION horsies_encode_task_attempts(p_task_id uuid)
                 RETURNS bytea LANGUAGE plpgsql AS $f$
                 BEGIN RAISE EXCEPTION 'injected terminalization fault'; END $f$"
            }
            Self::HistoryInsert => {
                "CREATE TRIGGER lifecycle_test_fault BEFORE INSERT ON horsies_task_history
                 FOR EACH STATEMENT EXECUTE FUNCTION lifecycle_test_fault()"
            }
            Self::PendingInsert => {
                "CREATE TRIGGER lifecycle_test_fault BEFORE INSERT
                 ON horsies_workflow_phase2_pending
                 FOR EACH STATEMENT EXECUTE FUNCTION lifecycle_test_fault()"
            }
            Self::AttemptDelete => {
                "CREATE TRIGGER lifecycle_test_fault BEFORE DELETE ON horsies_task_attempts
                 FOR EACH STATEMENT EXECUTE FUNCTION lifecycle_test_fault()"
            }
            Self::LiveDelete => {
                "CREATE TRIGGER lifecycle_test_fault BEFORE DELETE ON horsies_tasks
                 FOR EACH STATEMENT EXECUTE FUNCTION lifecycle_test_fault()"
            }
        }
    }

    /// A deferring move writes the phase-2 record; the others do not.
    fn deferred(self) -> bool {
        matches!(self, Self::PendingInsert)
    }
}

/// Python `test_terminalization_boundary_failure_rolls_back_every_relation`:
/// a fault at a step of the move fails the call, and the move leaves no
/// history row, no phase-2 record, and the live row and its attempts in
/// place. Each fault is installed inside the test's transaction and leaves
/// with its rollback. The task-notification and coupled-node steps are not
/// faulted: the move calls `pg_notify` directly, and the core move writes no
/// node (#785 owns the coupled node write).
#[tokio::test]
#[serial]
async fn terminalization_boundary_failure_rolls_back_every_relation() {
    let pool = pool().await;
    for boundary in [
        Boundary::AttemptCapture,
        Boundary::HistoryInsert,
        Boundary::PendingInsert,
        Boundary::AttemptDelete,
        Boundary::LiveDelete,
    ] {
        let (task_id, workflow_id, command) = match boundary.deferred() {
            true => {
                let task_id = seed_task(
                    &pool,
                    &TaskSeed {
                        origin: Origin::WorkflowNode,
                        ..TaskSeed::claimed()
                    },
                )
                .await;
                set_deadline(&pool, task_id, Deadline::PassedSecondsAgo(60.0)).await;
                let workflow_id = link_workflow_task(&pool, task_id, "ENQUEUED", "RUNNING").await;
                let command = TerminalizationCommand::ExpireOwnedClaim {
                    task_id,
                    fence: crate::core::lifecycle::WorkerOwned {
                        worker_id: WORKER.to_owned(),
                    },
                    result_json: r#"{"err": "expired"}"#.to_owned(),
                    error_code: "TASK_EXPIRED".to_owned(),
                };
                (task_id, Some(workflow_id), command)
            }
            false => {
                let task_id = seed_task(&pool, &TaskSeed::running()).await;
                (task_id, None, fused(task_id))
            }
        };
        record_failed_first_attempt(&pool, task_id, "attempt one crashed").await;

        let mut transaction = pool.begin().await.expect("begin");
        sql::query(
            "CREATE FUNCTION lifecycle_test_fault() RETURNS trigger LANGUAGE plpgsql AS $f$
             BEGIN RAISE EXCEPTION 'injected terminalization fault'; END $f$",
        )
        .execute(&mut *transaction)
        .await
        .expect("create the fault function");
        sql::query(boundary.fault())
            .execute(&mut *transaction)
            .await
            .expect("install the fault");
        let mut savepoint = transaction.begin().await.expect("savepoint");
        let refused = terminalize_in_transaction(&mut savepoint, &command).await;
        savepoint
            .rollback()
            .await
            .expect("roll back to the savepoint");
        let live: i64 = sql::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_one(&mut *transaction)
            .await
            .expect("count live rows");
        let attempts: i64 =
            sql::query_scalar("SELECT count(*) FROM horsies_task_attempts WHERE task_id = $1")
                .bind(task_id)
                .fetch_one(&mut *transaction)
                .await
                .expect("count attempts");
        transaction.rollback().await.expect("remove the fault");

        assert_refused_with_message(
            Err::<(), _>(database_error(refused)),
            "P0001",
            "injected terminalization fault",
        );
        assert_eq!((live, attempts), (1, 1), "{boundary:?}");
        assert_eq!(history_row(&pool, task_id).await, None, "{boundary:?}");
        assert!(!phase2_pending(&pool, task_id).await, "{boundary:?}");
        cleanup_tasks(&pool, &[task_id]).await;
        if let Some(workflow_id) = workflow_id {
            cleanup_workflows(&pool, &[workflow_id]).await;
        }
    }
}

/// Python `test_capacity_notification_failure_rolls_back_fused_completion`.
/// The baseline function builds the wake from the locked row: channel
/// `task_queue_<queue>`, payload `capacity`. The queue name CHECK bounds the
/// name to 52 bytes, so the channel fits PostgreSQL's 63-byte limit and the
/// wake cannot be refused: the CHECK refuses a 53-byte queue name, and the
/// completion of a seeded row applies with its wake.
#[tokio::test]
#[serial]
async fn capacity_notification_failure_rolls_back_fused_completion() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;
    let before = live_row(&pool, task_id).await;

    let refused = sql::query("UPDATE horsies_tasks SET queue_name = $2 WHERE id = $1")
        .bind(task_id)
        .bind("q".repeat(53))
        .execute(&pool)
        .await
        .map(|done| done.rows_affected());
    assert_refused(
        refused,
        "23514",
        Constraint::Named("horsies_tasks_queue_name_length"),
    );
    assert_eq!(live_row(&pool, task_id).await, before);

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CompleteTaskFused {
            task_id,
            fence: owned_claim(),
            result_json: RESULT.to_owned(),
        },
    )
    .await
    .expect("fused completion");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{outcomes:?}"
    );
    assert_eq!(live_row(&pool, task_id).await, None);
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_completion_equivalence_replays_from_history`: either
/// completion writer, replayed after the other, is `AlreadyApplied`.
#[tokio::test]
#[serial]
async fn completion_equivalence_replays_from_history() {
    let pool = pool().await;
    let success = |task_id: Uuid| FinishWorkflowSuccess {
        task_id,
        fence: owned_claim(),
        result_json: RESULT.to_owned(),
    };

    // Fused first, then the workflow-success writer (kind COMPLETE_LOCKED).
    let plain = seed_task(&pool, &TaskSeed::running()).await;
    let first = terminalize(&pool, &fused(plain)).await.expect("fused");
    assert!(
        matches!(first.as_slice(), [TerminalizationOutcome::Applied { .. }]),
        "{first:?}"
    );
    let replay = write_workflow_success(&pool, &success(plain))
        .await
        .expect("workflow success replay");
    assert!(
        matches!(
            replay,
            WorkflowSuccessOutcome::Refused(
                crate::core::lifecycle::AttemptWriteRefusal::AlreadyApplied {
                    kind: TerminalizationKind::CompleteFused,
                    ..
                }
            )
        ),
        "{replay:?}"
    );

    // The workflow-success writer first, then fused.
    let workflow_task = seed_task(&pool, &running_workflow_task()).await;
    let workflow_id = link_workflow_task(&pool, workflow_task, "RUNNING", "RUNNING").await;
    let applied = write_workflow_success(&pool, &success(workflow_task))
        .await
        .expect("workflow success");
    assert!(
        matches!(applied, WorkflowSuccessOutcome::SuccessApplied { .. }),
        "{applied:?}"
    );
    let replay = terminalize(&pool, &fused(workflow_task))
        .await
        .expect("fused replay");
    assert!(
        matches!(
            replay.as_slice(),
            [TerminalizationOutcome::AlreadyApplied {
                kind: TerminalizationKind::CompleteLocked,
                ..
            }]
        ),
        "{replay:?}"
    );
    cleanup_tasks(&pool, &[plain, workflow_task]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// Python `test_missing_workflow_link_rolls_back_the_move`: a deferring move
/// of a workflow task with no node is refused, and the task stays live.
#[tokio::test]
#[serial]
async fn missing_workflow_link_rolls_back_the_move() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &running_workflow_task()).await;

    let refused =
        move_to_history(&pool, task_id, "COMPLETED", "COMPLETE_LOCKED", Some(RESULT)).await;

    assert_refused(refused, "P0002", Constraint::Unnamed);
    assert_eq!(status_of(&pool, task_id).await, "RUNNING");
    assert!(!phase2_pending(&pool, task_id).await);
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_missing_history_leaf_rolls_back_fused_attempt_and_live_delete`:
/// with no history leaf for the row's retention class, the fused completion
/// fails with the named refusal (55H01, retryable), which names the class
/// and the missing UTC day, and its attempt and its live delete roll back
/// with it.
#[tokio::test]
#[serial]
async fn missing_history_leaf_rolls_back_fused_attempt_and_live_delete() {
    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            retention_class_key: "standard_1777d",
            ..TaskSeed::running()
        },
    )
    .await;
    let before = live_row(&pool, task_id).await;

    let failed = terminalize(&pool, &fused(task_id)).await;
    assert!(
        matches!(&failed, Err(error) if error.is_retryable()),
        "{failed:?}"
    );
    let refused = server_refusal(failed);

    assert_server_refusal(
        &refused,
        HISTORY_LEAF_MISSING_SQLSTATE,
        "no task-history leaf of class standard_1777d covers [",
    );
    assert_eq!(
        refused.detail(),
        Some(
            "the class partition horsies_task_history_standard_1777d does not exist: \
             the class is not registered"
        ),
        "{refused}"
    );
    assert_eq!(live_row(&pool, task_id).await, before);
    assert!(live_attempts(&pool, task_id).await.is_empty());
    assert_eq!(history_row(&pool, task_id).await, None);
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_fused_completion_rejects_workflow_task_without_losing_phase2`:
/// the fused completion refuses a workflow task, whose success owes phase 2,
/// and writes nothing.
#[tokio::test]
#[serial]
async fn fused_completion_rejects_workflow_task_without_losing_phase2() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &running_workflow_task()).await;
    let workflow_id = link_workflow_task(&pool, task_id, "RUNNING", "RUNNING").await;
    let before = live_row(&pool, task_id).await;

    let refused = server_refusal(terminalize(&pool, &fused(task_id)).await);

    assert_server_refusal(
        &refused,
        "22023",
        &format!("operation COMPLETE_FUSED cannot terminalize workflow task {task_id}"),
    );
    assert_eq!(live_row(&pool, task_id).await, before);
    assert!(!phase2_pending(&pool, task_id).await);
    assert_eq!(
        node_state(&pool, workflow_id).await,
        ("RUNNING".to_owned(), Some(task_id))
    );
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// Python `test_task_done_notification_is_transactional_raw_task_id`:
/// `task_done` carries the raw task id and is sent only when the move
/// commits.
#[tokio::test]
#[serial]
async fn task_done_notification_is_transactional_raw_task_id() {
    let pool = pool().await;
    let mut wakes = Wakes::listen(&pool, "task_done").await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;
    let raw_id = task_id.to_string();

    let mut transaction = pool.begin().await.expect("begin");
    let rolled_back = terminalize_in_transaction(&mut transaction, &fused(task_id))
        .await
        .expect("completion in a transaction");
    assert!(
        matches!(
            rolled_back.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{rolled_back:?}"
    );
    transaction.rollback().await.expect("roll back the move");
    assert!(
        !wakes.payloads(&pool).await.contains(&raw_id),
        "a rolled-back move sent task_done"
    );

    let committed = terminalize(&pool, &fused(task_id))
        .await
        .expect("completion");
    assert!(
        matches!(
            committed.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{committed:?}"
    );
    let payloads = wakes.payloads(&pool).await;
    assert_eq!(
        payloads
            .iter()
            .filter(|payload| **payload == raw_id)
            .count(),
        1,
        "{payloads:?}"
    );
    cleanup_tasks(&pool, &[task_id]).await;
}

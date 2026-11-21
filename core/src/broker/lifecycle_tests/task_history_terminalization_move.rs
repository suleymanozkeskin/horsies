//! Port of Python `clients/python/tests/integration/test_task_history_terminalization_move.py`.

use super::support::*;
use crate::broker::terminalization::on_test_pool::terminalize;
use crate::core::lifecycle::{TerminalizationCommand, TerminalizationKind, TerminalizationOutcome};
use crate::test_support::sql;

/// Python `TestAppliedCompletion::test_fused_completion_moves_the_row`: the
/// fused completion moves the row to history with its result and the digest
/// of the result, its last owner, and its one COMPLETED attempt in the
/// snapshot; no live row and no live attempt row stay.
#[tokio::test]
async fn fused_completion_moves_the_row() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;
    let result = r#"{"ok":true}"#;

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CompleteTaskFused {
            task_id,
            fence: owned_claim(),
            result_json: result.to_owned(),
        },
    )
    .await
    .expect("completion");

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
    let history = history_row(&pool, task_id)
        .await
        .expect("the completion moved the row to history");
    assert_eq!(history.status, "COMPLETED");
    assert_eq!(history.terminalization_kind, "COMPLETE_FUSED");
    assert_eq!(history.result_payload.as_deref(), Some(result.as_bytes()));
    assert_eq!(history.last_claimed_worker_id.as_deref(), Some(WORKER));
    let result_digest: Option<Vec<u8>> =
        sql::query_scalar("SELECT result_digest FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("read the result digest");
    assert_eq!(result_digest, Some(sha256_of(result.as_bytes())));
    let attempts = archived_attempt_records(&pool, task_id).await;
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].attempt(), 1);
    assert_eq!(attempts[0].outcome(), "COMPLETED");
    assert_eq!(attempts[0].worker_id(), Some(WORKER));
    assert!(live_row(&pool, task_id).await.is_none());
    assert!(live_attempts(&pool, task_id).await.is_empty());
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `TestAppliedCompletion::test_completed_is_never_eligible_even_with_retained_input`
/// (the test map names it `test_completed_is_never_eligible_even_with_prepared_inline`):
/// a completed task archives NEVER_ELIGIBLE with no envelope, even when its
/// input is retained.
#[tokio::test]
async fn completed_is_never_eligible_even_with_retained_input() {
    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            retain_rerun_input: true,
            input: r#"{"precious":"input"}"#,
            ..TaskSeed::running()
        },
    )
    .await;

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CompleteTaskFused {
            task_id,
            fence: owned_claim(),
            result_json: r#"{"ok": 1}"#.to_owned(),
        },
    )
    .await
    .expect("completion");

    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{outcomes:?}"
    );
    assert_eq!(
        rerun_input_of(&pool, task_id).await,
        RerunInputImage {
            rerun_input_disposition: "NEVER_ELIGIBLE".to_owned(),
            rerun_input_version: None,
            rerun_input_digest: None,
            rerun_input_inline: None,
        }
    );
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `TestMoveInvariants::test_deferred_terminalization_requires_a_result_payload`:
/// a workflow task cannot defer phase 2 with nothing to defer, so the move
/// refuses a deferred completion with no result.
#[tokio::test]
async fn deferred_terminalization_requires_a_result_payload() {
    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            ..TaskSeed::running()
        },
    )
    .await;
    let workflow_id = link_workflow_task(&pool, task_id, "RUNNING", "RUNNING").await;

    let refused = move_to_history(&pool, task_id, "COMPLETED", "COMPLETE_LOCKED", None).await;

    assert_refused_with_message(refused, "23502", "requires a result payload");
    assert!(live_row(&pool, task_id).await.is_some());
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// Python `TestMoveInvariants::test_duplicate_identity_fails_closed` and
/// `test_task_history_terminalization_prototypes.py::test_live_and_history_duplicate_is_rejected`:
/// a task that is live and in history at once is a corrupt identity, and the
/// move refuses it and writes nothing: the live row and its attempts stay,
/// history keeps the one planted row, and no phase-2 record is written.
#[tokio::test]
async fn duplicate_identity_fails_closed() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;
    let attempts_before = live_attempts(&pool, task_id).await;
    let mut connection = pool.acquire().await.expect("acquire");
    let copied = copy_live_row_to_history(
        &mut *connection,
        task_id,
        "COMPLETED",
        Some("COMPLETE_FUSED"),
        TerminalStamp::Now,
    )
    .await
    .expect("plant the duplicate history row");
    assert_eq!(copied, 1);
    drop(connection);

    let refused = terminalize(
        &pool,
        &TerminalizationCommand::CompleteTaskFused {
            task_id,
            fence: owned_claim(),
            result_json: r#"{"ok": 1}"#.to_owned(),
        },
    )
    .await;

    assert_server_refusal(&server_refusal(refused), "XX001", "multiple locations");
    assert!(live_row(&pool, task_id).await.is_some());
    assert_eq!(live_attempts(&pool, task_id).await, attempts_before);
    let history_rows: i64 =
        sql::query_scalar("SELECT count(*) FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("count the history rows");
    assert_eq!(history_rows, 1);
    assert!(!phase2_pending(&pool, task_id).await);
    cleanup_tasks(&pool, &[task_id]).await;
}

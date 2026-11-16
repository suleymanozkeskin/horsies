//! Port of Python `tests/integration/test_task_history_terminalization_move.py`.

use serial_test::serial;

use super::support::*;
use crate::broker::error::BrokerError;
use crate::broker::terminalization::terminalize;
use crate::core::lifecycle::{TerminalizationCommand, TerminalizationOutcome};

/// Python `TestAppliedCompletion::test_completed_is_never_eligible_even_with_retained_input`
/// (the test map names it `test_completed_is_never_eligible_even_with_prepared_inline`):
/// a completed task archives NEVER_ELIGIBLE with no envelope, even when its
/// input is retained.
#[tokio::test]
#[serial]
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
            notify_channel: "task_queue_default".to_owned(),
            notify_payload: format!("capacity:{task_id}"),
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
#[serial]
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

/// Python `TestMoveInvariants::test_duplicate_identity_fails_closed`: a task
/// that is live and in history at once is a corrupt identity, and the move
/// refuses it.
#[tokio::test]
#[serial]
async fn duplicate_identity_fails_closed() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;
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
            notify_channel: "task_queue_default".to_owned(),
            notify_payload: format!("capacity:{task_id}"),
        },
    )
    .await;

    match refused {
        Err(BrokerError::Database(error)) => {
            assert_refused_with_message(Err::<(), _>(error), "XX001", "multiple locations");
        }
        other => panic!("expected the duplicate identity refusal, got {other:?}"),
    }
    assert!(live_row(&pool, task_id).await.is_some());
    cleanup_tasks(&pool, &[task_id]).await;
}

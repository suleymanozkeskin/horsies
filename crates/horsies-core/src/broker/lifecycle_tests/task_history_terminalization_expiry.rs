//! Port of Python `tests/integration/test_task_history_terminalization_expiry.py`.
//!
//! `TestDiscoveryBatch::test_skip_locked_steps_around_a_held_row` is
//! `terminalization_functions::expire_pending_tasks::a_row_a_concurrent_claim_holds_is_stepped_around`.

use serial_test::serial;

use super::support::*;
use crate::broker::terminalization::on_test_pool::terminalize;
use crate::core::lifecycle::{
    BatchSize, TerminalizationCommand, TerminalizationKind, TerminalizationOutcome,
};
use crate::core::types::status::TaskStatus;
use crate::test_support::sql;

const EXPIRY_RESULT: &str = r#"{"err": "expired"}"#;

fn expire_pending() -> TerminalizationCommand {
    TerminalizationCommand::ExpirePendingTasks {
        batch_size: BatchSize::new(10).expect("positive batch size"),
        result_json: EXPIRY_RESULT.to_owned(),
        error_code: "TASK_EXPIRED".to_owned(),
    }
}

/// Python `TestDiscoveryBatch::test_mixed_batch_moves_only_eligible_rows`:
/// the batch moves the due PENDING rows, plain and workflow, and nothing
/// else. A moved row archives an empty attempt snapshot; a retained plain
/// input is framed inline, a workflow task is never eligible, and its
/// phase-2 record carries the digest of the expiry result.
#[tokio::test]
#[serial]
async fn mixed_batch_moves_only_eligible_rows() {
    let pool = pool().await;
    drain_pending_expiry(&pool).await;
    let mut expired_plain = Vec::new();
    for index in 0..3_u8 {
        let task_id = seed_task(
            &pool,
            &TaskSeed {
                retain_rerun_input: index == 0,
                ..TaskSeed::pending()
            },
        )
        .await;
        set_deadline(
            &pool,
            task_id,
            Deadline::PassedSecondsAgo(60.0 * f64::from(index + 1)),
        )
        .await;
        expired_plain.push(task_id);
    }
    let workflow_task = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            retain_rerun_input: true,
            ..TaskSeed::pending()
        },
    )
    .await;
    set_deadline(&pool, workflow_task, Deadline::PassedSecondsAgo(600.0)).await;
    let workflow_id = link_workflow_task(&pool, workflow_task, "PENDING", "RUNNING").await;
    let not_expired = seed_task(&pool, &TaskSeed::pending()).await;
    set_deadline(&pool, not_expired, Deadline::AheadSeconds(1_800.0)).await;
    let claimed = seed_task(&pool, &TaskSeed::claimed()).await;
    set_deadline(&pool, claimed, Deadline::PassedSecondsAgo(300.0)).await;

    let outcomes = terminalize(&pool, &expire_pending())
        .await
        .expect("expire pending");

    let mut moved: Vec<_> = outcomes
        .iter()
        .map(TerminalizationOutcome::task_id)
        .collect();
    moved.sort_unstable();
    let mut expected = expired_plain.clone();
    expected.push(workflow_task);
    expected.sort_unstable();
    assert_eq!(moved, expected);
    for outcome in &outcomes {
        assert!(
            matches!(
                outcome,
                TerminalizationOutcome::Applied {
                    kind: TerminalizationKind::ExpirePending,
                    observed,
                    ..
                } if observed.status == Some(TaskStatus::Pending)
            ),
            "{outcome:?}"
        );
    }
    for survivor in [not_expired, claimed] {
        assert!(live_row(&pool, survivor).await.is_some());
    }
    assert!(archived_attempts(&pool, expired_plain[0]).await.is_empty());
    assert_eq!(
        rerun_input_of(&pool, expired_plain[0])
            .await
            .rerun_input_disposition,
        "INLINE"
    );
    let record = phase2_record_of(&pool, workflow_task).await;
    assert_eq!(record.workflow_id, workflow_id);
    assert_eq!(record.recovery_source, "HISTORY");
    assert_eq!(record.terminalization_kind, "EXPIRE_PENDING");
    assert_eq!(record.result_digest, sha256_of(EXPIRY_RESULT.as_bytes()));
    assert_eq!(
        rerun_input_of(&pool, workflow_task)
            .await
            .rerun_input_disposition,
        "NEVER_ELIGIBLE"
    );

    let mut all = expected;
    all.extend([not_expired, claimed]);
    cleanup_tasks(&pool, &all).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// Python `TestDiscoveryBatch::test_batch_deferred_fence_raises_before_any_move`:
/// a due workflow task owes phase 2, and phase 2 needs a result payload, so a
/// sweep with no result is refused before any row moves.
#[tokio::test]
#[serial]
async fn batch_deferred_fence_raises_before_any_move() {
    let pool = pool().await;
    drain_pending_expiry(&pool).await;
    let workflow_task = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            ..TaskSeed::pending()
        },
    )
    .await;
    set_deadline(&pool, workflow_task, Deadline::PassedSecondsAgo(600.0)).await;
    let workflow_id = link_workflow_task(&pool, workflow_task, "PENDING", "RUNNING").await;

    let refused =
        sql::query("SELECT * FROM horsies_expire_pending_tasks(10, NULL, 'TASK_EXPIRED')")
            .fetch_all(&pool)
            .await
            .map(|rows| rows.len());

    assert_refused_with_message(refused, "23502", "requires a result payload");
    assert_eq!(
        live_row(&pool, workflow_task).await.expect("live row")["status"],
        "PENDING"
    );
    cleanup_tasks(&pool, &[workflow_task]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

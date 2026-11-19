//! Port of the Python history detail test of monitoring: the end stamp of a
//! moved task follows its status.
//!
//! Python specification: `clients/python/tests/integration/test_monitoring_history_detail.py`
//! (`TestTaskDetailFromHistory.test_moved_task_detail_resolves_with_attempts`).
//! The FAILED case and the attempts are in
//! `task_detail_uses_live_attempts_then_digest_verified_history`.

use super::*;

/// A COMPLETED task carries `completed_at` and no `failed_at`; a CANCELLED
/// task carries `failed_at` and no `completed_at`; an unknown id is not found.
#[tokio::test]
#[serial]
async fn a_moved_task_detail_carries_the_end_stamp_of_its_status() {
    let pool = migrated_pool().await;
    let scope = TestScope::new("end_stamp");
    clean_w2(&pool, &scope).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);

    let completed = scope.running("completed");
    seed_task(&pool, &completed).await;
    complete_task(&pool, completed.id).await;
    let cancelled = scope.pending("cancelled");
    seed_task(&pool, &cancelled).await;
    let outcomes = terminal_writes::cancel_live_task(&pool, cancelled.id, false)
        .await
        .expect("cancel the pending task");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{outcomes:?}"
    );

    let completed_detail = found_detail(
        get_task_detail(&broker, completed.id)
            .await
            .expect("completed detail"),
    );
    assert_eq!(completed_detail.leaf.status, "COMPLETED");
    assert!(completed_detail.leaf.completed_at.is_some());
    assert_eq!(completed_detail.leaf.failed_at, None);

    let cancelled_detail = found_detail(
        get_task_detail(&broker, cancelled.id)
            .await
            .expect("cancelled detail"),
    );
    assert_eq!(cancelled_detail.leaf.status, "CANCELLED");
    assert!(cancelled_detail.leaf.failed_at.is_some());
    assert_eq!(cancelled_detail.leaf.completed_at, None);

    let unknown = Uuid::new_v4();
    assert_eq!(
        get_task_detail(&broker, unknown)
            .await
            .expect("unknown detail"),
        TaskDetailRead::NotFound(TaskDetailNotFound {
            task_id: unknown,
            reason: TaskAbsenceReason::NotClassified,
        })
    );
    clean_w2(&pool, &scope).await;
}

//! Port of Python `clients/python/tests/integration/test_task_history_terminalization_cancellation.py`.
//!
//! Rows that a core test already checks are not repeated:
//! `TestOrphanSingle::test_terminal_only_link_still_orphans_and_archives_linkage`
//! is `terminalization_functions::cancel_owned_orphan::a_terminal_link_still_leaves_the_backing_task_orphaned`
//! (it also asserts the archived workflow id);
//! `TestOrphanSingle::test_fence_is_classified_before_the_guard` is
//! `terminalization_functions::cancel_owned_orphan::a_stale_generation_is_a_lost_claim_even_with_a_live_link`;
//! `TestOrphanSweep::test_sweep_moves_only_orphans_among_mixed_candidates` is
//! `terminalization_functions::cancel_orphaned_tasks::transitions_only_orphans_in_the_effective_source_set`.

use serial_test::serial;
use uuid::Uuid;

use super::support::*;

/// Python `TestOrphanSingle::test_links_to_distinct_workflows_fail_closed`:
/// a task backs at most one node. The unique index on the node's task id
/// refuses a second link, so no cancel can find a task that two workflows
/// claim.
#[tokio::test]
#[serial]
async fn links_to_distinct_workflows_fail_closed() {
    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            ..TaskSeed::claimed()
        },
    )
    .await;
    let first_workflow = link_workflow_task(&pool, task_id, "COMPLETED", "RUNNING").await;
    let second_workflow = Uuid::new_v4();
    seed_workflow(&pool, second_workflow, "RUNNING").await;

    let second_link = try_seed_node(&pool, second_workflow, Some(task_id), "FAILED").await;

    assert_refused(
        second_link,
        "23505",
        Constraint::Named("ix_horsies_workflow_tasks_task_id"),
    );
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[first_workflow, second_workflow]).await;
}

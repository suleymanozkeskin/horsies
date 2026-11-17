//! Completion edges: a success case and live nodes, the summary a parent node
//! gets, and workflows with no node. Python:
//! `tests/integration/test_success_policy_completion_repro.py`,
//! `tests/integration/test_workflow_finalization_contract.py`,
//! `tests/integration/test_workflow_sql_equivalence.py`.

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    node, ok, required, seed_node, seed_workflow, shared_pool, workflow, IsolatedDatabase,
    NodeSeed, ParentLink, WorkflowSeed,
};
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::{SubWorkflowSummary, WorkflowSpecRegistry, WorkflowStatus};
use crate::workflow_engine::engine::{check_workflow_completion, on_subworkflow_complete};

async fn complete(pool: &PgPool, workflow_id: Uuid) {
    check_workflow_completion(
        pool,
        workflow_id,
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await
    .expect("the completion check");
}

/// The summary stored on node 0 of `parent_id`.
async fn parent_summary(pool: &PgPool, parent_id: Uuid) -> SubWorkflowSummary {
    let text = node(pool, parent_id, 0)
        .await
        .sub_workflow_summary
        .expect("the parent node stores its child's summary");
    serde_json::from_str(&text).expect("the summary decodes")
}

/// A RUNNING parent with node 0 linked to `child`, and its child row.
async fn seed_parent_and_child(pool: &PgPool, child: WorkflowSeed) -> (Uuid, Uuid) {
    let parent_id = seed_workflow(pool, &WorkflowSeed::running()).await;
    let mut child = child;
    child.parent = Some(ParentLink::of_root(parent_id, 0));
    let child_id = seed_workflow(pool, &child).await;
    seed_node(
        pool,
        &NodeSeed::sub_workflow(
            parent_id,
            0,
            "RUNNING",
            "tests.port.child.v1",
            Some(child_id),
        ),
    )
    .await;
    (parent_id, child_id)
}

/// `test_success_policy_does_not_finalize_with_live_non_required_task`: a
/// satisfied case is evaluated only once every node is terminal.
#[tokio::test]
#[serial]
async fn a_met_success_case_waits_for_a_live_node_that_it_does_not_require() {
    let db = IsolatedDatabase::create().await;
    let pool = db.pool.clone();
    let workflow_id = seed_workflow(
        &pool,
        &WorkflowSeed::running()
            .policy(serde_json::to_value(required(&[&[0]])).expect("encode the policy")),
    )
    .await;
    seed_node(&pool, &NodeSeed::task(workflow_id, 0, "COMPLETED")).await;
    seed_node(&pool, &NodeSeed::task(workflow_id, 1, "ENQUEUED")).await;

    complete(&pool, workflow_id).await;

    let row = workflow(&pool, workflow_id).await;
    assert_eq!(row.status, "RUNNING");
    assert_eq!(row.completed_at, None);
    db.drop().await;
}

/// `test_a_failed_child_gives_its_parent_a_summary_without_output`: the
/// child's output node completed, but the child FAILED, so the parent's
/// summary has no output.
#[tokio::test]
#[serial]
async fn a_failed_child_gives_its_parent_a_summary_without_output() {
    let pool = shared_pool().await;
    let (parent_id, child_id) =
        seed_parent_and_child(&pool, WorkflowSeed::running().output(1)).await;
    let failure = serde_json::to_string(&super::support::err("CONTRACT_FAIL", "node failed"))
        .expect("encode the failure");
    let success = serde_json::to_string(&ok(serde_json::json!(1))).expect("encode the success");
    seed_node(
        &pool,
        &NodeSeed::task(child_id, 0, "FAILED").result(failure),
    )
    .await;
    seed_node(
        &pool,
        &NodeSeed::task(child_id, 1, "COMPLETED").result(success.clone()),
    )
    .await;
    seed_node(
        &pool,
        &NodeSeed::task(child_id, 2, "COMPLETED").result(success),
    )
    .await;

    complete(&pool, child_id).await;

    assert_eq!(workflow(&pool, child_id).await.status, "FAILED");
    let summary = parent_summary(&pool, parent_id).await;
    assert_eq!(summary.status, WorkflowStatus::Failed);
    assert_eq!(summary.output, None);
}

/// `test_child_info_single_pass_matches_correlated_form`: the counts of the
/// summary equal the child's node statuses, over ten mixes.
#[tokio::test]
#[serial]
async fn child_summary_counts_follow_the_child_node_statuses() {
    let pool = shared_pool().await;
    let statuses = ["COMPLETED", "FAILED", "SKIPPED"];
    for trial in 1..=10_usize {
        let (parent_id, child_id) =
            seed_parent_and_child(&pool, WorkflowSeed::running().status("FAILED")).await;
        let node_count = 4 * trial;
        let mut expected = [0_i32; 3];
        for index in 0..node_count {
            let kind = (index * trial + index / 3) % statuses.len();
            expected[kind] += 1;
            let task_index = i32::try_from(index).expect("a small index");
            seed_node(&pool, &NodeSeed::task(child_id, task_index, statuses[kind])).await;
        }

        on_subworkflow_complete(
            &pool,
            parent_id,
            0,
            child_id,
            "FAILED",
            None,
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("the parent node advances");

        let summary = parent_summary(&pool, parent_id).await;
        assert_eq!(
            (
                summary.total_tasks,
                summary.completed_tasks,
                summary.failed_tasks,
                summary.skipped_tasks,
            ),
            (
                i32::try_from(node_count).expect("a small count"),
                expected[0],
                expected[1],
                expected[2],
            ),
            "trial {trial}"
        );
    }
}

/// `test_terminal_two_step_handles_zero_task_workflow`: a workflow with no
/// node has an empty terminal set, and its completion reads no row and fails
/// on nothing. It has no output node, so it stores Python's outputless result
/// with three empty maps (`python/horsies/core/workflows/engine.py:3935-3983`; the
/// baseline `horsies_workflow_outputless_result`).
#[tokio::test]
#[serial]
async fn a_workflow_with_no_node_completes_with_an_empty_terminal_set() {
    let pool = shared_pool().await;
    let workflow_id = seed_workflow(&pool, &WorkflowSeed::running()).await;

    complete(&pool, workflow_id).await;

    let row = workflow(&pool, workflow_id).await;
    assert_eq!(row.status, "COMPLETED");
    let stored = row.result.expect("a COMPLETED workflow stores its result");
    let result: serde_json::Value =
        serde_json::from_str(&stored).expect("the stored result is JSON");
    assert_eq!(
        result,
        serde_json::json!({
            "__h_task_result__": true,
            "__h_outputless_terminals__": true,
            "ok": {"results_by_id": {}, "task_name_by_id": {}, "definition_key_by_id": {}},
            "err": null,
        })
    );
}

/// `test_child_info_counts_zero_for_taskless_workflow`: a child with no node
/// reports a total of 0.
#[tokio::test]
#[serial]
async fn a_child_with_no_node_reports_a_total_of_zero() {
    let pool = shared_pool().await;
    let (parent_id, child_id) =
        seed_parent_and_child(&pool, WorkflowSeed::running().status("FAILED")).await;

    on_subworkflow_complete(
        &pool,
        parent_id,
        0,
        child_id,
        "FAILED",
        None,
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await
    .expect("the parent node advances");

    let summary = parent_summary(&pool, parent_id).await;
    assert_eq!(
        (
            summary.total_tasks,
            summary.completed_tasks,
            summary.failed_tasks,
            summary.skipped_tasks,
        ),
        (0, 0, 0, 0)
    );
}

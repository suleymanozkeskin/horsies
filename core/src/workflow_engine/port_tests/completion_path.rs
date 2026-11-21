//! What phase 2 writes for one finished node of a chain A -> B: the node
//! write is idempotent, `on_error` decides between failing and pausing, a
//! paused workflow stores the result and promotes nothing, and a task with no
//! node changes nothing.
//!
//! Each node's task finishes as a worker finishes it (phase 1, then phase 2).

use uuid::Uuid;

use super::support::{
    code, end_workflows, err, finish_node, node, ok, shared_pool, spec, start, stored_error,
    task_node, workflow, workflow_error_code,
};
use crate::broker::PostgresBroker;
use crate::core::OnError;
use crate::test_support::child_specs::TestChildSpecs;
use crate::test_support::sql::{self, TestPool};
use crate::workflow_engine::engine::on_workflow_task_complete_with;

/// A -> B with `on_error` (Python's `_chain_spec`).
async fn start_chain(pool: &TestPool, name: &str, on_error: OnError) -> Uuid {
    let mut dependent = task_node(1, &format!("{name}_dep"));
    dependent.dependencies = vec![0];
    let mut chain = spec(name, vec![task_node(0, &format!("{name}_root")), dependent]);
    chain.on_error = on_error;
    start(pool, &chain, &TestChildSpecs::empty()).await
}

/// `test_terminal_cas_is_idempotent`: a second phase 2 of the same task does
/// not write the node again.
#[tokio::test]
async fn a_second_completion_of_a_node_keeps_the_first_result() {
    let pool = shared_pool().await;
    let workflow_id = start_chain(&pool, "completion_idempotent", OnError::Fail).await;
    let task_id = finish_node(
        &pool,
        workflow_id,
        0,
        &ok(serde_json::json!(1)),
        &TestChildSpecs::empty(),
    )
    .await;
    let first = node(&pool, workflow_id, 0).await;
    assert_eq!(first.status, "COMPLETED");

    on_workflow_task_complete_with(
        &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
        task_id,
        &TestChildSpecs::empty(),
    )
    .await
    .expect("a second phase 2 of the task");

    let second = node(&pool, workflow_id, 0).await;
    assert_eq!(second.status, "COMPLETED");
    assert_eq!(
        second.result, first.result,
        "the stored result is the first"
    );
    assert_eq!(second.completed_at, first.completed_at);
}

/// `test_on_error_fail_records_error_and_resolves_dag`: under
/// `on_error = fail`, the failure is stored as the workflow error, the
/// dependent is skipped and the workflow ends FAILED in the same phase 2.
#[tokio::test]
async fn a_failure_under_on_error_fail_stores_the_error_and_resolves_the_workflow() {
    let pool = shared_pool().await;
    let workflow_id = start_chain(&pool, "completion_fail", OnError::Fail).await;

    let _task = finish_node(
        &pool,
        workflow_id,
        0,
        &err("BENCH_FAIL", "forced"),
        &TestChildSpecs::empty(),
    )
    .await;

    assert_eq!(
        workflow_error_code(&pool, workflow_id).await.as_deref(),
        Some("BENCH_FAIL")
    );
    assert_eq!(node(&pool, workflow_id, 1).await.status, "SKIPPED");
    assert_eq!(workflow(&pool, workflow_id).await.status, "FAILED");
}

/// `test_on_error_pause_stops_propagation`: under `on_error = pause`, the
/// failure pauses the workflow with the failure as its error, and the
/// dependent stays PENDING for a resume.
#[tokio::test]
async fn a_failure_under_on_error_pause_pauses_and_leaves_the_dependent_pending() {
    let pool = shared_pool().await;
    let workflow_id = start_chain(&pool, "completion_pause", OnError::Pause).await;

    let _task = finish_node(
        &pool,
        workflow_id,
        0,
        &err("BENCH_FAIL", "forced"),
        &TestChildSpecs::empty(),
    )
    .await;

    let paused = workflow(&pool, workflow_id).await;
    assert_eq!(paused.status, "PAUSED");
    let error = stored_error(&paused.error.expect("the pause stores its error"));
    assert_eq!(code(&error).as_deref(), Some("BENCH_FAIL"));
    assert_eq!(node(&pool, workflow_id, 0).await.status, "FAILED");
    assert_eq!(node(&pool, workflow_id, 1).await.status, "PENDING");
    end_workflows(&pool, &[workflow_id]).await;
}

/// `test_paused_workflow_stores_result_without_promoting`: a node of a
/// PAUSED workflow takes its result, and its dependent stays PENDING.
#[tokio::test]
async fn a_node_of_a_paused_workflow_stores_its_result_and_promotes_nothing() {
    let pool = shared_pool().await;
    let workflow_id = start_chain(&pool, "completion_paused", OnError::Fail).await;
    sql::query("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = $1")
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("pause the workflow row");

    let _task = finish_node(
        &pool,
        workflow_id,
        0,
        &ok(serde_json::json!(1)),
        &TestChildSpecs::empty(),
    )
    .await;

    let root = node(&pool, workflow_id, 0).await;
    assert_eq!(root.status, "COMPLETED");
    assert!(root.result.is_some(), "the root stores its result");
    let dependent = node(&pool, workflow_id, 1).await;
    assert_eq!(
        (dependent.status.as_str(), dependent.task_id),
        ("PENDING", None)
    );
    end_workflows(&pool, &[workflow_id]).await;
}

/// `test_non_workflow_task_is_noop`: phase 2 of a task that no node links
/// returns without an error.
#[tokio::test]
async fn phase_two_of_a_task_with_no_node_changes_nothing() {
    let pool = shared_pool().await;
    let task_id = Uuid::new_v4();

    on_workflow_task_complete_with(
        &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
        task_id,
        &TestChildSpecs::empty(),
    )
    .await
    .expect("phase 2 of a task with no node returns");
}

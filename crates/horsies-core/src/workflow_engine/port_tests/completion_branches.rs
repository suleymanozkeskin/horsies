//! Phase 2 of a finished workflow task: a paused workflow, a node with no
//! dependents to read, and the capacity wake. Python:
//! `tests/unit/test_workflow_engine_coverage.py::TestOnWorkflowTaskComplete`
//! (Python ran phase 2 in `on_workflow_task_complete`; the core and Python
//! now run it through `horsies_workflow_node_finish`).
//!
//! Each node runs on [`COMPLETION_QUEUE`], which no other test uses, so a
//! listener on its channel hears only the test's own notifications.

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    end_workflows, finish_phase1, node, ok, seed_node, seed_node_task, seed_workflow, shared_pool,
    workflow_status, LiveStatus, NodeSeed, Notifications, WorkflowSeed,
};
use crate::broker::workflow_phase2::NodeFinishForm;
use crate::broker::PostgresBroker;
use crate::test_support::child_specs::TestChildSpecs;
use crate::workflow_engine::engine::{on_workflow_task_complete_with, WorkflowTaskCompletion};
use crate::workflow_engine::steps::{node_finish_alone, Dependents, NodeFinishDisposition};

/// The queue of the node rows of these tests.
const COMPLETION_QUEUE: &str = "port_completion_branch_q";

/// The channel of [`COMPLETION_QUEUE`].
const COMPLETION_CHANNEL: &str = "task_queue_port_completion_branch_q";

/// A workflow in `status` with node 0 RUNNING behind a RUNNING task, and,
/// when `with_dependent`, node 1 PENDING after node 0. Phase 1 of the task
/// has run, so phase 2 is owed. Returns (workflow, task).
async fn seed_owed_phase_two(
    pool: &PgPool,
    status: &'static str,
    with_dependent: bool,
) -> (Uuid, Uuid) {
    let workflow_id = seed_workflow(pool, &WorkflowSeed::running().status(status)).await;
    let task_id = Uuid::new_v4();
    let _claimed_at = seed_node_task(pool, task_id, LiveStatus::Running).await;
    let _root = seed_node(
        pool,
        &NodeSeed {
            queue_name: COMPLETION_QUEUE,
            ..NodeSeed::task(workflow_id, 0, "RUNNING").task_id(task_id)
        },
    )
    .await;
    if with_dependent {
        let _dependent = seed_node(
            pool,
            &NodeSeed {
                queue_name: COMPLETION_QUEUE,
                ..NodeSeed::task(workflow_id, 1, "PENDING").deps(&[0])
            },
        )
        .await;
    }
    let _stored = finish_phase1(pool, task_id, &ok(serde_json::json!(1))).await;
    (workflow_id, task_id)
}

/// `TestOnWorkflowTaskComplete::test_paused_workflow_stores_result_without_propagation`:
/// phase 2 under a PAUSED workflow writes the node and promotes nothing.
#[tokio::test]
#[serial]
async fn phase_two_under_a_paused_workflow_writes_the_node_and_promotes_nothing() {
    let pool = shared_pool().await;
    let (workflow_id, task_id) = seed_owed_phase_two(&pool, "PAUSED", true).await;

    let completion = on_workflow_task_complete_with(
        &PostgresBroker::from_pool(pool.clone(), crate::ClientFamily::Rust),
        task_id,
        &TestChildSpecs::empty(),
    )
    .await;

    let root = node(&pool, workflow_id, 0).await;
    let dependent = node(&pool, workflow_id, 1).await;
    let status = workflow_status(&pool, workflow_id).await;
    end_workflows(&pool, &[workflow_id]).await;
    assert!(
        matches!(completion, Ok(WorkflowTaskCompletion::Applied)),
        "{completion:?}"
    );
    assert_eq!(root.status, "COMPLETED");
    assert!(root.result.is_some());
    assert_eq!(dependent.status, "PENDING");
    assert_eq!(dependent.task_id, None);
    assert_eq!(status, "PAUSED");
}

/// `TestOnWorkflowTaskComplete::test_the_dependents_are_read_only_for_a_node_that_has_some`
/// (case `no_dependents`): a node finish told that no node depends on the
/// node reads no dependents, and the completion check still runs, so the
/// workflow completes. The `has_dependents` case is
/// `engine/task_complete_tests.rs::a_node_of_a_live_workflow_completes_and_promotes_its_dependent`.
#[tokio::test]
#[serial]
async fn a_node_finish_that_reads_no_dependents_still_completes_its_workflow() {
    let pool = shared_pool().await;
    let (workflow_id, task_id) = seed_owed_phase_two(&pool, "RUNNING", false).await;

    let finished = node_finish_alone(
        &PostgresBroker::from_pool(pool.clone(), crate::ClientFamily::Rust),
        task_id,
        false,
        Dependents::NoneToRead,
    )
    .await;

    let root = node(&pool, workflow_id, 0).await;
    let status = workflow_status(&pool, workflow_id).await;
    end_workflows(&pool, &[workflow_id]).await;
    let finished = finished.expect("the node finish");
    assert!(
        matches!(finished.disposition, NodeFinishDisposition::Applied),
        "{finished:?}"
    );
    assert_eq!(root.status, "COMPLETED");
    assert_eq!(status, "COMPLETED");
}

/// The form of a phase 2 decides whether the function reads the dependents
/// (`PostgresBroker::finish_workflow_node`): `NoDependents` sends `'NONE'`,
/// so a PENDING dependent stays PENDING with no task; `DependentsWithoutSubworkflow`
/// sends `'READ'`, so the dependent is promoted with its task row.
#[tokio::test]
#[serial]
async fn the_form_of_a_node_finish_decides_whether_the_dependents_are_read() {
    let pool = shared_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone(), crate::ClientFamily::Rust);
    let (unread_workflow, unread_task) = seed_owed_phase_two(&pool, "RUNNING", true).await;
    let (read_workflow, read_task) = seed_owed_phase_two(&pool, "RUNNING", true).await;

    let unread = broker
        .finish_workflow_node(
            unread_task,
            NodeFinishForm::NoDependents,
            false,
            &TestChildSpecs::empty(),
        )
        .await;
    let read = broker
        .finish_workflow_node(
            read_task,
            NodeFinishForm::DependentsWithoutSubworkflow,
            false,
            &TestChildSpecs::empty(),
        )
        .await;

    let unread_dependent = node(&pool, unread_workflow, 1).await;
    let read_dependent = node(&pool, read_workflow, 1).await;
    end_workflows(&pool, &[unread_workflow, read_workflow]).await;
    assert_eq!(
        unread
            .expect("the node finish that reads no dependents")
            .disposition,
        "APPLIED"
    );
    assert_eq!(
        read.expect("the node finish that reads the dependents")
            .disposition,
        "APPLIED"
    );
    assert_eq!(unread_dependent.status, "PENDING");
    assert_eq!(unread_dependent.task_id, None);
    assert_eq!(read_dependent.status, "ENQUEUED");
    assert!(read_dependent.task_id.is_some());
}

/// `TestOnWorkflowTaskComplete::test_no_wake_is_the_default`: the phase 2 of
/// `on_workflow_task_complete` sends no capacity wake on the queue of the
/// node.
#[tokio::test]
#[serial]
async fn the_phase_two_of_on_workflow_task_complete_sends_no_capacity_wake() {
    let pool = shared_pool().await;
    let (workflow_id, task_id) = seed_owed_phase_two(&pool, "RUNNING", false).await;
    let mut listener = Notifications::listen(&pool, &[COMPLETION_CHANNEL]).await;

    let completion = on_workflow_task_complete_with(
        &PostgresBroker::from_pool(pool.clone(), crate::ClientFamily::Rust),
        task_id,
        &TestChildSpecs::empty(),
    )
    .await;

    let received = listener.received().await;
    let root = node(&pool, workflow_id, 0).await;
    end_workflows(&pool, &[workflow_id]).await;
    assert!(
        matches!(completion, Ok(WorkflowTaskCompletion::Applied)),
        "{completion:?}"
    );
    assert_eq!(root.status, "COMPLETED");
    assert_eq!(received, Vec::new());
}

//! The recovery cases of the reaper's workflow pass, each on its own database.
//! Python: `tests/integration/test_workflow_recovery.py` and
//! `test_orphaned_workflow_task_repro.py`.

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    child_specs_of, node, node_status, ok, required, seed_node, seed_node_task, seed_workflow,
    spec, start_claimed_task, task_node, workflow, workflow_status, IsolatedDatabase, LiveStatus,
    NodeSeed, ParentLink, WorkflowSeed,
};
use crate::core::{TaskError, WorkflowSpec};
use crate::test_support::child_specs::TestChildSpecs;
use crate::workflow_engine::recovery::{recover_stuck_workflows_observed_with, RecoveryReport};

/// `workflow_recovery_failure_limit` of each pass.
const LIMIT: u32 = 3;

async fn pass(pool: &PgPool, child_specs: &TestChildSpecs) -> RecoveryReport {
    recover_stuck_workflows_observed_with(
        &crate::broker::PostgresBroker::from_pool(pool.clone()),
        child_specs,
        LIMIT,
    )
    .await
    .expect("the recovery pass returns its report")
}

fn ok_text(value: serde_json::Value) -> String {
    serde_json::to_string(&ok(value)).expect("encode a result")
}

fn err_text(error_code: &str) -> String {
    serde_json::to_string(&super::support::err(error_code, "fail")).expect("encode a failure")
}

/// The child of the sub-workflow cases: one task node that is its output.
fn recovery_child() -> WorkflowSpec {
    let mut child = spec("recovery_child", vec![task_node(0, "recovery_child_task")]);
    child.output_index = Some(0);
    child
}

/// `test_recover_ready_not_enqueued` (case 1) and `test_recover_idempotent`:
/// a READY node with no task is enqueued once; a second pass finds nothing.
#[tokio::test]
#[serial]
async fn a_ready_node_with_no_task_is_enqueued_once() {
    let db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(&db.pool, &NodeSeed::task(workflow_id, 0, "READY")).await;

    let first = pass(&db.pool, &child_specs).await;
    let second = pass(&db.pool, &child_specs).await;

    assert_eq!(first.total(), 1, "{first:?}");
    assert_eq!(second.total(), 0, "{second:?}");
    let row = node(&db.pool, workflow_id, 0).await;
    assert_eq!(row.status, "ENQUEUED");
    assert!(row.task_id.is_some());
    db.drop().await;
}

/// `test_recover_paused_not_touched`
#[tokio::test]
#[serial]
async fn recovery_leaves_a_paused_workflow_alone() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(
        &db.pool,
        &WorkflowSeed::running().status("PAUSED").on_error("pause"),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "FAILED").result(err_text("FAIL")),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 1, "PENDING").deps(&[0]),
    )
    .await;

    let _report = pass(&db.pool, &TestChildSpecs::empty()).await;

    assert_eq!(workflow_status(&db.pool, workflow_id).await, "PAUSED");
    assert_eq!(node_status(&db.pool, workflow_id, 1).await, "PENDING");
    db.drop().await;
}

/// `test_recover_completed_sets_timestamp` (case 2+3)
#[tokio::test]
#[serial]
async fn recovery_completes_a_workflow_whose_nodes_all_ended_and_sets_completed_at() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "COMPLETED").result(ok_text(serde_json::json!(10))),
    )
    .await;

    let report = pass(&db.pool, &TestChildSpecs::empty()).await;

    assert_eq!(report.total(), 1, "{report:?}");
    let row = workflow(&db.pool, workflow_id).await;
    assert_eq!(row.status, "COMPLETED");
    assert!(row.completed_at.is_some());
    db.drop().await;
}

/// `test_recover_respects_success_policy` and `test_recover_success_policy_not_satisfied`
#[tokio::test]
#[serial]
async fn recovery_decides_by_the_success_policy() {
    let db = IsolatedDatabase::create().await;
    let mut cases = Vec::new();
    for (policy, expected) in [
        (required(&[&[0]]), "COMPLETED"),
        (required(&[&[0, 1]]), "FAILED"),
    ] {
        let workflow_id = seed_workflow(
            &db.pool,
            &WorkflowSeed::running()
                .policy(serde_json::to_value(policy).expect("encode the policy")),
        )
        .await;
        seed_node(
            &db.pool,
            &NodeSeed::task(workflow_id, 0, "COMPLETED").result(ok_text(serde_json::json!(2))),
        )
        .await;
        seed_node(
            &db.pool,
            &NodeSeed::task(workflow_id, 1, "FAILED").result(err_text("TEST")),
        )
        .await;
        cases.push((workflow_id, expected));
    }

    let _report = pass(&db.pool, &TestChildSpecs::empty()).await;

    for (workflow_id, expected) in cases {
        assert_eq!(workflow_status(&db.pool, workflow_id).await, expected);
    }
    db.drop().await;
}

/// `test_recover_pending_deps_failed_skip` (case 0): the node is skipped and
/// the workflow fails. One step does both: `horsies_workflow_node_reevaluate`
/// runs the completion check (`horsies_workflow_try_complete`) in the
/// transaction of the skip, so the pass counts one case-0 recovery and case
/// 2/3 finds no workflow left.
#[tokio::test]
#[serial]
async fn a_pending_node_after_a_failed_dependency_is_skipped_and_the_workflow_fails() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "FAILED").result(err_text("TEST_FAIL")),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 1, "PENDING").deps(&[0]),
    )
    .await;

    let report = pass(&db.pool, &TestChildSpecs::empty()).await;

    assert_eq!(report.case0_pending_reevaluated, 1, "{report:?}");
    assert_eq!(report.total(), 1, "{report:?}");
    assert_eq!(node_status(&db.pool, workflow_id, 1).await, "SKIPPED");
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    db.drop().await;
}

/// `test_recover_pending_deps_failed_allow_continue` (case 0)
#[tokio::test]
#[serial]
async fn a_pending_allow_failed_deps_node_after_a_failure_is_enqueued() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "FAILED").result(err_text("TEST_FAIL")),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 1, "PENDING")
            .deps(&[0])
            .allow_failed_deps(),
    )
    .await;

    let report = pass(&db.pool, &TestChildSpecs::empty()).await;

    assert_eq!(report.total(), 1, "{report:?}");
    let row = node(&db.pool, workflow_id, 1).await;
    assert_eq!(row.status, "ENQUEUED");
    assert!(row.task_id.is_some());
    db.drop().await;
}

/// `test_recover_subworkflow_ready_with_broker` (case 1.5)
#[tokio::test]
#[serial]
async fn a_ready_sub_workflow_node_with_no_child_starts_its_child() {
    let db = IsolatedDatabase::create().await;
    let child = recovery_child();
    let key = child
        .definition_key
        .clone()
        .expect("the child has a definition key");
    let child_specs = child_specs_of(&[child]);
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running().output(1)).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "COMPLETED").result(ok_text(serde_json::json!(2))),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::sub_workflow(workflow_id, 1, "READY", &key, None).deps(&[0]),
    )
    .await;

    let report = pass(&db.pool, &child_specs).await;

    assert_eq!(report.total(), 1, "{report:?}");
    let sub = node(&db.pool, workflow_id, 1).await;
    assert_eq!(sub.status, "RUNNING");
    let child_id = sub.sub_workflow_id.expect("the child started");
    assert_eq!(workflow_status(&db.pool, child_id).await, "RUNNING");
    db.drop().await;
}

/// A RUNNING parent whose sub-workflow node 0 is RUNNING and linked to a
/// child that ended with `child_status`.
async fn seed_parent_of_an_ended_child(
    pool: &PgPool,
    child_status: &'static str,
    child_result: Option<String>,
) -> (Uuid, Uuid) {
    let parent = seed_workflow(pool, &WorkflowSeed::running().output(0)).await;
    let mut child_seed = WorkflowSeed::child(ParentLink::of_root(parent, 0))
        .status(child_status)
        .output(0);
    child_seed.result = child_result;
    let child_node = match child_status {
        "FAILED" => {
            child_seed.error = Some(
                serde_json::to_string(
                    &TaskError::new("CHILD_CRASH", "child task crashed")
                        .expect("a user error code"),
                )
                .expect("encode the child error"),
            );
            NodeSeed::task(Uuid::nil(), 0, "FAILED").result(err_text("CHILD_CRASH"))
        }
        _ => NodeSeed::task(Uuid::nil(), 0, "COMPLETED").result(ok_text(serde_json::json!(42))),
    };
    let child = seed_workflow(pool, &child_seed).await;
    seed_node(
        pool,
        &NodeSeed {
            workflow_id: child,
            ..child_node
        },
    )
    .await;
    seed_node(
        pool,
        &NodeSeed::sub_workflow(
            parent,
            0,
            "RUNNING",
            "tests.port.recovery_child.v1",
            Some(child),
        ),
    )
    .await;
    (parent, child)
}

/// `test_recover_child_completed_parent_not_updated` (case 1.6)
#[tokio::test]
#[serial]
async fn a_completed_child_advances_its_parent_node() {
    let db = IsolatedDatabase::create().await;
    let (parent, _child) =
        seed_parent_of_an_ended_child(&db.pool, "COMPLETED", Some(ok_text(serde_json::json!(42))))
            .await;

    let report = pass(&db.pool, &TestChildSpecs::empty()).await;

    assert!(report.total() >= 1, "{report:?}");
    assert_eq!(node_status(&db.pool, parent, 0).await, "COMPLETED");
    assert_eq!(workflow_status(&db.pool, parent).await, "COMPLETED");
    db.drop().await;
}

/// `test_recover_child_cancelled_parent_not_updated` (case 1.6) and
/// `test_workflow_subworkflow.py::TestSubworkflowIntegration.test_recovery_child_failed_parent_still_running`:
/// a child that did not complete fails its parent node.
#[tokio::test]
#[serial]
async fn a_cancelled_or_failed_child_fails_its_parent_node() {
    let db = IsolatedDatabase::create().await;
    let mut parents = Vec::new();
    for child_status in ["CANCELLED", "FAILED"] {
        let (parent, _child) = seed_parent_of_an_ended_child(&db.pool, child_status, None).await;
        parents.push((parent, child_status));
    }

    let report = pass(&db.pool, &TestChildSpecs::empty()).await;

    assert!(report.total() >= 2, "{report:?}");
    for (parent, child_status) in parents {
        assert_eq!(
            node_status(&db.pool, parent, 0).await,
            "FAILED",
            "{child_status}"
        );
    }
    db.drop().await;
}

/// `test_orphaned_workflow_task_repro.py::test_workflow_check_failed_disabled_leaves_orphan_claimed`:
/// a claimed workflow task with no runnable node is not run: the start of
/// the child host misses with `NODE_NOT_LINKED` and writes nothing, so the
/// task stays CLAIMED. With orphan self-heal off the outcome writer then
/// writes nothing either
/// (`worker/executor/slots/settle_tests.rs::a_task_whose_node_is_not_linked_stays_claimed_when_the_self_heal_is_off`).
#[tokio::test]
#[serial]
async fn an_orphan_stays_claimed_when_self_heal_is_off() {
    let db = IsolatedDatabase::create().await;
    let task_id = Uuid::new_v4();
    let claimed_at = seed_node_task(&db.pool, task_id, LiveStatus::Claimed)
        .await
        .expect("a CLAIMED row has a claim generation");
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "COMPLETED")
            .task_id(task_id)
            .result(ok_text(serde_json::json!(1))),
    )
    .await;

    let outcome = start_claimed_task(&db.pool, task_id, claimed_at).await;

    assert_eq!(
        outcome,
        crate::broker::claims::StartOutcome::Missed(
            crate::broker::claims::StartMiss::NodeNotLinked
        )
    );
    let status: String = sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(&db.pool)
        .await
        .expect("the orphan keeps its live row");
    assert_eq!(status, "CLAIMED");
    db.drop().await;
}

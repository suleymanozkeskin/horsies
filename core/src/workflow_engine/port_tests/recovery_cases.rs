//! The recovery cases of the reaper's workflow pass, each on its own database.
//! Python: `clients/python/tests/integration/test_workflow_recovery.py` and
//! `test_orphaned_workflow_task_repro.py`.

use uuid::Uuid;

use super::support::{
    args_from_inner, child_specs_of, code, error_of, finish_phase1, node, node_status,
    node_task_input, ok, required, seed_node, seed_node_task, seed_workflow, spec, start,
    start_claimed_task, task_node, workflow, workflow_error_code, workflow_status,
    IsolatedDatabase, LiveStatus, NodeSeed, ParentLink, WorkflowSeed,
};
use crate::core::{OperationalErrorCode, TaskError, TaskResult, WorkflowSpec};
use crate::test_support::child_specs::TestChildSpecs;
use crate::test_support::sql::{self, TestPool};
use crate::workflow_engine::phase2_recovery::drive_phase2_recovery_with;
use crate::workflow_engine::recovery::{recover_stuck_workflows_observed_with, RecoveryReport};

/// `workflow_recovery_failure_limit` of each pass.
const LIMIT: u32 = 3;

async fn pass(pool: &TestPool, child_specs: &TestChildSpecs) -> RecoveryReport {
    recover_stuck_workflows_observed_with(
        &crate::broker::PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
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

/// `test_recover_respects_success_policy` and `test_recover_success_policy_not_satisfied`:
/// each workflow counts one recovery; the workflow whose case is not met ends
/// FAILED with the error of its failed required node.
#[tokio::test]
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
    let cases_failed = cases[1];

    let report = pass(&db.pool, &TestChildSpecs::empty()).await;

    assert_eq!(report.total(), 2, "{report:?}");
    for (workflow_id, expected) in cases {
        assert_eq!(workflow_status(&db.pool, workflow_id).await, expected);
    }
    let (failed, _) = cases_failed;
    assert_eq!(
        workflow_error_code(&db.pool, failed).await.as_deref(),
        Some("TEST")
    );
    db.drop().await;
}

/// `test_recover_pending_deps_failed_skip` (case 0): the node is skipped and
/// the workflow fails. One step does both: `horsies_workflow_node_reevaluate`
/// runs the completion check (`horsies_workflow_try_complete`) in the
/// transaction of the skip, so the pass counts one case-0 recovery and case
/// 2/3 finds no workflow left.
#[tokio::test]
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
async fn a_pending_allow_failed_dependencies_node_after_a_failure_is_enqueued() {
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
            .allow_failed_dependencies(),
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
    pool: &TestPool,
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
    let status: String = sql::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(&db.pool)
        .await
        .expect("the orphan keeps its live row");
    assert_eq!(status, "CLAIMED");
    db.drop().await;
}

/// `test_recover_completed_not_marked` (case 2+3): a RUNNING workflow whose
/// only node COMPLETED ends COMPLETED and stores its result.
#[tokio::test]
async fn recovery_completes_a_workflow_whose_node_completed_and_stores_its_result() {
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
    assert!(row.result.is_some(), "{row:?}");
    db.drop().await;
}

/// `test_recover_failed_not_marked` (case 2+3): a RUNNING workflow whose only
/// node FAILED, with no stored workflow error, ends FAILED with the node's
/// error and no result.
#[tokio::test]
async fn recovery_fails_a_workflow_whose_node_failed_with_its_error_and_no_result() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "FAILED").result(err_text("TEST_ERROR")),
    )
    .await;

    let report = pass(&db.pool, &TestChildSpecs::empty()).await;

    assert_eq!(report.total(), 1, "{report:?}");
    let row = workflow(&db.pool, workflow_id).await;
    assert_eq!(row.status, "FAILED");
    assert_eq!(row.result, None);
    assert_eq!(
        workflow_error_code(&db.pool, workflow_id).await.as_deref(),
        Some("TEST_ERROR")
    );
    db.drop().await;
}

/// `test_recover_failed_recomputes_first_failed_error` (case 2+3): a RUNNING
/// workflow that stores the error of its second failed node is finalized
/// FAILED with the error of its first failed node by index, and no result.
#[tokio::test]
async fn recovery_fails_a_workflow_with_the_error_of_its_first_failed_node() {
    let db = IsolatedDatabase::create().await;
    let later = serde_json::to_string(
        &TaskError::new("SECOND_ERROR", "Second failure").expect("a user error code"),
    )
    .expect("encode the stored error");
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running().error(later)).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "FAILED").result(err_text("FIRST_ERROR")),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 1, "FAILED").result(err_text("SECOND_ERROR")),
    )
    .await;

    let report = pass(&db.pool, &TestChildSpecs::empty()).await;

    assert_eq!(report.total(), 1, "{report:?}");
    let row = workflow(&db.pool, workflow_id).await;
    assert_eq!(row.status, "FAILED");
    assert_eq!(row.result, None);
    let error = row.error.expect("the workflow stores an error");
    assert!(error.contains("FIRST_ERROR"), "{error}");
    assert!(!error.contains("SECOND_ERROR"), "{error}");
    db.drop().await;
}

/// `test_recover_pending_dep_without_stored_result_injects_the_error`
/// (case 0): a COMPLETED dependency with no stored result gives its
/// `args_from` consumer `RESULT_DESERIALIZATION_ERROR` naming the node, its
/// index, its task and its status, with the dependency index in `data`; the
/// envelope names the source task.
#[tokio::test]
async fn a_dependency_with_no_stored_result_gives_its_consumer_the_missing_result_error() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    let mut source = NodeSeed::task(workflow_id, 0, "COMPLETED");
    source.task_name = "recover_pend_nores_a".to_owned();
    seed_node(&db.pool, &source).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 1, "PENDING")
            .deps(&[0])
            .args_from("input_result", 0),
    )
    .await;

    let report = pass(&db.pool, &TestChildSpecs::empty()).await;

    assert_eq!(report.total(), 1, "{report:?}");
    let input = node_task_input(&db.pool, workflow_id, 1).await;
    assert_eq!(
        input["input_result"]["source_task_name"],
        serde_json::json!("recover_pend_nores_a")
    );
    let injected = args_from_inner(&input, "input_result");
    let error = error_of(&injected);
    assert_eq!(code(error).as_deref(), Some("RESULT_DESERIALIZATION_ERROR"));
    assert_eq!(
        error.message.as_deref(),
        Some(
            "Dependency node node_0 (index 0, task recover_pend_nores_a) \
             is COMPLETED with no stored result"
        )
    );
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({"dependency_index": 0}))
    );
    db.drop().await;
}

/// One phase-2 recovery pass with no grace, as the reaper runs it. Returns
/// the nodes it advanced.
async fn phase2_pass(pool: &TestPool) -> u64 {
    drive_phase2_recovery_with(
        &crate::broker::PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
        &TestChildSpecs::empty(),
        0,
        100,
        25,
    )
    .await
    .expect("the phase-2 pass")
    .applied
}

/// The task of node 0 of `workflow_id` ends FAILED with `WORKER_CRASHED` in
/// phase 1 alone (Python `force_terminal`): the node stays RUNNING and its
/// phase 2 stays owed. The phase-2 record is aged past any grace.
async fn crash_the_task_of_node_0(pool: &TestPool, workflow_id: Uuid) {
    let task_id = node(pool, workflow_id, 0)
        .await
        .task_id
        .expect("node 0 has a task");
    let crashed: TaskResult<serde_json::Value> = TaskResult::Err(TaskError::builtin(
        OperationalErrorCode::WorkerCrashed,
        "Worker died",
    ));
    let _stored = finish_phase1(pool, task_id, &crashed).await;
    assert_eq!(node_status(pool, workflow_id, 0).await, "RUNNING");
    sql::query(
        "UPDATE horsies_workflow_phase2_pending
         SET created_at = NOW() - INTERVAL '1 hour'
         WHERE task_id = $1",
    )
    .bind(task_id)
    .execute(pool)
    .await
    .expect("age the phase-2 record");
}

/// `test_recover_crashed_worker_workflow_task` (case 1.7): a RUNNING node
/// whose task already FAILED with `WORKER_CRASHED` is FAILED by the phase-2
/// pass, and the workflow ends FAILED.
#[tokio::test]
async fn a_crashed_task_of_a_running_node_fails_the_node_and_the_workflow() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = start(
        &db.pool,
        &spec("recover_crash", vec![task_node(0, "recover_crash_a")]),
        &TestChildSpecs::empty(),
    )
    .await;
    crash_the_task_of_node_0(&db.pool, workflow_id).await;

    let applied = phase2_pass(&db.pool).await;

    assert_eq!(applied, 1);
    assert_eq!(node_status(&db.pool, workflow_id, 0).await, "FAILED");
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    db.drop().await;
}

/// `test_recover_crashed_worker_idempotent` (case 1.7): a second phase-2
/// pass after the pass that failed the crashed node advances nothing.
#[tokio::test]
async fn a_second_pass_after_a_crashed_task_advances_nothing() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = start(
        &db.pool,
        &spec(
            "recover_idem_crash",
            vec![task_node(0, "recover_idem_crash_a")],
        ),
        &TestChildSpecs::empty(),
    )
    .await;
    crash_the_task_of_node_0(&db.pool, workflow_id).await;

    let first = phase2_pass(&db.pool).await;
    let second = phase2_pass(&db.pool).await;

    assert_eq!(first, 1);
    assert_eq!(second, 0);
    assert_eq!(node_status(&db.pool, workflow_id, 0).await, "FAILED");
    db.drop().await;
}

/// `test_recover_crashed_worker_dependent_propagation` (case 1.7): after the
/// phase-2 pass fails the crashed node, its dependent is SKIPPED.
#[tokio::test]
async fn a_crashed_task_fails_its_node_and_skips_the_dependent() {
    let db = IsolatedDatabase::create().await;
    let mut dependent = task_node(1, "recover_dep_b");
    dependent.dependencies = vec![0];
    let workflow_id = start(
        &db.pool,
        &spec(
            "recover_dep",
            vec![task_node(0, "recover_dep_a"), dependent],
        ),
        &TestChildSpecs::empty(),
    )
    .await;
    crash_the_task_of_node_0(&db.pool, workflow_id).await;

    let applied = phase2_pass(&db.pool).await;

    assert_eq!(applied, 1);
    assert_eq!(node_status(&db.pool, workflow_id, 0).await, "FAILED");
    assert_eq!(node_status(&db.pool, workflow_id, 1).await, "SKIPPED");
    db.drop().await;
}

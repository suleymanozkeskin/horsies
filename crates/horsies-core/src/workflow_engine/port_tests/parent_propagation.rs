//! The advance of a parent from its finished child: from a child that is
//! already COMPLETED, in a recovery pass where one parent's write fails, and
//! in a phase 2 whose parent write fails once with a connection error.
//! Python: `tests/integration/test_workflow_denest_propagation.py`.
//!
//! Python drives the parent advance after the child's transaction
//! (`_drive_parent_propagations`), with a retry stage of its own. The core
//! advances the parent after the commit that ends the child, one
//! transaction per level (`horsies_workflow_child_finish`, from phase 2 and
//! from recovery case 1.6).

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    advance_parent_of, execute, finish_phase1, node, ok, seed_node, seed_node_task, seed_workflow,
    shared_pool, stored_result, workflow_status, IsolatedDatabase, LiveStatus, NodeSeed,
    ParentLink, WorkflowSeed,
};
use crate::broker::PostgresBroker;
use crate::core::config::retention::RetentionConfig;
use crate::core::{TaskResult, WorkflowSpecRegistry};
use crate::worker::execution::{run_phase2, Phase2Work, PhaseTwoForm};
use crate::workflow_engine::recovery::recover_stuck_workflows;
use crate::workflow_engine::steps::ChildBuild;

/// The definition key that the parent nodes name (the tests resolve no child).
const CHILD_KEY: &str = "tests.port.denest_child.v1";

/// `workflow_recovery_failure_limit` of a recovery pass.
const RECOVERY_FAILURE_LIMIT: u32 = 3;

/// The output value of every child (Python's `_CHILD_RESULT_ENVELOPE`).
fn child_result() -> String {
    serde_json::to_string(&ok(serde_json::json!(7))).expect("encode the child result")
}

/// A RUNNING parent whose node 0 is a RUNNING sub-workflow node of a child
/// that is COMPLETED with [`child_result`] as its output. Returns the parent
/// and the child.
async fn seed_parent_of_a_completed_child(pool: &PgPool) -> (Uuid, Uuid) {
    let parent = seed_workflow(pool, &WorkflowSeed::running()).await;
    let child = seed_workflow(
        pool,
        &WorkflowSeed::child(ParentLink::of_root(parent, 0))
            .status("COMPLETED")
            .output(0)
            .result(child_result()),
    )
    .await;
    seed_node(
        pool,
        &NodeSeed::sub_workflow(parent, 0, "RUNNING", CHILD_KEY, Some(child)),
    )
    .await;
    (parent, child)
}

/// (status of node 0, status of the workflow) of `workflow_id`.
async fn statuses(pool: &PgPool, workflow_id: Uuid) -> (String, String) {
    (
        node(pool, workflow_id, 0).await.status,
        workflow_status(pool, workflow_id).await,
    )
}

fn completed_twice() -> (String, String) {
    ("COMPLETED".to_owned(), "COMPLETED".to_owned())
}

/// `test_driver_advances_parent_from_already_terminal_child`: the child is
/// already COMPLETED; its completion call alone advances the parent node
/// with the child's output and completes the parent.
#[tokio::test]
#[serial]
async fn a_completed_child_advances_and_completes_its_parent() {
    let pool = shared_pool().await;
    let (parent, child) = seed_parent_of_a_completed_child(&pool).await;

    advance_parent_of(
        &pool,
        child,
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await;

    assert_eq!(statuses(&pool, parent).await, completed_twice());
    let parent_node = node(&pool, parent, 0).await;
    match stored_result(
        &parent_node
            .result
            .expect("the node stores the child output"),
    ) {
        TaskResult::Ok(value) => assert_eq!(value, serde_json::json!(7)),
        TaskResult::Err(error) => panic!("the parent node failed: {error:?}"),
    }
}

/// `test_recovery_isolates_poison_child_and_continues`: two parents of
/// COMPLETED children wait for recovery case 1.6. The node write of the
/// first parent is refused; that candidate fails alone, and the pass
/// advances and completes the second parent.
#[tokio::test]
#[serial]
async fn recovery_contains_a_failing_parent_advance_and_completes_the_next_parent() {
    let db = IsolatedDatabase::create().await;
    let (poison_parent, _poison_child) = seed_parent_of_a_completed_child(&db.pool).await;
    let (ok_parent, _ok_child) = seed_parent_of_a_completed_child(&db.pool).await;
    execute(
        &db.pool,
        &format!(
            "CREATE FUNCTION port_test_refuse_parent_write() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
                 IF NEW.workflow_id = '{poison_parent}'::uuid THEN
                     RAISE EXCEPTION 'parent node write refused by the test';
                 END IF;
                 RETURN NEW;
             END
             $body$"
        ),
    )
    .await;
    execute(
        &db.pool,
        "CREATE TRIGGER port_test_refuse_parent_write BEFORE UPDATE ON horsies_workflow_tasks
         FOR EACH ROW EXECUTE FUNCTION port_test_refuse_parent_write()",
    )
    .await;

    let report = recover_stuck_workflows(
        &crate::broker::PostgresBroker::from_pool(db.pool.clone()),
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
        RECOVERY_FAILURE_LIMIT,
        &RetentionConfig::default(),
    )
    .await
    .expect("the recovery pass returns its report");

    assert_eq!(report.case1_6_subworkflow_completed, 1, "{report:?}");
    assert_eq!(report.errors, 1, "{report:?}");
    assert_eq!(
        statuses(&db.pool, poison_parent).await,
        ("RUNNING".to_owned(), "RUNNING".to_owned())
    );
    assert_eq!(statuses(&db.pool, ok_parent).await, completed_twice());
    db.drop().await;
}

/// `test_transient_failure_retries_via_parent_stage_and_completes`: the last
/// task of a child finishes; its phase 2 ends the child and writes the
/// parent node, and that write fails once with SQLSTATE `08006` (a
/// connection failure). Phase 2 is retryable, and its retry ends the child,
/// advances the parent node and completes the parent.
#[tokio::test]
#[serial]
async fn a_transient_failure_of_the_parent_advance_is_retried_and_completes_the_parent() {
    let db = IsolatedDatabase::create().await;
    let parent = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    let child = seed_workflow(
        &db.pool,
        &WorkflowSeed::child(ParentLink::of_root(parent, 0)).output(0),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::sub_workflow(parent, 0, "RUNNING", CHILD_KEY, Some(child)),
    )
    .await;
    let child_task = Uuid::new_v4();
    let _claimed_at = seed_node_task(&db.pool, child_task, LiveStatus::Running).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(child, 0, "RUNNING").task_id(child_task),
    )
    .await;
    let _stored = finish_phase1(&db.pool, child_task, &ok(serde_json::json!(7))).await;
    // A sequence counts the parent node writes: its value is not rolled back
    // with the refused transaction.
    execute(&db.pool, "CREATE SEQUENCE port_test_parent_writes").await;
    execute(
        &db.pool,
        &format!(
            "CREATE FUNCTION port_test_refuse_first_parent_write() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
                 IF NEW.workflow_id = '{parent}'::uuid THEN
                     IF nextval('port_test_parent_writes') = 1 THEN
                         RAISE EXCEPTION 'connection dropped by the test'
                             USING ERRCODE = '08006';
                     END IF;
                 END IF;
                 RETURN NEW;
             END
             $body$"
        ),
    )
    .await;
    execute(
        &db.pool,
        "CREATE TRIGGER port_test_refuse_first_parent_write BEFORE UPDATE ON horsies_workflow_tasks
         FOR EACH ROW EXECUTE FUNCTION port_test_refuse_first_parent_write()",
    )
    .await;

    run_phase2(
        &PostgresBroker::from_pool(db.pool.clone()),
        ChildBuild::Registry {
            registry: &WorkflowSpecRegistry::new(
                crate::core::registry::BuilderHookName::SPEC_BUILDER,
            ),
            retention: &RetentionConfig::default(),
        },
        Phase2Work {
            task_id: child_task,
            queue_name: "default".to_owned(),
            is_workflow_task: true,
            capacity_notified: false,
            phase_two: PhaseTwoForm::NotKnown,
        },
    )
    .await;

    assert_eq!(workflow_status(&db.pool, child).await, "COMPLETED");
    assert_eq!(statuses(&db.pool, parent).await, completed_twice());
    let parent_writes: i64 = sqlx::query_scalar("SELECT last_value FROM port_test_parent_writes")
        .fetch_one(&db.pool)
        .await
        .expect("read the parent write count");
    assert_eq!(parent_writes, 2, "the refused write and its retry");
    let records: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $1",
    )
    .bind(child_task)
    .fetch_one(&db.pool)
    .await
    .expect("count the phase-2 records");
    assert_eq!(records, 0, "the retry consumed the phase-2 record");
    db.drop().await;
}

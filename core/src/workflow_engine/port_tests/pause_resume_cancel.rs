//! Pause, resume and cancel. Python: the cancel and pause/resume classes of
//! `clients/python/tests/integration/test_workflow_handle.py`, and
//! `test_workflow_pause_notification.py`, `test_cascade_resume_bug.py`,
//! `test_resume_dependency_results.py`, and the
//! pause and crash rows of `clients/python/tests/e2e/test_layer6_workflow_advanced.py`.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serial_test::serial;
use uuid::Uuid;

use super::support::{
    args_from_inner, broker, child_of, child_specs_of, claim_once, claim_pass, code, err,
    finish_node, finish_task, handle, history_row, node, node_status, node_task_input, ok,
    seed_node, seed_node_task, seed_workflow, shared_pool, spec, start, stored_result_error,
    sub_workflow_node, task_node, workflow_status, IsolatedDatabase, LiveStatus, NodeSeed,
    ParentLink, WorkflowDoneListener, WorkflowSeed, PORT_QUEUE, WORKER,
};
use crate::broker::terminalization::on_test_pool::terminalize;
use crate::core::lifecycle::outcomes::{GuardEvidence, ObservedWorkflowState};
use crate::core::lifecycle::{
    OwnedClaim, OwnedClaimBatch, TerminalizationCommand, TerminalizationOutcome,
};
use crate::core::workflow::handle_types::HandleErrorCode;
use crate::core::{OnError, TaskResult, WorkflowSpec, WorkflowStatus};
use crate::failure_cause::{FailureCause, ServerErrorFacts};
use crate::test_support::child_specs::TestChildSpecs;
use crate::test_support::sql::{self, TestPool};
use crate::workflow_engine::lifecycle::{cancel_workflow, pause_workflow, resume_client_workflow};
use crate::workflow_engine::phase2_recovery::drive_phase2_recovery_with;

/// How long a test waits to see that a call is blocked on a lock.
const BLOCKED_WAIT: Duration = Duration::from_millis(500);

/// Bound of a wait for a lock that must be free, or for a call to finish.
const LOCK_WAIT: Duration = Duration::from_secs(10);
const CALL_WAIT: Duration = Duration::from_secs(15);

/// A → B: B waits for A.
fn chain(name: &str) -> WorkflowSpec {
    let mut second = task_node(1, &format!("{name}_b"));
    second.dependencies = vec![0];
    spec(name, vec![task_node(0, &format!("{name}_a")), second])
}

fn single(name: &str) -> WorkflowSpec {
    spec(name, vec![task_node(0, &format!("{name}_a"))])
}

async fn resume(pool: &TestPool, workflow_id: Uuid, child_specs: &TestChildSpecs) -> bool {
    resume_client_workflow(&broker(pool), workflow_id, child_specs)
        .await
        .expect("resume")
}

/// `TestWorkflowHandleCancel.test_cancel_skips_pending`
#[tokio::test]
#[serial]
async fn cancel_skips_a_pending_node() {
    let pool = shared_pool().await;
    let workflow_id = start(&pool, &chain("cancel_skip"), &TestChildSpecs::empty()).await;
    let workflow_handle = handle(&pool, workflow_id);

    workflow_handle.cancel().await.expect("cancel()");

    let tasks = workflow_handle.tasks().await.expect("tasks()");
    assert_eq!(
        tasks[1].status,
        crate::core::WorkflowTaskStatus::Skipped,
        "{tasks:?}"
    );
}

/// `TestWorkflowHandleCancel.test_cancel_completed_is_noop`
#[tokio::test]
#[serial]
async fn cancel_of_a_completed_workflow_changes_nothing() {
    let pool = shared_pool().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_id = start(&pool, &single("cancel_noop"), &child_specs).await;
    let _task = finish_node(
        &pool,
        workflow_id,
        0,
        &ok(serde_json::json!(10)),
        &child_specs,
    )
    .await;
    let workflow_handle = handle(&pool, workflow_id);
    assert_eq!(
        workflow_handle.status().await.expect("status()"),
        WorkflowStatus::Completed
    );

    workflow_handle.cancel().await.expect("cancel()");

    assert_eq!(
        workflow_handle.status().await.expect("status()"),
        WorkflowStatus::Completed
    );
}

/// `TestWorkflowHandleCancel.test_descendant_cancel_uses_completion_lock_order`:
/// the cancel of a descendant locks its workflow row before its nodes, so a
/// peer that holds the workflow row and then takes the node does not
/// deadlock with it.
#[tokio::test]
#[serial]
async fn a_descendant_cancel_locks_the_workflow_row_before_its_nodes() {
    let pool = shared_pool().await;
    let parent = start(
        &pool,
        &single("descendant_cancel_lock_order"),
        &TestChildSpecs::empty(),
    )
    .await;
    let child = seed_workflow(&pool, &WorkflowSeed::child(ParentLink::of_root(parent, 0))).await;
    let child_node = seed_node(&pool, &NodeSeed::task(child, 0, "PENDING")).await;

    let mut peer = pool.begin().await.expect("begin the peer");
    let _locked: Uuid =
        sql::query_scalar("SELECT id FROM horsies_workflows WHERE id = $1 FOR UPDATE")
            .bind(child)
            .fetch_one(&mut *peer)
            .await
            .expect("the peer locks the child workflow row");
    let cancel_broker = broker(&pool);
    let cancel = tokio::spawn(async move { cancel_workflow(&cancel_broker, parent).await });
    tokio::time::sleep(BLOCKED_WAIT).await;
    assert!(
        !cancel.is_finished(),
        "the cancel waits on the descendant workflow row"
    );

    let node_lock = tokio::time::timeout(
        LOCK_WAIT,
        sql::query_scalar::<Uuid>("SELECT id FROM horsies_workflow_tasks WHERE id = $1 FOR UPDATE")
            .bind(child_node)
            .fetch_one(&mut *peer),
    )
    .await
    .expect("the cancel holds no lock on the descendant's node");
    let _node: Uuid = node_lock.expect("the peer locks the child node");
    peer.commit().await.expect("the peer commits");

    let cancelled = tokio::time::timeout(CALL_WAIT, cancel)
        .await
        .expect("the cancel finishes")
        .expect("the cancel task does not panic");
    assert!(cancelled.expect("cancel"));
    assert_eq!(workflow_status(&pool, child).await, "CANCELLED");
}

/// `TestWorkflowHandleCancel.test_cancel_leaves_no_pending_ready_enqueued_workflow_tasks`
#[tokio::test]
#[serial]
async fn cancel_leaves_no_queueable_node_and_moves_the_root_task_to_history() {
    let pool = shared_pool().await;
    let workflow_id = start(
        &pool,
        &chain("cancel_guarantee_states"),
        &TestChildSpecs::empty(),
    )
    .await;
    let root_task = node(&pool, workflow_id, 0)
        .await
        .task_id
        .expect("the root has its task");

    handle(&pool, workflow_id).cancel().await.expect("cancel()");

    let statuses: Vec<String> =
        sql::query_scalar("SELECT status FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .fetch_all(&pool)
            .await
            .expect("read the node statuses");
    assert!(!statuses.is_empty());
    for status in &statuses {
        assert!(
            !matches!(status.as_str(), "PENDING" | "READY" | "ENQUEUED"),
            "{statuses:?}"
        );
    }
    assert_eq!(node_status(&pool, workflow_id, 0).await, "SKIPPED");
    let (history_status, _kind) = history_row(&pool, root_task)
        .await
        .expect("the cancelled task moved to history");
    assert_eq!(history_status, "CANCELLED");
}

/// `TestWorkflowHandleCancel.test_cancel_marks_claimed_not_started_task_not_claimable`
#[tokio::test]
#[serial]
async fn cancel_moves_a_claimed_task_that_did_not_start_to_history() {
    let pool = shared_pool().await;
    let workflow_id = start(
        &pool,
        &single("cancel_guarantee_claimed"),
        &TestChildSpecs::empty(),
    )
    .await;
    let task_id = node(&pool, workflow_id, 0)
        .await
        .task_id
        .expect("the root has its task");
    sql::query(
        "UPDATE horsies_tasks
         SET status = 'CLAIMED', claimed_at = NOW(), claimed_by_worker_id = 'test-worker',
             claim_expires_at = NOW() + INTERVAL '5 minutes', updated_at = NOW()
         WHERE id = $1",
    )
    .bind(task_id)
    .execute(&pool)
    .await
    .expect("claim the task");

    handle(&pool, workflow_id).cancel().await.expect("cancel()");

    let live: bool = sql::query_scalar("SELECT EXISTS (SELECT 1 FROM horsies_tasks WHERE id = $1)")
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .expect("read the live row");
    assert!(!live, "nothing claimable stays in the live table");
    assert_eq!(
        history_row(&pool, task_id).await,
        Some((
            "CANCELLED".to_owned(),
            "WORKFLOW_CANCEL_WORKFLOW".to_owned()
        ))
    );
    assert_eq!(node_status(&pool, workflow_id, 0).await, "SKIPPED");
}

/// `TestWorkflowHandleCancel.test_cancel_locks_backing_task_before_workflow_status_update`:
/// while the cancel waits in its status write, a claim cannot take the task.
#[tokio::test]
#[serial]
async fn a_claim_cannot_take_a_task_while_its_cancel_is_uncommitted() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = start(
        &db.pool,
        &single("cancel_claim_race"),
        &TestChildSpecs::empty(),
    )
    .await;
    let task_id = node(&db.pool, workflow_id, 0)
        .await
        .task_id
        .expect("the root has its task");
    super::support::execute(
        &db.pool,
        &format!(
            "CREATE FUNCTION port_test_sleep_on_cancel() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
                 IF NEW.status = 'CANCELLED' AND NEW.id = '{workflow_id}'::uuid THEN
                     PERFORM pg_sleep(0.5);
                 END IF;
                 RETURN NEW;
             END
             $body$"
        ),
    )
    .await;
    super::support::execute(
        &db.pool,
        "CREATE TRIGGER port_test_sleep_on_cancel BEFORE UPDATE OF status ON horsies_workflows
         FOR EACH ROW EXECUTE FUNCTION port_test_sleep_on_cancel()",
    )
    .await;

    let cancel_broker = broker(&db.pool);
    let cancel = tokio::spawn(async move { cancel_workflow(&cancel_broker, workflow_id).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let claimed = claim_once(&db.pool, &claim_pass(PORT_QUEUE, 10)).await;

    let cancelled = tokio::time::timeout(CALL_WAIT, cancel)
        .await
        .expect("the cancel finishes")
        .expect("the cancel task does not panic");
    assert!(cancelled.expect("cancel"));
    assert!(
        claimed.iter().all(|row| row.id != task_id),
        "the claim took a task of the workflow being cancelled"
    );
    db.drop().await;
}

/// `TestWorkflowHandleCancel.test_cancel_nonexistent_returns_not_found`
#[tokio::test]
#[serial]
async fn cancel_of_an_unknown_id_is_workflow_not_found() {
    let pool = shared_pool().await;

    let error = handle(&pool, Uuid::new_v4())
        .cancel()
        .await
        .expect_err("an unknown id cannot be cancelled");

    assert_eq!(error.code(), HandleErrorCode::WorkflowNotFound);
    assert!(!error.retryable());
}

/// `TestWorkflowHandlePauseResume.test_pause_non_running_returns_false`
#[tokio::test]
#[serial]
async fn pause_of_a_completed_workflow_returns_false() {
    let pool = shared_pool().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_id = start(&pool, &single("pause_completed"), &child_specs).await;
    let _task = finish_node(
        &pool,
        workflow_id,
        0,
        &ok(serde_json::json!(10)),
        &child_specs,
    )
    .await;

    let paused = handle(&pool, workflow_id).pause().await.expect("pause()");

    assert!(!paused);
    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestWorkflowHandlePauseResume.test_pause_nonexistent_returns_not_found`
/// and `.test_resume_nonexistent_returns_not_found`
#[tokio::test]
#[serial]
async fn pause_and_resume_of_an_unknown_id_are_workflow_not_found() {
    let pool = shared_pool().await;
    let workflow_handle = handle(&pool, Uuid::new_v4());

    let paused = workflow_handle
        .pause()
        .await
        .expect_err("pause of an unknown id");
    let resumed = workflow_handle
        .resume(&TestChildSpecs::empty())
        .await
        .expect_err("resume of an unknown id");

    assert_eq!(paused.code(), HandleErrorCode::WorkflowNotFound);
    assert_eq!(resumed.code(), HandleErrorCode::WorkflowNotFound);
}

/// `TestResumeCommitSplit.test_db_error_before_the_commit_returns_db_operation_failed`:
/// a database error after the status change and before the commit rolls the
/// whole resume back.
#[tokio::test]
#[serial]
async fn a_database_error_before_the_resume_commit_keeps_the_workflow_paused() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running().status("PAUSED")).await;
    let done = serde_json::to_string(&ok(serde_json::json!(1))).expect("encode the result");
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "COMPLETED").result(done),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 1, "PENDING").deps(&[0]),
    )
    .await;
    super::support::execute(
        &db.pool,
        &format!(
            "CREATE FUNCTION port_test_refuse_resume() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
                 IF NEW.workflow_id = '{workflow_id}'::uuid THEN
                     RAISE EXCEPTION 'refused by the test' USING ERRCODE = '57P01';
                 END IF;
                 RETURN NEW;
             END
             $body$"
        ),
    )
    .await;
    super::support::execute(
        &db.pool,
        "CREATE TRIGGER port_test_refuse_resume BEFORE UPDATE ON horsies_workflow_tasks
         FOR EACH ROW EXECUTE FUNCTION port_test_refuse_resume()",
    )
    .await;

    let error = handle(&db.pool, workflow_id)
        .resume(&TestChildSpecs::empty())
        .await
        .expect_err("the resume meets the refused node write");

    assert_eq!(error.code(), HandleErrorCode::DbOperationFailed);
    assert!(error.retryable());
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "PAUSED");
    assert_eq!(node_status(&db.pool, workflow_id, 1).await, "PENDING");
    db.drop().await;
}

/// `TestResumeCommitSplit.test_recovery_db_error_returns_resume_recovery_failed`:
/// a database error that only the recovery pass after the commit meets. The
/// resume committed: `RESUME_RECOVERY_FAILED`, not retryable, and the
/// workflow is RUNNING with B enqueued.
///
/// A deferred constraint trigger on the status change PAUSED -> RUNNING runs
/// at the resume's `COMMIT` and sets the session's `search_path` to a schema
/// with no table. The setting outlives the commit, so the next statement on
/// that connection, the tree read of the recovery pass, fails with 42P01.
/// The handle's broker is new and its calls run one at a time, so its main
/// core pool holds one connection, which the resume and the recovery pass
/// both take.
#[tokio::test]
#[serial]
async fn a_database_error_after_the_resume_commit_is_resume_recovery_failed() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running().status("PAUSED")).await;
    let done = serde_json::to_string(&ok(serde_json::json!(1))).expect("encode the result");
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "COMPLETED").result(done),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 1, "PENDING").deps(&[0]),
    )
    .await;
    super::support::execute(
        &db.pool,
        "CREATE FUNCTION port_test_lose_search_path() RETURNS trigger
         LANGUAGE plpgsql AS $body$
         BEGIN
             PERFORM set_config('search_path', 'port_test_no_tables', false);
             RETURN NULL;
         END
         $body$",
    )
    .await;
    super::support::execute(
        &db.pool,
        "CREATE CONSTRAINT TRIGGER port_test_lose_search_path
         AFTER UPDATE ON horsies_workflows
         DEFERRABLE INITIALLY DEFERRED
         FOR EACH ROW
         WHEN (OLD.status = 'PAUSED' AND NEW.status = 'RUNNING')
         EXECUTE FUNCTION port_test_lose_search_path()",
    )
    .await;

    let error = handle(&db.pool, workflow_id)
        .resume(&TestChildSpecs::empty())
        .await
        .expect_err("the recovery pass after the commit meets the lost search path");

    assert_eq!(error.code(), HandleErrorCode::ResumeRecoveryFailed);
    assert!(!error.retryable());
    assert_eq!(error.workflow_id(), workflow_id);
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "RUNNING");
    assert_eq!(node_status(&db.pool, workflow_id, 1).await, "ENQUEUED");
    db.drop().await;
}

/// `TestResumeCommitSplit.test_a_failed_parent_advance_returns_resume_recovery_failed`:
/// the resume committed and ended a child workflow, and the advance of the
/// parent node after the commit failed. `RESUME_RECOVERY_FAILED`, not
/// retryable, with the error of the advance as its cause; the child stays
/// COMPLETED, and the recovery pass does not run.
///
/// The resumed child has one COMPLETED node, so the resume completes it and
/// owes the advance of its parent node. A trigger refuses every update of a
/// node of the parent and counts each refusal in a sequence (a rollback does
/// not reset it). The recovery pass of the child's tree writes the same
/// parent node (case 1.6: a terminal child under a RUNNING parent node), so
/// one refusal shows that the advance ran and the recovery pass did not.
///
/// The message names the parent advance and the child whose advance failed.
#[tokio::test]
#[serial]
async fn a_failed_parent_advance_after_the_resume_commit_is_resume_recovery_failed() {
    const REFUSAL: &str = "parent advance refused by the test";
    let db = IsolatedDatabase::create().await;
    let parent = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    let child = seed_workflow(
        &db.pool,
        &WorkflowSeed::child(ParentLink::of_root(parent, 0)).status("PAUSED"),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::sub_workflow(parent, 0, "RUNNING", "tests.port.child.v1", Some(child)),
    )
    .await;
    let done = serde_json::to_string(&ok(serde_json::json!(1))).expect("encode the result");
    seed_node(
        &db.pool,
        &NodeSeed::task(child, 0, "COMPLETED").result(done),
    )
    .await;
    super::support::execute(
        &db.pool,
        "CREATE SEQUENCE port_test_parent_advance_refusals",
    )
    .await;
    super::support::execute(
        &db.pool,
        &format!(
            "CREATE FUNCTION port_test_refuse_parent_advance() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
                 IF NEW.workflow_id = '{parent}'::uuid THEN
                     PERFORM nextval('port_test_parent_advance_refusals');
                     RAISE EXCEPTION '{REFUSAL}' USING ERRCODE = 'P0001';
                 END IF;
                 RETURN NEW;
             END
             $body$"
        ),
    )
    .await;
    super::support::execute(
        &db.pool,
        "CREATE TRIGGER port_test_refuse_parent_advance BEFORE UPDATE ON horsies_workflow_tasks
         FOR EACH ROW EXECUTE FUNCTION port_test_refuse_parent_advance()",
    )
    .await;

    let error = handle(&db.pool, child)
        .resume(&TestChildSpecs::empty())
        .await
        .expect_err("the parent advance after the commit meets the refused node write");

    assert_eq!(error.code(), HandleErrorCode::ResumeRecoveryFailed);
    assert!(!error.retryable());
    assert_eq!(error.workflow_id(), child);
    assert_eq!(
        error.failure_cause(),
        FailureCause::Server(ServerErrorFacts::of_code("P0001", REFUSAL.to_owned()))
    );
    let message = error.message();
    let expected_start =
        format!("Workflow {child} resumed; the parent advance of child workflow {child} failed: ");
    assert!(message.starts_with(&expected_start), "{message}");
    assert!(message.contains(REFUSAL), "{message}");
    assert_eq!(workflow_status(&db.pool, child).await, "COMPLETED");
    assert_eq!(node_status(&db.pool, parent, 0).await, "RUNNING");
    assert_eq!(workflow_status(&db.pool, parent).await, "RUNNING");
    let refusals: i64 = sql::query_scalar(
        "SELECT CASE WHEN is_called THEN last_value ELSE 0 END
         FROM port_test_parent_advance_refusals",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read the refusal count");
    assert_eq!(
        refusals, 1,
        "the advance ran once, and no recovery pass wrote the parent node"
    );
    db.drop().await;
}

/// A RUNNING parent with one running child, both with no node.
async fn seed_parent_with_child(pool: &TestPool, on_error: &'static str) -> (Uuid, Uuid) {
    let parent = seed_workflow(pool, &WorkflowSeed::running().on_error(on_error)).await;
    let child = seed_workflow(pool, &WorkflowSeed::child(ParentLink::of_root(parent, 0))).await;
    (parent, child)
}

fn sorted(mut ids: Vec<String>) -> Vec<String> {
    ids.sort();
    ids
}

/// `test_workflow_pause_notification.py::test_handle_pause_notifies_each_paused_workflow_through_the_trigger`:
/// one `workflow_done` per paused workflow. (The Python test also checks that
/// the pause path sends no `pg_notify` of its own; that is #551.)
#[tokio::test]
#[serial]
async fn a_handle_pause_notifies_each_paused_workflow_once() {
    let db = IsolatedDatabase::create().await;
    let (parent, child) = seed_parent_with_child(&db.pool, "fail").await;
    let mut listener = WorkflowDoneListener::listen(&db.pool).await;

    let paused = pause_workflow(&broker(&db.pool), parent)
        .await
        .expect("pause");

    assert!(paused);
    assert_eq!(
        sorted(listener.received().await),
        sorted(vec![parent.to_string(), child.to_string()])
    );
    assert_eq!(workflow_status(&db.pool, parent).await, "PAUSED");
    assert_eq!(workflow_status(&db.pool, child).await, "PAUSED");
    db.drop().await;
}

/// `test_workflow_pause_notification.py::test_on_error_pause_notifies_each_paused_workflow_after_commit`:
/// a node failure under `on_error = pause` pauses the tree and notifies each
/// paused workflow once.
#[tokio::test]
#[serial]
async fn an_on_error_pause_notifies_each_paused_workflow_once() {
    let db = IsolatedDatabase::create().await;
    let (parent, child) = seed_parent_with_child(&db.pool, "pause").await;
    let failing_task = Uuid::new_v4();
    let _claimed_at = seed_node_task(&db.pool, failing_task, LiveStatus::Running).await;
    seed_node(
        &db.pool,
        &NodeSeed::sub_workflow(parent, 0, "RUNNING", "tests.port.child.v1", Some(child)),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(parent, 1, "RUNNING").task_id(failing_task),
    )
    .await;
    let mut listener = WorkflowDoneListener::listen(&db.pool).await;

    finish_task(
        &db.pool,
        failing_task,
        &err("NODE_FAILED", "node failed"),
        &TestChildSpecs::empty(),
    )
    .await;

    assert_eq!(
        sorted(listener.received().await),
        sorted(vec![parent.to_string(), child.to_string()])
    );
    assert_eq!(workflow_status(&db.pool, parent).await, "PAUSED");
    assert_eq!(workflow_status(&db.pool, child).await, "PAUSED");
    db.drop().await;
}

/// A PAUSED workflow whose node 0 is ENQUEUED and holds a task that
/// [`WORKER`] claimed. Returns the workflow, the task and its claim generation.
async fn seed_paused_workflow_with_claimed_node(pool: &TestPool) -> (Uuid, Uuid, DateTime<Utc>) {
    let workflow_id = seed_workflow(pool, &WorkflowSeed::running().status("PAUSED")).await;
    let task_id = Uuid::new_v4();
    let claimed_at = seed_node_task(pool, task_id, LiveStatus::Claimed)
        .await
        .expect("a CLAIMED row has a claim generation");
    seed_node(
        pool,
        &NodeSeed::task(workflow_id, 0, "ENQUEUED").task_id(task_id),
    )
    .await;
    (workflow_id, task_id, claimed_at)
}

/// (node status, node task, workflow status, live task status)
async fn node_and_task(
    pool: &TestPool,
    workflow_id: Uuid,
    task_id: Uuid,
) -> (String, Option<Uuid>, String, Option<String>) {
    let row = node(pool, workflow_id, 0).await;
    let live: Option<String> = sql::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the live task");
    (
        row.status,
        row.task_id,
        workflow_status(pool, workflow_id).await,
        live,
    )
}

fn assert_refused_by_a_running_workflow(
    outcomes: &[TerminalizationOutcome],
    task_id: Uuid,
    workflow_id: Uuid,
) {
    match outcomes {
        [TerminalizationOutcome::SourceStateConflict {
            task_id: refused,
            evidence:
                GuardEvidence::WorkflowState(ObservedWorkflowState {
                    workflow_id: observed_workflow,
                    workflow_status,
                }),
            ..
        }] => {
            assert_eq!(*refused, task_id);
            assert_eq!(*observed_workflow, workflow_id);
            assert_eq!(workflow_status, "RUNNING");
        }
        other => panic!("expected a workflow-status refusal, got {other:?}"),
    }
}

/// A claim whose workflow was resumed is dispatched; its node and task stay.
/// The core settles the claim check in one statement, so a resume commits
/// before or after it, never between the read and the abandon.
#[tokio::test]
#[serial]
async fn the_claim_filter_dispatches_a_claim_whose_workflow_was_resumed() {
    let db = IsolatedDatabase::create().await;
    let (workflow_id, task_id, claimed_at) = seed_paused_workflow_with_claimed_node(&db.pool).await;
    assert!(resume(&db.pool, workflow_id, &TestChildSpecs::empty()).await);

    let stopped = broker(&db.pool)
        .filter_non_runnable_workflow_tasks(&[(task_id, claimed_at)], WORKER)
        .await
        .expect("the claim filter");

    assert!(!stopped.contains(&task_id), "{stopped:?}");
    assert_eq!(
        node_and_task(&db.pool, workflow_id, task_id).await,
        (
            "ENQUEUED".to_owned(),
            Some(task_id),
            "RUNNING".to_owned(),
            Some("CLAIMED".to_owned()),
        )
    );
    db.drop().await;
}

#[tokio::test]
#[serial]
async fn the_batch_abandon_refuses_after_a_committed_resume() {
    let db = IsolatedDatabase::create().await;
    let (workflow_id, task_id, claimed_at) = seed_paused_workflow_with_claimed_node(&db.pool).await;
    assert!(resume(&db.pool, workflow_id, &TestChildSpecs::empty()).await);

    let outcomes = terminalize(
        &db.pool,
        &TerminalizationCommand::AbandonOwnedNodes {
            fence: OwnedClaimBatch::new(WORKER.to_owned(), vec![(task_id, claimed_at)])
                .expect("one claim"),
        },
    )
    .await
    .expect("the batch abandon");

    assert_refused_by_a_running_workflow(&outcomes, task_id, workflow_id);
    assert_eq!(
        node_and_task(&db.pool, workflow_id, task_id).await,
        (
            "ENQUEUED".to_owned(),
            Some(task_id),
            "RUNNING".to_owned(),
            Some("CLAIMED".to_owned()),
        )
    );
    db.drop().await;
}

/// The start-time abandon of one node refuses after a
/// committed resume, so the start can run the task. The start half is
/// `worker::executor::child_host::start_tests::a_resume_before_the_abandon_starts_the_task_on_the_next_attempt`.
#[tokio::test]
#[serial]
async fn the_start_abandon_refuses_after_a_committed_resume() {
    let db = IsolatedDatabase::create().await;
    let (workflow_id, task_id, claimed_at) = seed_paused_workflow_with_claimed_node(&db.pool).await;
    assert!(resume(&db.pool, workflow_id, &TestChildSpecs::empty()).await);

    let outcomes = terminalize(
        &db.pool,
        &TerminalizationCommand::AbandonOwnedNode {
            task_id,
            fence: OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at,
            },
        },
    )
    .await
    .expect("the start abandon");

    assert_refused_by_a_running_workflow(&outcomes, task_id, workflow_id);
    assert_eq!(
        node_and_task(&db.pool, workflow_id, task_id).await.3,
        Some("CLAIMED".to_owned())
    );
    db.drop().await;
}

/// The child of the cascade-resume test: C → D, output D, `on_error = pause`.
fn pausing_child() -> WorkflowSpec {
    let mut second = task_node(1, "pausing_child_d");
    second.dependencies = vec![0];
    let mut child = spec(
        "pausing_child_workflow",
        vec![task_node(0, "pausing_child_c"), second],
    );
    child.output_index = Some(1);
    child.on_error = OnError::Pause;
    child
}

/// `test_cascade_resume_bug.py::test_cascade_resume_child_all_terminal_completes`:
/// a resume cascades to the paused child, skips its pending node and
/// finalizes it FAILED.
#[tokio::test]
#[serial]
async fn a_cascade_resume_finalizes_a_child_whose_nodes_all_ended() {
    let pool = shared_pool().await;
    let child_specs = child_specs_of(&[pausing_child()]);
    let mut parent = spec(
        "cascade_resume_parent",
        vec![
            task_node(0, "cascade_parent_task_a"),
            sub_workflow_node(1, &pausing_child()),
        ],
    );
    parent.output_index = Some(1);
    parent.on_error = OnError::Pause;
    let parent_id = start(&pool, &parent, &child_specs).await;
    let child_id = child_of(&pool, parent_id, 1).await;

    let _c = finish_node(
        &pool,
        child_id,
        0,
        &err("CHILD_FAIL", "C failed"),
        &child_specs,
    )
    .await;
    assert_eq!(workflow_status(&pool, child_id).await, "PAUSED");
    let _a = finish_node(
        &pool,
        parent_id,
        0,
        &err("PARENT_FAIL", "A failed"),
        &child_specs,
    )
    .await;
    assert_eq!(workflow_status(&pool, parent_id).await, "PAUSED");

    assert!(resume(&pool, parent_id, &child_specs).await);

    assert_eq!(node_status(&pool, child_id, 1).await, "SKIPPED");
    assert_eq!(workflow_status(&pool, child_id).await, "FAILED");
    super::support::end_workflows(&pool, &[parent_id]).await;
}

/// `test_resume_dependency_results.py::test_resume_injects_real_dependency_results`:
/// a node re-enqueued by resume gets its dependency's stored result.
#[tokio::test]
#[serial]
async fn a_resumed_node_receives_its_dependency_result() {
    let db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let mut consumer = task_node(1, "resume_dep_consumer");
    consumer.dependencies = vec![0];
    consumer.args_from.insert("value".to_owned(), 0);
    let mut workflow_spec = spec(
        "resume_dep_results",
        vec![task_node(0, "resume_dep_producer"), consumer],
    );
    workflow_spec.output_index = Some(1);
    let workflow_id = start(&db.pool, &workflow_spec, &child_specs).await;
    let _producer = finish_node(
        &db.pool,
        workflow_id,
        0,
        &ok(serde_json::json!(42)),
        &child_specs,
    )
    .await;
    sql::query(
        "UPDATE horsies_tasks
         SET status = 'CLAIMED', claimed_at = NOW(), claimed_by_worker_id = 'w-resume-test',
             claim_expires_at = NOW() + INTERVAL '60 seconds', updated_at = NOW()
         WHERE id = (SELECT task_id FROM horsies_workflow_tasks
                     WHERE workflow_id = $1 AND task_index = 1)",
    )
    .bind(workflow_id)
    .execute(&db.pool)
    .await
    .expect("claim the consumer's task");

    assert!(handle(&db.pool, workflow_id)
        .pause()
        .await
        .expect("pause()"));
    assert_eq!(node_status(&db.pool, workflow_id, 1).await, "READY");
    assert!(resume(&db.pool, workflow_id, &child_specs).await);

    let input = node_task_input(&db.pool, workflow_id, 1).await;
    match args_from_inner(&input, "value") {
        TaskResult::Ok(value) => assert_eq!(value, serde_json::json!(42)),
        TaskResult::Err(error) => panic!("the resumed node got an error: {error:?}"),
    }
    db.drop().await;
}

/// `clients/python/tests/e2e/test_layer6_workflow_advanced.py::test_pause_then_cancel`
#[tokio::test]
#[serial]
async fn a_paused_workflow_cancels_and_skips_its_pending_node() {
    let pool = shared_pool().await;
    let workflow_id = start(&pool, &chain("pause_then_cancel"), &TestChildSpecs::empty()).await;
    let workflow_handle = handle(&pool, workflow_id);
    assert!(workflow_handle.pause().await.expect("pause()"));

    workflow_handle.cancel().await.expect("cancel()");

    assert_eq!(workflow_status(&pool, workflow_id).await, "CANCELLED");
    assert_eq!(node_status(&pool, workflow_id, 1).await, "SKIPPED");
}

/// `clients/python/tests/e2e/test_layer6_workflow_advanced.py::test_workflow_recovers_after_worker_crash`:
/// the reaper fails the two running nodes of a crashed worker with
/// `WORKER_CRASHED`; the `allow_failed_deps` node and its dependent then run
/// on their own queue, and the workflow ends FAILED.
#[tokio::test]
#[serial]
async fn a_crashed_worker_fails_its_nodes_and_allow_failed_deps_nodes_run() {
    let db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let done = serde_json::to_string(&ok(serde_json::json!(1))).expect("encode the result");
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "COMPLETED").result(done.clone()),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 1, "COMPLETED")
            .deps(&[0])
            .result(done),
    )
    .await;
    let mut crashed = Vec::new();
    for task_index in [2, 3] {
        let task_id = Uuid::new_v4();
        let _claimed_at = seed_node_task(&db.pool, task_id, LiveStatus::Running).await;
        seed_node(
            &db.pool,
            &NodeSeed::task(workflow_id, task_index, "RUNNING")
                .deps(&[1])
                .task_id(task_id),
        )
        .await;
        crashed.push(task_id);
    }
    sql::query(
        "UPDATE horsies_tasks SET started_at = NOW() - INTERVAL '1 hour' WHERE id = ANY($1)",
    )
    .bind(&crashed)
    .execute(&db.pool)
    .await
    .expect("age the crashed tasks");
    let mut recovery_node = NodeSeed::task(workflow_id, 4, "PENDING")
        .deps(&[2, 3])
        .allow_failed_deps();
    recovery_node.queue_name = "recovery";
    seed_node(&db.pool, &recovery_node).await;
    let mut after = NodeSeed::task(workflow_id, 5, "PENDING").deps(&[4]);
    after.queue_name = "recovery";
    seed_node(&db.pool, &after).await;

    let failed = crate::worker::recovery::on_test_pool::mark_stale_running_as_failed(
        &db.pool, 10.0, 10.0, 100,
    )
    .await
    .expect("the stale pass");
    assert_eq!(failed, 2);
    let _summary = drive_phase2_recovery_with(
        &crate::broker::PostgresBroker::from_test_url(db.pool.url(), crate::ClientFamily::Rust),
        &child_specs,
        0,
        100,
        3,
    )
    .await
    .expect("phase-2 recovery");

    for task_index in [2, 3] {
        let row = node(&db.pool, workflow_id, task_index).await;
        assert_eq!(row.status, "FAILED");
        let error = stored_result_error(&row.result.expect("the node stores its failure"));
        assert_eq!(code(&error).as_deref(), Some("WORKER_CRASHED"));
    }
    for task_index in [4, 5] {
        let task_queue: String = sql::query_scalar(
            "SELECT t.queue_name FROM horsies_tasks t
             JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
             WHERE wt.workflow_id = $1 AND wt.task_index = $2",
        )
        .bind(workflow_id)
        .bind(task_index)
        .fetch_one(&db.pool)
        .await
        .expect("the node has its task");
        assert_eq!(task_queue, "recovery");
        let _task = finish_node(
            &db.pool,
            workflow_id,
            task_index,
            &ok(serde_json::json!(task_index)),
            &child_specs,
        )
        .await;
    }
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    db.drop().await;
}

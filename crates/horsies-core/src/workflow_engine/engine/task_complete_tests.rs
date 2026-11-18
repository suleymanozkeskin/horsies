//! Tests of `on_workflow_task_complete`: phase 2 of a finished workflow task
//! through `horsies_workflow_node_finish` (database, needs
//! `0005_workflow_functions.sql`). One test per outcome of its catalog that a
//! seed can reach: `Applied`, `AppliedWorkflowEnded` (CANCELLED and FAILED),
//! `AlreadyApplied`, `PendingAbsent`, `RecordKept` and `NodeFinish`. Two
//! more `Applied` cases: the error that a pausing workflow records, and a
//! record whose evidence is in quarantine.

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use crate::broker::PostgresBroker;
use crate::core::lifecycle::TerminalizationOutcome;
use crate::core::{TaskError, TaskResult};
use crate::test_support::child_specs::TestChildSpecs;
use crate::test_support::terminal_writes::{self, TerminalFailure};
use crate::workflow_engine::engine::{
    on_workflow_task_complete, SourceFailure, WorkflowTaskCompletion, WorkflowTaskCompletionError,
};
use crate::workflow_engine::lifecycle::cancel_workflow;

const WORKER: &str = "task-complete-worker";

async fn test_broker() -> PostgresBroker {
    PostgresBroker::from_pool(
        crate::broker::terminalization_matrix::migrated_pool().await,
        crate::ClientFamily::Rust,
    )
}

/// A RUNNING workflow with node 0 RUNNING behind a RUNNING task claimed by
/// [`WORKER`] (with the process facts of its start), and node 1, a task node
/// that waits for node 0, PENDING.
async fn seed_chain(pool: &PgPool, workflow_id: Uuid, task_id: Uuid) {
    sqlx::query(
        "INSERT INTO horsies_workflows (
             id, name, status, on_error, definition_key, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'task_complete', 'RUNNING', 'fail', $2, 0, $1,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .bind(format!("tests.task_complete.{workflow_id}"))
    .execute(pool)
    .await
    .expect("seed workflow");
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, status, sent_at, enqueued_at,
             started_at, claimed_at, claimed_by_worker_id, is_workflow_task, retry_count,
             max_retries, command_fingerprint_version, command_fingerprint,
             retention_class_key, retain_rerun_input, created_at, updated_at, execution,
             worker_pid, worker_hostname, worker_process_name
         ) VALUES (
             $1, 'task_complete_root', 'default', 100, '{}', 'RUNNING', NOW(), NOW(), NOW(),
             NOW(), $2, TRUE, 0, 0, NULL, NULL, 'standard_30d', FALSE, NOW(), NOW(),
             'ISOLATED', 4242, 'task-complete-host', 'task-complete-process'
         )",
    )
    .bind(task_id)
    .bind(WORKER)
    .execute(pool)
    .await
    .expect("seed task");
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, status,
             is_subworkflow, task_id, created_at, started_at, max_retries, execution,
             retention_class_key
         ) VALUES (
             'NONE', $1, $2, 0, 'node_0', 'task_complete_root', '{}', 'default', 100, '{}',
             FALSE, 'all', 'RUNNING', FALSE, $3, NOW(), NOW(), 0, 'ISOLATED', 'standard_30d'
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(task_id)
    .execute(pool)
    .await
    .expect("seed task node");
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, status,
             is_subworkflow, created_at, max_retries, execution, retention_class_key
         ) VALUES (
             'NONE', $1, $2, 1, 'node_1', 'task_complete_dependent', '{}', 'default', 100,
             '{0}', FALSE, 'all', 'PENDING', FALSE, NOW(), 0, 'ISOLATED', 'standard_30d'
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .execute(pool)
    .await
    .expect("seed dependent");
}

/// Completes the task: phase 1 of a worker, which writes its phase-2 record.
async fn complete_task(pool: &PgPool, task_id: Uuid) {
    let result = serde_json::to_string(&TaskResult::Ok(serde_json::json!({"value": 7}))).unwrap();
    let outcomes = terminal_writes::complete_workflow_task(pool, task_id, WORKER, &result)
        .await
        .expect("terminalize the workflow task");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { .. }]
    ));
}

/// Per node, in index order: the index, the status, whether a task is linked,
/// and whether a result is stored.
async fn nodes(pool: &PgPool, workflow_id: Uuid) -> Vec<(i32, String, bool, bool)> {
    sqlx::query_as(
        "SELECT task_index, status, task_id IS NOT NULL, result IS NOT NULL
         FROM horsies_workflow_tasks WHERE workflow_id = $1 ORDER BY task_index",
    )
    .bind(workflow_id)
    .fetch_all(pool)
    .await
    .expect("read the nodes")
}

async fn workflow_status(pool: &PgPool, workflow_id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .fetch_one(pool)
        .await
        .expect("read the workflow status")
}

async fn pending_records(pool: &PgPool, task_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("count the phase-2 records")
}

async fn complete(
    broker: &PostgresBroker,
    task_id: Uuid,
) -> Result<WorkflowTaskCompletion, WorkflowTaskCompletionError> {
    on_workflow_task_complete(broker, task_id, &TestChildSpecs::empty()).await
}

/// A live workflow: the node takes COMPLETED with the stored result, the
/// record is deleted, and the dependent is promoted with a task row. A second
/// call is a replay that writes nothing.
#[tokio::test]
#[serial]
async fn a_node_of_a_live_workflow_completes_and_promotes_its_dependent() {
    let broker = test_broker().await;
    let pool = broker.pool().clone();
    let workflow_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    seed_chain(&pool, workflow_id, task_id).await;
    complete_task(&pool, task_id).await;

    let completion = complete(&broker, task_id).await.expect("phase 2");

    assert_eq!(completion, WorkflowTaskCompletion::Applied);
    assert_eq!(
        nodes(&pool, workflow_id).await,
        vec![
            (0, "COMPLETED".to_owned(), true, true),
            (1, "ENQUEUED".to_owned(), true, false),
        ]
    );
    assert_eq!(pending_records(&pool, task_id).await, 0);
    assert_eq!(workflow_status(&pool, workflow_id).await, "RUNNING");

    let replay = complete(&broker, task_id).await.expect("replay");
    assert_eq!(replay, WorkflowTaskCompletion::AlreadyApplied);
}

/// The workflow ended after phase 1 and before phase 2. Phase 2 writes the
/// node and deletes the record; the ended workflow promotes nothing: the
/// dependent keeps the status that the end left it (a cancel skips a PENDING
/// node; a failure leaves it PENDING), and the workflow keeps its status.
/// Python `tests/integration/test_workflow_node_finalize.py::
/// test_a_node_of_an_ended_workflow_completes_and_enqueues_nothing`.
#[tokio::test]
#[serial]
async fn a_node_of_an_ended_workflow_completes_and_enqueues_nothing() {
    for ended_as in ["CANCELLED", "FAILED"] {
        let broker = test_broker().await;
        let pool = broker.pool().clone();
        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        seed_chain(&pool, workflow_id, task_id).await;
        complete_task(&pool, task_id).await;
        match ended_as {
            "CANCELLED" => assert!(cancel_workflow(&broker, workflow_id)
                .await
                .expect("cancel the workflow")),
            _ => {
                sqlx::query(
                    "UPDATE horsies_workflows
                     SET status = 'FAILED', completed_at = NOW(), updated_at = NOW()
                     WHERE id = $1",
                )
                .bind(workflow_id)
                .execute(&pool)
                .await
                .expect("fail the workflow");
            }
        }
        let dependent_before = nodes(&pool, workflow_id).await[1].clone();

        let completion = complete(&broker, task_id).await.expect("phase 2");

        assert_eq!(
            completion,
            WorkflowTaskCompletion::AppliedWorkflowEnded,
            "{ended_as}"
        );
        assert_eq!(workflow_status(&pool, workflow_id).await, ended_as);
        assert_eq!(
            nodes(&pool, workflow_id).await,
            vec![
                (0, "COMPLETED".to_owned(), true, true),
                dependent_before.clone(),
            ],
            "{ended_as}"
        );
        assert!(
            ["PENDING", "SKIPPED"].contains(&dependent_before.1.as_str()),
            "{ended_as}: {dependent_before:?}"
        );
        assert!(!dependent_before.2, "{ended_as}: the dependent has no task");
        assert_eq!(pending_records(&pool, task_id).await, 0, "{ended_as}");
    }
}

/// A task with no phase-2 record and no terminal node writes nothing.
#[tokio::test]
#[serial]
async fn a_task_without_a_record_is_pending_absent() {
    let broker = test_broker().await;

    let completion = complete(&broker, Uuid::new_v4()).await.expect("phase 2");

    assert_eq!(completion, WorkflowTaskCompletion::PendingAbsent);
}

/// A record whose stored digest differs from the history row is kept, and
/// the node is not written.
#[tokio::test]
#[serial]
async fn a_record_whose_result_fails_its_check_is_kept() {
    let broker = test_broker().await;
    let pool = broker.pool().clone();
    let workflow_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    seed_chain(&pool, workflow_id, task_id).await;
    complete_task(&pool, task_id).await;
    sqlx::query(
        "UPDATE horsies_workflow_phase2_pending
         SET result_digest = decode(repeat('ff', 32), 'hex')
         WHERE task_id = $1",
    )
    .bind(task_id)
    .execute(&pool)
    .await
    .expect("change the stored digest");

    let refused = complete(&broker, task_id).await;

    assert!(
        matches!(
            refused,
            Err(WorkflowTaskCompletionError::RecordKept {
                task_id: kept,
                check: SourceFailure::DigestMismatch,
            }) if kept == task_id
        ),
        "{refused:?}"
    );
    assert_eq!(nodes(&pool, workflow_id).await[0].1, "RUNNING");
    assert_eq!(pending_records(&pool, task_id).await, 1);
}

/// A node write that raises fails the node finish; nothing commits and the
/// record stays.
#[tokio::test]
#[serial]
async fn a_node_finish_that_raises_commits_nothing() {
    let broker = test_broker().await;
    let pool = broker.pool().clone();
    let workflow_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    seed_chain(&pool, workflow_id, task_id).await;
    complete_task(&pool, task_id).await;
    sqlx::query(&format!(
        "CREATE OR REPLACE FUNCTION task_complete_inject_raise() RETURNS trigger
         LANGUAGE plpgsql AS $body$
         BEGIN
             IF NEW.workflow_id = '{workflow_id}'::uuid AND NEW.status = 'COMPLETED' THEN
                 RAISE EXCEPTION 'injected node finish raise' USING ERRCODE = 'P0001';
             END IF;
             RETURN NEW;
         END
         $body$",
    ))
    .execute(&pool)
    .await
    .expect("create the trigger function");
    sqlx::query(
        "CREATE TRIGGER task_complete_inject_raise BEFORE UPDATE ON horsies_workflow_tasks
         FOR EACH ROW EXECUTE FUNCTION task_complete_inject_raise()",
    )
    .execute(&pool)
    .await
    .expect("create the trigger");

    let refused = complete(&broker, task_id).await;

    sqlx::query("DROP TRIGGER task_complete_inject_raise ON horsies_workflow_tasks")
        .execute(&pool)
        .await
        .expect("drop the trigger");
    sqlx::query("DROP FUNCTION task_complete_inject_raise()")
        .execute(&pool)
        .await
        .expect("drop the trigger function");
    assert!(
        matches!(
            refused,
            Err(WorkflowTaskCompletionError::NodeFinish { task_id: failed, .. }) if failed == task_id
        ),
        "{refused:?}"
    );
    assert_eq!(nodes(&pool, workflow_id).await[0].1, "RUNNING");
    assert_eq!(pending_records(&pool, task_id).await, 1);
}

/// A RUNNING workflow whose `on_error` is `pause`, with two nodes and no
/// dependency between them: node 0 FAILED earlier with the error
/// `PRIOR_FAILURE` and no task, and node 1 RUNNING behind a RUNNING task
/// claimed by [`WORKER`].
async fn seed_after_a_failed_node(pool: &PgPool, workflow_id: Uuid, task_id: Uuid) {
    let prior_result = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(
        TaskError::new("PRIOR_FAILURE", "prior node failed").expect("a user error code"),
    ))
    .expect("encode the prior result");
    sqlx::query(
        "INSERT INTO horsies_workflows (
             id, name, status, on_error, definition_key, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'task_complete_pause', 'RUNNING', 'pause', $2, 0, $1,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .bind(format!("tests.task_complete.{workflow_id}"))
    .execute(pool)
    .await
    .expect("seed workflow");
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, status, sent_at, enqueued_at,
             started_at, claimed_at, claimed_by_worker_id, is_workflow_task, retry_count,
             max_retries, command_fingerprint_version, command_fingerprint,
             retention_class_key, retain_rerun_input, created_at, updated_at, execution,
             worker_pid, worker_hostname, worker_process_name
         ) VALUES (
             $1, 'task_complete_current', 'default', 100, '{}', 'RUNNING', NOW(), NOW(), NOW(),
             NOW(), $2, TRUE, 0, 0, NULL, NULL, 'standard_30d', FALSE, NOW(), NOW(),
             'ISOLATED', 4242, 'task-complete-host', 'task-complete-process'
         )",
    )
    .bind(task_id)
    .bind(WORKER)
    .execute(pool)
    .await
    .expect("seed task");
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, status,
             is_subworkflow, result, created_at, completed_at, max_retries, execution,
             retention_class_key
         ) VALUES (
             'NONE', $1, $2, 0, 'node_0', 'task_complete_prior', '{}', 'default', 100, '{}',
             FALSE, 'all', 'FAILED', FALSE, $3, NOW(), NOW(), 0, 'ISOLATED', 'standard_30d'
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(&prior_result)
    .execute(pool)
    .await
    .expect("seed the failed node");
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, status,
             is_subworkflow, task_id, created_at, started_at, max_retries, execution,
             retention_class_key
         ) VALUES (
             'NONE', $1, $2, 1, 'node_1', 'task_complete_current', '{}', 'default', 100, '{}',
             FALSE, 'all', 'RUNNING', FALSE, $3, NOW(), NOW(), 0, 'ISOLATED', 'standard_30d'
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(task_id)
    .execute(pool)
    .await
    .expect("seed the running node");
}

/// Fails the task with `error`: phase 1 of a worker, which writes its
/// phase-2 record.
async fn fail_task(pool: &PgPool, task_id: Uuid, error: &TaskError) {
    let result = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(error.clone()))
        .expect("encode the error result");
    let outcomes = terminal_writes::fail_running_task(
        pool,
        task_id,
        WORKER,
        &TerminalFailure::of_task(
            &result,
            Some("CURRENT_FAILURE"),
            Some("current node failed"),
        ),
    )
    .await
    .expect("terminalize the workflow task");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { .. }]
    ));
}

/// The node that phase 2 writes FAILED pauses its workflow (`on_error`
/// `pause`), and the workflow takes the error of that node, not the error of
/// the node that failed before it (`horsies_workflow_record_failure`).
#[tokio::test]
#[serial]
async fn a_pausing_workflow_records_the_error_of_the_node_that_phase_two_failed() {
    let broker = test_broker().await;
    let pool = broker.pool().clone();
    let workflow_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    seed_after_a_failed_node(&pool, workflow_id, task_id).await;
    let current_error =
        TaskError::new("CURRENT_FAILURE", "current node failed").expect("a user error code");
    fail_task(&pool, task_id, &current_error).await;

    let completion = complete(&broker, task_id).await.expect("phase 2");

    assert_eq!(completion, WorkflowTaskCompletion::Applied);
    assert_eq!(workflow_status(&pool, workflow_id).await, "PAUSED");
    let error_json: String =
        sqlx::query_scalar("SELECT error FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .fetch_one(&pool)
            .await
            .expect("read the workflow error");
    let stored: TaskError = serde_json::from_str(&error_json).expect("decode the workflow error");
    assert_eq!(stored.error_code, current_error.error_code);
    assert_eq!(stored.message, current_error.message);
    assert_eq!(
        nodes(&pool, workflow_id).await,
        vec![
            (0, "FAILED".to_owned(), false, true),
            (1, "FAILED".to_owned(), true, true),
        ]
    );
    assert_eq!(pending_records(&pool, task_id).await, 0);
}

/// A record whose evidence `horsies_phase2_quarantine_one` moved to the
/// quarantine table (verdict `REPOINTED`): phase 2 reads the result from the
/// copy, completes the node, and deletes the record and the copy.
#[tokio::test]
#[serial]
async fn a_record_whose_evidence_is_in_quarantine_completes_the_node_and_drains_both_rows() {
    let broker = test_broker().await;
    let pool = broker.pool().clone();
    let workflow_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    seed_chain(&pool, workflow_id, task_id).await;
    complete_task(&pool, task_id).await;
    let verdict: String =
        sqlx::query_scalar("SELECT verdict FROM horsies_phase2_quarantine_one($1)")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("quarantine the evidence");
    assert_eq!(verdict, "REPOINTED");

    let completion = complete(&broker, task_id).await.expect("phase 2");

    assert_eq!(completion, WorkflowTaskCompletion::Applied);
    assert_eq!(
        nodes(&pool, workflow_id).await[0],
        (0, "COMPLETED".to_owned(), true, true)
    );
    let evidence: (i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $1),
             (SELECT count(*) FROM horsies_workflow_phase2_quarantine WHERE task_id = $1)",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .expect("count the record and the copy");
    assert_eq!(evidence, (0, 0));
}

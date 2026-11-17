//! Failure handling of the engine: failure marks, pause, resume no-ops,
//! `allow_failed_deps` chains, and start refusals. Python:
//! `tests/integration/test_workflow_error_handling.py`.

use std::sync::Arc;

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    args_from_inner, code, err, error_of, finish_node, finish_task, handle, node, node_status,
    node_task_input, ok, required, seed_node, seed_node_task, seed_workflow, shared_pool, spec,
    start, stored_result, sub_workflow_node, task_node, try_start, unreachable_pool, workflow,
    workflow_error_code, workflow_status, IsolatedDatabase, LiveStatus, NodeSeed, WorkflowSeed,
    POISON_TASK_OPTIONS,
};
use crate::broker::workflow_start::ClientStartError;
use crate::core::{AnyNode, HorsiesError, OnError, TaskResult, WorkflowSpec};
use crate::test_support::child_specs::{TestBuildRefused, TestChildSpecs};
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::start_statement::{node_failure_result, subworkflow_load_failed_error};

fn after(index: usize, task_name: &str, dependencies: &[usize]) -> AnyNode {
    let mut node = task_node(index, task_name);
    node.dependencies = dependencies.to_vec();
    node
}

/// A node that takes `source`'s result as `input_result` and runs after a failure.
fn recovery(index: usize, task_name: &str, source: usize) -> AnyNode {
    let mut node = after(index, task_name, &[source]);
    node.args_from.insert("input_result".to_owned(), source);
    node.allow_failed_deps = true;
    node
}

async fn succeed(pool: &PgPool, workflow_id: Uuid, index: i32, value: serde_json::Value) {
    let _task = finish_node(
        pool,
        workflow_id,
        index,
        &ok(value),
        &TestChildSpecs::empty(),
    )
    .await;
}

async fn fail(pool: &PgPool, workflow_id: Uuid, index: i32, error_code: &str) {
    let _task = finish_node(
        pool,
        workflow_id,
        index,
        &err(error_code, "failed"),
        &TestChildSpecs::empty(),
    )
    .await;
}

/// `test_subworkflow_load_failure_does_not_overwrite_terminal_task`: the
/// load-failure mark of a sub-workflow node changes nothing on a node that
/// is already COMPLETED, FAILED or SKIPPED. The core writes the mark through
/// the `FAILURE` child start (`horsies_workflow_child_start`), which answers
/// `STALE` for a terminal node and keeps its result.
#[tokio::test]
#[serial]
async fn a_load_failure_mark_does_not_overwrite_a_terminal_node() {
    let pool = shared_pool().await;
    let failure = node_failure_result(subworkflow_load_failed_error(
        "subworkflow_terminal_guard_workflow",
        0,
        Some("tests.unregistered_child.v1"),
    ))
    .expect("encode the load failure");
    for terminal in ["COMPLETED", "FAILED", "SKIPPED"] {
        let workflow_id = seed_workflow(&pool, &WorkflowSeed::running()).await;
        let stored = format!(
            "pre-existing-subworkflow-{}-payload",
            terminal.to_lowercase()
        );
        let mut sub = NodeSeed::sub_workflow(
            workflow_id,
            0,
            terminal,
            "tests.unregistered_child.v1",
            None,
        );
        sub.result = Some(stored.clone());
        seed_node(&pool, &sub).await;

        let disposition: String = sqlx::query_scalar(LOAD_FAILURE_MARK_SQL)
            .bind(workflow_id)
            .bind(0_i32)
            .bind(&failure)
            .fetch_one(&pool)
            .await
            .expect("the failure mark");

        assert_eq!(disposition, "STALE", "{terminal}");
        let row = node(&pool, workflow_id, 0).await;
        assert_eq!(row.status, terminal);
        assert_eq!(row.result, Some(stored));
        super::support::end_workflows(&pool, &[workflow_id]).await;
    }
}

/// The `FAILURE` child start of node `$2` of workflow `$1` with the node
/// error envelope `$3`: NULL for the call time and for the 34 tree arrays
/// (`child_start::failure_statement` binds the same).
const LOAD_FAILURE_MARK_SQL: &str = "\
SELECT disposition FROM horsies_workflow_child_start($1, $2, 'FAILURE', $3, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL)
WHERE row_kind = 'OUTCOME'";

/// `test_fail_with_allow_failed_deps_output` and `test_allow_failed_deps_can_recover`:
/// the recovering output node keeps its result; the FAILED workflow stores none.
#[tokio::test]
#[serial]
async fn a_recovering_node_keeps_its_result_and_the_failed_workflow_stores_none() {
    let pool = shared_pool().await;
    let mut workflow_spec = spec(
        "output_recovery",
        vec![
            after(0, "output_fail_a", &[]),
            recovery(1, "output_fail_b", 0),
        ],
    );
    workflow_spec.output_index = Some(1);
    let workflow_id = start(&pool, &workflow_spec, &TestChildSpecs::empty()).await;

    fail(&pool, workflow_id, 0, "TEST_FAIL").await;
    succeed(&pool, workflow_id, 1, serde_json::json!(999)).await;

    let row = workflow(&pool, workflow_id).await;
    assert_eq!(row.status, "FAILED");
    assert_eq!(row.result, None);
    let recovered = stored_result(
        &node(&pool, workflow_id, 1)
            .await
            .result
            .expect("the recovering node stores its result"),
    );
    match recovered {
        TaskResult::Ok(value) => assert_eq!(value, serde_json::json!(999)),
        TaskResult::Err(error) => panic!("the recovering node failed: {error:?}"),
    }
}

/// `test_pause_second_failure_does_not_overwrite_error`
#[tokio::test]
#[serial]
async fn a_second_failure_while_paused_keeps_the_first_error() {
    let pool = shared_pool().await;
    let mut workflow_spec = spec(
        "pause_double",
        vec![after(0, "pause_a", &[]), after(1, "pause_b", &[])],
    );
    workflow_spec.on_error = OnError::Pause;
    let workflow_id = start(&pool, &workflow_spec, &TestChildSpecs::empty()).await;

    fail(&pool, workflow_id, 0, "FIRST_ERROR").await;
    assert_eq!(workflow_status(&pool, workflow_id).await, "PAUSED");
    fail(&pool, workflow_id, 1, "SECOND_ERROR").await;

    assert_eq!(
        workflow_error_code(&pool, workflow_id).await.as_deref(),
        Some("FIRST_ERROR")
    );
    assert_eq!(workflow_status(&pool, workflow_id).await, "PAUSED");
    super::support::end_workflows(&pool, &[workflow_id]).await;
}

/// Rust `test_on_error_fail_keeps_first_failed_error_by_index`: under
/// `on_error = fail`, the stored workflow error is the error of the FAILED
/// node with the lowest index while the workflow is RUNNING, whatever order
/// the failures finish in. Node 2 never finishes, so the workflow stays
/// RUNNING.
#[tokio::test]
#[serial]
async fn the_stored_error_is_the_one_of_the_lowest_failed_index() {
    let pool = shared_pool().await;
    let mut workflow_spec = spec(
        "fail_first_error_by_index",
        vec![
            after(0, "index_a", &[]),
            after(1, "index_b", &[]),
            after(2, "index_blocker", &[]),
        ],
    );
    workflow_spec.on_error = OnError::Fail;
    let workflow_id = start(&pool, &workflow_spec, &TestChildSpecs::empty()).await;

    fail(&pool, workflow_id, 1, "SECOND_ERROR").await;
    fail(&pool, workflow_id, 0, "FIRST_ERROR").await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "RUNNING");
    assert_eq!(
        workflow_error_code(&pool, workflow_id).await.as_deref(),
        Some("FIRST_ERROR")
    );
    super::support::end_workflows(&pool, &[workflow_id]).await;
}

/// `test_pause_guard_blocks_ready_to_enqueued`: a READY node of a PAUSED
/// workflow gets no task when a dependency finishes.
#[tokio::test]
#[serial]
async fn a_ready_node_of_a_paused_workflow_is_not_enqueued() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(
        &db.pool,
        &WorkflowSeed::running().status("PAUSED").on_error("pause"),
    )
    .await;
    let running_task = Uuid::new_v4();
    let _claimed_at = seed_node_task(&db.pool, running_task, LiveStatus::Running).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "RUNNING").task_id(running_task),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 1, "READY").deps(&[0]),
    )
    .await;

    finish_task(
        &db.pool,
        running_task,
        &ok(serde_json::json!(1)),
        &TestChildSpecs::empty(),
    )
    .await;

    let ready = node(&db.pool, workflow_id, 1).await;
    assert_eq!(ready.status, "READY");
    assert_eq!(ready.task_id, None);
    db.drop().await;
}

/// `test_enqueue_workflow_task_args_from_conflict_marks_failed`: an
/// `args_from` key that names a member of the node's input fails the node.
#[tokio::test]
#[serial]
async fn an_args_from_key_that_names_an_input_member_fails_the_node() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    let running_task = Uuid::new_v4();
    let _claimed_at = seed_node_task(&db.pool, running_task, LiveStatus::Running).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "RUNNING").task_id(running_task),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 1, "PENDING")
            .deps(&[0])
            .args_from("input_result", 0)
            .allow_failed_deps()
            .input(r#"{"input_result": 123}"#),
    )
    .await;

    finish_task(
        &db.pool,
        running_task,
        &ok(serde_json::json!(1)),
        &TestChildSpecs::empty(),
    )
    .await;

    let conflicted = node(&db.pool, workflow_id, 1).await;
    assert_eq!(conflicted.status, "FAILED");
    assert_eq!(conflicted.task_id, None);
    let error = super::support::stored_result_error(
        &conflicted.result.expect("the node stores its failure"),
    );
    assert_eq!(code(&error).as_deref(), Some("WORKFLOW_ENQUEUE_FAILED"));
    db.drop().await;
}

/// `test_enqueue_subworkflow_build_exception_marks_failed`
#[tokio::test]
#[serial]
async fn a_child_builder_failure_at_promotion_fails_the_node() {
    let db = IsolatedDatabase::create().await;
    let broken_key = "tests.port.broken_child_enqueue.v1";
    let child_specs = TestChildSpecs::empty().with_build(
        broken_key,
        Arc::new(|_input: &str| -> Result<WorkflowSpec, TestBuildRefused> {
            Err(TestBuildRefused::Failed(HorsiesError::new(
                "boom from build_with",
            )))
        }),
    );
    let broken = WorkflowSpec {
        definition_key: Some(broken_key.to_owned()),
        ..spec("broken_child_for_enqueue", vec![task_node(0, "unused")])
    };
    let mut sub = sub_workflow_node(1, &broken);
    sub.dependencies = vec![0];
    let workflow_id = start(
        &db.pool,
        &spec(
            "enq_sub_build_exception",
            vec![after(0, "enq_sub_a", &[]), sub],
        ),
        &child_specs,
    )
    .await;

    let _a = finish_node(
        &db.pool,
        workflow_id,
        0,
        &ok(serde_json::json!(2)),
        &child_specs,
    )
    .await;

    let failed = node(&db.pool, workflow_id, 1).await;
    assert_eq!(failed.status, "FAILED");
    assert_eq!(failed.task_id, None);
    assert_eq!(failed.sub_workflow_id, None);
    let error =
        super::support::stored_result_error(&failed.result.expect("the node stores its failure"));
    assert_eq!(code(&error).as_deref(), Some("WORKFLOW_ENQUEUE_FAILED"));
    db.drop().await;
}

/// Stores `options` as the task options of node `task_index`: a node row whose stored data the promotion
/// cannot read (the start refuses such options, so only a changed row holds them).
async fn store_node_options(pool: &PgPool, workflow_id: Uuid, task_index: i32, options: &str) {
    sqlx::query(
        "UPDATE horsies_workflow_tasks SET task_options = $3
         WHERE workflow_id = $1 AND task_index = $2",
    )
    .bind(workflow_id)
    .bind(task_index)
    .bind(options)
    .execute(pool)
    .await
    .expect("store the node options");
}

/// Python `_build_enqueued_task_params` and `_fail_enqueued_task`
/// (`python/horsies/core/workflows/engine.py:513-597, 752-820`): a task node whose stored retry policy does not read
/// at promotion fails alone with `WORKER_SERIALIZATION_ERROR` and Python's text, which names the node index
/// (`Invalid retry_policy for task <index>: <detail>`, the detail of `stored_max_retries`,
/// `python/horsies/core/codec/task_options.py:103`), with the node index and the workflow id in `data`. It gets no
/// task row, and its failure is applied: with `on_error = fail` and no other node left, the workflow fails.
/// The node is not left READY, and the step that promoted it is not rolled back. The baseline step writes the
/// same error (`horsies_workflow_unbuildable_nodes`, `horsies_workflow_task_options_refusal`).
#[tokio::test]
#[serial]
async fn a_task_node_that_cannot_be_prepared_at_promotion_fails_alone() {
    let db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_id = start(
        &db.pool,
        &spec(
            "unprepared_task_node",
            vec![
                after(0, "unprepared_source", &[]),
                after(1, "unprepared_dependent", &[0]),
            ],
        ),
        &child_specs,
    )
    .await;
    store_node_options(&db.pool, workflow_id, 1, r#"{"retry_policy": 5}"#).await;

    let _source = finish_node(
        &db.pool,
        workflow_id,
        0,
        &ok(serde_json::json!(1)),
        &child_specs,
    )
    .await;

    assert_eq!(node_status(&db.pool, workflow_id, 0).await, "COMPLETED");
    let dependent = node(&db.pool, workflow_id, 1).await;
    assert_eq!(dependent.status, "FAILED");
    assert_eq!(dependent.task_id, None);
    let error = super::support::stored_result_error(
        &dependent.result.expect("the node stores its failure"),
    );
    assert_eq!(code(&error).as_deref(), Some("WORKER_SERIALIZATION_ERROR"));
    assert_eq!(
        error.message.as_deref(),
        Some("Invalid retry_policy for task 1: retry_policy is not an object: int"),
        "{error:?}"
    );
    assert_eq!(
        error.data,
        Some(serde_json::json!({
            "workflow_id": workflow_id.to_string(),
            "task_index": 1,
        }))
    );
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    db.drop().await;
}

/// Python `_fail_enqueued_task` returns "stop" when `on_error = pause` paused the workflow, and the promotion
/// promotes no other node (`python/horsies/core/workflows/engine.py:513-597`). The node that cannot be prepared
/// fails; the workflow is PAUSED; its sibling of the same level holds no live task row and is not ENQUEUED.
/// The node stores task options that are not an object, which Python refuses at promotion
/// (`python/horsies/core/workflows/engine.py:798-805`). A stored `good_until` that the database cannot read does not
/// fail the node: Python passes its text to the task insert, which raises, and the step rolls back
/// (`python/horsies/core/workflows/engine.py:832-835`; the baseline `horsies_workflow_node_tasks_insert`).
#[tokio::test]
#[serial]
async fn a_node_that_cannot_be_prepared_pauses_the_workflow_and_promotes_no_sibling() {
    let db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let mut paused_spec = spec(
        "unprepared_task_node_pause",
        vec![
            after(0, "unprepared_pause_source", &[]),
            after(1, "unprepared_pause_dependent", &[0]),
            after(2, "unprepared_pause_sibling", &[0]),
        ],
    );
    paused_spec.on_error = OnError::Pause;
    let workflow_id = start(&db.pool, &paused_spec, &child_specs).await;
    store_node_options(&db.pool, workflow_id, 1, POISON_TASK_OPTIONS).await;

    let _source = finish_node(
        &db.pool,
        workflow_id,
        0,
        &ok(serde_json::json!(1)),
        &child_specs,
    )
    .await;

    assert_eq!(workflow_status(&db.pool, workflow_id).await, "PAUSED");
    let dependent = node(&db.pool, workflow_id, 1).await;
    assert_eq!(dependent.status, "FAILED");
    assert_eq!(dependent.task_id, None);
    let sibling = node(&db.pool, workflow_id, 2).await;
    assert!(
        matches!(sibling.status.as_str(), "PENDING" | "READY"),
        "{}",
        sibling.status
    );
    let live_sibling_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM horsies_tasks WHERE task_name = 'unprepared_pause_sibling'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(live_sibling_rows, 0);
    db.drop().await;
}

/// `test_resume_noop_when_completed` and `test_resume_noop_when_failed`
#[tokio::test]
#[serial]
async fn resume_of_a_completed_or_failed_workflow_returns_false() {
    let pool = shared_pool().await;
    let child_specs = TestChildSpecs::empty();
    let mut completed = spec("resume_completed", vec![after(0, "resume_a", &[])]);
    completed.on_error = OnError::Pause;
    let completed_id = start(&pool, &completed, &child_specs).await;
    succeed(&pool, completed_id, 0, serde_json::json!(1)).await;
    let failed_id = start(
        &pool,
        &spec("resume_failed", vec![after(0, "resume_fail_a", &[])]),
        &child_specs,
    )
    .await;
    fail(&pool, failed_id, 0, "TEST_FAIL").await;

    for (workflow_id, status) in [(completed_id, "COMPLETED"), (failed_id, "FAILED")] {
        assert_eq!(workflow_status(&pool, workflow_id).await, status);
        let resumed = handle(&pool, workflow_id)
            .resume(&child_specs)
            .await
            .expect("resume()");
        assert!(!resumed, "{status}");
        assert_eq!(workflow_status(&pool, workflow_id).await, status);
    }
}

/// `test_allow_failed_deps_chain`
#[tokio::test]
#[serial]
async fn a_chain_of_allow_failed_deps_nodes_is_enqueued() {
    let pool = shared_pool().await;
    let workflow_id = start(
        &pool,
        &spec(
            "afd_chain",
            vec![
                after(0, "afd_chain_a", &[]),
                recovery(1, "afd_chain_b", 0),
                recovery(2, "afd_chain_c", 1),
            ],
        ),
        &TestChildSpecs::empty(),
    )
    .await;

    fail(&pool, workflow_id, 0, "TEST").await;
    succeed(&pool, workflow_id, 1, serde_json::json!(999)).await;

    assert_eq!(node_status(&pool, workflow_id, 2).await, "ENQUEUED");
}

/// `test_allow_failed_deps_partial`
#[tokio::test]
#[serial]
async fn a_node_with_one_failed_and_one_ok_dependency_is_enqueued() {
    let pool = shared_pool().await;
    let mut receiver = after(2, "partial_receiver", &[0, 1]);
    receiver.args_from.insert("failed_input".to_owned(), 0);
    receiver.args_from.insert("success_input".to_owned(), 1);
    receiver.allow_failed_deps = true;
    let workflow_id = start(
        &pool,
        &spec(
            "afd_partial",
            vec![
                after(0, "afd_partial_a", &[]),
                after(1, "afd_partial_b", &[]),
                receiver,
            ],
        ),
        &TestChildSpecs::empty(),
    )
    .await;

    fail(&pool, workflow_id, 0, "FAIL").await;
    succeed(&pool, workflow_id, 1, serde_json::json!(100)).await;

    assert_eq!(node_status(&pool, workflow_id, 2).await, "ENQUEUED");
    let input = node_task_input(&pool, workflow_id, 2).await;
    assert_eq!(
        code(error_of(&args_from_inner(&input, "failed_input"))).as_deref(),
        Some("FAIL")
    );
    assert!(matches!(
        args_from_inner(&input, "success_input"),
        TaskResult::Ok(_)
    ));
}

/// `test_allow_failed_deps_receives_upstream_skipped`
#[tokio::test]
#[serial]
async fn an_allow_failed_deps_node_receives_upstream_skipped_for_a_skipped_dependency() {
    let pool = shared_pool().await;
    let workflow_id = start(
        &pool,
        &spec(
            "afd_skipped",
            vec![
                after(0, "afd_skipped_fail", &[]),
                after(1, "afd_skipped_skip", &[0]),
                recovery(2, "afd_skipped_recv", 1),
            ],
        ),
        &TestChildSpecs::empty(),
    )
    .await;

    fail(&pool, workflow_id, 0, "FAIL").await;

    assert_eq!(node_status(&pool, workflow_id, 1).await, "SKIPPED");
    assert_eq!(node_status(&pool, workflow_id, 2).await, "ENQUEUED");
    let input = node_task_input(&pool, workflow_id, 2).await;
    assert_eq!(
        code(error_of(&args_from_inner(&input, "input_result"))).as_deref(),
        Some("UPSTREAM_SKIPPED")
    );
}

/// `test_direct_workflow_spec_fails_without_queue` and `test_start_unresolved_queue`
#[tokio::test]
#[serial]
async fn start_refuses_a_task_node_with_no_resolved_queue() {
    let pool = shared_pool().await;
    let mut unresolved = task_node(0, "val_q_task");
    unresolved.queue = None;
    unresolved.priority = None;
    let workflow_id = Uuid::new_v4();

    let (_, started) = try_start(
        &pool,
        &spec("val_q_wf", vec![unresolved]),
        Some(workflow_id),
        &TestChildSpecs::empty(),
    )
    .await;
    let error = started.expect_err("a node with no queue is refused");

    assert!(
        matches!(
            &error,
            ClientStartError::Start(WorkflowError::Validation(_) | WorkflowError::Serialization(_))
        ),
        "{error:?}"
    );
    assert!(!error.is_retryable());
    assert!(error.to_string().contains("queue"), "{error}");
}

/// `test_success_case_not_met_error`: the required node was SKIPPED, not
/// FAILED, so the workflow error is the named outcome.
#[tokio::test]
#[serial]
async fn a_skipped_required_node_gives_workflow_success_case_not_met() {
    let pool = shared_pool().await;
    let mut workflow_spec = spec(
        "sp_notmet",
        vec![after(0, "sp_notmet_a", &[]), after(1, "sp_notmet_b", &[0])],
    );
    workflow_spec.success_policy = Some(required(&[&[1]]));
    let workflow_id = start(&pool, &workflow_spec, &TestChildSpecs::empty()).await;

    fail(&pool, workflow_id, 0, "TEST_FAIL").await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    assert_eq!(
        workflow_error_code(&pool, workflow_id).await.as_deref(),
        Some("WORKFLOW_SUCCESS_CASE_NOT_MET")
    );
}

/// `test_start_schema_init_failure` and `test_start_db_transient_error`: a
/// start that cannot reach its database fails at the schema install, and a
/// resend can succeed.
#[tokio::test]
async fn a_start_that_cannot_reach_the_database_is_a_retryable_schema_install_failure() {
    let workflow_spec = spec("db_err_wf", vec![task_node(0, "db_err_a")]);

    let (_, started) = try_start(
        &unreachable_pool(),
        &workflow_spec,
        None,
        &TestChildSpecs::empty(),
    )
    .await;
    let error = started.expect_err("no database");

    assert!(
        matches!(error, ClientStartError::SchemaInstall(_)),
        "{error:?}"
    );
    assert!(error.is_retryable());
}

/// `test_start_rollback_integrity`: a database failure after the workflow row
/// is written, inside the start, leaves no row.
#[tokio::test]
#[serial]
async fn a_failure_in_the_middle_of_a_start_leaves_no_row() {
    let db = IsolatedDatabase::create().await;
    super::support::execute(
        &db.pool,
        "CREATE FUNCTION port_test_refuse_nodes() RETURNS trigger
         LANGUAGE plpgsql AS $body$
         BEGIN
             RAISE EXCEPTION 'refused by the test' USING ERRCODE = '08006';
         END
         $body$",
    )
    .await;
    super::support::execute(
        &db.pool,
        "CREATE TRIGGER port_test_refuse_nodes BEFORE INSERT ON horsies_workflow_tasks
         FOR EACH ROW EXECUTE FUNCTION port_test_refuse_nodes()",
    )
    .await;

    let (_, started) = try_start(
        &db.pool,
        &spec("rollback_wf", vec![task_node(0, "rollback_a")]),
        None,
        &TestChildSpecs::empty(),
    )
    .await;
    let error = started.expect_err("the node insert is refused");

    assert!(
        matches!(
            &error,
            ClientStartError::Start(
                WorkflowError::Database(_)
                    | WorkflowError::Broker(_)
                    | WorkflowError::StepContract(_)
                    | WorkflowError::ChildSpecSource(_)
            )
        ),
        "{error:?}"
    );
    let rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM horsies_workflows WHERE name = 'rollback_wf'")
            .fetch_one(&db.pool)
            .await
            .expect("count the workflow rows");
    assert_eq!(rows, 0);
    db.drop().await;
}

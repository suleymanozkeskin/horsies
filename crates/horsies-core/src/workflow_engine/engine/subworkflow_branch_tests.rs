//! The sub-workflow branches of the engine: the child start of a promoted
//! sub-workflow node (`enqueue_subworkflow_task`), the write of the parent
//! node when a child ends (`on_subworkflow_complete`), and the workflow
//! context of a node (`build_workflow_context_data`). Python: the
//! sub-workflow branches of `tests/unit/test_workflow_engine_coverage.py`.
//! Each test names the Python tests that it ports.
//!
//! The database tests use the seeds of [`super::promotion_branch_tests`] on
//! the shared migrated database and remove the rows that they write.

use std::time::Duration;

use serial_test::serial;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use super::promotion_branch_tests::{
    child_count, child_registry, child_task_node, empty_registry, node_state, recover, remove_tree,
    retry_options, seed_sub_workflow_dependent, seed_sub_workflow_nodes, seed_task_node,
    seed_workflow, shared_pool, stored_node_error, update_node, CHILD_KEY,
};
use super::*;
use crate::workflow_engine::start::MAX_SUBWORKFLOW_DEPTH;

/// The time the unreachable pool waits for a connection.
const UNREACHABLE_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(1);

/// A top-level workflow and, under it, a RUNNING workflow at `depth` with
/// the nodes of [`seed_sub_workflow_nodes`]. Returns (top, deep).
async fn seed_workflow_at_depth(pool: &PgPool, depth: i32) -> (Uuid, Uuid) {
    let top_id = seed_workflow(pool).await;
    let workflow_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_workflows (
             id, name, status, on_error, definition_key, parent_workflow_id, parent_task_index,
             depth, root_workflow_id, sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'promotion_branch_deep', 'RUNNING', 'fail', $2, $3, 0, $4, $3,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .bind(format!(
        "tests.promotion_branch.deep.{}",
        workflow_id.simple()
    ))
    .bind(top_id)
    .bind(depth)
    .execute(pool)
    .await
    .expect("seed the deep workflow");
    seed_sub_workflow_nodes(pool, workflow_id).await;
    (top_id, workflow_id)
}

/// A child workflow of `parent_id` at node `parent_task_index`, with no
/// output node and no nodes, in `status`.
async fn seed_child_workflow(
    pool: &PgPool,
    parent_id: Uuid,
    parent_task_index: i32,
    status: &str,
) -> Uuid {
    let child_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_workflows (
             id, name, status, on_error, definition_key, parent_workflow_id, parent_task_index,
             depth, root_workflow_id, sent_at, created_at, started_at, completed_at, updated_at
         ) VALUES ($1, 'promotion_branch_child', $2, 'fail', $3, $4, $5, 1, $4,
                   NOW(), NOW(), NOW(),
                   CASE WHEN $2 IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN NOW() END,
                   NOW())",
    )
    .bind(child_id)
    .bind(status)
    .bind(CHILD_KEY)
    .bind(parent_id)
    .bind(parent_task_index)
    .execute(pool)
    .await
    .expect("seed the child workflow");
    child_id
}

/// A RUNNING sub-workflow node `node_<index>` of [`CHILD_KEY`] with no
/// dependency, linked to `child_id` when given.
async fn seed_running_sub_node(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
    child_id: Option<Uuid>,
) {
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, status,
             is_subworkflow, sub_workflow_id, sub_workflow_name, sub_definition_key,
             sub_workflow_params, created_at, started_at
         ) VALUES (
             'NONE', $1, $2, $3, $4, 'promotion_branch_child', '{}', 'default', 100, '{}',
             FALSE, 'all', 'RUNNING', TRUE, $5, 'promotion_branch_child', $6, 'NOT_GIVEN',
             NOW(), NOW()
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(task_index)
    .bind(format!("node_{task_index}"))
    .bind(child_id)
    .bind(CHILD_KEY)
    .execute(pool)
    .await
    .expect("seed the running sub-workflow node");
}

async fn workflow_status(pool: &PgPool, workflow_id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .fetch_one(pool)
        .await
        .expect("read the workflow status")
}

/// Promotes sub-workflow node 1 of a workflow at `depth` with an empty
/// registry; returns the node status, its error code and the child count.
async fn promote_at_depth(pool: &PgPool, depth: i32) -> (String, String, i64) {
    let (top_id, workflow_id) = seed_workflow_at_depth(pool, depth).await;

    let promoted = recover(pool, workflow_id, 1, &empty_registry()).await;

    let (status, _task_id, result) = node_state(pool, workflow_id, 1).await;
    let children = child_count(pool, workflow_id).await;
    remove_tree(pool, top_id).await;
    assert!(matches!(promoted, Ok(true)), "depth {depth}: {promoted:?}");
    let code = stored_node_error(result.as_deref()).error_code.to_string();
    (status, code, children)
}

/// Promotes sub-workflow node 1 after `statement` (with the workflow id as
/// `$1`) changed it; returns the promotion, the node state and the child count.
async fn promote_changed_sub_node(
    pool: &PgPool,
    statement: &str,
    registry: &WorkflowSpecRegistry,
) -> (
    Result<bool, WorkflowError>,
    (String, Option<Uuid>, Option<String>),
    i64,
) {
    let workflow_id = seed_sub_workflow_dependent(pool).await;
    update_node(pool, workflow_id, statement).await;

    let promoted = recover(pool, workflow_id, 1, registry).await;

    let state = node_state(pool, workflow_id, 1).await;
    let children = child_count(pool, workflow_id).await;
    remove_tree(pool, workflow_id).await;
    (promoted, state, children)
}

/// `TestEnqueueSubworkflowTask::test_no_row_returns_none`: a sub-workflow
/// node that another promotion claimed after it was read starts no child.
#[tokio::test]
#[serial]
async fn a_sub_workflow_promotion_that_finds_its_node_already_claimed_starts_no_child() {
    let pool = shared_pool().await;
    let workflow_id = seed_sub_workflow_dependent(&pool).await;
    let task: DependentRow = sqlx::query_as(GET_PENDING_WORKFLOW_TASK_SQL)
        .bind(workflow_id)
        .bind(1)
        .fetch_one(&pool)
        .await
        .expect("read the PENDING node");
    update_node(
        &pool,
        workflow_id,
        "UPDATE horsies_workflow_tasks SET status = 'READY'
         WHERE workflow_id = $1 AND task_index = 1",
    )
    .await;
    let registry = child_registry(child_task_node(None, None));

    let enqueued = enqueue_subworkflow_task(
        &pool,
        workflow_id,
        &task,
        &registry,
        &HashMap::new(),
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await;

    let (status, _task_id, result) = node_state(&pool, workflow_id, 1).await;
    let children = child_count(&pool, workflow_id).await;
    remove_tree(&pool, workflow_id).await;
    assert!(enqueued.is_ok(), "{enqueued:?}");
    assert_eq!(status, "READY");
    assert_eq!(result, None);
    assert_eq!(children, 0);
}

/// `TestEnqueueSubworkflowTask::test_a_child_past_the_depth_limit_fails_the_node_and_loads_nothing`:
/// the depth check comes before the definition is resolved, so a node of a
/// workflow at the limit fails with `SUBWORKFLOW_DEPTH_LIMIT_REACHED` even
/// with a registry that holds no child.
#[tokio::test]
#[serial]
async fn a_child_past_the_depth_limit_fails_the_node_before_its_definition_is_resolved() {
    let pool = shared_pool().await;

    let (status, code, children) = promote_at_depth(&pool, MAX_SUBWORKFLOW_DEPTH).await;

    assert_eq!(status, "FAILED");
    assert_eq!(code, "SUBWORKFLOW_DEPTH_LIMIT_REACHED");
    assert_eq!(children, 0);
}

/// `TestEnqueueSubworkflowTask::test_a_child_at_the_depth_limit_goes_on_to_load_its_definition`:
/// a child that reaches the limit exactly passes the depth check and goes on
/// to resolve its definition, which an empty registry does not hold.
#[tokio::test]
#[serial]
async fn a_child_at_the_depth_limit_goes_on_to_resolve_its_definition() {
    let pool = shared_pool().await;

    let (status, code, children) = promote_at_depth(&pool, MAX_SUBWORKFLOW_DEPTH - 1).await;

    assert_eq!(status, "FAILED");
    assert_eq!(code, "SUBWORKFLOW_LOAD_FAILED");
    assert_eq!(children, 0);
}

/// `TestEnqueueSubworkflowTask::test_corrupt_kwargs_json_fails`: a stored
/// input of a sub-workflow node that does not parse fails the node with
/// `WORKER_SERIALIZATION_ERROR`, and no child starts.
#[tokio::test]
#[serial]
#[ignore = "waits on #1120"]
async fn a_sub_workflow_node_whose_stored_input_does_not_parse_fails_alone() {
    let pool = shared_pool().await;
    let (promoted, (status, _task_id, result), children) = promote_changed_sub_node(
        &pool,
        "UPDATE horsies_workflow_tasks SET input = 'broken{json'
         WHERE workflow_id = $1 AND task_index = 1",
        &child_registry(child_task_node(None, None)),
    )
    .await;

    assert!(matches!(promoted, Ok(true)), "{promoted:?}");
    assert_eq!(status, "FAILED");
    assert_eq!(
        stored_node_error(result.as_deref()).error_code.to_string(),
        "WORKER_SERIALIZATION_ERROR"
    );
    assert_eq!(children, 0);
}

/// `TestEnqueueSubworkflowTask::test_args_from_non_int_values_fails_task`:
/// an `args_from` value that is not an integer fails the sub-workflow node
/// with `WORKER_SERIALIZATION_ERROR`, and no child starts.
#[tokio::test]
#[serial]
#[ignore = "waits on #1120"]
async fn a_sub_workflow_node_whose_args_from_holds_a_value_that_is_not_an_index_fails_alone() {
    let pool = shared_pool().await;
    let (promoted, (status, _task_id, result), children) = promote_changed_sub_node(
        &pool,
        r#"UPDATE horsies_workflow_tasks SET args_from = '{"x": "not_an_int"}'::jsonb
           WHERE workflow_id = $1 AND task_index = 1"#,
        &child_registry(child_task_node(None, None)),
    )
    .await;

    assert!(matches!(promoted, Ok(true)), "{promoted:?}");
    assert_eq!(status, "FAILED");
    assert_eq!(
        stored_node_error(result.as_deref()).error_code.to_string(),
        "WORKER_SERIALIZATION_ERROR"
    );
    assert_eq!(children, 0);
}

/// `TestEnqueueSubworkflowTask::test_corrupt_args_from_string_fails`: an
/// `args_from` stored as a JSON string fails the sub-workflow node with
/// `WORKER_SERIALIZATION_ERROR`, and no child starts.
#[tokio::test]
#[serial]
#[ignore = "waits on #1222"]
async fn a_sub_workflow_node_whose_args_from_is_a_json_string_fails_alone() {
    let pool = shared_pool().await;
    let (promoted, (status, _task_id, result), children) = promote_changed_sub_node(
        &pool,
        r#"UPDATE horsies_workflow_tasks SET args_from = '"{broken"'::jsonb
           WHERE workflow_id = $1 AND task_index = 1"#,
        &child_registry(child_task_node(None, None)),
    )
    .await;

    assert!(matches!(promoted, Ok(true)), "{promoted:?}");
    assert_eq!(status, "FAILED");
    assert_eq!(
        stored_node_error(result.as_deref()).error_code.to_string(),
        "WORKER_SERIALIZATION_ERROR"
    );
    assert_eq!(children, 0);
}

/// `TestEnqueueSubworkflowTask::test_build_with_not_overridden_with_args_raises`:
/// a node that gives params to a child with no builder fails with
/// `WORKFLOW_ENQUEUE_FAILED` (the build failure), and no child starts.
#[tokio::test]
#[serial]
async fn a_sub_workflow_node_with_params_for_a_child_without_a_builder_fails_alone() {
    let pool = shared_pool().await;
    let (promoted, (status, _task_id, result), children) = promote_changed_sub_node(
        &pool,
        "UPDATE horsies_workflow_tasks SET sub_workflow_params = 'GIVEN'
         WHERE workflow_id = $1 AND task_index = 1",
        &child_registry(child_task_node(None, None)),
    )
    .await;

    assert!(matches!(promoted, Ok(true)), "{promoted:?}");
    assert_eq!(status, "FAILED");
    let error = stored_node_error(result.as_deref());
    assert_eq!(error.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
    let message = error.message.unwrap_or_default();
    assert!(message.contains("received runtime parameters"), "{message}");
    assert_eq!(children, 0);
}

/// `TestEnqueueSubworkflowTask::test_child_task_good_until_options_merge`:
/// the child start merges the deadline of a child root into its task
/// options; the node row keeps both members and the task row its deadline.
#[tokio::test]
#[serial]
async fn a_child_root_with_a_deadline_carries_it_in_its_options_and_task_row() {
    let pool = shared_pool().await;
    let deadline = chrono::Utc::now() + chrono::Duration::hours(1);
    let options = retry_options(2);
    let workflow_id = seed_sub_workflow_dependent(&pool).await;
    let registry = child_registry(child_task_node(Some(options.as_str()), Some(deadline)));

    let promoted = recover(&pool, workflow_id, 1, &registry).await;

    let root: Option<(Option<String>, bool)> = sqlx::query_as(
        "SELECT n.task_options, t.good_until IS NOT NULL
         FROM horsies_workflow_tasks n
         JOIN horsies_workflows w ON w.id = n.workflow_id
         JOIN horsies_tasks t ON t.id = n.task_id
         WHERE w.parent_workflow_id = $1",
    )
    .bind(workflow_id)
    .fetch_optional(&pool)
    .await
    .expect("read the child root and its task row");
    remove_tree(&pool, workflow_id).await;
    assert!(matches!(promoted, Ok(true)), "{promoted:?}");
    let (node_options, task_has_deadline) = root.expect("the child root has a task row");
    let node_options: serde_json::Value =
        serde_json::from_str(&node_options.expect("the child root stores its options")).unwrap();
    assert_eq!(
        node_options["good_until"].as_str(),
        Some(deadline.to_rfc3339().as_str())
    );
    assert_eq!(node_options["retry_policy"]["max_retries"], 2);
    assert!(task_has_deadline);
}

/// `TestEnqueueSubworkflowTask::test_custom_mode_child_binds_queue_priority`:
/// a child task node stores the priority that its queue resolved, on the node
/// row and on its task row, not the default 100.
#[tokio::test]
#[serial]
async fn a_child_task_node_keeps_the_priority_of_its_queue() {
    let pool = shared_pool().await;
    let workflow_id = seed_sub_workflow_dependent(&pool).await;
    let mut child_root = child_task_node(None, None);
    child_root.priority = Some(30);
    let registry = child_registry(child_root);

    let promoted = recover(&pool, workflow_id, 1, &registry).await;

    let priorities: Option<(i32, i32)> = sqlx::query_as(
        "SELECT n.priority, t.priority
         FROM horsies_workflow_tasks n
         JOIN horsies_workflows w ON w.id = n.workflow_id
         JOIN horsies_tasks t ON t.id = n.task_id
         WHERE w.parent_workflow_id = $1",
    )
    .bind(workflow_id)
    .fetch_optional(&pool)
    .await
    .expect("read the child root priorities");
    remove_tree(&pool, workflow_id).await;
    assert!(matches!(promoted, Ok(true)), "{promoted:?}");
    assert_eq!(priorities, Some((30, 30)));
}

/// `TestEnqueueSubworkflowTask::test_custom_mode_invalid_child_queue_fails_closed`:
/// a child task node whose queue does not resolve fails the parent node with
/// `WORKFLOW_ENQUEUE_FAILED`; it is not written and the start does not raise.
#[tokio::test]
#[serial]
#[ignore = "waits on #1229"]
async fn a_child_with_a_task_node_without_a_resolved_queue_fails_the_parent_node() {
    let pool = shared_pool().await;
    let workflow_id = seed_sub_workflow_dependent(&pool).await;
    let mut child_root = child_task_node(None, None);
    child_root.queue = None;
    let registry = child_registry(child_root);

    let promoted = recover(&pool, workflow_id, 1, &registry).await;

    let (status, _task_id, result) = node_state(&pool, workflow_id, 1).await;
    let children = child_count(&pool, workflow_id).await;
    remove_tree(&pool, workflow_id).await;
    assert!(matches!(promoted, Ok(true)), "{promoted:?}");
    assert_eq!(status, "FAILED");
    assert_eq!(
        stored_node_error(result.as_deref()).error_code.to_string(),
        "WORKFLOW_ENQUEUE_FAILED"
    );
    assert_eq!(children, 0);
}

/// `TestOnChildWorkflowComplete::test_child_not_found_returns`: the end of a
/// child that has no row writes nothing into the parent node.
#[tokio::test]
#[serial]
async fn a_child_end_with_no_child_row_writes_nothing() {
    let pool = shared_pool().await;
    let workflow_id = seed_workflow(&pool).await;
    seed_running_sub_node(&pool, workflow_id, 0, None).await;

    let completed = on_subworkflow_complete(
        &pool,
        workflow_id,
        0,
        Uuid::new_v4(),
        "COMPLETED",
        None,
        &empty_registry(),
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await;

    let (status, _task_id, result) = node_state(&pool, workflow_id, 0).await;
    remove_tree(&pool, workflow_id).await;
    assert!(completed.is_ok(), "{completed:?}");
    assert_eq!(status, "RUNNING");
    assert_eq!(result, None);
}

/// `TestOnChildWorkflowComplete::test_parent_paused_stops_propagation`: the
/// end of a child writes its parent node, and a PAUSED parent promotes
/// nothing: the dependent of the node stays PENDING with no task row.
#[tokio::test]
#[serial]
async fn a_child_end_under_a_paused_parent_writes_the_node_and_promotes_nothing() {
    let pool = shared_pool().await;
    let workflow_id = seed_workflow(&pool).await;
    let child_id = seed_child_workflow(&pool, workflow_id, 0, "COMPLETED").await;
    seed_running_sub_node(&pool, workflow_id, 0, Some(child_id)).await;
    seed_task_node(&pool, workflow_id, 1, &[0], "PENDING").await;
    update_node(
        &pool,
        workflow_id,
        "UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = $1",
    )
    .await;
    let child_result = serde_json::to_string(&TaskResult::Ok(serde_json::json!(1))).unwrap();

    let completed = on_subworkflow_complete(
        &pool,
        workflow_id,
        0,
        child_id,
        "COMPLETED",
        Some(child_result.as_str()),
        &empty_registry(),
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await;

    let (node_status, _task_id, _result) = node_state(&pool, workflow_id, 0).await;
    let (dependent_status, dependent_task, _result) = node_state(&pool, workflow_id, 1).await;
    let parent_status = workflow_status(&pool, workflow_id).await;
    remove_tree(&pool, workflow_id).await;
    assert!(completed.is_ok(), "{completed:?}");
    assert_eq!(node_status, "COMPLETED");
    assert_eq!(dependent_status, "PENDING");
    assert_eq!(dependent_task, None);
    assert_eq!(parent_status, "PAUSED");
}

/// `TestBuildWorkflowContextData::test_fetches_subworkflow_summaries`: the
/// context entry of a sub-workflow node carries its stored summary text.
#[test]
fn a_context_entry_of_a_sub_workflow_node_carries_its_stored_summary() {
    let summary = r#"{"status":"COMPLETED"}"#;
    let ok = serde_json::to_string(&TaskResult::Ok(serde_json::json!(1))).unwrap();
    let row = CtxResultRow {
        node_id: Some("sub-node".to_owned()),
        task_index: 0,
        task_name: "promotion_branch_child".to_owned(),
        sub_definition_key: Some(CHILD_KEY.to_owned()),
        status: "COMPLETED".to_owned(),
        result: Some(ok),
        sub_workflow_summary: Some(summary.to_owned()),
    };

    let context = WorkflowContextData::from_rows(Uuid::nil(), 1, "my_task", vec![row]);

    assert_eq!(context.summaries_by_id.len(), 1);
    assert_eq!(
        context.summaries_by_id.get("sub-node").map(String::as_str),
        Some(summary)
    );
}

/// `TestBuildWorkflowContextData::test_empty_ctx_from_ids_no_summaries`: a
/// node with an empty context list reads nothing (the pool here cannot
/// connect) and gets an empty context.
#[tokio::test]
async fn an_empty_context_list_reads_nothing() {
    let unreachable = PgPoolOptions::new()
        .acquire_timeout(UNREACHABLE_ACQUIRE_TIMEOUT)
        .connect_lazy_with(
            PgConnectOptions::new()
                .host("127.0.0.1")
                .port(1)
                .username("postgres")
                .database("horsies_unreachable"),
        );

    let context = build_workflow_context_data(&unreachable, Uuid::nil(), 0, "my_task", &[])
        .await
        .expect("an empty context list reads nothing");

    assert!(context.results_by_id.is_empty());
    assert!(context.summaries_by_id.is_empty());
}

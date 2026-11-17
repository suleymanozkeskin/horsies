//! The promotion of one node: `recover_pending_workflow_task` →
//! `try_make_ready_and_enqueue` → `enqueue_workflow_task` or
//! `enqueue_subworkflow_task`, and the stored node data that the promotion
//! reads (`build_enqueued_task_params`). Python: the promotion branches of
//! `tests/unit/test_workflow_engine_coverage.py`,
//! `tests/unit/test_workflow_child_start_task_options.py` and
//! `tests/unit/test_stored_max_retries.py`. Each test names the Python tests
//! that it ports.
//!
//! The tests seed rows on the shared migrated database (needs
//! `0005_workflow_functions.sql`). Every node runs on [`QUEUE`], which no
//! worker test claims, and each test removes the rows that it wrote.

use std::sync::Arc;

use serial_test::serial;

use super::*;
use crate::core::registry::workflow::RegisteredWorkflowSpec;
use crate::core::workflow::node::SubWorkflowParams;
use crate::core::{AnyNode, OnError, WorkflowSpec, WorkflowStartErrorCode};
use crate::workflow_engine::args_merge::{merge_args_from_sync, ARGS_FROM_ENVELOPE_KEY};

/// The queue of every seeded task node and of the child task nodes.
pub(super) const QUEUE: &str = "promotion_branch_q";

/// The definition key of the child workflow of the sub-workflow node.
pub(super) const CHILD_KEY: &str = "tests.promotion_branch.child.v1";

/// A retry policy of Python's shape (`_policy` of
/// `tests/unit/test_stored_max_retries.py`) with `max_retries`.
pub(super) fn retry_options(max_retries: i64) -> String {
    format!(
        r#"{{"retry_policy": {{"max_retries": {max_retries}, "intervals": [5],
            "backoff_strategy": "exponential", "jitter": true, "auto_retry_for": ["TRANSIENT"]}}}}"#
    )
}

pub(super) fn empty_registry() -> WorkflowSpecRegistry {
    WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER)
}

pub(super) async fn shared_pool() -> PgPool {
    crate::broker::terminalization_matrix::migrated_pool().await
}

/// A RUNNING top-level workflow with `on_error = fail`.
pub(super) async fn seed_workflow(pool: &PgPool) -> Uuid {
    let workflow_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_workflows (
             id, name, status, on_error, definition_key, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'promotion_branch', 'RUNNING', 'fail', $2, 0, $1,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .bind(format!("tests.promotion_branch.{}", workflow_id.simple()))
    .execute(pool)
    .await
    .expect("seed the workflow");
    workflow_id
}

/// A task node `node_<index>` of task `promotion_branch_task` on [`QUEUE`]
/// with the input `{}`, no options and no `args_from`.
pub(super) async fn seed_task_node(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
    dependencies: &[i32],
    status: &str,
) {
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, status,
             is_subworkflow, max_retries, execution, retention_class_key,
             created_at, started_at, completed_at
         ) VALUES (
             'NONE', $1, $2, $3, $4, 'promotion_branch_task', '{}', $5, 100, $6, FALSE, 'all',
             $7, FALSE, 0, 'ISOLATED', 'forever', NOW(),
             CASE WHEN $7 IN ('ENQUEUED', 'RUNNING', 'COMPLETED', 'FAILED') THEN NOW() END,
             CASE WHEN $7 IN ('COMPLETED', 'FAILED', 'SKIPPED') THEN NOW() END
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(task_index)
    .bind(format!("node_{task_index}"))
    .bind(QUEUE)
    .bind(dependencies)
    .bind(status)
    .execute(pool)
    .await
    .expect("seed the task node");
}

/// Node 0 COMPLETED with an ok result, and node 1, a task node that waits for
/// node 0, PENDING. Returns the workflow id.
async fn seed_completed_root_and_dependent(pool: &PgPool) -> Uuid {
    let workflow_id = seed_workflow(pool).await;
    seed_task_node(pool, workflow_id, 0, &[], "COMPLETED").await;
    seed_task_node(pool, workflow_id, 1, &[0], "PENDING").await;
    let ok = serde_json::to_string(&TaskResult::Ok(serde_json::json!(7))).unwrap();
    sqlx::query(
        "UPDATE horsies_workflow_tasks SET result = $2 WHERE workflow_id = $1 AND task_index = 0",
    )
    .bind(workflow_id)
    .bind(ok)
    .execute(pool)
    .await
    .expect("store the result of node 0");
    workflow_id
}

/// Runs `statement` with the workflow id as `$1` and checks that it wrote one row.
pub(super) async fn update_node(pool: &PgPool, workflow_id: Uuid, statement: &str) {
    let updated = sqlx::query(statement)
        .bind(workflow_id)
        .execute(pool)
        .await
        .unwrap_or_else(|error| panic!("{statement}: {error}"));
    assert_eq!(updated.rows_affected(), 1, "{statement}");
}

/// The status, the linked task and the stored result of a node.
pub(super) async fn node_state(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
) -> (String, Option<Uuid>, Option<String>) {
    sqlx::query_as(
        "SELECT status, task_id, result FROM horsies_workflow_tasks
         WHERE workflow_id = $1 AND task_index = $2",
    )
    .bind(workflow_id)
    .bind(task_index)
    .fetch_one(pool)
    .await
    .expect("read the node")
}

/// The error of a stored node result that must be an error envelope.
pub(super) fn stored_node_error(result: Option<&str>) -> TaskError {
    let text = result.expect("the node stores a result");
    match serde_json::from_str::<TaskResult<serde_json::Value>>(text).expect("a result envelope") {
        TaskResult::Err(error) => error,
        TaskResult::Ok(value) => panic!("expected an error envelope, got ok {value}"),
    }
}

/// Live task rows on [`QUEUE`].
async fn queued_task_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE queue_name = $1")
        .bind(QUEUE)
        .fetch_one(pool)
        .await
        .expect("count the task rows of the queue")
}

/// Child workflows of `workflow_id`.
pub(super) async fn child_count(pool: &PgPool, workflow_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_workflows WHERE parent_workflow_id = $1")
        .bind(workflow_id)
        .fetch_one(pool)
        .await
        .expect("count the child workflows")
}

/// Removes the task rows of the workflow tree of `workflow_id`, then the tree
/// (its nodes and child workflows go with it).
pub(super) async fn remove_tree(pool: &PgPool, workflow_id: Uuid) {
    sqlx::query(
        "DELETE FROM horsies_tasks WHERE id IN (
             SELECT n.task_id FROM horsies_workflow_tasks n
             JOIN horsies_workflows w ON w.id = n.workflow_id
             WHERE w.root_workflow_id = $1 AND n.task_id IS NOT NULL
         )",
    )
    .bind(workflow_id)
    .execute(pool)
    .await
    .expect("remove the task rows of the tree");
    sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .execute(pool)
        .await
        .expect("remove the workflow tree");
}

pub(super) async fn recover(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
    registry: &WorkflowSpecRegistry,
) -> Result<bool, WorkflowError> {
    recover_pending_workflow_task(
        pool,
        workflow_id,
        task_index,
        registry,
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await
}

/// `TestValidateArgsFromMap::test_valid_map_returns_typed`: a map of
/// integer indexes gives one `args_from` member per key.
#[test]
fn an_args_from_map_of_integer_indexes_merges_one_member_per_key() {
    let ok = serde_json::to_string(&TaskResult::Ok(serde_json::json!(1))).unwrap();
    let dependencies: HashMap<i32, DepResultValue> = [0, 1]
        .into_iter()
        .map(|index| {
            (
                index,
                DepResultValue {
                    node_id: Some(format!("node_{index}")),
                    task_name: "produce".to_owned(),
                    sub_definition_key: None,
                    status: "COMPLETED".to_owned(),
                    result: Some(ok.clone()),
                },
            )
        })
        .collect();

    let merge = merge_args_from_sync(
        "{}",
        &Some(serde_json::json!({"x": 0, "y": 1})),
        &dependencies,
    )
    .unwrap();

    let merged = match merge {
        ArgsFromMerge::Merged(merged) => merged,
        other @ ArgsFromMerge::StaticMemberConflict(_) => {
            panic!("unexpected merge outcome: {other:?}")
        }
    };
    let object: serde_json::Value = serde_json::from_str(&merged).unwrap();
    assert_eq!(
        object.as_object().map(serde_json::Map::len),
        Some(2),
        "{merged}"
    );
    for key in ["x", "y"] {
        assert_eq!(object[key][ARGS_FROM_ENVELOPE_KEY], true, "{merged}");
    }
}

/// `TestValidateArgsFromMap::test_non_int_value_returns_none`,
/// `test_float_value_returns_none`, `test_none_value_returns_none`,
/// `test_mixed_valid_and_invalid_returns_none`;
/// `TestEnqueueWorkflowTask::test_args_from_non_int_values_fails_task`,
/// `test_corrupt_args_from_string_fails_task`: a stored `args_from` that is
/// not a map of integer indexes fails the node alone with
/// `WORKER_SERIALIZATION_ERROR`, and no task row is written.
#[tokio::test]
#[serial]
#[ignore = "waits on #1222"]
async fn a_promoted_node_whose_args_from_is_not_a_map_of_indexes_fails_alone() {
    let pool = shared_pool().await;
    for args_from in [
        r#"{"x": "not_an_int"}"#,
        r#"{"x": 1.5}"#,
        r#"{"x": null}"#,
        r#"{"x": 0, "y": "bad"}"#,
        r#""{broken""#,
    ] {
        let workflow_id = seed_completed_root_and_dependent(&pool).await;
        sqlx::query(
            "UPDATE horsies_workflow_tasks SET args_from = $2::jsonb
             WHERE workflow_id = $1 AND task_index = 1",
        )
        .bind(workflow_id)
        .bind(args_from)
        .execute(&pool)
        .await
        .expect("store the args_from");

        let promoted = recover(&pool, workflow_id, 1, &empty_registry()).await;

        let (status, task_id, result) = node_state(&pool, workflow_id, 1).await;
        remove_tree(&pool, workflow_id).await;
        assert!(matches!(promoted, Ok(true)), "{args_from}: {promoted:?}");
        assert_eq!(status, "FAILED", "{args_from}");
        assert_eq!(task_id, None, "{args_from}");
        let error = stored_node_error(result.as_deref());
        assert_eq!(
            error.error_code.to_string(),
            "WORKER_SERIALIZATION_ERROR",
            "{args_from}"
        );
    }
}

/// `TestEnqueueWorkflowTask::test_no_row_returns_none`,
/// `TestCheckAndPromoteTask::test_mark_ready_race_returns`: a node that
/// another promotion claimed after it was read is left to that promotion:
/// the claim finds no PENDING node, and nothing is written.
#[tokio::test]
#[serial]
async fn a_promotion_that_finds_its_node_already_claimed_writes_nothing() {
    let pool = shared_pool().await;
    let workflow_id = seed_completed_root_and_dependent(&pool).await;
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
    let tasks_before = queued_task_count(&pool).await;

    let enqueued = enqueue_workflow_task(
        &pool,
        workflow_id,
        &task,
        &HashMap::new(),
        &empty_registry(),
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await;

    let (status, task_id, result) = node_state(&pool, workflow_id, 1).await;
    let tasks_after = queued_task_count(&pool).await;
    remove_tree(&pool, workflow_id).await;
    assert!(enqueued.is_ok(), "{enqueued:?}");
    assert_eq!(status, "READY");
    assert_eq!(task_id, None);
    assert_eq!(result, None);
    assert_eq!(tasks_after, tasks_before);
}

/// `TestCheckAndPromoteTask::test_config_row_not_found`: the recovery of a
/// node that is not PENDING, or of an index with no node, promotes nothing.
#[tokio::test]
#[serial]
async fn the_recovery_of_a_node_that_is_not_pending_promotes_nothing() {
    let pool = shared_pool().await;
    let workflow_id = seed_completed_root_and_dependent(&pool).await;
    let tasks_before = queued_task_count(&pool).await;

    let completed = recover(&pool, workflow_id, 0, &empty_registry()).await;
    let missing = recover(&pool, workflow_id, 9, &empty_registry()).await;

    let (status, _task_id, _result) = node_state(&pool, workflow_id, 0).await;
    let tasks_after = queued_task_count(&pool).await;
    remove_tree(&pool, workflow_id).await;
    assert!(matches!(completed, Ok(false)), "{completed:?}");
    assert!(matches!(missing, Ok(false)), "{missing:?}");
    assert_eq!(status, "COMPLETED");
    assert_eq!(tasks_after, tasks_before);
}

/// `TestCheckAndPromoteTask::test_no_dependencies_returns`, with the core's
/// rule (#1224): a PENDING node with no dependencies meets its join `all`
/// (0 of 0 dependencies terminal), so recovery promotes it and mints its task
/// row. Python leaves such a node PENDING; the core keeps its rule, because it
/// recovers a root that a start left PENDING.
#[tokio::test]
#[serial]
async fn a_pending_node_without_dependencies_is_promoted() {
    let pool = shared_pool().await;
    let workflow_id = seed_workflow(&pool).await;
    seed_task_node(&pool, workflow_id, 0, &[], "PENDING").await;

    let promoted = recover(&pool, workflow_id, 0, &empty_registry()).await;

    let (status, task_id, _result) = node_state(&pool, workflow_id, 0).await;
    remove_tree(&pool, workflow_id).await;
    assert!(matches!(promoted, Ok(true)), "{promoted:?}");
    assert_eq!(status, "ENQUEUED");
    assert!(task_id.is_some(), "the promoted node links its task row");
}

/// Stored task options of the promoted node 1 that do not read, with
/// Python's message for each (`_build_enqueued_task_params`, and the baseline
/// `horsies_workflow_task_options_refusal`).
fn refused_options() -> Vec<(String, &'static str)> {
    vec![
        (
            "{not-json".to_owned(),
            "Corrupt task_options JSON for task 1",
        ),
        (
            "\"just_a_string\"".to_owned(),
            "Invalid task_options payload for task 1: expected object",
        ),
        (retry_options(-1), "Invalid retry_policy for task 1: "),
    ]
}

/// Promotes node 1 with `options` and returns its status, its task and its
/// stored error. Node 1 must leave PENDING.
async fn promote_with_options(
    pool: &PgPool,
    options: &str,
) -> (String, Option<Uuid>, TaskError, Uuid) {
    let workflow_id = seed_completed_root_and_dependent(pool).await;
    sqlx::query(
        "UPDATE horsies_workflow_tasks SET task_options = $2
         WHERE workflow_id = $1 AND task_index = 1",
    )
    .bind(workflow_id)
    .bind(options)
    .execute(pool)
    .await
    .expect("store the task options");

    let promoted = recover(pool, workflow_id, 1, &empty_registry()).await;

    let (status, task_id, result) = node_state(pool, workflow_id, 1).await;
    remove_tree(pool, workflow_id).await;
    assert!(matches!(promoted, Ok(true)), "{options}: {promoted:?}");
    (
        status,
        task_id,
        stored_node_error(result.as_deref()),
        workflow_id,
    )
}

/// `TestEnqueueWorkflowTask::test_corrupt_task_options_json_fails_task`,
/// `test_task_options_non_dict_fails_task`;
/// `TestPromotionReadsTheStoredOptions::test_stored_options_fail_only_that_node`
/// (`test_workflow_child_start_task_options.py`);
/// `TestWorkflowPaths::test_node_promotion_refuses_a_negative_limit`
/// (`test_stored_max_retries.py`): stored options that do not read fail the
/// promoted node alone with `WORKER_SERIALIZATION_ERROR` naming the workflow
/// and the node, and no task row is written.
#[tokio::test]
#[serial]
async fn a_promoted_node_whose_stored_options_do_not_read_fails_alone() {
    let pool = shared_pool().await;
    for (options, _python_message) in refused_options() {
        let (status, task_id, error, workflow_id) = promote_with_options(&pool, &options).await;

        assert_eq!(status, "FAILED", "{options}");
        assert_eq!(task_id, None, "{options}");
        assert_eq!(
            error.error_code.to_string(),
            "WORKER_SERIALIZATION_ERROR",
            "{options}"
        );
        assert_eq!(
            error.data,
            Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 1})),
            "{options}"
        );
    }
}

/// The message half of the tests of
/// [`a_promoted_node_whose_stored_options_do_not_read_fails_alone`]: the node
/// stores Python's message for each refusal.
#[tokio::test]
#[serial]
#[ignore = "waits on #1223"]
async fn a_promoted_node_refused_for_its_stored_options_stores_python_s_message() {
    let pool = shared_pool().await;
    for (options, python_message) in refused_options() {
        let (_status, _task_id, error, _workflow_id) = promote_with_options(&pool, &options).await;

        let message = error.message.unwrap_or_default();
        assert!(
            message.starts_with(python_message),
            "{options}: {message:?} does not start with {python_message:?}"
        );
    }
}

/// `TestEnqueueWorkflowTask::test_corrupt_kwargs_json_fails_task`: a stored
/// node input that does not parse fails the node alone with
/// `WORKER_SERIALIZATION_ERROR`, and no task row is written.
#[tokio::test]
#[serial]
#[ignore = "waits on #1222"]
async fn a_promoted_node_whose_stored_input_does_not_parse_fails_alone() {
    let pool = shared_pool().await;
    let workflow_id = seed_completed_root_and_dependent(&pool).await;
    update_node(
        &pool,
        workflow_id,
        "UPDATE horsies_workflow_tasks SET input = 'broken{json'
         WHERE workflow_id = $1 AND task_index = 1",
    )
    .await;

    let promoted = recover(&pool, workflow_id, 1, &empty_registry()).await;

    let (status, task_id, result) = node_state(&pool, workflow_id, 1).await;
    remove_tree(&pool, workflow_id).await;
    assert!(matches!(promoted, Ok(true)), "{promoted:?}");
    assert_eq!(status, "FAILED");
    assert_eq!(task_id, None);
    assert_eq!(
        stored_node_error(result.as_deref()).error_code.to_string(),
        "WORKER_SERIALIZATION_ERROR"
    );
}

/// A task node of a child spec: index 0, no dependency, on [`QUEUE`].
pub(super) fn child_task_node(
    task_options: Option<&str>,
    good_until: Option<chrono::DateTime<chrono::Utc>>,
) -> AnyNode {
    AnyNode {
        task_name: "promotion_branch_child_task".to_owned(),
        input_json: "{}".to_owned(),
        dependencies: Vec::new(),
        args_from: HashMap::new(),
        workflow_ctx_from: None,
        workflow_ctx_from_refs: None,
        queue: Some(QUEUE.to_owned()),
        priority: Some(100),
        allow_failed_deps: false,
        join: JoinType::All,
        min_success: None,
        good_until,
        index: 0,
        node_id: Some("child_0".to_owned()),
        task_options_json: task_options.map(str::to_owned),
        is_subworkflow: false,
        sub_definition_key: None,
        sub_workflow_params: SubWorkflowParams::NotGiven,
    }
}

/// A registry that resolves [`CHILD_KEY`] to a child with the one task node `child_root`.
pub(super) fn child_registry(child_root: AnyNode) -> WorkflowSpecRegistry {
    let mut registry = empty_registry();
    registry
        .register(RegisteredWorkflowSpec {
            spec: WorkflowSpec {
                name: "promotion_branch_child".to_owned(),
                definition_key: Some(CHILD_KEY.to_owned()),
                tasks: vec![child_root],
                on_error: OnError::Fail,
                output_index: None,
                success_policy: None,
            },
            spec_builder: None,
        })
        .unwrap_or_else(|error| panic!("register the child: {error}"));
    registry
}

/// Node 0 COMPLETED, and node 1, a sub-workflow node of [`CHILD_KEY`] that
/// waits for node 0, PENDING, in a new top-level workflow. Returns the
/// workflow id.
pub(super) async fn seed_sub_workflow_dependent(pool: &PgPool) -> Uuid {
    let workflow_id = seed_workflow(pool).await;
    seed_sub_workflow_nodes(pool, workflow_id).await;
    workflow_id
}

/// The nodes of [`seed_sub_workflow_dependent`] in `workflow_id`.
pub(super) async fn seed_sub_workflow_nodes(pool: &PgPool, workflow_id: Uuid) {
    seed_task_node(pool, workflow_id, 0, &[], "COMPLETED").await;
    let ok = serde_json::to_string(&TaskResult::Ok(serde_json::json!(7))).unwrap();
    sqlx::query(
        "UPDATE horsies_workflow_tasks SET result = $2 WHERE workflow_id = $1 AND task_index = 0",
    )
    .bind(workflow_id)
    .bind(ok)
    .execute(pool)
    .await
    .expect("store the result of node 0");
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, status,
             is_subworkflow, sub_workflow_name, sub_definition_key, sub_workflow_params, created_at
         ) VALUES (
             'NONE', $1, $2, 1, 'node_1', 'promotion_branch_child', '{}', 'default', 100, '{0}',
             FALSE, 'all', 'PENDING', TRUE, 'promotion_branch_child', $3, 'NOT_GIVEN', NOW()
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(CHILD_KEY)
    .execute(pool)
    .await
    .expect("seed the sub-workflow node");
}

/// `TestChildStartReadsTheTaskFunctionOptions::test_valid_options_of_a_fast_root_reach_the_insert`
/// (`test_workflow_child_start_task_options.py`): with options that read,
/// the child start writes the child and its root task row.
#[tokio::test]
#[serial]
async fn a_child_whose_root_options_read_is_started() {
    let pool = shared_pool().await;
    let workflow_id = seed_sub_workflow_dependent(&pool).await;
    let registry = child_registry(child_task_node(Some(r#"{"retry_policy": null}"#), None));

    let promoted = recover(&pool, workflow_id, 1, &registry).await;

    let (status, _task_id, _result) = node_state(&pool, workflow_id, 1).await;
    let children = child_count(&pool, workflow_id).await;
    let root_tasks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM horsies_workflow_tasks n
         JOIN horsies_workflows w ON w.id = n.workflow_id
         WHERE w.parent_workflow_id = $1 AND n.task_id IS NOT NULL",
    )
    .bind(workflow_id)
    .fetch_one(&pool)
    .await
    .expect("count the linked child roots");
    remove_tree(&pool, workflow_id).await;
    assert!(matches!(promoted, Ok(true)), "{promoted:?}");
    assert_eq!(status, "RUNNING");
    assert_eq!(children, 1);
    assert_eq!(root_tasks, 1);
}

/// `TestChildStartReadsTheTaskFunctionOptions::test_options_of_a_fast_root_raise_before_any_insert`
/// (unparseable, not-an-object) and
/// `test_options_merged_with_good_until_raise_before_any_insert`: task
/// options of a child root that do not read raise from the child start; the
/// step rolls back, so no child row stays and the parent node is not failed.
#[tokio::test]
#[serial]
async fn a_child_whose_root_options_do_not_read_raises_and_writes_nothing() {
    let pool = shared_pool().await;
    let deadline = chrono::Utc::now() + chrono::Duration::hours(1);
    for (options, good_until) in [
        ("{not-json", None),
        ("\"just a string\"", None),
        ("{not-json", Some(deadline)),
    ] {
        let workflow_id = seed_sub_workflow_dependent(&pool).await;
        let registry = child_registry(child_task_node(Some(options), good_until));

        let promoted = recover(&pool, workflow_id, 1, &registry).await;

        let (status, task_id, result) = node_state(&pool, workflow_id, 1).await;
        let children = child_count(&pool, workflow_id).await;
        remove_tree(&pool, workflow_id).await;
        assert!(promoted.is_err(), "{options} {good_until:?}: {promoted:?}");
        assert_eq!(status, "PENDING", "{options} {good_until:?}");
        assert_eq!(task_id, None, "{options} {good_until:?}");
        assert_eq!(result, None, "{options} {good_until:?}");
        assert_eq!(children, 0, "{options} {good_until:?}");
    }
}

/// `TestWorkflowPaths::test_the_fast_path_root_refuses_a_negative_limit`
/// (`test_stored_max_retries.py`): the start refuses a root whose retry
/// policy has a negative `max_retries` before it writes any row.
#[tokio::test]
#[serial]
async fn a_root_with_a_negative_retry_limit_is_refused_before_any_row() {
    let pool = shared_pool().await;
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let workflow_id = Uuid::new_v4();
    let negative_limit = retry_options(-1);
    let mut root = child_task_node(Some(negative_limit.as_str()), None);
    root.task_name = "promotion_branch_negative_limit".to_owned();
    let spec = WorkflowSpec {
        name: "promotion_branch_negative_limit".to_owned(),
        definition_key: Some("tests.promotion_branch.negative_limit.v1".to_owned()),
        tasks: vec![root],
        on_error: OnError::Fail,
        output_index: None,
        success_policy: None,
    };

    let started = crate::workflow_engine::start::start_workflow::<serde_json::Value>(
        &broker,
        &spec,
        Some(workflow_id),
        &empty_registry(),
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await;

    let refused = match started {
        Ok(_handle) => panic!("the start must be refused"),
        Err(refused) => refused,
    };
    let rows: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM horsies_workflows WHERE id = $1),
                (SELECT count(*) FROM horsies_tasks WHERE task_name = $2)",
    )
    .bind(workflow_id)
    .bind("promotion_branch_negative_limit")
    .fetch_one(&pool)
    .await
    .expect("count the rows of the start");
    assert_eq!(refused.code, WorkflowStartErrorCode::ValidationFailed);
    assert!(!refused.retryable);
    assert!(
        refused
            .message
            .contains("task_options of workflow node 0 (promotion_branch_negative_limit) refused"),
        "{}",
        refused.message
    );
    assert_eq!(rows, (0, 0));
}

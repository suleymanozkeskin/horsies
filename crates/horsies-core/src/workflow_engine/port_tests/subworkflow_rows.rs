//! Sub-workflow nodes through the engine: the child start, the child links,
//! the depth limit, start failures, and the parent advance. Python:
//! `tests/integration/test_workflow_subworkflow.py`.

use std::sync::Arc;

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    advance_parent_of, child_of, child_specs_of, code, err, execute, finish_node, node,
    node_status, ok, shared_pool, spec, start, stored_result, stored_result_error,
    sub_workflow_node, task_node, try_finish_task, workflow, workflow_status, IsolatedDatabase,
};
use crate::core::workflow::node::{JoinType, SubWorkflowParams};
use crate::core::{
    AnyNode, HorsiesError, SubWorkflowSummary, TaskError, TaskResult, WorkflowSpec, WorkflowStatus,
};
use crate::test_support::child_specs::{TestBuildRefused, TestChildBuild, TestChildSpecs};
use crate::workflow_engine::start::MAX_SUBWORKFLOW_DEPTH;

/// A child with one task node that is its output (Python `StartChildWorkflow`).
fn start_child() -> WorkflowSpec {
    let mut child = spec("child_workflow_start", vec![task_node(0, "sub_child_task")]);
    child.output_index = Some(0);
    child
}

/// A second one-node child (Python `ImportableChildWorkflow`).
fn importable_child() -> WorkflowSpec {
    let mut child = spec(
        "importable_child_workflow",
        vec![task_node(0, "importable_child_task")],
    );
    child.output_index = Some(0);
    child
}

/// A one-node child whose summary the parent reads (Python `SummaryChildWorkflow`).
fn summary_child() -> WorkflowSpec {
    let mut child = spec(
        "summary_child_workflow",
        vec![task_node(0, "summary_child_task")],
    );
    child.output_index = Some(0);
    child
}

fn subworkflow_children() -> TestChildSpecs {
    child_specs_of(&[start_child(), importable_child(), summary_child()])
}

fn after(mut node: AnyNode, dependencies: &[usize]) -> AnyNode {
    node.dependencies = dependencies.to_vec();
    node
}

/// A sub-workflow node at `index` of the definition `definition_key`, which
/// gives `input` as params.
fn dynamic_node(
    index: usize,
    name: &str,
    definition_key: &str,
    input: serde_json::Value,
) -> AnyNode {
    AnyNode {
        input_json: input.to_string(),
        sub_workflow_params: SubWorkflowParams::Given,
        sub_definition_key: Some(definition_key.to_owned()),
        ..sub_workflow_node(index, &spec(name, Vec::new()))
    }
}

/// Finishes node 0 of the child that node `task_index` of `parent_id` started,
/// and returns the child id (Python `_complete_child_workflow`).
async fn complete_child(
    pool: &PgPool,
    parent_id: Uuid,
    task_index: i32,
    outcome: &TaskResult<serde_json::Value>,
    child_specs: &TestChildSpecs,
) -> Uuid {
    let child_id = child_of(pool, parent_id, task_index).await;
    let _task = finish_node(pool, child_id, 0, outcome, child_specs).await;
    child_id
}

async fn children_of(pool: &PgPool, parent_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_workflows WHERE parent_workflow_id = $1")
        .bind(parent_id)
        .fetch_one(pool)
        .await
        .expect("count the children")
}

/// `TestSubworkflowIntegration.test_subworkflow_starts_on_dependency_complete`
/// and `test_recovery_ready_subworkflow_not_started`: the dependency
/// completion starts the child, links it to the parent node, and stamps its
/// `sent_at`.
#[tokio::test]
#[serial]
async fn a_dependency_completion_starts_the_child_and_links_it() {
    let pool = shared_pool().await;
    let child_specs = subworkflow_children();
    let mut parent = spec(
        "parent_workflow_start",
        vec![
            task_node(0, "sub_parent_task"),
            after(sub_workflow_node(1, &start_child()), &[0]),
        ],
    );
    parent.output_index = Some(1);
    let parent_id = start(&pool, &parent, &child_specs).await;

    let _a = finish_node(&pool, parent_id, 0, &ok(serde_json::json!(4)), &child_specs).await;

    let sub = node(&pool, parent_id, 1).await;
    assert_eq!(sub.status, "RUNNING");
    let child_id = sub.sub_workflow_id.expect("the child started");
    let (linked_parent, linked_index, sent_at): (
        Option<Uuid>,
        Option<i32>,
        Option<chrono::DateTime<chrono::Utc>>,
    ) = sqlx::query_as(
        "SELECT parent_workflow_id, parent_task_index, sent_at FROM horsies_workflows WHERE id = $1",
    )
    .bind(child_id)
    .fetch_one(&pool)
    .await
    .expect("read the child row");
    assert_eq!(linked_parent, Some(parent_id));
    assert_eq!(linked_index, Some(1));
    assert!(sent_at.is_some());
}

/// `TestSubworkflowIntegration.test_subworkflow_completion_does_not_rewrite_terminal_parent_node`
/// (child completed, child failed): a replayed child completion leaves a
/// terminal parent node and the parent's error as they are.
#[tokio::test]
#[serial]
async fn a_replayed_child_completion_does_not_rewrite_a_terminal_parent_node() {
    let pool = shared_pool().await;
    let child_specs = subworkflow_children();
    for child_status in ["COMPLETED", "FAILED"] {
        let mut parent = spec(
            "parent_terminal_replay_guard",
            vec![sub_workflow_node(0, &start_child())],
        );
        parent.output_index = Some(0);
        let parent_id = start(&pool, &parent, &child_specs).await;
        let child_id = child_of(&pool, parent_id, 0).await;
        let preserved = serde_json::to_string(&ok(serde_json::json!(11))).expect("encode");
        sqlx::query(
            "UPDATE horsies_workflow_tasks
             SET status = 'COMPLETED', result = $2,
                 completed_at = TIMESTAMPTZ '2026-01-01 00:00:00+00'
             WHERE workflow_id = $1 AND task_index = 0",
        )
        .bind(parent_id)
        .bind(&preserved)
        .execute(&pool)
        .await
        .expect("pin the parent node");
        let child_result = match child_status {
            "COMPLETED" => Some(serde_json::to_string(&ok(serde_json::json!(99))).expect("encode")),
            _ => None,
        };
        let child_error = match child_status {
            "FAILED" => Some(
                serde_json::to_string(
                    &TaskError::new(
                        "SUBWORKFLOW_REPLAY_PROBE",
                        "child failed for replay-guard test",
                    )
                    .expect("a user error code"),
                )
                .expect("encode"),
            ),
            _ => None,
        };
        sqlx::query(
            "UPDATE horsies_workflows
             SET status = $2, result = $3, error = $4, completed_at = NOW()
             WHERE id = $1",
        )
        .bind(child_id)
        .bind(child_status)
        .bind(&child_result)
        .bind(&child_error)
        .execute(&pool)
        .await
        .expect("end the child");

        advance_parent_of(&pool, child_id, &child_specs).await;

        let replayed = node(&pool, parent_id, 0).await;
        assert_eq!(replayed.status, "COMPLETED", "{child_status}");
        assert_eq!(replayed.result.as_deref(), Some(preserved.as_str()));
        assert_eq!(replayed.sub_workflow_summary, None);
        assert_eq!(
            replayed.completed_at,
            Some(
                chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                    .expect("a fixed time")
                    .with_timezone(&chrono::Utc)
            )
        );
        assert_eq!(
            workflow(&pool, parent_id).await.error,
            None,
            "{child_status}"
        );
        super::support::end_workflows(&pool, &[parent_id, child_id]).await;
    }
}

/// A build that holds its definition `key`, one level deeper, as its only
/// root node (Python `RootNestingWorkflow`). Its params are `{"level": n}`.
fn root_nesting(key: &'static str) -> TestChildBuild {
    Arc::new(
        move |input: &str| -> Result<WorkflowSpec, TestBuildRefused> {
            let level = nesting_level(input)?;
            let mut nesting = spec(
                "root_nesting_workflow",
                vec![dynamic_node(
                    0,
                    "root_nesting_workflow",
                    key,
                    serde_json::json!({"level": level + 1}),
                )],
            );
            nesting.definition_key = Some(key.to_owned());
            nesting.output_index = Some(0);
            Ok(nesting)
        },
    )
}

/// Nests through root nodes to the depth limit; there, a task runs first and
/// the deeper child waits for it (Python `DependentNestingWorkflow`).
fn dependent_nesting(key: &'static str) -> TestChildBuild {
    Arc::new(
        move |input: &str| -> Result<WorkflowSpec, TestBuildRefused> {
            let level = nesting_level(input)?;
            let deeper = serde_json::json!({"level": level + 1});
            let mut nesting = match level < i64::from(MAX_SUBWORKFLOW_DEPTH) {
                true => {
                    let mut root = spec(
                        "dependent_nesting_workflow",
                        vec![dynamic_node(0, "dependent_nesting_workflow", key, deeper)],
                    );
                    root.output_index = Some(0);
                    root
                }
                false => {
                    let mut leaf = spec(
                        "dependent_nesting_workflow",
                        vec![
                            task_node(0, "dependent_nesting_leaf_task"),
                            after(
                                dynamic_node(1, "dependent_nesting_workflow", key, deeper),
                                &[0],
                            ),
                        ],
                    );
                    leaf.output_index = Some(1);
                    leaf
                }
            };
            nesting.definition_key = Some(key.to_owned());
            Ok(nesting)
        },
    )
}

fn nesting_level(input: &str) -> Result<i64, TestBuildRefused> {
    serde_json::from_str::<serde_json::Value>(input)
        .ok()
        .and_then(|params| params.get("level").and_then(serde_json::Value::as_i64))
        .ok_or_else(|| {
            TestBuildRefused::Failed(HorsiesError::new(format!(
                "nesting params without a level: {input}"
            )))
        })
}

async fn workflows_at_depth(pool: &PgPool, depth: i32) -> Vec<Uuid> {
    sqlx::query_scalar("SELECT id FROM horsies_workflows WHERE depth = $1")
        .bind(depth)
        .fetch_all(pool)
        .await
        .expect("read the workflows at a depth")
}

async fn deeper_workflow_count(pool: &PgPool, depth: i32) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_workflows WHERE depth > $1")
        .bind(depth)
        .fetch_one(pool)
        .await
        .expect("count the deeper workflows")
}

async fn failed_node_code(pool: &PgPool, workflow_id: Uuid, task_index: i32) -> Option<String> {
    let failed = node(pool, workflow_id, task_index).await;
    assert_eq!(failed.status, "FAILED");
    code(&stored_result_error(
        &failed.result.expect("a FAILED node stores its error"),
    ))
}

/// `TestSubworkflowIntegration.test_a_root_nesting_stops_at_the_depth_limit`:
/// the workflow at depth 32 fails its root node, and the failure reaches the
/// root workflow.
#[tokio::test]
#[serial]
async fn a_root_nesting_stops_at_the_depth_limit() {
    let db = IsolatedDatabase::create().await;
    let key = "tests.port.root_nesting.v1";
    let child_specs = TestChildSpecs::empty().with_build(key, root_nesting(key));
    let mut parent = spec(
        "root_nesting_parent",
        vec![dynamic_node(
            0,
            "root_nesting_workflow",
            key,
            serde_json::json!({"level": 1}),
        )],
    );
    parent.output_index = Some(0);
    // The start builds the whole nesting in one statement: the depth-32
    // workflow fails its root node, and the start advances each parent node
    // up to the root workflow before it commits (`horsies_workflow_start_advance`).
    let root_id = start(&db.pool, &parent, &child_specs).await;

    let deepest = workflows_at_depth(&db.pool, MAX_SUBWORKFLOW_DEPTH).await;
    assert_eq!(deepest.len(), 1);
    assert_eq!(
        deeper_workflow_count(&db.pool, MAX_SUBWORKFLOW_DEPTH).await,
        0
    );
    assert_eq!(
        failed_node_code(&db.pool, deepest[0], 0).await.as_deref(),
        Some("SUBWORKFLOW_DEPTH_LIMIT_REACHED")
    );
    assert_eq!(workflow_status(&db.pool, root_id).await, "FAILED");
    db.drop().await;
}

/// `TestSubworkflowIntegration.test_a_dependent_nesting_stops_at_the_depth_limit`
#[tokio::test]
#[serial]
async fn a_dependent_nesting_stops_at_the_depth_limit() {
    let db = IsolatedDatabase::create().await;
    let key = "tests.port.dependent_nesting.v1";
    let child_specs = TestChildSpecs::empty().with_build(key, dependent_nesting(key));
    let mut parent = spec(
        "dependent_nesting_parent",
        vec![dynamic_node(
            0,
            "dependent_nesting_workflow",
            key,
            serde_json::json!({"level": 1}),
        )],
    );
    parent.output_index = Some(0);
    let _root = start(&db.pool, &parent, &child_specs).await;

    let deepest = workflows_at_depth(&db.pool, MAX_SUBWORKFLOW_DEPTH).await;
    assert_eq!(deepest.len(), 1);
    let _leaf = finish_node(
        &db.pool,
        deepest[0],
        0,
        &ok(serde_json::json!(2)),
        &child_specs,
    )
    .await;

    assert_eq!(
        deeper_workflow_count(&db.pool, MAX_SUBWORKFLOW_DEPTH).await,
        0
    );
    assert_eq!(
        failed_node_code(&db.pool, deepest[0], 1).await.as_deref(),
        Some("SUBWORKFLOW_DEPTH_LIMIT_REACHED")
    );
    db.drop().await;
}

/// `TestSubworkflowIntegration.test_child_task_prepare_failure_does_not_commit_partial_child`:
/// a later child node that cannot be prepared (no resolved queue) fails the
/// parent node with `WORKFLOW_ENQUEUE_FAILED` naming that node, and no child
/// row is kept.
#[tokio::test]
#[serial]
async fn a_child_node_that_cannot_be_prepared_fails_the_parent_node_and_keeps_no_child() {
    let db = IsolatedDatabase::create().await;
    let key = "tests.port.later_node_prep_failure_child_workflow.v1";
    // The client refuses the child it built: its later node has no resolved
    // queue, so the client cannot prepare it.
    let child_specs = TestChildSpecs::empty().with_build(
        key,
        Arc::new(|_input: &str| -> Result<WorkflowSpec, TestBuildRefused> {
            Err(TestBuildRefused::Failed(HorsiesError::new(
                "workflow 'later_node_prep_failure_child_workflow' node 'node_1' \
                 (task 'partial_child_later_task') has no resolved queue",
            )))
        }),
    );
    let mut parent = spec(
        "parent_partial_child_prepare_failure",
        vec![
            task_node(0, "partial_child_parent_task"),
            after(
                dynamic_node(
                    1,
                    "later_node_prep_failure_child_workflow",
                    key,
                    serde_json::json!({}),
                ),
                &[0],
            ),
        ],
    );
    parent.output_index = Some(1);
    let parent_id = start(&db.pool, &parent, &child_specs).await;

    let _a = finish_node(
        &db.pool,
        parent_id,
        0,
        &ok(serde_json::json!(1)),
        &child_specs,
    )
    .await;

    let sub = node(&db.pool, parent_id, 1).await;
    assert_eq!(sub.status, "FAILED");
    assert_eq!(sub.sub_workflow_id, None);
    let error = stored_result_error(&sub.result.expect("the node stores its failure"));
    assert_eq!(code(&error).as_deref(), Some("WORKFLOW_ENQUEUE_FAILED"));
    assert!(
        error
            .message
            .as_deref()
            .is_some_and(|message| message.contains("partial_child_later_task")),
        "{error:?}"
    );
    assert_eq!(children_of(&db.pool, parent_id).await, 0);
    db.drop().await;
}

/// `TestSubworkflowIntegration.test_nested_subworkflow_kwargs_write_failure_raises_and_rolls_back`:
/// a database failure while the child start writes its nested sub-workflow
/// node is an error of phase 2, and the step rolls back: no child row, the
/// parent node stays PENDING, and the upstream node is not written.
#[tokio::test]
#[serial]
async fn a_failure_while_the_child_start_writes_a_nested_node_rolls_the_step_back() {
    let db = IsolatedDatabase::create().await;
    let grandchild_key = "tests.port.nested_sub_partial_grandchild.v1";
    let child_key = "tests.port.nested_sub_partial_child.v1";
    let mut grandchild = spec(
        "nested_sub_partial_grandchild",
        vec![task_node(0, "nested_sub_partial_grandchild_task")],
    );
    grandchild.definition_key = Some(grandchild_key.to_owned());
    grandchild.output_index = Some(0);
    let mut child = spec(
        "nested_sub_partial_child",
        vec![
            task_node(0, "nested_sub_partial_child_first_task"),
            after(sub_workflow_node(1, &grandchild), &[0]),
        ],
    );
    child.definition_key = Some(child_key.to_owned());
    child.output_index = Some(1);
    let child_specs = child_specs_of(&[grandchild, child.clone()]);
    let mut parent = spec(
        "parent_nested_sub_prepare_failure",
        vec![
            task_node(0, "nested_sub_partial_parent_task"),
            after(sub_workflow_node(1, &child), &[0]),
        ],
    );
    parent.output_index = Some(1);
    let parent_id = start(&db.pool, &parent, &child_specs).await;
    let upstream_task = node(&db.pool, parent_id, 0)
        .await
        .task_id
        .expect("the upstream node is enqueued");
    execute(
        &db.pool,
        "CREATE FUNCTION port_test_refuse_nested_nodes() RETURNS trigger
         LANGUAGE plpgsql AS $body$
         BEGIN
             RAISE EXCEPTION 'nested node refused by the test' USING ERRCODE = 'XX000';
         END
         $body$",
    )
    .await;
    execute(
        &db.pool,
        "CREATE TRIGGER port_test_refuse_nested_nodes BEFORE INSERT ON horsies_workflow_tasks
         FOR EACH ROW WHEN (NEW.is_subworkflow) EXECUTE FUNCTION port_test_refuse_nested_nodes()",
    )
    .await;

    let phase2 = try_finish_task(
        &db.pool,
        upstream_task,
        &ok(serde_json::json!(1)),
        &child_specs,
    )
    .await;

    assert!(phase2.is_err(), "{phase2:?}");
    assert_ne!(node_status(&db.pool, parent_id, 0).await, "COMPLETED");
    let sub = node(&db.pool, parent_id, 1).await;
    assert_eq!(sub.status, "PENDING");
    assert_eq!(sub.sub_workflow_id, None);
    assert_eq!(sub.result, None);
    assert_eq!(children_of(&db.pool, parent_id).await, 0);
    db.drop().await;
}

/// `TestSubworkflowIntegration.test_parallel_child_one_fails_downstream_skipped`
#[tokio::test]
#[serial]
async fn one_failed_child_of_two_skips_a_join_all_dependent() {
    let pool = shared_pool().await;
    let child_specs = subworkflow_children();
    let mut parent = spec(
        "parent_parallel_one_fails",
        vec![
            sub_workflow_node(0, &start_child()),
            sub_workflow_node(1, &importable_child()),
            after(task_node(2, "parallel_fail_post"), &[0, 1]),
        ],
    );
    parent.output_index = Some(2);
    let parent_id = start(&pool, &parent, &child_specs).await;

    let _c1 = complete_child(
        &pool,
        parent_id,
        0,
        &err("C1_FAIL", "child 1 failed"),
        &child_specs,
    )
    .await;
    let _c2 = complete_child(&pool, parent_id, 1, &ok(serde_json::json!(5)), &child_specs).await;

    assert_eq!(node_status(&pool, parent_id, 2).await, "SKIPPED");
}

/// `TestSubworkflowIntegration.test_parallel_child_failures_keep_first_parent_error`:
/// while a third root runs, the parent stays RUNNING and keeps the error of
/// the first failed child.
#[tokio::test]
#[serial]
async fn two_failed_children_keep_the_first_error_on_the_parent() {
    let pool = shared_pool().await;
    let child_specs = subworkflow_children();
    let parent = spec(
        "parent_parallel_failures_first_error",
        vec![
            sub_workflow_node(0, &start_child()),
            sub_workflow_node(1, &importable_child()),
            task_node(2, "parallel_fail_blocker"),
        ],
    );
    let parent_id = start(&pool, &parent, &child_specs).await;

    let child1 = complete_child(
        &pool,
        parent_id,
        0,
        &err("C1_FAIL", "child 1 failed"),
        &child_specs,
    )
    .await;
    let child2 = complete_child(
        &pool,
        parent_id,
        1,
        &err("C2_FAIL", "child 2 failed"),
        &child_specs,
    )
    .await;

    let row = workflow(&pool, parent_id).await;
    assert_eq!(row.status, "RUNNING");
    let error = row.error.expect("the parent stores the first error");
    assert!(error.contains(&child1.to_string()), "{error}");
    assert!(!error.contains(&child2.to_string()), "{error}");
    super::support::end_workflows(&pool, &[parent_id]).await;
}

/// `TestSubworkflowIntegration.test_subworkflow_join_any_starts_on_first_dep`
#[tokio::test]
#[serial]
async fn a_join_any_child_starts_on_the_first_completed_dependency() {
    let pool = shared_pool().await;
    let child_specs = subworkflow_children();
    let mut sub = after(sub_workflow_node(2, &start_child()), &[0, 1]);
    sub.join = JoinType::Any;
    let mut parent = spec(
        "parent_join_any",
        vec![task_node(0, "join_any_a"), task_node(1, "join_any_b"), sub],
    );
    parent.output_index = Some(2);
    let parent_id = start(&pool, &parent, &child_specs).await;

    let _a = finish_node(
        &pool,
        parent_id,
        0,
        &ok(serde_json::json!(10)),
        &child_specs,
    )
    .await;

    let started = node(&pool, parent_id, 2).await;
    assert_eq!(started.status, "RUNNING");
    assert!(started.sub_workflow_id.is_some());
}

/// `TestSubworkflowIntegration.test_subworkflow_mid_chain_result_flows`
#[tokio::test]
#[serial]
async fn a_child_output_completes_its_node_and_enqueues_the_dependent() {
    let pool = shared_pool().await;
    let child_specs = subworkflow_children();
    let mut parent = spec(
        "parent_mid_chain",
        vec![
            task_node(0, "mid_chain_a"),
            after(sub_workflow_node(1, &start_child()), &[0]),
            after(task_node(2, "mid_chain_post"), &[1]),
        ],
    );
    parent.output_index = Some(2);
    let parent_id = start(&pool, &parent, &child_specs).await;

    let _a = finish_node(&pool, parent_id, 0, &ok(serde_json::json!(6)), &child_specs).await;
    let _child = complete_child(
        &pool,
        parent_id,
        1,
        &ok(serde_json::json!(50)),
        &child_specs,
    )
    .await;

    assert_eq!(node_status(&pool, parent_id, 2).await, "ENQUEUED");
    let sub = node(&pool, parent_id, 1).await;
    assert_eq!(sub.status, "COMPLETED");
    match stored_result(&sub.result.expect("the node stores the child output")) {
        TaskResult::Ok(value) => assert_eq!(value, serde_json::json!(50)),
        TaskResult::Err(error) => panic!("the node failed: {error:?}"),
    }
}

/// `TestSubworkflowIntegration.test_summary_for_failed_child_has_error_fields`
#[tokio::test]
#[serial]
async fn a_failed_child_summary_holds_its_counts_and_error_summary() {
    let pool = shared_pool().await;
    let child_specs = subworkflow_children();
    let mut post = after(task_node(1, "summary_fail_post"), &[0]);
    post.workflow_ctx_from = Some(vec!["node_0".to_owned()]);
    post.allow_failed_deps = true;
    let parent_id = start(
        &pool,
        &spec(
            "parent_summary_failed",
            vec![sub_workflow_node(0, &summary_child()), post],
        ),
        &child_specs,
    )
    .await;

    let _child = complete_child(
        &pool,
        parent_id,
        0,
        &err("CHILD_ERR", "Something went wrong"),
        &child_specs,
    )
    .await;

    let summary: SubWorkflowSummary = serde_json::from_str(
        node(&pool, parent_id, 0)
            .await
            .sub_workflow_summary
            .as_deref()
            .expect("the node stores the child summary"),
    )
    .expect("the summary decodes");
    assert_eq!(summary.status, WorkflowStatus::Failed);
    assert!(summary.failed_tasks >= 1);
    assert!(summary.total_tasks >= 1);
    assert!(summary.error_summary.is_some_and(|text| !text.is_empty()));
    assert_eq!(node_status(&pool, parent_id, 1).await, "ENQUEUED");
}

/// `TestSubworkflowIntegration.test_multiple_args_from_keys`: the child
/// builder gets both `args_from` keys, and the child starts.
#[tokio::test]
#[serial]
async fn a_child_node_with_two_args_from_keys_starts() {
    let pool = shared_pool().await;
    let key = "tests.port.multi_param_child.v1";
    let child_specs = TestChildSpecs::empty().with_build(
        key,
        Arc::new(
            move |input: &str| -> Result<WorkflowSpec, TestBuildRefused> {
                let params: serde_json::Value = serde_json::from_str(input).map_err(|error| {
                    TestBuildRefused::Failed(HorsiesError::new(format!(
                        "params do not decode: {error}"
                    )))
                })?;
                match (params.get("first"), params.get("second")) {
                    (Some(_), Some(_)) => {
                        let mut child = spec(
                            "multi_param_child_workflow",
                            vec![task_node(0, "multi_param_child_task")],
                        );
                        child.definition_key = Some(key.to_owned());
                        child.output_index = Some(0);
                        Ok(child)
                    }
                    _ => Err(TestBuildRefused::Failed(HorsiesError::new(format!(
                        "params miss first or second: {input}"
                    )))),
                }
            },
        ),
    );
    let mut sub = after(
        dynamic_node(2, "multi_param_child_workflow", key, serde_json::json!({})),
        &[0, 1],
    );
    sub.args_from.insert("first".to_owned(), 0);
    sub.args_from.insert("second".to_owned(), 1);
    let mut parent = spec(
        "parent_multi_args",
        vec![
            task_node(0, "multi_args_a"),
            task_node(1, "multi_args_b"),
            sub,
        ],
    );
    parent.output_index = Some(2);
    let parent_id = start(&pool, &parent, &child_specs).await;

    let _a = finish_node(
        &pool,
        parent_id,
        0,
        &ok(serde_json::json!(10)),
        &child_specs,
    )
    .await;
    let _b = finish_node(
        &pool,
        parent_id,
        1,
        &ok(serde_json::json!(20)),
        &child_specs,
    )
    .await;

    let started = node(&pool, parent_id, 2).await;
    assert_eq!(started.status, "RUNNING");
    assert!(started.sub_workflow_id.is_some());
}

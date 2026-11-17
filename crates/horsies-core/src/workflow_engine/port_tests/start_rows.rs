//! What a workflow start stores. Python: `tests/integration/test_workflow_start.py`,
//! `test_workflow_start_batched.py`, `test_subworkflow_start_batched.py`,
//! `test_workflow_kwargs_compat.py`, `test_subworkflow_priority_binding.py`,
//! and `tests/e2e/test_layer7_subworkflow_priority.py`.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, SubsecRound, Utc};
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    broker, child_of, claim_once, claim_pass, finish_node, node, node_status, node_task_input, ok,
    registry_of, seed_node, seed_workflow, shared_pool, spec, start, sub_workflow_node, task_node,
    try_start, workflow_status, IsolatedDatabase, NodeSeed, WorkflowSeed, PORT_QUEUE,
};
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::registry::workflow::RegisteredWorkflowDefinition;
use crate::core::workflow::node::TaskDefaults;
use crate::core::{
    HorsiesError, OnError, SpecBuildError, WorkflowSpec, WorkflowSpecRegistry,
    WorkflowStartErrorCode,
};
use crate::workflow_engine::lifecycle::resume_workflow;
use crate::workflow_engine::recovery::recover_stuck_workflows;

/// The stored task options of a task with a fixed retry policy of
/// `max_retries` retries (Python `RetryPolicy.fixed([60] * n,
/// auto_retry_for=['TASK_EXCEPTION'])`).
fn retry_options(max_retries: usize) -> String {
    serde_json::json!({
        "retry_policy": {
            "max_retries": max_retries,
            "intervals": vec![60; max_retries],
            "backoff_strategy": "fixed",
            "jitter": true,
            "auto_retry_for": ["TASK_EXCEPTION"],
        }
    })
    .to_string()
}

/// The node columns that a start writes from the spec.
#[derive(Debug, sqlx::FromRow)]
struct StoredNode {
    dependencies: Vec<i32>,
    args_from: Option<serde_json::Value>,
    workflow_ctx_from: Option<Vec<String>>,
    allow_failed_deps: bool,
    task_options: Option<String>,
    max_retries: Option<i32>,
}

async fn stored_node(pool: &PgPool, workflow_id: Uuid, task_index: i32) -> StoredNode {
    sqlx::query_as(
        "SELECT dependencies, args_from, workflow_ctx_from::text[] AS workflow_ctx_from,
                allow_failed_deps, task_options, max_retries
         FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = $2",
    )
    .bind(workflow_id)
    .bind(task_index)
    .fetch_one(pool)
    .await
    .expect("read the stored node")
}

/// The workflow columns that a start writes from the spec.
#[derive(Debug, sqlx::FromRow)]
struct StoredWorkflow {
    on_error: String,
    output_task_index: Option<i32>,
    sent_at: DateTime<Utc>,
}

async fn stored_workflow(pool: &PgPool, workflow_id: Uuid) -> StoredWorkflow {
    sqlx::query_as(
        "SELECT on_error, output_task_index, sent_at FROM horsies_workflows WHERE id = $1",
    )
    .bind(workflow_id)
    .fetch_one(pool)
    .await
    .expect("read the stored workflow")
}

/// The live task row of a node: (status, max_retries, good_until, task_options).
async fn node_task_row(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
) -> (String, i32, Option<DateTime<Utc>>, Option<String>) {
    sqlx::query_as(
        "SELECT t.status, t.max_retries, t.good_until, t.task_options
         FROM horsies_tasks t
         JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
         WHERE wt.workflow_id = $1 AND wt.task_index = $2",
    )
    .bind(workflow_id)
    .bind(task_index)
    .fetch_one(pool)
    .await
    .expect("the node has a live task")
}

async fn workflow_rows_named(pool: &PgPool, name: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_workflows WHERE name = $1")
        .bind(name)
        .fetch_one(pool)
        .await
        .expect("count the workflow rows")
}

async fn node_count(pool: &PgPool, workflow_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_workflow_tasks WHERE workflow_id = $1")
        .bind(workflow_id)
        .fetch_one(pool)
        .await
        .expect("count the nodes")
}

/// A name no other test run uses, for a spec whose rows a test counts by name.
fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

/// `n_roots` roots, and with `with_dependent` one node that waits for all of
/// them (Python `_fanout_spec`).
fn fanout(name: &str, n_roots: usize, with_dependent: bool) -> WorkflowSpec {
    let mut tasks: Vec<_> = (0..n_roots)
        .map(|index| {
            let mut root = task_node(index, "batched_root");
            root.input_json = format!(r#"{{"value": {index}}}"#);
            root
        })
        .collect();
    if with_dependent {
        let mut dependent = task_node(n_roots, "batched_dependent");
        dependent.dependencies = (0..n_roots).collect();
        tasks.push(dependent);
    }
    spec(name, tasks)
}

/// `test_workflow_start.py::TestWorkflowStart.test_start_sets_workflow_sent_at_from_python_call_site`
#[tokio::test]
#[serial]
async fn start_stores_the_caller_start_time_as_sent_at() {
    let pool = shared_pool().await;
    let before = Utc::now().trunc_subsecs(6);

    let workflow_id = start(
        &pool,
        &spec("sent_at_wf", vec![task_node(0, "sent_at_task")]),
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await;

    let after = Utc::now();
    let sent_at = stored_workflow(&pool, workflow_id).await.sent_at;
    assert!(
        before <= sent_at && sent_at <= after,
        "{before} <= {sent_at} <= {after}"
    );
}

/// `TestWorkflowStart.test_custom_workflow_id`
#[tokio::test]
#[serial]
async fn start_uses_the_caller_workflow_id() {
    let pool = shared_pool().await;
    let workflow_id = Uuid::new_v4();

    let handle = try_start(
        &pool,
        &spec("custom_id_wf", vec![task_node(0, "custom_id_task")]),
        Some(workflow_id),
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await
    .expect("start");

    assert_eq!(handle.workflow_id(), workflow_id);
    assert_eq!(workflow_status(&pool, workflow_id).await, "RUNNING");
}

/// `TestWorkflowStart.test_on_error_stored`
#[tokio::test]
#[serial]
async fn start_stores_the_failure_policy() {
    let pool = shared_pool().await;
    for (on_error, stored) in [(OnError::Fail, "fail"), (OnError::Pause, "pause")] {
        let mut workflow_spec = spec("on_error_wf", vec![task_node(0, "on_error_task")]);
        workflow_spec.on_error = on_error;

        let workflow_id = start(
            &pool,
            &workflow_spec,
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
        )
        .await;

        assert_eq!(stored_workflow(&pool, workflow_id).await.on_error, stored);
    }
}

/// `TestWorkflowStart.test_output_task_index_stored` and
/// `.test_output_task_index_null_when_no_output`
#[tokio::test]
#[serial]
async fn start_stores_the_output_index_or_null() {
    let pool = shared_pool().await;
    let mut second = task_node(1, "output_b");
    second.dependencies = vec![0];
    let mut with_output = spec("output_wf", vec![task_node(0, "output_a"), second]);
    with_output.output_index = Some(1);
    let without_output = spec("no_output_wf", vec![task_node(0, "no_output_a")]);
    let registry = WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);

    let with_id = start(&pool, &with_output, &registry).await;
    let without_id = start(&pool, &without_output, &registry).await;

    assert_eq!(
        stored_workflow(&pool, with_id).await.output_task_index,
        Some(1)
    );
    assert_eq!(
        stored_workflow(&pool, without_id).await.output_task_index,
        None
    );
}

/// `TestWorkflowStart.test_dependencies_stored_as_array`,
/// `.test_args_from_stored_as_jsonb`, `.test_workflow_ctx_from_stored` and
/// `.test_allow_failed_deps_stored`
#[tokio::test]
#[serial]
async fn start_stores_the_edges_and_the_dataflow_of_each_node() {
    let pool = shared_pool().await;
    let mut join = task_node(2, "edges_join");
    join.dependencies = vec![0, 1];
    join.args_from.insert("left".to_owned(), 0);
    join.workflow_ctx_from = Some(vec!["node_1".to_owned()]);
    join.allow_failed_deps = true;
    let workflow_id = start(
        &pool,
        &spec(
            "edges_wf",
            vec![task_node(0, "edges_a"), task_node(1, "edges_b"), join],
        ),
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await;

    let root = stored_node(&pool, workflow_id, 0).await;
    assert!(root.dependencies.is_empty());
    assert_eq!(root.args_from, None);
    assert_eq!(root.workflow_ctx_from, None);
    assert!(!root.allow_failed_deps);
    let stored_join = stored_node(&pool, workflow_id, 2).await;
    assert_eq!(stored_join.dependencies, vec![0, 1]);
    assert_eq!(stored_join.args_from, Some(serde_json::json!({"left": 0})));
    assert_eq!(
        stored_join.workflow_ctx_from,
        Some(vec!["node_1".to_owned()])
    );
    assert!(stored_join.allow_failed_deps);
}

/// `TestWorkflowStart.test_retry_policy_honored_in_workflow_tasks`
#[tokio::test]
#[serial]
async fn a_root_node_stores_its_retry_options_and_its_task_gets_max_retries() {
    let pool = shared_pool().await;
    let mut root = task_node(0, "retry_wf_task");
    root.task_options_json = Some(retry_options(5));
    let workflow_id = start(
        &pool,
        &spec("retry_policy_test", vec![root]),
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await;

    let stored = stored_node(&pool, workflow_id, 0).await;
    let options = stored
        .task_options
        .expect("the node stores its task options");
    assert!(options.contains("retry_policy"), "{options}");
    assert_eq!(stored.max_retries, Some(5));
    let (_status, max_retries, _good_until, task_options) =
        node_task_row(&pool, workflow_id, 0).await;
    assert_eq!(max_retries, 5);
    assert!(task_options.is_some());
}

/// `TestWorkflowStart.test_retry_policy_honored_for_non_root_task`
#[tokio::test]
#[serial]
async fn a_promoted_node_task_gets_its_max_retries() {
    let pool = shared_pool().await;
    let registry = WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
    let mut child = task_node(1, "retry_child");
    child.dependencies = vec![0];
    child.task_options_json = Some(retry_options(4));
    let workflow_id = start(
        &pool,
        &spec("retry_non_root", vec![task_node(0, "retry_root"), child]),
        &registry,
    )
    .await;

    let _root = finish_node(&pool, workflow_id, 0, &ok(serde_json::json!(10)), &registry).await;

    let (_status, max_retries, _good_until, task_options) =
        node_task_row(&pool, workflow_id, 1).await;
    assert_eq!(max_retries, 4);
    assert!(task_options.is_some());
}

/// `TestWorkflowStart.test_duplicate_workflow_id_returns_existing_handle`
#[tokio::test]
#[serial]
async fn a_second_start_with_the_same_id_and_name_returns_the_existing_workflow() {
    let pool = shared_pool().await;
    let registry = WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
    let workflow_id = Uuid::new_v4();
    let first = spec("idempotent_wf", vec![task_node(0, "idempotent_a")]);
    let mut second = spec(
        "idempotent_wf",
        vec![task_node(0, "idempotent_b"), task_node(1, "idempotent_c")],
    );
    second.definition_key = first.definition_key.clone();

    let first_handle = try_start(&pool, &first, Some(workflow_id), &registry)
        .await
        .expect("first start");
    let second_handle = try_start(&pool, &second, Some(workflow_id), &registry)
        .await
        .expect("second start");

    assert_eq!(first_handle.workflow_id(), second_handle.workflow_id());
    assert_eq!(
        node_count(&pool, workflow_id).await,
        1,
        "the first spec's nodes only"
    );
}

/// `TestWorkflowStart.test_duplicate_workflow_id_with_different_name_is_an_id_conflict`
#[tokio::test]
#[serial]
async fn a_start_with_an_id_of_another_workflow_name_is_an_id_conflict() {
    let pool = shared_pool().await;
    let registry = WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
    let workflow_id = Uuid::new_v4();
    let first = spec("idempotent_name_guard_first", vec![task_node(0, "guard_a")]);
    let second = spec(
        "idempotent_name_guard_second",
        vec![task_node(0, "guard_b")],
    );
    let _first_handle = try_start(&pool, &first, Some(workflow_id), &registry)
        .await
        .expect("first start");

    let error = try_start(&pool, &second, Some(workflow_id), &registry)
        .await
        .expect_err("the id belongs to another workflow name");

    assert_eq!(error.code, WorkflowStartErrorCode::WorkflowIdConflict);
    assert!(!error.retryable);
    assert!(
        error.message.contains(&workflow_id.to_string()),
        "{}",
        error.message
    );
    assert!(
        error.message.contains("idempotent_name_guard_first"),
        "{}",
        error.message
    );
    assert_eq!(node_count(&pool, workflow_id).await, 1);
}

/// `TestWorkflowStart.test_concurrent_duplicate_workflow_id_returns_existing_handle`
#[tokio::test]
#[serial]
async fn two_concurrent_starts_with_one_id_make_one_workflow() {
    let pool = shared_pool().await;
    let registry = WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
    let workflow_id = Uuid::new_v4();
    let workflow_spec = spec(
        "idempotent_wf_concurrent",
        vec![task_node(0, "concurrent_a")],
    );

    let (first, second) = tokio::join!(
        try_start(&pool, &workflow_spec, Some(workflow_id), &registry),
        try_start(&pool, &workflow_spec, Some(workflow_id), &registry),
    );

    assert_eq!(first.expect("first start").workflow_id(), workflow_id);
    assert_eq!(second.expect("second start").workflow_id(), workflow_id);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .expect("count the workflow rows");
    assert_eq!(rows, 1);
    assert_eq!(node_count(&pool, workflow_id).await, 1);
}

/// `TestWorkflowStart.test_concurrent_auto_id_creates_distinct_workflows`
#[tokio::test]
#[serial]
async fn two_concurrent_starts_with_no_id_make_two_workflows() {
    let pool = shared_pool().await;
    let registry = WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
    let name = unique_name("auto_id_wf");
    let workflow_spec = spec(&name, vec![task_node(0, "auto_id_a")]);

    let (first, second) = tokio::join!(
        try_start(&pool, &workflow_spec, None, &registry),
        try_start(&pool, &workflow_spec, None, &registry),
    );

    let first_id = first.expect("first start").workflow_id();
    let second_id = second.expect("second start").workflow_id();
    assert_ne!(first_id, second_id);
    assert_eq!(workflow_rows_named(&pool, &name).await, 2);
}

/// `test_workflow_start_batched.py::TestBatchedStartShape.test_fast_roots_enqueued_linked_and_dependent_pending`
#[tokio::test]
#[serial]
async fn plain_roots_start_enqueued_with_linked_pending_tasks_and_a_dependent_waits() {
    let pool = shared_pool().await;
    let workflow_id = start(
        &pool,
        &fanout("batched_start", 3, true),
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await;

    for task_index in 0..3 {
        let root = node(&pool, workflow_id, task_index).await;
        assert_eq!(root.status, "ENQUEUED");
        assert!(root.started_at.is_some(), "a node starts at its enqueue");
        let task_id = root.task_id.expect("a root is linked to its task");
        let (status, fingerprint): (String, Option<Vec<u8>>) =
            sqlx::query_as("SELECT status, command_fingerprint FROM horsies_tasks WHERE id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .expect("the linked task row");
        assert_eq!(status, "PENDING");
        assert_eq!(
            fingerprint, None,
            "an engine-minted task carries no fingerprint"
        );
    }
    let dependent = node(&pool, workflow_id, 3).await;
    assert_eq!(dependent.status, "PENDING");
    assert_eq!(dependent.task_id, None);
}

/// The meta half of `test_fast_roots_enqueued_linked_and_dependent_pending`.
#[tokio::test]
#[serial]
async fn start_writes_the_workflow_meta_into_each_root_task() {
    let pool = shared_pool().await;
    let workflow_id = start(
        &pool,
        &fanout("batched_start_meta", 3, false),
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await;

    for task_index in 0..3 {
        let input = node_task_input(&pool, workflow_id, task_index).await;
        let meta = &input["__h_workflow_meta__"];
        assert_eq!(
            meta["workflow_id"],
            serde_json::json!(workflow_id.to_string())
        );
        assert_eq!(meta["task_index"], serde_json::json!(task_index));
        assert_eq!(meta["task_name"], serde_json::json!("batched_root"));
    }
}

/// `TestBatchedStartShape.test_idempotent_restart_returns_existing_handle`
#[tokio::test]
#[serial]
async fn a_restart_with_the_same_id_writes_nothing_more() {
    let pool = shared_pool().await;
    let registry = WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
    let workflow_id = Uuid::new_v4();
    let workflow_spec = fanout("batched_restart", 3, true);
    let _first = try_start(&pool, &workflow_spec, Some(workflow_id), &registry)
        .await
        .expect("first start");

    let again = try_start(&pool, &workflow_spec, Some(workflow_id), &registry)
        .await
        .expect("restart");

    assert_eq!(again.workflow_id(), workflow_id);
    assert_eq!(node_count(&pool, workflow_id).await, 4);
    let tasks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM horsies_tasks t
         JOIN horsies_workflow_tasks wt ON wt.task_id = t.id WHERE wt.workflow_id = $1",
    )
    .bind(workflow_id)
    .fetch_one(&pool)
    .await
    .expect("count the task rows");
    assert_eq!(tasks, 3);
}

/// `TestBatchedStartShape.test_serialization_failure_rolls_back_everything`:
/// a node input that the start cannot store fails the start, and no row stays.
#[tokio::test]
#[serial]
async fn a_node_input_that_cannot_be_stored_fails_the_start_and_leaves_no_row() {
    let pool = shared_pool().await;
    let name = unique_name("batched_start_bad_input");
    let mut workflow_spec = fanout(&name, 2, false);
    workflow_spec.tasks[1].input_json = "{not json".to_owned();

    let error = try_start(
        &pool,
        &workflow_spec,
        None,
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await
    .expect_err("the second node's input does not parse");

    assert_eq!(error.code, WorkflowStartErrorCode::ValidationFailed);
    assert_eq!(workflow_rows_named(&pool, &name).await, 0);
}

/// `TestBatchedStartShape.test_fast_root_carries_retry_policy_and_good_until`
#[tokio::test]
#[serial]
async fn a_plain_root_task_carries_its_retries_deadline_and_options() {
    let pool = shared_pool().await;
    let deadline = (Utc::now() + Duration::hours(1)).trunc_subsecs(6);
    let mut root = task_node(0, "batched_retry");
    root.task_options_json = Some(retry_options(5));
    root.good_until = Some(deadline);
    let workflow_id = start(
        &pool,
        &spec("batched_retry_wf", vec![root]),
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await;

    let (_status, max_retries, good_until, task_options) =
        node_task_row(&pool, workflow_id, 0).await;

    assert_eq!(max_retries, 5);
    assert_eq!(good_until, Some(deadline));
    let options: serde_json::Value =
        serde_json::from_str(&task_options.expect("the task stores its options"))
            .expect("the options decode");
    let stored_deadline = DateTime::parse_from_rfc3339(
        options["good_until"]
            .as_str()
            .expect("the options name the deadline"),
    )
    .expect("the deadline is RFC 3339");
    assert_eq!(stored_deadline, deadline);
    assert_eq!(options["retry_policy"]["max_retries"], serde_json::json!(5));
}

/// `TestBatchedStartShape.test_corrupt_task_options_propagate_and_roll_back`
#[tokio::test]
#[serial]
async fn task_options_that_do_not_parse_fail_the_start_and_leave_no_row() {
    let pool = shared_pool().await;
    let name = unique_name("batched_corrupt_wf");
    let mut root = task_node(0, "batched_corrupt");
    root.task_options_json = Some("{not-json".to_owned());

    let error = try_start(
        &pool,
        &spec(&name, vec![root]),
        None,
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await
    .expect_err("the root's task options do not parse");

    assert_eq!(error.code, WorkflowStartErrorCode::ValidationFailed);
    assert_eq!(workflow_rows_named(&pool, &name).await, 0);
}

/// A child definition whose spec builder fails (Python `_broken_build_def`).
fn broken_build_definition(key: &str) -> RegisteredWorkflowDefinition {
    RegisteredWorkflowDefinition {
        name: "broken_build_wf".to_owned(),
        definition_key: key.to_owned(),
        declared_children: Vec::new(),
        spec_builder: Arc::new(|_input| {
            Err(SpecBuildError::Failed(HorsiesError::new(
                "build_with refused",
            )))
        }),
    }
}

/// Live task rows of a workflow's nodes.
async fn linked_task_count(pool: &PgPool, workflow_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM horsies_tasks t
         JOIN horsies_workflow_tasks wt ON wt.task_id = t.id WHERE wt.workflow_id = $1",
    )
    .bind(workflow_id)
    .fetch_one(pool)
    .await
    .expect("count the live task rows")
}

/// Checks the pause gate of a start: `workflow_id` is PAUSED, its failed
/// slow root is FAILED, its plain root is READY with no task, and no task
/// row exists; a resume then enqueues the plain root.
async fn assert_paused_before_plain_roots_got_tasks(
    pool: &PgPool,
    workflow_id: Uuid,
    plain_root: i32,
    failed_root: i32,
    registry: &WorkflowSpecRegistry,
) {
    assert_eq!(node_status(pool, workflow_id, failed_root).await, "FAILED");
    let plain = node(pool, workflow_id, plain_root).await;
    assert_eq!(plain.status, "READY");
    assert_eq!(plain.task_id, None);
    assert_eq!(workflow_status(pool, workflow_id).await, "PAUSED");
    assert_eq!(linked_task_count(pool, workflow_id).await, 0);

    let resumed = resume_workflow(
        &broker(pool),
        workflow_id,
        registry,
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await
    .expect("resume");
    assert!(resumed);
    assert_eq!(node_status(pool, workflow_id, plain_root).await, "ENQUEUED");
}

/// `test_workflow_start_batched.py::TestStartPauseGate.test_pause_policy_slow_root_failure_reverts_fast_roots`:
/// the child of the sub-workflow root has no definition under its key.
#[tokio::test]
#[serial]
async fn a_root_child_that_cannot_load_pauses_the_workflow_before_plain_roots_get_tasks() {
    let db = IsolatedDatabase::create().await;
    let registry = WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
    let missing_child = spec(
        "pause_gate_child_wf",
        vec![task_node(0, "pause_gate_child")],
    );
    let mut workflow_spec = spec(
        "pause_gate",
        vec![
            task_node(0, "pause_gate_root"),
            sub_workflow_node(1, &missing_child),
        ],
    );
    workflow_spec.on_error = OnError::Pause;

    let workflow_id = try_start(&db.pool, &workflow_spec, None, &registry)
        .await
        .expect("the start succeeds; only the child node fails")
        .workflow_id();

    assert_paused_before_plain_roots_got_tasks(&db.pool, workflow_id, 0, 1, &registry).await;
    db.drop().await;
}

/// `TestStartPauseGate.test_queued_parent_propagation_pause_reverts_fast_roots`:
/// the child fails in its own start, because its only root is a child whose
/// builder fails, and the parent hears it before its plain roots get tasks.
#[tokio::test]
#[serial]
async fn a_child_that_fails_in_its_start_pauses_the_parent_before_plain_roots_get_tasks() {
    let db = IsolatedDatabase::create().await;
    let grandchild_key = "tests.port.pause_prop_grandchild.v1";
    let grandchild_stub = WorkflowSpec {
        definition_key: Some(grandchild_key.to_owned()),
        ..spec("broken_build_wf", vec![task_node(0, "unused")])
    };
    let mut bad_child = spec(
        "pause_prop_child_wf",
        vec![sub_workflow_node(0, &grandchild_stub)],
    );
    bad_child.output_index = Some(0);
    let mut registry = registry_of(&[bad_child.clone()]);
    registry
        .register_definition(broken_build_definition(grandchild_key))
        .expect("register the broken grandchild");
    let mut workflow_spec = spec(
        "pause_prop",
        vec![
            task_node(0, "pause_prop_root"),
            sub_workflow_node(1, &bad_child),
        ],
    );
    workflow_spec.on_error = OnError::Pause;

    let workflow_id = try_start(&db.pool, &workflow_spec, None, &registry)
        .await
        .expect("the start succeeds; only the child fails")
        .workflow_id();

    assert_paused_before_plain_roots_got_tasks(&db.pool, workflow_id, 0, 1, &registry).await;
    db.drop().await;
}

/// A chain of `n` task nodes, the first one with `root_options`.
fn child_chain(name: &str, n: usize, root_options: Option<&str>) -> WorkflowSpec {
    let mut tasks = Vec::new();
    for index in 0..n {
        let mut task = task_node(index, "bulk_child");
        task.input_json = format!(r#"{{"value": {index}}}"#);
        if index > 0 {
            task.dependencies = vec![index - 1];
        }
        tasks.push(task);
    }
    tasks[0].task_options_json = root_options.map(str::to_owned);
    let mut child = spec(name, tasks);
    child.output_index = Some(0);
    child
}

/// `test_subworkflow_start_batched.py::TestBulkChildStart.test_child_rows_equivalent_to_per_node_path`
#[tokio::test]
#[serial]
async fn a_child_start_enqueues_its_root_and_leaves_its_chain_pending() {
    let pool = shared_pool().await;
    let child = child_chain("bulk_child_wf", 3, None);
    let registry = registry_of(&[child.clone()]);
    let parent_id = start(
        &pool,
        &spec("bulk_parent", vec![sub_workflow_node(0, &child)]),
        &registry,
    )
    .await;
    let child_id = child_of(&pool, parent_id, 0).await;

    let root = node(&pool, child_id, 0).await;
    assert_eq!(root.status, "ENQUEUED");
    assert!(root.started_at.is_some());
    assert!(root.task_id.is_some());
    let (status, _max_retries, _good_until, _options) = node_task_row(&pool, child_id, 0).await;
    assert_eq!(status, "PENDING");
    for task_index in 1..3 {
        let dependent = node(&pool, child_id, task_index).await;
        assert_eq!(dependent.status, "PENDING");
        assert_eq!(dependent.task_id, None);
    }
}

/// The meta half of `test_child_rows_equivalent_to_per_node_path`.
#[tokio::test]
#[serial]
async fn a_child_start_writes_the_child_meta_into_its_root_task() {
    let pool = shared_pool().await;
    let child = child_chain("bulk_child_meta_wf", 2, None);
    let registry = registry_of(&[child.clone()]);
    let parent_id = start(
        &pool,
        &spec("bulk_parent_meta", vec![sub_workflow_node(0, &child)]),
        &registry,
    )
    .await;
    let child_id = child_of(&pool, parent_id, 0).await;

    let input = node_task_input(&pool, child_id, 0).await;

    assert_eq!(
        input["__h_workflow_meta__"]["workflow_id"],
        serde_json::json!(child_id.to_string())
    );
}

/// `TestBulkChildStart.test_corrupt_root_options_raise_and_roll_back_the_start`:
/// task options of a child root that do not parse fail the start, and no
/// parent or child row stays.
#[tokio::test]
#[serial]
async fn child_root_options_that_do_not_parse_fail_the_start_and_leave_no_row() {
    let pool = shared_pool().await;
    let child_name = unique_name("bulk_child_corrupt_wf");
    let parent_name = unique_name("bulk_parent_corrupt");
    let mut child = child_chain(&child_name, 2, Some("{not-json"));
    child.tasks[1].dependencies = Vec::new();
    let registry = registry_of(&[child.clone()]);

    let error = try_start(
        &pool,
        &spec(&parent_name, vec![sub_workflow_node(0, &child)]),
        None,
        &registry,
    )
    .await
    .expect_err("the child root's options do not parse");

    assert_eq!(error.code, WorkflowStartErrorCode::ValidationFailed);
    assert_eq!(workflow_rows_named(&pool, &parent_name).await, 0);
    assert_eq!(workflow_rows_named(&pool, &child_name).await, 0);
}

/// `TestBulkChildStart.test_pause_policy_slow_root_failure_reverts_fast_siblings`
#[tokio::test]
#[serial]
async fn a_child_root_that_cannot_build_pauses_the_child_before_its_plain_roots_get_tasks() {
    let db = IsolatedDatabase::create().await;
    let broken_key = "tests.port.broken_build.v1";
    let broken_stub = WorkflowSpec {
        definition_key: Some(broken_key.to_owned()),
        ..spec("broken_build_wf", vec![task_node(0, "unused")])
    };
    let mut child = spec(
        "bulk_child_pause_wf",
        vec![
            sub_workflow_node(0, &broken_stub),
            task_node(1, "bulk_child"),
        ],
    );
    child.output_index = Some(0);
    child.on_error = OnError::Pause;
    let mut registry = registry_of(&[child.clone()]);
    registry
        .register_definition(broken_build_definition(broken_key))
        .expect("register the broken child");

    let parent_id = start(
        &db.pool,
        &spec("bulk_parent_pause", vec![sub_workflow_node(0, &child)]),
        &registry,
    )
    .await;
    let child_id = child_of(&db.pool, parent_id, 0).await;

    assert_paused_before_plain_roots_got_tasks(&db.pool, child_id, 1, 0, &registry).await;
    db.drop().await;
}

/// `TestBulkChildStart.test_nested_parent_propagation_pause_reverts_child_fast_siblings`
#[tokio::test]
#[serial]
async fn a_grandchild_failure_pauses_the_child_before_its_plain_roots_get_tasks() {
    let db = IsolatedDatabase::create().await;
    let broken_key = "tests.port.nested_broken_build.v1";
    let broken_stub = WorkflowSpec {
        definition_key: Some(broken_key.to_owned()),
        ..spec("broken_build_wf", vec![task_node(0, "unused")])
    };
    let mut grandchild = spec(
        "nested_pause_grandchild",
        vec![sub_workflow_node(0, &broken_stub)],
    );
    grandchild.output_index = Some(0);
    let mut child = spec(
        "nested_pause_child",
        vec![
            task_node(0, "nested_pause_fast"),
            sub_workflow_node(1, &grandchild),
        ],
    );
    child.output_index = Some(0);
    child.on_error = OnError::Pause;
    let mut registry = registry_of(&[grandchild.clone(), child.clone()]);
    registry
        .register_definition(broken_build_definition(broken_key))
        .expect("register the broken grandchild");

    let parent_id = start(
        &db.pool,
        &spec("nested_pause_parent", vec![sub_workflow_node(0, &child)]),
        &registry,
    )
    .await;
    let child_id = child_of(&db.pool, parent_id, 0).await;

    assert_paused_before_plain_roots_got_tasks(&db.pool, child_id, 0, 1, &registry).await;
    db.drop().await;
}

/// `test_workflow_kwargs_compat.py::TestKwargsOnlyWrites.test_new_tasknode_persists_empty_task_args`:
/// a task node stores its input as one JSON value, and its task gets it.
/// (Python's `task_args` / `task_kwargs` pair is one `input` column on the
/// baseline: decision "How task input is stored".)
#[tokio::test]
#[serial]
async fn a_task_node_stores_its_input_as_one_value() {
    let pool = shared_pool().await;
    let mut root = task_node(0, "kwargs_only_task");
    root.input_json = r#"{"value": 1}"#.to_owned();
    let workflow_id = start(
        &pool,
        &spec("kwargs_only_wf", vec![root]),
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
    )
    .await;

    let stored: serde_json::Value =
        serde_json::from_str(&node(&pool, workflow_id, 0).await.input).expect("the input decodes");
    assert_eq!(stored, serde_json::json!({"value": 1}));
    assert_eq!(
        node_task_input(&pool, workflow_id, 0).await["value"],
        serde_json::json!(1)
    );
}

/// `TestKwargsOnlyWrites.test_new_subworkflow_node_persists_empty_task_args`:
/// a sub-workflow node that gives no params stores the empty input `{}`.
#[tokio::test]
#[serial]
async fn a_sub_workflow_node_with_no_params_stores_an_empty_input() {
    let pool = shared_pool().await;
    let child = spec(
        "kwargs_only_child",
        vec![task_node(0, "kwargs_only_child_task")],
    );
    let registry = registry_of(&[child.clone()]);
    let workflow_id = start(
        &pool,
        &spec("kwargs_only_parent", vec![sub_workflow_node(0, &child)]),
        &registry,
    )
    .await;

    let stored: serde_json::Value =
        serde_json::from_str(&node(&pool, workflow_id, 0).await.input).expect("the input decodes");
    assert_eq!(stored, serde_json::json!({}));
}

/// `TestRecoveryKwargsCompat.test_recovery_ready_kwargs_task_enqueued`: the
/// recovery pass enqueues a READY node with no task, with the node's input.
#[tokio::test]
#[serial]
async fn recovery_enqueues_a_ready_node_with_its_input() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 0, "READY").input(r#"{"value": 7}"#),
    )
    .await;

    let report = recover_stuck_workflows(
        &crate::broker::PostgresBroker::from_pool(db.pool.clone()),
        &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
        3,
        &RetentionConfig::default(),
    )
    .await
    .expect("recovery pass");

    assert_eq!(report.case1_ready_enqueued, 1, "{report:?}");
    assert_eq!(node_status(&db.pool, workflow_id, 0).await, "ENQUEUED");
    assert_eq!(
        node_task_input(&db.pool, workflow_id, 0).await["value"],
        serde_json::json!(7)
    );
    db.drop().await;
}

/// The queue and priority of the `scraping` queue (Python `SCRAPING_PRIORITY`).
const SCRAPING_QUEUE: &str = "scraping";
const SCRAPING_PRIORITY: i32 = 30;

/// A registry whose task `scrape_task` is declared on the `scraping` queue,
/// and a child definition that builds its spec directly, with no queue or
/// priority on the node (Python `ClientSync.build_with`).
fn scraping_registry(child_key: &str) -> WorkflowSpecRegistry {
    let mut registry =
        WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
    registry.set_task_defaults_map(HashMap::from([(
        "scrape_task".to_owned(),
        TaskDefaults {
            queue_name: SCRAPING_QUEUE.to_owned(),
            priority: SCRAPING_PRIORITY as u32,
        },
    )]));
    registry.set_queue_priority_map(HashMap::from([
        (SCRAPING_QUEUE.to_owned(), SCRAPING_PRIORITY as u32),
        (PORT_QUEUE.to_owned(), 100),
    ]));
    registry
        .register_definition(RegisteredWorkflowDefinition {
            name: "client_sync".to_owned(),
            definition_key: child_key.to_owned(),
            declared_children: Vec::new(),
            spec_builder: Arc::new(|_input| {
                let mut scrape = task_node(0, "scrape_task");
                scrape.queue = None;
                scrape.priority = None;
                scrape.node_id = Some("scrape".to_owned());
                let mut child = spec("client_sync", vec![scrape]);
                child.output_index = Some(0);
                Ok(child)
            }),
        })
        .expect("register the child definition");
    registry
}

/// The parent of the scraping child.
fn scraping_parent(child_key: &str) -> WorkflowSpec {
    let stub = WorkflowSpec {
        definition_key: Some(child_key.to_owned()),
        ..spec("client_sync", vec![task_node(0, "scrape_task")])
    };
    spec("scraping_parent", vec![sub_workflow_node(0, &stub)])
}

/// `test_subworkflow_priority_binding.py::TestSubworkflowPriorityBinding.test_direct_build_with_binds_queue_priority`
#[tokio::test]
#[serial]
async fn a_child_built_by_its_builder_binds_the_queue_priority_on_node_and_task() {
    let db = IsolatedDatabase::create().await;
    let child_key = "tests.port.client_sync.v1";
    let parent_id = start(
        &db.pool,
        &scraping_parent(child_key),
        &scraping_registry(child_key),
    )
    .await;
    let child_id = child_of(&db.pool, parent_id, 0).await;

    let (task_queue, task_priority, node_priority): (String, i32, i32) = sqlx::query_as(
        "SELECT t.queue_name, t.priority, wt.priority
         FROM horsies_workflow_tasks wt JOIN horsies_tasks t ON t.id = wt.task_id
         WHERE wt.workflow_id = $1 AND wt.node_id = 'scrape'",
    )
    .bind(child_id)
    .fetch_one(&db.pool)
    .await
    .expect("the scrape node has its task");

    assert_eq!(task_queue, SCRAPING_QUEUE);
    assert_eq!(task_priority, SCRAPING_PRIORITY);
    assert_eq!(node_priority, SCRAPING_PRIORITY);
    db.drop().await;
}

/// `tests/e2e/test_layer7_subworkflow_priority.py::test_subworkflow_child_binds_queue_priority_and_claim_order`:
/// the child's task and a task sent later on the same queue have the same
/// priority, and the claim takes the child's task first.
#[tokio::test]
#[serial]
async fn a_built_child_task_keeps_fifo_order_with_a_later_send_on_its_queue() {
    let db = IsolatedDatabase::create().await;
    let child_key = "tests.port.client_sync_order.v1";
    let parent_id = start(
        &db.pool,
        &scraping_parent(child_key),
        &scraping_registry(child_key),
    )
    .await;
    let child_id = child_of(&db.pool, parent_id, 0).await;
    let scrape_task = node(&db.pool, child_id, 0)
        .await
        .task_id
        .expect("the scrape node has its task");
    let direct_task = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, task_options, execution,
             retention_class_key, sent_at, created_at, command_fingerprint_version,
             command_fingerprint, idempotency_key_digest, status, enqueued_at,
             claimed_at, claimed_by_worker_id, claim_expires_at, started_at,
             finalizing_at, finalizing_by_worker_id, worker_pid, worker_hostname,
             worker_process_name, retry_count, max_retries, next_retry_at, good_until,
             is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id,
             rerun_root_task_id
         ) VALUES (
             $1, 'direct_task', $2, $3, '{}', NULL, 'ISOLATED', 'forever',
             NOW(), NOW(), 1, sha256(convert_to($1::text, 'UTF8')), NULL, 'PENDING',
             clock_timestamp() + INTERVAL '1 second',
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, NULL, NULL,
             FALSE, NOW(), FALSE, NULL, NULL
         )",
    )
    .bind(direct_task)
    .bind(SCRAPING_QUEUE)
    .bind(SCRAPING_PRIORITY)
    .execute(&db.pool)
    .await
    .expect("send a direct task after the child's task");
    let mut one_at_a_time = claim_pass(SCRAPING_QUEUE, 1);
    one_at_a_time.max_claim_batch = 1;

    let claimed = claim_once(&db.pool, &one_at_a_time).await;

    let ids: Vec<Uuid> = claimed.iter().map(|row| row.id).collect();
    assert_eq!(ids, vec![scrape_task]);
    let direct_priority: i32 =
        sqlx::query_scalar("SELECT priority FROM horsies_tasks WHERE id = $1")
            .bind(direct_task)
            .fetch_one(&db.pool)
            .await
            .expect("read the direct task");
    assert_eq!(direct_priority, SCRAPING_PRIORITY);
    db.drop().await;
}

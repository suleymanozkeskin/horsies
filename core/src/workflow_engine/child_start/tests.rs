//! Tests of the owed child start.
//!
//! - The host build (pure): the kind that each owed child start passes.
//! - Each step that can owe a child start commits the child with the step
//!   (database, needs `0005_workflow_functions.sql`): phase 2 in the
//!   transaction form, the parent advance, the start with a failed-node
//!   record, and the resume. Each checks the parent node RUNNING with its
//!   `sub_workflow_id` and the child rows present.

use std::collections::HashMap;
use std::sync::Arc;

use super::*;
use crate::broker::workflow_start::ClientStartOutcome;
use crate::broker::PostgresBroker;
use crate::core::lifecycle::TerminalizationOutcome;
use crate::core::workflow::node::{JoinType, SubWorkflowParams};
use crate::core::{AnyNode, OnError, TaskResult, WorkflowSpec};
use crate::db::CheckoutClass;
use crate::test_support::child_specs::{TestBuildRefused, TestChildBuild, TestChildSpecs};
use crate::test_support::sql::{self, TestPool};
use crate::test_support::terminal_writes;
use crate::workflow_engine::child_spec::{ChildSpecRequest, ChildSpecSource};
use crate::workflow_engine::lifecycle::{pause_workflow, resume_client_workflow};
use crate::workflow_engine::start_statement::{merged_child_input, NodeInput};
use crate::workflow_engine::steps::{
    advance_parents, node_finish_in_transaction, ChildStartSource, NodeFinishDisposition,
    ParentAdvance,
};

const CHILD_KEY: &str = "tests.child_start.child.v1";
const CHILD_NAME: &str = "child_start_child";
/// The name of the parent workflow that [`seed_workflow`] writes.
const PARENT_NAME: &str = "child_start_parent";
const WORKER: &str = "child-start-worker";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The host build of `owed`. The test source answers when its future is
/// made, so the future ends on its first poll.
fn host_build(
    owed: &OwedChildStart,
    build: &dyn ChildSpecSource,
) -> Result<ChildStartKind, WorkflowError> {
    futures::executor::block_on(child_start_kind(owed, build))
}

/// The request that the host build of `owed` makes for a child at depth
/// `parent_depth + 1`.
fn request_of(owed: &OwedChildStart) -> ChildSpecRequest {
    ChildSpecRequest {
        parent_workflow_id: owed.workflow_id,
        parent_workflow_name: owed.workflow_name.clone(),
        task_index: owed.task_index,
        child_depth: owed.parent_depth + 1,
        definition_key: owed.sub_definition_key.clone(),
        params: owed.sub_workflow_params,
        node_input: owed.node_input.clone(),
        args_from: owed.args_from.clone(),
        sources: owed.sources.clone(),
    }
}

/// The child under [`CHILD_KEY`]: one task node on the `default` queue,
/// which is its output.
fn child_spec() -> WorkflowSpec {
    WorkflowSpec {
        name: CHILD_NAME.to_owned(),
        definition_key: Some(CHILD_KEY.to_owned()),
        tasks: vec![AnyNode {
            task_name: "child_start_task".to_owned(),
            input_json: "{}".to_owned(),
            dependencies: Vec::new(),
            args_from: HashMap::new(),
            workflow_ctx_from: None,
            queue: Some("default".to_owned()),
            priority: Some(100),
            allow_failed_deps: false,
            join: JoinType::All,
            min_success: None,
            good_until: None,
            index: 0,
            node_id: Some(format!("{CHILD_NAME}:0")),
            task_options_json: None,
            is_subworkflow: false,
            sub_definition_key: None,
            sub_workflow_params: SubWorkflowParams::NotGiven,
        }],
        on_error: OnError::Fail,
        output_index: Some(0),
        success_policy: None,
    }
}

/// A sub-workflow node at `index` of the child `name` under
/// `definition_key`, with the empty input and no params.
fn sub_workflow_node(index: usize, name: &str, definition_key: &str) -> AnyNode {
    AnyNode {
        task_name: name.to_owned(),
        input_json: "{}".to_owned(),
        dependencies: Vec::new(),
        args_from: HashMap::new(),
        workflow_ctx_from: None,
        queue: None,
        priority: None,
        allow_failed_deps: false,
        join: JoinType::All,
        min_success: None,
        good_until: None,
        index,
        node_id: None,
        task_options_json: None,
        is_subworkflow: true,
        sub_definition_key: Some(definition_key.to_owned()),
        sub_workflow_params: SubWorkflowParams::NotGiven,
    }
}

/// The child specs with [`child_spec`] under [`CHILD_KEY`].
fn children_with_child() -> TestChildSpecs {
    TestChildSpecs::empty().with_spec(child_spec())
}

/// An owed child start of node 1 of `workflow_id` at depth `parent_depth`,
/// whose child is registered under `definition_key`.
fn owed(workflow_id: Uuid, parent_depth: i32, definition_key: &str) -> OwedChildStart {
    OwedChildStart {
        workflow_id,
        workflow_name: PARENT_NAME.to_owned(),
        task_index: 1,
        node_input: "{}".to_owned(),
        args_from: None,
        sub_workflow_name: Some(CHILD_NAME.to_owned()),
        sub_definition_key: Some(definition_key.to_owned()),
        sub_workflow_params: SubWorkflowParams::NotGiven,
        parent_depth,
        sources: Vec::new(),
    }
}

/// The error of a `FAILURE` kind.
fn failure_error(kind: ChildStartKind) -> crate::core::TaskError {
    let ChildStartKind::Failure { failure_result } = kind else {
        panic!("expected a FAILURE child start, got {kind:?}");
    };
    let stored: TaskResult<serde_json::Value> =
        serde_json::from_str(&failure_result).expect("an error envelope");
    let TaskResult::Err(error) = stored else {
        panic!("a failure result stores an error");
    };
    error
}

async fn test_broker() -> PostgresBroker {
    PostgresBroker::from_test_url(
        crate::test_support::migrated_pool().await.url(),
        crate::ClientFamily::Rust,
    )
}

/// A workflow row; a child names its parent, the node index 0 and depth 1.
async fn seed_workflow(
    pool: &TestPool,
    id: Uuid,
    status: &str,
    parent: Option<Uuid>,
    result: Option<&str>,
) {
    sql::query(
        "INSERT INTO horsies_workflows (
             id, name, status, on_error, definition_key, depth, result,
             root_workflow_id, parent_workflow_id, parent_task_index,
             sent_at, created_at, started_at, updated_at, completed_at
         ) VALUES ($1, 'child_start_parent', $2, 'fail', $3, $4, $5, $6, $7, $8,
                   NOW(), NOW(), NOW(), NOW(),
                   CASE WHEN $2 IN ('COMPLETED', 'FAILED') THEN NOW() END)",
    )
    .bind(id)
    .bind(status)
    .bind(format!("tests.child_start.{id}"))
    .bind(if parent.is_some() { 1_i32 } else { 0_i32 })
    .bind(result)
    .bind(parent.unwrap_or(id))
    .bind(parent)
    .bind(parent.map(|_| 0_i32))
    .execute(pool)
    .await
    .expect("seed workflow");
}

/// A sub-workflow node at `index` of `workflow_id` whose child is registered
/// under `definition_key`.
async fn seed_subworkflow_node(
    pool: &TestPool,
    workflow_id: Uuid,
    index: i32,
    status: &str,
    dependencies: &str,
    child: Option<Uuid>,
    definition_key: &str,
) {
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, status,
             is_subworkflow, sub_workflow_id, sub_workflow_name, sub_definition_key,
             sub_workflow_params, created_at, started_at
         ) VALUES (
             'NONE', $1, $2, $3, $4, $5, '{}', 'default', 100, $6::int[], FALSE, 'all', $7,
             TRUE, $8, $5, $9, 'NOT_GIVEN', NOW(),
             CASE WHEN $7 = 'RUNNING' THEN NOW() END
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(index)
    .bind(format!("node_{index}"))
    .bind(CHILD_NAME)
    .bind(dependencies)
    .bind(status)
    .bind(child)
    .bind(definition_key)
    .execute(pool)
    .await
    .expect("seed sub-workflow node");
}

/// A RUNNING task claimed by [`WORKER`], with the process facts of its start,
/// behind the RUNNING task node 0 of `workflow_id`, from which a sub-workflow
/// node is reachable.
async fn seed_running_task_node(pool: &TestPool, workflow_id: Uuid, task_id: Uuid) {
    sql::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, status, sent_at, enqueued_at,
             started_at, claimed_at, claimed_by_worker_id, is_workflow_task, retry_count,
             max_retries, command_fingerprint_version, command_fingerprint,
             retention_class_key, retain_rerun_input, created_at, updated_at, execution,
             worker_pid, worker_hostname, worker_process_name
         ) VALUES (
             $1, 'child_start_root', 'default', 100, '{}', 'RUNNING', NOW(), NOW(), NOW(),
             NOW(), $2, TRUE, 0, 0, NULL, NULL, 'standard_30d', FALSE, NOW(), NOW(),
             'ISOLATED', 4242, 'child-start-host', 'child-start-process'
         )",
    )
    .bind(task_id)
    .bind(WORKER)
    .execute(pool)
    .await
    .expect("seed task");
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, status,
             is_subworkflow, task_id, created_at, started_at, max_retries, execution,
             retention_class_key
         ) VALUES (
             'REACHABLE', $1, $2, 0, 'node_0', 'child_start_root', '{}', 'default', 100, '{}',
             FALSE, 'all', 'RUNNING', FALSE, $3, NOW(), NOW(), 0, 'ISOLATED', 'standard_30d'
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(task_id)
    .execute(pool)
    .await
    .expect("seed task node");
}

/// Completes the task: phase 1 of a worker, which writes its phase-2 record.
async fn complete_task(pool: &TestPool, task_id: Uuid) {
    let result = serde_json::to_string(&TaskResult::Ok(serde_json::json!({"value": 7}))).unwrap();
    let outcomes = terminal_writes::complete_workflow_task(pool, task_id, WORKER, &result)
        .await
        .expect("terminalize the workflow task");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { .. }]
    ));
}

/// The status and child of the node at `index`.
async fn node(pool: &TestPool, workflow_id: Uuid, index: i32) -> (String, Option<Uuid>) {
    sql::query_as(
        "SELECT status, sub_workflow_id FROM horsies_workflow_tasks
         WHERE workflow_id = $1 AND task_index = $2",
    )
    .bind(workflow_id)
    .bind(index)
    .fetch_one(pool)
    .await
    .expect("read the node")
}

/// The node at `index` is RUNNING and linked to a RUNNING child of depth
/// `parent_depth + 1` under that node, whose root task node is ENQUEUED with
/// a PENDING task row.
async fn assert_child_started(pool: &TestPool, workflow_id: Uuid, index: i32, parent_depth: i32) {
    let (status, child) = node(pool, workflow_id, index).await;
    assert_eq!(status, "RUNNING");
    let child = child.expect("the sub-workflow node links its child");
    let (child_status, parent, parent_index, depth): (String, Option<Uuid>, Option<i32>, i32) =
        sql::query_as(
            "SELECT status, parent_workflow_id, parent_task_index, depth
             FROM horsies_workflows WHERE id = $1",
        )
        .bind(child)
        .fetch_one(pool)
        .await
        .expect("the child workflow row");
    assert_eq!(
        (child_status.as_str(), parent, parent_index, depth),
        ("RUNNING", Some(workflow_id), Some(index), parent_depth + 1)
    );
    let (child_node_status, task_status): (String, String) = sql::query_as(
        "SELECT n.status, t.status FROM horsies_workflow_tasks n
         JOIN horsies_tasks t ON t.id = n.task_id
         WHERE n.workflow_id = $1 AND n.task_index = 0",
    )
    .bind(child)
    .fetch_one(pool)
    .await
    .expect("the child's root task");
    assert_eq!(
        (child_node_status.as_str(), task_status.as_str()),
        ("ENQUEUED", "PENDING")
    );
}

// ---------------------------------------------------------------------------
// Dispositions and the host build
// ---------------------------------------------------------------------------

#[test]
fn every_child_start_disposition_text_reads_and_others_do_not() {
    for (text, disposition) in [
        ("STARTED", ChildStartDisposition::Started),
        ("NODE_FAILED", ChildStartDisposition::NodeFailed),
        ("STALE", ChildStartDisposition::Stale),
        (
            "WORKFLOW_NOT_RUNNING",
            ChildStartDisposition::WorkflowNotRunning,
        ),
    ] {
        assert_eq!(ChildStartDisposition::from_text(text), Some(disposition));
    }
    assert_eq!(ChildStartDisposition::from_text("APPLIED"), None);
}

/// A registered child builds into a tree whose top row names the node and
/// has the parent's depth plus one.
#[test]
fn a_registered_child_is_a_tree_under_its_node() {
    let workflow_id = Uuid::new_v4();
    let children = children_with_child();
    let build: &dyn ChildSpecSource = &children;

    let kind = host_build(&owed(workflow_id, 3, CHILD_KEY), build).expect("host build");

    let ChildStartKind::Tree(arguments) = kind else {
        panic!("expected a TREE child start, got {kind:?}");
    };
    assert_eq!(
        arguments.top_row_for_test(),
        (Some(workflow_id), Some(1), 4)
    );
    assert!(!arguments.has_failed_records());
}

/// A child with no registration under its key fails the node with
/// `SUBWORKFLOW_LOAD_FAILED`; a child past the depth limit with
/// `SUBWORKFLOW_DEPTH_LIMIT_REACHED`. Each message names the parent workflow
/// by the name on its `CHILD_START` row (Python
/// `core/workflows/engine.py:577-581`, `:1017-1033`).
#[test]
fn a_child_that_cannot_load_or_is_too_deep_fails_its_node() {
    let workflow_id = Uuid::new_v4();
    let children = children_with_child();
    let build: &dyn ChildSpecSource = &children;

    let missing = failure_error(
        host_build(&owed(workflow_id, 0, "tests.child_start.missing.v1"), build)
            .expect("host build"),
    );
    assert_eq!(missing.error_code.to_string(), "SUBWORKFLOW_LOAD_FAILED");
    assert_eq!(
        missing.message.as_deref(),
        Some("Failed to load subworkflow definition for child_start_parent:1")
    );
    assert_eq!(
        missing.data_value(),
        Some(serde_json::json!({"definition_key": "tests.child_start.missing.v1"}))
    );
    let too_deep = host_build(
        &owed(
            workflow_id,
            crate::workflow_engine::start::MAX_SUBWORKFLOW_DEPTH,
            CHILD_KEY,
        ),
        build,
    )
    .expect("host build");
    let too_deep = failure_error(too_deep);
    assert_eq!(
        too_deep.error_code.to_string(),
        "SUBWORKFLOW_DEPTH_LIMIT_REACHED"
    );
    assert_eq!(
        too_deep.message.as_deref(),
        Some(
            "Subworkflow not started: the child workflow would have depth 33, \
             and the limit is 32 (workflow child_start_parent:1)"
        )
    );
    assert_eq!(
        too_deep.data_value(),
        Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 1}))
    );
}

/// `TestEnqueueSubworkflowTask::test_a_child_past_the_depth_limit_fails_the_node_and_loads_nothing`,
/// `test_a_child_at_the_depth_limit_goes_on_to_load_its_definition`: the
/// depth check comes before the child definition is resolved. A child past
/// the limit fails with `SUBWORKFLOW_DEPTH_LIMIT_REACHED` although no child
/// is registered under its key; a child that reaches the limit exactly goes
/// on to resolve its definition, and the unregistered key fails it with
/// `SUBWORKFLOW_LOAD_FAILED`.
#[test]
fn the_depth_limit_is_checked_before_the_child_definition_is_resolved() {
    let workflow_id = Uuid::new_v4();
    let children = children_with_child();
    let build: &dyn ChildSpecSource = &children;
    let missing_key = "tests.child_start.missing.v1";
    let depth_limit = crate::workflow_engine::start::MAX_SUBWORKFLOW_DEPTH;

    let past = failure_error(
        host_build(&owed(workflow_id, depth_limit, missing_key), build).expect("host build"),
    );
    let at_limit = failure_error(
        host_build(&owed(workflow_id, depth_limit - 1, missing_key), build).expect("host build"),
    );

    assert_eq!(
        past.error_code.to_string(),
        "SUBWORKFLOW_DEPTH_LIMIT_REACHED"
    );
    assert_eq!(at_limit.error_code.to_string(), "SUBWORKFLOW_LOAD_FAILED");
}

/// An `args_from` key that the static input also sets fails the node with
/// Python's conflict error.
#[test]
fn an_args_from_key_that_names_a_static_member_fails_the_node() {
    let workflow_id = Uuid::new_v4();
    let children = children_with_child();
    let mut conflicting = owed(workflow_id, 0, CHILD_KEY);
    conflicting.node_input = r#"{"value": 1}"#.to_owned();
    conflicting.args_from = Some(r#"{"value": 0}"#.to_owned());

    let kind = host_build(&conflicting, &children).expect("host build");

    let error = failure_error(kind);
    assert_eq!(error.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
    assert_eq!(
        error.message.as_deref(),
        Some("args_from key 'value' conflicts with static kwarg (subworkflow task 1)")
    );
}

/// The registered builder gets the node input with each `args_from` member
/// set to the stored result of its `SOURCE` dependency; a builder that fails
/// fails the node with `WORKFLOW_ENQUEUE_FAILED`, the Python class of its
/// error and its text, and the parent workflow by name (Python
/// `core/workflows/engine.py:1196-1211`).
#[test]
fn the_builder_takes_the_merged_input_and_a_failing_builder_fails_the_node() {
    let workflow_id = Uuid::new_v4();
    let builder: TestChildBuild =
        Arc::new(|input: &str| -> Result<WorkflowSpec, TestBuildRefused> {
            let parsed: serde_json::Value =
                serde_json::from_str(input).expect("the merged input is JSON");
            match parsed["value"]["ok"] == serde_json::json!(7) {
                true => Ok(child_spec()),
                false => Err(TestBuildRefused::Failed(crate::core::HorsiesError::new(
                    "no merged value",
                ))),
            }
        });
    let children = TestChildSpecs::empty().with_build(CHILD_KEY, builder);
    let build: &dyn ChildSpecSource = &children;
    let mut merged = owed(workflow_id, 0, CHILD_KEY);
    merged.sub_workflow_params = SubWorkflowParams::Given;
    merged.args_from = Some(r#"{"value": 0}"#.to_owned());
    merged.sources = vec![ChildStartSource {
        task_index: 0,
        node_id: Some("source".to_owned()),
        status: "COMPLETED".to_owned(),
        task_name: "source_task".to_owned(),
        definition_key: None,
        result: Some(r#"{"__h_task_result__":true,"ok":7,"err":null}"#.to_owned()),
    }];

    assert!(matches!(
        host_build(&merged, build).expect("host build"),
        ChildStartKind::Tree(_)
    ));

    merged.sources = Vec::new();
    let error = failure_error(host_build(&merged, build).expect("host build"));
    assert_eq!(error.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
    assert_eq!(
        error.message.as_deref(),
        Some(
            "Subworkflow build failed: HorsiesError: error: no merged value \
             (workflow child_start_parent:1)"
        )
    );
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 1}))
    );
}

/// The child specs whose child under [`CHILD_KEY`] has `builder`, for the
/// client whose builder hook is `hook`.
fn children_with_builder(hook: &'static str, builder: TestChildBuild) -> TestChildSpecs {
    TestChildSpecs::empty()
        .with_builder_hook(hook)
        .with_build(CHILD_KEY, builder)
}

/// A static input member that the builder's decode refuses fails the node
/// with `WORKER_SERIALIZATION_ERROR` and Python's text, not as a build
/// failure (Python `core/workflows/engine.py:1063-1080`).
#[test]
fn a_static_member_that_the_builder_refuses_fails_the_node_as_a_serialization_error() {
    let workflow_id = Uuid::new_v4();
    let builder: TestChildBuild =
        Arc::new(|_input: &str| -> Result<WorkflowSpec, TestBuildRefused> {
            Err(TestBuildRefused::Member {
                key: "count".to_owned(),
                cause: "count: invalid type: string \"x\", expected i64".to_owned(),
            })
        });
    let children = children_with_builder("spec_builder", builder);
    let mut refused = owed(workflow_id, 0, CHILD_KEY);
    refused.node_input = r#"{"count": "x"}"#.to_owned();
    refused.sub_workflow_params = SubWorkflowParams::Given;

    let error = failure_error(host_build(&refused, &children).expect("host build"));

    assert_eq!(error.error_code.to_string(), "WORKER_SERIALIZATION_ERROR");
    assert_eq!(
        error.message.as_deref(),
        Some(
            "Typed decode of subworkflow kwargs failed for task 1: \
             count: invalid type: string \"x\", expected i64"
        )
    );
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 1}))
    );
}

/// An `args_from` value that the builder's decode refuses fails the node with
/// `WORKER_SERIALIZATION_ERROR`; the text names the client's builder hook and
/// keeps 200 characters of the refusal (Python
/// `core/workflows/engine.py:1134-1157`). The builder gets the static
/// members before the `args_from` members, so it meets a static member first,
/// as Python's decode does.
#[test]
fn an_args_from_value_that_the_builder_refuses_fails_the_node_as_a_serialization_error() {
    let workflow_id = Uuid::new_v4();
    let builder: TestChildBuild =
        Arc::new(|input: &str| -> Result<WorkflowSpec, TestBuildRefused> {
            match input.starts_with(r#"{"zeta":"#) {
                true => Err(TestBuildRefused::Member {
                    key: "alpha".to_owned(),
                    cause: "x".repeat(250),
                }),
                false => Err(TestBuildRefused::Failed(crate::core::HorsiesError::new(
                    format!("the static members are not first: {input}"),
                ))),
            }
        });
    let children = children_with_builder("build_with()", builder);
    let mut refused = owed(workflow_id, 0, CHILD_KEY);
    refused.node_input = r#"{"zeta": 1}"#.to_owned();
    refused.sub_workflow_params = SubWorkflowParams::Given;
    refused.args_from = Some(r#"{"alpha": 0}"#.to_owned());
    refused.sources = vec![ChildStartSource {
        task_index: 0,
        node_id: Some("source".to_owned()),
        status: "COMPLETED".to_owned(),
        task_name: "source_task".to_owned(),
        definition_key: None,
        result: Some(r#"{"__h_task_result__":true,"ok":"seven","err":null}"#.to_owned()),
    }];

    let error = failure_error(host_build(&refused, &children).expect("host build"));

    assert_eq!(error.error_code.to_string(), "WORKER_SERIALIZATION_ERROR");
    assert_eq!(
        error.message,
        Some(format!(
            "args_from value for 'alpha' does not fit the type that build_with() declares \
             (subworkflow task 1): {}",
            "x".repeat(200)
        ))
    );
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 1}))
    );
}

/// A COMPLETED dependency with no stored result gives its `args_from` member
/// a `RESULT_DESERIALIZATION_ERROR` that names the node id of the `SOURCE`
/// row, the same text as `horsies_workflow_source_value` and Python
/// `_missing_result_entry` (`core/workflows/engine.py:169-194`).
#[test]
fn a_source_with_no_stored_result_is_named_by_its_node_id() {
    let mut merged = owed(Uuid::new_v4(), 0, CHILD_KEY);
    merged.args_from = Some(r#"{"value": 0}"#.to_owned());
    merged.sources = vec![ChildStartSource {
        task_index: 0,
        node_id: Some("fetch".to_owned()),
        status: "COMPLETED".to_owned(),
        task_name: "source_task".to_owned(),
        definition_key: None,
        result: None,
    }];

    let NodeInput::Input(input) = merged_child_input(&request_of(&merged)).expect("the merge")
    else {
        panic!("the merge failed the node");
    };
    let members: serde_json::Value =
        serde_json::from_str(&input.text).expect("the merged input parses");
    let entry: TaskResult<serde_json::Value> = serde_json::from_str(&members["value"].to_string())
        .expect("the member is a result envelope");
    let TaskResult::Err(error) = entry else {
        panic!("a source with no stored result gives an error entry");
    };
    assert_eq!(error.error_code.to_string(), "RESULT_DESERIALIZATION_ERROR");
    assert_eq!(
        error.message.as_deref(),
        Some(
            "Dependency node fetch (index 0, task source_task) is COMPLETED with no stored result"
        )
    );
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({"dependency_index": 0}))
    );
}

/// An `args_from` text that does not parse fails the node with
/// `WORKER_SERIALIZATION_ERROR` and Python's text (`_args_from_map`,
/// `core/workflows/child_build.py`), with the workflow id and the node in
/// `data`; it is not an error of the step.
#[test]
fn an_args_from_text_that_does_not_parse_fails_the_node() {
    let workflow_id = Uuid::new_v4();
    let mut corrupt = owed(workflow_id, 0, CHILD_KEY);
    corrupt.args_from = Some("{not json".to_owned());

    let NodeInput::NodeFails(error) =
        merged_child_input(&request_of(&corrupt)).expect("a node failure, not a step error")
    else {
        panic!("the merge took an args_from text that does not parse");
    };
    assert_eq!(error.error_code.to_string(), "WORKER_SERIALIZATION_ERROR");
    assert_eq!(
        error.message.as_deref(),
        Some("Corrupt args_from JSON for subworkflow task 1")
    );
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 1}))
    );
}

/// The node error of a merge that fails the node; a merge that builds an
/// input fails the test.
fn merge_refusal(owed: &OwedChildStart) -> crate::core::TaskError {
    match merged_child_input(&request_of(owed)).expect("a node failure, not a step error") {
        NodeInput::NodeFails(error) => error,
        NodeInput::Input(input) => panic!("the merge built the input {}", input.text),
    }
}

/// The builder input of a merge; a merge that fails the node fails the test.
fn merge_input(owed: &OwedChildStart) -> String {
    match merged_child_input(&request_of(owed)).expect("the merge") {
        NodeInput::Input(input) => input.text,
        NodeInput::NodeFails(error) => panic!("the merge failed the node: {error:?}"),
    }
}

/// A stored input that is empty, that the strict read refuses, or `null`,
/// fails the node with `WORKER_SERIALIZATION_ERROR`, `Corrupt kwargs JSON for
/// subworkflow task <index>` (Python `_input_kwargs`,
/// `core/workflows/child_build.py`), with the workflow id and the node in
/// `data`; with `args_from` too, the input is refused first. A node without
/// params stores `{}`; no writer stores an empty input.
#[test]
fn a_stored_input_that_does_not_read_fails_the_node() {
    let workflow_id = Uuid::new_v4();
    for (input, args_from) in [
        ("", None),
        ("broken{json", None),
        ("null", None),
        (r#"{"a": 1, "a": 2}"#, None),
        ("broken{json", Some(r#"{"x": "not_an_int"}"#)),
    ] {
        let mut corrupt = owed(workflow_id, 0, CHILD_KEY);
        corrupt.node_input = input.to_owned();
        corrupt.args_from = args_from.map(str::to_owned);

        let error = merge_refusal(&corrupt);

        assert_eq!(
            error.error_code.to_string(),
            "WORKER_SERIALIZATION_ERROR",
            "{input}"
        );
        assert_eq!(
            error.message.as_deref(),
            Some("Corrupt kwargs JSON for subworkflow task 1"),
            "{input}"
        );
        assert_eq!(
            error.data_value(),
            Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 1})),
            "{input}"
        );
    }
}

/// An `args_from` member value that is not a JSON integer fails the node
/// with `WORKER_SERIALIZATION_ERROR`, `args_from has non-int values for
/// subworkflow task <index>` (Python `_args_from_map`); `null` or an empty
/// text as the whole text fails it with `Corrupt args_from JSON for
/// subworkflow task <index>`. A node without `args_from` stores NULL; no
/// writer stores an empty text.
/// `true` is refused as a non-integer, as Python's `_args_from_map` refuses a
/// `bool`.
#[test]
fn an_args_from_value_that_is_not_an_index_fails_the_node() {
    let workflow_id = Uuid::new_v4();
    for (args_from, expected) in [
        (
            r#"{"x": "not_an_int"}"#,
            "args_from has non-int values for subworkflow task 1",
        ),
        (
            r#"{"x": 1.5}"#,
            "args_from has non-int values for subworkflow task 1",
        ),
        (
            r#"{"x": 1e2}"#,
            "args_from has non-int values for subworkflow task 1",
        ),
        (
            r#"{"x": null}"#,
            "args_from has non-int values for subworkflow task 1",
        ),
        (
            r#"{"x": 0, "y": "bad"}"#,
            "args_from has non-int values for subworkflow task 1",
        ),
        (
            r#"{"x": true}"#,
            "args_from has non-int values for subworkflow task 1",
        ),
        ("null", "Corrupt args_from JSON for subworkflow task 1"),
        ("", "Corrupt args_from JSON for subworkflow task 1"),
    ] {
        let mut refused = owed(workflow_id, 0, CHILD_KEY);
        refused.args_from = Some(args_from.to_owned());

        let error = merge_refusal(&refused);

        assert_eq!(
            error.error_code.to_string(),
            "WORKER_SERIALIZATION_ERROR",
            "{args_from}"
        );
        assert_eq!(error.message.as_deref(), Some(expected), "{args_from}");
        assert_eq!(
            error.data_value(),
            Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 1})),
            "{args_from}"
        );
    }
}

/// Stored data that Python reads with no refusal: an input that is not an
/// object gives no static members (the builder input is `{}`); no `args_from`
/// (NULL) and an `args_from` that is not an object give no members; an index
/// outside the `integer` range names no node, so its member is left out.
#[test]
fn stored_data_that_python_reads_as_no_members_builds_the_input() {
    let source = ChildStartSource {
        task_index: 0,
        node_id: Some("source".to_owned()),
        status: "COMPLETED".to_owned(),
        task_name: "source_task".to_owned(),
        definition_key: None,
        result: Some(r#"{"__h_task_result__":true,"ok":7,"err":null}"#.to_owned()),
    };
    for (input, args_from) in [
        ("[1, 2]", None),
        (" \"text\" ", None),
        ("7", Some("[]")),
        ("{}", Some("false")),
        ("{}", Some(r#"{"x": 99999999999999999999999}"#)),
        ("{}", Some(r#"{"x": 4294967296}"#)),
    ] {
        let mut readable = owed(Uuid::new_v4(), 0, CHILD_KEY);
        readable.node_input = input.to_owned();
        readable.args_from = args_from.map(str::to_owned);
        readable.sources = vec![source.clone()];

        let text = merge_input(&readable);

        let members: serde_json::Value =
            serde_json::from_str(&text).expect("the builder input parses");
        assert_eq!(members, serde_json::json!({}), "{input} {args_from:?}");
    }
}
#[test]
fn a_step_takes_child_starts_up_to_the_node_bound() {
    let start = owed(Uuid::new_v4(), 0, CHILD_KEY);
    let mut owed_starts = OwedChildStarts::new(vec![start.clone(); MAX_WORKFLOW_NODES - 1]);
    owed_starts.extend(vec![start]);
    for _ in 0..MAX_WORKFLOW_NODES {
        assert!(owed_starts.next().expect("within the bound").is_some());
    }
    assert!(matches!(owed_starts.next(), Ok(None)));
}

/// One child start past the bound fails the step closed with
/// `TooManyChildStarts`.
#[test]
fn a_step_past_the_node_bound_fails_closed() {
    let start = owed(Uuid::new_v4(), 0, CHILD_KEY);
    let mut owed_starts = OwedChildStarts::new(vec![start.clone(); MAX_WORKFLOW_NODES]);
    owed_starts.extend(vec![start]);
    for _ in 0..MAX_WORKFLOW_NODES {
        assert!(owed_starts.next().expect("within the bound").is_some());
    }
    assert!(matches!(
        owed_starts.next(),
        Err(StepContractError::TooManyChildStarts { bound }) if bound == MAX_WORKFLOW_NODES
    ));
}

// ---------------------------------------------------------------------------
// Each step commits the child with the step
// ---------------------------------------------------------------------------

/// Phase 2 in the transaction form promotes a sub-workflow node; its child
/// starts in the same transaction.
#[tokio::test]
async fn phase_two_commits_the_child_of_the_node_it_promotes() {
    let broker = test_broker().await;
    let pool = crate::test_support::migrated_pool().await;
    let workflow_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    seed_workflow(&pool, workflow_id, "RUNNING", None, None).await;
    seed_running_task_node(&pool, workflow_id, task_id).await;
    seed_subworkflow_node(&pool, workflow_id, 1, "PENDING", "{0}", None, CHILD_KEY).await;
    complete_task(&pool, task_id).await;
    let children = children_with_child();

    let committed = node_finish_in_transaction(&broker, task_id, false, &children)
        .await
        .expect("phase 2");

    assert_eq!(committed.disposition, NodeFinishDisposition::Applied);
    assert!(committed.parent_advances.is_empty());
    assert_eq!(node(&pool, workflow_id, 0).await.0, "COMPLETED");
    assert_child_started(&pool, workflow_id, 1, 0).await;
}

/// A promoted sub-workflow node whose child has no registration fails in the
/// same transaction (`FAILURE`), and the failure policy ends the workflow.
#[tokio::test]
async fn phase_two_fails_a_promoted_node_whose_child_cannot_load() {
    let broker = test_broker().await;
    let pool = crate::test_support::migrated_pool().await;
    let workflow_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    seed_workflow(&pool, workflow_id, "RUNNING", None, None).await;
    seed_running_task_node(&pool, workflow_id, task_id).await;
    seed_subworkflow_node(&pool, workflow_id, 1, "PENDING", "{0}", None, CHILD_KEY).await;
    complete_task(&pool, task_id).await;
    let children = TestChildSpecs::empty();

    let committed = node_finish_in_transaction(&broker, task_id, false, &children)
        .await
        .expect("phase 2");

    assert_eq!(committed.disposition, NodeFinishDisposition::Applied);
    assert_eq!(
        node(&pool, workflow_id, 1).await,
        ("FAILED".to_owned(), None)
    );
    let (status, result): (String, Option<String>) = sql::query_as(
        "SELECT w.status, n.result FROM horsies_workflows w
         JOIN horsies_workflow_tasks n ON n.workflow_id = w.id AND n.task_index = 1
         WHERE w.id = $1",
    )
    .bind(workflow_id)
    .fetch_one(&pool)
    .await
    .expect("workflow and node");
    assert_eq!(status, "FAILED");
    let stored: TaskResult<serde_json::Value> =
        serde_json::from_str(&result.expect("the failed node stores its error")).unwrap();
    let TaskResult::Err(error) = stored else {
        panic!("the failed node stores an error");
    };
    assert_eq!(error.error_code.to_string(), "SUBWORKFLOW_LOAD_FAILED");
    assert_eq!(
        error.message.as_deref(),
        Some("Failed to load subworkflow definition for child_start_parent:1")
    );
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({"definition_key": CHILD_KEY}))
    );
}

/// The advance of a parent node promotes its sub-workflow dependent; that
/// child starts in the advance's transaction.
#[tokio::test]
async fn a_parent_advance_commits_the_child_of_the_node_it_promotes() {
    let broker = test_broker().await;
    let pool = crate::test_support::migrated_pool().await;
    let parent = Uuid::new_v4();
    let ended_child = Uuid::new_v4();
    seed_workflow(&pool, parent, "RUNNING", None, None).await;
    seed_workflow(
        &pool,
        ended_child,
        "COMPLETED",
        Some(parent),
        Some(r#"{"__h_task_result__":true,"ok":1,"err":null}"#),
    )
    .await;
    seed_subworkflow_node(
        &pool,
        parent,
        0,
        "RUNNING",
        "{}",
        Some(ended_child),
        CHILD_KEY,
    )
    .await;
    seed_subworkflow_node(&pool, parent, 1, "PENDING", "{0}", None, CHILD_KEY).await;
    let children = children_with_child();

    advance_parents(
        &broker,
        CheckoutClass::Batch,
        OwedParentAdvances::of(ParentAdvance::Owed {
            child_workflow_id: ended_child,
        }),
        &children,
    )
    .await
    .expect("parent advance");

    assert_eq!(node(&pool, parent, 0).await.0, "COMPLETED");
    assert_child_started(&pool, parent, 1, 0).await;
}

/// A start whose sub-workflow root cannot load fails that node only; the
/// failure promotes a sub-workflow node that allows failed dependencies, and
/// its child starts in the start's transaction.
#[tokio::test]
async fn a_start_with_a_failed_root_commits_the_child_its_progression_owes() {
    let pool = crate::test_support::migrated_pool().await;
    let broker = Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    ));
    let workflow_id = Uuid::new_v4();
    let unloadable = sub_workflow_node(0, "child_start_missing", "tests.child_start.missing.v1");
    let mut dependent = sub_workflow_node(1, CHILD_NAME, CHILD_KEY);
    dependent.dependencies = vec![0];
    dependent.allow_failed_deps = true;
    let spec = WorkflowSpec {
        name: "child_start_failed_root".to_owned(),
        definition_key: Some("tests.child_start.failed_root.v1".to_owned()),
        tasks: vec![unloadable, dependent],
        on_error: OnError::Fail,
        output_index: None,
        success_policy: None,
    };

    let children = children_with_child();
    let started = broker
        .start_client_workflow(
            &children.client_spec(&spec),
            workflow_id,
            chrono::Utc::now(),
            crate::telemetry::trace_carrier::SendTrace::NoCarrier,
            false,
            &children,
        )
        .await
        .expect("start");
    assert_eq!(started, ClientStartOutcome::Started);

    let (status, result): (String, Option<String>) = sql::query_as(
        "SELECT status, result FROM horsies_workflow_tasks
         WHERE workflow_id = $1 AND task_index = 0",
    )
    .bind(workflow_id)
    .fetch_one(&pool)
    .await
    .expect("the failed root");
    assert_eq!(status, "FAILED");
    let stored: TaskResult<serde_json::Value> =
        serde_json::from_str(&result.expect("the failed root stores its error")).unwrap();
    let TaskResult::Err(error) = stored else {
        panic!("the failed root stores an error");
    };
    assert_eq!(error.error_code.to_string(), "SUBWORKFLOW_LOAD_FAILED");
    assert_eq!(
        error.message.as_deref(),
        Some("Failed to load subworkflow definition for child_start_failed_root:0")
    );
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({"definition_key": "tests.child_start.missing.v1"}))
    );
    assert_child_started(&pool, workflow_id, 1, 0).await;
}

/// A resume enqueues a READY sub-workflow node; its child starts in the
/// resume's transaction.
#[tokio::test]
async fn a_resume_commits_the_child_of_a_ready_subworkflow_node() {
    let broker = test_broker().await;
    let pool = crate::test_support::migrated_pool().await;
    let workflow_id = Uuid::new_v4();
    seed_workflow(&pool, workflow_id, "RUNNING", None, None).await;
    seed_subworkflow_node(&pool, workflow_id, 1, "READY", "{}", None, CHILD_KEY).await;
    assert!(pause_workflow(&broker, workflow_id).await.expect("pause"));

    assert!(
        resume_client_workflow(&broker, workflow_id, &children_with_child())
            .await
            .expect("resume")
    );

    assert_child_started(&pool, workflow_id, 1, 0).await;
}

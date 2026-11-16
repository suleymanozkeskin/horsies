//! Tests of the owed child start.
//!
//! - The host build (pure): the kind that each owed child start passes.
//! - Each step that can owe a child start commits the child with the step
//!   (database, needs `0005_workflow_functions.sql`): phase 2 in the
//!   transaction form, the parent advance, the start with a failed-node
//!   record, and the resume. Each checks the parent node RUNNING with its
//!   `sub_workflow_id` and the child rows present.

use std::sync::Arc;

use serial_test::serial;
use sqlx::PgPool;

use super::*;
use crate::broker::terminalization::terminalize;
use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::lifecycle::{PriorLockedRead, TerminalizationCommand, TerminalizationOutcome};
use crate::core::registry::workflow::RegisteredWorkflowSpec;
use crate::core::workflow::node::SubWorkflowParams;
use crate::core::workflow::spec::WorkflowSpecBuilder;
use crate::core::workflow::sub_workflow::SubWorkflowNode;
use crate::core::{OnError, TaskResult, WorkflowSpec, WorkflowSpecRegistry};
use crate::db::CheckoutClass;
use crate::workflow_engine::bound_handle::WorkflowHandle;
use crate::workflow_engine::lifecycle::{pause_workflow, resume_workflow};
use crate::workflow_engine::start::start_workflow;
use crate::workflow_engine::steps::{
    advance_parents, node_finish_in_transaction, ChildStartSource, NodeFinishDisposition,
    ParentAdvance,
};
use crate::TaskNode;

const CHILD_KEY: &str = "tests.child_start.child.v1";
const CHILD_NAME: &str = "child_start_child";
const WORKER: &str = "child-start-worker";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A registry with one static child of one task node under [`CHILD_KEY`].
fn registry_with_child() -> WorkflowSpecRegistry {
    let mut builder = WorkflowSpecBuilder::new(CHILD_NAME);
    builder.definition_key(CHILD_KEY);
    let node = builder.task(
        TaskNode::<String>::raw("child_start_task", "{}")
            .queue("default")
            .priority(100),
    );
    builder.output(node);
    let mut registry = WorkflowSpecRegistry::new();
    registry
        .register(RegisteredWorkflowSpec {
            spec: builder.build().expect("child spec"),
            spec_builder: None,
        })
        .expect("register the child");
    registry
}

/// An owed child start of node 1 of `workflow_id` at depth `parent_depth`,
/// whose child is registered under `definition_key`.
fn owed(workflow_id: Uuid, parent_depth: i32, definition_key: &str) -> OwedChildStart {
    OwedChildStart {
        workflow_id,
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
    PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await)
}

/// A workflow row; a child names its parent, the node index 0 and depth 1.
async fn seed_workflow(
    pool: &PgPool,
    id: Uuid,
    status: &str,
    parent: Option<Uuid>,
    result: Option<&str>,
) {
    sqlx::query(
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
    pool: &PgPool,
    workflow_id: Uuid,
    index: i32,
    status: &str,
    dependencies: &str,
    child: Option<Uuid>,
    definition_key: &str,
) {
    sqlx::query(
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

/// A RUNNING task claimed by [`WORKER`] behind the RUNNING task node 0 of
/// `workflow_id`, from which a sub-workflow node is reachable.
async fn seed_running_task_node(pool: &PgPool, workflow_id: Uuid, task_id: Uuid) {
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, status, sent_at, enqueued_at,
             started_at, claimed_at, claimed_by_worker_id, is_workflow_task, retry_count,
             max_retries, command_fingerprint_version, command_fingerprint,
             retention_class_key, retain_rerun_input, created_at, updated_at, execution
         ) VALUES (
             $1, 'child_start_root', 'default', 100, '{}', 'RUNNING', NOW(), NOW(), NOW(),
             NOW(), $2, TRUE, 0, 0, NULL, NULL, 'standard_30d', FALSE, NOW(), NOW(),
             'ISOLATED'
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
async fn complete_task(pool: &PgPool, task_id: Uuid) {
    let result = serde_json::to_string(&TaskResult::Ok(serde_json::json!({"value": 7}))).unwrap();
    let outcomes = terminalize(
        pool,
        &TerminalizationCommand::CompleteLockedTask {
            task_id,
            fence: PriorLockedRead {
                worker_id: WORKER.to_owned(),
            },
            result_json: result,
        },
    )
    .await
    .expect("terminalize the workflow task");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { .. }]
    ));
}

/// The status and child of the node at `index`.
async fn node(pool: &PgPool, workflow_id: Uuid, index: i32) -> (String, Option<Uuid>) {
    sqlx::query_as(
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
async fn assert_child_started(pool: &PgPool, workflow_id: Uuid, index: i32, parent_depth: i32) {
    let (status, child) = node(pool, workflow_id, index).await;
    assert_eq!(status, "RUNNING");
    let child = child.expect("the sub-workflow node links its child");
    let (child_status, parent, parent_index, depth): (String, Option<Uuid>, Option<i32>, i32) =
        sqlx::query_as(
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
    let (child_node_status, task_status): (String, String) = sqlx::query_as(
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
    let registry = registry_with_child();
    let retention = RetentionConfig::default();
    let build = ChildBuild {
        registry: &registry,
        retention: &retention,
    };

    let kind = child_start_kind(&owed(workflow_id, 3, CHILD_KEY), build).expect("host build");

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
/// `SUBWORKFLOW_DEPTH_LIMIT_REACHED`.
#[test]
fn a_child_that_cannot_load_or_is_too_deep_fails_its_node() {
    let workflow_id = Uuid::new_v4();
    let registry = registry_with_child();
    let retention = RetentionConfig::default();
    let build = ChildBuild {
        registry: &registry,
        retention: &retention,
    };

    let missing = child_start_kind(&owed(workflow_id, 0, "tests.child_start.missing.v1"), build)
        .expect("host build");
    assert_eq!(
        failure_error(missing).error_code.to_string(),
        "SUBWORKFLOW_LOAD_FAILED"
    );
    let too_deep = child_start_kind(
        &owed(
            workflow_id,
            crate::workflow_engine::start::MAX_SUBWORKFLOW_DEPTH,
            CHILD_KEY,
        ),
        build,
    )
    .expect("host build");
    assert_eq!(
        failure_error(too_deep).error_code.to_string(),
        "SUBWORKFLOW_DEPTH_LIMIT_REACHED"
    );
}

/// An `args_from` key that the static input also sets fails the node with
/// Python's conflict error.
#[test]
fn an_args_from_key_that_names_a_static_member_fails_the_node() {
    let workflow_id = Uuid::new_v4();
    let registry = registry_with_child();
    let retention = RetentionConfig::default();
    let mut conflicting = owed(workflow_id, 0, CHILD_KEY);
    conflicting.node_input = r#"{"value": 1}"#.to_owned();
    conflicting.args_from = Some(r#"{"value": 0}"#.to_owned());

    let kind = child_start_kind(
        &conflicting,
        ChildBuild {
            registry: &registry,
            retention: &retention,
        },
    )
    .expect("host build");

    let error = failure_error(kind);
    assert_eq!(error.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
    assert_eq!(
        error.message.as_deref(),
        Some("args_from key 'value' conflicts with static kwarg (subworkflow task 1)")
    );
}

/// The registered builder gets the node input with each `args_from` member
/// set to the stored result of its `SOURCE` dependency; a builder that fails
/// fails the node with `WORKFLOW_ENQUEUE_FAILED`.
#[test]
fn the_builder_takes_the_merged_input_and_a_failing_builder_fails_the_node() {
    let workflow_id = Uuid::new_v4();
    let builder: crate::core::SpecBuilderFn = Arc::new(
        |input: &str| -> Result<WorkflowSpec, crate::core::HorsiesError> {
            let parsed: serde_json::Value =
                serde_json::from_str(input).expect("the merged input is JSON");
            match parsed["value"]["ok"] == serde_json::json!(7) {
                true => {
                    let mut spec_builder = WorkflowSpecBuilder::new(CHILD_NAME);
                    spec_builder.definition_key(CHILD_KEY);
                    let node = spec_builder.task(
                        TaskNode::<String>::raw("child_start_task", "{}")
                            .queue("default")
                            .priority(100),
                    );
                    spec_builder.output(node);
                    spec_builder.build()
                }
                false => Err(crate::core::HorsiesError::new("no merged value")),
            }
        },
    );
    let mut child = WorkflowSpecBuilder::new(CHILD_NAME);
    child.definition_key(CHILD_KEY);
    let node = child.task(
        TaskNode::<String>::raw("child_start_task", "{}")
            .queue("default")
            .priority(100),
    );
    child.output(node);
    let mut registry = WorkflowSpecRegistry::new();
    registry
        .register(RegisteredWorkflowSpec {
            spec: child.build().expect("child spec"),
            spec_builder: Some(builder),
        })
        .expect("register the child");
    let retention = RetentionConfig::default();
    let build = ChildBuild {
        registry: &registry,
        retention: &retention,
    };
    let mut merged = owed(workflow_id, 0, CHILD_KEY);
    merged.sub_workflow_params = SubWorkflowParams::Given;
    merged.args_from = Some(r#"{"value": 0}"#.to_owned());
    merged.sources = vec![ChildStartSource {
        task_index: 0,
        status: "COMPLETED".to_owned(),
        task_name: "source_task".to_owned(),
        definition_key: None,
        result: Some(r#"{"__h_task_result__":true,"ok":7,"err":null}"#.to_owned()),
    }];

    assert!(matches!(
        child_start_kind(&merged, build).expect("host build"),
        ChildStartKind::Tree(_)
    ));

    merged.sources = Vec::new();
    let error = failure_error(child_start_kind(&merged, build).expect("host build"));
    assert_eq!(error.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
    assert!(
        error
            .message
            .as_deref()
            .is_some_and(|message| message.starts_with("Subworkflow build failed: ")),
        "{error:?}"
    );
}

/// A step takes up to `MAX_WORKFLOW_NODES` child starts, those that its
/// child starts owe included.
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
#[serial]
async fn phase_two_commits_the_child_of_the_node_it_promotes() {
    let broker = test_broker().await;
    let pool = broker.pool().clone();
    let workflow_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    seed_workflow(&pool, workflow_id, "RUNNING", None, None).await;
    seed_running_task_node(&pool, workflow_id, task_id).await;
    seed_subworkflow_node(&pool, workflow_id, 1, "PENDING", "{0}", None, CHILD_KEY).await;
    complete_task(&pool, task_id).await;
    let registry = registry_with_child();
    let retention = RetentionConfig::default();

    let committed = node_finish_in_transaction(
        &broker,
        task_id,
        false,
        ChildBuild {
            registry: &registry,
            retention: &retention,
        },
    )
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
#[serial]
async fn phase_two_fails_a_promoted_node_whose_child_cannot_load() {
    let broker = test_broker().await;
    let pool = broker.pool().clone();
    let workflow_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    seed_workflow(&pool, workflow_id, "RUNNING", None, None).await;
    seed_running_task_node(&pool, workflow_id, task_id).await;
    seed_subworkflow_node(&pool, workflow_id, 1, "PENDING", "{0}", None, CHILD_KEY).await;
    complete_task(&pool, task_id).await;
    let registry = WorkflowSpecRegistry::new();
    let retention = RetentionConfig::default();

    let committed = node_finish_in_transaction(
        &broker,
        task_id,
        false,
        ChildBuild {
            registry: &registry,
            retention: &retention,
        },
    )
    .await
    .expect("phase 2");

    assert_eq!(committed.disposition, NodeFinishDisposition::Applied);
    assert_eq!(
        node(&pool, workflow_id, 1).await,
        ("FAILED".to_owned(), None)
    );
    let (status, result): (String, Option<String>) = sqlx::query_as(
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
}

/// The advance of a parent node promotes its sub-workflow dependent; that
/// child starts in the advance's transaction.
#[tokio::test]
#[serial]
async fn a_parent_advance_commits_the_child_of_the_node_it_promotes() {
    let broker = test_broker().await;
    let pool = broker.pool().clone();
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
    let registry = registry_with_child();
    let retention = RetentionConfig::default();

    advance_parents(
        &broker,
        CheckoutClass::Batch,
        OwedParentAdvances::of(ParentAdvance::Owed {
            child_workflow_id: ended_child,
        }),
        ChildBuild {
            registry: &registry,
            retention: &retention,
        },
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
#[serial]
async fn a_start_with_a_failed_root_commits_the_child_its_progression_owes() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let workflow_id = Uuid::new_v4();
    let unloadable = SubWorkflowNode::<(), String>::typed(
        "child_start_missing",
        "{}",
        SubWorkflowParams::NotGiven,
    )
    .definition_key("tests.child_start.missing.v1")
    .into_any_node(0);
    let mut dependent =
        SubWorkflowNode::<(), String>::typed(CHILD_NAME, "{}", SubWorkflowParams::NotGiven)
            .definition_key(CHILD_KEY)
            .into_any_node(1);
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

    let _handle: WorkflowHandle<serde_json::Value> = start_workflow(
        &broker,
        &spec,
        Some(workflow_id),
        &registry_with_child(),
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await
    .expect("start");

    let (status, result): (String, Option<String>) = sqlx::query_as(
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
    assert_child_started(&pool, workflow_id, 1, 0).await;
}

/// A resume enqueues a READY sub-workflow node; its child starts in the
/// resume's transaction.
#[tokio::test]
#[serial]
async fn a_resume_commits_the_child_of_a_ready_subworkflow_node() {
    let broker = test_broker().await;
    let pool = broker.pool().clone();
    let workflow_id = Uuid::new_v4();
    seed_workflow(&pool, workflow_id, "RUNNING", None, None).await;
    seed_subworkflow_node(&pool, workflow_id, 1, "READY", "{}", None, CHILD_KEY).await;
    assert!(pause_workflow(&broker, workflow_id).await.expect("pause"));

    assert!(resume_workflow(
        &broker,
        workflow_id,
        &registry_with_child(),
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await
    .expect("resume"));

    assert_child_started(&pool, workflow_id, 1, 0).await;
}

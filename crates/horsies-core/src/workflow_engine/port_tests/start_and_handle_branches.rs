//! Branches of the workflow start and of the handle reads. Python:
//! `tests/unit/test_workflow_start_catches.py`,
//! `tests/unit/test_success_policy_subworkflow_node.py` (the start),
//! `tests/unit/test_workflow_handle_missing_workflow.py`,
//! `tests/unit/test_workflow_handle_error_variants.py`
//! (`TestStoredSubworkflowSummary`) and
//! `tests/unit/test_workflow_node_status_check.py`. Each test names the
//! Python tests that it ports.

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    child_of, child_specs_of, end_workflows, execute, handle, required, seed_node, seed_workflow,
    shared_pool, spec, start, sub_workflow_node, task_node, try_start, IsolatedDatabase, NodeSeed,
    ParentLink, WorkflowSeed,
};
use crate::broker::workflow_start::ClientStartError;
use crate::core::{AnyNode, WorkflowTaskStatus};
use crate::test_support::child_specs::TestChildSpecs;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::query::{NodeResultsRead, NodesRead};

/// Every node status, in the order of the enum. The match does not compile
/// when a status is added, so the list follows the enum.
fn every_node_status() -> Vec<WorkflowTaskStatus> {
    let every = [
        WorkflowTaskStatus::Pending,
        WorkflowTaskStatus::Ready,
        WorkflowTaskStatus::Enqueued,
        WorkflowTaskStatus::Running,
        WorkflowTaskStatus::Completed,
        WorkflowTaskStatus::Failed,
        WorkflowTaskStatus::Skipped,
    ];
    for status in every {
        match status {
            WorkflowTaskStatus::Pending
            | WorkflowTaskStatus::Ready
            | WorkflowTaskStatus::Enqueued
            | WorkflowTaskStatus::Running
            | WorkflowTaskStatus::Completed
            | WorkflowTaskStatus::Failed
            | WorkflowTaskStatus::Skipped => {}
        }
    }
    every.to_vec()
}

/// The text that the enum writes for `status`.
fn stored_status(status: WorkflowTaskStatus) -> &'static str {
    let text = match status {
        WorkflowTaskStatus::Pending => "PENDING",
        WorkflowTaskStatus::Ready => "READY",
        WorkflowTaskStatus::Enqueued => "ENQUEUED",
        WorkflowTaskStatus::Running => "RUNNING",
        WorkflowTaskStatus::Completed => "COMPLETED",
        WorkflowTaskStatus::Failed => "FAILED",
        WorkflowTaskStatus::Skipped => "SKIPPED",
    };
    assert_eq!(
        serde_json::to_value(status).expect("a status encodes"),
        text,
        "the text of {status:?}"
    );
    text
}

/// Rows of a start: the workflow rows with `workflow_id`, and the task rows of `task_name`.
async fn start_rows(pool: &PgPool, workflow_id: Uuid, task_name: &str) -> (i64, i64) {
    sqlx::query_as(
        "SELECT (SELECT count(*) FROM horsies_workflows WHERE id = $1),
                (SELECT count(*) FROM horsies_tasks WHERE task_name = $2)",
    )
    .bind(workflow_id)
    .bind(task_name)
    .fetch_one(pool)
    .await
    .expect("count the rows of the start")
}

fn after(mut node: AnyNode, dependencies: &[usize]) -> AnyNode {
    node.dependencies = dependencies.to_vec();
    node
}

/// The task name of the nodes whose options do not parse.
const CORRUPT_OPTIONS_TASK: &str = "start_branch_corrupt_options";

/// Starts a workflow whose root, then whose dependent, has task options that
/// do not parse. Returns per case: its name, the refusal, and the rows the
/// start left (workflow rows, task rows of [`CORRUPT_OPTIONS_TASK`]).
async fn starts_with_corrupt_options(
    pool: &PgPool,
) -> Vec<(&'static str, ClientStartError, (i64, i64))> {
    let mut root = task_node(0, CORRUPT_OPTIONS_TASK);
    root.task_options_json = Some("{not-json".to_owned());
    let mut dependent = after(task_node(1, CORRUPT_OPTIONS_TASK), &[0]);
    dependent.task_options_json = Some("{not-json".to_owned());
    let cases = [
        ("root", vec![root]),
        (
            "dependent",
            vec![task_node(0, "start_branch_root"), dependent],
        ),
    ];
    let mut refusals = Vec::new();
    for (case, tasks) in cases {
        let workflow_id = Uuid::new_v4();
        let (_, started) = try_start(
            pool,
            &spec("start_branch_corrupt_options", tasks),
            Some(workflow_id),
            &TestChildSpecs::empty(),
        )
        .await;
        let refused = match started {
            Ok(outcome) => panic!("{case}: the start must be refused, got {outcome:?}"),
            Err(refused) => refused,
        };
        let rows = start_rows(pool, workflow_id, CORRUPT_OPTIONS_TASK).await;
        refusals.push((case, refused, rows));
    }
    refusals
}

/// `TestTransactionSerializationError::test_corrupt_task_options_of_a_root_propagate_and_nothing_commits`
/// (and, after the ledger's snapshot, `..._of_a_dependent_propagate_and_nothing_is_sent`):
/// task options of a node that do not parse refuse the start, and nothing is
/// written.
#[tokio::test]
#[serial]
async fn a_node_whose_options_do_not_parse_refuses_the_start_and_writes_nothing() {
    let pool = shared_pool().await;
    for (case, refused, rows) in starts_with_corrupt_options(&pool).await {
        assert!(
            matches!(
                &refused,
                ClientStartError::Start(
                    WorkflowError::Validation(_) | WorkflowError::Serialization(_)
                )
            ),
            "{case}: {refused:?}"
        );
        assert!(!refused.is_retryable(), "{case}");
        assert_eq!(rows, (0, 0), "{case}");
    }
}

/// The message half of
/// [`a_node_whose_options_do_not_parse_refuses_the_start_and_writes_nothing`]:
/// the refusal names the task of the node, as Python's does.
#[tokio::test]
#[serial]
async fn the_refusal_of_options_that_do_not_parse_names_the_task_of_the_node() {
    let pool = shared_pool().await;
    for (case, refused, _rows) in starts_with_corrupt_options(&pool).await {
        assert!(
            refused.to_string().contains(CORRUPT_OPTIONS_TASK),
            "{case}: {refused}"
        );
    }
}

/// `TestTransactionAsserts::test_a_priority_that_turns_none_after_prevalidation_fails_an_assert`:
/// a task node with no resolved priority refuses the start, and nothing is
/// written.
#[tokio::test]
#[serial]
async fn a_task_node_without_a_resolved_priority_refuses_the_start_and_writes_nothing() {
    let pool = shared_pool().await;
    let task_name = "start_branch_no_priority";
    let mut root = task_node(0, task_name);
    root.priority = None;
    let workflow_id = Uuid::new_v4();

    let (_, started) = try_start(
        &pool,
        &spec("start_branch_no_priority", vec![root]),
        Some(workflow_id),
        &TestChildSpecs::empty(),
    )
    .await;

    let refused = match started {
        Ok(outcome) => panic!("the start must be refused, got {outcome:?}"),
        Err(refused) => refused,
    };
    assert!(refused.to_string().contains("priority"), "{refused}");
    assert_eq!(start_rows(&pool, workflow_id, task_name).await, (0, 0));
}

/// `TestStartSerialization::test_start_stores_the_index_of_a_subworkflow_node`
/// (`test_success_policy_subworkflow_node.py`): the start stores a success
/// policy whose cases and optional list name sub-workflow nodes.
#[tokio::test]
#[serial]
async fn the_start_stores_a_success_policy_that_names_sub_workflow_nodes() {
    let pool = shared_pool().await;
    let child = spec(
        "start_branch_policy_child",
        vec![task_node(0, "start_branch_policy_child_task")],
    );
    let child_specs = child_specs_of(std::slice::from_ref(&child));
    let mut parent = spec(
        "start_branch_policy_parent",
        vec![
            task_node(0, "start_branch_policy_task"),
            sub_workflow_node(1, &child),
            sub_workflow_node(2, &child),
        ],
    );
    let mut policy = required(&[&[0, 1]]);
    policy.optional_indices = Some(vec![2]);
    parent.success_policy = Some(policy);

    let workflow_id = start(&pool, &parent, &child_specs).await;

    let stored: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT success_policy FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .fetch_one(&pool)
            .await
            .expect("read the stored policy");
    let children = [
        child_of(&pool, workflow_id, 1).await,
        child_of(&pool, workflow_id, 2).await,
    ];
    end_workflows(&pool, &[workflow_id, children[0], children[1]]).await;
    let stored = stored.expect("the workflow stores its policy");
    assert_eq!(
        stored["cases"],
        serde_json::json!([{"required_indices": [0, 1]}])
    );
    assert_eq!(stored["optional_indices"], serde_json::json!([2]));
}

/// `TestResults::test_no_workflow_row_is_workflow_not_found`
/// (`test_workflow_handle_missing_workflow.py`): the node results read of a
/// missing workflow is absent, not an empty list; a client reads it as
/// `WORKFLOW_NOT_FOUND`.
#[tokio::test]
#[serial]
async fn the_node_results_of_a_missing_workflow_are_absent() {
    let pool = shared_pool().await;
    let workflow_id = Uuid::new_v4();

    let read = handle(&pool, workflow_id).node_results().await;

    assert_eq!(read.expect("the read"), NodeResultsRead::WorkflowAbsent);
}

/// `TestTasks::test_no_workflow_row_is_workflow_not_found`
/// (`test_workflow_handle_missing_workflow.py`): the node read of a missing
/// workflow is absent, not an empty list; a client reads it as
/// `WORKFLOW_NOT_FOUND`.
#[tokio::test]
#[serial]
async fn the_nodes_of_a_missing_workflow_are_absent() {
    let pool = shared_pool().await;
    let workflow_id = Uuid::new_v4();

    let read = handle(&pool, workflow_id).nodes().await;

    assert_eq!(read.expect("the read"), NodesRead::WorkflowAbsent);
}

/// `TestTasks::test_a_workflow_without_a_node_is_an_empty_list`
/// (`test_workflow_handle_missing_workflow.py`).
#[tokio::test]
#[serial]
async fn tasks_of_a_workflow_without_a_node_is_an_empty_list() {
    let pool = shared_pool().await;
    let workflow_id = seed_workflow(&pool, &WorkflowSeed::running()).await;

    let read = handle(&pool, workflow_id).tasks().await;

    end_workflows(&pool, &[workflow_id]).await;
    assert!(read.expect("tasks()").is_empty());
}

/// `TestStoredSubworkflowSummary::test_a_summary_past_the_depth_bound_is_stored_data_invalid`
/// (`test_workflow_handle_error_variants.py`): the node read returns a
/// stored summary nested past the read's depth bound as stored; a client
/// refuses it with `STORED_DATA_INVALID`.
#[tokio::test]
#[serial]
async fn a_summary_past_the_depth_bound_is_read_as_stored() {
    let pool = shared_pool().await;
    let parent = seed_workflow(&pool, &WorkflowSeed::running()).await;
    let child_key = "tests.start_branch.deep_summary_child.v1";
    let child = seed_workflow(
        &pool,
        &WorkflowSeed::child(ParentLink::of_root(parent, 0)).status("COMPLETED"),
    )
    .await;
    let deep_summary = format!("{{\"status\":{}{}}}", "[".repeat(1_100), "]".repeat(1_100));
    let _node = seed_node(
        &pool,
        &NodeSeed {
            sub_workflow_summary: Some(deep_summary.clone()),
            ..NodeSeed::sub_workflow(parent, 0, "COMPLETED", child_key, Some(child))
        },
    )
    .await;

    let read = handle(&pool, parent).nodes().await;

    end_workflows(&pool, &[parent, child]).await;
    match read.expect("the node read") {
        NodesRead::Nodes(nodes) => {
            assert_eq!(
                nodes[0].sub_workflow_summary.as_deref(),
                Some(deep_summary.as_str())
            );
        }
        NodesRead::WorkflowAbsent => panic!("the parent {parent} has a row"),
    }
}

/// A workflow with one task node per status of the enum, in the order of
/// [`every_node_status`].
async fn seed_one_node_per_status(pool: &PgPool) -> Uuid {
    let workflow_id = seed_workflow(pool, &WorkflowSeed::running()).await;
    for (task_index, status) in (0_i32..).zip(every_node_status()) {
        let _node = seed_node(
            pool,
            &NodeSeed::task(workflow_id, task_index, stored_status(status)),
        )
        .await;
    }
    workflow_id
}

/// `TestCheckMatchesEnum::test_the_baseline_check_equals_the_enum`
/// (`test_workflow_node_status_check.py`): the node status CHECK accepts the
/// text of each status of the enum and refuses any other text.
#[tokio::test]
#[serial]
async fn the_node_status_check_accepts_each_status_of_the_enum_and_refuses_another() {
    let pool = shared_pool().await;
    let workflow_id = seed_one_node_per_status(&pool).await;

    let refused = sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, status,
             is_subworkflow, max_retries, execution, retention_class_key, created_at
         ) VALUES (
             'NONE', $1, $2, 99, 'node_99', 'port_task', '{}', 'default', 100, '{}', FALSE,
             'all', 'BOGUS', FALSE, 0, 'ISOLATED', 'forever', NOW()
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .execute(&pool)
    .await;

    let stored: i64 =
        sqlx::query_scalar("SELECT count(*) FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .fetch_one(&pool)
            .await
            .expect("count the nodes");
    end_workflows(&pool, &[workflow_id]).await;
    assert_eq!(stored, every_node_status().len() as i64);
    let error = refused.expect_err("a status outside the enum is refused");
    assert_eq!(
        error
            .as_database_error()
            .and_then(|database_error| database_error.constraint()),
        Some("horsies_workflow_tasks_status_known"),
        "{error}"
    );
}

/// `TestTasksAsync::test_a_stored_member_value_is_the_node_status`
/// (`test_workflow_node_status_check.py`): `tasks()` reads the stored text of
/// each status as that member of the enum.
#[tokio::test]
#[serial]
async fn tasks_reads_each_stored_node_status_as_its_enum_member() {
    let pool = shared_pool().await;
    let workflow_id = seed_one_node_per_status(&pool).await;

    let read = handle(&pool, workflow_id).tasks().await;

    end_workflows(&pool, &[workflow_id]).await;
    let statuses: Vec<WorkflowTaskStatus> = read
        .expect("tasks()")
        .into_iter()
        .map(|info| info.status)
        .collect();
    assert_eq!(statuses, every_node_status());
}

/// `TestTasksAsync::test_another_stored_value_crashes_with_its_subject`
/// (`test_workflow_node_status_check.py`): the node read returns a stored
/// status outside the enum (possible only without the CHECK) as stored; a
/// client refuses it with `STORED_DATA_INVALID`.
#[tokio::test]
#[serial]
async fn a_node_status_outside_the_enum_is_read_as_stored() {
    let db = IsolatedDatabase::create().await;
    execute(
        &db.pool,
        "ALTER TABLE horsies_workflow_tasks DROP CONSTRAINT horsies_workflow_tasks_status_known",
    )
    .await;
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    let _node = seed_node(
        &db.pool,
        &NodeSeed {
            node_id: "node-9".to_owned(),
            ..NodeSeed::task(workflow_id, 0, "BOGUS")
        },
    )
    .await;

    let read = handle(&db.pool, workflow_id).nodes().await;

    db.drop().await;
    match read.expect("the node read") {
        NodesRead::Nodes(nodes) => {
            assert_eq!(nodes[0].node_id.as_deref(), Some("node-9"));
            assert_eq!(nodes[0].status, "BOGUS");
        }
        NodesRead::WorkflowAbsent => panic!("the workflow {workflow_id} has a row"),
    }
}

//! Workflow names, runs, run detail, node detail, schedules and the list
//! order plan.
//!
//! Python specification: `tests/integration/test_monitoring_queries.py`
//! (`TestWorkflowNames`, `TestListWorkflowRuns`, `TestGetWorkflowRun`,
//! `TestGetWorkflowNode`, `TestListSchedules`) and
//! `tests/integration/test_monitoring_query_plans.py`, baseline versions on
//! `program/0.8.0`. Each test runs on a database of its own
//! ([`SpecDatabase`]).

use std::collections::BTreeMap;

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::spec_support::{
    seed_node, seed_run, seed_terminal, seed_terminal_with_attempts, set_history_status,
    AttemptText, Ending, LiveTask, NodeKind, NodeSeed, RunSeed, SpecDatabase,
};
use crate::core::task::error::{OutcomeCode, TaskError};
use crate::core::task::result::TaskResult;
use crate::monitoring::{
    get_workflow_node, get_workflow_run, list_schedules, list_workflow_names, list_workflow_runs,
    MonitoringQueryErrorCode, WorkflowNodeInfo, WorkflowRunsQuery,
};

/// Upper bound of a span that counts up to now (Python
/// `_LIVE_SPAN_CEILING_S`).
const LIVE_SPAN_CEILING_S: i64 = 600;

/// Live rows of the plan test: enough that the planner prefers the index
/// to a sort (the population of the kept index test).
const PLAN_POPULATION: i64 = 2_500;

fn child_of(parent: Uuid, name: &str) -> RunSeed {
    RunSeed {
        parent: Some(parent),
        ..RunSeed::running(name)
    }
}

/// A stored node result that failed with `message`.
fn failed_result(message: &str) -> String {
    let result: TaskResult<serde_json::Value> =
        TaskResult::Err(TaskError::builtin(OutcomeCode::UpstreamSkipped, message));
    serde_json::to_string(&result).expect("encode the stored node result")
}

fn nodes_by_index(nodes: &[WorkflowNodeInfo]) -> BTreeMap<i32, &WorkflowNodeInfo> {
    nodes.iter().map(|node| (node.task_index, node)).collect()
}

/// Insert one schedule state row. A schedule that ran carries its last run
/// fields and a positive run count, as the baseline CHECK requires.
async fn seed_schedule(
    pool: &PgPool,
    schedule_name: &str,
    next_run_in_seconds: i64,
    last_run: Option<(Uuid, i32)>,
) {
    let (last_task_id, run_count) = match last_run {
        Some((task_id, run_count)) => (Some(task_id), run_count),
        None => (None, 0),
    };
    sqlx::query(
        "INSERT INTO horsies_schedule_state (
            schedule_name, next_run_at, last_run_at, last_task_id, run_count,
            config_hash, updated_at
        ) VALUES (
            $1, NOW() + make_interval(secs => $2),
            CASE WHEN $3::uuid IS NULL THEN NULL ELSE NOW() - interval '60 seconds' END,
            $3, $4, decode(repeat('00', 32), 'hex'), NOW()
        )",
    )
    .bind(schedule_name)
    .bind(next_run_in_seconds)
    .bind(last_task_id)
    .bind(run_count)
    .execute(pool)
    .await
    .expect("seed a schedule state row");
}

/// Python `TestWorkflowNames::test_returns_sorted_distinct_root_names`.
#[tokio::test]
#[serial]
async fn workflow_names_are_the_sorted_distinct_names_of_root_runs() {
    let database = SpecDatabase::create().await;
    let root = RunSeed::running("zeta_flow");
    seed_run(&database.pool, &root).await;
    seed_run(&database.pool, &RunSeed::running("alpha_flow")).await;
    seed_run(&database.pool, &RunSeed::running("alpha_flow")).await;
    seed_run(&database.pool, &child_of(root.id, "child_flow")).await;

    let names = list_workflow_names(&database.broker())
        .await
        .expect("read the workflow names");

    assert_eq!(names, ["alpha_flow", "zeta_flow"]);
    database.drop().await;
}

/// Python `TestWorkflowNames::test_empty_database_yields_empty_list`.
#[tokio::test]
#[serial]
async fn no_run_gives_no_workflow_name() {
    let database = SpecDatabase::create().await;

    let names = list_workflow_names(&database.broker())
        .await
        .expect("read the workflow names");

    assert!(names.is_empty(), "{names:?}");
    database.drop().await;
}

/// Python `TestListWorkflowRuns::test_newest_first_and_children_excluded`.
#[tokio::test]
#[serial]
async fn runs_come_newest_first_without_child_runs() {
    let database = SpecDatabase::create().await;
    let mut root = RunSeed::running("alpha_flow");
    root.created_seconds_ago = 300;
    let mut newer = RunSeed::running("beta_flow");
    newer.created_seconds_ago = 100;
    let mut child = child_of(root.id, "child_flow");
    child.created_seconds_ago = 10;
    for run in [&root, &newer, &child] {
        seed_run(&database.pool, run).await;
    }

    let runs = list_workflow_runs(&database.broker(), &WorkflowRunsQuery::new())
        .await
        .expect("list the runs");

    let names: Vec<&str> = runs.iter().map(|run| run.name.as_str()).collect();
    assert_eq!(names, ["beta_flow", "alpha_flow"]);
    database.drop().await;
}

/// Python `TestListWorkflowRuns::test_limit_truncates_the_result`.
#[tokio::test]
#[serial]
async fn the_run_limit_truncates_the_list() {
    let database = SpecDatabase::create().await;
    for index in 0..4 {
        let mut run = RunSeed::running("alpha_flow");
        run.created_seconds_ago = index + 1;
        seed_run(&database.pool, &run).await;
    }

    let runs = list_workflow_runs(
        &database.broker(),
        &WorkflowRunsQuery::new()
            .with_limit(2)
            .expect("a limit of 2 is accepted"),
    )
    .await
    .expect("list the runs");

    assert_eq!(runs.len(), 2);
    database.drop().await;
}

/// Python `TestListWorkflowRuns::test_wall_time_counts_up_until_completion`:
/// an open run's wall time counts up; a finished run's wall time is closed
/// at its completion.
#[tokio::test]
#[serial]
async fn wall_time_counts_up_until_the_run_completes() {
    let database = SpecDatabase::create().await;
    let mut open = RunSeed::running("open_flow");
    open.status = "PAUSED".to_owned();
    open.created_seconds_ago = 45;
    let mut closed = RunSeed::running("closed_flow");
    closed.status = "COMPLETED".to_owned();
    closed.created_seconds_ago = 200;
    closed.completed_seconds_ago = Some(140);
    seed_run(&database.pool, &open).await;
    seed_run(&database.pool, &closed).await;

    let runs = list_workflow_runs(&database.broker(), &WorkflowRunsQuery::new())
        .await
        .expect("list the runs");

    let walls: BTreeMap<&str, Option<i64>> = runs
        .iter()
        .map(|run| (run.name.as_str(), run.wall_s))
        .collect();
    assert_eq!(walls["closed_flow"], Some(60));
    let open_wall = walls["open_flow"].expect("an open run has a wall time");
    assert!(
        (45..=LIVE_SPAN_CEILING_S).contains(&open_wall),
        "{open_wall}"
    );
    database.drop().await;
}

/// Python `TestGetWorkflowRun::test_failed_nodes_are_indexed_in_ascending_order`.
#[tokio::test]
#[serial]
async fn failed_node_indices_come_in_ascending_order() {
    let database = SpecDatabase::create().await;
    let mut run = RunSeed::running("alpha_flow");
    run.status = "FAILED".to_owned();
    seed_run(&database.pool, &run).await;
    for (task_index, status) in [(0, "COMPLETED"), (1, "FAILED"), (2, "FAILED")] {
        let mut node = NodeSeed::task(run.id, task_index);
        node.status = status.to_owned();
        seed_node(&database.pool, &node).await;
    }

    let detail = get_workflow_run(&database.broker(), run.id)
        .await
        .expect("read the run")
        .expect("the run exists");

    assert_eq!(detail.failed_indices, [1, 2]);
    assert_eq!(detail.failed_count, 2);
    database.drop().await;
}

/// Python `TestGetWorkflowRun::test_subworkflow_node_rolls_up_its_child_run`:
/// a sub-workflow node counts the nodes of its child run; a child run with
/// no node, and a task node, carry no counts.
#[tokio::test]
#[serial]
async fn a_sub_workflow_node_counts_its_child_and_an_empty_child_gives_no_counts() {
    let database = SpecDatabase::create().await;
    let run = RunSeed::running("alpha_flow");
    let child = child_of(run.id, "child_flow");
    let empty_child = child_of(run.id, "empty_child");
    for seeded in [&run, &child, &empty_child] {
        seed_run(&database.pool, seeded).await;
    }
    for (task_index, kind) in [
        (
            0,
            NodeKind::SubWorkflow {
                child: Some(child.id),
            },
        ),
        (
            1,
            NodeKind::SubWorkflow {
                child: Some(empty_child.id),
            },
        ),
        (2, NodeKind::Task { task_id: None }),
    ] {
        let mut node = NodeSeed::task(run.id, task_index);
        node.kind = kind;
        seed_node(&database.pool, &node).await;
    }
    for (task_index, status) in [(0, "COMPLETED"), (1, "FAILED")] {
        let mut node = NodeSeed::task(child.id, task_index);
        node.status = status.to_owned();
        seed_node(&database.pool, &node).await;
    }

    let detail = get_workflow_run(&database.broker(), run.id)
        .await
        .expect("read the run")
        .expect("the run exists");

    let nodes = nodes_by_index(&detail.nodes);
    assert_eq!(
        (nodes[&0].child_total, nodes[&0].child_failed),
        (Some(2), Some(1))
    );
    assert_eq!(
        (nodes[&1].child_total, nodes[&1].child_failed),
        (None, None)
    );
    assert_eq!(
        (nodes[&2].child_total, nodes[&2].child_failed),
        (None, None)
    );
    database.drop().await;
}

/// Python `TestGetWorkflowRun::test_child_run_is_reachable_by_the_same_query`.
#[tokio::test]
#[serial]
async fn a_child_run_is_read_by_the_run_detail() {
    let database = SpecDatabase::create().await;
    let run = RunSeed::running("alpha_flow");
    let child = child_of(run.id, "child_flow");
    seed_run(&database.pool, &run).await;
    seed_run(&database.pool, &child).await;
    seed_node(&database.pool, &NodeSeed::task(child.id, 0)).await;

    let detail = get_workflow_run(&database.broker(), child.id)
        .await
        .expect("read the child run")
        .expect("the child run exists");

    assert_eq!(detail.run.id, child.id);
    assert_eq!(detail.nodes.len(), 1);
    database.drop().await;
}

/// Python `TestGetWorkflowRun::test_node_execution_time_per_status`: a
/// RUNNING node counts up, a COMPLETED node is closed, a SKIPPED node with
/// no completion and an ENQUEUED node have none.
#[tokio::test]
#[serial]
async fn node_execution_time_follows_the_node_status() {
    let database = SpecDatabase::create().await;
    let run = RunSeed::running("alpha_flow");
    seed_run(&database.pool, &run).await;
    for (task_index, status, started, completed) in [
        (0, "RUNNING", Some(35), None),
        (1, "COMPLETED", Some(90), Some(60)),
        (2, "SKIPPED", Some(90), None),
        (3, "ENQUEUED", Some(90), None),
    ] {
        let mut node = NodeSeed::task(run.id, task_index);
        node.status = status.to_owned();
        node.started_seconds_ago = started;
        node.completed_seconds_ago = completed;
        seed_node(&database.pool, &node).await;
    }

    let detail = get_workflow_run(&database.broker(), run.id)
        .await
        .expect("read the run")
        .expect("the run exists");

    let nodes = nodes_by_index(&detail.nodes);
    let running = nodes[&0].exec_s.expect("a running node counts up");
    assert!((35..=LIVE_SPAN_CEILING_S).contains(&running), "{running}");
    assert_eq!(nodes[&1].exec_s, Some(30));
    assert_eq!(nodes[&2].exec_s, None);
    assert_eq!(nodes[&3].exec_s, None);
    database.drop().await;
}

/// Python `TestGetWorkflowNode::test_returns_backing_task_and_attempts`: the
/// node detail carries the node id, the node error, the backing task and its
/// attempts in ascending order.
#[tokio::test]
#[serial]
async fn a_node_reports_its_id_error_backing_task_and_attempts() {
    let database = SpecDatabase::create().await;
    let run = RunSeed::running("alpha_flow");
    seed_run(&database.pool, &run).await;
    let mut task = LiveTask::pending("alpha_task");
    task.is_workflow_task = true;
    let empty = || AttemptText {
        error_code: None,
        error_message: None,
        failed_reason: None,
    };
    // The node comes first: the move of a failed workflow task reads its
    // node (`horsies_move_task_to_history`, deferred phase 2).
    let mut node = NodeSeed::task(run.id, 0);
    node.node_id = Some("step-one".to_owned());
    node.status = "FAILED".to_owned();
    node.kind = NodeKind::Task {
        task_id: Some(task.id),
    };
    node.result = Some(failed_result("node blew up"));
    seed_node(&database.pool, &node).await;
    seed_terminal_with_attempts(
        &database.pool,
        &task,
        Ending::failed("TASK_EXCEPTION"),
        &[(2, false, empty()), (1, true, empty())],
    )
    .await;

    let detail = get_workflow_node(&database.broker(), run.id, 0)
        .await
        .expect("read the node")
        .expect("the node exists");

    assert_eq!(detail.node_id.as_deref(), Some("step-one"));
    assert_eq!(detail.node_error.as_deref(), Some("node blew up"));
    let leaf = detail.leaf.expect("the node has its task");
    assert_eq!(leaf.task_id, task.id);
    let attempts: Vec<i32> = detail
        .attempts
        .iter()
        .map(|attempt| attempt.attempt)
        .collect();
    assert_eq!(attempts, [1, 2]);
    database.drop().await;
}

/// Python `TestGetWorkflowNode::test_backing_history_row_with_an_unknown_status_is_a_contract_violation`.
#[tokio::test]
#[serial]
async fn an_unknown_status_of_the_backing_history_row_is_a_contract_violation() {
    let database = SpecDatabase::create().await;
    let run = RunSeed::running("alpha_flow");
    seed_run(&database.pool, &run).await;
    let mut task = LiveTask::pending("alpha_task");
    task.is_workflow_task = true;
    // The node comes first: the move of a failed workflow task reads its
    // node (`horsies_move_task_to_history`, deferred phase 2).
    let mut node = NodeSeed::task(run.id, 0);
    node.status = "FAILED".to_owned();
    node.kind = NodeKind::Task {
        task_id: Some(task.id),
    };
    seed_node(&database.pool, &node).await;
    seed_terminal(&database.pool, &task, Ending::failed("TASK_EXCEPTION")).await;
    set_history_status(&database.pool, task.id, "NOT_KNOWN").await;

    let error = get_workflow_node(&database.broker(), run.id, 0)
        .await
        .expect_err("an unknown status is refused");

    assert_eq!(
        error.code,
        MonitoringQueryErrorCode::HistoryContractViolation
    );
    assert!(!error.retryable);
    assert!(error.message.contains("NOT_KNOWN"), "{}", error.message);
    database.drop().await;
}

/// Python `TestGetWorkflowNode::test_vanished_backing_task_still_returns_the_node`:
/// retention can remove the task while the node row stays; the node still
/// resolves, with no leaf and no attempts.
#[tokio::test]
#[serial]
async fn a_node_whose_task_is_gone_still_resolves_without_a_leaf() {
    let database = SpecDatabase::create().await;
    let run = RunSeed::running("alpha_flow");
    seed_run(&database.pool, &run).await;
    let mut node = NodeSeed::task(run.id, 0);
    node.status = "COMPLETED".to_owned();
    node.kind = NodeKind::Task {
        task_id: Some(Uuid::new_v4()),
    };
    seed_node(&database.pool, &node).await;

    let detail = get_workflow_node(&database.broker(), run.id, 0)
        .await
        .expect("read the node")
        .expect("the node exists");

    assert!(detail.leaf.is_none(), "{:?}", detail.leaf);
    assert!(detail.attempts.is_empty(), "{:?}", detail.attempts);
    database.drop().await;
}

/// Python `TestGetWorkflowNode::test_empty_node_error_is_normalized_to_none`:
/// a stored error with an empty message shows no node error.
#[tokio::test]
#[serial]
async fn an_empty_node_error_reads_as_absent() {
    let database = SpecDatabase::create().await;
    let run = RunSeed::running("alpha_flow");
    seed_run(&database.pool, &run).await;
    let mut node = NodeSeed::task(run.id, 0);
    node.status = "FAILED".to_owned();
    node.result = Some(failed_result(""));
    seed_node(&database.pool, &node).await;

    let detail = get_workflow_node(&database.broker(), run.id, 0)
        .await
        .expect("read the node")
        .expect("the node exists");

    assert_eq!(detail.node_error, None);
    database.drop().await;
}

/// Python `TestListSchedules::test_empty_state_yields_empty_list`.
#[tokio::test]
#[serial]
async fn no_schedule_state_gives_an_empty_schedule_list() {
    let database = SpecDatabase::create().await;

    let schedules = list_schedules(&database.broker())
        .await
        .expect("list the schedules");

    assert!(schedules.is_empty(), "{schedules:?}");
    database.drop().await;
}

/// Python `TestListSchedules::test_ordered_by_next_run`: the schedule that
/// runs first comes first, with its run count. The schedule that ran carries
/// the id of its last task, read from the `uuid` column; the one that never
/// ran carries none.
#[tokio::test]
#[serial]
async fn schedules_come_in_next_run_order_with_their_run_count() {
    let database = SpecDatabase::create().await;
    let last_task_id = Uuid::new_v4();
    seed_schedule(&database.pool, "later", 3 * 3_600, None).await;
    seed_schedule(&database.pool, "soon", 300, Some((last_task_id, 4))).await;

    let schedules = list_schedules(&database.broker())
        .await
        .expect("list the schedules");

    let names: Vec<&str> = schedules
        .iter()
        .map(|schedule| schedule.schedule_name.as_str())
        .collect();
    assert_eq!(names, ["soon", "later"]);
    assert!(schedules[0].next_run_at < schedules[1].next_run_at);
    assert_eq!(schedules[0].run_count, 4);
    assert_eq!(schedules[0].last_task_id, Some(last_task_id));
    assert_eq!(schedules[1].last_task_id, None);
    assert_eq!(
        serde_json::to_value(&schedules[0]).expect("encode the schedule")["last_task_id"],
        serde_json::Value::String(last_task_id.to_string())
    );
    database.drop().await;
}

/// The plan of `statement` as text, after the table was sampled.
async fn plan_of(pool: &PgPool, statement: &str) -> String {
    let lines: Vec<String> = sqlx::query_scalar(&format!("EXPLAIN {statement}"))
        .fetch_all(pool)
        .await
        .expect("explain the statement");
    lines.join("\n")
}

/// Python `test_monitoring_query_plans.py::test_list_default_sort_plans_on_enqueued_at_index`,
/// both directions, on the live page statement of Python's baseline list
/// (the live row's columns, ordered by `enqueued_at`, limited to the page
/// reach): the order comes from `idx_horsies_tasks_enqueued_at` with no
/// sort. The kept `broker::monitoring_schema_tests` test checks the
/// descending direction only.
#[tokio::test]
#[serial]
async fn the_enqueue_order_reads_the_enqueue_index_in_both_directions() {
    let database = SpecDatabase::create().await;
    sqlx::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at,
            command_fingerprint_version, command_fingerprint, idempotency_key_digest,
            status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
            started_at, finalizing_at, finalizing_by_worker_id,
            worker_pid, worker_hostname, worker_process_name,
            retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
            updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
        )
        SELECT gen_random_uuid(), 'plan_task_' || (g % 5), 'default', 100, '{}', NULL,
               'ISOLATED', 'forever', NOW(), NOW(),
               1, decode(repeat('08', 32), 'hex'), NULL,
               'PENDING', NOW() - make_interval(secs => g), NULL, NULL, NULL,
               NULL, NULL, NULL,
               NULL, NULL, NULL,
               0, 0, NULL, NULL, FALSE,
               NOW(), FALSE, NULL, NULL
        FROM generate_series(1, $1) AS g",
    )
    .bind(PLAN_POPULATION)
    .execute(&database.pool)
    .await
    .expect("seed the plan population");
    sqlx::query("ANALYZE horsies_tasks")
        .execute(&database.pool)
        .await
        .expect("sample the live table");

    for direction in ["ASC", "DESC"] {
        let plan = plan_of(
            &database.pool,
            &format!(
                "SELECT id, task_name, queue_name, status, priority, retry_count, max_retries,
                        is_workflow_task, worker_hostname, claimed_by_worker_id,
                        enqueued_at, started_at
                 FROM horsies_tasks ORDER BY enqueued_at {direction} LIMIT 50"
            ),
        )
        .await;

        assert!(
            plan.contains("idx_horsies_tasks_enqueued_at"),
            "{direction}: {plan}"
        );
        assert!(!plan.contains("Sort"), "{direction}: {plan}");
    }
    database.drop().await;
}

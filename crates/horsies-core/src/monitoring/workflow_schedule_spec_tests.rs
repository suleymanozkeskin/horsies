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
use std::sync::PoisonError;

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
use crate::test_support::connect_collecting_notices;

/// Upper bound of a span that counts up to now (Python
/// `_LIVE_SPAN_CEILING_S`).
const LIVE_SPAN_CEILING_S: i64 = 600;

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
    // The second run fails: attempt 1 is the earlier failure that was
    // retried, and the end writes attempt 2.
    let mut task = LiveTask::pending("alpha_task").with_retry_count(1);
    task.is_workflow_task = true;
    let empty = AttemptText {
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
        &[(1, true, empty)],
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
        error.code(),
        MonitoringQueryErrorCode::HistoryContractViolation
    );
    assert!(!error.retryable());
    assert!(error.message().contains("NOT_KNOWN"), "{}", error.message());
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

/// One plan read: no sequential scan, and the plan of every statement
/// inside a function sent as a notice (Python `_nested_plans`). Seeded
/// tables fit in a few pages, where the planner prefers a sequential scan
/// and would hide the index choice of a production-sized table.
const NESTED_PLAN_SETTINGS: &str = "SET LOCAL enable_seqscan = off;
     LOAD 'auto_explain';
     SET LOCAL auto_explain.log_min_duration = 0;
     SET LOCAL auto_explain.log_nested_statements = on;
     SET LOCAL auto_explain.log_level = 'notice';";

/// The task list in its default sort, one page of 50 (Python
/// `_DEFAULT_SORT_LIST_SQL`). Parameter: the sort direction.
const DEFAULT_SORT_LIST: &str = "SELECT * FROM horsies_monitoring_task_list(
         now() - interval '1 day', now(), '{}', '{}', '{}', '{}', '{}', '{}', FALSE,
         '{}', '{}', 'enqueued_at', $1::text, 0, 50
     )";

/// The live rows of the plan test (Python `_seed_tasks`): 500 RUNNING rows
/// over five task names and a backlog of 2,000 PENDING rows, each with its
/// own enqueue time, then sampled. With only a few PENDING rows, a sort over
/// another index costs the same as the ordered index and the planner's pick
/// between them is arbitrary.
async fn seed_plan_population(pool: &PgPool) {
    for (status, task_name, started_at, rows) in [
        ("RUNNING", "'plan_task_' || (g % 5)", "NOW()", 500_i32),
        ("PENDING", "'plan_task_pending'", "NULL", 2_000_i32),
    ] {
        sqlx::query(&format!(
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
            SELECT gen_random_uuid(), {task_name}, 'default', 100, '{{}}', NULL,
                   'ISOLATED', 'standard_30d', NOW(), NOW(),
                   1, sha256(convert_to(gen_random_uuid()::text, 'UTF8')), NULL,
                   '{status}', NOW() - make_interval(secs => g), NULL, NULL, NULL,
                   {started_at}, NULL, NULL,
                   NULL, NULL, NULL,
                   0, 0, NULL, NULL, FALSE,
                   NOW(), FALSE, NULL, NULL
            FROM generate_series(1, $1) AS g"
        ))
        .bind(rows)
        .execute(pool)
        .await
        .unwrap_or_else(|error| panic!("seed the {status} rows of the plan test: {error}"));
    }
    sqlx::query("ANALYZE horsies_tasks")
        .execute(pool)
        .await
        .expect("sample the live table");
}

/// Python `test_monitoring_query_plans.py::test_list_default_sort_plans_on_enqueued_at_index`:
/// in both directions of the default sort, the page statement inside
/// `horsies_monitoring_task_list` reads the live side on
/// `idx_horsies_tasks_enqueued_at`, with no sequential scan of the live
/// table. The task id tie-break is an incremental sort on the index order
/// (presorted key `enqueued_at`): it sorts only the rows of one
/// `enqueued_at`. A `NULLS LAST` on the NOT NULL key, or a projection
/// change, moves the live side off the index and fails this test. A plain
/// `EXPLAIN` of the call shows a Function Scan only, so the plans come from
/// `auto_explain` with `log_nested_statements`, as notices, in a
/// transaction that is rolled back.
#[tokio::test]
#[serial]
async fn the_task_list_page_reads_the_live_enqueue_index_in_both_directions() {
    let database = SpecDatabase::create().await;
    seed_plan_population(&database.pool).await;
    let (client, notices) = connect_collecting_notices(database.url()).await;

    for direction in ["DESC", "ASC"] {
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute(NESTED_PLAN_SETTINGS)
            .await
            .expect("send the nested plans as notices");
        let rows = client
            .query(DEFAULT_SORT_LIST, &[&direction])
            .await
            .unwrap_or_else(|error| panic!("{direction}: read the task list: {error}"));
        client.batch_execute("ROLLBACK").await.expect("rollback");
        let plans =
            std::mem::take(&mut *notices.lock().unwrap_or_else(PoisonError::into_inner)).join("\n");

        assert!(!rows.is_empty(), "{direction}: the list has no TOTALS row");
        let page_plan = plans
            .split("duration:")
            .find(|plan| plan.contains("WITH live AS"))
            .unwrap_or_else(|| panic!("{direction}: no plan of the page statement: {plans}"));
        assert!(
            page_plan.contains("idx_horsies_tasks_enqueued_at"),
            "{direction}: {page_plan}"
        );
        assert!(
            page_plan.contains("Presorted Key: t.enqueued_at"),
            "{direction}: {page_plan}"
        );
        assert!(
            !page_plan.contains("Seq Scan on horsies_tasks"),
            "{direction}: {page_plan}"
        );
    }
    drop(client);
    database.drop().await;
}

/// Creates the `standard_30d` leaf of the UTC day `days_back` days before
/// today, or finds it when it exists, and returns its name.
async fn standard_leaf(pool: &PgPool, days_back: i32) -> String {
    sqlx::query_scalar(
        "SELECT leaf_name FROM horsies_create_history_leaf(
             'standard_30d', date_trunc('day', now(), 'UTC') - make_interval(days => $1)
         )
         WHERE outcome IN ('CREATED', 'ALREADY_CONFORMANT')",
    )
    .bind(days_back)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|error| panic!("create the leaf {days_back} days back: {error}"))
}

/// 600 history rows, half in the current `standard_30d` leaf and half in
/// the forever leaf, each with its own enqueue time, then sampled. The rows
/// pass the page reach (50), so the planner reads the leaves in index order
/// and stops at the reach instead of sorting every row. The live rows are
/// inserted RUNNING and moved in one statement through
/// `horsies_move_task_to_history`, which takes the locked live row.
async fn seed_history_population(pool: &PgPool) {
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
        SELECT gen_random_uuid(), 'history_task_' || g, 'default', 100, '{}', NULL,
               'ISOLATED', CASE WHEN g % 2 = 0 THEN 'forever' ELSE 'standard_30d' END,
               NOW(), NOW(),
               1, sha256(convert_to(gen_random_uuid()::text, 'UTF8')), NULL,
               'RUNNING', NOW() - make_interval(secs => 600 + g), NOW(), 'spec-worker', NULL,
               NOW() - make_interval(secs => 30), NULL, NULL,
               NULL, NULL, NULL,
               0, 3, NULL, NULL, FALSE,
               NOW(), FALSE, NULL, NULL
        FROM generate_series(1, 600) AS g",
    )
    .execute(pool)
    .await
    .expect("seed the running rows of the history plan test");
    let moved: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (
             SELECT horsies_move_task_to_history(
                 t, 'COMPLETED', 'COMPLETE_FUSED', now(), '{\"ok\": true}', NULL, NULL
             )
             FROM (SELECT * FROM horsies_tasks FOR UPDATE) AS t
         ) AS moved",
    )
    .fetch_one(pool)
    .await
    .expect("move the seeded rows to history");
    assert_eq!(moved, 600);
    sqlx::query("ANALYZE horsies_task_history")
        .execute(pool)
        .await
        .expect("sample the history");
}

/// In both directions of the default sort, the page statement inside
/// `horsies_monitoring_task_list` reads the history leaves of the window
/// through their `enqueued_at` index (backward for `DESC`), merged in that
/// order (Merge Append). The task id tie-break is an incremental sort on
/// that order (presorted key `h.enqueued_at`). A leaf outside the window is
/// pruned: the plan does not name it. The current `standard_30d` leaf and
/// the forever leaf are in the window; the leaf of ten days back is not.
#[tokio::test]
#[serial]
async fn the_task_list_page_reads_the_history_leaves_on_the_enqueue_index_and_prunes_the_rest() {
    let database = SpecDatabase::create().await;
    let current_leaf = standard_leaf(&database.pool, 0).await;
    let old_leaf = standard_leaf(&database.pool, 10).await;
    seed_history_population(&database.pool).await;
    let (client, notices) = connect_collecting_notices(database.url()).await;

    for (direction, scan) in [
        ("DESC", "Index Scan Backward using "),
        ("ASC", "Index Scan using "),
    ] {
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute(NESTED_PLAN_SETTINGS)
            .await
            .expect("send the nested plans as notices");
        let rows = client
            .query(DEFAULT_SORT_LIST, &[&direction])
            .await
            .unwrap_or_else(|error| panic!("{direction}: read the task list: {error}"));
        client.batch_execute("ROLLBACK").await.expect("rollback");
        let plans =
            std::mem::take(&mut *notices.lock().unwrap_or_else(PoisonError::into_inner)).join("\n");

        assert!(!rows.is_empty(), "{direction}: the list has no TOTALS row");
        let page_plan = plans
            .split("duration:")
            .find(|plan| plan.contains("WITH live AS"))
            .unwrap_or_else(|| panic!("{direction}: no plan of the page statement: {plans}"));
        let history_scans: Vec<&str> = page_plan
            .lines()
            .filter(|line| line.contains(" on horsies_task_history_"))
            .collect();
        assert!(
            history_scans
                .iter()
                .any(|line| line.contains(&format!(" on {current_leaf} "))),
            "{direction}: {page_plan}"
        );
        assert!(
            history_scans
                .iter()
                .any(|line| line.contains(" on horsies_task_history_forever_open ")),
            "{direction}: {page_plan}"
        );
        assert!(
            history_scans
                .iter()
                .all(|line| line.contains(scan) && line.contains("_enqueued_idx on ")),
            "{direction}: {page_plan}"
        );
        assert!(
            page_plan.contains("Merge Append"),
            "{direction}: {page_plan}"
        );
        assert!(
            page_plan.contains("Presorted Key: h.enqueued_at"),
            "{direction}: {page_plan}"
        );
        assert!(!page_plan.contains(&old_leaf), "{direction}: {page_plan}");
    }
    drop(client);
    database.drop().await;
}

/// The task stats of the last day with no filter
/// (`horsies_monitoring_task_stats`).
const LAST_DAY_STATS: &str = "SELECT * FROM horsies_monitoring_task_stats(
         now() - interval '1 day', now(), '{}', '{}', '{}', '{}', '{}', FALSE, '{}', '{}'
     )";

/// Python `test_task_history_detail_reads.py::TestPagesFacetsAggregates::test_aggregate_plan_visits_no_out_of_range_leaf`,
/// on the status counts of `horsies_monitoring_task_stats`: the count
/// statement is planned with the window bounds as values, so a leaf outside
/// the window is pruned and the plan does not name it. The current
/// `standard_30d` leaf and the forever leaf are in the window and read; the
/// leaf of ten days back is not.
#[tokio::test]
#[serial]
async fn the_task_stats_read_prunes_the_history_leaves_outside_the_window() {
    let database = SpecDatabase::create().await;
    let current_leaf = standard_leaf(&database.pool, 0).await;
    let old_leaf = standard_leaf(&database.pool, 10).await;
    seed_history_population(&database.pool).await;
    let (client, notices) = connect_collecting_notices(database.url()).await;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute(NESTED_PLAN_SETTINGS)
        .await
        .expect("send the nested plans as notices");
    let rows = client
        .query(LAST_DAY_STATS, &[])
        .await
        .expect("read the task stats");
    client.batch_execute("ROLLBACK").await.expect("rollback");
    let plans =
        std::mem::take(&mut *notices.lock().unwrap_or_else(PoisonError::into_inner)).join("\n");

    assert_eq!(rows.len(), 7, "one row per task status");
    let count_plan = plans
        .split("duration:")
        .find(|plan| plan.contains("WITH side_counts AS"))
        .unwrap_or_else(|| panic!("no plan of the count statement: {plans}"));
    assert!(
        count_plan.contains(&format!(" on {current_leaf} ")),
        "{count_plan}"
    );
    assert!(
        count_plan.contains(" on horsies_task_history_forever_open "),
        "{count_plan}"
    );
    assert!(!count_plan.contains(&old_leaf), "{count_plan}");
    drop(client);
    database.drop().await;
}

//! The workflow recovery failure bound against stored rows that raise on
//! every pass.
//!
//! Each test enters from the pass that the reaper calls and asserts on stored
//! rows; each has its own database, because a global pass reads every
//! workflow. Three stored rows raise on every pass, as in Python:
//!
//! - [`BAD_GOOD_UNTIL`] on a node: the insert of its task refuses the
//!   deadline text (a data exception). Phase-2 recovery tests refuse the
//!   insert of the workflow's task rows with a trigger instead
//!   ([`start_a_then_b`]), as Python's do.
//! - [`poison_success_policy`] on a workflow: the success evaluation fails.
//! - An output index with no node row: the final result fails.
//!
//! A child start raises on [`POISON_TASK_OPTIONS`] with a deadline
//! ([`poison_child_specs`]).

use serial_test::serial;
use uuid::Uuid;

use super::support::{
    code, finish_task, handle, node, node_status, ok, poison_success_policy, seed_node,
    seed_node_task, seed_workflow, spec, start, stored_result_error, task_node, workflow,
    workflow_error_code, workflow_status, IsolatedDatabase, LiveStatus, NodeSeed, ParentLink,
    WorkflowSeed, POISON_TASK_OPTIONS, WORKER,
};
use crate::core::config::retention::RetentionConfig;
use crate::core::lifecycle::TerminalizationOutcome;
use crate::core::{TaskError, TaskResult};
use crate::test_support::child_specs::TestChildSpecs;
use crate::test_support::sql::{self, TestPool};
use crate::test_support::terminal_writes;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::phase2_recovery::{drive_phase2_recovery_with, Phase2RecoverySummary};
use crate::workflow_engine::recovery::{
    recover_stuck_workflows_observed_with, recover_workflow_tree, RecoveryReport,
    GLOBAL_SCAN_ROW_CAP,
};
use crate::workflow_engine::recovery_limit::{
    record_candidate_failure, CandidateFailure, CountFailures, FailureClass, FailureRecord,
    RecoveryCase, RecoveryFailurePolicy,
};

/// `workflow_recovery_failure_limit` of every pass here (Python's `LIMIT`).
const LIMIT: u32 = 3;

/// The definition key of the child whose task options do not parse.
const POISON_CHILD_KEY: &str = "tests.port.poison_options_child.v1";

/// Text that the limit error message starts with (Python `recovery_limit_error`).
const LIMIT_MESSAGE: &str = "Workflow recovery raised a persistent error";

/// Stored task options whose `good_until` the database cannot read (Python's
/// `BAD_GOOD_UNTIL`). The options parse, so the node is not refused when its
/// task is built; the deadline text goes to the task insert, which raises
/// (Python `_build_enqueued_task_params`,
/// `clients/python/horsies/core/workflows/engine.py:831-835`).
const BAD_GOOD_UNTIL: &str = r#"{"good_until":"not-a-date"}"#;

async fn pass(pool: &TestPool, child_specs: &TestChildSpecs) -> RecoveryReport {
    recover_stuck_workflows_observed_with(
        &crate::broker::PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
        child_specs,
        LIMIT,
    )
    .await
    .expect("the recovery pass returns its report")
}

async fn passes(pool: &TestPool, child_specs: &TestChildSpecs, count: u32) -> Vec<RecoveryReport> {
    let mut reports = Vec::new();
    for _ in 0..count {
        reports.push(pass(pool, child_specs).await);
    }
    reports
}

async fn phase2_pass(pool: &TestPool) -> Phase2RecoverySummary {
    drive_phase2_recovery_with(
        &crate::broker::PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
        &TestChildSpecs::empty(),
        0,
        GLOBAL_SCAN_ROW_CAP,
        LIMIT,
    )
    .await
    .expect("the phase-2 recovery pass returns its summary")
}

/// The stored count of a workflow, or `None` when it has no row.
async fn failed_passes(pool: &TestPool, workflow_id: Uuid) -> Option<i32> {
    sql::query_scalar(
        "SELECT failed_passes FROM horsies_workflow_recovery_failures WHERE workflow_id = $1",
    )
    .bind(workflow_id)
    .fetch_optional(pool)
    .await
    .expect("read the stored count")
}

async fn failure_rows(pool: &TestPool) -> i64 {
    sql::query_scalar("SELECT count(*) FROM horsies_workflow_recovery_failures")
        .fetch_one(pool)
        .await
        .expect("count the failure rows")
}

/// A RUNNING workflow with one READY node whose task insert raises
/// ([`BAD_GOOD_UNTIL`]).
async fn seed_poison_ready(pool: &TestPool, seed: WorkflowSeed) -> Uuid {
    let workflow_id = seed_workflow(pool, &seed).await;
    seed_node(
        pool,
        &NodeSeed::task(workflow_id, 0, "READY").options(BAD_GOOD_UNTIL),
    )
    .await;
    workflow_id
}

/// Makes every update of this workflow's rows in `table` refuse with `sqlstate`.
async fn refuse_updates(
    pool: &TestPool,
    table: &str,
    id_column: &str,
    workflow_id: Uuid,
    sqlstate: &str,
) {
    super::support::execute(
        pool,
        &format!(
            "CREATE FUNCTION port_test_refuse() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
                 IF NEW.{id_column} = '{workflow_id}'::uuid THEN
                     RAISE EXCEPTION 'refused by the test' USING ERRCODE = '{sqlstate}';
                 END IF;
                 RETURN NEW;
             END
             $body$"
        ),
    )
    .await;
    super::support::execute(
        pool,
        &format!(
            "CREATE TRIGGER port_test_refuse BEFORE UPDATE ON {table}
             FOR EACH ROW EXECUTE FUNCTION port_test_refuse()"
        ),
    )
    .await;
}

async fn stop_refusing_updates(pool: &TestPool, table: &str) {
    super::support::execute(pool, &format!("DROP TRIGGER port_test_refuse ON {table}")).await;
    super::support::execute(pool, "DROP FUNCTION port_test_refuse()").await;
}

/// Makes an update of `table` (of the row `id_column = filter`, or of any row)
/// end its own backend: the connection is lost in the middle of a statement.
async fn kill_on_update(pool: &TestPool, table: &str, filter: Option<(&str, Uuid)>) {
    let condition = match filter {
        Some((id_column, id)) => format!("NEW.{id_column} = '{id}'::uuid"),
        None => "TRUE".to_owned(),
    };
    super::support::execute(
        pool,
        &format!(
            "CREATE FUNCTION port_test_kill() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
                 IF {condition} THEN
                     PERFORM pg_terminate_backend(pg_backend_pid());
                 END IF;
                 RETURN NEW;
             END
             $body$"
        ),
    )
    .await;
    super::support::execute(
        pool,
        &format!(
            "CREATE TRIGGER port_test_kill BEFORE UPDATE ON {table}
             FOR EACH ROW EXECUTE FUNCTION port_test_kill()"
        ),
    )
    .await;
}

async fn stop_killing(pool: &TestPool, table: &str) {
    super::support::execute(pool, &format!("DROP TRIGGER port_test_kill ON {table}")).await;
    super::support::execute(pool, "DROP FUNCTION port_test_kill()").await;
}

/// The child specs of a child whose task has options that are not a JSON
/// object and a `good_until` (Python's `PoisonOptionsChild`): the merge of
/// the deadline into the options raises at each start of the child (Python
/// `_child_task_options_json`, `clients/python/horsies/core/workflows/engine.py:425-475`).
fn poison_child_specs() -> TestChildSpecs {
    let mut child_task = task_node(0, "limit_child_task");
    child_task.task_options_json = Some(POISON_TASK_OPTIONS.to_owned());
    child_task.good_until = Some(chrono::Utc::now() + chrono::Duration::hours(1));
    let mut child = spec("poison_options_child", vec![child_task]);
    child.definition_key = Some(POISON_CHILD_KEY.to_owned());
    child.output_index = Some(0);
    TestChildSpecs::empty().with_spec(child)
}

/// Makes every insert of a task row of `workflow_id` refuse with a data
/// exception (22007), as the insert of a stored deadline that does not parse
/// does (Python `_refuse_task_inserts`).
async fn refuse_task_inserts(pool: &TestPool, workflow_id: Uuid) {
    super::support::execute(
        pool,
        &format!(
            "CREATE FUNCTION port_test_refuse_task() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
                 IF NEW.input::jsonb -> '__h_workflow_meta__' ->> 'workflow_id' = '{workflow_id}' THEN
                     RAISE EXCEPTION 'refused by the test' USING ERRCODE = '22007';
                 END IF;
                 RETURN NEW;
             END
             $body$"
        ),
    )
    .await;
    super::support::execute(
        pool,
        "CREATE TRIGGER port_test_refuse_task BEFORE INSERT ON horsies_tasks
         FOR EACH ROW EXECUTE FUNCTION port_test_refuse_task()",
    )
    .await;
}

/// Starts A then B, refuses the task row of B, and runs phase 1 of A, so
/// phase-2 recovery owes A's progression, and that progression raises on the
/// task row of B. Returns the workflow id and A's task id. Python:
/// `_start_a_then_b`.
async fn start_a_then_b(pool: &TestPool) -> (Uuid, Uuid) {
    let first = task_node(0, "limit_task");
    let mut second = task_node(1, "limit_task");
    second.dependencies = vec![0];
    let workflow_id = start(
        pool,
        &spec("phase2_a_then_b", vec![first, second]),
        &TestChildSpecs::empty(),
    )
    .await;
    refuse_task_inserts(pool, workflow_id).await;
    let task_id = node(pool, workflow_id, 0)
        .await
        .task_id
        .expect("A has its task at start");
    sql::query(
        "UPDATE horsies_tasks
         SET status = 'RUNNING', claimed_at = NOW(), claimed_by_worker_id = $2,
             claim_expires_at = NULL, started_at = NOW(), worker_pid = 4242,
             worker_hostname = 'port-host', worker_process_name = 'port-process',
             updated_at = NOW()
         WHERE id = $1",
    )
    .bind(task_id)
    .bind(WORKER)
    .execute(pool)
    .await
    .expect("hold A's task");
    let result = serde_json::to_string(&ok(serde_json::json!(1))).expect("encode A's result");
    let outcomes = terminal_writes::complete_workflow_task(pool, task_id, WORKER, &result)
        .await
        .expect("phase 1 of A");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{outcomes:?}"
    );
    (workflow_id, task_id)
}

async fn pending_attempts(pool: &TestPool, task_id: Uuid) -> (i32, Option<String>) {
    sql::query_as(
        "SELECT attempt_count, last_failure_class
         FROM horsies_workflow_phase2_pending WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read the phase-2 record")
}

/// `test_each_pass_counts_once_and_changes_nothing_else`
#[tokio::test]
#[serial]
async fn each_pass_counts_once_and_changes_nothing_else() {
    let db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_id = seed_poison_ready(&db.pool, WorkflowSeed::running()).await;

    let reports = passes(&db.pool, &child_specs, LIMIT - 1).await;

    assert_eq!(failed_passes(&db.pool, workflow_id).await, Some(2));
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "RUNNING");
    assert_eq!(node_status(&db.pool, workflow_id, 0).await, "READY");
    for report in &reports {
        assert_eq!(report.metrics.case1.failures_counted, 1, "{report:?}");
        assert_eq!(report.metrics.case1.limit_reached, 0, "{report:?}");
        assert_eq!(report.errors, 1, "{report:?}");
    }
    db.drop().await;
}

/// `test_two_cases_of_one_workflow_count_once_in_one_pass`: one pass id
/// serves every case, so a fan-out cannot reach the limit in one pass.
#[tokio::test]
#[serial]
async fn two_cases_of_one_workflow_count_once_in_one_pass() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(&db.pool, &NodeSeed::task(workflow_id, 0, "COMPLETED")).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 1, "PENDING")
            .deps(&[0])
            .options(BAD_GOOD_UNTIL),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 2, "READY").options(BAD_GOOD_UNTIL),
    )
    .await;

    let report = pass(&db.pool, &TestChildSpecs::empty()).await;

    assert_eq!(failed_passes(&db.pool, workflow_id).await, Some(1));
    assert_eq!(report.metrics.case0.errors, 1, "{report:?}");
    assert_eq!(report.metrics.case1.errors, 1, "{report:?}");
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "RUNNING");
    db.drop().await;
}

/// `test_a_new_process_continues_the_count`: the reaper moves to another
/// process; the stored count goes on.
#[tokio::test]
#[serial]
async fn a_new_process_continues_the_count() {
    let db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_id = seed_poison_ready(&db.pool, WorkflowSeed::running()).await;
    let _reports = passes(&db.pool, &child_specs, LIMIT - 1).await;

    let other = db.other_process_pool().await;
    let _report = pass(&other, &child_specs).await;
    other.close().await;

    assert_eq!(failed_passes(&db.pool, workflow_id).await, Some(3));
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    db.drop().await;
}

/// `test_a_repair_before_the_limit_saves_the_workflow`
#[tokio::test]
#[serial]
async fn a_repair_before_the_limit_saves_the_workflow() {
    let db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_id = seed_poison_ready(&db.pool, WorkflowSeed::running()).await;
    let _reports = passes(&db.pool, &child_specs, LIMIT - 1).await;

    sql::query("UPDATE horsies_workflow_tasks SET task_options = NULL WHERE workflow_id = $1")
        .bind(workflow_id)
        .execute(&db.pool)
        .await
        .expect("repair the node");
    let report = pass(&db.pool, &child_specs).await;

    assert_eq!(report.total(), 1, "{report:?}");
    assert_eq!(node_status(&db.pool, workflow_id, 0).await, "ENQUEUED");
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "RUNNING");
    // No reset: the row stays as evidence.
    assert_eq!(failed_passes(&db.pool, workflow_id).await, Some(2));
    db.drop().await;
}

/// `test_a_healthy_pass_writes_nothing`: a recovered candidate and an idle
/// workflow cost no count write.
#[tokio::test]
#[serial]
async fn a_healthy_pass_writes_nothing() {
    let db = IsolatedDatabase::create().await;
    let recovered = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(&db.pool, &NodeSeed::task(recovered, 0, "READY")).await;
    let idle = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    let idle_task = Uuid::new_v4();
    seed_node_task(&db.pool, idle_task, LiveStatus::Running).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(idle, 0, "RUNNING").task_id(idle_task),
    )
    .await;

    let report = pass(&db.pool, &TestChildSpecs::empty()).await;

    assert_eq!(report.total(), 1, "{report:?}");
    assert_eq!(node_status(&db.pool, recovered, 0).await, "ENQUEUED");
    assert_eq!(failure_rows(&db.pool).await, 0);
    assert_eq!(report.over_failure_limit, 0);
    db.drop().await;
}

/// `test_a_transient_raise_never_counts` (lock, deadlock, statement timeout,
/// too many connections).
#[tokio::test]
#[serial]
async fn a_transient_raise_never_counts() {
    for sqlstate in ["55P03", "40P01", "57014", "53300"] {
        let db = IsolatedDatabase::create().await;
        let child_specs = TestChildSpecs::empty();
        let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
        seed_node(&db.pool, &NodeSeed::task(workflow_id, 0, "READY")).await;
        refuse_updates(
            &db.pool,
            "horsies_workflow_tasks",
            "workflow_id",
            workflow_id,
            sqlstate,
        )
        .await;

        let reports = passes(&db.pool, &child_specs, LIMIT + 1).await;

        assert_eq!(
            failed_passes(&db.pool, workflow_id).await,
            None,
            "{sqlstate}"
        );
        assert_eq!(workflow_status(&db.pool, workflow_id).await, "RUNNING");
        for report in &reports {
            assert_eq!(report.metrics.case1.errors, 1, "{sqlstate}: {report:?}");
            assert_eq!(report.metrics.case1.failures_counted, 0, "{sqlstate}");
        }

        stop_refusing_updates(&db.pool, "horsies_workflow_tasks").await;
        let _report = pass(&db.pool, &child_specs).await;
        assert_eq!(node_status(&db.pool, workflow_id, 0).await, "ENQUEUED");
        db.drop().await;
    }
}

/// `test_a_connection_loss_counts_no_candidate_of_the_pass`: the backend of a
/// candidate dies in its update. A lost connection is not a verdict on any
/// workflow. In the core each candidate takes its own pooled connection, so
/// the pass goes on with the next candidate (Python's pass fails as a whole,
/// because one session serves it).
#[tokio::test]
#[serial]
async fn a_connection_loss_counts_no_candidate_of_the_pass() {
    let db = IsolatedDatabase::create().await;
    let first = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    let second = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(&db.pool, &NodeSeed::task(first, 0, "READY")).await;
    seed_node(&db.pool, &NodeSeed::task(second, 0, "READY")).await;
    kill_on_update(&db.pool, "horsies_workflow_tasks", None).await;

    let report = pass(&db.pool, &TestChildSpecs::empty()).await;

    stop_killing(&db.pool, "horsies_workflow_tasks").await;
    assert_eq!(report.metrics.case1.errors, 2, "{report:?}");
    assert_eq!(report.metrics.case1.failures_counted, 0, "{report:?}");
    assert_eq!(failure_rows(&db.pool).await, 0);
    for workflow_id in [first, second] {
        assert_eq!(workflow_status(&db.pool, workflow_id).await, "RUNNING");
        assert_eq!(node_status(&db.pool, workflow_id, 0).await, "READY");
    }
    db.drop().await;
}

/// `test_the_real_raise_after_a_connection_loss_is_transient`: the classifier
/// rule rests on the error the driver gives after a lost connection.
#[tokio::test]
#[serial]
async fn the_real_raise_after_a_connection_loss_is_transient() {
    let db = IsolatedDatabase::create().await;
    // The lost session is a core connection, so the classifier sees the
    // statement failure that the statement layer gives.
    let core = crate::broker::pools::unpinged_single_connection_core_pool_for_url(db.pool.url());
    let mut connection = core
        .checkout(crate::db::CheckoutClass::Reserved)
        .await
        .expect("check out the core connection");
    let rows = connection
        .query(
            &BACKEND_PID.bind().finish(),
            crate::db::StatementBound::ConnectionTimeouts,
        )
        .await
        .expect("read the backend pid");
    let pid: i32 = rows.first().expect("one row").get(0);
    let terminated: bool = sql::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(&db.pool)
        .await
        .expect("terminate the backend");
    assert!(terminated);

    let error = connection
        .query(
            &BACKEND_PID.bind().finish(),
            crate::db::StatementBound::ConnectionTimeouts,
        )
        .await
        .expect_err("a statement on a terminated backend fails");
    drop(connection);
    core.close();

    let failure = CandidateFailure::Error(WorkflowError::Broker(
        crate::broker::BrokerError::Statement(error),
    ));
    assert_eq!(failure.class(), FailureClass::Transient, "{failure:?}");
    db.drop().await;
}

/// The backend pid of a core connection.
static BACKEND_PID: crate::db::TypedStatement =
    crate::db::TypedStatement::new("test_backend_pid", "SELECT pg_backend_pid()", &[]);

/// `test_the_resume_pass_logs_and_does_not_count`
#[tokio::test]
#[serial]
async fn the_resume_pass_logs_and_does_not_count() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_poison_ready(&db.pool, WorkflowSeed::running()).await;

    let report = recover_workflow_tree(
        &crate::broker::PostgresBroker::from_test_url(db.pool.url(), crate::ClientFamily::Rust),
        &TestChildSpecs::empty(),
        workflow_id,
    )
    .await
    .expect("the tree pass returns its report");

    assert_eq!(report.metrics.case1.errors, 1, "{report:?}");
    assert_eq!(report.metrics.case1.failures_counted, 0, "{report:?}");
    assert_eq!(failed_passes(&db.pool, workflow_id).await, None);
    assert_eq!(node_status(&db.pool, workflow_id, 0).await, "READY");
    db.drop().await;
}

/// `test_case_0_pending_node`
#[tokio::test]
#[serial]
async fn the_limit_bounds_case_0() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(&db.pool, &NodeSeed::task(workflow_id, 0, "COMPLETED")).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 1, "PENDING")
            .deps(&[0])
            .options(BAD_GOOD_UNTIL),
    )
    .await;

    let _reports = passes(&db.pool, &TestChildSpecs::empty(), LIMIT).await;

    assert_eq!(node_status(&db.pool, workflow_id, 1).await, "FAILED");
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    db.drop().await;
}

/// `test_case_1_ready_node_with_other_nodes`
#[tokio::test]
#[serial]
async fn the_limit_bounds_case_1_and_get_returns_the_limit_error() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_poison_ready(&db.pool, WorkflowSeed::running()).await;
    seed_node(
        &db.pool,
        &NodeSeed::task(workflow_id, 1, "PENDING").deps(&[0]),
    )
    .await;
    seed_node(&db.pool, &NodeSeed::task(workflow_id, 2, "COMPLETED")).await;

    let reports = passes(&db.pool, &TestChildSpecs::empty(), LIMIT).await;

    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    assert_eq!(node_status(&db.pool, workflow_id, 0).await, "FAILED");
    assert_eq!(node_status(&db.pool, workflow_id, 1).await, "SKIPPED");
    assert_eq!(node_status(&db.pool, workflow_id, 2).await, "COMPLETED");
    let last = reports.last().expect("three reports");
    assert_eq!(last.metrics.case1.limit_reached, 1, "{last:?}");

    match handle(&db.pool, workflow_id)
        .get(Some(std::time::Duration::ZERO))
        .await
        .expect("the result read")
    {
        TaskResult::Err(error) => assert_eq!(
            code(&error).as_deref(),
            Some("WORKFLOW_RECOVERY_LIMIT_REACHED"),
            "{error:?}"
        ),
        TaskResult::Ok(value) => panic!("get() of a failed workflow returned ok {value}"),
    }
    db.drop().await;
}

/// `test_case_1_5_serialization_defect`: a child start that fails on stored
/// data is counted and bounded.
#[tokio::test]
#[serial]
async fn the_limit_bounds_case_1_5() {
    let db = IsolatedDatabase::create().await;
    let child_specs = poison_child_specs();
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(
        &db.pool,
        &NodeSeed::sub_workflow(workflow_id, 0, "READY", POISON_CHILD_KEY, None),
    )
    .await;

    let reports = passes(&db.pool, &child_specs, LIMIT).await;

    let errors: Vec<u32> = reports.iter().map(|r| r.metrics.case1_5.errors).collect();
    assert_eq!(errors, vec![1, 1, 1]);
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    assert_eq!(node_status(&db.pool, workflow_id, 0).await, "FAILED");
    let error = super::support::stored_error(
        &workflow(&db.pool, workflow_id)
            .await
            .error
            .expect("the limit stores its error"),
    );
    assert_eq!(
        code(&error).as_deref(),
        Some("WORKFLOW_RECOVERY_LIMIT_REACHED")
    );
    let data = error
        .data_value()
        .expect("the limit error carries its facts");
    assert_eq!(data["case"], "ready_subworkflow_not_started");
    assert_eq!(data["task_index"], 0);
    db.drop().await;
}

/// `test_case_1_6_fails_the_parent_workflow`: the candidate is atomic, so the
/// parent node is still RUNNING at the limit and the limit fails it.
#[tokio::test]
#[serial]
async fn the_limit_bounds_case_1_6_and_fails_the_parent_workflow() {
    let db = IsolatedDatabase::create().await;
    let parent = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    let ok_envelope =
        serde_json::to_string(&ok(serde_json::json!(1))).expect("encode the child result");
    let child = seed_workflow(
        &db.pool,
        &WorkflowSeed::child(ParentLink::of_root(parent, 0))
            .status("COMPLETED")
            .result(ok_envelope),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::sub_workflow(parent, 0, "RUNNING", "tests.port.child.v1", Some(child)),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(parent, 1, "PENDING")
            .deps(&[0])
            .options(BAD_GOOD_UNTIL),
    )
    .await;

    let _reports = passes(&db.pool, &TestChildSpecs::empty(), LIMIT).await;

    assert_eq!(failed_passes(&db.pool, child).await, None);
    assert_eq!(failed_passes(&db.pool, parent).await, Some(3));
    assert_eq!(workflow_status(&db.pool, parent).await, "FAILED");
    assert_eq!(node_status(&db.pool, parent, 0).await, "FAILED");
    assert_eq!(node_status(&db.pool, parent, 1).await, "SKIPPED");
    assert_eq!(workflow_status(&db.pool, child).await, "COMPLETED");
    db.drop().await;
}

/// `test_case_2_3_keeps_a_stored_first_failure`
#[tokio::test]
#[serial]
async fn the_limit_bounds_case_2_3_and_keeps_a_stored_first_failure() {
    let db = IsolatedDatabase::create().await;
    let first_failure = serde_json::to_string(
        &TaskError::new("FIRST", "first failure").expect("a user error code"),
    )
    .expect("encode the first failure");
    let workflow_id = seed_workflow(
        &db.pool,
        &WorkflowSeed::running()
            .policy(poison_success_policy())
            .error(first_failure.clone()),
    )
    .await;
    seed_node(&db.pool, &NodeSeed::task(workflow_id, 0, "FAILED")).await;

    let reports = passes(&db.pool, &TestChildSpecs::empty(), LIMIT).await;

    let stored = workflow(&db.pool, workflow_id).await;
    assert_eq!(stored.status, "FAILED");
    assert_eq!(stored.error, Some(first_failure));
    let last = reports.last().expect("three reports");
    assert_eq!(last.metrics.case2_3.limit_reached, 1, "{last:?}");
    db.drop().await;
}

/// `test_case_2_3_assertion_defect`: the output index has no node row, so the
/// final result fails on each pass, and the limit bounds it.
#[tokio::test]
#[serial]
async fn the_limit_bounds_a_missing_output_node_row() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running().output(7)).await;
    seed_node(&db.pool, &NodeSeed::task(workflow_id, 0, "COMPLETED")).await;

    let reports = passes(&db.pool, &TestChildSpecs::empty(), LIMIT).await;

    let errors: Vec<u32> = reports.iter().map(|r| r.metrics.case2_3.errors).collect();
    assert_eq!(errors, vec![1, 1, 1]);
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    assert_eq!(
        workflow_error_code(&db.pool, workflow_id).await.as_deref(),
        Some("WORKFLOW_RECOVERY_LIMIT_REACHED")
    );
    db.drop().await;
}

/// `test_on_error_pause_still_ends_failed`
#[tokio::test]
#[serial]
async fn the_limit_fails_an_on_error_pause_workflow() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_poison_ready(&db.pool, WorkflowSeed::running().on_error("pause")).await;

    let _reports = passes(&db.pool, &TestChildSpecs::empty(), LIMIT).await;

    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    db.drop().await;
}

/// A RUNNING workflow with a poison READY node 0 and a RUNNING node 1 whose
/// task is live. Returns the workflow id and the live task id.
async fn seed_poison_with_live_node(pool: &TestPool, seed: WorkflowSeed) -> (Uuid, Uuid) {
    let workflow_id = seed_poison_ready(pool, seed).await;
    let live_task = Uuid::new_v4();
    seed_node_task(pool, live_task, LiveStatus::Running).await;
    seed_node(
        pool,
        &NodeSeed::task(workflow_id, 1, "RUNNING").task_id(live_task),
    )
    .await;
    (workflow_id, live_task)
}

/// `test_live_node_finishes_and_ends_the_workflow`
#[tokio::test]
#[serial]
async fn a_live_node_finishes_and_ends_the_workflow() {
    let db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let (workflow_id, live_task) =
        seed_poison_with_live_node(&db.pool, WorkflowSeed::running()).await;

    let reports = passes(&db.pool, &child_specs, LIMIT).await;

    assert_eq!(workflow_status(&db.pool, workflow_id).await, "RUNNING");
    assert_eq!(node_status(&db.pool, workflow_id, 0).await, "FAILED");
    assert_eq!(node_status(&db.pool, workflow_id, 1).await, "RUNNING");
    let last = reports.last().expect("three reports");
    assert_eq!(last.metrics.case1.limit_reached, 1, "{last:?}");
    assert_eq!(last.over_failure_limit, 1, "{last:?}");

    finish_task(&db.pool, live_task, &ok(serde_json::json!(1)), &child_specs).await;

    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    assert_eq!(
        workflow_error_code(&db.pool, workflow_id).await.as_deref(),
        Some("WORKFLOW_RECOVERY_LIMIT_REACHED")
    );
    assert_eq!(pass(&db.pool, &child_specs).await.over_failure_limit, 0);
    db.drop().await;
}

/// `test_success_policy_met_by_a_live_node_can_complete`: the nodes that the
/// success case requires completed, so the declared policy decides.
#[tokio::test]
#[serial]
async fn a_success_policy_met_by_a_live_node_completes_the_workflow() {
    let db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let (workflow_id, live_task) = seed_poison_with_live_node(
        &db.pool,
        WorkflowSeed::running().policy(serde_json::json!({"cases": [{"required_indices": [1]}]})),
    )
    .await;
    let _reports = passes(&db.pool, &child_specs, LIMIT).await;

    finish_task(&db.pool, live_task, &ok(serde_json::json!(1)), &child_specs).await;

    assert_eq!(node_status(&db.pool, workflow_id, 0).await, "FAILED");
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "COMPLETED");
    db.drop().await;
}

/// `test_child_at_the_limit_fails_the_parent_node`
#[tokio::test]
#[serial]
async fn a_child_at_the_limit_fails_the_parent_node() {
    let db = IsolatedDatabase::create().await;
    let parent = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    let child = seed_poison_ready(
        &db.pool,
        WorkflowSeed::child(ParentLink::of_root(parent, 0)),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::sub_workflow(parent, 0, "RUNNING", "tests.port.child.v1", Some(child)),
    )
    .await;

    let _reports = passes(&db.pool, &TestChildSpecs::empty(), LIMIT + 1).await;

    assert_eq!(workflow_status(&db.pool, child).await, "FAILED");
    assert_eq!(node_status(&db.pool, parent, 0).await, "FAILED");
    assert_eq!(workflow_status(&db.pool, parent).await, "FAILED");
    assert_eq!(failed_passes(&db.pool, parent).await, None);
    let stored = node(&db.pool, parent, 0)
        .await
        .result
        .expect("the parent node stores its child's failure");
    let error = stored_result_error(&stored);
    assert_eq!(code(&error).as_deref(), Some("SUBWORKFLOW_FAILED"));
    assert!(stored.contains(LIMIT_MESSAGE), "{stored}");
    db.drop().await;
}

/// `test_a_drain_raise_keeps_the_child_and_counts_on_the_parent`: the child
/// completes in its own candidate and the parent advance raises in another.
/// If that raise reached the child completion, the child would come back on
/// every pass with no count.
#[tokio::test]
#[serial]
async fn a_parent_advance_raise_keeps_the_child_and_counts_on_the_parent() {
    let db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let parent = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    let child = seed_workflow(
        &db.pool,
        &WorkflowSeed::child(ParentLink::of_root(parent, 0)),
    )
    .await;
    seed_node(&db.pool, &NodeSeed::task(child, 0, "COMPLETED")).await;
    seed_node(
        &db.pool,
        &NodeSeed::sub_workflow(parent, 0, "RUNNING", "tests.port.child.v1", Some(child)),
    )
    .await;
    seed_node(
        &db.pool,
        &NodeSeed::task(parent, 1, "PENDING")
            .deps(&[0])
            .options(BAD_GOOD_UNTIL),
    )
    .await;

    let first = pass(&db.pool, &child_specs).await;

    assert_eq!(first.total(), 1, "{first:?}");
    assert_eq!(workflow_status(&db.pool, child).await, "COMPLETED");
    assert_eq!(failed_passes(&db.pool, child).await, None);

    let _reports = passes(&db.pool, &child_specs, LIMIT).await;

    assert_eq!(failed_passes(&db.pool, parent).await, Some(3));
    assert_eq!(workflow_status(&db.pool, parent).await, "FAILED");
    db.drop().await;
}

/// `test_a_connection_loss_in_a_completion_is_one_error`: the pass goes on,
/// and the workflow finishes on the next pass.
#[tokio::test]
#[serial]
async fn a_connection_loss_in_a_completion_is_one_uncounted_error() {
    let db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(&db.pool, &NodeSeed::task(workflow_id, 0, "COMPLETED")).await;
    kill_on_update(&db.pool, "horsies_workflows", Some(("id", workflow_id))).await;

    let report = pass(&db.pool, &child_specs).await;

    stop_killing(&db.pool, "horsies_workflows").await;
    assert_eq!(report.metrics.case2_3.errors, 1, "{report:?}");
    assert_eq!(report.errors, 1, "{report:?}");
    assert_eq!(failed_passes(&db.pool, workflow_id).await, None);

    let _report = pass(&db.pool, &child_specs).await;
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "COMPLETED");
    db.drop().await;
}

/// `test_a_raised_completion_commits_its_count`
#[tokio::test]
#[serial]
async fn a_raised_completion_commits_its_count() {
    let db = IsolatedDatabase::create().await;
    let workflow_id = seed_workflow(
        &db.pool,
        &WorkflowSeed::running().policy(poison_success_policy()),
    )
    .await;
    seed_node(&db.pool, &NodeSeed::task(workflow_id, 0, "COMPLETED")).await;

    let _report = pass(&db.pool, &TestChildSpecs::empty()).await;

    assert_eq!(failed_passes(&db.pool, workflow_id).await, Some(1));
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "RUNNING");
    db.drop().await;
}

/// `test_a_failed_count_write_changes_nothing_and_the_pass_goes_on`
#[tokio::test]
#[serial]
async fn a_failed_count_write_changes_nothing_and_the_pass_goes_on() {
    let db = IsolatedDatabase::create().await;
    let poison = seed_poison_ready(&db.pool, WorkflowSeed::running()).await;
    let healthy = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    seed_node(&db.pool, &NodeSeed::task(healthy, 0, "READY")).await;
    super::support::execute(
        &db.pool,
        "CREATE FUNCTION port_test_refuse_count() RETURNS trigger
         LANGUAGE plpgsql AS $body$
         BEGIN
             RAISE EXCEPTION 'refused by the test' USING ERRCODE = 'XX000';
         END
         $body$",
    )
    .await;
    super::support::execute(
        &db.pool,
        "CREATE TRIGGER port_test_refuse_count
         BEFORE INSERT ON horsies_workflow_recovery_failures
         FOR EACH ROW EXECUTE FUNCTION port_test_refuse_count()",
    )
    .await;

    let report = pass(&db.pool, &TestChildSpecs::empty()).await;

    assert_eq!(report.total(), 1, "{report:?}");
    assert_eq!(report.metrics.case1.failures_counted, 0, "{report:?}");
    assert_eq!(failure_rows(&db.pool).await, 0);
    assert_eq!(node_status(&db.pool, healthy, 0).await, "ENQUEUED");
    assert_eq!(node_status(&db.pool, poison, 0).await, "READY");
    assert_eq!(workflow_status(&db.pool, poison).await, "RUNNING");
    db.drop().await;
}

/// `test_a_failed_limit_write_keeps_the_count_and_retries`
#[tokio::test]
#[serial]
async fn a_failed_limit_write_keeps_the_count_and_retries() {
    let db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let workflow_id = seed_poison_ready(&db.pool, WorkflowSeed::running()).await;
    refuse_updates(&db.pool, "horsies_workflows", "id", workflow_id, "XX000").await;

    let reports = passes(&db.pool, &child_specs, LIMIT).await;

    assert_eq!(failed_passes(&db.pool, workflow_id).await, Some(3));
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "RUNNING");
    assert_eq!(node_status(&db.pool, workflow_id, 0).await, "READY");
    let last = reports.last().expect("three reports");
    assert_eq!(last.metrics.case1.limit_reached, 0, "{last:?}");
    assert_eq!(last.over_failure_limit, 1, "{last:?}");

    stop_refusing_updates(&db.pool, "horsies_workflows").await;
    let _report = pass(&db.pool, &child_specs).await;

    assert_eq!(failed_passes(&db.pool, workflow_id).await, Some(4));
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    db.drop().await;
}

/// `test_a_workflow_that_is_not_running_is_refused` (PAUSED and CANCELLED):
/// the limit write changes nothing on a workflow that is not RUNNING.
#[tokio::test]
#[serial]
async fn the_limit_write_refuses_a_workflow_that_is_not_running() {
    let db = IsolatedDatabase::create().await;
    for status in ["PAUSED", "CANCELLED"] {
        let workflow_id = seed_workflow(&db.pool, &WorkflowSeed::running().status(status)).await;
        seed_node(&db.pool, &NodeSeed::task(workflow_id, 0, "READY")).await;
        // A stored count of 2 and a limit of 3 (the lowest limit): this failure reaches it.
        sql::query(
            "INSERT INTO horsies_workflow_recovery_failures (
                 workflow_id, failed_passes, last_pass_id, first_failed_at,
                 last_failed_at, last_case, last_task_index, last_error_type
             ) VALUES ($1, 2, $2, NOW(), NOW(), 'ready_not_enqueued', 0, 'Validation')",
        )
        .bind(workflow_id)
        .bind(Uuid::new_v4())
        .execute(&db.pool)
        .await
        .expect("seed a count of 2");
        let record = record_candidate_failure(
            &crate::broker::PostgresBroker::from_test_url(db.pool.url(), crate::ClientFamily::Rust),
            RecoveryFailurePolicy::Count(CountFailures::new_pass(3)),
            workflow_id,
            RecoveryCase::ReadyNotEnqueued { task_index: 0 },
            &CandidateFailure::Error(WorkflowError::Validation("refused".to_owned())),
        )
        .await;

        assert_eq!(record, FailureRecord::Counted, "{status}");
        let stored = workflow(&db.pool, workflow_id).await;
        assert_eq!(stored.status, status);
        assert_eq!(stored.error, None, "{status}");
        assert_eq!(node_status(&db.pool, workflow_id, 0).await, "READY");
    }
    db.drop().await;
}

/// `test_a_raised_row_is_counted_and_fails_the_workflow_at_the_bound`
#[tokio::test]
#[serial]
async fn a_raised_phase2_row_is_counted_and_fails_the_workflow_at_the_bound() {
    let db = IsolatedDatabase::create().await;
    let (workflow_id, task_id) = start_a_then_b(&db.pool).await;

    let mut summaries = Vec::new();
    for _ in 0..LIMIT {
        summaries.push(phase2_pass(&db.pool).await);
    }

    let quarantined: Vec<u64> = summaries.iter().map(|s| s.quarantined).collect();
    assert_eq!(quarantined, vec![0, 0, 1]);
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    assert_eq!(node_status(&db.pool, workflow_id, 0).await, "FAILED");
    assert_eq!(node_status(&db.pool, workflow_id, 1).await, "SKIPPED");
    let (attempts, failure_class) = pending_attempts(&db.pool, task_id).await;
    assert_eq!(attempts, 3);
    assert_eq!(failure_class.as_deref(), Some("RAISED"));
    assert_eq!(
        workflow_error_code(&db.pool, workflow_id).await.as_deref(),
        Some("WORKFLOW_RECOVERY_LIMIT_REACHED")
    );
    assert_eq!(phase2_pass(&db.pool).await.considered, 0);
    db.drop().await;
}

/// `test_a_transient_raise_stays_uncounted`
#[tokio::test]
#[serial]
async fn a_transient_phase2_raise_stays_uncounted() {
    let db = IsolatedDatabase::create().await;
    let (workflow_id, task_id) = start_a_then_b(&db.pool).await;
    refuse_updates(
        &db.pool,
        "horsies_workflow_tasks",
        "workflow_id",
        workflow_id,
        "55P03",
    )
    .await;

    for _ in 0..=LIMIT {
        let summary = phase2_pass(&db.pool).await;
        assert_eq!(summary.failed, 1, "{summary:?}");
        assert_eq!(summary.quarantined, 0, "{summary:?}");
    }

    assert_eq!(pending_attempts(&db.pool, task_id).await.0, 0);
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "RUNNING");
    db.drop().await;
}

/// `test_a_failed_limit_write_keeps_the_row_under_the_bound`: discovery stops
/// at the bound, so the row stays under it, or no later pass would try the
/// write again.
#[tokio::test]
#[serial]
async fn a_failed_phase2_limit_write_keeps_the_row_under_the_bound() {
    let db = IsolatedDatabase::create().await;
    let (workflow_id, task_id) = start_a_then_b(&db.pool).await;
    refuse_updates(&db.pool, "horsies_workflows", "id", workflow_id, "XX000").await;

    for _ in 0..LIMIT {
        let _summary = phase2_pass(&db.pool).await;
    }

    assert_eq!(pending_attempts(&db.pool, task_id).await.0, 2);
    assert_eq!(workflow_status(&db.pool, workflow_id).await, "RUNNING");

    stop_refusing_updates(&db.pool, "horsies_workflows").await;
    let _summary = phase2_pass(&db.pool).await;

    assert_eq!(workflow_status(&db.pool, workflow_id).await, "FAILED");
    db.drop().await;
}

/// `test_rows_go_with_their_workflow_and_with_a_cascaded_child`: a parent
/// delete cascades to its child, and the foreign key of the count removes the
/// child's count in the same statement.
#[tokio::test]
#[serial]
async fn count_rows_go_with_their_workflow_and_with_a_cascaded_child() {
    let db = IsolatedDatabase::create().await;
    let parent = seed_workflow(&db.pool, &WorkflowSeed::running().status("FAILED")).await;
    let child = seed_workflow(
        &db.pool,
        &WorkflowSeed::child(ParentLink::of_root(parent, 0)).status("FAILED"),
    )
    .await;
    let kept = seed_workflow(&db.pool, &WorkflowSeed::running()).await;
    super::support::execute(
        &db.pool,
        "UPDATE horsies_workflows SET completed_at = NOW() - INTERVAL '10 days'
         WHERE status = 'FAILED'",
    )
    .await;
    for workflow_id in [parent, child, kept] {
        sql::query(
            "INSERT INTO horsies_workflow_recovery_failures (
                 workflow_id, failed_passes, last_pass_id, first_failed_at,
                 last_failed_at, last_case, last_task_index, last_error_type
             ) VALUES ($1, 1, $2, NOW(), NOW(), 'ready_not_enqueued', 0, 'Validation')",
        )
        .bind(workflow_id)
        .bind(Uuid::new_v4())
        .execute(&db.pool)
        .await
        .expect("seed a count row");
    }

    crate::worker::recovery::run_retention_cleanup(
        &crate::broker::PostgresBroker::from_test_url(db.pool.url(), crate::ClientFamily::Rust),
        &RetentionConfig {
            worker_state_retention_hours: None,
            terminal_record_retention_hours: Some(24),
            ..RetentionConfig::default()
        },
        &mut crate::worker::recovery::failure_log::RecoveryFailures::new(
            crate::telemetry::registry::OptionalCounter::NotRecorded,
        ),
    )
    .await;

    assert_eq!(failed_passes(&db.pool, parent).await, None);
    assert_eq!(failed_passes(&db.pool, child).await, None);
    assert_eq!(failed_passes(&db.pool, kept).await, Some(1));
    db.drop().await;
}

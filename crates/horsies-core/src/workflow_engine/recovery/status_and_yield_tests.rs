//! The node statuses that the recovery discovery and the partial index of
//! nonterminal nodes treat as live, and the completion candidate yield of the
//! global workflow audit. Python: `tests/unit/test_nonterminal_node_predicate.py`
//! and `tests/unit/test_completion_candidate_yield.py`. Each test names the
//! Python tests that it ports.
//!
//! The discovery and audit queries read every workflow, so their tests run
//! on a database that the test owns.

use std::collections::HashSet;

use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use super::*;
use crate::core::WorkflowTaskStatus;
use crate::test_support::TestDatabase;

/// Connections of the pool on the owned database.
const TEST_POOL_CONNECTIONS: u32 = 5;

/// Row bound of one discovery query in these tests; far above the rows seeded.
const DISCOVERY_ROW_LIMIT: i64 = 1_000;

/// Workflows of one audit page in these tests; above the workflows seeded.
const AUDIT_PAGE_ROWS: i64 = 200;

/// The partial index of nonterminal nodes (`0002_indexes.sql`).
const NONTERMINAL_NODE_INDEX: &str = "idx_horsies_workflow_tasks_nonterminal";

/// Every node status with the text that the enum writes for it. The match
/// does not compile when a status is added.
fn every_node_status() -> Vec<(WorkflowTaskStatus, &'static str)> {
    [
        WorkflowTaskStatus::Pending,
        WorkflowTaskStatus::Ready,
        WorkflowTaskStatus::Enqueued,
        WorkflowTaskStatus::Running,
        WorkflowTaskStatus::Completed,
        WorkflowTaskStatus::Failed,
        WorkflowTaskStatus::Skipped,
    ]
    .into_iter()
    .map(|status| {
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
        (status, text)
    })
    .collect()
}

/// A database that the test owns, from the installed test template.
async fn owned_database() -> (TestDatabase, PgPool) {
    let database = TestDatabase::create().await;
    let pool = PgPoolOptions::new()
        .max_connections(TEST_POOL_CONNECTIONS)
        .connect(database.url())
        .await
        .expect("connect to the owned database");
    (database, pool)
}

/// A RUNNING top-level workflow.
async fn seed_running_workflow(pool: &PgPool) -> Uuid {
    let workflow_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_workflows (
             id, name, status, on_error, definition_key, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'recovery_status', 'RUNNING', 'fail', $2, 0, $1,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .bind(format!("tests.recovery_status.{}", workflow_id.simple()))
    .execute(pool)
    .await
    .expect("seed the workflow");
    workflow_id
}

/// A task node `node_<index>` in `status` with `dependencies`.
async fn seed_node(
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
             is_subworkflow, max_retries, execution, retention_class_key, created_at
         ) VALUES (
             'NONE', $1, $2, $3, $4, 'recovery_status_task', '{}', 'default', 100, $5, FALSE,
             'all', $6, FALSE, 0, 'ISOLATED', 'forever', NOW()
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(task_index)
    .bind(format!("node_{task_index}"))
    .bind(dependencies)
    .bind(status)
    .execute(pool)
    .await
    .expect("seed the node");
}

/// Per status: a workflow whose node 1 waits for node 0 in that status (a
/// case-0 candidate when node 0 is terminal), and a workflow with one node in
/// that status (a completion candidate when the node is terminal).
struct StatusWorkflows {
    status: WorkflowTaskStatus,
    text: &'static str,
    waiting: Uuid,
    single: Uuid,
}

async fn seed_status_workflows(pool: &PgPool) -> Vec<StatusWorkflows> {
    let mut seeded = Vec::new();
    for (status, text) in every_node_status() {
        let waiting = seed_running_workflow(pool).await;
        seed_node(pool, waiting, 0, &[], text).await;
        seed_node(pool, waiting, 1, &[0], "PENDING").await;
        let single = seed_running_workflow(pool).await;
        seed_node(pool, single, 0, &[], text).await;
        seeded.push(StatusWorkflows {
            status,
            text,
            waiting,
            single,
        });
    }
    seeded
}

/// The workflows whose node 1 a case-0 query returned.
fn waiting_nodes(rows: Vec<StuckPendingRow>) -> HashSet<Uuid> {
    rows.into_iter()
        .filter(|row| row.task_index == 1)
        .map(|row| row.workflow_id)
        .collect()
}

/// `test_statement_spells_the_nonterminal_statuses_of_the_enum` (the five
/// statements: global and tree case 0, tree and global completion
/// candidates, the global audit): each discovery query treats a node in each
/// status by the enum: a terminal dependency lets case 0 select its
/// dependent, and only a workflow whose nodes are all terminal is a
/// completion candidate.
#[tokio::test]
#[serial]
async fn the_recovery_discovery_treats_each_node_status_by_the_enum() {
    let (database, pool) = owned_database().await;
    let seeded = seed_status_workflows(&pool).await;
    let every_workflow: Vec<Uuid> = seeded
        .iter()
        .flat_map(|workflows| [workflows.waiting, workflows.single])
        .collect();

    let global_case0: Vec<StuckPendingRow> = sqlx::query_as(GLOBAL_CASE0_STUCK_PENDING_SQL)
        .bind(DISCOVERY_ROW_LIMIT)
        .fetch_all(&pool)
        .await
        .expect("global case 0");
    let tree_case0: Vec<StuckPendingRow> = sqlx::query_as(TREE_CASE0_STUCK_PENDING_SQL)
        .bind(&every_workflow)
        .bind(DISCOVERY_ROW_LIMIT)
        .fetch_all(&pool)
        .await
        .expect("tree case 0");
    let tree_completion: Vec<StuckWorkflowRow> = sqlx::query_as(TREE_CASE2_3_STUCK_WORKFLOW_SQL)
        .bind(&every_workflow)
        .bind(DISCOVERY_ROW_LIMIT)
        .fetch_all(&pool)
        .await
        .expect("tree completion candidates");
    let audit: Option<GlobalWorkflowAuditRow> = sqlx::query_as(GLOBAL_WORKFLOW_AUDIT_SQL)
        .bind(AUDIT_PAGE_ROWS)
        .bind(Uuid::new_v4())
        .bind(GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS)
        .fetch_optional(&pool)
        .await
        .expect("global audit");

    pool.close().await;
    database.drop().await;
    let global_case0 = waiting_nodes(global_case0);
    let tree_case0 = waiting_nodes(tree_case0);
    let tree_completion: HashSet<Uuid> = tree_completion
        .into_iter()
        .map(|row| row.workflow_id)
        .collect();
    let audit_completion: HashSet<Uuid> = audit
        .expect("the audit page")
        .completion_ids
        .into_iter()
        .collect();
    for workflows in &seeded {
        let terminal = workflows.status.is_terminal();
        let text = workflows.text;
        assert_eq!(
            global_case0.contains(&workflows.waiting),
            terminal,
            "{text}"
        );
        assert_eq!(tree_case0.contains(&workflows.waiting), terminal, "{text}");
        assert_eq!(
            tree_completion.contains(&workflows.single),
            terminal,
            "{text}"
        );
        assert_eq!(
            audit_completion.contains(&workflows.single),
            terminal,
            "{text}"
        );
        assert!(!tree_completion.contains(&workflows.waiting), "{text}");
        assert!(!audit_completion.contains(&workflows.waiting), "{text}");
    }
}

/// `test_partial_index_spells_the_nonterminal_statuses_of_the_enum`: the
/// predicate of the partial index of nonterminal nodes holds for each
/// nonterminal status of the enum and for no terminal one. The stored
/// predicate is evaluated for each status, not compared as text.
#[tokio::test]
#[serial]
async fn the_nonterminal_node_index_holds_exactly_the_nonterminal_statuses() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let predicate: String = sqlx::query_scalar(
        "SELECT pg_get_expr(i.indpred, i.indrelid)
         FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
         WHERE c.relname = $1",
    )
    .bind(NONTERMINAL_NODE_INDEX)
    .fetch_one(&pool)
    .await
    .expect("read the predicate of the partial index");
    let evaluation = format!(
        "SELECT ({predicate}) FROM (SELECT $1::character varying AS status) AS horsies_workflow_tasks"
    );

    for (status, text) in every_node_status() {
        let covered: bool = sqlx::query_scalar(&evaluation)
            .bind(text)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|error| panic!("{text}: {error}: {evaluation}"));
        assert_eq!(covered, !status.is_terminal(), "{text}: {predicate}");
    }
}

/// `test_yield_per_audit_result` (case `no-page`) and
/// `test_report_without_an_audit_page_has_no_yield`: a report with no audit
/// page has no completion yield, and its health snapshot holds `null`.
#[test]
fn a_report_without_an_audit_page_has_no_completion_yield() {
    let report = RecoveryReport::default();

    assert_eq!(report.completion_candidate_yield, None);
    assert!(serde_json::to_value(&report).unwrap()["completion_candidate_yield"].is_null());
}

/// `test_partial_failure_keeps_the_audited_yield`: the health snapshot of a
/// pass that failed after its audit keeps the yield of that audit.
#[test]
fn a_failed_pass_keeps_the_completion_yield_of_its_audit() {
    let failure = RecoveryPassFailure {
        report: RecoveryReport {
            completion_candidate_yield: Some(0.2),
            ..RecoveryReport::default()
        },
        error: WorkflowError::Validation("candidate failed".to_owned()),
    };

    let snapshot = failure.into_health_snapshot();

    assert_eq!(
        snapshot["completion_candidate_yield"],
        serde_json::json!(0.2)
    );
    assert_eq!(snapshot["state"], "error");
}

/// `test_yield_per_audit_result` (case `candidates`): the yield is the
/// completion candidates divided by the workflows that the audit scanned.
#[tokio::test]
#[serial]
async fn the_completion_yield_is_the_share_of_scanned_workflows_that_are_candidates() {
    let (database, pool) = owned_database().await;
    let candidate = seed_running_workflow(&pool).await;
    seed_node(&pool, candidate, 0, &[], "COMPLETED").await;
    for _live in 0..3 {
        let live = seed_running_workflow(&pool).await;
        seed_node(&pool, live, 0, &[], "RUNNING").await;
    }
    let mut report = RecoveryReport::default();

    let pass = audit_end_states(
        &PostgresBroker::from_pool(pool.clone(), crate::ClientFamily::Rust),
        &crate::test_support::child_specs::TestChildSpecs::empty(),
        AUDIT_PAGE_ROWS,
        &mut report,
        RecoveryFailurePolicy::LogOnly,
    )
    .await;

    pool.close().await;
    database.drop().await;
    assert!(pass.is_ok(), "{pass:?}");
    assert_eq!(report.metrics.case2_3.rows_selected, 4);
    assert_eq!(report.metrics.case2_3.candidates_returned, 1);
    assert_eq!(report.completion_candidate_yield, Some(0.25));
}

/// `test_yield_per_audit_result` (case `empty-page`): an audit page that
/// scanned no workflow has no completion yield, not a yield of zero. The case
/// `no-candidates` (a page with no candidate gives 0.0) is
/// `tests::empty_global_workflow_audit_is_bounded_at_fifty_thousand_rows`.
#[tokio::test]
#[serial]
async fn an_audit_page_that_scanned_no_workflow_has_no_completion_yield() {
    let (database, pool) = owned_database().await;
    let mut report = RecoveryReport::default();

    let pass = audit_end_states(
        &PostgresBroker::from_pool(pool.clone(), crate::ClientFamily::Rust),
        &crate::test_support::child_specs::TestChildSpecs::empty(),
        AUDIT_PAGE_ROWS,
        &mut report,
        RecoveryFailurePolicy::LogOnly,
    )
    .await;

    pool.close().await;
    database.drop().await;
    assert!(pass.is_ok(), "{pass:?}");
    assert_eq!(report.metrics.case2_3.rows_selected, 0);
    assert_eq!(report.completion_candidate_yield, None);
    assert!(serde_json::to_value(&report).unwrap()["completion_candidate_yield"].is_null());
}

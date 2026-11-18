//! Ports of the Python phase-2 tests: consumption from history, the caller's
//! plan setting, the quarantine copy and the locator keys.
//!
//! Python specification: `tests/integration/test_task_history_phase2_consumption.py`,
//! `test_phase2_consume_plan.py`, `test_task_history_phase2_quarantine.py` and
//! the core rows of `test_task_history_workflow_recovery_prototypes.py`. Each
//! test runs in one transaction that it rolls back, on the shared migrated
//! database.

use serial_test::serial;
use uuid::Uuid;

use super::consumption::{consume_phase2, Phase2CallError, Phase2DispositionKind};
use super::quarantine::{quarantine_one, QuarantineVerdict};
use crate::core::history::errors::HistoryError;
use crate::core::lifecycle::TerminalizationOutcome;
use crate::test_support::sql::{self, TestPool, TestTransaction};
use crate::test_support::terminal_writes::{self, TerminalFailure};

const PORT_WORKER: &str = "port-phase2-worker";
const CLASS_KEY: &str = "standard_30d";

async fn shared_pool() -> TestPool {
    crate::test_support::migrated_pool().await
}

async fn seed_workflow(transaction: &mut TestTransaction<'_>, workflow_id: Uuid, status: &str) {
    sql::query(
        "INSERT INTO horsies_workflows (
             id, name, definition_key, status, on_error, output_task_index,
             success_policy, result, error, parent_workflow_id, parent_task_index,
             root_workflow_id, depth, sent_at, created_at, started_at, completed_at,
             updated_at
         ) VALUES (
             $1, 'port_phase2', NULL, $2, 'fail', NULL,
             NULL, NULL, NULL, NULL, NULL,
             $1, 0, NOW(), NOW(), NOW(), NULL,
             NOW()
         )",
    )
    .bind(workflow_id)
    .bind(status)
    .execute(&mut *transaction)
    .await
    .expect("insert workflow");
}

async fn seed_node(
    transaction: &mut TestTransaction<'_>,
    node_id: Uuid,
    workflow_id: Uuid,
    task_id: Uuid,
) {
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow,
             id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
             dependencies, args_from, workflow_ctx_from, allow_failed_deps, join_type,
             min_success, task_options, max_retries, execution, good_until,
             retention_class_key, status, task_id, is_subworkflow, sub_workflow_id,
             sub_workflow_name, sub_definition_key, sub_workflow_summary, result, error,
             created_at, started_at, completed_at
         ) VALUES (
             'NONE', $1, $2, 0, 'root', 'port.phase2', '{}', 'default', 100,
             '{}', NULL, NULL, FALSE, 'all',
             NULL, NULL, 0, 'ISOLATED', NULL,
             $4, 'RUNNING', $3, FALSE, NULL,
             NULL, NULL, NULL, NULL, NULL,
             NOW(), NOW(), NULL
         )",
    )
    .bind(node_id)
    .bind(workflow_id)
    .bind(task_id)
    .bind(CLASS_KEY)
    .execute(&mut *transaction)
    .await
    .expect("insert workflow node");
}

async fn seed_running_workflow_task(transaction: &mut TestTransaction<'_>, task_id: Uuid) {
    sql::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, task_options, execution,
             retention_class_key, sent_at, created_at,
             command_fingerprint_version, command_fingerprint, idempotency_key_digest,
             status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
             started_at, finalizing_at, finalizing_by_worker_id,
             worker_pid, worker_hostname, worker_process_name,
             retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
             updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
         ) VALUES (
             $1, 'port.phase2', 'default', 100, '{}', NULL, 'ISOLATED',
             $2, NOW(), NOW(),
             NULL, NULL, NULL,
             'RUNNING', NOW(), NOW(), $3, NULL,
             NOW(), NULL, NULL,
             9, 'port-host', 'port-process',
             0, 0, NULL, NULL, TRUE,
             NOW(), FALSE, NULL, NULL
         )",
    )
    .bind(task_id)
    .bind(CLASS_KEY)
    .bind(PORT_WORKER)
    .execute(&mut *transaction)
    .await
    .expect("insert running workflow task");
}

/// The ids of one workflow task that failed and moved to history, leaving a
/// HISTORY pending row for its node.
struct PendingEvidence {
    task_id: Uuid,
    workflow_id: Uuid,
    node_id: Uuid,
}

async fn seed_pending(
    transaction: &mut TestTransaction<'_>,
    workflow_status: &str,
) -> PendingEvidence {
    let evidence = PendingEvidence {
        task_id: Uuid::new_v4(),
        workflow_id: Uuid::new_v4(),
        node_id: Uuid::new_v4(),
    };
    seed_workflow(transaction, evidence.workflow_id, workflow_status).await;
    seed_running_workflow_task(transaction, evidence.task_id).await;
    seed_node(
        transaction,
        evidence.node_id,
        evidence.workflow_id,
        evidence.task_id,
    )
    .await;
    let outcomes = terminal_writes::fail_running_task(
        &mut *transaction,
        evidence.task_id,
        PORT_WORKER,
        &TerminalFailure::of_task(
            "{\"Err\":{}}",
            Some("TASK_ERROR"),
            Some("port phase-2 failure"),
        ),
    )
    .await
    .expect("fail the workflow task");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{outcomes:?}"
    );
    assert_eq!(
        pending_source(transaction, evidence.task_id)
            .await
            .as_deref(),
        Some("HISTORY")
    );
    evidence
}

async fn pending_source(transaction: &mut TestTransaction<'_>, task_id: Uuid) -> Option<String> {
    sql::query_scalar(
        "SELECT recovery_source FROM horsies_workflow_phase2_pending WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(&mut *transaction)
    .await
    .expect("read pending row")
}

async fn history_result_text(transaction: &mut TestTransaction<'_>, task_id: Uuid) -> String {
    sql::query_scalar(
        "SELECT convert_from(result_payload, 'UTF8') FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(&mut *transaction)
    .await
    .expect("read history result")
}

async fn node_state(
    transaction: &mut TestTransaction<'_>,
    node_id: Uuid,
) -> (String, Option<String>) {
    sql::query_as("SELECT status, result FROM horsies_workflow_tasks WHERE id = $1")
        .bind(node_id)
        .fetch_one(&mut *transaction)
        .await
        .expect("read node")
}

/// `test_phase2_consume_plan.py::test_caller_plan_setting_survives_the_call`:
/// the function's own plan setting does not leak into the caller's session.
#[tokio::test]
#[serial]
async fn the_caller_plan_setting_survives_the_consume_call() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    sql::query("SET LOCAL plan_cache_mode = force_custom_plan")
        .execute(&mut *transaction)
        .await
        .expect("set the caller plan mode");
    let disposition = consume_phase2(&mut transaction, Uuid::new_v4(), "FAILED")
        .await
        .expect("consume an unknown task");
    assert_eq!(
        disposition.disposition,
        Phase2DispositionKind::PendingAbsent
    );
    let setting: String = sql::query_scalar("SHOW plan_cache_mode")
        .fetch_one(&mut *transaction)
        .await
        .expect("read the caller plan mode");
    assert_eq!(setting, "force_custom_plan");
    transaction.rollback().await.expect("rollback");
}

/// `TestDurableDispositions.test_applied_to_node_records_the_history_result`:
/// consumption writes the node from the history row and deletes the pending
/// row.
#[tokio::test]
#[serial]
async fn consumption_applies_the_history_result_to_the_node() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let evidence = seed_pending(&mut transaction, "RUNNING").await;
    let verdict = consume_phase2(&mut transaction, evidence.task_id, "FAILED")
        .await
        .expect("consume");
    assert_eq!(verdict.disposition, Phase2DispositionKind::AppliedToNode);
    assert_eq!(verdict.workflow_id, Some(evidence.workflow_id));
    assert_eq!(verdict.node_status.as_deref(), Some("FAILED"));
    assert_eq!(verdict.workflow_status.as_deref(), Some("RUNNING"));
    let history_result = history_result_text(&mut transaction, evidence.task_id).await;
    let (status, result) = node_state(&mut transaction, evidence.node_id).await;
    assert_eq!(status, "FAILED");
    assert_eq!(result, Some(history_result));
    assert_eq!(
        pending_source(&mut transaction, evidence.task_id).await,
        None
    );
    transaction.rollback().await.expect("rollback");
}

/// `TestDurableDispositions.test_replay_after_evidence_is_gone_is_already_applied`:
/// a second consumption, after the first deleted the pending row, is
/// `ALREADY_APPLIED` and reports the node status that the first wrote.
#[tokio::test]
#[serial]
async fn a_replay_after_the_evidence_is_gone_is_already_applied() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let evidence = seed_pending(&mut transaction, "RUNNING").await;
    let first = consume_phase2(&mut transaction, evidence.task_id, "FAILED")
        .await
        .expect("consume");
    assert_eq!(first.disposition, Phase2DispositionKind::AppliedToNode);
    assert_eq!(
        pending_source(&mut transaction, evidence.task_id).await,
        None
    );

    let replay = consume_phase2(&mut transaction, evidence.task_id, "FAILED")
        .await
        .expect("consume again");

    assert_eq!(replay.disposition, Phase2DispositionKind::AlreadyApplied);
    assert_eq!(replay.node_status.as_deref(), Some("FAILED"));
    transaction.rollback().await.expect("rollback");
}

/// `TestDurableDispositions.test_paused_workflow_still_records_the_node` and
/// `test_phase2_applies_node_from_history_while_running_or_paused` (prototype,
/// PAUSED half).
#[tokio::test]
#[serial]
async fn a_paused_workflow_still_records_the_node() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let evidence = seed_pending(&mut transaction, "PAUSED").await;
    let verdict = consume_phase2(&mut transaction, evidence.task_id, "FAILED")
        .await
        .expect("consume");
    assert_eq!(verdict.disposition, Phase2DispositionKind::AppliedToNode);
    assert_eq!(verdict.workflow_status.as_deref(), Some("PAUSED"));
    let (status, _) = node_state(&mut transaction, evidence.node_id).await;
    assert_eq!(status, "FAILED");
    let replay = consume_phase2(&mut transaction, evidence.task_id, "FAILED")
        .await
        .expect("consume again");
    assert_eq!(replay.disposition, Phase2DispositionKind::AlreadyApplied);
    transaction.rollback().await.expect("rollback");
}

/// `TestRepoint.test_copy_goes_with_its_node`: deleting the workflow removes
/// the node, its pending row and its quarantine copy.
#[tokio::test]
#[serial]
async fn deleting_a_workflow_removes_the_pending_row_and_the_quarantine_copy() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let evidence = seed_pending(&mut transaction, "RUNNING").await;
    // The repoint that `horsies_phase2_quarantine_one` performs, written out:
    // copy the history row, then point the pending row at the copy.
    sql::query(
        "INSERT INTO horsies_workflow_phase2_quarantine (
             task_id, workflow_id, workflow_node_row_id, terminalization_kind, terminal_at,
             retention_class_key, history_schema_version, result_envelope_version,
             result_payload, result_digest, quarantined_at
         )
         SELECT h.task_id, p.workflow_id, p.workflow_node_row_id, h.terminalization_kind,
                h.terminal_at, p.history_class, h.history_schema_version,
                h.result_envelope_version, h.result_payload, h.result_digest,
                statement_timestamp()
         FROM horsies_workflow_phase2_pending AS p
         JOIN horsies_task_history AS h ON h.task_id = p.task_id
         WHERE p.task_id = $1",
    )
    .bind(evidence.task_id)
    .execute(&mut *transaction)
    .await
    .expect("copy the evidence");
    sql::query(
        "UPDATE horsies_workflow_phase2_pending
         SET recovery_source = 'QUARANTINE', quarantine_task_id = task_id,
             history_class = NULL, history_anchor = NULL
         WHERE task_id = $1",
    )
    .bind(evidence.task_id)
    .execute(&mut *transaction)
    .await
    .expect("repoint the pending row");

    sql::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(evidence.workflow_id)
        .execute(&mut *transaction)
        .await
        .expect("delete the workflow");

    let (pending, copies): (i64, i64) = sql::query_as(
        "SELECT
             (SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $1),
             (SELECT count(*) FROM horsies_workflow_phase2_quarantine WHERE task_id = $1)",
    )
    .bind(evidence.task_id)
    .fetch_one(&mut *transaction)
    .await
    .expect("count evidence");
    assert_eq!((pending, copies), (0, 0));
    transaction.rollback().await.expect("rollback");
}

/// `test_pending_locator_structurally_pins_node_to_workflow` (prototype): a
/// pending row whose node belongs to another workflow is refused by the
/// locator key.
#[tokio::test]
#[serial]
async fn the_pending_locator_pins_its_node_to_its_workflow() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let evidence = seed_pending(&mut transaction, "RUNNING").await;
    let other_workflow = Uuid::new_v4();
    seed_workflow(&mut transaction, other_workflow, "RUNNING").await;
    let refused = sql::query(
        "UPDATE horsies_workflow_phase2_pending SET workflow_id = $2 WHERE task_id = $1",
    )
    .bind(evidence.task_id)
    .bind(other_workflow)
    .execute(&mut *transaction)
    .await
    .expect_err("a node of another workflow is refused");
    assert_eq!(
        refused
            .as_database_error()
            .and_then(|error| error.constraint()),
        Some("horsies_workflow_phase2_pending_node_fkey")
    );
    transaction.rollback().await.expect("rollback");
}

/// `TestRepoint.test_locator_is_copied_verified_repointed`: the copy holds the
/// kind, the class and the payload of the history row, and the pending row
/// points at it.
#[tokio::test]
#[serial]
async fn a_locator_is_copied_verified_and_repointed() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let evidence = seed_pending(&mut transaction, "RUNNING").await;
    let outcome = quarantine_one(&mut transaction, evidence.task_id)
        .await
        .expect("quarantine");
    assert_eq!(outcome.verdict, QuarantineVerdict::Repointed);
    let copy_matches: bool = sql::query_scalar(
        "SELECT q.terminalization_kind = h.terminalization_kind
                AND q.retention_class_key = h.retention_class_key
                AND q.result_payload = h.result_payload
                AND q.result_digest = h.result_digest
         FROM horsies_workflow_phase2_quarantine AS q
         JOIN horsies_task_history AS h ON h.task_id = q.task_id
         WHERE q.task_id = $1",
    )
    .bind(evidence.task_id)
    .fetch_one(&mut *transaction)
    .await
    .expect("compare the copy");
    assert!(copy_matches);
    assert_eq!(
        pending_source(&mut transaction, evidence.task_id)
            .await
            .as_deref(),
        Some("QUARANTINE")
    );
    transaction.rollback().await.expect("rollback");
}

/// `TestRefusal.test_broken_locator_refuses_and_retains_history_source`: a
/// locator that points at no history row is refused, and the pending row
/// keeps its history source.
#[tokio::test]
#[serial]
async fn a_broken_locator_refuses_and_keeps_its_history_source() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let evidence = seed_pending(&mut transaction, "RUNNING").await;
    sql::query(
        "UPDATE horsies_workflow_phase2_pending
         SET history_anchor = history_anchor + interval '1 second'
         WHERE task_id = $1",
    )
    .bind(evidence.task_id)
    .execute(&mut *transaction)
    .await
    .expect("break the locator");
    let outcome = quarantine_one(&mut transaction, evidence.task_id)
        .await
        .expect("quarantine");
    assert_eq!(outcome.verdict, QuarantineVerdict::SourceAbsent);
    assert_eq!(
        pending_source(&mut transaction, evidence.task_id)
            .await
            .as_deref(),
        Some("HISTORY")
    );
    transaction.rollback().await.expect("rollback");
}

/// `TestRefusal.test_unknown_task_is_pending_gone`.
#[tokio::test]
#[serial]
async fn an_unknown_task_is_pending_gone() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let outcome = quarantine_one(&mut transaction, Uuid::new_v4())
        .await
        .expect("quarantine");
    assert_eq!(outcome.verdict, QuarantineVerdict::PendingGone);
    transaction.rollback().await.expect("rollback");
}

/// `TestEvidenceRetention.test_invalid_node_status_raises_typed`: consumption
/// refuses a node status other than COMPLETED and FAILED. The database
/// refuses it with `invalid_parameter_value` (22023) before it reads
/// anything, and the client returns that refusal.
#[tokio::test]
#[serial]
async fn consumption_refuses_a_node_status_that_is_not_terminal() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");

    let refused = consume_phase2(&mut transaction, Uuid::new_v4(), "RUNNING")
        .await
        .expect_err("RUNNING is not a terminal node status");

    assert_eq!(
        refusal_message(&refused),
        "terminal node status must be COMPLETED or FAILED"
    );
    match &refused {
        Phase2CallError::Database(error) => assert_eq!(error.sqlstate(), Some("22023")),
        other => panic!("expected a database refusal, got {other:?}"),
    }
    transaction.rollback().await.expect("rollback");
}

/// The primary message of a database refusal.
fn refusal_message(error: &Phase2CallError) -> String {
    match error {
        Phase2CallError::Database(error) => error
            .as_database_error()
            .map(|database| database.message().to_owned())
            .expect("the refusal comes from the database"),
        other => panic!("expected a database refusal, got {other:?}"),
    }
}

/// The pending-row write that a test makes the database refuse.
#[derive(Debug, Clone, Copy)]
enum PendingWrite {
    Update,
    Delete,
}

impl PendingWrite {
    fn event(self) -> &'static str {
        match self {
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

/// Makes the database raise `message` for each pending row that `write`
/// touches and `condition` (the trigger's `WHEN` clause) selects, until
/// `transaction` ends.
async fn refuse_pending_rows(
    transaction: &mut TestTransaction<'_>,
    name: &str,
    write: PendingWrite,
    condition: &str,
    message: &str,
) {
    sql::query(&format!(
        "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $function$
         BEGIN
             RAISE EXCEPTION '{message}';
         END
         $function$"
    ))
    .execute(&mut *transaction)
    .await
    .expect("create the refusing function");
    sql::query(&format!(
        "CREATE TRIGGER {name}
         BEFORE {} ON horsies_workflow_phase2_pending
         FOR EACH ROW WHEN ({condition})
         EXECUTE FUNCTION {name}()",
        write.event()
    ))
    .execute(&mut *transaction)
    .await
    .expect("create the refusing trigger");
}

async fn quarantine_copies(transaction: &mut TestTransaction<'_>, task_id: Uuid) -> i64 {
    sql::query_scalar("SELECT count(*) FROM horsies_workflow_phase2_quarantine WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(&mut *transaction)
        .await
        .expect("count quarantine copies")
}

/// `test_task_history_workflow_recovery_prototypes.py::test_quarantine_copy_rolls_back_when_repoint_fails`:
/// the copy and the repoint are one call of `horsies_phase2_quarantine_one`,
/// so a refused repoint leaves no copy and the pending row keeps its history
/// source.
#[tokio::test]
#[serial]
async fn a_refused_repoint_leaves_no_quarantine_copy() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let evidence = seed_pending(&mut transaction, "RUNNING").await;
    refuse_pending_rows(
        &mut transaction,
        "port_refuse_pending_repoint",
        PendingWrite::Update,
        "NEW.recovery_source = 'QUARANTINE'",
        "repoint disabled by test",
    )
    .await;

    let mut savepoint = transaction.begin().await.expect("begin savepoint");
    let refused = quarantine_one(&mut savepoint, evidence.task_id)
        .await
        .expect_err("the repoint is refused");
    savepoint.rollback().await.expect("roll back the savepoint");

    assert_eq!(refusal_message(&refused), "repoint disabled by test");
    assert_eq!(
        pending_source(&mut transaction, evidence.task_id)
            .await
            .as_deref(),
        Some("HISTORY")
    );
    assert_eq!(
        quarantine_copies(&mut transaction, evidence.task_id).await,
        0
    );
    transaction.rollback().await.expect("rollback");
}

/// `test_task_history_workflow_recovery_prototypes.py::test_phase2_delete_failure_rolls_back_all_durable_effects`:
/// consumption writes the node, then deletes the pending row and the
/// quarantine copy in one call of `horsies_phase2_consume`, so a refused
/// pending delete leaves the node unwritten and both recovery rows in place.
/// The prototype's parent-propagation rows have no baseline counterpart.
#[tokio::test]
#[serial]
async fn a_refused_pending_delete_leaves_the_node_and_both_recovery_rows() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let evidence = seed_pending(&mut transaction, "RUNNING").await;
    let quarantined = quarantine_one(&mut transaction, evidence.task_id)
        .await
        .expect("quarantine");
    assert_eq!(quarantined.verdict, QuarantineVerdict::Repointed);
    refuse_pending_rows(
        &mut transaction,
        "port_refuse_pending_delete",
        PendingWrite::Delete,
        "TRUE",
        "pending delete disabled by test",
    )
    .await;

    let mut savepoint = transaction.begin().await.expect("begin savepoint");
    let refused = consume_phase2(&mut savepoint, evidence.task_id, "FAILED")
        .await
        .expect_err("the pending delete is refused");
    savepoint.rollback().await.expect("roll back the savepoint");

    assert_eq!(refusal_message(&refused), "pending delete disabled by test");
    let (status, result) = node_state(&mut transaction, evidence.node_id).await;
    assert_eq!(status, "RUNNING");
    assert_eq!(result, None);
    assert_eq!(
        pending_source(&mut transaction, evidence.task_id)
            .await
            .as_deref(),
        Some("QUARANTINE")
    );
    assert_eq!(
        quarantine_copies(&mut transaction, evidence.task_id).await,
        1
    );
    transaction.rollback().await.expect("rollback");
}

/// The text of `horsies_phase2_consume` in the baseline, from its
/// `CREATE FUNCTION` to the end of its body.
fn consume_function_text() -> &'static str {
    let sql = crate::broker::migrations::BASELINE_INTERIM_FUNCTIONS_SQL;
    let start = sql
        .find("CREATE FUNCTION horsies_phase2_consume(")
        .expect("the baseline defines horsies_phase2_consume");
    let length = sql[start..]
        .find("\n$$;")
        .expect("the body of horsies_phase2_consume ends");
    &sql[start..start + length]
}

/// The first 400 characters of `text` from byte `start`.
fn span_from(text: &str, start: usize) -> String {
    text[start..].chars().take(400).collect()
}

/// `tests/unit/test_task_history_phase2.py::TestLockOrder::test_n6_order_workflow_then_node_then_pending`:
/// on its main path the consumption locks the workflow row, then the node
/// row, then the pending row, each `FOR UPDATE` (the lock order of the
/// engine).
#[test]
fn consumption_locks_the_workflow_then_the_node_then_the_pending_row() {
    let body = consume_function_text();
    let order = body
        .find("Lock order:")
        .expect("the body states its lock order");
    let workflow = order
        + body[order..]
            .find("FROM horsies_workflows w")
            .expect("the workflow lock");
    let node = workflow
        + body[workflow..]
            .find("FROM horsies_workflow_tasks wt")
            .expect("the node lock");
    let pending = node
        + body[node..]
            .find("FROM horsies_workflow_phase2_pending")
            .expect("the pending lock");
    assert!(workflow < node && node < pending);
    for start in [workflow, node, pending] {
        let span = span_from(body, start);
        assert!(span.contains("FOR UPDATE"), "{span}");
    }
}

/// `tests/unit/test_task_history_phase2.py::TestPrunedProbe::test_history_read_carries_both_partition_keys`:
/// the history read of the consumption names both partition keys of the one
/// leaf that holds the row (class and terminal time) and the task id, after
/// the comment that rejects the fan-out over every leaf.
#[test]
fn the_history_read_names_both_partition_keys_of_its_leaf() {
    let body = consume_function_text();
    let probe = body
        .find("h.retention_class_key = v_pending.history_class")
        .expect("the read names the class");
    assert!(body.contains("h.terminal_at = v_pending.history_anchor"));
    assert!(body.contains("h.task_id = p_task_id"));
    let comment = body
        .find("NOT the rejected fan-out")
        .expect("the comment of the one-leaf probe");
    assert!(comment < probe);
}

/// `tests/unit/test_task_history_phase2.py::TestPrunedProbe::test_composite_node_lock_uses_structural_pinning`:
/// the node lock of the consumption names the node row id and its workflow,
/// so a locator cannot reach a node of another workflow.
#[test]
fn the_node_lock_is_pinned_by_its_row_id_and_its_workflow() {
    let body = consume_function_text();
    assert!(body.contains("wt.id = v_pending.workflow_node_row_id"));
    assert!(body.contains("wt.workflow_id = v_pending.workflow_id"));
}

/// Every disposition that the decoder reads.
const EVERY_DISPOSITION: [Phase2DispositionKind; 8] = [
    Phase2DispositionKind::AppliedToNode,
    Phase2DispositionKind::AlreadyApplied,
    Phase2DispositionKind::SupersededByWorkflowTerminal,
    Phase2DispositionKind::SourceStateConflict,
    Phase2DispositionKind::PendingAbsent,
    Phase2DispositionKind::SourceAbsent,
    Phase2DispositionKind::SourceVersionConflict,
    Phase2DispositionKind::SourceDigestMismatch,
];

/// The place of `kind` in [`EVERY_DISPOSITION`]. A new disposition fails
/// this match until it is listed there.
fn listed_place(kind: Phase2DispositionKind) -> usize {
    match kind {
        Phase2DispositionKind::AppliedToNode => 0,
        Phase2DispositionKind::AlreadyApplied => 1,
        Phase2DispositionKind::SupersededByWorkflowTerminal => 2,
        Phase2DispositionKind::SourceStateConflict => 3,
        Phase2DispositionKind::PendingAbsent => 4,
        Phase2DispositionKind::SourceAbsent => 5,
        Phase2DispositionKind::SourceVersionConflict => 6,
        Phase2DispositionKind::SourceDigestMismatch => 7,
    }
}

/// `tests/unit/test_task_history_phase2.py::TestDispositionVocabulary::test_sql_and_decoder_vocabularies_are_closed_together`
/// and `TestDecoder::test_unknown_disposition_raises`: each disposition of the
/// decoder is a text that the baseline body of `horsies_phase2_consume`
/// returns, and it reads back as its kind; the durable dispositions are among
/// them; any other text is a contract error that names it.
#[test]
fn each_disposition_of_the_consume_function_reads_and_an_unknown_one_is_refused() {
    let body = consume_function_text();
    for kind in EVERY_DISPOSITION {
        assert_eq!(EVERY_DISPOSITION[listed_place(kind)], kind);
        assert!(body.contains(&format!("'{}'", kind.as_str())), "{kind:?}");
        assert!(
            matches!(Phase2DispositionKind::try_from(kind.as_str()), Ok(read) if read == kind),
            "{kind:?}"
        );
    }
    assert!(Phase2DispositionKind::DURABLE
        .iter()
        .all(|kind| EVERY_DISPOSITION.contains(kind)));
    match Phase2DispositionKind::try_from("EXPLODED") {
        Err(HistoryError::Contract(detail)) => {
            assert!(detail.contains("unknown phase-2 disposition"), "{detail}");
            assert!(detail.contains("EXPLODED"), "{detail}");
        }
        other => panic!("an unknown disposition must be a contract error, got {other:?}"),
    }
}

/// `tests/unit/test_task_history_phase2.py::TestDispositionVocabulary::test_pending_deletes_exist_only_on_durable_paths`:
/// the body of `horsies_phase2_consume` deletes the pending row in two
/// statements, on its durable paths: the workflow is already terminal, and
/// the node write.
#[test]
fn the_consumption_deletes_the_pending_row_in_two_statements() {
    assert_eq!(
        consume_function_text()
            .matches("DELETE FROM horsies_workflow_phase2_pending")
            .count(),
        2
    );
}

/// `tests/unit/test_task_history_phase2.py::TestDispositionVocabulary::test_the_type_is_created_before_the_function`
/// and `::test_type_carries_the_progression_context`: the baseline creates
/// the row type `horsies_phase2_disposition` before `horsies_phase2_consume`,
/// and the type carries the workflow status, depth, root and `on_error`.
#[test]
fn the_disposition_type_precedes_the_function_and_carries_the_progression_context() {
    let sql = crate::broker::migrations::BASELINE_INTERIM_FUNCTIONS_SQL;
    let type_start = sql
        .find("CREATE TYPE horsies_phase2_disposition AS (")
        .expect("the baseline defines horsies_phase2_disposition");
    let function_start = sql
        .find("CREATE FUNCTION horsies_phase2_consume(")
        .expect("the baseline defines horsies_phase2_consume");
    assert!(type_start < function_start);
    let type_length = sql[type_start..]
        .find(");")
        .expect("the type definition ends");
    let type_text = &sql[type_start..type_start + type_length];
    for column in [
        "workflow_status text",
        "workflow_depth integer",
        "root_workflow_id uuid",
        "on_error text",
    ] {
        assert!(type_text.contains(column), "{column}: {type_text}");
    }
}

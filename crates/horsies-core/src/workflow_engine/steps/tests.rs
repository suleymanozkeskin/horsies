//! Tests of the workflow step calls.
//!
//! - The decoders read rows of the shape each function returns. These tests
//!   build the rows with literal `SELECT`s, so they need a database but not
//!   the step functions.
//! - The operation tests call the step functions of `0005_workflow_functions.sql`
//!   through the public lifecycle functions.

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;
use crate::core::workflow::handle_types::HandleErrorCode;
use crate::workflow_engine::lifecycle::{cancel_workflow, pause_workflow};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn test_broker() -> PostgresBroker {
    PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await)
}

/// The rows of a literal statement, read through the main core pool.
async fn rows_of(broker: &PostgresBroker, statement: &BoundStatement<'_>) -> Vec<Row> {
    broker
        .query_main(statement)
        .await
        .expect("literal statement runs")
}

async fn seed_workflow(pool: &PgPool, id: Uuid, status: &str, parent: Option<Uuid>) {
    sqlx::query(
        "INSERT INTO horsies_workflows (
             id, name, status, on_error, definition_key, depth,
             root_workflow_id, parent_workflow_id, parent_task_index,
             sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'workflow_steps', $2, 'fail', $3, $4, $5, $6, $7,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(id)
    .bind(status)
    .bind(format!("test.workflow-steps.{id}"))
    .bind(if parent.is_some() { 1_i32 } else { 0_i32 })
    .bind(parent.unwrap_or(id))
    .bind(parent)
    .bind(parent.map(|_| 0_i32))
    .execute(pool)
    .await
    .expect("seed workflow");
}

/// A CLAIMED task behind an ENQUEUED node at index 0 of `workflow_id`.
async fn seed_claimed_node(pool: &PgPool, workflow_id: Uuid, task_id: Uuid) {
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, status, sent_at, enqueued_at,
             claimed_at, claimed_by_worker_id, max_retries, retry_count, is_workflow_task,
             command_fingerprint_version, command_fingerprint, retention_class_key,
             retain_rerun_input, created_at, updated_at, execution, claim_expires_at
         ) VALUES (
             $1, 'workflow_steps_task', 'default', 100, 'null', 'CLAIMED', NOW(), NOW(),
             NOW(), 'steps-worker', 0, 0, TRUE, NULL, NULL, 'forever', FALSE, NOW(), NOW(),
             'ISOLATED', NOW() + interval '1 hour'
         )",
    )
    .bind(task_id)
    .execute(pool)
    .await
    .expect("seed claimed task");
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, status,
             is_subworkflow, task_id, created_at, max_retries, execution, retention_class_key
         ) VALUES (
             'NONE', $1, $2, 0, 'root', 'workflow_steps_task', 'null', 'default', 100, '{}',
             FALSE, 'all', 'ENQUEUED', FALSE, $3, NOW(), 0, 'ISOLATED', 'forever'
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(task_id)
    .execute(pool)
    .await
    .expect("seed enqueued node");
}

async fn workflow_status(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("workflow status")
}

/// Status and task id of the node at index 0.
async fn root_node(pool: &PgPool, workflow_id: Uuid) -> (String, Option<Uuid>) {
    sqlx::query_as(
        "SELECT status, task_id FROM horsies_workflow_tasks
         WHERE workflow_id = $1 AND task_index = 0",
    )
    .bind(workflow_id)
    .fetch_one(pool)
    .await
    .expect("root node")
}

// ---------------------------------------------------------------------------
// Decoders
// ---------------------------------------------------------------------------

static PAUSE_ROW: TypedStatement = TypedStatement::new(
    "test_pause_row",
    "SELECT $1::text AS outcome,
            CASE WHEN $2 THEN ARRAY[$3::uuid] END AS paused_workflow_ids,
            CASE WHEN $2 THEN ARRAY[]::uuid[] END AS abandoned_task_ids",
    &[
        ParameterType::Text,
        ParameterType::Bool,
        ParameterType::Uuid,
    ],
);

static CANCEL_ROW: TypedStatement = TypedStatement::new(
    "test_cancel_row",
    "SELECT $1::text AS outcome,
            CASE WHEN $2 THEN ARRAY[$3::uuid] END AS cancelled_workflow_ids,
            CASE WHEN $2 THEN ARRAY[$3::uuid] END AS cancelled_task_ids",
    &[
        ParameterType::Text,
        ParameterType::Bool,
        ParameterType::Uuid,
    ],
);

async fn pause_row(broker: &PostgresBroker, outcome: &str, with_lists: bool, id: Uuid) -> Vec<Row> {
    let outcome = outcome.to_owned();
    rows_of(
        broker,
        &PAUSE_ROW
            .bind()
            .value(&outcome)
            .value(&with_lists)
            .value(&id)
            .finish(),
    )
    .await
}

async fn cancel_row(
    broker: &PostgresBroker,
    outcome: &str,
    with_lists: bool,
    id: Uuid,
) -> Vec<Row> {
    let outcome = outcome.to_owned();
    rows_of(
        broker,
        &CANCEL_ROW
            .bind()
            .value(&outcome)
            .value(&with_lists)
            .value(&id)
            .finish(),
    )
    .await
}

#[tokio::test]
#[serial]
async fn each_pause_outcome_reads_into_its_variant() {
    let broker = test_broker().await;
    let id = Uuid::new_v4();

    assert_eq!(
        pause_outcome(pause_row(&broker, "PAUSED", true, id).await).expect("paused"),
        PauseOutcome::Paused {
            paused_workflow_ids: vec![id],
            abandoned_task_ids: Vec::new(),
        }
    );
    assert_eq!(
        pause_outcome(pause_row(&broker, "NOT_RUNNING", false, id).await).expect("not running"),
        PauseOutcome::NotRunning
    );
    assert_eq!(
        pause_outcome(pause_row(&broker, "NOT_FOUND", false, id).await).expect("not found"),
        PauseOutcome::NotFound
    );
}

#[tokio::test]
#[serial]
async fn a_pause_row_outside_the_contract_is_refused() {
    let broker = test_broker().await;
    let id = Uuid::new_v4();

    let unknown = pause_outcome(pause_row(&broker, "SUSPENDED", false, id).await)
        .expect_err("unknown outcome");
    assert!(
        matches!(
            &unknown,
            StepContractError::UnknownOutcome { function: "horsies_workflow_pause", outcome }
                if outcome == "SUSPENDED"
        ),
        "{unknown:?}"
    );
    let without_lists = pause_outcome(pause_row(&broker, "PAUSED", false, id).await)
        .expect_err("PAUSED without its id lists");
    assert!(
        matches!(
            without_lists,
            StepContractError::Row(RowDecodeError::Column { .. })
        ),
        "{without_lists:?}"
    );
    let no_row = pause_outcome(Vec::new()).expect_err("no row");
    assert!(
        matches!(no_row, StepContractError::Row(RowDecodeError::NoRow { .. })),
        "{no_row:?}"
    );
}

#[tokio::test]
#[serial]
async fn each_cancel_outcome_reads_into_its_variant() {
    let broker = test_broker().await;
    let id = Uuid::new_v4();

    assert_eq!(
        cancel_outcome(cancel_row(&broker, "CANCELLED", true, id).await).expect("cancelled"),
        CancelOutcome::Cancelled {
            cancelled_workflow_ids: vec![id],
            cancelled_task_ids: vec![id],
        }
    );
    assert_eq!(
        cancel_outcome(cancel_row(&broker, "NOT_CANCELLABLE", false, id).await)
            .expect("not cancellable"),
        CancelOutcome::NotCancellable
    );
    assert_eq!(
        cancel_outcome(cancel_row(&broker, "NOT_FOUND", false, id).await).expect("not found"),
        CancelOutcome::NotFound
    );
    let unknown = cancel_outcome(cancel_row(&broker, "CANCELLING", false, id).await)
        .expect_err("unknown outcome");
    assert!(
        matches!(unknown, StepContractError::UnknownOutcome { .. }),
        "{unknown:?}"
    );
}

#[test]
fn a_step_contract_error_is_internal_and_not_retryable() {
    let workflow_id = Uuid::new_v4();
    let error = crate::workflow_engine::lifecycle::handle_operation_error(
        &WorkflowError::StepContract(StepContractError::UnknownOutcome {
            function: "horsies_workflow_pause",
            outcome: "SUSPENDED".to_owned(),
        }),
        workflow_id,
    );
    assert_eq!(error.code, HandleErrorCode::InternalFailed);
    assert!(!error.retryable);
}

// ---------------------------------------------------------------------------
// Pause and cancel through the step functions
// ---------------------------------------------------------------------------

/// Pause moves the RUNNING root and its RUNNING child to PAUSED, leaves a
/// COMPLETED child alone, abandons the CLAIMED task behind the child's
/// ENQUEUED node and puts that node back to READY with no task.
#[tokio::test]
#[serial]
async fn pause_pauses_the_running_tree_and_abandons_its_claimed_tasks() {
    let broker = test_broker().await;
    let pool = broker.pool().clone();
    let root = Uuid::new_v4();
    let running_child = Uuid::new_v4();
    let completed_child = Uuid::new_v4();
    let claimed_task = Uuid::new_v4();
    seed_workflow(&pool, root, "RUNNING", None).await;
    seed_workflow(&pool, running_child, "RUNNING", Some(root)).await;
    seed_workflow(&pool, completed_child, "COMPLETED", Some(root)).await;
    seed_claimed_node(&pool, running_child, claimed_task).await;

    assert!(pause_workflow(&broker, root).await.expect("pause"));

    assert_eq!(workflow_status(&pool, root).await, "PAUSED");
    assert_eq!(workflow_status(&pool, running_child).await, "PAUSED");
    assert_eq!(workflow_status(&pool, completed_child).await, "COMPLETED");
    assert_eq!(
        root_node(&pool, running_child).await,
        ("READY".to_owned(), None)
    );
    let archived: String = sqlx::query_scalar(
        "SELECT terminalization_kind FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(claimed_task)
    .fetch_one(&pool)
    .await
    .expect("abandoned task in history");
    assert_eq!(archived, "PAUSE_ABANDON_WORKFLOW");
}

/// A workflow that is not RUNNING is not paused (`false`, nothing written);
/// an id with no workflow is `WORKFLOW_NOT_FOUND`.
#[tokio::test]
#[serial]
async fn pause_answers_not_running_and_missing_workflows() {
    let broker = test_broker().await;
    let pool = broker.pool().clone();
    let paused = Uuid::new_v4();
    seed_workflow(&pool, paused, "PAUSED", None).await;

    assert!(!pause_workflow(&broker, paused)
        .await
        .expect("pause of PAUSED"));
    assert_eq!(workflow_status(&pool, paused).await, "PAUSED");

    let missing = Uuid::new_v4();
    let error = pause_workflow(&broker, missing)
        .await
        .expect_err("pause of a missing workflow");
    assert_eq!(error.code, HandleErrorCode::WorkflowNotFound);
    assert_eq!(error.workflow_id, missing);
}

/// Cancel moves the root and its PAUSED child to CANCELLED, moves the CLAIMED
/// task behind the child's ENQUEUED node to history, and skips that node.
#[tokio::test]
#[serial]
async fn cancel_cancels_the_tree_and_the_tasks_behind_its_enqueued_nodes() {
    let broker = test_broker().await;
    let pool = broker.pool().clone();
    let root = Uuid::new_v4();
    let paused_child = Uuid::new_v4();
    let claimed_task = Uuid::new_v4();
    seed_workflow(&pool, root, "RUNNING", None).await;
    seed_workflow(&pool, paused_child, "PAUSED", Some(root)).await;
    seed_claimed_node(&pool, paused_child, claimed_task).await;

    assert!(cancel_workflow(&broker, root).await.expect("cancel"));

    assert_eq!(workflow_status(&pool, root).await, "CANCELLED");
    assert_eq!(workflow_status(&pool, paused_child).await, "CANCELLED");
    assert_eq!(root_node(&pool, paused_child).await.0, "SKIPPED".to_owned());
    let archived: String =
        sqlx::query_scalar("SELECT status FROM horsies_task_history WHERE task_id = $1")
            .bind(claimed_task)
            .fetch_one(&pool)
            .await
            .expect("cancelled task in history");
    assert_eq!(archived, "CANCELLED");
}

/// A terminal workflow is not cancelled again (`false`, nothing written); an
/// id with no workflow is `WORKFLOW_NOT_FOUND`.
#[tokio::test]
#[serial]
async fn cancel_answers_terminal_and_missing_workflows() {
    let broker = test_broker().await;
    let pool = broker.pool().clone();
    let completed = Uuid::new_v4();
    let cancelled = Uuid::new_v4();
    seed_workflow(&pool, completed, "COMPLETED", None).await;
    seed_workflow(&pool, cancelled, "CANCELLED", None).await;

    assert!(!cancel_workflow(&broker, completed)
        .await
        .expect("cancel of COMPLETED"));
    assert!(!cancel_workflow(&broker, cancelled)
        .await
        .expect("cancel of CANCELLED"));
    assert_eq!(workflow_status(&pool, completed).await, "COMPLETED");

    let missing = Uuid::new_v4();
    let error = cancel_workflow(&broker, missing)
        .await
        .expect_err("cancel of a missing workflow");
    assert_eq!(error.code, HandleErrorCode::WorkflowNotFound);
}

// ---------------------------------------------------------------------------
// Step rows
// ---------------------------------------------------------------------------

/// A step whose `OUTCOME` row comes last, after a `CHILD_START` row and its
/// `SOURCE` row, as `horsies_workflow_node_finish` returns them.
static STEP_ROWS_WITH_CHILD_START: TypedStatement = TypedStatement::new(
    "test_step_rows_with_child_start",
    "SELECT * FROM (VALUES
         ('CHILD_START'::text, NULL::text, $1::uuid, NULL::uuid, NULL::text, 3::integer),
         ('SOURCE', NULL, $1, NULL, NULL, 3),
         ('OUTCOME', 'APPLIED', $1, $2::uuid, NULL, NULL)
     ) AS r(row_kind, disposition, workflow_id, advance_child_workflow_id, detail, task_index)",
    &[ParameterType::Uuid, ParameterType::Uuid],
);

/// Two `OUTCOME` rows.
static STEP_ROWS_TWO_OUTCOMES: TypedStatement = TypedStatement::new(
    "test_step_rows_two_outcomes",
    "SELECT * FROM (VALUES
         ('OUTCOME'::text, 'APPLIED'::text, NULL::uuid, NULL::uuid, NULL::text, NULL::integer),
         ('OUTCOME', 'ALREADY_APPLIED', NULL, NULL, NULL, NULL)
     ) AS r(row_kind, disposition, workflow_id, advance_child_workflow_id, detail, task_index)",
    &[],
);

/// One row of the given kind and disposition, with a detail.
static STEP_ROW_OF_KIND: TypedStatement = TypedStatement::new(
    "test_step_row_of_kind",
    "SELECT $1::text AS row_kind, $2::text AS disposition, NULL::uuid AS workflow_id,
            NULL::uuid AS advance_child_workflow_id, 'observed'::text AS detail,
            NULL::integer AS task_index",
    &[ParameterType::Text, ParameterType::Text],
);

async fn step_row_of_kind(broker: &PostgresBroker, row_kind: &str, disposition: &str) -> Vec<Row> {
    let row_kind = row_kind.to_owned();
    let disposition = disposition.to_owned();
    rows_of(
        broker,
        &STEP_ROW_OF_KIND
            .bind()
            .value(&row_kind)
            .value(&disposition)
            .finish(),
    )
    .await
}

#[tokio::test]
#[serial]
async fn step_rows_read_the_outcome_and_the_owed_child_starts_in_any_order() {
    let broker = test_broker().await;
    let workflow_id = Uuid::new_v4();
    let ended_child = Uuid::new_v4();
    let rows = rows_of(
        &broker,
        &STEP_ROWS_WITH_CHILD_START
            .bind()
            .value(&workflow_id)
            .value(&ended_child)
            .finish(),
    )
    .await;

    let step =
        step_rows::<NodeFinishDisposition>(&rows, &STEP_ROWS_WITH_CHILD_START).expect("step rows");
    assert_eq!(
        step,
        StepRows {
            outcome: StepOutcome {
                disposition: NodeFinishDisposition::Applied,
                parent_advance: ParentAdvance::Owed {
                    child_workflow_id: ended_child,
                },
                detail: None,
            },
            owed_child_starts: vec![OwedChildStart {
                workflow_id,
                task_index: 3,
            }],
        }
    );
}

#[tokio::test]
#[serial]
async fn an_outcome_row_alone_owes_no_child_start_and_keeps_its_detail() {
    let broker = test_broker().await;
    let rows = step_row_of_kind(&broker, "OUTCOME", "NOT_PAUSED").await;

    let step = step_rows::<ChildFinishDisposition>(&rows, &STEP_ROW_OF_KIND);
    assert!(
        matches!(step, Err(StepContractError::UnknownOutcome { .. })),
        "NOT_PAUSED is not a child-finish disposition: {step:?}"
    );
    let rows = step_row_of_kind(&broker, "OUTCOME", "CHILD_NOT_TERMINAL").await;
    let step = step_rows::<ChildFinishDisposition>(&rows, &STEP_ROW_OF_KIND).expect("step rows");
    assert_eq!(
        step.outcome,
        StepOutcome {
            disposition: ChildFinishDisposition::ChildNotTerminal,
            parent_advance: ParentAdvance::NotOwed,
            detail: Some("observed".to_owned()),
        }
    );
    assert!(step.owed_child_starts.is_empty());
}

#[tokio::test]
#[serial]
async fn step_rows_outside_the_contract_are_refused() {
    let broker = test_broker().await;

    let two = rows_of(&broker, &STEP_ROWS_TWO_OUTCOMES.bind().finish()).await;
    assert!(matches!(
        step_rows::<NodeFinishDisposition>(&two, &STEP_ROWS_TWO_OUTCOMES),
        Err(StepContractError::OutcomeRows { count: 2, .. })
    ));
    assert!(matches!(
        step_rows::<NodeFinishDisposition>(&[], &NODE_FINISH),
        Err(StepContractError::OutcomeRows { count: 0, .. })
    ));
    let unknown_kind = step_row_of_kind(&broker, "NOTE", "APPLIED").await;
    assert!(matches!(
        step_rows::<NodeFinishDisposition>(&unknown_kind, &STEP_ROW_OF_KIND),
        Err(StepContractError::UnknownRowKind { .. })
    ));
}

#[test]
fn every_node_finish_disposition_text_reads_and_others_do_not() {
    let cases = [
        ("APPLIED", NodeFinishDisposition::Applied),
        (
            "APPLIED_WORKFLOW_ENDED",
            NodeFinishDisposition::AppliedWorkflowEnded,
        ),
        ("ALREADY_APPLIED", NodeFinishDisposition::AlreadyApplied),
        ("PENDING_ABSENT", NodeFinishDisposition::PendingAbsent),
        (
            "SOURCE_ABSENT",
            NodeFinishDisposition::Retained(SourceFailure::Absent),
        ),
        (
            "SOURCE_STATE_CONFLICT",
            NodeFinishDisposition::Retained(SourceFailure::StateConflict),
        ),
        (
            "SOURCE_VERSION_CONFLICT",
            NodeFinishDisposition::Retained(SourceFailure::VersionConflict),
        ),
        (
            "SOURCE_DIGEST_MISMATCH",
            NodeFinishDisposition::Retained(SourceFailure::DigestMismatch),
        ),
    ];
    for (text, disposition) in cases {
        assert_eq!(NodeFinishDisposition::from_text(text), Some(disposition));
        if let NodeFinishDisposition::Retained(failure) = disposition {
            assert_eq!(failure.as_str(), text);
        }
    }
    assert_eq!(NodeFinishDisposition::from_text("applied"), None);
    assert_eq!(NodeFinishDisposition::from_text("SUPERSEDED"), None);
}

#[test]
fn every_child_finish_disposition_text_reads_and_others_do_not() {
    let cases = [
        ("APPLIED", ChildFinishDisposition::Applied),
        ("ALREADY_APPLIED", ChildFinishDisposition::AlreadyApplied),
        ("CHILD_ABSENT", ChildFinishDisposition::ChildAbsent),
        ("NOT_A_CHILD", ChildFinishDisposition::NotAChild),
        (
            "CHILD_NOT_TERMINAL",
            ChildFinishDisposition::ChildNotTerminal,
        ),
        ("PARENT_ABSENT", ChildFinishDisposition::ParentAbsent),
    ];
    for (text, disposition) in cases {
        assert_eq!(ChildFinishDisposition::from_text(text), Some(disposition));
    }
    assert_eq!(ChildFinishDisposition::from_text("PENDING_ABSENT"), None);
}

#[test]
fn the_dependents_choice_names_the_function_values() {
    assert_eq!(Dependents::NoneToRead.as_sql(), "NONE");
    assert_eq!(Dependents::Read.as_sql(), "READ");
}

#[test]
fn a_chain_of_parent_advances_is_bounded_by_the_tree_depth() {
    // Depth 0 to 32: a chain from the deepest child advances 32 parents.
    assert_eq!(
        MAX_PARENT_ADVANCES,
        (crate::workflow_engine::start::MAX_SUBWORKFLOW_DEPTH + 1) as usize
    );
}

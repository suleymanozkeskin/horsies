//! Ports of the Python tests of the move to history: rerun input
//! dispositions, refusals that keep the live row, the stale probe's recency
//! bound and the workflow scope of the sweeps.
//!
//! Python specification: `tests/integration/test_task_history_terminalization_*.py`,
//! `test_task_history_heartbeat_partitions.py` and the core rows of
//! `test_task_history_terminalization_prototypes.py` and
//! `test_task_history_identity_prototypes.py`. Each test runs in one
//! transaction that it rolls back, on the shared migrated database. A move
//! that must be refused runs inside a savepoint, so the test can read the
//! live state after the refusal.

use chrono::{DateTime, Duration, Utc};
use serial_test::serial;
use sqlx::{Acquire, PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::broker::terminalization::terminalize_in_tx;
use crate::broker::BrokerError;
use crate::core::history::ddl::classes::register_period_class;
use crate::core::history::heartbeats::partitioning::{
    create_hourly_heartbeat_leaf, hourly_leaf_ref, CreateHourlyHeartbeatLeaf,
};
use crate::core::history::identity::reservations::{claim_key_reservation, ReservationClaim};
use crate::core::history::periods::RetentionDays;
use crate::core::lifecycle::{
    OwnedClaim, PriorLockedRead, TerminalizationCommand, TerminalizationOutcome,
};

const PORT_WORKER: &str = "port-move-worker";
const CLASS_KEY: &str = "standard_30d";
const FAILURE_RESULT: &str = "{\"Err\":{}}";
const SUCCESS_RESULT: &str = "{\"Ok\":true}";

async fn shared_pool() -> PgPool {
    crate::broker::terminalization_matrix::migrated_pool().await
}

/// The live row of one test task. Every baseline column is named.
struct LiveTask {
    status: &'static str,
    class_key: &'static str,
    workflow_task: bool,
    retain_rerun_input: bool,
    started_minutes_ago: i64,
    good_until_minutes: Option<i64>,
    idempotency_key_digest: Option<Vec<u8>>,
}

impl LiveTask {
    fn running() -> Self {
        Self {
            status: "RUNNING",
            class_key: CLASS_KEY,
            workflow_task: false,
            retain_rerun_input: false,
            started_minutes_ago: 1,
            good_until_minutes: None,
            idempotency_key_digest: None,
        }
    }
}

async fn seed_task(transaction: &mut Transaction<'_, Postgres>, task: &LiveTask) -> Uuid {
    let task_id = Uuid::new_v4();
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
         ) VALUES (
             $1, 'port.move', 'default', 100, '{\"x\":1}', NULL, 'ISOLATED',
             $2, NOW() - make_interval(mins => $5), NOW() - make_interval(mins => $5),
             CASE WHEN $3 THEN NULL ELSE 1 END,
             CASE WHEN $3 THEN NULL ELSE decode(repeat('0d', 32), 'hex') END,
             $8,
             $4, NOW() - make_interval(mins => $5),
             CASE WHEN $4 IN ('CLAIMED', 'RUNNING') THEN NOW() - make_interval(mins => $5) END,
             CASE WHEN $4 IN ('CLAIMED', 'RUNNING') THEN $9 END,
             CASE WHEN $4 = 'CLAIMED' THEN NOW() + interval '1 hour' END,
             CASE WHEN $4 = 'RUNNING' THEN NOW() - make_interval(mins => $5) END,
             NULL, NULL,
             CASE WHEN $4 = 'RUNNING' THEN 13 END,
             CASE WHEN $4 = 'RUNNING' THEN 'port-host' END,
             CASE WHEN $4 = 'RUNNING' THEN 'port-process' END,
             0, 0, NULL,
             CASE WHEN $7::bigint IS NULL THEN NULL
                  ELSE NOW() + make_interval(mins => $7::integer) END,
             $3,
             NOW(), $6, NULL, NULL
         )",
    )
    .bind(task_id)
    .bind(task.class_key)
    .bind(task.workflow_task)
    .bind(task.status)
    .bind(i32::try_from(task.started_minutes_ago).expect("minutes fit"))
    .bind(task.retain_rerun_input)
    .bind(task.good_until_minutes)
    .bind(task.idempotency_key_digest.as_deref())
    .bind(PORT_WORKER)
    .execute(transaction.as_mut())
    .await
    .expect("insert live task");
    task_id
}

async fn seed_workflow(transaction: &mut Transaction<'_, Postgres>, status: &str) -> Uuid {
    let workflow_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_workflows (
             id, name, definition_key, status, on_error, output_task_index,
             success_policy, result, error, parent_workflow_id, parent_task_index,
             root_workflow_id, depth, sent_at, created_at, started_at, completed_at,
             updated_at
         ) VALUES (
             $1, 'port_move', NULL, $2, 'fail', NULL,
             NULL, NULL, NULL, NULL, NULL,
             $1, 0, NOW(), NOW(), NOW(), NULL,
             NOW()
         )",
    )
    .bind(workflow_id)
    .bind(status)
    .execute(transaction.as_mut())
    .await
    .expect("insert workflow");
    workflow_id
}

async fn seed_node(
    transaction: &mut Transaction<'_, Postgres>,
    workflow_id: Uuid,
    task_index: i32,
    status: &str,
    task_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
             dependencies, args_from, workflow_ctx_from, allow_failed_deps, join_type,
             min_success, task_options, max_retries, execution, good_until,
             retention_class_key, status, task_id, is_subworkflow, sub_workflow_id,
             sub_workflow_name, sub_definition_key, sub_workflow_summary, result, error,
             created_at, started_at, completed_at
         ) VALUES (
             $1, $2, $3, NULL, 'port.move', '{}', 'default', 100,
             '{}', NULL, NULL, FALSE, 'all',
             NULL, NULL, 0, 'ISOLATED', NULL,
             $6, $4, $5, FALSE, NULL,
             NULL, NULL, NULL, NULL, NULL,
             NOW(), NULL, NULL
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(task_index)
    .bind(status)
    .bind(task_id)
    .bind(CLASS_KEY)
    .execute(transaction.as_mut())
    .await
    .map(|_| ())
}

fn complete(task_id: Uuid) -> TerminalizationCommand {
    TerminalizationCommand::CompleteLockedTask {
        task_id,
        fence: PriorLockedRead {
            worker_id: PORT_WORKER.to_owned(),
        },
        result_json: SUCCESS_RESULT.to_owned(),
    }
}

fn fail(task_id: Uuid) -> TerminalizationCommand {
    TerminalizationCommand::FailLockedTask {
        task_id,
        fence: PriorLockedRead {
            worker_id: PORT_WORKER.to_owned(),
        },
        result_json: FAILURE_RESULT.to_owned(),
        error_code: Some("TASK_ERROR".to_owned()),
        failed_reason: Some("port failure".to_owned()),
    }
}

async fn applied(transaction: &mut Transaction<'_, Postgres>, command: &TerminalizationCommand) {
    let outcomes = terminalize_in_tx(transaction, command)
        .await
        .expect("terminal write");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{outcomes:?}"
    );
}

/// Run `command` in a savepoint that the database refuses, roll the
/// savepoint back and return the SQLSTATE.
async fn refused_sqlstate(
    transaction: &mut Transaction<'_, Postgres>,
    command: &TerminalizationCommand,
) -> String {
    let mut savepoint = transaction.begin().await.expect("begin savepoint");
    let refused = terminalize_in_tx(&mut savepoint, command)
        .await
        .expect_err("the database refuses the move");
    savepoint.rollback().await.expect("roll back the savepoint");
    match refused {
        BrokerError::Database(error) => error
            .as_database_error()
            .and_then(|database_error| database_error.code())
            .map(|code| code.into_owned())
            .unwrap_or_else(|| panic!("no SQLSTATE: {error}")),
        other => panic!("expected a database refusal, got {other:?}"),
    }
}

/// Run `sql` with `task_id` and `PORT_WORKER` in a savepoint that the
/// database refuses, and return the SQLSTATE.
async fn refused_sql_sqlstate(
    transaction: &mut Transaction<'_, Postgres>,
    sql: &str,
    task_id: Uuid,
) -> String {
    let mut savepoint = transaction.begin().await.expect("begin savepoint");
    let refused = sqlx::query(sql)
        .bind(task_id)
        .bind(PORT_WORKER)
        .execute(savepoint.as_mut())
        .await
        .expect_err("the database refuses the call");
    savepoint.rollback().await.expect("roll back the savepoint");
    refused
        .as_database_error()
        .and_then(|database_error| database_error.code())
        .map(|code| code.into_owned())
        .unwrap_or_else(|| panic!("no SQLSTATE: {refused}"))
}

async fn is_live(transaction: &mut Transaction<'_, Postgres>, task_id: Uuid) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM horsies_tasks WHERE id = $1)")
        .bind(task_id)
        .fetch_one(transaction.as_mut())
        .await
        .expect("read live row")
}

/// `(rerun_input_disposition, rerun_input_version IS NULL AND digest IS NULL
/// AND inline IS NULL)` of the history row.
async fn rerun_disposition(
    transaction: &mut Transaction<'_, Postgres>,
    task_id: Uuid,
) -> (String, bool) {
    sqlx::query_as(
        "SELECT rerun_input_disposition,
                rerun_input_version IS NULL AND rerun_input_digest IS NULL
                    AND rerun_input_inline IS NULL
         FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(transaction.as_mut())
    .await
    .expect("read history rerun columns")
}

/// `TestDispositionLadder.test_declined_snapshot_archives_declined_by_policy`.
#[tokio::test]
#[serial]
async fn a_failure_that_declined_input_archives_declined_by_policy() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let task_id = seed_task(&mut transaction, &LiveTask::running()).await;
    applied(&mut transaction, &fail(task_id)).await;
    assert_eq!(
        rerun_disposition(&mut transaction, task_id).await,
        ("DECLINED_BY_POLICY".to_owned(), true)
    );
    transaction.rollback().await.expect("rollback");
}

/// `TestAppliedCompletion.test_completed_is_never_eligible_even_with_prepared_inline`.
#[tokio::test]
#[serial]
async fn a_completion_is_never_eligible_even_when_the_input_was_kept() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let task_id = seed_task(
        &mut transaction,
        &LiveTask {
            retain_rerun_input: true,
            ..LiveTask::running()
        },
    )
    .await;
    applied(&mut transaction, &complete(task_id)).await;
    assert_eq!(
        rerun_disposition(&mut transaction, task_id).await,
        ("NEVER_ELIGIBLE".to_owned(), true)
    );
    transaction.rollback().await.expect("rollback");
}

/// `TestDispositionLadder.test_workflow_backing_failure_is_never_eligible_with_pending`.
#[tokio::test]
#[serial]
async fn a_workflow_failure_is_never_eligible_and_leaves_a_pending_row() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let workflow_id = seed_workflow(&mut transaction, "RUNNING").await;
    let task_id = seed_task(
        &mut transaction,
        &LiveTask {
            workflow_task: true,
            retain_rerun_input: true,
            ..LiveTask::running()
        },
    )
    .await;
    seed_node(&mut transaction, workflow_id, 0, "RUNNING", task_id)
        .await
        .expect("link the node");
    applied(&mut transaction, &fail(task_id)).await;
    assert_eq!(
        rerun_disposition(&mut transaction, task_id).await,
        ("NEVER_ELIGIBLE".to_owned(), true)
    );
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(transaction.as_mut())
    .await
    .expect("count pending rows");
    assert_eq!(pending, 1);
    transaction.rollback().await.expect("rollback");
}

/// `TestOrphanSingle.test_links_to_distinct_workflows_fail_closed`: on the
/// baseline a task backs at most one node, so the second link is refused
/// when it is written, not at the move.
#[tokio::test]
#[serial]
async fn a_second_node_link_to_one_task_is_refused() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let task_id = seed_task(
        &mut transaction,
        &LiveTask {
            workflow_task: true,
            ..LiveTask::running()
        },
    )
    .await;
    let first = seed_workflow(&mut transaction, "RUNNING").await;
    let second = seed_workflow(&mut transaction, "RUNNING").await;
    seed_node(&mut transaction, first, 0, "RUNNING", task_id)
        .await
        .expect("link the first node");
    let refused = seed_node(&mut transaction, second, 0, "RUNNING", task_id)
        .await
        .expect_err("a second link is refused");
    assert_eq!(
        refused
            .as_database_error()
            .and_then(|error| error.constraint()),
        Some("ix_horsies_workflow_tasks_task_id")
    );
    transaction.rollback().await.expect("rollback");
}

/// `TestMoveInvariants.test_duplicate_identity_fails_closed`: a task id that
/// is in history already cannot move a second time; the live row stays.
#[tokio::test]
#[serial]
async fn a_task_in_both_live_and_history_fails_closed() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let task_id = seed_task(&mut transaction, &LiveTask::running()).await;
    // Put a history row of the same id in place first, by moving a copy.
    sqlx::query(
        "INSERT INTO horsies_task_history (
             task_id, task_name, queue_name, priority, command_fingerprint_version,
             command_fingerprint, retention_class_key, is_workflow_task, workflow_id,
             rerun_of_task_id, rerun_root_task_id, sent_at, created_at, enqueued_at,
             claimed_at, started_at, good_until, terminal_at, status, terminalization_kind,
             retry_count, max_retries, last_claimed_worker_id, last_worker_hostname,
             last_worker_pid, last_worker_process_name, error_code, final_failed_reason,
             result_envelope_version, result_payload, result_digest, history_schema_version,
             attempt_archive_version, attempt_snapshot, attempt_snapshot_digest,
             rerun_input_disposition, rerun_input_version, rerun_input_digest,
             rerun_input_inline
         )
         SELECT t.id, t.task_name, t.queue_name, t.priority, t.command_fingerprint_version,
                t.command_fingerprint, t.retention_class_key, t.is_workflow_task, NULL,
                NULL, NULL, t.sent_at, t.created_at, t.enqueued_at,
                t.claimed_at, t.started_at, t.good_until, NOW(), 'COMPLETED', 'COMPLETE_FUSED',
                0, 0, t.claimed_by_worker_id, t.worker_hostname,
                t.worker_pid, t.worker_process_name, NULL, NULL,
                1, NULL, NULL, 1,
                1, horsies_encode_task_attempts(t.id),
                sha256(horsies_encode_task_attempts(t.id)),
                'NEVER_ELIGIBLE', NULL, NULL, NULL
         FROM horsies_tasks AS t WHERE t.id = $1",
    )
    .bind(task_id)
    .execute(transaction.as_mut())
    .await
    .expect("plant a history row of the live id");

    assert_eq!(
        refused_sqlstate(&mut transaction, &complete(task_id)).await,
        "XX001"
    );
    assert!(is_live(&mut transaction, task_id).await);
    let history_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .fetch_one(transaction.as_mut())
            .await
            .expect("count history rows");
    assert_eq!(history_rows, 1);
    transaction.rollback().await.expect("rollback");
}

/// `TestMoveInvariants.test_deferred_terminalization_requires_a_result_payload`
/// and `TestDeferredResultFence.test_workflow_failure_without_result_raises_typed`.
/// Every terminal command of the core carries `result_json`, so the fence is
/// reached through the SQL writers: a workflow task's locked completion and
/// locked failure with no result are refused, and the task stays live.
#[tokio::test]
#[serial]
async fn a_deferred_move_without_a_result_payload_is_refused() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let workflow_id = seed_workflow(&mut transaction, "RUNNING").await;
    let task_id = seed_task(
        &mut transaction,
        &LiveTask {
            workflow_task: true,
            ..LiveTask::running()
        },
    )
    .await;
    seed_node(&mut transaction, workflow_id, 0, "RUNNING", task_id)
        .await
        .expect("link the node");
    for sql in [
        "SELECT * FROM horsies_complete_locked_task($1, $2, NULL)",
        "SELECT * FROM horsies_fail_locked_task($1, $2, NULL, 'TASK_ERROR', 'port failure')",
    ] {
        assert_eq!(
            refused_sql_sqlstate(&mut transaction, sql, task_id).await,
            "23502",
            "{sql}"
        );
        assert!(is_live(&mut transaction, task_id).await);
    }
    transaction.rollback().await.expect("rollback");
}

/// `TestDiscoveryBatch.test_batch_deferred_fence_raises_before_any_move`: a
/// due workflow row with no result payload refuses the whole expiry batch, so
/// the due plain row stays live too.
#[tokio::test]
#[serial]
async fn an_expiry_batch_with_a_deferred_row_and_no_result_is_refused_before_any_move() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let workflow_id = seed_workflow(&mut transaction, "RUNNING").await;
    let due = LiveTask {
        status: "PENDING",
        good_until_minutes: Some(-5),
        ..LiveTask::running()
    };
    let plain = seed_task(&mut transaction, &due).await;
    let workflow_task = seed_task(
        &mut transaction,
        &LiveTask {
            workflow_task: true,
            ..due
        },
    )
    .await;
    seed_node(&mut transaction, workflow_id, 0, "ENQUEUED", workflow_task)
        .await
        .expect("link the node");
    let mut savepoint = transaction.begin().await.expect("begin savepoint");
    let refused =
        sqlx::query("SELECT * FROM horsies_expire_pending_tasks(10000, NULL, 'TASK_EXPIRED')")
            .execute(savepoint.as_mut())
            .await
            .expect_err("the batch is refused");
    savepoint.rollback().await.expect("roll back the savepoint");
    assert_eq!(
        refused
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("23502")
    );
    assert!(is_live(&mut transaction, plain).await);
    assert!(is_live(&mut transaction, workflow_task).await);
    transaction.rollback().await.expect("rollback");
}

/// `TestWorkflowStateMatrix.test_sweeps_respect_the_workflow_scope`: the
/// paused-workflow sweep moves the claimed tasks of the listed workflow only.
#[tokio::test]
#[serial]
async fn the_paused_sweep_touches_only_the_listed_workflows() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let claimed = LiveTask {
        status: "CLAIMED",
        workflow_task: true,
        ..LiveTask::running()
    };
    let listed = seed_workflow(&mut transaction, "PAUSED").await;
    let unlisted = seed_workflow(&mut transaction, "PAUSED").await;
    let listed_task = seed_task(&mut transaction, &claimed).await;
    let unlisted_task = seed_task(&mut transaction, &claimed).await;
    seed_node(&mut transaction, listed, 0, "ENQUEUED", listed_task)
        .await
        .expect("link the listed node");
    seed_node(&mut transaction, unlisted, 0, "ENQUEUED", unlisted_task)
        .await
        .expect("link the unlisted node");
    let outcomes = terminalize_in_tx(
        &mut transaction,
        &TerminalizationCommand::AbandonNodesOfPausedWorkflows {
            workflow_ids: vec![listed],
        },
    )
    .await
    .expect("sweep");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { task_id, .. }] if *task_id == listed_task
        ),
        "{outcomes:?}"
    );
    assert!(!is_live(&mut transaction, listed_task).await);
    assert!(is_live(&mut transaction, unlisted_task).await);
    transaction.rollback().await.expect("rollback");
}

/// `TestStaleProbeOnPartitionedStorage.test_heartbeat_beyond_the_recency_bound_cannot_flip_verdict`:
/// a runner beat older than every threshold gives the verdict of no beat:
/// the stale failure applies.
#[tokio::test]
#[serial]
async fn a_heartbeat_beyond_the_recency_bound_cannot_flip_the_verdict() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT statement_timestamp()")
        .fetch_one(transaction.as_mut())
        .await
        .expect("database now");
    let beat_at = now - Duration::minutes(80);
    let beat_hour = beat_at
        .date_naive()
        .and_hms_opt(chrono::Timelike::hour(&beat_at), 0, 0)
        .expect("hour start")
        .and_utc();
    let leaf = hourly_leaf_ref(beat_hour).expect("beat hour leaf");
    let _ = create_hourly_heartbeat_leaf(
        &mut transaction,
        &CreateHourlyHeartbeatLeaf::new(leaf).expect("hourly command"),
    )
    .await
    .expect("the beat's hour has a leaf");
    let task_id = seed_task(
        &mut transaction,
        &LiveTask {
            started_minutes_ago: 90,
            ..LiveTask::running()
        },
    )
    .await;
    sqlx::query(
        "INSERT INTO horsies_heartbeats (task_id, role, sent_at, sender_id, hostname, pid)
         VALUES ($1, 'runner', $2, $3, 'port-host', 13)",
    )
    .bind(task_id)
    .bind(beat_at)
    .bind(PORT_WORKER)
    .execute(transaction.as_mut())
    .await
    .expect("write the old beat");
    applied(
        &mut transaction,
        &TerminalizationCommand::FailStaleTask {
            task_id,
            stale_after_ms: 600_000,
            finalizing_stale_after_ms: 600_000,
            result_json: FAILURE_RESULT.to_owned(),
            error_code: "WORKER_CRASHED".to_owned(),
            failed_reason: "port stale runner".to_owned(),
        },
    )
    .await;
    transaction.rollback().await.expect("rollback");
}

/// `test_fused_completion_rejects_workflow_task_without_losing_phase2`
/// (prototype): the fused completion refuses a workflow task; the task stays
/// live for its deferred path.
#[tokio::test]
#[serial]
async fn the_fused_completion_refuses_a_workflow_task() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let workflow_id = seed_workflow(&mut transaction, "RUNNING").await;
    let task_id = seed_task(
        &mut transaction,
        &LiveTask {
            workflow_task: true,
            ..LiveTask::running()
        },
    )
    .await;
    seed_node(&mut transaction, workflow_id, 0, "RUNNING", task_id)
        .await
        .expect("link the node");
    let claimed_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT claimed_at FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_one(transaction.as_mut())
            .await
            .expect("read the claim time");
    let fused = TerminalizationCommand::CompleteTaskFused {
        task_id,
        fence: OwnedClaim {
            worker_id: PORT_WORKER.to_owned(),
            claimed_at,
        },
        result_json: SUCCESS_RESULT.to_owned(),
        notify_channel: "port_capacity".to_owned(),
        notify_payload: "port".to_owned(),
    };
    assert_eq!(refused_sqlstate(&mut transaction, &fused).await, "22023");
    assert!(is_live(&mut transaction, task_id).await);
    transaction.rollback().await.expect("rollback");
}

/// `test_missing_workflow_link_rolls_back_the_move` (prototype): a workflow
/// task with no node row cannot move; the live row stays.
#[tokio::test]
#[serial]
async fn a_workflow_task_without_its_node_link_does_not_move() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let task_id = seed_task(
        &mut transaction,
        &LiveTask {
            workflow_task: true,
            ..LiveTask::running()
        },
    )
    .await;
    assert_eq!(
        refused_sqlstate(&mut transaction, &complete(task_id)).await,
        "P0002"
    );
    assert!(is_live(&mut transaction, task_id).await);
    transaction.rollback().await.expect("rollback");
}

/// `test_missing_history_leaf_rolls_back_fused_attempt_and_live_delete`
/// (prototype) and `test_missing_history_leaf_rolls_back_location_transition`
/// (identity prototype): a row whose class has no leaf for its terminal time
/// cannot move; the live row stays and its key reservation stays LIVE.
#[tokio::test]
#[serial]
async fn a_move_with_no_history_leaf_keeps_the_task_and_its_reservation_live() {
    let pool = shared_pool().await;
    let mut transaction = pool.begin().await.expect("begin");
    let period = RetentionDays::parse_period(Duration::days(9)).expect("nine days");
    let _ = register_period_class(&mut transaction, period)
        .await
        .expect("register a class with no leaf");
    let key: Vec<u8> = sqlx::query_scalar("SELECT sha256(convert_to($1, 'UTF8'))")
        .bind(format!("port-key-{}", Uuid::new_v4()))
        .fetch_one(transaction.as_mut())
        .await
        .expect("key digest");
    let task_id = seed_task(
        &mut transaction,
        &LiveTask {
            class_key: "standard_9d",
            idempotency_key_digest: Some(key.clone()),
            ..LiveTask::running()
        },
    )
    .await;
    let claimed = claim_key_reservation(&mut transaction, &key, 1, 3_600, 1, &[13_u8; 32], task_id)
        .await
        .expect("reserve the key");
    assert_eq!(claimed, ReservationClaim::Applied { task_id });
    assert_eq!(
        refused_sqlstate(&mut transaction, &complete(task_id)).await,
        "23514"
    );
    assert!(is_live(&mut transaction, task_id).await);
    let disposition: String = sqlx::query_scalar(
        "SELECT disposition FROM horsies_key_reservations WHERE idempotency_key_digest = $1",
    )
    .bind(&key)
    .fetch_one(transaction.as_mut())
    .await
    .expect("read the reservation");
    assert_eq!(disposition, "LIVE");
    transaction.rollback().await.expect("rollback");
}

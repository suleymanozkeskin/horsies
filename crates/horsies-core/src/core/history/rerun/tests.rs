use chrono::{DateTime, Duration, DurationRound, Utc};
use serial_test::serial;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::operations::{
    rerun_task_in_transaction, NotEligibleReason, RerunEnqueuePolicy, RerunError, RerunOutcome,
    RerunTask,
};
use super::provenance::RERUN_FIELD_PROVENANCE;
use crate::broker::client_family::ClientFamily;
use crate::broker::core_rows::StatementLog;
use crate::broker::pools::core_pool_for_test_pool;
use crate::broker::terminalization::on_test_pool::terminalize;
use crate::broker::PostgresBroker;
use crate::core::history::ddl::classes::register_period_class;
use crate::core::history::identity::fingerprint::EnqueueCommandV1;
use crate::core::history::identity::keys::{ScopedIdempotencyKey, IDEMPOTENCY_SCOPE_VERSION};
use crate::core::history::identity::reservations::{claim_key_reservation, ReservationClaim};
use crate::core::history::partitions::publication::LoaderPublication;
use crate::core::history::periods::RetentionDays;
use crate::core::history::reads::detail::TaskAbsenceReason;
use crate::core::history::reads::publisher::StagedLoaderPublisher;
use crate::core::lifecycle::{TerminalizationCommand, TerminalizationOutcome, WorkerOwned};
use crate::core::{OperationalErrorCode, TaskError, TaskResult, TaskStatus};
use crate::db::{
    CheckoutClass, IsolationLevel, ParameterType, RollbackOutcome, SqlScript, StatementBound,
    StatementFailure, TypedStatement,
};
use crate::test_support::terminal_writes::{self, TerminalFailure};

mod ports;

/// Drops the shape check of the rerun input columns, in a test transaction
/// that rolls back, so a row can hold a shape the check refuses.
static DROP_RERUN_INPUT_SHAPE: SqlScript = SqlScript::new(
    "drop_rerun_input_shape",
    "ALTER TABLE horsies_task_history
     DROP CONSTRAINT horsies_task_history_rerun_input_shape",
);

/// An INLINE input with a payload that does not decode, on each history row
/// of `$1`.
static STORE_CORRUPT_INLINE_INPUT: TypedStatement = TypedStatement::new(
    "store_corrupt_inline_input",
    "UPDATE horsies_task_history
     SET rerun_input_disposition = 'INLINE', rerun_input_version = 1,
         rerun_input_digest = decode(repeat('00', 32), 'hex'),
         rerun_input_inline = 'corrupt'::bytea
     WHERE task_id = ANY($1)",
    &[ParameterType::UuidArray],
);

/// An INLINE disposition with no envelope version on the history row `$1`,
/// with the digest `$2` and the payload `$3`.
static STORE_INCOMPLETE_INLINE_INPUT: TypedStatement = TypedStatement::new(
    "store_incomplete_inline_input",
    "UPDATE horsies_task_history
     SET rerun_input_disposition = 'INLINE', rerun_input_version = NULL,
         rerun_input_digest = $2, rerun_input_inline = $3
     WHERE task_id = $1",
    &[
        ParameterType::Uuid,
        ParameterType::Bytea,
        ParameterType::Bytea,
    ],
);

/// The record of a period-class leaf that retention removed: a detached and
/// dropped catalog row of `standard_30d` that ended a day ago.
static RECORD_REMOVED_LEAF: SqlScript = SqlScript::new(
    "record_removed_leaf",
    "INSERT INTO horsies_task_history_leaf_catalog (
         leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor,
         index_schema_version, created_at, detached_at, dropped_at
     ) VALUES (
         'p8_removed_standard_30d_leaf', 'standard_30d', 'AT',
         date_trunc('day', NOW(), 'UTC') - interval '2 days',
         date_trunc('day', NOW(), 'UTC') - interval '1 day',
         1, NOW(), NOW(), NOW()
     )",
);

/// Drops the staged detail reader that the rerun source read calls.
static DROP_DETAIL_READER: SqlScript = SqlScript::new(
    "drop_detail_reader",
    "DROP FUNCTION horsies_task_detail_staged(uuid)",
);

/// Whether a test transaction rolled back and kept its connection.
fn rolled_back(outcome: RollbackOutcome) -> bool {
    matches!(outcome, RollbackOutcome::RolledBack)
}

const WORKER: &str = "p8-rerun-worker";
const SOURCE_CLASS: &str = "forever";
const CURRENT_POLICY_CLASS: &str = "standard_14d";
const PAYLOAD_INPUT: &str = "{\"k\":\"v\"}";
const PAYLOAD_OPTIONS: &str = "{\"timeout_ms\":5}";

/// The stored envelope of a raw result record, parsed for a comparison.
fn envelope_value(record: &crate::broker::RawResultRecord) -> Option<serde_json::Value> {
    record
        .raw_result
        .as_ref()
        .map(|text| serde_json::from_str(text.as_str()).expect("a stored envelope text is JSON"))
}

fn digest_hex(payload: &[u8]) -> String {
    Sha256::digest(payload)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Clone, Copy)]
enum TerminalSource {
    Failed,
    Cancelled,
    Expired,
    Completed,
}

fn policy(class_key: &str, retain: bool) -> RerunEnqueuePolicy {
    RerunEnqueuePolicy::new(class_key, retain, Duration::hours(24)).unwrap()
}

fn terminal_result(kind: TerminalSource) -> String {
    match kind {
        TerminalSource::Completed => {
            serde_json::to_string(&TaskResult::Ok(serde_json::json!({"value": 7}))).unwrap()
        }
        TerminalSource::Failed | TerminalSource::Expired | TerminalSource::Cancelled => {
            serde_json::to_string(&TaskResult::<serde_json::Value>::Err(TaskError::builtin(
                OperationalErrorCode::TaskException,
                "p8 terminal source",
            )))
            .unwrap()
        }
    }
}

async fn seed_source(
    pool: &PgPool,
    kind: TerminalSource,
    workflow_task: bool,
    rerun_root: Option<Uuid>,
) -> (Uuid, Option<Uuid>) {
    let task_id = Uuid::new_v4();
    let (status, worker, started_at, claimed_at, good_until) = match kind {
        TerminalSource::Failed | TerminalSource::Completed => (
            "RUNNING",
            Some(WORKER),
            Some(Utc::now()),
            Some(Utc::now()),
            None,
        ),
        TerminalSource::Cancelled => ("PENDING", None, None, None, None),
        TerminalSource::Expired => (
            "CLAIMED",
            Some(WORKER),
            None,
            Some(Utc::now()),
            Some(Utc::now() - Duration::minutes(1)),
        ),
    };
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, task_options, execution,
             status, sent_at, enqueued_at, started_at, claimed_at,
             claimed_by_worker_id, claim_expires_at, good_until, is_workflow_task,
             worker_pid, worker_hostname, worker_process_name,
             retry_count, max_retries,
             command_fingerprint_version, command_fingerprint,
             retention_class_key, rerun_of_task_id,
             rerun_root_task_id, retain_rerun_input,
             created_at, updated_at
         ) VALUES (
             $1, 'p8.rerun', 'default', 50, $2, $3, 'ISOLATED',
             $4, NOW(), NOW(), $5, $6, $7,
             CASE WHEN $4 = 'CLAIMED' THEN NOW() + interval '1 hour' END, $8, $9,
             CASE WHEN $4 = 'RUNNING' THEN 81 END,
             CASE WHEN $4 = 'RUNNING' THEN 'p8-host' END,
             CASE WHEN $4 = 'RUNNING' THEN 'p8-process' END,
             0, 3, CASE WHEN $9 THEN NULL ELSE 1 END,
             CASE WHEN $9 THEN NULL ELSE $10 END, $11, $12, $13, TRUE,
             NOW(), NOW()
         )",
    )
    .bind(task_id)
    .bind(PAYLOAD_INPUT)
    .bind(PAYLOAD_OPTIONS)
    .bind(status)
    .bind(started_at)
    .bind(claimed_at)
    .bind(worker)
    .bind(good_until)
    .bind(workflow_task)
    .bind(vec![9_u8; 32])
    .bind(SOURCE_CLASS)
    .bind(rerun_root.map(|_| Uuid::new_v4()))
    .bind(rerun_root)
    .execute(pool)
    .await
    .unwrap();

    let workflow_id = if workflow_task {
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, definition_key, depth,
                 root_workflow_id, sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'p8_rerun', 'RUNNING', 'fail', $2, 0, $1,
                       NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .bind(format!("test.p8.rerun.{workflow_id}"))
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                 downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                 queue_name, priority, dependencies,
                 allow_failed_deps, join_type, status, is_subworkflow,
                 max_retries, execution, retention_class_key,
                 task_id, created_at
             ) VALUES ('NONE', $1, $2, 0, 'root', 'p8.rerun', $3, 'default',
                       50, '{}', FALSE, 'all', 'RUNNING', FALSE,
                       3, 'ISOLATED', 'forever', $4, NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .bind(PAYLOAD_INPUT)
        .bind(task_id)
        .execute(pool)
        .await
        .unwrap();
        Some(workflow_id)
    } else {
        None
    };

    // The cancel and the expiry write no attempt row: the attempt they
    // archive is an earlier one. The failure and the success write the row of
    // their own attempt.
    match kind {
        TerminalSource::Cancelled | TerminalSource::Expired => {
            sqlx::query(
                "INSERT INTO horsies_task_attempts (
                     task_id, attempt, outcome, will_retry, started_at, finished_at,
                     error_code, error_message, failed_reason, worker_id,
                     worker_hostname, worker_pid, worker_process_name
                 ) VALUES ($1, 1, 'FAILED', FALSE, NOW() - interval '1 second', NOW(),
                           'P8', 'p8 attempt', 'p8 failure', $2, 'p8-host', 81,
                           'p8-process')",
            )
            .bind(task_id)
            .bind(WORKER)
            .execute(pool)
            .await
            .unwrap();
        }
        TerminalSource::Failed | TerminalSource::Completed => {}
    }

    let result_json = terminal_result(kind);
    let outcomes = match (kind, workflow_task) {
        (TerminalSource::Failed, _) => terminal_writes::fail_running_task(
            pool,
            task_id,
            WORKER,
            &TerminalFailure::of_task(&result_json, Some("P8"), Some("p8 failure")),
        )
        .await
        .unwrap(),
        (TerminalSource::Cancelled, _) => terminal_writes::cancel_live_task(pool, task_id, false)
            .await
            .unwrap(),
        (TerminalSource::Expired, _) => terminalize(
            pool,
            &TerminalizationCommand::ExpireOwnedClaim {
                task_id,
                fence: WorkerOwned {
                    worker_id: WORKER.to_owned(),
                },
                result_json,
                error_code: "TASK_EXPIRED".to_owned(),
            },
        )
        .await
        .unwrap(),
        (TerminalSource::Completed, false) => {
            terminal_writes::complete_plain_task(pool, task_id, WORKER, &result_json)
                .await
                .unwrap()
        }
        (TerminalSource::Completed, true) => {
            terminal_writes::complete_workflow_task(pool, task_id, WORKER, &result_json)
                .await
                .unwrap()
        }
    };
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { .. }]
    ));
    (task_id, workflow_id)
}

async fn cleanup(pool: &PgPool, task_ids: &[Uuid], workflow_ids: &[Uuid]) {
    sqlx::query("DELETE FROM horsies_workflow_phase2_pending WHERE task_id = ANY($1)")
        .bind(task_ids)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM horsies_key_reservations WHERE task_id = ANY($1)")
        .bind(task_ids)
        .execute(pool)
        .await
        .ok();
    // The attempt rows first: their foreign key refuses the task delete.
    sqlx::query("DELETE FROM horsies_task_attempts WHERE task_id = ANY($1)")
        .bind(task_ids)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM horsies_tasks WHERE id = ANY($1)")
        .bind(task_ids)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM horsies_task_history WHERE task_id = ANY($1)")
        .bind(task_ids)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = ANY($1)")
        .bind(workflow_ids)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM horsies_workflows WHERE id = ANY($1)")
        .bind(workflow_ids)
        .execute(pool)
        .await
        .ok();
}

#[tokio::test]
#[serial]
async fn failed_cancelled_and_expired_sources_enqueue_fresh_lineage() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let mut registration = pool.acquire().await.unwrap();
    register_period_class(
        &mut registration,
        RetentionDays::parse_period(Duration::days(14)).unwrap(),
    )
    .await
    .unwrap();
    drop(registration);
    let deadline = (Utc::now() + Duration::hours(2))
        .duration_trunc(Duration::microseconds(1))
        .expect("truncate PostgreSQL timestamp expectation to microseconds");
    let mut ids = Vec::new();
    for (index, kind) in [
        TerminalSource::Failed,
        TerminalSource::Cancelled,
        TerminalSource::Expired,
    ]
    .into_iter()
    .enumerate()
    {
        let target_class = if index == 0 {
            CURRENT_POLICY_CLASS
        } else {
            SOURCE_CLASS
        };
        let retain_rerun_input = index != 2;
        let (source, _) = seed_source(&pool, kind, false, None).await;
        let before: serde_json::Value =
            sqlx::query_scalar("SELECT to_jsonb(h) FROM horsies_task_history h WHERE task_id = $1")
                .bind(source)
                .fetch_one(&pool)
                .await
                .unwrap();
        let outcome = broker
            .rerun_task(
                RerunTask::new(source, Some(deadline), None),
                policy(target_class, retain_rerun_input),
            )
            .await
            .unwrap();
        let RerunOutcome::Enqueued {
            new_task_id,
            source_task_id,
            rerun_root_task_id,
        } = outcome
        else {
            panic!("expected enqueue");
        };
        assert_eq!(source_task_id, source);
        assert_eq!(rerun_root_task_id, source);
        assert_ne!(new_task_id, source);
        assert_eq!(new_task_id.get_version_num(), 7);
        let row = sqlx::query(
            "SELECT task_name, queue_name, priority, input, task_options, execution,
                    status, retry_count, max_retries, good_until,
                    retention_class_key, rerun_of_task_id, rerun_root_task_id,
                    retain_rerun_input, command_fingerprint, claimed_at,
                    claimed_by_worker_id, claim_expires_at, next_retry_at,
                    started_at, finalizing_at, finalizing_by_worker_id, worker_pid,
                    worker_hostname, worker_process_name
             FROM horsies_tasks WHERE id = $1",
        )
        .bind(new_task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("task_name"), "p8.rerun");
        assert_eq!(row.get::<String, _>("queue_name"), "default");
        assert_eq!(row.get::<i32, _>("priority"), 50);
        assert_eq!(row.get::<String, _>("input"), PAYLOAD_INPUT);
        assert_eq!(row.get::<String, _>("task_options"), PAYLOAD_OPTIONS);
        assert_eq!(row.get::<String, _>("execution"), "ISOLATED");
        assert_eq!(row.get::<String, _>("status"), "PENDING");
        assert_eq!(row.get::<i32, _>("retry_count"), 0);
        assert_eq!(row.get::<i32, _>("max_retries"), 3);
        assert_eq!(row.get::<DateTime<Utc>, _>("good_until"), deadline);
        assert_eq!(row.get::<String, _>("retention_class_key"), target_class);
        assert_eq!(row.get::<Uuid, _>("rerun_of_task_id"), source);
        assert_eq!(row.get::<Uuid, _>("rerun_root_task_id"), source);
        assert_eq!(row.get::<bool, _>("retain_rerun_input"), retain_rerun_input);
        let expected_fingerprint = EnqueueCommandV1::new(
            "p8.rerun",
            "default",
            50,
            None,
            Some(PAYLOAD_INPUT.to_owned()),
            Some(deadline),
            None,
            Some(PAYLOAD_OPTIONS.to_owned()),
            target_class,
            retain_rerun_input,
            Some(source),
            Some(source),
        )
        .unwrap()
        .fingerprint()
        .unwrap();
        assert_eq!(
            row.get::<Vec<u8>, _>("command_fingerprint"),
            expected_fingerprint
        );
        for field in [
            "claimed_at",
            "claim_expires_at",
            "next_retry_at",
            "started_at",
            "finalizing_at",
        ] {
            assert!(
                row.get::<Option<DateTime<Utc>>, _>(field).is_none(),
                "{field}"
            );
        }
        for field in [
            "claimed_by_worker_id",
            "finalizing_by_worker_id",
            "worker_hostname",
            "worker_process_name",
        ] {
            assert!(row.get::<Option<String>, _>(field).is_none(), "{field}");
        }
        assert!(row.get::<Option<i32>, _>("worker_pid").is_none());
        let after: serde_json::Value =
            sqlx::query_scalar("SELECT to_jsonb(h) FROM horsies_task_history h WHERE task_id = $1")
                .bind(source)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(after, before, "rerun must not mutate the source record");
        ids.extend([source, new_task_id]);
    }
    cleanup(&pool, &ids, &[]).await;
}

#[tokio::test]
#[serial]
async fn root_lineage_is_preserved_across_multiple_reruns() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let original = Uuid::new_v4();
    let (source, _) = seed_source(&pool, TerminalSource::Failed, false, Some(original)).await;
    let outcome = broker
        .rerun_task(
            RerunTask::new(source, None, None),
            policy(SOURCE_CLASS, true),
        )
        .await
        .unwrap();
    let RerunOutcome::Enqueued {
        new_task_id,
        rerun_root_task_id,
        ..
    } = outcome
    else {
        panic!("expected enqueue");
    };
    assert_eq!(rerun_root_task_id, original);
    cleanup(&pool, &[source, new_task_id], &[]).await;
}

#[tokio::test]
#[serial]
async fn python_float_exponent_bytes_drive_rerun_input_and_fingerprint() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let (source, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    let python_payload = br#"{"input":{"v":1e-06},"options":{"nested":[1e+20,0.0001,-0.0]}}"#;
    assert_eq!(
        digest_hex(python_payload),
        "b1f1429240dc21f6584df5cd92073ea0b620826459b8b2624cd54f7bb74b6a4e"
    );
    sqlx::query(
        "UPDATE horsies_task_history
         SET rerun_input_inline = $2, rerun_input_digest = sha256($2)
         WHERE task_id = $1",
    )
    .bind(source)
    .bind(python_payload.as_slice())
    .execute(&pool)
    .await
    .unwrap();

    let outcome = broker
        .rerun_task(
            RerunTask::new(source, None, None),
            policy(SOURCE_CLASS, true),
        )
        .await
        .unwrap();
    let RerunOutcome::Enqueued { new_task_id, .. } = outcome else {
        panic!("expected enqueue");
    };
    let row = sqlx::query(
        "SELECT input, task_options, command_fingerprint
         FROM horsies_tasks WHERE id = $1",
    )
    .bind(new_task_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("input"), r#"{"v":1e-06}"#);
    assert_eq!(
        row.get::<String, _>("task_options"),
        r#"{"nested":[1e+20,0.0001,-0.0]}"#
    );
    let expected_fingerprint = EnqueueCommandV1::new(
        "p8.rerun",
        "default",
        50,
        None,
        Some(r#"{"v":1e-06}"#.to_owned()),
        None,
        None,
        Some(r#"{"nested":[1e+20,0.0001,-0.0]}"#.to_owned()),
        SOURCE_CLASS,
        true,
        Some(source),
        Some(source),
    )
    .unwrap()
    .fingerprint()
    .unwrap();
    assert_eq!(
        row.get::<Vec<u8>, _>("command_fingerprint"),
        expected_fingerprint
    );
    cleanup(&pool, &[source, new_task_id], &[]).await;
}

#[tokio::test]
#[serial]
async fn eligibility_live_absence_and_purged_floor_are_typed_before_input() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let (completed, _) = seed_source(&pool, TerminalSource::Completed, false, None).await;
    let (workflow_task, workflow_id) = seed_source(&pool, TerminalSource::Failed, true, None).await;
    let core = core_pool_for_test_pool(&pool);
    let mut connection = core.checkout(CheckoutClass::Batch).await.unwrap();
    let mut corrupt_eligibility = connection
        .begin(IsolationLevel::ServerDefault)
        .await
        .unwrap();
    // The baseline keeps the eligibility rule (no INLINE input on a completed
    // or workflow row) in the shape check (0001_tables.sql,
    // horsies_task_history_rerun_input_shape).
    corrupt_eligibility
        .run_script(&DROP_RERUN_INPUT_SHAPE, StatementBound::ConnectionTimeouts)
        .await
        .unwrap();
    let corrupted = vec![completed, workflow_task];
    corrupt_eligibility
        .execute(
            &STORE_CORRUPT_INLINE_INPUT.bind().value(&corrupted).finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
        .unwrap();
    assert_eq!(
        rerun_task_in_transaction(
            &mut corrupt_eligibility,
            StatementLog::Off,
            &RerunTask::new(completed, None, None),
            &policy(SOURCE_CLASS, true),
            ClientFamily::Rust,
        )
        .await
        .unwrap(),
        RerunOutcome::NotEligible {
            task_id: completed,
            reason: NotEligibleReason::CompletedSource,
        }
    );
    assert_eq!(
        rerun_task_in_transaction(
            &mut corrupt_eligibility,
            StatementLog::Off,
            &RerunTask::new(workflow_task, None, None),
            &policy(SOURCE_CLASS, true),
            ClientFamily::Rust,
        )
        .await
        .unwrap(),
        RerunOutcome::NotEligible {
            task_id: workflow_task,
            reason: NotEligibleReason::WorkflowTask,
        }
    );
    assert!(rolled_back(corrupt_eligibility.rollback().await));
    let live = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, status, sent_at, enqueued_at,
            retry_count, max_retries, command_fingerprint_version, command_fingerprint,
            retention_class_key, retain_rerun_input, created_at, updated_at, execution,
            is_workflow_task
        ) VALUES (
            $1, 'p8.live', 'default', 50, 'null', 'PENDING', NOW(), NOW(), 0, 0, 1, $2,
            'forever', FALSE, NOW(), NOW(), 'ISOLATED', FALSE
        )",
    )
    .bind(live)
    .bind(vec![4_u8; 32])
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        broker
            .rerun_task(RerunTask::new(live, None, None), policy(SOURCE_CLASS, true))
            .await
            .unwrap(),
        RerunOutcome::SourceLive { task_id: live }
    );
    let live_snapshot = broker
        .get_raw_result_record(live, Some(std::time::Duration::ZERO))
        .await
        .unwrap()
        .expect_found();
    assert_eq!(live_snapshot.task_id, live);
    assert_eq!(live_snapshot.task_name, "p8.live");
    assert_eq!(live_snapshot.status, TaskStatus::Pending);
    assert!(live_snapshot.raw_result.is_none());
    // The worker identity columns are NOT NULL in the baseline attempt
    // table.
    sqlx::query(
        "INSERT INTO horsies_task_attempts (
             task_id, attempt, outcome, will_retry, started_at, finished_at,
             worker_id, worker_hostname, worker_pid, worker_process_name
         ) VALUES ($1, 1, 'WORKER_FAILURE', TRUE,
                   NOW() - interval '1 second', NOW(),
                   $2, 'rerun-test-host', 4242, 'rerun-test-process')",
    )
    .bind(live)
    .bind(WORKER)
    .execute(&pool)
    .await
    .unwrap();
    let live_info = broker
        .get_task_info_with_attempts(live, false, false, true)
        .await
        .unwrap()
        .expect_found();
    assert_eq!(live_info.task_id, live);
    assert_eq!(live_info.status, TaskStatus::Pending);
    assert_eq!(
        live_info.execution,
        crate::core::ExecutionClass::Live(crate::core::Execution::Isolated)
    );
    let live_attempts = live_info.attempts.unwrap();
    assert_eq!(live_attempts.len(), 1);
    assert_eq!(
        live_attempts[0].outcome,
        crate::TaskAttemptOutcome::WorkerFailure
    );

    // A live row stores no result (0001_tables.sql): the stored result of a
    // terminal task is in history. A stored result that is not a task result
    // is INVALID_JSON_PAYLOAD.
    let non_object = b"[]".to_vec();
    sqlx::query(
        "UPDATE horsies_task_history
         SET result_payload = $2, result_digest = sha256($2)
         WHERE task_id = $1",
    )
    .bind(completed)
    .bind(&non_object)
    .execute(&pool)
    .await
    .unwrap();
    let history_error = broker
        .get_task_info(completed, true, false)
        .await
        .unwrap_err();
    assert_eq!(
        history_error.code,
        crate::BrokerErrorCode::InvalidJsonPayload
    );
    let absent = Uuid::new_v4();
    assert!(matches!(
        broker
            .rerun_task(
                RerunTask::new(absent, None, None),
                policy(SOURCE_CLASS, true)
            )
            .await
            .unwrap(),
        RerunOutcome::SourceAbsent {
            task_id,
            reason: TaskAbsenceReason::NotClassified,
        } if task_id == absent
    ));

    let old_birth = Utc::now() - Duration::days(3650);
    let milliseconds = old_birth.timestamp_millis() as u128;
    let raw = (milliseconds << 80) | (0x7_u128 << 76) | (0b10_u128 << 62) | 1;
    let old_absent = Uuid::from_u128(raw);
    // Retention removed a period-class leaf that ended after the id's birth:
    // the record of that removal is a detached catalog row.
    let mut transaction = connection
        .begin(IsolationLevel::ServerDefault)
        .await
        .unwrap();
    transaction
        .run_script(&RECORD_REMOVED_LEAF, StatementBound::ConnectionTimeouts)
        .await
        .unwrap();
    let outcome = rerun_task_in_transaction(
        &mut transaction,
        StatementLog::Off,
        &RerunTask::new(old_absent, None, None),
        &policy(SOURCE_CLASS, true),
        ClientFamily::Rust,
    )
    .await
    .unwrap();
    assert_eq!(
        outcome,
        RerunOutcome::SourceAbsent {
            task_id: old_absent,
            reason: TaskAbsenceReason::PredatesRetainedHistory,
        }
    );
    assert!(rolled_back(transaction.rollback().await));
    drop(connection);
    core.close();

    cleanup(
        &pool,
        &[completed, workflow_task, live],
        &workflow_id.into_iter().collect::<Vec<_>>(),
    )
    .await;
}

#[tokio::test]
#[serial]
async fn non_inline_and_corrupt_input_shapes_fail_without_writes() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let (source, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    for disposition in ["DECLINED_BY_POLICY", "OVER_BOUND", "NEVER_ELIGIBLE"] {
        sqlx::query(
            "UPDATE horsies_task_history
             SET rerun_input_disposition = $2,
                 rerun_input_version = NULL,
                 rerun_input_digest = NULL,
                 rerun_input_inline = NULL
             WHERE task_id = $1",
        )
        .bind(source)
        .bind(disposition)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            broker
                .rerun_task(
                    RerunTask::new(source, None, None),
                    policy(SOURCE_CLASS, true)
                )
                .await
                .unwrap(),
            RerunOutcome::InputUnavailable {
                task_id: source,
                disposition: disposition.to_owned(),
            }
        );
    }

    let valid_payload =
        format!("{{\"input\":{PAYLOAD_INPUT},\"options\":{PAYLOAD_OPTIONS}}}").into_bytes();
    let valid_digest = Sha256::digest(&valid_payload).to_vec();

    let core = core_pool_for_test_pool(&pool);
    let mut connection = core.checkout(CheckoutClass::Batch).await.unwrap();
    let mut incomplete = connection
        .begin(IsolationLevel::ServerDefault)
        .await
        .unwrap();
    incomplete
        .run_script(&DROP_RERUN_INPUT_SHAPE, StatementBound::ConnectionTimeouts)
        .await
        .unwrap();
    incomplete
        .execute(
            &STORE_INCOMPLETE_INLINE_INPUT
                .bind()
                .value(&source)
                .value(&valid_digest)
                .value(&valid_payload)
                .finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
        .unwrap();
    assert!(matches!(
        rerun_task_in_transaction(
            &mut incomplete,
            StatementLog::Off,
            &RerunTask::new(source, None, None),
            &policy(SOURCE_CLASS, true),
            ClientFamily::Rust,
        )
        .await
        .unwrap(),
        RerunOutcome::InputCorrupt { task_id, ref detail }
            if task_id == source && detail.contains("incomplete")
    ));
    assert!(rolled_back(incomplete.rollback().await));
    drop(connection);
    core.close();

    sqlx::query(
        "UPDATE horsies_task_history
         SET rerun_input_disposition = 'INLINE', rerun_input_version = 2,
             rerun_input_digest = $2, rerun_input_inline = $3
         WHERE task_id = $1",
    )
    .bind(source)
    .bind(&valid_digest)
    .bind(&valid_payload)
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        broker
            .rerun_task(
                RerunTask::new(source, None, None),
                policy(SOURCE_CLASS, true)
            )
            .await
            .unwrap(),
        RerunOutcome::InputCorrupt { task_id, ref detail }
            if task_id == source && detail.contains("version 2")
    ));

    for payload in [b"not-json".to_vec(), br#"{"args":[],"kwargs":{}}"#.to_vec()] {
        let digest = Sha256::digest(&payload).to_vec();
        sqlx::query(
            "UPDATE horsies_task_history
             SET rerun_input_disposition = 'INLINE', rerun_input_version = 1,
                 rerun_input_digest = $2, rerun_input_inline = $3
             WHERE task_id = $1",
        )
        .bind(source)
        .bind(digest)
        .bind(payload)
        .execute(&pool)
        .await
        .unwrap();
        assert!(matches!(
            broker
                .rerun_task(
                    RerunTask::new(source, None, None),
                    policy(SOURCE_CLASS, true)
                )
                .await
                .unwrap(),
            RerunOutcome::InputCorrupt { task_id, .. } if task_id == source
        ));
    }
    sqlx::query(
        "UPDATE horsies_task_history SET rerun_input_inline = 'tampered'::bytea
         WHERE task_id = $1",
    )
    .bind(source)
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        broker
            .rerun_task(
                RerunTask::new(source, None, None),
                policy(SOURCE_CLASS, true)
            )
            .await
            .unwrap(),
        RerunOutcome::InputCorrupt { task_id, ref detail }
            if task_id == source && detail.contains("digest")
    ));
    let new_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE rerun_of_task_id = $1")
            .bind(source)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(new_count, 0);
    cleanup(&pool, &[source], &[]).await;
}

/// Store a valid version-1 inline envelope with `options_json` as the source's
/// rerun input.
async fn store_inline_input(pool: &PgPool, source: Uuid, options_json: &str) {
    let payload = format!("{{\"input\":{PAYLOAD_INPUT},\"options\":{options_json}}}").into_bytes();
    let digest = Sha256::digest(&payload).to_vec();
    sqlx::query(
        "UPDATE horsies_task_history
         SET rerun_input_disposition = 'INLINE', rerun_input_version = 1,
             rerun_input_digest = $2, rerun_input_inline = $3
         WHERE task_id = $1",
    )
    .bind(source)
    .bind(digest)
    .bind(payload)
    .execute(pool)
    .await
    .unwrap();
}

async fn rerun_count(pool: &PgPool, source: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE rerun_of_task_id = $1")
        .bind(source)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
#[serial]
async fn an_unknown_stored_status_is_source_corrupt() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let (source, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    sqlx::query("UPDATE horsies_task_history SET status = 'BOGUS' WHERE task_id = $1")
        .bind(source)
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        broker
            .rerun_task(
                RerunTask::new(source, None, None),
                policy(SOURCE_CLASS, true)
            )
            .await
            .unwrap(),
        RerunOutcome::SourceCorrupt { task_id, ref detail }
            if task_id == source && detail.contains("BOGUS")
    ));
    assert_eq!(rerun_count(&pool, source).await, 0);
    cleanup(&pool, &[source], &[]).await;
}

#[tokio::test]
#[serial]
async fn a_rerun_keeps_the_execution_class_of_the_request() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let (source, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    store_inline_input(&pool, source, "{\"execution\":\"SHARED_THREAD\"}").await;
    let RerunOutcome::Enqueued { new_task_id, .. } = broker
        .rerun_task(
            RerunTask::new(source, None, None),
            policy(SOURCE_CLASS, true),
        )
        .await
        .unwrap()
    else {
        panic!("a source with a stored execution class must enqueue");
    };
    let execution: String = sqlx::query_scalar("SELECT execution FROM horsies_tasks WHERE id = $1")
        .bind(new_task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(execution, "SHARED_THREAD");
    cleanup(&pool, &[source, new_task_id], &[]).await;
}

#[tokio::test]
#[serial]
async fn an_invalid_stored_execution_is_input_corrupt() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let (source, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    store_inline_input(&pool, source, "{\"execution\":\"PARALLEL\"}").await;
    assert!(matches!(
        broker
            .rerun_task(
                RerunTask::new(source, None, None),
                policy(SOURCE_CLASS, true)
            )
            .await
            .unwrap(),
        RerunOutcome::InputCorrupt { task_id, ref detail }
            if task_id == source && detail.starts_with("stored task options: ")
    ));
    assert_eq!(rerun_count(&pool, source).await, 0);
    cleanup(&pool, &[source], &[]).await;
}

#[tokio::test]
#[serial]
async fn keyed_replay_and_source_owned_key_conflict_are_exact() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let (source, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    let command = || RerunTask::new(source, None, Some("p8-replay".to_owned()));
    let first = broker
        .rerun_task(command(), policy(SOURCE_CLASS, true))
        .await
        .unwrap();
    let RerunOutcome::Enqueued { new_task_id, .. } = first else {
        panic!("expected keyed enqueue");
    };
    assert_eq!(
        broker
            .rerun_task(command(), policy(SOURCE_CLASS, true))
            .await
            .unwrap(),
        RerunOutcome::KeyReplay {
            existing_task_id: new_task_id,
        }
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE rerun_of_task_id = $1")
            .bind(source)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);

    let (conflict_source, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    let scoped = ScopedIdempotencyKey::new("p8.rerun", "p8-source-key").unwrap();
    let mut connection = pool.acquire().await.unwrap();
    let claim = claim_key_reservation(
        &mut connection,
        &scoped.digest(),
        IDEMPOTENCY_SCOPE_VERSION,
        86_400_000_000,
        1,
        &EnqueueCommandV1::new(
            "p8.rerun",
            "default",
            50,
            None,
            Some(PAYLOAD_INPUT.to_owned()),
            None,
            None,
            Some(PAYLOAD_OPTIONS.to_owned()),
            SOURCE_CLASS,
            true,
            None,
            None,
        )
        .unwrap()
        .fingerprint()
        .unwrap(),
        conflict_source,
    )
    .await
    .unwrap();
    assert!(matches!(claim, ReservationClaim::Applied { .. }));
    drop(connection);
    assert_eq!(
        broker
            .rerun_task(
                RerunTask::new(conflict_source, None, Some("p8-source-key".to_owned())),
                policy(SOURCE_CLASS, true),
            )
            .await
            .unwrap(),
        RerunOutcome::KeyConflict {
            task_id: conflict_source,
            reserved_by_task_id: conflict_source,
        }
    );
    cleanup(&pool, &[source, new_task_id, conflict_source], &[]).await;
}

#[tokio::test]
#[serial]
async fn broker_rolls_back_an_applied_reservation_when_the_live_insert_fails() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let (source, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    let caller_key = "p8-post-claim-insert-failure";
    let key_digest = ScopedIdempotencyKey::new("p8.rerun", caller_key)
        .unwrap()
        .digest();

    sqlx::query(
        "CREATE OR REPLACE FUNCTION horsies_p8_reject_rerun_insert()
         RETURNS trigger LANGUAGE plpgsql AS $function$
         BEGIN
             RAISE EXCEPTION 'p8 injected post-reservation insert failure';
         END
         $function$",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER horsies_p8_reject_rerun_insert_trigger
         BEFORE INSERT ON horsies_tasks
         FOR EACH ROW
         WHEN (NEW.rerun_of_task_id IS NOT NULL
               AND NEW.idempotency_key_digest IS NOT NULL)
         EXECUTE FUNCTION horsies_p8_reject_rerun_insert()",
    )
    .execute(&pool)
    .await
    .unwrap();

    let error = broker
        .rerun_task(
            RerunTask::new(source, None, Some(caller_key.to_owned())),
            policy(SOURCE_CLASS, true),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            RerunError::Statement(StatementFailure::StatementRefused { .. })
        ),
        "{error:?}"
    );

    sqlx::query("DROP TRIGGER horsies_p8_reject_rerun_insert_trigger ON horsies_tasks")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION horsies_p8_reject_rerun_insert()")
        .execute(&pool)
        .await
        .unwrap();

    let reservation_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM horsies_key_reservations
             WHERE idempotency_key_digest = $1",
    )
    .bind(key_digest.as_slice())
    .fetch_one(&pool)
    .await
    .unwrap();
    let row_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE rerun_of_task_id = $1")
            .bind(source)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((reservation_count, row_count), (0, 0));
    cleanup(&pool, &[source], &[]).await;
}

#[tokio::test]
#[serial]
async fn unregistered_class_refuses_before_source_or_reservation_writes() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let (source, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    let before: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM horsies_tasks),
                (SELECT count(*) FROM horsies_key_reservations)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let error = broker
        .rerun_task(
            RerunTask::new(source, None, Some("must-not-claim".to_owned())),
            policy("p8_not_registered", true),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, RerunError::UnknownRetentionClass(ref key) if key == "p8_not_registered")
    );
    let after: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM horsies_tasks),
                (SELECT count(*) FROM horsies_key_reservations)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after, before);
    // The class check and the detail read are one statement, which calls the
    // published detail reader (Python `read_rerun_source`): a missing reader
    // is the server's `42883` for the whole read, whatever the class.
    let core = core_pool_for_test_pool(&pool);
    let mut connection = core.checkout(CheckoutClass::Batch).await.unwrap();
    let mut missing_reader = connection
        .begin(IsolationLevel::ServerDefault)
        .await
        .unwrap();
    missing_reader
        .run_script(&DROP_DETAIL_READER, StatementBound::ConnectionTimeouts)
        .await
        .unwrap();
    let error = rerun_task_in_transaction(
        &mut missing_reader,
        StatementLog::Off,
        &RerunTask::new(source, None, None),
        &policy("p8_not_registered", true),
        ClientFamily::Rust,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            error,
            RerunError::Statement(StatementFailure::StatementRefused { ref cause, .. })
                if cause.server_error().code().code() == "42883"
        ),
        "{error:?}"
    );
    assert!(rolled_back(missing_reader.rollback().await));
    drop(connection);
    core.close();
    cleanup(&pool, &[source], &[]).await;
}

#[tokio::test]
#[serial]
async fn public_history_reads_verify_result_and_use_attempt_snapshot() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let (completed, _) = seed_source(&pool, TerminalSource::Completed, false, None).await;
    let raw = broker
        .get_raw_result_record(completed, None)
        .await
        .unwrap()
        .expect_found();
    assert_eq!(raw.task_id, completed);
    assert_eq!(raw.task_name, "p8.rerun");
    assert_eq!(raw.status, TaskStatus::Completed);
    assert_eq!(
        envelope_value(&raw),
        Some(serde_json::json!({"__h_task_result__": true, "ok": {"value": 7}, "err": null}))
    );
    let info = broker
        .get_task_info_with_attempts(completed, true, true, true)
        .await
        .unwrap()
        .expect_found();
    assert_eq!(info.status, TaskStatus::Completed);
    assert_eq!(
        info.execution,
        crate::core::ExecutionClass::NotKeptInHistory
    );
    let attempts = info.attempts.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].task_id, completed);
    assert_eq!(attempts[0].attempt, 1);
    assert_eq!(attempts[0].worker_hostname.as_deref(), Some("p8-host"));

    let (failed, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    let failed_raw = broker
        .get_raw_result_record(failed, None)
        .await
        .unwrap()
        .expect_found();
    assert_eq!(failed_raw.status, TaskStatus::Failed);
    let expected_error =
        TaskError::builtin(OperationalErrorCode::TaskException, "p8 terminal source");
    let expected_error_envelope =
        serde_json::to_value(TaskResult::<serde_json::Value>::Err(expected_error.clone())).unwrap();
    assert_eq!(
        envelope_value(&failed_raw),
        Some(expected_error_envelope.clone())
    );

    let (expired, _) = seed_source(&pool, TerminalSource::Expired, false, None).await;
    let expired_raw = broker
        .get_raw_result_record(expired, None)
        .await
        .unwrap()
        .expect_found();
    assert_eq!(expired_raw.status, TaskStatus::Expired);
    assert_eq!(envelope_value(&expired_raw), Some(expected_error_envelope));

    let (cancelled, _) = seed_source(&pool, TerminalSource::Cancelled, false, None).await;
    let cancelled_raw = broker
        .get_raw_result_record(cancelled, None)
        .await
        .unwrap()
        .expect_found();
    assert_eq!(cancelled_raw.status, TaskStatus::Cancelled);
    assert!(cancelled_raw.raw_result.is_none());
    assert!(broker
        .get_raw_result_record(Uuid::new_v4(), None)
        .await
        .unwrap()
        .is_absent());

    sqlx::query(
        "UPDATE horsies_task_history SET result_digest = decode(repeat('00', 32), 'hex')
         WHERE task_id = $1",
    )
    .bind(completed)
    .execute(&pool)
    .await
    .unwrap();
    let history_info_error = broker
        .get_task_info_with_attempts(completed, true, false, true)
        .await
        .unwrap_err();
    assert_eq!(
        history_info_error.code,
        crate::BrokerErrorCode::InvalidJsonPayload
    );
    let raw_error = broker
        .get_raw_result_record(completed, None)
        .await
        .unwrap_err();
    assert_eq!(raw_error.code, crate::BrokerErrorCode::InvalidJsonPayload);

    let non_object = b"[]".to_vec();
    sqlx::query(
        "UPDATE horsies_task_history
         SET result_payload = $2, result_digest = sha256($2)
         WHERE task_id = $1",
    )
    .bind(completed)
    .bind(&non_object)
    .execute(&pool)
    .await
    .unwrap();
    let shape_error = broker
        .get_raw_result_record(completed, None)
        .await
        .unwrap_err();
    assert_eq!(shape_error.code, crate::BrokerErrorCode::InvalidJsonPayload);

    // The baseline stores no result content type: format version 1 implies
    // it, so a stored content type cannot disagree with the version.

    let attempt_payload: Vec<u8> =
        sqlx::query_scalar("SELECT attempt_snapshot FROM horsies_task_history WHERE task_id = $1")
            .bind(failed)
            .fetch_one(&pool)
            .await
            .unwrap();
    let mut attempt_rows: serde_json::Value = serde_json::from_slice(&attempt_payload).unwrap();
    attempt_rows[0][1] = serde_json::json!("FOREIGN_OUTCOME");
    let corrupt_attempt_payload = serde_json::to_vec(&attempt_rows).unwrap();
    sqlx::query(
        "UPDATE horsies_task_history
         SET attempt_snapshot = $2, attempt_snapshot_digest = sha256($2)
         WHERE task_id = $1",
    )
    .bind(failed)
    .bind(&corrupt_attempt_payload)
    .execute(&pool)
    .await
    .unwrap();
    let attempt_error = broker
        .get_task_info_with_attempts(failed, false, false, true)
        .await
        .unwrap_err();
    // Python's code: only a stored result that fails its check is
    // INVALID_JSON_PAYLOAD; an attempt snapshot that does not decode is
    // TASK_INFO_QUERY_FAILED, not retryable.
    assert_eq!(
        attempt_error.code,
        crate::BrokerErrorCode::TaskInfoQueryFailed
    );
    assert!(!attempt_error.retryable);

    cleanup(&pool, &[completed, failed, expired, cancelled], &[]).await;

    // A detail reader removed out of band refuses the history read
    // (SQLSTATE 42883, TASK_INFO_QUERY_FAILED); the next publication of the
    // readers makes the read answer again.
    sqlx::query("DROP FUNCTION horsies_task_detail_staged(uuid)")
        .execute(&pool)
        .await
        .unwrap();
    let refused = broker
        .get_raw_result_record(Uuid::new_v4(), None)
        .await
        .expect_err("a removed detail reader refuses the history read");
    assert_eq!(refused.code, crate::BrokerErrorCode::TaskInfoQueryFailed);
    assert!(!refused.retryable);
    assert!(refused.message.contains("42883"), "{}", refused.message);
    let mut publication = pool.begin().await.unwrap();
    StagedLoaderPublisher
        .republish(publication.as_mut())
        .await
        .unwrap();
    publication.commit().await.unwrap();
    assert!(broker
        .get_raw_result_record(Uuid::new_v4(), None)
        .await
        .unwrap()
        .is_absent());
}

#[tokio::test]
#[serial]
async fn provenance_table_is_exhaustive_over_the_live_schema() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let actual: std::collections::BTreeSet<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = 'horsies_tasks'",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    // This raw-migration column belongs only to the database-owned terminal
    // move. Python's canonical model and provenance table intentionally do
    // not treat it as enqueue-visible.
    .filter(|column| column != "terminalization_kind")
    .collect();
    let classified: std::collections::BTreeSet<String> = RERUN_FIELD_PROVENANCE
        .iter()
        .map(|(field, _)| (*field).to_owned())
        .collect();
    assert_eq!(classified, actual);
}

#[test]
fn public_facade_exports_the_complete_typed_surface() {
    let command = crate::RerunTask::new(Uuid::nil(), None, None);
    assert_eq!(command.source_task_id(), Uuid::nil());
    let _: crate::RerunOutcome = crate::RerunOutcome::SourceLive {
        task_id: Uuid::nil(),
    };
    let _: crate::NotEligibleReason = crate::NotEligibleReason::CompletedSource;
    let _: crate::NotEligibleReason = crate::NotEligibleReason::WorkflowTask;
    let _: Option<crate::RawResultRecord> = None;

    fn exhaustive(outcome: crate::RerunOutcome) {
        match outcome {
            crate::RerunOutcome::Enqueued { .. }
            | crate::RerunOutcome::SourceLive { .. }
            | crate::RerunOutcome::SourceAbsent { .. }
            | crate::RerunOutcome::SourceCorrupt { .. }
            | crate::RerunOutcome::NotEligible { .. }
            | crate::RerunOutcome::InputUnavailable { .. }
            | crate::RerunOutcome::InputCorrupt { .. }
            | crate::RerunOutcome::KeyConflict { .. }
            | crate::RerunOutcome::KeyReplay { .. } => {}
        }
    }
    exhaustive(crate::RerunOutcome::SourceLive {
        task_id: Uuid::nil(),
    });
}

/// A handle of another client family is refused before the rerun reads or
/// writes anything: no new task row and no key reservation.
#[tokio::test]
#[serial]
async fn a_handle_of_another_client_family_reruns_nothing() {
    use crate::broker::client_family::{ClientFamily, RecordedFamily};

    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let (source, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    let broker = PostgresBroker::from_pool(pool.clone()).with_client_family(ClientFamily::Python);
    let before: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM horsies_tasks),
                (SELECT count(*) FROM horsies_key_reservations)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let refused = broker
        .rerun_task(
            RerunTask::new(source, None, Some("family-refused".to_owned())),
            policy(SOURCE_CLASS, true),
        )
        .await
        .expect_err("a handle of another family must not rerun");
    assert!(matches!(
        refused,
        RerunError::ClientFamilyMismatch {
            stored: RecordedFamily::Recorded(ClientFamily::Rust),
            own: ClientFamily::Python,
        }
    ));
    let after: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM horsies_tasks),
                (SELECT count(*) FROM horsies_key_reservations)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after, before);
    cleanup(&pool, &[source], &[]).await;
}

/// The prepared write of a rerun, from its read on `transaction`.
async fn prepared_write(
    transaction: &mut crate::db::Transaction<'_>,
    command: &RerunTask,
    policy: &RerunEnqueuePolicy,
) -> super::steps::RerunWrite {
    use super::caller::CallerTransaction;
    use super::steps::{prepare_rerun_write, rerun_read, RerunPreparation};

    let read = rerun_read(command, policy);
    let row = transaction.query_one(&read).await.unwrap();
    match prepare_rerun_write(&read, &row, command, policy, ClientFamily::Rust, Utc::now()).unwrap()
    {
        RerunPreparation::Write(write) => write,
        other => panic!("expected a write, got {other:?}"),
    }
}

/// The same prepared rerun write run twice in one transaction gives one
/// task: the second run answers `SAME_COMMAND`, which is the enqueued rerun.
#[tokio::test]
#[serial]
async fn the_same_prepared_rerun_write_run_twice_gives_one_task() {
    use super::caller::CallerTransaction;
    use super::steps::{decode_rerun_write, RerunWriteOutcome};

    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let (source, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    let core = core_pool_for_test_pool(&pool);
    let mut connection = core.checkout(CheckoutClass::Batch).await.unwrap();
    let mut transaction = connection
        .begin(IsolationLevel::ServerDefault)
        .await
        .unwrap();
    let command = RerunTask::new(source, None, None);
    let policy = policy(SOURCE_CLASS, true);
    let write = prepared_write(&mut transaction, &command, &policy).await;
    for _ in 0..2 {
        let row = transaction.query_one(write.statement()).await.unwrap();
        assert!(matches!(
            decode_rerun_write(&write, &row).unwrap(),
            RerunWriteOutcome::Enqueued { new_task_id, .. } if new_task_id == write.new_task_id()
        ));
    }
    transaction.commit().await.unwrap();
    drop(connection);
    let tasks: i64 = sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
        .bind(write.new_task_id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(tasks, 1);
    core.close();
    cleanup(&pool, &[source, write.new_task_id()], &[]).await;
}

/// A keyed rerun that the caller's transaction rolls back leaves no task and
/// no key reservation.
#[tokio::test]
#[serial]
async fn a_rerun_rolls_back_with_the_callers_transaction() {
    use super::caller::CallerTransaction;
    use super::steps::{decode_rerun_write, RerunWriteOutcome};

    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let (source, _) = seed_source(&pool, TerminalSource::Failed, false, None).await;
    let core = core_pool_for_test_pool(&pool);
    let mut connection = core.checkout(CheckoutClass::Batch).await.unwrap();
    let mut transaction = connection
        .begin(IsolationLevel::ServerDefault)
        .await
        .unwrap();
    let command = RerunTask::new(source, None, Some("p8-rollback-key".to_owned()));
    let policy = policy(SOURCE_CLASS, true);
    let write = prepared_write(&mut transaction, &command, &policy).await;
    let row = transaction.query_one(write.statement()).await.unwrap();
    assert!(matches!(
        decode_rerun_write(&write, &row).unwrap(),
        RerunWriteOutcome::Enqueued { .. }
    ));
    assert!(rolled_back(transaction.rollback().await));
    drop(connection);
    let (tasks, reservations): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM horsies_tasks WHERE id = $1),
                (SELECT count(*) FROM horsies_key_reservations WHERE task_id = $1)",
    )
    .bind(write.new_task_id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((tasks, reservations), (0, 0));
    core.close();
    cleanup(&pool, &[source], &[]).await;
}

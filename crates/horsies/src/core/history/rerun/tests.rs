use chrono::{DateTime, Duration, Utc};
use serial_test::serial;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::input_envelope::encode_input_envelope_v1;
use super::operations::{
    rerun_task_in_tx, NotEligibleReason, RerunEnqueuePolicy, RerunError, RerunOutcome, RerunTask,
};
use super::provenance::RERUN_FIELD_PROVENANCE;
use crate::broker::terminalization::terminalize;
use crate::broker::PostgresBroker;
use crate::core::history::ddl::classes::register_finite_retention_class;
use crate::core::history::identity::fingerprint::EnqueueCommandV1;
use crate::core::history::identity::keys::{ScopedIdempotencyKey, IDEMPOTENCY_SCOPE_VERSION};
use crate::core::history::identity::reservations::{claim_key_reservation, ReservationClaim};
use crate::core::history::partitions::publication::LoaderPublication;
use crate::core::history::reads::publisher::StagedLoaderPublisher;
use crate::core::lifecycle::{
    CallerHoldsRowLock, PriorLockedRead, TerminalizationCommand, TerminalizationOutcome,
    WorkerOwned,
};
use crate::core::{OperationalErrorCode, TaskError, TaskResult, TaskStatus};

const WORKER: &str = "p8-rerun-worker";
const SOURCE_CLASS: &str = "forever";
const CURRENT_POLICY_CLASS: &str = "p8_current_policy";
const PAYLOAD_ARGS: &str = "[1,\"x\"]";
const PAYLOAD_KWARGS: &str = "{\"k\":\"v\"}";
const PAYLOAD_OPTIONS: &str = "{\"timeout_ms\":5}";

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
                OperationalErrorCode::TaskError,
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
    let input_payload = encode_input_envelope_v1(
        &[serde_json::json!(1), serde_json::json!("x")],
        &serde_json::from_str(PAYLOAD_KWARGS).unwrap(),
        Some(&serde_json::from_str(PAYLOAD_OPTIONS).unwrap()),
    )
    .unwrap();
    let input_digest = Sha256::digest(&input_payload).to_vec();
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
             id, task_name, queue_name, priority, args, kwargs, task_options,
             status, sent_at, enqueued_at, started_at, claimed, claimed_at,
             claimed_by_worker_id, good_until, is_workflow_task,
             retry_count, max_retries, enqueue_sha,
             command_fingerprint_version, command_fingerprint,
             retention_class_key, input_digest, rerun_of_task_id,
             rerun_root_task_id, retain_rerun_input,
             prepared_rerun_input_disposition, prepared_rerun_input_version,
             prepared_rerun_input_codec, prepared_rerun_input_content_type,
             prepared_rerun_input_digest, prepared_rerun_input_inline,
             created_at, updated_at
         ) VALUES (
             $1, 'p8.rerun', 'default', 50, $2, $3, $4,
             $5, NOW(), NOW(), $6, $7::text IS NOT NULL, $8, $7, $9, $10,
             0, 3, $1::text, 1, $11, $12, $13, $14, $15, TRUE,
             'INLINE', 1, 'json-utf8', 'application/json', $13, $16,
             NOW(), NOW()
         )",
    )
    .bind(task_id)
    .bind(PAYLOAD_ARGS)
    .bind(PAYLOAD_KWARGS)
    .bind(PAYLOAD_OPTIONS)
    .bind(status)
    .bind(started_at)
    .bind(worker)
    .bind(claimed_at)
    .bind(good_until)
    .bind(workflow_task)
    .bind(vec![9_u8; 32])
    .bind(SOURCE_CLASS)
    .bind(&input_digest)
    .bind(rerun_root.map(|_| Uuid::new_v4()))
    .bind(rerun_root)
    .bind(&input_payload)
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
                 id, workflow_id, task_index, node_id, task_name, task_args,
                 task_kwargs, queue_name, priority, dependencies,
                 allow_failed_deps, join_type, status, is_subworkflow,
                 task_id, created_at
             ) VALUES ($1, $2, 0, 'root', 'p8.rerun', $3, $4, 'default',
                       50, '{}', FALSE, 'all', 'RUNNING', FALSE, $5, NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .bind(PAYLOAD_ARGS)
        .bind(PAYLOAD_KWARGS)
        .bind(task_id)
        .execute(pool)
        .await
        .unwrap();
        Some(workflow_id)
    } else {
        None
    };

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

    let result_json = terminal_result(kind);
    let command = match kind {
        TerminalSource::Failed => TerminalizationCommand::FailLockedTask {
            task_id,
            fence: PriorLockedRead {
                worker_id: WORKER.to_owned(),
            },
            result_json,
            error_code: Some("P8".to_owned()),
            failed_reason: Some("p8 failure".to_owned()),
        },
        TerminalSource::Cancelled => TerminalizationCommand::CancelLockedTask {
            task_id,
            fence: CallerHoldsRowLock,
            permitted_source_statuses: vec![TaskStatus::Pending],
        },
        TerminalSource::Expired => TerminalizationCommand::ExpireOwnedClaim {
            task_id,
            fence: WorkerOwned {
                worker_id: WORKER.to_owned(),
            },
            result_json,
            error_code: "TASK_EXPIRED".to_owned(),
        },
        TerminalSource::Completed => TerminalizationCommand::CompleteLockedTask {
            task_id,
            fence: PriorLockedRead {
                worker_id: WORKER.to_owned(),
            },
            result_json,
        },
    };
    let outcomes = terminalize(pool, &command).await.unwrap();
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
    register_finite_retention_class(&mut registration, CURRENT_POLICY_CLASS, Duration::days(14))
        .await
        .unwrap();
    drop(registration);
    let deadline = Utc::now() + Duration::hours(2);
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
            "SELECT task_name, queue_name, priority, args, kwargs, task_options,
                    status, retry_count, max_retries, good_until,
                    retention_class_key, rerun_of_task_id, rerun_root_task_id,
                    command_fingerprint, input_digest,
                    prepared_rerun_input_disposition,
                    prepared_rerun_input_inline, claimed, claimed_at,
                    claimed_by_worker_id, claim_expires_at, next_retry_at,
                    started_at, completed_at, failed_at, terminal_at,
                    terminalization_kind, result, failed_reason, error_code,
                    finalizing_at, finalizing_by_worker_id, worker_pid,
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
        assert_eq!(row.get::<String, _>("args"), PAYLOAD_ARGS);
        assert_eq!(row.get::<String, _>("kwargs"), PAYLOAD_KWARGS);
        assert_eq!(row.get::<String, _>("task_options"), PAYLOAD_OPTIONS);
        assert_eq!(row.get::<String, _>("status"), "PENDING");
        assert_eq!(row.get::<i32, _>("retry_count"), 0);
        assert_eq!(row.get::<i32, _>("max_retries"), 3);
        assert_eq!(row.get::<DateTime<Utc>, _>("good_until"), deadline);
        assert_eq!(row.get::<String, _>("retention_class_key"), target_class);
        assert_eq!(row.get::<Uuid, _>("rerun_of_task_id"), source);
        assert_eq!(row.get::<Uuid, _>("rerun_root_task_id"), source);
        let canonical_input = encode_input_envelope_v1(
            &[serde_json::json!(1), serde_json::json!("x")],
            &serde_json::from_str(PAYLOAD_KWARGS).unwrap(),
            Some(&serde_json::from_str(PAYLOAD_OPTIONS).unwrap()),
        )
        .unwrap();
        let expected_disposition = if retain_rerun_input {
            "INLINE"
        } else {
            "DECLINED_BY_POLICY"
        };
        assert_eq!(
            row.get::<String, _>("prepared_rerun_input_disposition"),
            expected_disposition
        );
        assert_eq!(
            row.get::<Option<Vec<u8>>, _>("prepared_rerun_input_inline"),
            retain_rerun_input.then_some(canonical_input.clone())
        );
        let expected_fingerprint = EnqueueCommandV1::new(
            "p8.rerun",
            "default",
            50,
            Some(PAYLOAD_ARGS.to_owned()),
            Some(PAYLOAD_KWARGS.to_owned()),
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
        assert_eq!(
            row.get::<Vec<u8>, _>("input_digest"),
            Sha256::digest(&canonical_input).to_vec()
        );
        assert!(!row.get::<bool, _>("claimed"));
        for field in [
            "claimed_at",
            "claim_expires_at",
            "next_retry_at",
            "started_at",
            "completed_at",
            "failed_at",
            "terminal_at",
            "finalizing_at",
        ] {
            assert!(
                row.get::<Option<DateTime<Utc>>, _>(field).is_none(),
                "{field}"
            );
        }
        for field in [
            "terminalization_kind",
            "result",
            "failed_reason",
            "error_code",
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
    let python_payload =
        br#"{"args":[1e-07],"kwargs":{"v":1e-06},"options":{"nested":[1e+20,0.0001,-0.0]}}"#;
    assert_eq!(
        digest_hex(python_payload),
        "5c58503b12b755a4dba5612b59af861473b6b0dca19635b04d936de8e10d3d05"
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
        "SELECT args, kwargs, task_options, input_digest,
                prepared_rerun_input_inline, command_fingerprint
         FROM horsies_tasks WHERE id = $1",
    )
    .bind(new_task_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("args"), "[1e-07]");
    assert_eq!(row.get::<String, _>("kwargs"), r#"{"v":1e-06}"#);
    assert_eq!(
        row.get::<String, _>("task_options"),
        r#"{"nested":[1e+20,0.0001,-0.0]}"#
    );
    assert_eq!(
        row.get::<Vec<u8>, _>("prepared_rerun_input_inline"),
        python_payload
    );
    assert_eq!(
        row.get::<Vec<u8>, _>("input_digest"),
        Sha256::digest(python_payload).to_vec()
    );
    let expected_fingerprint = EnqueueCommandV1::new(
        "p8.rerun",
        "default",
        50,
        Some("[1e-07]".to_owned()),
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
    let mut corrupt_eligibility = pool.begin().await.unwrap();
    sqlx::query(
        "ALTER TABLE horsies_task_history
         DROP CONSTRAINT horsies_task_history_rerun_input_eligibility",
    )
    .execute(&mut *corrupt_eligibility)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE horsies_task_history
         SET rerun_input_disposition = 'INLINE', rerun_input_version = 1,
             rerun_input_codec = 'json-utf8',
             rerun_input_content_type = 'application/json',
             rerun_input_digest = decode(repeat('00', 32), 'hex'),
             rerun_input_inline = 'corrupt'::bytea
         WHERE task_id = ANY($1)",
    )
    .bind(vec![completed, workflow_task])
    .execute(&mut *corrupt_eligibility)
    .await
    .unwrap();
    assert_eq!(
        rerun_task_in_tx(
            corrupt_eligibility.as_mut(),
            &RerunTask::new(completed, None, None),
            &policy(SOURCE_CLASS, true),
        )
        .await
        .unwrap(),
        RerunOutcome::NotEligible {
            task_id: completed,
            reason: NotEligibleReason::CompletedSource,
        }
    );
    assert_eq!(
        rerun_task_in_tx(
            corrupt_eligibility.as_mut(),
            &RerunTask::new(workflow_task, None, None),
            &policy(SOURCE_CLASS, true),
        )
        .await
        .unwrap(),
        RerunOutcome::NotEligible {
            task_id: workflow_task,
            reason: NotEligibleReason::WorkflowTask,
        }
    );
    corrupt_eligibility.rollback().await.unwrap();
    let live = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, args, kwargs, status,
             sent_at, enqueued_at, retry_count, max_retries, enqueue_sha,
             command_fingerprint_version, command_fingerprint,
             retention_class_key, retain_rerun_input,
             prepared_rerun_input_disposition, created_at, updated_at
         ) VALUES ($1, 'p8.live', 'default', 50, '[]', '{}', 'PENDING',
                   NOW(), NOW(), 0, 0, $1::text, 1, $2, 'forever', FALSE,
                   'DECLINED_BY_POLICY', NOW(), NOW())",
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
        .unwrap();
    assert_eq!(live_snapshot.task_id, live);
    assert_eq!(live_snapshot.task_name, "p8.live");
    assert_eq!(live_snapshot.status, TaskStatus::Pending);
    assert!(live_snapshot.raw_result.is_none());
    sqlx::query(
        "INSERT INTO horsies_task_attempts (
             task_id, attempt, outcome, will_retry, started_at, finished_at
         ) VALUES ($1, 1, 'WORKER_FAILURE', TRUE,
                   NOW() - interval '1 second', NOW())",
    )
    .bind(live)
    .execute(&pool)
    .await
    .unwrap();
    let live_info = broker
        .get_task_info_with_attempts(live, false, false, true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live_info.task_id, live);
    assert_eq!(live_info.status, TaskStatus::Pending);
    let live_attempts = live_info.attempts.unwrap();
    assert_eq!(live_attempts.len(), 1);
    assert_eq!(
        live_attempts[0].outcome,
        crate::TaskAttemptOutcome::WorkerFailure
    );

    sqlx::query("UPDATE horsies_tasks SET result = '[]' WHERE id = $1")
        .bind(live)
        .execute(&pool)
        .await
        .unwrap();
    let live_error = broker.get_task_info(live, true, false).await.unwrap_err();
    assert_eq!(live_error.code, crate::BrokerErrorCode::InvalidJsonPayload);
    sqlx::query("UPDATE horsies_tasks SET result = NULL WHERE id = $1")
        .bind(live)
        .execute(&pool)
        .await
        .unwrap();
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
            predates_retained_floor: None,
        } if task_id == absent
    ));

    let old_birth = Utc::now() - Duration::days(3650);
    let milliseconds = old_birth.timestamp_millis() as u128;
    let raw = (milliseconds << 80) | (0x7_u128 << 76) | (0b10_u128 << 62) | 1;
    let old_absent = Uuid::from_u128(raw);
    let mut transaction = pool.begin().await.unwrap();
    sqlx::query(
        "UPDATE horsies_task_history_leaf_catalog
         SET min_birth_at = NOW(), min_birth_verified = TRUE
         WHERE detached_at IS NULL AND dropped_at IS NULL
           AND class_key <> 'heartbeats'",
    )
    .execute(&mut *transaction)
    .await
    .unwrap();
    let outcome = rerun_task_in_tx(
        transaction.as_mut(),
        &RerunTask::new(old_absent, None, None),
        &policy(SOURCE_CLASS, true),
    )
    .await
    .unwrap();
    assert_eq!(
        outcome,
        RerunOutcome::SourceAbsent {
            task_id: old_absent,
            predates_retained_floor: Some(true),
        }
    );
    transaction.rollback().await.unwrap();

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
    for (disposition, reference) in [
        ("DECLINED_BY_POLICY", None),
        ("OVER_BOUND", None),
        ("NEVER_ELIGIBLE", None),
        ("REFERENCE", Some("s3://caller-owned/p8")),
    ] {
        sqlx::query(
            "UPDATE horsies_task_history
             SET rerun_input_disposition = $2,
                 rerun_input_version = CASE WHEN $2 = 'REFERENCE' THEN 1 END,
                 rerun_input_codec = CASE WHEN $2 = 'REFERENCE' THEN 'json-utf8' END,
                 rerun_input_content_type = CASE WHEN $2 = 'REFERENCE' THEN 'application/json' END,
                 rerun_input_digest = CASE WHEN $2 = 'REFERENCE' THEN decode(repeat('01', 32), 'hex') END,
                 rerun_input_inline = NULL, rerun_input_reference = $3
             WHERE task_id = $1",
        )
        .bind(source)
        .bind(disposition)
        .bind(reference)
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
                reference_locator: reference.map(str::to_owned),
            }
        );
    }

    let valid_payload = encode_input_envelope_v1(
        &[serde_json::json!(1), serde_json::json!("x")],
        &serde_json::from_str(PAYLOAD_KWARGS).unwrap(),
        Some(&serde_json::from_str(PAYLOAD_OPTIONS).unwrap()),
    )
    .unwrap();
    let valid_digest = Sha256::digest(&valid_payload).to_vec();

    let mut incomplete = pool.begin().await.unwrap();
    sqlx::query(
        "ALTER TABLE horsies_task_history
         DROP CONSTRAINT horsies_task_history_rerun_input_shape",
    )
    .execute(&mut *incomplete)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE horsies_task_history
         SET rerun_input_disposition = 'INLINE', rerun_input_version = NULL,
             rerun_input_codec = 'json-utf8',
             rerun_input_content_type = 'application/json',
             rerun_input_digest = $2, rerun_input_inline = $3,
             rerun_input_reference = NULL
         WHERE task_id = $1",
    )
    .bind(source)
    .bind(&valid_digest)
    .bind(&valid_payload)
    .execute(&mut *incomplete)
    .await
    .unwrap();
    assert!(matches!(
        rerun_task_in_tx(
            incomplete.as_mut(),
            &RerunTask::new(source, None, None),
            &policy(SOURCE_CLASS, true),
        )
        .await
        .unwrap(),
        RerunOutcome::InputCorrupt { task_id, ref detail }
            if task_id == source && detail.contains("incomplete")
    ));
    incomplete.rollback().await.unwrap();

    sqlx::query(
        "UPDATE horsies_task_history
         SET rerun_input_disposition = 'INLINE', rerun_input_version = 2,
             rerun_input_codec = 'json-utf8',
             rerun_input_content_type = 'application/json',
             rerun_input_digest = $2, rerun_input_inline = $3,
             rerun_input_reference = NULL
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
                 rerun_input_codec = 'json-utf8',
                 rerun_input_content_type = 'application/json',
                 rerun_input_digest = $2, rerun_input_inline = $3,
                 rerun_input_reference = NULL
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
        86_400,
        1,
        &EnqueueCommandV1::new(
            "p8.rerun",
            "default",
            50,
            Some(PAYLOAD_ARGS.to_owned()),
            Some(PAYLOAD_KWARGS.to_owned()),
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
    assert!(matches!(error, RerunError::Database(_)));

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
    let mut missing_reader = pool.begin().await.unwrap();
    sqlx::query("DROP FUNCTION horsies_task_detail_staged(uuid)")
        .execute(&mut *missing_reader)
        .await
        .unwrap();
    let error = rerun_task_in_tx(
        missing_reader.as_mut(),
        &RerunTask::new(source, None, None),
        &policy("p8_not_registered", true),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        RerunError::UnknownRetentionClass(ref key) if key == "p8_not_registered"
    ));
    missing_reader.rollback().await.unwrap();
    cleanup(&pool, &[source], &[]).await;
}

#[tokio::test]
#[serial]
async fn public_history_reads_verify_result_and_use_attempt_snapshot() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let (completed, _) = seed_source(&pool, TerminalSource::Completed, false, None).await;
    let result = broker
        .get_result::<serde_json::Value>(completed, Some(std::time::Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(result.unwrap(), serde_json::json!({"value": 7}));
    let raw = broker
        .get_raw_result_record(completed, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raw.task_id, completed);
    assert_eq!(raw.task_name, "p8.rerun");
    assert_eq!(raw.status, TaskStatus::Completed);
    assert_eq!(
        raw.raw_result,
        Some(
            serde_json::json!({"__type": "ok", "value": {"value": 7}})
                .as_object()
                .unwrap()
                .clone()
        )
    );
    let info = broker
        .get_task_info_with_attempts(completed, true, true, true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(info.status, TaskStatus::Completed);
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
        .unwrap();
    assert_eq!(failed_raw.status, TaskStatus::Failed);
    let expected_error = TaskError::builtin(OperationalErrorCode::TaskError, "p8 terminal source");
    let expected_error_envelope =
        serde_json::to_value(TaskResult::<serde_json::Value>::Err(expected_error.clone()))
            .unwrap()
            .as_object()
            .unwrap()
            .clone();
    assert_eq!(failed_raw.raw_result, Some(expected_error_envelope.clone()));
    let failed_error = broker
        .get_result::<serde_json::Value>(failed, None)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(failed_error.error_code, expected_error.error_code);
    assert_eq!(failed_error.message, expected_error.message);
    assert_eq!(failed_error.cause, None);
    assert_eq!(failed_error.data, None);

    let (expired, _) = seed_source(&pool, TerminalSource::Expired, false, None).await;
    let expired_raw = broker
        .get_raw_result_record(expired, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expired_raw.status, TaskStatus::Expired);
    assert_eq!(expired_raw.raw_result, Some(expected_error_envelope));
    let expired_error = broker
        .get_result::<serde_json::Value>(expired, None)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(expired_error.error_code, expected_error.error_code);
    assert_eq!(expired_error.message, expected_error.message);
    assert_eq!(expired_error.cause, None);
    assert_eq!(expired_error.data, None);

    let (cancelled, _) = seed_source(&pool, TerminalSource::Cancelled, false, None).await;
    assert!(broker
        .get_result::<serde_json::Value>(cancelled, Some(std::time::Duration::from_secs(1)))
        .await
        .unwrap()
        .is_err());
    let cancelled_raw = broker
        .get_raw_result_record(cancelled, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancelled_raw.status, TaskStatus::Cancelled);
    assert!(cancelled_raw.raw_result.is_none());
    assert!(broker
        .get_raw_result_record(Uuid::new_v4(), None)
        .await
        .unwrap()
        .is_none());

    sqlx::query(
        "UPDATE horsies_task_history SET result_digest = decode(repeat('00', 32), 'hex')
         WHERE task_id = $1",
    )
    .bind(completed)
    .execute(&pool)
    .await
    .unwrap();
    assert!(broker
        .get_result::<serde_json::Value>(completed, Some(std::time::Duration::from_secs(1)))
        .await
        .is_err());
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
         SET result_payload = $2, result_digest = sha256($2),
             result_codec = 'json-utf8',
             result_content_type = 'application/json'
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

    sqlx::query(
        "UPDATE horsies_task_history SET result_content_type = 'text/plain'
         WHERE task_id = $1",
    )
    .bind(completed)
    .execute(&pool)
    .await
    .unwrap();
    let content_error = broker
        .get_raw_result_record(completed, None)
        .await
        .unwrap_err();
    assert_eq!(
        content_error.code,
        crate::BrokerErrorCode::InvalidJsonPayload
    );

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
    assert_eq!(
        attempt_error.code,
        crate::BrokerErrorCode::InvalidJsonPayload
    );

    cleanup(&pool, &[completed, failed, expired, cancelled], &[]).await;

    sqlx::query("DROP FUNCTION horsies_task_detail_staged(uuid)")
        .execute(&pool)
        .await
        .unwrap();
    assert!(broker
        .get_raw_result_record(Uuid::new_v4(), None)
        .await
        .unwrap()
        .is_none());
    let mut publication = pool.begin().await.unwrap();
    StagedLoaderPublisher
        .republish(publication.as_mut())
        .await
        .unwrap();
    publication.commit().await.unwrap();
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
    let _ = crate::rerun_task;
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

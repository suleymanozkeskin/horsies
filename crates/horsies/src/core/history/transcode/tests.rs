use std::str::FromStr;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use clap::Parser;
use serial_test::serial;
use sha2::{Digest, Sha256};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

use super::executor::{
    build_swap_exhausted, finalize_transcode, plan_transcode, run_copy_batch, swap_transcode,
    swap_with_retry_policy, verify_transcode,
};
use super::jobs::{job_relations, TRANSCODE_BATCHES, TRANSCODE_JOBS};
use super::maintenance::{begin_transcode_maintenance, finish_transcode_maintenance};
use super::outcomes::{
    ArchiveComponent, SwapLockMode, TranscodeCopyOutcome, TranscodeJobState, TranscodePlanOutcome,
    TranscodeSwapBusy, TranscodeSwapOutcome, BLOCKER_QUERY_TRUNCATION_CHARS,
    MAINTENANCE_SECONDS_MAXIMUM, SWAP_LOCK_ATTEMPTS_MAXIMUM, SWAP_LOCK_SECONDS_MAXIMUM,
    SWAP_RETRY_BACKOFF_SECONDS,
};
use super::signature::{relation_schema_signature, RELATION_SCHEMA_SIGNATURE_SQL};
use super::transforms::{
    backup_relation_name, component_columns, encoded_source_select, quoted_identifier,
    replacement_relation_name, transformed_select,
};
use crate::broker::migrations::run_horsies_migrations;
use crate::core::history::archive::rerun_input::RerunInputDisposition;
use crate::core::history::archive::versions::archive_digest;
use crate::core::history::maintenance::coverage::{ensure_partition_coverage, CoverageOutcome};
use crate::core::history::names::{LEAF_LOCK_KEY_FUNCTION, TASK_HISTORY_PARENT};
use crate::core::history::reads::publisher::StagedLoaderPublisher;
use crate::worker::cli::{Cli, Command};

const P10_DATABASE_PREFIX: &str = "horsies_p10_transcode_";

fn test_db_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|path| path.join(".env").exists());
    let password = root
        .and_then(|path| std::fs::read_to_string(path.join(".env")).ok())
        .and_then(|content| {
            content
                .lines()
                .filter_map(|line| line.trim().split_once('='))
                .find(|(key, _)| key.trim() == "DB_PASSWORD")
                .map(|(_, value)| value.trim().to_owned())
        })
        .unwrap_or_else(|| "W0rklane".to_owned());
    format!("postgresql://postgres:{password}@localhost:5432/postgres")
}

struct P10Database {
    name: String,
    pool: PgPool,
    base_options: PgConnectOptions,
}

impl P10Database {
    async fn create() -> Self {
        let base_options = PgConnectOptions::from_str(&test_db_url()).unwrap();
        let mut admin = PgConnection::connect_with(&base_options.clone().database("postgres"))
            .await
            .unwrap();
        sqlx::query("SELECT pg_advisory_lock(hashtext('horsies_p10_transcode_setup'))")
            .execute(&mut admin)
            .await
            .unwrap();
        let stale: Vec<String> = sqlx::query_scalar(
            "SELECT d.datname FROM pg_database AS d
             WHERE left(d.datname, length('horsies_p10_transcode_')) =
                   'horsies_p10_transcode_'
               AND NOT EXISTS (
                   SELECT 1 FROM pg_stat_activity AS a WHERE a.datname = d.datname
               )
             ORDER BY d.datname",
        )
        .fetch_all(&mut admin)
        .await
        .unwrap();
        for database in stale {
            let suffix = database.strip_prefix(P10_DATABASE_PREFIX).unwrap();
            assert!(
                suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "refuse to drop non-generated P10 database {database:?}"
            );
            sqlx::query(&format!("DROP DATABASE \"{database}\""))
                .execute(&mut admin)
                .await
                .unwrap();
        }
        let name = format!("{P10_DATABASE_PREFIX}{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE DATABASE \"{name}\""))
            .execute(&mut admin)
            .await
            .unwrap();
        let pool = PgPoolOptions::new()
            .min_connections(1)
            .max_connections(8)
            .max_lifetime(None)
            .idle_timeout(None)
            .connect_with(base_options.clone().database(&name))
            .await
            .unwrap();
        let unlocked: bool = sqlx::query_scalar(
            "SELECT pg_advisory_unlock(hashtext('horsies_p10_transcode_setup'))",
        )
        .fetch_one(&mut admin)
        .await
        .unwrap();
        assert!(unlocked);
        run_horsies_migrations(&pool).await.unwrap();
        let mut coverage = pool.begin().await.unwrap();
        assert!(matches!(
            ensure_partition_coverage(&mut coverage, 2, 2, &[], &StagedLoaderPublisher,)
                .await
                .unwrap(),
            CoverageOutcome::Ensured(_)
        ));
        coverage.commit().await.unwrap();
        Self {
            name,
            pool,
            base_options,
        }
    }

    async fn destroy(self) {
        self.pool.close().await;
        let mut admin = PgConnection::connect_with(&self.base_options.database("postgres"))
            .await
            .unwrap();
        let active: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = $1 AND backend_type = 'client backend'",
        )
        .bind(&self.name)
        .fetch_one(&mut admin)
        .await
        .unwrap();
        assert_eq!(
            active, 0,
            "generated P10 database still has client sessions"
        );
        sqlx::query(&format!("DROP DATABASE \"{}\"", self.name))
            .execute(&mut admin)
            .await
            .unwrap();
    }
}

async fn seed_result_rows(pool: &PgPool, count_today: usize, count_tomorrow: usize) -> Vec<Uuid> {
    let mut ids = Vec::new();
    let now = Utc::now();
    for (offset, count) in [(0_i64, count_today), (1_i64, count_tomorrow)] {
        for index in 0..count {
            let id = crate::core::history::identity::uuid7::mint_task_id().unwrap();
            let anchor = now + ChronoDuration::days(offset) + ChronoDuration::seconds(index as i64);
            let payload = format!("{{\"day\":{offset},\"row\":{index}}}").into_bytes();
            sqlx::query(&format!(
                r#"
                INSERT INTO {TASK_HISTORY_PARENT} (
                    task_id, task_name, queue_name, priority,
                    command_fingerprint_version, command_fingerprint,
                    status, terminalization_kind, terminal_at,
                    retention_anchor_at, retention_class_key,
                    sent_at, enqueued_at, started_at, created_at,
                    retry_count, max_retries, last_claimed_worker_id,
                    last_worker_hostname, last_worker_pid,
                    result_envelope_version, result_codec, result_content_type,
                    result_payload, result_digest, error_code, final_failed_reason,
                    is_workflow_task, history_schema_version,
                    attempt_archive_version, attempt_snapshot_codec,
                    attempt_snapshot_content_type, attempt_snapshot,
                    attempt_snapshot_digest,
                    rerun_input_disposition, rerun_input_version,
                    rerun_input_codec, rerun_input_content_type,
                    rerun_input_inline, rerun_input_digest
                ) VALUES (
                    $1, 'p10.history', 'default', 100,
                    1, $2, 'FAILED', 'FAIL_RUNNING', $3, $3, 'standard_30d',
                    $3, $3, $3, $3, 0, 0, 'worker-p10', 'host-p10', 10,
                    1, 'json-utf8', 'application/json', $4, $5,
                    'BOOM', 'failed', FALSE, 1,
                    1, 'json-utf8', 'application/json', $6, $7,
                    'INLINE', 1, 'json-utf8', 'application/json', $8, $9
                )
                "#
            ))
            .bind(id)
            .bind(vec![index as u8; 32])
            .bind(anchor)
            .bind(&payload)
            .bind(archive_digest(&payload).to_vec())
            .bind(b"[]".as_slice())
            .bind(archive_digest(b"[]").to_vec())
            .bind(b"{\"args\":[]}".as_slice())
            .bind(archive_digest(b"{\"args\":[]}").to_vec())
            .execute(pool)
            .await
            .unwrap();
            ids.push(id);
        }
    }
    ids
}

async fn begin(pool: &PgPool, session_id: Uuid) {
    let mut transaction = pool.begin().await.unwrap();
    begin_transcode_maintenance(&mut transaction, session_id)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
}

async fn plan_result(
    pool: &PgPool,
    job_id: Uuid,
    source_version: i16,
    target_version: i16,
    source_codec: &str,
    target_codec: &str,
) -> super::outcomes::TranscodePlan {
    let mut transaction = pool.begin().await.unwrap();
    let outcome = plan_transcode(
        &mut transaction,
        job_id,
        ArchiveComponent::Result,
        source_version,
        target_version,
        source_codec,
        target_codec,
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    match outcome {
        TranscodePlanOutcome::Planned(plan) => plan,
        other => panic!("expected plan, got {other:?}"),
    }
}

async fn complete_component(
    pool: &PgPool,
    component: ArchiveComponent,
    source_version: i16,
    target_version: i16,
    source_codec: &str,
    target_codec: &str,
) {
    let session_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();
    begin(pool, session_id).await;
    let mut transaction = pool.begin().await.unwrap();
    let plan = plan_transcode(
        &mut transaction,
        job_id,
        component,
        source_version,
        target_version,
        source_codec,
        target_codec,
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        plan,
        TranscodePlanOutcome::Planned(ref planned)
            if planned.transformed_rows == 1 && planned.copied_rows == 1
    ));
    assert_eq!(copy_all(pool, job_id, 1).await, vec![1]);
    assert!(verify(pool, job_id).await.verified);
    swap(pool, job_id).await;
    finalize_and_finish(pool, job_id, session_id).await;
}

async fn copy_all(pool: &PgPool, job_id: Uuid, batch_size: i64) -> Vec<i32> {
    let mut batches = Vec::new();
    loop {
        let mut transaction = pool.begin().await.unwrap();
        match run_copy_batch(&mut transaction, job_id, batch_size)
            .await
            .unwrap()
        {
            TranscodeCopyOutcome::Batch(batch) => {
                assert!(batch.rows_copied > 0);
                batches.push(batch.rows_copied);
                transaction.commit().await.unwrap();
            }
            TranscodeCopyOutcome::Ready(_) => {
                transaction.commit().await.unwrap();
                return batches;
            }
            other => panic!("copy refused: {other:?}"),
        }
    }
}

async fn verify(pool: &PgPool, job_id: Uuid) -> super::outcomes::TranscodeVerification {
    let mut transaction = pool.begin().await.unwrap();
    let report = verify_transcode(&mut transaction, job_id).await.unwrap();
    transaction.commit().await.unwrap();
    report
}

async fn swap(pool: &PgPool, job_id: Uuid) {
    let mut transaction = pool.begin().await.unwrap();
    assert!(matches!(
        swap_transcode(&mut transaction, job_id).await.unwrap(),
        TranscodeSwapOutcome::Swapped(_)
    ));
    transaction.commit().await.unwrap();
}

async fn finalize_and_finish(pool: &PgPool, job_id: Uuid, session_id: Uuid) {
    let mut transaction = pool.begin().await.unwrap();
    let finalized = finalize_transcode(&mut transaction, job_id).await.unwrap();
    assert!(finalized.decoder_retirement_ready);
    transaction.commit().await.unwrap();
    let mut transaction = pool.begin().await.unwrap();
    finish_transcode_maintenance(&mut transaction, session_id)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
}

#[test]
fn vocabulary_transforms_signature_and_cli_are_pinned() {
    assert_eq!(SWAP_LOCK_ATTEMPTS_MAXIMUM, 120);
    assert_eq!(SWAP_RETRY_BACKOFF_SECONDS, 0.25);
    assert_eq!(SWAP_LOCK_SECONDS_MAXIMUM, 2.0);
    assert_eq!(MAINTENANCE_SECONDS_MAXIMUM, 600.0);
    assert_eq!(BLOCKER_QUERY_TRUNCATION_CHARS, 1024);
    assert_eq!(SwapLockMode::Parent.as_str(), "ACCESS_EXCLUSIVE");
    assert_eq!(SwapLockMode::Leaves.as_str(), "SHARE");
    assert_eq!(SwapLockMode::LeafAdvisory.as_str(), "ADVISORY");
    for component in ArchiveComponent::ALL {
        assert_eq!(ArchiveComponent::parse(component.as_str()), Some(component));
        let columns = component_columns(component);
        assert!(!columns.version.is_empty());
        let encoded = encoded_source_select(component, "source", 1, "json-utf8", true).unwrap();
        let decoded = encoded_source_select(component, "source", 2, "framed-v2", false).unwrap();
        match component {
            ArchiveComponent::HistoryRow => {
                assert_eq!(encoded, "source.*");
                assert_eq!(decoded, "source.*");
            }
            _ => {
                assert!(encoded.contains("decode('4832', 'hex') ||"));
                assert!(decoded.contains("substring("));
            }
        }
    }
    assert_eq!(ArchiveComponent::parse("UNKNOWN"), None);
    let rerun = component_columns(ArchiveComponent::RerunInput);
    assert!(rerun.presence_predicate.contains("rerun_input_disposition"));
    assert!(!rerun.presence_predicate.contains("rerun_input_form"));
    assert_eq!(
        RerunInputDisposition::ALL.map(RerunInputDisposition::as_str),
        [
            "INLINE",
            "REFERENCE",
            "DECLINED_BY_POLICY",
            "OVER_BOUND",
            "NEVER_ELIGIBLE",
        ]
    );
    let transformed = transformed_select(
        &[
            "task_id".to_owned(),
            "attempt_archive_version".to_owned(),
            "attempt_snapshot_codec".to_owned(),
            "attempt_snapshot".to_owned(),
            "attempt_snapshot_digest".to_owned(),
        ],
        ArchiveComponent::Attempts,
        1,
        "json-utf8",
        2,
        "framed-v2",
        "source",
    )
    .unwrap();
    assert!(transformed.contains("sha256("));
    assert!(transformed.contains("archive_target_attempt_snapshot"));
    assert!(quoted_identifier("").is_err());
    assert!(quoted_identifier("bad; DROP TABLE x").is_err());
    assert_eq!(
        quoted_identifier("attempt_snapshot").unwrap(),
        "\"attempt_snapshot\""
    );
    for state in TranscodeJobState::ALL {
        assert_eq!(TranscodeJobState::parse(state.as_str()), Some(state));
    }
    assert_eq!(TranscodeJobState::parse("UNKNOWN"), None);
    let capture_failed = build_swap_exhausted(
        Uuid::nil(),
        TranscodeSwapBusy {
            job_id: Uuid::nil(),
            lock_mode: SwapLockMode::Parent,
            relation_names: vec!["parent".to_owned()],
        },
        120,
        Duration::from_millis(250),
        None,
    );
    assert!(capture_failed.blocker_capture_failed);
    assert!(capture_failed.blockers.is_empty());
    assert_eq!(capture_failed.retry_sleep_seconds, 29.75);
    assert!(RELATION_SCHEMA_SIGNATURE_SQL.contains("pg_get_expr(defaults.adbin"));
    assert!(RELATION_SCHEMA_SIGNATURE_SQL.contains("pg_get_constraintdef"));
    assert!(RELATION_SCHEMA_SIGNATURE_SQL.contains("pg_get_indexdef"));
    assert!(RELATION_SCHEMA_SIGNATURE_SQL.contains("pg_get_triggerdef"));
    let id = Uuid::nil();
    assert_eq!(
        replacement_relation_name(id, 2),
        "archive_replacement_000000000000_2"
    );
    assert_eq!(
        backup_relation_name(id, 2),
        "archive_replaced_000000000000_2"
    );

    for command in [
        "begin", "plan", "copy", "verify", "swap", "finalize", "finish", "status", "run",
    ] {
        let result = Cli::try_parse_from(["horsies", "transcode", command, "--help"]);
        assert!(result.is_err_and(|error| {
            error.kind() == clap::error::ErrorKind::DisplayHelp
                && error.to_string().contains(command)
        }));
    }
    let parsed = Cli::try_parse_from([
        "horsies",
        "transcode",
        "--database-url",
        "postgresql://invalid/example",
        "status",
        "--job-id",
        "00000000-0000-0000-0000-000000000000",
    ])
    .unwrap();
    assert!(matches!(parsed.command, Command::Transcode(_)));
}

#[tokio::test]
#[serial]
async fn forward_reverse_pipeline_is_resumable_multi_relation_and_exact() {
    let database = P10Database::create().await;
    let ids = seed_result_rows(&database.pool, 3, 2).await;
    let before: Vec<(Uuid, Vec<u8>, Vec<u8>)> = sqlx::query_as(
        "SELECT task_id, result_payload, result_digest FROM horsies_task_history ORDER BY task_id",
    )
    .fetch_all(&database.pool)
    .await
    .unwrap();
    let original_relation_names: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT tableoid::regclass::text FROM horsies_task_history ORDER BY 1",
    )
    .fetch_all(&database.pool)
    .await
    .unwrap();
    assert_eq!(original_relation_names.len(), 2);

    let session_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();
    let mut no_maintenance = database.pool.begin().await.unwrap();
    assert!(matches!(
        plan_transcode(
            &mut no_maintenance,
            Uuid::new_v4(),
            ArchiveComponent::Result,
            1,
            2,
            "json-utf8",
            "framed-v2",
        )
        .await
        .unwrap(),
        TranscodePlanOutcome::Rejected(ref rejected)
            if rejected.reason == "archive maintenance is required"
    ));
    assert!(matches!(
        plan_transcode(
            &mut no_maintenance,
            Uuid::new_v4(),
            ArchiveComponent::Result,
            1,
            3,
            "json-utf8",
            "framed-v3",
        )
        .await
        .unwrap(),
        TranscodePlanOutcome::Rejected(ref rejected)
            if rejected.reason == "unsupported transcode direction"
    ));
    no_maintenance.rollback().await.unwrap();
    begin(&database.pool, session_id).await;
    let availability = sqlx::query("SELECT horsies_assert_archive_available()")
        .execute(&database.pool)
        .await;
    assert!(
        availability.is_err(),
        "archive reads must fail during maintenance"
    );
    sqlx::query(
        "UPDATE horsies_task_history
         SET result_digest = decode(repeat('00', 32), 'hex')
         WHERE task_id = $1",
    )
    .bind(ids[0])
    .execute(&database.pool)
    .await
    .unwrap();
    let mut corrupt_tx = database.pool.begin().await.unwrap();
    assert!(matches!(
        plan_transcode(
            &mut corrupt_tx,
            Uuid::new_v4(),
            ArchiveComponent::Result,
            1,
            2,
            "json-utf8",
            "framed-v2",
        )
        .await
        .unwrap(),
        TranscodePlanOutcome::Rejected(ref rejected)
            if rejected.reason == "source rows fail component validity"
                && rejected.affected_rows == 1
    ));
    corrupt_tx.rollback().await.unwrap();
    sqlx::query(
        "UPDATE horsies_task_history
         SET result_digest = sha256(result_payload)
         WHERE task_id = $1",
    )
    .bind(ids[0])
    .execute(&database.pool)
    .await
    .unwrap();
    let plan = plan_result(&database.pool, job_id, 1, 2, "json-utf8", "framed-v2").await;
    assert_eq!(plan.copied_rows, 5);
    assert_eq!(plan.transformed_rows, 5);
    assert_eq!(plan.relation_count, 2);
    assert_eq!(
        plan.peak_additional_disk_budget_bytes,
        (plan.affected_relation_bytes * 5 + 3) / 4
    );
    assert_eq!(
        plan.wal_budget_bytes,
        (plan.affected_relation_bytes * 3 + 1) / 2
    );
    assert_eq!(plan.rollback_wal_budget_bytes, plan.wal_budget_bytes);
    assert!(plan.reversible);
    let mut early_finish = database.pool.begin().await.unwrap();
    let early_finish_error = finish_transcode_maintenance(&mut early_finish, session_id)
        .await
        .unwrap_err();
    assert!(early_finish_error
        .to_string()
        .contains("unfinished replacement job"));
    early_finish.rollback().await.unwrap();

    let mut rejected_tx = database.pool.begin().await.unwrap();
    assert!(matches!(
        plan_transcode(
            &mut rejected_tx,
            Uuid::new_v4(),
            ArchiveComponent::Result,
            1,
            2,
            "json-utf8",
            "framed-v2",
        )
        .await
        .unwrap(),
        TranscodePlanOutcome::Rejected(ref rejected)
            if rejected.reason == "another replacement job is active"
    ));
    rejected_tx.rollback().await.unwrap();

    let mut first = database.pool.begin().await.unwrap();
    let first_batch = run_copy_batch(&mut first, job_id, 2).await.unwrap();
    assert!(
        matches!(first_batch, TranscodeCopyOutcome::Batch(ref batch) if batch.rows_copied == 2)
    );
    first.commit().await.unwrap();
    let batches = copy_all(&database.pool, job_id, 2).await;
    assert_eq!(batches.iter().sum::<i32>(), 3);
    let ledger: Vec<i32> = sqlx::query_scalar(&format!(
        "SELECT rows_copied FROM {TRANSCODE_BATCHES} WHERE job_id = $1 ORDER BY batch_number"
    ))
    .bind(job_id)
    .fetch_all(&database.pool)
    .await
    .unwrap();
    assert_eq!(ledger, vec![2, 1, 2]);

    let mut timezone_tx = database.pool.begin().await.unwrap();
    sqlx::query("SET LOCAL TIME ZONE 'Pacific/Auckland'")
        .execute(&mut *timezone_tx)
        .await
        .unwrap();
    let report = verify_transcode(&mut timezone_tx, job_id).await.unwrap();
    assert!(report.verified);
    assert_eq!(report.replacement_row_mismatches, 0);
    assert_eq!(report.invalid_target_rows, 0);
    let timezone: String = sqlx::query_scalar("SHOW TimeZone")
        .fetch_one(&mut *timezone_tx)
        .await
        .unwrap();
    assert_eq!(timezone, "Pacific/Auckland");
    timezone_tx.commit().await.unwrap();

    let relations = {
        let mut transaction = database.pool.begin().await.unwrap();
        let relations = job_relations(&mut transaction, job_id).await.unwrap();
        transaction.rollback().await.unwrap();
        relations
    };
    for relation in &relations {
        let signature = relation_schema_signature(
            &mut database.pool.acquire().await.unwrap(),
            relation.replacement_relation_oid.unwrap(),
        )
        .await
        .unwrap();
        assert!(signature.is_some());
        let index_columns: Vec<String> = sqlx::query_scalar(
            "SELECT attribute.attname
             FROM pg_index AS index
             JOIN pg_attribute AS attribute
               ON attribute.attrelid = index.indrelid
              AND attribute.attnum = ANY(index.indkey)
             WHERE index.indrelid = $1::oid ORDER BY attribute.attname",
        )
        .bind(relation.replacement_relation_oid.unwrap())
        .fetch_all(&database.pool)
        .await
        .unwrap();
        assert!(index_columns.iter().any(|column| column == "task_id"));
        assert!(index_columns.iter().any(|column| column == "enqueued_at"));
    }

    swap(&database.pool, job_id).await;
    let names_after_swap: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT tableoid::regclass::text FROM horsies_task_history ORDER BY 1",
    )
    .fetch_all(&database.pool)
    .await
    .unwrap();
    assert_eq!(names_after_swap, original_relation_names);
    for relation in &relations {
        let backup_exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(&relation.backup_relation_name)
            .fetch_one(&database.pool)
            .await
            .unwrap();
        assert!(backup_exists);
    }
    finalize_and_finish(&database.pool, job_id, session_id).await;
    for relation in &relations {
        let backup_exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(&relation.backup_relation_name)
            .fetch_one(&database.pool)
            .await
            .unwrap();
        assert!(!backup_exists);
    }
    let forward: Vec<(Uuid, i16, String, Vec<u8>, Vec<u8>)> = sqlx::query_as(
        "SELECT task_id, result_envelope_version, result_codec, result_payload, result_digest FROM horsies_task_history ORDER BY task_id",
    )
    .fetch_all(&database.pool)
    .await
    .unwrap();
    assert_eq!(forward.len(), ids.len());
    for ((_, old_payload, _), (_, version, codec, payload, digest)) in before.iter().zip(&forward) {
        assert_eq!((*version, codec.as_str()), (2, "framed-v2"));
        assert_eq!(
            payload,
            &[b'H', b'2']
                .into_iter()
                .chain(old_payload.iter().copied())
                .collect::<Vec<_>>()
        );
        assert_eq!(*digest, Sha256::digest(payload).to_vec());
    }
    let wal: i64 = sqlx::query_scalar(&format!(
        "SELECT wal_bytes FROM {TRANSCODE_JOBS} WHERE job_id = $1"
    ))
    .bind(job_id)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert!(wal > 0);
    sqlx::query("SELECT horsies_assert_archive_available()")
        .execute(&database.pool)
        .await
        .unwrap();

    let reverse_session = Uuid::new_v4();
    let reverse_job = Uuid::new_v4();
    begin(&database.pool, reverse_session).await;
    plan_result(&database.pool, reverse_job, 2, 1, "framed-v2", "json-utf8").await;
    assert_eq!(
        copy_all(&database.pool, reverse_job, 3)
            .await
            .iter()
            .sum::<i32>(),
        5
    );
    assert!(verify(&database.pool, reverse_job).await.verified);
    swap(&database.pool, reverse_job).await;
    finalize_and_finish(&database.pool, reverse_job, reverse_session).await;
    let after: Vec<(Uuid, Vec<u8>, Vec<u8>)> = sqlx::query_as(
        "SELECT task_id, result_payload, result_digest FROM horsies_task_history ORDER BY task_id",
    )
    .fetch_all(&database.pool)
    .await
    .unwrap();
    assert_eq!(after, before);
    database.destroy().await;
}

#[tokio::test]
#[serial]
async fn every_component_transcodes_forward_and_reverse_with_exact_bytes() {
    let database = P10Database::create().await;
    seed_result_rows(&database.pool, 1, 0).await;
    let before: (Vec<u8>, Vec<u8>, Vec<u8>) = sqlx::query_as(
        "SELECT result_payload, attempt_snapshot, rerun_input_inline
         FROM horsies_task_history",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();

    for (component, source_codec, target_codec) in [
        (ArchiveComponent::HistoryRow, "row-v1", "row-v2"),
        (ArchiveComponent::Result, "json-utf8", "framed-v2"),
        (ArchiveComponent::Attempts, "json-utf8", "framed-v2"),
        (ArchiveComponent::RerunInput, "json-utf8", "framed-v2"),
    ] {
        complete_component(&database.pool, component, 1, 2, source_codec, target_codec).await;
    }
    let forward: (
        i16,
        i16,
        String,
        Vec<u8>,
        Vec<u8>,
        i16,
        String,
        Vec<u8>,
        Vec<u8>,
        i16,
        String,
        Vec<u8>,
        Vec<u8>,
    ) = sqlx::query_as(
        "SELECT history_schema_version,
                result_envelope_version, result_codec,
                result_payload, result_digest,
                attempt_archive_version, attempt_snapshot_codec,
                attempt_snapshot, attempt_snapshot_digest,
                rerun_input_version, rerun_input_codec,
                rerun_input_inline, rerun_input_digest
         FROM horsies_task_history",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(forward.0, 2);
    for (version, codec, payload, digest, original) in [
        (forward.1, &forward.2, &forward.3, &forward.4, &before.0),
        (forward.5, &forward.6, &forward.7, &forward.8, &before.1),
        (forward.9, &forward.10, &forward.11, &forward.12, &before.2),
    ] {
        assert_eq!(version, 2);
        assert_eq!(codec, "framed-v2");
        assert_eq!(
            payload,
            &[b'H', b'2']
                .into_iter()
                .chain(original.iter().copied())
                .collect::<Vec<_>>()
        );
        assert_eq!(digest, &Sha256::digest(payload).to_vec());
    }

    for (component, source_codec, target_codec) in [
        (ArchiveComponent::RerunInput, "framed-v2", "json-utf8"),
        (ArchiveComponent::Attempts, "framed-v2", "json-utf8"),
        (ArchiveComponent::Result, "framed-v2", "json-utf8"),
        (ArchiveComponent::HistoryRow, "row-v2", "row-v1"),
    ] {
        complete_component(&database.pool, component, 2, 1, source_codec, target_codec).await;
    }
    let after: (
        i16,
        i16,
        String,
        Vec<u8>,
        i16,
        String,
        Vec<u8>,
        i16,
        String,
        Vec<u8>,
    ) = sqlx::query_as(
        "SELECT history_schema_version,
                    result_envelope_version, result_codec, result_payload,
                    attempt_archive_version, attempt_snapshot_codec, attempt_snapshot,
                    rerun_input_version, rerun_input_codec, rerun_input_inline
             FROM horsies_task_history",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(after.0, 1);
    assert_eq!(
        (after.1, after.2.as_str(), after.3),
        (1, "json-utf8", before.0)
    );
    assert_eq!(
        (after.4, after.5.as_str(), after.6),
        (1, "json-utf8", before.1)
    );
    assert_eq!(
        (after.7, after.8.as_str(), after.9),
        (1, "json-utf8", before.2)
    );
    database.destroy().await;
}

#[tokio::test]
#[serial]
async fn schema_signature_is_timezone_invariant_and_structure_sensitive() {
    let database = P10Database::create().await;
    sqlx::query(
        "CREATE TABLE transcode_sig_probe (
             id integer PRIMARY KEY,
             recorded_at timestamptz NOT NULL
                 DEFAULT TIMESTAMPTZ '2026-06-01 00:00:00+00',
             CHECK (recorded_at >= TIMESTAMPTZ '2026-01-01 00:00:00+00')
         )",
    )
    .execute(&database.pool)
    .await
    .unwrap();
    let oid: i64 = sqlx::query_scalar("SELECT to_regclass('transcode_sig_probe')::oid::bigint")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    let mut raw = Vec::new();
    let mut pinned = Vec::new();
    for timezone in ["Etc/GMT+12", "UTC", "Etc/GMT-12"] {
        let mut transaction = database.pool.begin().await.unwrap();
        sqlx::query("SELECT set_config('timezone', $1, true)")
            .bind(timezone)
            .execute(&mut *transaction)
            .await
            .unwrap();
        raw.push(
            sqlx::query_scalar::<_, String>(RELATION_SCHEMA_SIGNATURE_SQL)
                .bind(oid)
                .fetch_one(&mut *transaction)
                .await
                .unwrap(),
        );
        pinned.push(
            relation_schema_signature(&mut transaction, oid)
                .await
                .unwrap()
                .unwrap(),
        );
        let restored: String = sqlx::query_scalar("SHOW TimeZone")
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
        assert_eq!(restored, timezone);
        transaction.rollback().await.unwrap();
    }
    assert_ne!(raw[0], raw[2]);
    assert_eq!(pinned[0], pinned[1]);
    assert_eq!(pinned[1], pinned[2]);
    let mut connection = database.pool.acquire().await.unwrap();
    assert_eq!(
        relation_schema_signature(&mut connection, 999_999_999)
            .await
            .unwrap(),
        None
    );
    let before = relation_schema_signature(&mut connection, oid)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE transcode_sig_probe ADD COLUMN note text")
        .execute(&mut *connection)
        .await
        .unwrap();
    let after = relation_schema_signature(&mut connection, oid)
        .await
        .unwrap();
    assert_ne!(before, after);
    drop(connection);
    database.destroy().await;
}

#[tokio::test]
#[serial]
async fn verification_tokens_and_copy_refusals_fail_closed() {
    let database = P10Database::create().await;
    seed_result_rows(&database.pool, 3, 0).await;
    let session_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();
    begin(&database.pool, session_id).await;
    plan_result(&database.pool, job_id, 1, 2, "json-utf8", "framed-v2").await;
    copy_all(&database.pool, job_id, 2).await;
    assert!(verify(&database.pool, job_id).await.verified);
    let relation = {
        let mut transaction = database.pool.begin().await.unwrap();
        let relation = job_relations(&mut transaction, job_id)
            .await
            .unwrap()
            .remove(0);
        transaction.rollback().await.unwrap();
        relation
    };
    sqlx::query(&format!(
        "UPDATE \"{}\"
         SET result_digest = decode(repeat('00', 32), 'hex')
         WHERE task_id = (SELECT task_id FROM \"{}\" LIMIT 1)",
        relation.replacement_relation_name, relation.replacement_relation_name
    ))
    .execute(&database.pool)
    .await
    .unwrap();
    let invalid_target = verify(&database.pool, job_id).await;
    assert!(!invalid_target.verified);
    assert_eq!(invalid_target.replacement_row_mismatches, 1);
    assert_eq!(invalid_target.invalid_target_rows, 1);
    sqlx::query(&format!(
        "UPDATE \"{}\" SET result_digest = sha256(result_payload)",
        relation.replacement_relation_name
    ))
    .execute(&database.pool)
    .await
    .unwrap();
    assert!(verify(&database.pool, job_id).await.verified);
    sqlx::query(
        "UPDATE horsies_task_history_leaf_catalog
         SET detached_at = statement_timestamp()
         WHERE leaf_name = $1",
    )
    .bind(&relation.source_relation_name)
    .execute(&database.pool)
    .await
    .unwrap();
    let mut swap_tx = database.pool.begin().await.unwrap();
    let error = swap_transcode(&mut swap_tx, job_id).await.unwrap_err();
    assert!(error.to_string().contains("catalog attachment changed"));
    swap_tx.rollback().await.unwrap();
    sqlx::query(
        "UPDATE horsies_task_history_leaf_catalog
         SET detached_at = NULL
         WHERE leaf_name = $1",
    )
    .bind(&relation.source_relation_name)
    .execute(&database.pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "ALTER TABLE \"{}\" ADD COLUMN p10_schema_drift text",
        relation.replacement_relation_name
    ))
    .execute(&database.pool)
    .await
    .unwrap();
    let mut swap_tx = database.pool.begin().await.unwrap();
    let error = swap_transcode(&mut swap_tx, job_id).await.unwrap_err();
    assert!(error.to_string().contains("verification changed"));
    swap_tx.rollback().await.unwrap();
    sqlx::query(&format!(
        "ALTER TABLE \"{}\" DROP COLUMN p10_schema_drift",
        relation.replacement_relation_name
    ))
    .execute(&database.pool)
    .await
    .unwrap();
    assert!(verify(&database.pool, job_id).await.verified);
    sqlx::query(&format!(
        "UPDATE \"{}\" SET last_worker_pid = last_worker_pid + 1 WHERE task_id = (SELECT task_id FROM \"{}\" LIMIT 1)",
        relation.source_relation_name, relation.source_relation_name
    ))
    .execute(&database.pool)
    .await
    .unwrap();
    let mut swap_tx = database.pool.begin().await.unwrap();
    let error = swap_transcode(&mut swap_tx, job_id).await.unwrap_err();
    assert!(error.to_string().contains("verification changed"));
    swap_tx.rollback().await.unwrap();
    let report = verify(&database.pool, job_id).await;
    assert!(!report.verified);
    assert_eq!(report.replacement_row_mismatches, 1);
    database.destroy().await;

    let database = P10Database::create().await;
    seed_result_rows(&database.pool, 2, 0).await;
    let session_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();
    begin(&database.pool, session_id).await;
    plan_result(&database.pool, job_id, 1, 2, "json-utf8", "framed-v2").await;
    sqlx::query(
        "UPDATE horsies_task_history
         SET result_digest = decode(repeat('00', 32), 'hex')
         WHERE task_id = (SELECT task_id FROM horsies_task_history LIMIT 1)",
    )
    .execute(&database.pool)
    .await
    .unwrap();
    let mut corrupt_copy = database.pool.begin().await.unwrap();
    assert!(matches!(
        run_copy_batch(&mut corrupt_copy, job_id, 1).await.unwrap(),
        TranscodeCopyOutcome::Rejected(ref rejected)
            if rejected.kind.as_str() == "SOURCE_CORRUPT"
                && rejected.observed_rows == 1
    ));
    corrupt_copy.rollback().await.unwrap();
    sqlx::query("UPDATE horsies_task_history SET result_digest = sha256(result_payload)")
        .execute(&database.pool)
        .await
        .unwrap();
    let mut first = database.pool.begin().await.unwrap();
    assert!(matches!(
        run_copy_batch(&mut first, job_id, 1).await.unwrap(),
        TranscodeCopyOutcome::Batch(_)
    ));
    first.commit().await.unwrap();
    seed_result_rows(&database.pool, 1, 0).await;
    let mut copy_tx = database.pool.begin().await.unwrap();
    let first = run_copy_batch(&mut copy_tx, job_id, 10).await.unwrap();
    assert!(matches!(first, TranscodeCopyOutcome::Rejected(_)));
    copy_tx.rollback().await.unwrap();
    database.destroy().await;
}

#[tokio::test]
#[serial]
async fn swap_nowait_exhaustion_reports_parent_and_leaf_blockers() {
    let database = P10Database::create().await;
    seed_result_rows(&database.pool, 2, 0).await;
    let session_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();
    begin(&database.pool, session_id).await;
    plan_result(&database.pool, job_id, 1, 2, "json-utf8", "framed-v2").await;
    copy_all(&database.pool, job_id, 2).await;
    assert!(verify(&database.pool, job_id).await.verified);
    let relation = {
        let mut transaction = database.pool.begin().await.unwrap();
        let relation = job_relations(&mut transaction, job_id)
            .await
            .unwrap()
            .remove(0);
        transaction.rollback().await.unwrap();
        relation
    };

    let (class_key, lower_anchor): (String, chrono::DateTime<Utc>) = sqlx::query_as(
        "SELECT class_key, lower_anchor FROM horsies_task_history_leaf_catalog WHERE leaf_name = $1",
    )
    .bind(&relation.source_relation_name)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    let advisory_lock_sql = format!("SELECT pg_advisory_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let advisory_unlock_sql =
        format!("SELECT pg_advisory_unlock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let mut advisory_holder = database.pool.acquire().await.unwrap();
    sqlx::query(&advisory_lock_sql)
        .bind(&class_key)
        .bind(lower_anchor)
        .execute(&mut *advisory_holder)
        .await
        .unwrap();
    let exhausted = swap_with_retry_policy(&database.pool, job_id, 2, Duration::from_millis(1))
        .await
        .unwrap();
    let TranscodeSwapOutcome::Exhausted(exhausted) = exhausted else {
        panic!("expected advisory-lock exhaustion");
    };
    assert_eq!(exhausted.lock_mode, SwapLockMode::LeafAdvisory);
    assert_eq!(
        exhausted.relation_names,
        vec![relation.source_relation_name.clone()]
    );
    assert!(exhausted.blocker_capture_failed);
    let unlocked: bool = sqlx::query_scalar(&advisory_unlock_sql)
        .bind(&class_key)
        .bind(lower_anchor)
        .fetch_one(&mut *advisory_holder)
        .await
        .unwrap();
    assert!(unlocked);
    drop(advisory_holder);

    let mut parent_holder = database.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE horsies_task_history IN ACCESS SHARE MODE")
        .execute(&mut *parent_holder)
        .await
        .unwrap();
    let long_query = format!("SELECT 1 /*{}*/", "x".repeat(1400));
    sqlx::query(&long_query)
        .execute(&mut *parent_holder)
        .await
        .unwrap();
    let mut busy_tx = database.pool.begin().await.unwrap();
    assert!(matches!(
        swap_transcode(&mut busy_tx, job_id).await.unwrap(),
        TranscodeSwapOutcome::Busy(ref busy)
            if busy.lock_mode == SwapLockMode::Parent
                && busy.relation_names == vec![relation.parent_relation_name.clone()]
    ));
    busy_tx.commit().await.unwrap();
    let exhausted = swap_with_retry_policy(&database.pool, job_id, 2, Duration::from_millis(1))
        .await
        .unwrap();
    let TranscodeSwapOutcome::Exhausted(exhausted) = exhausted else {
        panic!("expected parent-lock exhaustion");
    };
    assert_eq!(exhausted.lock_mode, SwapLockMode::Parent);
    assert_eq!(exhausted.attempts, 2);
    assert_eq!(
        exhausted.relation_names,
        vec![relation.parent_relation_name.clone()]
    );
    assert!(!exhausted.blocker_capture_failed);
    assert!(exhausted.blockers.iter().any(|blocker| {
        blocker.relation_name == relation.parent_relation_name
            && blocker
                .query
                .as_ref()
                .is_some_and(|query| query.len() <= 1024)
    }));
    parent_holder.rollback().await.unwrap();

    let mut leaf_holder = database.pool.begin().await.unwrap();
    sqlx::query(&format!(
        "LOCK TABLE \"{}\" IN ROW EXCLUSIVE MODE",
        relation.replacement_relation_name
    ))
    .execute(&mut *leaf_holder)
    .await
    .unwrap();
    let exhausted = swap_with_retry_policy(&database.pool, job_id, 2, Duration::from_millis(1))
        .await
        .unwrap();
    let TranscodeSwapOutcome::Exhausted(exhausted) = exhausted else {
        panic!("expected leaf-lock exhaustion");
    };
    assert_eq!(exhausted.lock_mode, SwapLockMode::Leaves);
    assert_eq!(
        exhausted.relation_names,
        vec![
            relation.source_relation_name.clone(),
            relation.replacement_relation_name.clone()
        ]
    );
    assert!(exhausted
        .blockers
        .iter()
        .any(|blocker| blocker.relation_name == relation.replacement_relation_name));
    leaf_holder.rollback().await.unwrap();

    swap(&database.pool, job_id).await;
    finalize_and_finish(&database.pool, job_id, session_id).await;
    database.destroy().await;
}

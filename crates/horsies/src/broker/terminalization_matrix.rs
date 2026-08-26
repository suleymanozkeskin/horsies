//! Transition matrix for the v35 live-to-history terminalization program.
//!
//! Drives every installed function against seeded rows in each relevant
//! source state and asserts (a) the outcome variant and its evidence, (b)
//! the row's post-image, (c) replay within the equivalence class vs
//! cross-class foreign terminalization, (d) the batch input contracts, and
//! (e) the revert-proof properties. This module is the behavioral safety
//! net that must be green before any call site moves.
//!
//! Each test process owns a UUID-named disposable database. Within that
//! process, global discovery batches (pending expiry, orphan sweep) are
//! pre-drained before seeding so one serial test cannot affect another.

use chrono::{DateTime, Duration, Utc};
use serial_test::serial;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, Connection, PgConnection, PgPool};
use std::str::FromStr;
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::broker::migrations::run_horsies_migrations;
use crate::broker::terminalization::terminalize;
use crate::core::history::identity::reservations::{claim_key_reservation, ReservationClaim};
use crate::core::history::maintenance::coverage::{ensure_partition_coverage, CoverageOutcome};
use crate::core::history::maintenance::gate::{
    begin_archive_maintenance, finish_archive_maintenance,
};
use crate::core::history::reads::publisher::StagedLoaderPublisher;
use crate::core::lifecycle::outcomes::GuardEvidence;
use crate::core::lifecycle::{
    BatchSize, CallerHoldsRowLock, OwnedClaim, OwnedClaimBatch, PriorLockedRead,
    TerminalizationCommand, TerminalizationKind, TerminalizationOutcome, WorkerOwned,
};
use crate::core::types::status::TaskStatus;

pub(super) const WIRE_PHASE2_EXPECTATIONS: [(&str, bool); 15] = [
    ("COMPLETE_LOCKED", true),
    ("COMPLETE_FUSED", false),
    ("FAIL_RUNNING", true),
    ("FAIL_STALE", true),
    ("EXPIRE_CLAIMED", true),
    ("EXPIRE_PENDING", true),
    ("CANCEL_ADMIN", false),
    ("CANCEL_ORPHAN", false),
    ("CANCEL_ORPHAN_SWEEP", false),
    ("PAUSE_ABANDON_CLAIM", false),
    ("PAUSE_ABANDON_CLAIM_BATCH", false),
    ("PAUSE_ABANDON_WORKFLOW", false),
    ("WORKFLOW_CANCEL_CLAIM", false),
    ("WORKFLOW_CANCEL_CLAIM_BATCH", false),
    ("WORKFLOW_CANCEL_WORKFLOW", false),
];

fn uuid(value: &str) -> Uuid {
    Uuid::parse_str(value).expect("test identity must be UUID")
}

fn plan_has_sequential_scan(plan: &serde_json::Value, relation: &str) -> bool {
    match plan {
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| plan_has_sequential_scan(value, relation)),
        serde_json::Value::Object(fields) => {
            let is_target_scan = fields.get("Node Type").and_then(serde_json::Value::as_str)
                == Some("Seq Scan")
                && fields
                    .get("Relation Name")
                    .and_then(serde_json::Value::as_str)
                    == Some(relation);
            is_target_scan
                || fields
                    .values()
                    .any(|value| plan_has_sequential_scan(value, relation))
        }
        _ => false,
    }
}

fn root_shared_buffers(plan: &serde_json::Value) -> u64 {
    let root = &plan[0]["Plan"];
    ["Shared Hit Blocks", "Shared Read Blocks"]
        .into_iter()
        .map(|field| root[field].as_u64().unwrap_or(0))
        .sum()
}

fn relation_rows_examined(plan: &serde_json::Value, relation: &str) -> f64 {
    match plan {
        serde_json::Value::Array(values) => values
            .iter()
            .map(|value| relation_rows_examined(value, relation))
            .sum(),
        serde_json::Value::Object(fields) => {
            let current = if fields
                .get("Relation Name")
                .and_then(serde_json::Value::as_str)
                == Some(relation)
            {
                let rows = [
                    "Actual Rows",
                    "Rows Removed by Filter",
                    "Rows Removed by Index Recheck",
                ]
                .into_iter()
                .map(|field| {
                    fields
                        .get(field)
                        .and_then(serde_json::Value::as_f64)
                        .unwrap_or(0.0)
                })
                .sum::<f64>();
                let loops = fields
                    .get("Actual Loops")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                rows * loops
            } else {
                0.0
            };
            current
                + fields
                    .values()
                    .map(|value| relation_rows_examined(value, relation))
                    .sum::<f64>()
        }
        _ => 0.0,
    }
}

fn test_db_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let root = std::path::Path::new(manifest_dir)
        .ancestors()
        .find(|p| p.join(".env").exists());
    let pw = root
        .and_then(|r| std::fs::read_to_string(r.join(".env")).ok())
        .and_then(|c| {
            c.lines()
                .filter_map(|l| l.trim().split_once('='))
                .find(|(k, _)| k.trim() == "DB_PASSWORD")
                .map(|(_, v)| v.trim().to_owned())
        })
        .unwrap_or_else(|| "W0rklane".to_owned());
    format!("postgresql://postgres:{pw}@localhost:5432/horsies-rust-port")
}

struct P5TestDatabase {
    name: String,
    url: String,
    _anchor: PgPool,
}

static P5_DATABASE: OnceCell<P5TestDatabase> = OnceCell::const_new();

pub(crate) async fn migrated_pool() -> PgPool {
    let database = P5_DATABASE
        .get_or_init(|| async {
            let base_options =
                PgConnectOptions::from_str(&test_db_url()).expect("invalid P5 database URL");
            let admin_options = base_options.clone().database("postgres");
            let database_name = format!("horsies_p5_matrix_{}", Uuid::new_v4().simple());
            let mut admin = PgConnection::connect_with(&admin_options)
                .await
                .expect("connect to P5 admin database");
            sqlx::query("SELECT pg_advisory_lock(hashtext('horsies_p5_matrix_setup'))")
                .execute(&mut admin)
                .await
                .expect("lock P5 database setup");
            let stale_databases: Vec<String> = sqlx::query_scalar(
                "SELECT d.datname
                 FROM pg_database d
                 WHERE left(d.datname, length('horsies_p5_matrix_')) =
                       'horsies_p5_matrix_'
                   AND NOT EXISTS (
                       SELECT 1 FROM pg_stat_activity a
                       WHERE a.datname = d.datname
                   )
                 ORDER BY d.datname",
            )
            .fetch_all(&mut admin)
            .await
            .expect("list inactive P5 databases");
            for stale_database in stale_databases {
                let suffix = stale_database
                    .strip_prefix("horsies_p5_matrix_")
                    .expect("query enforces P5 database prefix");
                assert!(
                    suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit()),
                    "refuse to drop non-generated P5 database {stale_database:?}"
                );
                sqlx::query(&format!("DROP DATABASE \"{stale_database}\""))
                    .execute(&mut admin)
                    .await
                    .expect("drop inactive P5 database");
            }
            sqlx::query(&format!("CREATE DATABASE \"{database_name}\""))
                .execute(&mut admin)
                .await
                .expect("create P5 database");
            let generated_options = base_options.database(&database_name);
            let database_url = generated_options.to_url_lossy().to_string();
            let anchor = PgPoolOptions::new()
                .min_connections(1)
                .max_connections(1)
                .max_lifetime(None)
                .idle_timeout(None)
                .connect_with(generated_options)
                .await
                .expect("connect P5 database");
            let unlocked: bool = sqlx::query_scalar(
                "SELECT pg_advisory_unlock(hashtext('horsies_p5_matrix_setup'))",
            )
            .fetch_one(&mut admin)
            .await
            .expect("unlock P5 database setup");
            assert!(unlocked, "P5 database setup lock was held");
            run_horsies_migrations(&anchor)
                .await
                .expect("migrate P5 database");
            let mut transaction = anchor.begin().await.expect("coverage transaction");
            let coverage =
                ensure_partition_coverage(&mut transaction, 2, 2, &[], &StagedLoaderPublisher)
                    .await
                    .expect("partition coverage");
            assert!(
                matches!(coverage, CoverageOutcome::Ensured(_)),
                "{coverage:?}"
            );
            transaction.commit().await.expect("commit coverage");
            P5TestDatabase {
                url: database_url,
                name: database_name,
                _anchor: anchor,
            }
        })
        .await;
    let base_options = PgConnectOptions::from_str(&test_db_url())
        .expect("invalid P5 database URL")
        .database(&database.name);
    PgPoolOptions::new()
        .max_connections(5)
        .connect_with(base_options)
        .await
        .expect("connect current P5 test runtime")
}

pub(crate) async fn migrated_database_url() -> String {
    let pool = migrated_pool().await;
    drop(pool);
    P5_DATABASE
        .get()
        .expect("migrated_pool initializes the P5 database")
        .url
        .clone()
}

struct Seed {
    status: &'static str,
    worker: Option<String>,
    claimed_at: Option<DateTime<Utc>>,
    good_until: Option<DateTime<Utc>>,
    is_workflow_task: bool,
    started_at: Option<DateTime<Utc>>,
    failed_reason: Option<String>,
    idempotency_key_digest: Option<Vec<u8>>,
}

impl Default for Seed {
    fn default() -> Self {
        Self {
            status: "RUNNING",
            worker: Some("w1".to_owned()),
            claimed_at: Some(Utc::now()),
            good_until: None,
            is_workflow_task: false,
            started_at: Some(Utc::now()),
            failed_reason: None,
            idempotency_key_digest: None,
        }
    }
}

async fn seed_task(pool: &PgPool, id: &str, seed: Seed) {
    let task_id = Uuid::parse_str(id).expect("UUID task id");
    sqlx::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, args, kwargs, status,
            sent_at, enqueued_at, started_at, claimed, claimed_at,
            claimed_by_worker_id, good_until, is_workflow_task,
            failed_reason, retry_count, max_retries, enqueue_sha,
            command_fingerprint_version, command_fingerprint,
            retention_class_key, idempotency_key_digest, retain_rerun_input,
            prepared_rerun_input_disposition, created_at, updated_at
        ) VALUES (
            $1, 'matrix_task', 'default', 100, '[]', '{}', $2,
            NOW(), NOW(), $3, $4 IS NOT NULL, $5,
            $4, $6, $7,
            $8, 0, 3, $1::text, 1, $9,
            'forever', $10, TRUE, 'DECLINED_BY_POLICY', NOW(), NOW()
        )",
    )
    .bind(task_id)
    .bind(seed.status)
    .bind(seed.started_at)
    .bind(&seed.worker)
    .bind(seed.claimed_at)
    .bind(seed.good_until)
    .bind(seed.is_workflow_task)
    .bind(&seed.failed_reason)
    .bind(vec![7_u8; 32])
    .bind(seed.idempotency_key_digest)
    .execute(pool)
    .await
    .expect("seed task");
}

async fn seed_workflow(pool: &PgPool, id: &str, status: &str) {
    sqlx::query(
        "INSERT INTO horsies_workflows (
            id, name, status, on_error, output_task_index,
            definition_key, depth, root_workflow_id,
            sent_at, created_at, started_at, updated_at
        ) VALUES (
            $1, 'matrix_wf', $2, 'fail', NULL, 'test.matrix.v1', 0, $1,
            NOW(), NOW(), NOW(), NOW()
        )",
    )
    .bind(Uuid::parse_str(id).expect("UUID workflow id"))
    .bind(status)
    .execute(pool)
    .await
    .expect("seed workflow");
}

async fn seed_wf_task(pool: &PgPool, wf_id: &str, task_id: &str, status: &str) {
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
            id, workflow_id, task_index, node_id, task_name, task_args,
            task_kwargs, queue_name, priority, dependencies, allow_failed_deps,
            join_type, status, is_subworkflow, task_id, created_at
        ) VALUES (
            $1, $2,
            COALESCE((SELECT MAX(task_index) + 1 FROM horsies_workflow_tasks
                      WHERE workflow_id = $2), 0),
            'node_' || $1, 'matrix_task', '[]', '{}',
            'default', 100, '{}', FALSE, 'all', $3, FALSE, $4, NOW()
        )",
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::parse_str(wf_id).expect("UUID workflow id"))
    .bind(status)
    .bind(Uuid::parse_str(task_id).expect("UUID task id"))
    .execute(pool)
    .await
    .expect("seed workflow task");
}

async fn seed_linked_workflow_task_at(
    pool: &PgPool,
    workflow_id: &str,
    task_id: &str,
    created_at: DateTime<Utc>,
) {
    seed_task(
        pool,
        task_id,
        Seed {
            status: "PENDING",
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    sqlx::query("UPDATE horsies_tasks SET created_at = $2 WHERE id = $1")
        .bind(uuid(task_id))
        .bind(created_at)
        .execute(pool)
        .await
        .expect("set task scan order");
    seed_wf_task(pool, workflow_id, task_id, "PENDING").await;
}

#[derive(Debug, sqlx::FromRow)]
struct PostImage {
    status: String,
    terminal_at: Option<DateTime<Utc>>,
    terminalization_kind: Option<String>,
    error_code: Option<String>,
    failed_reason: Option<String>,
    claimed_by_worker_id: Option<String>,
}

async fn post_image(pool: &PgPool, id: &str) -> PostImage {
    sqlx::query_as(
        "SELECT status, terminal_at, terminalization_kind, error_code,
                failed_reason, claimed_by_worker_id
         FROM horsies_tasks WHERE id = $1
         UNION ALL
         SELECT status, terminal_at, terminalization_kind, error_code,
                final_failed_reason AS failed_reason,
                last_claimed_worker_id AS claimed_by_worker_id
         FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(Uuid::parse_str(id).expect("UUID task id"))
    .fetch_one(pool)
    .await
    .expect("post image")
}

async fn assert_phase2_presence(pool: &PgPool, id: &str, expected: bool) {
    let present: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = $1
         )",
    )
    .bind(Uuid::parse_str(id).expect("UUID task id"))
    .fetch_one(pool)
    .await
    .expect("phase2 presence");
    assert_eq!(present, expected, "phase2 presence for task {id}");
}

async fn cleanup(pool: &PgPool, ids: &[&str]) {
    for id in ids {
        let task_id = Uuid::parse_str(id).expect("UUID task id");
        sqlx::query("DELETE FROM horsies_workflow_phase2_pending WHERE task_id = $1")
            .bind(task_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_task_attempts WHERE task_id = $1")
            .bind(task_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE task_id = $1")
            .bind(task_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_key_reservations WHERE task_id = $1")
            .bind(task_id)
            .execute(pool)
            .await
            .ok();
    }
}

async fn cleanup_workflow(pool: &PgPool, wf_id: &str) {
    let workflow_id = Uuid::parse_str(wf_id).expect("UUID workflow id");
    sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
        .bind(workflow_id)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .execute(pool)
        .await
        .ok();
}

fn owned(worker: &str, claimed_at: Option<DateTime<Utc>>) -> OwnedClaim {
    OwnedClaim {
        worker_id: worker.to_owned(),
        claimed_at,
    }
}

// ---------------------------------------------------------------------------
// COMPLETED family
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn complete_locked_moves_to_history_and_preserves_last_claim_evidence() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    let claimed_at = Utc::now();
    seed_task(
        &pool,
        &id,
        Seed {
            failed_reason: Some("stale reason from an earlier attempt".to_owned()),
            claimed_at: Some(claimed_at),
            ..Seed::default()
        },
    )
    .await;

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CompleteLockedTask {
            task_id: uuid(&id),
            fence: PriorLockedRead {
                worker_id: "w1".to_owned(),
            },
            result_json: "{\"Ok\":1}".to_owned(),
        },
    )
    .await
    .expect("terminalize");

    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { kind: TerminalizationKind::CompleteLocked, observed, .. }]
            if observed.status == Some(TaskStatus::Running)
            && observed.worker_id.as_deref() == Some("w1")
    ));
    let image = post_image(&pool, &id).await;
    assert_eq!(image.status, "COMPLETED");
    assert!(image.terminal_at.is_some());
    assert_eq!(
        image.terminalization_kind.as_deref(),
        Some("COMPLETE_LOCKED")
    );
    assert_eq!(image.error_code, None);
    assert_eq!(image.failed_reason, None, "completion clears failed_reason");
    // The live row is gone; the immutable projection retains its last owner.
    assert_eq!(image.claimed_by_worker_id.as_deref(), Some("w1"));

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn complete_replay_within_class_is_already_applied() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    seed_task(&pool, &id, Seed::default()).await;
    terminalize(
        &pool,
        &TerminalizationCommand::CompleteTaskFused {
            task_id: uuid(&id),
            fence: owned("w1", None),
            result_json: "{}".to_owned(),
            notify_channel: "matrix_unused".to_owned(),
            notify_payload: "x".to_owned(),
        },
    )
    .await
    .expect("first completion");

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CompleteLockedTask {
            task_id: uuid(&id),
            fence: PriorLockedRead {
                worker_id: "w1".to_owned(),
            },
            result_json: "{}".to_owned(),
        },
    )
    .await
    .expect("terminalize");

    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::AlreadyApplied {
            kind: TerminalizationKind::CompleteFused,
            ..
        }]
    ));

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn complete_locked_wrong_worker_is_lost_claim_and_absent_is_absent() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    seed_task(&pool, &id, Seed::default()).await;

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CompleteLockedTask {
            task_id: uuid(&id),
            fence: PriorLockedRead {
                worker_id: "w-other".to_owned(),
            },
            result_json: "{}".to_owned(),
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::LostClaim { observed, .. }]
            if observed.worker_id.as_deref() == Some("w1")
    ));
    assert_eq!(
        post_image(&pool, &id).await.status,
        "RUNNING",
        "refusal must not mutate"
    );

    let absent = terminalize(
        &pool,
        &TerminalizationCommand::CompleteLockedTask {
            task_id: Uuid::new_v4(),
            fence: PriorLockedRead {
                worker_id: "w1".to_owned(),
            },
            result_json: "{}".to_owned(),
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        absent.as_slice(),
        [TerminalizationOutcome::TaskAbsent { .. }]
    ));

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn fused_applies_with_attempt_notify_and_generation_fence() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    let claimed_at = Utc::now();
    seed_task(
        &pool,
        &id,
        Seed {
            claimed_at: Some(claimed_at),
            ..Seed::default()
        },
    )
    .await;

    let channel = format!("matrix_fused_{}", Uuid::new_v4().simple());
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool)
        .await
        .expect("listener");
    listener
        .listen("task_done")
        .await
        .expect("listen task_done");
    listener.listen(&channel).await.expect("listen");

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CompleteTaskFused {
            task_id: uuid(&id),
            fence: owned("w1", Some(claimed_at)),
            result_json: "{\"Ok\":7}".to_owned(),
            notify_channel: channel.clone(),
            notify_payload: format!("capacity:{id}"),
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied {
            kind: TerminalizationKind::CompleteFused,
            ..
        }]
    ));

    let image = post_image(&pool, &id).await;
    assert_eq!(image.status, "COMPLETED");
    assert_eq!(
        image.terminalization_kind.as_deref(),
        Some("COMPLETE_FUSED")
    );
    assert_phase2_presence(&pool, &id, false).await;

    let (version, codec, content_type, snapshot, digest): (i16, String, String, Vec<u8>, Vec<u8>) =
        sqlx::query_as(
            "SELECT attempt_archive_version, attempt_snapshot_codec,
                attempt_snapshot_content_type, attempt_snapshot,
                attempt_snapshot_digest
         FROM horsies_task_history WHERE task_id = $1",
        )
        .bind(Uuid::parse_str(&id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("history attempt snapshot");
    let attempts = crate::core::history::archive::attempts::decode_attempt_snapshot(
        version,
        &codec,
        &content_type,
        &snapshot,
        &digest,
    )
    .expect("decode archived attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].attempt(), 1);
    assert_eq!(attempts[0].outcome(), "COMPLETED");
    let live_attempts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM horsies_task_attempts WHERE task_id = $1")
            .bind(Uuid::parse_str(&id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("live attempt count");
    assert_eq!(
        live_attempts, 0,
        "attempt snapshot is the only retained home"
    );

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), listener.recv())
        .await
        .expect("first notify within timeout")
        .expect("first notification");
    let second = tokio::time::timeout(std::time::Duration::from_secs(5), listener.recv())
        .await
        .expect("second notify within timeout")
        .expect("second notification");
    let capacity_payload = format!("capacity:{id}");
    let notifications = [
        (first.channel(), first.payload()),
        (second.channel(), second.payload()),
    ];
    assert!(notifications.contains(&("task_done", id.as_str())));
    assert!(notifications.contains(&(channel.as_str(), capacity_payload.as_str())));

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn move_terminalizes_the_key_reservation_at_the_history_anchor() {
    let pool = migrated_pool().await;
    let task_uuid = Uuid::new_v4();
    let id = task_uuid.to_string();
    let key_digest = vec![11_u8; 32];
    seed_task(
        &pool,
        &id,
        Seed {
            idempotency_key_digest: Some(key_digest.clone()),
            ..Seed::default()
        },
    )
    .await;

    let mut connection = pool.acquire().await.expect("reservation connection");
    let claim = claim_key_reservation(
        &mut connection,
        &key_digest,
        1,
        3600,
        1,
        &[7_u8; 32],
        task_uuid,
    )
    .await
    .expect("claim reservation");
    assert!(matches!(claim, ReservationClaim::Applied { task_id } if task_id == task_uuid));
    drop(connection);

    terminalize(
        &pool,
        &TerminalizationCommand::CompleteTaskFused {
            task_id: uuid(&id),
            fence: owned("w1", None),
            result_json: "{}".to_owned(),
            notify_channel: "matrix_unused".to_owned(),
            notify_payload: "x".to_owned(),
        },
    )
    .await
    .expect("terminalize");

    let (disposition, expires_at, terminal_at): (String, DateTime<Utc>, DateTime<Utc>) =
        sqlx::query_as(
            "SELECT r.disposition, r.expires_at, h.terminal_at
             FROM horsies_key_reservations r
             JOIN horsies_task_history h ON h.task_id = r.task_id
             WHERE r.idempotency_key_digest = $1",
        )
        .bind(&key_digest)
        .fetch_one(&pool)
        .await
        .expect("terminal reservation");
    assert_eq!(disposition, "TERMINAL");
    assert_eq!(expires_at, terminal_at + Duration::hours(1));

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn archive_availability_refusal_rolls_back_the_whole_move() {
    let pool = migrated_pool().await;
    let task_uuid = Uuid::new_v4();
    let id = task_uuid.to_string();
    let workflow_id = Uuid::new_v4().to_string();
    let key_digest = vec![19_u8; 32];
    seed_workflow(&pool, &workflow_id, "RUNNING").await;
    seed_task(
        &pool,
        &id,
        Seed {
            is_workflow_task: true,
            idempotency_key_digest: Some(key_digest.clone()),
            ..Seed::default()
        },
    )
    .await;
    seed_wf_task(&pool, &workflow_id, &id, "RUNNING").await;
    sqlx::query(
        "INSERT INTO horsies_task_attempts (
             task_id, attempt, outcome, will_retry, started_at, finished_at,
             error_code, error_message, failed_reason, worker_id
         ) VALUES ($1, 1, 'FAILED', TRUE, NOW() - interval '2 minutes',
                   NOW() - interval '1 minute', 'RETRY', 'retry once',
                   'transient', 'w1')",
    )
    .bind(task_uuid)
    .execute(&pool)
    .await
    .expect("seed retained attempt");
    let mut reservation_connection = pool.acquire().await.expect("reservation connection");
    let claim = claim_key_reservation(
        &mut reservation_connection,
        &key_digest,
        1,
        3600,
        1,
        &[7_u8; 32],
        task_uuid,
    )
    .await
    .expect("claim live reservation");
    assert!(matches!(claim, ReservationClaim::Applied { task_id } if task_id == task_uuid));
    drop(reservation_connection);
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool)
        .await
        .expect("task_done listener");
    listener
        .listen("task_done")
        .await
        .expect("listen task_done");

    let session_id = Uuid::new_v4();
    let mut maintenance = pool.begin().await.expect("maintenance transaction");
    begin_archive_maintenance(&mut maintenance, session_id)
        .await
        .expect("begin archive maintenance");
    maintenance
        .commit()
        .await
        .expect("publish maintenance session");

    let error = terminalize(
        &pool,
        &TerminalizationCommand::CompleteLockedTask {
            task_id: uuid(&id),
            fence: PriorLockedRead {
                worker_id: "w1".to_owned(),
            },
            result_json: "{}".to_owned(),
        },
    )
    .await
    .expect_err("active archive maintenance refuses terminalization");
    assert!(error.to_string().contains("archive maintenance"), "{error}");

    let (live, history, attempts, pending, reservation_disposition, reservation_expires_at): (
        i64,
        i64,
        i64,
        i64,
        String,
        Option<DateTime<Utc>>,
    ) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM horsies_tasks WHERE id = $1),
             (SELECT count(*) FROM horsies_task_history WHERE task_id = $1),
             (SELECT count(*) FROM horsies_task_attempts WHERE task_id = $1),
             (SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $1),
             (SELECT disposition FROM horsies_key_reservations
              WHERE idempotency_key_digest = $2),
             (SELECT expires_at FROM horsies_key_reservations
              WHERE idempotency_key_digest = $2)",
    )
    .bind(task_uuid)
    .bind(&key_digest)
    .fetch_one(&pool)
    .await
    .expect("atomic refusal state");
    let notification_absent =
        tokio::time::timeout(std::time::Duration::from_millis(150), listener.recv())
            .await
            .is_err();

    let mut finish = pool.begin().await.expect("finish maintenance transaction");
    finish_archive_maintenance(&mut finish, session_id)
        .await
        .expect("finish archive maintenance");
    finish.commit().await.expect("commit maintenance finish");
    assert_eq!((live, history, attempts, pending), (1, 0, 1, 0));
    assert_eq!(reservation_disposition, "LIVE");
    assert_eq!(reservation_expires_at, None);
    assert!(
        notification_absent,
        "refused move emits no task_done notification"
    );
    cleanup(&pool, &[&id]).await;
    cleanup_workflow(&pool, &workflow_id).await;
}

#[tokio::test]
#[serial]
async fn fused_stale_generation_is_lost_claim() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    seed_task(
        &pool,
        &id,
        Seed {
            claimed_at: Some(Utc::now()),
            ..Seed::default()
        },
    )
    .await;

    let stale = Utc::now() - Duration::hours(1);
    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CompleteTaskFused {
            task_id: uuid(&id),
            fence: owned("w1", Some(stale)),
            result_json: "{}".to_owned(),
            notify_channel: "matrix_unused".to_owned(),
            notify_payload: "x".to_owned(),
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::LostClaim { .. }]
    ));
    assert_eq!(post_image(&pool, &id).await.status, "RUNNING");
    let attempts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM horsies_task_attempts WHERE task_id = $1")
            .bind(Uuid::parse_str(&id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(attempts, 0, "refusal writes no attempt row");

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn fused_on_matching_claim_outside_source_state_is_conflict() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    let claimed_at = Utc::now();
    seed_task(
        &pool,
        &id,
        Seed {
            status: "CLAIMED",
            claimed_at: Some(claimed_at),
            ..Seed::default()
        },
    )
    .await;

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CompleteTaskFused {
            task_id: uuid(&id),
            fence: owned("w1", Some(claimed_at)),
            result_json: "{}".to_owned(),
            notify_channel: "matrix_unused".to_owned(),
            notify_payload: "x".to_owned(),
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::Claim(_),
            observed,
            ..
        }] if observed.status == Some(TaskStatus::Claimed)
    ));

    cleanup(&pool, &[&id]).await;
}

// ---------------------------------------------------------------------------
// FAILED family
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn fail_locked_assigns_failed_reason_unconditionally() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    seed_task(
        &pool,
        &id,
        Seed {
            failed_reason: Some("reason from a requeued earlier attempt".to_owned()),
            ..Seed::default()
        },
    )
    .await;

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::FailLockedTask {
            task_id: uuid(&id),
            fence: PriorLockedRead {
                worker_id: "w1".to_owned(),
            },
            result_json: "{\"Err\":{}}".to_owned(),
            error_code: Some("TASK_ERROR".to_owned()),
            failed_reason: None,
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied {
            kind: TerminalizationKind::FailRunning,
            ..
        }]
    ));

    let image = post_image(&pool, &id).await;
    assert_eq!(image.status, "FAILED");
    assert_eq!(image.error_code.as_deref(), Some("TASK_ERROR"));
    assert_eq!(
        image.failed_reason, None,
        "the terminal writer owns the final-attempt summary; None clears \
         a requeued attempt's leftover reason"
    );

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn cross_class_replay_names_the_foreign_kind() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    seed_task(
        &pool,
        &id,
        Seed {
            status: "PENDING",
            worker: None,
            claimed_at: None,
            ..Seed::default()
        },
    )
    .await;
    terminalize(
        &pool,
        &TerminalizationCommand::CancelLockedTask {
            task_id: uuid(&id),
            fence: CallerHoldsRowLock,
            permitted_source_statuses: vec![TaskStatus::Pending],
        },
    )
    .await
    .expect("first cancellation");

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::FailLockedTask {
            task_id: uuid(&id),
            fence: PriorLockedRead {
                worker_id: "w1".to_owned(),
            },
            result_json: "{}".to_owned(),
            error_code: None,
            failed_reason: None,
        },
    )
    .await
    .expect("terminalize");

    let [TerminalizationOutcome::SourceStateConflict {
        evidence: GuardEvidence::ForeignTerminalization(foreign),
        ..
    }] = outcomes.as_slice()
    else {
        panic!("expected foreign terminalization, got {outcomes:?}");
    };
    assert_eq!(foreign.observed_status, TaskStatus::Cancelled);
    assert_eq!(
        foreign.committed_kind,
        Some(TerminalizationKind::CancelAdmin)
    );
    assert_eq!(
        post_image(&pool, &id).await.status,
        "CANCELLED",
        "terminal never overwritten"
    );

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn legacy_kind_is_foreign_and_never_inferred_as_a_wire_replay() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    seed_task(&pool, &id, Seed::default()).await;
    terminalize(
        &pool,
        &TerminalizationCommand::CompleteTaskFused {
            task_id: uuid(&id),
            fence: owned("w1", None),
            result_json: "{}".to_owned(),
            notify_channel: "matrix_unused".to_owned(),
            notify_payload: "x".to_owned(),
        },
    )
    .await
    .expect("first completion");
    sqlx::query(
        "UPDATE horsies_task_history
         SET terminalization_kind = 'LEGACY_TERMINAL'
         WHERE task_id = $1",
    )
    .bind(Uuid::parse_str(&id).unwrap())
    .execute(&pool)
    .await
    .expect("model relocated legacy provenance");

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::FailLockedTask {
            task_id: uuid(&id),
            fence: PriorLockedRead {
                worker_id: "w1".to_owned(),
            },
            result_json: "{}".to_owned(),
            error_code: None,
            failed_reason: None,
        },
    )
    .await
    .expect("terminalize");
    let [TerminalizationOutcome::SourceStateConflict {
        evidence: GuardEvidence::ForeignTerminalization(foreign),
        ..
    }] = outcomes.as_slice()
    else {
        panic!("expected conflict with unknown provenance, got {outcomes:?}");
    };
    assert_eq!(
        foreign.committed_kind,
        Some(TerminalizationKind::LegacyTerminal),
        "relocation-only provenance shares no wire equivalence class"
    );

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn fail_stale_applies_on_silence_and_refuses_on_heartbeat() {
    let pool = migrated_pool().await;
    let silent = Uuid::new_v4().to_string();
    let alive = Uuid::new_v4().to_string();
    let old = Utc::now() - Duration::hours(2);
    seed_task(
        &pool,
        &silent,
        Seed {
            started_at: Some(old),
            ..Seed::default()
        },
    )
    .await;
    seed_task(
        &pool,
        &alive,
        Seed {
            started_at: Some(old),
            ..Seed::default()
        },
    )
    .await;
    sqlx::query(
        "INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
         VALUES ($1, 'w1', 'runner', NOW(), 'h', 1)",
    )
    .bind(Uuid::parse_str(&alive).unwrap())
    .execute(&pool)
    .await
    .expect("heartbeat");

    let command = |task_id: &str| TerminalizationCommand::FailStaleTask {
        task_id: uuid(task_id),
        stale_after_ms: 60_000,
        finalizing_stale_after_ms: 60_000,
        result_json: "{\"Err\":{}}".to_owned(),
        error_code: "WORKER_CRASHED".to_owned(),
        failed_reason: "stale runner".to_owned(),
    };

    let outcomes = terminalize(&pool, &command(&silent))
        .await
        .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied {
            kind: TerminalizationKind::FailStale,
            ..
        }]
    ));
    let image = post_image(&pool, &silent).await;
    assert_eq!(image.status, "FAILED");
    assert_eq!(image.failed_reason.as_deref(), Some("stale runner"));

    let outcomes = terminalize(&pool, &command(&alive))
        .await
        .expect("terminalize");
    let [TerminalizationOutcome::SourceStateConflict {
        evidence: GuardEvidence::Staleness(staleness),
        ..
    }] = outcomes.as_slice()
    else {
        panic!("expected staleness refusal, got {outcomes:?}");
    };
    assert_eq!(staleness.stale_after_ms, 60_000);
    assert!(staleness.last_heartbeat_at.is_some());
    assert_eq!(post_image(&pool, &alive).await.status, "RUNNING");

    sqlx::query("DELETE FROM horsies_heartbeats WHERE task_id = $1")
        .bind(Uuid::parse_str(&alive).unwrap())
        .execute(&pool)
        .await
        .ok();
    cleanup(&pool, &[&silent, &alive]).await;
}

// ---------------------------------------------------------------------------
// EXPIRED family
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn expire_owned_claim_applies_for_any_generation_and_reports_deadline() {
    let pool = migrated_pool().await;
    let expired = Uuid::new_v4().to_string();
    let not_due = Uuid::new_v4().to_string();
    seed_task(
        &pool,
        &expired,
        Seed {
            status: "CLAIMED",
            claimed_at: Some(Utc::now() - Duration::minutes(30)),
            good_until: Some(Utc::now() - Duration::minutes(1)),
            ..Seed::default()
        },
    )
    .await;
    seed_task(
        &pool,
        &not_due,
        Seed {
            status: "CLAIMED",
            good_until: Some(Utc::now() + Duration::hours(1)),
            ..Seed::default()
        },
    )
    .await;

    let command = |task_id: &str| TerminalizationCommand::ExpireOwnedClaim {
        task_id: uuid(task_id),
        fence: WorkerOwned {
            worker_id: "w1".to_owned(),
        },
        result_json: "{\"Err\":{}}".to_owned(),
        error_code: "TASK_EXPIRED".to_owned(),
    };

    // No generation in the fence: the deadline makes expiry correct for
    // whichever generation holds the row.
    let outcomes = terminalize(&pool, &command(&expired))
        .await
        .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied {
            kind: TerminalizationKind::ExpireClaimed,
            ..
        }]
    ));
    let image = post_image(&pool, &expired).await;
    assert_eq!(image.status, "EXPIRED");
    assert_eq!(image.error_code.as_deref(), Some("TASK_EXPIRED"));
    assert_eq!(image.failed_reason, None);

    let outcomes = terminalize(&pool, &command(&not_due))
        .await
        .expect("terminalize");
    let [TerminalizationOutcome::SourceStateConflict {
        evidence: GuardEvidence::Deadline(deadline),
        ..
    }] = outcomes.as_slice()
    else {
        panic!("expected deadline refusal, got {outcomes:?}");
    };
    assert!(deadline.good_until.is_some());
    assert_eq!(post_image(&pool, &not_due).await.status, "CLAIMED");

    let foreign = terminalize(
        &pool,
        &TerminalizationCommand::ExpireOwnedClaim {
            task_id: uuid(&not_due),
            fence: WorkerOwned {
                worker_id: "w-other".to_owned(),
            },
            result_json: "{}".to_owned(),
            error_code: "TASK_EXPIRED".to_owned(),
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        foreign.as_slice(),
        [TerminalizationOutcome::LostClaim { .. }]
    ));

    cleanup(&pool, &[&expired, &not_due]).await;
}

async fn drain_pending_expiry(pool: &PgPool) {
    loop {
        let drained = terminalize(
            pool,
            &TerminalizationCommand::ExpirePendingTasks {
                batch_size: BatchSize::new(500).unwrap(),
                result_json: "{\"Err\":{}}".to_owned(),
                error_code: "TASK_EXPIRED".to_owned(),
            },
        )
        .await
        .expect("drain");
        if drained.len() < 500 {
            break;
        }
    }
}

#[tokio::test]
#[serial]
async fn expire_pending_batch_is_bounded_and_deadline_ordered() {
    let pool = migrated_pool().await;
    drain_pending_expiry(&pool).await;

    let ids: Vec<String> = (0..3).map(|_| Uuid::new_v4().to_string()).collect();
    for (index, id) in ids.iter().enumerate() {
        seed_task(
            &pool,
            id,
            Seed {
                status: "PENDING",
                worker: None,
                claimed_at: None,
                good_until: Some(Utc::now() - Duration::minutes(10 - index as i64)),
                ..Seed::default()
            },
        )
        .await;
    }

    let first = terminalize(
        &pool,
        &TerminalizationCommand::ExpirePendingTasks {
            batch_size: BatchSize::new(2).unwrap(),
            result_json: "{\"Err\":{}}".to_owned(),
            error_code: "TASK_EXPIRED".to_owned(),
        },
    )
    .await
    .expect("terminalize");
    let applied_ids: Vec<Uuid> = first.iter().map(|o| o.task_id()).collect();
    assert_eq!(
        applied_ids,
        vec![
            Uuid::parse_str(&ids[0]).unwrap(),
            Uuid::parse_str(&ids[1]).unwrap(),
        ],
        "batch takes the earliest deadlines first"
    );
    assert!(first.iter().all(|o| matches!(
        o,
        TerminalizationOutcome::Applied {
            kind: TerminalizationKind::ExpirePending,
            ..
        }
    )));
    assert_eq!(post_image(&pool, &ids[2]).await.status, "PENDING");

    let second = terminalize(
        &pool,
        &TerminalizationCommand::ExpirePendingTasks {
            batch_size: BatchSize::new(500).unwrap(),
            result_json: "{}".to_owned(),
            error_code: "TASK_EXPIRED".to_owned(),
        },
    )
    .await
    .expect("terminalize");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].task_id(), Uuid::parse_str(&ids[2]).unwrap());

    let empty = terminalize(
        &pool,
        &TerminalizationCommand::ExpirePendingTasks {
            batch_size: BatchSize::new(500).unwrap(),
            result_json: "{}".to_owned(),
            error_code: "TASK_EXPIRED".to_owned(),
        },
    )
    .await
    .expect("terminalize");
    assert!(empty.is_empty(), "zero eligible rows is a valid answer");

    let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    cleanup(&pool, &id_refs).await;
}

#[tokio::test]
#[serial]
async fn discovery_bounds_raise_in_function_too() {
    let pool = migrated_pool().await;
    for sql in [
        "SELECT * FROM horsies_expire_pending_tasks(NULL::integer, 'r', 'e')",
        "SELECT * FROM horsies_expire_pending_tasks(0, 'r', 'e')",
        "SELECT * FROM horsies_expire_pending_tasks(-5, 'r', 'e')",
        "SELECT * FROM horsies_cancel_orphaned_tasks(NULL::integer)",
        "SELECT * FROM horsies_cancel_orphaned_tasks(0)",
    ] {
        let err = sqlx::query(sql).fetch_all(&pool).await.expect_err(sql);
        let sqlx::Error::Database(db_err) = err else {
            panic!("{sql}: expected database error");
        };
        assert_eq!(db_err.code().as_deref(), Some("22023"), "{sql}");
    }
}

#[tokio::test]
#[serial]
async fn phase2_expectation_table_matches_every_installed_wire_projection() {
    let pool = migrated_pool().await;
    let mut vocabulary: Vec<&str> = TerminalizationKind::ALL
        .iter()
        .map(|kind| kind.as_str())
        .filter(|kind| *kind != "LEGACY_TERMINAL")
        .collect();
    vocabulary.sort_unstable();
    let mut expected_vocabulary: Vec<&str> = WIRE_PHASE2_EXPECTATIONS
        .iter()
        .map(|(kind, _)| *kind)
        .collect();
    expected_vocabulary.sort_unstable();
    assert_eq!(expected_vocabulary, vocabulary);

    let move_definition: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef(
             'horsies_move_task_to_history(uuid,text,text,timestamptz,text,text,text)'
             ::regprocedure
         )",
    )
    .fetch_one(&pool)
    .await
    .expect("shared move definition");
    for (kind, deferred) in WIRE_PHASE2_EXPECTATIONS {
        if matches!(kind, "EXPIRE_PENDING" | "CANCEL_ORPHAN_SWEEP") {
            continue;
        }
        let marker = format!("WHEN '{kind}' THEN");
        let start = move_definition
            .find(&marker)
            .expect("kind arm in shared move");
        let tail = &move_definition[start + marker.len()..];
        let end = tail
            .find("WHEN '")
            .or_else(|| tail.find("ELSE"))
            .unwrap_or(tail.len());
        let arm = &tail[..end];
        let assignment = if deferred {
            "v_requires_deferred_phase2 := TRUE"
        } else {
            "v_requires_deferred_phase2 := FALSE"
        };
        assert!(
            arm.contains(assignment),
            "{kind} projection must set {assignment}"
        );
    }

    for (function, deferred) in [
        ("horsies_expire_pending_tasks(integer,text,text)", true),
        ("horsies_cancel_orphaned_tasks(integer)", false),
    ] {
        let definition: String = sqlx::query_scalar("SELECT pg_get_functiondef($1::regprocedure)")
            .bind(function)
            .fetch_one(&pool)
            .await
            .expect("batch definition");
        assert_eq!(
            definition.contains("INSERT INTO horsies_workflow_phase2_pending"),
            deferred,
            "{function} phase2 projection"
        );
    }
}

// ---------------------------------------------------------------------------
// CANCELLED — administrative
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn cancel_locked_applies_within_permitted_statuses_only() {
    let pool = migrated_pool().await;
    let pending = Uuid::new_v4().to_string();
    let running = Uuid::new_v4().to_string();
    seed_task(
        &pool,
        &pending,
        Seed {
            status: "PENDING",
            worker: None,
            claimed_at: None,
            ..Seed::default()
        },
    )
    .await;
    seed_task(&pool, &running, Seed::default()).await;

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CancelLockedTask {
            task_id: uuid(&pending),
            fence: CallerHoldsRowLock,
            permitted_source_statuses: vec![TaskStatus::Pending, TaskStatus::Claimed],
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied {
            kind: TerminalizationKind::CancelAdmin,
            ..
        }]
    ));
    let image = post_image(&pool, &pending).await;
    assert_eq!(image.status, "CANCELLED");
    assert_eq!(image.error_code.as_deref(), Some("TASK_CANCELLED"));
    assert_eq!(
        image.failed_reason.as_deref(),
        Some("Cancelled via monitoring API")
    );
    assert_eq!(
        image.claimed_by_worker_id, None,
        "admin cancel clears the claim"
    );
    assert_phase2_presence(&pool, &pending, false).await;

    // RUNNING outside the permitted set: the operator's opt-in is explicit.
    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CancelLockedTask {
            task_id: uuid(&running),
            fence: CallerHoldsRowLock,
            permitted_source_statuses: vec![TaskStatus::Pending],
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::SourceStateConflict { .. }]
    ));
    assert_eq!(post_image(&pool, &running).await.status, "RUNNING");

    cleanup(&pool, &[&pending, &running]).await;
}

#[tokio::test]
#[serial]
async fn cancel_locked_refuses_workflow_tasks_and_never_overwrites_terminal() {
    let pool = migrated_pool().await;
    let wf_task = Uuid::new_v4().to_string();
    let done = Uuid::new_v4().to_string();
    seed_task(
        &pool,
        &wf_task,
        Seed {
            status: "PENDING",
            worker: None,
            claimed_at: None,
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    seed_task(&pool, &done, Seed::default()).await;
    terminalize(
        &pool,
        &TerminalizationCommand::CompleteTaskFused {
            task_id: uuid(&done),
            fence: owned("w1", None),
            result_json: "{}".to_owned(),
            notify_channel: "matrix_unused".to_owned(),
            notify_payload: "x".to_owned(),
        },
    )
    .await
    .expect("first completion");

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CancelLockedTask {
            task_id: uuid(&wf_task),
            fence: CallerHoldsRowLock,
            permitted_source_statuses: vec![TaskStatus::Pending],
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::SourceStateConflict { .. }]
    ));
    assert_eq!(post_image(&pool, &wf_task).await.status, "PENDING");
    assert_phase2_presence(&pool, &wf_task, false).await;

    // Revert-proof: a malformed permitted array naming a terminal status
    // cannot resurrect or overwrite the row.
    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CancelLockedTask {
            task_id: uuid(&done),
            fence: CallerHoldsRowLock,
            permitted_source_statuses: vec![TaskStatus::Completed],
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::ForeignTerminalization(_),
            ..
        }]
    ));
    let image = post_image(&pool, &done).await;
    assert_eq!(image.status, "COMPLETED");
    assert_eq!(
        image.terminalization_kind.as_deref(),
        Some("COMPLETE_FUSED")
    );

    cleanup(&pool, &[&wf_task, &done]).await;
}

// ---------------------------------------------------------------------------
// CANCELLED — orphan family
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn cancel_owned_orphan_applies_only_without_runnable_linkage() {
    let pool = migrated_pool().await;
    let wf_id = Uuid::new_v4().to_string();
    let orphan = Uuid::new_v4().to_string();
    let linked = Uuid::new_v4().to_string();
    let claimed_at = Utc::now();
    seed_workflow(&pool, &wf_id, "RUNNING").await;
    seed_task(
        &pool,
        &orphan,
        Seed {
            status: "CLAIMED",
            claimed_at: Some(claimed_at),
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    seed_task(
        &pool,
        &linked,
        Seed {
            status: "CLAIMED",
            claimed_at: Some(claimed_at),
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    seed_wf_task(&pool, &wf_id, &linked, "ENQUEUED").await;

    let command = |task_id: &str| TerminalizationCommand::CancelOwnedOrphan {
        task_id: uuid(task_id),
        fence: owned("w1", Some(claimed_at)),
    };

    let outcomes = terminalize(&pool, &command(&orphan))
        .await
        .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied {
            kind: TerminalizationKind::CancelOrphan,
            ..
        }]
    ));
    let image = post_image(&pool, &orphan).await;
    assert_eq!(image.status, "CANCELLED");
    assert_eq!(image.error_code.as_deref(), Some("WORKFLOW_CHECK_FAILED"));
    assert_phase2_presence(&pool, &orphan, false).await;

    // Revert-proof: the runnable-link guard retains linked tasks, and names
    // the node state that refused.
    let outcomes = terminalize(&pool, &command(&linked))
        .await
        .expect("terminalize");
    let [TerminalizationOutcome::SourceStateConflict {
        evidence: GuardEvidence::WorkflowLink(link),
        ..
    }] = outcomes.as_slice()
    else {
        panic!("expected workflow-link refusal, got {outcomes:?}");
    };
    assert_eq!(link.node_status.as_deref(), Some("ENQUEUED"));
    assert_eq!(post_image(&pool, &linked).await.status, "CLAIMED");

    cleanup(&pool, &[&orphan, &linked]).await;
    cleanup_workflow(&pool, &wf_id).await;
}

async fn drain_orphan_sweep(pool: &PgPool) {
    let initial_cycles: i64 = sqlx::query_scalar(
        "SELECT completed_cycles FROM horsies_recovery_scan_cursors
         WHERE scan_name = 'orphan_workflow_tasks'",
    )
    .fetch_one(pool)
    .await
    .expect("read orphan audit cycle");
    for _ in 0..10_000 {
        terminalize(
            pool,
            &TerminalizationCommand::CancelOrphanedTasks {
                batch_size: BatchSize::new(500).unwrap(),
            },
        )
        .await
        .expect("drain");
        let (completed_cycles, scanned): (i64, i32) = sqlx::query_as(
            "SELECT completed_cycles, last_scan_rows
             FROM horsies_recovery_scan_cursors
             WHERE scan_name = 'orphan_workflow_tasks'",
        )
        .fetch_one(pool)
        .await
        .expect("read orphan audit progress");
        if scanned == 0 || completed_cycles > initial_cycles {
            return;
        }
    }
    panic!("orphan audit did not complete one cursor cycle");
}

#[tokio::test]
#[serial]
async fn orphan_sweep_cancels_unlinked_and_retains_linked() {
    let pool = migrated_pool().await;
    drain_orphan_sweep(&pool).await;

    let wf_id = Uuid::new_v4().to_string();
    let orphan = Uuid::new_v4().to_string();
    let linked = Uuid::new_v4().to_string();
    seed_workflow(&pool, &wf_id, "RUNNING").await;
    seed_task(
        &pool,
        &orphan,
        Seed {
            status: "CLAIMED",
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    seed_task(
        &pool,
        &linked,
        Seed {
            status: "CLAIMED",
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    seed_wf_task(&pool, &wf_id, &linked, "RUNNING").await;

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CancelOrphanedTasks {
            batch_size: BatchSize::new(500).unwrap(),
        },
    )
    .await
    .expect("terminalize");
    let swept: Vec<Uuid> = outcomes.iter().map(|o| o.task_id()).collect();
    assert!(swept.contains(&Uuid::parse_str(&orphan).unwrap()));
    assert!(
        !swept.contains(&Uuid::parse_str(&linked).unwrap()),
        "runnable linkage retains the task"
    );
    assert!(outcomes.iter().all(|o| matches!(
        o,
        TerminalizationOutcome::Applied { kind: TerminalizationKind::CancelOrphanSweep, observed, .. }
            if observed.status == Some(TaskStatus::Claimed)
    )));
    assert_eq!(post_image(&pool, &orphan).await.status, "CANCELLED");
    assert_eq!(post_image(&pool, &linked).await.status, "CLAIMED");
    assert_phase2_presence(&pool, &orphan, false).await;

    cleanup(&pool, &[&orphan, &linked]).await;
    cleanup_workflow(&pool, &wf_id).await;
}

#[tokio::test]
#[serial]
async fn orphan_sweep_cursor_bounds_and_completes_a_fifty_thousand_row_audit() {
    let pool = migrated_pool().await;
    let workflow_id = Uuid::parse_str("30000000-0000-7000-8000-000000000001").unwrap();
    let orphan_id = Uuid::parse_str("40000000-0000-7000-8000-00000000c351").unwrap();
    sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
        .bind(workflow_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM horsies_tasks WHERE task_name = 'bounded_recovery_cursor_task'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
        .bind(orphan_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE horsies_recovery_scan_cursors
         SET last_created_at = NULL, last_id = NULL,
             cycle_upper_created_at = NULL, cycle_upper_id = NULL,
             claim_token = NULL, claim_expires_at = NULL,
             completed_cycles = 0,
             last_scan_rows = 0, last_candidate_rows = 0,
             last_scan_at = NULL
         WHERE scan_name = 'orphan_workflow_tasks'",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO horsies_workflows (
             id, name, status, on_error, output_task_index,
             definition_key, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at
         ) VALUES (
             $1, 'bounded_recovery_cursor_workflow', 'RUNNING', 'fail', NULL,
             'test.bounded-recovery.cursor.v1', 0, $1,
             NOW(), NOW(), NOW(), NOW()
         )",
    )
    .bind(workflow_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "WITH generated AS (
             SELECT g,
                    ('40000000-0000-7000-8000-' ||
                     lpad(to_hex(g), 12, '0'))::uuid AS task_id
             FROM generate_series(1, 50001) AS g
         )
         INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, args, kwargs, status,
             sent_at, enqueued_at, claimed, is_workflow_task,
             retry_count, max_retries, enqueue_sha,
             command_fingerprint_version, command_fingerprint,
             retention_class_key, retain_rerun_input,
             prepared_rerun_input_disposition, created_at, updated_at
         )
         SELECT task_id, 'bounded_recovery_cursor_task', 'default', 100, '[]', '{}',
                'PENDING', NOW(), NOW(), FALSE, TRUE,
                0, 3, task_id::text, 1, decode(repeat('07', 32), 'hex'),
                'forever', TRUE, 'DECLINED_BY_POLICY', NOW(), NOW()
         FROM generated",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "WITH generated AS (
             SELECT g,
                    ('40000000-0000-7000-8000-' ||
                     lpad(to_hex(g), 12, '0'))::uuid AS task_id,
                    ('50000000-0000-7000-8000-' ||
                     lpad(to_hex(g), 12, '0'))::uuid AS node_row_id
             FROM generate_series(1, 50000) AS g
         )
         INSERT INTO horsies_workflow_tasks (
             id, workflow_id, task_index, node_id, task_name,
             queue_name, priority, dependencies, allow_failed_deps,
             join_type, status, is_subworkflow, task_id, created_at
         )
         SELECT node_row_id, $1, g, 'node_' || g, 'bounded_recovery_cursor_task',
                'default', 100, '{}'::integer[], FALSE,
                'all', 'PENDING', FALSE, task_id, NOW()
         FROM generated",
    )
    .bind(workflow_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("ANALYZE horsies_tasks, horsies_workflow_tasks")
        .execute(&pool)
        .await
        .unwrap();

    let command = TerminalizationCommand::CancelOrphanedTasks {
        batch_size: BatchSize::new(500).unwrap(),
    };
    let first = terminalize(&pool, &command).await.unwrap();
    assert!(first.is_empty());
    let first_stats: (i32, i32) = sqlx::query_as(
        "SELECT last_scan_rows, last_candidate_rows
         FROM horsies_recovery_scan_cursors
         WHERE scan_name = 'orphan_workflow_tasks'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(first_stats, (500, 0));

    let mut explain_transaction = pool.begin().await.unwrap();
    sqlx::query(
        "WITH lower_bound AS (
             SELECT created_at, id
             FROM horsies_tasks
             WHERE is_workflow_task = TRUE
               AND status IN ('CLAIMED', 'PENDING')
             ORDER BY created_at, id
             OFFSET 49499 LIMIT 1
         ),
         upper_bound AS (
             SELECT created_at, id
             FROM horsies_tasks
             WHERE is_workflow_task = TRUE
               AND status IN ('CLAIMED', 'PENDING')
             ORDER BY created_at DESC, id DESC
             LIMIT 1
         )
         UPDATE horsies_recovery_scan_cursors cursor
         SET last_created_at = lower_bound.created_at,
             last_id = lower_bound.id,
             cycle_upper_created_at = upper_bound.created_at,
             cycle_upper_id = upper_bound.id
         FROM lower_bound, upper_bound
         WHERE cursor.scan_name = 'orphan_workflow_tasks'",
    )
    .execute(explain_transaction.as_mut())
    .await
    .unwrap();
    let production_plan: serde_json::Value = sqlx::query_scalar(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
         SELECT * FROM horsies_cancel_orphaned_tasks(500)",
    )
    .fetch_one(explain_transaction.as_mut())
    .await
    .unwrap();
    assert!(
        root_shared_buffers(&production_plan) <= 3_500,
        "the exact orphan function must not read the complete 50,001-row fixture: {production_plan}",
    );
    explain_transaction.rollback().await.unwrap();

    let mut discovery_transaction = pool.begin().await.unwrap();
    let plan: serde_json::Value = sqlx::query_scalar(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
         WITH cursor_row AS MATERIALIZED (
             SELECT last_created_at, last_id,
                    cycle_upper_created_at, cycle_upper_id
             FROM horsies_recovery_scan_cursors
             WHERE scan_name = 'orphan_workflow_tasks'
             FOR UPDATE NOWAIT
         ),
         upper_bound AS MATERIALIZED (
             SELECT COALESCE(c.cycle_upper_created_at, latest.created_at) AS created_at,
                    COALESCE(c.cycle_upper_id, latest.id) AS id
             FROM cursor_row c
             LEFT JOIN LATERAL (
                 SELECT t.created_at, t.id
                 FROM horsies_tasks t
                 WHERE c.cycle_upper_id IS NULL
                   AND t.is_workflow_task = TRUE
                   AND t.status IN ('CLAIMED', 'PENDING')
                 ORDER BY t.created_at DESC, t.id DESC
                 LIMIT 1
             ) latest ON TRUE
         ),
         scanned AS MATERIALIZED (
             SELECT page.created_at, page.id
             FROM cursor_row c
             CROSS JOIN upper_bound u
             CROSS JOIN LATERAL (
                 SELECT bounded.created_at, bounded.id
                 FROM (
                     (
                         SELECT t.created_at, t.id
                         FROM horsies_tasks t
                         WHERE c.last_id IS NULL
                           AND t.is_workflow_task = TRUE
                           AND t.status IN ('CLAIMED', 'PENDING')
                           AND u.id IS NOT NULL
                           AND (t.created_at, t.id) <= (u.created_at, u.id)
                         ORDER BY t.created_at, t.id
                         LIMIT CAST($1 AS integer)
                     )
                     UNION ALL
                     (
                         SELECT t.created_at, t.id
                         FROM horsies_tasks t
                         WHERE c.last_id IS NOT NULL
                           AND t.is_workflow_task = TRUE
                           AND t.status IN ('CLAIMED', 'PENDING')
                           AND u.id IS NOT NULL
                           AND (t.created_at, t.id)
                               > (c.last_created_at, c.last_id)
                           AND (t.created_at, t.id) <= (u.created_at, u.id)
                         ORDER BY t.created_at, t.id
                         LIMIT CAST($1 AS integer)
                     )
                 ) bounded
                 ORDER BY bounded.created_at, bounded.id
                 LIMIT CAST($1 AS integer)
             ) page
         ),
         scan_summary AS MATERIALIZED (
             SELECT count(*)::integer AS scan_count,
                    array_agg(created_at ORDER BY created_at, id) AS scan_created_at,
                    array_agg(id ORDER BY created_at, id) AS scan_ids
             FROM scanned
         ),
         candidates AS MATERIALIZED (
             SELECT candidate.id
             FROM scan_summary summary
             CROSS JOIN LATERAL
                  unnest(COALESCE(summary.scan_ids, '{}'::uuid[])) AS scanned(id)
             CROSS JOIN LATERAL (
                 SELECT t.id
                 FROM horsies_tasks t
                 LEFT JOIN LATERAL (
                     SELECT TRUE AS found
                     FROM horsies_workflow_tasks wt
                     WHERE wt.task_id = t.id
                       AND wt.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
                     LIMIT 1
                 ) runnable_link ON TRUE
                 WHERE t.id = scanned.id
                   AND t.is_workflow_task = TRUE
                   AND t.status IN ('CLAIMED', 'PENDING')
                   AND runnable_link.found IS NULL
                 LIMIT 1
                 FOR UPDATE OF t SKIP LOCKED
             ) candidate
         ),
         candidate_summary AS MATERIALIZED (
             SELECT COALESCE(array_agg(id ORDER BY id), '{}'::uuid[]) AS ids
             FROM candidates
         ),
         progress AS MATERIALIZED (
             SELECT summary.scan_count, summary.scan_created_at, summary.scan_ids,
                    candidates.ids, u.created_at AS upper_created_at, u.id AS upper_id,
                    summary.scan_count < CAST($1 AS integer)
                        OR (
                            summary.scan_count > 0
                            AND (
                                summary.scan_created_at[summary.scan_count],
                                summary.scan_ids[summary.scan_count]
                            ) = (u.created_at, u.id)
                        ) AS cycle_complete
             FROM scan_summary summary
             CROSS JOIN candidate_summary candidates
             CROSS JOIN upper_bound u
         ),
         advance AS (
             UPDATE horsies_recovery_scan_cursors cursor
             SET last_created_at = CASE WHEN progress.cycle_complete THEN NULL
                                        ELSE progress.scan_created_at[progress.scan_count] END,
                 last_id = CASE WHEN progress.cycle_complete THEN NULL
                                ELSE progress.scan_ids[progress.scan_count] END,
                 cycle_upper_created_at = CASE WHEN progress.cycle_complete THEN NULL
                                               ELSE progress.upper_created_at END,
                 cycle_upper_id = CASE WHEN progress.cycle_complete THEN NULL
                                       ELSE progress.upper_id END,
                 completed_cycles = completed_cycles
                     + CASE WHEN progress.cycle_complete THEN 1 ELSE 0 END,
                 last_scan_rows = progress.scan_count,
                 last_candidate_rows = cardinality(progress.ids),
                 last_scan_at = statement_timestamp()
             FROM progress
             WHERE cursor.scan_name = 'orphan_workflow_tasks'
             RETURNING progress.scan_count, progress.ids
         )
         SELECT * FROM advance",
    )
    .bind(500_i32)
    .fetch_one(discovery_transaction.as_mut())
    .await
    .unwrap();
    assert!(
        plan.to_string()
            .contains("idx_horsies_tasks_orphan_recovery_scan"),
        "orphan audit must use the partial scan index: {plan}",
    );
    assert!(
        plan.to_string().contains("idx_horsies_workflow_tasks_task"),
        "orphan audit must use workflow-task index probes: {plan}",
    );
    assert!(
        !plan_has_sequential_scan(&plan, "horsies_tasks")
            && !plan_has_sequential_scan(&plan, "horsies_workflow_tasks"),
        "orphan audit must not scan complete task tables: {plan}",
    );
    assert!(
        relation_rows_examined(&plan, "horsies_tasks") <= 1_001.0,
        "orphan audit must examine one task page, one upper bound, and one identity probe per page row: {plan}",
    );
    assert!(
        relation_rows_examined(&plan, "horsies_workflow_tasks") <= 500.0,
        "orphan audit must use at most one workflow-task probe per page row: {plan}",
    );
    discovery_transaction.rollback().await.unwrap();

    let mut found = false;
    for _ in 0..100 {
        let outcomes = terminalize(&pool, &command).await.unwrap();
        if outcomes
            .iter()
            .any(|outcome| outcome.task_id() == orphan_id)
        {
            found = true;
            break;
        }
    }
    assert!(found, "the cyclic audit must reach the final orphan");
    let linked_live: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM horsies_tasks
         WHERE task_name = 'bounded_recovery_cursor_task'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(linked_live, 50_000);

    sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
        .bind(workflow_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM horsies_tasks WHERE task_name = 'bounded_recovery_cursor_task'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
        .bind(orphan_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("ANALYZE horsies_tasks, horsies_workflow_tasks")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
#[serial]
async fn orphan_sweep_rollback_keeps_the_candidate_for_the_next_audit() {
    let pool = migrated_pool().await;
    let task_id = "00000000-0000-7000-8000-000000000001";
    cleanup(&pool, &[task_id]).await;
    sqlx::query(
        "UPDATE horsies_recovery_scan_cursors
         SET last_created_at = NULL, last_id = NULL,
             cycle_upper_created_at = NULL, cycle_upper_id = NULL,
             claim_token = NULL, claim_expires_at = NULL,
             completed_cycles = 0,
             last_scan_rows = 0, last_candidate_rows = 0,
             last_scan_at = NULL
         WHERE scan_name = 'orphan_workflow_tasks'",
    )
    .execute(&pool)
    .await
    .unwrap();
    seed_task(
        &pool,
        task_id,
        Seed {
            status: "PENDING",
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    let command = TerminalizationCommand::CancelOrphanedTasks {
        batch_size: BatchSize::new(500).unwrap(),
    };

    let mut interrupted = pool.begin().await.unwrap();
    let first = crate::broker::terminalization::terminalize_in_tx(&mut interrupted, &command)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    interrupted.rollback().await.unwrap();
    let status: String = sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
        .bind(uuid(task_id))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "PENDING");

    let retried = terminalize(&pool, &command).await.unwrap();
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].task_id(), uuid(task_id));
    assert_eq!(post_image(&pool, task_id).await.status, "CANCELLED");
    cleanup(&pool, &[task_id]).await;
}

#[tokio::test]
#[serial]
async fn orphan_sweep_cursor_lock_refuses_a_concurrent_audit() {
    let pool = migrated_pool().await;
    let mut holder = pool.begin().await.unwrap();
    sqlx::query(
        "SELECT last_id FROM horsies_recovery_scan_cursors
         WHERE scan_name = 'orphan_workflow_tasks' FOR UPDATE",
    )
    .execute(holder.as_mut())
    .await
    .unwrap();
    let command = TerminalizationCommand::CancelOrphanedTasks {
        batch_size: BatchSize::new(500).unwrap(),
    };
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        terminalize(&pool, &command),
    )
    .await
    .expect("concurrent orphan audit must not wait")
    .expect_err("concurrent orphan audit must be refused");
    assert!(
        error.is_retryable(),
        "lock refusal must be retryable: {error}"
    );
    holder.rollback().await.unwrap();
}

#[tokio::test]
#[serial]
async fn orphan_cycle_watermark_revisits_old_rows_while_new_rows_arrive() {
    let pool = migrated_pool().await;
    let workflow_id = Uuid::new_v4().to_string();
    let task_ids: Vec<String> = (0..7).map(|_| Uuid::new_v4().to_string()).collect();
    cleanup(
        &pool,
        &task_ids.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await;
    seed_workflow(&pool, &workflow_id, "RUNNING").await;
    sqlx::query(
        "UPDATE horsies_recovery_scan_cursors
         SET last_created_at = NULL, last_id = NULL,
             cycle_upper_created_at = NULL, cycle_upper_id = NULL,
             claim_token = NULL, claim_expires_at = NULL,
             completed_cycles = 0, last_scan_rows = 0,
             last_candidate_rows = 0, last_scan_at = NULL
         WHERE scan_name = 'orphan_workflow_tasks'",
    )
    .execute(&pool)
    .await
    .unwrap();

    let base = Utc::now() - chrono::Duration::hours(1);
    for (offset, task_id) in task_ids[..3].iter().enumerate() {
        seed_linked_workflow_task_at(
            &pool,
            &workflow_id,
            task_id,
            base + chrono::Duration::seconds(i64::try_from(offset).unwrap()),
        )
        .await;
    }
    let command = TerminalizationCommand::CancelOrphanedTasks {
        batch_size: BatchSize::new(2).unwrap(),
    };
    assert!(terminalize(&pool, &command).await.unwrap().is_empty());

    sqlx::query("DELETE FROM horsies_workflow_tasks WHERE task_id = $1")
        .bind(uuid(&task_ids[0]))
        .execute(&pool)
        .await
        .unwrap();
    for (offset, task_id) in task_ids[3..5].iter().enumerate() {
        seed_linked_workflow_task_at(
            &pool,
            &workflow_id,
            task_id,
            base + chrono::Duration::minutes(10)
                + chrono::Duration::seconds(i64::try_from(offset).unwrap()),
        )
        .await;
    }

    assert!(terminalize(&pool, &command).await.unwrap().is_empty());
    let second_rows: i32 = sqlx::query_scalar(
        "SELECT last_scan_rows FROM horsies_recovery_scan_cursors
         WHERE scan_name = 'orphan_workflow_tasks'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(second_rows, 1);

    for (offset, task_id) in task_ids[5..].iter().enumerate() {
        seed_linked_workflow_task_at(
            &pool,
            &workflow_id,
            task_id,
            base + chrono::Duration::minutes(20)
                + chrono::Duration::seconds(i64::try_from(offset).unwrap()),
        )
        .await;
    }
    let third = terminalize(&pool, &command).await.unwrap();
    assert_eq!(third.len(), 1);
    assert_eq!(third[0].task_id(), uuid(&task_ids[0]));
    assert_eq!(post_image(&pool, &task_ids[0]).await.status, "CANCELLED");

    cleanup(
        &pool,
        &task_ids.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await;
    cleanup_workflow(&pool, &workflow_id).await;
}

// ---------------------------------------------------------------------------
// CANCELLED — pause family
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn abandon_owned_node_applies_and_fences_generation() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    let claimed_at = Utc::now();
    seed_task(
        &pool,
        &id,
        Seed {
            status: "CLAIMED",
            claimed_at: Some(claimed_at),
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;

    let stale = terminalize(
        &pool,
        &TerminalizationCommand::AbandonOwnedNode {
            task_id: uuid(&id),
            fence: owned("w1", Some(claimed_at - Duration::hours(1))),
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        stale.as_slice(),
        [TerminalizationOutcome::LostClaim { .. }]
    ));
    assert_eq!(post_image(&pool, &id).await.status, "CLAIMED");

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::AbandonOwnedNode {
            task_id: uuid(&id),
            fence: owned("w1", Some(claimed_at)),
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied {
            kind: TerminalizationKind::PauseAbandonClaim,
            ..
        }]
    ));
    let image = post_image(&pool, &id).await;
    assert_eq!(image.status, "CANCELLED");
    assert_eq!(
        image.failed_reason.as_deref(),
        Some("Workflow paused before task start")
    );
    assert_phase2_presence(&pool, &id, false).await;

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn abandon_owned_nodes_reports_per_ordinal_in_caller_order() {
    let pool = migrated_pool().await;
    let mine = Uuid::new_v4().to_string();
    let theirs = Uuid::new_v4().to_string();
    let done = Uuid::new_v4().to_string();
    let absent = Uuid::new_v4().to_string();
    let claimed_at = Utc::now();
    seed_task(
        &pool,
        &mine,
        Seed {
            status: "CLAIMED",
            claimed_at: Some(claimed_at),
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    seed_task(
        &pool,
        &theirs,
        Seed {
            status: "CLAIMED",
            worker: Some("w-other".to_owned()),
            claimed_at: Some(claimed_at),
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    seed_task(
        &pool,
        &done,
        Seed {
            status: "CLAIMED",
            claimed_at: Some(claimed_at),
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    terminalize(
        &pool,
        &TerminalizationCommand::AbandonOwnedNode {
            task_id: uuid(&done),
            fence: owned("w1", Some(claimed_at)),
        },
    )
    .await
    .expect("first pause abandonment");

    let fence = OwnedClaimBatch::new(
        "w1".to_owned(),
        vec![
            (uuid(&mine), Some(claimed_at)),
            (uuid(&absent), None),
            (uuid(&theirs), Some(claimed_at)),
            (uuid(&done), Some(claimed_at)),
        ],
    )
    .unwrap();
    let outcomes = terminalize(&pool, &TerminalizationCommand::AbandonOwnedNodes { fence })
        .await
        .expect("terminalize");

    assert_eq!(outcomes.len(), 4, "exact ordinal set, one answer per input");
    assert!(matches!(
        &outcomes[0],
        TerminalizationOutcome::Applied {
            kind: TerminalizationKind::PauseAbandonClaimBatch,
            ordinality: Some(1),
            ..
        }
    ));
    assert!(matches!(
        &outcomes[1],
        TerminalizationOutcome::TaskAbsent {
            ordinality: Some(2),
            ..
        }
    ));
    assert!(matches!(
        &outcomes[2],
        TerminalizationOutcome::LostClaim { ordinality: Some(3), observed, .. }
            if observed.worker_id.as_deref() == Some("w-other")
    ));
    assert!(matches!(
        &outcomes[3],
        TerminalizationOutcome::AlreadyApplied {
            kind: TerminalizationKind::PauseAbandonClaim,
            ordinality: Some(4),
            ..
        }
    ));
    assert_eq!(outcomes[0].task_id(), Uuid::parse_str(&mine).unwrap());
    assert_eq!(post_image(&pool, &theirs).await.status, "CLAIMED");
    assert_phase2_presence(&pool, &mine, false).await;

    cleanup(&pool, &[&mine, &theirs, &done]).await;
}

#[tokio::test]
#[serial]
async fn batch_input_contracts_raise_22023_in_function() {
    let pool = migrated_pool().await;
    for (sql, label) in [
        (
            "SELECT * FROM horsies_abandon_owned_nodes(ARRAY['00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002']::uuid[], ARRAY[NOW()]::timestamptz[], 'w1')",
            "unequal arrays",
        ),
        (
            "SELECT * FROM horsies_abandon_owned_nodes(ARRAY['00000000-0000-0000-0000-000000000001',NULL]::uuid[], ARRAY[NOW(),NOW()]::timestamptz[], 'w1')",
            "NULL id",
        ),
        (
            "SELECT * FROM horsies_abandon_owned_nodes(ARRAY['00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000001']::uuid[], ARRAY[NOW(),NOW()]::timestamptz[], 'w1')",
            "duplicate ids",
        ),
        (
            "SELECT * FROM horsies_cancel_owned_nodes(NULL::uuid[], ARRAY[]::timestamptz[], 'w1')",
            "NULL array",
        ),
    ] {
        let err = sqlx::query(sql).fetch_all(&pool).await.expect_err(label);
        let sqlx::Error::Database(db_err) = err else {
            panic!("{label}: expected database error");
        };
        assert_eq!(db_err.code().as_deref(), Some("22023"), "{label}");
    }
}

#[tokio::test]
#[serial]
async fn abandon_nodes_of_paused_workflows_reaches_only_paused_enqueued() {
    let pool = migrated_pool().await;
    let paused_wf = Uuid::new_v4().to_string();
    let running_wf = Uuid::new_v4().to_string();
    let paused_task = Uuid::new_v4().to_string();
    let running_task = Uuid::new_v4().to_string();
    seed_workflow(&pool, &paused_wf, "PAUSED").await;
    seed_workflow(&pool, &running_wf, "RUNNING").await;
    // Claims held by another worker are exactly what must be abandoned.
    seed_task(
        &pool,
        &paused_task,
        Seed {
            status: "CLAIMED",
            worker: Some("w-remote".to_owned()),
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    seed_task(
        &pool,
        &running_task,
        Seed {
            status: "CLAIMED",
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    seed_wf_task(&pool, &paused_wf, &paused_task, "ENQUEUED").await;
    seed_wf_task(&pool, &running_wf, &running_task, "ENQUEUED").await;

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::AbandonNodesOfPausedWorkflows {
            workflow_ids: vec![uuid(&paused_wf), uuid(&running_wf)],
        },
    )
    .await
    .expect("terminalize");

    assert_eq!(
        outcomes.len(),
        1,
        "only the paused workflow's claim is reached"
    );
    assert!(matches!(
        &outcomes[0],
        TerminalizationOutcome::Applied { kind: TerminalizationKind::PauseAbandonWorkflow, observed, .. }
            if observed.worker_id.as_deref() == Some("w-remote")
    ));
    assert_eq!(
        outcomes[0].task_id(),
        Uuid::parse_str(&paused_task).unwrap()
    );
    assert_eq!(post_image(&pool, &running_task).await.status, "CLAIMED");
    assert_phase2_presence(&pool, &paused_task, false).await;

    cleanup(&pool, &[&paused_task, &running_task]).await;
    cleanup_workflow(&pool, &paused_wf).await;
    cleanup_workflow(&pool, &running_wf).await;
}

// ---------------------------------------------------------------------------
// CANCELLED — workflow-cancel family
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn cancel_owned_node_honors_requeued_pending_carveout() {
    let pool = migrated_pool().await;
    let requeued = Uuid::new_v4().to_string();
    seed_task(
        &pool,
        &requeued,
        Seed {
            status: "PENDING",
            worker: None,
            claimed_at: None,
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;

    let refused = terminalize(
        &pool,
        &TerminalizationCommand::CancelOwnedNode {
            task_id: uuid(&requeued),
            fence: owned("w1", None),
            accepts_requeued_pending: false,
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        refused.as_slice(),
        [TerminalizationOutcome::LostClaim { .. }]
    ));
    assert_eq!(post_image(&pool, &requeued).await.status, "PENDING");

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CancelOwnedNode {
            task_id: uuid(&requeued),
            fence: owned("w1", None),
            accepts_requeued_pending: true,
        },
    )
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied {
            kind: TerminalizationKind::WorkflowCancelClaim,
            ..
        }]
    ));
    assert_eq!(post_image(&pool, &requeued).await.status, "CANCELLED");
    assert_phase2_presence(&pool, &requeued, false).await;

    cleanup(&pool, &[&requeued]).await;
}

#[tokio::test]
#[serial]
async fn cancel_owned_nodes_batch_applies_owned_claims() {
    let pool = migrated_pool().await;
    let a = Uuid::new_v4().to_string();
    let b = Uuid::new_v4().to_string();
    let gen_a = Utc::now() - Duration::minutes(5);
    let gen_b = Utc::now() - Duration::minutes(2);
    seed_task(
        &pool,
        &a,
        Seed {
            status: "CLAIMED",
            claimed_at: Some(gen_a),
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    seed_task(
        &pool,
        &b,
        Seed {
            status: "CLAIMED",
            claimed_at: Some(gen_b),
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;

    let fence = OwnedClaimBatch::new(
        "w1".to_owned(),
        vec![(uuid(&a), Some(gen_a)), (uuid(&b), Some(gen_b))],
    )
    .unwrap();
    let outcomes = terminalize(&pool, &TerminalizationCommand::CancelOwnedNodes { fence })
        .await
        .expect("terminalize");
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|o| matches!(
        o,
        TerminalizationOutcome::Applied {
            kind: TerminalizationKind::WorkflowCancelClaimBatch,
            ..
        }
    )));
    for id in [&a, &b] {
        let image = post_image(&pool, id).await;
        assert_eq!(image.status, "CANCELLED");
        assert_eq!(
            image.claimed_by_worker_id.as_deref(),
            Some("w1"),
            "history preserves the last claim owner"
        );
        assert_phase2_presence(&pool, id, false).await;
    }

    cleanup(&pool, &[&a, &b]).await;
}

#[tokio::test]
#[serial]
async fn cancel_nodes_of_cancelled_workflow_reaches_briefly_running_enqueued() {
    let pool = migrated_pool().await;
    let wf_id = Uuid::new_v4().to_string();
    let briefly_running = Uuid::new_v4().to_string();
    let started = Uuid::new_v4().to_string();
    seed_workflow(&pool, &wf_id, "CANCELLED").await;
    // ENQUEUED node whose backing row is briefly RUNNING: user code starts
    // only after the node's own RUNNING handoff, so this is cancellable.
    seed_task(
        &pool,
        &briefly_running,
        Seed {
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    seed_wf_task(&pool, &wf_id, &briefly_running, "ENQUEUED").await;
    // A node already RUNNING is not: its user code is executing.
    seed_task(
        &pool,
        &started,
        Seed {
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    seed_wf_task(&pool, &wf_id, &started, "RUNNING").await;

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CancelNodesOfCancelledWorkflow {
            workflow_ids: vec![uuid(&wf_id)],
        },
    )
    .await
    .expect("terminalize");

    assert_eq!(outcomes.len(), 1);
    assert!(matches!(
        &outcomes[0],
        TerminalizationOutcome::Applied { kind: TerminalizationKind::WorkflowCancelWorkflow, observed, .. }
            if observed.status == Some(TaskStatus::Running)
    ));
    assert_eq!(
        outcomes[0].task_id(),
        Uuid::parse_str(&briefly_running).unwrap()
    );
    assert_eq!(
        post_image(&pool, &briefly_running).await.status,
        "CANCELLED"
    );
    assert_eq!(post_image(&pool, &started).await.status, "RUNNING");
    assert_phase2_presence(&pool, &briefly_running, false).await;

    cleanup(&pool, &[&briefly_running, &started]).await;
    cleanup_workflow(&pool, &wf_id).await;
}

#[tokio::test]
#[serial]
async fn deferred_projection_table_mints_phase2_for_all_five_deferred_wire_kinds() {
    let pool = migrated_pool().await;
    drain_pending_expiry(&pool).await;
    let workflow_id = Uuid::new_v4().to_string();
    seed_workflow(&pool, &workflow_id, "RUNNING").await;
    let claimed_at = Utc::now() - Duration::hours(2);
    let complete = Uuid::new_v4().to_string();
    let failed = Uuid::new_v4().to_string();
    let stale = Uuid::new_v4().to_string();
    let expired = Uuid::new_v4().to_string();
    let expired_pending = Uuid::new_v4().to_string();
    let nondeferred = Uuid::new_v4().to_string();

    for id in [&complete, &failed, &stale] {
        seed_task(
            &pool,
            id,
            Seed {
                claimed_at: Some(claimed_at),
                started_at: Some(claimed_at),
                is_workflow_task: true,
                ..Seed::default()
            },
        )
        .await;
        seed_wf_task(&pool, &workflow_id, id, "RUNNING").await;
    }
    for id in [&expired, &nondeferred] {
        seed_task(
            &pool,
            id,
            Seed {
                status: "CLAIMED",
                claimed_at: Some(claimed_at),
                good_until: Some(Utc::now() - Duration::minutes(1)),
                is_workflow_task: true,
                ..Seed::default()
            },
        )
        .await;
        seed_wf_task(&pool, &workflow_id, id, "ENQUEUED").await;
    }
    seed_task(
        &pool,
        &expired_pending,
        Seed {
            status: "PENDING",
            worker: None,
            claimed_at: None,
            good_until: Some(Utc::now() - Duration::minutes(1)),
            is_workflow_task: true,
            ..Seed::default()
        },
    )
    .await;
    seed_wf_task(&pool, &workflow_id, &expired_pending, "ENQUEUED").await;

    terminalize(
        &pool,
        &TerminalizationCommand::CompleteLockedTask {
            task_id: uuid(&complete),
            fence: PriorLockedRead {
                worker_id: "w1".to_owned(),
            },
            result_json: "{}".to_owned(),
        },
    )
    .await
    .expect("deferred completion");
    terminalize(
        &pool,
        &TerminalizationCommand::FailLockedTask {
            task_id: uuid(&failed),
            fence: PriorLockedRead {
                worker_id: "w1".to_owned(),
            },
            result_json: "{}".to_owned(),
            error_code: Some("TASK_ERROR".to_owned()),
            failed_reason: Some("failed".to_owned()),
        },
    )
    .await
    .expect("deferred running failure");
    terminalize(
        &pool,
        &TerminalizationCommand::FailStaleTask {
            task_id: uuid(&stale),
            stale_after_ms: 60_000,
            finalizing_stale_after_ms: 60_000,
            result_json: "{}".to_owned(),
            error_code: "WORKER_CRASHED".to_owned(),
            failed_reason: "stale".to_owned(),
        },
    )
    .await
    .expect("deferred stale failure");
    terminalize(
        &pool,
        &TerminalizationCommand::ExpireOwnedClaim {
            task_id: uuid(&expired),
            fence: WorkerOwned {
                worker_id: "w1".to_owned(),
            },
            result_json: "{}".to_owned(),
            error_code: "TASK_EXPIRED".to_owned(),
        },
    )
    .await
    .expect("deferred claimed expiry");
    let pending_expiry = terminalize(
        &pool,
        &TerminalizationCommand::ExpirePendingTasks {
            batch_size: BatchSize::new(500).unwrap(),
            result_json: "{}".to_owned(),
            error_code: "TASK_EXPIRED".to_owned(),
        },
    )
    .await
    .expect("deferred pending expiry");
    assert!(pending_expiry.iter().any(|outcome| {
        outcome.task_id() == Uuid::parse_str(&expired_pending).unwrap()
            && matches!(
                outcome,
                TerminalizationOutcome::Applied {
                    kind: TerminalizationKind::ExpirePending,
                    ..
                }
            )
    }));
    terminalize(
        &pool,
        &TerminalizationCommand::AbandonOwnedNode {
            task_id: uuid(&nondeferred),
            fence: owned("w1", Some(claimed_at)),
        },
    )
    .await
    .expect("nondeferred pause abandonment");

    let pending: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT task_id, terminalization_kind
         FROM horsies_workflow_phase2_pending
         WHERE workflow_id = $1
         ORDER BY terminalization_kind",
    )
    .bind(Uuid::parse_str(&workflow_id).unwrap())
    .fetch_all(&pool)
    .await
    .expect("phase2 pending rows");
    let kinds: Vec<&str> = pending.iter().map(|(_, kind)| kind.as_str()).collect();
    assert_eq!(
        kinds,
        vec![
            "COMPLETE_LOCKED",
            "EXPIRE_CLAIMED",
            "EXPIRE_PENDING",
            "FAIL_RUNNING",
            "FAIL_STALE"
        ]
    );
    assert!(!pending
        .iter()
        .any(|(task_id, _)| *task_id == Uuid::parse_str(&nondeferred).unwrap()));

    cleanup(
        &pool,
        &[
            &complete,
            &failed,
            &stale,
            &expired,
            &expired_pending,
            &nondeferred,
        ],
    )
    .await;
    cleanup_workflow(&pool, &workflow_id).await;
}

//! Transition matrix for the terminalization operations (pre-cutover).
//!
//! Drives every installed function against seeded rows in each relevant
//! source state and asserts (a) the outcome variant and its evidence, (b)
//! the row's post-image, (c) replay within the equivalence class vs
//! cross-class foreign terminalization, (d) the batch input contracts, and
//! (e) the revert-proof properties. This module is the behavioral safety
//! net that must be green before any call site moves.
//!
//! Shared-DB hygiene: global discovery batches (pending expiry, orphan
//! sweep) are pre-drained before seeding so leftovers from earlier runs
//! cannot enter an assertion.

use chrono::{DateTime, Duration, Utc};
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use crate::broker::postgres::PostgresBroker;
use crate::broker::terminalization::terminalize;
use crate::core::lifecycle::outcomes::GuardEvidence;
use crate::core::lifecycle::{
    BatchSize, CallerHoldsRowLock, OwnedClaim, OwnedClaimBatch, PriorLockedRead,
    TerminalizationCommand, TerminalizationKind, TerminalizationOutcome, WorkerOwned,
};
use crate::core::types::status::TaskStatus;

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

async fn migrated_pool() -> PgPool {
    let broker = PostgresBroker::connect(&test_db_url()).await.expect("connect");
    broker.ensure_schema_initialized().await.expect("schema");
    broker.pool().clone()
}

struct Seed {
    status: &'static str,
    worker: Option<String>,
    claimed_at: Option<DateTime<Utc>>,
    good_until: Option<DateTime<Utc>>,
    is_workflow_task: bool,
    started_at: Option<DateTime<Utc>>,
    failed_reason: Option<String>,
    terminalization_kind: Option<String>,
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
            terminalization_kind: None,
        }
    }
}

async fn seed_task(pool: &PgPool, id: &str, seed: Seed) {
    let terminal = matches!(seed.status, "COMPLETED" | "FAILED" | "CANCELLED" | "EXPIRED");
    sqlx::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, args, kwargs, status,
            sent_at, enqueued_at, started_at, claimed, claimed_at,
            claimed_by_worker_id, good_until, is_workflow_task,
            failed_reason, terminal_at, terminalization_kind,
            retry_count, max_retries, enqueue_sha, created_at, updated_at
        ) VALUES (
            $1, 'matrix_task', 'default', 100, '[]', '{}', $2,
            NOW(), NOW(), $3, $4 IS NOT NULL, $5,
            $4, $6, $7,
            $8, CASE WHEN $9 THEN NOW() END, $10,
            0, 3, $1, NOW(), NOW()
        )",
    )
    .bind(id)
    .bind(seed.status)
    .bind(seed.started_at)
    .bind(&seed.worker)
    .bind(seed.claimed_at)
    .bind(seed.good_until)
    .bind(seed.is_workflow_task)
    .bind(&seed.failed_reason)
    .bind(terminal)
    .bind(&seed.terminalization_kind)
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
    .bind(id)
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
    .bind(Uuid::new_v4().to_string())
    .bind(wf_id)
    .bind(status)
    .bind(task_id)
    .execute(pool)
    .await
    .expect("seed workflow task");
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
         FROM horsies_tasks WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("post image")
}

async fn cleanup(pool: &PgPool, ids: &[&str]) {
    for id in ids {
        sqlx::query("DELETE FROM horsies_task_attempts WHERE task_id = $1")
            .bind(id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE task_id = $1")
            .bind(id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await
            .ok();
    }
}

async fn cleanup_workflow(pool: &PgPool, wf_id: &str) {
    sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
        .bind(wf_id)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(wf_id)
        .execute(pool)
        .await
        .ok();
}

fn owned(worker: &str, claimed_at: Option<DateTime<Utc>>) -> OwnedClaim {
    OwnedClaim { worker_id: worker.to_owned(), claimed_at }
}

// ---------------------------------------------------------------------------
// COMPLETED family
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn complete_locked_applies_and_leaves_claim_columns() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    let claimed_at = Utc::now();
    seed_task(&pool, &id, Seed {
        failed_reason: Some("stale reason from an earlier attempt".to_owned()),
        claimed_at: Some(claimed_at),
        ..Seed::default()
    })
    .await;

    let outcomes = terminalize(&pool, &TerminalizationCommand::CompleteLockedTask {
        task_id: id.clone(),
        fence: PriorLockedRead { worker_id: "w1".to_owned() },
        result_json: "{\"Ok\":1}".to_owned(),
    })
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
    assert_eq!(image.terminalization_kind.as_deref(), Some("COMPLETE_LOCKED"));
    assert_eq!(image.error_code, None);
    assert_eq!(image.failed_reason, None, "v26: completion clears failed_reason");
    // The locked shape leaves claim columns to the caller's transaction.
    assert_eq!(image.claimed_by_worker_id.as_deref(), Some("w1"));

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn complete_replay_within_class_is_already_applied() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    seed_task(&pool, &id, Seed {
        status: "COMPLETED",
        terminalization_kind: Some("COMPLETE_FUSED".to_owned()),
        ..Seed::default()
    })
    .await;

    let outcomes = terminalize(&pool, &TerminalizationCommand::CompleteLockedTask {
        task_id: id.clone(),
        fence: PriorLockedRead { worker_id: "w1".to_owned() },
        result_json: "{}".to_owned(),
    })
    .await
    .expect("terminalize");

    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::AlreadyApplied { kind: TerminalizationKind::CompleteFused, .. }]
    ));

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn complete_locked_wrong_worker_is_lost_claim_and_absent_is_absent() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    seed_task(&pool, &id, Seed::default()).await;

    let outcomes = terminalize(&pool, &TerminalizationCommand::CompleteLockedTask {
        task_id: id.clone(),
        fence: PriorLockedRead { worker_id: "w-other".to_owned() },
        result_json: "{}".to_owned(),
    })
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::LostClaim { observed, .. }]
            if observed.worker_id.as_deref() == Some("w1")
    ));
    assert_eq!(post_image(&pool, &id).await.status, "RUNNING", "refusal must not mutate");

    let absent = terminalize(&pool, &TerminalizationCommand::CompleteLockedTask {
        task_id: "matrix-no-such-task".to_owned(),
        fence: PriorLockedRead { worker_id: "w1".to_owned() },
        result_json: "{}".to_owned(),
    })
    .await
    .expect("terminalize");
    assert!(matches!(absent.as_slice(), [TerminalizationOutcome::TaskAbsent { .. }]));

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn fused_applies_with_attempt_notify_and_generation_fence() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    let claimed_at = Utc::now();
    seed_task(&pool, &id, Seed { claimed_at: Some(claimed_at), ..Seed::default() }).await;

    let channel = format!("matrix_fused_{}", Uuid::new_v4().simple());
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool)
        .await
        .expect("listener");
    listener.listen(&channel).await.expect("listen");

    let outcomes = terminalize(&pool, &TerminalizationCommand::CompleteTaskFused {
        task_id: id.clone(),
        fence: owned("w1", Some(claimed_at)),
        result_json: "{\"Ok\":7}".to_owned(),
        notify_channel: channel.clone(),
        notify_payload: format!("capacity:{id}"),
    })
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { kind: TerminalizationKind::CompleteFused, .. }]
    ));

    let image = post_image(&pool, &id).await;
    assert_eq!(image.status, "COMPLETED");
    assert_eq!(image.terminalization_kind.as_deref(), Some("COMPLETE_FUSED"));

    let (attempt, outcome): (i32, String) = sqlx::query_as(
        "SELECT attempt, outcome FROM horsies_task_attempts WHERE task_id = $1",
    )
    .bind(&id)
    .fetch_one(&pool)
    .await
    .expect("attempt row");
    assert_eq!((attempt, outcome.as_str()), (1, "COMPLETED"));

    let notification = tokio::time::timeout(std::time::Duration::from_secs(5), listener.recv())
        .await
        .expect("notify within timeout")
        .expect("notification");
    assert_eq!(notification.payload(), format!("capacity:{id}"));

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn fused_stale_generation_is_lost_claim() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    seed_task(&pool, &id, Seed { claimed_at: Some(Utc::now()), ..Seed::default() }).await;

    let stale = Utc::now() - Duration::hours(1);
    let outcomes = terminalize(&pool, &TerminalizationCommand::CompleteTaskFused {
        task_id: id.clone(),
        fence: owned("w1", Some(stale)),
        result_json: "{}".to_owned(),
        notify_channel: "matrix_unused".to_owned(),
        notify_payload: "x".to_owned(),
    })
    .await
    .expect("terminalize");
    assert!(matches!(outcomes.as_slice(), [TerminalizationOutcome::LostClaim { .. }]));
    assert_eq!(post_image(&pool, &id).await.status, "RUNNING");
    let attempts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM horsies_task_attempts WHERE task_id = $1")
            .bind(&id)
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
    seed_task(&pool, &id, Seed {
        status: "CLAIMED",
        claimed_at: Some(claimed_at),
        ..Seed::default()
    })
    .await;

    let outcomes = terminalize(&pool, &TerminalizationCommand::CompleteTaskFused {
        task_id: id.clone(),
        fence: owned("w1", Some(claimed_at)),
        result_json: "{}".to_owned(),
        notify_channel: "matrix_unused".to_owned(),
        notify_payload: "x".to_owned(),
    })
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
    seed_task(&pool, &id, Seed {
        failed_reason: Some("reason from a requeued earlier attempt".to_owned()),
        ..Seed::default()
    })
    .await;

    let outcomes = terminalize(&pool, &TerminalizationCommand::FailLockedTask {
        task_id: id.clone(),
        fence: PriorLockedRead { worker_id: "w1".to_owned() },
        result_json: "{\"Err\":{}}".to_owned(),
        error_code: Some("TASK_ERROR".to_owned()),
        failed_reason: None,
    })
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { kind: TerminalizationKind::FailRunning, .. }]
    ));

    let image = post_image(&pool, &id).await;
    assert_eq!(image.status, "FAILED");
    assert_eq!(image.error_code.as_deref(), Some("TASK_ERROR"));
    assert_eq!(
        image.failed_reason, None,
        "v26: the terminal writer owns the final-attempt summary; None clears \
         a requeued attempt's leftover reason"
    );

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn cross_class_replay_names_the_foreign_kind() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    seed_task(&pool, &id, Seed {
        status: "CANCELLED",
        terminalization_kind: Some("CANCEL_ADMIN".to_owned()),
        ..Seed::default()
    })
    .await;

    let outcomes = terminalize(&pool, &TerminalizationCommand::FailLockedTask {
        task_id: id.clone(),
        fence: PriorLockedRead { worker_id: "w1".to_owned() },
        result_json: "{}".to_owned(),
        error_code: None,
        failed_reason: None,
    })
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
    assert_eq!(foreign.committed_kind, Some(TerminalizationKind::CancelAdmin));
    assert_eq!(post_image(&pool, &id).await.status, "CANCELLED", "terminal never overwritten");

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn null_kind_terminal_row_is_conflict_not_already_applied() {
    let pool = migrated_pool().await;
    let id = Uuid::new_v4().to_string();
    seed_task(&pool, &id, Seed {
        status: "FAILED",
        terminalization_kind: None,
        ..Seed::default()
    })
    .await;

    let outcomes = terminalize(&pool, &TerminalizationCommand::FailLockedTask {
        task_id: id.clone(),
        fence: PriorLockedRead { worker_id: "w1".to_owned() },
        result_json: "{}".to_owned(),
        error_code: None,
        failed_reason: None,
    })
    .await
    .expect("terminalize");
    let [TerminalizationOutcome::SourceStateConflict {
        evidence: GuardEvidence::ForeignTerminalization(foreign),
        ..
    }] = outcomes.as_slice()
    else {
        panic!("expected conflict with unknown provenance, got {outcomes:?}");
    };
    assert_eq!(foreign.committed_kind, None, "NULL kind = unknown provenance, never inferred");

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn fail_stale_applies_on_silence_and_refuses_on_heartbeat() {
    let pool = migrated_pool().await;
    let silent = Uuid::new_v4().to_string();
    let alive = Uuid::new_v4().to_string();
    let old = Utc::now() - Duration::hours(2);
    seed_task(&pool, &silent, Seed { started_at: Some(old), ..Seed::default() }).await;
    seed_task(&pool, &alive, Seed { started_at: Some(old), ..Seed::default() }).await;
    sqlx::query(
        "INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
         VALUES ($1, 'w1', 'runner', NOW(), 'h', 1)",
    )
    .bind(&alive)
    .execute(&pool)
    .await
    .expect("heartbeat");

    let command = |task_id: &str| TerminalizationCommand::FailStaleTask {
        task_id: task_id.to_owned(),
        stale_after_ms: 60_000,
        finalizing_stale_after_ms: 60_000,
        result_json: "{\"Err\":{}}".to_owned(),
        error_code: "WORKER_CRASHED".to_owned(),
        failed_reason: "stale runner".to_owned(),
    };

    let outcomes = terminalize(&pool, &command(&silent)).await.expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { kind: TerminalizationKind::FailStale, .. }]
    ));
    let image = post_image(&pool, &silent).await;
    assert_eq!(image.status, "FAILED");
    assert_eq!(image.failed_reason.as_deref(), Some("stale runner"));

    let outcomes = terminalize(&pool, &command(&alive)).await.expect("terminalize");
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
        .bind(&alive)
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
    seed_task(&pool, &expired, Seed {
        status: "CLAIMED",
        claimed_at: Some(Utc::now() - Duration::minutes(30)),
        good_until: Some(Utc::now() - Duration::minutes(1)),
        ..Seed::default()
    })
    .await;
    seed_task(&pool, &not_due, Seed {
        status: "CLAIMED",
        good_until: Some(Utc::now() + Duration::hours(1)),
        ..Seed::default()
    })
    .await;

    let command = |task_id: &str| TerminalizationCommand::ExpireOwnedClaim {
        task_id: task_id.to_owned(),
        fence: WorkerOwned { worker_id: "w1".to_owned() },
        result_json: "{\"Err\":{}}".to_owned(),
        error_code: "TASK_EXPIRED".to_owned(),
    };

    // No generation in the fence: the deadline makes expiry correct for
    // whichever generation holds the row.
    let outcomes = terminalize(&pool, &command(&expired)).await.expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { kind: TerminalizationKind::ExpireClaimed, .. }]
    ));
    let image = post_image(&pool, &expired).await;
    assert_eq!(image.status, "EXPIRED");
    assert_eq!(image.error_code.as_deref(), Some("TASK_EXPIRED"));
    assert_eq!(image.failed_reason, None);

    let outcomes = terminalize(&pool, &command(&not_due)).await.expect("terminalize");
    let [TerminalizationOutcome::SourceStateConflict {
        evidence: GuardEvidence::Deadline(deadline),
        ..
    }] = outcomes.as_slice()
    else {
        panic!("expected deadline refusal, got {outcomes:?}");
    };
    assert!(deadline.good_until.is_some());
    assert_eq!(post_image(&pool, &not_due).await.status, "CLAIMED");

    let foreign = terminalize(&pool, &TerminalizationCommand::ExpireOwnedClaim {
        task_id: not_due.clone(),
        fence: WorkerOwned { worker_id: "w-other".to_owned() },
        result_json: "{}".to_owned(),
        error_code: "TASK_EXPIRED".to_owned(),
    })
    .await
    .expect("terminalize");
    assert!(matches!(foreign.as_slice(), [TerminalizationOutcome::LostClaim { .. }]));

    cleanup(&pool, &[&expired, &not_due]).await;
}

async fn drain_pending_expiry(pool: &PgPool) {
    loop {
        let drained = terminalize(pool, &TerminalizationCommand::ExpirePendingTasks {
            batch_size: BatchSize::new(500).unwrap(),
            result_json: "{\"Err\":{}}".to_owned(),
            error_code: "TASK_EXPIRED".to_owned(),
        })
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
        seed_task(&pool, id, Seed {
            status: "PENDING",
            worker: None,
            claimed_at: None,
            good_until: Some(Utc::now() - Duration::minutes(10 - index as i64)),
            ..Seed::default()
        })
        .await;
    }

    let first = terminalize(&pool, &TerminalizationCommand::ExpirePendingTasks {
        batch_size: BatchSize::new(2).unwrap(),
        result_json: "{\"Err\":{}}".to_owned(),
        error_code: "TASK_EXPIRED".to_owned(),
    })
    .await
    .expect("terminalize");
    let applied_ids: Vec<&str> = first.iter().map(|o| o.task_id()).collect();
    assert_eq!(
        applied_ids,
        vec![ids[0].as_str(), ids[1].as_str()],
        "batch takes the earliest deadlines first"
    );
    assert!(first.iter().all(|o| matches!(
        o,
        TerminalizationOutcome::Applied { kind: TerminalizationKind::ExpirePending, .. }
    )));
    assert_eq!(post_image(&pool, &ids[2]).await.status, "PENDING");

    let second = terminalize(&pool, &TerminalizationCommand::ExpirePendingTasks {
        batch_size: BatchSize::new(500).unwrap(),
        result_json: "{}".to_owned(),
        error_code: "TASK_EXPIRED".to_owned(),
    })
    .await
    .expect("terminalize");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].task_id(), ids[2]);

    let empty = terminalize(&pool, &TerminalizationCommand::ExpirePendingTasks {
        batch_size: BatchSize::new(500).unwrap(),
        result_json: "{}".to_owned(),
        error_code: "TASK_EXPIRED".to_owned(),
    })
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

// ---------------------------------------------------------------------------
// CANCELLED — administrative
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn cancel_locked_applies_within_permitted_statuses_only() {
    let pool = migrated_pool().await;
    let pending = Uuid::new_v4().to_string();
    let running = Uuid::new_v4().to_string();
    seed_task(&pool, &pending, Seed {
        status: "PENDING",
        worker: None,
        claimed_at: None,
        ..Seed::default()
    })
    .await;
    seed_task(&pool, &running, Seed::default()).await;

    let outcomes = terminalize(&pool, &TerminalizationCommand::CancelLockedTask {
        task_id: pending.clone(),
        fence: CallerHoldsRowLock,
        permitted_source_statuses: vec![TaskStatus::Pending, TaskStatus::Claimed],
    })
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { kind: TerminalizationKind::CancelAdmin, .. }]
    ));
    let image = post_image(&pool, &pending).await;
    assert_eq!(image.status, "CANCELLED");
    assert_eq!(image.error_code.as_deref(), Some("TASK_CANCELLED"));
    assert_eq!(image.failed_reason.as_deref(), Some("Cancelled via monitoring API"));
    assert_eq!(image.claimed_by_worker_id, None, "admin cancel clears the claim");

    // RUNNING outside the permitted set: the operator's opt-in is explicit.
    let outcomes = terminalize(&pool, &TerminalizationCommand::CancelLockedTask {
        task_id: running.clone(),
        fence: CallerHoldsRowLock,
        permitted_source_statuses: vec![TaskStatus::Pending],
    })
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
    seed_task(&pool, &wf_task, Seed {
        status: "PENDING",
        worker: None,
        claimed_at: None,
        is_workflow_task: true,
        ..Seed::default()
    })
    .await;
    seed_task(&pool, &done, Seed {
        status: "COMPLETED",
        terminalization_kind: Some("COMPLETE_FUSED".to_owned()),
        ..Seed::default()
    })
    .await;

    let outcomes = terminalize(&pool, &TerminalizationCommand::CancelLockedTask {
        task_id: wf_task.clone(),
        fence: CallerHoldsRowLock,
        permitted_source_statuses: vec![TaskStatus::Pending],
    })
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::SourceStateConflict { .. }]
    ));
    assert_eq!(post_image(&pool, &wf_task).await.status, "PENDING");

    // Revert-proof: a malformed permitted array naming a terminal status
    // cannot resurrect or overwrite the row.
    let outcomes = terminalize(&pool, &TerminalizationCommand::CancelLockedTask {
        task_id: done.clone(),
        fence: CallerHoldsRowLock,
        permitted_source_statuses: vec![TaskStatus::Completed],
    })
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
    assert_eq!(image.terminalization_kind.as_deref(), Some("COMPLETE_FUSED"));

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
    seed_task(&pool, &orphan, Seed {
        status: "CLAIMED",
        claimed_at: Some(claimed_at),
        is_workflow_task: true,
        ..Seed::default()
    })
    .await;
    seed_task(&pool, &linked, Seed {
        status: "CLAIMED",
        claimed_at: Some(claimed_at),
        is_workflow_task: true,
        ..Seed::default()
    })
    .await;
    seed_wf_task(&pool, &wf_id, &linked, "ENQUEUED").await;

    let command = |task_id: &str| TerminalizationCommand::CancelOwnedOrphan {
        task_id: task_id.to_owned(),
        fence: owned("w1", Some(claimed_at)),
    };

    let outcomes = terminalize(&pool, &command(&orphan)).await.expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { kind: TerminalizationKind::CancelOrphan, .. }]
    ));
    let image = post_image(&pool, &orphan).await;
    assert_eq!(image.status, "CANCELLED");
    assert_eq!(image.error_code.as_deref(), Some("WORKFLOW_CHECK_FAILED"));

    // Revert-proof: the runnable-link guard retains linked tasks, and names
    // the node state that refused.
    let outcomes = terminalize(&pool, &command(&linked)).await.expect("terminalize");
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
    loop {
        let drained = terminalize(pool, &TerminalizationCommand::CancelOrphanedTasks {
            batch_size: BatchSize::new(500).unwrap(),
        })
        .await
        .expect("drain");
        if drained.len() < 500 {
            break;
        }
    }
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
    seed_task(&pool, &orphan, Seed {
        status: "CLAIMED",
        is_workflow_task: true,
        ..Seed::default()
    })
    .await;
    seed_task(&pool, &linked, Seed {
        status: "CLAIMED",
        is_workflow_task: true,
        ..Seed::default()
    })
    .await;
    seed_wf_task(&pool, &wf_id, &linked, "RUNNING").await;

    let outcomes = terminalize(&pool, &TerminalizationCommand::CancelOrphanedTasks {
        batch_size: BatchSize::new(500).unwrap(),
    })
    .await
    .expect("terminalize");
    let swept: Vec<&str> = outcomes.iter().map(|o| o.task_id()).collect();
    assert!(swept.contains(&orphan.as_str()));
    assert!(!swept.contains(&linked.as_str()), "runnable linkage retains the task");
    assert!(outcomes.iter().all(|o| matches!(
        o,
        TerminalizationOutcome::Applied { kind: TerminalizationKind::CancelOrphanSweep, observed, .. }
            if observed.status == Some(TaskStatus::Claimed)
    )));
    assert_eq!(post_image(&pool, &orphan).await.status, "CANCELLED");
    assert_eq!(post_image(&pool, &linked).await.status, "CLAIMED");

    cleanup(&pool, &[&orphan, &linked]).await;
    cleanup_workflow(&pool, &wf_id).await;
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
    seed_task(&pool, &id, Seed {
        status: "CLAIMED",
        claimed_at: Some(claimed_at),
        is_workflow_task: true,
        ..Seed::default()
    })
    .await;

    let stale = terminalize(&pool, &TerminalizationCommand::AbandonOwnedNode {
        task_id: id.clone(),
        fence: owned("w1", Some(claimed_at - Duration::hours(1))),
    })
    .await
    .expect("terminalize");
    assert!(matches!(stale.as_slice(), [TerminalizationOutcome::LostClaim { .. }]));
    assert_eq!(post_image(&pool, &id).await.status, "CLAIMED");

    let outcomes = terminalize(&pool, &TerminalizationCommand::AbandonOwnedNode {
        task_id: id.clone(),
        fence: owned("w1", Some(claimed_at)),
    })
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { kind: TerminalizationKind::PauseAbandonClaim, .. }]
    ));
    let image = post_image(&pool, &id).await;
    assert_eq!(image.status, "CANCELLED");
    assert_eq!(image.failed_reason.as_deref(), Some("Workflow paused before task start"));

    cleanup(&pool, &[&id]).await;
}

#[tokio::test]
#[serial]
async fn abandon_owned_nodes_reports_per_ordinal_in_caller_order() {
    let pool = migrated_pool().await;
    let mine = Uuid::new_v4().to_string();
    let theirs = Uuid::new_v4().to_string();
    let done = Uuid::new_v4().to_string();
    let absent = format!("matrix-absent-{}", Uuid::new_v4());
    let claimed_at = Utc::now();
    seed_task(&pool, &mine, Seed {
        status: "CLAIMED",
        claimed_at: Some(claimed_at),
        is_workflow_task: true,
        ..Seed::default()
    })
    .await;
    seed_task(&pool, &theirs, Seed {
        status: "CLAIMED",
        worker: Some("w-other".to_owned()),
        claimed_at: Some(claimed_at),
        is_workflow_task: true,
        ..Seed::default()
    })
    .await;
    seed_task(&pool, &done, Seed {
        status: "CANCELLED",
        terminalization_kind: Some("PAUSE_ABANDON_WORKFLOW".to_owned()),
        ..Seed::default()
    })
    .await;

    let fence = OwnedClaimBatch::new(
        "w1".to_owned(),
        vec![
            (mine.clone(), Some(claimed_at)),
            (absent.clone(), None),
            (theirs.clone(), Some(claimed_at)),
            (done.clone(), Some(claimed_at)),
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
        TerminalizationOutcome::TaskAbsent { ordinality: Some(2), .. }
    ));
    assert!(matches!(
        &outcomes[2],
        TerminalizationOutcome::LostClaim { ordinality: Some(3), observed, .. }
            if observed.worker_id.as_deref() == Some("w-other")
    ));
    assert!(matches!(
        &outcomes[3],
        TerminalizationOutcome::AlreadyApplied {
            kind: TerminalizationKind::PauseAbandonWorkflow,
            ordinality: Some(4),
            ..
        }
    ));
    assert_eq!(outcomes[0].task_id(), mine);
    assert_eq!(post_image(&pool, &theirs).await.status, "CLAIMED");

    cleanup(&pool, &[&mine, &theirs, &done]).await;
}

#[tokio::test]
#[serial]
async fn batch_input_contracts_raise_22023_in_function() {
    let pool = migrated_pool().await;
    for (sql, label) in [
        (
            "SELECT * FROM horsies_abandon_owned_nodes(ARRAY['a','b']::varchar[], ARRAY[NOW()]::timestamptz[], 'w1')",
            "unequal arrays",
        ),
        (
            "SELECT * FROM horsies_abandon_owned_nodes(ARRAY['a',NULL]::varchar[], ARRAY[NOW(),NOW()]::timestamptz[], 'w1')",
            "NULL id",
        ),
        (
            "SELECT * FROM horsies_abandon_owned_nodes(ARRAY['a','a']::varchar[], ARRAY[NOW(),NOW()]::timestamptz[], 'w1')",
            "duplicate ids",
        ),
        (
            "SELECT * FROM horsies_cancel_owned_nodes(NULL::varchar[], ARRAY[]::timestamptz[], 'w1')",
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
    seed_task(&pool, &paused_task, Seed {
        status: "CLAIMED",
        worker: Some("w-remote".to_owned()),
        is_workflow_task: true,
        ..Seed::default()
    })
    .await;
    seed_task(&pool, &running_task, Seed {
        status: "CLAIMED",
        is_workflow_task: true,
        ..Seed::default()
    })
    .await;
    seed_wf_task(&pool, &paused_wf, &paused_task, "ENQUEUED").await;
    seed_wf_task(&pool, &running_wf, &running_task, "ENQUEUED").await;

    let outcomes = terminalize(&pool, &TerminalizationCommand::AbandonNodesOfPausedWorkflows {
        workflow_ids: vec![paused_wf.clone(), running_wf.clone()],
    })
    .await
    .expect("terminalize");

    assert_eq!(outcomes.len(), 1, "only the paused workflow's claim is reached");
    assert!(matches!(
        &outcomes[0],
        TerminalizationOutcome::Applied { kind: TerminalizationKind::PauseAbandonWorkflow, observed, .. }
            if observed.worker_id.as_deref() == Some("w-remote")
    ));
    assert_eq!(outcomes[0].task_id(), paused_task);
    assert_eq!(post_image(&pool, &running_task).await.status, "CLAIMED");

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
    seed_task(&pool, &requeued, Seed {
        status: "PENDING",
        worker: None,
        claimed_at: None,
        is_workflow_task: true,
        ..Seed::default()
    })
    .await;

    let refused = terminalize(&pool, &TerminalizationCommand::CancelOwnedNode {
        task_id: requeued.clone(),
        fence: owned("w1", None),
        accepts_requeued_pending: false,
    })
    .await
    .expect("terminalize");
    assert!(matches!(refused.as_slice(), [TerminalizationOutcome::LostClaim { .. }]));
    assert_eq!(post_image(&pool, &requeued).await.status, "PENDING");

    let outcomes = terminalize(&pool, &TerminalizationCommand::CancelOwnedNode {
        task_id: requeued.clone(),
        fence: owned("w1", None),
        accepts_requeued_pending: true,
    })
    .await
    .expect("terminalize");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { kind: TerminalizationKind::WorkflowCancelClaim, .. }]
    ));
    assert_eq!(post_image(&pool, &requeued).await.status, "CANCELLED");

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
    seed_task(&pool, &a, Seed {
        status: "CLAIMED",
        claimed_at: Some(gen_a),
        is_workflow_task: true,
        ..Seed::default()
    })
    .await;
    seed_task(&pool, &b, Seed {
        status: "CLAIMED",
        claimed_at: Some(gen_b),
        is_workflow_task: true,
        ..Seed::default()
    })
    .await;

    let fence = OwnedClaimBatch::new(
        "w1".to_owned(),
        vec![(a.clone(), Some(gen_a)), (b.clone(), Some(gen_b))],
    )
    .unwrap();
    let outcomes = terminalize(&pool, &TerminalizationCommand::CancelOwnedNodes { fence })
        .await
        .expect("terminalize");
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|o| matches!(
        o,
        TerminalizationOutcome::Applied { kind: TerminalizationKind::WorkflowCancelClaimBatch, .. }
    )));
    for id in [&a, &b] {
        let image = post_image(&pool, id).await;
        assert_eq!(image.status, "CANCELLED");
        assert_eq!(image.claimed_by_worker_id, None);
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
    seed_task(&pool, &briefly_running, Seed { is_workflow_task: true, ..Seed::default() }).await;
    seed_wf_task(&pool, &wf_id, &briefly_running, "ENQUEUED").await;
    // A node already RUNNING is not: its user code is executing.
    seed_task(&pool, &started, Seed { is_workflow_task: true, ..Seed::default() }).await;
    seed_wf_task(&pool, &wf_id, &started, "RUNNING").await;

    let outcomes =
        terminalize(&pool, &TerminalizationCommand::CancelNodesOfCancelledWorkflow {
            workflow_ids: vec![wf_id.clone()],
        })
        .await
        .expect("terminalize");

    assert_eq!(outcomes.len(), 1);
    assert!(matches!(
        &outcomes[0],
        TerminalizationOutcome::Applied { kind: TerminalizationKind::WorkflowCancelWorkflow, observed, .. }
            if observed.status == Some(TaskStatus::Running)
    ));
    assert_eq!(outcomes[0].task_id(), briefly_running);
    assert_eq!(post_image(&pool, &briefly_running).await.status, "CANCELLED");
    assert_eq!(post_image(&pool, &started).await.status, "RUNNING");

    cleanup(&pool, &[&briefly_running, &started]).await;
    cleanup_workflow(&pool, &wf_id).await;
}

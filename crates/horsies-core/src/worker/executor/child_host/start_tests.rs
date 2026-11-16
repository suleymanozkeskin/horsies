//! The start of a run in a Rust child on the test database: one
//! `horsies_start_attempt` statement, and the stop of each miss (Python
//! `horsies/core/worker/child_runner.py`, `_confirm_ownership_and_set_running`,
//! `_after_missed_start`).

use std::os::unix::net::UnixStream;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::host::{ChildApp, OrphanSelfHeal};
use super::link::FrameWriter;
use super::run::{start_attempt, HostContext, RunFacts, Started};
use crate::broker::PostgresBroker;
use crate::core::registry::task::TaskRegistry;
use crate::worker::executor::protocol::{StopReason, TaskKind};

const WORKER_ID: &str = "start-attempt-worker";
const PID: i32 = 4242;

/// A child context on `pool`; its frames go to a socket no one reads.
fn context(pool: PgPool) -> (HostContext, UnixStream) {
    let (ours, theirs) = UnixStream::pair().expect("a socket pair");
    let writer = FrameWriter::start(ours, 16).expect("the frame writer starts");
    let context = HostContext {
        broker: Arc::new(PostgresBroker::from_pool(pool)),
        app: Arc::new(ChildApp {
            registry: Arc::new(TaskRegistry::new()),
            orphan_self_heal: OrphanSelfHeal::Off,
            hostname: "start-host".to_owned(),
        }),
        pid: PID,
        writer,
    };
    (context, theirs)
}

/// A CLAIMED plain row held by [`WORKER_ID`]; `good_until` passed when
/// `expired`. Returns the id and the claim generation.
async fn seed_claimed(pool: &PgPool, expired: bool) -> (Uuid, DateTime<Utc>) {
    let id = Uuid::new_v4();
    let generation = sqlx::query_scalar(
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
            $1, 'app.tasks.start', 'default', 100, '{}', NULL, 'ISOLATED',
            'forever', NOW(), NOW(),
            1, decode(repeat('07', 32), 'hex'), NULL,
            'CLAIMED', NOW(), date_trunc('milliseconds', NOW()), $2,
            NOW() + interval '1 minute',
            NULL, NULL, NULL,
            NULL, NULL, NULL,
            0, 0, NULL,
            CASE WHEN $3 THEN NOW() - interval '1 second' ELSE NULL END, FALSE,
            NOW(), FALSE, NULL, NULL
         ) RETURNING claimed_at",
    )
    .bind(id)
    .bind(WORKER_ID)
    .bind(expired)
    .fetch_one(pool)
    .await
    .expect("seed a CLAIMED row");
    (id, generation)
}

fn facts(task_id: Uuid, claimed_at: DateTime<Utc>) -> RunFacts {
    RunFacts {
        task_id,
        task_name: "app.tasks.start".to_owned(),
        worker_id: WORKER_ID.to_owned(),
        task_kind: TaskKind::Plain,
        claimed_at,
    }
}

async fn live_row(pool: &PgPool, task_id: Uuid) -> Option<serde_json::Value> {
    sqlx::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the live row")
}

async fn remove_task(pool: &PgPool, task_id: Uuid) {
    for statement in [
        "DELETE FROM horsies_heartbeats WHERE task_id = $1",
        "DELETE FROM horsies_tasks WHERE id = $1",
        "DELETE FROM horsies_task_history WHERE task_id = $1",
    ] {
        sqlx::query(statement)
            .bind(task_id)
            .execute(pool)
            .await
            .expect("remove the test task");
    }
}

/// Python `TestConfirmOwnership` (the start): a held claim is set RUNNING in
/// one statement, with the process of the child: its pid and the name
/// `worker-<pid>`.
#[tokio::test]
#[serial]
async fn a_held_claim_starts_running_with_the_process_of_its_child() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let (context, _peer) = context(pool.clone());

    let started = start_attempt(&context, &facts(task_id, generation)).await;

    assert!(matches!(started, Started::Running));
    let row = live_row(&pool, task_id).await.expect("the row stays live");
    assert_eq!(row["status"], "RUNNING");
    assert_eq!(row["worker_pid"], PID);
    assert_eq!(row["worker_hostname"], "start-host");
    assert_eq!(row["worker_process_name"], format!("worker-{PID}"));
    remove_task(&pool, task_id).await;
}

/// Python `_after_missed_start`, `NOT_HELD`: a start of an older claim
/// generation writes nothing and stops with `CLAIM_LOST`.
#[tokio::test]
#[serial]
async fn a_start_of_another_claim_generation_is_a_lost_claim() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let (context, _peer) = context(pool.clone());
    let before = live_row(&pool, task_id).await;

    let started = start_attempt(
        &context,
        &facts(task_id, generation - chrono::Duration::seconds(5)),
    )
    .await;

    assert!(matches!(started, Started::Stopped(StopReason::ClaimLost)));
    assert_eq!(live_row(&pool, task_id).await, before);
    remove_task(&pool, task_id).await;
}

/// Python `_after_missed_start`, `DEADLINE_PASSED`: the claim of a row whose
/// `good_until` passed is expired, and the run stops with `TASK_EXPIRED`.
#[tokio::test]
#[serial]
async fn a_passed_deadline_expires_the_claim_before_the_start() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, true).await;
    let (context, _peer) = context(pool.clone());

    let started = start_attempt(&context, &facts(task_id, generation)).await;

    assert!(matches!(started, Started::Stopped(StopReason::TaskExpired)));
    assert_eq!(live_row(&pool, task_id).await, None);
    let status: Option<String> =
        sqlx::query_scalar("SELECT status::text FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .fetch_optional(&pool)
            .await
            .expect("read the history row");
    assert_eq!(status.as_deref(), Some("EXPIRED"));
    remove_task(&pool, task_id).await;
}

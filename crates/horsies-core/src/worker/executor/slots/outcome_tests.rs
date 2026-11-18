//! The writes of [`OutcomeWriter::settle`] for a child that stopped before
//! RUNNING, and for a reported result that is not a result envelope, on a
//! migrated database.
//!
//! Python specification: `tests/unit/test_unconfirmed_start_release.py` and
//! the unconfirmed-start cases of
//! `tests/integration/test_worker_persist_terminal_state.py` (an
//! `OWNERSHIP_UNCONFIRMED` stop releases the claim of its dispatch
//! generation at once; a later claim of the row is not touched), and
//! `test_worker_persist_terminal_state.py::test_invalid_task_result_structure_marks_error`.

use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;
use crate::broker::terminalization_matrix::sqlx_migrated_pool as migrated_pool;
use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::task::error::{OperationalErrorCode, TaskErrorCode};
use crate::core::task::result::{read_task_result_text, TaskResultText};
use crate::test_support::{CapturedEvent, LogCapture};
use crate::worker::executor::protocol::{ResultEnvelopeText, RunEnd, StopReason};
use crate::worker::retry::read_stored_options;

const WORKER: &str = "outcome-stop-worker";

/// A CLAIMED plain row of `WORKER` whose claim lease runs one more minute.
/// Every baseline column is named. Returns the claim generation.
async fn seed_claimed(pool: &PgPool, task_id: Uuid) -> DateTime<Utc> {
    sqlx::query_scalar(
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
            $1, 'outcome_stop_task', 'default', 100, '{}', NULL, 'SHARED_THREAD',
            'forever', NOW(), NOW(),
            1, decode(repeat('00', 32), 'hex'), NULL,
            'CLAIMED', NOW(), NOW(), $2, NOW() + interval '1 minute',
            NULL, NULL, NULL,
            NULL, NULL, NULL,
            0, 0, NULL, NULL, FALSE,
            NOW(), FALSE, NULL, NULL
        )
        RETURNING claimed_at",
    )
    .bind(task_id)
    .bind(WORKER)
    .fetch_one(pool)
    .await
    .expect("seed a claimed row")
}

/// The claimed task of the dispatch at `generation`.
fn dispatched(task_id: Uuid, generation: DateTime<Utc>) -> ClaimedTask {
    ClaimedTask {
        task_id,
        task_name: "outcome_stop_task".to_owned(),
        queue_name: "default".to_owned(),
        claimed_at: generation,
        execution: ExecutionClass::SharedThread,
        kind: TaskKind::Plain,
        retry: RetryFacts {
            retry_count: 0,
            max_retries: 0,
        },
        options: read_stored_options(None),
    }
}

/// The writer of `WORKER`, with the orphan self-heal off: a stop for a node
/// that is not linked then writes nothing (`settle_tests` covers the
/// self-heal).
fn writer(pool: &PgPool) -> OutcomeWriter {
    writer_with_payload(pool, PayloadPolicy::default())
}

/// The writer of [`writer`] with the payload policy `payload`.
fn writer_with_payload(pool: &PgPool, payload: PayloadPolicy) -> OutcomeWriter {
    let broker = Arc::new(PostgresBroker::from_pool(
        pool.clone(),
        crate::ClientFamily::Rust,
    ));
    let successes = super::outcome::test_successes(&broker, WORKER);
    OutcomeWriter::new(
        broker,
        crate::test_support::child_specs::no_child_specs(),
        WORKER.to_owned(),
        payload,
        successes,
    )
}

fn stopped(reason: StopReason) -> AttemptEnd {
    AttemptEnd::Reported(RunEnd::StoppedBeforeRunning(reason))
}

async fn snapshot(pool: &PgPool, task_id: Uuid) -> serde_json::Value {
    sqlx::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read the row")
}

async fn attempts(pool: &PgPool, task_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_task_attempts WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("count the attempts")
}

async fn remove(pool: &PgPool, task_id: Uuid) {
    sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .execute(pool)
        .await
        .expect("remove the row");
}

/// An unconfirmed start returns the row of its dispatch generation to the
/// queue at once, with its class and every claim and run field cleared, and
/// writes no attempt.
#[tokio::test]
#[serial]
async fn an_unconfirmed_start_releases_the_claim_at_once() {
    let pool = migrated_pool().await;
    let task_id = Uuid::new_v4();
    let generation = seed_claimed(&pool, task_id).await;

    let settled = writer(&pool)
        .settle(
            &dispatched(task_id, generation),
            stopped(StopReason::OwnershipUnconfirmed),
        )
        .await;

    assert_eq!(settled, Settlement::Written);
    let row = snapshot(&pool, task_id).await;
    assert_eq!(row["status"], "PENDING");
    assert_eq!(row["execution"], "SHARED_THREAD");
    for cleared in [
        "claimed_at",
        "claimed_by_worker_id",
        "claim_expires_at",
        "started_at",
        "worker_pid",
        "worker_hostname",
        "worker_process_name",
        "finalizing_at",
        "finalizing_by_worker_id",
    ] {
        assert!(row[cleared].is_null(), "{cleared} is not cleared: {row}");
    }
    assert_eq!(attempts(&pool, task_id).await, 0);
    remove(&pool, task_id).await;
}

/// The release is fenced on the dispatch generation: a row that the same
/// worker claimed again is not touched.
#[tokio::test]
#[serial]
async fn an_unconfirmed_start_does_not_touch_a_later_claim() {
    let pool = migrated_pool().await;
    let task_id = Uuid::new_v4();
    let later = seed_claimed(&pool, task_id).await;
    let before = snapshot(&pool, task_id).await;

    let settled = writer(&pool)
        .settle(
            &dispatched(task_id, later - ChronoDuration::seconds(1)),
            stopped(StopReason::OwnershipUnconfirmed),
        )
        .await;

    assert_eq!(settled, Settlement::NotHeld);
    assert_eq!(snapshot(&pool, task_id).await, before);
    remove(&pool, task_id).await;
}

/// Every other stop of a plain task owes nothing: the child's start wrote
/// what its case needs, and the parent leaves the row alone. With the
/// orphan self-heal off, `NODE_NOT_LINKED` writes nothing either, and an
/// orphan that the child's stop function cancelled owes nothing.
#[tokio::test]
#[serial]
async fn other_stops_of_a_plain_task_write_nothing() {
    let pool = migrated_pool().await;
    for reason in [
        StopReason::ClaimLost,
        StopReason::WorkflowStopped,
        StopReason::TaskExpired,
        StopReason::NodeNotLinked,
        StopReason::OrphanCancelled,
    ] {
        let task_id = Uuid::new_v4();
        let generation = seed_claimed(&pool, task_id).await;
        let before = snapshot(&pool, task_id).await;

        let settled = writer(&pool)
            .settle(&dispatched(task_id, generation), stopped(reason))
            .await;

        assert_eq!(settled, Settlement::NothingOwed, "{reason:?}");
        assert_eq!(snapshot(&pool, task_id).await, before, "{reason:?}");
        remove(&pool, task_id).await;
    }
}

/// A RUNNING plain row of `WORKER`, as the child's start leaves it. Every
/// baseline column is named. Returns the claim generation.
async fn seed_running(pool: &PgPool, task_id: Uuid) -> DateTime<Utc> {
    sqlx::query_scalar(
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
            $1, 'outcome_stop_task', 'default', 100, '{}', NULL, 'SHARED_THREAD',
            'forever', NOW(), NOW(),
            1, decode(repeat('00', 32), 'hex'), NULL,
            'RUNNING', NOW(), NOW(), $2, NULL,
            NOW(), NULL, NULL,
            4242, 'outcome-host', 'outcome-process',
            0, 0, NULL, NULL, FALSE,
            NOW(), FALSE, NULL, NULL
        )
        RETURNING claimed_at",
    )
    .bind(task_id)
    .bind(WORKER)
    .fetch_one(pool)
    .await
    .expect("seed a running row")
}

/// The terminal facts of a history row: status, error code, retry count and
/// the stored result as text.
type HistoryResult = (String, Option<String>, i32, Option<String>);

async fn history_result(pool: &PgPool, task_id: Uuid) -> Option<HistoryResult> {
    sqlx::query_as(
        "SELECT status::text, error_code, retry_count, convert_from(result_payload, 'UTF8')
         FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await
    .expect("read the history row")
}

async fn phase_two_record_waits(pool: &PgPool, task_id: Uuid) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = $1)",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read the phase-2 record")
}

async fn remove_history(pool: &PgPool, task_id: Uuid) {
    sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .execute(pool)
        .await
        .expect("remove the history row");
}

/// Python `test_worker_persist_terminal_state.py::test_invalid_task_result_structure_marks_error`:
/// a reported result that is valid JSON but not a result envelope
/// (`{"random": 1}`) fails the task with `WORKER_SERIALIZATION_ERROR`, with
/// no retry. A plain task owes no phase 2: the row moves to history and no
/// phase-2 record waits for it.
#[tokio::test]
#[serial]
async fn a_result_that_is_json_but_not_an_envelope_fails_the_task_as_a_serialization_error() {
    let pool = migrated_pool().await;
    let task_id = Uuid::new_v4();
    let generation = seed_running(&pool, task_id).await;
    let not_an_envelope = ResultEnvelopeText::new(r#"{"random": 1}"#.to_owned());

    let settled = writer(&pool)
        .settle(
            &dispatched(task_id, generation),
            AttemptEnd::Reported(RunEnd::Result(not_an_envelope)),
        )
        .await;

    assert_eq!(settled, Settlement::Written);
    let (status, error_code, retry_count, stored) = history_result(&pool, task_id)
        .await
        .expect("the task moved to history");
    assert_eq!(status, "FAILED");
    assert_eq!(error_code.as_deref(), Some("WORKER_SERIALIZATION_ERROR"));
    assert_eq!(retry_count, 0);
    let stored = stored.expect("the failure stores its error envelope");
    match read_task_result_text(&stored) {
        Ok(TaskResultText::Err(error)) => assert_eq!(
            error.error_code,
            TaskErrorCode::from(OperationalErrorCode::WorkerSerializationError),
        ),
        other => panic!("the stored result is the worker's error envelope, got {other:?}"),
    }
    let live: Option<String> = sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_optional(&pool)
        .await
        .expect("read the live row");
    assert_eq!(live, None);
    assert!(!phase_two_record_waits(&pool, task_id).await);
    remove_history(&pool, task_id).await;
}

/// Python `test_worker_helpers.py::TestFinalizeResultPayloadWarn::test_oversized_result_warns`:
/// a result over `payload.warn_bytes` gives one warning that names the task
/// and the result payload, and the result is still written. The warning is
/// logged once per task name and process, so the task name is this test's
/// own.
#[tokio::test]
#[serial]
async fn an_oversized_result_is_logged_once_and_written() {
    let pool = migrated_pool().await;
    let task_id = Uuid::new_v4();
    let generation = seed_running(&pool, task_id).await;
    let task_name = format!("big_result_task_{}", task_id.simple());
    let mut task = dispatched(task_id, generation);
    task.task_name = task_name.clone();
    let envelope = format!(
        r#"{{"__h_task_result__":true,"ok":{{"big":"{}"}},"err":null}}"#,
        "x".repeat(1024)
    );
    let capture = LogCapture::new();
    let _default = capture.set_default();

    let settled = writer_with_payload(
        &pool,
        PayloadPolicy {
            warn_bytes: Some(64),
            reject_bytes: None,
        },
    )
    .settle(
        &task,
        AttemptEnd::Reported(RunEnd::Result(ResultEnvelopeText::new(envelope))),
    )
    .await;

    assert_eq!(settled, Settlement::Written);
    let warnings: Vec<CapturedEvent> =
        capture.lines_starting(tracing::Level::WARN, "payload size guardrail");
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(warnings[0].field("task_name"), Some(task_name.as_str()));
    assert_eq!(warnings[0].field("kind"), Some("result"));
    let (status, ..) = history_result(&pool, task_id)
        .await
        .expect("the task moved to history");
    assert_eq!(status, "COMPLETED");
    remove_history(&pool, task_id).await;
}

//! Port of Python `tests/integration/test_autocommit_outcomes.py`.
//!
//! The fused completion runs as its own transaction. These tests pin what
//! the caller relies on: a replay and a lost claim write nothing (no history
//! row, no attempt, no wake), and a lost reply or a dead pooled connection
//! is a retryable error whose retry ends the task once.
//!
//! The Python tests reach the writes through the worker's finalize. The
//! core's phase-1 retry is private to the worker; these ports call the
//! writer and repeat the same command, as the retry does.
//! `test_a_second_statement_is_refused_by_the_server` tests a Python helper
//! and is not ported (test map: NOT CORE).

use chrono::Duration;
use serial_test::serial;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::*;
use crate::broker::error::{is_retryable_sqlx_error, BrokerError};
use crate::broker::postgres::PostgresBroker;
use crate::broker::terminalization::terminalize;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::lifecycle::{
    OwnedClaim, TerminalizationCommand, TerminalizationKind, TerminalizationOutcome,
};
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::worker::execution::{finalize_workflow_phase, FinalizeStage};

const QUEUE: &str = "lifecycle_autocommit";
const CHANNEL: &str = "task_queue_lifecycle_autocommit";
const CLAIM_QUEUE: &str = "lifecycle_dead_claim";
/// Bound of the wait for the sentinel notification.
const NOTIFY_WAIT: std::time::Duration = std::time::Duration::from_secs(5);
/// Bound of the notifications read before the sentinel.
const NOTIFY_LIMIT: u32 = 100;

fn complete(task_id: Uuid, fence: OwnedClaim) -> TerminalizationCommand {
    TerminalizationCommand::CompleteTaskFused {
        task_id,
        fence,
        result_json: r#"{"ok": 1}"#.to_owned(),
        notify_channel: CHANNEL.to_owned(),
        notify_payload: format!("capacity:{task_id}"),
    }
}

fn running_in_queue() -> TaskSeed {
    TaskSeed {
        queue_name: QUEUE,
        ..TaskSeed::running()
    }
}

async fn listen(pool: &PgPool) -> PgListener {
    let mut listener = PgListener::connect_with(pool)
        .await
        .expect("connect the listener");
    listener.listen(CHANNEL).await.expect("listen");
    listener
}

/// Every payload on `CHANNEL` committed before a sentinel notification that
/// this call commits. Postgres delivers notifications in commit order.
async fn payloads_until_sentinel(listener: &mut PgListener, pool: &PgPool) -> Vec<String> {
    let sentinel = format!("sentinel:{}", Uuid::new_v4());
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(CHANNEL)
        .bind(&sentinel)
        .execute(pool)
        .await
        .expect("send the sentinel");
    let mut payloads = Vec::new();
    for _ in 0..NOTIFY_LIMIT {
        let notification = tokio::time::timeout(NOTIFY_WAIT, listener.recv())
            .await
            .expect("a notification within the wait")
            .expect("receive a notification");
        if notification.payload() == sentinel {
            return payloads;
        }
        payloads.push(notification.payload().to_owned());
    }
    panic!("sentinel not seen; payloads {payloads:?}");
}

async fn history_rows(pool: &PgPool, task_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("count history rows")
}

fn assert_retryable(result: Result<Vec<TerminalizationOutcome>, BrokerError>) {
    match result {
        Err(BrokerError::Database(error)) => {
            assert!(is_retryable_sqlx_error(&error), "not retryable: {error}");
        }
        other => panic!("expected a retryable database error, got {other:?}"),
    }
}

/// Python `test_a_replay_is_already_applied_and_writes_nothing`.
#[tokio::test]
#[serial]
async fn a_replay_is_already_applied_and_writes_nothing() {
    let pool = pool().await;
    let mut listener = listen(&pool).await;
    let task_id = seed_task(&pool, &running_in_queue()).await;
    let command = complete(task_id, owned_claim());

    let first = terminalize(&pool, &command).await.expect("completion");
    assert!(
        matches!(first.as_slice(), [TerminalizationOutcome::Applied { .. }]),
        "{first:?}"
    );
    assert_eq!(
        payloads_until_sentinel(&mut listener, &pool).await,
        vec![format!("capacity:{task_id}")]
    );

    let replay = terminalize(&pool, &command).await.expect("replay");

    assert!(
        matches!(
            replay.as_slice(),
            [TerminalizationOutcome::AlreadyApplied {
                kind: TerminalizationKind::CompleteFused,
                ..
            }]
        ),
        "{replay:?}"
    );
    assert_eq!(history_rows(&pool, task_id).await, 1);
    assert_eq!(archived_attempts(&pool, task_id).await.len(), 1);
    assert!(payloads_until_sentinel(&mut listener, &pool)
        .await
        .is_empty());
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_a_lost_claim_generation_writes_nothing`.
#[tokio::test]
#[serial]
async fn a_lost_claim_generation_writes_nothing() {
    let pool = pool().await;
    let mut listener = listen(&pool).await;
    let task_id = seed_task(&pool, &running_in_queue()).await;
    let before = live_row(&pool, task_id).await;

    let outcomes = terminalize(
        &pool,
        &complete(
            task_id,
            OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at: generation() + Duration::seconds(1),
            },
        ),
    )
    .await
    .expect("completion");

    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::LostClaim { .. }]
        ),
        "{outcomes:?}"
    );
    assert_eq!(live_row(&pool, task_id).await, before);
    assert_eq!(history_rows(&pool, task_id).await, 0);
    assert!(live_attempts(&pool, task_id).await.is_empty());
    assert!(payloads_until_sentinel(&mut listener, &pool)
        .await
        .is_empty());
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_a_lost_reply_is_retried_and_classified_on_the_replay`: the
/// completion commits and its reply never reaches the caller. The error is
/// retryable, and the retry is `AlreadyApplied`.
#[tokio::test]
#[serial]
async fn a_lost_reply_is_retried_and_classified_on_the_replay() {
    let pool = pool().await;
    let upstream = database_options(&pool);
    let proxy = ReplyDroppingProxy::start(&upstream).await;
    let proxied = unpinged_single_connection_pool(proxy.options(&upstream)).await;
    // Prepare the statement on the proxied connection, so the completion is
    // one request with one reply.
    let warm_up = terminalize(&proxied, &complete(Uuid::new_v4(), owned_claim()))
        .await
        .expect("warm up");
    assert!(
        matches!(
            warm_up.as_slice(),
            [TerminalizationOutcome::TaskAbsent { .. }]
        ),
        "{warm_up:?}"
    );
    wait_until_idle(&proxied).await;
    let task_id = seed_task(&pool, &running_in_queue()).await;
    let command = complete(task_id, owned_claim());

    proxy.drop_next_reply();
    let first = terminalize(&proxied, &command).await;

    assert!(proxy.reply_was_dropped(), "the proxy relayed the reply");
    assert_retryable(first);
    // The premise of the replay: the statement committed.
    assert_eq!(live_row(&pool, task_id).await, None);
    assert_eq!(history_rows(&pool, task_id).await, 1);
    let retry = terminalize(&pool, &command).await.expect("retry");
    assert!(
        matches!(
            retry.as_slice(),
            [TerminalizationOutcome::AlreadyApplied {
                kind: TerminalizationKind::CompleteFused,
                ..
            }]
        ),
        "{retry:?}"
    );
    assert_eq!(history_rows(&pool, task_id).await, 1);
    assert_eq!(archived_attempts(&pool, task_id).await.len(), 1);
    proxied.close().await;
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_a_dead_connection_fails_the_completion_and_the_retry_applies`:
/// with no ping at checkout, the dead connection shows at the completion,
/// which never reached the server; the retry runs on a new connection.
#[tokio::test]
#[serial]
async fn a_dead_connection_fails_the_completion_and_the_retry_applies() {
    let pool = pool().await;
    let unpinged = unpinged_single_connection_pool(database_options(&pool)).await;
    let pid = pooled_backend_pid(&unpinged).await;
    let task_id = seed_task(&pool, &running_in_queue()).await;
    let before = live_row(&pool, task_id).await;
    let command = complete(task_id, owned_claim());
    terminate_backend(&pool, pid).await;

    let first = terminalize(&unpinged, &command).await;

    assert_retryable(first);
    assert_eq!(live_row(&pool, task_id).await, before);
    let retry = terminalize(&unpinged, &command)
        .await
        .expect("retry on a new connection");
    assert!(
        matches!(
            retry.as_slice(),
            [TerminalizationOutcome::Applied {
                kind: TerminalizationKind::CompleteFused,
                ..
            }]
        ),
        "{retry:?}"
    );
    assert_eq!(live_row(&pool, task_id).await, None);
    assert_eq!(history_rows(&pool, task_id).await, 1);
    unpinged.close().await;
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_a_dead_connection_fails_phase_two_as_retryable`: the wake of
/// phase 2 runs on the dead pooled connection, and its error is a retryable
/// phase-2 error.
#[tokio::test]
#[serial]
#[ignore = "waits on #722"]
async fn a_dead_connection_fails_phase_two_as_retryable() {
    let pool = pool().await;
    let unpinged = unpinged_single_connection_pool(database_options(&pool)).await;
    let pid = pooled_backend_pid(&unpinged).await;
    terminate_backend(&pool, pid).await;

    let result = finalize_workflow_phase(
        &unpinged,
        &WorkflowSpecRegistry::new(),
        Uuid::new_v4(),
        true,
        QUEUE,
        false,
        false,
        &PayloadPolicy::default(),
        &RetentionConfig::default(),
    )
    .await;

    match result {
        Err(error) => {
            assert_eq!(error.stage, FinalizeStage::Phase2Workflow);
            assert!(error.retryable, "{error}");
        }
        Ok(()) => panic!("the wake succeeded on a dead connection"),
    }
    unpinged.close().await;
}

/// Python `test_a_dead_connection_fails_the_claim_pass_as_retryable`: a claim
/// pass on the dead pooled connection is a retryable error, and the next pass
/// claims the row.
#[tokio::test]
#[serial]
async fn a_dead_connection_fails_the_claim_pass_as_retryable() {
    let pool = pool().await;
    let unpinged = unpinged_single_connection_pool(database_options(&pool)).await;
    let pid = pooled_backend_pid(&unpinged).await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            queue_name: CLAIM_QUEUE,
            ..TaskSeed::pending()
        },
    )
    .await;
    terminate_backend(&pool, pid).await;
    let broker = PostgresBroker::from_pool(unpinged.clone());

    match broker.claim_batch(&claim_pass(CLAIM_QUEUE)).await {
        Err(BrokerError::Database(error)) => {
            assert!(is_retryable_sqlx_error(&error), "not retryable: {error}");
        }
        other => panic!("expected a retryable database error, got {other:?}"),
    }
    assert_eq!(status_of(&pool, task_id).await, "PENDING");

    let rows = broker
        .claim_batch(&claim_pass(CLAIM_QUEUE))
        .await
        .expect("the next pass");
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![task_id]
    );
    unpinged.close().await;
    cleanup_tasks(&pool, &[task_id]).await;
}

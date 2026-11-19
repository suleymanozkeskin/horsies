//! The fused completion runs as its own transaction. These tests pin what
//! the caller relies on: a replay and a lost claim write nothing (no history
//! row, no attempt, no wake), and a lost reply or a dead pooled connection
//! is a retryable error whose retry ends the task once.
//!
//! The Python tests reach the writes through the worker's finalize. The
//! core's phase-1 retry is private to the worker; these ports call the
//! writer and repeat the same command, as the retry does. The completion
//! and the claim pass run on a core pool.
//! `test_a_second_statement_is_refused_by_the_server` tests a Python helper
//! and is not ported (test map: NOT CORE).

use chrono::Duration;
use serial_test::serial;
use uuid::Uuid;

use super::support::*;
use crate::broker::claims::claim_pass;
use crate::broker::core_rows::StatementLog;
use crate::broker::error::BrokerError;
use crate::broker::pools::unpinged_single_connection_core_pool_for_url;
use crate::broker::postgres::PostgresBroker;
use crate::broker::terminalization;
use crate::broker::terminalization::on_test_pool::terminalize;
use crate::core::lifecycle::{
    OwnedClaim, TerminalizationCommand, TerminalizationKind, TerminalizationOutcome,
};
use crate::db::StatementFailure;
use crate::test_support::sql::{self, TestPool};
use crate::worker::execution::{
    finalize_workflow_phase, FinalizeError, FinalizeStage, PhaseTwoForm,
};

const QUEUE: &str = "lifecycle_autocommit";
const CHANNEL: &str = "task_queue_lifecycle_autocommit";
const CLAIM_QUEUE: &str = "lifecycle_dead_claim";

fn complete(task_id: Uuid, fence: OwnedClaim) -> TerminalizationCommand {
    TerminalizationCommand::CompleteTaskFused {
        task_id,
        fence,
        result_json: r#"{"ok": 1}"#.to_owned(),
    }
}

fn running_in_queue() -> TaskSeed {
    TaskSeed {
        queue_name: QUEUE,
        ..TaskSeed::running()
    }
}

async fn history_rows(pool: &TestPool, task_id: Uuid) -> i64 {
    sql::query_scalar("SELECT count(*) FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("count history rows")
}

fn assert_retryable(result: Result<Vec<TerminalizationOutcome>, BrokerError>) {
    match result {
        Err(error @ BrokerError::Statement(_)) => {
            assert!(error.is_retryable(), "not retryable: {error}");
        }
        other => panic!("expected a retryable statement failure, got {other:?}"),
    }
}

/// Python `test_a_replay_is_already_applied_and_writes_nothing`.
#[tokio::test]
#[serial]
async fn a_replay_is_already_applied_and_writes_nothing() {
    let pool = pool().await;
    let mut wakes = Wakes::listen(&pool, CHANNEL).await;
    let task_id = seed_task(&pool, &running_in_queue()).await;
    let command = complete(task_id, owned_claim());

    let first = terminalize(&pool, &command).await.expect("completion");
    assert!(
        matches!(first.as_slice(), [TerminalizationOutcome::Applied { .. }]),
        "{first:?}"
    );
    assert_eq!(wakes.payloads(&pool).await, vec!["capacity".to_owned()]);

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
    assert!(wakes.payloads(&pool).await.is_empty());
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_a_lost_claim_generation_writes_nothing`.
#[tokio::test]
#[serial]
async fn a_lost_claim_generation_writes_nothing() {
    let pool = pool().await;
    let mut wakes = Wakes::listen(&pool, CHANNEL).await;
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
    assert!(wakes.payloads(&pool).await.is_empty());
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_a_lost_reply_is_retried_and_classified_on_the_replay`: the
/// completion commits and its reply never reaches the caller. The failure is
/// `OutcomeUnknown` and retryable, and the retry is `AlreadyApplied`.
#[tokio::test]
#[serial]
async fn a_lost_reply_is_retried_and_classified_on_the_replay() {
    let pool = pool().await;
    let upstream = pool.url();
    let proxy = WireProxy::start(upstream).await;
    let proxied = unpinged_single_connection_core_pool_for_url(&proxy.url(upstream));
    // Open the connection, run the pool's first-use check and prepare the
    // statement on it, so the completion is one request with one reply. The
    // connection is back in the pool when the call returns.
    let warm_up = terminalization::terminalize(
        &proxied,
        StatementLog::Off,
        &complete(Uuid::new_v4(), owned_claim()),
    )
    .await
    .expect("warm up");
    assert!(
        matches!(
            warm_up.as_slice(),
            [TerminalizationOutcome::TaskAbsent { .. }]
        ),
        "{warm_up:?}"
    );
    let task_id = seed_task(&pool, &running_in_queue()).await;
    let command = complete(task_id, owned_claim());

    proxy.drop_next_reply();
    let first = terminalization::terminalize(&proxied, StatementLog::Off, &command).await;

    assert!(proxy.reply_was_dropped(), "the proxy relayed the reply");
    assert!(
        matches!(
            first,
            Err(BrokerError::Statement(
                StatementFailure::OutcomeUnknown { .. }
            ))
        ),
        "{first:?}"
    );
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
    proxied.close();
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_a_dead_connection_fails_the_completion_and_the_retry_applies`,
/// on a core pool with no idle ping. The pool replaces a pooled connection
/// that its driver already saw closed, at checkout; a connection that it
/// sees closed only at the send fails the completion before the server got
/// it. Either way the completion applies once: the first call applies, or it
/// fails as retryable with nothing written and the retry applies.
#[tokio::test]
#[serial]
async fn a_dead_connection_fails_the_completion_and_the_retry_applies() {
    let pool = pool().await;
    let unpinged = unpinged_single_connection_core_pool_for_url(pool.url());
    let pid = core_pool_backend_pid(&unpinged).await;
    let task_id = seed_task(&pool, &running_in_queue()).await;
    let before = live_row(&pool, task_id).await;
    let command = complete(task_id, owned_claim());
    terminate_backend(&pool, pid).await;

    let first = terminalization::terminalize(&unpinged, StatementLog::Off, &command).await;

    let applied = match first {
        Ok(outcomes) => outcomes,
        failed @ Err(_) => {
            assert_retryable(failed);
            assert_eq!(live_row(&pool, task_id).await, before);
            terminalization::terminalize(&unpinged, StatementLog::Off, &command)
                .await
                .expect("retry on a new connection")
        }
    };
    assert!(
        matches!(
            applied.as_slice(),
            [TerminalizationOutcome::Applied {
                kind: TerminalizationKind::CompleteFused,
                ..
            }]
        ),
        "{applied:?}"
    );
    assert_ne!(core_pool_backend_pid(&unpinged).await, pid);
    assert_eq!(live_row(&pool, task_id).await, None);
    assert_eq!(history_rows(&pool, task_id).await, 1);
    unpinged.close();
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Phase 2 of a plain task on `broker`: the capacity wake of [`QUEUE`], which
/// phase 1 did not send.
async fn plain_phase_two(broker: &PostgresBroker) -> Result<(), FinalizeError> {
    finalize_workflow_phase(
        broker,
        &crate::test_support::child_specs::TestChildSpecs::empty(),
        Uuid::new_v4(),
        QUEUE,
        false,
        false,
        PhaseTwoForm::NotKnown,
    )
    .await
    .map(|_parent_advances| ())
}

/// A connection lost while the phase-2 wake is in flight fails phase 2 as
/// retryable.
///
/// The wake of a plain task runs on the broker's main core pool, and the
/// broker's core pools connect through the proxy (`WireProxy`). The first
/// wake opens the pool's one connection, runs the pool's first-use check,
/// prepares the wake statement on it and returns it to the pool. The wake
/// under test takes that connection within the idle ping limit, so the wake
/// is its one request: the server runs it, and the proxy drops the reply and
/// closes the connection.
#[tokio::test]
#[serial]
async fn a_dead_connection_fails_phase_two_as_retryable() {
    let pool = pool().await;
    let upstream = pool.url();
    let proxy = WireProxy::start(upstream).await;
    let proxied = single_session_pool(&proxy.url(upstream)).await;
    let broker = PostgresBroker::from_test_url(proxied.url(), crate::ClientFamily::Rust);
    plain_phase_two(&broker).await.expect("warm up");
    proxy.reset_round_trips();

    proxy.drop_next_reply();
    let result = plain_phase_two(&broker).await;

    assert!(proxy.reply_was_dropped(), "the proxy relayed the reply");
    assert_eq!(
        proxy.round_trips(),
        1,
        "the wake is the one request of the pooled connection"
    );
    match result {
        Err(error) => {
            assert_eq!(error.stage, FinalizeStage::Phase2Workflow);
            assert!(error.retryable, "{error}");
        }
        Ok(()) => panic!("the wake succeeded on a dead connection"),
    }
    broker.close().await;
}

/// Python `test_a_dead_connection_fails_the_claim_pass_as_retryable`, on a
/// core pool with no idle ping. The pool replaces a pooled connection that
/// its driver already saw closed, at checkout; a connection that it sees
/// closed only at the send fails the pass before the server got it. Either
/// way the row is claimed once: the first pass claims it, or it fails as
/// retryable with the row still PENDING and the next pass claims it.
#[tokio::test]
#[serial]
async fn a_dead_connection_fails_the_claim_pass_as_retryable() {
    let pool = pool().await;
    let unpinged = unpinged_single_connection_core_pool_for_url(pool.url());
    let pid = core_pool_backend_pid(&unpinged).await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            queue_name: CLAIM_QUEUE,
            ..TaskSeed::pending()
        },
    )
    .await;
    terminate_backend(&pool, pid).await;
    let request = claim_request(CLAIM_QUEUE);

    let pass = match claim_pass(&unpinged, StatementLog::Off, &request).await {
        Ok(pass) => pass,
        Err(error @ BrokerError::Statement(_)) => {
            assert!(error.is_retryable(), "not retryable: {error}");
            assert_eq!(status_of(&pool, task_id).await, "PENDING");
            claim_pass(&unpinged, StatementLog::Off, &request)
                .await
                .expect("the next pass on a new connection")
        }
        Err(other) => panic!("expected a retryable statement failure, got {other:?}"),
    };

    assert_eq!(
        pass.claimed.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![task_id]
    );
    assert_ne!(core_pool_backend_pid(&unpinged).await, pid);
    unpinged.close();
    cleanup_tasks(&pool, &[task_id]).await;
}

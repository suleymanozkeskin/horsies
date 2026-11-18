//! Ports of the Python key reservation tests: the window bound, ownership of
//! the terminal step, replay inside the window, reuse after it, and cleanup.
//!
//! Python specification: `tests/integration/test_task_history_key_reservations.py`.
//! Each test runs in one transaction that it rolls back, on the shared
//! migrated database, under a key digest of its own.

use serial_test::serial;
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use super::reservations::{
    claim_key_reservation, cleanup_expired_reservations, terminalize_key_reservation,
    ReservationClaim,
};

const HOUR_MICROS: i64 = 3_600 * 1_000_000;
const THIRTY_ONE_DAYS_MICROS: i64 = 31 * 86_400 * 1_000_000;

fn digest(text: &str) -> Vec<u8> {
    Sha256::digest(text.as_bytes()).to_vec()
}

/// A key digest no other test uses.
fn fresh_key() -> Vec<u8> {
    digest(&format!("port-key-{}", Uuid::new_v4()))
}

async fn claim(
    connection: &mut PgConnection,
    key: &[u8],
    fingerprint: &[u8],
    task_id: Uuid,
) -> ReservationClaim {
    claim_key_reservation(connection, key, 1, HOUR_MICROS, 1, fingerprint, task_id)
        .await
        .expect("claim a reservation")
}

/// Terminalize as of two hours ago: a one-hour window has passed.
async fn terminalize_two_hours_ago(connection: &mut PgConnection, key: &[u8], task_id: Uuid) {
    let terminalized: bool = sqlx::query_scalar(
        "SELECT horsies_key_reservation_terminalize(
             $1, $2, statement_timestamp() - interval '2 hours'
         )",
    )
    .bind(key)
    .bind(task_id)
    .fetch_one(connection)
    .await
    .expect("terminalize in the past");
    assert!(terminalized);
}

async fn shared_pool() -> PgPool {
    crate::broker::terminalization_matrix::sqlx_migrated_pool().await
}

/// `TestClaimReplayConflict.test_window_above_maximum_is_rejected_before_mutation`:
/// the claim function refuses a window above 30 days and writes nothing.
#[tokio::test]
#[serial]
async fn a_window_above_thirty_days_is_refused_before_any_write() {
    let pool = shared_pool().await;
    let key = fresh_key();
    let mut connection = pool.acquire().await.expect("acquire claimer");
    let refused = claim_key_reservation(
        &mut connection,
        &key,
        1,
        THIRTY_ONE_DAYS_MICROS,
        1,
        &digest("command"),
        Uuid::new_v4(),
    )
    .await
    .expect_err("a window above 30 days is refused");
    drop(connection);
    match refused {
        crate::core::history::errors::HistoryError::Database(error) => {
            let database_error = error.as_database_error().expect("a database refusal");
            assert_eq!(database_error.code().as_deref(), Some("22023"));
            assert!(
                database_error.message().contains("30 days"),
                "{}",
                database_error.message()
            );
        }
        other => panic!("expected a database refusal, got {other:?}"),
    }
    let rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM horsies_key_reservations WHERE idempotency_key_digest = $1",
    )
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("count reservations");
    assert_eq!(rows, 0);
}

/// `TestTerminalizationAndReuse.test_terminalize_verifies_ownership`: only the
/// owner terminalizes the reservation, and only once.
#[tokio::test]
#[serial]
async fn only_the_owner_terminalizes_a_reservation_once() {
    let pool = shared_pool().await;
    let key = fresh_key();
    let owner = Uuid::new_v4();
    let mut transaction = pool.begin().await.expect("begin");
    assert_eq!(
        claim(&mut transaction, &key, &digest("command"), owner).await,
        ReservationClaim::Applied { task_id: owner }
    );
    assert!(
        !terminalize_key_reservation(&mut transaction, &key, Uuid::new_v4())
            .await
            .expect("terminalize as another task")
    );
    assert!(terminalize_key_reservation(&mut transaction, &key, owner)
        .await
        .expect("terminalize as the owner"));
    assert!(!terminalize_key_reservation(&mut transaction, &key, owner)
        .await
        .expect("terminalize again"));
    transaction.rollback().await.expect("rollback");
}

/// `TestClaimReplayConflict.test_different_fingerprint_is_a_typed_conflict`:
/// a claim of a reserved key with another fingerprint is a typed conflict
/// that names the task that holds the key and the fingerprint version it
/// observed.
#[tokio::test]
#[serial]
async fn a_claim_with_another_fingerprint_is_a_typed_conflict() {
    let pool = shared_pool().await;
    let key = fresh_key();
    let original = Uuid::new_v4();
    let mut transaction = pool.begin().await.expect("begin");
    assert_eq!(
        claim(&mut transaction, &key, &digest("command"), original).await,
        ReservationClaim::Applied { task_id: original }
    );

    let outcome = claim(
        &mut transaction,
        &key,
        &digest("another command"),
        Uuid::new_v4(),
    )
    .await;

    assert_eq!(
        outcome,
        ReservationClaim::Conflict {
            task_id: original,
            observed_fingerprint_version: 1,
        }
    );
    transaction.rollback().await.expect("rollback");
}

/// `TestTerminalizationAndReuse.test_unexpired_terminal_reservation_still_replays`.
#[tokio::test]
#[serial]
async fn an_unexpired_terminal_reservation_still_replays() {
    let pool = shared_pool().await;
    let key = fresh_key();
    let original = Uuid::new_v4();
    let mut transaction = pool.begin().await.expect("begin");
    let _ = claim(&mut transaction, &key, &digest("command"), original).await;
    assert!(
        terminalize_key_reservation(&mut transaction, &key, original)
            .await
            .expect("terminalize")
    );
    assert_eq!(
        claim(&mut transaction, &key, &digest("command"), Uuid::new_v4()).await,
        ReservationClaim::Replay { task_id: original }
    );
    transaction.rollback().await.expect("rollback");
}

/// `TestTerminalizationAndReuse.test_expired_terminal_reservation_is_reused`:
/// after the window passed, the key applies to a new task, even with another
/// command.
#[tokio::test]
#[serial]
async fn an_expired_terminal_reservation_is_reused() {
    let pool = shared_pool().await;
    let key = fresh_key();
    let original = Uuid::new_v4();
    let replacement = Uuid::new_v4();
    let mut transaction = pool.begin().await.expect("begin");
    let _ = claim(&mut transaction, &key, &digest("command"), original).await;
    terminalize_two_hours_ago(&mut transaction, &key, original).await;
    assert_eq!(
        claim(
            &mut transaction,
            &key,
            &digest("different-command"),
            replacement
        )
        .await,
        ReservationClaim::Applied {
            task_id: replacement
        }
    );
    transaction.rollback().await.expect("rollback");
}

/// `TestCleanup.test_cleanup_deletes_only_expired_terminal_rows`: cleanup
/// removes an expired terminal reservation and never a live one.
#[tokio::test]
#[serial]
async fn cleanup_deletes_only_expired_terminal_rows_and_never_live_ones() {
    let pool = shared_pool().await;
    let expired_key = fresh_key();
    let live_key = fresh_key();
    let expired = Uuid::new_v4();
    let live = Uuid::new_v4();
    let mut transaction = pool.begin().await.expect("begin");
    // Expired rows that other tests left in the shared database go first.
    while cleanup_expired_reservations(&mut transaction, 1_000)
        .await
        .expect("clear earlier expired rows")
        > 0
    {}
    let _ = claim(&mut transaction, &expired_key, &digest("command"), expired).await;
    terminalize_two_hours_ago(&mut transaction, &expired_key, expired).await;
    let _ = claim(&mut transaction, &live_key, &digest("command"), live).await;

    assert_eq!(
        cleanup_expired_reservations(&mut transaction, 10)
            .await
            .expect("first cleanup"),
        1
    );
    assert_eq!(
        cleanup_expired_reservations(&mut transaction, 10)
            .await
            .expect("second cleanup"),
        0
    );
    let remaining: Vec<(Vec<u8>, String)> = sqlx::query_as(
        "SELECT idempotency_key_digest, disposition FROM horsies_key_reservations
         WHERE idempotency_key_digest = ANY($1)",
    )
    .bind(vec![expired_key.clone(), live_key.clone()])
    .fetch_all(&mut *transaction)
    .await
    .expect("read reservations");
    assert_eq!(remaining, vec![(live_key, "LIVE".to_owned())]);
    transaction.rollback().await.expect("rollback");
}

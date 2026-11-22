//! `horsies_key_reservation_claim`
//! (`core/baseline/0003_interim_functions.sql`) under
//! concurrent claims of one key.
//!
//! Each case has a database of its own that holds the baseline migration.
//!
//! - The two-session tests order the sessions by lock waits: a holder session
//!   keeps a transaction open, the caller's claim waits on it (read from
//!   `pg_stat_activity`), and the holder then commits or rolls back.
//! - The send tests run [`CONCURRENT_SENDS`] calls of `horsies_enqueue_task`
//!   under one key. A table lock holds all of them before the key claim and
//!   releases them together.

use std::time::Duration;

use futures::future::join_all;
use sha2::{Digest, Sha256};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::ToSql;
use tokio_postgres::Row;
use uuid::Uuid;

use super::harness::{describe, BaselineDatabase, Session};

/// Sends of one key in the send tests, as in the identity prototype tests.
const CONCURRENT_SENDS: usize = 64;

/// `c_claim_rounds` of the claim: read, delete and insert rounds before it
/// raises 40001.
const CLAIM_ROUNDS: u32 = 3;

/// Bound of every statement, so a lock wait that never ends fails the test.
const STATEMENT_TIMEOUT_SQL: &str = "SET statement_timeout = '10s'";

/// Time between two reads of the waiting sessions' state.
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Reads before a test gives up on sessions that never wait (5 s).
const WAIT_POLL_LIMIT: u32 = 250;

/// Advisory lock that the pause trigger ([`PAUSE_TRIGGER_SQL`]) of the round
/// test waits on.
const PAUSE_LOCK_KEY: i64 = 937_001;

const CLAIM_SQL: &str = "SELECT outcome, task_id FROM horsies_key_reservation_claim(
    $1::bytea, 1::smallint, interval '1 hour', 1::smallint, $2::bytea, $3::uuid)";

/// Terminal two hours ago with a one-hour window: the reservation is expired.
const TERMINALIZE_IN_THE_PAST_SQL: &str =
    "SELECT horsies_key_reservation_terminalize($1::bytea, $2::uuid, now() - interval '2 hours')";

const CLEANUP_SQL: &str = "SELECT horsies_key_reservation_cleanup(10)";

const RESERVATIONS_SQL: &str = "SELECT task_id, disposition FROM horsies_key_reservations
    WHERE idempotency_key_digest = $1::bytea";

const LOCK_RESERVATION_SQL: &str =
    "SELECT 1 FROM horsies_key_reservations WHERE idempotency_key_digest = $1::bytea FOR UPDATE";

const WAIT_STATE_SQL: &str =
    "SELECT wait_event_type, wait_event FROM pg_stat_activity WHERE pid = $1";

const WAITING_COUNT_SQL: &str = "SELECT count(*) FROM pg_stat_activity
    WHERE pid = ANY($1::int4[]) AND wait_event_type = 'Lock' AND wait_event = 'relation'";

/// A BEFORE INSERT trigger fires before the conflict check of INSERT ... ON
/// CONFLICT. In a session that sets `horsies_test.pause_lock_key` it waits on
/// that advisory lock before every insert of a reservation, so the test can
/// commit rows between the claim's read and its insert.
const PAUSE_TRIGGER_SQL: &str = "
CREATE FUNCTION key_claim_test_pause() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    v_lock_key text := nullif(current_setting('horsies_test.pause_lock_key', TRUE), '');
BEGIN
    IF v_lock_key IS NOT NULL THEN
        PERFORM pg_advisory_lock(v_lock_key::bigint);
        PERFORM pg_advisory_unlock(v_lock_key::bigint);
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER key_claim_test_pause BEFORE INSERT ON horsies_key_reservations
    FOR EACH ROW EXECUTE FUNCTION key_claim_test_pause();
";

const SEND_SQL: &str = r#"
    SELECT outcome, task_id FROM horsies_enqueue_task(
        $1::uuid, 'key.claim', 'q_key_claim', 100, '{"order_id":"order-1"}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL,
        now(), 'NOW', NULL, NULL, 1::smallint, $2::bytea, 'FIRST_RUN', NULL, NULL,
        'KEYED', $3::bytea, 1::smallint, interval '1 hour', 'FIRST', 'NO_CARRIER', NULL, NULL)
"#;

/// The key of every claim and send.
fn key() -> Vec<u8> {
    Sha256::digest(b"key-claim-test").to_vec()
}

/// The command fingerprint of the first command.
fn command_a() -> Vec<u8> {
    Sha256::digest(b"command-a").to_vec()
}

/// The command fingerprint of the second command.
fn command_b() -> Vec<u8> {
    Sha256::digest(b"command-b").to_vec()
}

/// `count` autocommit sessions whose statements end after the bound of
/// [`STATEMENT_TIMEOUT_SQL`].
async fn connect_many(database: &BaselineDatabase, count: usize) -> Vec<Session> {
    let mut sessions = Vec::with_capacity(count);
    for _ in 0..count {
        let session = database.session().await;
        session
            .run_script("STATEMENT_TIMEOUT_SQL", STATEMENT_TIMEOUT_SQL)
            .await;
        sessions.push(session);
    }
    sessions
}

/// Three sessions: holder, caller and observer.
async fn three_sessions(database: &BaselineDatabase) -> (Session, Session, Session) {
    let mut sessions = connect_many(database, 3).await.into_iter();
    match (sessions.next(), sessions.next(), sessions.next()) {
        (Some(holder), Some(caller), Some(observer)) => (holder, caller, observer),
        _ => unreachable!("connect_many returns the count it was asked for"),
    }
}

async fn run(session: &Session, statement: &str) {
    session.run_script(statement, statement).await;
}

async fn row(session: &Session, statement: &str, parameters: &[&(dyn ToSql + Sync)]) -> Row {
    session
        .client()
        .query(statement, parameters)
        .await
        .unwrap_or_else(|error| panic!("{statement}: {}", describe(&error)))
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{statement}: no row"))
}

/// The claim of [`key`] for `command` and `task_id`: the outcome and the
/// task id of the reservation, or the server error.
async fn try_claim(
    session: &Session,
    command: Vec<u8>,
    task_id: Uuid,
) -> Result<(String, Uuid), tokio_postgres::Error> {
    let rows = session
        .client()
        .query(CLAIM_SQL, &[&key(), &command, &task_id])
        .await?;
    let row = rows.first().expect("the claim returns one row");
    Ok((row.get(0), row.get(1)))
}

async fn claim(session: &Session, command: Vec<u8>, task_id: Uuid) -> (String, Uuid) {
    try_claim(session, command, task_id)
        .await
        .unwrap_or_else(|error| panic!("claim: {}", describe(&error)))
}

fn applied(task_id: Uuid) -> (String, Uuid) {
    ("APPLIED".to_owned(), task_id)
}

async fn terminalize_in_the_past(session: &Session, owner: Uuid) -> bool {
    row(session, TERMINALIZE_IN_THE_PAST_SQL, &[&key(), &owner])
        .await
        .get(0)
}

async fn cleanup(session: &Session) -> i32 {
    row(session, CLEANUP_SQL, &[]).await.get(0)
}

async fn reservations(session: &Session) -> Vec<(Uuid, String)> {
    session
        .client()
        .query(RESERVATIONS_SQL, &[&key()])
        .await
        .unwrap_or_else(|error| panic!("read the reservations: {}", describe(&error)))
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

fn live(task_id: Uuid) -> Vec<(Uuid, String)> {
    vec![(task_id, "LIVE".to_owned())]
}

/// Returns once session `pid` waits on a lock of kind `wait_event`.
///
/// # Panics
///
/// When the session does not wait within [`WAIT_POLL_LIMIT`] reads.
async fn wait_for_lock(observer: &Session, pid: i32, wait_event: &str) {
    for _ in 0..WAIT_POLL_LIMIT {
        let state = observer
            .client()
            .query_opt(WAIT_STATE_SQL, &[&pid])
            .await
            .unwrap_or_else(|error| panic!("read the wait state: {}", describe(&error)));
        let waiting = state.is_some_and(|row| {
            row.get::<_, Option<&str>>(0) == Some("Lock")
                && row.get::<_, Option<&str>>(1) == Some(wait_event)
        });
        if waiting {
            return;
        }
        tokio::time::sleep(WAIT_POLL_INTERVAL).await;
    }
    panic!("session {pid} never waited on a lock of kind {wait_event}");
}

/// Commits an expired TERMINAL reservation of [`key`] and returns its task
/// id.
async fn expired_reservation(session: &Session) -> Uuid {
    let owner = Uuid::new_v4();
    assert_eq!(claim(session, command_a(), owner).await, applied(owner));
    assert!(terminalize_in_the_past(session, owner).await);
    owner
}

/// Asserts that `outcome` is the server error serialization_failure.
fn assert_serialization_failure(outcome: Result<(String, Uuid), tokio_postgres::Error>) {
    let error = outcome.expect_err("the claim raises serialization_failure");
    assert_eq!(
        error.code(),
        Some(&SqlState::T_R_SERIALIZATION_FAILURE),
        "{}",
        describe(&error)
    );
}

// ---------------------------------------------------------------------------
// Two sessions
// ---------------------------------------------------------------------------

/// The caller's insert waits on the holder's uncommitted reservation; after
/// the commit the caller reads it. One case per command: the same command
/// (REPLAY) and another command (CONFLICT).
#[tokio::test]
async fn a_claim_that_waits_on_a_committed_first_claim_is_classified_against_it() {
    for (command, expected) in [(command_a(), "REPLAY"), (command_b(), "CONFLICT")] {
        let database = BaselineDatabase::create(&[]).await;
        let (holder, caller, observer) = three_sessions(&database).await;
        let (first, second) = (Uuid::new_v4(), Uuid::new_v4());
        run(&holder, "BEGIN").await;
        assert_eq!(claim(&holder, command_a(), first).await, applied(first));
        let (outcome, ()) = tokio::join!(claim(&caller, command.clone(), second), async {
            wait_for_lock(&observer, caller.backend_pid(), "transactionid").await;
            run(&holder, "COMMIT").await;
        });
        assert_eq!(outcome, (expected.to_owned(), first), "{expected}");
        assert_eq!(reservations(&observer).await, live(first), "{expected}");
    }
}

#[tokio::test]
async fn a_claim_that_waits_on_a_rolled_back_first_claim_applies() {
    let database = BaselineDatabase::create(&[]).await;
    let (holder, caller, observer) = three_sessions(&database).await;
    let (first, second) = (Uuid::new_v4(), Uuid::new_v4());
    run(&holder, "BEGIN").await;
    assert_eq!(claim(&holder, command_a(), first).await, applied(first));
    let (outcome, ()) = tokio::join!(claim(&caller, command_a(), second), async {
        wait_for_lock(&observer, caller.backend_pid(), "transactionid").await;
        run(&holder, "ROLLBACK").await;
    });
    assert_eq!(outcome, applied(second));
    assert_eq!(reservations(&observer).await, live(second));
}

/// The cleanup holds the expired reservation; the caller's delete of it
/// waits. Whether the cleanup's delete commits or rolls back, the caller
/// takes the key.
#[tokio::test]
async fn a_claim_that_waits_on_the_cleanup_of_an_expired_reservation_applies() {
    for holder_end in ["COMMIT", "ROLLBACK"] {
        let database = BaselineDatabase::create(&[]).await;
        let (holder, caller, observer) = three_sessions(&database).await;
        let _expired = expired_reservation(&observer).await;
        let second = Uuid::new_v4();
        run(&holder, "BEGIN").await;
        assert_eq!(cleanup(&holder).await, 1, "{holder_end}");
        let (outcome, ()) = tokio::join!(claim(&caller, command_a(), second), async {
            wait_for_lock(&observer, caller.backend_pid(), "transactionid").await;
            run(&holder, holder_end).await;
        });
        assert_eq!(outcome, applied(second), "{holder_end}");
        assert_eq!(reservations(&observer).await, live(second), "{holder_end}");
    }
}

/// The holder's claim deletes the expired reservation and inserts its own;
/// the caller's delete waits on it. After the commit the caller's insert
/// does nothing and the caller replays the holder's reservation.
#[tokio::test]
async fn two_claims_of_a_key_with_an_expired_reservation_apply_once() {
    let database = BaselineDatabase::create(&[]).await;
    let (holder, caller, observer) = three_sessions(&database).await;
    let _expired = expired_reservation(&observer).await;
    let (first, second) = (Uuid::new_v4(), Uuid::new_v4());
    run(&holder, "BEGIN").await;
    assert_eq!(claim(&holder, command_a(), first).await, applied(first));
    let (outcome, ()) = tokio::join!(claim(&caller, command_a(), second), async {
        wait_for_lock(&observer, caller.backend_pid(), "transactionid").await;
        run(&holder, "COMMIT").await;
    });
    assert_eq!(outcome, ("REPLAY".to_owned(), first));
    assert_eq!(reservations(&observer).await, live(first));
}

/// The reservation that stopped the caller's insert is expired when the
/// caller reads again: the next round deletes it and inserts the caller's
/// reservation.
#[tokio::test]
async fn a_reservation_that_is_expired_at_the_second_read_is_reused() {
    let database = BaselineDatabase::create(&[]).await;
    let (holder, caller, observer) = three_sessions(&database).await;
    let (first, second) = (Uuid::new_v4(), Uuid::new_v4());
    run(&holder, "BEGIN").await;
    assert_eq!(claim(&holder, command_a(), first).await, applied(first));
    assert!(terminalize_in_the_past(&holder, first).await);
    let (outcome, ()) = tokio::join!(claim(&caller, command_b(), second), async {
        wait_for_lock(&observer, caller.backend_pid(), "transactionid").await;
        run(&holder, "COMMIT").await;
    });
    assert_eq!(outcome, applied(second));
    assert_eq!(reservations(&observer).await, live(second));
}

/// In each round a reservation commits between the caller's read and its
/// insert, and is gone before the next read. After [`CLAIM_ROUNDS`] rounds
/// the claim raises serialization_failure and writes nothing.
#[tokio::test]
async fn a_claim_whose_reservation_changes_in_every_round_raises_40001() {
    let database = BaselineDatabase::create(&[]).await;
    let mut sessions = connect_many(&database, 5).await.into_iter();
    let (Some(setup), Some(gate), Some(locker), Some(caller), Some(observer)) = (
        sessions.next(),
        sessions.next(),
        sessions.next(),
        sessions.next(),
        sessions.next(),
    ) else {
        unreachable!("connect_many returns the count it was asked for")
    };
    setup
        .run_script("PAUSE_TRIGGER_SQL", PAUSE_TRIGGER_SQL)
        .await;
    let _locked = row(
        &gate,
        "SELECT pg_advisory_lock($1::bigint)",
        &[&PAUSE_LOCK_KEY],
    )
    .await;
    let _set = row(
        &caller,
        "SELECT set_config('horsies_test.pause_lock_key', $1::text, FALSE)",
        &[&PAUSE_LOCK_KEY.to_string()],
    )
    .await;
    let (outcome, owners) = tokio::join!(try_claim(&caller, command_a(), Uuid::new_v4()), async {
        let mut owners = Vec::new();
        for round_number in 1..=CLAIM_ROUNDS {
            wait_for_lock(&observer, caller.backend_pid(), "advisory").await;
            let owner = Uuid::new_v4();
            owners.push(owner);
            assert_eq!(claim(&setup, command_a(), owner).await, applied(owner));
            run(&locker, "BEGIN").await;
            let _locked = row(&locker, LOCK_RESERVATION_SQL, &[&key()]).await;
            let _unlocked = row(
                &gate,
                "SELECT pg_advisory_unlock($1::bigint)",
                &[&PAUSE_LOCK_KEY],
            )
            .await;
            if round_number == CLAIM_ROUNDS {
                break;
            }
            // The insert did nothing; the next round's read waits on the
            // locker's row lock.
            wait_for_lock(&observer, caller.backend_pid(), "transactionid").await;
            let _locked = row(
                &gate,
                "SELECT pg_advisory_lock($1::bigint)",
                &[&PAUSE_LOCK_KEY],
            )
            .await;
            assert!(terminalize_in_the_past(&locker, owner).await);
            assert_eq!(cleanup(&locker).await, 1);
            run(&locker, "COMMIT").await;
        }
        owners
    });
    assert_serialization_failure(outcome);
    run(&locker, "ROLLBACK").await;
    let last = *owners.last().expect("every round records its owner");
    assert_eq!(reservations(&observer).await, live(last));
}

/// The reservation committed after the caller's snapshot: PostgreSQL's ON
/// CONFLICT rule raises serialization_failure under REPEATABLE READ, not
/// unique_violation.
#[tokio::test]
async fn under_repeatable_read_a_claim_that_waits_on_a_first_claim_raises_40001() {
    let database = BaselineDatabase::create(&[]).await;
    let (holder, caller, observer) = three_sessions(&database).await;
    let first = Uuid::new_v4();
    run(&caller, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
    run(&holder, "BEGIN").await;
    assert_eq!(claim(&holder, command_a(), first).await, applied(first));
    let (outcome, ()) = tokio::join!(try_claim(&caller, command_a(), Uuid::new_v4()), async {
        wait_for_lock(&observer, caller.backend_pid(), "transactionid").await;
        run(&holder, "COMMIT").await;
    });
    assert_serialization_failure(outcome);
    run(&caller, "ROLLBACK").await;
    assert_eq!(reservations(&observer).await, live(first));
}

// ---------------------------------------------------------------------------
// Concurrent sends (identity prototype tests: one applied, the rest
// classified against it)
// ---------------------------------------------------------------------------

/// One send of the send tests: its task id, its outcome, and the task id it
/// reports.
type SendOutcome = (Uuid, String, Uuid);

async fn send(sender: &Session, task_id: Uuid, command: Vec<u8>) -> SendOutcome {
    let sent = row(sender, SEND_SQL, &[&task_id, &command, &key()]).await;
    (task_id, sent.get(0), sent.get(1))
}

/// Sends `commands[i]` under [`key`] with a new task id each, all at once.
async fn concurrent_sends(database: &BaselineDatabase, commands: &[Vec<u8>]) -> Vec<SendOutcome> {
    let mut sessions = connect_many(database, 2 + commands.len()).await.into_iter();
    let (Some(gate), Some(observer)) = (sessions.next(), sessions.next()) else {
        unreachable!("connect_many returns the count it was asked for")
    };
    let senders: Vec<Session> = sessions.collect();
    let pids: Vec<i32> = senders.iter().map(Session::backend_pid).collect();
    let expected_waiting = i64::try_from(senders.len()).expect("a small count");
    run(&gate, "BEGIN").await;
    run(
        &gate,
        "LOCK TABLE horsies_key_reservations IN EXCLUSIVE MODE",
    )
    .await;
    let sends = senders
        .iter()
        .zip(commands)
        .map(|(sender, command)| send(sender, Uuid::new_v4(), command.clone()));
    let (outcomes, ()) = tokio::join!(join_all(sends), async {
        let mut all_waiting = false;
        for _ in 0..WAIT_POLL_LIMIT {
            let waiting: i64 = row(&observer, WAITING_COUNT_SQL, &[&pids]).await.get(0);
            all_waiting = waiting == expected_waiting;
            if all_waiting {
                break;
            }
            tokio::time::sleep(WAIT_POLL_INTERVAL).await;
        }
        assert!(
            all_waiting,
            "the sends never all waited on the reservation table lock"
        );
        run(&gate, "COMMIT").await;
    });
    outcomes
}

/// The index of the one send that inserted.
fn the_applied_send(outcomes: &[SendOutcome]) -> usize {
    let applied: Vec<usize> = outcomes
        .iter()
        .enumerate()
        .filter(|(_, (_, outcome, _))| outcome == "INSERTED")
        .map(|(index, _)| index)
        .collect();
    assert_eq!(applied.len(), 1, "{outcomes:?}");
    applied[0]
}

/// The live task count and the reservations of [`key`], on a new session.
async fn live_rows(database: &BaselineDatabase) -> (i64, Vec<(Uuid, String)>) {
    let session = connect_many(database, 1).await.pop().expect("one session");
    let tasks: i64 = row(&session, "SELECT count(*) FROM horsies_tasks", &[])
        .await
        .get(0);
    (tasks, reservations(&session).await)
}

#[tokio::test]
async fn concurrent_sends_of_one_command_under_one_key_apply_once() {
    let database = BaselineDatabase::create(&[]).await;
    let commands = vec![command_a(); CONCURRENT_SENDS];
    let outcomes = concurrent_sends(&database, &commands).await;

    let winner = outcomes[the_applied_send(&outcomes)].0;
    let replays: Vec<(&str, Uuid)> = outcomes
        .iter()
        .filter(|(task_id, _, _)| *task_id != winner)
        .map(|(_, outcome, owner)| (outcome.as_str(), *owner))
        .collect();
    assert_eq!(replays, vec![("KEY_REPLAY", winner); CONCURRENT_SENDS - 1]);
    assert_eq!(live_rows(&database).await, (1, live(winner)));
}

#[tokio::test]
async fn concurrent_sends_of_two_commands_are_classified_against_the_applied_one() {
    let database = BaselineDatabase::create(&[]).await;
    let commands: Vec<Vec<u8>> = (0..CONCURRENT_SENDS)
        .map(|index| match index % 2 {
            0 => command_a(),
            _ => command_b(),
        })
        .collect();
    let outcomes = concurrent_sends(&database, &commands).await;

    let applied = the_applied_send(&outcomes);
    let winner = outcomes[applied].0;
    for (index, (task_id, outcome, owner)) in outcomes.iter().enumerate() {
        let observed = (outcome.as_str(), *owner);
        match (index == applied, commands[index] == commands[applied]) {
            (true, _) => assert_eq!(observed, ("INSERTED", *task_id)),
            (false, true) => assert_eq!(observed, ("KEY_REPLAY", winner), "{index}"),
            (false, false) => assert_eq!(observed, ("KEY_CONFLICT", winner), "{index}"),
        }
    }
    assert_eq!(live_rows(&database).await, (1, live(winner)));
}

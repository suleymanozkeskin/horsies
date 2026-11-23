//! The lifecycle functions of the baseline (`0004_lifecycle_functions.sql`)
//! on a fresh database.
//!
//! The single-session cases are the case file
//! `cases/baseline_lifecycle_functions.sql`, run in one transaction that is
//! rolled back. The two-session cases are below. Each runs on a database of
//! its own with the run-time fixtures of [`PREPARE_SQL`], and every session
//! of a case has a statement timeout, so a lock wait that never ends fails
//! the case.
//!
//! The lock cases check the lock order directly: a holder session takes the
//! lock that a function must take first, the function's session waits for
//! it, and the holder then takes the second lock with NOWAIT. The second
//! lock is free only when the waiting function took nothing else before its
//! first lock.

use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio_postgres::types::ToSql;
use tokio_postgres::Row;
use uuid::Uuid;

use super::harness::{describe, BaselineDatabase, Session};

pub(super) const CASES_SQL: &str = include_str!("cases/baseline_lifecycle_functions.sql");

/// Time between two reads of the waiting session's state.
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Reads before a case gives up on a session that never waits (5 s).
const WAIT_POLL_LIMIT: u32 = 250;

/// Time the listener waits for the notifications of one committed write.
const NOTIFY_SETTLE: Duration = Duration::from_millis(300);

/// Bound of every statement of a case, so a lock wait that never ends fails
/// the case.
const STATEMENT_TIMEOUT_SQL: &str = "SET statement_timeout = '10s'";

/// The channels that the wake case listens on: the queue of its tasks and the
/// three event channels.
const LISTEN_SQL: [&str; 4] = [
    "LISTEN task_queue_q_wake",
    "LISTEN task_new",
    "LISTEN task_done",
    "LISTEN horsies_task_status",
];

/// Fixtures that the published history readers and the partition manager
/// provide at run time: a provenance stand-in that reads the history parent,
/// a duplicate guard stand-in on top of it, and one heartbeat leaf around
/// now.
const PREPARE_SQL: &str = r#"
CREATE FUNCTION horsies_task_provenance_staged(p_task_id uuid)
RETURNS TABLE (
    task_id uuid, status text, terminal_at timestamptz, terminalization_kind text, is_workflow_task boolean,
    command_fingerprint_version smallint, command_fingerprint bytea, sent_at timestamptz
)
LANGUAGE sql STABLE AS $$
    SELECT h.task_id, h.status::text, h.terminal_at, h.terminalization_kind::text, h.is_workflow_task,
        h.command_fingerprint_version, h.command_fingerprint, h.sent_at
    FROM horsies_task_history h
    WHERE h.task_id = p_task_id
    LIMIT 1
$$;
CREATE FUNCTION horsies_task_history_duplicate_staged(p_task_id uuid)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS (SELECT 1 FROM horsies_task_provenance_staged(p_task_id))
        OR EXISTS (SELECT 1 FROM horsies_task_history_clock_exceptions AS e WHERE e.task_id = p_task_id)
$$;
DO $$
BEGIN
    EXECUTE format(
        'CREATE TABLE horsies_heartbeats_lc_sessions PARTITION OF horsies_heartbeats FOR VALUES FROM (%L) TO (%L)',
        now() - interval '2 days', now() + interval '2 days'
    );
END
$$;
CREATE INDEX horsies_heartbeats_lc_sessions_probe ON horsies_heartbeats_lc_sessions (task_id, role, sent_at DESC);
"#;

/// The queue of the tasks of most cases.
const SESSION_QUEUE: &str = "q_sessions";

/// One committed plain task of worker w1. Parameters: `$1` id, `$2` queue,
/// `$3` status.
pub(super) const INSERT_TASK_SQL: &str = r#"
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
    created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at,
    claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id,
    worker_pid, worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
    is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
) VALUES (
    $1::uuid, 'lc.task', $2, 100, '{"x":1}', NULL, 'ISOLATED', 'forever', now(),
    now(), 1, sha256('lc'::bytea), NULL, $3::text, now() - interval '1 minute',
    CASE WHEN $3::text = 'PENDING' THEN NULL ELSE now() - interval '1 second' END,
    CASE WHEN $3::text = 'PENDING' THEN NULL ELSE 'w1' END,
    CASE WHEN $3::text = 'CLAIMED' THEN now() + interval '1 minute' END,
    CASE WHEN $3::text = 'RUNNING' THEN now() - interval '10 minutes' END,
    NULL, NULL,
    CASE WHEN $3::text = 'RUNNING' THEN 7 END,
    CASE WHEN $3::text = 'RUNNING' THEN 'host-a' END,
    CASE WHEN $3::text = 'RUNNING' THEN 'worker-7' END,
    0, 3, NULL, NULL, FALSE, now(), TRUE, NULL, NULL
)
"#;

const CLAIM_TIME_SQL: &str = "SELECT claimed_at FROM horsies_tasks WHERE id = $1::uuid";
const SENT_AT_SQL: &str = "SELECT sent_at FROM horsies_tasks WHERE id = $1::uuid";
const ADVISORY_LOCK_SQL: &str =
    "SELECT pg_advisory_xact_lock(hashtextextended($1::uuid::text, 731))";
const ROW_LOCK_SQL: &str = "SELECT 1 FROM horsies_tasks WHERE id = $1::uuid FOR UPDATE";
const ROW_LOCK_NOWAIT_SQL: &str =
    "SELECT 1 FROM horsies_tasks WHERE id = $1::uuid FOR UPDATE NOWAIT";
const WAIT_STATE_SQL: &str =
    "SELECT wait_event_type, wait_event FROM pg_stat_activity WHERE pid = $1";
const LIVE_COUNT_SQL: &str = "SELECT count(*) FROM horsies_tasks WHERE id = $1::uuid";
const CANCEL_ROWS_SQL: &str = "SELECT * FROM horsies_cancel_task($1::uuid, TRUE)";

const CANCEL_SQL: &str = "SELECT outcome FROM horsies_cancel_task($1::uuid, TRUE)";
const COMPLETE_SQL: &str =
    "SELECT outcome FROM horsies_complete_task_fused($1::uuid, 'w1', $2::timestamptz, '{}', TRUE)";
const FAIL_SQL: &str = "SELECT outcome FROM horsies_finish_failed_attempt(
    $1::uuid, 'w1', $2::timestamptz, 'NONE', NULL, NULL, NULL, '{}', 'E', 'r', 'FAILED', 'm', 'r', 'REFUSE', TRUE)";
const FAIL_STALE_SQL: &str = "SELECT outcome FROM horsies_finish_stale_attempt(
    $1::uuid, 'BEAT_AGE', 60000, 60000, NULL, NULL, 'NONE', NULL, NULL, '{}', 'WORKER_CRASHED', 'r', 'm', 'r')";

const BATCH_SUCCESS_SQL: &str = "SELECT string_agg(outcome, ',' ORDER BY ordinality)
    FROM horsies_complete_tasks_fused(
        ARRAY[$1::uuid, $2::uuid]::uuid[], ARRAY[$3::timestamptz, $4::timestamptz]::timestamptz[],
        ARRAY['{}', '{}'], 'w1', TRUE)";

const START_SQL: &str = "SELECT outcome, miss_reason
    FROM horsies_start_attempt($1::uuid, 'w1', $2::timestamptz, 7, 'h', 'p')";

const KEYED_SEND_SQL: &str = r#"SELECT outcome FROM horsies_enqueue_task(
    $1::uuid, 'lc.task', 'q_sessions', 100, '{"x":1}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL,
    $2::timestamptz, 'NOW', NULL, NULL, 1::smallint, sha256('lc'::bytea), 'FIRST_RUN', NULL, NULL,
    'KEYED', sha256('session-key'::bytea), 1::smallint, interval '1 hour', 'FIRST', 'NO_CARRIER', NULL, NULL)"#;

const REPEAT_SEND_SQL: &str = r#"SELECT outcome FROM horsies_enqueue_task(
    $1::uuid, 'lc.task', 'q_sessions', 100, '{"x":1}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL,
    $2::timestamptz, 'NOW', NULL, NULL, 1::smallint, sha256('lc'::bytea), 'FIRST_RUN', NULL, NULL,
    'UNKEYED', NULL, NULL, NULL, 'REPEAT', 'NO_CARRIER', NULL, NULL)"#;

const SESSION_KEY_RESERVATIONS_SQL: &str = "SELECT count(*) FROM horsies_key_reservations
    WHERE idempotency_key_digest = sha256('session-key'::bytea)";

const CLAIM_SQL: &str = r#"SELECT row_kind, short_by FROM horsies_claim(
    'w1', '["q_skip"]'::jsonb, '{}'::jsonb, '{}'::jsonb, FALSE, '{"ISOLATED": 1}'::jsonb,
    '{"ISOLATED": "ISOLATED"}'::jsonb, 0, 1, 1, NULL, 60000, '[]'::jsonb)"#;

const WAKE_SEND_SQL: &str = "SELECT * FROM horsies_enqueue_task(
    $1::uuid, 'lc.task', 'q_wake', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(), 'NOW', NULL,
    NULL, 1::smallint, sha256('wake'::bytea), 'FIRST_RUN', NULL, NULL, 'UNKEYED', NULL, NULL, NULL,
    'FIRST', 'NO_CARRIER', NULL, NULL)";
const WAKE_BATCH_SUCCESS_SQL: &str = "SELECT * FROM horsies_complete_tasks_fused(
    ARRAY[$1::uuid, $2::uuid]::uuid[], ARRAY[$3::timestamptz, $4::timestamptz]::timestamptz[],
    ARRAY['{}', '{}'], 'w1', TRUE)";
const SILENT_SUCCESS_SQL: &str =
    "SELECT * FROM horsies_complete_task_fused($1::uuid, 'w1', $2::timestamptz, '{}', FALSE)";
const TERMINAL_FAILURE_SQL: &str = "SELECT * FROM horsies_finish_failed_attempt(
    $1::uuid, 'w1', $2::timestamptz, 'NONE', NULL, NULL, NULL, '{}', 'E', 'r', 'FAILED', 'm', 'r', 'REFUSE', TRUE)";
const RETRY_FAILURE_SQL: &str = "SELECT * FROM horsies_finish_failed_attempt(
    $1::uuid, 'w1', $2::timestamptz, 'RETRY', 0, 30.0, 'SAME_CLASS', '{}', 'E', 'r', 'FAILED', 'm', 'r', 'REFUSE',
    TRUE)";
const RELEASE_ISOLATED_SQL: &str = "SELECT * FROM horsies_release_claims(
    ARRAY[$1::uuid, $2::uuid]::uuid[], ARRAY[$3::timestamptz, $4::timestamptz]::timestamptz[], 'w1', 'ISOLATED')";
const RELEASE_SAME_CLASS_SQL: &str = "SELECT * FROM horsies_release_claims(
    ARRAY[$1::uuid]::uuid[], ARRAY[$2::timestamptz]::timestamptz[], 'w1', 'SAME_CLASS')";
const START_ROWS_SQL: &str =
    "SELECT * FROM horsies_start_attempt($1::uuid, 'w1', $2::timestamptz, 7, 'h', 'p')";

const TAKE_LIVENESS_SQL: &str = "SELECT horsies_take_worker_liveness($1::text)";
const LIVENESS_GENERATION_SQL: &str =
    "SELECT generation FROM horsies_worker_liveness WHERE worker_id = $1::text";

/// A failed case raises with its label, which fails this test with the case
/// in the error.
#[tokio::test]
async fn single_session_cases() {
    let database = BaselineDatabase::create(&[]).await;
    let session = database.session().await;
    session
        .run_script_rolled_back("baseline_lifecycle_functions.sql", CASES_SQL)
        .await;
}

/// Every two-session case, each on a database of its own.
#[tokio::test]
async fn two_session_case() {
    advisory_lock_comes_first(
        &prepared().await,
        "cancel lock order",
        LockedCall::Cancel,
        "PENDING",
    )
    .await;
    advisory_lock_comes_first(
        &prepared().await,
        "success lock order",
        LockedCall::Complete,
        "RUNNING",
    )
    .await;
    advisory_lock_comes_first(
        &prepared().await,
        "failed attempt lock order",
        LockedCall::Fail,
        "RUNNING",
    )
    .await;
    advisory_lock_comes_first(
        &prepared().await,
        "stale attempt lock order",
        LockedCall::FailStale,
        "RUNNING",
    )
    .await;
    batch_success_locks_in_task_id_order(&prepared().await).await;
    start_of_a_row_that_moves_is_not_held(&prepared().await).await;
    keyed_send_on_a_row_that_moves_is_unverifiable(&prepared().await).await;
    repeat_send_during_a_move_never_runs_twice(&prepared().await).await;
    claim_skips_a_locked_row(&prepared().await).await;
    wakes_and_events(&prepared().await).await;
    liveness_take_of_a_lock_held_elsewhere_writes_nothing(&prepared().await).await;
}

/// A database with the baseline and the run-time fixtures of
/// [`PREPARE_SQL`], committed.
async fn prepared() -> BaselineDatabase {
    let database = BaselineDatabase::create(&[]).await;
    let session = connect(&database).await;
    session.run_script("PREPARE_SQL", PREPARE_SQL).await;
    database
}

/// A session in autocommit whose statements end after the bound of
/// [`STATEMENT_TIMEOUT_SQL`].
async fn connect(database: &BaselineDatabase) -> Session {
    let session = database.session().await;
    session
        .run_script("STATEMENT_TIMEOUT_SQL", STATEMENT_TIMEOUT_SQL)
        .await;
    session
}

async fn execute(session: &Session, statement: &str, parameters: &[&(dyn ToSql + Sync)]) {
    let _rows = session
        .client()
        .execute(statement, parameters)
        .await
        .unwrap_or_else(|error| panic!("{statement}: {}", describe(&error)));
}

/// The first row of `statement`.
async fn first_row(session: &Session, statement: &str, parameters: &[&(dyn ToSql + Sync)]) -> Row {
    session
        .client()
        .query(statement, parameters)
        .await
        .unwrap_or_else(|error| panic!("{statement}: {}", describe(&error)))
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{statement}: no row"))
}

/// Inserts one committed plain task of worker w1 and returns its id.
async fn insert_task(session: &Session, status: &str, queue: &str) -> Uuid {
    let task_id = Uuid::new_v4();
    execute(session, INSERT_TASK_SQL, &[&task_id, &queue, &status]).await;
    task_id
}

async fn claim_time(session: &Session, task_id: Uuid) -> Option<DateTime<Utc>> {
    first_row(session, CLAIM_TIME_SQL, &[&task_id]).await.get(0)
}

async fn count(session: &Session, statement: &str, parameters: &[&(dyn ToSql + Sync)]) -> i64 {
    first_row(session, statement, parameters).await.get(0)
}

/// Returns once session `pid` waits on a lock of kind `wait_event`.
///
/// # Panics
///
/// When the session does not wait within [`WAIT_POLL_LIMIT`] reads.
async fn wait_for_lock(observer: &Session, pid: i32, wait_event: &str, case: &str) {
    for _ in 0..WAIT_POLL_LIMIT {
        let row = observer
            .client()
            .query_opt(WAIT_STATE_SQL, &[&pid])
            .await
            .unwrap_or_else(|error| panic!("{case}: {}", describe(&error)));
        let waiting = row.is_some_and(|row| {
            row.get::<_, Option<&str>>(0) == Some("Lock")
                && row.get::<_, Option<&str>>(1) == Some(wait_event)
        });
        if waiting {
            return;
        }
        tokio::time::sleep(WAIT_POLL_INTERVAL).await;
    }
    panic!("{case}: the session never waited on a lock of kind {wait_event}");
}

// ---------------------------------------------------------------------------
// Lock order: the task advisory lock before the row lock.
// ---------------------------------------------------------------------------

/// A single-task writer that takes the task advisory lock first.
#[derive(Clone, Copy, Debug)]
enum LockedCall {
    Cancel,
    Complete,
    Fail,
    FailStale,
}

/// The outcome of `call` on `task_id`, claimed at `claimed_at`.
async fn call_locked(
    caller: &Session,
    call: LockedCall,
    task_id: Uuid,
    claimed_at: Option<DateTime<Utc>>,
) -> String {
    let row = match call {
        LockedCall::Cancel => first_row(caller, CANCEL_SQL, &[&task_id]).await,
        LockedCall::Complete => first_row(caller, COMPLETE_SQL, &[&task_id, &claimed_at]).await,
        LockedCall::Fail => first_row(caller, FAIL_SQL, &[&task_id, &claimed_at]).await,
        LockedCall::FailStale => first_row(caller, FAIL_STALE_SQL, &[&task_id]).await,
    };
    row.get(0)
}

async fn advisory_lock_comes_first(
    database: &BaselineDatabase,
    case: &str,
    call: LockedCall,
    status: &str,
) {
    let (setup, holder) = (connect(database).await, connect(database).await);
    let (caller, observer) = (connect(database).await, connect(database).await);
    let task_id = insert_task(&setup, status, SESSION_QUEUE).await;
    let claimed_at = claim_time(&setup, task_id).await;
    holder.run_script("BEGIN", "BEGIN").await;
    execute(&holder, ADVISORY_LOCK_SQL, &[&task_id]).await;
    let (outcome, ()) = tokio::join!(call_locked(&caller, call, task_id, claimed_at), async {
        wait_for_lock(&observer, caller.backend_pid(), "advisory", case).await;
        // The waiting function holds no row lock yet: the holder takes it at once.
        execute(&holder, ROW_LOCK_NOWAIT_SQL, &[&task_id]).await;
        holder.run_script("COMMIT", "COMMIT").await;
    });
    assert_eq!(
        outcome, "APPLIED",
        "{case}: expected APPLIED after the holder ended"
    );
}

async fn batch_success(
    caller: &Session,
    first: (Uuid, Option<DateTime<Utc>>),
    second: (Uuid, Option<DateTime<Utc>>),
) -> Option<String> {
    first_row(
        caller,
        BATCH_SUCCESS_SQL,
        &[&first.0, &second.0, &first.1, &second.1],
    )
    .await
    .get(0)
}

/// The batch takes the low id first whatever the input order: while it waits
/// for the low id's advisory lock it holds nothing of the high id.
async fn batch_success_locks_in_task_id_order(database: &BaselineDatabase) {
    let case = "batch lock order";
    let (setup, holder) = (connect(database).await, connect(database).await);
    let (caller, observer) = (connect(database).await, connect(database).await);
    let first = insert_task(&setup, "RUNNING", SESSION_QUEUE).await;
    let second = insert_task(&setup, "RUNNING", SESSION_QUEUE).await;
    let (low, high) = match first < second {
        true => (first, second),
        false => (second, first),
    };
    let low_claim = claim_time(&setup, low).await;
    let high_claim = claim_time(&setup, high).await;
    holder.run_script("BEGIN", "BEGIN").await;
    execute(&holder, ADVISORY_LOCK_SQL, &[&low]).await;
    let (outcomes, ()) = tokio::join!(
        batch_success(&caller, (high, high_claim), (low, low_claim)),
        async {
            wait_for_lock(&observer, caller.backend_pid(), "advisory", case).await;
            execute(&holder, ROW_LOCK_NOWAIT_SQL, &[&high]).await;
            holder.run_script("COMMIT", "COMMIT").await;
        }
    );
    assert_eq!(
        outcomes.as_deref(),
        Some("APPLIED,APPLIED"),
        "{case}: expected APPLIED,APPLIED"
    );
}

// ---------------------------------------------------------------------------
// A row that moves while another function waits for it.
// ---------------------------------------------------------------------------

async fn start_attempt(
    caller: &Session,
    task_id: Uuid,
    claimed_at: Option<DateTime<Utc>>,
) -> (String, Option<String>) {
    let row = first_row(caller, START_SQL, &[&task_id, &claimed_at]).await;
    (row.get(0), row.get(1))
}

/// A start that waits on a row which a move deletes misses, and writes no
/// beat.
async fn start_of_a_row_that_moves_is_not_held(database: &BaselineDatabase) {
    let case = "start during a move";
    let (setup, holder) = (connect(database).await, connect(database).await);
    let (caller, observer) = (connect(database).await, connect(database).await);
    let task_id = insert_task(&setup, "CLAIMED", SESSION_QUEUE).await;
    let claimed_at = claim_time(&setup, task_id).await;
    holder.run_script("BEGIN", "BEGIN").await;
    execute(&holder, CANCEL_ROWS_SQL, &[&task_id]).await;
    let (outcome, ()) = tokio::join!(start_attempt(&caller, task_id, claimed_at), async {
        wait_for_lock(&observer, caller.backend_pid(), "transactionid", case).await;
        holder.run_script("COMMIT", "COMMIT").await;
    });
    let beats = count(
        &setup,
        "SELECT count(*) FROM horsies_heartbeats WHERE task_id = $1::uuid",
        &[&task_id],
    )
    .await;
    assert_eq!(
        (outcome.0.as_str(), outcome.1.as_deref(), beats),
        ("MISSED", Some("NOT_HELD"), 0),
        "{case}: expected MISSED NOT_HELD with no beat"
    );
}

async fn send(caller: &Session, statement: &str, task_id: Uuid, sent_at: DateTime<Utc>) -> String {
    first_row(caller, statement, &[&task_id, &sent_at])
        .await
        .get(0)
}

async fn read_sent_at(session: &Session, task_id: Uuid) -> DateTime<Utc> {
    first_row(session, SENT_AT_SQL, &[&task_id]).await.get(0)
}

/// The conflict read of a keyed send waits on the row; the move deletes it;
/// the send answers CONFLICT_UNVERIFIABLE and leaves no reservation and no
/// second live row.
async fn keyed_send_on_a_row_that_moves_is_unverifiable(database: &BaselineDatabase) {
    let case = "keyed send during a move";
    let (setup, holder) = (connect(database).await, connect(database).await);
    let (caller, observer) = (connect(database).await, connect(database).await);
    let task_id = insert_task(&setup, "PENDING", SESSION_QUEUE).await;
    let sent_at = read_sent_at(&setup, task_id).await;
    holder.run_script("BEGIN", "BEGIN").await;
    execute(&holder, ADVISORY_LOCK_SQL, &[&task_id]).await;
    execute(&holder, ROW_LOCK_SQL, &[&task_id]).await;
    let (outcome, ()) = tokio::join!(send(&caller, KEYED_SEND_SQL, task_id, sent_at), async {
        wait_for_lock(&observer, caller.backend_pid(), "transactionid", case).await;
        execute(&holder, CANCEL_ROWS_SQL, &[&task_id]).await;
        holder.run_script("COMMIT", "COMMIT").await;
    });
    let reservations = count(&setup, SESSION_KEY_RESERVATIONS_SQL, &[]).await;
    let live = count(&setup, LIVE_COUNT_SQL, &[&task_id]).await;
    assert_eq!(
        (outcome.as_str(), reservations, live),
        ("CONFLICT_UNVERIFIABLE", 0, 0),
        "{case}: expected CONFLICT_UNVERIFIABLE, no reservation, no live row"
    );
}

/// A repeat send waits on the task advisory lock of a move in progress, then
/// finds the task in history: SAME_COMMAND, and no second live row.
async fn repeat_send_during_a_move_never_runs_twice(database: &BaselineDatabase) {
    let case = "repeat send during a move";
    let (setup, holder) = (connect(database).await, connect(database).await);
    let (caller, observer) = (connect(database).await, connect(database).await);
    let task_id = insert_task(&setup, "PENDING", SESSION_QUEUE).await;
    let sent_at = read_sent_at(&setup, task_id).await;
    holder.run_script("BEGIN", "BEGIN").await;
    execute(&holder, CANCEL_ROWS_SQL, &[&task_id]).await;
    let (outcome, ()) = tokio::join!(send(&caller, REPEAT_SEND_SQL, task_id, sent_at), async {
        wait_for_lock(&observer, caller.backend_pid(), "advisory", case).await;
        holder.run_script("COMMIT", "COMMIT").await;
    });
    let live = count(&setup, LIVE_COUNT_SQL, &[&task_id]).await;
    assert_eq!(
        (outcome.as_str(), live),
        ("SAME_COMMAND", 0),
        "{case}: expected SAME_COMMAND and no live row"
    );
}

/// The claim never waits on a row: a locked due row is skipped and the pass
/// reports SHORT.
async fn claim_skips_a_locked_row(database: &BaselineDatabase) {
    let case = "claim skips locked rows";
    let (setup, holder, caller) = (
        connect(database).await,
        connect(database).await,
        connect(database).await,
    );
    let task_id = insert_task(&setup, "PENDING", "q_skip").await;
    holder.run_script("BEGIN", "BEGIN").await;
    execute(&holder, ROW_LOCK_SQL, &[&task_id]).await;
    let rows: Vec<(Option<String>, Option<i32>)> = caller
        .client()
        .query(CLAIM_SQL, &[])
        .await
        .unwrap_or_else(|error| panic!("{case}: {}", describe(&error)))
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    holder.run_script("COMMIT", "COMMIT").await;
    assert_eq!(
        rows,
        vec![(Some("SHORT".to_owned()), Some(1))],
        "{case}: expected one SHORT row of 1"
    );
}

/// The liveness take of a lock that another session holds answers false
/// and writes no generation: the generation stays the one of the session
/// that holds the lock.
async fn liveness_take_of_a_lock_held_elsewhere_writes_nothing(database: &BaselineDatabase) {
    let case = "liveness take of a lock held elsewhere";
    let worker_id = "w-liveness";
    let (holder, taker) = (connect(database).await, connect(database).await);
    let taken: bool = first_row(&holder, TAKE_LIVENESS_SQL, &[&worker_id])
        .await
        .get(0);
    let held_generation: i64 = first_row(&taker, LIVENESS_GENERATION_SQL, &[&worker_id])
        .await
        .get(0);
    let answer: bool = first_row(&taker, TAKE_LIVENESS_SQL, &[&worker_id])
        .await
        .get(0);
    let generation_after: i64 = first_row(&taker, LIVENESS_GENERATION_SQL, &[&worker_id])
        .await
        .get(0);
    assert!(taken, "{case}: the first session takes the free lock");
    assert!(!answer, "{case}: the second session finds the lock held");
    assert_eq!(
        generation_after, held_generation,
        "{case}: the refused take writes no generation"
    );
    drop(holder);
}

// ---------------------------------------------------------------------------
// Wakes and status events.
// ---------------------------------------------------------------------------

/// The notifications that arrived within [`NOTIFY_SETTLE`].
async fn drain(listener: &Session) -> Vec<(String, String)> {
    tokio::time::sleep(NOTIFY_SETTLE).await;
    listener.take_notifications()
}

fn event(channel: &str, payload: &str) -> (String, String) {
    (channel.to_owned(), payload.to_owned())
}

/// Asserts that `received` and `expected` hold the same notifications, in
/// any order.
fn check(step: &str, mut received: Vec<(String, String)>, mut expected: Vec<(String, String)>) {
    received.sort();
    expected.sort();
    assert_eq!(received, expected, "wakes and events, {step}");
}

async fn wakes_and_events(database: &BaselineDatabase) {
    let (setup, listener) = (connect(database).await, connect(database).await);
    // The fixture rows exist before the listener starts, so their insert
    // events are not received.
    let first = insert_task(&setup, "RUNNING", "q_wake").await;
    let second = insert_task(&setup, "RUNNING", "q_wake").await;
    let silent = insert_task(&setup, "RUNNING", "q_wake").await;
    let failing = insert_task(&setup, "RUNNING", "q_wake").await;
    let retrying = insert_task(&setup, "RUNNING", "q_wake").await;
    let released = [
        insert_task(&setup, "CLAIMED", "q_wake").await,
        insert_task(&setup, "CLAIMED", "q_wake").await,
        insert_task(&setup, "CLAIMED", "q_wake").await,
    ];
    let starting = insert_task(&setup, "CLAIMED", "q_wake").await;
    for listen in LISTEN_SQL {
        listener.run_script(listen, listen).await;
    }

    let sent = Uuid::new_v4();
    execute(&setup, WAKE_SEND_SQL, &[&sent]).await;
    check(
        "enqueue",
        drain(&listener).await,
        vec![
            event("task_new", &sent.to_string()),
            event("task_queue_q_wake", "new"),
            event("horsies_task_status", &sent.to_string()),
        ],
    );

    let first_claim = claim_time(&setup, first).await;
    let second_claim = claim_time(&setup, second).await;
    let silent_claim = claim_time(&setup, silent).await;
    let failing_claim = claim_time(&setup, failing).await;
    let retrying_claim = claim_time(&setup, retrying).await;
    execute(
        &setup,
        WAKE_BATCH_SUCCESS_SQL,
        &[&first, &second, &first_claim, &second_claim],
    )
    .await;
    check(
        "batch success: one capacity wake for the queue",
        drain(&listener).await,
        vec![
            event("task_queue_q_wake", "capacity"),
            event("task_done", &first.to_string()),
            event("horsies_task_status", &first.to_string()),
            event("task_done", &second.to_string()),
            event("horsies_task_status", &second.to_string()),
        ],
    );
    execute(&setup, SILENT_SUCCESS_SQL, &[&silent, &silent_claim]).await;
    check(
        "success with wake off",
        drain(&listener).await,
        vec![
            event("task_done", &silent.to_string()),
            event("horsies_task_status", &silent.to_string()),
        ],
    );
    execute(&setup, TERMINAL_FAILURE_SQL, &[&failing, &failing_claim]).await;
    check(
        "terminal failure of a plain task",
        drain(&listener).await,
        vec![
            event("task_queue_q_wake", "capacity"),
            event("task_done", &failing.to_string()),
            event("horsies_task_status", &failing.to_string()),
        ],
    );
    execute(&setup, RETRY_FAILURE_SQL, &[&retrying, &retrying_claim]).await;
    check(
        "retry: no wake, one status event",
        drain(&listener).await,
        vec![event("horsies_task_status", &retrying.to_string())],
    );

    let released_claims = [
        claim_time(&setup, released[0]).await,
        claim_time(&setup, released[1]).await,
        claim_time(&setup, released[2]).await,
    ];
    execute(
        &setup,
        RELEASE_ISOLATED_SQL,
        &[
            &released[0],
            &released[1],
            &released_claims[0],
            &released_claims[1],
        ],
    )
    .await;
    check(
        "release as ISOLATED: one class wake",
        drain(&listener).await,
        vec![
            event("task_queue_q_wake", "class"),
            event("horsies_task_status", &released[0].to_string()),
            event("horsies_task_status", &released[1].to_string()),
        ],
    );
    execute(
        &setup,
        RELEASE_SAME_CLASS_SQL,
        &[&released[2], &released_claims[2]],
    )
    .await;
    check(
        "release in the same class: no wake",
        drain(&listener).await,
        vec![event("horsies_task_status", &released[2].to_string())],
    );

    let start_claim = claim_time(&setup, starting).await;
    execute(&setup, START_ROWS_SQL, &[&starting, &start_claim]).await;
    check("start: no event", drain(&listener).await, vec![]);
    execute(&setup, CANCEL_ROWS_SQL, &[&starting]).await;
    check(
        "cancel: done and status, no wake",
        drain(&listener).await,
        vec![
            event("task_done", &starting.to_string()),
            event("horsies_task_status", &starting.to_string()),
        ],
    );
}

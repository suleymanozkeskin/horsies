//! The statement of a plain-success batch (`complete_plain_successes`,
//! `horsies_complete_tasks_fused` of 0004) on the test database.
//!
//! Python enters through the worker's finalizers, which hand their rows to
//! the finalize batcher. The batcher itself (one batch per flusher, the
//! split of a refused batch, the answer after the write) has its own tests
//! in `worker::finalize_batch::tests`; these tests send the batch statement
//! as the batcher's write does, and read the rows, the attempts, the wakes,
//! the round trips and the locks that it leaves.

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use super::support::*;
use crate::broker::core_rows::StatementLog;
use crate::broker::pools::{core_pool_for_test_url, unpinged_single_connection_core_pool_for_url};
use crate::core::lifecycle::{TerminalizationKind, TerminalizationOutcome};
use crate::db::{CorePool, Transience};
use crate::test_support::sql::{self, TestConnection, TestPool};
use crate::worker::finalize_batch::statement::complete_plain_successes;
use crate::worker::finalize_batch::{BatchWriteError, PlainSuccess, SuccessBatch};

/// The rows of a full batch in the Python tests.
const BATCH_ROWS: usize = 8;
/// The queue of the seeded rows, and its wake channel.
const QUEUE: &str = "finalize_batch_rows";
const CHANNEL: &str = "task_queue_finalize_batch_rows";
/// The seed of the task advisory lock (`hashtextextended(id, 731)`).
const TASK_LOCK_SEED: i64 = 731;
/// A retention class of the baseline key form that no test registers: no
/// history leaf covers it, so the move of its row raises 55H01.
const UNREGISTERED_CLASS: &str = "standard_1777d";
/// Bound of the wait for the batch statement to wait on a lock: 100 polls
/// of 50 ms.
const WAIT_POLLS: u32 = 100;
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(50);
/// Bound of the wait for a batch statement that a released lock lets end.
const WRITE_END_WAIT: Duration = Duration::from_secs(30);

fn result_text() -> String {
    r#"{"__h_task_result__":true,"ok":1,"err":null}"#.to_owned()
}

fn success_at(task_id: Uuid, claimed_at: chrono::DateTime<chrono::Utc>) -> PlainSuccess {
    PlainSuccess {
        task_id,
        claimed_at,
        result_json: result_text(),
        placement: crate::core::lifecycle::AttemptPlacement::NotObserved,
    }
}

fn batch_of(task_ids: &[Uuid]) -> SuccessBatch {
    SuccessBatch::new(
        task_ids
            .iter()
            .map(|task_id| success_at(*task_id, generation()))
            .collect(),
    )
    .expect("a test batch inside its bound")
}

/// A RUNNING row in [`QUEUE`] that `WORKER` holds at `generation()`.
fn running() -> TaskSeed {
    TaskSeed {
        queue_name: QUEUE,
        ..TaskSeed::running()
    }
}

/// `count` rows of [`running`].
async fn running_tasks(pool: &TestPool, count: usize) -> Vec<Uuid> {
    let mut task_ids = Vec::with_capacity(count);
    for _ in 0..count {
        task_ids.push(seed_task(pool, &running()).await);
    }
    task_ids
}

/// The write of a batch that a missing history leaf refused: transient,
/// SQLSTATE 55H01, with the class in the message.
fn assert_refused_for_the_leaf(refused: Result<Vec<TerminalizationOutcome>, BatchWriteError>) {
    match refused {
        Err(BatchWriteError::Refused {
            transience,
            detail,
            server_error,
        }) => {
            assert_eq!(transience, Transience::Transient, "{detail}");
            assert_eq!(server_error.sqlstate().code(), "55H01", "{detail}");
            assert!(
                server_error.primary_message().starts_with(&format!(
                    "no task-history leaf of class {UNREGISTERED_CLASS} covers ["
                )),
                "{detail}"
            );
        }
        other => panic!("expected a transient refusal of the leaf, got {other:?}"),
    }
}

/// The batch statement, as the batcher's write sends it.
async fn write(
    core: &CorePool,
    batch: &SuccessBatch,
) -> Result<Vec<TerminalizationOutcome>, BatchWriteError> {
    complete_plain_successes(core, StatementLog::Off, WORKER, batch)
        .await
        .map(|outcomes| outcomes.into_outcomes())
}

/// The kind of each outcome, in order: `Applied`, `AlreadyApplied`,
/// `LostClaim`, `SourceStateConflict` or `TaskAbsent`.
fn kinds(outcomes: &[TerminalizationOutcome]) -> Vec<&'static str> {
    outcomes
        .iter()
        .map(|outcome| match outcome {
            TerminalizationOutcome::Applied { .. } => "Applied",
            TerminalizationOutcome::AlreadyApplied { .. } => "AlreadyApplied",
            TerminalizationOutcome::LostClaim { .. } => "LostClaim",
            TerminalizationOutcome::SourceStateConflict { .. } => "SourceStateConflict",
            TerminalizationOutcome::TaskAbsent { .. } => "TaskAbsent",
        })
        .collect()
}

/// The status of each task that still has a live row.
async fn live_statuses(pool: &TestPool, task_ids: &[Uuid]) -> Vec<(Uuid, String)> {
    sql::query_as("SELECT id, status::text FROM horsies_tasks WHERE id = ANY($1) ORDER BY id")
        .bind(task_ids)
        .fetch_all(pool)
        .await
        .expect("read live statuses")
}

/// The tasks of `task_ids` that have a history row.
async fn moved(pool: &TestPool, task_ids: &[Uuid]) -> Vec<Uuid> {
    sql::query_scalar(
        "SELECT task_id FROM horsies_task_history WHERE task_id = ANY($1) ORDER BY task_id",
    )
    .bind(task_ids)
    .fetch_all(pool)
    .await
    .expect("read history rows")
}

fn sorted(task_ids: &[Uuid]) -> Vec<Uuid> {
    let mut sorted = task_ids.to_vec();
    sorted.sort();
    sorted
}

/// A connection that holds task advisory locks, as another writer would.
async fn lock_holder(pool: &TestPool) -> TestConnection {
    TestConnection::connect(pool.url())
        .await
        .expect("connect the lock holder")
}

async fn hold_task_lock(holder: &mut TestConnection, task_id: Uuid) {
    sql::query("SELECT pg_advisory_lock(hashtextextended($1::text, $2))")
        .bind(task_id)
        .bind(TASK_LOCK_SEED)
        .execute(holder)
        .await
        .expect("hold the task's advisory lock");
}

async fn release_task_lock(holder: &mut TestConnection, task_id: Uuid) {
    sql::query("SELECT pg_advisory_unlock(hashtextextended($1::text, $2))")
        .bind(task_id)
        .bind(TASK_LOCK_SEED)
        .execute(holder)
        .await
        .expect("release the task's advisory lock");
}

/// The backend of this test's database that runs the batch statement and
/// waits on a lock.
async fn batch_backend_waiting(pool: &TestPool) -> i32 {
    for _ in 0..WAIT_POLLS {
        let waiting: Option<i32> = sql::query_scalar(
            "SELECT pid FROM pg_stat_activity
             WHERE datname = current_database()
               AND query LIKE '%horsies_complete_tasks_fused(%'
               AND wait_event_type = 'Lock'
               AND pid <> pg_backend_pid()
             LIMIT 1",
        )
        .fetch_optional(pool)
        .await
        .expect("read the backends");
        if let Some(pid) = waiting {
            return pid;
        }
        tokio::time::sleep(WAIT_POLL_INTERVAL).await;
    }
    panic!("no batch statement waited on a lock");
}

/// Python `test_a_batch_applies_every_row_and_answers_each_finalizer` (the
/// statement) and `test_a_replay_of_a_whole_batch_is_already_applied_and_writes_nothing`:
/// one statement completes every row of a batch, each with one attempt,
/// and wakes the queue once. The same batch sent again answers each row
/// `AlreadyApplied`, wakes nothing and adds no attempt.
#[tokio::test]
async fn a_batch_completes_every_row_once_and_its_replay_writes_nothing() {
    let pool = pool().await;
    let core = core_pool_for_test_url(pool.url());
    let mut wakes = Wakes::listen(&pool, CHANNEL).await;
    let task_ids = running_tasks(&pool, BATCH_ROWS).await;
    let batch = batch_of(&task_ids);

    let written = write(&core, &batch).await.expect("the batch statement");

    assert_eq!(kinds(&written), vec!["Applied"; BATCH_ROWS]);
    assert!(written.iter().all(|outcome| matches!(
        outcome,
        TerminalizationOutcome::Applied {
            kind: TerminalizationKind::CompleteFused,
            ..
        }
    )));
    assert_eq!(live_statuses(&pool, &task_ids).await, Vec::new());
    for task_id in &task_ids {
        let image = history_row(&pool, *task_id).await.expect("history row");
        assert_eq!(
            (image.status.as_str(), image.error_code),
            ("COMPLETED", None)
        );
        assert_eq!(archived_attempts(&pool, *task_id).await.len(), 1);
    }
    assert_eq!(wakes.payloads(&pool).await, vec!["capacity".to_owned()]);

    let replayed = write(&core, &batch).await.expect("the replay");

    assert_eq!(kinds(&replayed), vec!["AlreadyApplied"; BATCH_ROWS]);
    assert_eq!(wakes.payloads(&pool).await, Vec::<String>::new());
    for task_id in &task_ids {
        assert_eq!(archived_attempts(&pool, *task_id).await.len(), 1);
    }
    core.close();
    cleanup_tasks(&pool, &task_ids).await;
}

/// Python `test_each_row_keeps_its_own_fence_in_one_batch`: a row of this
/// claim, a row of another worker, a row of an older claim generation, an
/// absent task and a committed row in one statement: each row answers on
/// its own fence, and only the held row moves.
#[tokio::test]
async fn each_row_keeps_its_own_fence_in_one_batch() {
    let pool = pool().await;
    let core = core_pool_for_test_url(pool.url());
    let mine = seed_task(&pool, &running()).await;
    let foreign = seed_task(
        &pool,
        &TaskSeed {
            holder: Holder::other_worker(),
            ..running()
        },
    )
    .await;
    let stale = seed_task(&pool, &running()).await;
    let done = seed_task(&pool, &running()).await;
    let committed = write(&core, &batch_of(&[done]))
        .await
        .expect("commit `done`");
    assert_eq!(kinds(&committed), vec!["Applied"]);
    let absent = Uuid::new_v4();
    let batch = SuccessBatch::new(vec![
        success_at(mine, generation()),
        success_at(foreign, generation()),
        success_at(stale, generation() - chrono::Duration::seconds(1)),
        success_at(absent, generation()),
        success_at(done, generation()),
    ])
    .expect("a batch of five rows");

    let written = write(&core, &batch).await.expect("the batch statement");

    assert_eq!(
        kinds(&written),
        vec![
            "Applied",
            "LostClaim",
            "LostClaim",
            "TaskAbsent",
            "AlreadyApplied"
        ]
    );
    assert_eq!(
        written
            .iter()
            .map(TerminalizationOutcome::task_id)
            .collect::<Vec<_>>(),
        vec![mine, foreign, stale, absent, done]
    );
    let held_rows = [mine, foreign, stale];
    let mut still_running = vec![
        (foreign, "RUNNING".to_owned()),
        (stale, "RUNNING".to_owned()),
    ];
    still_running.sort();
    assert_eq!(live_statuses(&pool, &held_rows).await, still_running);
    assert_eq!(
        moved(&pool, &[mine, foreign, stale, done]).await,
        sorted(&[mine, done])
    );
    core.close();
    cleanup_tasks(&pool, &[mine, foreign, stale, done]).await;
}

/// Python `test_a_batch_writes_each_result_text_as_given`: escapes,
/// whitespace and characters outside ASCII reach the history row
/// unchanged.
#[tokio::test]
async fn a_batch_writes_each_result_text_as_given() {
    let pool = pool().await;
    let core = core_pool_for_test_url(pool.url());
    let first = seed_task(&pool, &running()).await;
    let second = seed_task(&pool, &running()).await;
    let first_text = format!(
        "{{\n\t{}",
        r#""ok": "quote \" backslash \\ escaped nul \u0000"}"#
    );
    let second_text = r#"{"ok":"é 中 𝄞"}"#.to_owned();
    let batch = SuccessBatch::new(vec![
        PlainSuccess {
            result_json: first_text.clone(),
            ..success_at(first, generation())
        },
        PlainSuccess {
            result_json: second_text.clone(),
            ..success_at(second, generation())
        },
    ])
    .expect("a batch of two rows");

    let written = write(&core, &batch).await.expect("the batch statement");

    assert_eq!(kinds(&written), vec!["Applied", "Applied"]);
    for (task_id, text) in [(first, &first_text), (second, &second_text)] {
        let payload = history_row(&pool, task_id)
            .await
            .expect("history row")
            .result_payload
            .expect("a stored result");
        assert_eq!(
            String::from_utf8(payload).expect("the stored result is UTF-8"),
            *text
        );
    }
    core.close();
    cleanup_tasks(&pool, &[first, second]).await;
}

/// Python `test_a_claim_generation_keeps_its_microseconds_and_its_offset`
/// (the microseconds): the fence compares instants to the microsecond; one
/// microsecond later is another claim. The offset half has no counterpart:
/// a core claim generation is a `DateTime<Utc>`, an instant with no offset.
#[tokio::test]
async fn a_claim_generation_keeps_its_microseconds() {
    let pool = pool().await;
    let core = core_pool_for_test_url(pool.url());
    let same = seed_task(&pool, &running()).await;
    let later = seed_task(&pool, &running()).await;
    let batch = SuccessBatch::new(vec![
        success_at(same, generation()),
        success_at(later, generation() + chrono::Duration::microseconds(1)),
    ])
    .expect("a batch of two rows");

    let written = write(&core, &batch).await.expect("the batch statement");

    assert_eq!(kinds(&written), vec!["Applied", "LostClaim"]);
    assert_eq!(
        live_statuses(&pool, &[same, later]).await,
        vec![(later, "RUNNING".to_owned())]
    );
    core.close();
    cleanup_tasks(&pool, &[same, later]).await;
}

/// Python `test_a_batch_of_rows_is_one_round_trip`: a batch of
/// [`BATCH_ROWS`] rows is one round trip on a warm connection, counted at
/// the wire.
#[tokio::test]
async fn a_batch_of_rows_is_one_round_trip() {
    let pool = pool().await;
    let upstream = pool.url();
    let proxy = WireProxy::start(upstream).await;
    let proxied = unpinged_single_connection_core_pool_for_url(&proxy.url(upstream));
    // Open the connection, run the pool's first-use check and prepare the
    // statement on it, with a task that does not exist.
    let warm_up = write(&proxied, &batch_of(&[Uuid::new_v4()]))
        .await
        .expect("warm up");
    assert_eq!(kinds(&warm_up), vec!["TaskAbsent"]);
    let task_ids = running_tasks(&pool, BATCH_ROWS).await;

    proxy.reset_round_trips();
    let written = write(&proxied, &batch_of(&task_ids))
        .await
        .expect("the batch statement");
    let round_trips = proxy.round_trips();

    assert_eq!(round_trips, 1);
    assert_eq!(kinds(&written), vec!["Applied"; BATCH_ROWS]);
    assert_eq!(moved(&pool, &task_ids).await, sorted(&task_ids));
    proxied.close();
    cleanup_tasks(&pool, &task_ids).await;
}

/// Python `test_a_bad_row_rolls_back_its_batch_and_each_row_is_written_alone_once`
/// (the statements; the split of a refused batch is
/// `worker::finalize_batch::tests::a_refused_batch_writes_each_row_alone_once`):
/// no history leaf covers the class of one row, so its move raises 55H01
/// and the whole statement writes nothing; the refusal may clear (a later
/// coverage pass creates the leaf). Each row sent alone then completes,
/// except the bad row, which is refused again and stays RUNNING.
#[tokio::test]
async fn a_bad_row_refuses_its_whole_batch_and_each_row_alone_writes_its_own() {
    let pool = pool().await;
    let core = core_pool_for_test_url(pool.url());
    let good_a = seed_task(&pool, &running()).await;
    let bad = seed_task(
        &pool,
        &TaskSeed {
            retention_class_key: UNREGISTERED_CLASS,
            ..running()
        },
    )
    .await;
    let good_b = seed_task(&pool, &running()).await;
    let all = [good_a, bad, good_b];

    let refused = write(&core, &batch_of(&all)).await;

    assert_refused_for_the_leaf(refused);
    let mut all_running: Vec<(Uuid, String)> = all
        .iter()
        .map(|task_id| (*task_id, "RUNNING".to_owned()))
        .collect();
    all_running.sort();
    assert_eq!(live_statuses(&pool, &all).await, all_running);
    assert_eq!(moved(&pool, &all).await, Vec::<Uuid>::new());

    let first_alone = write(&core, &batch_of(&[good_a])).await.expect("good_a");
    assert_refused_for_the_leaf(write(&core, &batch_of(&[bad])).await);
    let last_alone = write(&core, &batch_of(&[good_b])).await.expect("good_b");

    assert_eq!(kinds(&first_alone), vec!["Applied"]);
    assert_eq!(kinds(&last_alone), vec!["Applied"]);
    assert_eq!(
        live_statuses(&pool, &all).await,
        vec![(bad, "RUNNING".to_owned())]
    );
    core.close();
    cleanup_tasks(&pool, &all).await;
}

/// Python `test_a_lost_reply_of_a_batch_is_classified_by_the_replay_of_each_row`:
/// the batch commits and its reply never reaches the worker. The write
/// fails with an unknown outcome, which may clear; the replay answers each
/// row `AlreadyApplied`, and each task keeps one attempt.
#[tokio::test]
async fn a_lost_reply_of_a_batch_is_classified_by_the_replay_of_each_row() {
    let pool = pool().await;
    let upstream = pool.url();
    let proxy = WireProxy::start(upstream).await;
    let proxied = unpinged_single_connection_core_pool_for_url(&proxy.url(upstream));
    // Warm the connection first, so the batch is its one request.
    let warm_up = write(&proxied, &batch_of(&[Uuid::new_v4()]))
        .await
        .expect("warm up");
    assert_eq!(kinds(&warm_up), vec!["TaskAbsent"]);
    let task_ids = running_tasks(&pool, 4).await;
    let batch = batch_of(&task_ids);

    proxy.drop_next_reply();
    let first = write(&proxied, &batch).await;

    assert!(proxy.reply_was_dropped(), "the proxy relayed the reply");
    match &first {
        Err(error @ BatchWriteError::CommitUnknown { .. }) => {
            assert_eq!(error.transience(), Transience::Transient);
        }
        other => panic!("expected an unknown outcome, got {other:?}"),
    }
    // The premise of the replay: the statement committed.
    assert_eq!(live_statuses(&pool, &task_ids).await, Vec::new());
    let core = core_pool_for_test_url(pool.url());
    let replayed = write(&core, &batch).await.expect("the replay");

    assert_eq!(kinds(&replayed), vec!["AlreadyApplied"; 4]);
    for task_id in &task_ids {
        assert_eq!(archived_attempts(&pool, *task_id).await.len(), 1);
    }
    core.close();
    proxied.close();
    cleanup_tasks(&pool, &task_ids).await;
}

/// Python `test_the_finalizer_end_and_the_capacity_wake_follow_the_commit`
/// (the statement; the answer of each row after the write is
/// `worker::finalize_batch::tests::the_answer_resolves_only_after_the_write_returned`):
/// while the batch statement waits on the lock of its last row, the write
/// has not returned, every row is RUNNING, and no capacity wake is out.
/// Once the lock is free, every row completes and the queue wakes.
#[tokio::test]
async fn the_answer_and_the_capacity_wake_follow_the_commit() {
    let pool = pool().await;
    let core = Arc::new(core_pool_for_test_url(pool.url()));
    let mut wakes = Wakes::listen(&pool, CHANNEL).await;
    let task_ids = running_tasks(&pool, 3).await;
    let last = *sorted(&task_ids).last().expect("three rows");
    let mut holder = lock_holder(&pool).await;
    hold_task_lock(&mut holder, last).await;
    let writer = Arc::clone(&core);
    let batch = batch_of(&task_ids);
    let writing = tokio::spawn(async move { write(&writer, &batch).await });
    let _ = batch_backend_waiting(&pool).await;

    assert!(!writing.is_finished());
    let mut all_running: Vec<(Uuid, String)> = task_ids
        .iter()
        .map(|task_id| (*task_id, "RUNNING".to_owned()))
        .collect();
    all_running.sort();
    assert_eq!(live_statuses(&pool, &task_ids).await, all_running);
    assert_eq!(wakes.payloads(&pool).await, Vec::<String>::new());

    release_task_lock(&mut holder, last).await;
    let written = tokio::time::timeout(WRITE_END_WAIT, writing)
        .await
        .expect("the write ends once the lock is free")
        .expect("the write task ended without a panic")
        .expect("the batch statement");

    assert_eq!(kinds(&written), vec!["Applied"; 3]);
    assert_eq!(live_statuses(&pool, &task_ids).await, Vec::new());
    assert_eq!(wakes.payloads(&pool).await, vec!["capacity".to_owned()]);
    core.close();
    cleanup_tasks(&pool, &task_ids).await;
}

/// Python `test_a_batch_killed_mid_statement_leaves_its_rows_for_stale_recovery`
/// (the write; stale RUNNING recovery that fails such rows as crashed is
/// `stale_recovery_functions::reaper_pass::a_pass_retries_by_policy_fails_the_rest_and_wakes_the_retried_queue`):
/// the statement waits on the lock of its last row and its backend ends.
/// Nothing of it commits: the write fails with an unknown outcome, which
/// may clear, and each row stays RUNNING with no history row.
#[tokio::test]
async fn a_batch_killed_mid_statement_leaves_its_rows_running() {
    let pool = pool().await;
    let core = Arc::new(core_pool_for_test_url(pool.url()));
    let task_ids = running_tasks(&pool, 3).await;
    let last = *sorted(&task_ids).last().expect("three rows");
    let mut holder = lock_holder(&pool).await;
    hold_task_lock(&mut holder, last).await;
    let writer = Arc::clone(&core);
    let batch = batch_of(&task_ids);
    let writing = tokio::spawn(async move { write(&writer, &batch).await });
    let pid = batch_backend_waiting(&pool).await;

    terminate_backend(&pool, pid).await;
    let written = tokio::time::timeout(WRITE_END_WAIT, writing)
        .await
        .expect("the write ends with its backend")
        .expect("the write task ended without a panic");
    release_task_lock(&mut holder, last).await;

    match &written {
        Err(error @ BatchWriteError::CommitUnknown { .. }) => {
            assert_eq!(error.transience(), Transience::Transient);
        }
        other => panic!("expected an unknown outcome, got {other:?}"),
    }
    let mut all_running: Vec<(Uuid, String)> = task_ids
        .iter()
        .map(|task_id| (*task_id, "RUNNING".to_owned()))
        .collect();
    all_running.sort();
    assert_eq!(live_statuses(&pool, &task_ids).await, all_running);
    assert_eq!(moved(&pool, &task_ids).await, Vec::<Uuid>::new());
    core.close();
    cleanup_tasks(&pool, &task_ids).await;
}

/// Python `test_the_batch_locks_its_rows_in_sorted_task_id_order`: rows
/// given high, middle, low. With the lock of the middle row held elsewhere,
/// the statement holds the lock of the low row, waits on the middle one,
/// and has not reached the high one. Once the lock is free, every row
/// completes, answered in input order.
#[tokio::test]
async fn the_batch_locks_its_rows_in_sorted_task_id_order() {
    let pool = pool().await;
    let core = Arc::new(core_pool_for_test_url(pool.url()));
    let task_ids = running_tasks(&pool, 3).await;
    let ordered = sorted(&task_ids);
    let (low, middle, high) = (ordered[0], ordered[1], ordered[2]);
    let mut holder = lock_holder(&pool).await;
    hold_task_lock(&mut holder, middle).await;
    let writer = Arc::clone(&core);
    let batch = batch_of(&[high, middle, low]);
    let writing = tokio::spawn(async move { write(&writer, &batch).await });
    let pid = batch_backend_waiting(&pool).await;

    let mut locks: Vec<(Uuid, bool)> = sql::query_as(
        "SELECT t.task_id, l.granted
         FROM unnest($1::uuid[]) AS t(task_id)
         JOIN pg_locks AS l
           ON l.locktype = 'advisory'
          AND l.pid = $2
          AND l.classid = ((hashtextextended(t.task_id::text, $3) >> 32) & 4294967295)::oid
          AND l.objid = (hashtextextended(t.task_id::text, $3) & 4294967295)::oid
          AND l.objsubid = 1",
    )
    .bind(vec![low, middle, high])
    .bind(pid)
    .bind(TASK_LOCK_SEED)
    .fetch_all(&pool)
    .await
    .expect("read the advisory locks of the batch backend");
    release_task_lock(&mut holder, middle).await;
    let written = tokio::time::timeout(WRITE_END_WAIT, writing)
        .await
        .expect("the write ends once the lock is free")
        .expect("the write task ended without a panic")
        .expect("the batch statement");

    locks.sort();
    let mut expected = vec![(low, true), (middle, false)];
    expected.sort();
    assert_eq!(locks, expected);
    assert_eq!(
        written
            .iter()
            .map(TerminalizationOutcome::task_id)
            .collect::<Vec<_>>(),
        vec![high, middle, low]
    );
    assert_eq!(kinds(&written), vec!["Applied"; 3]);
    core.close();
    cleanup_tasks(&pool, &task_ids).await;
}

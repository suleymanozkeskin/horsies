//! The runner beat writer, and the parent half of Python
//! `clients/python/tests/e2e/test_child_isolation.py::test_a_task_that_holds_the_gil_is_not_failed_as_stale`.
//!
//! The worker parent writes one statement per interval for every task it
//! holds. Each row is fenced: RUNNING, owned by this worker, at the claim
//! generation of the dispatch, and without the finalizing marker. The reaper
//! reads the newest runner beat, so a beat that lands keeps a running task
//! out of stale recovery, and a fence that fails leaves the row to it.

use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    end_child, launch_runner_beats, sleeping_child, test_beat_pool, write_beats, BeatSender,
    BeatWrite, FailClosedStop, LiveDispatches, StaleRunningRecovery, INSERT_RUNNER_HEARTBEATS_SQL,
};
use crate::broker::cancelled_runs::RunClaim;
use crate::core::history::maintenance::coverage::ensure_startup_coverage_in_pool;
use crate::test_support::sql::{self, TestPool};
use crate::worker::executor::slots::dispatch::AttemptLog;
use crate::worker::executor::supervisor::Spawner;
use crate::worker::recovery::on_test_pool::mark_stale_running_as_failed;

const WORKER_ID: &str = "beat-spec-worker-1";
const OTHER_WORKER_ID: &str = "beat-spec-worker-2";
const BEAT_PID: i32 = 4242;

/// The heartbeat leaves the startup coverage creates ahead of the current
/// hour, as in the other runner-beat tests.
const COVERAGE_HORIZON: u32 = 2;

/// The stale threshold of the reaper call, in seconds (Python
/// `stale_threshold_ms=60_000`).
const STALE_THRESHOLD_SECONDS: f64 = 60.0;

/// The finalizing threshold of the reaper call, in seconds. No seeded row
/// carries the marker in the reaper tests.
const FINALIZING_THRESHOLD_SECONDS: f64 = 300.0;

/// The candidate bound of one reaper scan.
const STALE_SCAN_LIMIT: i64 = 1_000;

/// The interval of the beat loop in the loop test: the smallest interval the
/// recovery config accepts.
const LOOP_BEAT_INTERVAL: Duration = Duration::from_secs(1);

/// How long the loop test waits for the first landed beat.
const FIRST_BEAT_BOUND: Duration = Duration::from_secs(15);

/// How often the loop test reads the beat count.
const BEAT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// The stale threshold of the reaper call in the loop test: well above one
/// beat interval, far below the age of the row.
const LOOP_STALE_THRESHOLD_SECONDS: f64 = 10.0;

/// The status a seeded row has.
#[derive(Debug, Clone, Copy)]
enum SeedStatus {
    Pending,
    Claimed,
    Running,
}

impl SeedStatus {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Claimed => "CLAIMED",
            Self::Running => "RUNNING",
        }
    }
}

/// Whether a seeded row carries the finalizing marker.
#[derive(Debug, Clone, Copy)]
enum SeedMarker {
    NoMarker,
    Finalizing,
}

/// One seeded task row on the baseline schema. Every row was sent, claimed
/// and started long ago and has no heartbeat.
struct RowSeed<'a> {
    status: SeedStatus,
    owner: &'a str,
    claimed_at: DateTime<Utc>,
    marker: SeedMarker,
}

async fn migrated_pool_with_heartbeat_leaves() -> TestPool {
    let pool = crate::test_support::migrated_pool().await;
    // The heartbeat table has no default partition: the current hour needs
    // its leaf, as at worker start.
    ensure_startup_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        COVERAGE_HORIZON,
        COVERAGE_HORIZON,
        &[],
    )
    .await
    .expect("partition coverage");
    pool
}

/// The database clock, at millisecond precision, minus `minutes`.
async fn claim_time_minutes_ago(pool: &TestPool, minutes: i32) -> DateTime<Utc> {
    sql::query_scalar("SELECT date_trunc('milliseconds', now()) - make_interval(mins => $1)")
        .bind(minutes)
        .fetch_one(pool)
        .await
        .expect("database clock")
}

async fn seed_task(pool: &TestPool, task_id: Uuid, seed: &RowSeed<'_>) {
    let finalizing = matches!(seed.marker, SeedMarker::Finalizing);
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at, command_fingerprint_version,
            command_fingerprint, idempotency_key_digest, status, enqueued_at,
            claimed_at, claimed_by_worker_id, claim_expires_at, started_at,
            finalizing_at, finalizing_by_worker_id, worker_pid, worker_hostname,
            worker_process_name, retry_count, max_retries, next_retry_at, good_until,
            is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id,
            rerun_root_task_id
        ) VALUES (
            $1, 'runner_beat_spec', 'default', 100, '{}', NULL, 'ISOLATED',
            'forever', NOW() - interval '31 minutes', NOW() - interval '31 minutes', 1,
            decode(repeat('00', 32), 'hex'), NULL, $2, NOW() - interval '31 minutes',
            $3, $4, CASE WHEN $2 = 'CLAIMED' THEN NOW() + interval '1 minute' END,
            NOW() - interval '30 minutes',
            CASE WHEN $5 THEN NOW() END, CASE WHEN $5 THEN $4 END, $6, 'beat-host',
            'beat-process', 0, 0, NULL, NULL,
            FALSE, NOW(), FALSE, NULL,
            NULL
        )",
    )
    .bind(task_id)
    .bind(seed.status.as_sql())
    .bind(seed.claimed_at)
    .bind(seed.owner)
    .bind(finalizing)
    .bind(BEAT_PID)
    .execute(pool)
    .await
    .expect("seed task row");
}

/// The runs whose beat landed, of a write whose rows decoded.
fn landed(written: &BeatWrite) -> Vec<RunClaim> {
    match written {
        BeatWrite::Landed(landed) => landed.clone(),
        BeatWrite::LandedNotRead => panic!("the rows of the beat statement did not decode"),
    }
}

fn sender() -> BeatSender {
    BeatSender {
        worker_id: WORKER_ID.to_owned(),
        hostname: "beat-host".to_owned(),
        pid: BEAT_PID,
    }
}

/// The runner beats of one task: sender, role and pid.
async fn beats(pool: &TestPool, task_id: Uuid) -> Vec<(String, String, i32)> {
    sql::query_as(
        "SELECT sender_id, role, pid FROM horsies_heartbeats
         WHERE task_id = $1 ORDER BY sent_at",
    )
    .bind(task_id)
    .fetch_all(pool)
    .await
    .expect("read beats")
}

/// The live status of a task, or the status of its history record.
async fn task_status(pool: &TestPool, task_id: Uuid) -> String {
    sql::query_scalar(
        "SELECT status FROM horsies_tasks WHERE id = $1
         UNION ALL
         SELECT status::text FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("task status")
}

async fn cleanup(pool: &TestPool, task_ids: &[Uuid]) {
    for statement in [
        "DELETE FROM horsies_heartbeats WHERE task_id = ANY($1)",
        "DELETE FROM horsies_task_attempts WHERE task_id = ANY($1)",
        "DELETE FROM horsies_tasks WHERE id = ANY($1)",
        "DELETE FROM horsies_task_history WHERE task_id = ANY($1)",
    ] {
        sql::query(statement)
            .bind(task_ids)
            .execute(pool)
            .await
            .expect("cleanup");
    }
}

/// Python `test_a_running_row_of_this_claim_gets_a_beat`: the row gets one
/// beat that names this worker, the runner role and the worker's pid.
#[tokio::test]
async fn a_running_row_of_this_claim_gets_a_beat() {
    let pool = migrated_pool_with_heartbeat_leaves().await;
    let claimed_at = claim_time_minutes_ago(&pool, 31).await;
    let task_id = Uuid::new_v4();
    seed_task(
        &pool,
        task_id,
        &RowSeed {
            status: SeedStatus::Running,
            owner: WORKER_ID,
            claimed_at,
            marker: SeedMarker::NoMarker,
        },
    )
    .await;

    let rows = write_beats(
        &crate::broker::pools::core_pool_for_test_url(pool.url()),
        &sender(),
        &[task_id],
        &[claimed_at],
    )
    .await
    .expect("beat write");

    assert_eq!(
        landed(&rows),
        vec![RunClaim {
            task_id,
            claimed_at
        }]
    );
    assert_eq!(
        beats(&pool, task_id).await,
        vec![(WORKER_ID.to_owned(), "runner".to_owned(), BEAT_PID)],
    );
    cleanup(&pool, &[task_id]).await;
}

/// Python `test_a_row_outside_the_fence_gets_no_beat` (parametrized): a
/// claimed row that has not started, a row of another worker, a row claimed
/// again since the dispatch, a row whose attempt ended (finalizing marker),
/// and a row back in the queue each get no beat.
#[tokio::test]
async fn a_row_outside_the_fence_gets_no_beat() {
    let pool = migrated_pool_with_heartbeat_leaves().await;
    let dispatch_generation = claim_time_minutes_ago(&pool, 31).await;
    let claimed_again = dispatch_generation + chrono::Duration::seconds(5);
    let cases: [(&str, RowSeed<'_>); 5] = [
        (
            "claimed, not started",
            RowSeed {
                status: SeedStatus::Claimed,
                owner: WORKER_ID,
                claimed_at: dispatch_generation,
                marker: SeedMarker::NoMarker,
            },
        ),
        (
            "owned by another worker",
            RowSeed {
                status: SeedStatus::Running,
                owner: OTHER_WORKER_ID,
                claimed_at: dispatch_generation,
                marker: SeedMarker::NoMarker,
            },
        ),
        (
            "claimed again since",
            RowSeed {
                status: SeedStatus::Running,
                owner: WORKER_ID,
                claimed_at: claimed_again,
                marker: SeedMarker::NoMarker,
            },
        ),
        (
            "the attempt ended",
            RowSeed {
                status: SeedStatus::Running,
                owner: WORKER_ID,
                claimed_at: dispatch_generation,
                marker: SeedMarker::Finalizing,
            },
        ),
        (
            "back in the queue",
            RowSeed {
                status: SeedStatus::Pending,
                owner: WORKER_ID,
                claimed_at: dispatch_generation,
                marker: SeedMarker::NoMarker,
            },
        ),
    ];

    for (case, seed) in &cases {
        let task_id = Uuid::new_v4();
        seed_task(&pool, task_id, seed).await;

        let rows = write_beats(
            &crate::broker::pools::core_pool_for_test_url(pool.url()),
            &sender(),
            &[task_id],
            &[dispatch_generation],
        )
        .await
        .expect("beat write");

        assert_eq!(landed(&rows), Vec::new(), "{case}");
        assert_eq!(beats(&pool, task_id).await, Vec::new(), "{case}");
        cleanup(&pool, &[task_id]).await;
    }
}

/// Python `test_a_null_generation_gets_no_beat`: the fence compares the
/// claim generation with no NULL branch, so a dispatch row without one
/// matches no task, also a task of this worker. [`LiveDispatches`] cannot
/// hold such a row, so the statement runs with hand-built parameters.
#[tokio::test]
async fn a_null_generation_gets_no_beat() {
    let pool = migrated_pool_with_heartbeat_leaves().await;
    let claimed_at = claim_time_minutes_ago(&pool, 31).await;
    let ours = Uuid::new_v4();
    let theirs = Uuid::new_v4();
    for (task_id, owner) in [(ours, WORKER_ID), (theirs, OTHER_WORKER_ID)] {
        seed_task(
            &pool,
            task_id,
            &RowSeed {
                status: SeedStatus::Running,
                owner,
                claimed_at,
                marker: SeedMarker::NoMarker,
            },
        )
        .await;
    }
    let no_generations: Vec<Option<DateTime<Utc>>> = vec![None, None];

    let written = sql::query(INSERT_RUNNER_HEARTBEATS_SQL)
        .bind(WORKER_ID)
        .bind("beat-host")
        .bind(BEAT_PID)
        .bind(vec![ours, theirs])
        .bind(no_generations)
        .execute(&pool)
        .await
        .expect("beat statement")
        .rows_affected();

    assert_eq!(written, 0);
    assert_eq!(beats(&pool, ours).await, Vec::new());
    assert_eq!(beats(&pool, theirs).await, Vec::new());
    cleanup(&pool, &[ours, theirs]).await;
}

/// Python `test_one_statement_beats_every_fenced_row`: one write beats three
/// held rows, each at its own claim generation.
#[tokio::test]
async fn one_statement_beats_every_fenced_row() {
    let pool = migrated_pool_with_heartbeat_leaves().await;
    let newest = claim_time_minutes_ago(&pool, 31).await;
    let generations: Vec<DateTime<Utc>> = (0..3_i64)
        .map(|index| newest - chrono::Duration::seconds(index))
        .collect();
    let task_ids: Vec<Uuid> = generations.iter().map(|_| Uuid::new_v4()).collect();
    for (task_id, claimed_at) in task_ids.iter().zip(&generations) {
        seed_task(
            &pool,
            *task_id,
            &RowSeed {
                status: SeedStatus::Running,
                owner: WORKER_ID,
                claimed_at: *claimed_at,
                marker: SeedMarker::NoMarker,
            },
        )
        .await;
    }

    let rows = write_beats(
        &crate::broker::pools::core_pool_for_test_url(pool.url()),
        &sender(),
        &task_ids,
        &generations,
    )
    .await
    .expect("beat write");

    assert_eq!(landed(&rows).len(), 3);
    for task_id in &task_ids {
        assert_eq!(beats(&pool, *task_id).await.len(), 1, "{task_id}");
    }
    cleanup(&pool, &task_ids).await;
}

/// Python `test_the_reaper_leaves_a_beaten_row_and_fails_an_unbeaten_one`:
/// entered at the reaper, only the newest runner beat decides staleness.
/// Both rows started 30 minutes ago; one gets a beat from the writer.
#[tokio::test]
async fn the_reaper_leaves_a_beaten_row_and_fails_an_unbeaten_one() {
    let pool = migrated_pool_with_heartbeat_leaves().await;
    let beaten_claim = claim_time_minutes_ago(&pool, 31).await;
    let unbeaten_claim = claim_time_minutes_ago(&pool, 32).await;
    let beaten = Uuid::new_v4();
    let unbeaten = Uuid::new_v4();
    for (task_id, claimed_at) in [(beaten, beaten_claim), (unbeaten, unbeaten_claim)] {
        seed_task(
            &pool,
            task_id,
            &RowSeed {
                status: SeedStatus::Running,
                owner: WORKER_ID,
                claimed_at,
                marker: SeedMarker::NoMarker,
            },
        )
        .await;
    }
    let rows = write_beats(
        &crate::broker::pools::core_pool_for_test_url(pool.url()),
        &sender(),
        &[beaten],
        &[beaten_claim],
    )
    .await
    .expect("beat write");
    assert_eq!(landed(&rows).len(), 1);

    let _processed = mark_stale_running_as_failed(
        &pool,
        STALE_THRESHOLD_SECONDS,
        FINALIZING_THRESHOLD_SECONDS,
        STALE_SCAN_LIMIT,
    )
    .await
    .expect("stale recovery");

    assert_eq!(task_status(&pool, beaten).await, "RUNNING");
    assert_eq!(task_status(&pool, unbeaten).await, "FAILED");
    cleanup(&pool, &[beaten, unbeaten]).await;
}

/// The parent half of Python
/// `test_child_isolation.py::test_a_task_that_holds_the_gil_is_not_failed_as_stale`:
/// the worker parent's beat loop, not the task, keeps a held row beaten. A
/// row the worker holds gets beats while nothing else writes to it, so the
/// reaper leaves it RUNNING; a row of the same age that the worker does not
/// hold fails. The child half (a task that holds its interpreter lock)
/// waits for the executor.
#[tokio::test]
async fn the_beat_loop_keeps_a_held_row_out_of_stale_recovery() {
    let pool = migrated_pool_with_heartbeat_leaves().await;
    let held_claim = claim_time_minutes_ago(&pool, 31).await;
    let silent_claim = claim_time_minutes_ago(&pool, 32).await;
    let held = Uuid::new_v4();
    let silent = Uuid::new_v4();
    for (task_id, claimed_at) in [(held, held_claim), (silent, silent_claim)] {
        seed_task(
            &pool,
            task_id,
            &RowSeed {
                status: SeedStatus::Running,
                owner: WORKER_ID,
                claimed_at,
                marker: SeedMarker::NoMarker,
            },
        )
        .await;
    }
    let spawner = Spawner::start().expect("the spawner starts");
    let (spawned, child) = sleeping_child(&spawner).await;
    let live = LiveDispatches::default();
    let dispatch = live
        .hold(held, held_claim, AttemptLog::FIRST_UNTRACED, child)
        .expect("an open registry holds the dispatch");
    let cancel = CancellationToken::new();
    let (watch, writer) = launch_runner_beats(
        test_beat_pool(pool.url()),
        sender(),
        LOOP_BEAT_INTERVAL,
        StaleRunningRecovery::Off,
        live.clone(),
        cancel.clone(),
        FailClosedStop::new(CancellationToken::new()),
    )
    .expect("start the beat loop");

    let first_beat = tokio::time::timeout(FIRST_BEAT_BOUND, async {
        while beats(&pool, held).await.is_empty() {
            tokio::time::sleep(BEAT_POLL_INTERVAL).await;
        }
    })
    .await;
    let _processed = mark_stale_running_as_failed(
        &pool,
        LOOP_STALE_THRESHOLD_SECONDS,
        FINALIZING_THRESHOLD_SECONDS,
        STALE_SCAN_LIMIT,
    )
    .await
    .expect("stale recovery");
    let held_status = task_status(&pool, held).await;
    let silent_status = task_status(&pool, silent).await;

    cancel.cancel();
    writer.await.expect("the beat loop ends");
    watch.stop();
    drop(dispatch);
    end_child(spawner, spawned).await;

    first_beat.expect("the beat loop beats a held row within the bound");
    assert_eq!(held_status, "RUNNING");
    assert_eq!(silent_status, "FAILED");
    assert_eq!(beats(&pool, silent).await, Vec::new());
    cleanup(&pool, &[held, silent]).await;
}

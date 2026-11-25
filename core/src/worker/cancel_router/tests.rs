//! The cancel router: the notice parser, the route of each notice, and the
//! reads after a listener gap, a payload that does not parse, and a beat
//! miss.

use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    on_step, parse_cancel_notice, read_and_route, route_cancel, CancelledRunsReader, NoticeRefused,
    Routed, RouterStep, StepEnd,
};
use crate::broker::cancelled_runs::{read_cancelled_runs, CancelledRunsReadError, RunClaim};
use crate::broker::core_rows::StatementLog;
use crate::db::{CorePool, PayloadEvent};
use crate::worker::executor::slots::dispatch::AttemptLog;
use crate::worker::executor::supervisor::Spawner;
use crate::worker::runner_beats::{
    end_child, launch_runner_beats, sleeping_child, test_beat_pool, BeatSender, CancelRouting,
    CancelTake, FailClosedStop, HeldDispatch, LiveDispatches, StaleRunningRecovery,
};

/// The longest wait for a bell or a beat miss that must come.
const EVENT_BOUND: Duration = Duration::from_secs(15);

fn generation() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 24, 10, 0, 0).unwrap() + chrono::Duration::microseconds(123_456)
}

fn run(task_id: Uuid, claimed_at: DateTime<Utc>) -> RunClaim {
    RunClaim {
        task_id,
        claimed_at,
    }
}

/// The payload that `horsies_cancel_task` sends for `run`.
fn payload_of(run: RunClaim) -> String {
    format!("{} {}", run.task_id, run.claimed_at.timestamp_micros())
}

/// A reader that answers every read with the runs of `cancelled` that the
/// read asks for, and records each read.
#[derive(Default)]
struct ScriptedReader {
    cancelled: Vec<RunClaim>,
    reads: Mutex<Vec<Vec<RunClaim>>>,
}

impl ScriptedReader {
    fn cancelled(runs: &[RunClaim]) -> Self {
        Self {
            cancelled: runs.to_vec(),
            reads: Mutex::new(Vec::new()),
        }
    }

    fn reads(&self) -> Vec<Vec<RunClaim>> {
        self.reads.lock().unwrap().clone()
    }
}

impl CancelledRunsReader for ScriptedReader {
    async fn read_cancelled(
        &self,
        runs: &[RunClaim],
    ) -> Result<Vec<RunClaim>, CancelledRunsReadError> {
        self.reads.lock().unwrap().push(runs.to_vec());
        Ok(self
            .cancelled
            .iter()
            .filter(|cancelled| runs.contains(*cancelled))
            .copied()
            .collect())
    }
}

/// A reader on a test database: the read of the worker.
struct PoolReader(CorePool);

impl CancelledRunsReader for PoolReader {
    async fn read_cancelled(
        &self,
        runs: &[RunClaim],
    ) -> Result<Vec<RunClaim>, CancelledRunsReadError> {
        read_cancelled_runs(&self.0, StatementLog::Off, runs).await
    }
}

/// A registry that holds `runs`, each on one sleeping child, with the
/// guards and the spawner to end it.
struct Held {
    live: LiveDispatches,
    guards: Vec<HeldDispatch>,
    spawner: Spawner,
    spawned: crate::worker::executor::supervisor::SpawnedChild,
    bell: crate::worker::runner_beats::CancelBell,
}

async fn hold(runs: &[RunClaim]) -> Held {
    let spawner = Spawner::start().unwrap();
    let (spawned, child) = sleeping_child(&spawner).await;
    let bell = child.cancel_bell.clone();
    let live = LiveDispatches::default();
    let guards = runs
        .iter()
        .map(|run| {
            live.hold(
                run.task_id,
                run.claimed_at,
                AttemptLog::FIRST_UNTRACED,
                child.clone(),
            )
            .unwrap()
        })
        .collect();
    Held {
        live,
        guards,
        spawner,
        spawned,
        bell,
    }
}

impl Held {
    async fn end(self) {
        drop(self.guards);
        end_child(self.spawner, self.spawned).await;
    }
}

// ---------------------------------------------------------------------------
// The notice parser
// ---------------------------------------------------------------------------

#[test]
fn a_notice_names_the_task_and_its_claim_generation_to_the_microsecond() {
    let expected = run(Uuid::new_v4(), generation());
    assert_eq!(parse_cancel_notice(&payload_of(expected)), Ok(expected));
}

#[test]
fn a_notice_that_is_not_two_fields_is_refused() {
    let task_id = Uuid::new_v4();
    for payload in [
        String::new(),
        task_id.to_string(),
        format!("{task_id} 1 2"),
        format!("{task_id}  1"),
    ] {
        assert!(
            matches!(
                parse_cancel_notice(&payload),
                Err(NoticeRefused::NotTwoFields { .. })
            ),
            "{payload:?}"
        );
    }
}

#[test]
fn a_notice_with_a_bad_field_names_the_field() {
    let task_id = Uuid::new_v4();
    assert!(matches!(
        parse_cancel_notice("not-a-uuid 1"),
        Err(NoticeRefused::TaskIdInvalid { .. })
    ));
    assert!(matches!(
        parse_cancel_notice(&format!("{task_id} 1.5")),
        Err(NoticeRefused::ClaimGenerationInvalid { .. })
    ));
    assert!(matches!(
        parse_cancel_notice(&format!("{task_id} {}", i64::MAX)),
        Err(NoticeRefused::ClaimGenerationOutOfRange { .. })
    ));
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

/// A notice for a held run is routed once: the bell of its slot rings, the
/// slot takes the cancel once, and the beats leave the run out. A second
/// notice is `AlreadyRequested`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_notice_for_a_held_run_is_routed_once() {
    let target = run(Uuid::new_v4(), generation());
    let held = hold(&[target]).await;

    let first = route_cancel(&held.live, target);
    let second = route_cancel(&held.live, target);

    assert_eq!(
        first.routing,
        CancelRouting::Routed {
            log: AttemptLog::FIRST_UNTRACED,
        }
    );
    assert_eq!(
        second.routing,
        CancelRouting::AlreadyRequested {
            log: AttemptLog::FIRST_UNTRACED,
        }
    );
    tokio::time::timeout(EVENT_BOUND, held.bell.rung())
        .await
        .expect("the bell of the slot rings");
    assert_eq!(held.guards[0].take_cancel(), CancelTake::Requested);
    assert_eq!(held.guards[0].take_cancel(), CancelTake::NoRequest);
    assert_eq!(held.live.held_task_ids(), Vec::<Uuid>::new());
    assert_eq!(held.live.cancel_candidates(), Vec::new());
    held.end().await;
}

/// A notice for a task this worker does not hold, or holds at another claim
/// generation, is dropped: the held run keeps its beats.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_notice_for_a_run_that_is_not_held_is_dropped() {
    let target = run(Uuid::new_v4(), generation());
    let held = hold(&[target]).await;

    let other_task = route_cancel(&held.live, run(Uuid::new_v4(), generation()));
    let other_generation = route_cancel(
        &held.live,
        run(
            target.task_id,
            generation() + chrono::Duration::microseconds(1),
        ),
    );

    assert_eq!(other_task.routing, CancelRouting::NotHeld);
    assert_eq!(other_generation.routing, CancelRouting::NotHeld);
    assert_eq!(held.guards[0].take_cancel(), CancelTake::NoRequest);
    assert_eq!(held.live.held_task_ids(), vec![target.task_id]);
    held.end().await;
}

/// A payload that does not parse reads the cancels of every held run with
/// no cancel, and routes what the read finds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_payload_that_does_not_parse_reads_the_held_runs() {
    let cancelled = run(Uuid::new_v4(), generation());
    let running = run(Uuid::new_v4(), generation());
    let held = hold(&[cancelled, running]).await;
    let reader = ScriptedReader::cancelled(&[cancelled]);

    let (end, routed) = on_step(
        RouterStep::Notice(PayloadEvent::Payload("garbled".to_owned())),
        &held.live,
        &reader,
    )
    .await;

    assert_eq!(end, StepEnd::Continue);
    assert_eq!(reader.reads(), vec![vec![cancelled, running]]);
    assert_eq!(
        routed,
        vec![Routed {
            run: cancelled,
            routing: CancelRouting::Routed {
                log: AttemptLog::FIRST_UNTRACED,
            },
        }]
    );
    assert_eq!(held.live.held_task_ids(), vec![running.task_id]);
    held.end().await;
}

/// A listener gap and an overflow each read the cancels of the held runs:
/// a notice that the listener lost is routed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_listener_gap_or_overflow_finds_a_lost_notice() {
    for event in [PayloadEvent::Gap, PayloadEvent::Overflowed] {
        let lost = run(Uuid::new_v4(), generation());
        let held = hold(&[lost]).await;
        let reader = ScriptedReader::cancelled(&[lost]);

        let (end, routed) = on_step(RouterStep::Notice(event), &held.live, &reader).await;

        assert_eq!(end, StepEnd::Continue);
        assert_eq!(
            routed,
            vec![Routed {
                run: lost,
                routing: CancelRouting::Routed {
                    log: AttemptLog::FIRST_UNTRACED,
                },
            }]
        );
        assert_eq!(held.guards[0].take_cancel(), CancelTake::Requested);
        held.end().await;
    }
}

/// A notice that comes after a read routed the same run is
/// `AlreadyRequested`; a run already cancelled is not read again, and a
/// read with no run left sends no statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_routed_run_is_not_read_again_and_its_late_notice_changes_nothing() {
    let lost = run(Uuid::new_v4(), generation());
    let held = hold(&[lost]).await;
    let reader = ScriptedReader::cancelled(&[lost]);

    let _routed = on_step(RouterStep::Notice(PayloadEvent::Gap), &held.live, &reader).await;
    let (_, late) = on_step(
        RouterStep::Notice(PayloadEvent::Payload(payload_of(lost))),
        &held.live,
        &reader,
    )
    .await;
    let (_, second_gap) = on_step(RouterStep::Notice(PayloadEvent::Gap), &held.live, &reader).await;

    assert_eq!(
        late[0].routing,
        CancelRouting::AlreadyRequested {
            log: AttemptLog::FIRST_UNTRACED,
        }
    );
    assert_eq!(second_gap, Vec::new());
    assert_eq!(reader.reads(), vec![vec![lost]]);
    held.end().await;
}

#[tokio::test]
async fn a_stopped_listener_ends_the_router() {
    let live = LiveDispatches::default();
    let reader = ScriptedReader::default();
    let (end, routed) = on_step(RouterStep::Notice(PayloadEvent::Stopped), &live, &reader).await;
    assert_eq!(end, StepEnd::ListenerStopped);
    assert_eq!(routed, Vec::new());
    assert_eq!(reader.reads(), Vec::<Vec<RunClaim>>::new());
}

/// The beat misses of a step are read and routed; an empty read routes
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn beat_misses_are_read_and_routed() {
    let cancelled = run(Uuid::new_v4(), generation());
    let finalizing = run(Uuid::new_v4(), generation());
    let held = hold(&[cancelled, finalizing]).await;
    let reader = ScriptedReader::cancelled(&[cancelled]);

    let routed = read_and_route(&held.live, &reader, &[cancelled, finalizing]).await;

    assert_eq!(
        routed,
        vec![Routed {
            run: cancelled,
            routing: CancelRouting::Routed {
                log: AttemptLog::FIRST_UNTRACED,
            },
        }]
    );
    assert_eq!(held.guards[1].take_cancel(), CancelTake::NoRequest);
    held.end().await;
}

// ---------------------------------------------------------------------------
// On a database
// ---------------------------------------------------------------------------

const WORKER_ID: &str = "cancel-router-worker";

/// The heartbeat and history leaves that the worker start creates.
const COVERAGE_HORIZON: u32 = 2;

async fn migrated_pool() -> crate::test_support::sql::TestPool {
    let pool = crate::test_support::migrated_pool().await;
    crate::core::history::maintenance::coverage::ensure_startup_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(pool.url()),
        StatementLog::Off,
        COVERAGE_HORIZON,
        COVERAGE_HORIZON,
        &[],
    )
    .await
    .expect("partition coverage");
    pool
}

/// One RUNNING plain task of [`WORKER_ID`], started 30 minutes ago, with
/// the finalizing marker when `finalizing`; returns its run.
async fn seed_running(pool: &crate::test_support::sql::TestPool, finalizing: bool) -> RunClaim {
    use crate::test_support::sql;
    let task_id = Uuid::new_v4();
    let claimed_at: DateTime<Utc> =
        sql::query_scalar("SELECT date_trunc('milliseconds', now()) - interval '31 minutes'")
            .fetch_one(pool)
            .await
            .expect("database clock");
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
            $1, 'cancel_router_spec', 'default', 100, '{}', NULL, 'ISOLATED',
            'forever', NOW() - interval '31 minutes', NOW() - interval '31 minutes', 1,
            decode(repeat('00', 32), 'hex'), NULL, 'RUNNING', NOW() - interval '31 minutes',
            $2, $3, NULL, NOW() - interval '30 minutes',
            CASE WHEN $4 THEN NOW() END, CASE WHEN $4 THEN $3 END, 4242, 'router-host',
            'router-process', 0, 0, NULL, NULL,
            FALSE, NOW(), FALSE, NULL,
            NULL
        )",
    )
    .bind(task_id)
    .bind(claimed_at)
    .bind(WORKER_ID)
    .bind(finalizing)
    .execute(pool)
    .await
    .expect("seed a running task");
    run(task_id, claimed_at)
}

/// The operator cancel of a RUNNING task: `horsies_cancel_task` with
/// `include_running`.
async fn cancel_running(pool: &crate::test_support::sql::TestPool, task_id: Uuid) {
    let outcome: String =
        crate::test_support::sql::query_scalar("SELECT outcome FROM horsies_cancel_task($1, TRUE)")
            .bind(task_id)
            .fetch_one(pool)
            .await
            .expect("cancel the running task");
    assert_eq!(outcome, "APPLIED");
}

async fn cleanup(pool: &crate::test_support::sql::TestPool, task_ids: &[Uuid]) {
    for statement in [
        "DELETE FROM horsies_heartbeats WHERE task_id = ANY($1)",
        "DELETE FROM horsies_task_attempts WHERE task_id = ANY($1)",
        "DELETE FROM horsies_tasks WHERE id = ANY($1)",
        "DELETE FROM horsies_task_history WHERE task_id = ANY($1)",
    ] {
        crate::test_support::sql::query(statement)
            .bind(task_ids)
            .execute(pool)
            .await
            .expect("cleanup");
    }
}

/// The read finds a run cancelled at its claim generation, and not a run
/// that still runs, nor the same task at another claim generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_read_finds_a_cancelled_run_at_its_claim_generation_only() {
    let pool = migrated_pool().await;
    let cancelled = seed_running(&pool, false).await;
    let running = seed_running(&pool, false).await;
    cancel_running(&pool, cancelled.task_id).await;
    let reader = PoolReader(crate::broker::pools::core_pool_for_test_url(pool.url()));
    let other_generation = run(
        cancelled.task_id,
        cancelled.claimed_at - chrono::Duration::seconds(1),
    );

    let found = reader
        .read_cancelled(&[cancelled, running, other_generation])
        .await
        .expect("the read runs");

    assert_eq!(found, vec![cancelled]);
    cleanup(&pool, &[cancelled.task_id, running.task_id]).await;
}

/// The cancel notice was lost and the listener stays down: the beat of the
/// cancelled run does not land, and the router reads the miss and routes
/// the cancel within one beat interval. A finalizing row also misses its
/// beat; the read finds no cancel for it, and it is not routed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_notice_is_found_by_the_beat_miss_read() {
    let pool = migrated_pool().await;
    let cancelled = seed_running(&pool, false).await;
    let finalizing = seed_running(&pool, true).await;
    cancel_running(&pool, cancelled.task_id).await;
    let held = hold(&[cancelled, finalizing]).await;
    let beats_cancel = CancellationToken::new();
    let (watch, writer) = launch_runner_beats(
        test_beat_pool(pool.url()),
        BeatSender {
            worker_id: WORKER_ID.to_owned(),
            hostname: "router-host".to_owned(),
            pid: 4242,
        },
        Duration::from_secs(1),
        StaleRunningRecovery::Off,
        held.live.clone(),
        beats_cancel.clone(),
        FailClosedStop::new(CancellationToken::new()),
        crate::process_exit::ProcessExit::Standard,
    )
    .expect("start the beat loop");
    let reader = PoolReader(crate::broker::pools::core_pool_for_test_url(pool.url()));

    let misses = tokio::time::timeout(EVENT_BOUND, held.live.next_beat_misses())
        .await
        .expect("the first beat pass notes its misses");
    let routed = read_and_route(&held.live, &reader, &misses).await;

    let mut missed = misses.clone();
    missed.sort_by_key(|run| run.task_id);
    let mut expected = vec![cancelled, finalizing];
    expected.sort_by_key(|run| run.task_id);
    assert_eq!(missed, expected);
    assert_eq!(
        routed,
        vec![Routed {
            run: cancelled,
            routing: CancelRouting::Routed {
                log: AttemptLog::FIRST_UNTRACED,
            },
        }]
    );
    assert_eq!(held.guards[0].take_cancel(), CancelTake::Requested);
    assert_eq!(held.guards[1].take_cancel(), CancelTake::NoRequest);
    beats_cancel.cancel();
    let _ = writer.await;
    tokio::task::spawn_blocking(move || watch.stop())
        .await
        .expect("the watchdog stops");
    held.end().await;
    cleanup(&pool, &[cancelled.task_id, finalizing.task_id]).await;
}

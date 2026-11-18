//! Ports of Python unit tests of the worker parent: a lost child whose exit
//! was or was not seen (`tests/unit/test_child_loss_exit_unknown.py`), the
//! retry of a run that timed out (`test_shared_stop.py`), a start hook that
//! fails in a replacement child (`test_child_process_start_hooks.py`), and
//! the drain of the pool at the stop (`test_worker_stop_ordering.py`).

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use super::scripted::{
    answer_of, boot, ended, exit_at_stop, input, isolated_config, isolated_init, reported,
    result_frame, run_frame, send, serving, shell, start_isolated, submit_isolated, take,
    take_init, task, until, TestDir, SPAWNER_STOP_WAIT, STATE_WAIT, WORKER_ID,
};
use super::seat_runs::{LossSight, RunStopReport, RunWork, SeatRun, SeatRuns};
use super::*;
use crate::broker::attempt_writes::RetryPlacement;
use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::test_support::{CapturedEvent, LogCapture};
use crate::worker::executor::protocol::{
    CancelGraceMs, ChildAfterFrame, DispatchNumber, FailureDetail, IsolatedWork, ProcessId,
    RssReading, RunRef, SeatEnd, SharedResultFrame, TaskTimeoutMs,
};
use crate::worker::executor::supervisor::{ChildExit, ChildLost, LossCause, PartialFrame, Spawner};

// ---------------------------------------------------------------------------
// The retry of a run that timed out
// ---------------------------------------------------------------------------

/// Python `test_shared_stop.py::test_a_run_that_ends_after_it_was_stuck_retries_isolated`,
/// the `NOT_STUCK` case: a run that timed out without a stuck report
/// retries in its class. The stuck case is
/// `shared_rules_tests::an_async_run_reports_its_stop_in_order`.
#[test]
fn a_timed_out_run_that_was_not_stuck_retries_in_its_class() {
    let timeout = TaskTimeoutMs::new(1_000).unwrap();
    let work = RunWork::AsyncTask {
        timeout,
        grace: CancelGraceMs::new(500).unwrap(),
    };
    let run = RunRef {
        dispatch: DispatchNumber::new(1).unwrap(),
        task_id: Uuid::from_u128(1),
    };
    for (report, cancel_end) in [
        (RunStopReport::NotReported, CancelEnd::ReturnedAfterSignal),
        (
            RunStopReport::DeadlineReported,
            CancelEnd::ReturnedAfterSignal,
        ),
        (
            RunStopReport::SecondCancelReported,
            CancelEnd::DroppedAfterGrace,
        ),
    ] {
        let mut runs = SeatRuns::new(NonZeroUsize::MIN);
        runs.add(SeatRun {
            run,
            work,
            sent_at: tokio::time::Instant::now(),
            report: report.clone(),
            attached: (),
        });
        let (_, end) = runs
            .end_run(SharedResultFrame {
                run,
                end: SeatEnd::TimedOut,
                rss: RssReading::Unreadable,
                child: ChildAfterFrame::ServesOn,
            })
            .unwrap();
        assert_eq!(
            end,
            AttemptEnd::TimedOut {
                timeout,
                retry: RetryPlacement::SameClass,
                cancel: TimeoutCancel::Ended(cancel_end),
            },
            "{report:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// A loss whose exit was not seen
// ---------------------------------------------------------------------------

/// Python `test_child_loss_exit_unknown.py::TestLossWithNoExitSeen`: a lost
/// channel, a bad frame and a cleared slot, each with no exit seen, give
/// no verdict; the attempt may still run.
#[test]
fn a_loss_whose_exit_was_not_seen_gives_no_verdict_for_any_cause() {
    for cause in [
        LossCause::ChannelLost,
        LossCause::ProtocolViolation,
        LossCause::SlotDefect,
        LossCause::KilledByParent,
    ] {
        let lost = ChildLost {
            cause,
            exit: ChildExit::ExitUnknown,
            partial: PartialFrame::NoneAtHand,
            pid: ProcessId::new(4242).unwrap(),
        };
        assert!(
            matches!(LossSight::of(lost), LossSight::NotSeen(_)),
            "{cause}"
        );
    }
}

/// No server listens on port 1: a write that reached the pool would fail
/// after the acquire timeout and settle as a failed write, not as left to
/// recovery.
fn unreachable_writer() -> OutcomeWriter {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(1))
        .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies")
        .expect("a lazy pool opens no connection");
    let broker = Arc::new(PostgresBroker::from_pool(pool, crate::ClientFamily::Rust));
    let successes = super::outcome::test_successes(&broker, WORKER_ID);
    OutcomeWriter::new(
        broker,
        crate::test_support::child_specs::no_child_specs(),
        WORKER_ID.to_owned(),
        PayloadPolicy::default(),
        successes,
    )
}

/// Python `test_child_loss_exit_unknown.py::TestLossWithNoExitSeen` (the
/// finalizer half), and `test_task_timeout_order.py::TestNoExitSeen::test_no_retry_and_no_phase_2_without_the_exit`
/// and `TestSlotAnsweredAfterTheDeadline::test_a_late_loss_with_no_exit_seen_writes_nothing`
/// (the writer half, with the log line): crash recovery writes nothing, and
/// the row is left to stale recovery. One line names the task and the pid
/// of its child (Python logs it at `CRITICAL`; the core at warning).
#[tokio::test]
async fn an_end_whose_exit_was_not_seen_is_left_to_recovery_with_no_write() {
    let writer = unreachable_writer();
    let claimed = task(1, ExecutionClass::Isolated);
    let capture = LogCapture::new();
    let _default = capture.set_default();
    let settled = tokio::time::timeout(
        Duration::from_millis(500),
        writer.settle(
            &claimed,
            AttemptEnd::EndUnknown {
                pid: ProcessId::new(4242).unwrap(),
            },
        ),
    )
    .await
    .expect("no statement is sent, so the settle does not wait for the pool");
    assert_eq!(settled, Settlement::LeftToRecovery);
    let task_id = claimed.task_id.to_string();
    let lines: Vec<CapturedEvent> = capture.lines_starting(
        tracing::Level::WARN,
        "the exit of the task's child was not seen",
    );
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(lines[0].field("task_id"), Some(task_id.as_str()));
    assert_eq!(lines[0].field("pid"), Some("4242"));
}

// ---------------------------------------------------------------------------
// A loss whose exit was seen
// ---------------------------------------------------------------------------

/// A RUNNING plain task held by [`WORKER_ID`] on queue `queue`. Returns the
/// id and the claim generation.
async fn seed_running(pool: &PgPool, queue: &str) -> (Uuid, DateTime<Utc>) {
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
            $1, 'app.tasks.scripted', $2, 100, '{}', NULL, 'ISOLATED',
            'forever', date_trunc('milliseconds', NOW()) - interval '31 minutes',
            NOW() - interval '31 minutes',
            1, decode(repeat('05', 32), 'hex'), NULL,
            'RUNNING', NOW() - interval '31 minutes',
            date_trunc('milliseconds', NOW()) - interval '30 minutes', $3, NULL,
            NOW() - interval '30 minutes', NULL, NULL,
            4242, 'slots-host', 'slots-process',
            0, 0, NULL, NULL, FALSE,
            NOW(), FALSE, NULL, NULL
         ) RETURNING claimed_at",
    )
    .bind(id)
    .bind(queue)
    .bind(WORKER_ID)
    .fetch_one(pool)
    .await
    .expect("seed a RUNNING task");
    (id, generation)
}

async fn history_row(pool: &PgPool, task_id: Uuid) -> serde_json::Value {
    sqlx::query_scalar("SELECT to_jsonb(h) FROM horsies_task_history h WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read the history row")
}

async fn remove_task(pool: &PgPool, task_id: Uuid) {
    for statement in [
        "DELETE FROM horsies_task_attempts WHERE task_id = $1",
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

/// Python `test_child_loss_exit_unknown.py::TestLossWithTheExitSeen::test_crash_recovery_writes_the_row`
/// and `tests/integration/test_worker_finalize_after.py::test_child_lost_marks_non_retryable_running_task_failed`:
/// the crash path of a loss whose exit was seen fails the row with
/// `WORKER_CRASHED`. The live row is gone, and the history row keeps the
/// claimant and the process that ran the task as provenance.
#[tokio::test]
#[serial]
async fn a_crash_whose_exit_was_seen_fails_the_row_with_worker_crashed() {
    let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
    let queue = format!("slots_{}", &Uuid::new_v4().simple().to_string()[..8]);
    let (task_id, generation) = seed_running(&pool, &queue).await;
    let broker = Arc::new(PostgresBroker::from_pool(
        pool.clone(),
        crate::ClientFamily::Rust,
    ));
    let successes = super::outcome::test_successes(&broker, WORKER_ID);
    let writer = OutcomeWriter::new(
        broker,
        crate::test_support::child_specs::no_child_specs(),
        WORKER_ID.to_owned(),
        PayloadPolicy::default(),
        successes,
    );
    let claimed = ClaimedTask {
        task_id,
        queue_name: queue,
        claimed_at: generation,
        ..task(1, ExecutionClass::Isolated)
    };

    let settled = writer
        .settle(
            &claimed,
            AttemptEnd::Crashed {
                reason: FailureDetail::cut("child died: signal 9 (SIGKILL)"),
            },
        )
        .await;

    assert_eq!(settled, Settlement::Written);
    let live: Option<i64> = sqlx::query_scalar("SELECT 1::bigint FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_optional(&pool)
        .await
        .expect("read the live row");
    assert_eq!(live, None);
    let history = history_row(&pool, task_id).await;
    assert_eq!(history["status"], "FAILED");
    assert_eq!(history["error_code"], "WORKER_CRASHED");
    assert_eq!(history["last_claimed_worker_id"], WORKER_ID);
    assert!(!history["started_at"].is_null(), "{history}");
    assert_eq!(history["last_worker_hostname"], "slots-host");
    assert_eq!(history["last_worker_pid"], 4242);
    assert_eq!(history["last_worker_process_name"], "slots-process");
    remove_task(&pool, task_id).await;
}

// ---------------------------------------------------------------------------
// A start hook that fails in a replacement child
// ---------------------------------------------------------------------------

/// Python `test_child_process_start_hooks.py::TestRunChildStartHooks::test_mid_run_hook_failure_stops_worker_instead_of_restart_looping`
/// (the pool half; the worker stop is the worker loop's): the first child
/// serves one task and retires; its replacement exits with the hook code.
/// The pool fails with `ChildHookFailed`, boots no further child, and
/// refuses the next task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_start_hook_of_a_replacement_fails_the_pool_with_no_restart_loop() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let init = isolated_init();
    let script = format!(
        r#"echo boot >> "{boots}"; if mkdir "{marker}" 2>/dev/null; then {boot}{take_run}{result}{stop}; else {take_init}exit 173; fi"#,
        boots = dir.file("boots"),
        marker = dir.file("first"),
        boot = boot(&init),
        take_run = take(&run_frame(&first)),
        result = send(&[result_frame(&first, 1)]),
        stop = exit_at_stop(),
        take_init = take_init(),
    );
    let spawner = Spawner::start().unwrap();
    let config = IsolatedPoolConfig {
        tasks_per_child: TaskLimit::After(NonZeroU64::MIN),
        ..isolated_config(shell(&script))
    };
    let (pool, board) = start_isolated(&spawner, 1, config);

    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;

    assert_eq!(answer, reported(1));
    until("the pool failed", STATE_WAIT, || {
        board.health() == PoolHealth::Failed(PoolFailure::ChildHookFailed)
    })
    .await;
    // A restart loop would boot more children while the test waits.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(dir.lines("boots").len(), 2);
    let next = task(2, ExecutionClass::Isolated);
    assert!(matches!(
        pool.submit(
            &next,
            input(),
            IsolatedWork::Run,
            IsolatedDeadline::NoDeadline
        ),
        Submitted::NotDispatched(NotRunCause::PoolFailed)
    ));
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

// ---------------------------------------------------------------------------
// The drain at the stop
// ---------------------------------------------------------------------------

/// Python `test_worker_stop_ordering.py::test_stop_persists_results_of_late_finishing_tasks`
/// (the pool half; the order of the outcome writes after the drain is the
/// worker loop's): a task that ends during the drain gets its result, and
/// the stop reports a drained pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_that_ends_during_the_drain_keeps_its_result() {
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        "{}{}sleep 1; {}{}",
        boot(&isolated_init()),
        take(&run_frame(&first)),
        send(&[result_frame(&first, 5)]),
        exit_at_stop(),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let _ = serving(&board, 0).await;
    let receiver = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    // The slot waits for work, so it takes the task at once.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let report = pool.stop(Duration::from_secs(20)).await;

    assert_eq!(report, PoolStopReport::Drained);
    assert_eq!(answer_of(receiver).await, reported(5));
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `test_worker_stop_ordering.py::test_stop_drains_for_the_configured_time`
/// and `test_slot_pool.py::TestStop::test_drain_deadline_kills_and_reports`:
/// the stop gives a busy child its drain timeout, then kills it; its task
/// takes the crash path. The kill is the worker's (`KILLED_BY_PARENT`,
/// Python `_kill_at_deadline`), so the reason is Python's text of a lost
/// child (`_finalize_child_loss`, `python/horsies/core/worker/finalize.py:1057`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_drain_deadline_kills_a_busy_child_and_its_task_takes_the_crash_path() {
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        "{}{}exec sleep 60",
        boot(&isolated_init()),
        take(&run_frame(&first)),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let pid = serving(&board, 0).await;
    let receiver = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let started = Instant::now();

    let report = pool.stop(Duration::from_secs(1)).await;

    assert!(started.elapsed() >= Duration::from_secs(1));
    assert_eq!(
        report,
        PoolStopReport::KilledAtDrainDeadline {
            killed: vec![first.task_id]
        }
    );
    assert_eq!(
        answer_of(receiver).await,
        AttemptEnd::Crashed {
            reason: FailureDetail::cut(
                "Worker child lost: killed by the worker: signal 9 (SIGKILL)"
            ),
        }
    );
    ended(pid.as_pid_t()).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// The report of a pool's stop names the tasks that the drain deadline
/// killed. The deadline is the board's: when another pool passes it, the
/// slots of this pool can end within their own drain timeout after their
/// kills, and the report still names those kills.
#[test]
fn the_stop_report_names_the_kills_of_the_drain_deadline() {
    use super::pool::SlotsEnded;

    let killed = vec![Uuid::from_u128(1)];
    assert_eq!(
        PoolStopReport::of(SlotsEnded::InDrain, Vec::new()),
        PoolStopReport::Drained
    );
    assert_eq!(
        PoolStopReport::of(SlotsEnded::InDrain, killed.clone()),
        PoolStopReport::KilledAtDrainDeadline {
            killed: killed.clone()
        }
    );
    assert_eq!(
        PoolStopReport::of(SlotsEnded::AfterKills, killed.clone()),
        PoolStopReport::KilledAtDrainDeadline {
            killed: killed.clone()
        }
    );
    assert_eq!(
        PoolStopReport::of(SlotsEnded::Aborted, killed.clone()),
        PoolStopReport::AbortedAfterDrain { killed }
    );
}

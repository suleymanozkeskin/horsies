//! Tests of the board of every slot, and of shared pools driven by
//! scripted children (bash scripts that write frames with `printf`).

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use uuid::Uuid;

use super::scripted::{drop_bytes, take_init, ScriptCount};
use super::*;
use crate::broker::attempt_writes::RetryPlacement;
use crate::worker::executor::placement::ChildCpus;
use crate::worker::executor::protocol::{
    encode_child_frame, encode_parent_frame, AsyncSeats, BlockedWhere, CancelGraceMs,
    ChildAfterFrame, ChildConnection, ChildFrame, ChildKind, ChildLogLevel, ChildPool,
    ChildProgram, DatabaseUrl, DispatchNumber, ExecutorThreads, InitFrame, LeftoverName,
    LoopBlockedFrame, LoopCulprit, ParentFrame, PoolMaxSize, PoolMinSize, ProcessId, ReadyFrame,
    ResultEnvelopeText, RssReading, RunEnd, RunRef, RunStuckFrame, SeatEnd, SharedAsyncChild,
    SharedResultFrame, SharedRunFrame, SharedThreadChild, SharedWork, StuckWork, TaskInputText,
    TaskSendPool, TaskTimeoutMs, ThreadSeats,
};
use crate::worker::executor::supervisor::{
    BootFailure, BootFailureCause, ChildCommand, ChildExit, LossCause, ReapedExit, Spawner,
    CHILD_KILL_WAIT,
};
use crate::worker::retry::read_stored_options;
use crate::worker::runner_beats::LiveDispatches;

// ---------------------------------------------------------------------------
// The board
// ---------------------------------------------------------------------------

fn layout(isolated: usize, shared_async: usize, shared_thread: usize) -> ChildLayout {
    ChildLayout {
        isolated,
        shared_async,
        shared_thread,
    }
}

#[test]
fn slots_are_numbered_isolated_then_async_then_thread() {
    let board = PoolBoard::new(layout(2, 1, 3));
    assert_eq!(board.slots_of(ExecutionClass::Isolated), 0..2);
    assert_eq!(board.slots_of(ExecutionClass::SharedAsync), 2..3);
    assert_eq!(board.slots_of(ExecutionClass::SharedThread), 3..6);
    assert_eq!(board.phases_of(ExecutionClass::SharedThread).len(), 3);
}

/// Python `_another_child_serves` and `_all_slots`: the restart rules of
/// an isolated slot see a shared child that serves.
#[test]
fn the_restart_rules_see_every_kind_of_slot() {
    let board = PoolBoard::new(layout(1, 1, 0));
    let pid = ProcessId::new(10).unwrap();
    let at_start = board.pool_at_failure(0);
    assert!(!at_start.first_generation_in);
    assert!(!at_start.another_child_serves);

    board.set(0, SlotPhase::Serving { pid });
    board.set(1, SlotPhase::Serving { pid });
    board.set(1, SlotPhase::Closing { pid, runs: 2 });
    let isolated_fails = board.pool_at_failure(0);
    assert!(isolated_fails.first_generation_in);
    assert!(isolated_fails.another_child_serves);
    assert!(!isolated_fails.every_other_slot_down);

    board.set(
        1,
        SlotPhase::Down(DownState {
            cause: BootFailureCause::ThreadStartRefused,
            detail: "refused".to_owned(),
            down_since: Utc::now(),
            failed_boots: std::num::NonZeroU32::MIN,
        }),
    );
    let with_shared_down = board.pool_at_failure(0);
    assert!(!with_shared_down.another_child_serves);
    assert!(with_shared_down.every_other_slot_down);
    assert_eq!(board.down_slots().len(), 1);
    assert_eq!(board.down_slots()[0].0, 1);
}

#[test]
fn the_first_failure_fails_every_pool_and_wins() {
    let board = PoolBoard::new(layout(1, 1, 1));
    assert_eq!(board.health(), PoolHealth::Serving);
    board.fail(PoolFailure::ChildHookFailed);
    board.fail(PoolFailure::BootAttemptsExhausted {
        attempts: 3,
        last: BootFailure::new(BootFailureCause::NotReadyInTime, "late"),
    });
    assert_eq!(
        board.health(),
        PoolHealth::Failed(PoolFailure::ChildHookFailed)
    );
    assert!(board.not_serving());
}

// ---------------------------------------------------------------------------
// Shared pools with scripted children
// ---------------------------------------------------------------------------

const TASK: &str = "0190f5a2-7b1c-7d3e-8f00-0000000000bb";

fn claimed(execution: ExecutionClass) -> ClaimedTask {
    ClaimedTask {
        task_id: Uuid::parse_str(TASK).unwrap(),
        task_name: "app.tasks.shared".to_owned(),
        queue_name: "default".to_owned(),
        claimed_at: Utc.with_ymd_and_hms(2026, 11, 16, 9, 30, 0).unwrap(),
        execution,
        kind: TaskKind::Plain,
        retry: RetryFacts {
            retry_count: 0,
            max_retries: 0,
        },
        options: read_stored_options(Some("{}")),
    }
}

fn child_pool() -> ChildPool {
    ChildPool {
        min_size: PoolMinSize::new(0).unwrap(),
        max_size: PoolMaxSize::new(2).unwrap(),
        check: true,
    }
}

fn init_frame(kind: SharedChildKindOfTest) -> InitFrame {
    let child_kind = match kind {
        SharedChildKindOfTest::Async => ChildKind::SharedAsync(SharedAsyncChild {
            seats: AsyncSeats::new(2).unwrap(),
            pool: child_pool(),
            executor_threads: ExecutorThreads::new(2).unwrap(),
            task_send_pool: TaskSendPool::new(1).unwrap(),
        }),
        SharedChildKindOfTest::Thread => ChildKind::SharedThread(SharedThreadChild {
            seats: ThreadSeats::new(2).unwrap(),
            pool: child_pool(),
            task_send_pool: TaskSendPool::new(1).unwrap(),
        }),
    };
    InitFrame {
        program: ChildProgram::Rust,
        log_level: ChildLogLevel::Info,
        database_url: DatabaseUrl::new("postgresql://u@h/db".to_owned()),
        connection: ChildConnection::default(),
        process_name: "horsies-test-shared".to_owned(),
        child_kind,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedChildKindOfTest {
    Async,
    Thread,
}

fn timeout() -> TaskTimeoutMs {
    TaskTimeoutMs::new(1_000).unwrap()
}

fn work_of(kind: SharedChildKindOfTest) -> SharedWork {
    match kind {
        SharedChildKindOfTest::Async => SharedWork::AsyncTask {
            timeout_ms: timeout(),
            cancel_grace_ms: CancelGraceMs::new(100).unwrap(),
        },
        SharedChildKindOfTest::Thread => SharedWork::ThreadTask {
            timeout_ms: timeout(),
        },
    }
}

fn run_ref() -> RunRef {
    RunRef {
        dispatch: DispatchNumber::new(1).unwrap(),
        task_id: Uuid::parse_str(TASK).unwrap(),
    }
}

/// `printf` text that writes `bytes`: one octal escape per byte.
fn printf_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("\\{byte:03o}")).collect()
}

fn child_frames(frames: &[ChildFrame]) -> String {
    frames
        .iter()
        .map(|frame| printf_bytes(&encode_child_frame(frame).unwrap().to_bytes()))
        .collect()
}

/// A child script: take the init frame, send ready, take the first run
/// frame, write `frames`, then do `after`.
fn scripted_child(kind: SharedChildKindOfTest, frames: &[ChildFrame], after: &str) -> ChildCommand {
    let ready = encode_child_frame(&ChildFrame::Ready(ReadyFrame {
        pid: ProcessId::new(1).unwrap(),
        rss: RssReading::Unreadable,
    }))
    .unwrap()
    .to_bytes();
    let task = claimed(ExecutionClass::SharedAsync);
    let run = encode_parent_frame(&ParentFrame::SharedRun(SharedRunFrame {
        dispatch: run_ref().dispatch,
        task_id: task.task_id,
        task_name: task.task_name.clone(),
        worker_id: "worker-1".to_owned(),
        claimed_at: task.claimed_at,
        orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
        work: work_of(kind),
        input: TaskInputText::new("{}".to_owned()),
    }))
    .unwrap()
    .to_bytes();
    let script = format!(
        r#"{init}printf '{ready}' >&"$0"; {run}printf '{frames}' >&"$0"; {after}"#,
        init = take_init(),
        ready = printf_bytes(&ready),
        run = drop_bytes(ScriptCount::Known(run.len())),
        frames = child_frames(frames),
    );
    ChildCommand {
        program: crate::test_support::SCRIPT_SHELL.into(),
        args: vec!["-c".into(), script.into()],
    }
}

/// After the frames: take the stop frame and exit.
fn exit_at_stop() -> String {
    let stop = encode_parent_frame(&ParentFrame::Stop).unwrap().to_bytes();
    format!("{}exit 0", drop_bytes(ScriptCount::Known(stop.len())))
}

async fn run_one(
    kind: SharedChildKindOfTest,
    frames: &[ChildFrame],
    after: &str,
    wait: Duration,
) -> AttemptEnd {
    let (execution, board_layout) = match kind {
        SharedChildKindOfTest::Async => (ExecutionClass::SharedAsync, layout(0, 1, 0)),
        SharedChildKindOfTest::Thread => (ExecutionClass::SharedThread, layout(0, 0, 1)),
    };
    let spawner = Spawner::start().unwrap();
    let pool = SharedPool::start(
        SharedPoolConfig {
            command: scripted_child(kind, frames, after),
            init: init_frame(kind),
            worker_id: "worker-1".to_owned(),
            orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
            pending_limit: NonZeroUsize::new(4).unwrap(),
            tasks_per_seat: TaskLimit::Off,
            memory_per_child: MemoryLimit::Off,
            exit_not_seen: ExitNotSeen::LeaveToRecovery,
            child_cpus: ChildCpus::Inherited,
            kill_wait: CHILD_KILL_WAIT,
        },
        Arc::new(PoolBoard::new(board_layout)),
        spawner.handle(),
        LiveDispatches::default(),
    );
    let receiver = match pool.submit(
        &claimed(execution),
        TaskInputText::new("{}".to_owned()),
        work_of(kind),
    ) {
        Submitted::Queued(receiver) => receiver,
        Submitted::NotDispatched(cause) => panic!("not dispatched: {cause}"),
    };
    let end = tokio::time::timeout(wait, receiver).await.unwrap().unwrap();
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(Duration::from_secs(10));
    end
}

fn ok_envelope() -> ResultEnvelopeText {
    ResultEnvelopeText::new(r#"{"__h_task_result__":true,"ok":7,"err":null}"#.to_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_seat_reports_the_result_of_its_run() {
    let result = ChildFrame::SharedResult(SharedResultFrame {
        run: run_ref(),
        end: SeatEnd::Finished(RunEnd::Result(ok_envelope())),
        rss: RssReading::Unreadable,
        child: ChildAfterFrame::ServesOn,
    });
    assert_eq!(
        run_one(
            SharedChildKindOfTest::Async,
            &[result],
            &exit_at_stop(),
            Duration::from_secs(30)
        )
        .await,
        AttemptEnd::Reported(RunEnd::Result(ok_envelope()))
    );
}

/// Python `test_shared_async_tasks.py`: a run stuck after its second cancel
/// ends at its timeout and retries isolated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stuck_async_run_ends_at_its_timeout_and_retries_isolated() {
    let frames = [
        ChildFrame::DeadlineFired(run_ref()),
        ChildFrame::SecondCancel(run_ref()),
        ChildFrame::RunStuck(RunStuckFrame {
            run: run_ref(),
            work: StuckWork::new(1, 0, vec![LeftoverName::cut("app.tasks.shared")]).unwrap(),
        }),
        ChildFrame::SharedResult(SharedResultFrame {
            run: run_ref(),
            end: SeatEnd::TimedOut,
            rss: RssReading::Unreadable,
            child: ChildAfterFrame::ServesOn,
        }),
    ];
    assert_eq!(
        run_one(
            SharedChildKindOfTest::Async,
            &frames,
            &exit_at_stop(),
            Duration::from_secs(30)
        )
        .await,
        AttemptEnd::TimedOut {
            timeout: timeout(),
            retry: RetryPlacement::Isolated,
            cancel: TimeoutCancel::Ended(CancelEnd::DroppedAfterGrace),
        }
    );
}

/// Python `test_shared_async_tasks.py`: a blocked loop kills the child at
/// once; the run that held it fails with `TASK_BLOCKED_LOOP`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blocked_loop_kills_the_child_and_names_its_holder() {
    let blocked = ChildFrame::LoopBlocked(LoopBlockedFrame {
        culprit: LoopCulprit::Run(run_ref()),
        blocked_ms: 5_100,
        location: BlockedWhere::cut("app/tasks.py:12"),
    });
    match run_one(
        SharedChildKindOfTest::Async,
        &[blocked],
        "exec sleep 60",
        Duration::from_secs(30),
    )
    .await
    {
        AttemptEnd::KilledForLoop { code, retry, .. } => {
            assert_eq!(code, LoopKillCode::TaskBlockedLoop);
            assert_eq!(retry, RetryPlacement::Isolated);
        }
        other => panic!("expected the blocked-loop verdict, got {other:?}"),
    }
}

/// A stop report out of order is a protocol violation: the child is
/// killed, and the run takes the crash path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_report_out_of_order_kills_the_child() {
    match run_one(
        SharedChildKindOfTest::Async,
        &[ChildFrame::SecondCancel(run_ref())],
        "exec sleep 60",
        Duration::from_secs(30),
    )
    .await
    {
        AttemptEnd::Crashed { reason } => {
            assert!(reason.as_str().contains("bad frame"), "{reason}");
        }
        other => panic!("expected the crash path, got {other:?}"),
    }
}

/// Python `test_shared_thread_tasks.py`: the first deadline closes a thread
/// child; with no run left that has time, the child is killed and the
/// culprit retries isolated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thread_child_is_killed_once_no_run_has_time_left() {
    assert_eq!(
        run_one(
            SharedChildKindOfTest::Thread,
            &[ChildFrame::DeadlineFired(run_ref())],
            "exec sleep 60",
            Duration::from_secs(30),
        )
        .await,
        AttemptEnd::TimedOut {
            timeout: timeout(),
            retry: RetryPlacement::Isolated,
            cancel: TimeoutCancel::NoSignal,
        }
    );
}

/// A frozen thread child reports nothing: the backstop at `timeout + 10 s`
/// kills it, and the run, past its deadline, ends at its timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_backstop_kills_a_frozen_child() {
    assert_eq!(
        run_one(
            SharedChildKindOfTest::Thread,
            &[],
            "exec sleep 60",
            Duration::from_secs(40),
        )
        .await,
        AttemptEnd::TimedOut {
            timeout: timeout(),
            retry: RetryPlacement::SameClass,
            cancel: TimeoutCancel::NoSignal,
        }
    );
}

#[test]
fn a_killed_child_loss_reads_as_a_kill_by_the_worker() {
    // The verdicts above rely on this cause after `kill_for_caller`.
    let lost = crate::worker::executor::supervisor::ChildLost {
        cause: LossCause::KilledByParent,
        exit: ChildExit::Reaped(ReapedExit::KilledBySignal(libc::SIGKILL)),
        partial: crate::worker::executor::supervisor::PartialFrame::NoneAtHand,
        pid: ProcessId::new(5).unwrap(),
    };
    assert!(lost.to_string().contains("killed by the worker"));
}

/// The free seats of a shared pool, for the soft-cap start check: every seat of an open child
/// is free; a submitted task takes one at once; a stopping pool has none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_free_seats_of_an_open_child_less_the_submitted_tasks() {
    let kind = SharedChildKindOfTest::Async;
    let spawner = Spawner::start().unwrap();
    let board = Arc::new(PoolBoard::new(layout(0, 1, 0)));
    let pool = SharedPool::start(
        SharedPoolConfig {
            command: scripted_child(kind, &[], "exec sleep 60"),
            init: init_frame(kind),
            worker_id: "worker-1".to_owned(),
            orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
            pending_limit: NonZeroUsize::new(4).unwrap(),
            tasks_per_seat: TaskLimit::Off,
            memory_per_child: MemoryLimit::Off,
            exit_not_seen: ExitNotSeen::LeaveToRecovery,
            child_cpus: ChildCpus::Inherited,
            kill_wait: CHILD_KILL_WAIT,
        },
        Arc::clone(&board),
        spawner.handle(),
        LiveDispatches::default(),
    );
    super::scripted::until(
        "the child opens its 2 seats",
        Duration::from_secs(30),
        || pool.free_seats() == 2,
    )
    .await;

    let submitted = pool.submit(
        &claimed(ExecutionClass::SharedAsync),
        TaskInputText::new("{}".to_owned()),
        work_of(kind),
    );
    assert!(matches!(submitted, Submitted::Queued(_)));
    assert_eq!(pool.free_seats(), 1, "the submitted task holds one seat");

    board.begin_stop();
    assert_eq!(pool.free_seats(), 0, "a stopping pool has no free seat");
    let _ = pool.stop(Duration::from_secs(1)).await;
    let _ = spawner.stop(Duration::from_secs(10));
}

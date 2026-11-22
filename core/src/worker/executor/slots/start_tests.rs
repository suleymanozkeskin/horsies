//! The start of the children: the first generation, its baselines, a
//! degraded start, and the memory baseline guard of each kind.

use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;

use super::scripted::{
    answer_of, answer_runs, boot, fail_boot, idle_child, isolated_config, isolated_init, layout,
    reported, result_frame, serving, serving_pid, shared_config, shared_thread_init, shell,
    short_of_descriptors, start_isolated, start_shared, submit_isolated, task, ticket, until,
    Shown, TestDir, SPAWNER_STOP_WAIT, STATE_WAIT,
};
use super::*;
use crate::worker::executor::placement::{CpuPlacement, WorkerThreads};
use crate::worker::executor::protocol::{ProcessId, RssMb, RssReading, ShortageErrno};
use crate::worker::executor::supervisor::{BootFailureCause, Spawner};

/// The longest wait of a test for the start.
const START_WAIT: Duration = Duration::from_secs(30);

fn pid(number: u32) -> ProcessId {
    ProcessId::new(number).unwrap()
}

fn mb(value: f64) -> RssReading {
    RssReading::Mb(RssMb::new(value).unwrap())
}

fn limit(mb: u64) -> MemoryLimit {
    MemoryLimit::AtMb(NonZeroU64::new(mb).unwrap())
}

fn limits(
    isolated: MemoryLimit,
    shared_async: MemoryLimit,
    shared_thread: MemoryLimit,
) -> BaselineLimits {
    BaselineLimits {
        isolated,
        shared_async,
        shared_thread,
    }
}

fn baseline(class: ExecutionClass, rss: RssReading) -> ChildBaseline {
    ChildBaseline {
        slot: 0,
        class,
        pid: pid(1),
        rss,
    }
}

/// A listing of no thread: a start with shared CPUs never reads it.
fn no_threads() -> WorkerThreads {
    WorkerThreads::listed_in(std::path::PathBuf::from("/nonexistent/horsies-threads"))
}

async fn first_generation(board: &PoolBoard) -> Result<Vec<ChildBaseline>, StartEnd> {
    tokio::time::timeout(START_WAIT, board.first_generation())
        .await
        .expect("the start ends within the test wait")
}

// ---------------------------------------------------------------------------
// The first generation
// ---------------------------------------------------------------------------

/// Python `TestBoot::test_start_reports_one_baseline_per_child`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_start_reports_one_baseline_per_child() {
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(
        &spawner,
        2,
        isolated_config(shell(&idle_child(&isolated_init()))),
    );

    let baselines = first_generation(&board).await.unwrap();

    assert_eq!(baselines.len(), 2);
    assert_eq!(
        baselines.iter().map(|item| item.slot).collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert!(baselines
        .iter()
        .all(|item| item.class == ExecutionClass::Isolated));
    assert_ne!(baselines[0].pid, baselines[1].pid);
    assert_eq!(Some(baselines[0].pid), serving_pid(&board, 0));
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// The start of the children with no memory limit gives the baselines of
/// the first generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_start_of_the_children_gives_the_first_generation() {
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(
        &spawner,
        1,
        isolated_config(shell(&idle_child(&isolated_init()))),
    );
    let off = limits(MemoryLimit::Off, MemoryLimit::Off, MemoryLimit::Off);

    let started = tokio::time::timeout(
        START_WAIT,
        start_children(&board, &CpuPlacement::SharedCpus, &no_threads(), &off),
    )
    .await
    .unwrap();

    match started {
        Ok(ChildrenStart::Started { baselines }) => assert_eq!(baselines.len(), 1),
        other => panic!("expected a start, got {other:?}"),
    }
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `TestDownSlots::test_the_first_start_is_degraded_when_one_slot_boots`:
/// the start returns with one baseline while the other slot is down; the
/// down slot keeps its cause, leaves the budget, and the pool serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_first_start_is_degraded_when_one_slot_boots() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1) {}{};; *) {};; esac",
        ticket(&dir),
        boot(&init),
        answer_runs(&[(&first, vec![result_frame(&first, 1)])], 1),
        fail_boot(&init, &short_of_descriptors()),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 2, isolated_config(shell(&script)));

    let baselines = first_generation(&board).await.unwrap();

    assert_eq!(baselines.len(), 1);
    let down = board.down_slots();
    assert_eq!(down.len(), 1);
    assert_ne!(down[0].0, baselines[0].slot);
    assert_eq!(
        down[0].1.cause,
        BootFailureCause::ResourceShortage(ShortageErrno::Emfile)
    );
    assert!(down[0].1.failed_boots >= NonZeroU32::MIN);
    assert_eq!(isolated_capacity(&pool.service()), 1);
    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;
    assert_eq!(answer, reported(1));
    assert_eq!(board.health(), PoolHealth::Serving);
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// A failed start hook ends the start with the pool failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_start_hook_ends_the_start_with_the_failure() {
    let script = format!("{}exit 173", super::scripted::take_init());
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let off = limits(MemoryLimit::Off, MemoryLimit::Off, MemoryLimit::Off);

    let started = tokio::time::timeout(
        START_WAIT,
        start_children(&board, &CpuPlacement::SharedCpus, &no_threads(), &off),
    )
    .await
    .unwrap();

    assert_eq!(
        started,
        Err(ChildrenStartFailed::Pool(PoolFailure::ChildHookFailed))
    );
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `TestStopDuringBoot::test_stop_ends_the_child_and_the_start`, the
/// start half: a stop during the start ends it with no failure.
#[tokio::test]
async fn a_stop_during_the_start_ends_it_with_no_failure() {
    let board = PoolBoard::new(layout(1, 0, 0));
    let off = limits(MemoryLimit::Off, MemoryLimit::Off, MemoryLimit::Off);
    board.begin_stop();

    let started = start_children(&board, &CpuPlacement::SharedCpus, &no_threads(), &off).await;

    assert_eq!(started, Ok(ChildrenStart::Stopped));
}

/// The first generation stays in once it is: a slot that boots again after
/// it does not take it back.
#[test]
fn the_first_generation_stays_in_when_a_slot_boots_again() {
    let board = PoolBoard::new(layout(2, 0, 0));
    board.ready(
        0,
        ReadyChild {
            pid: pid(10),
            rss: RssReading::Unreadable,
        },
    );
    board.ready(
        1,
        ReadyChild {
            pid: pid(11),
            rss: RssReading::Unreadable,
        },
    );
    assert!(board.pool_at_failure(0).first_generation_in);

    board.set(1, SlotPhase::Booting);

    assert!(board.pool_at_failure(1).first_generation_in);
}

// ---------------------------------------------------------------------------
// A kind down at the first start
// ---------------------------------------------------------------------------

/// Python `TestNewKindBaseline::test_the_first_ready_child_of_a_kind_down_at_the_first_start_reports_its_baseline`:
/// the thread slot is short of descriptors at the first start, so the
/// start returns the isolated baseline only. The first thread child that
/// becomes ready later reports its baseline once; replacements of either
/// kind report nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_first_ready_child_of_a_kind_down_at_the_first_start_reports_its_baseline() {
    let dir = TestDir::new();
    let thread_init = shared_thread_init(1);
    let thread_script = format!(
        "{}case $n in 1) {};; *) {};; esac",
        ticket(&dir),
        fail_boot(&thread_init, &short_of_descriptors()),
        idle_child(&thread_init),
    );
    let spawner = Spawner::start().unwrap();
    let board = Arc::new(PoolBoard::new(layout(1, 0, 1)));
    let isolated = IsolatedPool::start(
        isolated_config(shell(&idle_child(&isolated_init()))),
        Arc::clone(&board),
        spawner.handle(),
        crate::worker::runner_beats::LiveDispatches::default(),
    );
    let thread = start_shared(
        &spawner,
        &board,
        shared_config(shell(&thread_script), thread_init),
    );

    let baselines = first_generation(&board).await.unwrap();
    assert_eq!(
        baselines.iter().map(|item| item.class).collect::<Vec<_>>(),
        vec![ExecutionClass::Isolated]
    );

    let late = tokio::time::timeout(START_WAIT, board.wait_new_kind_baseline())
        .await
        .expect("the thread kind reports its baseline");
    assert_eq!(late.class, ExecutionClass::SharedThread);
    assert_eq!(late.slot, 1);
    let first_thread = serving(&board, 1).await;
    let first_isolated = serving(&board, 0).await;
    assert_eq!(late.pid, first_thread);

    for child in [first_thread, first_isolated] {
        // SAFETY: SIGKILL to a child of this test that the spawner has not
        // reaped.
        unsafe { libc::kill(child.as_pid_t(), libc::SIGKILL) };
    }
    until(
        "both slots serve a replacement",
        STATE_WAIT,
        &[Shown::Board(&board), Shown::Tickets(&dir)],
        || {
            let thread_child = serving_pid(&board, 1);
            let isolated_child = serving_pid(&board, 0);
            thread_child.is_some_and(|child| child != first_thread)
                && isolated_child.is_some_and(|child| child != first_isolated)
        },
    )
    .await;
    let more =
        tokio::time::timeout(Duration::from_millis(500), board.wait_new_kind_baseline()).await;
    assert!(more.is_err(), "a replacement reported a baseline: {more:?}");

    let _ = thread.stop(Duration::from_secs(5)).await;
    let _ = isolated.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

// ---------------------------------------------------------------------------
// The memory baseline guard
// ---------------------------------------------------------------------------

/// Python `TestSharedMemoryBaseline::test_a_shared_baseline_at_its_limit_is_refused`.
#[test]
fn a_shared_async_baseline_at_its_limit_is_refused() {
    let checked = check_memory_baselines(
        &[baseline(ExecutionClass::SharedAsync, mb(100.0))],
        &limits(MemoryLimit::Off, limit(100), MemoryLimit::Off),
    );

    let over = checked.expect_err("a baseline at the limit is refused");
    assert_eq!(over.class, ExecutionClass::SharedAsync);
    assert!(over.to_string().contains("shared async"), "{over}");
}

/// Python `TestSharedMemoryBaseline::test_the_shared_limit_does_not_apply_to_isolated_children`.
#[test]
fn the_shared_limit_does_not_apply_to_isolated_children() {
    let checked = check_memory_baselines(
        &[baseline(ExecutionClass::Isolated, mb(500.0))],
        &limits(MemoryLimit::Off, limit(100), MemoryLimit::Off),
    );

    assert_eq!(checked, Ok(()));
}

/// Python `TestSharedMemoryBaseline::test_the_isolated_limit_does_not_apply_to_shared_children`.
#[test]
fn the_isolated_limit_does_not_apply_to_shared_children() {
    let checked = check_memory_baselines(
        &[baseline(ExecutionClass::SharedAsync, mb(500.0))],
        &limits(limit(100), MemoryLimit::Off, MemoryLimit::Off),
    );

    assert_eq!(checked, Ok(()));
}

/// Python `TestThreadMemoryBaseline::test_a_thread_baseline_at_its_limit_is_refused`.
#[test]
fn a_thread_baseline_at_its_limit_is_refused() {
    let checked = check_memory_baselines(
        &[baseline(ExecutionClass::SharedThread, mb(100.0))],
        &limits(MemoryLimit::Off, MemoryLimit::Off, limit(100)),
    );

    let over = checked.expect_err("a baseline at the limit is refused");
    assert_eq!(over.class, ExecutionClass::SharedThread);
    assert!(over.to_string().contains("shared thread"), "{over}");
}

/// Python `TestThreadMemoryBaseline::test_the_thread_limit_applies_to_thread_children_only`.
#[test]
fn the_thread_limit_applies_to_thread_children_only() {
    for class in [ExecutionClass::Isolated, ExecutionClass::SharedAsync] {
        let checked = check_memory_baselines(
            &[baseline(class, mb(500.0))],
            &limits(MemoryLimit::Off, MemoryLimit::Off, limit(100)),
        );

        assert_eq!(checked, Ok(()), "{class}");
    }
}

/// An unreadable baseline skips the guard of its kind.
#[test]
fn an_unreadable_baseline_skips_the_guard() {
    let checked = check_memory_baselines(
        &[baseline(ExecutionClass::Isolated, RssReading::Unreadable)],
        &limits(limit(1), MemoryLimit::Off, MemoryLimit::Off),
    );

    assert_eq!(checked, Ok(()));
}

/// A start whose baseline is at the limit of its kind fails.
#[tokio::test]
async fn a_first_generation_baseline_at_its_limit_fails_the_start() {
    let board = PoolBoard::new(layout(1, 0, 0));
    board.ready(
        0,
        ReadyChild {
            pid: pid(10),
            rss: mb(120.0),
        },
    );

    let started = start_children(
        &board,
        &CpuPlacement::SharedCpus,
        &no_threads(),
        &limits(limit(100), MemoryLimit::Off, MemoryLimit::Off),
    )
    .await;

    match started {
        Err(ChildrenStartFailed::BaselineOverLimit(over)) => {
            assert_eq!(over.class, ExecutionClass::Isolated);
            assert_eq!(over.baseline_mb, 120.0);
        }
        other => panic!("expected a baseline over its limit, got {other:?}"),
    }
}

/// A board whose isolated slot booted and whose shared async slot was down
/// at the first start; then the async child becomes ready with `rss`.
fn late_async_child(rss: RssReading) -> PoolBoard {
    let board = PoolBoard::new(layout(1, 1, 0));
    board.ready(
        0,
        ReadyChild {
            pid: pid(10),
            rss: RssReading::Unreadable,
        },
    );
    board.set(
        1,
        SlotPhase::Down(DownState {
            cause: BootFailureCause::ResourceShortage(ShortageErrno::Emfile),
            detail: "short".to_owned(),
            down_since: Utc::now(),
            failed_boots: NonZeroU32::MIN,
        }),
    );
    assert!(board.pool_at_failure(0).first_generation_in);
    board.ready(1, ReadyChild { pid: pid(11), rss });
    board
}

/// Python `TestNewKindBaselineGuard::test_a_late_baseline_at_its_limit_stops_the_worker`:
/// the guard returns the breach, and the caller stops the worker.
#[tokio::test]
async fn a_late_baseline_at_its_limit_ends_the_guard_with_the_breach() {
    let board = late_async_child(mb(100.0));
    let async_limit = limits(MemoryLimit::Off, limit(100), MemoryLimit::Off);

    let over = tokio::time::timeout(
        Duration::from_secs(5),
        guard_new_kind_baselines(&board, &async_limit),
    )
    .await
    .expect("the guard ends at the breach");

    assert_eq!(over.class, ExecutionClass::SharedAsync);
    assert!(over.to_string().contains("shared async"), "{over}");
}

/// Python `TestNewKindBaselineGuard::test_a_late_baseline_under_its_limit_keeps_the_worker`.
#[tokio::test]
async fn a_late_baseline_under_its_limit_keeps_the_guard_waiting() {
    let board = late_async_child(mb(50.0));
    let async_limit = limits(MemoryLimit::Off, limit(100), MemoryLimit::Off);

    let guard = tokio::time::timeout(
        Duration::from_millis(200),
        guard_new_kind_baselines(&board, &async_limit),
    )
    .await;

    assert!(guard.is_err(), "the guard ended: {guard:?}");
}

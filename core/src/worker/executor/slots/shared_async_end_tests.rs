//! How the runs of a
//! shared async child end when the child ends, the capacity, the start and
//! the stop of a shared pool, and the second backstop.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::oneshot::error::TryRecvError;

use super::dispatch::BodyReturn;
use super::scripted::{
    act_on_runs, answer_of, answer_runs, boot, boot_failed, ended, fail_boot, idle_child, input,
    isolated_config, isolated_init, layout, leftover_cancel, leftover_stuck, ok_envelope,
    process_ended, read_frame, reported, result_frame, run_ref, run_stuck, send, serving,
    shared_async_init, shared_config, shared_result, shell, short_of_descriptors, start_shared,
    started_at, submit_isolated, submit_shared, take_init, task, ticket, tickets, timeout_ms,
    until, TestDir, SPAWNER_STOP_WAIT, STATE_WAIT,
};
use super::seat_runs::{
    due_step_reported, end_at_loss, stuck_step_reported, CloseCulprit, LossSight, OutOfTurn,
    RunStopReport, RunWork, SeatRun, SeatRuns, SeenLoss, StopReportFrame,
};
use super::shared_stop::{ChildKill, LoopHolder};
use super::*;
use crate::broker::attempt_writes::RetryPlacement;
use crate::worker::executor::protocol::{
    BlockedWhere, BodyEnd, CancelGraceMs, ChildFrame, ProcessId, ReportedBootCause, RunRef,
    SharedWork,
};
use crate::worker::executor::supervisor::{
    BootFailureCause, ChildExit, ChildLost, LossCause, PartialFrame, ReapedExit, Spawner,
    CHILD_BOOT_ATTEMPTS,
};

const SHORT_MS: u64 = 1_000;
const GRACE_MS: u64 = 100;

fn long_work() -> SharedWork {
    super::scripted::async_work(60_000, 1_000)
}

fn short_work() -> SharedWork {
    super::scripted::async_work(SHORT_MS, GRACE_MS)
}

fn shared_task(number: u128) -> ClaimedTask {
    task(number, ExecutionClass::SharedAsync)
}

fn async_pool(
    spawner: &Spawner,
    children: usize,
    config: SharedPoolConfig,
) -> (SharedPool, Arc<PoolBoard>) {
    let board = Arc::new(PoolBoard::new(layout(0, children, 0)));
    let pool = start_shared(spawner, &board, config);
    (pool, board)
}

fn children(dir: &TestDir, seats: u64, actions: &[(&ClaimedTask, String)], runs: usize) -> String {
    format!(
        "{}{}{}",
        ticket(dir),
        boot(&shared_async_init(seats)),
        act_on_runs(actions, runs)
    )
}

async fn stop_shared(pool: SharedPool, spawner: Spawner) {
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

fn crash_reason(end: AttemptEnd) -> String {
    match end {
        AttemptEnd::Crashed { reason } => reason.into_string(),
        other => panic!("expected the crash path, got {other:?}"),
    }
}

fn timed_out(retry: RetryPlacement, cancel: TimeoutCancel) -> AttemptEnd {
    AttemptEnd::TimedOut {
        timeout: timeout_ms(SHORT_MS),
        retry,
        cancel,
    }
}

fn not_answered(receiver: &mut tokio::sync::oneshot::Receiver<AttemptEnd>) -> bool {
    matches!(receiver.try_recv(), Err(TryRecvError::Empty))
}

async fn failure_of(board: &PoolBoard) -> PoolFailure {
    until("the pool failed", STATE_WAIT, || {
        matches!(board.health(), PoolHealth::Failed(_))
    })
    .await;
    match board.health() {
        PoolHealth::Failed(failure) => failure,
        other => panic!("expected a failed pool, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Pure rules
// ---------------------------------------------------------------------------

fn async_run_work() -> RunWork {
    RunWork::AsyncTask {
        timeout: timeout_ms(SHORT_MS),
        grace: CancelGraceMs::new(GRACE_MS).unwrap(),
    }
}

fn seat(dispatch: u64, work: RunWork, report: RunStopReport) -> SeatRun<()> {
    SeatRun {
        run: run_ref(&shared_task(u128::from(dispatch)), dispatch),
        work,
        sent_at: tokio::time::Instant::now(),
        report,
        attached: (),
    }
}

fn returned() -> BodyReturn {
    BodyReturn {
        end: BodyEnd::Result(ok_envelope(1)),
        started_at: started_at(),
    }
}

fn seen(cause: LossCause) -> SeenLoss {
    match LossSight::of(ChildLost {
        cause,
        exit: ChildExit::Reaped(ReapedExit::KilledBySignal(libc::SIGKILL)),
        partial: PartialFrame::NoneAtHand,
        pid: ProcessId::new(7).unwrap(),
    }) {
        LossSight::Seen(seen) => seen,
        LossSight::NotSeen(lost) => panic!("a reaped exit is seen: {lost}"),
    }
}

fn blocked_by(holder: LoopHolder) -> ChildKill {
    ChildKill::BlockedLoop {
        holder,
        blocked_ms: 5_000,
        location: BlockedWhere::cut("app/tasks.py:9"),
    }
}

/// The stop report that `frame` carries.
fn stop_report(frame: ChildFrame) -> StopReportFrame {
    match frame {
        ChildFrame::SecondCancel(run) => StopReportFrame::SecondCancel(run),
        ChildFrame::RunStuck(stuck) => StopReportFrame::RunStuck(stuck),
        ChildFrame::LeftoverCancel(cancel) => StopReportFrame::LeftoverCancel(cancel),
        ChildFrame::LeftoverStuck(stuck) => StopReportFrame::LeftoverStuck(stuck),
        other => panic!("not a stop report: {other:?}"),
    }
}

/// Python `TestStopReportOrder::test_a_report_out_of_order_is_refused` and
/// `::test_a_report_of_another_task_is_refused`: each report out of the
/// order of the stop is refused and leaves the run's report as it was.
#[test]
fn every_stop_report_out_of_order_is_refused_and_changes_nothing() {
    let run = run_ref(&shared_task(1), 1);
    let left_cancel = RunStopReport::LeftoverCancelReported {
        returned: returned(),
    };
    let cases = [
        (RunStopReport::NotReported, ChildFrame::SecondCancel(run)),
        (RunStopReport::DeadlineReported, run_stuck(run)),
        (RunStopReport::StuckReported, ChildFrame::SecondCancel(run)),
        (RunStopReport::DeadlineReported, leftover_cancel(run, 1)),
        (left_cancel.clone(), leftover_cancel(run, 1)),
        (RunStopReport::NotReported, leftover_stuck(run, 1)),
        (RunStopReport::SecondCancelReported, leftover_stuck(run, 1)),
        (left_cancel.clone(), ChildFrame::SecondCancel(run)),
        (left_cancel, run_stuck(run)),
    ];
    for (report, frame) in cases {
        let mut runs = SeatRuns::new(NonZeroUsize::new(2).unwrap());
        runs.add(seat(1, async_run_work(), report.clone()));
        let refused = runs.note_stop_report(&stop_report(frame.clone()));
        assert_eq!(
            refused.unwrap_err().why,
            OutOfTurn::StepOutOfOrder,
            "{report:?} then {frame:?}"
        );
        assert_eq!(runs.get(run.dispatch).unwrap().report, report);
    }
    let mut runs = SeatRuns::new(NonZeroUsize::new(2).unwrap());
    runs.add(seat(1, async_run_work(), RunStopReport::DeadlineReported));
    let another_task = RunRef {
        dispatch: run.dispatch,
        task_id: shared_task(99).task_id,
    };
    assert_eq!(
        runs.note_stop_report(&StopReportFrame::SecondCancel(another_task))
            .unwrap_err()
            .why,
        OutOfTurn::AnotherTask
    );
}

/// Python `TestReturnedRunAtAKill::test_stuck_left_work_keeps_what_the_task_returned_at_any_kill`,
/// `::test_an_end_the_worker_did_not_cause_keeps_what_the_task_returned`,
/// `::test_work_in_its_cancel_rounds_keeps_what_the_task_returned_at_any_kill`
/// and `::test_a_crash_the_worker_did_not_cause_takes_the_crash_path`.
#[test]
fn a_run_that_returned_keeps_what_it_returned_at_any_end_whose_exit_was_seen() {
    let now = tokio::time::Instant::now();
    let other_run = run_ref(&shared_task(9), 9);
    let reports = [
        RunStopReport::LeftoverCancelReported {
            returned: returned(),
        },
        RunStopReport::LeftoverStuckReported {
            returned: returned(),
        },
    ];
    for report in reports {
        let returned_run = seat(1, async_run_work(), report.clone());
        let kills = [
            ChildKill::NoNamedKill,
            blocked_by(LoopHolder::Run(returned_run.run)),
            blocked_by(LoopHolder::Run(other_run)),
            blocked_by(LoopHolder::Unknown),
        ];
        for kill in &kills {
            for cause in [
                LossCause::KilledByParent,
                LossCause::ChildDied,
                LossCause::ChannelLost,
                LossCause::ProtocolViolation,
                LossCause::SlotDefect,
            ] {
                assert_eq!(
                    end_at_loss(
                        &returned_run,
                        kill,
                        CloseCulprit::NotNamed,
                        seen(cause),
                        now
                    ),
                    AttemptEnd::returned(returned()),
                    "{report:?}, {kill:?}, {cause}"
                );
            }
        }
    }
    let not_returned = seat(1, async_run_work(), RunStopReport::SecondCancelReported);
    assert!(matches!(
        end_at_loss(
            &not_returned,
            &ChildKill::NoNamedKill,
            CloseCulprit::NotNamed,
            seen(LossCause::ChildDied),
            now
        ),
        AttemptEnd::Crashed { .. }
    ));
}

/// Python `TestNamedCulpritAtAKill::test_a_run_reported_stuck_retries_isolated`,
/// `::test_the_run_that_closed_a_thread_child_retries_isolated` and
/// `::test_a_crash_the_worker_did_not_cause_isolates_no_run`.
#[test]
fn the_named_culprit_retries_isolated_at_a_kill_by_the_worker_only() {
    let now = tokio::time::Instant::now();
    let killed = seen(LossCause::KilledByParent);
    let stuck = seat(1, async_run_work(), RunStopReport::StuckReported);
    let other_run = run_ref(&shared_task(9), 9);
    for kill in [
        ChildKill::NoNamedKill,
        blocked_by(LoopHolder::Run(other_run)),
    ] {
        assert_eq!(
            end_at_loss(&stuck, &kill, CloseCulprit::NotNamed, killed, now),
            timed_out(
                RetryPlacement::Isolated,
                TimeoutCancel::Ended(CancelEnd::KilledWithChild)
            ),
            "{kill:?}"
        );
    }
    let thread_work = RunWork::ThreadTask {
        timeout: timeout_ms(SHORT_MS),
    };
    let closer = seat(1, thread_work, RunStopReport::DeadlineReported);
    let beside = seat(2, thread_work, RunStopReport::DeadlineReported);
    assert_eq!(
        end_at_loss(
            &closer,
            &ChildKill::NoNamedKill,
            CloseCulprit::Named(closer.run),
            killed,
            now
        ),
        timed_out(RetryPlacement::Isolated, TimeoutCancel::NoSignal)
    );
    assert_eq!(
        end_at_loss(
            &beside,
            &ChildKill::NoNamedKill,
            CloseCulprit::Named(closer.run),
            killed,
            now
        ),
        timed_out(RetryPlacement::SameClass, TimeoutCancel::NoSignal)
    );
    assert!(matches!(
        end_at_loss(
            &stuck,
            &ChildKill::NoNamedKill,
            CloseCulprit::Named(stuck.run),
            seen(LossCause::ChildDied),
            now
        ),
        AttemptEnd::Crashed { .. }
    ));
}

/// Python `TestReturnedRunAtAKill::test_a_leftover_report_answers_the_backstop`
/// and `TestStuckStepReported::test_a_queued_stuck_report_answers_the_stuck_backstop`
/// (the reports; a queued frame is read before any backstop, because the
/// slot waits for a frame with the backstop as its bound).
#[test]
fn leftover_and_stuck_reports_answer_the_backstops() {
    for report in [
        RunStopReport::LeftoverCancelReported {
            returned: returned(),
        },
        RunStopReport::LeftoverStuckReported {
            returned: returned(),
        },
    ] {
        assert!(due_step_reported(async_run_work(), &report), "{report:?}");
    }
    assert!(stuck_step_reported(&RunStopReport::StuckReported));
    assert!(stuck_step_reported(&RunStopReport::LeftoverStuckReported {
        returned: returned()
    }));
    assert!(!stuck_step_reported(&RunStopReport::SecondCancelReported));
    assert!(!stuck_step_reported(
        &RunStopReport::LeftoverCancelReported {
            returned: returned()
        }
    ));
}

// ---------------------------------------------------------------------------
// The end of a child with real children
// ---------------------------------------------------------------------------

/// Python `TestLoss::test_a_crash_ends_every_task_of_the_child` and
/// `::test_a_task_that_escapes_the_seat_runner_ends_the_child`: a child that
/// dies ends every run on it on the crash path; the next task runs on a new
/// child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_that_dies_ends_every_run_on_the_crash_path() {
    for (death, exit) in [("kill -9 $$", "SIGKILL"), ("exit 1", "exit code 1")] {
        let dir = TestDir::new();
        let tasks: Vec<ClaimedTask> = (1..=5).map(shared_task).collect();
        let mut actions: Vec<(&ClaimedTask, String)> = tasks[..3]
            .iter()
            .map(|task| (task, ":".to_owned()))
            .collect();
        actions.push((&tasks[3], format!("sleep 0.2; {death}")));
        actions.push((&tasks[4], send(&[shared_result(run_ref(&tasks[4], 5), 5)])));
        let script = children(&dir, 4, &actions, 4);
        let spawner = Spawner::start().unwrap();
        let (pool, _board) = async_pool(
            &spawner,
            1,
            shared_config(shell(&script), shared_async_init(4)),
        );

        let receivers: Vec<_> = tasks[..4]
            .iter()
            .map(|task| submit_shared(&pool, task, long_work()))
            .collect();

        for receiver in receivers {
            let reason = crash_reason(answer_of(receiver).await);
            assert!(reason.contains("child died"), "{reason}");
            assert!(reason.contains(exit), "{reason}");
        }
        assert_eq!(
            answer_of(submit_shared(&pool, &tasks[4], long_work())).await,
            reported(5)
        );
        assert_eq!(tickets(&dir), 2);
        stop_shared(pool, spawner).await;
    }
}

// ---------------------------------------------------------------------------
// Capacity
// ---------------------------------------------------------------------------

/// Python `TestCapacity::test_open_children_count_every_seat`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_shared_children_count_every_seat() {
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        2,
        shared_config(
            shell(&idle_child(&shared_async_init(5))),
            shared_async_init(5),
        ),
    );
    let _ = serving(&board, 0).await;
    let _ = serving(&board, 1).await;

    assert_eq!(shared_capacity(&pool.service()), 10);
    stop_shared(pool, spawner).await;
}

/// Python `TestCapacity::test_a_down_child_counts_nothing`: the shared
/// slots come after the isolated ones on the board; a down shared slot
/// counts no seat.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_down_shared_child_counts_nothing() {
    let dir = TestDir::new();
    let init = shared_async_init(5);
    let script = format!(
        "{}case $n in 1) {};; *) {};; esac",
        ticket(&dir),
        idle_child(&init),
        fail_boot(&init, &short_of_descriptors()),
    );
    let spawner = Spawner::start().unwrap();
    let board = Arc::new(PoolBoard::new(layout(1, 2, 0)));
    let isolated = IsolatedPool::start(
        isolated_config(shell(&idle_child(&isolated_init()))),
        Arc::clone(&board),
        spawner.handle(),
        crate::worker::runner_beats::LiveDispatches::default(),
    );
    let shared = start_shared(&spawner, &board, shared_config(shell(&script), init));

    until("a shared slot is down", STATE_WAIT, || {
        board.down_slots().len() == 1
    })
    .await;

    let down = board.down_slots();
    assert!(down[0].0 >= 1, "the down slot is {}", down[0].0);
    assert_eq!(
        down[0].1.cause,
        BootFailureCause::ResourceShortage(
            crate::worker::executor::protocol::ShortageErrno::Emfile
        )
    );
    let _ = serving(&board, 0).await;
    assert_eq!(shared_capacity(&shared.service()), 5);
    assert_eq!(isolated_capacity(&isolated.service()), 1);
    let _ = shared.stop(Duration::from_secs(5)).await;
    let _ = isolated.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

// ---------------------------------------------------------------------------
// Start and stop
// ---------------------------------------------------------------------------

/// Python `TestStartAndStop::test_a_seat_start_that_fails_is_a_boot_failure`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seat_setup_that_fails_is_a_boot_failure_of_the_shared_pool() {
    let init = shared_async_init(2);
    let no_pool = boot_failed(
        ReportedBootCause::ChildPoolSetupFailed,
        "OperationalError",
        "connection refused",
    );
    let script = fail_boot(&init, &no_pool);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(&spawner, 1, shared_config(shell(&script), init));

    match failure_of(&board).await {
        PoolFailure::BootAttemptsExhausted { attempts, last } => {
            assert_eq!(attempts, CHILD_BOOT_ATTEMPTS);
            assert_eq!(last.cause, BootFailureCause::ChildPoolSetupFailed);
        }
        other => panic!("expected the boot attempts to run out, got {other:?}"),
    }
    stop_shared(pool, spawner).await;
}

/// Python `TestStartAndStop::test_a_failing_async_start_hook_stops_the_pool`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_start_hook_of_a_shared_child_fails_the_pool() {
    let init = shared_async_init(2);
    let script = format!("{}exit 173", take_init());
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(&spawner, 1, shared_config(shell(&script), init));

    assert_eq!(failure_of(&board).await, PoolFailure::ChildHookFailed);
    stop_shared(pool, spawner).await;
}

/// Python `TestStartAndStop::test_stop_lets_shared_tasks_finish`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stop_lets_shared_runs_finish() {
    let dir = TestDir::new();
    let first = shared_task(1);
    let actions = vec![(
        &first,
        format!(
            "sleep 0.3; {}",
            send(&[shared_result(run_ref(&first, 1), 1)])
        ),
    )];
    let script = children(&dir, 2, &actions, 1);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(2)),
    );
    let _ = serving(&board, 0).await;
    let receiver = submit_shared(&pool, &first, long_work());
    tokio::time::sleep(Duration::from_millis(100)).await;

    let report = pool.stop(Duration::from_secs(10)).await;

    assert_eq!(report, PoolStopReport::Drained);
    assert_eq!(answer_of(receiver).await, reported(1));
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `TestStartAndStop::test_stop_kills_a_shared_child_past_the_drain`:
/// at the drain deadline the child is killed; its runs had time left and
/// no named cause, so they take the crash path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_drain_deadline_kills_a_shared_child_and_its_runs_take_the_crash_path() {
    let dir = TestDir::new();
    let tasks: Vec<ClaimedTask> = (1..=2).map(shared_task).collect();
    let actions: Vec<(&ClaimedTask, String)> =
        tasks.iter().map(|task| (task, ":".to_owned())).collect();
    let script = children(&dir, 2, &actions, 2);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(2)),
    );
    let pid = serving(&board, 0).await;
    let receivers: Vec<_> = tasks
        .iter()
        .map(|task| submit_shared(&pool, task, long_work()))
        .collect();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let report = pool.stop(Duration::from_millis(300)).await;

    assert_eq!(
        report,
        PoolStopReport::KilledAtDrainDeadline {
            killed: tasks.iter().map(|task| task.task_id).collect()
        }
    );
    for receiver in receivers {
        let _ = crash_reason(answer_of(receiver).await);
    }
    ended(pid.as_pid_t()).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `TestStartAndStop::test_a_task_after_stop_is_not_dispatched`
/// (`SharedPool::stop` takes the pool, so the stop begins on the board).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_after_the_stop_began_is_not_sent_to_a_shared_child() {
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        1,
        shared_config(
            shell(&idle_child(&shared_async_init(1))),
            shared_async_init(1),
        ),
    );
    let _ = serving(&board, 0).await;

    board.begin_stop();
    let refused = pool.submit(&shared_task(1), input(), long_work());

    assert!(matches!(
        refused,
        Submitted::NotDispatched(NotRunCause::PoolStopping)
    ));
    stop_shared(pool, spawner).await;
}

/// Python `TestStartAndStop::test_isolated_and_shared_slots_serve_side_by_side`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isolated_and_shared_slots_serve_side_by_side() {
    let isolated_task = task(1, ExecutionClass::Isolated);
    let shared = shared_task(2);
    let spawner = Spawner::start().unwrap();
    let board = Arc::new(PoolBoard::new(layout(1, 1, 0)));
    let isolated_script = format!(
        "{}{}",
        boot(&isolated_init()),
        answer_runs(
            &[(&isolated_task, vec![result_frame(&isolated_task, 1)])],
            1
        ),
    );
    let isolated = IsolatedPool::start(
        isolated_config(shell(&isolated_script)),
        Arc::clone(&board),
        spawner.handle(),
        crate::worker::runner_beats::LiveDispatches::default(),
    );
    let shared_script = format!(
        "{}{}",
        boot(&shared_async_init(1)),
        answer_runs(&[(&shared, vec![shared_result(run_ref(&shared, 1), 2)])], 1),
    );
    let shared_pool = start_shared(
        &spawner,
        &board,
        shared_config(shell(&shared_script), shared_async_init(1)),
    );

    let isolated_answer = submit_isolated(&isolated, &isolated_task, IsolatedDeadline::NoDeadline);
    let shared_answer = submit_shared(&shared_pool, &shared, long_work());

    assert_eq!(answer_of(isolated_answer).await, reported(1));
    assert_eq!(answer_of(shared_answer).await, reported(2));
    assert_ne!(serving(&board, 0).await, serving(&board, 1).await);
    let _ = shared_pool.stop(Duration::from_secs(5)).await;
    let _ = isolated.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

// ---------------------------------------------------------------------------
// The second backstop
// ---------------------------------------------------------------------------

/// Python `TestFrozenAfterTheLastCancel::test_the_stuck_backstop_kills_a_child_frozen_after_its_last_cancel`
/// (both cases in one child): the child reports a second cancel and a
/// leftover cancel, then freezes. The first backstop finds the due step of
/// each run reported; the second backstop finds neither run stuck nor ended
/// and kills the child. The run past its deadline times out in its class;
/// the run that returned keeps what it returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stuck_backstop_kills_a_child_frozen_after_its_last_cancel() {
    let second = shared_task(1);
    let returned_task = shared_task(2);
    let second_run = run_ref(&second, 1);
    let returned_run = run_ref(&returned_task, 2);
    let script = format!(
        "{}{}{}{}kill -STOP $$",
        boot(&shared_async_init(2)),
        read_frame(),
        read_frame(),
        send(&[
            ChildFrame::DeadlineFired(second_run),
            ChildFrame::SecondCancel(second_run),
            leftover_cancel(returned_run, 2),
        ]),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(2)),
    );
    let pid = serving(&board, 0).await;
    let started = Instant::now();

    let second_answer = submit_shared(&pool, &second, short_work());
    let returned_answer = submit_shared(&pool, &returned_task, short_work());

    assert_eq!(
        answer_of(second_answer).await,
        timed_out(
            RetryPlacement::SameClass,
            TimeoutCancel::Ended(CancelEnd::KilledWithChild)
        )
    );
    assert!(started.elapsed() >= Duration::from_secs(21));
    assert_eq!(answer_of(returned_answer).await, reported(2));
    assert!(process_ended(pid.as_pid_t()));
    stop_shared(pool, spawner).await;
}

/// Python `TestFrozenAfterTheLastCancel::test_a_stuck_run_keeps_its_seat_at_the_stuck_backstop`:
/// the child reported the run stuck, so it is alive; both backstops pass,
/// and the run keeps its seat beside a neighbour with time left.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stuck_run_keeps_its_seat_at_the_stuck_backstop() {
    let stuck = shared_task(1);
    let neighbour = shared_task(2);
    let stuck_run = run_ref(&stuck, 1);
    let script = format!(
        "{}{}{}{}exec sleep 120",
        boot(&shared_async_init(2)),
        read_frame(),
        read_frame(),
        send(&[
            ChildFrame::DeadlineFired(stuck_run),
            ChildFrame::SecondCancel(stuck_run),
            run_stuck(stuck_run),
        ]),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(2)),
    );
    let pid = serving(&board, 0).await;
    let started = Instant::now();
    let mut stuck_answer = submit_shared(&pool, &stuck, short_work());
    let mut neighbour_answer = submit_shared(&pool, &neighbour, long_work());

    tokio::time::sleep_until((started + Duration::from_secs(23)).into()).await;

    assert!(!process_ended(pid.as_pid_t()));
    assert!(not_answered(&mut stuck_answer));
    assert!(not_answered(&mut neighbour_answer));
    let report = pool.stop(Duration::from_millis(500)).await;
    assert_eq!(
        report,
        PoolStopReport::KilledAtDrainDeadline {
            killed: vec![stuck.task_id, neighbour.task_id]
        }
    );
    assert_eq!(
        answer_of(stuck_answer).await,
        timed_out(
            RetryPlacement::Isolated,
            TimeoutCancel::Ended(CancelEnd::KilledWithChild)
        )
    );
    let _ = crash_reason(answer_of(neighbour_answer).await);
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

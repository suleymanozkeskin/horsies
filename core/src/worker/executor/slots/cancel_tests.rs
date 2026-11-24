//! The operator cancel of a running task in the slots: the rules of the
//! stop reports of a shared run after a cancel, and scripted children of
//! each kind that answer `cancel_run`.
//!
//! In each scripted test the run is held, the test routes the cancel
//! through the registry of held dispatches ([`LiveDispatches::request_cancel`])
//! as the cancel router does, and the child answers the `cancel_run` frame
//! that the slot sends.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::dispatch::CancelledEnd;
use super::route::IsolatedCancelStop;
use super::scripted::{
    act_on_runs, answer_of, boot, cancel_fired, claimed_at, isolated_config, isolated_init, layout,
    read_frame, reported, run_cancelled, run_ref, run_stuck, send, shared_async_init,
    shared_cancelled, shared_config, shared_result, shared_thread_init, shell,
    start_isolated_with_live, start_shared_with_live, started_at, submit_isolated,
    submit_isolated_with_cancel_stop, submit_shared, task, ticket, tickets, timeout_ms, until,
    Shown, TestDir, SPAWNER_STOP_WAIT, STATE_WAIT,
};
use super::seat_runs::{
    end_at_loss, end_past_deadlines, CancelSend, CloseCulprit, CloseReason, LossSight,
    OperatorCancel, OutOfTurn, RunStopReport, RunWork, SeatRun, SeatRuns, SharedChildKind,
    StopReportFrame, StopStep,
};
use super::shared_stop::ChildKill;
use super::*;
use crate::events::{AttemptStart, OperatorCancelEnd};
use crate::worker::executor::protocol::{
    CancelGraceMs, CancelledBody, CancelledLoop, ChildAfterFrame, ChildFrame, LeftoverName,
    LoopHoldsTasks, NoLeftWork, ProcessId, RssReading, SeatEnd, SharedResultFrame,
};
use crate::worker::executor::supervisor::{
    ChildExit, ChildLost, LossCause, PartialFrame, ReapedExit, Spawner,
};
use crate::worker::runner_beats::{CancelRouting, LiveDispatches};

/// A long deadline: no deadline or backstop of such a run comes due in a
/// test.
const LONG_MS: u64 = 60_000;

/// The start that the scripted frames report.
fn reported_start() -> AttemptStart {
    AttemptStart::Reported {
        started_at: started_at(),
    }
}

/// The end of a cancelled run that its child reported.
fn cancelled(end: OperatorCancelEnd) -> AttemptEnd {
    AttemptEnd::Cancelled(CancelledEnd::ReportedByChild {
        end,
        start: reported_start(),
    })
}

/// Read the next parent frame; when it is a `cancel_run`, send `frames`,
/// else end the child with exit code 3.
fn on_cancel(frames: &[ChildFrame]) -> String {
    format!(
        "{}case $meta in *cancel_run*) {};; *) exit 3;; esac; ",
        read_frame(),
        send(frames)
    )
}

/// Wait until the registry holds a dispatch of `task`, then route its
/// cancel as the cancel router does.
async fn route_cancel_when_held(live: &LiveDispatches, task: &ClaimedTask) {
    until("the run of the task is held", STATE_WAIT, &[], || {
        live.held_task_ids().contains(&task.task_id)
    })
    .await;
    assert_eq!(
        live.request_cancel(task.task_id, claimed_at()),
        CancelRouting::Routed
    );
}

// ---------------------------------------------------------------------------
// The rules of a shared run after a cancel (pure)
// ---------------------------------------------------------------------------

fn seat(work: RunWork, report: RunStopReport) -> SeatRun<()> {
    SeatRun {
        run: run_ref(&task(1, ExecutionClass::SharedAsync), 1),
        work,
        sent_at: tokio::time::Instant::now(),
        report,
        attached: (),
    }
}

fn async_work() -> RunWork {
    RunWork::AsyncTask {
        timeout: timeout_ms(LONG_MS),
        grace: CancelGraceMs::new(100).unwrap(),
    }
}

fn thread_work() -> RunWork {
    RunWork::ThreadTask {
        timeout: timeout_ms(LONG_MS),
    }
}

fn runs_with(seat: SeatRun<()>) -> SeatRuns<()> {
    let mut runs = SeatRuns::new(NonZeroUsize::new(2).unwrap());
    runs.add(seat);
    runs
}

fn cancelled_result(
    run: crate::worker::executor::protocol::RunRef,
    body: CancelledBody,
) -> SharedResultFrame {
    match shared_cancelled(run, body) {
        ChildFrame::SharedResult(frame) => frame,
        other => panic!("a shared result, not {other:?}"),
    }
}

/// A run whose stop is not reported gets the cancel once; a refusal, a run
/// past its deadline and a run whose body returned get none.
#[test]
fn only_a_run_of_task_code_whose_stop_is_not_reported_gets_the_cancel() {
    let mut runs = runs_with(seat(async_work(), RunStopReport::NotReported));
    let run = runs.iter().next().unwrap().run;
    assert_eq!(runs.note_cancel_sent(run.dispatch), CancelSend::Send(run));
    assert_eq!(runs.note_cancel_sent(run.dispatch), CancelSend::Skip);
    assert_eq!(
        runs.get(run.dispatch).unwrap().report.operator_cancel(),
        OperatorCancel::Sent
    );
    for (work, report) in [
        (RunWork::Refusal, RunStopReport::NotReported),
        (async_work(), RunStopReport::DeadlineReported),
        (thread_work(), RunStopReport::DeadlineReported),
    ] {
        let mut runs = runs_with(seat(work, report.clone()));
        assert_eq!(
            runs.note_cancel_sent(run.dispatch),
            CancelSend::Skip,
            "{work:?} {report:?}"
        );
    }
}

/// `cancel_fired` follows the cancel only; the `cancelled` end follows
/// `cancel_fired` only.
#[test]
fn a_cancel_report_out_of_turn_is_a_violation() {
    let mut runs = runs_with(seat(async_work(), RunStopReport::NotReported));
    let run = runs.iter().next().unwrap().run;
    assert_eq!(
        runs.note_cancel_fired(run).unwrap_err().why,
        OutOfTurn::StepOutOfOrder
    );
    let _sent = runs.note_cancel_sent(run.dispatch);
    assert_eq!(
        runs.end_run(cancelled_result(run, CancelledBody::Ran))
            .unwrap_err()
            .why,
        OutOfTurn::StepOutOfOrder
    );
    runs.note_cancel_fired(run).unwrap();
    assert_eq!(
        runs.note_cancel_fired(run).unwrap_err().why,
        OutOfTurn::StepOutOfOrder
    );
    let (_seat, end) = runs
        .end_run(cancelled_result(run, CancelledBody::Ran))
        .unwrap();
    assert_eq!(end, cancelled(OperatorCancelEnd::ReturnedAfterSignal));
}

/// After `cancel_fired` an async run reports the second cancel and then
/// stuck; its end is dropped after the grace. The second cancel closes the
/// child; the stuck run has no time left.
#[test]
fn an_async_run_alive_after_the_cancel_grace_is_dropped() {
    let mut runs = runs_with(seat(async_work(), RunStopReport::CancelReported));
    let run = runs.iter().next().unwrap().run;
    assert_eq!(
        runs.note_stop_report(&StopReportFrame::SecondCancel(run))
            .unwrap(),
        StopStep::Close(CloseReason::SecondCancel)
    );
    assert!(!runs.all_out_of_time(SharedChildKind::Async));
    let stuck = match run_stuck(run) {
        ChildFrame::RunStuck(stuck) => stuck,
        other => panic!("a stuck report, not {other:?}"),
    };
    assert_eq!(
        runs.note_stop_report(&StopReportFrame::RunStuck(stuck))
            .unwrap(),
        StopStep::Close(CloseReason::SecondCancel)
    );
    assert!(runs.all_out_of_time(SharedChildKind::Async));
    let (_seat, end) = runs
        .end_run(cancelled_result(run, CancelledBody::Ran))
        .unwrap();
    assert_eq!(end, cancelled(OperatorCancelEnd::DroppedAfterGrace));
}

/// A cancelled async run that is not stuck has time left: its child is not
/// killed for it.
#[test]
fn a_cancelled_async_run_with_time_left_is_no_reason_to_kill() {
    for report in [
        RunStopReport::CancelRequested,
        RunStopReport::CancelReported,
        RunStopReport::CancelSecondReported,
    ] {
        assert!(!report.out_of_time(SharedChildKind::Async), "{report:?}");
    }
    assert!(RunStopReport::CancelStuckReported.out_of_time(SharedChildKind::Async));
}

/// The deadline fired in the child before it read the cancel: the deadline
/// report wins, and the run ends as a timeout.
#[test]
fn a_deadline_before_the_cancel_ends_the_run_as_a_timeout() {
    let mut runs = runs_with(seat(async_work(), RunStopReport::NotReported));
    let run = runs.iter().next().unwrap().run;
    let _sent = runs.note_cancel_sent(run.dispatch);
    runs.note_deadline(run).unwrap();
    assert_eq!(
        runs.get(run.dispatch).unwrap().report,
        RunStopReport::DeadlineReported
    );
    let (_seat, end) = runs
        .end_run(SharedResultFrame {
            run,
            end: SeatEnd::TimedOut,
            started_at: reported_start(),
            rss: RssReading::Unreadable,
            child: ChildAfterFrame::ServesOn,
        })
        .unwrap();
    assert!(matches!(end, AttemptEnd::TimedOut { .. }), "{end:?}");
}

/// The cancel came first: the deadline adds nothing to an async run (a
/// deadline report or a timeout end is out of turn), and a thread run runs
/// on past its deadline and ends cancelled.
#[test]
fn after_the_cancel_the_deadline_adds_nothing() {
    let mut runs = runs_with(seat(async_work(), RunStopReport::CancelReported));
    let run = runs.iter().next().unwrap().run;
    assert_eq!(
        runs.note_deadline(run).unwrap_err().why,
        OutOfTurn::StepOutOfOrder
    );
    assert_eq!(
        runs.end_run(SharedResultFrame {
            run,
            end: SeatEnd::TimedOut,
            started_at: reported_start(),
            rss: RssReading::Unreadable,
            child: ChildAfterFrame::ServesOn,
        })
        .unwrap_err()
        .why,
        OutOfTurn::StepOutOfOrder
    );

    let mut threads = runs_with(seat(thread_work(), RunStopReport::CancelReported));
    let _timeout = threads.note_deadline(run).unwrap();
    assert_eq!(
        threads.get(run.dispatch).unwrap().report,
        RunStopReport::CancelDeadlineReported
    );
    assert!(threads.all_out_of_time(SharedChildKind::Thread));
    let (_seat, end) = threads
        .end_run(cancelled_result(run, CancelledBody::Ran))
        .unwrap();
    assert_eq!(end, cancelled(OperatorCancelEnd::ReturnedAfterSignal));
}

/// The body returned before the child read the cancel: the left work is
/// reported as without a cancel, and the run ends with what it returned.
#[test]
fn a_return_before_the_cancel_keeps_what_the_body_returned() {
    let mut runs = runs_with(seat(async_work(), RunStopReport::CancelRequested));
    let run = runs.iter().next().unwrap().run;
    let cancel = match super::scripted::leftover_cancel(run, 4) {
        ChildFrame::LeftoverCancel(cancel) => cancel,
        other => panic!("a leftover cancel, not {other:?}"),
    };
    assert_eq!(
        runs.note_stop_report(&StopReportFrame::LeftoverCancel(cancel))
            .unwrap(),
        StopStep::Close(CloseReason::LeftoverWork)
    );
    assert_eq!(
        runs.get(run.dispatch).unwrap().report.operator_cancel(),
        OperatorCancel::NotSent
    );
}

fn seen_loss(cause: LossCause) -> super::seat_runs::SeenLoss {
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

/// Once the parent sent the cancel, a kill by the worker ends the run as
/// killed after the cancel and any other loss as lost after it, whatever the
/// verdict of the kill would be; a run with no cancel keeps its verdict.
#[test]
fn a_kill_or_a_loss_after_the_cancel_ends_the_run_cancelled() {
    let now = tokio::time::Instant::now();
    for report in [
        RunStopReport::CancelRequested,
        RunStopReport::CancelReported,
        RunStopReport::CancelSecondReported,
        RunStopReport::CancelStuckReported,
    ] {
        let cancelled_seat = seat(async_work(), report.clone());
        assert_eq!(
            end_at_loss(
                &cancelled_seat,
                &ChildKill::NoNamedKill,
                CloseCulprit::NotNamed,
                seen_loss(LossCause::KilledByParent),
                now,
            ),
            AttemptEnd::Cancelled(CancelledEnd::ChildKilled),
            "{report:?}"
        );
        assert_eq!(
            end_at_loss(
                &cancelled_seat,
                &ChildKill::NoNamedKill,
                CloseCulprit::NotNamed,
                seen_loss(LossCause::ChildDied),
                now,
            ),
            AttemptEnd::Cancelled(CancelledEnd::ChildLost),
            "{report:?}"
        );
    }
    assert!(matches!(
        end_at_loss(
            &seat(async_work(), RunStopReport::NotReported),
            &ChildKill::NoNamedKill,
            CloseCulprit::NotNamed,
            seen_loss(LossCause::ChildDied),
            now,
        ),
        AttemptEnd::Crashed { .. }
    ));
    assert_eq!(
        end_past_deadlines(
            &seat(async_work(), RunStopReport::CancelStuckReported),
            SharedChildKind::Async,
            CloseReason::SecondCancel,
        ),
        AttemptEnd::Cancelled(CancelledEnd::ChildKilled)
    );
    assert_eq!(
        end_past_deadlines(
            &seat(thread_work(), RunStopReport::CancelDeadlineReported),
            SharedChildKind::Thread,
            CloseReason::AtDeadline {
                culprit: run_ref(&task(1, ExecutionClass::SharedThread), 1)
            },
        ),
        AttemptEnd::Cancelled(CancelledEnd::ChildKilled)
    );
}

// ---------------------------------------------------------------------------
// Isolated children
// ---------------------------------------------------------------------------

async fn stop_isolated(pool: IsolatedPool, spawner: Spawner) {
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// An isolated child that reads the run frame of `task`, then the cancel
/// frame, then does `after_cancel`; it exits at the stop frame.
fn isolated_child_answering_the_cancel(task: &ClaimedTask, after_cancel: &str) -> String {
    format!(
        "{}{}case $meta in *{}*) ;; *) exit 3;; esac; {}case $meta in *cancel_run*) {after_cancel};; *) exit 3;; esac; {}exit 3",
        boot(&isolated_init()),
        read_frame(),
        task.task_id,
        read_frame(),
        read_frame(),
    )
}

/// The slot sends `cancel_run` to the child of a cancelled run; the child
/// gives the cancel to the body and reports `run_cancelled`; the end is
/// cancelled after the signal, nothing is held, and the child serves on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_isolated_child_gets_the_cancel_and_ends_the_run_cancelled() {
    for (body, end) in [
        (CancelledBody::Ran, OperatorCancelEnd::ReturnedAfterSignal),
        (CancelledBody::NotStarted, OperatorCancelEnd::BeforeBody),
    ] {
        let first = task(1, ExecutionClass::Isolated);
        let script = isolated_child_answering_the_cancel(
            &first,
            &send(&[run_cancelled(
                &first,
                body,
                CancelledLoop::NoLeftWork(NoLeftWork::NoTaskLoop),
            )]),
        );
        let spawner = Spawner::start().unwrap();
        let live = LiveDispatches::default();
        let (pool, _board) =
            start_isolated_with_live(&spawner, 1, isolated_config(shell(&script)), live.clone());

        let receiver = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
        route_cancel_when_held(&live, &first).await;

        assert_eq!(answer_of(receiver).await, cancelled(end), "{body:?}");
        assert_eq!(live.held_task_ids(), Vec::new());
        assert_eq!(
            live.request_cancel(first.task_id, claimed_at()),
            CancelRouting::NotHeld
        );
        stop_isolated(pool, spawner).await;
    }
}

/// The run ended before the child read the cancel: its result is the end,
/// as without a cancel (its write finds the row moved).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_result_sent_before_the_cancel_was_read_is_the_end() {
    let first = task(1, ExecutionClass::Isolated);
    let script = isolated_child_answering_the_cancel(
        &first,
        &send(&[super::scripted::result_frame(&first, 3)]),
    );
    let spawner = Spawner::start().unwrap();
    let live = LiveDispatches::default();
    let (pool, _board) =
        start_isolated_with_live(&spawner, 1, isolated_config(shell(&script)), live.clone());

    let receiver = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    route_cancel_when_held(&live, &first).await;

    assert_eq!(answer_of(receiver).await, reported(3));
    stop_isolated(pool, spawner).await;
}

/// The hard stop: a body that ignores the cancel is killed with its child
/// once the grace after the cancel passed; the end is killed after the
/// cancel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_hard_stop_kills_a_child_that_ignores_the_cancel_after_the_grace() {
    let first = task(1, ExecutionClass::Isolated);
    let script = isolated_child_answering_the_cancel(&first, "sleep 60");
    let spawner = Spawner::start().unwrap();
    let live = LiveDispatches::default();
    let (pool, _board) =
        start_isolated_with_live(&spawner, 1, isolated_config(shell(&script)), live.clone());
    let grace = CancelGraceMs::new(200).unwrap();

    let receiver = submit_isolated_with_cancel_stop(
        &pool,
        &first,
        IsolatedDeadline::NoDeadline,
        IsolatedCancelStop::KillAfterGrace(grace),
    );
    route_cancel_when_held(&live, &first).await;
    let routed_at = Instant::now();

    assert_eq!(
        answer_of(receiver).await,
        AttemptEnd::Cancelled(CancelledEnd::ChildKilled)
    );
    assert!(routed_at.elapsed() >= Duration::from_millis(grace.get()));
    stop_isolated(pool, spawner).await;
}

/// No hard stop by default: a body that ends a while after the cancel is
/// not killed, and its end is cancelled after the signal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_no_hard_stop_a_slow_body_is_not_killed() {
    let first = task(1, ExecutionClass::Isolated);
    let script = isolated_child_answering_the_cancel(
        &first,
        &format!(
            "sleep 0.5; {}",
            send(&[run_cancelled(
                &first,
                CancelledBody::Ran,
                CancelledLoop::NoLeftWork(NoLeftWork::LoopHoldsNoTask),
            )])
        ),
    );
    let spawner = Spawner::start().unwrap();
    let live = LiveDispatches::default();
    let (pool, _board) =
        start_isolated_with_live(&spawner, 1, isolated_config(shell(&script)), live.clone());

    let receiver = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    route_cancel_when_held(&live, &first).await;

    assert_eq!(
        answer_of(receiver).await,
        cancelled(OperatorCancelEnd::ReturnedAfterSignal)
    );
    stop_isolated(pool, spawner).await;
}

/// The cancel, then the deadline: the deadline stays a bound, and the kill
/// at it ends the run as killed after the cancel, not as a timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deadline_after_the_cancel_kills_with_a_cancelled_end() {
    let first = task(1, ExecutionClass::Isolated);
    let script = isolated_child_answering_the_cancel(&first, "sleep 60");
    let spawner = Spawner::start().unwrap();
    let live = LiveDispatches::default();
    let (pool, _board) =
        start_isolated_with_live(&spawner, 1, isolated_config(shell(&script)), live.clone());

    let receiver = submit_isolated(&pool, &first, IsolatedDeadline::After(timeout_ms(1_000)));
    route_cancel_when_held(&live, &first).await;

    assert_eq!(
        answer_of(receiver).await,
        AttemptEnd::Cancelled(CancelledEnd::ChildKilled)
    );
    stop_isolated(pool, spawner).await;
}

/// The deadline, then the cancel: the kill at the deadline ends the run as
/// a timeout, and a cancel after it finds no held run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deadline_before_the_cancel_is_a_timeout() {
    let first = task(1, ExecutionClass::Isolated);
    let script = format!("{}{}sleep 60", boot(&isolated_init()), read_frame());
    let spawner = Spawner::start().unwrap();
    let live = LiveDispatches::default();
    let (pool, _board) =
        start_isolated_with_live(&spawner, 1, isolated_config(shell(&script)), live.clone());

    let end = answer_of(submit_isolated(
        &pool,
        &first,
        IsolatedDeadline::After(timeout_ms(500)),
    ))
    .await;

    assert!(matches!(end, AttemptEnd::TimedOut { .. }), "{end:?}");
    assert_eq!(
        live.request_cancel(first.task_id, claimed_at()),
        CancelRouting::NotHeld
    );
    stop_isolated(pool, spawner).await;
}

/// Work that a cancelled body left on its loop retires the child: the next
/// task runs on a new child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn work_left_by_a_cancelled_body_retires_the_child() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let left =
        LoopHoldsTasks::new(NonZeroU64::MIN, vec![LeftoverName::cut("_write_after")]).unwrap();
    let first_child = isolated_child_answering_the_cancel(
        &first,
        &send(&[run_cancelled(
            &first,
            CancelledBody::Ran,
            CancelledLoop::WorkLeft(left),
        )]),
    );
    let second_child = format!(
        "{}{}",
        boot(&isolated_init()),
        super::scripted::answer_runs(
            &[(&second, vec![super::scripted::result_frame(&second, 2)])],
            1
        )
    );
    let script = format!(
        "{}case $n in 1) {first_child};; *) {second_child};; esac",
        ticket(&dir)
    );
    let spawner = Spawner::start().unwrap();
    let live = LiveDispatches::default();
    let (pool, board) =
        start_isolated_with_live(&spawner, 1, isolated_config(shell(&script)), live.clone());

    let receiver = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    route_cancel_when_held(&live, &first).await;
    assert_eq!(
        answer_of(receiver).await,
        cancelled(OperatorCancelEnd::ReturnedAfterSignal)
    );
    assert_eq!(
        answer_of(submit_isolated(
            &pool,
            &second,
            IsolatedDeadline::NoDeadline
        ))
        .await,
        reported(2)
    );
    until(
        "a second child ran the next task",
        STATE_WAIT,
        &[Shown::Board(&board), Shown::Tickets(&dir)],
        || tickets(&dir) == 2,
    )
    .await;
    stop_isolated(pool, spawner).await;
}

// ---------------------------------------------------------------------------
// Shared children
// ---------------------------------------------------------------------------

async fn stop_shared(pool: SharedPool, spawner: Spawner) {
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// A shared pool of one child of `kind` with `seats` seats, on `live`.
fn shared_pool(
    spawner: &Spawner,
    kind: ExecutionClass,
    seats: u64,
    script: &str,
    live: &LiveDispatches,
) -> (SharedPool, Arc<PoolBoard>) {
    let (board_layout, init) = match kind {
        ExecutionClass::SharedAsync => (layout(0, 1, 0), shared_async_init(seats)),
        ExecutionClass::SharedThread => (layout(0, 0, 1), shared_thread_init(seats)),
        ExecutionClass::Isolated => panic!("a shared pool has a shared kind"),
    };
    let board = Arc::new(PoolBoard::new(board_layout));
    let pool = start_shared_with_live(
        spawner,
        &board,
        shared_config(shell(script), init),
        live.clone(),
    );
    (pool, board)
}

fn work_of(kind: ExecutionClass) -> crate::worker::executor::protocol::SharedWork {
    match kind {
        ExecutionClass::SharedAsync => super::scripted::async_work(LONG_MS, 100),
        ExecutionClass::SharedThread => super::scripted::thread_work(LONG_MS),
        ExecutionClass::Isolated => panic!("a shared run has a shared kind"),
    }
}

/// The slot sends `cancel_run` to the shared child of a cancelled run; the
/// child answers `cancel_fired` and ends the run cancelled. Each body end,
/// in an async and a thread child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_child_gets_the_cancel_and_ends_the_run_cancelled() {
    for kind in [ExecutionClass::SharedAsync, ExecutionClass::SharedThread] {
        for (body, end) in [
            (CancelledBody::Ran, OperatorCancelEnd::ReturnedAfterSignal),
            (CancelledBody::NotStarted, OperatorCancelEnd::BeforeBody),
        ] {
            let first = task(1, kind);
            let run = run_ref(&first, 1);
            let init = match kind {
                ExecutionClass::SharedThread => shared_thread_init(2),
                ExecutionClass::SharedAsync | ExecutionClass::Isolated => shared_async_init(2),
            };
            let script = format!(
                "{}{}",
                boot(&init),
                act_on_runs(
                    &[(
                        &first,
                        on_cancel(&[cancel_fired(run), shared_cancelled(run, body)])
                    )],
                    1
                )
            );
            let spawner = Spawner::start().unwrap();
            let live = LiveDispatches::default();
            let (pool, _board) = shared_pool(&spawner, kind, 2, &script, &live);

            let receiver = submit_shared(&pool, &first, work_of(kind));
            route_cancel_when_held(&live, &first).await;

            assert_eq!(answer_of(receiver).await, cancelled(end), "{kind} {body:?}");
            assert_eq!(live.held_task_ids(), Vec::new());
            stop_shared(pool, spawner).await;
        }
    }
}

/// An async run alive after the cancel grace: the child sends the second
/// cancel, and the end is dropped after the grace.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_async_run_alive_after_the_grace_ends_dropped() {
    let first = task(1, ExecutionClass::SharedAsync);
    let run = run_ref(&first, 1);
    let script = format!(
        "{}{}",
        boot(&shared_async_init(2)),
        act_on_runs(
            &[(
                &first,
                on_cancel(&[
                    cancel_fired(run),
                    ChildFrame::SecondCancel(run),
                    shared_cancelled(run, CancelledBody::Ran),
                ])
            )],
            1
        )
    );
    let spawner = Spawner::start().unwrap();
    let live = LiveDispatches::default();
    let (pool, _board) = shared_pool(&spawner, ExecutionClass::SharedAsync, 2, &script, &live);

    let receiver = submit_shared(&pool, &first, work_of(ExecutionClass::SharedAsync));
    route_cancel_when_held(&live, &first).await;

    assert_eq!(
        answer_of(receiver).await,
        cancelled(OperatorCancelEnd::DroppedAfterGrace)
    );
    stop_shared(pool, spawner).await;
}

/// A `cancelled` end with no `cancel_fired` first breaks the frame rules:
/// the child is killed, and the run, which the parent sent the cancel to,
/// ends as lost after the cancel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_end_without_cancel_fired_is_a_violation() {
    let first = task(1, ExecutionClass::SharedAsync);
    let run = run_ref(&first, 1);
    let script = format!(
        "{}{}",
        boot(&shared_async_init(2)),
        act_on_runs(
            &[(
                &first,
                format!(
                    "{}sleep 60",
                    on_cancel(&[shared_cancelled(run, CancelledBody::Ran)])
                )
            )],
            1
        )
    );
    let spawner = Spawner::start().unwrap();
    let live = LiveDispatches::default();
    let (pool, _board) = shared_pool(&spawner, ExecutionClass::SharedAsync, 2, &script, &live);

    let receiver = submit_shared(&pool, &first, work_of(ExecutionClass::SharedAsync));
    route_cancel_when_held(&live, &first).await;

    assert_eq!(
        answer_of(receiver).await,
        AttemptEnd::Cancelled(CancelledEnd::ChildLost)
    );
    stop_shared(pool, spawner).await;
}

/// A neighbour of a cancelled async run that does not stop is not killed
/// before it ends: the child is killed only once no run in it has time
/// left, and the stuck cancelled run then ends as killed after the cancel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_neighbour_of_a_stuck_cancelled_run_ends_in_its_own_time() {
    let stuck_task = task(1, ExecutionClass::SharedAsync);
    let neighbour = task(2, ExecutionClass::SharedAsync);
    let stuck = run_ref(&stuck_task, 1);
    let script = format!(
        "{}{}",
        boot(&shared_async_init(2)),
        act_on_runs(
            &[
                (&stuck_task, ":".to_owned()),
                (
                    &neighbour,
                    format!(
                        "{}sleep 0.5; {}sleep 60",
                        on_cancel(&[
                            cancel_fired(stuck),
                            ChildFrame::SecondCancel(stuck),
                            run_stuck(stuck),
                        ]),
                        send(&[shared_result(run_ref(&neighbour, 2), 2)]),
                    )
                ),
            ],
            2
        )
    );
    let spawner = Spawner::start().unwrap();
    let live = LiveDispatches::default();
    let (pool, _board) = shared_pool(&spawner, ExecutionClass::SharedAsync, 2, &script, &live);

    let stuck_answer = submit_shared(&pool, &stuck_task, work_of(ExecutionClass::SharedAsync));
    let neighbour_answer = submit_shared(&pool, &neighbour, work_of(ExecutionClass::SharedAsync));
    until("both runs are held", STATE_WAIT, &[], || {
        live.held_task_ids().len() == 2
    })
    .await;
    assert_eq!(
        live.request_cancel(stuck_task.task_id, claimed_at()),
        CancelRouting::Routed
    );

    assert_eq!(answer_of(neighbour_answer).await, reported(2));
    assert_eq!(
        answer_of(stuck_answer).await,
        AttemptEnd::Cancelled(CancelledEnd::ChildKilled)
    );
    stop_shared(pool, spawner).await;
}

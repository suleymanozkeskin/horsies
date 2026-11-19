//! Tests of the rules of shared children: verdicts at a kill, stop reports,
//! ends at a loss, backstops, placement and recycle.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use tokio::time::Instant;
use uuid::Uuid;

use super::dispatch::{AttemptEnd, CancelEnd, LoopKillCode, TimeoutCancel};
use super::pool::{MemoryLimit, TaskLimit};
use super::seat_runs::*;
use super::shared_stop::*;
use crate::broker::attempt_writes::RetryPlacement;
use crate::worker::executor::protocol::{
    BlockedWhere, BodyEnd, CancelGraceMs, ChildAfterFrame, DispatchNumber, LeftoverCancelFrame,
    LeftoverName, LeftoverStuckFrame, LoopBlockedFrame, LoopCulprit, ProcessId, ResultEnvelopeText,
    RssMb, RssReading, RunEnd, RunRef, RunStuckFrame, SeatEnd, SharedResultFrame, StuckWork,
    TaskTimeoutMs,
};
use crate::worker::executor::supervisor::{
    ChildExit, ChildLost, LossCause, PartialFrame, ReapedExit,
};

fn run_ref(dispatch: u64) -> RunRef {
    RunRef {
        dispatch: DispatchNumber::new(dispatch).unwrap(),
        task_id: Uuid::from_u128(u128::from(dispatch)),
    }
}

fn timeout() -> TaskTimeoutMs {
    TaskTimeoutMs::new(1_000).unwrap()
}

fn async_work() -> RunWork {
    RunWork::AsyncTask {
        timeout: timeout(),
        grace: CancelGraceMs::new(500).unwrap(),
    }
}

fn thread_work() -> RunWork {
    RunWork::ThreadTask { timeout: timeout() }
}

fn seat(dispatch: u64, work: RunWork, sent_at: Instant) -> SeatRun<()> {
    SeatRun {
        run: run_ref(dispatch),
        work,
        sent_at,
        report: RunStopReport::NotReported,
        attached: (),
    }
}

fn runs_with(seats: usize, entries: Vec<SeatRun<()>>) -> SeatRuns<()> {
    let mut runs = SeatRuns::new(NonZeroUsize::new(seats).unwrap());
    for entry in entries {
        runs.add(entry);
    }
    runs
}

fn returned() -> BodyEnd {
    BodyEnd::Result(ResultEnvelopeText::new(
        r#"{"__h_task_result__":true,"ok":1,"err":null}"#.to_owned(),
    ))
}

fn stuck_work() -> StuckWork {
    StuckWork::new(1, 0, vec![LeftoverName::cut("leftover")]).unwrap()
}

fn loss(cause: LossCause, exit: ChildExit) -> ChildLost {
    ChildLost {
        cause,
        exit,
        partial: PartialFrame::NoneAtHand,
        pid: ProcessId::new(4242).unwrap(),
    }
}

fn seen(cause: LossCause) -> SeenLoss {
    match LossSight::of(loss(
        cause,
        ChildExit::Reaped(ReapedExit::KilledBySignal(libc::SIGKILL)),
    )) {
        LossSight::Seen(seen) => seen,
        LossSight::NotSeen(lost) => panic!("a reaped exit is seen: {lost}"),
    }
}

fn blocked_kill(holder: LoopHolder) -> ChildKill {
    ChildKill::BlockedLoop {
        holder,
        blocked_ms: 5_200,
        location: BlockedWhere::cut("app/tasks.py:12"),
    }
}

// ---------------------------------------------------------------------------
// Verdicts at a kill
// ---------------------------------------------------------------------------

#[test]
fn the_holder_of_a_blocked_loop_retries_isolated() {
    let holder = run_ref(1);
    let kill = blocked_kill(LoopHolder::Run(holder));
    assert_eq!(
        verdict_at_kill(&kill, holder.dispatch, DeadlineState::Pending),
        KillVerdict::Verdict(KilledRunVerdict::BlockedTheLoop {
            blocked_ms: 5_200,
            location: BlockedWhere::cut("app/tasks.py:12"),
        })
    );
    for deadline in [DeadlineState::Passed, DeadlineState::Culprit] {
        assert_eq!(
            verdict_at_kill(&kill, holder.dispatch, deadline),
            KillVerdict::Verdict(KilledRunVerdict::TimedOutAtKill {
                retry: RetryPlacement::Isolated
            })
        );
    }
}

#[test]
fn every_other_run_gets_the_verdict_of_its_deadline() {
    let neighbour = run_ref(2).dispatch;
    let named = blocked_kill(LoopHolder::Run(run_ref(1)));
    let unknown = blocked_kill(LoopHolder::Unknown);
    for kill in [&named, &unknown, &ChildKill::NoNamedKill] {
        assert_eq!(
            verdict_at_kill(kill, neighbour, DeadlineState::Culprit),
            KillVerdict::Verdict(KilledRunVerdict::TimedOutAtKill {
                retry: RetryPlacement::Isolated
            })
        );
        assert_eq!(
            verdict_at_kill(kill, neighbour, DeadlineState::Passed),
            KillVerdict::Verdict(KilledRunVerdict::TimedOutAtKill {
                retry: RetryPlacement::SameClass
            })
        );
    }
    assert_eq!(
        verdict_at_kill(&named, neighbour, DeadlineState::Pending),
        KillVerdict::Verdict(KilledRunVerdict::BesideBlockedLoop {
            holder_task_id: run_ref(1).task_id,
            blocked_ms: 5_200,
        })
    );
    assert_eq!(
        verdict_at_kill(&unknown, neighbour, DeadlineState::Pending),
        KillVerdict::CrashPath
    );
    assert_eq!(
        verdict_at_kill(&ChildKill::NoNamedKill, neighbour, DeadlineState::Pending),
        KillVerdict::CrashPath
    );
}

#[test]
fn only_the_culprit_of_a_kill_past_deadlines_retries_isolated() {
    let culprit = run_ref(3);
    assert_eq!(
        culprit_retry(culprit, culprit.dispatch),
        RetryPlacement::Isolated
    );
    assert_eq!(
        culprit_retry(culprit, run_ref(4).dispatch),
        RetryPlacement::SameClass
    );
}

// ---------------------------------------------------------------------------
// Stop reports
// ---------------------------------------------------------------------------

#[test]
fn an_async_run_reports_its_stop_in_order() {
    let now = Instant::now();
    let mut runs = runs_with(2, vec![seat(1, async_work(), now)]);
    let run = run_ref(1);
    assert_eq!(runs.note_deadline(run), Ok(timeout()));
    assert_eq!(
        runs.note_stop_report(&StopReportFrame::SecondCancel(run)),
        Ok(StopStep::Close(CloseReason::SecondCancel))
    );
    assert!(!runs.all_out_of_time(SharedChildKind::Async));
    assert_eq!(
        runs.note_stop_report(&StopReportFrame::RunStuck(RunStuckFrame {
            run,
            work: stuck_work(),
        })),
        Ok(StopStep::Close(CloseReason::AtDeadline { culprit: run }))
    );
    assert!(runs.all_out_of_time(SharedChildKind::Async));
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
            timeout: timeout(),
            retry: RetryPlacement::Isolated,
            cancel: TimeoutCancel::Ended(CancelEnd::DroppedAfterGrace),
        }
    );
    assert_eq!(runs.count(), 0);
}

#[test]
fn a_step_out_of_order_or_of_another_kind_is_refused() {
    let now = Instant::now();
    let mut runs = runs_with(
        3,
        vec![
            seat(1, async_work(), now),
            seat(2, thread_work(), now),
            seat(3, RunWork::Refusal, now),
        ],
    );
    let refused = |result: Result<StopStep, FrameOutOfTurn>| result.unwrap_err().why;
    assert_eq!(
        refused(runs.note_stop_report(&StopReportFrame::SecondCancel(run_ref(1)))),
        OutOfTurn::StepOutOfOrder
    );
    assert_eq!(
        refused(runs.note_stop_report(&StopReportFrame::SecondCancel(run_ref(2)))),
        OutOfTurn::NoSuchStep
    );
    assert_eq!(
        runs.note_deadline(run_ref(3)).unwrap_err().why,
        OutOfTurn::NoSuchStep
    );
    assert_eq!(
        runs.note_deadline(run_ref(9)).unwrap_err().why,
        OutOfTurn::NoRunInFlight
    );
    let other_task = RunRef {
        dispatch: run_ref(1).dispatch,
        task_id: Uuid::from_u128(77),
    };
    assert_eq!(
        runs.note_deadline(other_task).unwrap_err().why,
        OutOfTurn::AnotherTask
    );
    assert_eq!(runs.note_deadline(run_ref(2)), Ok(timeout()));
    assert_eq!(
        runs.note_deadline(run_ref(2)).unwrap_err().why,
        OutOfTurn::StepOutOfOrder
    );
    assert!(!runs.all_out_of_time(SharedChildKind::Thread));
}

#[test]
fn a_return_with_left_work_keeps_what_it_returned() {
    let now = Instant::now();
    let mut runs = runs_with(1, vec![seat(1, async_work(), now)]);
    let run = run_ref(1);
    assert_eq!(
        runs.note_stop_report(&StopReportFrame::LeftoverCancel(LeftoverCancelFrame {
            run,
            returned: returned(),
        })),
        Ok(StopStep::Close(CloseReason::LeftoverWork))
    );
    assert_eq!(
        runs.note_stop_report(&StopReportFrame::LeftoverStuck(LeftoverStuckFrame {
            run,
            work: stuck_work(),
            returned: returned(),
        })),
        Ok(StopStep::Recorded)
    );
    assert!(runs.all_out_of_time(SharedChildKind::Async));
    // A timeout end after a return is out of turn.
    assert_eq!(
        runs.end_run(SharedResultFrame {
            run,
            end: SeatEnd::TimedOut,
            rss: RssReading::Unreadable,
            child: ChildAfterFrame::ServesOn,
        })
        .unwrap_err()
        .why,
        OutOfTurn::NoSuchStep
    );
    let (_, end) = runs
        .end_run(SharedResultFrame {
            run,
            end: SeatEnd::Finished(RunEnd::from(returned())),
            rss: RssReading::Unreadable,
            child: ChildAfterFrame::ServesOn,
        })
        .unwrap();
    assert_eq!(end, AttemptEnd::Reported(RunEnd::from(returned())));
}

#[test]
fn a_refusal_never_ends_at_a_timeout() {
    let mut runs = runs_with(1, vec![seat(1, RunWork::Refusal, Instant::now())]);
    assert_eq!(
        runs.end_run(SharedResultFrame {
            run: run_ref(1),
            end: SeatEnd::TimedOut,
            rss: RssReading::Unreadable,
            child: ChildAfterFrame::ServesOn,
        })
        .unwrap_err()
        .why,
        OutOfTurn::NoSuchStep
    );
}

#[test]
fn a_thread_run_is_out_of_time_once_its_deadline_is_reported() {
    let now = Instant::now();
    let mut runs = runs_with(2, vec![seat(1, thread_work(), now)]);
    assert!(!runs.all_out_of_time(SharedChildKind::Thread));
    assert_eq!(runs.note_deadline(run_ref(1)), Ok(timeout()));
    assert!(runs.all_out_of_time(SharedChildKind::Thread));
    runs.add(seat(2, thread_work(), now));
    assert!(!runs.all_out_of_time(SharedChildKind::Thread));
    let empty: SeatRuns<()> = SeatRuns::new(NonZeroUsize::MIN);
    assert!(!empty.all_out_of_time(SharedChildKind::Thread));
}

#[test]
fn a_blocked_loop_names_its_holder_once() {
    let now = Instant::now();
    let mut runs = runs_with(2, vec![seat(1, async_work(), now)]);
    let frame = |culprit| LoopBlockedFrame {
        culprit,
        blocked_ms: 5_200,
        location: BlockedWhere::cut("app/tasks.py:12"),
    };
    let other_task = RunRef {
        dispatch: run_ref(1).dispatch,
        task_id: Uuid::from_u128(77),
    };
    assert_eq!(
        runs.note_loop_blocked(&frame(LoopCulprit::Run(other_task)))
            .unwrap_err()
            .why,
        OutOfTurn::AnotherTask
    );
    assert_eq!(
        runs.note_loop_blocked(&frame(LoopCulprit::Run(run_ref(1)))),
        Ok(LoopBlockStep::KillNow {
            holder: LoopHolder::Run(run_ref(1))
        })
    );
    assert_eq!(runs.kill(), &blocked_kill(LoopHolder::Run(run_ref(1))));
    assert_eq!(
        runs.note_loop_blocked(&frame(LoopCulprit::Unknown)),
        Ok(LoopBlockStep::AlreadyKilled)
    );
    let mut ended = runs_with(1, vec![seat(1, async_work(), now)]);
    assert_eq!(
        ended.note_loop_blocked(&frame(LoopCulprit::Run(run_ref(5)))),
        Ok(LoopBlockStep::KillNow {
            holder: LoopHolder::Unknown
        })
    );
}

// ---------------------------------------------------------------------------
// The service of a child
// ---------------------------------------------------------------------------

#[test]
fn a_child_closed_at_a_deadline_keeps_its_culprit() {
    let mut service = ChildService::Open;
    service.close_at_deadline(SharedChildKind::Async, run_ref(1));
    assert_eq!(service, ChildService::Open);
    service.close_for_stop(CloseReason::SecondCancel);
    assert_eq!(service, ChildService::Closing(CloseReason::SecondCancel));
    service.close_at_deadline(SharedChildKind::Thread, run_ref(1));
    service.close_for_stop(CloseReason::AtDeadline {
        culprit: run_ref(2),
    });
    service.close_for_stop(CloseReason::LoopBlocked);
    assert_eq!(service.close_culprit(), CloseCulprit::Named(run_ref(1)));
    service.close_for_pool_stop();
    service.close_after_task(Recycle::Close(CloseReason::TaskCount));
    assert_eq!(
        service,
        ChildService::Closing(CloseReason::AtDeadline {
            culprit: run_ref(1)
        })
    );
    let mut open = ChildService::Open;
    open.close_after_task(Recycle::ServeOn);
    assert_eq!(open.close_culprit(), CloseCulprit::NotNamed);
    open.close_for_pool_stop();
    assert_eq!(open, ChildService::Closing(CloseReason::PoolStopping));
}

// ---------------------------------------------------------------------------
// Ends at a loss or a kill
// ---------------------------------------------------------------------------

#[test]
fn the_deadline_state_comes_from_the_report_the_clock_and_the_closer() {
    let sent_at = Instant::now();
    let mut run = seat(1, async_work(), sent_at);
    let before = sent_at + Duration::from_millis(999);
    let after = sent_at + Duration::from_millis(1_000);
    assert_eq!(
        deadline_state(&run, CloseCulprit::NotNamed, before),
        DeadlineState::Pending
    );
    assert_eq!(
        deadline_state(&run, CloseCulprit::NotNamed, after),
        DeadlineState::Passed
    );
    assert_eq!(
        deadline_state(&run, CloseCulprit::Named(run_ref(1)), before),
        DeadlineState::Culprit
    );
    run.report = RunStopReport::DeadlineReported;
    assert_eq!(
        deadline_state(&run, CloseCulprit::NotNamed, before),
        DeadlineState::Passed
    );
    run.report = RunStopReport::StuckReported;
    assert_eq!(
        deadline_state(&run, CloseCulprit::NotNamed, before),
        DeadlineState::Culprit
    );
    run.report = RunStopReport::LeftoverStuckReported {
        returned: returned(),
    };
    assert_eq!(
        deadline_state(&run, CloseCulprit::NotNamed, after),
        DeadlineState::Pending
    );
    let refusal = seat(2, RunWork::Refusal, sent_at);
    assert_eq!(
        deadline_state(&refusal, CloseCulprit::NotNamed, after),
        DeadlineState::Pending
    );
}

#[test]
fn a_run_of_a_lost_child_gets_the_verdict_of_its_kill() {
    let sent_at = Instant::now();
    let now = sent_at + Duration::from_millis(500);
    let pending = seat(2, async_work(), sent_at);
    let mut past = seat(3, async_work(), sent_at);
    past.report = RunStopReport::DeadlineReported;
    let holder = seat(1, async_work(), sent_at);
    let kill = blocked_kill(LoopHolder::Run(holder.run));
    let killed = seen(LossCause::KilledByParent);

    match end_at_loss(&holder, &kill, CloseCulprit::NotNamed, killed, now) {
        AttemptEnd::KilledForLoop { code, retry, .. } => {
            assert_eq!(code, LoopKillCode::TaskBlockedLoop);
            assert_eq!(retry, RetryPlacement::Isolated);
        }
        other => panic!("expected the blocked-loop verdict, got {other:?}"),
    }
    match end_at_loss(&pending, &kill, CloseCulprit::NotNamed, killed, now) {
        AttemptEnd::KilledForLoop {
            code,
            retry,
            reason,
        } => {
            assert_eq!(code, LoopKillCode::NeighbourBlockedLoop);
            assert_eq!(retry, RetryPlacement::SameClass);
            assert!(reason.as_str().contains("pid=4242"), "{reason}");
        }
        other => panic!("expected the neighbour verdict, got {other:?}"),
    }
    assert_eq!(
        end_at_loss(&past, &kill, CloseCulprit::NotNamed, killed, now),
        AttemptEnd::TimedOut {
            timeout: timeout(),
            retry: RetryPlacement::SameClass,
            cancel: TimeoutCancel::Ended(CancelEnd::KilledWithChild),
        }
    );
    assert!(matches!(
        end_at_loss(
            &pending,
            &ChildKill::NoNamedKill,
            CloseCulprit::NotNamed,
            killed,
            now
        ),
        AttemptEnd::Crashed { .. }
    ));
}

#[test]
fn a_death_the_worker_did_not_cause_takes_the_crash_path() {
    let sent_at = Instant::now();
    let mut past = seat(1, thread_work(), sent_at);
    past.report = RunStopReport::DeadlineReported;
    let died = seen(LossCause::ChildDied);
    assert!(matches!(
        end_at_loss(
            &past,
            &ChildKill::NoNamedKill,
            CloseCulprit::Named(past.run),
            died,
            sent_at
        ),
        AttemptEnd::Crashed { .. }
    ));
    let mut leftover = seat(2, async_work(), sent_at);
    leftover.report = RunStopReport::LeftoverCancelReported {
        returned: returned(),
    };
    for cause in [LossCause::ChildDied, LossCause::KilledByParent] {
        assert_eq!(
            end_at_loss(
                &leftover,
                &ChildKill::NoNamedKill,
                CloseCulprit::NotNamed,
                seen(cause),
                sent_at
            ),
            AttemptEnd::Reported(RunEnd::from(returned()))
        );
    }
    assert!(matches!(
        LossSight::of(loss(LossCause::KilledByParent, ChildExit::ExitUnknown)),
        LossSight::NotSeen(_)
    ));
}

#[test]
fn a_kill_past_deadlines_isolates_only_the_culprit() {
    let sent_at = Instant::now();
    let mut culprit = seat(1, thread_work(), sent_at);
    culprit.report = RunStopReport::DeadlineReported;
    let mut other = seat(2, thread_work(), sent_at);
    other.report = RunStopReport::DeadlineReported;
    let reason = CloseReason::AtDeadline {
        culprit: culprit.run,
    };
    assert_eq!(
        end_past_deadlines(&culprit, SharedChildKind::Thread, reason),
        AttemptEnd::TimedOut {
            timeout: timeout(),
            retry: RetryPlacement::Isolated,
            cancel: TimeoutCancel::NoSignal,
        }
    );
    assert_eq!(
        end_past_deadlines(&other, SharedChildKind::Thread, reason),
        AttemptEnd::TimedOut {
            timeout: timeout(),
            retry: RetryPlacement::SameClass,
            cancel: TimeoutCancel::NoSignal,
        }
    );
    let mut stuck = seat(3, async_work(), sent_at);
    stuck.report = RunStopReport::StuckReported;
    assert_eq!(
        end_past_deadlines(&stuck, SharedChildKind::Async, CloseReason::SecondCancel),
        AttemptEnd::TimedOut {
            timeout: timeout(),
            retry: RetryPlacement::Isolated,
            cancel: TimeoutCancel::Ended(CancelEnd::KilledWithChild),
        }
    );
    let mut leftover = seat(4, async_work(), sent_at);
    leftover.report = RunStopReport::LeftoverStuckReported {
        returned: returned(),
    };
    assert_eq!(
        end_past_deadlines(&leftover, SharedChildKind::Async, CloseReason::LeftoverWork),
        AttemptEnd::Reported(RunEnd::from(returned()))
    );
}

#[test]
#[should_panic(expected = "had time left")]
fn a_kill_past_deadlines_of_a_run_with_time_left_is_a_defect() {
    let run = seat(1, async_work(), Instant::now());
    let _ = end_past_deadlines(&run, SharedChildKind::Async, CloseReason::SecondCancel);
}

#[test]
fn a_late_exit_after_the_first_backstop_is_a_timeout_or_a_crash() {
    let killed = seen(LossCause::KilledByParent);
    assert_eq!(
        end_at_late_backstop_exit(thread_work(), run_ref(1), killed),
        AttemptEnd::TimedOut {
            timeout: timeout(),
            retry: RetryPlacement::SameClass,
            cancel: TimeoutCancel::NoSignal,
        }
    );
    assert!(matches!(
        end_at_late_backstop_exit(RunWork::Refusal, run_ref(2), killed),
        AttemptEnd::Crashed { .. }
    ));
}

// ---------------------------------------------------------------------------
// Backstops
// ---------------------------------------------------------------------------

#[test]
fn the_backstops_keep_their_times() {
    assert_eq!(LOOP_BLOCK_REPORT_WAIT, Duration::from_secs(5));
    assert_eq!(SHARED_BACKSTOP_MARGIN, Duration::from_secs(10));
    assert_eq!(SHARED_STUCK_BACKSTOP, Duration::from_millis(10_300));
    assert_eq!(SHARED_REFUSAL_BACKSTOP, Duration::from_secs(155));
    assert_eq!(first_backstop(async_work()), Duration::from_millis(11_500));
    assert_eq!(first_backstop(thread_work()), Duration::from_secs(11));
    assert_eq!(first_backstop(RunWork::Refusal), Duration::from_secs(155));
}

/// Python `TestRefusalBound::test_the_refusal_backstop_comes_after_the_writes_and_a_block_report`:
/// the refusal backstop is the longest bound of the writes of a refusal in
/// a child of any kind, each start attempt and the finalizing marker with
/// the end of a cancelled statement, and the margin. A refusal whose writes
/// end within the bound of its child kind never meets it.
#[test]
fn the_refusal_backstop_comes_after_the_refusal_writes() {
    use crate::broker::child_writes::{
        CHILD_STATEMENT_BOUND, CHILD_WRITE_CHECKOUT_WAIT, FINALIZING_MARKER_END,
        PRE_START_CONFIRM_ATTEMPTS, START_ATTEMPT_END,
    };
    use crate::db::CANCEL_REQUEST_BOUND;

    let attempts = START_ATTEMPT_END * PRE_START_CONFIRM_ATTEMPTS;
    let async_child_writes = attempts + FINALIZING_MARKER_END;
    let thread_child_writes =
        attempts + CHILD_WRITE_CHECKOUT_WAIT + CHILD_STATEMENT_BOUND + CANCEL_REQUEST_BOUND;
    assert_eq!(
        SHARED_REFUSAL_BACKSTOP,
        async_child_writes.max(thread_child_writes) + SHARED_BACKSTOP_MARGIN
    );
    for writes in [async_child_writes, thread_child_writes] {
        assert!(SHARED_REFUSAL_BACKSTOP > writes, "{writes:?}");
    }
}

#[test]
fn a_backstop_kills_only_a_child_that_did_not_report_the_due_step() {
    let at = Instant::now();
    let first = Backstop::First { at };
    let stuck = Backstop::Stuck { at };
    assert_eq!(
        backstop_step(first, async_work(), &RunStopReport::DeadlineReported),
        BackstopStep::KillChild
    );
    assert_eq!(
        backstop_step(first, async_work(), &RunStopReport::SecondCancelReported),
        BackstopStep::CheckStuckLater
    );
    assert_eq!(
        backstop_step(
            first,
            async_work(),
            &RunStopReport::LeftoverCancelReported {
                returned: returned()
            }
        ),
        BackstopStep::CheckStuckLater
    );
    assert_eq!(
        backstop_step(first, thread_work(), &RunStopReport::NotReported),
        BackstopStep::KillChild
    );
    assert_eq!(
        backstop_step(first, thread_work(), &RunStopReport::DeadlineReported),
        BackstopStep::WaitForEnd
    );
    assert_eq!(
        backstop_step(first, RunWork::Refusal, &RunStopReport::NotReported),
        BackstopStep::KillChild
    );
    assert_eq!(
        backstop_step(stuck, async_work(), &RunStopReport::SecondCancelReported),
        BackstopStep::KillChild
    );
    assert_eq!(
        backstop_step(stuck, async_work(), &RunStopReport::StuckReported),
        BackstopStep::WaitForEnd
    );
    assert_eq!(Backstop::Done.due_at(), None);
    assert_eq!(
        Backstop::after_send(thread_work(), at).due_at(),
        Some(at + Duration::from_secs(11))
    );
}

// ---------------------------------------------------------------------------
// Placement and recycle
// ---------------------------------------------------------------------------

#[test]
fn a_task_goes_to_the_open_child_with_the_fewest_runs() {
    let seats = NonZeroUsize::new(2).unwrap();
    let load = |slot, runs| OpenChildLoad { slot, runs, seats };
    assert_eq!(
        place_on_shared_child(&[load(4, 1), load(5, 0), load(6, 0)]),
        Placement::OnSlot(5)
    );
    assert_eq!(
        place_on_shared_child(&[load(4, 2), load(5, 2)]),
        Placement::NoSeatFree
    );
    assert_eq!(place_on_shared_child(&[]), Placement::NoSeatFree);
}

#[test]
fn a_shared_child_recycles_by_task_count_per_seat_or_memory() {
    let seats = NonZeroUsize::new(4).unwrap();
    let per_seat = TaskLimit::After(NonZeroU64::new(100).unwrap());
    let memory = MemoryLimit::AtMb(NonZeroU64::new(512).unwrap());
    let rss = |mb| RssReading::Mb(RssMb::new(mb).unwrap());
    assert_eq!(
        shared_recycle(
            per_seat,
            MemoryLimit::Off,
            seats,
            399,
            RssReading::Unreadable
        ),
        Recycle::ServeOn
    );
    assert_eq!(
        shared_recycle(
            per_seat,
            MemoryLimit::Off,
            seats,
            400,
            RssReading::Unreadable
        ),
        Recycle::Close(CloseReason::TaskCount)
    );
    assert_eq!(
        shared_recycle(TaskLimit::Off, memory, seats, 1, rss(512.0)),
        Recycle::Close(CloseReason::MemoryLimit)
    );
    assert_eq!(
        shared_recycle(TaskLimit::Off, memory, seats, 1, rss(511.9)),
        Recycle::ServeOn
    );
    assert_eq!(
        shared_recycle(TaskLimit::Off, memory, seats, 1, RssReading::Unreadable),
        Recycle::ServeOn
    );
}

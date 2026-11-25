//! The runs in flight on one shared child, the rules of their stop reports,
//! and the end of each run when its child ends. Pure.
//!
//! The stop report of a run moves one frame at a time (Python executor
//! design, section 8.4). A frame outside these rules is a protocol
//! violation, and the slot kills the child:
//!
//! | From | Frame | To | Work |
//! |---|---|---|---|
//! | `NotReported` | `deadline_fired` | `DeadlineReported` | async, thread |
//! | `DeadlineReported` | `second_cancel` | `SecondCancelReported` | async |
//! | `SecondCancelReported` | `run_stuck` | `StuckReported` | async |
//! | `NotReported` | `leftover_cancel` | `LeftoverCancelReported` | async |
//! | `LeftoverCancelReported` | `leftover_stuck` | `LeftoverStuckReported` | async |
//! | `NotReported` | the parent sends `cancel_run` | `CancelRequested` | async, thread |
//! | `CancelRequested` | `cancel_fired` | `CancelReported` | async, thread |
//! | `CancelRequested` | `deadline_fired` | `DeadlineReported` (the deadline came first in the child) | async, thread |
//! | `CancelRequested` | `leftover_cancel` | `LeftoverCancelReported` (the body returned first) | async |
//! | `CancelReported` | `second_cancel` | `CancelSecondReported` | async |
//! | `CancelSecondReported` | `run_stuck` | `CancelStuckReported` | async |
//! | `CancelReported` | `deadline_fired` | `CancelDeadlineReported` | thread |
//! | any | `shared_result` finished | the run ends | every work |
//! | not a leftover report, no cancel report | `shared_result` timed out | the run ends at its timeout | async, thread |
//! | a cancel report | `shared_result` cancelled | the run ends cancelled | async, thread |
//!
//! An operator cancel goes to a run only while its stop is not reported: a
//! run that passed its deadline, or whose body returned, is ending already.
//! Once the parent sent the cancel, every end of the run at a kill or a loss
//! of its child is `Cancelled`: the cancel statement recorded the attempt.

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::time::Duration;

use tokio::time::Instant;
use uuid::Uuid;

use super::dispatch::{
    AttemptEnd, BodyReturn, CancelEnd, CancelledEnd, LoopKillCode, SecondCancelAfterSignal,
    TimeoutCancel, TimeoutSeen,
};
use super::pool::{MemoryLimit, TaskLimit};
use super::shared_stop::{
    culprit_retry, verdict_at_kill, ChildKill, DeadlineState, KillVerdict, KilledRunVerdict,
    LoopHolder,
};
use crate::broker::attempt_writes::RetryPlacement;
use crate::broker::child_writes::LONGEST_REFUSAL_WRITES_BOUND;
use crate::events::AttemptStart;
use crate::worker::executor::protocol::{
    CancelGraceMs, CancelledBody, DispatchNumber, FailureDetail, LeftoverCancelFrame,
    LeftoverStuckFrame, LoopBlockedFrame, LoopCulprit, RssReading, RunRef, RunStuckFrame, SeatEnd,
    SharedResultFrame, SharedWork, TaskTimeoutMs, LOOP_BLOCK_LIMIT, STUCK_REPORT_DELAY,
};
use crate::worker::executor::supervisor::{ChildExit, ChildLost, LossCause};

/// The time past the block limit that the parent leaves for the report of a
/// blocked loop to reach it (Python `LOOP_BLOCK_REPORT_WAIT_S`).
pub const LOOP_BLOCK_REPORT_WAIT: Duration = Duration::from_secs(5);
/// The parent checks a run this long after the due step of its stop: past
/// the block limit and the wait for its report, so a blocked loop is
/// reported first, with its holder (Python `SHARED_BACKSTOP_MARGIN_S`).
pub const SHARED_BACKSTOP_MARGIN: Duration =
    LOOP_BLOCK_LIMIT.saturating_add(LOOP_BLOCK_REPORT_WAIT);
/// After the first backstop found the cancel reported, the parent checks an
/// async run again this long later (Python `SHARED_STUCK_BACKSTOP_S`).
pub const SHARED_STUCK_BACKSTOP: Duration =
    STUCK_REPORT_DELAY.saturating_add(SHARED_BACKSTOP_MARGIN);
/// A refusal runs only the RUNNING transition and the finalizing marker; a
/// refusal in flight longer than the longest bound of these writes in a
/// child of any kind ([`LONGEST_REFUSAL_WRITES_BOUND`], 145 s: a version-1
/// Python shared thread child) and the margin means a frozen child (Python
/// `SHARED_REFUSAL_BACKSTOP_S`). A refusal whose writes end within their
/// bound, in a child of any kind, never meets it.
pub const SHARED_REFUSAL_BACKSTOP: Duration =
    LONGEST_REFUSAL_WRITES_BOUND.saturating_add(SHARED_BACKSTOP_MARGIN);

/// The kind of a shared child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedChildKind {
    /// Async runs on one runtime; the child cancels a run at its deadline.
    Async,
    /// Blocking runs on seat threads; a thread cannot be cancelled.
    Thread,
}

/// What a run does on its seat: the deadline facts the parent keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunWork {
    AsyncTask {
        timeout: TaskTimeoutMs,
        grace: CancelGraceMs,
    },
    ThreadTask {
        timeout: TaskTimeoutMs,
    },
    /// A refusal of the stored options: no task code, no deadline.
    Refusal,
}

impl RunWork {
    pub fn of(work: &SharedWork) -> Self {
        match work {
            SharedWork::AsyncTask {
                timeout_ms,
                cancel_grace_ms,
            } => Self::AsyncTask {
                timeout: *timeout_ms,
                grace: *cancel_grace_ms,
            },
            SharedWork::ThreadTask { timeout_ms } => Self::ThreadTask {
                timeout: *timeout_ms,
            },
            SharedWork::Refusal(_) => Self::Refusal,
        }
    }
}

/// What the child reported about the stop of a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunStopReport {
    NotReported,
    /// The deadline fired; an async child cancelled the run.
    DeadlineReported,
    /// The cancel grace ran out while the run was alive.
    SecondCancelReported,
    /// The run did not stop after its second cancel and its cancel rounds.
    StuckReported,
    /// The body returned before its deadline; work it left was alive after
    /// one cancel grace, and the child cancels it in rounds. `returned` is
    /// what the body returned, with the start of the attempt.
    LeftoverCancelReported {
        returned: BodyReturn,
    },
    /// That work did not stop after its cancel rounds.
    LeftoverStuckReported {
        returned: BodyReturn,
    },
    /// An operator cancelled the task: the parent sent `cancel_run`, and
    /// the child has not reported that it applied it.
    CancelRequested,
    /// The child applied the cancel (`cancel_fired`).
    CancelReported,
    /// The cancel grace after the cancel ran out while the async run was
    /// alive; the child sent the second cancel.
    CancelSecondReported,
    /// The run did not stop after that second cancel and its cancel rounds.
    CancelStuckReported,
    /// A thread run that the child applied the cancel to passed its
    /// deadline: the thread runs on.
    CancelDeadlineReported,
}

/// Whether the parent sent an operator cancel to a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorCancel {
    NotSent,
    Sent,
}

/// What the parent does with an operator cancel routed to a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelSend {
    /// Send `cancel_run` for the run; its report is `CancelRequested`.
    Send(RunRef),
    /// Send nothing: the run is a refusal with no task code, or its stop is
    /// reported already (its deadline passed, or its body returned).
    Skip,
}

impl RunStopReport {
    fn deadline_reported(&self) -> bool {
        match self {
            Self::DeadlineReported
            | Self::SecondCancelReported
            | Self::StuckReported
            | Self::CancelDeadlineReported => true,
            Self::NotReported
            | Self::LeftoverCancelReported { .. }
            | Self::LeftoverStuckReported { .. }
            | Self::CancelRequested
            | Self::CancelReported
            | Self::CancelSecondReported
            | Self::CancelStuckReported => false,
        }
    }

    /// Whether the parent sent an operator cancel to the run. A run whose
    /// deadline or return the child reported first keeps that report.
    pub fn operator_cancel(&self) -> OperatorCancel {
        match self {
            Self::CancelRequested
            | Self::CancelReported
            | Self::CancelSecondReported
            | Self::CancelStuckReported
            | Self::CancelDeadlineReported => OperatorCancel::Sent,
            Self::NotReported
            | Self::DeadlineReported
            | Self::SecondCancelReported
            | Self::StuckReported
            | Self::LeftoverCancelReported { .. }
            | Self::LeftoverStuckReported { .. } => OperatorCancel::NotSent,
        }
    }

    /// Whether the run has no time left: a thread run past its deadline,
    /// an async run that is stuck. A cancelled async run that is not stuck
    /// still has time: it is never a reason to kill its child.
    pub fn out_of_time(&self, kind: SharedChildKind) -> bool {
        match (kind, self) {
            (SharedChildKind::Thread, report) => report.deadline_reported(),
            (
                SharedChildKind::Async,
                Self::StuckReported
                | Self::LeftoverStuckReported { .. }
                | Self::CancelStuckReported,
            ) => true,
            (
                SharedChildKind::Async,
                Self::NotReported
                | Self::DeadlineReported
                | Self::SecondCancelReported
                | Self::LeftoverCancelReported { .. }
                | Self::CancelRequested
                | Self::CancelReported
                | Self::CancelSecondReported
                | Self::CancelDeadlineReported,
            ) => false,
        }
    }
}

/// One run in flight on a shared child. `attached` is what the slot keeps
/// with it (its answer and its runner beat hold); tests attach `()`.
#[derive(Debug)]
pub struct SeatRun<P> {
    pub run: RunRef,
    pub work: RunWork,
    /// When the run frame was sent. The child starts the run later, so its
    /// deadline is not earlier than `sent_at + timeout`.
    pub sent_at: Instant,
    pub report: RunStopReport,
    pub attached: P,
}

/// Why a frame about a run breaks the rules of its stop report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutOfTurn {
    /// No run with this number is in flight.
    NoRunInFlight,
    /// The run with this number has another task.
    AnotherTask,
    /// The run has no deadline (a refusal), or its kind has no such step.
    NoSuchStep,
    /// The step does not follow the run's last report.
    StepOutOfOrder,
}

impl fmt::Display for OutOfTurn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::NoRunInFlight => "no run with this number is in flight",
            Self::AnotherTask => "the run with this number has another task",
            Self::NoSuchStep => "the run has no such step",
            Self::StepOutOfOrder => "the step does not follow the run's last report",
        };
        f.write_str(text)
    }
}

/// A frame about a run that breaks the rules of its stop report: a
/// protocol violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "a frame about run {} (task {}) is out of turn: {why}",
    .run.dispatch.get(),
    .run.task_id
)]
pub struct FrameOutOfTurn {
    pub run: RunRef,
    pub why: OutOfTurn,
}

fn out_of_turn(run: RunRef, why: OutOfTurn) -> FrameOutOfTurn {
    FrameOutOfTurn { run, why }
}

/// A report of a step of the stop of an async run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReportFrame {
    SecondCancel(RunRef),
    RunStuck(RunStuckFrame),
    LeftoverCancel(LeftoverCancelFrame),
    LeftoverStuck(LeftoverStuckFrame),
}

impl StopReportFrame {
    fn run(&self) -> RunRef {
        match self {
            Self::SecondCancel(run) => *run,
            Self::RunStuck(frame) => frame.run,
            Self::LeftoverCancel(frame) => frame.run,
            Self::LeftoverStuck(frame) => frame.run,
        }
    }
}

/// What the slot does after a valid stop report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopStep {
    /// Close the child for this reason.
    Close(CloseReason),
    /// Only the run's report changed.
    Recorded,
}

/// What the slot does after a valid blocked-loop report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopBlockStep {
    /// Close the child and kill it now.
    KillNow { holder: LoopHolder },
    /// A blocked-loop kill was sent already.
    AlreadyKilled,
}

/// The runs in flight on one shared child, by dispatch number; at most
/// `seats` of them. `kill` is the named cause of a kill that the worker
/// sent to the child.
#[derive(Debug)]
pub struct SeatRuns<P> {
    seats: NonZeroUsize,
    runs: BTreeMap<DispatchNumber, SeatRun<P>>,
    kill: ChildKill,
}

impl<P> SeatRuns<P> {
    pub fn new(seats: NonZeroUsize) -> Self {
        Self {
            seats,
            runs: BTreeMap::new(),
            kill: ChildKill::NoNamedKill,
        }
    }

    pub fn count(&self) -> usize {
        self.runs.len()
    }

    pub fn has_free_seat(&self) -> bool {
        self.runs.len() < self.seats.get()
    }

    pub fn kill(&self) -> &ChildKill {
        &self.kill
    }

    /// Hold a new run. A full table or a number in use is a defect of the
    /// placement: it crashes loud.
    pub fn add(&mut self, run: SeatRun<P>) {
        assert!(
            self.has_free_seat(),
            "run {} placed on a child with no free seat",
            run.run.dispatch.get()
        );
        let dispatch = run.run.dispatch;
        let previous = self.runs.insert(dispatch, run);
        assert!(
            previous.is_none(),
            "run {} placed twice on one child",
            dispatch.get()
        );
    }

    pub fn get(&self, dispatch: DispatchNumber) -> Option<&SeatRun<P>> {
        self.runs.get(&dispatch)
    }

    pub fn iter(&self) -> impl Iterator<Item = &SeatRun<P>> {
        self.runs.values()
    }

    /// What the slot keeps with each run, to change. The stop reports
    /// change only through the frame rules.
    pub fn attached_mut(&mut self) -> impl Iterator<Item = &mut P> {
        self.runs.values_mut().map(|seat| &mut seat.attached)
    }

    /// What the slot keeps with run `dispatch`, to change.
    pub fn attached_of(&mut self, dispatch: DispatchNumber) -> Option<&mut P> {
        self.runs.get_mut(&dispatch).map(|seat| &mut seat.attached)
    }

    /// Drop every run, and return them in dispatch order.
    pub fn drain(&mut self) -> Vec<SeatRun<P>> {
        std::mem::take(&mut self.runs).into_values().collect()
    }

    pub fn task_ids(&self) -> Vec<Uuid> {
        self.runs.values().map(|seat| seat.run.task_id).collect()
    }

    /// Whether a run is in flight and no run has time left.
    pub fn all_out_of_time(&self, kind: SharedChildKind) -> bool {
        !self.runs.is_empty() && self.runs.values().all(|seat| seat.report.out_of_time(kind))
    }

    fn checked(&mut self, run: RunRef) -> Result<&mut SeatRun<P>, FrameOutOfTurn> {
        match self.runs.get_mut(&run.dispatch) {
            None => Err(out_of_turn(run, OutOfTurn::NoRunInFlight)),
            Some(seat) if seat.run.task_id != run.task_id => {
                Err(out_of_turn(run, OutOfTurn::AnotherTask))
            }
            Some(seat) => Ok(seat),
        }
    }

    /// End the run of a result frame, and give its end (Python `_end_run`).
    /// A timeout end is valid only for a run with a deadline whose body did
    /// not return first. The timeout of an async run records how it ended
    /// after its cancel signal.
    pub fn end_run(
        &mut self,
        frame: SharedResultFrame,
    ) -> Result<(SeatRun<P>, AttemptEnd), FrameOutOfTurn> {
        let seat = self.checked(frame.run)?;
        let end = match (frame.end, &seat.report, seat.work) {
            (SeatEnd::Finished(end), _, _) => AttemptEnd::reported(end, frame.started_at),
            (SeatEnd::Cancelled(body), report, _) => {
                cancelled_end_of(body, report, frame.started_at)
                    .ok_or(out_of_turn(frame.run, OutOfTurn::StepOutOfOrder))?
            }
            (
                SeatEnd::TimedOut,
                RunStopReport::LeftoverCancelReported { .. }
                | RunStopReport::LeftoverStuckReported { .. },
                _,
            )
            | (SeatEnd::TimedOut, _, RunWork::Refusal) => {
                return Err(out_of_turn(frame.run, OutOfTurn::NoSuchStep))
            }
            (
                SeatEnd::TimedOut,
                RunStopReport::CancelReported
                | RunStopReport::CancelSecondReported
                | RunStopReport::CancelStuckReported
                | RunStopReport::CancelDeadlineReported,
                _,
            ) => return Err(out_of_turn(frame.run, OutOfTurn::StepOutOfOrder)),
            (SeatEnd::TimedOut, report, RunWork::AsyncTask { timeout, .. }) => {
                AttemptEnd::TimedOut {
                    timeout,
                    retry: retry_after_timeout(report),
                    cancel: TimeoutCancel::Ended(cancel_end_of(report)),
                    seen: TimeoutSeen::ReportedByChild {
                        start: frame.started_at,
                    },
                }
            }
            (SeatEnd::TimedOut, report, RunWork::ThreadTask { timeout }) => AttemptEnd::TimedOut {
                timeout,
                retry: retry_after_timeout(report),
                cancel: TimeoutCancel::NoSignal,
                seen: TimeoutSeen::ReportedByChild {
                    start: frame.started_at,
                },
            },
        };
        let seat = self
            .runs
            .remove(&frame.run.dispatch)
            .expect("the run was found in flight just above");
        Ok((seat, end))
    }

    /// Mark that the deadline of a run fired (Python `_note_deadline`).
    /// Returns its timeout, for the log. A deadline that fired in the child
    /// before it read an operator cancel is the deadline report; a thread
    /// run that the child applied the cancel to runs on past its deadline.
    pub fn note_deadline(&mut self, run: RunRef) -> Result<TaskTimeoutMs, FrameOutOfTurn> {
        let seat = self.checked(run)?;
        let (timeout, report) = match (seat.work, &seat.report) {
            (
                RunWork::AsyncTask { timeout, .. } | RunWork::ThreadTask { timeout },
                RunStopReport::NotReported | RunStopReport::CancelRequested,
            ) => (timeout, RunStopReport::DeadlineReported),
            (RunWork::ThreadTask { timeout }, RunStopReport::CancelReported) => {
                (timeout, RunStopReport::CancelDeadlineReported)
            }
            (RunWork::Refusal, _) => return Err(out_of_turn(run, OutOfTurn::NoSuchStep)),
            (RunWork::AsyncTask { .. } | RunWork::ThreadTask { .. }, _) => {
                return Err(out_of_turn(run, OutOfTurn::StepOutOfOrder))
            }
        };
        seat.report = report;
        Ok(timeout)
    }

    /// Note an operator cancel routed to run `dispatch`: a run of task code
    /// whose stop is not reported gets `cancel_run`, and its report is
    /// `CancelRequested`. A run that ended before the route is not in flight:
    /// `Skip`.
    pub fn note_cancel_sent(&mut self, dispatch: DispatchNumber) -> CancelSend {
        let Some(seat) = self.runs.get_mut(&dispatch) else {
            return CancelSend::Skip;
        };
        match (seat.work, &seat.report) {
            (
                RunWork::AsyncTask { .. } | RunWork::ThreadTask { .. },
                RunStopReport::NotReported,
            ) => {
                seat.report = RunStopReport::CancelRequested;
                CancelSend::Send(seat.run)
            }
            (RunWork::Refusal, _)
            | (
                RunWork::AsyncTask { .. } | RunWork::ThreadTask { .. },
                RunStopReport::DeadlineReported
                | RunStopReport::SecondCancelReported
                | RunStopReport::StuckReported
                | RunStopReport::LeftoverCancelReported { .. }
                | RunStopReport::LeftoverStuckReported { .. }
                | RunStopReport::CancelRequested
                | RunStopReport::CancelReported
                | RunStopReport::CancelSecondReported
                | RunStopReport::CancelStuckReported
                | RunStopReport::CancelDeadlineReported,
            ) => CancelSend::Skip,
        }
    }

    /// Mark that the child applied the operator cancel of a run
    /// (`cancel_fired`). Valid only after the parent sent it.
    pub fn note_cancel_fired(&mut self, run: RunRef) -> Result<(), FrameOutOfTurn> {
        let seat = self.checked(run)?;
        match (seat.work, &seat.report) {
            (RunWork::Refusal, _) => Err(out_of_turn(run, OutOfTurn::NoSuchStep)),
            (
                RunWork::AsyncTask { .. } | RunWork::ThreadTask { .. },
                RunStopReport::CancelRequested,
            ) => {
                seat.report = RunStopReport::CancelReported;
                Ok(())
            }
            (RunWork::AsyncTask { .. } | RunWork::ThreadTask { .. }, _) => {
                Err(out_of_turn(run, OutOfTurn::StepOutOfOrder))
            }
        }
    }

    /// Record a step of the stop of an async run (Python
    /// `_note_stop_report`).
    pub fn note_stop_report(
        &mut self,
        frame: &StopReportFrame,
    ) -> Result<StopStep, FrameOutOfTurn> {
        let run = frame.run();
        let seat = self.checked(run)?;
        match seat.work {
            RunWork::AsyncTask { .. } => {}
            RunWork::ThreadTask { .. } | RunWork::Refusal => {
                return Err(out_of_turn(run, OutOfTurn::NoSuchStep))
            }
        }
        let (report, step) = match (frame, &seat.report) {
            (StopReportFrame::SecondCancel(_), RunStopReport::DeadlineReported) => (
                RunStopReport::SecondCancelReported,
                StopStep::Close(CloseReason::SecondCancel),
            ),
            (StopReportFrame::RunStuck(_), RunStopReport::SecondCancelReported) => (
                RunStopReport::StuckReported,
                StopStep::Close(CloseReason::AtDeadline { culprit: run }),
            ),
            (StopReportFrame::SecondCancel(_), RunStopReport::CancelReported) => (
                RunStopReport::CancelSecondReported,
                StopStep::Close(CloseReason::SecondCancel),
            ),
            (StopReportFrame::RunStuck(_), RunStopReport::CancelSecondReported) => (
                RunStopReport::CancelStuckReported,
                StopStep::Close(CloseReason::SecondCancel),
            ),
            (
                StopReportFrame::LeftoverCancel(cancel),
                RunStopReport::NotReported | RunStopReport::CancelRequested,
            ) => (
                RunStopReport::LeftoverCancelReported {
                    returned: BodyReturn {
                        end: cancel.returned.clone(),
                        started_at: cancel.started_at,
                    },
                },
                StopStep::Close(CloseReason::LeftoverWork),
            ),
            (
                StopReportFrame::LeftoverStuck(stuck),
                RunStopReport::LeftoverCancelReported { .. },
            ) => (
                RunStopReport::LeftoverStuckReported {
                    returned: BodyReturn {
                        end: stuck.returned.clone(),
                        started_at: stuck.started_at,
                    },
                },
                StopStep::Recorded,
            ),
            (
                StopReportFrame::SecondCancel(_)
                | StopReportFrame::RunStuck(_)
                | StopReportFrame::LeftoverCancel(_)
                | StopReportFrame::LeftoverStuck(_),
                _,
            ) => return Err(out_of_turn(run, OutOfTurn::StepOutOfOrder)),
        };
        seat.report = report;
        Ok(step)
    }

    /// Record a blocked loop (Python `_kill_for_blocked_loop`). The holder
    /// is named when its run is still in flight; a report that names a run
    /// in flight with another task is out of turn.
    pub fn note_loop_blocked(
        &mut self,
        frame: &LoopBlockedFrame,
    ) -> Result<LoopBlockStep, FrameOutOfTurn> {
        let holder = match frame.culprit {
            LoopCulprit::Run(run) => match self.runs.get(&run.dispatch) {
                None => LoopHolder::Unknown,
                Some(seat) if seat.run.task_id != run.task_id => {
                    return Err(out_of_turn(run, OutOfTurn::AnotherTask))
                }
                Some(_) => LoopHolder::Run(run),
            },
            LoopCulprit::Unknown => LoopHolder::Unknown,
        };
        match self.kill {
            ChildKill::BlockedLoop { .. } => Ok(LoopBlockStep::AlreadyKilled),
            ChildKill::NoNamedKill => {
                self.kill = ChildKill::BlockedLoop {
                    holder,
                    blocked_ms: frame.blocked_ms,
                    location: frame.location.clone(),
                };
                Ok(LoopBlockStep::KillNow { holder })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The service of a shared child
// ---------------------------------------------------------------------------

/// Why a shared child takes no new task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    TaskCount,
    MemoryLimit,
    PoolStopping,
    SecondCancel,
    LoopBlocked,
    LeftoverWork,
    /// A thread run passed its deadline, or an async run was reported
    /// stuck: `culprit` retries isolated at the kill.
    AtDeadline {
        culprit: RunRef,
    },
}

impl fmt::Display for CloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TaskCount => f.write_str("the task limit per seat was reached"),
            Self::MemoryLimit => f.write_str("the memory limit was reached"),
            Self::PoolStopping => f.write_str("the worker is stopping"),
            Self::SecondCancel => f.write_str("a task needed a second cancel"),
            Self::LoopBlocked => f.write_str("a task blocked the event loop"),
            Self::LeftoverWork => {
                f.write_str("work that a task left after its return needed a cancel")
            }
            Self::AtDeadline { culprit } => {
                write!(f, "task {} closed it at its deadline", culprit.task_id)
            }
        }
    }
}

/// Whether a shared child takes new tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildService {
    Open,
    /// Its runs finish in their own time, or it is killed once no run has
    /// time left; then it retires.
    Closing(CloseReason),
}

/// The run that closed a child at its deadline, if one did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseCulprit {
    Named(RunRef),
    NotNamed,
}

impl ChildService {
    /// Close for the stop of a run, or a blocked loop. A child closed at a
    /// deadline keeps that reason and its culprit.
    pub fn close_for_stop(&mut self, reason: CloseReason) {
        match self {
            Self::Closing(CloseReason::AtDeadline { .. }) => {}
            Self::Open | Self::Closing(_) => *self = Self::Closing(reason),
        }
    }

    /// Close a thread child at the first deadline it reports: a thread
    /// cannot be cancelled. An async child cancelled its run and stays.
    pub fn close_at_deadline(&mut self, kind: SharedChildKind, culprit: RunRef) {
        match kind {
            SharedChildKind::Async => {}
            SharedChildKind::Thread => self.close_for_stop(CloseReason::AtDeadline { culprit }),
        }
    }

    /// Close an open child that reached a recycle limit.
    pub fn close_after_task(&mut self, recycle: Recycle) {
        match (*self, recycle) {
            (Self::Open, Recycle::Close(reason)) => *self = Self::Closing(reason),
            (Self::Open, Recycle::ServeOn) | (Self::Closing(_), _) => {}
        }
    }

    /// Close an open child when the pool stops or fails.
    pub fn close_for_pool_stop(&mut self) {
        match self {
            Self::Open => *self = Self::Closing(CloseReason::PoolStopping),
            Self::Closing(_) => {}
        }
    }

    pub fn close_culprit(self) -> CloseCulprit {
        match self {
            Self::Closing(CloseReason::AtDeadline { culprit }) => CloseCulprit::Named(culprit),
            Self::Open | Self::Closing(_) => CloseCulprit::NotNamed,
        }
    }
}

// ---------------------------------------------------------------------------
// Ends at a lost or killed child
// ---------------------------------------------------------------------------

/// A lost child whose exit the worker saw: the verdicts apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeenLoss(ChildLost);

/// Whether the exit of a lost child was seen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LossSight {
    Seen(SeenLoss),
    /// The attempt may still run: stale recovery decides, or the slot
    /// waits for the exit.
    NotSeen(ChildLost),
}

impl LossSight {
    pub fn of(lost: ChildLost) -> Self {
        match lost.exit {
            ChildExit::Reaped(_) | ChildExit::ReapedElsewhere => Self::Seen(SeenLoss(lost)),
            ChildExit::ExitUnknown => Self::NotSeen(lost),
        }
    }
}

impl SeenLoss {
    fn crashed(self) -> AttemptEnd {
        AttemptEnd::of_lost_child(&self.0)
    }

    fn killed_by(self) -> String {
        format!("pid={}, {}", self.0.pid.get(), self.0.exit)
    }
}

/// Whether the deadline of a run had passed (Python `_deadline_state`):
/// the child reported it, or `timeout` passed since the send. The run that
/// closed its child at its deadline and a run reported stuck are the
/// culprit.
pub fn deadline_state<P>(seat: &SeatRun<P>, closer: CloseCulprit, now: Instant) -> DeadlineState {
    if let CloseCulprit::Named(culprit) = closer {
        if culprit.dispatch == seat.run.dispatch {
            return DeadlineState::Culprit;
        }
    }
    match (&seat.report, seat.work) {
        (RunStopReport::StuckReported, _) => DeadlineState::Culprit,
        (
            RunStopReport::DeadlineReported
            | RunStopReport::SecondCancelReported
            | RunStopReport::CancelDeadlineReported,
            _,
        ) => DeadlineState::Passed,
        (
            RunStopReport::LeftoverCancelReported { .. }
            | RunStopReport::LeftoverStuckReported { .. },
            _,
        ) => DeadlineState::Pending,
        (
            RunStopReport::NotReported
            | RunStopReport::CancelRequested
            | RunStopReport::CancelReported
            | RunStopReport::CancelSecondReported
            | RunStopReport::CancelStuckReported,
            RunWork::AsyncTask { timeout, .. } | RunWork::ThreadTask { timeout },
        ) => match now >= seat.sent_at + Duration::from_millis(timeout.get()) {
            true => DeadlineState::Passed,
            false => DeadlineState::Pending,
        },
        (
            RunStopReport::NotReported
            | RunStopReport::CancelRequested
            | RunStopReport::CancelReported
            | RunStopReport::CancelSecondReported
            | RunStopReport::CancelStuckReported,
            RunWork::Refusal,
        ) => DeadlineState::Pending,
    }
}

/// Where the retry of a run that its child ended as a timeout runs: an
/// isolated child for a run reported stuck, else its class. Pure.
fn retry_after_timeout(report: &RunStopReport) -> RetryPlacement {
    match report {
        RunStopReport::StuckReported => RetryPlacement::Isolated,
        RunStopReport::NotReported
        | RunStopReport::DeadlineReported
        | RunStopReport::SecondCancelReported
        | RunStopReport::LeftoverCancelReported { .. }
        | RunStopReport::LeftoverStuckReported { .. }
        | RunStopReport::CancelRequested
        | RunStopReport::CancelReported
        | RunStopReport::CancelSecondReported
        | RunStopReport::CancelStuckReported
        | RunStopReport::CancelDeadlineReported => RetryPlacement::SameClass,
    }
}

/// How an async run that its child ended as a timeout ended: within the
/// grace after the cancel signal, or dropped after the grace. The child sends
/// `second_cancel` only when the run is alive after the grace, so the report
/// names it. A run that returned before its deadline never ends as a
/// timeout ([`SeatRuns::end_run`] refuses it first): a defect. Pure.
fn cancel_end_of(report: &RunStopReport) -> CancelEnd {
    match report {
        RunStopReport::NotReported
        | RunStopReport::DeadlineReported
        | RunStopReport::CancelRequested => CancelEnd::ReturnedAfterSignal,
        RunStopReport::SecondCancelReported | RunStopReport::StuckReported => {
            CancelEnd::DroppedAfterGrace
        }
        RunStopReport::LeftoverCancelReported { .. }
        | RunStopReport::LeftoverStuckReported { .. } => {
            unreachable!("a run that returned before its deadline has no cancel end ({report:?})")
        }
        RunStopReport::CancelReported
        | RunStopReport::CancelSecondReported
        | RunStopReport::CancelStuckReported
        | RunStopReport::CancelDeadlineReported => unreachable!(
            "a run that its child cancelled for an operator never ends as a timeout ({report:?})"
        ),
    }
}

/// The end of a run that its child reported `cancelled` with `body`, by its
/// report: dropped after the grace when the child sent the second cancel
/// after `cancel_fired`. `None` when the child reported no `cancel_fired`
/// for the run: the frame is out of turn. Pure.
fn cancelled_end_of(
    body: CancelledBody,
    report: &RunStopReport,
    start: AttemptStart,
) -> Option<AttemptEnd> {
    let dropped = match report {
        RunStopReport::CancelReported | RunStopReport::CancelDeadlineReported => {
            SecondCancelAfterSignal::NotSent
        }
        RunStopReport::CancelSecondReported | RunStopReport::CancelStuckReported => {
            SecondCancelAfterSignal::Sent
        }
        RunStopReport::NotReported
        | RunStopReport::DeadlineReported
        | RunStopReport::SecondCancelReported
        | RunStopReport::StuckReported
        | RunStopReport::LeftoverCancelReported { .. }
        | RunStopReport::LeftoverStuckReported { .. }
        | RunStopReport::CancelRequested => return None,
    };
    Some(AttemptEnd::Cancelled(CancelledEnd::reported(
        body, dropped, start,
    )))
}

/// The end of a run that the parent sent an operator cancel to, at a kill
/// or a loss of its child: the cancel recorded the attempt, so it is
/// `Cancelled` whatever ended the child. `None` for a run with no cancel.
/// Pure.
fn cancelled_at_loss(report: &RunStopReport, cause: LossCause) -> Option<AttemptEnd> {
    match report.operator_cancel() {
        OperatorCancel::NotSent => None,
        OperatorCancel::Sent => Some(AttemptEnd::Cancelled(match cause {
            LossCause::KilledByParent => CancelledEnd::ChildKilled,
            LossCause::ChildDied
            | LossCause::ChannelLost
            | LossCause::ProtocolViolation
            | LossCause::ProtocolMismatch(_)
            | LossCause::SlotDefect
            | LossCause::SpawnerStopped => CancelledEnd::ChildLost,
        })),
    }
}

/// The `TASK_TIMEOUT` end of a run with a deadline whose child the worker
/// killed (`TimeoutSeen::AtKill`): an async run records `KilledWithChild`.
/// The child reported no end for it. A refusal has none, and the report
/// rules never give it one: a defect, which crashes loud.
fn timed_out(work: RunWork, run: RunRef, retry: RetryPlacement) -> AttemptEnd {
    match work {
        RunWork::AsyncTask { timeout, .. } => AttemptEnd::TimedOut {
            timeout,
            retry,
            cancel: TimeoutCancel::Ended(CancelEnd::KilledWithChild),
            seen: TimeoutSeen::AtKill,
        },
        RunWork::ThreadTask { timeout } => AttemptEnd::TimedOut {
            timeout,
            retry,
            cancel: TimeoutCancel::NoSignal,
            seen: TimeoutSeen::AtKill,
        },
        RunWork::Refusal => unreachable!(
            "the refused task {} ended at a deadline it does not have",
            run.task_id
        ),
    }
}

/// The end of a killed run with a verdict.
fn verdict_end(
    verdict: KilledRunVerdict,
    work: RunWork,
    run: RunRef,
    loss: SeenLoss,
) -> AttemptEnd {
    match verdict {
        KilledRunVerdict::TimedOutAtKill { retry } => timed_out(work, run, retry),
        KilledRunVerdict::BlockedTheLoop {
            blocked_ms,
            location,
        } => AttemptEnd::KilledForLoop {
            code: LoopKillCode::TaskBlockedLoop,
            retry: RetryPlacement::Isolated,
            reason: FailureDetail::cut(&format!(
                "Task held the event loop of its shared async child for {blocked_ms} ms, past \
                 the block limit, at {location}; the worker killed the child ({})",
                loss.killed_by()
            )),
        },
        KilledRunVerdict::BesideBlockedLoop {
            holder_task_id,
            blocked_ms,
        } => AttemptEnd::KilledForLoop {
            code: LoopKillCode::NeighbourBlockedLoop,
            retry: RetryPlacement::SameClass,
            reason: FailureDetail::cut(&format!(
                "Task {holder_task_id} held the event loop of the shared async child of this \
                 task for {blocked_ms} ms, past the block limit; the worker killed the child \
                 ({})",
                loss.killed_by()
            )),
        },
    }
}

/// The end of a run of a lost shared child whose exit was seen (Python
/// `_outcome_at_loss`). A run whose body returned and whose left work was
/// reported ends with what it returned, whatever ended the child. Any other
/// run gets the verdict of a kill by the worker; a death the worker did
/// not cause takes the crash path.
pub fn end_at_loss<P>(
    seat: &SeatRun<P>,
    kill: &ChildKill,
    closer: CloseCulprit,
    loss: SeenLoss,
    now: Instant,
) -> AttemptEnd {
    if let Some(cancelled) = cancelled_at_loss(&seat.report, loss.0.cause) {
        return cancelled;
    }
    match (&seat.report, loss.0.cause) {
        (
            RunStopReport::LeftoverCancelReported { returned }
            | RunStopReport::LeftoverStuckReported { returned },
            _,
        ) => return AttemptEnd::returned(returned.clone()),
        (_, LossCause::KilledByParent) => {}
        (
            _,
            LossCause::ChildDied
            | LossCause::ChannelLost
            | LossCause::ProtocolViolation
            | LossCause::ProtocolMismatch(_)
            | LossCause::SlotDefect
            | LossCause::SpawnerStopped,
        ) => return loss.crashed(),
    }
    let deadline = deadline_state(seat, closer, now);
    match verdict_at_kill(kill, seat.run.dispatch, deadline) {
        KillVerdict::Verdict(verdict) => verdict_end(verdict, seat.work, seat.run, loss),
        KillVerdict::CrashPath => loss.crashed(),
    }
}

/// The end of a run of a child killed when no run had time left, with its
/// exit seen (Python `_killed_past_deadline`). A thread run is past its
/// deadline, and the run that closed the child is the culprit. An async
/// run is stuck: stuck after its deadline it is its own culprit; stuck only
/// in the work it left after its return, it ends with what it returned. A
/// run with time left means the kill rule did not hold: a defect.
pub fn end_past_deadlines<P>(
    seat: &SeatRun<P>,
    kind: SharedChildKind,
    reason: CloseReason,
) -> AttemptEnd {
    if let Some(cancelled) = cancelled_at_loss(&seat.report, LossCause::KilledByParent) {
        return cancelled;
    }
    match (kind, &seat.report, reason) {
        (SharedChildKind::Async, RunStopReport::LeftoverStuckReported { returned }, _) => {
            AttemptEnd::returned(returned.clone())
        }
        (SharedChildKind::Async, RunStopReport::StuckReported, _) => {
            timed_out(seat.work, seat.run, RetryPlacement::Isolated)
        }
        (SharedChildKind::Thread, _, CloseReason::AtDeadline { culprit }) => timed_out(
            seat.work,
            seat.run,
            culprit_retry(culprit, seat.run.dispatch),
        ),
        (SharedChildKind::Async | SharedChildKind::Thread, report, reason) => panic!(
            "run {} (task {}) had time left when its child was killed for having none \
             ({report:?}, {reason})",
            seat.run.dispatch.get(),
            seat.run.task_id
        ),
    }
}

/// The end of a run whose child the first backstop killed, when its exit
/// was seen only after the bounded wait (Python
/// `_outcome_at_late_backstop_exit`): past its deadline, `TASK_TIMEOUT` in
/// its class; a refusal has none and takes the crash path.
pub fn end_at_late_backstop_exit(work: RunWork, run: RunRef, loss: SeenLoss) -> AttemptEnd {
    match work {
        RunWork::AsyncTask { .. } | RunWork::ThreadTask { .. } => {
            timed_out(work, run, RetryPlacement::SameClass)
        }
        RunWork::Refusal => loss.crashed(),
    }
}

// ---------------------------------------------------------------------------
// Backstops
// ---------------------------------------------------------------------------

/// How long after its send the parent checks a run that has not ended: the
/// due step of its stop (the deadline of a thread run; the deadline and the
/// cancel grace of an async run) and the margin. A refusal has no stop.
pub fn first_backstop(work: RunWork) -> Duration {
    match work {
        RunWork::AsyncTask { timeout, grace } => {
            Duration::from_millis(timeout.get().saturating_add(grace.get()))
                + SHARED_BACKSTOP_MARGIN
        }
        RunWork::ThreadTask { timeout } => {
            Duration::from_millis(timeout.get()) + SHARED_BACKSTOP_MARGIN
        }
        RunWork::Refusal => SHARED_REFUSAL_BACKSTOP,
    }
}

/// Whether the child reported the step of the stop that is due at the first
/// backstop: the deadline of a thread run, the second cancel of an async
/// run (a leftover cancel counts the same). A refusal reports none, so its
/// backstop always kills.
pub fn due_step_reported(work: RunWork, report: &RunStopReport) -> bool {
    match (work, report) {
        // A thread run that the child applied a cancel to runs on: the
        // child answered, and the thread may pass its deadline with no
        // report.
        (RunWork::ThreadTask { .. }, RunStopReport::CancelReported) => true,
        (RunWork::ThreadTask { .. }, report) => report.deadline_reported(),
        (
            RunWork::AsyncTask { .. },
            RunStopReport::SecondCancelReported
            | RunStopReport::StuckReported
            | RunStopReport::LeftoverCancelReported { .. }
            | RunStopReport::LeftoverStuckReported { .. }
            | RunStopReport::CancelSecondReported
            | RunStopReport::CancelStuckReported,
        ) => true,
        (
            RunWork::AsyncTask { .. },
            RunStopReport::NotReported
            | RunStopReport::DeadlineReported
            | RunStopReport::CancelRequested
            | RunStopReport::CancelReported
            | RunStopReport::CancelDeadlineReported,
        ) => false,
        (RunWork::Refusal, _) => false,
    }
}

/// Whether the child reported an async run stuck, after its second cancel
/// or after the cancel of the work it left.
pub fn stuck_step_reported(report: &RunStopReport) -> bool {
    match report {
        RunStopReport::StuckReported
        | RunStopReport::LeftoverStuckReported { .. }
        | RunStopReport::CancelStuckReported => true,
        RunStopReport::NotReported
        | RunStopReport::DeadlineReported
        | RunStopReport::SecondCancelReported
        | RunStopReport::LeftoverCancelReported { .. }
        | RunStopReport::CancelRequested
        | RunStopReport::CancelReported
        | RunStopReport::CancelSecondReported
        | RunStopReport::CancelDeadlineReported => false,
    }
}

/// What a backstop that came due decides for its run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackstopStep {
    /// The child is frozen: kill it, bound to this run.
    KillChild,
    /// The step was reported; check this async run again at
    /// [`SHARED_STUCK_BACKSTOP`] from now.
    CheckStuckLater,
    /// The step was reported; the run keeps its seat until it ends or its
    /// child is killed.
    WaitForEnd,
}

/// Where the backstop of a run stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backstop {
    First { at: Instant },
    Stuck { at: Instant },
    Done,
}

impl Backstop {
    pub fn after_send(work: RunWork, sent_at: Instant) -> Self {
        Self::First {
            at: sent_at + first_backstop(work),
        }
    }

    /// When this backstop comes due, if it waits.
    pub fn due_at(self) -> Option<Instant> {
        match self {
            Self::First { at } | Self::Stuck { at } => Some(at),
            Self::Done => None,
        }
    }
}

/// The decision of a backstop that came due for a run with `work` and
/// `report` (Python `backstop_kill`, `stuck_backstop_kill`).
pub fn backstop_step(backstop: Backstop, work: RunWork, report: &RunStopReport) -> BackstopStep {
    match (backstop, work) {
        (Backstop::First { .. }, _) if !due_step_reported(work, report) => BackstopStep::KillChild,
        (Backstop::First { .. }, RunWork::AsyncTask { .. }) => BackstopStep::CheckStuckLater,
        (Backstop::First { .. }, RunWork::ThreadTask { .. } | RunWork::Refusal) => {
            BackstopStep::WaitForEnd
        }
        (Backstop::Stuck { .. }, _) => match stuck_step_reported(report) {
            true => BackstopStep::WaitForEnd,
            false => BackstopStep::KillChild,
        },
        (Backstop::Done, _) => BackstopStep::WaitForEnd,
    }
}

// ---------------------------------------------------------------------------
// Placement and recycle
// ---------------------------------------------------------------------------

/// The load of one open shared child that can take a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenChildLoad {
    pub slot: usize,
    pub runs: usize,
    pub seats: NonZeroUsize,
}

/// Where the next task goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    OnSlot(usize),
    NoSeatFree,
}

/// The open child with the fewest runs and a free seat; the lowest slot
/// among equals (Python `place_on_shared_child`). Spreading tasks keeps the
/// tasks that one crash or one kill ends few.
pub fn place_on_shared_child(loads: &[OpenChildLoad]) -> Placement {
    loads
        .iter()
        .filter(|load| load.runs < load.seats.get())
        .min_by_key(|load| (load.runs, load.slot))
        .map_or(Placement::NoSeatFree, |load| Placement::OnSlot(load.slot))
}

/// Whether a shared child serves on after a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recycle {
    ServeOn,
    Close(CloseReason),
}

/// Why a shared child closes after a task (Python `shared_recycle_reason`):
/// the tasks that ended on it reached `per_seat x seats`, or the RSS it
/// reported reached the memory limit. An unreadable RSS never recycles.
pub fn shared_recycle(
    per_seat: TaskLimit,
    memory: MemoryLimit,
    seats: NonZeroUsize,
    tasks_done: u64,
    rss: RssReading,
) -> Recycle {
    let seat_count = u64::try_from(seats.get()).unwrap_or(u64::MAX);
    let count_reached = match per_seat {
        TaskLimit::Off => false,
        TaskLimit::After(per_seat) => tasks_done >= per_seat.get().saturating_mul(seat_count),
    };
    let memory_reached = match (memory, rss) {
        // The limit is a whole number of MiB; the RSS a finite number.
        (MemoryLimit::AtMb(limit), RssReading::Mb(rss)) => rss.get() >= limit.get() as f64,
        (MemoryLimit::Off, _) | (MemoryLimit::AtMb(_), RssReading::Unreadable) => false,
    };
    match (count_reached, memory_reached) {
        (true, _) => Recycle::Close(CloseReason::TaskCount),
        (false, true) => Recycle::Close(CloseReason::MemoryLimit),
        (false, false) => Recycle::ServeOn,
    }
}

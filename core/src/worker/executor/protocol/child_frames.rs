//! Frames from a child to the worker parent.

use std::collections::HashSet;
use std::num::NonZeroU64;

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use super::bounds::MAX_EVENT_HANDLERS;
use super::header::{pack, EncodedFrame, FrameEncodeError, NO_BODY};
use super::metadata::{
    check_protocol, no_body, parse_metadata, text_body, time_text, Fields, MalformedFrame,
    ProtocolCheck, ProtocolMismatch,
};
use super::values::{
    BlockedWhere, BootDetail, BootErrorName, DispatchNumber, FailureDetail, HandlerName,
    LeftoverName, LoopHoldsTasks, ProcessId, ResultEnvelopeText, RssReading, StuckWork,
};
use super::PROTOCOL_VERSION;
use crate::events::AttemptStart;

pub use crate::core::boot_failure::ShortageErrno;

/// A boot failure cause that a child can report (Python
/// `core/models/boot_failure.py`). A spawn failure is the parent's to see,
/// so a child cannot report it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportedBootCause {
    /// Locating or importing the app or its task modules failed. Repeats.
    AppLoadFailed,
    /// The child could not set up its connection pool. Repeats.
    ChildPoolSetupFailed,
    /// A boot step failed with an errno of a resource limit. Can clear.
    ResourceShortage(ShortageErrno),
    /// The child could not create the event loop for async tasks. Repeats.
    TaskLoopCreateFailed,
    /// The OS refused a thread that the child starts at boot. Can clear.
    ThreadStartRefused,
}

/// Why a child stopped before it set its row RUNNING. No task code ran. The
/// start's miss path wrote what each case needs, except for
/// `OwnershipUnconfirmed`, whose claim the parent releases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub enum StopReason {
    /// The row is not CLAIMED by this worker at this claim generation.
    ClaimLost,
    /// The start was not confirmed: the start statement raised (a database
    /// error, or its bound cancelled it), or a resume raced the start on
    /// each attempt. The parent releases the claim at once with the fenced
    /// release.
    OwnershipUnconfirmed,
    /// The workflow of the task is PAUSED or CANCELLED.
    WorkflowStopped,
    /// The task's deadline passed while it was CLAIMED; it is EXPIRED.
    TaskExpired,
    /// The workflow node of the task is not linked to it, and the orphan
    /// was not cancelled: the self-heal is off, or a runnable link exists.
    NodeNotLinked,
    /// The workflow node of the task is not linked to it, and the stop
    /// function cancelled the task as an owned orphan, under the workflow
    /// lock (the self-heal is on).
    OrphanCancelled,
}

impl StopReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::ClaimLost => "CLAIM_LOST",
            Self::OwnershipUnconfirmed => "OWNERSHIP_UNCONFIRMED",
            Self::WorkflowStopped => "WORKFLOW_STOPPED",
            Self::TaskExpired => "TASK_EXPIRED",
            Self::NodeNotLinked => "NODE_NOT_LINKED",
            Self::OrphanCancelled => "ORPHAN_CANCELLED",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "CLAIM_LOST" => Some(Self::ClaimLost),
            "OWNERSHIP_UNCONFIRMED" => Some(Self::OwnershipUnconfirmed),
            "WORKFLOW_STOPPED" => Some(Self::WorkflowStopped),
            "TASK_EXPIRED" => Some(Self::TaskExpired),
            "NODE_NOT_LINKED" => Some(Self::NodeNotLinked),
            "ORPHAN_CANCELLED" => Some(Self::OrphanCancelled),
            _ => None,
        }
    }
}

/// How a run ended in its child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunEnd {
    /// The task produced a result envelope: a value or a task error. It
    /// travels in the body.
    Result(ResultEnvelopeText),
    /// The child stopped before it set the row RUNNING.
    StoppedBeforeRunning(StopReason),
    /// The worker code of the child failed, not the task.
    WorkerFailure(FailureDetail),
}

/// What the body of a task returned. The body ran, so it did not stop
/// before running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyEnd {
    Result(ResultEnvelopeText),
    WorkerFailure(FailureDetail),
}

impl From<BodyEnd> for RunEnd {
    fn from(end: BodyEnd) -> Self {
        match end {
            BodyEnd::Result(envelope) => Self::Result(envelope),
            BodyEnd::WorkerFailure(detail) => Self::WorkerFailure(detail),
        }
    }
}

/// How a run of a shared child ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeatEnd {
    /// The run ended before its deadline.
    Finished(RunEnd),
    /// The deadline fired and the task has ended; what it returned is
    /// dropped.
    TimedOut,
}

/// The task loop of an isolated child after a task that left no work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoLeftWork {
    /// The child has no task loop.
    NoTaskLoop,
    /// The loop holds no task that is not done.
    LoopHoldsNoTask,
}

/// How the work that an isolated task left on its loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeftWorkEnd {
    /// It ended before the child cancelled it.
    InGrace,
    /// It was alive at the end of the grace and ended after a cancel.
    Cancelled,
}

impl LeftWorkEnd {
    fn as_str(self) -> &'static str {
        match self {
            Self::InGrace => "in_grace",
            Self::Cancelled => "cancelled",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "in_grace" => Some(Self::InGrace),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// What a child does once the frame that ends a run is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildAfterFrame {
    /// The child takes the next run.
    ServesOn,
    /// The core of the child stopped: the child ends itself with
    /// [`CORE_STOPPED_EXIT_CODE`](super::CORE_STOPPED_EXIT_CODE) once the
    /// frame is written. The parent sends it no run, waits for its exit,
    /// and starts a new child.
    EndsItself,
}

impl ChildAfterFrame {
    fn as_str(self) -> &'static str {
        match self {
            Self::ServesOn => "serves_on",
            Self::EndsItself => "ends_itself",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "serves_on" => Some(Self::ServesOn),
            "ends_itself" => Some(Self::EndsItself),
            _ => None,
        }
    }
}

/// One run of a shared child: its number and its task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RunRef {
    pub dispatch: DispatchNumber,
    pub task_id: Uuid,
}

/// What held a blocked loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopCulprit {
    /// The task of this run, or a task with its mark.
    Run(RunRef),
    /// Code that belongs to no run.
    Unknown,
}

/// Boot finished; the child can take a task. The protocol version is always
/// [`PROTOCOL_VERSION`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReadyFrame {
    pub pid: ProcessId,
    pub rss: RssReading,
}

/// In place of the ready frame: the boot failed, and the child exits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootFailedFrame {
    pub cause: ReportedBootCause,
    pub error_name: BootErrorName,
    pub detail: BootDetail,
}

/// An isolated task ended and left no work on a task loop.
///
/// `started_at` is the start of the attempt: `Reported` for an end with a
/// result envelope, `NotReported` for a stop before running, either for a
/// worker failure.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultFrame {
    pub task_id: Uuid,
    pub end: RunEnd,
    pub started_at: AttemptStart,
    pub rss: RssReading,
    pub task_loop: NoLeftWork,
    pub child: ChildAfterFrame,
}

/// The body of an isolated task returned, and work that it started is still
/// on the task loop. The deadline no longer applies. The body ran, so the
/// attempt started at `started_at`, the time of its start write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyReturnedFrame {
    pub task_id: Uuid,
    pub returned: BodyEnd,
    pub started_at: DateTime<Utc>,
    pub left: LoopHoldsTasks,
}

/// The work an isolated task left has ended. The parent writes what the body
/// returned and replaces the child.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LeftWorkEndedFrame {
    pub task_id: Uuid,
    pub end: LeftWorkEnd,
    pub rss: RssReading,
}

/// The work an isolated task left did not stop after the cancel rounds. The
/// parent kills the child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeftWorkStuckFrame {
    pub task_id: Uuid,
    pub still: LoopHoldsTasks,
}

/// A run of a shared child ended; its seat is free.
///
/// `started_at` is the start of the attempt, by the rule of
/// [`ResultFrame::started_at`]; a timed out run reports it or not.
#[derive(Debug, Clone, PartialEq)]
pub struct SharedResultFrame {
    pub run: RunRef,
    pub end: SeatEnd,
    pub started_at: AttemptStart,
    pub rss: RssReading,
    pub child: ChildAfterFrame,
}

/// A run did not stop after its second cancel and its cancel rounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunStuckFrame {
    pub run: RunRef,
    pub work: StuckWork,
}

/// The body of a run returned before its deadline, and work with its mark
/// was still alive one cancel grace later; the child cancels it in rounds.
/// The body ran, so the attempt started at `started_at`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeftoverCancelFrame {
    pub run: RunRef,
    pub returned: BodyEnd,
    pub started_at: DateTime<Utc>,
}

/// The work a run left after its return did not stop after its cancel
/// rounds. The body ran, so the attempt started at `started_at`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeftoverStuckFrame {
    pub run: RunRef,
    pub work: StuckWork,
    pub returned: BodyEnd,
    pub started_at: DateTime<Utc>,
}

/// The event loop of a shared async child has not turned for `blocked_ms`.
/// The parent kills the child at once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoopBlockedFrame {
    pub culprit: LoopCulprit,
    pub blocked_ms: u64,
    pub location: BlockedWhere,
}

/// The cumulative counts of one event handler since its child started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerCounts {
    /// The name of the handler; not empty.
    pub handler: HandlerName,
    /// The events for which the handler failed.
    pub errors: u64,
    /// The events that the full queue of the handler dropped.
    pub dropped: u64,
}

/// The counts of each event handler of a handler child: at most
/// [`MAX_EVENT_HANDLERS`] handlers, each name once and not empty. A child
/// with no handler has none. Only [`HandlerCountsList::new`] builds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerCountsList(Vec<HandlerCounts>);

/// Why a list of handler counts is not valid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HandlerCountsInvalid {
    #[error(
        "a handler_stats frame counts at most {max} handlers, not {count}",
        max = MAX_EVENT_HANDLERS
    )]
    TooMany { count: usize },
    #[error("a handler has an empty name")]
    EmptyName,
    #[error("the handler '{name}' is counted twice")]
    RepeatedName { name: HandlerName },
}

impl HandlerCountsList {
    /// The counts of `handlers`, in their order. Pure.
    ///
    /// Errors: [`HandlerCountsInvalid`].
    pub fn new(handlers: Vec<HandlerCounts>) -> Result<Self, HandlerCountsInvalid> {
        match handlers.len() <= MAX_EVENT_HANDLERS {
            true => {}
            false => {
                return Err(HandlerCountsInvalid::TooMany {
                    count: handlers.len(),
                })
            }
        }
        check_handler_names(&handlers)?;
        Ok(Self(handlers))
    }

    pub fn handlers(&self) -> &[HandlerCounts] {
        &self.0
    }
}

/// Every name of `handlers` is not empty and is there once. Pure.
fn check_handler_names(handlers: &[HandlerCounts]) -> Result<(), HandlerCountsInvalid> {
    let mut names = HashSet::with_capacity(handlers.len());
    for counts in handlers {
        let name = counts.handler.as_str();
        match (name.is_empty(), names.insert(name)) {
            (true, _) => return Err(HandlerCountsInvalid::EmptyName),
            (false, false) => {
                return Err(HandlerCountsInvalid::RepeatedName {
                    name: counts.handler.clone(),
                })
            }
            (false, true) => {}
        }
    }
    Ok(())
}

/// The counts of each event handler of a handler child since it started,
/// and its RSS. The child sends one at a fixed interval. Each holds the
/// totals, so a lost frame loses no count.
#[derive(Debug, Clone, PartialEq)]
pub struct HandlerStatsFrame {
    pub handlers: HandlerCountsList,
    pub rss: RssReading,
}

/// A frame from a child to the worker parent.
#[derive(Debug, Clone, PartialEq)]
pub enum ChildFrame {
    Ready(ReadyFrame),
    BootFailed(BootFailedFrame),
    Result(ResultFrame),
    BodyReturned(BodyReturnedFrame),
    LeftWorkEnded(LeftWorkEndedFrame),
    LeftWorkStuck(LeftWorkStuckFrame),
    SharedResult(SharedResultFrame),
    /// The deadline of a run fired. A shared async child cancelled its task.
    DeadlineFired(RunRef),
    /// The cancel grace of a run ran out while it was alive; the child sent
    /// the second cancel.
    SecondCancel(RunRef),
    RunStuck(RunStuckFrame),
    LeftoverCancel(LeftoverCancelFrame),
    LeftoverStuck(LeftoverStuckFrame),
    LoopBlocked(LoopBlockedFrame),
    HandlerStats(HandlerStatsFrame),
}

/// Why the parent refuses a child frame. Each refusal kills the child.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChildFrameRefusal {
    #[error(transparent)]
    Malformed(#[from] MalformedFrame),
    /// Only a ready frame gives this.
    #[error(transparent)]
    ProtocolMismatch(ProtocolMismatch),
}

fn protocol_refusal(check: ProtocolCheck) -> ChildFrameRefusal {
    match check {
        ProtocolCheck::Mismatch(mismatch) => ChildFrameRefusal::ProtocolMismatch(mismatch),
        ProtocolCheck::Malformed(malformed) => ChildFrameRefusal::Malformed(malformed),
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ChildWire<'a> {
    Ready {
        protocol: u32,
        pid: u32,
        rss_mb: Option<f64>,
    },
    BootFailed {
        cause: BootCauseWire,
        error_name: &'a str,
        detail: &'a str,
    },
    Result {
        task_id: Uuid,
        end: EndWire<'a>,
        started_at: StartWire,
        rss_mb: Option<f64>,
        task_loop: TaskLoopWire,
        child: &'static str,
    },
    BodyReturned {
        task_id: Uuid,
        returned: EndWire<'a>,
        started_at: String,
        left: HeldWire<'a>,
    },
    LeftWorkEnded {
        task_id: Uuid,
        end: &'static str,
        rss_mb: Option<f64>,
    },
    LeftWorkStuck {
        task_id: Uuid,
        still: HeldWire<'a>,
    },
    SharedResult {
        dispatch: u64,
        task_id: Uuid,
        end: EndWire<'a>,
        started_at: StartWire,
        rss_mb: Option<f64>,
        child: &'static str,
    },
    DeadlineFired {
        dispatch: u64,
        task_id: Uuid,
    },
    SecondCancel {
        dispatch: u64,
        task_id: Uuid,
    },
    RunStuck {
        dispatch: u64,
        task_id: Uuid,
        tasks: u64,
        executor_items: u64,
        names: &'a [LeftoverName],
    },
    LeftoverCancel {
        dispatch: u64,
        task_id: Uuid,
        returned: EndWire<'a>,
        started_at: String,
    },
    LeftoverStuck {
        dispatch: u64,
        task_id: Uuid,
        tasks: u64,
        executor_items: u64,
        names: &'a [LeftoverName],
        returned: EndWire<'a>,
        started_at: String,
    },
    LoopBlocked {
        culprit: CulpritWire,
        blocked_ms: u64,
        #[serde(rename = "where")]
        location: &'a str,
    },
    HandlerStats {
        handlers: Vec<HandlerCountsWire<'a>>,
        rss_mb: Option<f64>,
    },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StartWire {
    Reported { time: String },
    NotReported,
}

#[derive(Serialize)]
struct HandlerCountsWire<'a> {
    handler: &'a str,
    errors: u64,
    dropped: u64,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BootCauseWire {
    AppLoadFailed,
    ChildPoolSetupFailed,
    ResourceShortage { errno: &'static str },
    TaskLoopCreateFailed,
    ThreadStartRefused,
}

/// The end of a run, a returned body or a seat. The result envelope travels
/// in the body, so `result` has no field of its own.
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum EndWire<'a> {
    Result,
    StoppedBeforeRunning { reason: &'static str },
    WorkerFailure { detail: &'a str },
    TimedOut,
}

#[derive(Serialize)]
#[serde(tag = "kind")]
enum TaskLoopWire {
    #[serde(rename = "none")]
    NoTaskLoop,
    #[serde(rename = "empty")]
    LoopHoldsNoTask,
}

#[derive(Serialize)]
struct HeldWire<'a> {
    count: u64,
    names: &'a [LeftoverName],
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CulpritWire {
    Run { dispatch: u64, task_id: Uuid },
    Unknown,
}

fn boot_cause_wire(cause: ReportedBootCause) -> BootCauseWire {
    match cause {
        ReportedBootCause::AppLoadFailed => BootCauseWire::AppLoadFailed,
        ReportedBootCause::ChildPoolSetupFailed => BootCauseWire::ChildPoolSetupFailed,
        ReportedBootCause::ResourceShortage(errno) => BootCauseWire::ResourceShortage {
            errno: errno.as_str(),
        },
        ReportedBootCause::TaskLoopCreateFailed => BootCauseWire::TaskLoopCreateFailed,
        ReportedBootCause::ThreadStartRefused => BootCauseWire::ThreadStartRefused,
    }
}

/// The metadata of a run end, and the body it carries.
fn run_end_wire(end: &RunEnd) -> (EndWire<'_>, &[u8]) {
    match end {
        RunEnd::Result(envelope) => (EndWire::Result, envelope.as_str().as_bytes()),
        RunEnd::StoppedBeforeRunning(reason) => (
            EndWire::StoppedBeforeRunning {
                reason: reason.as_str(),
            },
            NO_BODY,
        ),
        RunEnd::WorkerFailure(detail) => (
            EndWire::WorkerFailure {
                detail: detail.as_str(),
            },
            NO_BODY,
        ),
    }
}

fn body_end_wire(end: &BodyEnd) -> (EndWire<'_>, &[u8]) {
    match end {
        BodyEnd::Result(envelope) => (EndWire::Result, envelope.as_str().as_bytes()),
        BodyEnd::WorkerFailure(detail) => (
            EndWire::WorkerFailure {
                detail: detail.as_str(),
            },
            NO_BODY,
        ),
    }
}

fn seat_end_wire(end: &SeatEnd) -> (EndWire<'_>, &[u8]) {
    match end {
        SeatEnd::Finished(run_end) => run_end_wire(run_end),
        SeatEnd::TimedOut => (EndWire::TimedOut, NO_BODY),
    }
}

fn task_loop_wire(task_loop: NoLeftWork) -> TaskLoopWire {
    match task_loop {
        NoLeftWork::NoTaskLoop => TaskLoopWire::NoTaskLoop,
        NoLeftWork::LoopHoldsNoTask => TaskLoopWire::LoopHoldsNoTask,
    }
}

fn held_wire(held: &LoopHoldsTasks) -> HeldWire<'_> {
    HeldWire {
        count: held.count().get(),
        names: held.names(),
    }
}

fn start_wire(started_at: AttemptStart) -> StartWire {
    match started_at {
        AttemptStart::Reported { started_at } => StartWire::Reported {
            time: time_text(&started_at),
        },
        AttemptStart::NotReported => StartWire::NotReported,
    }
}

fn handler_counts_wire(handlers: &HandlerCountsList) -> Vec<HandlerCountsWire<'_>> {
    handlers
        .handlers()
        .iter()
        .map(|counts| HandlerCountsWire {
            handler: counts.handler.as_str(),
            errors: counts.errors,
            dropped: counts.dropped,
        })
        .collect()
}

fn culprit_wire(culprit: LoopCulprit) -> CulpritWire {
    match culprit {
        LoopCulprit::Run(run) => CulpritWire::Run {
            dispatch: run.dispatch.get(),
            task_id: run.task_id,
        },
        LoopCulprit::Unknown => CulpritWire::Unknown,
    }
}

/// Encode a child frame. `Err`: the metadata or the result is over a bound
/// of the header; nothing is sent then.
pub fn encode_child_frame(frame: &ChildFrame) -> Result<EncodedFrame<'_>, FrameEncodeError> {
    match frame {
        ChildFrame::Ready(ready) => pack(
            &ChildWire::Ready {
                protocol: PROTOCOL_VERSION,
                pid: ready.pid.get(),
                rss_mb: ready.rss.wire(),
            },
            NO_BODY,
        ),
        ChildFrame::BootFailed(failed) => pack(
            &ChildWire::BootFailed {
                cause: boot_cause_wire(failed.cause),
                error_name: failed.error_name.as_str(),
                detail: failed.detail.as_str(),
            },
            NO_BODY,
        ),
        ChildFrame::Result(result) => {
            let (end, body) = run_end_wire(&result.end);
            pack(
                &ChildWire::Result {
                    task_id: result.task_id,
                    end,
                    started_at: start_wire(result.started_at),
                    rss_mb: result.rss.wire(),
                    task_loop: task_loop_wire(result.task_loop),
                    child: result.child.as_str(),
                },
                body,
            )
        }
        ChildFrame::BodyReturned(returned) => {
            let (end, body) = body_end_wire(&returned.returned);
            pack(
                &ChildWire::BodyReturned {
                    task_id: returned.task_id,
                    returned: end,
                    started_at: time_text(&returned.started_at),
                    left: held_wire(&returned.left),
                },
                body,
            )
        }
        ChildFrame::LeftWorkEnded(ended) => pack(
            &ChildWire::LeftWorkEnded {
                task_id: ended.task_id,
                end: ended.end.as_str(),
                rss_mb: ended.rss.wire(),
            },
            NO_BODY,
        ),
        ChildFrame::LeftWorkStuck(stuck) => pack(
            &ChildWire::LeftWorkStuck {
                task_id: stuck.task_id,
                still: held_wire(&stuck.still),
            },
            NO_BODY,
        ),
        ChildFrame::SharedResult(result) => {
            let (end, body) = seat_end_wire(&result.end);
            pack(
                &ChildWire::SharedResult {
                    dispatch: result.run.dispatch.get(),
                    task_id: result.run.task_id,
                    end,
                    started_at: start_wire(result.started_at),
                    rss_mb: result.rss.wire(),
                    child: result.child.as_str(),
                },
                body,
            )
        }
        ChildFrame::DeadlineFired(run) => pack(
            &ChildWire::DeadlineFired {
                dispatch: run.dispatch.get(),
                task_id: run.task_id,
            },
            NO_BODY,
        ),
        ChildFrame::SecondCancel(run) => pack(
            &ChildWire::SecondCancel {
                dispatch: run.dispatch.get(),
                task_id: run.task_id,
            },
            NO_BODY,
        ),
        ChildFrame::RunStuck(stuck) => pack(
            &ChildWire::RunStuck {
                dispatch: stuck.run.dispatch.get(),
                task_id: stuck.run.task_id,
                tasks: stuck.work.tasks(),
                executor_items: stuck.work.executor_items(),
                names: stuck.work.names(),
            },
            NO_BODY,
        ),
        ChildFrame::LeftoverCancel(cancel) => {
            let (end, body) = body_end_wire(&cancel.returned);
            pack(
                &ChildWire::LeftoverCancel {
                    dispatch: cancel.run.dispatch.get(),
                    task_id: cancel.run.task_id,
                    returned: end,
                    started_at: time_text(&cancel.started_at),
                },
                body,
            )
        }
        ChildFrame::LeftoverStuck(stuck) => {
            let (end, body) = body_end_wire(&stuck.returned);
            pack(
                &ChildWire::LeftoverStuck {
                    dispatch: stuck.run.dispatch.get(),
                    task_id: stuck.run.task_id,
                    tasks: stuck.work.tasks(),
                    executor_items: stuck.work.executor_items(),
                    names: stuck.work.names(),
                    returned: end,
                    started_at: time_text(&stuck.started_at),
                },
                body,
            )
        }
        ChildFrame::LoopBlocked(blocked) => pack(
            &ChildWire::LoopBlocked {
                culprit: culprit_wire(blocked.culprit),
                blocked_ms: blocked.blocked_ms,
                location: blocked.location.as_str(),
            },
            NO_BODY,
        ),
        ChildFrame::HandlerStats(stats) => pack(
            &ChildWire::HandlerStats {
                handlers: handler_counts_wire(&stats.handlers),
                rss_mb: stats.rss.wire(),
            },
            NO_BODY,
        ),
    }
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Build a child frame from received bytes. Used by the parent.
///
/// A ready frame names its protocol version first: a frame of another
/// version gives [`ChildFrameRefusal::ProtocolMismatch`] before any other
/// field is read.
pub fn decode_child_frame(meta: &[u8], body: Vec<u8>) -> Result<ChildFrame, ChildFrameRefusal> {
    let map = parse_metadata(meta)?;
    let fields = Fields::root(&map);
    let frame = match fields.kind()? {
        "ready" => {
            check_protocol(&map).map_err(protocol_refusal)?;
            ChildFrame::Ready(decode_ready(&fields, &body)?)
        }
        "boot_failed" => ChildFrame::BootFailed(decode_boot_failed(&fields, &body)?),
        "result" => ChildFrame::Result(decode_result(&fields, body)?),
        "body_returned" => ChildFrame::BodyReturned(decode_body_returned(&fields, body)?),
        "left_work_ended" => ChildFrame::LeftWorkEnded(decode_left_work_ended(&fields, &body)?),
        "left_work_stuck" => ChildFrame::LeftWorkStuck(decode_left_work_stuck(&fields, &body)?),
        "shared_result" => ChildFrame::SharedResult(decode_shared_result(&fields, body)?),
        "deadline_fired" => ChildFrame::DeadlineFired(decode_run_report(&fields, &body)?),
        "second_cancel" => ChildFrame::SecondCancel(decode_run_report(&fields, &body)?),
        "run_stuck" => ChildFrame::RunStuck(decode_run_stuck(&fields, &body)?),
        "leftover_cancel" => ChildFrame::LeftoverCancel(decode_leftover_cancel(&fields, body)?),
        "leftover_stuck" => ChildFrame::LeftoverStuck(decode_leftover_stuck(&fields, body)?),
        "loop_blocked" => ChildFrame::LoopBlocked(decode_loop_blocked(&fields, &body)?),
        "handler_stats" => ChildFrame::HandlerStats(decode_handler_stats(&fields, &body)?),
        other => return Err(fields.unknown_kind(other).into()),
    };
    Ok(frame)
}

fn decode_ready(fields: &Fields<'_>, body: &[u8]) -> Result<ReadyFrame, MalformedFrame> {
    fields.only(&["kind", "protocol", "pid", "rss_mb"])?;
    no_body("ready", body)?;
    let pid = u32::try_from(fields.whole("pid")?)
        .ok()
        .and_then(ProcessId::new)
        .ok_or_else(|| fields.out_of_bounds("pid"))?;
    Ok(ReadyFrame {
        pid,
        rss: fields.rss("rss_mb")?,
    })
}

fn decode_boot_cause(cause: &Fields<'_>) -> Result<ReportedBootCause, MalformedFrame> {
    match cause.kind()? {
        "app_load_failed" => {
            cause.only(&["kind"])?;
            Ok(ReportedBootCause::AppLoadFailed)
        }
        "child_pool_setup_failed" => {
            cause.only(&["kind"])?;
            Ok(ReportedBootCause::ChildPoolSetupFailed)
        }
        "resource_shortage" => {
            cause.only(&["kind", "errno"])?;
            ShortageErrno::from_name(cause.text("errno")?)
                .map(ReportedBootCause::ResourceShortage)
                .ok_or_else(|| {
                    cause.broken_rule("errno", "is not one of EMFILE, ENFILE, ENOMEM, EAGAIN")
                })
        }
        "task_loop_create_failed" => {
            cause.only(&["kind"])?;
            Ok(ReportedBootCause::TaskLoopCreateFailed)
        }
        "thread_start_refused" => {
            cause.only(&["kind"])?;
            Ok(ReportedBootCause::ThreadStartRefused)
        }
        "spawn_failed" => Err(cause.broken_rule("kind", "a child cannot report a spawn failure")),
        other => Err(cause.unknown_kind(other)),
    }
}

fn decode_boot_failed(fields: &Fields<'_>, body: &[u8]) -> Result<BootFailedFrame, MalformedFrame> {
    fields.only(&["kind", "cause", "error_name", "detail"])?;
    no_body("boot_failed", body)?;
    Ok(BootFailedFrame {
        cause: decode_boot_cause(&fields.object("cause")?)?,
        error_name: fields.bounded_text("error_name")?,
        detail: fields.bounded_text("detail")?,
    })
}

fn decode_stop_reason(end: &Fields<'_>) -> Result<StopReason, MalformedFrame> {
    StopReason::parse(end.text("reason")?)
        .ok_or_else(|| end.broken_rule("reason", "is not a known stop reason"))
}

/// A run end; the body is the envelope of a `result` end and empty for
/// every other end.
fn decode_run_end(end: &Fields<'_>, body: Vec<u8>) -> Result<RunEnd, MalformedFrame> {
    match end.kind()? {
        "result" => {
            end.only(&["kind"])?;
            Ok(RunEnd::Result(ResultEnvelopeText::new(text_body(body)?)))
        }
        "stopped_before_running" => {
            end.only(&["kind", "reason"])?;
            no_body("stopped_before_running", &body)?;
            Ok(RunEnd::StoppedBeforeRunning(decode_stop_reason(end)?))
        }
        "worker_failure" => {
            end.only(&["kind", "detail"])?;
            no_body("worker_failure", &body)?;
            Ok(RunEnd::WorkerFailure(end.bounded_text("detail")?))
        }
        other => Err(end.unknown_kind(other)),
    }
}

fn decode_body_end(end: &Fields<'_>, body: Vec<u8>) -> Result<BodyEnd, MalformedFrame> {
    match end.kind()? {
        "result" => {
            end.only(&["kind"])?;
            Ok(BodyEnd::Result(ResultEnvelopeText::new(text_body(body)?)))
        }
        "worker_failure" => {
            end.only(&["kind", "detail"])?;
            no_body("worker_failure", &body)?;
            Ok(BodyEnd::WorkerFailure(end.bounded_text("detail")?))
        }
        other => Err(end.unknown_kind(other)),
    }
}

fn decode_seat_end(end: &Fields<'_>, body: Vec<u8>) -> Result<SeatEnd, MalformedFrame> {
    match end.kind()? {
        "timed_out" => {
            end.only(&["kind"])?;
            no_body("timed_out", &body)?;
            Ok(SeatEnd::TimedOut)
        }
        _ => decode_run_end(end, body).map(SeatEnd::Finished),
    }
}

fn decode_task_loop(task_loop: &Fields<'_>) -> Result<NoLeftWork, MalformedFrame> {
    let kind = task_loop.kind()?;
    task_loop.only(&["kind"])?;
    match kind {
        "none" => Ok(NoLeftWork::NoTaskLoop),
        "empty" => Ok(NoLeftWork::LoopHoldsNoTask),
        other => Err(task_loop.unknown_kind(other)),
    }
}

fn decode_held(held: &Fields<'_>) -> Result<LoopHoldsTasks, MalformedFrame> {
    held.only(&["count", "names"])?;
    let count = NonZeroU64::new(held.whole("count")?).ok_or_else(|| held.out_of_bounds("count"))?;
    let names = held.bounded_text_list("names")?;
    LoopHoldsTasks::new(count, names)
        .map_err(|_| held.broken_rule("names", "must name min(count, 10) tasks"))
}

/// What the child does after a frame that ends a run: its `child` field.
fn decode_child_after(fields: &Fields<'_>) -> Result<ChildAfterFrame, MalformedFrame> {
    ChildAfterFrame::parse(fields.text("child")?)
        .ok_or_else(|| fields.broken_rule("child", "is not one of serves_on, ends_itself"))
}

/// The start of an attempt: `reported` with its time, or `not_reported`.
fn decode_start(start: &Fields<'_>) -> Result<AttemptStart, MalformedFrame> {
    match start.kind()? {
        "reported" => {
            start.only(&["kind", "time"])?;
            Ok(AttemptStart::Reported {
                started_at: start.time("time")?,
            })
        }
        "not_reported" => {
            start.only(&["kind"])?;
            Ok(AttemptStart::NotReported)
        }
        other => Err(start.unknown_kind(other)),
    }
}

/// The start of an attempt that ended with `end`, read from the
/// `started_at` field. A stop before running has no start; a result
/// envelope comes after the start write; a worker failure may come before
/// or after it.
fn decode_start_of(fields: &Fields<'_>, end: &RunEnd) -> Result<AttemptStart, MalformedFrame> {
    let started_at = decode_start(&fields.object("started_at")?)?;
    match (end, started_at) {
        (RunEnd::StoppedBeforeRunning(_), AttemptStart::Reported { .. }) => Err(fields
            .broken_rule(
                "started_at",
                "a run that stopped before running has no start",
            )),
        (RunEnd::Result(_), AttemptStart::NotReported) => Err(fields.broken_rule(
            "started_at",
            "a run that ended with a result envelope reports its start",
        )),
        (RunEnd::StoppedBeforeRunning(_), AttemptStart::NotReported)
        | (RunEnd::Result(_), AttemptStart::Reported { .. })
        | (RunEnd::WorkerFailure(_), _) => Ok(started_at),
    }
}

fn decode_result(fields: &Fields<'_>, body: Vec<u8>) -> Result<ResultFrame, MalformedFrame> {
    fields.only(&[
        "kind",
        "task_id",
        "end",
        "started_at",
        "rss_mb",
        "task_loop",
        "child",
    ])?;
    let end = decode_run_end(&fields.object("end")?, body)?;
    Ok(ResultFrame {
        task_id: fields.task_id("task_id")?,
        started_at: decode_start_of(fields, &end)?,
        end,
        rss: fields.rss("rss_mb")?,
        task_loop: decode_task_loop(&fields.object("task_loop")?)?,
        child: decode_child_after(fields)?,
    })
}

fn decode_body_returned(
    fields: &Fields<'_>,
    body: Vec<u8>,
) -> Result<BodyReturnedFrame, MalformedFrame> {
    fields.only(&["kind", "task_id", "returned", "started_at", "left"])?;
    Ok(BodyReturnedFrame {
        task_id: fields.task_id("task_id")?,
        returned: decode_body_end(&fields.object("returned")?, body)?,
        started_at: fields.time("started_at")?,
        left: decode_held(&fields.object("left")?)?,
    })
}

fn decode_left_work_ended(
    fields: &Fields<'_>,
    body: &[u8],
) -> Result<LeftWorkEndedFrame, MalformedFrame> {
    fields.only(&["kind", "task_id", "end", "rss_mb"])?;
    no_body("left_work_ended", body)?;
    let end = LeftWorkEnd::parse(fields.text("end")?)
        .ok_or_else(|| fields.broken_rule("end", "is not one of in_grace, cancelled"))?;
    Ok(LeftWorkEndedFrame {
        task_id: fields.task_id("task_id")?,
        end,
        rss: fields.rss("rss_mb")?,
    })
}

fn decode_left_work_stuck(
    fields: &Fields<'_>,
    body: &[u8],
) -> Result<LeftWorkStuckFrame, MalformedFrame> {
    fields.only(&["kind", "task_id", "still"])?;
    no_body("left_work_stuck", body)?;
    Ok(LeftWorkStuckFrame {
        task_id: fields.task_id("task_id")?,
        still: decode_held(&fields.object("still")?)?,
    })
}

fn decode_run_ref(fields: &Fields<'_>) -> Result<RunRef, MalformedFrame> {
    Ok(RunRef {
        dispatch: fields.dispatch("dispatch")?,
        task_id: fields.task_id("task_id")?,
    })
}

fn decode_shared_result(
    fields: &Fields<'_>,
    body: Vec<u8>,
) -> Result<SharedResultFrame, MalformedFrame> {
    fields.only(&[
        "kind",
        "dispatch",
        "task_id",
        "end",
        "started_at",
        "rss_mb",
        "child",
    ])?;
    let end = decode_seat_end(&fields.object("end")?, body)?;
    let started_at = match &end {
        SeatEnd::Finished(run_end) => decode_start_of(fields, run_end)?,
        SeatEnd::TimedOut => decode_start(&fields.object("started_at")?)?,
    };
    Ok(SharedResultFrame {
        run: decode_run_ref(fields)?,
        end,
        started_at,
        rss: fields.rss("rss_mb")?,
        child: decode_child_after(fields)?,
    })
}

/// A report about one run with no other field: `deadline_fired` or
/// `second_cancel`.
fn decode_run_report(fields: &Fields<'_>, body: &[u8]) -> Result<RunRef, MalformedFrame> {
    fields.only(&["kind", "dispatch", "task_id"])?;
    no_body("run report", body)?;
    decode_run_ref(fields)
}

fn decode_stuck_work(fields: &Fields<'_>) -> Result<StuckWork, MalformedFrame> {
    StuckWork::new(
        fields.whole("tasks")?,
        fields.whole("executor_items")?,
        fields.bounded_text_list("names")?,
    )
    .map_err(|_| {
        fields.broken_rule(
            "tasks",
            "a stuck report needs one live task or item and min(tasks, 10) names",
        )
    })
}

fn decode_run_stuck(fields: &Fields<'_>, body: &[u8]) -> Result<RunStuckFrame, MalformedFrame> {
    fields.only(&[
        "kind",
        "dispatch",
        "task_id",
        "tasks",
        "executor_items",
        "names",
    ])?;
    no_body("run_stuck", body)?;
    Ok(RunStuckFrame {
        run: decode_run_ref(fields)?,
        work: decode_stuck_work(fields)?,
    })
}

fn decode_leftover_cancel(
    fields: &Fields<'_>,
    body: Vec<u8>,
) -> Result<LeftoverCancelFrame, MalformedFrame> {
    fields.only(&["kind", "dispatch", "task_id", "returned", "started_at"])?;
    Ok(LeftoverCancelFrame {
        run: decode_run_ref(fields)?,
        returned: decode_body_end(&fields.object("returned")?, body)?,
        started_at: fields.time("started_at")?,
    })
}

fn decode_leftover_stuck(
    fields: &Fields<'_>,
    body: Vec<u8>,
) -> Result<LeftoverStuckFrame, MalformedFrame> {
    fields.only(&[
        "kind",
        "dispatch",
        "task_id",
        "tasks",
        "executor_items",
        "names",
        "returned",
        "started_at",
    ])?;
    Ok(LeftoverStuckFrame {
        run: decode_run_ref(fields)?,
        work: decode_stuck_work(fields)?,
        returned: decode_body_end(&fields.object("returned")?, body)?,
        started_at: fields.time("started_at")?,
    })
}

fn decode_culprit(culprit: &Fields<'_>) -> Result<LoopCulprit, MalformedFrame> {
    match culprit.kind()? {
        "run" => {
            culprit.only(&["kind", "dispatch", "task_id"])?;
            Ok(LoopCulprit::Run(decode_run_ref(culprit)?))
        }
        "unknown" => {
            culprit.only(&["kind"])?;
            Ok(LoopCulprit::Unknown)
        }
        other => Err(culprit.unknown_kind(other)),
    }
}

fn decode_loop_blocked(
    fields: &Fields<'_>,
    body: &[u8],
) -> Result<LoopBlockedFrame, MalformedFrame> {
    fields.only(&["kind", "culprit", "blocked_ms", "where"])?;
    no_body("loop_blocked", body)?;
    Ok(LoopBlockedFrame {
        culprit: decode_culprit(&fields.object("culprit")?)?,
        blocked_ms: fields.whole("blocked_ms")?,
        location: fields.bounded_text("where")?,
    })
}

fn decode_handler_counts(counts: &Fields<'_>) -> Result<HandlerCounts, MalformedFrame> {
    counts.only(&["handler", "errors", "dropped"])?;
    Ok(HandlerCounts {
        handler: counts.bounded_text("handler")?,
        errors: counts.whole("errors")?,
        dropped: counts.whole("dropped")?,
    })
}

fn decode_handler_stats(
    fields: &Fields<'_>,
    body: &[u8],
) -> Result<HandlerStatsFrame, MalformedFrame> {
    fields.only(&["kind", "handlers", "rss_mb"])?;
    no_body("handler_stats", body)?;
    match fields.list_len("handlers")? <= MAX_EVENT_HANDLERS {
        true => {}
        false => return Err(fields.out_of_bounds("handlers")),
    }
    let handlers = fields
        .objects("handlers")?
        .iter()
        .map(decode_handler_counts)
        .collect::<Result<Vec<_>, _>>()?;
    let handlers = HandlerCountsList::new(handlers).map_err(|invalid| match invalid {
        HandlerCountsInvalid::TooMany { .. } => fields.out_of_bounds("handlers"),
        HandlerCountsInvalid::EmptyName => {
            fields.broken_rule("handlers", "a handler has an empty name")
        }
        HandlerCountsInvalid::RepeatedName { .. } => {
            fields.broken_rule("handlers", "names a handler twice")
        }
    })?;
    Ok(HandlerStatsFrame {
        handlers,
        rss: fields.rss("rss_mb")?,
    })
}

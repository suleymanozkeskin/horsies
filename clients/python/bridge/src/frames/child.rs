//! The frames from a child to the worker parent as Python classes. The
//! child builds each frame with its constructor and sends the bytes of
//! `encode_child_frame(frame)`.
//!
//! A constructor cuts each text to its bound, as the version-1 child did. Any
//! other value outside its bound (a process id of 0, a dispatch number of 0,
//! a report whose names do not match its count, an unknown choice, a start
//! that does not fit the end of its run, a handler name that is empty or
//! over its bound) is a defect of the child and raises `ValueError`. A
//! handler name is the key of its counts, so it is refused, never cut.

use std::num::NonZeroU64;

use chrono::{DateTime, Utc};
use horsies_core::events::AttemptStart;
use horsies_core::frame_protocol::{
    BlockedWhere, BodyEnd, BodyReturnedFrame, BootDetail, BootErrorName, BootFailedFrame,
    ChildAfterFrame, ChildFrame, DispatchNumber, FailureDetail, HandlerCounts, HandlerCountsList,
    HandlerName, HandlerStatsFrame, LeftWorkEnd, LeftWorkEndedFrame, LeftWorkStuckFrame,
    LeftoverCancelFrame, LeftoverName, LeftoverStuckFrame, LoopBlockedFrame, LoopCulprit,
    LoopHoldsTasks, NoLeftWork, ProcessId, ReadyFrame, ReportedBootCause, ResultEnvelopeText,
    ResultFrame, RssMb, RssReading, RunEnd, RunRef, RunStuckFrame, SeatEnd, SharedResultFrame,
    ShortageErrno, SpanReadCounts, StopReason, StuckWork,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use uuid::Uuid;

use crate::operations::arguments::utc_instant;

/// The RSS of a child in MiB, or `'UNREADABLE'`.
#[derive(FromPyObject)]
pub enum RssArgument {
    Mb(f64),
    Text(String),
}

impl RssArgument {
    fn reading(self) -> PyResult<RssReading> {
        match self {
            Self::Mb(value) => RssMb::new(value).map(RssReading::Mb).ok_or_else(|| {
                PyValueError::new_err(format!(
                    "rss_mb must be a finite number of 0 or more, not {value}"
                ))
            }),
            Self::Text(text) => match text.as_str() {
                "UNREADABLE" => Ok(RssReading::Unreadable),
                other => Err(PyValueError::new_err(format!(
                    "rss_mb must be a number or 'UNREADABLE', not {other:?}"
                ))),
            },
        }
    }
}

fn process_id(pid: u32) -> PyResult<ProcessId> {
    ProcessId::new(pid).ok_or_else(|| {
        PyValueError::new_err(format!("pid must be from 1 to {}, not {pid}", i32::MAX))
    })
}

fn dispatch_number(dispatch: u64) -> PyResult<DispatchNumber> {
    DispatchNumber::new(dispatch)
        .ok_or_else(|| PyValueError::new_err("dispatch must be 1 or more, not 0"))
}

fn run_ref(dispatch: u64, task_id: Uuid) -> PyResult<RunRef> {
    Ok(RunRef {
        dispatch: dispatch_number(dispatch)?,
        task_id,
    })
}

/// Each name cut to its bound.
fn leftover_names(names: Vec<String>) -> Vec<LeftoverName> {
    names
        .into_iter()
        .map(|name| LeftoverName::cut(&name))
        .collect()
}

/// What the child does once its result frame is written: `'SERVES_ON'`,
/// or `'ENDS_ITSELF'` when its core stopped and it ends itself.
fn child_after_frame(child: &str) -> PyResult<ChildAfterFrame> {
    match child {
        "SERVES_ON" => Ok(ChildAfterFrame::ServesOn),
        "ENDS_ITSELF" => Ok(ChildAfterFrame::EndsItself),
        other => Err(PyValueError::new_err(format!(
            "child must be 'SERVES_ON' or 'ENDS_ITSELF', not {other:?}"
        ))),
    }
}

/// The start of an attempt, as the child passes it: the aware `datetime`
/// that its start write returned, or `'NOT_REPORTED'`.
#[derive(FromPyObject)]
pub enum StartArgument<'py> {
    Text(String),
    Time(Bound<'py, PyAny>),
}

impl StartArgument<'_> {
    /// The start: a naive `datetime` raises `TypeError`, another text than
    /// `'NOT_REPORTED'` raises `ValueError`.
    fn attempt_start(self) -> PyResult<AttemptStart> {
        match self {
            Self::Text(text) => match text.as_str() {
                "NOT_REPORTED" => Ok(AttemptStart::NotReported),
                other => Err(PyValueError::new_err(format!(
                    "started_at must be an aware datetime or 'NOT_REPORTED', not {other:?}"
                ))),
            },
            Self::Time(time) => Ok(AttemptStart::Reported {
                started_at: utc_instant(&time)?,
            }),
        }
    }
}

/// The start of a run that ended with `end`: a stop before running has no
/// start, and a result envelope comes after the start write. A start that
/// does not fit its end is a defect of the child.
fn start_of_end(end: &RunEnd, started_at: AttemptStart) -> PyResult<AttemptStart> {
    match (end, started_at) {
        (RunEnd::StoppedBeforeRunning(_), AttemptStart::Reported { .. }) => {
            Err(PyValueError::new_err(
                "started_at of a run that stopped before running must be 'NOT_REPORTED'",
            ))
        }
        (RunEnd::Result(_), AttemptStart::NotReported) => Err(PyValueError::new_err(
            "started_at of a run that ended with a result envelope must be its start time",
        )),
        (RunEnd::StoppedBeforeRunning(_), AttemptStart::NotReported)
        | (RunEnd::Result(_), AttemptStart::Reported { .. })
        | (RunEnd::WorkerFailure(_), _) => Ok(started_at),
    }
}

/// The task produced a result envelope: a value or a task error.
#[pyclass(module = "horsies._core", frozen)]
pub struct TaskResult {
    envelope: Py<PyString>,
}

#[pymethods]
impl TaskResult {
    #[new]
    #[pyo3(signature = (*, envelope))]
    fn new(envelope: Py<PyString>) -> Self {
        Self { envelope }
    }
}

impl TaskResult {
    fn envelope_text(&self, py: Python<'_>) -> PyResult<ResultEnvelopeText> {
        let text = self.envelope.bind(py).to_str()?;
        Ok(ResultEnvelopeText::new(text.to_owned()))
    }
}

/// The child stopped before it set the row RUNNING; no task code ran.
#[pyclass(module = "horsies._core", frozen)]
pub struct StoppedBeforeRunning {
    reason: StopReason,
}

#[pymethods]
impl StoppedBeforeRunning {
    /// `reason`: `'CLAIM_LOST'`, `'OWNERSHIP_UNCONFIRMED'`,
    /// `'WORKFLOW_STOPPED'`, `'TASK_EXPIRED'`, `'NODE_NOT_LINKED'` or
    /// `'ORPHAN_CANCELLED'`.
    #[new]
    #[pyo3(signature = (*, reason))]
    fn new(reason: &str) -> PyResult<Self> {
        let reason = match reason {
            "CLAIM_LOST" => StopReason::ClaimLost,
            "OWNERSHIP_UNCONFIRMED" => StopReason::OwnershipUnconfirmed,
            "WORKFLOW_STOPPED" => StopReason::WorkflowStopped,
            "TASK_EXPIRED" => StopReason::TaskExpired,
            "NODE_NOT_LINKED" => StopReason::NodeNotLinked,
            "ORPHAN_CANCELLED" => StopReason::OrphanCancelled,
            other => {
                return Err(PyValueError::new_err(format!(
                    "reason must be 'CLAIM_LOST', 'OWNERSHIP_UNCONFIRMED', \
                     'WORKFLOW_STOPPED', 'TASK_EXPIRED', 'NODE_NOT_LINKED' or \
                     'ORPHAN_CANCELLED', not {other:?}"
                )))
            }
        };
        Ok(Self { reason })
    }
}

/// The worker code of the child failed, not the task. The detail is cut to
/// its bound.
#[pyclass(module = "horsies._core", frozen)]
pub struct WorkerFailure {
    detail: FailureDetail,
}

#[pymethods]
impl WorkerFailure {
    #[new]
    #[pyo3(signature = (*, detail))]
    fn new(detail: &str) -> Self {
        Self {
            detail: FailureDetail::cut(detail),
        }
    }
}

/// The end of a run: `TaskResult`, `StoppedBeforeRunning` or
/// `WorkerFailure`.
#[derive(FromPyObject)]
pub enum RunEndArgument<'py> {
    Result(Bound<'py, TaskResult>),
    Stopped(Bound<'py, StoppedBeforeRunning>),
    Failure(Bound<'py, WorkerFailure>),
}

impl RunEndArgument<'_> {
    fn run_end(self) -> PyResult<RunEnd> {
        match self {
            Self::Result(result) => Ok(RunEnd::Result(result.get().envelope_text(result.py())?)),
            Self::Stopped(stopped) => Ok(RunEnd::StoppedBeforeRunning(stopped.get().reason)),
            Self::Failure(failure) => Ok(RunEnd::WorkerFailure(failure.get().detail.clone())),
        }
    }
}

/// What a body returned: `TaskResult` or `WorkerFailure`.
#[derive(FromPyObject)]
pub enum BodyEndArgument<'py> {
    Result(Bound<'py, TaskResult>),
    Failure(Bound<'py, WorkerFailure>),
}

impl BodyEndArgument<'_> {
    fn body_end(self) -> PyResult<BodyEnd> {
        match self {
            Self::Result(result) => Ok(BodyEnd::Result(result.get().envelope_text(result.py())?)),
            Self::Failure(failure) => Ok(BodyEnd::WorkerFailure(failure.get().detail.clone())),
        }
    }
}

/// The end of a run of a shared child: a run end, or `'TIMED_OUT'`.
#[derive(FromPyObject)]
pub enum SeatEndArgument<'py> {
    Finished(RunEndArgument<'py>),
    Text(String),
}

impl SeatEndArgument<'_> {
    fn seat_end(self) -> PyResult<SeatEnd> {
        match self {
            Self::Finished(end) => Ok(SeatEnd::Finished(end.run_end()?)),
            Self::Text(text) => match text.as_str() {
                "TIMED_OUT" => Ok(SeatEnd::TimedOut),
                other => Err(PyValueError::new_err(format!(
                    "end must be a run end or 'TIMED_OUT', not {other:?}"
                ))),
            },
        }
    }
}

/// `count` asyncio tasks that are not done are on a task loop. `names`
/// holds `min(count, 10)` names; each is cut to its bound.
#[pyclass(module = "horsies._core", name = "LoopHoldsTasks", frozen)]
pub struct LoopHoldsTasksRecord {
    held: LoopHoldsTasks,
}

#[pymethods]
impl LoopHoldsTasksRecord {
    #[new]
    #[pyo3(signature = (*, count, names))]
    fn new(count: u64, names: Vec<String>) -> PyResult<Self> {
        let count = NonZeroU64::new(count)
            .ok_or_else(|| PyValueError::new_err("count must be 1 or more, not 0"))?;
        let held = LoopHoldsTasks::new(count, leftover_names(names))
            .map_err(|mismatch| PyValueError::new_err(mismatch.to_string()))?;
        Ok(Self { held })
    }
}

/// The work of a run that did not stop after its cancel rounds: `tasks`
/// asyncio tasks and `executor_items` executor items, at least one in all;
/// `names` holds `min(tasks, 10)` names.
#[pyclass(module = "horsies._core", name = "StuckWork", frozen)]
pub struct StuckWorkRecord {
    work: StuckWork,
}

#[pymethods]
impl StuckWorkRecord {
    #[new]
    #[pyo3(signature = (*, tasks, executor_items, names))]
    fn new(tasks: u64, executor_items: u64, names: Vec<String>) -> PyResult<Self> {
        let work = StuckWork::new(tasks, executor_items, leftover_names(names))
            .map_err(|invalid| PyValueError::new_err(invalid.to_string()))?;
        Ok(Self { work })
    }
}

/// The run whose task held a blocked loop.
#[pyclass(module = "horsies._core", frozen)]
pub struct BlockingRun {
    run: RunRef,
}

#[pymethods]
impl BlockingRun {
    #[new]
    #[pyo3(signature = (*, dispatch, task_id))]
    fn new(dispatch: u64, task_id: Uuid) -> PyResult<Self> {
        Ok(Self {
            run: run_ref(dispatch, task_id)?,
        })
    }
}

/// What held a blocked loop: `BlockingRun`, or `'UNKNOWN'`.
#[derive(FromPyObject)]
pub enum CulpritArgument<'py> {
    Run(Bound<'py, BlockingRun>),
    Text(String),
}

impl CulpritArgument<'_> {
    fn culprit(self) -> PyResult<LoopCulprit> {
        match self {
            Self::Run(run) => Ok(LoopCulprit::Run(run.get().run)),
            Self::Text(text) => match text.as_str() {
                "UNKNOWN" => Ok(LoopCulprit::Unknown),
                other => Err(PyValueError::new_err(format!(
                    "culprit must be a BlockingRun or 'UNKNOWN', not {other:?}"
                ))),
            },
        }
    }
}

/// A boot step failed with an errno of a resource limit.
#[pyclass(module = "horsies._core", frozen)]
pub struct ResourceShortage {
    errno: ShortageErrno,
}

#[pymethods]
impl ResourceShortage {
    /// `errno`: `'EMFILE'`, `'ENFILE'`, `'ENOMEM'` or `'EAGAIN'`.
    #[new]
    #[pyo3(signature = (*, errno))]
    fn new(errno: &str) -> PyResult<Self> {
        let errno = match errno {
            "EMFILE" => ShortageErrno::Emfile,
            "ENFILE" => ShortageErrno::Enfile,
            "ENOMEM" => ShortageErrno::Enomem,
            "EAGAIN" => ShortageErrno::Eagain,
            other => {
                return Err(PyValueError::new_err(format!(
                    "errno must be 'EMFILE', 'ENFILE', 'ENOMEM' or 'EAGAIN', not {other:?}"
                )))
            }
        };
        Ok(Self { errno })
    }
}

/// The cause of a boot failure: `ResourceShortage`, or one of
/// `'APP_LOAD_FAILED'`, `'CHILD_POOL_SETUP_FAILED'`,
/// `'TASK_LOOP_CREATE_FAILED'`, `'THREAD_START_REFUSED'`.
#[derive(FromPyObject)]
pub enum BootCauseArgument<'py> {
    Shortage(Bound<'py, ResourceShortage>),
    Text(String),
}

impl BootCauseArgument<'_> {
    fn cause(self) -> PyResult<ReportedBootCause> {
        match self {
            Self::Shortage(shortage) => {
                Ok(ReportedBootCause::ResourceShortage(shortage.get().errno))
            }
            Self::Text(text) => match text.as_str() {
                "APP_LOAD_FAILED" => Ok(ReportedBootCause::AppLoadFailed),
                "CHILD_POOL_SETUP_FAILED" => Ok(ReportedBootCause::ChildPoolSetupFailed),
                "TASK_LOOP_CREATE_FAILED" => Ok(ReportedBootCause::TaskLoopCreateFailed),
                "THREAD_START_REFUSED" => Ok(ReportedBootCause::ThreadStartRefused),
                other => Err(PyValueError::new_err(format!(
                    "cause must be a ResourceShortage, 'APP_LOAD_FAILED', \
                     'CHILD_POOL_SETUP_FAILED', 'TASK_LOOP_CREATE_FAILED' or \
                     'THREAD_START_REFUSED', not {other:?}"
                ))),
            },
        }
    }
}

/// Defines a child frame class: a frozen class that holds its encoded-ready
/// core frame.
macro_rules! child_frame_class {
    ($(#[$attr:meta])* $record:ident as $python:tt) => {
        $(#[$attr])*
        #[pyclass(module = "horsies._core", name = $python, frozen)]
        pub struct $record {
            frame: ChildFrame,
        }

        impl $record {
            pub fn frame(&self) -> &ChildFrame {
                &self.frame
            }
        }
    };
}

child_frame_class!(
    /// Boot finished; the child can take a task.
    ReadyFrameRecord as "ReadyFrame"
);
child_frame_class!(
    /// In place of the ready frame: the boot failed, and the child exits.
    BootFailedFrameRecord as "BootFailedFrame"
);
child_frame_class!(
    /// An isolated task ended and left no work on a task loop.
    ResultFrameRecord as "ResultFrame"
);
child_frame_class!(
    /// The body of an isolated task returned, and work it started is still
    /// on the task loop.
    BodyReturnedFrameRecord as "BodyReturnedFrame"
);
child_frame_class!(
    /// The work that an isolated task left has ended.
    LeftWorkEndedFrameRecord as "LeftWorkEndedFrame"
);
child_frame_class!(
    /// The work that an isolated task left did not stop after the cancel
    /// rounds.
    LeftWorkStuckFrameRecord as "LeftWorkStuckFrame"
);
child_frame_class!(
    /// A run of a shared child ended; its seat is free.
    SharedResultFrameRecord as "SharedResultFrame"
);
child_frame_class!(
    /// The deadline of a run fired.
    DeadlineFiredFrameRecord as "DeadlineFiredFrame"
);
child_frame_class!(
    /// The cancel grace of a run ran out while it was alive.
    SecondCancelFrameRecord as "SecondCancelFrame"
);
child_frame_class!(
    /// A run did not stop after its second cancel and its cancel rounds.
    RunStuckFrameRecord as "RunStuckFrame"
);
child_frame_class!(
    /// The body of a run returned before its deadline, and its marked work
    /// was alive one cancel grace later.
    LeftoverCancelFrameRecord as "LeftoverCancelFrame"
);
child_frame_class!(
    /// The work a run left after its return did not stop after its cancel
    /// rounds.
    LeftoverStuckFrameRecord as "LeftoverStuckFrame"
);
child_frame_class!(
    /// The event loop of a shared async child has not turned for
    /// `blocked_ms`.
    LoopBlockedFrameRecord as "LoopBlockedFrame"
);
child_frame_class!(
    /// The counts of each event handler of a handler child since it
    /// started, and its RSS.
    HandlerStatsFrameRecord as "HandlerStatsFrame"
);

/// The cumulative counts of one event handler: `errors` events for which it
/// failed, `dropped` events that its full queue dropped. `handler` is its
/// name: 1 to `max_handler_name_chars` characters.
#[pyclass(module = "horsies._core", name = "HandlerCounts", frozen)]
pub struct HandlerCountsRecord {
    counts: HandlerCounts,
}

#[pymethods]
impl HandlerCountsRecord {
    #[new]
    #[pyo3(signature = (*, handler, errors, dropped))]
    fn new(handler: String, errors: u64, dropped: u64) -> PyResult<Self> {
        let handler = match handler.is_empty() {
            true => return Err(PyValueError::new_err("handler must not be empty")),
            false => HandlerName::new(handler)
                .map_err(|over| PyValueError::new_err(format!("handler: {over}")))?,
        };
        Ok(Self {
            counts: HandlerCounts {
                handler,
                errors,
                dropped,
            },
        })
    }
}

#[pymethods]
impl ReadyFrameRecord {
    #[new]
    #[pyo3(signature = (*, pid, rss_mb))]
    fn new(pid: u32, rss_mb: RssArgument) -> PyResult<Self> {
        Ok(Self {
            frame: ChildFrame::Ready(ReadyFrame {
                pid: process_id(pid)?,
                rss: rss_mb.reading()?,
            }),
        })
    }
}

#[pymethods]
impl BootFailedFrameRecord {
    #[new]
    #[pyo3(signature = (*, cause, error_name, detail))]
    fn new(cause: BootCauseArgument<'_>, error_name: &str, detail: &str) -> PyResult<Self> {
        Ok(Self {
            frame: ChildFrame::BootFailed(BootFailedFrame {
                cause: cause.cause()?,
                error_name: BootErrorName::cut(error_name),
                detail: BootDetail::cut(detail),
            }),
        })
    }
}

#[pymethods]
impl ResultFrameRecord {
    /// `started_at`: the aware `datetime` that the start write returned, or
    /// `'NOT_REPORTED'` for a run that stopped before running.
    /// `task_loop`: `'NO_TASK_LOOP'` or `'LOOP_HOLDS_NO_TASK'`. `child`:
    /// `'SERVES_ON'`, or `'ENDS_ITSELF'` when the child ends itself once the
    /// frame is written.
    #[new]
    #[pyo3(signature = (*, task_id, end, started_at, rss_mb, task_loop, child))]
    fn new(
        task_id: Uuid,
        end: RunEndArgument<'_>,
        started_at: StartArgument<'_>,
        rss_mb: RssArgument,
        task_loop: &str,
        child: &str,
    ) -> PyResult<Self> {
        let task_loop = match task_loop {
            "NO_TASK_LOOP" => NoLeftWork::NoTaskLoop,
            "LOOP_HOLDS_NO_TASK" => NoLeftWork::LoopHoldsNoTask,
            other => {
                return Err(PyValueError::new_err(format!(
                    "task_loop must be 'NO_TASK_LOOP' or 'LOOP_HOLDS_NO_TASK', not {other:?}"
                )))
            }
        };
        let end = end.run_end()?;
        Ok(Self {
            frame: ChildFrame::Result(ResultFrame {
                task_id,
                started_at: start_of_end(&end, started_at.attempt_start()?)?,
                end,
                rss: rss_mb.reading()?,
                task_loop,
                child: child_after_frame(child)?,
            }),
        })
    }
}

#[pymethods]
impl BodyReturnedFrameRecord {
    /// `started_at`: the aware `datetime` that the start write returned.
    #[new]
    #[pyo3(signature = (*, task_id, returned, started_at, left))]
    fn new(
        task_id: Uuid,
        returned: BodyEndArgument<'_>,
        started_at: &Bound<'_, PyAny>,
        left: &Bound<'_, LoopHoldsTasksRecord>,
    ) -> PyResult<Self> {
        let started_at: DateTime<Utc> = utc_instant(started_at)?;
        Ok(Self {
            frame: ChildFrame::BodyReturned(BodyReturnedFrame {
                task_id,
                returned: returned.body_end()?,
                started_at,
                left: left.get().held.clone(),
            }),
        })
    }
}

#[pymethods]
impl LeftWorkEndedFrameRecord {
    /// `end`: `'IN_GRACE'` or `'CANCELLED'`.
    #[new]
    #[pyo3(signature = (*, task_id, end, rss_mb))]
    fn new(task_id: Uuid, end: &str, rss_mb: RssArgument) -> PyResult<Self> {
        let end = match end {
            "IN_GRACE" => LeftWorkEnd::InGrace,
            "CANCELLED" => LeftWorkEnd::Cancelled,
            other => {
                return Err(PyValueError::new_err(format!(
                    "end must be 'IN_GRACE' or 'CANCELLED', not {other:?}"
                )))
            }
        };
        Ok(Self {
            frame: ChildFrame::LeftWorkEnded(LeftWorkEndedFrame {
                task_id,
                end,
                rss: rss_mb.reading()?,
            }),
        })
    }
}

#[pymethods]
impl LeftWorkStuckFrameRecord {
    #[new]
    #[pyo3(signature = (*, task_id, still))]
    fn new(task_id: Uuid, still: &Bound<'_, LoopHoldsTasksRecord>) -> Self {
        Self {
            frame: ChildFrame::LeftWorkStuck(LeftWorkStuckFrame {
                task_id,
                still: still.get().held.clone(),
            }),
        }
    }
}

#[pymethods]
impl SharedResultFrameRecord {
    /// `started_at`: the aware `datetime` that the start write returned, or
    /// `'NOT_REPORTED'` for a run that stopped before running. `child`:
    /// `'SERVES_ON'`, or `'ENDS_ITSELF'` when the child ends itself once the
    /// frame is written.
    #[new]
    #[pyo3(signature = (*, dispatch, task_id, end, started_at, rss_mb, child))]
    fn new(
        dispatch: u64,
        task_id: Uuid,
        end: SeatEndArgument<'_>,
        started_at: StartArgument<'_>,
        rss_mb: RssArgument,
        child: &str,
    ) -> PyResult<Self> {
        let end = end.seat_end()?;
        let started_at = started_at.attempt_start()?;
        let started_at = match &end {
            SeatEnd::Finished(run_end) => start_of_end(run_end, started_at)?,
            SeatEnd::TimedOut => started_at,
        };
        Ok(Self {
            frame: ChildFrame::SharedResult(SharedResultFrame {
                run: run_ref(dispatch, task_id)?,
                end,
                started_at,
                rss: rss_mb.reading()?,
                child: child_after_frame(child)?,
            }),
        })
    }
}

#[pymethods]
impl DeadlineFiredFrameRecord {
    #[new]
    #[pyo3(signature = (*, dispatch, task_id))]
    fn new(dispatch: u64, task_id: Uuid) -> PyResult<Self> {
        Ok(Self {
            frame: ChildFrame::DeadlineFired(run_ref(dispatch, task_id)?),
        })
    }
}

#[pymethods]
impl SecondCancelFrameRecord {
    #[new]
    #[pyo3(signature = (*, dispatch, task_id))]
    fn new(dispatch: u64, task_id: Uuid) -> PyResult<Self> {
        Ok(Self {
            frame: ChildFrame::SecondCancel(run_ref(dispatch, task_id)?),
        })
    }
}

#[pymethods]
impl RunStuckFrameRecord {
    #[new]
    #[pyo3(signature = (*, dispatch, task_id, work))]
    fn new(dispatch: u64, task_id: Uuid, work: &Bound<'_, StuckWorkRecord>) -> PyResult<Self> {
        Ok(Self {
            frame: ChildFrame::RunStuck(RunStuckFrame {
                run: run_ref(dispatch, task_id)?,
                work: work.get().work.clone(),
            }),
        })
    }
}

#[pymethods]
impl LeftoverCancelFrameRecord {
    /// `started_at`: the aware `datetime` that the start write returned.
    #[new]
    #[pyo3(signature = (*, dispatch, task_id, returned, started_at))]
    fn new(
        dispatch: u64,
        task_id: Uuid,
        returned: BodyEndArgument<'_>,
        started_at: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let started_at: DateTime<Utc> = utc_instant(started_at)?;
        Ok(Self {
            frame: ChildFrame::LeftoverCancel(LeftoverCancelFrame {
                run: run_ref(dispatch, task_id)?,
                returned: returned.body_end()?,
                started_at,
            }),
        })
    }
}

#[pymethods]
impl LeftoverStuckFrameRecord {
    /// `started_at`: the aware `datetime` that the start write returned.
    #[new]
    #[pyo3(signature = (*, dispatch, task_id, work, returned, started_at))]
    fn new(
        dispatch: u64,
        task_id: Uuid,
        work: &Bound<'_, StuckWorkRecord>,
        returned: BodyEndArgument<'_>,
        started_at: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let started_at: DateTime<Utc> = utc_instant(started_at)?;
        Ok(Self {
            frame: ChildFrame::LeftoverStuck(LeftoverStuckFrame {
                run: run_ref(dispatch, task_id)?,
                work: work.get().work.clone(),
                returned: returned.body_end()?,
                started_at,
            }),
        })
    }
}

#[pymethods]
impl HandlerStatsFrameRecord {
    /// `handlers`: the counts of each handler, each name once, at most
    /// `max_event_handlers`. `span_read_failed`, `span_not_read`: the spans
    /// of attempts of workflow tasks that the worker ended, made with no
    /// workflow node because the node read failed, or did not run. `rss_mb`:
    /// the RSS of the child in MiB, or `'UNREADABLE'`.
    #[new]
    #[pyo3(signature = (*, handlers, span_read_failed, span_not_read, rss_mb))]
    fn new(
        handlers: Vec<Bound<'_, HandlerCountsRecord>>,
        span_read_failed: u64,
        span_not_read: u64,
        rss_mb: RssArgument,
    ) -> PyResult<Self> {
        let counts = handlers
            .into_iter()
            .map(|record| record.get().counts.clone())
            .collect();
        let handlers = HandlerCountsList::new(counts)
            .map_err(|invalid| PyValueError::new_err(invalid.to_string()))?;
        Ok(Self {
            frame: ChildFrame::HandlerStats(HandlerStatsFrame {
                handlers,
                span_reads: SpanReadCounts {
                    read_failed: span_read_failed,
                    not_read: span_not_read,
                },
                rss: rss_mb.reading()?,
            }),
        })
    }
}

#[pymethods]
impl LoopBlockedFrameRecord {
    /// `location` is cut to its bound.
    #[new]
    #[pyo3(signature = (*, culprit, blocked_ms, location))]
    fn new(culprit: CulpritArgument<'_>, blocked_ms: u64, location: &str) -> PyResult<Self> {
        Ok(Self {
            frame: ChildFrame::LoopBlocked(LoopBlockedFrame {
                culprit: culprit.culprit()?,
                blocked_ms,
                location: BlockedWhere::cut(location),
            }),
        })
    }
}

/// A child frame argument: one of the fourteen frame classes.
#[derive(FromPyObject)]
pub enum ChildFrameArgument<'py> {
    Ready(Bound<'py, ReadyFrameRecord>),
    BootFailed(Bound<'py, BootFailedFrameRecord>),
    Result(Bound<'py, ResultFrameRecord>),
    BodyReturned(Bound<'py, BodyReturnedFrameRecord>),
    LeftWorkEnded(Bound<'py, LeftWorkEndedFrameRecord>),
    LeftWorkStuck(Bound<'py, LeftWorkStuckFrameRecord>),
    SharedResult(Bound<'py, SharedResultFrameRecord>),
    DeadlineFired(Bound<'py, DeadlineFiredFrameRecord>),
    SecondCancel(Bound<'py, SecondCancelFrameRecord>),
    RunStuck(Bound<'py, RunStuckFrameRecord>),
    LeftoverCancel(Bound<'py, LeftoverCancelFrameRecord>),
    LeftoverStuck(Bound<'py, LeftoverStuckFrameRecord>),
    LoopBlocked(Bound<'py, LoopBlockedFrameRecord>),
    HandlerStats(Bound<'py, HandlerStatsFrameRecord>),
}

impl ChildFrameArgument<'_> {
    /// `with_frame` of the core frame that the class holds.
    pub fn with_frame<T>(self, with_frame: impl FnOnce(&ChildFrame) -> T) -> T {
        match self {
            Self::Ready(record) => with_frame(record.get().frame()),
            Self::BootFailed(record) => with_frame(record.get().frame()),
            Self::Result(record) => with_frame(record.get().frame()),
            Self::BodyReturned(record) => with_frame(record.get().frame()),
            Self::LeftWorkEnded(record) => with_frame(record.get().frame()),
            Self::LeftWorkStuck(record) => with_frame(record.get().frame()),
            Self::SharedResult(record) => with_frame(record.get().frame()),
            Self::DeadlineFired(record) => with_frame(record.get().frame()),
            Self::SecondCancel(record) => with_frame(record.get().frame()),
            Self::RunStuck(record) => with_frame(record.get().frame()),
            Self::LeftoverCancel(record) => with_frame(record.get().frame()),
            Self::LeftoverStuck(record) => with_frame(record.get().frame()),
            Self::LoopBlocked(record) => with_frame(record.get().frame()),
            Self::HandlerStats(record) => with_frame(record.get().frame()),
        }
    }
}

/// Every class of this module, for the module registration.
pub fn add_child_frame_classes(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<TaskResult>()?;
    module.add_class::<StoppedBeforeRunning>()?;
    module.add_class::<WorkerFailure>()?;
    module.add_class::<LoopHoldsTasksRecord>()?;
    module.add_class::<StuckWorkRecord>()?;
    module.add_class::<BlockingRun>()?;
    module.add_class::<ResourceShortage>()?;
    module.add_class::<ReadyFrameRecord>()?;
    module.add_class::<BootFailedFrameRecord>()?;
    module.add_class::<ResultFrameRecord>()?;
    module.add_class::<BodyReturnedFrameRecord>()?;
    module.add_class::<LeftWorkEndedFrameRecord>()?;
    module.add_class::<LeftWorkStuckFrameRecord>()?;
    module.add_class::<SharedResultFrameRecord>()?;
    module.add_class::<DeadlineFiredFrameRecord>()?;
    module.add_class::<SecondCancelFrameRecord>()?;
    module.add_class::<RunStuckFrameRecord>()?;
    module.add_class::<LeftoverCancelFrameRecord>()?;
    module.add_class::<LeftoverStuckFrameRecord>()?;
    module.add_class::<LoopBlockedFrameRecord>()?;
    module.add_class::<HandlerCountsRecord>()?;
    module.add_class::<HandlerStatsFrameRecord>()?;
    Ok(())
}

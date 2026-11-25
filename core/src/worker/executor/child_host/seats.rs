//! The serve loops of a Rust child, one per child kind.
//!
//! - Isolated: one task at a time; the parent holds the deadline. A body
//!   that returns while work it started with `spawn_in_run` is alive sends
//!   `body_returned`; the child waits 1 s for that work, aborts it in
//!   rounds, writes the marker and sends `left_work_ended`, or sends
//!   `left_work_stuck` for work that did not stop.
//! - Shared async: up to `seats` async bodies at once on the child's
//!   runtime. At the deadline the run's cancel signal fires and
//!   `deadline_fired` is sent; the run ends as a timeout whatever its body
//!   returns. When anything of the run (the body or its work) is alive after
//!   the cancel grace, `second_cancel` is sent and everything of the run is
//!   stopped in rounds (Rust work is dropped; host-held work gets a stop
//!   request and stays), then `run_stuck` names what is alive 0.3 s later,
//!   and the run keeps its seat until it ends or the parent kills the
//!   child. The parent reads from `second_cancel` which of the two ended the
//!   run. A body that returns before its deadline with work alive waits
//!   the cancel grace for it, then sends `leftover_cancel` and aborts it in
//!   rounds, then `leftover_stuck`. A run that timed out or needed a cancel
//!   writes no marker. A body, or work it started with `spawn_in_run`,
//!   that holds a runtime thread for 5 s in one poll is reported with
//!   `loop_blocked` and its run as the culprit, and the parent kills the
//!   child. A host loop that has not turned for 5 s is reported the same
//!   way (`event_loop`).
//! - Shared thread: up to `seats` blocking bodies at once, each on a seat
//!   thread. A thread cannot be stopped: at the deadline `deadline_fired` is
//!   sent, the run's cancel state reads the deadline, and the body runs on;
//!   when it returns, its value is dropped and the run ends timed out. One
//!   atomic gate decides between the return, the deadline and an operator
//!   cancel. When the app gives a host loop (the main thread that hands
//!   each run to the host), a watch thread reports a block of that loop
//!   with `loop_blocked`, with no named culprit.
//!
//! The operator cancel of a run (`cancel_run`, protocol version 4): the
//! cancel statement has moved the row to history and recorded the attempt
//! as `CANCELLED`. The child fires the run's cancel signal with the
//! operator cause, and the run ends cancelled whatever the body returns:
//!
//! - A cancel that comes before the body starts (while the start write is
//!   in flight) is kept; the body does not start, and the end is cancelled
//!   with body `not_started`. A start that ends the run another way reports
//!   that end, and the cancel is dropped.
//! - Isolated: the serve loop reads the parent frames while the run runs.
//!   When the body ends, the work it left is aborted in rounds, and
//!   `run_cancelled` goes out in place of the result; work that holds its
//!   thread past the rounds is reported `held`, and the parent replaces the
//!   child.
//! - Shared async: `cancel_fired`, then the cancel grace, `second_cancel`
//!   and the rounds, and `run_stuck`, as after a deadline. The deadline adds
//!   nothing after an operator cancel; a deadline that fired first keeps the
//!   timeout, and the later cancel is ignored.
//! - Shared thread: `cancel_fired`; the thread runs on, and its return ends
//!   the run cancelled. A deadline that fired first keeps the timeout.
//! - A cancel of a run that the child does not run (its end and the cancel
//!   crossed) is ignored. No run marker is written for a cancelled run: its
//!   row is in history.
//!
//! Work that a body starts with plain `tokio::spawn` is not tracked; work
//! started with `spawn_in_run` belongs to the run (`run_work`).

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinError;
use uuid::Uuid;

use super::body_scope::{BodyOutcome, OpenBody, SpanSource};
use super::event_loop::{HostCulprit, HostLoopWatched, HostPass};
use super::link::FrameWriter;
use super::panic::PanicRecord;
use super::rss::read_rss;
use super::run::{
    async_body_in, kind_mismatch_end, mark_finalizing, prepare, refusal_end, result_end,
    run_blocking_body, start_attempt, HostContext, Prepared, RunFacts, Started,
};
use super::run_work::{
    left_report, millis_since, stuck_report, Alive, CancelCause, Fire, PollsNow, RoundsEnd,
    RunCancelSignal, RunPolls, RunScope, SignalState, WorkWait, ISOLATED_LEFTOVER_GRACE,
};
use crate::core::task::error::{OperationalErrorCode, TaskError};
use crate::core::task::fencing::FencingToken;
use crate::core::task::fn_trait::{AsyncTaskFn, RawTaskResult, RegisteredTask, TaskModule};
use crate::core::task::result::TaskResult;
use crate::events::AttemptStart;
use crate::worker::executor::protocol::{
    BlockedWhere, BodyEnd, BodyReturnedFrame, CancelGraceMs, CancelTarget, CancelledBody,
    CancelledLoop, ChildAfterFrame, ChildFrame, DispatchNumber, FailureDetail, IsolatedWork,
    LeftWorkEnd, LeftWorkEndedFrame, LeftWorkStuckFrame, LeftoverCancelFrame, LeftoverStuckFrame,
    LoopBlockedFrame, LoopCulprit, NoLeftWork, ParentFrame, ResultFrame, RunCancelledFrame, RunEnd,
    RunFrame, RunRef, RunStuckFrame, SeatEnd, SharedResultFrame, SharedRunFrame, SharedWork,
    StoredOptionsRefusal, TaskInputText, TaskTimeoutMs, LOOP_BLOCK_LIMIT,
};
use crate::Execution;

/// How often the poll watch reads the polls in progress.
const POLL_WATCH_INTERVAL: Duration = Duration::from_millis(250);
const POLL_WATCH_THREAD_NAME: &str = "horsies-child-poll-watch";
/// The location of a `loop_blocked` report of a host loop: the core does
/// not see the code that the host runs.
const HOST_LOOP_LOCATION: &str = "the event loop of the host program";
const SEAT_THREAD_NAME: &str = "horsies-child-seat";

/// The longest wait of a seat thread whose body returned after an operator
/// cancel, for the serve loop to queue the `cancel_fired` frame of the run,
/// so that the frame goes out before the end of the run. The serve loop
/// queues it right after it marks the gate; only a writer that takes no
/// frame makes the wait reach its bound.
const CANCEL_REPORT_WAIT: Duration = Duration::from_secs(5);

/// The rule that keeps the deadline out of an isolated child: the parent
/// holds it and kills the child.
const NO_DEADLINE_IN_ISOLATED: &str =
    "the deadline of an isolated run never fires its cancel signal in the child";

/// The rule that keeps the deadline after the start of a shared body: the
/// seat starts the deadline with the body.
const NO_DEADLINE_BEFORE_BODY: &str =
    "the deadline of a shared run fires only after its body started";

/// How a serve loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ServeEnd {
    /// The stop frame came with no task in flight.
    Stopped,
    /// A frame that does not fit the child kind or its state.
    BadParentFrame,
    /// The control thread ended.
    ParentGone,
}

// ---------------------------------------------------------------------------
// Isolated
// ---------------------------------------------------------------------------

pub(super) async fn serve_isolated(
    context: HostContext,
    frames: &mut mpsc::Receiver<ParentFrame>,
) -> ServeEnd {
    let mut held: Option<ParentFrame> = None;
    loop {
        let next = match held.take() {
            Some(frame) => Some(frame),
            None => frames.recv().await,
        };
        match next {
            None => return ServeEnd::ParentGone,
            Some(ParentFrame::Stop) => return ServeEnd::Stopped,
            Some(ParentFrame::Run(run)) => match serve_isolated_run(&context, run, frames).await {
                AfterIsolatedRun::ReadOn => {}
                AfterIsolatedRun::Held(frame) => held = Some(*frame),
                AfterIsolatedRun::ParentGone => return ServeEnd::ParentGone,
            },
            Some(ParentFrame::CancelRun(CancelTarget::Isolated { task_id })) => {
                tracing::debug!(
                    task_id = %task_id,
                    "a cancel_run came with no run in flight; the run ended first, and the cancel \
                     is ignored",
                );
            }
            Some(
                ParentFrame::CancelRun(CancelTarget::Shared(_))
                | ParentFrame::Init(_)
                | ParentFrame::SharedRun(_)
                | ParentFrame::Events(_),
            ) => return ServeEnd::BadParentFrame,
        }
    }
}

/// What the serve loop of an isolated child does after a run.
enum AfterIsolatedRun {
    /// Read the next frame.
    ReadOn,
    /// A frame other than a cancel came during the run: it is served after
    /// the run, as it was before the child read frames during a run.
    Held(Box<ParentFrame>),
    /// The control thread ended during the run.
    ParentGone,
}

/// One run of an isolated child, with the parent frames read while it runs:
/// a `cancel_run` for its task fires the run's cancel signal with the
/// operator cause; a `cancel_run` for another task is ignored (that run
/// ended); any other frame is held until the run ends, and no later frame
/// is read during the run.
async fn serve_isolated_run(
    context: &HostContext,
    run: RunFrame,
    frames: &mut mpsc::Receiver<ParentFrame>,
) -> AfterIsolatedRun {
    let task_id = run.task_id;
    let scope = RunScope::new(FencingToken::new(run.task_id, run.attempt));
    let ran = run_isolated(context, run, &scope);
    tokio::pin!(ran);
    loop {
        tokio::select! {
            () = &mut ran => return AfterIsolatedRun::ReadOn,
            frame = frames.recv() => match frame {
                Some(ParentFrame::CancelRun(CancelTarget::Isolated { task_id: target }))
                    if target == task_id =>
                {
                    let _fired = scope.fire_cancel(CancelCause::Operator);
                }
                Some(ParentFrame::CancelRun(CancelTarget::Isolated { task_id: other })) => {
                    tracing::debug!(
                        task_id = %other,
                        running = %task_id,
                        "a cancel_run names a task that this child does not run; it is ignored",
                    );
                }
                Some(other) => {
                    ran.as_mut().await;
                    return AfterIsolatedRun::Held(Box::new(other));
                }
                None => {
                    ran.as_mut().await;
                    return AfterIsolatedRun::ParentGone;
                }
            },
        }
    }
}

/// Whether the marker of a run is written before its end is reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Marker {
    /// The run ended without a cancel.
    Write,
    /// The run timed out or needed a cancel: its end goes out at once.
    Skip,
}

/// What the body of a run returned: it ran, so it did not stop before
/// running.
fn body_end_of(end: &RunEnd) -> BodyEnd {
    match end {
        RunEnd::Result(envelope) => BodyEnd::Result(envelope.clone()),
        RunEnd::WorkerFailure(detail) => BodyEnd::WorkerFailure(detail.clone()),
        RunEnd::StoppedBeforeRunning(reason) => {
            unreachable!("a body that ran ended as stopped before running ({reason:?})")
        }
    }
}

async fn send_isolated_result(
    context: &HostContext,
    task_id: uuid::Uuid,
    end: RunEnd,
    started_at: AttemptStart,
) {
    let frame = ChildFrame::Result(ResultFrame {
        task_id,
        end,
        started_at,
        rss: read_rss(),
        task_loop: NoLeftWork::NoTaskLoop,
        child: ChildAfterFrame::ServesOn,
    });
    context.writer.send(frame).await;
}

/// One run of an isolated child and its report (Python `_report_run`), with
/// `scope` as its run: an operator cancel fires the signal of `scope`.
pub(super) async fn run_isolated(context: &HostContext, run: RunFrame, scope: &RunScope) {
    let RunFrame {
        task_id,
        task_name,
        queue_name,
        attempt,
        worker_id,
        claimed_at,
        orphan_self_heal,
        work,
        trace,
        input,
    } = run;
    let facts = RunFacts {
        task_id,
        task_name,
        queue_name,
        attempt,
        worker_id,
        claimed_at,
        orphan_self_heal,
    };
    let (started_at, workflow) = match start_attempt(context, &facts).await {
        Started::Running {
            started_at,
            workflow,
        } => (started_at, workflow),
        Started::Stopped(reason) => {
            let end = RunEnd::StoppedBeforeRunning(reason);
            send_isolated_result(context, task_id, end, AttemptStart::NotReported).await;
            return;
        }
    };
    match scope.cancel_state() {
        SignalState::NotFired => {}
        SignalState::Fired {
            cause: CancelCause::Operator,
        } => {
            // The cancel came while the start write was in flight: the body
            // does not start.
            scope.close();
            let task_loop = CancelledLoop::NoLeftWork(NoLeftWork::NoTaskLoop);
            let body = CancelledBody::NotStarted;
            send_run_cancelled(context, task_id, body, started_at, task_loop).await;
            return;
        }
        SignalState::Fired {
            cause: CancelCause::Deadline,
        } => unreachable!("{NO_DEADLINE_IN_ISOLATED}"),
    }
    let end = match work {
        IsolatedWork::Refusal(refusal) => refusal_end(&refusal),
        IsolatedWork::Run => match prepare(&context.app, &facts.task_name, &input) {
            Prepared::Ended(end) => end,
            Prepared::Body { task, input } => {
                let body = context.app.body_scope.open(
                    &facts,
                    Execution::Isolated,
                    SpanSource {
                        trace: &trace,
                        workflow,
                    },
                );
                let result = isolated_body(scope, &facts.task_name, *task, input, body).await;
                match scope.cancel_state() {
                    SignalState::NotFired => result_end(result),
                    SignalState::Fired {
                        cause: CancelCause::Operator,
                    } => {
                        // What the body returned is dropped: the task is
                        // cancelled.
                        end_cancelled_run(context, task_id, scope, started_at).await;
                        return;
                    }
                    SignalState::Fired {
                        cause: CancelCause::Deadline,
                    } => unreachable!("{NO_DEADLINE_IN_ISOLATED}"),
                }
            }
        },
    };
    match scope.alive() {
        Alive::Nothing => {
            scope.close();
            mark_finalizing(context, &facts).await;
            let started = AttemptStart::Reported { started_at };
            send_isolated_result(context, task_id, end, started).await;
        }
        Alive::Work { count, names } => {
            let left = left_report(count, names);
            end_left_work(context, &facts, scope, &end, left, started_at).await;
        }
    }
}

/// The body of an isolated run, inside the span of its open body scope
/// `body`, which it closes with what the body returned, or as cancelled
/// when an operator cancel fired on `scope` before the body ended. An async
/// body runs inside the run, so the work it starts with `spawn_in_run`
/// belongs to the run. A blocking body runs on a blocking thread of the
/// runtime. A panic of either is `UNHANDLED_EXCEPTION` with its record.
/// Both read the fencing token and the cancel signal of `scope`. The parent
/// ends a run past its timeout by killing the child, so the scope never
/// sees a timeout here.
pub(super) async fn isolated_body(
    scope: &RunScope,
    task_name: &str,
    task: RegisteredTask,
    input: Vec<u8>,
    body: OpenBody,
) -> RawTaskResult {
    let span = body.span();
    let result = match task {
        RegisteredTask::Async { task, meta } => joined_result(
            task_name,
            meta.module,
            scope
                .spawn_body(task_name, async_body_in(task, input, span))
                .await,
        ),
        RegisteredTask::Blocking { task, meta } => {
            let name = task_name.to_owned();
            let token = scope.fencing_token();
            let cancel = scope.cancel_signal();
            let thread = tokio::task::spawn_blocking(move || {
                run_blocking_body(&task, &input, &name, meta.module, &span, token, cancel)
            });
            match thread.await {
                Ok(result) => result,
                Err(error) => TaskResult::Err(TaskError::builtin(
                    OperationalErrorCode::UnhandledException,
                    format!(
                        "the thread of blocking task '{task_name}' ended with no result: {error}"
                    ),
                )),
            }
        }
    };
    match scope.cancel_state() {
        SignalState::NotFired => body.close(&BodyOutcome::of_result(&result)),
        SignalState::Fired {
            cause: CancelCause::Operator,
        } => body.close(&BodyOutcome::Cancelled),
        SignalState::Fired {
            cause: CancelCause::Deadline,
        } => unreachable!("{NO_DEADLINE_IN_ISOLATED}"),
    }
    result
}

/// `run_cancelled` for the isolated run of task `task_id`, whose start write
/// returned at `started_at`. No marker is written: the row is in history.
async fn send_run_cancelled(
    context: &HostContext,
    task_id: Uuid,
    body: CancelledBody,
    started_at: DateTime<Utc>,
    task_loop: CancelledLoop,
) {
    let frame = ChildFrame::RunCancelled(RunCancelledFrame {
        task_id,
        body,
        started_at,
        rss: read_rss(),
        task_loop,
        child: ChildAfterFrame::ServesOn,
    });
    context.writer.send(frame).await;
}

/// The body of an isolated run ended after an operator cancel: the work it
/// left is aborted at once, in rounds, and `run_cancelled` reports the run.
/// Work that holds its thread past the rounds is reported `held`: the
/// parent replaces the child.
async fn end_cancelled_run(
    context: &HostContext,
    task_id: Uuid,
    scope: &RunScope,
    started_at: DateTime<Utc>,
) {
    scope.close();
    let no_left_work = CancelledLoop::NoLeftWork(NoLeftWork::NoTaskLoop);
    let task_loop = match scope.alive() {
        Alive::Nothing => no_left_work,
        Alive::Work { .. } => match (scope.end_in_rounds().await, scope.alive()) {
            (RoundsEnd::Ended, _) | (RoundsEnd::Stuck, Alive::Nothing) => no_left_work,
            (RoundsEnd::Stuck, Alive::Work { count, names }) => {
                CancelledLoop::WorkLeft(left_report(count, names))
            }
        },
    };
    send_run_cancelled(context, task_id, CancelledBody::Ran, started_at, task_loop).await;
}

/// The body returned with work alive (Python `_report_run`,
/// `TaskLoop.end_left_work`): `body_returned` with what it returned and the
/// start of the attempt, then up to 1 s for the work, then the cancel
/// rounds. Work that ended gives the marker, then `left_work_ended`; work
/// that did not stop gives `left_work_stuck` and no marker: the parent kills
/// the child.
async fn end_left_work(
    context: &HostContext,
    facts: &RunFacts,
    scope: &RunScope,
    end: &RunEnd,
    left: crate::worker::executor::protocol::LoopHoldsTasks,
    started_at: DateTime<Utc>,
) {
    let task_id = facts.task_id;
    let returned = body_end_of(end);
    context
        .writer
        .send(ChildFrame::BodyReturned(BodyReturnedFrame {
            task_id,
            returned,
            started_at,
            left,
        }))
        .await;
    let ended = match scope.wait_done(ISOLATED_LEFTOVER_GRACE).await {
        WorkWait::Done => LeftWorkEnd::InGrace,
        WorkWait::StillAlive => {
            scope.close();
            match (scope.end_in_rounds().await, scope.alive()) {
                (RoundsEnd::Ended, _) | (RoundsEnd::Stuck, Alive::Nothing) => {
                    LeftWorkEnd::Cancelled
                }
                (RoundsEnd::Stuck, Alive::Work { count, names }) => {
                    let still = left_report(count, names);
                    context
                        .writer
                        .send(ChildFrame::LeftWorkStuck(LeftWorkStuckFrame {
                            task_id,
                            still,
                        }))
                        .await;
                    return;
                }
            }
        }
    };
    scope.close();
    mark_finalizing(context, facts).await;
    let frame = ChildFrame::LeftWorkEnded(LeftWorkEndedFrame {
        task_id,
        end: ended,
        rss: read_rss(),
    });
    context.writer.send(frame).await;
}

fn facts_of(run: &SharedRunFrame) -> RunFacts {
    RunFacts {
        task_id: run.task_id,
        task_name: run.task_name.clone(),
        queue_name: run.queue_name.clone(),
        attempt: run.attempt,
        worker_id: run.worker_id.clone(),
        claimed_at: run.claimed_at,
        orphan_self_heal: run.orphan_self_heal,
    }
}

fn run_ref_of(run: &SharedRunFrame) -> RunRef {
    RunRef {
        dispatch: run.dispatch,
        task_id: run.task_id,
    }
}

/// Count a run in flight; `false` when the parent sent more than `seats`.
fn take_seat(in_flight: &AtomicUsize, seats: usize) -> bool {
    in_flight
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
            (held < seats).then_some(held + 1)
        })
        .is_ok()
}

/// The stop frame is valid only with no run in flight. A seat frees its run
/// before the end of the run is queued ([`end_seat_run`]), so a stop that the
/// parent sends after it read the last end finds no run in flight.
fn stop_end(in_flight: &AtomicUsize) -> ServeEnd {
    match in_flight.load(Ordering::Acquire) {
        0 => ServeEnd::Stopped,
        _ => ServeEnd::BadParentFrame,
    }
}

/// Whether a shared child runs a run now, and what a cancel of it needs.
enum RunLookup<C> {
    InFlight(C),
    NotInFlight,
}

/// One run in flight on a seat: its task, and what a cancel of it needs.
struct InFlightRun<C> {
    task_id: Uuid,
    cancel: C,
}

/// The runs in flight of a shared child, by dispatch, with what a
/// `cancel_run` for each needs. A seat enters its run before the start
/// write and leaves it before it sends the end of the run, so a cancel
/// that finds no run is a cancel of a run that ended. At most `seats` runs.
pub(super) struct InFlightRuns<C> {
    runs: Arc<Mutex<HashMap<DispatchNumber, InFlightRun<C>>>>,
}

impl<C> Clone for InFlightRuns<C> {
    fn clone(&self) -> Self {
        Self {
            runs: Arc::clone(&self.runs),
        }
    }
}

impl<C: Clone> InFlightRuns<C> {
    pub(super) fn new() -> Self {
        Self {
            runs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<DispatchNumber, InFlightRun<C>>> {
        // A poisoned lock means a panic while it was held; the map is valid.
        self.runs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn enter(&self, run: RunRef, cancel: C) {
        let _replaced = self.lock().insert(
            run.dispatch,
            InFlightRun {
                task_id: run.task_id,
                cancel,
            },
        );
    }

    fn leave(&self, dispatch: DispatchNumber) {
        let _removed = self.lock().remove(&dispatch);
    }

    /// What a cancel of `run` needs, when the child runs it now: the
    /// dispatch is in flight with the same task.
    fn find(&self, run: RunRef) -> RunLookup<C> {
        match self.lock().get(&run.dispatch) {
            Some(in_flight) if in_flight.task_id == run.task_id => {
                RunLookup::InFlight(in_flight.cancel.clone())
            }
            Some(_) | None => RunLookup::NotInFlight,
        }
    }
}

/// Log a cancel of a run that the child does not run.
fn log_ignored_cancel(run: RunRef) {
    tracing::debug!(
        dispatch = run.dispatch.get(),
        task_id = %run.task_id,
        "a cancel_run names a run that this child does not run; the run ended first, and the \
         cancel is ignored",
    );
}

/// Free the seat of the run `dispatch`: the run leaves the runs that a
/// cancel can reach, and the count of runs in flight drops.
fn free_seat<C: Clone>(
    in_flight: &AtomicUsize,
    cancels: &InFlightRuns<C>,
    dispatch: DispatchNumber,
) {
    cancels.leave(dispatch);
    let _held_before = in_flight.fetch_sub(1, Ordering::AcqRel);
}

/// End a run on its seat: free the seat ([`free_seat`]), then queue `end`,
/// the end of the run. The parent frees the seat when it reads the end, and
/// may then send `stop` or a new run at once. The seat is free before the
/// end can be read, so neither frame finds the seat still taken (Python
/// `_send_end`).
pub(super) async fn end_seat_run<C: Clone>(
    writer: &FrameWriter,
    in_flight: &AtomicUsize,
    cancels: &InFlightRuns<C>,
    end: SharedResultFrame,
) {
    free_seat(in_flight, cancels, end.run.dispatch);
    writer.send(ChildFrame::SharedResult(end)).await;
}

/// [`end_seat_run`] on a seat thread, which is not a runtime thread.
pub(super) fn end_seat_run_blocking<C: Clone>(
    writer: &FrameWriter,
    in_flight: &AtomicUsize,
    cancels: &InFlightRuns<C>,
    end: SharedResultFrame,
) {
    free_seat(in_flight, cancels, end.run.dispatch);
    writer.send_blocking(ChildFrame::SharedResult(end));
}

// ---------------------------------------------------------------------------
// Shared async
// ---------------------------------------------------------------------------

/// The work of a seat of a shared async child.
enum AsyncSeatWork {
    Task {
        timeout: TaskTimeoutMs,
        grace: CancelGraceMs,
    },
    Refusal(StoredOptionsRefusal),
}

pub(super) async fn serve_shared_async(
    context: HostContext,
    watch: PollWatch,
    frames: &mut mpsc::Receiver<ParentFrame>,
    seats: usize,
) -> ServeEnd {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let cancels: InFlightRuns<RunCancelSignal> = InFlightRuns::new();
    loop {
        let run = match frames.recv().await {
            None => return ServeEnd::ParentGone,
            Some(ParentFrame::Stop) => return stop_end(&in_flight),
            Some(ParentFrame::SharedRun(run)) => run,
            Some(ParentFrame::CancelRun(CancelTarget::Shared(target))) => {
                match cancels.find(target) {
                    RunLookup::InFlight(signal) => {
                        // A deadline that fired first keeps its cause.
                        let _fired = signal.fire(CancelCause::Operator);
                    }
                    RunLookup::NotInFlight => log_ignored_cancel(target),
                }
                continue;
            }
            Some(
                ParentFrame::CancelRun(CancelTarget::Isolated { .. })
                | ParentFrame::Init(_)
                | ParentFrame::Run(_)
                | ParentFrame::Events(_),
            ) => return ServeEnd::BadParentFrame,
        };
        let work = match run.work.clone() {
            SharedWork::AsyncTask {
                timeout_ms,
                cancel_grace_ms,
            } => AsyncSeatWork::Task {
                timeout: timeout_ms,
                grace: cancel_grace_ms,
            },
            SharedWork::Refusal(refusal) => AsyncSeatWork::Refusal(refusal),
            SharedWork::ThreadTask { .. } => return ServeEnd::BadParentFrame,
        };
        if !take_seat(&in_flight, seats) {
            return ServeEnd::BadParentFrame;
        }
        // The signal exists before the start write, so a cancel that comes
        // before the body starts reaches the run.
        let signal = RunCancelSignal::new();
        cancels.enter(run_ref_of(&run), signal.clone());
        let seat = AsyncSeat {
            context: context.clone(),
            watch: watch.clone(),
            in_flight: Arc::clone(&in_flight),
            cancels: cancels.clone(),
            signal,
        };
        tokio::spawn(async_seat(seat, run, work));
    }
}

/// What one seat of a shared async child shares with its serve loop: the
/// child's context and poll watch, the count of runs in flight, the runs
/// that a cancel can reach, and the cancel signal of this run.
struct AsyncSeat {
    context: HostContext,
    watch: PollWatch,
    in_flight: Arc<AtomicUsize>,
    cancels: InFlightRuns<RunCancelSignal>,
    signal: RunCancelSignal,
}

async fn async_seat(seat: AsyncSeat, run: SharedRunFrame, work: AsyncSeatWork) {
    let AsyncSeat {
        context,
        watch,
        in_flight,
        cancels,
        signal,
    } = seat;
    let run_ref = run_ref_of(&run);
    let facts = facts_of(&run);
    let (end, started) = match start_attempt(&context, &facts).await {
        Started::Stopped(reason) => (
            SeatEnd::Finished(RunEnd::StoppedBeforeRunning(reason)),
            AttemptStart::NotReported,
        ),
        Started::Running {
            started_at,
            workflow,
        } => {
            let (end, marker) = match (signal.state(), work) {
                (
                    SignalState::Fired {
                        cause: CancelCause::Operator,
                    },
                    _,
                ) => cancelled_before_body(&context, run_ref).await,
                (
                    SignalState::Fired {
                        cause: CancelCause::Deadline,
                    },
                    _,
                ) => unreachable!("{NO_DEADLINE_BEFORE_BODY}"),
                (SignalState::NotFired, AsyncSeatWork::Refusal(refusal)) => {
                    (SeatEnd::Finished(refusal_end(&refusal)), Marker::Write)
                }
                (SignalState::NotFired, AsyncSeatWork::Task { timeout, grace }) => {
                    let body = SeatBody {
                        run_ref,
                        facts: &facts,
                        input: &run.input,
                        span: SpanSource {
                            trace: &run.trace,
                            workflow,
                        },
                        started_at,
                    };
                    let limits = SeatLimits { timeout, grace };
                    async_body(&context, &watch, body, limits, signal).await
                }
            };
            match marker {
                Marker::Write => mark_finalizing(&context, &facts).await,
                Marker::Skip => {}
            }
            (end, AttemptStart::Reported { started_at })
        }
    };
    let frame = SharedResultFrame {
        run: run_ref,
        end,
        started_at: started,
        rss: read_rss(),
        child: ChildAfterFrame::ServesOn,
    };
    end_seat_run(&context.writer, &in_flight, &cancels, frame).await;
}

/// What the async body of task `task_name` (defined in `module`) ended
/// with. Its panic is caught in its poll (`CatchPanic`) and is
/// `UNHANDLED_EXCEPTION` with its record. A join error means the body ended
/// with no result (the runtime shut down): `UNHANDLED_EXCEPTION`.
fn joined_result(
    task_name: &str,
    module: TaskModule,
    joined: Result<Result<RawTaskResult, PanicRecord>, JoinError>,
) -> RawTaskResult {
    match joined {
        Ok(Ok(result)) => result,
        Ok(Err(record)) => TaskResult::Err(super::panic::panic_error(task_name, module, record)),
        Err(error) => TaskResult::Err(TaskError::builtin(
            OperationalErrorCode::UnhandledException,
            format!("the async body of task '{task_name}' ended with no result: {error}"),
        )),
    }
}

/// The facts of one run on a seat after its start: its reference, its
/// facts, its input, its stored trace with the workflow node that its start
/// read, and the time that its start write returned.
struct SeatBody<'a> {
    run_ref: RunRef,
    facts: &'a RunFacts,
    input: &'a TaskInputText,
    span: SpanSource<'a>,
    started_at: DateTime<Utc>,
}

/// The deadline and the cancel grace of a shared async run.
#[derive(Debug, Clone, Copy)]
struct SeatLimits {
    timeout: TaskTimeoutMs,
    grace: CancelGraceMs,
}

/// A cancel came after the start write and before the body started: the
/// body does not start. `cancel_fired`, then the end.
async fn cancelled_before_body(context: &HostContext, run_ref: RunRef) -> (SeatEnd, Marker) {
    context.writer.send(ChildFrame::CancelFired(run_ref)).await;
    (SeatEnd::Cancelled(CancelledBody::NotStarted), Marker::Skip)
}

/// The body of a shared async run, inside the span of its body scope. The
/// scope is closed with what the body returned, with `TimedOut` at the
/// deadline, or with `Cancelled` at an operator cancel (the signal `signal`
/// of the run fired with the operator cause): the body may run on in the
/// cancel grace, and the span ends when the body ends or is dropped.
async fn async_body(
    context: &HostContext,
    watch: &PollWatch,
    body: SeatBody<'_>,
    limits: SeatLimits,
    signal: RunCancelSignal,
) -> (SeatEnd, Marker) {
    let SeatLimits { timeout, grace } = limits;
    let SeatBody {
        run_ref,
        facts,
        input,
        span,
        started_at,
    } = body;
    let (task, module, input): (Arc<dyn AsyncTaskFn>, TaskModule, Vec<u8>) =
        match prepare(&context.app, &facts.task_name, input) {
            Prepared::Ended(end) => return (SeatEnd::Finished(end), Marker::Write),
            Prepared::Body { task, input } => match *task {
                RegisteredTask::Async { task, meta } => (task, meta.module, input),
                RegisteredTask::Blocking { .. } => {
                    return (
                        SeatEnd::Finished(kind_mismatch_end(&facts.task_name, "shared async")),
                        Marker::Write,
                    )
                }
            },
        };
    let scope = RunScope::timed(watch.register(run_ref), facts.fencing_token(), signal);
    let open = context
        .app
        .body_scope
        .open(facts, Execution::SharedAsync, span);
    let body = async_body_in(task, input, open.span());
    let mut handle = scope.spawn_body(&facts.task_name, body);
    let deadline = tokio::time::sleep(Duration::from_millis(timeout.get()));
    tokio::pin!(deadline);
    let cancel = scope.cancel_signal();
    let outcome = tokio::select! {
        joined = &mut handle => {
            let result = joined_result(&facts.task_name, module, joined);
            match scope.cancel_state() {
                SignalState::NotFired => {
                    open.close(&BodyOutcome::of_result(&result));
                    let end = result_end(result);
                    after_return(context, run_ref, &scope, end, grace, started_at).await
                }
                SignalState::Fired { cause: CancelCause::Operator } => {
                    // The body returned after the operator cancel: what it
                    // returned is dropped.
                    open.close(&BodyOutcome::Cancelled);
                    stop_after_cancel(context, run_ref, &scope, grace).await
                }
                SignalState::Fired { cause: CancelCause::Deadline } => {
                    unreachable!("the seat fires the deadline only after the deadline won")
                }
            }
        }
        () = &mut deadline => match scope.fire_cancel(CancelCause::Deadline) {
            Fire::Won => {
                open.close(&BodyOutcome::TimedOut);
                // The body runs on: it sees the signal and has the grace to
                // end.
                context.writer.send(ChildFrame::DeadlineFired(run_ref)).await;
                stop_after_signal(context, run_ref, &scope, grace).await;
                (SeatEnd::TimedOut, Marker::Skip)
            }
            Fire::AlreadyFired => {
                // The operator cancel fired first: the deadline adds nothing.
                open.close(&BodyOutcome::Cancelled);
                stop_after_cancel(context, run_ref, &scope, grace).await
            }
        },
        () = cancel.cancelled() => {
            // Before the deadline branch fires it, only an operator cancel
            // fires the signal.
            open.close(&BodyOutcome::Cancelled);
            stop_after_cancel(context, run_ref, &scope, grace).await
        }
    };
    scope.close();
    watch.unregister(run_ref.dispatch);
    outcome
}

/// The body returned before its deadline: the deadline no longer applies.
/// Work it left gets the cancel grace, then `leftover_cancel` and the
/// cancel rounds, then `leftover_stuck`, each with the start of the attempt;
/// the run keeps its seat until that work ends or the parent kills the
/// child. The end is what the body returned.
async fn after_return(
    context: &HostContext,
    run_ref: RunRef,
    scope: &RunScope,
    end: RunEnd,
    grace: CancelGraceMs,
    started_at: DateTime<Utc>,
) -> (SeatEnd, Marker) {
    if let Alive::Nothing = scope.alive() {
        return (SeatEnd::Finished(end), Marker::Write);
    }
    let grace = Duration::from_millis(grace.get());
    if scope.wait_done(grace).await == WorkWait::Done {
        return (SeatEnd::Finished(end), Marker::Write);
    }
    let returned = body_end_of(&end);
    context
        .writer
        .send(ChildFrame::LeftoverCancel(LeftoverCancelFrame {
            run: run_ref,
            returned: returned.clone(),
            started_at,
        }))
        .await;
    scope.close();
    if scope.end_in_rounds().await == RoundsEnd::Ended {
        return (SeatEnd::Finished(end), Marker::Skip);
    }
    if let Alive::Work { count, names } = scope.alive() {
        context
            .writer
            .send(ChildFrame::LeftoverStuck(LeftoverStuckFrame {
                run: run_ref,
                work: stuck_report(count, names),
                returned,
                started_at,
            }))
            .await;
        scope.wait_all_ended().await;
    }
    (SeatEnd::Finished(end), Marker::Skip)
}

/// After an operator cancel reached the body of a shared async run:
/// `cancel_fired`, then the stop of [`stop_after_signal`]. The end is
/// cancelled whatever the body returns.
async fn stop_after_cancel(
    context: &HostContext,
    run_ref: RunRef,
    scope: &RunScope,
    grace: CancelGraceMs,
) -> (SeatEnd, Marker) {
    context.writer.send(ChildFrame::CancelFired(run_ref)).await;
    stop_after_signal(context, run_ref, scope, grace).await;
    (SeatEnd::Cancelled(CancelledBody::Ran), Marker::Skip)
}

/// After the run's cancel signal fired, at its deadline or at an operator
/// cancel: when anything of the run (the body or its work) is alive after
/// the cancel grace, report the second cancel and drop it all in rounds;
/// then report what is stuck, and wait for the end. A run that ended within
/// the grace sends no second cancel, so the parent records that the body
/// returned after the signal.
async fn stop_after_signal(
    context: &HostContext,
    run_ref: RunRef,
    scope: &RunScope,
    grace: CancelGraceMs,
) {
    let grace = Duration::from_millis(grace.get());
    if scope.wait_done(grace).await == WorkWait::Done {
        return;
    }
    context.writer.send(ChildFrame::SecondCancel(run_ref)).await;
    scope.close();
    if scope.end_in_rounds().await == RoundsEnd::Ended {
        return;
    }
    if let Alive::Work { count, names } = scope.alive() {
        context
            .writer
            .send(ChildFrame::RunStuck(RunStuckFrame {
                run: run_ref,
                work: stuck_report(count, names),
            }))
            .await;
        // The run keeps its seat until all of it ends or the parent kills
        // the child; the end is the timeout or the cancel either way.
        scope.wait_all_ended().await;
    }
}

/// Whether a run's blocked poll was reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockReport {
    NotReported,
    Reported,
}

struct WatchedRun {
    run: RunRef,
    /// The poll starts of the run's body and of each piece of its work.
    polls: RunPolls,
    report: BlockReport,
}

/// The poll watch of a shared async child: a thread of its own, so a body
/// that holds every runtime thread cannot keep it from reporting. The same
/// thread reads the host loop when the app gives one; in a shared thread
/// child it reads only that loop, and no run registers polls on it.
#[derive(Clone)]
pub(super) struct PollWatch {
    runs: Arc<Mutex<HashMap<DispatchNumber, WatchedRun>>>,
    epoch: Instant,
}

fn lock_runs(
    runs: &Mutex<HashMap<DispatchNumber, WatchedRun>>,
) -> MutexGuard<'_, HashMap<DispatchNumber, WatchedRun>> {
    // A poisoned lock means a panic while it was held; the map is valid.
    runs.lock().unwrap_or_else(PoisonError::into_inner)
}

impl PollWatch {
    /// A watch with no thread, for tests of the report rule.
    #[cfg(test)]
    pub(super) fn without_thread(epoch: Instant) -> Self {
        Self {
            runs: Arc::new(Mutex::new(HashMap::new())),
            epoch,
        }
    }

    /// Start the watch thread. Each pass reads every run once, then the
    /// host loop of `host` once.
    pub(super) fn start(
        writer: super::link::FrameWriter,
        host: HostLoopWatched,
    ) -> io::Result<Self> {
        let watch = Self {
            runs: Arc::new(Mutex::new(HashMap::new())),
            epoch: Instant::now(),
        };
        let thread_watch = watch.clone();
        let mut host_watch = host.start();
        thread::Builder::new()
            .name(POLL_WATCH_THREAD_NAME.to_owned())
            .spawn(move || loop {
                thread::sleep(POLL_WATCH_INTERVAL);
                for frame in thread_watch.blocked_reports() {
                    writer.send_blocking(frame);
                }
                match host_watch.pass() {
                    HostPass::NoReport => {}
                    HostPass::Blocked {
                        blocked_ms,
                        culprit,
                    } => writer.send_blocking(thread_watch.host_loop_frame(blocked_ms, culprit)),
                }
            })?;
        Ok(watch)
    }

    /// The `loop_blocked` frame of a host loop that has not turned for
    /// `blocked_ms`. The culprit is the run of the held task while that run
    /// is in flight on a seat; else no run is named.
    pub(super) fn host_loop_frame(&self, blocked_ms: u64, culprit: HostCulprit) -> ChildFrame {
        let culprit = match culprit {
            HostCulprit::Unknown => LoopCulprit::Unknown,
            HostCulprit::Run(task_id) => lock_runs(&self.runs)
                .values()
                .find(|watched| watched.run.task_id == task_id)
                .map_or(LoopCulprit::Unknown, |watched| {
                    LoopCulprit::Run(watched.run)
                }),
        };
        ChildFrame::LoopBlocked(LoopBlockedFrame {
            culprit,
            blocked_ms,
            location: BlockedWhere::cut(HOST_LOOP_LOCATION),
        })
    }

    /// Watch the polls of `run`: the run's scope times its body and each
    /// piece of its work in the returned polls.
    pub(super) fn register(&self, run: RunRef) -> RunPolls {
        let polls = RunPolls::new(self.epoch);
        lock_runs(&self.runs).insert(
            run.dispatch,
            WatchedRun {
                run,
                polls: polls.clone(),
                report: BlockReport::NotReported,
            },
        );
        polls
    }

    fn unregister(&self, dispatch: DispatchNumber) {
        let _removed = lock_runs(&self.runs).remove(&dispatch);
    }

    /// The `loop_blocked` frames of runs whose body or work has a poll in
    /// progress past the limit, each run reported once. The location is the
    /// task name for the body, the work's name for work.
    pub(super) fn blocked_reports(&self) -> Vec<ChildFrame> {
        let now = millis_since(self.epoch).saturating_add(1);
        let limit = u64::try_from(LOOP_BLOCK_LIMIT.as_millis()).unwrap_or(u64::MAX);
        let mut runs = lock_runs(&self.runs);
        let mut frames = Vec::new();
        for watched in runs.values_mut() {
            let (blocked_ms, location) = match watched.polls.longest_poll(now) {
                PollsNow::NoPollInProgress => continue,
                PollsNow::Longest {
                    blocked_ms,
                    location,
                } => (blocked_ms, location),
            };
            if blocked_ms >= limit && watched.report == BlockReport::NotReported {
                watched.report = BlockReport::Reported;
                frames.push(ChildFrame::LoopBlocked(LoopBlockedFrame {
                    culprit: LoopCulprit::Run(watched.run),
                    blocked_ms,
                    location,
                }));
            }
        }
        frames
    }
}

// ---------------------------------------------------------------------------
// Shared thread
// ---------------------------------------------------------------------------

/// The work of a seat of a shared thread child.
enum ThreadSeatWork {
    Task { timeout: TaskTimeoutMs },
    Refusal(StoredOptionsRefusal),
}

/// Whether the serve loop queued the `cancel_fired` frame of a thread run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelReport {
    NotQueued,
    Queued,
}

/// What a cancel of one thread run needs: its gate, its cancel signal (the
/// body reads it on its thread), and the mark that the serve loop queued
/// `cancel_fired`.
#[derive(Clone)]
struct ThreadSeatCancel {
    gate: Arc<RunGate>,
    signal: RunCancelSignal,
    report: Arc<watch::Sender<CancelReport>>,
}

impl ThreadSeatCancel {
    fn new() -> Self {
        let (report, _first_receiver) = watch::channel(CancelReport::NotQueued);
        Self {
            gate: Arc::new(RunGate::new()),
            signal: RunCancelSignal::new(),
            report: Arc::new(report),
        }
    }
}

pub(super) async fn serve_shared_thread(
    context: HostContext,
    frames: &mut mpsc::Receiver<ParentFrame>,
    seats: usize,
) -> ServeEnd {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let cancels: InFlightRuns<ThreadSeatCancel> = InFlightRuns::new();
    let runtime = tokio::runtime::Handle::current();
    loop {
        let run = match frames.recv().await {
            None => return ServeEnd::ParentGone,
            Some(ParentFrame::Stop) => return stop_end(&in_flight),
            Some(ParentFrame::SharedRun(run)) => run,
            Some(ParentFrame::CancelRun(CancelTarget::Shared(target))) => {
                cancel_thread_run(&context, &cancels, target).await;
                continue;
            }
            Some(
                ParentFrame::CancelRun(CancelTarget::Isolated { .. })
                | ParentFrame::Init(_)
                | ParentFrame::Run(_)
                | ParentFrame::Events(_),
            ) => return ServeEnd::BadParentFrame,
        };
        let work = match run.work.clone() {
            SharedWork::ThreadTask { timeout_ms } => ThreadSeatWork::Task {
                timeout: timeout_ms,
            },
            SharedWork::Refusal(refusal) => ThreadSeatWork::Refusal(refusal),
            SharedWork::AsyncTask { .. } => return ServeEnd::BadParentFrame,
        };
        if !take_seat(&in_flight, seats) {
            return ServeEnd::BadParentFrame;
        }
        let run_ref = run_ref_of(&run);
        let seat_cancel = ThreadSeatCancel::new();
        cancels.enter(run_ref, seat_cancel.clone());
        let seat = ThreadSeat {
            context: context.clone(),
            runtime: runtime.clone(),
            in_flight: Arc::clone(&in_flight),
            cancels: cancels.clone(),
            cancel: seat_cancel,
        };
        let started = thread::Builder::new()
            .name(SEAT_THREAD_NAME.to_owned())
            .spawn(move || thread_seat(&seat, &run, work));
        if let Err(error) = started {
            // No start was written: the row is still CLAIMED.
            let frame = SharedResultFrame {
                run: run_ref,
                end: SeatEnd::Finished(RunEnd::WorkerFailure(FailureDetail::cut(&format!(
                    "the OS refused a seat thread: {error}"
                )))),
                started_at: AttemptStart::NotReported,
                rss: read_rss(),
                child: ChildAfterFrame::ServesOn,
            };
            end_seat_run(&context.writer, &in_flight, &cancels, frame).await;
        }
    }
}

/// An operator cancel of the thread run `target`. A body that runs gets the
/// cancel in its cancel state, and `cancel_fired` is queued at once; the
/// thread runs on. A cancel before the body starts is kept for the seat,
/// which sends `cancel_fired` itself. A run that ended, or whose deadline
/// fired first, ignores the cancel.
async fn cancel_thread_run(
    context: &HostContext,
    cancels: &InFlightRuns<ThreadSeatCancel>,
    target: RunRef,
) {
    let RunLookup::InFlight(seat) = cancels.find(target) else {
        log_ignored_cancel(target);
        return;
    };
    match seat.gate.cancel() {
        GateCancel::BodyRunning => {
            let _fired = seat.signal.fire(CancelCause::Operator);
            context.writer.send(ChildFrame::CancelFired(target)).await;
            let _earlier = seat.report.send_replace(CancelReport::Queued);
        }
        GateCancel::BeforeBody => {
            let _fired = seat.signal.fire(CancelCause::Operator);
        }
        GateCancel::RunEnded => log_ignored_cancel(target),
    }
}

/// What one seat thread of a shared thread child shares with its serve
/// loop: the child's context and runtime, the count of runs in flight, the
/// runs that a cancel can reach, and the cancel of this run.
struct ThreadSeat {
    context: HostContext,
    runtime: tokio::runtime::Handle,
    in_flight: Arc<AtomicUsize>,
    cancels: InFlightRuns<ThreadSeatCancel>,
    cancel: ThreadSeatCancel,
}

fn thread_seat(seat: &ThreadSeat, run: &SharedRunFrame, work: ThreadSeatWork) {
    let ThreadSeat {
        context, runtime, ..
    } = seat;
    let run_ref = run_ref_of(run);
    let facts = facts_of(run);
    let (end, started) = match runtime.block_on(start_attempt(context, &facts)) {
        Started::Stopped(reason) => (
            SeatEnd::Finished(RunEnd::StoppedBeforeRunning(reason)),
            AttemptStart::NotReported,
        ),
        Started::Running {
            started_at,
            workflow,
        } => {
            let span = SpanSource {
                trace: &run.trace,
                workflow,
            };
            let (end, marker) = match work {
                ThreadSeatWork::Refusal(refusal) => {
                    end_without_body(context, run_ref, &seat.cancel.gate, refusal_end(&refusal))
                }
                ThreadSeatWork::Task { timeout } => {
                    let body = ThreadBody {
                        run_ref,
                        facts: &facts,
                        input: &run.input,
                        span,
                        timeout,
                    };
                    thread_body(seat, &body)
                }
            };
            match marker {
                Marker::Write => runtime.block_on(mark_finalizing(context, &facts)),
                Marker::Skip => {}
            }
            (end, AttemptStart::Reported { started_at })
        }
    };
    let frame = SharedResultFrame {
        run: run_ref,
        end,
        started_at: started,
        rss: read_rss(),
        child: ChildAfterFrame::ServesOn,
    };
    end_seat_run_blocking(&context.writer, &seat.in_flight, &seat.cancels, frame);
}

/// The body has not started.
const GATE_STARTING: u8 = 0;
/// The body runs.
const GATE_RUNNING: u8 = 1;
/// The body returned first.
const GATE_RETURNED: u8 = 2;
/// The deadline fired first.
const GATE_DEADLINE_FIRED: u8 = 3;
/// An operator cancel came before the body started.
const GATE_CANCELLED_BEFORE_BODY: u8 = 4;
/// An operator cancel came first while the body ran.
const GATE_CANCEL_FIRED: u8 = 5;

/// Which came first for a thread run whose body ran: its return, its
/// deadline, or an operator cancel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GateEnd {
    Returned,
    DeadlineFired,
    CancelFired,
}

/// Whether the body of a thread run starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BodyStart {
    Start,
    /// An operator cancel came first: the body does not start.
    CancelledBeforeBody,
}

/// Whether the deadline of a thread run won its gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeadlineFire {
    Won,
    /// The body returned, or an operator cancel came, first.
    Lost,
}

/// What an operator cancel found at the gate of a thread run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GateCancel {
    /// The body runs: the cancel won; the body sees it in its cancel state.
    BodyRunning,
    /// The body has not started: it will not start.
    BeforeBody,
    /// The body returned, its deadline fired, or the run ended with no body:
    /// the cancel changes nothing.
    RunEnded,
}

/// One gate per thread run. The body starts once, unless an operator cancel
/// came first; then exactly one of the return, the deadline and an operator
/// cancel wins.
pub(super) struct RunGate(AtomicU8);

impl RunGate {
    pub(super) fn new() -> Self {
        Self(AtomicU8::new(GATE_STARTING))
    }

    /// The body is about to start: `Start`, unless an operator cancel came
    /// first.
    pub(super) fn begin_body(&self) -> BodyStart {
        match self.0.compare_exchange(
            GATE_STARTING,
            GATE_RUNNING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => BodyStart::Start,
            Err(GATE_CANCELLED_BEFORE_BODY) => BodyStart::CancelledBeforeBody,
            Err(other) => unreachable!("a thread run begins its body once, not in state {other}"),
        }
    }

    /// The run ends with no body (a refusal, a task that is not found, a
    /// kind that does not fit): `Start` closes the gate to a later cancel,
    /// unless an operator cancel came first.
    pub(super) fn end_without_body(&self) -> BodyStart {
        match self.0.compare_exchange(
            GATE_STARTING,
            GATE_RETURNED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => BodyStart::Start,
            Err(GATE_CANCELLED_BEFORE_BODY) => BodyStart::CancelledBeforeBody,
            Err(other) => unreachable!("a thread run with no body ends once, not in state {other}"),
        }
    }

    /// The deadline: `Won` when the body still ran and no cancel came.
    pub(super) fn fire_deadline(&self) -> DeadlineFire {
        match self.0.compare_exchange(
            GATE_RUNNING,
            GATE_DEADLINE_FIRED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => DeadlineFire::Won,
            Err(_) => DeadlineFire::Lost,
        }
    }

    /// An operator cancel.
    pub(super) fn cancel(&self) -> GateCancel {
        let changed =
            self.0
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| match state {
                    GATE_STARTING => Some(GATE_CANCELLED_BEFORE_BODY),
                    GATE_RUNNING => Some(GATE_CANCEL_FIRED),
                    _ => None,
                });
        match changed {
            Ok(GATE_STARTING) => GateCancel::BeforeBody,
            Ok(GATE_RUNNING) => GateCancel::BodyRunning,
            Ok(other) => unreachable!("a cancel changed the gate from state {other}"),
            Err(_) => GateCancel::RunEnded,
        }
    }

    /// The return of the body: which one won.
    pub(super) fn finish(&self) -> GateEnd {
        match self.0.compare_exchange(
            GATE_RUNNING,
            GATE_RETURNED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => GateEnd::Returned,
            Err(GATE_DEADLINE_FIRED) => GateEnd::DeadlineFired,
            Err(GATE_CANCEL_FIRED) => GateEnd::CancelFired,
            Err(other) => unreachable!("the body of a thread run returned in state {other}"),
        }
    }
}

/// A cancel came after the start write and before the body started, on a
/// seat thread: `cancel_fired`, then the end.
fn cancelled_before_body_blocking(context: &HostContext, run_ref: RunRef) -> (SeatEnd, Marker) {
    context
        .writer
        .send_blocking(ChildFrame::CancelFired(run_ref));
    (SeatEnd::Cancelled(CancelledBody::NotStarted), Marker::Skip)
}

/// A thread run that ends with `end` and no body, unless an operator cancel
/// came first.
fn end_without_body(
    context: &HostContext,
    run_ref: RunRef,
    gate: &RunGate,
    end: RunEnd,
) -> (SeatEnd, Marker) {
    match gate.end_without_body() {
        BodyStart::Start => (SeatEnd::Finished(end), Marker::Write),
        BodyStart::CancelledBeforeBody => cancelled_before_body_blocking(context, run_ref),
    }
}

/// Wait until the serve loop queued the `cancel_fired` frame of a run whose
/// body returned after an operator cancel, at most [`CANCEL_REPORT_WAIT`].
fn wait_for_cancel_report(
    runtime: &tokio::runtime::Handle,
    report: &watch::Sender<CancelReport>,
    run_ref: RunRef,
) {
    let mut queued = report.subscribe();
    let waited = runtime.block_on(async move {
        tokio::time::timeout(CANCEL_REPORT_WAIT, async {
            queued
                .wait_for(|report| *report == CancelReport::Queued)
                .await
                .map(|_seen| ())
        })
        .await
    });
    match waited {
        Ok(Ok(())) => {}
        Ok(Err(_)) | Err(_) => tracing::warn!(
            dispatch = run_ref.dispatch.get(),
            task_id = %run_ref.task_id,
            "the cancel_fired frame of a cancelled thread run was not queued within {} ms; the \
             end of the run goes out now",
            CANCEL_REPORT_WAIT.as_millis(),
        ),
    }
}

/// The facts of a thread body: its run, its input, its span source, and
/// its deadline.
struct ThreadBody<'a> {
    run_ref: RunRef,
    facts: &'a RunFacts,
    input: &'a TaskInputText,
    span: SpanSource<'a>,
    timeout: TaskTimeoutMs,
}

/// The body of a shared thread run on the seat thread, inside the span of
/// its body scope. The scope is closed with what the body returned, with
/// `TimedOut` when the deadline came first, or with `Cancelled` when an
/// operator cancel came first: a thread cannot be stopped, so the body
/// returns before the scope closes either way. The deadline also fires the
/// run's cancel signal with the deadline cause, so the body reads it in
/// its cancel state.
fn thread_body(seat: &ThreadSeat, body: &ThreadBody<'_>) -> (SeatEnd, Marker) {
    let ThreadSeat {
        context,
        runtime,
        cancel,
        ..
    } = seat;
    let ThreadBody {
        run_ref,
        facts,
        input,
        span,
        timeout,
    } = *body;
    let (task, module, input) = match prepare(&context.app, &facts.task_name, input) {
        Prepared::Ended(end) => return end_without_body(context, run_ref, &cancel.gate, end),
        Prepared::Body { task, input } => match *task {
            RegisteredTask::Blocking { task, meta } => (task, meta.module, input),
            RegisteredTask::Async { .. } => {
                let end = kind_mismatch_end(&facts.task_name, "shared thread");
                return end_without_body(context, run_ref, &cancel.gate, end);
            }
        },
    };
    match cancel.gate.begin_body() {
        BodyStart::Start => {}
        BodyStart::CancelledBeforeBody => return cancelled_before_body_blocking(context, run_ref),
    }
    let timer_gate = Arc::clone(&cancel.gate);
    let timer_signal = cancel.signal.clone();
    let writer = context.writer.clone();
    let timer = runtime.spawn(async move {
        tokio::time::sleep(Duration::from_millis(timeout.get())).await;
        match timer_gate.fire_deadline() {
            DeadlineFire::Won => {
                // The frame goes out first: a body that returns once it
                // reads the deadline ends after it.
                writer.send(ChildFrame::DeadlineFired(run_ref)).await;
                let _fired = timer_signal.fire(CancelCause::Deadline);
            }
            DeadlineFire::Lost => {}
        }
    });
    let open = context
        .app
        .body_scope
        .open(facts, Execution::SharedThread, span);
    let result = run_blocking_body(
        &task,
        &input,
        &facts.task_name,
        module,
        &open.span(),
        facts.fencing_token(),
        cancel.signal.clone(),
    );
    match cancel.gate.finish() {
        GateEnd::Returned => {
            timer.abort();
            open.close(&BodyOutcome::of_result(&result));
            (SeatEnd::Finished(result_end(result)), Marker::Write)
        }
        // The deadline came first: what the body returned is dropped.
        GateEnd::DeadlineFired => {
            open.close(&BodyOutcome::TimedOut);
            (SeatEnd::TimedOut, Marker::Write)
        }
        // An operator cancel came first: what the body returned is dropped,
        // and the row is in history, so no marker is written.
        GateEnd::CancelFired => {
            timer.abort();
            open.close(&BodyOutcome::Cancelled);
            wait_for_cancel_report(runtime, &cancel.report, run_ref);
            (SeatEnd::Cancelled(CancelledBody::Ran), Marker::Skip)
        }
    }
}

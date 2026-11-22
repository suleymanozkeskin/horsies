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
//!   dropped (aborted) in rounds, then `run_stuck` names what is alive 0.3 s
//!   later, and the run keeps its seat until it ends or the parent kills the
//!   child. The parent reads from `second_cancel` which of the two ended the
//!   run. A body that returns before its deadline with work alive waits
//!   the cancel grace for it, then sends `leftover_cancel` and aborts it in
//!   rounds, then `leftover_stuck`. A run that timed out or needed a cancel
//!   writes no marker. A body, or work it started with `spawn_in_run`,
//!   that holds a runtime thread for 5 s in one poll is reported with
//!   `loop_blocked` and its run as the culprit, and the parent kills the
//!   child.
//! - Shared thread: up to `seats` blocking bodies at once, each on a seat
//!   thread. A thread cannot be stopped: at the deadline `deadline_fired` is
//!   sent and the body runs on; when it returns, its value is dropped and
//!   the run ends timed out. One atomic gate decides between the return and
//!   the deadline.
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
use tokio::sync::mpsc;
use tokio::task::JoinError;

use super::body_scope::{BodyOutcome, OpenBody, SpanSource};
use super::panic::PanicRecord;
use super::rss::read_rss;
use super::run::{
    async_body_in, kind_mismatch_end, mark_finalizing, prepare, refusal_end, result_end,
    run_blocking_body, start_attempt, HostContext, Prepared, RunFacts, Started,
};
use super::run_work::{
    left_report, millis_since, stuck_report, Alive, PollsNow, RoundsEnd, RunPolls, RunScope,
    WorkWait, ISOLATED_LEFTOVER_GRACE,
};
use crate::core::task::error::{OperationalErrorCode, TaskError};
use crate::core::task::fn_trait::{AsyncTaskFn, RawTaskResult, RegisteredTask, TaskModule};
use crate::core::task::result::TaskResult;
use crate::events::AttemptStart;
use crate::worker::executor::protocol::{
    BodyEnd, BodyReturnedFrame, CancelGraceMs, ChildAfterFrame, ChildFrame, DispatchNumber,
    FailureDetail, IsolatedWork, LeftWorkEnd, LeftWorkEndedFrame, LeftWorkStuckFrame,
    LeftoverCancelFrame, LeftoverStuckFrame, LoopBlockedFrame, LoopCulprit, NoLeftWork,
    ParentFrame, ResultFrame, RunEnd, RunFrame, RunRef, RunStuckFrame, SeatEnd, SharedResultFrame,
    SharedRunFrame, SharedWork, StoredOptionsRefusal, TaskInputText, TaskTimeoutMs,
    LOOP_BLOCK_LIMIT,
};
use crate::Execution;

/// How often the poll watch reads the polls in progress.
const POLL_WATCH_INTERVAL: Duration = Duration::from_millis(250);
const POLL_WATCH_THREAD_NAME: &str = "horsies-child-poll-watch";
const SEAT_THREAD_NAME: &str = "horsies-child-seat";

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
    loop {
        match frames.recv().await {
            None => return ServeEnd::ParentGone,
            Some(ParentFrame::Stop) => return ServeEnd::Stopped,
            Some(ParentFrame::Run(run)) => run_isolated(&context, run).await,
            Some(ParentFrame::Init(_) | ParentFrame::SharedRun(_) | ParentFrame::Events(_)) => {
                return ServeEnd::BadParentFrame
            }
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

/// One run of an isolated child and its report (Python `_report_run`).
pub(super) async fn run_isolated(context: &HostContext, run: RunFrame) {
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
    let scope = RunScope::new();
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
                result_end(isolated_body(&scope, &facts.task_name, *task, input, body).await)
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
            end_left_work(context, &facts, &scope, &end, left, started_at).await;
        }
    }
}

/// The body of an isolated run, inside the span of its open body scope
/// `body`, which it closes with what the body returned. An async body runs
/// inside the run, so the work it starts with `spawn_in_run` belongs to the
/// run. A blocking body runs on a blocking thread of the runtime. A panic
/// of either is `UNHANDLED_EXCEPTION` with its record. The parent ends a
/// run past its timeout by killing the child, so the scope never sees a
/// timeout here.
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
            let thread = tokio::task::spawn_blocking(move || {
                run_blocking_body(&task, &input, &name, meta.module, &span)
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
    body.close(&BodyOutcome::of_result(&result));
    result
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

/// The stop frame is valid only with no run in flight.
fn stop_end(in_flight: &AtomicUsize) -> ServeEnd {
    match in_flight.load(Ordering::Acquire) {
        0 => ServeEnd::Stopped,
        _ => ServeEnd::BadParentFrame,
    }
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
    loop {
        let run = match frames.recv().await {
            None => return ServeEnd::ParentGone,
            Some(ParentFrame::Stop) => return stop_end(&in_flight),
            Some(ParentFrame::SharedRun(run)) => run,
            Some(ParentFrame::Init(_) | ParentFrame::Run(_) | ParentFrame::Events(_)) => {
                return ServeEnd::BadParentFrame
            }
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
        tokio::spawn(async_seat(
            context.clone(),
            watch.clone(),
            run,
            work,
            Arc::clone(&in_flight),
        ));
    }
}

async fn async_seat(
    context: HostContext,
    watch: PollWatch,
    run: SharedRunFrame,
    work: AsyncSeatWork,
    in_flight: Arc<AtomicUsize>,
) {
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
            let (end, marker) = match work {
                AsyncSeatWork::Refusal(refusal) => {
                    (SeatEnd::Finished(refusal_end(&refusal)), Marker::Write)
                }
                AsyncSeatWork::Task { timeout, grace } => {
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
                    async_body(&context, &watch, body, timeout, grace).await
                }
            };
            match marker {
                Marker::Write => mark_finalizing(&context, &facts).await,
                Marker::Skip => {}
            }
            (end, AttemptStart::Reported { started_at })
        }
    };
    let frame = ChildFrame::SharedResult(SharedResultFrame {
        run: run_ref,
        end,
        started_at: started,
        rss: read_rss(),
        child: ChildAfterFrame::ServesOn,
    });
    context.writer.send(frame).await;
    in_flight.fetch_sub(1, Ordering::AcqRel);
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

/// The body of a shared async run, inside the span of its body scope. The
/// scope is closed with what the body returned, or with `TimedOut` at the
/// deadline: the body may run on in the cancel grace, and the span ends
/// when the body ends or is dropped.
async fn async_body(
    context: &HostContext,
    watch: &PollWatch,
    body: SeatBody<'_>,
    timeout: TaskTimeoutMs,
    grace: CancelGraceMs,
) -> (SeatEnd, Marker) {
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
    let scope = RunScope::timed(watch.register(run_ref));
    let open = context
        .app
        .body_scope
        .open(facts, Execution::SharedAsync, span);
    let body = async_body_in(task, input, open.span());
    let mut handle = scope.spawn_body(&facts.task_name, body);
    let deadline = tokio::time::sleep(Duration::from_millis(timeout.get()));
    tokio::pin!(deadline);
    let outcome = tokio::select! {
        joined = &mut handle => {
            let result = joined_result(&facts.task_name, module, joined);
            open.close(&BodyOutcome::of_result(&result));
            let end = result_end(result);
            after_return(context, run_ref, &scope, end, grace, started_at).await
        }
        () = &mut deadline => {
            open.close(&BodyOutcome::TimedOut);
            // The body runs on: it sees the signal and has the grace to end.
            scope.fire_cancel();
            context.writer.send(ChildFrame::DeadlineFired(run_ref)).await;
            stop_after_deadline(context, run_ref, &scope, grace).await;
            (SeatEnd::TimedOut, Marker::Skip)
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

/// After the run's cancel signal fired at its deadline: when anything of
/// the run (the body or its work) is alive after the cancel grace, report the
/// second cancel and drop it all in rounds; then report what is stuck, and
/// wait for the end. A run that ended within the grace sends no second
/// cancel, so the parent records that the body returned after the signal.
async fn stop_after_deadline(
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
        // the child; the end is the timeout either way.
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
/// that holds every runtime thread cannot keep it from reporting.
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

    /// Start the watch thread. Each pass reads every run once.
    pub(super) fn start(writer: super::link::FrameWriter) -> io::Result<Self> {
        let watch = Self {
            runs: Arc::new(Mutex::new(HashMap::new())),
            epoch: Instant::now(),
        };
        let thread_watch = watch.clone();
        thread::Builder::new()
            .name(POLL_WATCH_THREAD_NAME.to_owned())
            .spawn(move || loop {
                thread::sleep(POLL_WATCH_INTERVAL);
                for frame in thread_watch.blocked_reports() {
                    writer.send_blocking(frame);
                }
            })?;
        Ok(watch)
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

pub(super) async fn serve_shared_thread(
    context: HostContext,
    frames: &mut mpsc::Receiver<ParentFrame>,
    seats: usize,
) -> ServeEnd {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let runtime = tokio::runtime::Handle::current();
    loop {
        let run = match frames.recv().await {
            None => return ServeEnd::ParentGone,
            Some(ParentFrame::Stop) => return stop_end(&in_flight),
            Some(ParentFrame::SharedRun(run)) => run,
            Some(ParentFrame::Init(_) | ParentFrame::Run(_) | ParentFrame::Events(_)) => {
                return ServeEnd::BadParentFrame
            }
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
        let seat_context = context.clone();
        let seat_runtime = runtime.clone();
        let seat_in_flight = Arc::clone(&in_flight);
        let started = thread::Builder::new()
            .name(SEAT_THREAD_NAME.to_owned())
            .spawn(move || {
                thread_seat(&seat_context, &seat_runtime, &run, work);
                seat_in_flight.fetch_sub(1, Ordering::AcqRel);
            });
        if let Err(error) = started {
            // No start was written: the row is still CLAIMED.
            let frame = ChildFrame::SharedResult(SharedResultFrame {
                run: run_ref,
                end: SeatEnd::Finished(RunEnd::WorkerFailure(FailureDetail::cut(&format!(
                    "the OS refused a seat thread: {error}"
                )))),
                started_at: AttemptStart::NotReported,
                rss: read_rss(),
                child: ChildAfterFrame::ServesOn,
            });
            context.writer.send(frame).await;
            in_flight.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

fn thread_seat(
    context: &HostContext,
    runtime: &tokio::runtime::Handle,
    run: &SharedRunFrame,
    work: ThreadSeatWork,
) {
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
            let end = match work {
                ThreadSeatWork::Refusal(refusal) => SeatEnd::Finished(refusal_end(&refusal)),
                ThreadSeatWork::Task { timeout } => {
                    thread_body(context, runtime, run_ref, &facts, &run.input, span, timeout)
                }
            };
            runtime.block_on(mark_finalizing(context, &facts));
            (end, AttemptStart::Reported { started_at })
        }
    };
    context
        .writer
        .send_blocking(ChildFrame::SharedResult(SharedResultFrame {
            run: run_ref,
            end,
            started_at: started,
            rss: read_rss(),
            child: ChildAfterFrame::ServesOn,
        }));
}

const GATE_OPEN: u8 = 0;
const GATE_RETURNED: u8 = 1;
const GATE_FIRED: u8 = 2;

/// Which came first for a thread run: its return or its deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GateEnd {
    Returned,
    Fired,
}

/// One gate per run: exactly one of the return and the deadline wins.
pub(super) struct DeadlineGate(AtomicU8);

impl DeadlineGate {
    pub(super) fn new() -> Self {
        Self(AtomicU8::new(GATE_OPEN))
    }

    /// The deadline: `true` when it won.
    pub(super) fn fire(&self) -> bool {
        self.0
            .compare_exchange(GATE_OPEN, GATE_FIRED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// The return: which one won.
    pub(super) fn finish(&self) -> GateEnd {
        match self.0.compare_exchange(
            GATE_OPEN,
            GATE_RETURNED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => GateEnd::Returned,
            Err(_) => GateEnd::Fired,
        }
    }
}

/// The body of a shared thread run on the seat thread, inside the span of
/// its body scope. The scope is closed with what the body returned, or with
/// `TimedOut` when the deadline came first: a thread cannot be stopped, so
/// the body returns before the scope closes either way.
fn thread_body(
    context: &HostContext,
    runtime: &tokio::runtime::Handle,
    run_ref: RunRef,
    facts: &RunFacts,
    input: &TaskInputText,
    span: SpanSource<'_>,
    timeout: TaskTimeoutMs,
) -> SeatEnd {
    let (task, module, input) = match prepare(&context.app, &facts.task_name, input) {
        Prepared::Ended(end) => return SeatEnd::Finished(end),
        Prepared::Body { task, input } => match *task {
            RegisteredTask::Blocking { task, meta } => (task, meta.module, input),
            RegisteredTask::Async { .. } => {
                return SeatEnd::Finished(kind_mismatch_end(&facts.task_name, "shared thread"))
            }
        },
    };
    let gate = Arc::new(DeadlineGate::new());
    let timer_gate = Arc::clone(&gate);
    let writer = context.writer.clone();
    let timer = runtime.spawn(async move {
        tokio::time::sleep(Duration::from_millis(timeout.get())).await;
        if timer_gate.fire() {
            writer.send(ChildFrame::DeadlineFired(run_ref)).await;
        }
    });
    let open = context
        .app
        .body_scope
        .open(facts, Execution::SharedThread, span);
    let result = run_blocking_body(&task, &input, &facts.task_name, module, &open.span());
    match gate.finish() {
        GateEnd::Returned => {
            timer.abort();
            open.close(&BodyOutcome::of_result(&result));
            SeatEnd::Finished(result_end(result))
        }
        // The deadline came first: what the body returned is dropped.
        GateEnd::Fired => {
            open.close(&BodyOutcome::TimedOut);
            SeatEnd::TimedOut
        }
    }
}

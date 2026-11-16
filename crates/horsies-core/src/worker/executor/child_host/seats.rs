//! The serve loops of a Rust child, one per child kind.
//!
//! - Isolated: one task at a time; the parent holds the deadline.
//! - Shared async: up to `seats` async bodies at once on the child's
//!   runtime. At the deadline the body is aborted and `deadline_fired` is
//!   sent; a body whose drop does not land within the cancel grace gets
//!   `second_cancel`, then `run_stuck` 0.3 s later, and keeps its seat until
//!   it ends or the parent kills the child. A body that holds a runtime
//!   thread for 5 s in one poll is reported with `loop_blocked`, and the
//!   parent kills the child.
//! - Shared thread: up to `seats` blocking bodies at once, each on a seat
//!   thread. A thread cannot be stopped: at the deadline `deadline_fired` is
//!   sent and the body runs on; when it returns, its value is dropped and
//!   the run ends timed out. One atomic gate decides between the return and
//!   the deadline.
//!
//! A Rust body that spawns work of its own is not tracked, so a Rust child
//! sends no report of left work.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};

use super::rss::read_rss;
use super::run::{
    kind_mismatch_end, mark_finalizing, prepare, refusal_end, result_end, run_blocking_body,
    start_attempt, HostContext, Prepared, RunFacts, Started,
};
use crate::core::task::error::{OperationalErrorCode, TaskError};
use crate::core::task::fn_trait::{AsyncTaskFn, RawTaskResult, RegisteredTask};
use crate::core::task::result::TaskResult;
use crate::worker::execution::execute_task;
use crate::worker::executor::protocol::{
    BlockedWhere, CancelGraceMs, ChildFrame, DispatchNumber, FailureDetail, IsolatedWork,
    LeftoverName, LoopBlockedFrame, LoopCulprit, NoLeftWork, ParentFrame, ResultFrame, RunEnd,
    RunFrame, RunRef, RunStuckFrame, SeatEnd, SharedResultFrame, SharedRunFrame, SharedWork,
    StoredOptionsRefusal, StuckWork, TaskInputText, TaskTimeoutMs, LOOP_BLOCK_LIMIT,
    STUCK_REPORT_DELAY,
};

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
            Some(ParentFrame::Run(run)) => {
                let task_id = run.task_id;
                let end = run_isolated(&context, run).await;
                let frame = ChildFrame::Result(ResultFrame {
                    task_id,
                    end,
                    rss: read_rss(),
                    task_loop: NoLeftWork::NoTaskLoop,
                });
                context.writer.send(frame).await;
            }
            Some(ParentFrame::Init(_) | ParentFrame::SharedRun(_)) => {
                return ServeEnd::BadParentFrame
            }
        }
    }
}

async fn run_isolated(context: &HostContext, run: RunFrame) -> RunEnd {
    let RunFrame {
        task_id,
        task_name,
        worker_id,
        task_kind,
        claimed_at,
        work,
        input,
    } = run;
    let facts = RunFacts {
        task_id,
        task_name,
        worker_id,
        task_kind,
        claimed_at,
    };
    if let Started::Stopped(reason) = start_attempt(context, &facts).await {
        return RunEnd::StoppedBeforeRunning(reason);
    }
    let end = match work {
        IsolatedWork::Refusal(refusal) => refusal_end(&refusal),
        IsolatedWork::Run => match prepare(&context.app, &facts.task_name, &input) {
            Prepared::Ended(end) => end,
            Prepared::Body { task, input } => result_end(execute_task(task, input, None).await),
        },
    };
    mark_finalizing(context, &facts).await;
    end
}

fn facts_of(run: &SharedRunFrame) -> RunFacts {
    RunFacts {
        task_id: run.task_id,
        task_name: run.task_name.clone(),
        worker_id: run.worker_id.clone(),
        task_kind: run.task_kind,
        claimed_at: run.claimed_at,
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
            Some(ParentFrame::Init(_) | ParentFrame::Run(_)) => return ServeEnd::BadParentFrame,
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
    let end = match start_attempt(&context, &facts).await {
        Started::Stopped(reason) => SeatEnd::Finished(RunEnd::StoppedBeforeRunning(reason)),
        Started::Running => {
            let end = match work {
                AsyncSeatWork::Refusal(refusal) => SeatEnd::Finished(refusal_end(&refusal)),
                AsyncSeatWork::Task { timeout, grace } => {
                    async_body(
                        &context, &watch, run_ref, &facts, &run.input, timeout, grace,
                    )
                    .await
                }
            };
            mark_finalizing(&context, &facts).await;
            end
        }
    };
    let frame = ChildFrame::SharedResult(SharedResultFrame {
        run: run_ref,
        end,
        rss: read_rss(),
    });
    context.writer.send(frame).await;
    in_flight.fetch_sub(1, Ordering::AcqRel);
}

fn joined_result(joined: Result<RawTaskResult, JoinError>) -> RawTaskResult {
    match joined {
        Ok(result) => result,
        Err(error) => TaskResult::Err(TaskError::builtin(
            OperationalErrorCode::UnhandledError,
            format!("async task panicked: {error}"),
        )),
    }
}

async fn async_body(
    context: &HostContext,
    watch: &PollWatch,
    run_ref: RunRef,
    facts: &RunFacts,
    input: &TaskInputText,
    timeout: TaskTimeoutMs,
    grace: CancelGraceMs,
) -> SeatEnd {
    let (task, input): (Arc<dyn AsyncTaskFn>, Vec<u8>) =
        match prepare(&context.app, &facts.task_name, input) {
            Prepared::Ended(end) => return SeatEnd::Finished(end),
            Prepared::Body {
                task: RegisteredTask::Async { task, .. },
                input,
            } => (task, input),
            Prepared::Body {
                task: RegisteredTask::Blocking { .. },
                ..
            } => return SeatEnd::Finished(kind_mismatch_end(&facts.task_name, "shared async")),
        };
    let poll_started = watch.register(run_ref, &facts.task_name);
    let body = async move { task.execute(&input).await };
    let mut handle = tokio::spawn(Watched::new(body, poll_started, watch.epoch));
    let deadline = tokio::time::sleep(Duration::from_millis(timeout.get()));
    tokio::pin!(deadline);
    let end = tokio::select! {
        joined = &mut handle => SeatEnd::Finished(result_end(joined_result(joined))),
        () = &mut deadline => {
            handle.abort();
            context.writer.send(ChildFrame::DeadlineFired(run_ref)).await;
            stop_after_deadline(context, run_ref, &facts.task_name, &mut handle, grace).await;
            SeatEnd::TimedOut
        }
    };
    watch.unregister(run_ref.dispatch);
    end
}

/// After the deadline's abort: wait the cancel grace for the drop, then
/// report the second cancel, then the stuck run, then wait for the end.
async fn stop_after_deadline(
    context: &HostContext,
    run_ref: RunRef,
    task_name: &str,
    handle: &mut JoinHandle<RawTaskResult>,
    grace: CancelGraceMs,
) {
    let grace = Duration::from_millis(grace.get());
    if tokio::time::timeout(grace, &mut *handle).await.is_ok() {
        return;
    }
    context.writer.send(ChildFrame::SecondCancel(run_ref)).await;
    if tokio::time::timeout(STUCK_REPORT_DELAY, &mut *handle)
        .await
        .is_ok()
    {
        return;
    }
    let work = StuckWork::new(1, 0, vec![LeftoverName::cut(task_name)])
        .expect("one live task with its one name is a valid stuck report");
    context
        .writer
        .send(ChildFrame::RunStuck(RunStuckFrame { run: run_ref, work }))
        .await;
    // The run keeps its seat until its body ends or the parent kills the
    // child; the end is the timeout either way.
    let _ = (&mut *handle).await;
}

/// The poll of an async body, timed for the poll watch.
struct Watched<F> {
    inner: Pin<Box<F>>,
    poll_started: Arc<AtomicU64>,
    epoch: Instant,
}

impl<F> Watched<F> {
    fn new(inner: F, poll_started: Arc<AtomicU64>, epoch: Instant) -> Self {
        Self {
            inner: Box::pin(inner),
            poll_started,
            epoch,
        }
    }
}

fn millis_since(epoch: Instant) -> u64 {
    u64::try_from(epoch.elapsed().as_millis()).unwrap_or(u64::MAX)
}

impl<F: Future> Future for Watched<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        // 0 means "no poll in progress", so a poll start is stored from 1.
        let started = millis_since(self.epoch).saturating_add(1);
        self.poll_started.store(started, Ordering::Release);
        let polled = self.inner.as_mut().poll(cx);
        self.poll_started.store(0, Ordering::Release);
        polled
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
    location: BlockedWhere,
    poll_started: Arc<AtomicU64>,
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

    pub(super) fn register(&self, run: RunRef, task_name: &str) -> Arc<AtomicU64> {
        let poll_started = Arc::new(AtomicU64::new(0));
        lock_runs(&self.runs).insert(
            run.dispatch,
            WatchedRun {
                run,
                location: BlockedWhere::cut(task_name),
                poll_started: Arc::clone(&poll_started),
                report: BlockReport::NotReported,
            },
        );
        poll_started
    }

    fn unregister(&self, dispatch: DispatchNumber) {
        let _removed = lock_runs(&self.runs).remove(&dispatch);
    }

    /// The `loop_blocked` frames of runs whose current poll passed the
    /// limit, each reported once.
    pub(super) fn blocked_reports(&self) -> Vec<ChildFrame> {
        let now = millis_since(self.epoch).saturating_add(1);
        let limit = u64::try_from(LOOP_BLOCK_LIMIT.as_millis()).unwrap_or(u64::MAX);
        let mut runs = lock_runs(&self.runs);
        let mut frames = Vec::new();
        for watched in runs.values_mut() {
            let started = watched.poll_started.load(Ordering::Acquire);
            let blocked_ms = now.saturating_sub(started);
            let blocked = started != 0 && blocked_ms >= limit;
            if blocked && watched.report == BlockReport::NotReported {
                watched.report = BlockReport::Reported;
                frames.push(ChildFrame::LoopBlocked(LoopBlockedFrame {
                    culprit: LoopCulprit::Run(watched.run),
                    blocked_ms,
                    location: watched.location.clone(),
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
            Some(ParentFrame::Init(_) | ParentFrame::Run(_)) => return ServeEnd::BadParentFrame,
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
                thread_seat(&seat_context, &seat_runtime, run, work);
                seat_in_flight.fetch_sub(1, Ordering::AcqRel);
            });
        if let Err(error) = started {
            // No start was written: the row is still CLAIMED.
            let frame = ChildFrame::SharedResult(SharedResultFrame {
                run: run_ref,
                end: SeatEnd::Finished(RunEnd::WorkerFailure(FailureDetail::cut(&format!(
                    "the OS refused a seat thread: {error}"
                )))),
                rss: read_rss(),
            });
            context.writer.send(frame).await;
            in_flight.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

fn thread_seat(
    context: &HostContext,
    runtime: &tokio::runtime::Handle,
    run: SharedRunFrame,
    work: ThreadSeatWork,
) {
    let run_ref = run_ref_of(&run);
    let facts = facts_of(&run);
    let end = match runtime.block_on(start_attempt(context, &facts)) {
        Started::Stopped(reason) => SeatEnd::Finished(RunEnd::StoppedBeforeRunning(reason)),
        Started::Running => {
            let end = match work {
                ThreadSeatWork::Refusal(refusal) => SeatEnd::Finished(refusal_end(&refusal)),
                ThreadSeatWork::Task { timeout } => {
                    thread_body(context, runtime, run_ref, &facts, &run.input, timeout)
                }
            };
            runtime.block_on(mark_finalizing(context, &facts));
            end
        }
    };
    context
        .writer
        .send_blocking(ChildFrame::SharedResult(SharedResultFrame {
            run: run_ref,
            end,
            rss: read_rss(),
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

fn thread_body(
    context: &HostContext,
    runtime: &tokio::runtime::Handle,
    run_ref: RunRef,
    facts: &RunFacts,
    input: &TaskInputText,
    timeout: TaskTimeoutMs,
) -> SeatEnd {
    let (task, input) = match prepare(&context.app, &facts.task_name, input) {
        Prepared::Ended(end) => return SeatEnd::Finished(end),
        Prepared::Body {
            task: RegisteredTask::Blocking { task, .. },
            input,
        } => (task, input),
        Prepared::Body {
            task: RegisteredTask::Async { .. },
            ..
        } => return SeatEnd::Finished(kind_mismatch_end(&facts.task_name, "shared thread")),
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
    let result = run_blocking_body(&task, &input);
    match gate.finish() {
        GateEnd::Returned => {
            timer.abort();
            SeatEnd::Finished(result_end(result))
        }
        // The deadline came first: what the body returned is dropped.
        GateEnd::Fired => SeatEnd::TimedOut,
    }
}

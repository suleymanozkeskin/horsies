//! The work of one run in a Rust child: its async body and the work the
//! body starts with [`spawn_in_run`] (Python: the asyncio tasks that carry
//! the run's mark, `core/worker/run_mark.py`).
//!
//! Work that a body starts with plain `tokio::spawn` is not tracked. Work
//! started with [`spawn_in_run`] belongs to the run: the child counts it as
//! work the run left when its body returns, waits for it, cancels it in
//! rounds, and reports what did not stop, as a Python child does with the
//! tasks of its loop. Work that it starts in turn belongs to the same run.
//!
//! In a shared async child each poll of the body and of each piece of work
//! is timed under its run ([`RunPolls`]), so the poll watch reports a poll
//! that holds a runtime thread with that run as the culprit (Python: the
//! loop watchdog names the run of the mark that the blocking code carries).
//!
//! Each run has one cancel signal ([`run_cancel_signal`]) and its cause
//! ([`run_cancel_state`]). It fires at most once, with the first cause:
//!
//! - [`CancelCause::Operator`] in every child kind, when the parent sends
//!   `cancel_run` for the run: an operator cancelled the task with
//!   `include_running`, and the cancel statement already recorded the
//!   attempt as `CANCELLED`.
//! - [`CancelCause::Deadline`] in a shared async child at the run's deadline,
//!   and in a shared thread child at the run's deadline. In an isolated
//!   child the parent holds the deadline and kills the child, so the
//!   deadline never fires the signal there.
//!
//! In a shared async child the body and its work then have the run's cancel
//! grace to end before the child drops them (Python: the cancel of the task,
//! then the second cancel after the grace).
//!
//! Each run has one fencing token ([`run_fencing_token`]): the task id and
//! the attempt number of its run frame. The async body and the work it
//! starts with [`spawn_in_run`] read the token and the signal from the run;
//! a blocking body reads them from its thread, which [`in_blocking_run`]
//! marks while the body runs.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::Instant;

use crate::core::task::fencing::{FencingToken, FencingTokenRefused};
pub(super) use crate::worker::executor::protocol::ISOLATED_LEFTOVER_GRACE;
use crate::worker::executor::protocol::{
    BlockedWhere, LeftoverName, LoopHoldsTasks, StuckWork, MAX_LEFTOVER_TASK_NAMES,
};

/// The most pieces of work one run holds alive at once.
pub const MAX_RUN_WORK: usize = 10_000;

/// The rounds of aborts after the first one, for work that must stop
/// (Python `EXTRA_CANCEL_ROUNDS`).
pub(super) const EXTRA_CANCEL_ROUNDS: u32 = 2;
/// The time between two rounds, and from the last round to the stuck check
/// (Python `CANCEL_ROUND_INTERVAL_S`). Three intervals make the
/// protocol's `STUCK_REPORT_DELAY`.
pub(super) const CANCEL_ROUND_INTERVAL: Duration = Duration::from_millis(100);

tokio::task_local! {
    static RUN_SCOPE: RunScope;
}

/// Whether the calling thread runs a blocking body, and the fencing token and
/// the cancel signal of its run.
#[derive(Clone)]
enum BlockingRun {
    NoRun,
    Run {
        token: FencingToken,
        cancel: RunCancelSignal,
    },
}

thread_local! {
    /// Set while this thread runs the body of a blocking task.
    static BLOCKING_RUN: RefCell<BlockingRun> = const { RefCell::new(BlockingRun::NoRun) };
}

/// Run `body` with `token` as the fencing token and `cancel` as the cancel
/// signal of the calling thread. The earlier value of the thread comes back
/// when `body` ends, also when it panics. No borrow of the thread value is
/// held while `body` runs.
pub(super) fn in_blocking_run<R>(
    token: FencingToken,
    cancel: RunCancelSignal,
    body: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<BlockingRun>);
    impl Drop for Restore {
        fn drop(&mut self) {
            if let Some(earlier) = self.0.take() {
                let _ended_run = BLOCKING_RUN.with(|run| run.replace(earlier));
            }
        }
    }
    let earlier = BLOCKING_RUN.with(|run| run.replace(BlockingRun::Run { token, cancel }));
    let _restore = Restore(Some(earlier));
    body()
}

/// The run of the blocking body on the calling thread. Pure read.
fn blocking_run() -> BlockingRun {
    BLOCKING_RUN.with(|run| run.borrow().clone())
}

/// The fencing token of the current run, read by its async body, by work
/// that the body started with [`spawn_in_run`], or on the thread of its
/// blocking body. Every read in one run gives the same token. Pure read.
///
/// Work started with plain `tokio::spawn`, `spawn_blocking` from an async
/// body, and a thread that a blocking body starts have no run: read the
/// token in the body and pass it on.
///
/// Errors: [`FencingTokenRefused::NotInARun`].
pub fn run_fencing_token() -> Result<FencingToken, FencingTokenRefused> {
    RUN_SCOPE
        .try_with(|scope| scope.token)
        .or_else(|_outside_async_run| blocking_run_token())
}

/// The fencing token of the blocking body that runs on the calling thread.
fn blocking_run_token() -> Result<FencingToken, FencingTokenRefused> {
    match blocking_run() {
        BlockingRun::Run { token, .. } => Ok(token),
        BlockingRun::NoRun => Err(FencingTokenRefused::NotInARun),
    }
}

/// Why [`spawn_in_run`] started no work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RunSpawnRefused {
    /// The caller is not the async body of a run in a Rust child, nor work
    /// that such a body started with `spawn_in_run`.
    #[error("spawn_in_run was called outside the async body of a task run in a Rust child")]
    NotInARun,
    /// The run has ended; its left work is being stopped.
    #[error("the run has ended; it takes no new work")]
    RunEnded,
    /// The run holds [`MAX_RUN_WORK`] pieces of work alive already.
    #[error("the run holds {bound} pieces of work alive already")]
    TooMuchWork { bound: usize },
}

/// Start `future` as work of the current run, named `name` in reports. It
/// is counted as work the run left when its body returns, and it is
/// aborted with the run. The returned handle gives its output.
///
/// Errors: [`RunSpawnRefused`]; nothing was started then.
pub fn spawn_in_run<F>(name: &str, future: F) -> Result<JoinHandle<F::Output>, RunSpawnRefused>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let scope = RUN_SCOPE
        .try_with(RunScope::clone)
        .map_err(|_| RunSpawnRefused::NotInARun)?;
    let id = scope.register(name)?;
    Ok(scope.spawn_registered(id, name, future))
}

/// Why [`run_cancel_signal`] gave no signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RunSignalRefused {
    /// The caller is not the body of a run in a Rust child (its async body,
    /// or the thread of its blocking body), nor work that an async body
    /// started with `spawn_in_run`.
    #[error("run_cancel_signal was called outside the body of a task run in a Rust child")]
    NotInARun,
}

/// The cancel signal of the current run, read by its async body, by work
/// that the body started with [`spawn_in_run`], or on the thread of its
/// blocking body. Every read in one run gives the same signal.
///
/// Errors: [`RunSignalRefused::NotInARun`].
pub fn run_cancel_signal() -> Result<RunCancelSignal, RunSignalRefused> {
    RUN_SCOPE.try_with(|scope| scope.cancel.clone()).or_else(
        |_outside_async_run| match blocking_run() {
            BlockingRun::Run { cancel, .. } => Ok(cancel),
            BlockingRun::NoRun => Err(RunSignalRefused::NotInARun),
        },
    )
}

/// Why the cancel of a run was requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CancelCause {
    /// The deadline of the run (`timeout_ms`): a `SHARED_ASYNC` run, whose
    /// end is then `TASK_TIMEOUT`, or a `SHARED_THREAD` run, whose thread
    /// runs on.
    Deadline,
    /// An operator cancelled the task with `include_running`. The task is
    /// `CANCELLED` already; what the body returns is dropped.
    Operator,
}

/// The cancel state of the current run, as task code reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RunCancelState {
    /// The run runs, and no cancel was requested.
    NotRequested,
    /// A cancel was requested; `cause` is the first one.
    Requested { cause: CancelCause },
    /// The caller is not in a run: a direct call of the function in a test,
    /// work started with plain `tokio::spawn` or `spawn_blocking`, or a
    /// thread that a blocking body starts.
    NotInARun,
}

/// The cancel state of the current run, read by its async body, by work
/// that the body started with [`spawn_in_run`], or on the thread of its
/// blocking body. Pure read.
pub fn run_cancel_state() -> RunCancelState {
    match run_cancel_signal() {
        Ok(signal) => match signal.state() {
            SignalState::NotFired => RunCancelState::NotRequested,
            SignalState::Fired { cause } => RunCancelState::Requested { cause },
        },
        Err(RunSignalRefused::NotInARun) => RunCancelState::NotInARun,
    }
}

/// The stored state of a signal that has not fired.
const SIGNAL_NOT_FIRED: u8 = 0;
/// The stored state of a signal fired at the deadline.
const SIGNAL_DEADLINE: u8 = 1;
/// The stored state of a signal fired at an operator cancel.
const SIGNAL_OPERATOR: u8 = 2;

/// Whether a cancel signal fired, and for which cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SignalState {
    NotFired,
    Fired { cause: CancelCause },
}

/// What one fire of a signal did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Fire {
    /// This fire fired the signal, with its cause.
    Won,
    /// The signal had fired already; its cause stays the first one.
    AlreadyFired,
}

/// The state that every copy of one run's cancel signal shares.
struct CancelShared {
    /// [`SIGNAL_NOT_FIRED`], [`SIGNAL_DEADLINE`] or [`SIGNAL_OPERATOR`].
    state: AtomicU8,
    /// Woken once, when the signal fires.
    woken: Notify,
}

/// The cancel signal of one run. It fires at most once, with the first
/// cause: at an operator cancel in every child kind, and at the run's
/// deadline in a shared async or a shared thread child (see the module
/// docs). A shared async run cancelled at its deadline ends as a timeout
/// whatever its body returns; a run cancelled by an operator ends cancelled.
/// In a shared async child the body and its work are dropped when they are
/// alive after the run's cancel grace.
#[derive(Clone)]
pub struct RunCancelSignal {
    shared: Arc<CancelShared>,
}

impl RunCancelSignal {
    /// A signal that has not fired, for one run.
    pub(super) fn new() -> Self {
        Self {
            shared: Arc::new(CancelShared {
                state: AtomicU8::new(SIGNAL_NOT_FIRED),
                woken: Notify::new(),
            }),
        }
    }

    /// Whether the signal fired, for any cause.
    pub fn is_cancelled(&self) -> bool {
        self.shared.state.load(Ordering::Acquire) != SIGNAL_NOT_FIRED
    }

    /// Whether the signal fired, and its cause. Pure read.
    pub(super) fn state(&self) -> SignalState {
        match self.shared.state.load(Ordering::Acquire) {
            SIGNAL_NOT_FIRED => SignalState::NotFired,
            SIGNAL_DEADLINE => SignalState::Fired {
                cause: CancelCause::Deadline,
            },
            SIGNAL_OPERATOR => SignalState::Fired {
                cause: CancelCause::Operator,
            },
            other => unreachable!("a cancel signal holds the unknown state {other}"),
        }
    }

    /// Complete when the signal fires; at once when it fired already.
    pub async fn cancelled(&self) {
        loop {
            let woken = self.shared.woken.notified();
            tokio::pin!(woken);
            // Registered before the check, so a fire between the check and
            // the wait still wakes it.
            woken.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            woken.await;
        }
    }

    /// Fire the signal with `cause` and wake every waiter. A later fire
    /// changes nothing: the first cause stays.
    pub(super) fn fire(&self, cause: CancelCause) -> Fire {
        let stored = match cause {
            CancelCause::Deadline => SIGNAL_DEADLINE,
            CancelCause::Operator => SIGNAL_OPERATOR,
        };
        match self.shared.state.compare_exchange(
            SIGNAL_NOT_FIRED,
            stored,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.shared.woken.notify_waiters();
                Fire::Won
            }
            Err(_) => Fire::AlreadyFired,
        }
    }
}

impl fmt::Debug for RunCancelSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunCancelSignal")
            .field("state", &self.state())
            .finish()
    }
}

/// Milliseconds from `epoch` to now, saturated.
pub(super) fn millis_since(epoch: std::time::Instant) -> u64 {
    u64::try_from(epoch.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// One timed future of a run: the body, or one piece of work.
struct TimedPoll {
    /// The task name for the body, the name given to [`spawn_in_run`] for
    /// work.
    location: BlockedWhere,
    /// 0 when no poll is in progress; else the start of the poll in
    /// milliseconds after the epoch, plus 1.
    started: Arc<AtomicU64>,
}

/// What a read of a run's polls found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PollsNow {
    /// No future of the run is in a poll.
    NoPollInProgress,
    /// The poll in progress that started first.
    Longest {
        blocked_ms: u64,
        location: BlockedWhere,
    },
}

/// The poll starts of one run of a shared async child: one per future of
/// the run (its body and each piece of work), because the pieces poll on
/// different runtime threads at once. A future leaves the set when it ends.
#[derive(Clone)]
pub(super) struct RunPolls {
    epoch: std::time::Instant,
    starts: Arc<Mutex<BTreeMap<u64, TimedPoll>>>,
}

impl RunPolls {
    /// The polls of a new run, timed from `epoch` (the poll watch's).
    pub(super) fn new(epoch: std::time::Instant) -> Self {
        Self {
            epoch,
            starts: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<u64, TimedPoll>> {
        // A poisoned lock means a panic while it was held; the map is a
        // valid value.
        self.starts.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The poll start of the future `id` of the run, named `name` in a
    /// report.
    pub(super) fn start_of(&self, id: u64, name: &str) -> Arc<AtomicU64> {
        let started = Arc::new(AtomicU64::new(0));
        let _replaced = self.lock().insert(
            id,
            TimedPoll {
                location: BlockedWhere::cut(name),
                started: Arc::clone(&started),
            },
        );
        started
    }

    fn forget(&self, id: u64) {
        let _removed = self.lock().remove(&id);
    }

    /// The poll in progress that started first, read at `now` (milliseconds
    /// after the epoch, plus 1, as the starts are stored).
    pub(super) fn longest_poll(&self, now: u64) -> PollsNow {
        let starts = self.lock();
        let mut longest = PollsNow::NoPollInProgress;
        for timed in starts.values() {
            let started = timed.started.load(Ordering::Acquire);
            if started == 0 {
                continue;
            }
            let blocked_ms = now.saturating_sub(started);
            let longer = match &longest {
                PollsNow::NoPollInProgress => true,
                PollsNow::Longest {
                    blocked_ms: so_far, ..
                } => blocked_ms > *so_far,
            };
            if longer {
                longest = PollsNow::Longest {
                    blocked_ms,
                    location: timed.location.clone(),
                };
            }
        }
        longest
    }
}

/// One future of a run, each poll timed in its poll start.
struct Watched<F> {
    inner: Pin<Box<F>>,
    poll_started: Arc<AtomicU64>,
    epoch: std::time::Instant,
}

impl<F> Watched<F> {
    fn new(inner: F, poll_started: Arc<AtomicU64>, epoch: std::time::Instant) -> Self {
        Self {
            inner: Box::pin(inner),
            poll_started,
            epoch,
        }
    }
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

/// Whether the polls of a run are timed.
#[derive(Clone)]
enum PollTiming {
    /// An isolated child: the parent holds the deadline; no poll watch.
    Untimed,
    /// A shared async child: the poll watch reads these polls.
    Timed(RunPolls),
}

/// Whether the run takes new work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScopeState {
    Open,
    Closed,
}

/// How a piece of work can be aborted. The handle comes right after the
/// spawn; an abort before it is kept and done when the handle comes.
enum AbortSlot {
    Pending,
    AbortWhenAttached,
    Attached(AbortHandle),
}

struct AliveWork {
    name: LeftoverName,
    abort: AbortSlot,
}

struct ScopeWork {
    state: ScopeState,
    next_id: u64,
    alive: BTreeMap<u64, AliveWork>,
}

struct ScopeShared {
    work: Mutex<ScopeWork>,
    /// Woken each time a piece of work ends.
    ended: Notify,
}

impl ScopeShared {
    fn lock(&self) -> MutexGuard<'_, ScopeWork> {
        // A poisoned lock means a panic while it was held; the map is a
        // valid value.
        self.work.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Removes its work from the run, and its poll start from the timed polls,
/// when the work's future ends or is dropped: at its end, at an abort, or at
/// a panic (a poll that panics leaves its start set; the removal drops it).
struct WorkEnded {
    shared: Arc<ScopeShared>,
    timing: PollTiming,
    id: u64,
}

impl Drop for WorkEnded {
    fn drop(&mut self) {
        match &self.timing {
            PollTiming::Untimed => {}
            PollTiming::Timed(polls) => polls.forget(self.id),
        }
        let _removed = self.shared.lock().alive.remove(&self.id);
        self.shared.ended.notify_waiters();
    }
}

/// What of a run is alive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Alive {
    Nothing,
    Work {
        count: NonZeroU64,
        names: Vec<LeftoverName>,
    },
}

/// The report of the work a run left after its body returned.
pub(super) fn left_report(count: NonZeroU64, names: Vec<LeftoverName>) -> LoopHoldsTasks {
    LoopHoldsTasks::new(count, names).expect("the names are the first min(count, 10) alive")
}

/// The report of work that did not stop after its cancel rounds.
pub(super) fn stuck_report(count: NonZeroU64, names: Vec<LeftoverName>) -> StuckWork {
    StuckWork::new(count.get(), 0, names).expect("the names are the first min(count, 10) alive")
}

/// Whether a wait saw every piece of work end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WorkWait {
    Done,
    StillAlive,
}

/// How the cancel rounds ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RoundsEnd {
    Ended,
    Stuck,
}

/// The work of one run, its cancel signal and its fencing token.
#[derive(Clone)]
pub(super) struct RunScope {
    shared: Arc<ScopeShared>,
    timing: PollTiming,
    cancel: RunCancelSignal,
    token: FencingToken,
}

impl RunScope {
    /// A run with the fencing token `token` and a new cancel signal, whose
    /// polls are not timed (an isolated child).
    pub(super) fn new(token: FencingToken) -> Self {
        Self::with_timing(PollTiming::Untimed, token, RunCancelSignal::new())
    }

    /// A run with the fencing token `token` and the cancel signal `cancel`,
    /// whose body and work are timed in `polls` (a shared async child). The
    /// seat makes the signal before the start write, so a `cancel_run` that
    /// comes before the body starts reaches it.
    pub(super) fn timed(polls: RunPolls, token: FencingToken, cancel: RunCancelSignal) -> Self {
        Self::with_timing(PollTiming::Timed(polls), token, cancel)
    }

    fn with_timing(timing: PollTiming, token: FencingToken, cancel: RunCancelSignal) -> Self {
        Self {
            shared: Arc::new(ScopeShared {
                work: Mutex::new(ScopeWork {
                    state: ScopeState::Open,
                    next_id: 0,
                    alive: BTreeMap::new(),
                }),
                ended: Notify::new(),
            }),
            timing,
            cancel,
            token,
        }
    }

    /// The fencing token of the run.
    pub(super) fn fencing_token(&self) -> FencingToken {
        self.token
    }

    /// The cancel signal of the run, for the thread of a blocking body.
    pub(super) fn cancel_signal(&self) -> RunCancelSignal {
        self.cancel.clone()
    }

    /// Whether the run's cancel signal fired, and its cause. Pure read.
    pub(super) fn cancel_state(&self) -> SignalState {
        self.cancel.state()
    }

    /// Fire the run's cancel signal with `cause`: its body and its work see
    /// it. A later fire keeps the first cause.
    pub(super) fn fire_cancel(&self, cause: CancelCause) -> Fire {
        self.cancel.fire(cause)
    }

    fn register(&self, name: &str) -> Result<u64, RunSpawnRefused> {
        let mut work = self.shared.lock();
        match (work.state, work.alive.len() >= MAX_RUN_WORK) {
            (ScopeState::Closed, _) => return Err(RunSpawnRefused::RunEnded),
            (ScopeState::Open, true) => {
                return Err(RunSpawnRefused::TooMuchWork {
                    bound: MAX_RUN_WORK,
                })
            }
            (ScopeState::Open, false) => {}
        }
        let id = work.next_id;
        work.next_id = work.next_id.saturating_add(1);
        work.alive.insert(
            id,
            AliveWork {
                name: LeftoverName::cut(name),
                abort: AbortSlot::Pending,
            },
        );
        Ok(id)
    }

    fn attach(&self, id: u64, handle: AbortHandle) {
        let mut work = self.shared.lock();
        let Some(alive) = work.alive.get_mut(&id) else {
            // The work ended before its handle came.
            return;
        };
        match alive.abort {
            AbortSlot::Pending => alive.abort = AbortSlot::Attached(handle),
            AbortSlot::AbortWhenAttached => {
                handle.abort();
                alive.abort = AbortSlot::Attached(handle);
            }
            AbortSlot::Attached(_) => unreachable!("work {id} got its abort handle twice"),
        }
    }

    /// Spawn registered work inside this run, so the work it starts in turn
    /// belongs to the run too. In a timed run each poll of the work is timed
    /// under `name`.
    fn spawn_registered<F>(&self, id: u64, name: &str, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let ended = WorkEnded {
            shared: Arc::clone(&self.shared),
            timing: self.timing.clone(),
            id,
        };
        let handle = match &self.timing {
            PollTiming::Untimed => tokio::spawn(RUN_SCOPE.scope(self.clone(), async move {
                let _ended = ended;
                future.await
            })),
            PollTiming::Timed(polls) => {
                let watched = Watched::new(future, polls.start_of(id, name), polls.epoch);
                tokio::spawn(RUN_SCOPE.scope(self.clone(), async move {
                    let _ended = ended;
                    watched.await
                }))
            }
        };
        self.attach(id, handle.abort_handle());
        handle
    }

    /// Spawn the body of a run inside it. A new run takes its body.
    pub(super) fn spawn_body<F>(&self, task_name: &str, body: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let id = self
            .register(task_name)
            .expect("a new run is open and holds no work");
        self.spawn_registered(id, task_name, body)
    }

    /// Refuse every later spawn of the run.
    pub(super) fn close(&self) {
        self.shared.lock().state = ScopeState::Closed;
    }

    /// The work of the run that is alive, and the names of the first ones.
    pub(super) fn alive(&self) -> Alive {
        let work = self.shared.lock();
        let Some(count) = u64::try_from(work.alive.len())
            .ok()
            .and_then(NonZeroU64::new)
        else {
            return Alive::Nothing;
        };
        let names = work
            .alive
            .values()
            .take(MAX_LEFTOVER_TASK_NAMES)
            .map(|alive| alive.name.clone())
            .collect();
        Alive::Work { count, names }
    }

    /// Abort every piece of work alive.
    fn abort_alive(&self) {
        let mut work = self.shared.lock();
        for alive in work.alive.values_mut() {
            match alive.abort {
                AbortSlot::Pending => alive.abort = AbortSlot::AbortWhenAttached,
                AbortSlot::AbortWhenAttached => {}
                AbortSlot::Attached(ref handle) => handle.abort(),
            }
        }
    }

    /// Wait until no work of the run is alive, at most `bound`.
    pub(super) async fn wait_done(&self, bound: Duration) -> WorkWait {
        let until = Instant::now() + bound;
        loop {
            let ended = self.shared.ended.notified();
            tokio::pin!(ended);
            // Registered before the check, so an end between the check and
            // the wait still wakes it.
            ended.as_mut().enable();
            if self.shared.lock().alive.is_empty() {
                return WorkWait::Done;
            }
            if tokio::time::timeout_at(until, ended).await.is_err() {
                return WorkWait::StillAlive;
            }
        }
    }

    /// Wait until no work of the run is alive, with no bound: the parent
    /// kills a child whose runs are all stuck.
    pub(super) async fn wait_all_ended(&self) {
        loop {
            let ended = self.shared.ended.notified();
            tokio::pin!(ended);
            ended.as_mut().enable();
            if self.shared.lock().alive.is_empty() {
                return;
            }
            ended.await;
        }
    }

    /// Abort the work alive, and again in [`EXTRA_CANCEL_ROUNDS`] more
    /// rounds [`CANCEL_ROUND_INTERVAL`] apart; work alive one interval
    /// after the last round is stuck.
    pub(super) async fn end_in_rounds(&self) -> RoundsEnd {
        self.abort_alive();
        for _ in 0..EXTRA_CANCEL_ROUNDS {
            if self.wait_done(CANCEL_ROUND_INTERVAL).await == WorkWait::Done {
                return RoundsEnd::Ended;
            }
            self.abort_alive();
        }
        match self.wait_done(CANCEL_ROUND_INTERVAL).await {
            WorkWait::Done => RoundsEnd::Ended,
            WorkWait::StillAlive => RoundsEnd::Stuck,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::AttemptNumber;

    /// The token of a run of attempt 1 of a new task.
    fn test_token() -> FencingToken {
        FencingToken::new(uuid::Uuid::new_v4(), AttemptNumber::SMALLEST)
    }

    #[tokio::test]
    async fn a_spawn_outside_a_run_is_refused() {
        assert!(matches!(
            spawn_in_run("orphan", async {}),
            Err(RunSpawnRefused::NotInARun)
        ));
    }

    /// Python `test_run_mark.py`: work the body starts, and the work that
    /// work starts, carries the run; it is left work when the body returns.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn work_a_body_starts_belongs_to_its_run_and_ends_in_rounds() {
        let scope = RunScope::new(test_token());
        #[expect(
            clippy::async_yields_async,
            reason = "the body returns the handle of the work it started; that work is left work"
        )]
        let body = scope.spawn_body("app.tasks.body", async {
            spawn_in_run("outer", async {
                spawn_in_run("inner", tokio::time::sleep(Duration::from_secs(60)))
                    .expect("work of a run may start work");
                tokio::time::sleep(Duration::from_secs(60)).await;
            })
            .expect("a body may start work")
        });
        let _outer = body.await.expect("the body ends");
        // The inner spawn runs when the outer work is first polled.
        let _ = scope.wait_done(Duration::from_millis(200)).await;
        match scope.alive() {
            Alive::Work { count, names } => {
                assert_eq!(count.get(), 2);
                let names: Vec<&str> = names.iter().map(LeftoverName::as_str).collect();
                assert_eq!(names, ["outer", "inner"]);
            }
            Alive::Nothing => panic!("the work the body started is alive"),
        }
        scope.close();
        assert_eq!(scope.end_in_rounds().await, RoundsEnd::Ended);
        assert_eq!(scope.alive(), Alive::Nothing);
        assert_eq!(
            scope.wait_done(Duration::from_millis(10)).await,
            WorkWait::Done
        );
    }

    #[tokio::test]
    async fn a_signal_read_outside_a_run_is_refused() {
        assert!(matches!(
            run_cancel_signal(),
            Err(RunSignalRefused::NotInARun)
        ));
    }

    /// #657: the cancel signal of a run reaches its body and the work the
    /// body started; a run that ends within its grace leaves nothing alive.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_cancel_signal_reaches_the_body_and_its_work() {
        let scope = RunScope::new(test_token());
        let body = scope.spawn_body("app.tasks.body", async {
            let signal = run_cancel_signal().expect("a body reads the signal of its run");
            let work = spawn_in_run("watcher", async {
                let signal = run_cancel_signal().expect("work of a run reads its signal");
                signal.cancelled().await;
                signal.is_cancelled()
            })
            .expect("a body may start work");
            let before = signal.is_cancelled();
            signal.cancelled().await;
            let seen_by_work = work.await.expect("the work ends");
            (before, signal.is_cancelled(), seen_by_work)
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(matches!(scope.alive(), Alive::Work { .. }));
        assert_eq!(scope.fire_cancel(CancelCause::Deadline), Fire::Won);
        assert_eq!(body.await.expect("the body ends"), (false, true, true));
        assert_eq!(
            scope.wait_done(Duration::from_secs(1)).await,
            WorkWait::Done
        );
    }

    /// #657: a signal that fired before the read completes at once, and a
    /// second fire changes nothing: the first cause stays.
    #[tokio::test]
    async fn a_fired_signal_completes_at_once() {
        let scope = RunScope::new(test_token());
        assert_eq!(scope.fire_cancel(CancelCause::Operator), Fire::Won);
        assert_eq!(scope.fire_cancel(CancelCause::Deadline), Fire::AlreadyFired);
        let body = scope.spawn_body("app.tasks.body", async {
            let signal = run_cancel_signal().expect("a body reads the signal of its run");
            signal.cancelled().await;
            (signal.is_cancelled(), run_cancel_state())
        });
        assert_eq!(
            body.await.expect("the body ends"),
            (
                true,
                RunCancelState::Requested {
                    cause: CancelCause::Operator
                }
            )
        );
        assert_eq!(
            scope.cancel_state(),
            SignalState::Fired {
                cause: CancelCause::Operator
            }
        );
    }

    /// The cancel state of a run that runs and has no cancel is
    /// `NotRequested`; outside a run it is `NotInARun`.
    #[tokio::test]
    async fn the_cancel_state_names_a_run_with_no_cancel_and_no_run() {
        assert_eq!(run_cancel_state(), RunCancelState::NotInARun);
        let scope = RunScope::new(test_token());
        let body = scope.spawn_body("app.tasks.body", async { run_cancel_state() });
        assert_eq!(
            body.await.expect("the body ends"),
            RunCancelState::NotRequested
        );
    }

    /// The deadline that comes after an operator cancel keeps the operator
    /// as the cause, and work of the run reads the same cause.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_first_cause_stays_for_the_body_and_its_work() {
        let scope = RunScope::new(test_token());
        let body = scope.spawn_body("app.tasks.body", async {
            let signal = run_cancel_signal().expect("a body reads the signal of its run");
            let work = spawn_in_run("watcher", async {
                run_cancel_signal()
                    .expect("work of a run reads its signal")
                    .cancelled()
                    .await;
                run_cancel_state()
            })
            .expect("a body may start work");
            signal.cancelled().await;
            (run_cancel_state(), work.await.expect("the work ends"))
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(scope.fire_cancel(CancelCause::Operator), Fire::Won);
        assert_eq!(scope.fire_cancel(CancelCause::Deadline), Fire::AlreadyFired);
        let operator = RunCancelState::Requested {
            cause: CancelCause::Operator,
        };
        assert_eq!(body.await.expect("the body ends"), (operator, operator));
    }

    /// #657: a body that does not end within the grace after the signal is
    /// dropped by the cancel rounds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_body_that_ignores_the_signal_is_dropped_after_the_grace() {
        let scope = RunScope::new(test_token());
        let _body = scope.spawn_body(
            "app.tasks.body",
            tokio::time::sleep(Duration::from_secs(60)),
        );
        assert_eq!(scope.fire_cancel(CancelCause::Deadline), Fire::Won);
        assert_eq!(
            scope.wait_done(Duration::from_millis(100)).await,
            WorkWait::StillAlive
        );
        scope.close();
        assert_eq!(scope.end_in_rounds().await, RoundsEnd::Ended);
        assert_eq!(scope.alive(), Alive::Nothing);
    }

    #[tokio::test]
    async fn a_closed_run_takes_no_new_work() {
        let scope = RunScope::new(test_token());
        scope.close();
        assert_eq!(scope.register("late"), Err(RunSpawnRefused::RunEnded));
    }

    #[tokio::test]
    async fn a_run_holds_a_bounded_amount_of_work() {
        let scope = RunScope::new(test_token());
        for _ in 0..MAX_RUN_WORK {
            scope.register("work").expect("under the bound");
        }
        assert_eq!(
            scope.register("one more"),
            Err(RunSpawnRefused::TooMuchWork {
                bound: MAX_RUN_WORK
            })
        );
    }

    /// Work that holds a runtime thread in one poll cannot be aborted: it
    /// is stuck after the rounds, and the run waits for its end.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn work_that_holds_its_thread_is_stuck_after_the_rounds() {
        let scope = RunScope::new(test_token());
        let started = Instant::now();
        #[expect(
            clippy::async_yields_async,
            reason = "the body returns the handle of the work it started; that work is left work"
        )]
        let body = scope.spawn_body("app.tasks.body", async {
            spawn_in_run("holds", async {
                std::thread::sleep(Duration::from_secs(2));
            })
            .expect("a body may start work")
        });
        let _held = body.await.expect("the body ends");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(scope.end_in_rounds().await, RoundsEnd::Stuck);
        match scope.alive() {
            Alive::Work { count, names } => {
                let stuck = stuck_report(count, names);
                assert_eq!(stuck.tasks(), 1);
                assert_eq!(stuck.executor_items(), 0);
            }
            Alive::Nothing => panic!("the held work is alive"),
        }
        scope.wait_all_ended().await;
        assert!(started.elapsed() >= Duration::from_secs(2));
    }

    /// #875: in a timed run, a piece of work started with `spawn_in_run`
    /// has its own poll start under the run, named by the work, while it
    /// holds a runtime thread; it leaves the polls when it ends.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn work_of_a_timed_run_is_timed_under_the_run_until_it_ends() {
        let polls = RunPolls::new(std::time::Instant::now());
        let scope = RunScope::timed(polls.clone(), test_token(), RunCancelSignal::new());
        #[expect(
            clippy::async_yields_async,
            reason = "the body returns the handle of the work it started; that work is left work"
        )]
        let body = scope.spawn_body("app.tasks.body", async {
            spawn_in_run("holds", async {
                std::thread::sleep(Duration::from_millis(400));
            })
            .expect("a body may start work")
        });
        let _held = body.await.expect("the body ends");
        tokio::time::sleep(Duration::from_millis(150)).await;

        let now = millis_since(polls.epoch).saturating_add(1);
        match polls.longest_poll(now) {
            PollsNow::Longest {
                blocked_ms,
                location,
            } => {
                assert_eq!(location.as_str(), "holds");
                assert!(blocked_ms >= 100, "the poll started {blocked_ms} ms ago");
            }
            PollsNow::NoPollInProgress => panic!("the work holds its thread in one poll"),
        }

        assert_eq!(
            scope.wait_done(Duration::from_secs(2)).await,
            WorkWait::Done
        );
        let now = millis_since(polls.epoch).saturating_add(1);
        assert_eq!(polls.longest_poll(now), PollsNow::NoPollInProgress);
        assert!(
            polls.lock().is_empty(),
            "each future left the polls at its end"
        );
    }

    /// An untimed run (an isolated child) keeps no poll starts.
    #[tokio::test]
    async fn an_untimed_run_times_nothing() {
        let scope = RunScope::new(test_token());
        assert!(matches!(scope.timing, PollTiming::Untimed));
        let body = scope.spawn_body("app.tasks.body", async { 1 });
        assert_eq!(body.await.expect("the body ends"), 1);
    }

    #[test]
    fn a_report_names_the_first_ten_pieces_of_work() {
        let names: Vec<LeftoverName> = (0..10)
            .map(|n| LeftoverName::cut(&format!("w{n}")))
            .collect();
        let left = left_report(NonZeroU64::new(25).unwrap(), names.clone());
        assert_eq!(left.count().get(), 25);
        assert_eq!(left.names().len(), 10);
        assert_eq!(
            stuck_report(NonZeroU64::new(25).unwrap(), names).tasks(),
            25
        );
    }

    #[tokio::test]
    async fn a_token_read_outside_a_run_is_refused() {
        assert_eq!(run_fencing_token(), Err(FencingTokenRefused::NotInARun));
    }

    /// Two reads in one run give the token of the run, and so does work
    /// that the body started with `spawn_in_run`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_body_and_its_work_read_the_token_of_the_run() {
        let token = FencingToken::new(
            uuid::Uuid::new_v4(),
            AttemptNumber::try_from(2).expect("2 is an attempt"),
        );
        let scope = RunScope::new(token);
        let body = scope.spawn_body("app.tasks.body", async {
            let first = run_fencing_token();
            let second = run_fencing_token();
            let work = spawn_in_run("reader", async { run_fencing_token() })
                .expect("a body may start work");
            let from_work = work.await.expect("the work ends");
            (first, second, from_work)
        });
        let (first, second, from_work) = body.await.expect("the body ends");
        assert_eq!(first, Ok(token));
        assert_eq!(second, Ok(token));
        assert_eq!(from_work, Ok(token));
        assert_eq!(scope.fencing_token(), token);
    }

    /// Work that the body starts with plain `tokio::spawn` is not in the run,
    /// so it reads no token.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn work_started_with_plain_tokio_spawn_reads_no_token() {
        let scope = RunScope::new(test_token());
        let body = scope.spawn_body("app.tasks.body", async {
            tokio::spawn(async { run_fencing_token() })
                .await
                .expect("the work ends")
        });
        assert_eq!(
            body.await.expect("the body ends"),
            Err(FencingTokenRefused::NotInARun)
        );
    }

    /// Two runs at once read their own tokens.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_runs_at_once_read_their_own_tokens() {
        let first = RunScope::new(test_token());
        let second = RunScope::new(test_token());
        let first_body = first.spawn_body("app.tasks.first", async { run_fencing_token() });
        let second_body = second.spawn_body("app.tasks.second", async { run_fencing_token() });
        assert_eq!(
            first_body.await.expect("the body ends"),
            Ok(first.fencing_token())
        );
        assert_eq!(
            second_body.await.expect("the body ends"),
            Ok(second.fencing_token())
        );
        assert_ne!(first.fencing_token(), second.fencing_token());
    }

    /// The thread of a blocking body has the token while the body runs; the
    /// token goes with the end of the body, also with a panic, and never
    /// reaches a thread that the body starts.
    #[test]
    fn a_blocking_body_has_the_token_on_its_thread_while_it_runs() {
        let token = test_token();
        let signal = RunCancelSignal::new();
        assert_eq!(run_fencing_token(), Err(FencingTokenRefused::NotInARun));
        let inside = in_blocking_run(token, signal.clone(), || {
            (run_fencing_token(), run_fencing_token())
        });
        assert_eq!(inside, (Ok(token), Ok(token)));
        assert_eq!(run_fencing_token(), Err(FencingTokenRefused::NotInARun));
        let other_thread = in_blocking_run(token, signal.clone(), || {
            std::thread::spawn(run_fencing_token)
                .join()
                .expect("the thread ends")
        });
        assert_eq!(other_thread, Err(FencingTokenRefused::NotInARun));
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            in_blocking_run(token, signal.clone(), || panic!("body panic"))
        }));
        assert!(panicked.is_err());
        assert_eq!(run_fencing_token(), Err(FencingTokenRefused::NotInARun));
    }

    /// A blocking body reads the cancel state of its run on its thread: it
    /// sees an operator cancel that fires while it runs, and a thread that
    /// it starts is not in the run.
    #[test]
    fn a_blocking_body_reads_an_operator_cancel_on_its_thread() {
        let signal = RunCancelSignal::new();
        assert_eq!(run_cancel_state(), RunCancelState::NotInARun);
        let (before, after, other_thread) = in_blocking_run(test_token(), signal.clone(), || {
            let before = run_cancel_state();
            assert_eq!(signal.fire(CancelCause::Operator), Fire::Won);
            let after = run_cancel_state();
            let other_thread = std::thread::spawn(run_cancel_state)
                .join()
                .expect("the thread ends");
            (before, after, other_thread)
        });
        assert_eq!(before, RunCancelState::NotRequested);
        assert_eq!(
            after,
            RunCancelState::Requested {
                cause: CancelCause::Operator
            }
        );
        assert_eq!(other_thread, RunCancelState::NotInARun);
        assert!(run_cancel_signal().is_err());
        assert_eq!(run_cancel_state(), RunCancelState::NotInARun);
    }

    /// The signal that a blocking body reads is the signal of its run: a fire
    /// from another thread reaches it.
    #[test]
    fn a_blocking_body_sees_a_fire_from_another_thread() {
        let signal = RunCancelSignal::new();
        let firer = signal.clone();
        let seen = in_blocking_run(test_token(), signal, move || {
            let fired = std::thread::spawn(move || firer.fire(CancelCause::Deadline))
                .join()
                .expect("the thread ends");
            assert_eq!(fired, Fire::Won);
            let read = run_cancel_signal().expect("a blocking body reads its signal");
            (read.is_cancelled(), run_cancel_state())
        });
        assert_eq!(
            seen,
            (
                true,
                RunCancelState::Requested {
                    cause: CancelCause::Deadline
                }
            )
        );
    }
}

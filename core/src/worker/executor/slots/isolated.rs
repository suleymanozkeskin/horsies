//! One isolated slot: boot a child, serve tasks on it one at a time, hold
//! the deadline of each, recycle or replace the child.
//!
//! Rules of one task:
//! - The deadline starts when the run frame is sent. At the deadline the
//!   child is killed with its session and the end is `TimedOut` once its
//!   exit is seen; a result after the deadline is never read.
//! - A body that returned with work left on its loop keeps what it
//!   returned: the deadline no longer applies, and every later end of the
//!   child whose exit is seen writes what the body returned.
//! - A child lost with its exit seen takes the crash path.
//! - A result frame that says the child ends itself (its core stopped)
//!   answers the task, and the slot sends that child no other task: it
//!   waits for the exit of the child, then boots a new one.
//! - A child whose exit is not seen 30 s after the kill: the end is not
//!   known (stale recovery on), or the slot waits for the exit (off).
//! - The runner beats of the task stop when its end is read or a kill is
//!   sent. A closed runner beat registry (the worker failed closed) refuses
//!   the task before it is sent: it ends `NotDispatched`, and the child is
//!   killed.
//! - A slot defect (a run frame that cannot be encoded, a panic of the slot
//!   task) kills the child; its task takes the crash path once the exit is
//!   seen (what the body returned, after a return). Repeated defects fail
//!   the pools ([`super::defects`]).
//! - An operator cancel routed to the run ([`CancelBell`]) sends
//!   `cancel_run` to the child. The child ends the run as cancelled
//!   (`run_cancelled`), and the end is `Cancelled`: the cancel statement
//!   recorded the attempt, and nothing is written. The deadline stays a
//!   bound; with [`IsolatedCancelStop::KillAfterGrace`] the slot also kills
//!   the child once the grace after the cancel passed. A child killed or
//!   lost after the cancel ends the run as `Cancelled` too, whether its exit
//!   was seen or not: no write depends on it. Work that a cancelled body
//!   left on its loop retires the child. A result that the child sent before
//!   it read the cancel is the end, as without a cancel; its write finds the
//!   row moved.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use super::board::SlotPhase;
use super::booting::{boot_child, lost_after_kill, BootSlot, Booted};
use super::defects::{
    count_defect, AfterDefect, CatchPanic, GenerationEnd, Served, SlotDefect, SlotDefects,
    TasksFinished,
};
use super::dispatch::{
    AttemptEnd, AttemptLog, BodyReturn, CancelledEnd, NotRunCause, SecondCancelAfterSignal,
    TimeoutCancel, TimeoutSeen,
};
use super::pool::{Answer, ExitNotSeen, MemoryLimit, PoolShared, QueuedRun, TaskLimit};
use super::replacement::{reason_of_loss, tell_replaced, ChildLeave, GenerationChild};
use super::route::{IsolatedCancelStop, IsolatedDeadline};
use super::seat_runs::SHARED_BACKSTOP_MARGIN;
use crate::broker::attempt_writes::RetryPlacement;
use crate::broker::child_writes::{longer, FINALIZING_MARKER_END};
use crate::db::CANCEL_REQUEST_BOUND;
use crate::events::AttemptStart;
use crate::telemetry::labels::ReplaceReason;
use crate::worker::executor::protocol::{
    CancelTarget, CancelledLoop, ChildAfterFrame, ChildFrame, IsolatedWork, ParentFrame, ProcessId,
    RssReading, RunCancelledFrame, RunFrame, TaskTimeoutMs, ISOLATED_LEFTOVER_GRACE,
    STUCK_REPORT_DELAY,
};
use crate::worker::executor::supervisor::{
    log_core_stopped_child, ChildChannel, ChildExit, ChildLost, EndWithoutTask, ExitState,
    FrameWait, LossCause, MailboxBound, SendFailure, CHANNEL_LOST_GRACE,
};
use crate::worker::runner_beats::{
    CancelBell, CancelTake, HeldChild, HeldDispatch, RegistryClosed,
};

/// The longest wait of the finalizing marker of a version-1 Python isolated
/// child (the reference copy of the whole-run pairs) after a body returned
/// with work left, for a pooled connection (Python
/// `LEFT_WORK_MARKER_CONNECTION_WAIT_S` of that copy).
pub const LEFT_WORK_MARKER_CONNECTION_WAIT: Duration = Duration::from_secs(5);

/// The longest time of the statement of that marker once it holds its
/// connection (Python `LEFT_WORK_MARKER_STATEMENTS_S` of that copy).
pub const LEFT_WORK_MARKER_STATEMENTS: Duration = Duration::from_secs(5);

/// The longest time of that marker: the wait for a connection, the
/// statement, and the cancel request of a statement past its bound (Python
/// `LEFT_WORK_MARKER_BOUND_S` of that copy, with `CANCEL_REQUEST_TIMEOUT_S`).
pub const LEFT_WORK_MARKER_END: Duration = LEFT_WORK_MARKER_CONNECTION_WAIT
    .saturating_add(LEFT_WORK_MARKER_STATEMENTS)
    .saturating_add(CANCEL_REQUEST_BOUND);

/// After a body returned with work left on its loop, the longest time until
/// a version-1 Python isolated child (the reference copy of the whole-run
/// pairs) reports how that work ended: the grace
/// ([`ISOLATED_LEFTOVER_GRACE`], 1 s), the cancel rounds
/// ([`STUCK_REPORT_DELAY`], 0.3 s) and its marker
/// ([`LEFT_WORK_MARKER_END`], 15 s): 16.3 s.
pub const PYTHON_CHILD_LEFTOVER_END: Duration = ISOLATED_LEFTOVER_GRACE
    .saturating_add(STUCK_REPORT_DELAY)
    .saturating_add(LEFT_WORK_MARKER_END);

/// After a body returned with work left on its loop, the longest time until
/// a core child or a version-2 Python child reports how that work ended: the
/// grace, the cancel rounds and its finalizing marker, which goes through the
/// core ([`FINALIZING_MARKER_END`], 20 s): 21.3 s.
pub const CORE_CHILD_LEFTOVER_END: Duration = ISOLATED_LEFTOVER_GRACE
    .saturating_add(STUCK_REPORT_DELAY)
    .saturating_add(FINALIZING_MARKER_END);

/// The longest time until a child of any kind reports how the work left
/// after a return ended: the longer of [`PYTHON_CHILD_LEFTOVER_END`] and
/// [`CORE_CHILD_LEFTOVER_END`].
pub const LONGEST_LEFTOVER_END: Duration =
    longer(PYTHON_CHILD_LEFTOVER_END, CORE_CHILD_LEFTOVER_END);

/// After a body returned with work left on its loop, the child reports how
/// that work ended within this time, or it is killed: the longest end of a
/// child of any kind ([`LONGEST_LEFTOVER_END`], 21.3 s) and the margin
/// ([`SHARED_BACKSTOP_MARGIN`], 10 s): 31.3 s. The backstop never fires
/// before a child's own report, as the refusal backstop never fires before
/// a child's own refusal. Python `ISOLATED_LEFTOVER_BACKSTOP_S` of the
/// reference copy (26.3 s) counts only the version-1 Python child, the one
/// kind that its Python parent runs.
pub const ISOLATED_LEFTOVER_BACKSTOP: Duration =
    LONGEST_LEFTOVER_END.saturating_add(SHARED_BACKSTOP_MARGIN);

/// What the slot does after one task.
enum AfterRun {
    /// The task finished; the child serves on, and `rss` decides a
    /// recycle.
    Serve { rss: RssReading },
    /// The task finished, and the child stops after a clean end (left work
    /// was cancelled in it).
    Retire,
    /// The task finished, and its result frame said that the child ends
    /// itself (its core stopped): the slot sends it no task and waits for
    /// its exit.
    EndsItself,
    /// The child is lost or killed for `reason`. The task finished when
    /// its body returned before.
    ChildGone {
        finished: TasksFinished,
        reason: ReplaceReason,
    },
    /// A slot defect: the child is killed and the task answered.
    Defect(SlotDefect),
}

/// Whether the body of the task returned with work left on its loop
/// (Python executor design, section 8.5).
enum IsolatedReturn {
    NotSeen,
    /// What the body returned: the end at every later end of the child
    /// whose exit is seen.
    Seen(AttemptEnd),
}

/// Whether the slot sent `cancel_run` for the task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelSent {
    NotSent,
    Sent,
}

/// The answer of the task on the child, kept by the slot outside the code
/// that runs the task, so that a defect in that code still answers it
/// (Python `SlotHandle.finish`, `loss_outcome`).
struct TaskAnswer {
    answer: Option<Answer>,
    returned: IsolatedReturn,
    cancel: CancelSent,
}

impl TaskAnswer {
    fn new(answer: Answer) -> Self {
        Self {
            answer: Some(answer),
            returned: IsolatedReturn::NotSeen,
            cancel: CancelSent::NotSent,
        }
    }

    /// The slot sent `cancel_run` for the task: an end at a loss or a
    /// defect is `Cancelled`, unless the body returned first.
    fn cancel_sent(&mut self) {
        self.cancel = CancelSent::Sent;
    }

    fn child_took_it(&mut self, pid: ProcessId) {
        if let Some(answer) = &mut self.answer {
            answer.child_took_it(pid);
        }
    }

    /// Answer the task. A task is answered once; a second answer is a
    /// defect of the slot.
    fn send(&mut self, end: AttemptEnd) {
        match self.answer.take() {
            Some(answer) => answer.send(end),
            None => panic!("a task of an isolated slot was answered twice"),
        }
    }

    /// The body returned `end` with work left on its loop. A second return
    /// is a defect of the slot.
    fn body_returned(&mut self, end: AttemptEnd) {
        match self.returned {
            IsolatedReturn::NotSeen => self.returned = IsolatedReturn::Seen(end),
            IsolatedReturn::Seen(_) => panic!("the return of a task was noted twice"),
        }
    }

    /// The answer, when it is still open, and the end of the task once the
    /// exit of its child, lost by `lost`, is seen: what the body returned,
    /// else the end of a cancelled run killed by the worker, else the crash
    /// path.
    fn open_at_loss(&mut self, lost: &ChildLost) -> Option<(Answer, AttemptEnd)> {
        let answer = self.answer.take()?;
        let seen_end = match (&self.returned, self.cancel) {
            (IsolatedReturn::Seen(returned), _) => returned.clone(),
            (IsolatedReturn::NotSeen, CancelSent::Sent) => {
                AttemptEnd::Cancelled(CancelledEnd::ChildKilled)
            }
            (IsolatedReturn::NotSeen, CancelSent::NotSent) => AttemptEnd::of_lost_child(lost),
        };
        Some((answer, seen_end))
    }
}

/// Whether a slot waiting for work tells the claim loop that its child is
/// ready (#1148).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadyNotice {
    /// The first wait of a generation: the child is new, so no claim pass
    /// counted it. The wait notes it on the board once.
    AtFirstWait,
    /// A later wait: the end of the task before it already woke the claim
    /// loop (the outcome write).
    Quiet,
}

/// What came first for a task in flight.
enum FirstAnswer {
    Wait(FrameWait),
    DrainDeadline,
}

/// What came first for a task in flight whose slot watches for an operator
/// cancel.
enum RunWait {
    Answer(FirstAnswer),
    /// An operator cancel was routed to the run, and the slot took it.
    CancelRequested,
}

/// Whether the slot watches for an operator cancel of the run: only a run
/// of task code; a refusal of the stored options runs none.
#[derive(Clone, Copy)]
enum CancelWatch<'a> {
    Watching(&'a CancelBell, &'a HeldDispatch),
    NotWatching,
}

/// The bound of the wait after `cancel_run`: the deadline of the run, or
/// the end of the grace of its cancel stop, whichever comes first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelBound {
    /// Neither bound: the run has no deadline and no kill for the cancel.
    Unbounded,
    Deadline(Instant),
    GraceEnd(Instant),
}

impl CancelBound {
    /// The earlier of `deadline` and the end of the grace of `cancel_stop`
    /// counted from `sent_at`. Pure.
    fn of(deadline: Option<Instant>, cancel_stop: IsolatedCancelStop, sent_at: Instant) -> Self {
        let grace_end = match cancel_stop {
            IsolatedCancelStop::CooperativeOnly => None,
            IsolatedCancelStop::KillAfterGrace(grace) => {
                Some(sent_at + Duration::from_millis(grace.get()))
            }
        };
        match (deadline, grace_end) {
            (None, None) => Self::Unbounded,
            (Some(deadline), None) => Self::Deadline(deadline),
            (None, Some(grace_end)) => Self::GraceEnd(grace_end),
            (Some(deadline), Some(grace_end)) => match grace_end < deadline {
                true => Self::GraceEnd(grace_end),
                false => Self::Deadline(deadline),
            },
        }
    }

    fn until(self) -> Option<Instant> {
        match self {
            Self::Unbounded => None,
            Self::Deadline(at) | Self::GraceEnd(at) => Some(at),
        }
    }

    /// The replace reason of the kill at this bound.
    fn replace_reason(self) -> ReplaceReason {
        match self {
            Self::Deadline(_) | Self::Unbounded => ReplaceReason::AtDeadline,
            Self::GraceEnd(_) => ReplaceReason::KilledByParent,
        }
    }
}

/// The slot task: boot a child, serve it, replace it, until the pools stop
/// or fail. `index` is the slot's index on the board. A defect costs the
/// child and its task; [`SLOT_DEFECT_LIMIT`] defects with no finished task
/// between them fail the pools.
///
/// [`SLOT_DEFECT_LIMIT`]: super::defects::SLOT_DEFECT_LIMIT
pub(super) async fn run_slot(shared: Arc<PoolShared>, index: usize) {
    let mut defects = SlotDefects::default();
    loop {
        let mut child = GenerationChild::NotBooted;
        let served =
            CatchPanic::new(boot_and_serve(&shared, index, &mut defects, &mut child)).await;
        let (defect, leave) = match served {
            Ok(Served::PoolEnded) => return,
            Ok(Served::Generation(GenerationEnd::Ended(leave))) => (None, leave),
            Ok(Served::Generation(GenerationEnd::Defect(defect))) => {
                (Some(defect), ChildLeave::For(ReplaceReason::SlotDefect))
            }
            Err(payload) => {
                // The child of the generation was dropped with it; its
                // channel kills it on drop.
                shared.board.set(index, SlotPhase::Retiring);
                (
                    Some(SlotDefect::panicked(payload.as_ref())),
                    ChildLeave::For(ReplaceReason::SlotDefect),
                )
            }
        };
        if let Some(defect) = defect {
            match count_defect(&shared.board, index, &mut defects, defect) {
                AfterDefect::BootAgain => {}
                AfterDefect::PoolFailed => return,
            }
        }
        match shared.board.not_serving() {
            true => return,
            false => {
                tell_replaced(&shared.board, index, child, leave);
                shared.board.set(index, SlotPhase::Booting);
            }
        }
    }
}

/// Boot a child for the slot and serve it. `child` names the booted child
/// once the boot gave one.
async fn boot_and_serve(
    shared: &PoolShared,
    index: usize,
    defects: &mut SlotDefects,
    child: &mut GenerationChild,
) -> Served {
    let boot = BootSlot {
        board: &shared.board,
        index,
        source: shared.source.as_ref(),
        spawner: &shared.spawner,
        command: &shared.config.command,
        init: &shared.config.init,
        mailbox: MailboxBound::for_seats(NonZeroUsize::MIN),
        cpus: &shared.config.child_cpus,
        kill_wait: shared.config.kill_wait,
    };
    match boot_child(&boot).await {
        Booted::Child(channel) => {
            *child = GenerationChild::Booted(channel.pid());
            Served::Generation(serve_generation(shared, index, channel, defects).await)
        }
        Booted::PoolStopped | Booted::PoolFailed => Served::PoolEnded,
    }
}

/// How a child that takes no more tasks leaves the slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChildLeaves {
    /// The slot retires the child for `ReplaceReason` with the stop frame,
    /// or disposes of a child that already ended.
    Retired(ReplaceReason),
    /// The child said that it ends itself: the slot waits for its exit.
    EndsItself,
    /// The slot waited for work, and the child takes none.
    Idle(IdleEnd),
}

/// Why a child that waits for work takes none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdleEnd {
    /// The pools stop or failed.
    PoolNotServing,
    /// The child exited, or its channel closed, while it held no task.
    ChildEnded,
    /// The exit watch of the child closed: no exit of it can be seen.
    ExitWatchClosed,
}

/// What the wait for work gave.
enum NextRun {
    Run(Box<QueuedRun>),
    /// The child takes no task.
    NoRun(IdleEnd),
}

/// Whether a child that served a task serves on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AfterTask {
    ServeOn,
    /// The child reached a recycle limit: `ReplaceReason::TaskCount` or
    /// `ReplaceReason::MemoryLimit`.
    Recycle(ReplaceReason),
}

/// Serve tasks on one child until it is lost, retires, ends itself, or the
/// pool stops. A panic while a task runs is a defect: the child is killed
/// and the task answered here.
async fn serve_generation(
    shared: &PoolShared,
    index: usize,
    mut channel: ChildChannel,
    defects: &mut SlotDefects,
) -> GenerationEnd {
    let mut tasks_done: u64 = 0;
    let mut notice = ReadyNotice::AtFirstWait;
    let leaves = loop {
        let run = match next_run(shared, &channel, notice).await {
            NextRun::Run(run) => *run,
            NextRun::NoRun(idle) => break ChildLeaves::Idle(idle),
        };
        notice = ReadyNotice::Quiet;
        tasks_done = tasks_done.saturating_add(1);
        let QueuedRun {
            frame,
            deadline,
            cancel_stop,
            answer,
            log,
        } = run;
        let mut answer = TaskAnswer::new(answer);
        let ran = CatchPanic::new(run_one(
            shared,
            &mut channel,
            frame,
            log,
            RunBounds {
                deadline,
                cancel_stop,
            },
            &mut answer,
        ))
        .await;
        let after = match ran {
            Ok(after) => after,
            Err(payload) => {
                let defect = SlotDefect::panicked(payload.as_ref());
                end_at_defect(shared, &mut channel, &mut answer).await;
                AfterRun::Defect(defect)
            }
        };
        match after {
            AfterRun::Serve { rss } => {
                defects.task_finished();
                shared.board.note_rss(index, channel.pid(), rss);
                match after_task(shared, tasks_done, rss) {
                    AfterTask::ServeOn => {}
                    AfterTask::Recycle(reason) => {
                        tracing::debug!(
                            slot = index,
                            pid = channel.pid().get(),
                            reason = ?reason,
                            "retiring a child"
                        );
                        break ChildLeaves::Retired(reason);
                    }
                }
            }
            AfterRun::Retire => {
                defects.task_finished();
                break ChildLeaves::Retired(ReplaceReason::LeftoverWork);
            }
            AfterRun::EndsItself => {
                defects.task_finished();
                break ChildLeaves::EndsItself;
            }
            AfterRun::ChildGone { finished, reason } => {
                defects.note(finished);
                shared.board.set(index, SlotPhase::Retiring);
                let _ = channel.dispose_lost().await;
                return GenerationEnd::Ended(ChildLeave::For(reason));
            }
            AfterRun::Defect(defect) => {
                shared.board.set(index, SlotPhase::Retiring);
                let _ = channel.dispose_lost().await;
                return GenerationEnd::Defect(defect);
            }
        }
    };
    shared.board.set(index, SlotPhase::Retiring);
    GenerationEnd::Ended(leave_slot(channel, leaves).await)
}

/// End a child that takes no more tasks as `leaves` says, and give why it
/// left.
async fn leave_slot(mut channel: ChildChannel, leaves: ChildLeaves) -> ChildLeave {
    let pid = channel.pid();
    let exited = channel.has_exited();
    let ended = exited || channel.channel_closed();
    match (leaves, ended) {
        (ChildLeaves::EndsItself, _) => {
            log_end_without_task(pid, channel.await_own_end().await);
            ChildLeave::For(ReplaceReason::CoreStopped)
        }
        (ChildLeaves::Retired(reason), true) => {
            // The child ended by itself while it held no task.
            log_end_without_task(pid, channel.dispose_lost().await);
            ChildLeave::For(reason)
        }
        (ChildLeaves::Retired(reason), false) => {
            let _ = channel.retire().await;
            ChildLeave::For(reason)
        }
        (ChildLeaves::Idle(idle), true) => {
            // The child ended by itself while it held no task. A channel
            // that closed before the exit was seen waits for the exit as
            // long as the channel's own rule for a closed channel
            // ([`CHANNEL_LOST_GRACE`]), so a child that died is told apart
            // from a channel lost while its child lived.
            let exited = match (idle, exited) {
                (IdleEnd::ChildEnded, false) => {
                    let _ = channel.wait_exit(CHANNEL_LOST_GRACE).await;
                    channel.has_exited()
                }
                (IdleEnd::ChildEnded, true)
                | (IdleEnd::PoolNotServing | IdleEnd::ExitWatchClosed, _) => exited,
            };
            let exit = channel.dispose_lost().await;
            log_end_without_task(pid, exit);
            idle_leave(idle, exited, exit)
        }
        (ChildLeaves::Idle(idle), false) => {
            let exit = channel.retire().await;
            idle_leave(idle, exited, exit)
        }
    }
}

/// Why a child that held no task left the slot: the stop of the pools, its
/// own end, or a closed exit watch. `exited` says whether its exit was seen
/// before the slot disposed of it, and `exit` is its exit. Pure.
fn idle_leave(idle: IdleEnd, exited: bool, exit: ChildExit) -> ChildLeave {
    match (idle, EndWithoutTask::of(exit), exited) {
        (IdleEnd::PoolNotServing, _, _) => ChildLeave::AtPoolStop,
        (IdleEnd::ExitWatchClosed, _, _) => ChildLeave::For(ReplaceReason::SpawnerStopped),
        (IdleEnd::ChildEnded, EndWithoutTask::CoreStopped, _) => {
            ChildLeave::For(ReplaceReason::CoreStopped)
        }
        (IdleEnd::ChildEnded, EndWithoutTask::MemoryLimit, _) => {
            ChildLeave::For(ReplaceReason::MemoryLimit)
        }
        (IdleEnd::ChildEnded, EndWithoutTask::Other, true) => {
            ChildLeave::For(ReplaceReason::ChildDied)
        }
        (IdleEnd::ChildEnded, EndWithoutTask::Other, false) => {
            ChildLeave::For(ReplaceReason::ChannelLost)
        }
    }
}

/// Log the end of a child that held no task when it ended with the
/// core-stopped code. Every other end needs no log of its own.
fn log_end_without_task(pid: ProcessId, exit: ChildExit) {
    match EndWithoutTask::of(exit) {
        EndWithoutTask::CoreStopped => log_core_stopped_child(pid),
        EndWithoutTask::MemoryLimit | EndWithoutTask::Other => {}
    }
}

/// Whether a child that served `tasks_done` tasks, with the RSS `rss` at
/// its last result, serves on or retires at a recycle limit. Pure.
fn after_task(shared: &PoolShared, tasks_done: u64, rss: RssReading) -> AfterTask {
    let count_reached = match shared.config.tasks_per_child {
        TaskLimit::Off => false,
        TaskLimit::After(limit) => tasks_done >= limit.get(),
    };
    let memory_reached = match (shared.config.memory_per_child, rss) {
        (MemoryLimit::AtMb(limit), RssReading::Mb(rss)) => {
            // The limit is a whole number of MiB; the RSS a finite number.
            rss.get() >= limit.get() as f64
        }
        (MemoryLimit::Off, _) | (MemoryLimit::AtMb(_), RssReading::Unreadable) => false,
    };
    match (count_reached, memory_reached) {
        (true, _) => AfterTask::Recycle(ReplaceReason::TaskCount),
        (false, true) => AfterTask::Recycle(ReplaceReason::MemoryLimit),
        (false, false) => AfterTask::ServeOn,
    }
}

/// Wait for work. [`NextRun::NoRun`]: this child must not take any, and
/// why. While it waits, the slot counts as idle in the free slots of its
/// pool. With [`ReadyNotice::AtFirstWait`], the first time the slot waits
/// with no task queued it notes the ready child on the board, after it
/// counts as idle: the claim pass that the note wakes counts this slot as
/// free.
async fn next_run(shared: &PoolShared, channel: &ChildChannel, mut notice: ReadyNotice) -> NextRun {
    let mut exit = channel.exit_watch();
    let mut idle = shared.idle_slot();
    loop {
        let child_ended = channel.has_exited() || channel.channel_closed();
        match (shared.board.not_serving(), child_ended) {
            (true, _) => return NextRun::NoRun(IdleEnd::PoolNotServing),
            (false, true) => return NextRun::NoRun(IdleEnd::ChildEnded),
            (false, false) => {}
        }
        if let Some(run) = idle.take() {
            return NextRun::Run(Box::new(run));
        }
        match notice {
            ReadyNotice::AtFirstWait => {
                shared.board.note_child_ready();
                notice = ReadyNotice::Quiet;
            }
            ReadyNotice::Quiet => {}
        }
        tokio::select! {
            () = shared.queued.notified() => {}
            () = shared.board.stop.cancelled() => {}
            () = shared.board.failed.cancelled() => {}
            changed = exit.changed() => {
                if changed.is_err() {
                    return NextRun::NoRun(IdleEnd::ExitWatchClosed);
                }
            }
        }
    }
}

/// The first answer to a task in flight, bounded by its deadline and by the
/// drain deadline of the pool.
async fn first_answer(
    shared: &PoolShared,
    channel: &mut ChildChannel,
    until: Option<Instant>,
) -> FirstAnswer {
    let bound = until.map(|until| until.saturating_duration_since(Instant::now()));
    tokio::select! {
        wait = channel.await_frame(bound) => FirstAnswer::Wait(wait),
        () = shared.board.drain_deadline.cancelled() => FirstAnswer::DrainDeadline,
    }
}

/// As [`first_answer`], and an operator cancel of the run when `watch` names
/// its bell: a ring whose cancel the slot takes ends the wait. A ring with no
/// cancel to take (the registry closed since) waits again.
async fn first_answer_or_cancel(
    shared: &PoolShared,
    channel: &mut ChildChannel,
    until: Option<Instant>,
    watch: CancelWatch<'_>,
) -> RunWait {
    let (bell, held) = match watch {
        CancelWatch::Watching(bell, held) => (bell, held),
        CancelWatch::NotWatching => {
            return RunWait::Answer(first_answer(shared, channel, until).await)
        }
    };
    loop {
        let bound = until.map(|until| until.saturating_duration_since(Instant::now()));
        let rung = tokio::select! {
            wait = channel.await_frame(bound) => return RunWait::Answer(FirstAnswer::Wait(wait)),
            () = shared.board.drain_deadline.cancelled() => {
                return RunWait::Answer(FirstAnswer::DrainDeadline)
            }
            () = bell.rung() => held.take_cancel(),
        };
        match rung {
            CancelTake::Requested => return RunWait::CancelRequested,
            CancelTake::NoRequest => {}
        }
    }
}

/// The bounds that the parent holds for one isolated run: its deadline, and
/// the stop of an operator cancel.
#[derive(Debug, Clone, Copy)]
struct RunBounds {
    deadline: IsolatedDeadline,
    cancel_stop: IsolatedCancelStop,
}

/// Run one task on the child and answer it.
async fn run_one(
    shared: &PoolShared,
    channel: &mut ChildChannel,
    frame: RunFrame,
    log: AttemptLog,
    bounds: RunBounds,
    answer: &mut TaskAnswer,
) -> AfterRun {
    let task_id = frame.task_id;
    let bell = CancelBell::default();
    let child = HeldChild {
        id: channel.id(),
        pid: channel.pid(),
        spawner: shared.spawner.clone(),
        kill_mark: channel.kill_mark(),
        cancel_bell: bell.clone(),
    };
    let held = match shared.live.hold(task_id, frame.claimed_at, log, child) {
        Ok(held) => held,
        Err(RegistryClosed) => {
            // No beat would cover the task: it is not sent, and the child
            // is killed with the slot's lost-child path.
            tracing::error!(
                task_id = %task_id, attempt = log.attempt.get(), trace_id = log.trace_id.log_field(),
                pid = channel.pid().get(),
                "the runner heartbeats failed closed; the task is not sent",
            );
            answer.send(AttemptEnd::NotDispatched(
                NotRunCause::RunnerBeatsFailedClosed,
            ));
            return AfterRun::ChildGone {
                finished: TasksFinished::NoTask,
                reason: ReplaceReason::KilledByParent,
            };
        }
    };
    answer.child_took_it(channel.pid());
    // A refusal of the stored options runs no task code: a cancel of it
    // has nothing to stop, and its writes find the row moved.
    let watch = match &frame.work {
        IsolatedWork::Run => CancelWatch::Watching(&bell, &held),
        IsolatedWork::Refusal(_) => CancelWatch::NotWatching,
    };
    let parent_frame = ParentFrame::Run(frame);
    let sent = channel.send(&parent_frame).await;
    // The input is not kept once the frame is written.
    drop(parent_frame);
    let timeout = match bounds.deadline {
        IsolatedDeadline::NoDeadline => None,
        IsolatedDeadline::After(timeout) => Some(timeout),
    };
    let first = match sent {
        Ok(()) => {
            let until =
                timeout.map(|timeout| Instant::now() + Duration::from_millis(timeout.get()));
            match first_answer_or_cancel(shared, channel, until, watch).await {
                RunWait::Answer(first) => first,
                RunWait::CancelRequested => {
                    let cancel = CancelPhase {
                        task_id,
                        log,
                        until,
                        cancel_stop: bounds.cancel_stop,
                    };
                    return end_after_cancel(shared, channel, cancel, held, answer).await;
                }
            }
        }
        Err(SendFailure::Encode(error)) => {
            // Nothing reached the child: the frame failed in the slot's
            // own path, which is a defect (Python `_serve_slot`).
            tracing::error!(
                task_id = %task_id, attempt = log.attempt.get(), trace_id = log.trace_id.log_field(),
                error = %error,
                "the run frame could not be encoded: {error}",
            );
            drop(held);
            let lost = defect_kill(channel).await;
            answer.send(end_of_loss(shared, channel, lost).await);
            return AfterRun::Defect(SlotDefect::frame_not_encoded(&error));
        }
        Err(failure @ (SendFailure::Write(_) | SendFailure::TimedOut)) => {
            tracing::warn!(task_id = %task_id, attempt = log.attempt.get(), trace_id = log.trace_id.log_field(), failure = %failure, "the run frame could not be sent");
            drop(held);
            let lost = lost_after_kill(channel).await;
            answer.send(end_of_loss(shared, channel, lost).await);
            return AfterRun::ChildGone {
                finished: TasksFinished::NoTask,
                reason: reason_of_loss(&lost),
            };
        }
    };
    match first {
        FirstAnswer::Wait(FrameWait::Frame(ChildFrame::Result(result)))
            if result.task_id == task_id =>
        {
            drop(held);
            answer.send(AttemptEnd::reported(result.end, result.started_at));
            match result.child {
                ChildAfterFrame::ServesOn => AfterRun::Serve { rss: result.rss },
                ChildAfterFrame::EndsItself => AfterRun::EndsItself,
            }
        }
        FirstAnswer::Wait(FrameWait::Frame(ChildFrame::BodyReturned(returned)))
            if returned.task_id == task_id =>
        {
            tracing::debug!(
                task_id = %task_id, attempt = log.attempt.get(), trace_id = log.trace_id.log_field(),
                left = returned.left.count().get(),
                "the body returned with work left on its loop; the deadline no longer applies",
            );
            let body_return = BodyReturn {
                end: returned.returned,
                started_at: returned.started_at,
            };
            end_after_return(shared, channel, task_id, log, body_return, held, answer).await
        }
        FirstAnswer::Wait(FrameWait::Frame(other)) => {
            tracing::error!(task_id = %task_id, attempt = log.attempt.get(), trace_id = log.trace_id.log_field(), frame = ?other, "an isolated child sent a frame out of turn");
            drop(held);
            let lost = channel.discard_violating_child().await;
            answer.send(end_of_loss(shared, channel, lost).await);
            AfterRun::ChildGone {
                finished: TasksFinished::NoTask,
                reason: reason_of_loss(&lost),
            }
        }
        FirstAnswer::Wait(FrameWait::Lost(lost)) => {
            drop(held);
            answer.send(end_of_loss(shared, channel, lost).await);
            AfterRun::ChildGone {
                finished: TasksFinished::NoTask,
                reason: reason_of_loss(&lost),
            }
        }
        FirstAnswer::Wait(FrameWait::Expired) => {
            drop(held);
            let timed_out = timeout_end(timeout);
            answer.send(end_after_kill(shared, channel, timed_out).await);
            AfterRun::ChildGone {
                finished: TasksFinished::NoTask,
                reason: ReplaceReason::AtDeadline,
            }
        }
        FirstAnswer::DrainDeadline => {
            // Python `_kill_at_deadline` kills with `kill_for_caller`, so the
            // loss reads as a kill by the worker (`KILLED_BY_PARENT`), and
            // `_finalize_child_loss` writes its crash reason.
            drop(held);
            shared.drain_kills.note([task_id]);
            let lost = lost_after_kill(channel).await;
            answer.send(end_of_loss(shared, channel, lost).await);
            AfterRun::ChildGone {
                finished: TasksFinished::NoTask,
                reason: reason_of_loss(&lost),
            }
        }
    }
}

/// The end of a task whose deadline passed. Only a task with a deadline
/// can expire, so `timeout` is set; without one the wait has no bound.
fn timeout_end(timeout: Option<TaskTimeoutMs>) -> AttemptEnd {
    match timeout {
        Some(timeout) => AttemptEnd::TimedOut {
            timeout,
            retry: RetryPlacement::SameClass,
            cancel: TimeoutCancel::NoSignal,
            seen: TimeoutSeen::AtKill,
        },
        None => unreachable!("a wait with no deadline cannot pass its deadline"),
    }
}

/// What the wait after `cancel_run` needs to know of the run.
struct CancelPhase {
    task_id: uuid::Uuid,
    log: AttemptLog,
    /// The deadline of the run, if it has one.
    until: Option<Instant>,
    cancel_stop: IsolatedCancelStop,
}

/// An operator cancel was routed to the run: send `cancel_run`, then wait
/// for the end of the run, bounded by its deadline, by the end of the grace
/// of its cancel stop, and by the drain deadline of the pool. The end is
/// `Cancelled` unless the child ended the run before it read the cancel.
async fn end_after_cancel(
    shared: &PoolShared,
    channel: &mut ChildChannel,
    cancel: CancelPhase,
    held: HeldDispatch,
    answer: &mut TaskAnswer,
) -> AfterRun {
    let CancelPhase {
        task_id,
        log,
        until,
        cancel_stop,
    } = cancel;
    tracing::info!(
        task_id = %task_id, attempt = log.attempt.get(), trace_id = log.trace_id.log_field(),
        pid = channel.pid().get(), cancel_stop = ?cancel_stop,
        "an operator cancelled the task; sending the cancel to its child",
    );
    answer.cancel_sent();
    let sent_at = Instant::now();
    let sent = channel
        .send(&ParentFrame::CancelRun(CancelTarget::Isolated { task_id }))
        .await;
    match sent {
        Ok(()) => {}
        Err(SendFailure::Encode(error)) => {
            tracing::error!(
                task_id = %task_id, attempt = log.attempt.get(), trace_id = log.trace_id.log_field(),
                error = %error,
                "the cancel frame could not be encoded: {error}",
            );
            drop(held);
            let _lost = defect_kill(channel).await;
            answer.send(AttemptEnd::Cancelled(CancelledEnd::ChildKilled));
            return AfterRun::Defect(SlotDefect::frame_not_encoded(&error));
        }
        Err(failure @ (SendFailure::Write(_) | SendFailure::TimedOut)) => {
            tracing::warn!(
                task_id = %task_id, attempt = log.attempt.get(), trace_id = log.trace_id.log_field(),
                failure = %failure,
                "the cancel frame could not be sent; killing the child",
            );
            drop(held);
            let lost = lost_after_kill(channel).await;
            answer.send(AttemptEnd::Cancelled(CancelledEnd::ChildKilled));
            return AfterRun::ChildGone {
                finished: TasksFinished::NoTask,
                reason: reason_of_loss(&lost),
            };
        }
    }
    let bound = CancelBound::of(until, cancel_stop, sent_at);
    match first_answer(shared, channel, bound.until()).await {
        FirstAnswer::Wait(FrameWait::Frame(ChildFrame::RunCancelled(cancelled)))
            if cancelled.task_id == task_id =>
        {
            drop(held);
            answer.send(cancelled_end(&cancelled));
            after_cancelled_run(&cancelled)
        }
        FirstAnswer::Wait(FrameWait::Frame(ChildFrame::Result(result)))
            if result.task_id == task_id =>
        {
            // The run ended before the child read the cancel: its end is
            // written as without a cancel, and finds the row moved.
            drop(held);
            answer.send(AttemptEnd::reported(result.end, result.started_at));
            match result.child {
                ChildAfterFrame::ServesOn => AfterRun::Serve { rss: result.rss },
                ChildAfterFrame::EndsItself => AfterRun::EndsItself,
            }
        }
        FirstAnswer::Wait(FrameWait::Frame(ChildFrame::BodyReturned(returned)))
            if returned.task_id == task_id =>
        {
            // The body returned with work left before the child read the
            // cancel: as without a cancel.
            let body_return = BodyReturn {
                end: returned.returned,
                started_at: returned.started_at,
            };
            end_after_return(shared, channel, task_id, log, body_return, held, answer).await
        }
        FirstAnswer::Wait(FrameWait::Frame(other)) => {
            tracing::error!(task_id = %task_id, attempt = log.attempt.get(), trace_id = log.trace_id.log_field(), frame = ?other, "an isolated child sent a frame out of turn after a cancel");
            drop(held);
            let lost = channel.discard_violating_child().await;
            answer.send(AttemptEnd::Cancelled(CancelledEnd::ChildKilled));
            AfterRun::ChildGone {
                finished: TasksFinished::NoTask,
                reason: reason_of_loss(&lost),
            }
        }
        FirstAnswer::Wait(FrameWait::Lost(lost)) => {
            drop(held);
            answer.send(AttemptEnd::Cancelled(CancelledEnd::ChildLost));
            AfterRun::ChildGone {
                finished: TasksFinished::NoTask,
                reason: reason_of_loss(&lost),
            }
        }
        FirstAnswer::Wait(FrameWait::Expired) => {
            tracing::warn!(
                task_id = %task_id, attempt = log.attempt.get(), trace_id = log.trace_id.log_field(),
                bound = ?bound,
                "a cancelled task did not end by its deadline or its cancel grace; killing its child",
            );
            drop(held);
            answer.send(end_after_cancel_kill(channel).await);
            AfterRun::ChildGone {
                finished: TasksFinished::AtLeastOne,
                reason: bound.replace_reason(),
            }
        }
        FirstAnswer::DrainDeadline => {
            drop(held);
            shared.drain_kills.note([task_id]);
            let lost = lost_after_kill(channel).await;
            answer.send(AttemptEnd::Cancelled(CancelledEnd::ChildKilled));
            AfterRun::ChildGone {
                finished: TasksFinished::NoTask,
                reason: reason_of_loss(&lost),
            }
        }
    }
}

/// The end of a cancelled run that the child reported: before the body, or
/// after the signal. An isolated child sends no second cancel. Pure.
fn cancelled_end(cancelled: &RunCancelledFrame) -> AttemptEnd {
    AttemptEnd::Cancelled(CancelledEnd::reported(
        cancelled.body,
        SecondCancelAfterSignal::NotSent,
        AttemptStart::Reported {
            started_at: cancelled.started_at,
        },
    ))
}

/// What the slot does after a cancelled run that the child reported: a child
/// that ends itself is not served; work that the body left retires the
/// child; else the child serves on. Pure.
fn after_cancelled_run(cancelled: &RunCancelledFrame) -> AfterRun {
    match (&cancelled.task_loop, cancelled.child) {
        (_, ChildAfterFrame::EndsItself) => AfterRun::EndsItself,
        (CancelledLoop::WorkLeft(_), ChildAfterFrame::ServesOn) => AfterRun::Retire,
        (CancelledLoop::NoLeftWork(_), ChildAfterFrame::ServesOn) => {
            AfterRun::Serve { rss: cancelled.rss }
        }
    }
}

/// Kill the child of a cancelled run that did not end by its bound, and
/// wait for its exit. The cancel recorded the attempt, so the end does not
/// wait for an exit that is not seen: the child is disposed of by the slot.
async fn end_after_cancel_kill(channel: &mut ChildChannel) -> AttemptEnd {
    let _ = channel.kill_for_caller().await;
    match channel.wait_exit(channel.kill_wait()).await {
        ChildExit::Reaped(_) | ChildExit::ReapedElsewhere => {}
        ChildExit::ExitUnknown => tracing::warn!(
            pid = channel.pid().get(),
            "the exit of the killed child of a cancelled task was not seen",
        ),
    }
    AttemptEnd::Cancelled(CancelledEnd::ChildKilled)
}

/// Kill the child, wait for its exit, and end the task with `seen_end`
/// once the exit is seen.
async fn end_after_kill(
    shared: &PoolShared,
    channel: &mut ChildChannel,
    seen_end: AttemptEnd,
) -> AttemptEnd {
    let _ = channel.kill_for_caller().await;
    match channel.wait_exit(channel.kill_wait()).await {
        ChildExit::Reaped(_) | ChildExit::ReapedElsewhere => seen_end,
        ChildExit::ExitUnknown => exit_not_seen(shared, channel, seen_end).await,
    }
}

/// A lost child: the crash path once its exit is seen.
async fn end_of_loss(shared: &PoolShared, channel: &ChildChannel, lost: ChildLost) -> AttemptEnd {
    let crashed = AttemptEnd::of_lost_child(&lost);
    end_once_seen(shared, channel, lost, crashed).await
}

/// The end of a task whose child was lost by `lost`: `seen_end` when the
/// exit was seen, else the rule of an exit not seen.
async fn end_once_seen(
    shared: &PoolShared,
    channel: &ChildChannel,
    lost: ChildLost,
    seen_end: AttemptEnd,
) -> AttemptEnd {
    match lost.exit {
        ChildExit::Reaped(_) | ChildExit::ReapedElsewhere => seen_end,
        ChildExit::ExitUnknown => exit_not_seen(shared, channel, seen_end).await,
    }
}

/// Kill the child of a slot that hit a defect, wait for its exit, and
/// describe its end with the defect as its cause (Python `_clear_slot`).
async fn defect_kill(channel: &mut ChildChannel) -> ChildLost {
    let _ = channel.kill_for_caller().await;
    let _ = channel.wait_exit(channel.kill_wait()).await;
    channel.lost_for(LossCause::SlotDefect)
}

/// After a panic while a task ran on the child: kill the child, and answer
/// the task when the panic left it open, as at a loss (Python
/// `_clear_slot`, `SlotHandle.loss_outcome`).
async fn end_at_defect(shared: &PoolShared, channel: &mut ChildChannel, answer: &mut TaskAnswer) {
    let lost = defect_kill(channel).await;
    let Some((open, seen_end)) = answer.open_at_loss(&lost) else {
        return;
    };
    let end = match seen_end {
        // The cancel recorded the attempt; no write waits for the exit.
        cancelled @ AttemptEnd::Cancelled(_) => cancelled,
        other => end_once_seen(shared, channel, lost, other).await,
    };
    open.send(end);
}

/// The exit of a killed child was not seen: leave the row to recovery, or
/// wait for the exit as long as the pool lives.
async fn exit_not_seen(
    shared: &PoolShared,
    channel: &ChildChannel,
    seen_end: AttemptEnd,
) -> AttemptEnd {
    let unknown = AttemptEnd::EndUnknown { pid: channel.pid() };
    match shared.config.exit_not_seen {
        ExitNotSeen::LeaveToRecovery => unknown,
        ExitNotSeen::WaitForExit => {
            let mut exit = channel.exit_watch();
            loop {
                if *exit.borrow() != ExitState::Running {
                    return seen_end;
                }
                tokio::select! {
                    changed = exit.changed() => {
                        if changed.is_err() {
                            return unknown;
                        }
                    }
                    () = shared.board.stop.cancelled() => return unknown,
                }
            }
        }
    }
}

/// After a return with work left: wait for the report of that work, bounded
/// by [`ISOLATED_LEFTOVER_BACKSTOP`]. What the body returned, with the start
/// of its attempt, is the end at every end of the child whose exit is seen.
/// The task finished: its body returned.
async fn end_after_return(
    shared: &PoolShared,
    channel: &mut ChildChannel,
    task_id: uuid::Uuid,
    log: AttemptLog,
    returned: BodyReturn,
    held: HeldDispatch,
    answer: &mut TaskAnswer,
) -> AfterRun {
    let returned_end = AttemptEnd::returned(returned);
    answer.body_returned(returned_end.clone());
    let until = Some(Instant::now() + ISOLATED_LEFTOVER_BACKSTOP);
    match first_answer(shared, channel, until).await {
        FirstAnswer::Wait(FrameWait::Frame(ChildFrame::LeftWorkEnded(ended)))
            if ended.task_id == task_id =>
        {
            drop(held);
            answer.send(returned_end);
            // Cancelled code can leave broken state: the child is replaced.
            AfterRun::Retire
        }
        FirstAnswer::Wait(FrameWait::Frame(ChildFrame::LeftWorkStuck(stuck)))
            if stuck.task_id == task_id =>
        {
            tracing::warn!(
                task_id = %task_id, attempt = log.attempt.get(), trace_id = log.trace_id.log_field(),
                alive = stuck.still.count().get(),
                "the work a task left did not stop; killing its child",
            );
            drop(held);
            answer.send(end_after_kill(shared, channel, returned_end).await);
            AfterRun::ChildGone {
                finished: TasksFinished::AtLeastOne,
                reason: ReplaceReason::LeftoverWork,
            }
        }
        FirstAnswer::Wait(FrameWait::Expired) => {
            // The child did not report how the work it holds ended.
            drop(held);
            answer.send(end_after_kill(shared, channel, returned_end).await);
            AfterRun::ChildGone {
                finished: TasksFinished::AtLeastOne,
                reason: ReplaceReason::LeftoverWork,
            }
        }
        FirstAnswer::DrainDeadline => {
            drop(held);
            // The child still holds the work the task left on its loop
            // (Python `_kill_at_deadline`: the handle still has its child).
            shared.drain_kills.note([task_id]);
            answer.send(end_after_kill(shared, channel, returned_end).await);
            AfterRun::ChildGone {
                finished: TasksFinished::AtLeastOne,
                reason: ReplaceReason::KilledByParent,
            }
        }
        FirstAnswer::Wait(FrameWait::Frame(other)) => {
            tracing::error!(task_id = %task_id, attempt = log.attempt.get(), trace_id = log.trace_id.log_field(), frame = ?other, "an isolated child sent a frame out of turn");
            drop(held);
            let lost = channel.discard_violating_child().await;
            answer.send(end_once_seen(shared, channel, lost, returned_end).await);
            AfterRun::ChildGone {
                finished: TasksFinished::AtLeastOne,
                reason: reason_of_loss(&lost),
            }
        }
        FirstAnswer::Wait(FrameWait::Lost(lost)) => {
            drop(held);
            answer.send(end_once_seen(shared, channel, lost, returned_end).await);
            AfterRun::ChildGone {
                finished: TasksFinished::AtLeastOne,
                reason: reason_of_loss(&lost),
            }
        }
    }
}

#[cfg(test)]
mod answer_tests;
